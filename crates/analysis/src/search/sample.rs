//! [`PgProjectionSource`]: `ProjectionSource` over the search index's
//! documents and embeddings, dated by L7's watermark.
//!
//! A sample is every document confirmed in the spec's window that has an
//! embedding of the spec's model and that its pinned filter admits under
//! its version, reduced to the sample size by smallest sample key (BLAKE3
//! keyed by `derive_key("crosstalk projection sample v1", seed)` over the
//! transmission's ULID, little-endian), in ascending key order. The
//! candidates are read in one `REPEATABLE READ` snapshot and filtered in
//! Rust like a search's.

use crosstalk_spec::aggregates::projection::{FitFailure, PointRoute, ProjectionSpec};
use crosstalk_spec::aggregates::topic::Embedding;
use crosstalk_spec::ids::TransmissionId;
use crosstalk_spec::interfaces::l3_reconstruction::AgentDirectory;
use crosstalk_spec::interfaces::l5_flow::ChannelDirectory;
use crosstalk_spec::interfaces::l6_analysis::{ProjectionSource, Sample, SampleError, SampleRow};
use crosstalk_spec::interfaces::l7_topology::WatermarkRead;

use super::{Candidate, PgSearchIndex, TopicAssignments, admitted, aliases, retains};
use crate::pg::StorageFailure;
use crate::pg::codec::micros;

/// The spec's sample key of `transmission` under `seed`.
pub fn sample_key(seed: u64, transmission: TransmissionId) -> [u8; 32] {
    let key = blake3::derive_key("crosstalk projection sample v1", &seed.to_le_bytes());
    *blake3::keyed_hash(&key, &transmission.as_ulid().to_le_bytes()).as_bytes()
}

/// The projection source over a [`PgSearchIndex`]'s documents.
#[derive(Clone)]
pub struct PgProjectionSource<D, T, W> {
    index: PgSearchIndex<D, T>,
    watermark: W,
}

impl<D, T, W> PgProjectionSource<D, T, W> {
    pub fn new(index: PgSearchIndex<D, T>, watermark: W) -> Self {
        Self { index, watermark }
    }
}

fn sample_error(failure: impl Into<StorageFailure>) -> SampleError {
    SampleError::Store {
        reason: failure.into().reason(),
    }
}

type Row = (
    String,
    String,
    String,
    String,
    i64,
    Option<String>,
    Vec<f32>,
);

impl<D, T, W> ProjectionSource for PgProjectionSource<D, T, W>
where
    D: AgentDirectory + ChannelDirectory + Send + Sync,
    T: TopicAssignments,
    W: WatermarkRead + Send + Sync,
{
    async fn sample(&self, spec: &ProjectionSpec) -> Result<Sample, SampleError> {
        let index = &self.index;
        let version = spec.topic_version();
        let history = index
            .topics
            .history()
            .await
            .map_err(|error| SampleError::Store {
                reason: format!("topic catalog: {error:?}"),
            })?;
        if !retains(&history, version) {
            return Err(SampleError::Failed(FitFailure::VersionNotRetained {
                version,
            }));
        }
        let model = spec.embedding_model();
        let dimension = i32::from(model.dimension.get());
        let mut tx = index
            .pool
            .begin_with("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .await
            .map_err(sample_error)?;
        let dropped: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM analysis.search_dropped_models WHERE model_name = $1 AND model_dimension = $2)",
        )
        .bind(&model.name)
        .bind(dimension)
        .fetch_one(&mut *tx)
        .await
        .map_err(sample_error)?;
        if dropped {
            return Err(SampleError::Failed(FitFailure::EmbeddingModelUnavailable {
                model: model.clone(),
            }));
        }
        let watermark = self.watermark.current_watermark();
        let window = spec.window();
        let rows: Vec<Row> = sqlx::query_as(
            "SELECT d.transmission, d.from_agent, d.to_agent, d.route, d.confirmed_at, v.verdict, e.embedding::real[] \
               FROM analysis.search_docs AS d \
               JOIN analysis.search_embeddings AS e \
                 ON e.transmission = d.transmission AND e.model_name = $1 AND e.model_dimension = $2 \
               LEFT JOIN analysis.search_verdicts AS v ON v.transmission = d.transmission \
              WHERE d.confirmed_at >= $3 AND d.confirmed_at < $4",
        )
        .bind(&model.name)
        .bind(dimension)
        .bind(micros("window start", window.start()).map_err(sample_error)?)
        .bind(micros("window end", window.end()).map_err(sample_error)?)
        .fetch_all(&mut *tx)
        .await
        .map_err(sample_error)?;
        tx.commit().await.map_err(sample_error)?;
        let mut candidates = Vec::with_capacity(rows.len());
        for (id, from, to, route, at, verdict, values) in rows {
            let candidate = Candidate::decode(&id, &from, &to, &route, at, verdict.as_deref())
                .map_err(sample_error)?;
            let embedding =
                Embedding::new(model.clone(), values).map_err(|error| SampleError::Store {
                    reason: format!("stored embedding: {error:?}"),
                })?;
            candidates.push((candidate, embedding));
        }
        let plain: Vec<Candidate> = candidates.iter().map(|(c, _)| c.clone()).collect();
        let catalog = |error| SampleError::Store {
            reason: format!("topic catalog: {error:?}"),
        };
        let admits = admitted(
            &index.directory,
            &index.topics,
            version,
            spec.filter(),
            &plain,
        )
        .await
        .map_err(catalog)?;
        let kept: Vec<(Candidate, Embedding)> = candidates
            .into_iter()
            .zip(admits)
            .filter_map(|(candidate, admit)| admit.then_some(candidate))
            .collect();
        let ids: Vec<TransmissionId> = kept.iter().map(|(c, _)| c.transmission).collect();
        let topics = index
            .topics
            .assigned(version, &ids)
            .await
            .map_err(catalog)?;
        let resolve = aliases(&index.directory);
        let mut rows: Vec<SampleRow> = kept
            .into_iter()
            .map(|(candidate, embedding)| SampleRow {
                transmission: candidate.transmission,
                from: AgentDirectory::canonical(&index.directory, candidate.from),
                to: AgentDirectory::canonical(&index.directory, candidate.to),
                route: PointRoute::of(&candidate.route.resolved(resolve)),
                topic: topics.get(&candidate.transmission).copied().flatten(),
                confirmed_at: candidate.confirmed_at,
                embedding,
            })
            .collect();
        let matching = u64::try_from(rows.len()).unwrap_or(u64::MAX);
        let seed = spec.params().seed();
        rows.sort_by(|a, b| {
            sample_key(seed, a.transmission)
                .cmp(&sample_key(seed, b.transmission))
                .then_with(|| a.transmission.cmp(&b.transmission))
        });
        let limit = usize::try_from(spec.params().limit().get().get()).unwrap_or(usize::MAX);
        rows.truncate(limit);
        Ok(Sample {
            watermark,
            matching,
            rows,
        })
    }
}

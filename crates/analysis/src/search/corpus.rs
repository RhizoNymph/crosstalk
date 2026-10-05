//! `SearchCorpus` on [`PgSearchIndex`]: what `analyze` writes. None of it
//! publishes anything (search results are read, not announced). Each call
//! is one statement or one transaction.

use crosstalk_spec::aggregates::topic::EmbeddingModel;
use crosstalk_spec::derived::flow::verdict::{Observed, Verdict, VerdictRevision};
use crosstalk_spec::ids::TransmissionId;
use crosstalk_spec::interfaces::l3_reconstruction::AgentDirectory;
use crosstalk_spec::interfaces::l5_flow::ChannelDirectory;
use crosstalk_spec::interfaces::l6_analysis::corpus::{
    CorpusError, IndexedTransmission, SearchCorpus,
};

use super::{PgSearchIndex, TopicAssignments, corpus_error};
use crate::pg::codec::{id_text, micros, to_json};

fn dimension(model: &EmbeddingModel) -> i32 {
    i32::from(model.dimension.get())
}

impl<D, T> SearchCorpus for PgSearchIndex<D, T>
where
    D: AgentDirectory + ChannelDirectory + Send + Sync,
    T: TopicAssignments,
{
    /// The text and parties replace the stored ones; the embedding, if
    /// any, replaces the one of its model (other models' are kept).
    async fn index(&mut self, document: IndexedTransmission) -> Result<(), CorpusError> {
        let id = id_text(document.transmission);
        let route = to_json("route", &document.route).map_err(corpus_error)?;
        let at = micros("confirmed_at", document.confirmed_at).map_err(corpus_error)?;
        let mut tx = self.pool.begin().await.map_err(corpus_error)?;
        sqlx::query(
            "INSERT INTO analysis.search_docs (transmission, from_agent, to_agent, route, confirmed_at, body) \
             VALUES ($1, $2, $3, $4, $5, $6) \
             ON CONFLICT (transmission) DO UPDATE SET from_agent = EXCLUDED.from_agent, \
               to_agent = EXCLUDED.to_agent, route = EXCLUDED.route, \
               confirmed_at = EXCLUDED.confirmed_at, body = EXCLUDED.body",
        )
        .bind(&id)
        .bind(id_text(document.from))
        .bind(id_text(document.to))
        .bind(route)
        .bind(at)
        .bind(&document.text)
        .execute(&mut *tx)
        .await
        .map_err(corpus_error)?;
        if let Some(embedding) = &document.embedding {
            sqlx::query(
                "INSERT INTO analysis.search_embeddings (transmission, model_name, model_dimension, embedding) \
                 VALUES ($1, $2, $3, $4::real[]::vector) \
                 ON CONFLICT (transmission, model_name, model_dimension) DO UPDATE SET embedding = EXCLUDED.embedding",
            )
            .bind(&id)
            .bind(&embedding.model().name)
            .bind(dimension(embedding.model()))
            .bind(embedding.values())
            .execute(&mut *tx)
            .await
            .map_err(corpus_error)?;
        }
        tx.commit().await.map_err(corpus_error)
    }

    async fn remove(&mut self, transmission: TransmissionId) -> Result<(), CorpusError> {
        // The embeddings go with it (ON DELETE CASCADE).
        sqlx::query("DELETE FROM analysis.search_docs WHERE transmission = $1")
            .bind(id_text(transmission))
            .execute(&self.pool)
            .await
            .map_err(corpus_error)?;
        Ok(())
    }

    /// One conditional upsert, so concurrent verdicts for a transmission
    /// leave the newest revision whatever order they commit in: the row is
    /// written when absent or held at an older revision
    /// (`CurrentVerdict::observe`), and nothing changes otherwise.
    async fn judge(
        &mut self,
        transmission: TransmissionId,
        verdict: Option<Verdict>,
        revision: VerdictRevision,
    ) -> Result<Observed, CorpusError> {
        let json = verdict
            .map(|verdict| to_json("verdict", &verdict))
            .transpose()
            .map_err(corpus_error)?;
        let written = sqlx::query(
            "INSERT INTO analysis.search_verdicts (transmission, verdict, revision) VALUES ($1, $2, $3) \
             ON CONFLICT (transmission) DO UPDATE SET verdict = EXCLUDED.verdict, revision = EXCLUDED.revision \
             WHERE analysis.search_verdicts.revision < EXCLUDED.revision",
        )
        .bind(id_text(transmission))
        .bind(json)
        .bind(i64::from(revision.get().get()))
        .execute(&self.pool)
        .await
        .map_err(corpus_error)?;
        Ok(if written.rows_affected() == 0 {
            Observed::Stale
        } else {
            Observed::Newer
        })
    }

    async fn set_model(&mut self, model: EmbeddingModel) -> Result<(), CorpusError> {
        sqlx::query(
            "INSERT INTO analysis.search_model (model_name, model_dimension) VALUES ($1, $2) \
             ON CONFLICT (singleton) DO UPDATE SET model_name = EXCLUDED.model_name, \
               model_dimension = EXCLUDED.model_dimension",
        )
        .bind(&model.name)
        .bind(dimension(&model))
        .execute(&self.pool)
        .await
        .map_err(corpus_error)?;
        Ok(())
    }

    async fn drop_model(&mut self, model: &EmbeddingModel) -> Result<(), CorpusError> {
        let mut tx = self.pool.begin().await.map_err(corpus_error)?;
        sqlx::query(
            "DELETE FROM analysis.search_embeddings WHERE model_name = $1 AND model_dimension = $2",
        )
        .bind(&model.name)
        .bind(dimension(model))
        .execute(&mut *tx)
        .await
        .map_err(corpus_error)?;
        sqlx::query(
            "INSERT INTO analysis.search_dropped_models (model_name, model_dimension) VALUES ($1, $2) \
             ON CONFLICT DO NOTHING",
        )
        .bind(&model.name)
        .bind(dimension(model))
        .execute(&mut *tx)
        .await
        .map_err(corpus_error)?;
        tx.commit().await.map_err(corpus_error)
    }
}

//! `EdgeStore::transmissions`: the stored contributions behind one edge.
//!
//! Served from `topology.contributions`, so the window need not be
//! bucket-aligned. Rows are resolved and filtered as the fold does, with
//! each transmission's verdict read in the same snapshot, then listed
//! newest `confirmed_at` first (ties by transmission id, descending).
//!
//! **Cursors** are rows of `topology.cursors`: an opaque random token, the
//! request it was issued for (the edge, window and filter as JSON) and
//! what it resumes with (the version the first page resolved, and the last
//! row served). A token the table does not hold, or holds for another
//! request, is `InvalidCursor`; a token whose version has since been
//! dropped is `Version(NotRetained)`.

use crosstalk_spec::aggregates::edge::{
    EdgeSelector, EdgeTransmission, EdgeTransmissionPage, TopologyFilter,
};
use crosstalk_spec::aggregates::filter::{FilterSubject, VersionUnavailable};
use crosstalk_spec::aggregates::topic::TopicModelVersion;
use crosstalk_spec::aggregates::watermark::Watermarked;
use crosstalk_spec::ids::TransmissionId;
use crosstalk_spec::interfaces::l7_topology::EdgeQueryError;
use crosstalk_spec::paging::{Cursor, EdgeTransmissionList, Page, PageRequest};
use crosstalk_spec::support::{NonEmpty, TimeWindow, Timestamp};
use sqlx::Row;

use super::PgEdgeStore;
use super::error::{DbError, ReadResult};
use super::read::{read_head, resolve_version};
use crate::codec;
use crate::env::{EnvAliases, TopologyEnv};

/// Where a traversal resumes: after this row.
#[derive(Debug, Clone, Copy)]
struct Resume {
    confirmed_at: Timestamp,
    transmission: TransmissionId,
}

/// The request a cursor is bound to, as stored.
fn binding(
    edge: &EdgeSelector,
    window: TimeWindow,
    filter: &TopologyFilter,
) -> Result<String, DbError> {
    serde_json::to_string(&(edge, window, filter))
        .map_err(|error| DbError::inconsistent(format!("request does not encode: {error}")))
}

impl<V: TopologyEnv> PgEdgeStore<V> {
    pub(super) async fn transmissions_impl(
        &self,
        edge: &EdgeSelector,
        window: TimeWindow,
        filter: &TopologyFilter,
        page: &PageRequest<EdgeTransmissionList>,
    ) -> ReadResult<Watermarked<EdgeTransmissionPage>> {
        let binding = binding(edge, window, filter)?;
        let env = self.env.as_ref();
        let mut snapshot = self.snapshot().await?;
        let (watermark, dropped) = read_head(&mut snapshot).await?;
        let (version, resume) = match &page.after {
            None => (resolve_version(env, &dropped, filter).await?, None),
            Some(cursor) => {
                let row = sqlx::query(
                    "SELECT binding, version, confirmed_at, transmission FROM topology.cursors \
                     WHERE token = $1",
                )
                .bind(cursor.token())
                .fetch_optional(&mut *snapshot)
                .await?;
                let Some(row) = row else {
                    return Err(EdgeQueryError::InvalidCursor.into());
                };
                let bound: String = row.try_get("binding")?;
                if bound != binding {
                    return Err(EdgeQueryError::InvalidCursor.into());
                }
                let version = codec::stored_version(row.try_get("version")?)?;
                if dropped.contains(&version) {
                    return Err(
                        EdgeQueryError::Version(VersionUnavailable::NotRetained(version)).into(),
                    );
                }
                let resume = Resume {
                    confirmed_at: codec::timestamp("cursor time", row.try_get("confirmed_at")?)?,
                    transmission: codec::stored_transmission(row.try_get("transmission")?)?,
                };
                (version, Some(resume))
            }
        };
        let stored = sqlx::query(
            "SELECT c.transmission, c.from_agent, c.to_agent, c.route, c.topic, c.at_micros, \
             c.matched_bytes, coalesce(v.verdict = 1, false) AS false_detection \
             FROM topology.contributions c LEFT JOIN topology.verdicts v ON v.transmission = c.transmission \
             WHERE c.version = $1 AND c.at_micros >= $2 AND c.at_micros < $3",
        )
        .bind(codec::version(version))
        .bind(codec::time(window.start())?)
        .bind(codec::time(window.end())?)
        .fetch_all(&mut *snapshot)
        .await?;
        snapshot.commit().await?;

        let aliases = EnvAliases(env);
        let from = env.canonical_agent(edge.from());
        let to = env.canonical_agent(edge.to());
        let route = edge.route().resolved(aliases);
        let mut rows = Vec::new();
        for row in &stored {
            let sender = env.canonical_agent(codec::stored_agent(row.try_get("from_agent")?)?);
            let reader = env.canonical_agent(codec::stored_agent(row.try_get("to_agent")?)?);
            if sender != from || reader != to {
                continue;
            }
            let routed = codec::stored_route(row.try_get("route")?)?.resolved(aliases);
            if routed != route {
                continue;
            }
            let topic: Option<String> = row.try_get("topic")?;
            let topic = topic.as_deref().map(codec::stored_topic).transpose()?;
            let subject = FilterSubject {
                from,
                to,
                route: &route,
                topic,
                false_detection: row.try_get("false_detection")?,
            };
            if !filter.admits(&subject, aliases) {
                continue;
            }
            let one = EdgeTransmission {
                transmission: codec::stored_transmission(row.try_get("transmission")?)?,
                confirmed_at: codec::timestamp("contribution time", row.try_get("at_micros")?)?,
                matched_bytes: codec::stored_count("matched bytes", row.try_get("matched_bytes")?)?,
                topic,
            };
            let after = resume.is_none_or(|resume| {
                (one.confirmed_at, one.transmission) < (resume.confirmed_at, resume.transmission)
            });
            if after {
                rows.push(one);
            }
        }
        rows.sort_by(|a, b| {
            (b.confirmed_at, b.transmission).cmp(&(a.confirmed_at, a.transmission))
        });
        let page = self.cut(rows, page, &binding, version).await?;
        Ok(Watermarked {
            watermark,
            value: EdgeTransmissionPage {
                topic_version: version,
                page,
            },
        })
    }

    /// One page of `rows`; when more follow, a cursor issued for `binding`
    /// resuming after the page's last row.
    async fn cut(
        &self,
        mut rows: Vec<EdgeTransmission>,
        page: &PageRequest<EdgeTransmissionList>,
        binding: &str,
        version: TopicModelVersion,
    ) -> Result<Page<EdgeTransmission, EdgeTransmissionList>, DbError> {
        let overflow = |_| DbError::inconsistent("cut more rows than the page size");
        let limit = usize::from(page.size.get().get());
        if rows.len() <= limit {
            return Page::last(page.size, rows).map_err(overflow);
        }
        rows.truncate(limit);
        let last = rows
            .last()
            .copied()
            .ok_or_else(|| DbError::inconsistent("an empty page with more to follow"))?;
        let token: String = sqlx::query_scalar(
            "INSERT INTO topology.cursors (token, binding, version, confirmed_at, transmission) \
             VALUES (replace(gen_random_uuid()::text, '-', ''), $1, $2, $3, $4) RETURNING token",
        )
        .bind(binding)
        .bind(codec::version(version))
        .bind(codec::time(last.confirmed_at)?)
        .bind(codec::transmission(last.transmission))
        .fetch_one(&self.pool)
        .await?;
        let next = Cursor::from_token(token)
            .map_err(|error| DbError::inconsistent(format!("issued a bad cursor: {error:?}")))?;
        let items = NonEmpty::from_vec(rows)
            .ok_or_else(|| DbError::inconsistent("an empty page with more to follow"))?;
        Page::more(page.size, items, next).map_err(overflow)
    }
}

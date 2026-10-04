//! The store's writes: applies, verdicts, the version lifecycle and the
//! watermark.
//!
//! Every write that depends on the watermark, the active version or a
//! version's status locks the `state` row first, in `READ COMMITTED`:
//! applies `FOR SHARE` (they run concurrently), activation, retention and
//! the watermark `FOR UPDATE` (they wait for in-flight applies, and applies
//! wait for them). Each statement after the lock reads what the last
//! control change committed.

use crosstalk_spec::aggregates::access::AccessEdge;
use crosstalk_spec::aggregates::edge::{EdgeKey, TopicSlot};
use crosstalk_spec::aggregates::topic::TopicModelVersion;
use crosstalk_spec::aggregates::watermark::{PipelineFrontier, Watermark};
use crosstalk_spec::derived::flow::verdict::{Observed, Verdict, VerdictRevision};
use crosstalk_spec::events::BusEvent;
use crosstalk_spec::events::changed::Changed;
use crosstalk_spec::events::insight::{ClassificationCause, InsightEvent};
use crosstalk_spec::ids::TransmissionId;
use crosstalk_spec::interfaces::l7_topology::{
    AccessContribution, Activation, EdgeContribution, EdgeError,
};
use crosstalk_spec::support::{TimeWindow, Timestamp};
use sqlx::{PgConnection, Row};

use super::error::{DbError, Failed, WriteResult};
use super::partition::Bucketed;
use super::{PgEdgeStore, bucket_of};
use crate::codec;
use crate::outbox::enqueue;

/// How a write locks the state row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Lock {
    Share,
    Update,
}

/// The state row as a write sees it.
#[derive(Debug, Clone, Copy)]
struct State {
    active: TopicModelVersion,
    watermark: Watermark,
}

/// A version's row; a version the store has not heard of is neither.
#[derive(Debug, Clone, Copy, Default)]
struct Status {
    ready: Option<u64>,
    dropped: bool,
}

async fn lock_state(conn: &mut PgConnection, lock: Lock) -> Result<State, DbError> {
    let sql = match lock {
        Lock::Share => "SELECT active_version, watermark_micros FROM topology.state FOR SHARE",
        Lock::Update => "SELECT active_version, watermark_micros FROM topology.state FOR UPDATE",
    };
    let row = sqlx::query(sql).fetch_one(conn).await?;
    Ok(State {
        active: codec::stored_version(row.try_get("active_version")?)?,
        watermark: Watermark(codec::timestamp(
            "watermark",
            row.try_get("watermark_micros")?,
        )?),
    })
}

async fn status(conn: &mut PgConnection, version: TopicModelVersion) -> Result<Status, DbError> {
    let row = sqlx::query("SELECT ready_count, dropped FROM topology.versions WHERE version = $1")
        .bind(codec::version(version))
        .fetch_optional(conn)
        .await?;
    let Some(row) = row else {
        return Ok(Status::default());
    };
    let ready: Option<i64> = row.try_get("ready_count")?;
    Ok(Status {
        ready: ready
            .map(|count| codec::stored_sum("ready count", count))
            .transpose()?,
        dropped: row.try_get("dropped")?,
    })
}

/// The key `contribution` lands in, in `bucket`.
fn key_of(contribution: &EdgeContribution, bucket: TimeWindow) -> WriteResult<EdgeKey> {
    EdgeKey::new(
        contribution.from,
        contribution.to,
        contribution.route.clone(),
        TopicSlot {
            version: contribution.classification.version,
            topic: contribution.classification.topic,
        },
        bucket,
    )
    .map_err(|_| EdgeError::SelfEdge.into())
}

fn no_bucket(at: Timestamp) -> EdgeError {
    EdgeError::Store {
        reason: format!("no bucket holds {at:?}"),
    }
}

impl<V> PgEdgeStore<V> {
    /// The key a stored contribution landed in, if (`version`,
    /// `transmission`) has been applied.
    async fn stored_key(
        &self,
        conn: &mut PgConnection,
        version: TopicModelVersion,
        transmission: TransmissionId,
    ) -> Result<Option<EdgeKey>, DbError> {
        let row = sqlx::query(
            "SELECT from_agent, to_agent, route, topic, at_micros FROM topology.contributions \
             WHERE version = $1 AND transmission = $2",
        )
        .bind(codec::version(version))
        .bind(codec::transmission(transmission))
        .fetch_optional(conn)
        .await?;
        let Some(row) = row else { return Ok(None) };
        let topic: Option<String> = row.try_get("topic")?;
        let at = codec::timestamp("contribution time", row.try_get("at_micros")?)?;
        let bucket = bucket_of(self.config.bucket_width, at)
            .ok_or_else(|| DbError::inconsistent("a stored contribution has no bucket"))?;
        EdgeKey::new(
            codec::stored_agent(row.try_get("from_agent")?)?,
            codec::stored_agent(row.try_get("to_agent")?)?,
            codec::stored_route(row.try_get("route")?)?,
            TopicSlot {
                version,
                topic: topic.as_deref().map(codec::stored_topic).transpose()?,
            },
            bucket,
        )
        .map(Some)
        .map_err(|_| DbError::inconsistent("a stored contribution is a self-edge"))
    }

    /// `EdgeStore::apply`. Three round trips in the common case: the
    /// state lock, the version's status and any stored contribution in one
    /// query; the contribution, its bucket and its traffic row in one
    /// statement; the commit.
    pub(super) async fn apply_impl(&self, contribution: &EdgeContribution) -> WriteResult<EdgeKey> {
        let version = contribution.classification.version;
        let bucket = bucket_of(self.config.bucket_width, contribution.at)
            .ok_or_else(|| no_bucket(contribution.at))?;
        let bucket_start = codec::time(bucket.start())?;
        let self_edge = contribution.from == contribution.to;
        if !self_edge {
            self.partitions
                .ensure(&self.pool, Bucketed::Edges, bucket_start)
                .await?;
        }
        let mut tx = self.pool.begin().await?;
        let row = sqlx::query(
            "SELECT s.watermark_micros, coalesce(v.activated, false) AS activated, \
             coalesce(v.dropped, false) AS dropped, \
             c.from_agent, c.to_agent, c.route, c.topic, c.at_micros \
             FROM topology.state s \
             LEFT JOIN topology.versions v ON v.version = $1 \
             LEFT JOIN topology.contributions c ON c.version = $1 AND c.transmission = $2 \
             FOR SHARE OF s",
        )
        .bind(codec::version(version))
        .bind(codec::transmission(contribution.transmission))
        .fetch_one(&mut *tx)
        .await?;
        if row.try_get::<bool, _>("dropped")? {
            return Err(EdgeError::VersionNotRetained { version }.into());
        }
        let watermark = Watermark(codec::timestamp(
            "watermark",
            row.try_get("watermark_micros")?,
        )?);
        let mut changed = false;
        let result: WriteResult<EdgeKey> = if self_edge {
            Err(EdgeError::SelfEdge.into())
        } else if let Some(key) = self.key_from_row(&row, version)? {
            Ok(key)
        } else {
            if row.try_get::<bool, _>("activated")? && watermark.finalizes(bucket) {
                return Err(EdgeError::LateContribution { bucket, watermark }.into());
            }
            changed = self
                .insert_contribution(&mut tx, contribution, bucket)
                .await?;
            if changed {
                key_of(contribution, bucket)
            } else {
                // A concurrent apply of the same (version, transmission)
                // stored it first: return what it stored.
                match self
                    .stored_key(&mut tx, version, contribution.transmission)
                    .await?
                {
                    Some(key) => Ok(key),
                    None => {
                        Err(DbError::inconsistent("an applied contribution is not stored").into())
                    }
                }
            }
        };
        if contribution.cause == ClassificationCause::Refit
            && matches!(result, Ok(_) | Err(Failed::Refused(EdgeError::SelfEdge)))
        {
            sqlx::query(
                "INSERT INTO topology.refit_processed (version, transmission) VALUES ($1, $2) \
                 ON CONFLICT DO NOTHING",
            )
            .bind(codec::version(version))
            .bind(codec::transmission(contribution.transmission))
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        if changed {
            self.wake.poke();
        }
        result
    }

    /// The key of the stored contribution a row joined, if any.
    fn key_from_row(
        &self,
        row: &sqlx::postgres::PgRow,
        version: TopicModelVersion,
    ) -> Result<Option<EdgeKey>, DbError> {
        let Some(at) = row.try_get::<Option<i64>, _>("at_micros")? else {
            return Ok(None);
        };
        let at = codec::timestamp("contribution time", at)?;
        let bucket = bucket_of(self.config.bucket_width, at)
            .ok_or_else(|| DbError::inconsistent("a stored contribution has no bucket"))?;
        let from: Option<String> = row.try_get("from_agent")?;
        let to: Option<String> = row.try_get("to_agent")?;
        let route: Option<String> = row.try_get("route")?;
        let topic: Option<String> = row.try_get("topic")?;
        let (Some(from), Some(to), Some(route)) = (from, to, route) else {
            return Err(DbError::inconsistent(
                "a stored contribution misses a column",
            ));
        };
        EdgeKey::new(
            codec::stored_agent(&from)?,
            codec::stored_agent(&to)?,
            codec::stored_route(&route)?,
            TopicSlot {
                version,
                topic: topic.as_deref().map(codec::stored_topic).transpose()?,
            },
            bucket,
        )
        .map(Some)
        .map_err(|_| DbError::inconsistent("a stored contribution is a self-edge"))
    }

    /// Store `contribution`, count it into its bucket and record the
    /// bucket's traffic, in one statement, unless a concurrent apply of the
    /// same (version, transmission) stored it first. Returns whether this
    /// call counted it.
    async fn insert_contribution(
        &self,
        conn: &mut PgConnection,
        contribution: &EdgeContribution,
        bucket: TimeWindow,
    ) -> Result<bool, DbError> {
        let counted: i64 = sqlx::query_scalar(
            "WITH inserted AS ( \
               INSERT INTO topology.contributions \
               (version, transmission, from_agent, to_agent, route, topic, at_micros, matched_bytes) \
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8) ON CONFLICT DO NOTHING RETURNING version \
             ), counted AS ( \
               INSERT INTO topology.edge_buckets \
               (version, bucket_start, from_agent, to_agent, route, topic, transmissions, matched_bytes) \
               SELECT $1, $9, $3, $4, $5, $10, 1, $8 FROM inserted \
               ON CONFLICT (version, bucket_start, from_agent, to_agent, route, topic) DO UPDATE SET \
               transmissions = edge_buckets.transmissions + 1, \
               matched_bytes = edge_buckets.matched_bytes + EXCLUDED.matched_bytes \
             ), traffic AS ( \
               INSERT INTO topology.outbox (traffic_start, traffic_end) SELECT $9, $11 FROM inserted \
             ) SELECT count(*) FROM inserted",
        )
        .bind(codec::version(contribution.classification.version))
        .bind(codec::transmission(contribution.transmission))
        .bind(codec::agent(contribution.from))
        .bind(codec::agent(contribution.to))
        .bind(codec::route(&contribution.route)?)
        .bind(contribution.classification.topic.map(codec::topic))
        .bind(codec::time(contribution.at)?)
        .bind(codec::count("matched bytes", contribution.matched_bytes)?)
        .bind(codec::time(bucket.start())?)
        .bind(codec::bucket_topic(contribution.classification.topic))
        .bind(codec::time(bucket.end())?)
        .fetch_one(conn)
        .await?;
        Ok(counted == 1)
    }

    /// `EdgeStore::judge`: the verdict copy keeps the newest revision.
    pub(super) async fn judge_impl(
        &self,
        transmission: TransmissionId,
        verdict: Option<Verdict>,
        revision: VerdictRevision,
    ) -> WriteResult<Observed> {
        let newer = sqlx::query(
            "INSERT INTO topology.verdicts (transmission, verdict, revision) VALUES ($1, $2, $3) \
             ON CONFLICT (transmission) DO UPDATE SET verdict = EXCLUDED.verdict, \
             revision = EXCLUDED.revision WHERE verdicts.revision < EXCLUDED.revision",
        )
        .bind(codec::transmission(transmission))
        .bind(codec::verdict(verdict))
        .bind(i64::from(revision.get().get()))
        .execute(&self.pool)
        .await?
        .rows_affected()
            == 1;
        Ok(if newer {
            Observed::Newer
        } else {
            Observed::Stale
        })
    }

    /// `EdgeStore::version_ready`: the first count received is kept.
    pub(super) async fn version_ready_impl(
        &self,
        version: TopicModelVersion,
        transmissions: u64,
    ) -> WriteResult<()> {
        let mut tx = self.pool.begin().await?;
        lock_state(&mut tx, Lock::Share).await?;
        if status(&mut tx, version).await?.dropped {
            return Err(EdgeError::VersionNotRetained { version }.into());
        }
        sqlx::query(
            "INSERT INTO topology.versions (version, ready_count) VALUES ($1, $2) \
             ON CONFLICT (version) DO UPDATE SET \
             ready_count = COALESCE(versions.ready_count, EXCLUDED.ready_count)",
        )
        .bind(codec::version(version))
        .bind(codec::micros("ready count", transmissions)?)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }

    /// `EdgeStore::activate`: switch queries to `version` once its refit
    /// classifications are all processed, publishing
    /// `TopicVersionActivated` from the switching transaction.
    pub(super) async fn activate_impl(
        &self,
        version: TopicModelVersion,
    ) -> WriteResult<Activation> {
        let mut tx = self.pool.begin().await?;
        let state = lock_state(&mut tx, Lock::Update).await?;
        let status = status(&mut tx, version).await?;
        if status.dropped {
            return Err(EdgeError::VersionNotRetained { version }.into());
        }
        if version <= state.active {
            return Ok(Activation::Ignored);
        }
        let Some(expected) = status.ready else {
            return Ok(Activation::Pending);
        };
        let processed: i64 =
            sqlx::query_scalar("SELECT count(*) FROM topology.refit_processed WHERE version = $1")
                .bind(codec::version(version))
                .fetch_one(&mut *tx)
                .await?;
        if codec::stored_sum("processed", processed)? < expected {
            return Ok(Activation::Pending);
        }
        let previous = state.active;
        sqlx::query("UPDATE topology.state SET active_version = $1")
            .bind(codec::version(version))
            .execute(&mut *tx)
            .await?;
        sqlx::query(
            "INSERT INTO topology.versions (version, activated) VALUES ($1, true) \
             ON CONFLICT (version) DO UPDATE SET activated = true",
        )
        .bind(codec::version(version))
        .execute(&mut *tx)
        .await?;
        enqueue(
            &mut tx,
            &BusEvent::Insight(InsightEvent::TopicVersionActivated { version, previous }),
        )
        .await?;
        tx.commit().await?;
        self.wake.poke();
        tracing::info!(
            version = version.0,
            previous = previous.0,
            "topic version activated"
        );
        Ok(Activation::Switched { version, previous })
    }

    /// `EdgeStore::drop_version`: mark the version dropped and delete its
    /// buckets and contributions in one transaction.
    pub(super) async fn drop_impl(&self, version: TopicModelVersion) -> WriteResult<()> {
        let mut tx = self.pool.begin().await?;
        let state = lock_state(&mut tx, Lock::Update).await?;
        if status(&mut tx, version).await?.dropped {
            return Ok(());
        }
        if version >= state.active {
            return Err(EdgeError::VersionInUse { version }.into());
        }
        let stored = codec::version(version);
        sqlx::query(
            "INSERT INTO topology.versions (version, dropped) VALUES ($1, true) \
             ON CONFLICT (version) DO UPDATE SET dropped = true",
        )
        .bind(stored)
        .execute(&mut *tx)
        .await?;
        for sql in [
            "DELETE FROM topology.edge_buckets WHERE version = $1",
            "DELETE FROM topology.contributions WHERE version = $1",
            "DELETE FROM topology.refit_processed WHERE version = $1",
        ] {
            sqlx::query(sql).bind(stored).execute(&mut *tx).await?;
        }
        tx.commit().await?;
        tracing::info!(version = version.0, "topic version dropped");
        Ok(())
    }

    /// `EdgeStore::advance_watermark`: expose the settled watermark if it is
    /// later, publishing `WatermarkAdvanced` and `Changed::Watermark` from
    /// the transaction that persists it.
    pub(super) async fn advance_impl(
        &self,
        frontier: PipelineFrontier,
    ) -> WriteResult<Option<Watermark>> {
        let settled = Watermark::settled(frontier, self.config.timing, self.config.bucket_width);
        let mut tx = self.pool.begin().await?;
        let state = lock_state(&mut tx, Lock::Update).await?;
        let Some(advanced) = state.watermark.advance(settled) else {
            return Ok(None);
        };
        sqlx::query("UPDATE topology.state SET watermark_micros = $1")
            .bind(codec::time(advanced.at())?)
            .execute(&mut *tx)
            .await?;
        enqueue(
            &mut tx,
            &BusEvent::Insight(InsightEvent::WatermarkAdvanced(advanced)),
        )
        .await?;
        enqueue(&mut tx, &BusEvent::Changed(Changed::Watermark(advanced))).await?;
        tx.commit().await?;
        self.wake.poke();
        tracing::debug!(watermark = advanced.at().as_micros(), "watermark advanced");
        Ok(Some(advanced))
    }

    /// `EdgeStore::apply_access`: count an access once, return its bucket.
    /// One statement stores the access, counts it and records the
    /// bucket's traffic; a redelivery reads the stored access's bucket.
    pub(super) async fn apply_access_impl(
        &self,
        access: &AccessContribution,
    ) -> WriteResult<AccessEdge> {
        let bucket =
            bucket_of(self.config.bucket_width, access.at).ok_or_else(|| no_bucket(access.at))?;
        self.partitions
            .ensure(&self.pool, Bucketed::Accesses, codec::time(bucket.start())?)
            .await?;
        let counted: Option<i64> = sqlx::query_scalar(
            "WITH inserted AS ( \
               INSERT INTO topology.accesses (access, agent, resource, op, at_micros) \
               VALUES ($1, $2, $3, $4, $5) ON CONFLICT DO NOTHING RETURNING access \
             ), counted AS ( \
               INSERT INTO topology.access_buckets (bucket_start, agent, resource, op, accesses) \
               SELECT $6, $2, $3, $4, 1 FROM inserted \
               ON CONFLICT (bucket_start, agent, resource, op) \
               DO UPDATE SET accesses = access_buckets.accesses + 1 RETURNING accesses \
             ), traffic AS ( \
               INSERT INTO topology.outbox (traffic_start, traffic_end) SELECT $6, $7 FROM inserted \
             ) SELECT (SELECT accesses FROM counted)",
        )
        .bind(access.access.ulid_text())
        .bind(codec::agent(access.agent))
        .bind(codec::resource(access.resource))
        .bind(codec::op(access.op))
        .bind(codec::time(access.at)?)
        .bind(codec::time(bucket.start())?)
        .bind(codec::time(bucket.end())?)
        .fetch_one(&self.pool)
        .await?;
        if let Some(accesses) = counted {
            self.wake.poke();
            return Ok(AccessEdge {
                agent: access.agent,
                resource: access.resource,
                op: access.op,
                bucket,
                accesses: codec::stored_count("accesses", accesses)?,
            });
        }
        let stored = sqlx::query(
            "SELECT a.agent, a.resource, a.op, b.bucket_start, b.accesses \
             FROM topology.accesses a JOIN topology.access_buckets b \
             ON b.agent = a.agent AND b.resource = a.resource AND b.op = a.op \
             AND b.bucket_start = a.at_micros - a.at_micros % $2 \
             WHERE a.access = $1",
        )
        .bind(access.access.ulid_text())
        .bind(codec::micros(
            "bucket width",
            self.config.bucket_width.as_micros().get(),
        )?)
        .fetch_one(&self.pool)
        .await?;
        Ok(AccessEdge {
            agent: codec::stored_agent(stored.try_get("agent")?)?,
            resource: codec::stored_resource(stored.try_get("resource")?)?,
            op: codec::stored_op(stored.try_get("op")?)?,
            bucket: codec::bucket(stored.try_get("bucket_start")?, self.width())?,
            accesses: codec::stored_count("accesses", stored.try_get("accesses")?)?,
        })
    }
}

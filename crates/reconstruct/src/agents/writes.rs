//! The store's writes: the merge log, renames, the agent lifecycle, claims
//! and activity, and the outbox they publish through.

use std::sync::Arc;

use crosstalk_spec::events::BusEvent;
use crosstalk_spec::ids::{AgentId, MergeId, OperatorId};
use crosstalk_spec::interfaces::l3_reconstruction::agents::ActivityStore;
use crosstalk_spec::interfaces::l3_reconstruction::lifecycle::{
    Advance, AgentLifecycle, AgentLifecycleError, AgentOrigin, NewAgent,
};
use crosstalk_spec::interfaces::l3_reconstruction::{
    ClaimStore, IdentityResolver, Resolution, ResolveError,
};
use crosstalk_spec::observed::agent::{
    AgentLabel, AgentState, ClaimSet, IdentityEvidence, MergeRecord, MergeRequest, Reversal,
};
use crosstalk_spec::observed::client::HarnessClaim;
use crosstalk_spec::support::{Change, NonEmpty, Timestamp};
use crosstalk_store::{TxError, retry_serializable};
use sqlx::PgConnection;

use super::codec::{count, id_text, json, micros};
use super::table::{Diff, Table, changed};
use super::{PgAgents, load, resolve};
use crate::error::{StorageFailure, StoreReason, TxFailure, tx};
use crate::ids::IdSource;
use crate::publish::EventSink;

/// The variant tag of `evidence` on the wire.
pub(super) fn evidence_kind(evidence: &IdentityEvidence) -> &'static str {
    match evidence {
        IdentityEvidence::HarnessAgent { .. } => "harness_agent",
        IdentityEvidence::HarnessSession { .. } => "harness_session",
        IdentityEvidence::Account(_) => "account",
        IdentityEvidence::StableCredential(_) => "stable_credential",
        IdentityEvidence::RotatingCredential(_) => "rotating_credential",
        IdentityEvidence::PromptFingerprint(_) => "prompt_fingerprint",
    }
}

/// Write `agent`'s state and label as `table` holds them.
async fn write_agent(conn: &mut PgConnection, table: &Table, id: AgentId) -> Result<(), TxFailure> {
    let Some(agent) = table.agents.get(&id) else {
        return Err(StorageFailure::Inconsistent {
            reason: format!("changed agent {} is not in the table", id.ulid_text()),
        }
        .into());
    };
    sqlx::query(
        "UPDATE reconstruct.agents SET state = $2, merged_into = $3, label = $4 WHERE id = $1",
    )
    .bind(id_text(id))
    .bind(json(&agent.state)?)
    .bind(agent.state.merged_into().map(id_text))
    .bind(agent.label.as_ref().map(|label| label.as_str().to_owned()))
    .execute(&mut *conn)
    .await?;
    Ok(())
}

/// Write what `diff` names, as `table` holds it now.
async fn persist(conn: &mut PgConnection, table: &Table, diff: &Diff) -> Result<(), TxFailure> {
    // Agents a merge points elsewhere first: the merged_into foreign key
    // only needs the target row, which every change keeps.
    for id in &diff.agents {
        write_agent(conn, table, *id).await?;
    }
    if let Some(merge) = diff.merge {
        let Some(record) = table.merges.get(&merge) else {
            return Err(StorageFailure::Inconsistent {
                reason: format!("merge {} is not in the table", merge.ulid_text()),
            }
            .into());
        };
        sqlx::query(
            "INSERT INTO reconstruct.merges (id, source, target, record, reverted) \
             VALUES ($1, $2, $3, $4, $5) \
             ON CONFLICT (id) DO UPDATE SET record = EXCLUDED.record, reverted = EXCLUDED.reverted",
        )
        .bind(id_text(merge))
        .bind(id_text(record.source()))
        .bind(id_text(record.target()))
        .bind(json(record)?)
        .bind(record.reverted().is_some())
        .execute(&mut *conn)
        .await?;
    }
    for (a, b) in &diff.vetoes_removed {
        sqlx::query("DELETE FROM reconstruct.vetoes WHERE a = $1 AND b = $2")
            .bind(id_text(*a))
            .bind(id_text(*b))
            .execute(&mut *conn)
            .await?;
    }
    for veto in &diff.vetoes_added {
        sqlx::query(
            "INSERT INTO reconstruct.vetoes (a, b, veto) VALUES ($1, $2, $3) \
             ON CONFLICT (a, b) DO UPDATE SET veto = EXCLUDED.veto",
        )
        .bind(id_text(veto.a()))
        .bind(id_text(veto.b()))
        .bind(json(veto)?)
        .execute(&mut *conn)
        .await?;
    }
    Ok(())
}

/// Append `events` to the outbox, returning their rows.
pub(super) async fn outbox(
    conn: &mut PgConnection,
    events: &[BusEvent],
) -> Result<Vec<i64>, TxFailure> {
    let mut seqs = Vec::with_capacity(events.len());
    for event in events {
        let (seq,): (i64,) =
            sqlx::query_as("INSERT INTO reconstruct.outbox (event) VALUES ($1) RETURNING seq")
                .bind(json(event)?)
                .fetch_one(&mut *conn)
                .await?;
        seqs.push(seq);
    }
    Ok(seqs)
}

impl<S, M> PgAgents<S, M>
where
    S: EventSink,
    M: IdSource<MergeId> + 'static,
{
    /// Hand committed events to the sink, then delete their outbox rows. A
    /// sink failure leaves the rows for [`PgAgents::flush_outbox`].
    pub(super) async fn publish(&self, events: Vec<BusEvent>, seqs: Vec<i64>) {
        if events.is_empty() {
            return;
        }
        let count = events.len();
        match self.sink.publish(events).await {
            Ok(()) => {
                let deleted = sqlx::query("DELETE FROM reconstruct.outbox WHERE seq = ANY($1)")
                    .bind(&seqs)
                    .execute(&self.pool)
                    .await;
                if let Err(error) = deleted {
                    tracing::warn!(events = count, error = %error, "published outbox rows not deleted; they will be published again");
                }
            }
            Err(error) => {
                tracing::warn!(events = count, error = %error, "events not published; left in the outbox");
            }
        }
    }

    /// Publish every event left in the outbox (a sink failed, or the
    /// process stopped between a commit and its publish), oldest first.
    /// Returns how many were published.
    pub async fn flush_outbox(&self) -> Result<usize, StorageFailure> {
        let rows: Vec<(i64, String)> =
            sqlx::query_as("SELECT seq, event FROM reconstruct.outbox ORDER BY seq")
                .fetch_all(&self.pool)
                .await?;
        let mut events = Vec::with_capacity(rows.len());
        let mut seqs = Vec::with_capacity(rows.len());
        for (seq, event) in rows {
            events.push(super::codec::from_json("outbox.event", &event)?);
            seqs.push(seq);
        }
        let published = events.len();
        self.sink
            .publish(events)
            .await
            .map_err(|error| StorageFailure::Blobs {
                reason: format!("outbox not published: {error}"),
            })?;
        sqlx::query("DELETE FROM reconstruct.outbox WHERE seq = ANY($1)")
            .bind(&seqs)
            .execute(&self.pool)
            .await?;
        Ok(published)
    }

    /// Point the directory cache at what a committed merge or unmerge left.
    fn repoint_cache(&self, pointers: &[(AgentId, Option<AgentId>)]) {
        for (agent, into) in pointers {
            self.directory.point(*agent, *into);
        }
    }

    /// Run a merge-log decision in a serializable transaction over the
    /// whole merge table, persist it, and publish its events.
    async fn decide<T, F>(&self, decide: F) -> Result<T, ResolveError>
    where
        T: Clone + Send + 'static,
        F: Fn(&mut Table) -> Result<super::table::Applied<T>, ResolveError> + Send + Sync + 'static,
    {
        let decide = Arc::new(decide);
        let (applied, seqs, pointers) = retry_serializable(&self.pool, &self.retry, |conn| {
            let decide = Arc::clone(&decide);
            Box::pin(async move {
                let mut table = load::merge_table(conn).await.map_err(tx)?;
                let applied = decide(&mut table).map_err(TxError::Abort)?;
                persist(conn, &table, &applied.diff).await.map_err(tx)?;
                let seqs = outbox(conn, &applied.events).await.map_err(tx)?;
                let pointers: Vec<(AgentId, Option<AgentId>)> = applied
                    .diff
                    .agents
                    .iter()
                    .map(|id| {
                        let into = table
                            .agents
                            .get(id)
                            .and_then(|agent| agent.state.merged_into());
                        (*id, into)
                    })
                    .collect();
                Ok((applied, seqs, pointers))
            })
        })
        .await
        .map_err(ResolveError::from_tx)?;
        self.repoint_cache(&pointers);
        let value = applied.value.clone();
        self.publish(applied.events, seqs).await;
        Ok(value)
    }
}

impl<S, M> IdentityResolver for PgAgents<S, M>
where
    S: EventSink,
    M: IdSource<MergeId> + 'static,
{
    async fn merge(
        &mut self,
        request: MergeRequest,
        at: Timestamp,
    ) -> Result<MergeRecord, ResolveError> {
        let ids = Arc::clone(&self.merge_ids);
        let record = self
            .decide(move |table| {
                table.merge(request, at, || {
                    ids.next_id(at)
                        .map_err(|error| ResolveError::from_failure(&StorageFailure::Ids(error)))
                })
            })
            .await?;
        tracing::debug!(merge = %record.id().ulid_text(), from = %record.source().ulid_text(), into = %record.target().ulid_text(), "agents merged");
        Ok(record)
    }

    async fn unmerge(
        &mut self,
        merge: MergeId,
        by: OperatorId,
        at: Timestamp,
    ) -> Result<Reversal, ResolveError> {
        let reversal = self
            .decide(move |table| table.unmerge(merge, by, at))
            .await?;
        tracing::debug!(merge = %merge.ulid_text(), restored = reversal.restored.len(), "merge reverted");
        Ok(reversal)
    }

    async fn rename(
        &mut self,
        agent: AgentId,
        label: Option<AgentLabel>,
        by: OperatorId,
    ) -> Result<Change, ResolveError> {
        let (applied, seqs) = retry_serializable(&self.pool, &self.retry, |conn| {
            let label = label.clone();
            Box::pin(async move {
                let stored = load::agent(conn, agent).await.map_err(tx)?;
                let mut table = Table::default();
                if let Some(stored) = stored {
                    table.agents.insert(agent, stored);
                }
                let applied = table.rename(agent, label, by).map_err(TxError::Abort)?;
                persist(conn, &table, &applied.diff).await.map_err(tx)?;
                let seqs = outbox(conn, &applied.events).await.map_err(tx)?;
                Ok((applied, seqs))
            })
        })
        .await
        .map_err(ResolveError::from_tx)?;
        let change = applied.value;
        self.publish(applied.events, seqs).await;
        Ok(change)
    }

    async fn resolve(
        &self,
        evidence: &NonEmpty<IdentityEvidence>,
    ) -> Result<Resolution, ResolveError> {
        resolve::resolve(&self.pool, evidence)
            .await
            .map_err(|failure| ResolveError::from_failure(&failure))
    }
}

/// Insert `agent`'s evidence rows from `position` on.
async fn insert_evidence(
    conn: &mut PgConnection,
    agent: AgentId,
    from: usize,
    evidence: &[IdentityEvidence],
) -> Result<(), TxFailure> {
    for (offset, item) in evidence.iter().enumerate() {
        sqlx::query(
            "INSERT INTO reconstruct.agent_evidence (agent, position, item, kind) \
             VALUES ($1, $2, $3, $4)",
        )
        .bind(id_text(agent))
        .bind(count(from + offset)?)
        .bind(json(item)?)
        .bind(evidence_kind(item))
        .execute(&mut *conn)
        .await?;
    }
    Ok(())
}

/// Record activity: keep the latest time per agent.
async fn record_activity(
    conn: &mut PgConnection,
    agent: AgentId,
    at: Timestamp,
) -> Result<(), TxFailure> {
    sqlx::query(
        "INSERT INTO reconstruct.activity (agent, last_seen) VALUES ($1, $2) \
         ON CONFLICT (agent) DO UPDATE \
         SET last_seen = GREATEST(reconstruct.activity.last_seen, EXCLUDED.last_seen)",
    )
    .bind(id_text(agent))
    .bind(micros(at)?)
    .execute(&mut *conn)
    .await?;
    Ok(())
}

/// The canonical agent of a stored `id`, `None` when `id` is not stored.
async fn stored_canonical(
    conn: &mut PgConnection,
    id: AgentId,
) -> Result<Option<AgentId>, TxFailure> {
    let row: Option<(String,)> =
        sqlx::query_as("SELECT coalesce(merged_into, id) FROM reconstruct.agents WHERE id = $1")
            .bind(id_text(id))
            .fetch_optional(&mut *conn)
            .await?;
    Ok(row
        .map(|(canonical,)| super::codec::id_of("agents.merged_into", &canonical))
        .transpose()?)
}

impl<S, M> PgAgents<S, M>
where
    S: EventSink,
    M: IdSource<MergeId> + 'static,
{
    /// Run a lifecycle write in a serializable transaction and publish the
    /// events it returns.
    async fn lifecycle<F>(&self, write: F) -> Result<(), AgentLifecycleError>
    where
        F: for<'c> Fn(
                &'c mut PgConnection,
            )
                -> crosstalk_store::TxFuture<'c, Vec<BusEvent>, AgentLifecycleError>
            + Send
            + Sync
            + 'static,
    {
        let write = Arc::new(write);
        let (events, seqs) = retry_serializable(&self.pool, &self.retry, |conn| {
            let write = Arc::clone(&write);
            Box::pin(async move {
                let events = write(&mut *conn).await?;
                let seqs = outbox(conn, &events).await.map_err(tx)?;
                Ok((events, seqs))
            })
        })
        .await
        .map_err(AgentLifecycleError::from_tx)?;
        self.publish(events, seqs).await;
        Ok(())
    }
}

impl<S, M> AgentLifecycle for PgAgents<S, M>
where
    S: EventSink,
    M: IdSource<MergeId> + 'static,
{
    async fn create(&mut self, agent: NewAgent) -> Result<(), AgentLifecycleError> {
        let agent = Arc::new(agent);
        self.lifecycle(move |conn| {
            let agent = Arc::clone(&agent);
            Box::pin(async move {
                let (exists,): (bool,) = sqlx::query_as(
                    "SELECT EXISTS (SELECT 1 FROM reconstruct.agents WHERE id = $1)",
                )
                .bind(id_text(agent.id))
                .fetch_one(&mut *conn)
                .await?;
                if exists {
                    return Err(TxError::Abort(AgentLifecycleError::DuplicateAgent(
                        agent.id,
                    )));
                }
                let state = match agent.origin {
                    AgentOrigin::Config { at } => AgentState::Registered { at },
                    AgentOrigin::Traffic { first_seen } => AgentState::Provisional { first_seen },
                };
                sqlx::query(
                    "INSERT INTO reconstruct.agents (id, parent, state, merged_into, label) \
                     VALUES ($1, $2, $3, NULL, $4)",
                )
                .bind(id_text(agent.id))
                .bind(agent.parent.map(id_text))
                .bind(json(&state).map_err(|e| tx(e.into()))?)
                .bind(agent.label.as_ref().map(|label| label.as_str().to_owned()))
                .execute(&mut *conn)
                .await?;
                let evidence: Vec<IdentityEvidence> = agent.evidence.iter().cloned().collect();
                insert_evidence(conn, agent.id, 0, &evidence)
                    .await
                    .map_err(tx)?;
                if let AgentOrigin::Traffic { first_seen } = agent.origin {
                    record_activity(conn, agent.id, first_seen)
                        .await
                        .map_err(tx)?;
                }
                let parent = match agent.parent {
                    Some(parent) => stored_canonical(conn, parent).await.map_err(tx)?,
                    None => None,
                };
                Ok(changed(std::iter::once(agent.id).chain(parent)))
            })
        })
        .await
    }

    async fn advance(
        &mut self,
        agent: AgentId,
        advance: Advance,
    ) -> Result<(), AgentLifecycleError> {
        self.lifecycle(move |conn| {
            Box::pin(async move {
                let row: Option<(String,)> =
                    sqlx::query_as("SELECT state FROM reconstruct.agents WHERE id = $1")
                        .bind(id_text(agent))
                        .fetch_optional(&mut *conn)
                        .await?;
                let Some((state,)) = row else {
                    return Err(TxError::Abort(AgentLifecycleError::UnknownAgent(agent)));
                };
                let state: AgentState =
                    super::codec::from_json("agents.state", &state).map_err(|e| tx(e.into()))?;
                let next = match (&state, advance) {
                    (AgentState::Registered { .. }, Advance::FirstTraffic { at }) => {
                        AgentState::Provisional { first_seen: at }
                    }
                    (AgentState::Provisional { .. }, Advance::Establish { since }) => {
                        AgentState::Established { since }
                    }
                    _ => {
                        return Err(TxError::Abort(AgentLifecycleError::IllegalTransition {
                            agent,
                        }));
                    }
                };
                if let Advance::FirstTraffic { at } = advance {
                    record_activity(conn, agent, at).await.map_err(tx)?;
                }
                sqlx::query("UPDATE reconstruct.agents SET state = $2 WHERE id = $1")
                    .bind(id_text(agent))
                    .bind(json(&next).map_err(|e| tx(e.into()))?)
                    .execute(&mut *conn)
                    .await?;
                Ok(changed([agent]))
            })
        })
        .await
    }

    async fn attach_evidence(
        &mut self,
        agent: AgentId,
        evidence: IdentityEvidence,
    ) -> Result<(), AgentLifecycleError> {
        let evidence = Arc::new(evidence);
        self.lifecycle(move |conn| {
            let evidence = Arc::clone(&evidence);
            Box::pin(async move {
                let (exists, held, next): (bool, bool, i32) = sqlx::query_as(
                    "SELECT \
                         EXISTS (SELECT 1 FROM reconstruct.agents WHERE id = $1), \
                         EXISTS (SELECT 1 FROM reconstruct.agent_evidence WHERE agent = $1 AND item = $2), \
                         coalesce((SELECT max(position) + 1 FROM reconstruct.agent_evidence WHERE agent = $1), 0)",
                )
                .bind(id_text(agent))
                .bind(json(&*evidence).map_err(|e| tx(e.into()))?)
                .fetch_one(&mut *conn)
                .await?;
                if !exists {
                    return Err(TxError::Abort(AgentLifecycleError::UnknownAgent(agent)));
                }
                if held {
                    return Err(TxError::Abort(AgentLifecycleError::DuplicateEvidence(agent)));
                }
                let from = super::codec::count_of("agent_evidence.position", next)
                    .map_err(|e| tx(e.into()))?;
                insert_evidence(conn, agent, from as usize, std::slice::from_ref(&*evidence))
                    .await
                    .map_err(tx)?;
                Ok(changed([agent]))
            })
        })
        .await
    }
}

impl<S, M> ClaimStore for PgAgents<S, M>
where
    S: EventSink,
    M: IdSource<MergeId> + 'static,
{
    async fn record(
        &mut self,
        agent: AgentId,
        claim: &HarnessClaim,
        at: Timestamp,
    ) -> Result<(), ResolveError> {
        let write = async {
            sqlx::query(
                "INSERT INTO reconstruct.claims (agent, claim, last_seen) VALUES ($1, $2, $3) \
                 ON CONFLICT (agent, claim) DO UPDATE \
                 SET last_seen = GREATEST(reconstruct.claims.last_seen, EXCLUDED.last_seen)",
            )
            .bind(id_text(agent))
            .bind(json(claim)?)
            .bind(micros(at)?)
            .execute(&self.pool)
            .await?;
            Ok::<(), StorageFailure>(())
        };
        write
            .await
            .map_err(|failure| ResolveError::from_failure(&failure))
    }

    async fn claims(&self, agent: AgentId) -> Result<ClaimSet, ResolveError> {
        let read = async {
            let mut conn = self.pool.acquire().await?;
            let (sets, _) = load::cluster_activity(&mut conn, agent).await?;
            Ok::<ClaimSet, TxFailure>(ClaimSet::union(sets.values()))
        };
        read.await
            .map_err(|failure| ResolveError::from_failure(&failure.into()))
    }
}

impl<S, M> ActivityStore for PgAgents<S, M>
where
    S: EventSink,
    M: IdSource<MergeId> + 'static,
{
    async fn record(&mut self, agent: AgentId, at: Timestamp) -> Result<(), ResolveError> {
        let write = async {
            let mut conn = self.pool.acquire().await?;
            record_activity(&mut conn, agent, at).await
        };
        write
            .await
            .map_err(|failure| ResolveError::from_failure(&failure.into()))
    }

    async fn last_seen(&self, agent: AgentId) -> Result<Option<Timestamp>, ResolveError> {
        let read = async {
            let mut conn = self.pool.acquire().await?;
            let (_, last_seen) = load::cluster_activity(&mut conn, agent).await?;
            Ok::<Option<Timestamp>, TxFailure>(last_seen)
        };
        read.await
            .map_err(|failure| ResolveError::from_failure(&failure.into()))
    }
}

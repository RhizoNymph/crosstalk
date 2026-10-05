//! `IdentityResolver::resolve` on Postgres: which stored agents hold an
//! exchange's most specific evidence.
//!
//! One read-only `REPEATABLE READ` transaction (one snapshot of the agent
//! table):
//!
//! 1. Only the most specific items decide (`IdentityEvidence::specificity`
//!    at its highest); less specific items never make agents conflict
//!    (`reconstruct.resolve.most-specific-evidence-decides`).
//! 2. An agent holds an item when one of its evidence rows has the item's
//!    exact wire JSON, so harness ids under two scopes never meet
//!    (`reconstruct.resolve.harness-ids-scoped`). A `HarnessSession` item is
//!    held only by an agent with no `HarnessAgent` evidence: the session's
//!    main agent (`reconstruct.resolve.session-resolves-to-main-agent`).
//! 3. Holders resolve through the merge table (`agents.merged_into`).
//!
//! Labels and harness claims are never read
//! (`reconstruct.agent-label.never-evidence`).

use std::collections::{BTreeMap, BTreeSet};

use crosstalk_spec::ids::AgentId;
use crosstalk_spec::interfaces::l3_reconstruction::Resolution;
use crosstalk_spec::observed::agent::IdentityEvidence;
use crosstalk_spec::support::NonEmpty;
use sqlx::PgPool;

use super::codec::{id_of, id_text, json};
use crate::error::{StorageFailure, TxFailure};

/// Resolve `evidence` against the stored agents.
pub(super) async fn resolve(
    pool: &PgPool,
    evidence: &NonEmpty<IdentityEvidence>,
) -> Result<Resolution, StorageFailure> {
    let top = evidence
        .iter()
        .map(IdentityEvidence::specificity)
        .max()
        .unwrap_or_else(|| evidence.first().specificity());
    let deciding: Vec<IdentityEvidence> = evidence
        .iter()
        .filter(|item| item.specificity() == top)
        .cloned()
        .collect();
    let mut keys: BTreeMap<String, &IdentityEvidence> = BTreeMap::new();
    for item in &deciding {
        keys.insert(json(item)?, item);
    }
    let read = async {
        let mut tx = pool
            .begin_with("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .await?;
        let rows: Vec<(String, String, String, bool)> = sqlx::query_as(
            "SELECT e.agent, e.item, coalesce(a.merged_into, a.id), \
                    EXISTS (SELECT 1 FROM reconstruct.agent_evidence h \
                            WHERE h.agent = e.agent AND h.kind = 'harness_agent') \
             FROM reconstruct.agent_evidence e JOIN reconstruct.agents a ON a.id = e.agent \
             WHERE e.item = ANY($1)",
        )
        .bind(keys.keys().cloned().collect::<Vec<String>>())
        .fetch_all(&mut *tx)
        .await?;
        let mut holders: BTreeSet<AgentId> = BTreeSet::new();
        for (_agent, item, canonical, holds_harness_agent) in rows {
            let Some(item) = keys.get(&item) else {
                continue;
            };
            let counts = match item {
                IdentityEvidence::HarnessSession { .. } => !holds_harness_agent,
                IdentityEvidence::HarnessAgent { .. }
                | IdentityEvidence::Account(_)
                | IdentityEvidence::StableCredential(_)
                | IdentityEvidence::RotatingCredential(_)
                | IdentityEvidence::PromptFingerprint(_) => true,
            };
            if counts {
                holders.insert(id_of("agents.merged_into", &canonical)?);
            }
        }
        let mut canonical = holders.into_iter();
        let resolution = match (canonical.next(), canonical.next()) {
            (None, _) => Resolution::New {
                evidence: evidence.clone(),
            },
            (Some(agent), None) => {
                let held: Vec<(String,)> =
                    sqlx::query_as("SELECT item FROM reconstruct.agent_evidence WHERE agent = $1")
                        .bind(id_text(agent))
                        .fetch_all(&mut *tx)
                        .await?;
                let held: BTreeSet<String> = held.into_iter().map(|(item,)| item).collect();
                let mut new_evidence = Vec::new();
                for item in evidence.iter() {
                    if !held.contains(&json(item)?) {
                        new_evidence.push(item.clone());
                    }
                }
                Resolution::Known {
                    agent,
                    new_evidence,
                }
            }
            (Some(first), Some(second)) => {
                let mut candidates = NonEmpty::new(first);
                candidates.push(second);
                for more in canonical {
                    candidates.push(more);
                }
                let mut items = deciding.iter().cloned();
                let mut deciding_evidence =
                    NonEmpty::new(items.next().unwrap_or_else(|| evidence.first().clone()));
                for item in items {
                    deciding_evidence.push(item);
                }
                Resolution::Conflict {
                    candidates,
                    evidence: deciding_evidence,
                }
            }
        };
        tx.commit().await?;
        Ok::<Resolution, TxFailure>(resolution)
    };
    read.await.map_err(StorageFailure::from)
}

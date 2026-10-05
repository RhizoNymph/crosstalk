//! The threading oracle: after every step of a script, the outcome is
//! checked against the conversations stored before it, by the rules the
//! L3 invariants state, and the stored history against the concatenation
//! of the request suffixes the deltas were cut from (each delta's
//! `new_inputs` is its suffix less the messages another of the cluster's
//! conversations holds, `reconstruct.delta.excludes-seen-elsewhere`; the
//! scripts' clocks stay far inside the retention).

use std::collections::BTreeMap;

use crosstalk_spec::ids::{AgentId, ConversationId, MessageHash};
use crosstalk_spec::interfaces::l3_reconstruction::{ThreadOutcome, Threader};
use crosstalk_spec::observed::conversation::{Conversation, ConversationOrigin};
use crosstalk_spec::observed::message::Role;
use proptest::test_runner::TestCaseError;

use super::script::{Harness, Op, Step};
use crate::tests::support::MemoryThreader;
use crate::thread::store::outcome_conversation;
use crate::thread::{ConversationStore, MemoryConversations};

/// The conversations stored before a step.
pub(crate) type Snapshot = BTreeMap<ConversationId, Conversation>;

pub(crate) async fn snapshot<S: ConversationStore>(store: &S) -> Result<Snapshot, TestCaseError> {
    let mut snapshot = Snapshot::new();
    for id in store
        .conversations()
        .await
        .map_err(|error| TestCaseError::fail(format!("{error:?}")))?
    {
        if let Some(conversation) = store
            .conversation(id)
            .await
            .map_err(|error| TestCaseError::fail(format!("{error:?}")))?
        {
            snapshot.insert(id, conversation);
        }
    }
    Ok(snapshot)
}

fn lcp(a: &[MessageHash], b: &[MessageHash]) -> usize {
    a.iter().zip(b).take_while(|(x, y)| x == y).count()
}

fn check(condition: bool, what: impl FnOnce() -> String) -> Result<(), TestCaseError> {
    if condition {
        Ok(())
    } else {
        Err(TestCaseError::fail(what()))
    }
}

/// What the oracle remembers between steps.
#[derive(Debug, Default)]
pub(crate) struct Oracle {
    /// Each conversation's history as its deltas build it.
    pub(crate) expected: BTreeMap<ConversationId, Vec<MessageHash>>,
    /// The system message of each conversation's previous exchange.
    pub(crate) last_system: BTreeMap<ConversationId, Option<MessageHash>>,
    /// Every step's outcome, in order.
    pub(crate) outcomes: Vec<ThreadOutcome>,
    /// For each completed step with a response id: the conversation and
    /// history length through its response.
    pub(crate) responses: Vec<(usize, ConversationId, usize)>,
}

/// One step's request, analysed.
struct Request {
    non_system: Vec<MessageHash>,
    roles: BTreeMap<MessageHash, Role>,
    system: Option<MessageHash>,
}

impl Request {
    fn of(step: &Step) -> Self {
        let non_system = step
            .request
            .iter()
            .filter(|entry| entry.role != Role::System)
            .map(|entry| entry.message)
            .collect();
        let roles = step
            .request
            .iter()
            .map(|entry| (entry.message, entry.role))
            .collect();
        let system = step
            .request
            .iter()
            .rev()
            .find(|entry| entry.role == Role::System)
            .map(|entry| entry.message);
        Self {
            non_system,
            roles,
            system,
        }
    }

    fn has_assistant(&self, k: usize, outputs: &[MessageHash]) -> bool {
        self.non_system[..k].iter().any(|message| {
            self.roles.get(message) == Some(&Role::Assistant) || outputs.contains(message)
        })
    }
}

impl Oracle {
    /// Check `outcome` of `step` against `before`, then the stored
    /// conversation against the oracle's own history.
    pub(crate) async fn observe<S: ConversationStore>(
        &mut self,
        index: usize,
        step: &Step,
        cluster: &[AgentId],
        before: &Snapshot,
        outcome: &ThreadOutcome,
        store: &S,
    ) -> Result<(), TestCaseError> {
        let request = Request::of(step);
        let r = &request.non_system;
        let delta = outcome.delta();
        let conversation = outcome_conversation(outcome);
        let at = |what: &str| format!("step {index}: {what}: {outcome:?}");
        // delta.names-outcome-conversation, delta.output-is-response,
        // delta.agent-is-attributed.
        check(delta.conversation == conversation, || {
            at("delta names another conversation")
        })?;
        check(delta.output == step.output, || {
            at("delta output is not the response")
        })?;
        check(delta.agent == step.agent, || {
            at("delta agent is not the attributed agent")
        })?;
        let ours: Vec<(&ConversationId, &Conversation)> = before
            .iter()
            .filter(|(_, stored)| cluster.contains(&stored.agent))
            .collect();
        let outputs: Vec<MessageHash> = self.outputs(before);
        let longest_prefix = ours
            .iter()
            .filter(|(_, stored)| !stored.messages.is_empty())
            .filter(|(_, stored)| r.starts_with(&stored.messages))
            .map(|(_, stored)| stored.messages.len())
            .max();
        let best_fork = ours
            .iter()
            .map(|(_, stored)| lcp(r, &stored.messages))
            .filter(|k| *k > 0 && request.has_assistant(*k, &outputs))
            .max();
        let fresh = !before.contains_key(&conversation);
        // delta.excludes-seen-elsewhere: what another of the cluster's
        // conversations holds is withheld from new_inputs.
        let unseen = |suffix: &[MessageHash]| -> Vec<MessageHash> {
            suffix
                .iter()
                .copied()
                .filter(|message| {
                    !ours.iter().any(|(id, stored)| {
                        **id != conversation && stored.messages.contains(message)
                    })
                })
                .collect()
        };
        // The request messages the conversation stores after its base.
        let mut suffix: Vec<MessageHash> = Vec::new();
        match outcome {
            ThreadOutcome::Extends { .. } => {
                let stored = before
                    .get(&conversation)
                    .ok_or_else(|| TestCaseError::fail(at("extends an unstored conversation")))?;
                check(cluster.contains(&stored.agent), || {
                    at("extends another cluster's conversation")
                })?;
                // thread.extends-stored-prefix
                check(r.starts_with(&stored.messages), || {
                    at("extends a history that is not a prefix")
                })?;
                // thread.extends-longest-prefix-match
                check(Some(stored.messages.len()) == longest_prefix, || {
                    at(&format!("not the longest prefix ({longest_prefix:?})"))
                })?;
                // delta.new-inputs-are-request-suffix
                suffix = r[stored.messages.len()..].to_vec();
                check(delta.new_inputs == unseen(&suffix), || {
                    at("new inputs are not the suffix")
                })?;
                // delta.new-system-when-changed
                let previous = self.last_system.get(&conversation).copied().flatten();
                let expected = request.system.filter(|system| Some(*system) != previous);
                check(delta.new_system == expected, || {
                    at("new_system is not the changed system")
                })?;
            }
            other => {
                check(longest_prefix.is_none(), || {
                    at(&format!(
                        "a stored history of {longest_prefix:?} messages is a prefix"
                    ))
                })?;
                check(fresh, || at("a new conversation reuses a stored id"))?;
                // delta.new-system-when-changed: a new conversation's first.
                check(delta.new_system == request.system, || {
                    at("first new_system")
                })?;
                match other {
                    ThreadOutcome::Compacts { predecessor, .. } => {
                        let stored = before.get(predecessor).ok_or_else(|| {
                            TestCaseError::fail(at("compacts an unstored conversation"))
                        })?;
                        // thread.link-targets-exist
                        check(cluster.contains(&stored.agent), || {
                            at("predecessor of another cluster")
                        })?;
                        // thread.compaction-needs-content-evidence
                        let echoed = r.first().is_some_and(|first| outputs.contains(first));
                        check(step.summary.is_some() || echoed, || {
                            at("compaction without evidence")
                        })?;
                        // delta.compaction-excludes-carried-over
                        let expected: Vec<MessageHash> = r
                            .iter()
                            .copied()
                            .filter(|message| !stored.messages.contains(message))
                            .collect();
                        check(delta.new_inputs == unseen(&expected), || {
                            at("compaction new inputs")
                        })?;
                    }
                    ThreadOutcome::Forks {
                        parent,
                        shared_prefix,
                        ..
                    } => {
                        let k = *shared_prefix as usize;
                        let stored = before.get(parent).ok_or_else(|| {
                            TestCaseError::fail(at("forks an unstored conversation"))
                        })?;
                        check(cluster.contains(&stored.agent), || {
                            at("parent of another cluster")
                        })?;
                        check(k == lcp(r, &stored.messages), || {
                            at("shared prefix is not the lcp")
                        })?;
                        check(k < stored.messages.len(), || {
                            at("shared prefix out of bounds")
                        })?;
                        check(request.has_assistant(k, &outputs), || {
                            at("no assistant message shared")
                        })?;
                        check(Some(k) == best_fork, || {
                            at(&format!("parent not maximal ({best_fork:?})"))
                        })?;
                        suffix = r[k..].to_vec();
                        check(delta.new_inputs == unseen(&suffix), || {
                            at("fork new inputs")
                        })?;
                    }
                    ThreadOutcome::Starts { .. } => {
                        suffix = r.clone();
                        check(delta.new_inputs == unseen(&suffix), || {
                            at("start new inputs")
                        })?;
                        // thread.fork-or-start
                        check(best_fork.is_none(), || {
                            at(&format!(
                                "starts though a prefix of {best_fork:?} is shared"
                            ))
                        })?;
                    }
                    ThreadOutcome::Extends { .. } => {}
                }
            }
        }
        // The stored conversation: origin, and history = deltas.
        let expected = match outcome {
            ThreadOutcome::Starts { .. } => Vec::new(),
            ThreadOutcome::Forks {
                parent,
                shared_prefix,
                ..
            } => before
                .get(parent)
                .map(|stored| stored.messages[..*shared_prefix as usize].to_vec())
                .unwrap_or_default(),
            ThreadOutcome::Compacts { .. } => r
                .iter()
                .copied()
                .filter(|message| !delta.new_inputs.contains(message))
                .collect::<Vec<_>>(),
            ThreadOutcome::Extends { .. } => self
                .expected
                .get(&conversation)
                .cloned()
                .unwrap_or_default(),
        };
        let mut history = expected;
        if let ThreadOutcome::Compacts { .. } = outcome {
            // conversation.compaction-history: the first request's
            // non-system messages in request order, carried-over included.
            history = r.clone();
        } else {
            history.extend(suffix.iter().copied());
        }
        history.extend(delta.output);
        let stored = store
            .conversation(conversation)
            .await
            .map_err(|error| TestCaseError::fail(format!("{error:?}")))?
            .ok_or_else(|| TestCaseError::fail(at("outcome conversation not stored")))?;
        check(stored.messages == history, || {
            at(&format!(
                "stored history {:?} is not the deltas' {history:?}",
                stored.messages
            ))
        })?;
        let origin = match outcome {
            ThreadOutcome::Starts { .. } => ConversationOrigin::Root,
            ThreadOutcome::Extends { .. } => before
                .get(&conversation)
                .map(|stored| stored.origin)
                .unwrap_or(ConversationOrigin::Root),
            ThreadOutcome::Forks {
                parent,
                shared_prefix,
                ..
            } => ConversationOrigin::Fork {
                parent: *parent,
                shared_prefix: *shared_prefix,
            },
            ThreadOutcome::Compacts { predecessor, .. } => ConversationOrigin::Compaction {
                predecessor: *predecessor,
            },
        };
        check(stored.origin == origin, || at("stored origin"))?;
        check(
            stored.agent == before.get(&conversation).map_or(step.agent, |b| b.agent),
            || at("stored agent changed"),
        )?;
        self.expected.insert(conversation, history.clone());
        self.last_system.insert(conversation, request.system);
        self.outcomes.push(outcome.clone());
        if step.output.is_some() && step.exchange.outcome_response_id().is_some() {
            self.responses.push((index, conversation, history.len()));
        }
        Ok(())
    }

    fn outputs(&self, _before: &Snapshot) -> Vec<MessageHash> {
        self.outcomes
            .iter()
            .filter_map(|outcome| outcome.delta().output)
            .collect()
    }
}

/// Response ids of exchanges.
pub(crate) trait ResponseIdOf {
    fn outcome_response_id(&self) -> Option<String>;
}

impl ResponseIdOf for crosstalk_spec::observed::exchange::Exchange {
    fn outcome_response_id(&self) -> Option<String> {
        match &self.outcome {
            crosstalk_spec::observed::exchange::ExchangeOutcome::Completed {
                response_id, ..
            } => response_id.as_ref().map(|id| id.0.clone()),
            crosstalk_spec::observed::exchange::ExchangeOutcome::Failed { .. } => None,
        }
    }
}

/// A script played on a fresh harness and memory store, every step
/// checked. Returns the harness, threader and oracle for further checks.
pub(crate) async fn play(
    ops: &[Op],
) -> Result<(Harness, MemoryThreader, Oracle, Vec<Step>), TestCaseError> {
    let mut harness = Harness::new();
    let store = MemoryConversations::new();
    let mut threader = harness.scene.threader_in(store.clone(), harness.clusters());
    let mut oracle = Oracle::default();
    let mut steps = Vec::new();
    for (index, op) in ops.iter().enumerate() {
        let step = harness.step(op).await;
        let before = snapshot(&store).await?;
        let outcome = threader
            .thread(&step.exchange, step.agent)
            .await
            .map_err(|error| TestCaseError::fail(format!("step {index}: {error:?}")))?;
        let cluster = harness.cluster_of(step.agent);
        oracle
            .observe(index, &step, &cluster, &before, &outcome, &store)
            .await?;
        steps.push(step);
    }
    Ok((harness, threader, oracle, steps))
}

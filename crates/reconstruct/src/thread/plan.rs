//! The threading decision, shared by every [`super::ConversationStore`]:
//! given one call's input and read access to the stored conversations,
//! what the outcome is and what to write.
//!
//! In order:
//!
//! 1. **Already threaded.** The exchange's recorded outcome, unchanged
//!    (`reconstruct.thread.rethread-idempotent`).
//! 2. **Increment resolution.** An increment whose previous response is
//!    stored under the exchange's upstream and scope, in a conversation of
//!    the cluster, becomes the stored history through that response
//!    followed by the increment, and is threaded as that full history from
//!    here on (`reconstruct.thread.responses-state-agrees-with-prefix`). One
//!    whose previous response is not found `Starts` a conversation holding
//!    only the increment (`reconstruct.thread.unknown-previous-starts`).
//! 3. **Extends** the cluster conversation with the longest stored
//!    non-system history that is a prefix of the request's
//!    (`reconstruct.thread.extends-longest-prefix-match`).
//! 4. **Compacts** a cluster conversation when the request carries content
//!    evidence of summarizing one (`reconstruct.thread.compaction-needs-
//!    content-evidence`): a summary turn new to the cluster (the hint alone
//!    never counts), or a stored output echoed as the request's first
//!    message.
//! 5. **Forks** the cluster conversation sharing the longest common prefix
//!    with the request, when that prefix holds an assistant message
//!    (`reconstruct.thread.fork-or-start`).
//! 6. Otherwise **Starts** a root conversation.
//!
//! Every delta names the outcome's conversation, its `new_inputs` are the
//! non-system messages the conversation had not stored, less those the
//! cluster saw in another conversation within the store's retention
//! (`reconstruct.delta.excludes-seen-elsewhere`), its `new_system`
//! the request's system message when the conversation is new or that
//! message differs from the previous exchange's, and its `output` the
//! exchange's output.

use std::collections::HashSet;

use crosstalk_spec::events::ingest::ConversationDelta;
use crosstalk_spec::ids::{AgentId, ConversationId, MessageHash};
use crosstalk_spec::interfaces::l3_reconstruction::ThreadOutcome;
use crosstalk_spec::observed::conversation::ConversationOrigin;
use crosstalk_spec::observed::message::Role;

use super::history::{ChainHash, Entry, History};
use super::store::{RequestKind, ResponseKey, ThreadInput};
use crate::error::TxFailure;

/// The stored conversation an exchange extends.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Extension {
    pub(crate) conversation: ConversationId,
    /// Its non-system history length, at least 1.
    pub(crate) len: u32,
    pub(crate) last_system: Option<MessageHash>,
}

/// What the decision reads, inside the store's atomic step. Every lookup
/// is restricted to conversations of `members`; ties go to the most
/// recently threaded conversation.
pub(crate) trait ThreadReads: Send {
    /// The outcome recorded for `exchange`.
    fn recorded(
        &mut self,
        exchange: crosstalk_spec::ids::ExchangeId,
    ) -> impl Future<Output = Result<Option<ThreadOutcome>, TxFailure>> + Send;

    /// The conversation whose whole non-system history is the longest
    /// prefix of the request's: its head is `chains[len - 1]` for the
    /// largest such `len >= 1`.
    fn extension(
        &mut self,
        members: &[AgentId],
        chains: &[ChainHash],
    ) -> impl Future<Output = Result<Option<Extension>, TxFailure>> + Send;

    /// The conversation sharing the longest prefix with the request among
    /// prefixes of at least `from + 1` messages: the largest `k > from`
    /// with a stored history position whose chain is `chains[k - 1]`.
    fn common_prefix(
        &mut self,
        members: &[AgentId],
        chains: &[ChainHash],
        from: usize,
    ) -> impl Future<Output = Result<Option<(ConversationId, u32)>, TxFailure>> + Send;

    /// Where `key`'s response was stored: its conversation and the
    /// non-system history length through it.
    fn response(
        &mut self,
        key: &ResponseKey,
        members: &[AgentId],
    ) -> impl Future<Output = Result<Option<(ConversationId, u32)>, TxFailure>> + Send;

    /// The first `len` non-system messages of `conversation`'s history.
    fn history(
        &mut self,
        conversation: ConversationId,
        len: u32,
    ) -> impl Future<Output = Result<Vec<Entry>, TxFailure>> + Send;

    /// The conversation one of whose outputs is `message`.
    fn output_holder(
        &mut self,
        members: &[AgentId],
        message: MessageHash,
    ) -> impl Future<Output = Result<Option<ConversationId>, TxFailure>> + Send;

    /// The conversation whose non-system history holds any of `messages`.
    fn holder(
        &mut self,
        members: &[AgentId],
        messages: &[MessageHash],
    ) -> impl Future<Output = Result<Option<ConversationId>, TxFailure>> + Send;

    /// The most recently threaded conversation.
    fn latest(
        &mut self,
        members: &[AgentId],
    ) -> impl Future<Output = Result<Option<ConversationId>, TxFailure>> + Send;

    /// Which of `messages` an agent of `members` saw (received in a request
    /// or produced as an output) in a conversation other than
    /// `conversation`, no earlier than the store's retention cutoff for
    /// this call.
    fn seen_elsewhere(
        &mut self,
        members: &[AgentId],
        messages: &[MessageHash],
        conversation: ConversationId,
    ) -> impl Future<Output = Result<HashSet<MessageHash>, TxFailure>> + Send;

    /// Which of `messages` `conversation`'s non-system history holds.
    fn held(
        &mut self,
        conversation: ConversationId,
        messages: &[MessageHash],
    ) -> impl Future<Output = Result<HashSet<MessageHash>, TxFailure>> + Send;
}

/// A message to append to a conversation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct NewEntry {
    pub(crate) entry: Entry,
    /// Its non-system history position and chain; `None` for a system
    /// message.
    pub(crate) history: Option<(u32, ChainHash)>,
    pub(crate) output: bool,
}

/// The conversation a write lands in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Target {
    /// Append to a stored conversation.
    Existing,
    /// Create a conversation, its transcript starting with `base`: a
    /// parent's messages through its `k`-th non-system message.
    New {
        agent: AgentId,
        origin: ConversationOrigin,
        base: Option<(ConversationId, u32)>,
    },
}

/// What one threading call writes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Write {
    pub(crate) outcome: ThreadOutcome,
    pub(crate) conversation: ConversationId,
    pub(crate) target: Target,
    /// Appended after the base (or the stored messages), output last.
    pub(crate) appended: Vec<NewEntry>,
    /// The non-system history length afterwards.
    pub(crate) history_len: u32,
    /// The chain of the whole history afterwards; `None` when empty.
    pub(crate) head: Option<ChainHash>,
    pub(crate) last_system: Option<MessageHash>,
    /// The response to file, with the history length through it.
    pub(crate) response: Option<(ResponseKey, u32)>,
    /// What the attributed agent is recorded as having seen in
    /// `conversation`: the non-system messages the conversation stores
    /// that this request carried (all of them for a new conversation, a
    /// fork's base included; the appended ones for an extension) and the
    /// output, once each, ascending.
    pub(crate) seen: Vec<MessageHash>,
}

/// `messages` and `output`, once each, ascending.
fn seen(messages: &[MessageHash], output: Option<MessageHash>) -> Vec<MessageHash> {
    let mut seen: Vec<MessageHash> = messages.iter().copied().chain(output).collect();
    seen.sort_unstable();
    seen.dedup();
    seen
}

/// The decision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Planned {
    Recorded(ThreadOutcome),
    Write(Box<Write>),
}

fn len32(len: usize) -> Result<u32, TxFailure> {
    u32::try_from(len).map_err(|_| {
        TxFailure::Other(crate::error::StorageFailure::Inconsistent {
            reason: format!("a history of {len} messages is beyond the stored range"),
        })
    })
}

/// What the write appends and leaves: `segment` (request entries, system
/// ones included) after a base of `base_len` non-system messages, then the
/// output.
struct Appended {
    entries: Vec<NewEntry>,
    history_len: u32,
    head: Option<ChainHash>,
}

fn append(
    history: &History,
    base_len: usize,
    segment: &[Entry],
    lead_system: Option<MessageHash>,
    output: Option<MessageHash>,
) -> Result<Appended, TxFailure> {
    let mut entries = Vec::with_capacity(segment.len() + 2);
    if let Some(system) = lead_system {
        entries.push(NewEntry {
            entry: Entry {
                message: system,
                role: Role::System,
            },
            history: None,
            output: false,
        });
    }
    let mut len = base_len;
    let mut chain = history.chain(base_len);
    for entry in segment {
        let position = match entry.role {
            Role::System => None,
            Role::User | Role::Assistant | Role::Tool => {
                chain = chain.then(&entry.message);
                let position = (len32(len)?, chain);
                len += 1;
                Some(position)
            }
        };
        entries.push(NewEntry {
            entry: *entry,
            history: position,
            output: false,
        });
    }
    if let Some(output) = output {
        chain = chain.then(&output);
        entries.push(NewEntry {
            entry: Entry {
                message: output,
                role: Role::Assistant,
            },
            history: Some((len32(len)?, chain)),
            output: true,
        });
        len += 1;
    }
    Ok(Appended {
        entries,
        history_len: len32(len)?,
        head: (len > 0).then_some(chain),
    })
}

/// The delta of a call.
fn delta(
    input: &ThreadInput,
    conversation: ConversationId,
    new_inputs: Vec<MessageHash>,
    new_system: Option<MessageHash>,
) -> ConversationDelta {
    ConversationDelta {
        exchange: input.exchange,
        agent: input.agent,
        conversation,
        new_inputs,
        new_system,
        output: input.output,
    }
}

/// The response to file once the history has `history_len` messages.
fn filed(input: &ThreadInput, history_len: u32) -> Option<(ResponseKey, u32)> {
    input
        .response
        .clone()
        .filter(|_| input.output.is_some() && history_len > 0)
        .map(|key| (key, history_len))
}

/// `candidates` without the messages the cluster saw in a conversation
/// other than `conversation` (`reconstruct.delta.excludes-seen-elsewhere`),
/// in order.
async fn unseen<R: ThreadReads>(
    reads: &mut R,
    input: &ThreadInput,
    conversation: ConversationId,
    candidates: Vec<MessageHash>,
) -> Result<Vec<MessageHash>, TxFailure> {
    if candidates.is_empty() {
        return Ok(candidates);
    }
    let seen = reads
        .seen_elsewhere(&input.members, &candidates, conversation)
        .await?;
    Ok(candidates
        .into_iter()
        .filter(|message| !seen.contains(message))
        .collect())
}

/// A new conversation holding `history`'s messages from the start.
fn fresh(
    input: &ThreadInput,
    history: &History,
    origin: ConversationOrigin,
    new_inputs: Vec<MessageHash>,
) -> Result<Write, TxFailure> {
    let conversation = input.conversation;
    let appended = append(history, 0, &history.entries, None, input.output)?;
    let delta = delta(input, conversation, new_inputs, history.system);
    let outcome = match origin {
        ConversationOrigin::Root => ThreadOutcome::Starts {
            conversation,
            delta,
        },
        ConversationOrigin::Compaction { predecessor } => ThreadOutcome::Compacts {
            predecessor,
            conversation,
            delta,
        },
        ConversationOrigin::Fork {
            parent,
            shared_prefix,
        } => ThreadOutcome::Forks {
            parent,
            shared_prefix,
            conversation,
            delta,
        },
    };
    Ok(Write {
        outcome,
        conversation,
        target: Target::New {
            agent: input.agent,
            origin,
            base: None,
        },
        response: filed(input, appended.history_len),
        appended: appended.entries,
        history_len: appended.history_len,
        head: appended.head,
        last_system: history.system,
        seen: seen(&history.non_system().collect::<Vec<_>>(), input.output),
    })
}

/// The system message to record ahead of `tail` when it is new and not
/// already part of `tail`.
fn lead(new_system: Option<MessageHash>, tail: &[Entry]) -> Option<MessageHash> {
    new_system.filter(|system| !tail.iter().any(|entry| entry.message == *system))
}

/// Decide one threading call.
pub(crate) async fn plan<R: ThreadReads>(
    reads: &mut R,
    input: &ThreadInput,
) -> Result<Planned, TxFailure> {
    if let Some(outcome) = reads.recorded(input.exchange).await? {
        return Ok(Planned::Recorded(outcome));
    }
    let members = &input.members;
    let entries = match &input.kind {
        RequestKind::FullHistory => input.request.clone(),
        RequestKind::Increment { previous } => match reads.response(previous, members).await? {
            Some((conversation, len)) => {
                let mut full = reads.history(conversation, len).await?;
                full.extend(input.request.iter().copied());
                full
            }
            None => {
                let history = History::of(input.request.clone());
                let new_inputs = unseen(
                    reads,
                    input,
                    input.conversation,
                    history.non_system().collect(),
                )
                .await?;
                return Ok(Planned::Write(Box::new(fresh(
                    input,
                    &history,
                    ConversationOrigin::Root,
                    new_inputs,
                )?)));
            }
        },
    };
    let history = History::of(entries);
    let non_system: Vec<MessageHash> = history.non_system().collect();

    // Extends: the longest stored history that is a prefix of the request.
    if let Some(extension) = reads.extension(members, &history.chains).await? {
        let k = extension.len as usize;
        let tail = history.after(k);
        let new_system = history
            .system
            .filter(|system| Some(*system) != extension.last_system);
        let appended = append(&history, k, tail, lead(new_system, tail), input.output)?;
        let conversation = extension.conversation;
        let added = non_system.get(k..).unwrap_or(&[]);
        let new_inputs = unseen(reads, input, conversation, added.to_vec()).await?;
        let seen = seen(added, input.output);
        return Ok(Planned::Write(Box::new(Write {
            outcome: ThreadOutcome::Extends {
                conversation,
                delta: delta(input, conversation, new_inputs, new_system),
            },
            conversation,
            target: Target::Existing,
            response: filed(input, appended.history_len),
            appended: appended.entries,
            history_len: appended.history_len,
            head: appended.head,
            last_system: history.system,
            seen,
        })));
    }

    // Compacts: content evidence of summarizing a stored conversation.
    if let Some(predecessor) = compacted(reads, input, &non_system).await? {
        let carried = reads.held(predecessor, &non_system).await?;
        let new_inputs = non_system
            .iter()
            .copied()
            .filter(|message| !carried.contains(message))
            .collect();
        let new_inputs = unseen(reads, input, input.conversation, new_inputs).await?;
        return Ok(Planned::Write(Box::new(fresh(
            input,
            &history,
            ConversationOrigin::Compaction { predecessor },
            new_inputs,
        )?)));
    }

    // Forks: a shared prefix holding an assistant message.
    if let Some(first_assistant) = history.first_assistant
        && let Some((parent, k)) = reads
            .common_prefix(members, &history.chains, first_assistant)
            .await?
    {
        let shared = k as usize;
        let tail = history.after(shared);
        let appended = append(
            &history,
            shared,
            tail,
            lead(history.system, tail),
            input.output,
        )?;
        let conversation = input.conversation;
        let new_inputs = unseen(
            reads,
            input,
            conversation,
            non_system.get(shared..).unwrap_or(&[]).to_vec(),
        )
        .await?;
        return Ok(Planned::Write(Box::new(Write {
            outcome: ThreadOutcome::Forks {
                parent,
                shared_prefix: k,
                conversation,
                delta: delta(input, conversation, new_inputs, history.system),
            },
            conversation,
            target: Target::New {
                agent: input.agent,
                origin: ConversationOrigin::Fork {
                    parent,
                    shared_prefix: k,
                },
                base: Some((parent, k)),
            },
            response: filed(input, appended.history_len),
            appended: appended.entries,
            history_len: appended.history_len,
            head: appended.head,
            last_system: history.system,
            seen: seen(&non_system, input.output),
        })));
    }

    let new_inputs = unseen(reads, input, input.conversation, non_system).await?;
    Ok(Planned::Write(Box::new(fresh(
        input,
        &history,
        ConversationOrigin::Root,
        new_inputs,
    )?)))
}

/// The predecessor a compaction summarizes, when the request carries
/// content evidence of one:
///
/// - its first non-system message is a stored output of a cluster
///   conversation (a summary response echoed back as the opening of a new
///   history); or
/// - it carries a summary turn (`input.summary`) that no cluster
///   conversation holds yet.
///
/// The predecessor is the most recently threaded cluster conversation
/// holding any of the request's other messages (the ones it carries over),
/// else the most recently threaded one. `None` without evidence, or with
/// no cluster conversation stored.
async fn compacted<R: ThreadReads>(
    reads: &mut R,
    input: &ThreadInput,
    non_system: &[MessageHash],
) -> Result<Option<ConversationId>, TxFailure> {
    let members = &input.members;
    let echoed = match non_system.first() {
        Some(first) => reads.output_holder(members, *first).await?.is_some(),
        None => false,
    };
    let summary = match input.summary {
        Some(summary) if non_system.contains(&summary) => reads
            .holder(members, &[summary])
            .await?
            .is_none()
            .then_some(summary),
        Some(_) | None => None,
    };
    if !echoed && summary.is_none() {
        return Ok(None);
    }
    let others: Vec<MessageHash> = non_system
        .iter()
        .copied()
        .filter(|message| Some(*message) != summary)
        .collect();
    if let Some(holder) = reads.holder(members, &others).await? {
        return Ok(Some(holder));
    }
    reads.latest(members).await
}

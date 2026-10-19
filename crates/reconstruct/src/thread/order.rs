//! Order-tolerant threading (`reconstruct.thread.order-independent`).
//!
//! The transport promises no delivery order: a retry, a nack, a second
//! take, or a delivery cut by an outage and redelivered after later ones
//! can hand L3 an exchange after exchanges that ended after it. What a
//! store holds for a set of exchanges is defined as what threading them
//! one at a time in [`TurnKey`] order (when each ended, then its id)
//! leaves, whatever order they arrived in.
//!
//! A store keeps that by rewinding. Every threading call is numbered in
//! the order the store took it (its `seq`) and keeps what it was given (a
//! [`ThreadCall`]). An exchange `x` that arrives after turns of its cluster
//! whose keys are greater than its own:
//!
//! 1. finds those turns and every turn of the cluster threaded after the
//!    first of them ([`OrderedStore::later`]);
//! 2. undoes them, newest first ([`OrderedStore::unthread`]): each is the
//!    last turn of its conversation when it is undone, so undoing it
//!    removes exactly what its write added (a conversation it opened, or
//!    its entries, turn, record, filed response and sightings);
//! 3. threads `x`, then threads the undone calls again in key order.
//!
//! When the stored state was the in-order result for the exchanges before
//! `x`, it is the in-order result with `x` afterwards. A rewind is planned
//! as a [`Rewind`], which exists only when it can be carried out: every
//! undone turn's call is kept and the undone turns are the newest turns of
//! each conversation they touch. Otherwise the arrival is [`Stranded`] and
//! `x` is threaded where it arrived, as before this module, with a warning:
//!
//! - a call older than the [`ReorderWindow`] behind a later arrival is
//!   forgotten (calls are kept for the window, then pruned);
//! - a conversation holds turns of agents outside the cluster threaded
//!   after the undone ones (after an unmerge).
//!
//! The outcome returned is `x`'s. The rewound exchanges' recorded outcomes
//! change with them; their deltas were published when they arrived and are
//! not published again.

use std::collections::{BTreeSet, HashMap};
use std::time::Duration;

use crosstalk_spec::ids::{AgentId, ConversationId, ExchangeId, MessageHash};
use crosstalk_spec::interfaces::l3_reconstruction::ThreadOutcome;
use crosstalk_spec::observed::client::TrafficSource;
use crosstalk_spec::support::Timestamp;

use super::config::ThreadConfigError;
use super::history::Entry;
use super::store::{RequestKind, ResponseKey, ThreadInput};
use crate::error::TxFailure;

/// Where an exchange falls in the order a store's result is defined by:
/// when it ended (its response finished, or it failed), then its id. An
/// exchange's request can only carry outputs of exchanges that ended
/// before it started, so the order keeps every continuation after what
/// it continues, and, with no fault, it is the order capture publishes in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TurnKey {
    pub ended_at: Timestamp,
    pub exchange: ExchangeId,
}

/// The default [`ReorderWindow`]: a day, longer than any outage a spool
/// rides out before its captures are redelivered.
pub const DEFAULT_REORDER_WINDOW: Duration = Duration::from_secs(24 * 60 * 60);

/// How long a threading call is kept to be threaded again: an exchange
/// that arrives after exchanges that ended at most this long after it is
/// threaded in order; one later still is threaded where it arrived. Non-zero,
/// at most `u64::MAX` microseconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReorderWindow {
    micros: u64,
}

impl ReorderWindow {
    pub fn new(window: Duration) -> Result<Self, ThreadConfigError> {
        let micros = u64::try_from(window.as_micros())
            .map_err(|_| ThreadConfigError::ReorderWindowTooLong)?;
        if micros == 0 {
            return Err(ThreadConfigError::ZeroReorderWindow);
        }
        Ok(Self { micros })
    }

    pub fn as_duration(self) -> Duration {
        Duration::from_micros(self.micros)
    }

    /// `at` less the window, or the epoch.
    pub fn before(self, at: Timestamp) -> Timestamp {
        Timestamp::from_micros(at.as_micros().saturating_sub(self.micros))
    }
}

impl Default for ReorderWindow {
    fn default() -> Self {
        Self {
            micros: DEFAULT_REORDER_WINDOW.as_secs() * 1_000_000,
        }
    }
}

/// What a threading call was given, kept to thread it again: its
/// [`ThreadInput`] less the cluster, which a rethreading takes from the
/// exchange that caused it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ThreadCall {
    pub(crate) exchange: ExchangeId,
    pub(crate) at: Timestamp,
    pub(crate) ended_at: Timestamp,
    pub(crate) agent: AgentId,
    pub(crate) request: Vec<Entry>,
    pub(crate) kind: RequestKind,
    pub(crate) output: Option<MessageHash>,
    pub(crate) response: Option<ResponseKey>,
    pub(crate) summary: Option<MessageHash>,
    pub(crate) conversation: ConversationId,
    pub(crate) source: TrafficSource,
}

impl ThreadCall {
    pub(crate) fn of(input: &ThreadInput) -> Self {
        Self {
            exchange: input.exchange,
            at: input.at,
            ended_at: input.ended_at,
            agent: input.agent,
            request: input.request.clone(),
            kind: input.kind.clone(),
            output: input.output,
            response: input.response.clone(),
            summary: input.summary,
            conversation: input.conversation,
            source: input.source.clone(),
        }
    }

    /// The call again, matching against `members`' conversations.
    pub(crate) fn input(&self, members: &[AgentId]) -> ThreadInput {
        ThreadInput {
            exchange: self.exchange,
            at: self.at,
            ended_at: self.ended_at,
            agent: self.agent,
            members: members.to_vec(),
            request: self.request.clone(),
            kind: self.kind.clone(),
            output: self.output,
            response: self.response.clone(),
            summary: self.summary,
            conversation: self.conversation,
            source: self.source.clone(),
        }
    }

    pub(crate) fn key(&self) -> TurnKey {
        TurnKey {
            ended_at: self.ended_at,
            exchange: self.exchange,
        }
    }
}

/// A stored turn threaded after a late exchange's place.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct LaterTurn {
    /// When the store took it, among all calls.
    pub(crate) seq: u64,
    pub(crate) key: TurnKey,
    pub(crate) conversation: ConversationId,
    /// Its index among the conversation's turns.
    pub(crate) turn: u32,
    /// Where its entries start in the conversation's transcript.
    pub(crate) first_ordinal: u32,
}

/// A rewind that can be carried out: `undo` holds the newest turns of
/// every conversation it touches, newest first, and `replay` every one of
/// their calls, in key order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Rewind {
    undo: Vec<LaterTurn>,
    replay: Vec<ThreadCall>,
}

impl Rewind {
    pub(crate) fn undo(&self) -> &[LaterTurn] {
        &self.undo
    }

    pub(crate) fn replay(&self) -> &[ThreadCall] {
        &self.replay
    }
}

/// Why a late exchange is threaded where it arrived.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Stranded {
    /// The calls of `missing` later turns were pruned: they ended more than
    /// the reorder window after an exchange that arrived before this one.
    Forgotten { missing: usize },
    /// `conversation` holds turns threaded after the later turns that are
    /// not the cluster's.
    Interleaved { conversation: ConversationId },
}

/// How an exchange arrived relative to its cluster's stored turns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Arrival {
    /// Nothing threaded so far comes after it.
    InOrder,
    Late(Rewind),
    Stranded(Stranded),
}

/// Plan the arrival of an exchange given the turns threaded after its
/// place (`later`), the turn count of every conversation they touch and
/// the calls kept for them.
pub(crate) fn arrival(
    later: Vec<LaterTurn>,
    turns: &HashMap<ConversationId, u32>,
    mut calls: HashMap<ExchangeId, ThreadCall>,
) -> Arrival {
    if later.is_empty() {
        return Arrival::InOrder;
    }
    let mut touched: HashMap<ConversationId, BTreeSet<u32>> = HashMap::new();
    for turn in &later {
        touched
            .entry(turn.conversation)
            .or_default()
            .insert(turn.turn);
    }
    for (conversation, indexes) in &touched {
        let count = turns.get(conversation).copied().unwrap_or(0);
        let first = indexes.first().copied().unwrap_or(count);
        let suffix = u32::try_from(indexes.len()).ok() == count.checked_sub(first)
            && indexes.last().is_some_and(|last| last + 1 == count);
        if !suffix {
            return Arrival::Stranded(Stranded::Interleaved {
                conversation: *conversation,
            });
        }
    }
    let mut replay = Vec::with_capacity(later.len());
    let mut missing = 0;
    for turn in &later {
        match calls.remove(&turn.key.exchange) {
            Some(call) => replay.push(call),
            None => missing += 1,
        }
    }
    if missing > 0 {
        return Arrival::Stranded(Stranded::Forgotten { missing });
    }
    replay.sort_by_key(ThreadCall::key);
    let mut undo = later;
    undo.sort_by_key(|turn| std::cmp::Reverse(turn.seq));
    Arrival::Late(Rewind { undo, replay })
}

/// A store's atomic step, as the rewind drives it.
pub(crate) trait OrderedStore: Send {
    /// The outcome recorded for `exchange`.
    fn recorded(
        &mut self,
        exchange: ExchangeId,
    ) -> impl Future<Output = Result<Option<ThreadOutcome>, TxFailure>> + Send;

    /// Every turn of `members` taken at or after the first one whose key is
    /// greater than `key`, in the order they were taken.
    fn later(
        &mut self,
        members: &[AgentId],
        key: TurnKey,
    ) -> impl Future<Output = Result<Vec<LaterTurn>, TxFailure>> + Send;

    /// How many turns each of `conversations` holds.
    fn turn_counts(
        &mut self,
        conversations: &[ConversationId],
    ) -> impl Future<Output = Result<HashMap<ConversationId, u32>, TxFailure>> + Send;

    /// The kept calls of `exchanges`.
    fn calls(
        &mut self,
        exchanges: &[ExchangeId],
    ) -> impl Future<Output = Result<HashMap<ExchangeId, ThreadCall>, TxFailure>> + Send;

    /// Remove what `turn`'s write added; it is its conversation's last turn.
    fn unthread(&mut self, turn: &LaterTurn) -> impl Future<Output = Result<(), TxFailure>> + Send;

    /// Decide `input` against what is stored and record it, its call kept
    /// and numbered next.
    fn thread_now(
        &mut self,
        input: &ThreadInput,
    ) -> impl Future<Output = Result<ThreadOutcome, TxFailure>> + Send;
}

/// Thread `input` so the store holds what threading its exchanges in key
/// order leaves.
pub(crate) async fn thread<S: OrderedStore>(
    store: &mut S,
    input: &ThreadInput,
) -> Result<ThreadOutcome, TxFailure> {
    if let Some(outcome) = store.recorded(input.exchange).await? {
        return Ok(outcome);
    }
    let later = store.later(&input.members, input.key()).await?;
    if later.is_empty() {
        return store.thread_now(input).await;
    }
    let mut conversations: Vec<ConversationId> =
        later.iter().map(|turn| turn.conversation).collect();
    conversations.sort_unstable();
    conversations.dedup();
    let counts = store.turn_counts(&conversations).await?;
    let exchanges: Vec<ExchangeId> = later.iter().map(|turn| turn.key.exchange).collect();
    let calls = store.calls(&exchanges).await?;
    match arrival(later, &counts, calls) {
        Arrival::InOrder => store.thread_now(input).await,
        Arrival::Stranded(stranded) => {
            tracing::warn!(
                exchange = %input.exchange.ulid_text(),
                agent = %input.agent.ulid_text(),
                reason = ?stranded,
                "late exchange threaded where it arrived, not in order"
            );
            store.thread_now(input).await
        }
        Arrival::Late(rewind) => {
            for turn in rewind.undo() {
                store.unthread(turn).await?;
            }
            let outcome = store.thread_now(input).await?;
            for call in rewind.replay() {
                store.thread_now(&call.input(&input.members)).await?;
            }
            tracing::info!(
                exchange = %input.exchange.ulid_text(),
                agent = %input.agent.ulid_text(),
                rethreaded = rewind.replay().len(),
                "late exchange threaded in order; later turns threaded again"
            );
            Ok(outcome)
        }
    }
}

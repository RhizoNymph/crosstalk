//! L5's extraction step: each conversation delta's tool calls and tool
//! results, as the [`Extracted`] inputs the flow consumer takes.
//!
//! The spec has no event for an extracted access, so the step runs where
//! everything it needs is already decided: right after provenance
//! processed the same delta (the composer's L4 stage), so the writer's
//! spans of the exchange are stored when its write calls are extracted.
//! For one delta of exchange `X` (agent `a`, conversation `c`):
//!
//! 1. A new system prompt starts `a`'s context in `c` ([`ConversationContext`],
//!    one per agent and conversation); a changed one keeps what the context
//!    learnt and only fills a working directory it did not know.
//! 2. The delta's new inputs are taken in request order. A tool call in an
//!    assistant message among them (history the request carries: after a
//!    compaction, or in a new conversation that replays its transcript) is
//!    kept as `c`'s history call. Every tool result among them is matched,
//!    by call id, to the call it answers and extracted with it
//!    (`flow.extract.result-pairs-with-history-call`):
//!    - a call `c` made earlier in an output;
//!    - else a history call of `c`, and, when `a` made that same call (same
//!      id, name and arguments) in another conversation's output, that
//!      call, so its held writes are released;
//!    - else nothing: the result is dropped.
//!
//!    A call made in an output is extracted in the context it was made in
//!    (kept with the call), so its writes line up with the ones held. A
//!    write held without a result gets its [`Extracted::WriteResult`]; a
//!    read is an [`Extracted::Read`] by `a` in `X`, at `X`'s start, naming
//!    the result part, which is a part of a message of `X`'s request (or
//!    output, for a server tool) with text: a result with no text (an
//!    empty output) is no read (`flow.extract.read-locates-its-result`).
//!    A history call that no output of `a` made yields reads only (its
//!    writes happened in an exchange never seen). A result `a` was already
//!    delivered (the same call and result content, in any conversation) is
//!    not read again (`flow.extract.replayed-result-read-once`). Every
//!    result then teaches `a`'s context in `c` what it shows
//!    ([`ConversationContext::observe`]: the shell's directory, home and
//!    remotes), in request order, whether or not it was read.
//! 3. Every tool call in `X`'s output is an [`Extracted::ToolCall`]; a
//!    known tool's writes are [`Extracted::Write`]s held without an
//!    outcome, carrying the spans [`write_spans`] finds at the call's part
//!    (none for a write whose content is not in the call, `git push`);
//!    the call waits for its result. A server tool's result in the same
//!    output is extracted at once.
//!
//! Access ids are derived from the exchange, the call and the access's
//! place among the call's accesses, so a redelivered delta extracts the
//! same accesses again (the flow consumer records an access once).
//!
//! **Durability** ([`ExtractionStep::delta`]). What the step remembers
//! between deltas is in its [`ExtractionLedger`]. A delta whose exchange the
//! ledger marks done is skipped. Otherwise the step extracts it against a
//! per-delta view of the ledger (nothing in the ledger changes yet), hands
//! the inputs to the flow consumer ([`FlowInputs::deliver`]), and only once
//! the consumer holds them durably commits the delta's ledger changes with
//! its exchange marked done, in one commit. A crash or a refusal before the
//! commit leaves the ledger as it was, so the redelivered delta extracts
//! the same inputs again, which the consumer takes idempotently; a crash
//! after it finds the delta done.

mod keys;
pub mod ledger;
pub mod ports;
mod working;

#[cfg(test)]
mod tests;

use std::collections::BTreeMap;
use std::time::Duration;

use crosstalk_spec::derived::flow::resource::Locator;
use crosstalk_spec::derived::provenance::span::{Origin, RelaySource, Span};
use crosstalk_spec::events::ingest::ConversationDelta;
use crosstalk_spec::ids::{AccessId, AgentId, ExchangeId, MessageHash, SpanId};
use crosstalk_spec::interfaces::l5_flow::{ExtractedOp, ResourceExtractor, WritePayload};
use crosstalk_spec::observed::message::{
    AssistantPart, Message, MessageBody, PartRef, ToolCall, ToolExecution, ToolResult,
};
use crosstalk_spec::support::Timestamp;

use self::keys::{access_id, context_of, delivery_key, part_ref};
pub use self::ledger::{
    ContextKey, DeliveryKey, ExtractionLedger, HistoryKey, LedgerChanges, LedgerCommit,
    LedgerError, LedgerState, MemoryExtractionLedger, PendingCall, PendingKey,
};
pub use self::ports::{MessageReader, PortError, SpanReader};
use self::working::Working;
use crate::consumer::{Extracted, FlowInputs, NotDurable, Observed, ReadResult, WriteCall};
use crate::extract::{ConversationContext, ExtractConfig, ToolExtractors, write_spans};

/// Why a delta's extraction did not finish. Worth a retry: the ledger is
/// as it was before the delta.
#[derive(Debug, thiserror::Error)]
pub enum ExtractStepError {
    #[error("message {hash:?} could not be read: {reason}")]
    Message { hash: MessageHash, reason: String },
    #[error("provenance's spans could not be read: {reason}")]
    Spans { reason: String },
    #[error("the extraction ledger failed: {0}")]
    Ledger(#[from] LedgerError),
    /// The flow consumer did not confirm the inputs durable.
    #[error(transparent)]
    Flow(#[from] NotDurable),
}

/// What one delta came to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeltaOutcome {
    /// Its extraction had committed already; nothing was handed over.
    AlreadyDone,
    /// Extracted, handed over and committed: how many inputs.
    Extracted { inputs: usize },
}

/// The call a tool result answers, as found.
enum Answered {
    /// Made in an output (of this conversation, or of another one of the
    /// agent's when this request's history carries the same call).
    Output(PendingCall),
    /// Only in this request's or an earlier request's history.
    History(ToolCall),
}

impl Answered {
    fn call(&self) -> &ToolCall {
        match self {
            Self::Output(pending) => &pending.call,
            Self::History(call) => call,
        }
    }
}

/// The extraction step over its ledger, the spans provenance stored and
/// the message bodies.
#[derive(Debug)]
pub struct ExtractionStep<L, S, M> {
    ledger: L,
    spans: S,
    messages: M,
    config: ExtractConfig,
}

impl<L, S, M> ExtractionStep<L, S, M>
where
    L: ExtractionLedger,
    S: SpanReader,
    M: MessageReader,
{
    /// The step under `config` (the MCP tools, HTTP and fetch tools and
    /// site rules the extractors know).
    pub fn new(ledger: L, spans: S, messages: M, config: ExtractConfig) -> Self {
        Self {
            ledger,
            spans,
            messages,
            config,
        }
    }

    pub fn ledger(&self) -> &L {
        &self.ledger
    }

    /// Extract `delta` of an exchange that started at `at`, hand what it
    /// yields to `flow` in order, and commit the ledger once `flow` holds
    /// it (see the module docs).
    pub async fn delta<F: FlowInputs>(
        &self,
        delta: &ConversationDelta,
        at: Timestamp,
        flow: &mut F,
    ) -> Result<DeltaOutcome, ExtractStepError> {
        if self.ledger.done(delta.exchange).await? {
            tracing::debug!(exchange = %delta.exchange.ulid_text(), "delta already extracted; skipped");
            return Ok(DeltaOutcome::AlreadyDone);
        }
        let (inputs, changes) = self.extract(delta, at).await?;
        let count = inputs.len();
        flow.deliver(inputs).await?;
        self.ledger
            .commit(LedgerCommit {
                exchange: delta.exchange,
                at,
                changes,
            })
            .await?;
        Ok(DeltaOutcome::Extracted { inputs: count })
    }

    /// Forget deliveries, contexts and done marks stamped more than `keep`
    /// before `now`.
    pub async fn expire(&self, now: Timestamp, keep: Duration) -> Result<(), LedgerError> {
        let keep = u64::try_from(keep.as_micros()).unwrap_or(u64::MAX);
        let horizon = Timestamp::from_micros(now.as_micros().saturating_sub(keep));
        self.ledger.expire(horizon).await
    }

    /// `delta`'s inputs and the ledger changes they come with, nothing
    /// committed.
    pub async fn extract(
        &self,
        delta: &ConversationDelta,
        at: Timestamp,
    ) -> Result<(Vec<Extracted>, LedgerChanges), ExtractStepError> {
        let mut work = Working::new(&self.ledger);
        let mut out = Vec::new();
        let key = (delta.agent, delta.conversation);
        if let Some(system) = delta.new_system
            && let Some(message) = self.message(system).await?
        {
            let stated = context_of(&message);
            let context = match work.context(delta.agent, delta.conversation).await? {
                Some(mut context) => {
                    context.restate(&stated);
                    context
                }
                None => stated,
            };
            work.set_context(key, context);
        }
        for hash in &delta.new_inputs {
            let Some(message) = self.message(*hash).await? else {
                continue;
            };
            match &message.body {
                MessageBody::Assistant(parts) => {
                    for part in parts {
                        if let AssistantPart::ToolCall(call) = part
                            && self.handles(call)
                        {
                            work.set_history(
                                (delta.conversation, call.id.0.clone()),
                                Some(call.clone()),
                            );
                        }
                    }
                }
                MessageBody::Tool(results) => {
                    for (index, result) in results.iter().enumerate() {
                        let part = part_ref(message.hash, index);
                        let Some(answered) = self.answered(&mut work, delta, result).await? else {
                            tracing::debug!(exchange = %delta.exchange.ulid_text(), call = %result.call_id.0, "tool result without a known call; not extracted");
                            continue;
                        };
                        let key =
                            delivery_key(delta.agent, answered.call(), result, &message, part);
                        let call = answered.call().clone();
                        if matches!(answered, Answered::History(_)) && work.delivered(key).await? {
                            tracing::debug!(exchange = %delta.exchange.ulid_text(), call = %result.call_id.0, "result already delivered to the agent; not read again");
                        } else {
                            work.deliver(key);
                            let has_text = message
                                .part_text(part.index)
                                .is_ok_and(|text| !text.is_empty());
                            self.result(
                                &mut work, delta, at, answered, result, part, has_text, &mut out,
                            )
                            .await?;
                        }
                        self.observe(&mut work, delta, &call, result).await?;
                    }
                }
                MessageBody::System(_) | MessageBody::User(_) => {}
            }
        }
        if let Some(output) = delta.output
            && let Some(message) = self.message(output).await?
            && let MessageBody::Assistant(parts) = &message.body
        {
            let spans = self.spans_of(delta.exchange).await?;
            let sources = self.relay_sources(&spans).await?;
            let server_results: BTreeMap<String, (usize, &ToolResult)> = parts
                .iter()
                .enumerate()
                .filter_map(|(index, part)| match part {
                    AssistantPart::ServerToolResult(result) => {
                        Some((result.call_id.0.clone(), (index, result)))
                    }
                    _ => None,
                })
                .collect();
            for (index, part) in parts.iter().enumerate() {
                let AssistantPart::ToolCall(call) = part else {
                    continue;
                };
                let call_part = part_ref(message.hash, index);
                out.push(Extracted::ToolCall {
                    agent: delta.agent,
                    call: call.id.clone(),
                    name: call.name.clone(),
                    at,
                });
                self.call(
                    &mut work, delta, at, call, call_part, &spans, &sources, &mut out,
                )
                .await?;
                if call.execution == ToolExecution::Server
                    && let Some((result_index, result)) = server_results.get(&call.id.0)
                    && let Some(answered) =
                        Self::take_pending(&mut work, delta, &call.id.0, None).await?
                {
                    let result_part = part_ref(message.hash, *result_index);
                    work.deliver(delivery_key(
                        delta.agent,
                        answered.call(),
                        result,
                        &message,
                        result_part,
                    ));
                    let has_text = message
                        .part_text(result_part.index)
                        .is_ok_and(|text| !text.is_empty());
                    self.result(
                        &mut work,
                        delta,
                        at,
                        answered,
                        result,
                        result_part,
                        has_text,
                        &mut out,
                    )
                    .await?;
                    self.observe(&mut work, delta, call, result).await?;
                }
            }
        }
        Ok((out, work.into_changes()))
    }

    /// The call `result` (among `delta`'s new inputs) answers, taken out of
    /// what waits for a result.
    async fn answered(
        &self,
        work: &mut Working<'_, L>,
        delta: &ConversationDelta,
        result: &ToolResult,
    ) -> Result<Option<Answered>, LedgerError> {
        let id = &result.call_id.0;
        if let Some(answered) = Self::take_pending(work, delta, id, None).await? {
            work.set_history((delta.conversation, id.clone()), None);
            return Ok(Some(answered));
        }
        let Some(call) = work.history(delta.conversation, id).await? else {
            return Ok(None);
        };
        work.set_history((delta.conversation, id.clone()), None);
        Ok(Some(
            Self::take_pending(work, delta, id, Some(&call))
                .await?
                .unwrap_or(Answered::History(call)),
        ))
    }

    /// The call `id` the agent made in an output: of `delta`'s
    /// conversation when `same` is `None`, else of any conversation, the
    /// latest made with `same`'s name and arguments.
    async fn take_pending(
        work: &mut Working<'_, L>,
        delta: &ConversationDelta,
        id: &str,
        same: Option<&ToolCall>,
    ) -> Result<Option<Answered>, LedgerError> {
        let calls = work.pending(delta.agent, id).await?;
        let position = match same {
            None => calls
                .iter()
                .position(|pending| pending.conversation == delta.conversation),
            Some(call) => calls.iter().rposition(|pending| {
                pending.call.name == call.name && pending.call.arguments == call.arguments
            }),
        };
        let Some(position) = position else {
            return Ok(None);
        };
        let pending = calls.remove(position);
        work.pending_changed(delta.agent, id);
        Ok(Some(Answered::Output(pending)))
    }

    /// Whether the extractors know `call`'s tool: only such calls are kept
    /// as history and only their results remembered as delivered.
    fn handles(&self, call: &ToolCall) -> bool {
        let context = ConversationContext::default();
        ToolExtractors::new(&self.config, &context).handles(call)
    }

    /// `delta`'s agent's context in its conversation, as it stands.
    async fn context_of(
        work: &mut Working<'_, L>,
        delta: &ConversationDelta,
    ) -> Result<ConversationContext, LedgerError> {
        Ok(work
            .context(delta.agent, delta.conversation)
            .await?
            .unwrap_or_default())
    }

    /// Let `delta`'s agent's context in its conversation learn from a
    /// call's result (after the result was extracted).
    async fn observe(
        &self,
        work: &mut Working<'_, L>,
        delta: &ConversationDelta,
        call: &ToolCall,
        result: &ToolResult,
    ) -> Result<(), LedgerError> {
        let mut context = Self::context_of(work, delta).await?;
        context.observe(&self.config, call, Some(result));
        work.set_context((delta.agent, delta.conversation), context);
        Ok(())
    }

    /// A call in `delta`'s output: its writes held, the call kept for its
    /// result.
    #[allow(clippy::too_many_arguments)]
    async fn call(
        &self,
        work: &mut Working<'_, L>,
        delta: &ConversationDelta,
        at: Timestamp,
        call: &ToolCall,
        part: PartRef,
        spans: &[Span],
        sources: &BTreeMap<SpanId, AgentId>,
        out: &mut Vec<Extracted>,
    ) -> Result<(), LedgerError> {
        let context = Self::context_of(work, delta).await?;
        let mut writes: Vec<(Locator, AccessId)> = Vec::new();
        {
            let extractors = ToolExtractors::new(&self.config, &context);
            if !extractors.handles(call) {
                return Ok(());
            }
            let accesses = match extractors.extract(call, None) {
                Ok(accesses) => accesses,
                Err(error) => {
                    tracing::debug!(exchange = %delta.exchange.ulid_text(), call = %call.id.0, error = ?error, "call not extracted");
                    Vec::new()
                }
            };
            let written = write_spans(part, delta.agent, spans, |span| sources.get(&span).copied());
            for (index, access) in accesses.into_iter().enumerate() {
                let ExtractedOp::Write { payload, .. } = access.op else {
                    continue;
                };
                // A write whose content is not in the call (`git push`)
                // carries no spans.
                let spans = match payload {
                    WritePayload::CallArguments => written.clone(),
                    WritePayload::Unseen => Vec::new(),
                };
                let id = access_id(delta.exchange, &call.id.0, "write", index, at);
                writes.push((access.locator.clone(), id));
                out.push(Extracted::Write {
                    write: Observed {
                        id,
                        agent: delta.agent,
                        exchange: delta.exchange,
                        at,
                        locator: access.locator,
                        via: access.via,
                        op: WriteCall { call: part, spans },
                    },
                    outcome: None,
                });
            }
        }
        let calls = work.pending(delta.agent, &call.id.0).await?;
        // A redelivered output replaces its own earlier entry.
        calls.retain(|pending| pending.conversation != delta.conversation);
        calls.push(PendingCall {
            conversation: delta.conversation,
            context,
            call: call.clone(),
            writes,
        });
        work.pending_changed(delta.agent, &call.id.0);
        Ok(())
    }

    /// A result in `delta` answering `answered`, at `part` (with text or
    /// not): the call's held writes released with their outcome, its reads
    /// extracted when the part has text to locate them at.
    #[allow(clippy::too_many_arguments)]
    async fn result(
        &self,
        work: &mut Working<'_, L>,
        delta: &ConversationDelta,
        at: Timestamp,
        answered: Answered,
        result: &ToolResult,
        part: PartRef,
        has_text: bool,
        out: &mut Vec<Extracted>,
    ) -> Result<(), LedgerError> {
        // A call made in an output is extracted in the context it was made
        // in; a history call in the context of the conversation it is seen
        // in.
        let (call, writes, context) = match answered {
            Answered::Output(pending) => (pending.call, pending.writes, pending.context),
            Answered::History(call) => (call, Vec::new(), Self::context_of(work, delta).await?),
        };
        let extractors = ToolExtractors::new(&self.config, &context);
        let accesses = match extractors.extract(&call, Some(result)) {
            Ok(accesses) => accesses,
            Err(error) => {
                tracing::debug!(exchange = %delta.exchange.ulid_text(), call = %result.call_id.0, error = ?error, "result not extracted");
                return Ok(());
            }
        };
        let mut held = writes.into_iter();
        for (index, access) in accesses.into_iter().enumerate() {
            match access.op {
                ExtractedOp::Write { outcome, .. } => match held.next() {
                    Some((_, id)) => out.push(Extracted::WriteResult {
                        access: id,
                        outcome,
                    }),
                    None => tracing::debug!(
                        call = %result.call_id.0,
                        "a write extracted only with its result; not recorded"
                    ),
                },
                ExtractedOp::Read if !has_text => tracing::debug!(
                    exchange = %delta.exchange.ulid_text(),
                    call = %result.call_id.0,
                    "a read whose result has no text; not recorded"
                ),
                ExtractedOp::Read => out.push(Extracted::Read(Observed {
                    id: access_id(delta.exchange, &result.call_id.0, "read", index, at),
                    agent: delta.agent,
                    exchange: delta.exchange,
                    at,
                    locator: access.locator,
                    via: access.via,
                    op: ReadResult { result: part },
                })),
            }
        }
        Ok(())
    }

    async fn message(&self, hash: MessageHash) -> Result<Option<Message>, ExtractStepError> {
        self.messages
            .message(hash)
            .await
            .map_err(|error| ExtractStepError::Message {
                hash,
                reason: error.reason,
            })
    }

    async fn spans_of(&self, exchange: ExchangeId) -> Result<Vec<Span>, ExtractStepError> {
        self.spans
            .exchange_spans(exchange)
            .await
            .map_err(|error| ExtractStepError::Spans {
                reason: error.reason,
            })
    }

    /// The agent of every span `spans` relay from another span.
    async fn relay_sources(
        &self,
        spans: &[Span],
    ) -> Result<BTreeMap<SpanId, AgentId>, ExtractStepError> {
        let mut sources = BTreeMap::new();
        for span in spans {
            if let Some(Origin::Relayed(RelaySource::Span(source))) = span.state.origin()
                && let Some(agent) = self.spans.span_agent(source).await.map_err(|error| {
                    ExtractStepError::Spans {
                        reason: error.reason,
                    }
                })?
            {
                sources.insert(source, agent);
            }
        }
        Ok(sources)
    }
}

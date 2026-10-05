//! L5's extraction step: each conversation delta's tool calls and tool
//! results, as the [`Extracted`] inputs the flow consumer takes.
//!
//! The spec has no event for an extracted access, so the step runs where
//! everything it needs is already decided: right after provenance
//! processed the same delta (`Slot::L4Provenance`), so the writer's spans
//! of the exchange are stored when its write calls are extracted. For one
//! delta of exchange `X` (agent `a`, conversation `c`):
//!
//! 1. A new system prompt sets `c`'s [`ConversationContext`].
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
//!    A write held without a result gets its [`Extracted::WriteResult`]; a
//!    read is an [`Extracted::Read`] by `a` in `X`, at `X`'s start, naming
//!    the result part. A history call that no output of `a` made yields
//!    reads only (its writes happened in an exchange never seen). A
//!    result `a` was already delivered (the same call and result content,
//!    in any conversation) is not read again
//!    (`flow.extract.replayed-result-read-once`).
//! 3. Every tool call in `X`'s output is an [`Extracted::ToolCall`]; a
//!    known tool's writes are [`Extracted::Write`]s held without an
//!    outcome, carrying the spans [`write_spans`] finds at the call's part;
//!    the call waits for its result. A server tool's result in the same
//!    output is extracted at once.
//!
//! Access ids are derived from the exchange, the call and the access's
//! place among the call's accesses, so a redelivered delta extracts the
//! same accesses again (the flow consumer records an access once).

use std::collections::{BTreeMap, BTreeSet};

use crosstalk_flow::consumer::{Extracted, Observed, ReadResult, WriteCall};
use crosstalk_flow::extract::{ConversationContext, ExtractConfig, ToolExtractors, write_spans};
use crosstalk_provenance::scan::messages::{BlobMessages, MessageSource};
use crosstalk_provenance::store::{MemoryProvenanceStore, ProvenanceStore};
use crosstalk_spec::derived::provenance::span::{Origin, RelaySource, Span};
use crosstalk_spec::events::ingest::ConversationDelta;
use crosstalk_spec::ids::{AccessId, AgentId, ConversationId, ExchangeId, MessageHash, SpanId};
use crosstalk_spec::interfaces::l5_flow::{ExtractedOp, ResourceExtractor};
use crosstalk_spec::observed::message::{
    AssistantPart, Message, MessageBody, PartRef, SystemPart, ToolArguments, ToolCall,
    ToolExecution, ToolResult,
};
use crosstalk_spec::support::Timestamp;
use tokio::sync::mpsc::UnboundedSender;

use crate::live::blobs::LiveBlobs;

/// Why a delta's extraction did not finish. Worth a retry: nothing was
/// handed to the flow consumer.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ExtractStepError {
    #[error("message {hash:?} could not be read: {reason}")]
    Message { hash: MessageHash, reason: String },
    #[error("provenance's spans could not be read: {reason}")]
    Spans { reason: String },
    #[error("the flow consumer has stopped")]
    FlowStopped,
}

/// A call made in an output, waiting for its result.
#[derive(Debug, Clone)]
struct Pending {
    /// The conversation whose output made it.
    conversation: ConversationId,
    call: ToolCall,
    /// Its writes, held by the flow consumer, by locator order.
    writes: Vec<(crosstalk_spec::derived::flow::resource::Locator, AccessId)>,
}

/// The call a tool result answers, as found.
enum Answered {
    /// Made in an output (of this conversation, or of another one of the
    /// agent's when this request's history carries the same call).
    Output(Pending),
    /// Only in this request's or an earlier request's history.
    History(ToolCall),
}

/// A delivery's identity per agent: a digest of the call (id, name,
/// arguments) and the result (outcome, content).
type DeliveryKey = (AgentId, [u8; 32]);

/// The extraction step's state: each conversation's context, the calls
/// still waiting for a result, the history calls, and the results each
/// agent was delivered.
pub struct Extraction {
    messages: BlobMessages<LiveBlobs>,
    spans: MemoryProvenanceStore,
    config: ExtractConfig,
    contexts: BTreeMap<ConversationId, ConversationContext>,
    /// Calls made in an output, by agent and call id; one per conversation
    /// that made a call with that id.
    pending: BTreeMap<(AgentId, String), Vec<Pending>>,
    /// Calls of known tools seen only among a conversation's new inputs, by
    /// call id.
    history: BTreeMap<(ConversationId, String), ToolCall>,
    delivered: BTreeSet<DeliveryKey>,
    flow: UnboundedSender<Extracted>,
}

impl Extraction {
    /// The step under `config` (the MCP tools, HTTP and fetch tools and
    /// site rules the extractors know).
    pub fn new(
        blobs: LiveBlobs,
        spans: MemoryProvenanceStore,
        config: ExtractConfig,
        flow: UnboundedSender<Extracted>,
    ) -> Self {
        Self {
            messages: BlobMessages::new(blobs),
            spans,
            config,
            contexts: BTreeMap::new(),
            pending: BTreeMap::new(),
            history: BTreeMap::new(),
            delivered: BTreeSet::new(),
            flow,
        }
    }

    /// Extract `delta` of an exchange that started at `at`, and hand what
    /// it yields to the flow consumer, in order.
    pub async fn delta(
        &mut self,
        delta: &ConversationDelta,
        at: Timestamp,
    ) -> Result<(), ExtractStepError> {
        let mut out = Vec::new();
        let mut delivered = Vec::new();
        if let Some(system) = delta.new_system
            && let Some(message) = self.message(system).await?
        {
            self.contexts
                .insert(delta.conversation, context_of(&message));
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
                            self.history
                                .insert((delta.conversation, call.id.0.clone()), call.clone());
                        }
                    }
                }
                MessageBody::Tool(results) => {
                    for (index, result) in results.iter().enumerate() {
                        let part = part_ref(message.hash, index);
                        let Some(answered) = self.answered(delta, result) else {
                            tracing::debug!(exchange = %delta.exchange.ulid_text(), call = %result.call_id.0, "tool result without a known call; not extracted");
                            continue;
                        };
                        let key =
                            delivery_key(delta.agent, answered.call(), result, &message, part);
                        if matches!(answered, Answered::History(_))
                            && (self.delivered.contains(&key) || delivered.contains(&key))
                        {
                            tracing::debug!(exchange = %delta.exchange.ulid_text(), call = %result.call_id.0, "result already delivered to the agent; not read again");
                            continue;
                        }
                        delivered.push(key);
                        self.result(delta, at, answered, result, part, &mut out);
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
                self.call(delta, at, call, call_part, &spans, &sources, &mut out);
                if call.execution == ToolExecution::Server
                    && let Some((result_index, result)) = server_results.get(&call.id.0)
                    && let Some(answered) = self.take_pending(delta, &call.id.0, None)
                {
                    let result_part = part_ref(message.hash, *result_index);
                    delivered.push(delivery_key(
                        delta.agent,
                        answered.call(),
                        result,
                        &message,
                        result_part,
                    ));
                    self.result(delta, at, answered, result, result_part, &mut out);
                }
            }
        }
        for input in out {
            self.flow
                .send(input)
                .map_err(|_| ExtractStepError::FlowStopped)?;
        }
        self.delivered.extend(delivered);
        Ok(())
    }

    /// The call `result` (among `delta`'s new inputs) answers, taken out of
    /// what waits for a result.
    fn answered(&mut self, delta: &ConversationDelta, result: &ToolResult) -> Option<Answered> {
        let id = &result.call_id.0;
        if let Some(answered) = self.take_pending(delta, id, None) {
            self.history.remove(&(delta.conversation, id.clone()));
            return Some(answered);
        }
        let call = self.history.remove(&(delta.conversation, id.clone()))?;
        Some(
            self.take_pending(delta, id, Some(&call))
                .unwrap_or(Answered::History(call)),
        )
    }

    /// The call `id` the agent made in an output: of `delta`'s
    /// conversation when `same` is `None`, else of any conversation, the
    /// latest made with `same`'s name and arguments.
    fn take_pending(
        &mut self,
        delta: &ConversationDelta,
        id: &str,
        same: Option<&ToolCall>,
    ) -> Option<Answered> {
        let key = (delta.agent, id.to_owned());
        let calls = self.pending.get_mut(&key)?;
        let position = match same {
            None => calls
                .iter()
                .position(|pending| pending.conversation == delta.conversation),
            Some(call) => calls.iter().rposition(|pending| {
                pending.call.name == call.name && pending.call.arguments == call.arguments
            }),
        }?;
        let pending = calls.remove(position);
        if calls.is_empty() {
            self.pending.remove(&key);
        }
        Some(Answered::Output(pending))
    }

    /// Whether the extractors know `call`'s tool: only such calls are kept
    /// as history and only their results remembered as delivered.
    fn handles(&self, call: &ToolCall) -> bool {
        let context = ConversationContext::default();
        ToolExtractors::new(&self.config, &context).handles(call)
    }

    fn extractors_for(&self, conversation: ConversationId) -> (ExtractConfig, ConversationContext) {
        (
            self.config.clone(),
            self.contexts
                .get(&conversation)
                .cloned()
                .unwrap_or_default(),
        )
    }

    /// A call in `delta`'s output: its writes held, the call kept for its
    /// result.
    #[allow(clippy::too_many_arguments)]
    fn call(
        &mut self,
        delta: &ConversationDelta,
        at: Timestamp,
        call: &ToolCall,
        part: PartRef,
        spans: &[Span],
        sources: &BTreeMap<SpanId, AgentId>,
        out: &mut Vec<Extracted>,
    ) {
        let (config, context) = self.extractors_for(delta.conversation);
        let extractors = ToolExtractors::new(&config, &context);
        if !extractors.handles(call) {
            return;
        }
        let accesses = match extractors.extract(call, None) {
            Ok(accesses) => accesses,
            Err(error) => {
                tracing::debug!(exchange = %delta.exchange.ulid_text(), call = %call.id.0, error = ?error, "call not extracted");
                Vec::new()
            }
        };
        let written = write_spans(part, delta.agent, spans, |span| sources.get(&span).copied());
        let mut writes = Vec::new();
        for (index, access) in accesses.into_iter().enumerate() {
            let ExtractedOp::Write(_) = access.op else {
                continue;
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
                    op: WriteCall {
                        call: part,
                        spans: written.clone(),
                    },
                },
                outcome: None,
            });
        }
        let calls = self
            .pending
            .entry((delta.agent, call.id.0.clone()))
            .or_default();
        // A redelivered output replaces its own earlier entry.
        calls.retain(|pending| pending.conversation != delta.conversation);
        calls.push(Pending {
            conversation: delta.conversation,
            call: call.clone(),
            writes,
        });
    }

    /// A result in `delta` answering `answered`: the call's held writes
    /// released with their outcome, its reads extracted.
    fn result(
        &mut self,
        delta: &ConversationDelta,
        at: Timestamp,
        answered: Answered,
        result: &ToolResult,
        part: PartRef,
        out: &mut Vec<Extracted>,
    ) {
        // A call is extracted in the context of the conversation that made
        // it.
        let (call, writes, conversation) = match answered {
            Answered::Output(pending) => (pending.call, pending.writes, pending.conversation),
            Answered::History(call) => (call, Vec::new(), delta.conversation),
        };
        let (config, context) = self.extractors_for(conversation);
        let extractors = ToolExtractors::new(&config, &context);
        let accesses = match extractors.extract(&call, Some(result)) {
            Ok(accesses) => accesses,
            Err(error) => {
                tracing::debug!(exchange = %delta.exchange.ulid_text(), call = %result.call_id.0, error = ?error, "result not extracted");
                return;
            }
        };
        let mut held = writes.into_iter();
        for (index, access) in accesses.into_iter().enumerate() {
            match access.op {
                ExtractedOp::Write(outcome) => match held.next() {
                    Some((_, id)) => out.push(Extracted::WriteResult {
                        access: id,
                        outcome,
                    }),
                    None => tracing::debug!(
                        call = %result.call_id.0,
                        "a write extracted only with its result; not recorded"
                    ),
                },
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
    }

    async fn message(&self, hash: MessageHash) -> Result<Option<Message>, ExtractStepError> {
        self.messages
            .message(hash)
            .await
            .map_err(|error| ExtractStepError::Message {
                hash,
                reason: format!("{error:?}"),
            })
    }

    async fn spans_of(&self, exchange: ExchangeId) -> Result<Vec<Span>, ExtractStepError> {
        self.spans
            .exchange_spans(exchange)
            .await
            .map(|records| records.into_iter().map(|record| record.span).collect())
            .map_err(|error| ExtractStepError::Spans {
                reason: format!("{error:?}"),
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
                && let Some(record) =
                    self.spans
                        .span(source)
                        .await
                        .map_err(|error| ExtractStepError::Spans {
                            reason: format!("{error:?}"),
                        })?
            {
                sources.insert(source, record.span.agent);
            }
        }
        Ok(sources)
    }
}

impl Answered {
    fn call(&self) -> &ToolCall {
        match self {
            Self::Output(pending) => &pending.call,
            Self::History(call) => call,
        }
    }
}

/// `agent`'s delivery of `result` (at `part` of `message`) for `call`: a
/// digest of the call's id, name and arguments and the result's outcome
/// and text (or, for a result with no text, its part).
fn delivery_key(
    agent: AgentId,
    call: &ToolCall,
    result: &ToolResult,
    message: &Message,
    part: PartRef,
) -> DeliveryKey {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"crosstalk.live.extract.delivery.v1/");
    let mut field = |bytes: &[u8]| {
        hasher.update(&u64::try_from(bytes.len()).unwrap_or(u64::MAX).to_le_bytes());
        hasher.update(bytes);
    };
    field(call.id.0.as_bytes());
    field(call.name.0.as_bytes());
    match &call.arguments {
        ToolArguments::Json(json) => field(json.0.as_bytes()),
        ToolArguments::Invalid(text) => field(text.as_bytes()),
    }
    field(format!("{:?}", result.outcome).as_bytes());
    match message.part_text(part.index) {
        Ok(text) => field(text.as_bytes()),
        Err(_) => {
            field(&message.hash.digest().as_bytes()[..]);
            field(&part.index.to_le_bytes());
        }
    }
    (agent, *hasher.finalize().as_bytes())
}

fn part_ref(message: MessageHash, index: usize) -> PartRef {
    PartRef {
        message,
        index: u16::try_from(index).unwrap_or(u16::MAX),
    }
}

/// The conversation context a system prompt states.
fn context_of(message: &Message) -> ConversationContext {
    let MessageBody::System(parts) = &message.body else {
        return ConversationContext::default();
    };
    let text = parts
        .iter()
        .filter_map(|part| match part {
            SystemPart::Text(text) => Some(text.0.as_str()),
            SystemPart::Unknown(_) => None,
        })
        .collect::<Vec<_>>()
        .join("\n");
    ConversationContext::from_system_prompt(&text)
}

/// A ULID stamped at `at`'s millisecond whose random part is a digest of
/// the exchange, the call, the kind and the access's place.
fn access_id(
    exchange: ExchangeId,
    call: &str,
    kind: &str,
    index: usize,
    at: Timestamp,
) -> AccessId {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"crosstalk.live.extract.access.v1/");
    hasher.update(&exchange.as_ulid().to_be_bytes());
    hasher.update(&u64::try_from(call.len()).unwrap_or(u64::MAX).to_le_bytes());
    hasher.update(call.as_bytes());
    hasher.update(kind.as_bytes());
    hasher.update(&u64::try_from(index).unwrap_or(u64::MAX).to_le_bytes());
    let digest = hasher.finalize();
    let mut random = [0u8; 16];
    random[6..].copy_from_slice(&digest.as_bytes()[..10]);
    let millis = u128::from(at.as_micros() / 1_000) & ((1 << 48) - 1);
    AccessId::from_ulid((millis << 80) | u128::from_be_bytes(random))
}

#[cfg(test)]
mod tests;

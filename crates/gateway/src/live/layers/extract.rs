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
//! 2. Every tool result among the delta's new inputs is matched to the
//!    call `c` made earlier (by call id) and extracted with it: a write
//!    held without a result gets its [`Extracted::WriteResult`], a read is
//!    an [`Extracted::Read`] by `a` in `X`, at `X`'s start, naming the
//!    result part.
//! 3. Every tool call in `X`'s output is an [`Extracted::ToolCall`]; a
//!    known tool's writes are [`Extracted::Write`]s held without an
//!    outcome, carrying the spans [`write_spans`] finds at the call's part;
//!    the call waits for its result. A server tool's result in the same
//!    output is extracted at once.
//!
//! Access ids are derived from the exchange, the call and the access's
//! place among the call's accesses, so a redelivered delta extracts the
//! same accesses again (the flow consumer records an access once).

use std::collections::BTreeMap;

use crosstalk_flow::consumer::{Extracted, Observed, ReadResult, WriteCall};
use crosstalk_flow::extract::{ConversationContext, ExtractConfig, ToolExtractors, write_spans};
use crosstalk_provenance::scan::messages::{BlobMessages, MessageSource};
use crosstalk_provenance::store::{MemoryProvenanceStore, ProvenanceStore};
use crosstalk_spec::derived::provenance::span::{Origin, RelaySource, Span};
use crosstalk_spec::events::ingest::ConversationDelta;
use crosstalk_spec::ids::{AccessId, AgentId, ConversationId, ExchangeId, MessageHash, SpanId};
use crosstalk_spec::interfaces::l5_flow::{ExtractedOp, ResourceExtractor};
use crosstalk_spec::observed::message::{
    AssistantPart, Message, MessageBody, PartRef, SystemPart, ToolCall, ToolExecution, ToolResult,
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

/// A call waiting for its result.
#[derive(Debug, Clone)]
struct Pending {
    call: ToolCall,
    /// Its writes, held by the flow consumer, by locator order.
    writes: Vec<(crosstalk_spec::derived::flow::resource::Locator, AccessId)>,
}

/// The extraction step's state: each conversation's context and its calls
/// still waiting for a result.
pub struct Extraction {
    messages: BlobMessages<LiveBlobs>,
    spans: MemoryProvenanceStore,
    config: ExtractConfig,
    contexts: BTreeMap<ConversationId, ConversationContext>,
    pending: BTreeMap<(ConversationId, String), Pending>,
    flow: UnboundedSender<Extracted>,
}

impl Extraction {
    pub fn new(
        blobs: LiveBlobs,
        spans: MemoryProvenanceStore,
        flow: UnboundedSender<Extracted>,
    ) -> Self {
        Self {
            messages: BlobMessages::new(blobs),
            spans,
            config: ExtractConfig::default(),
            contexts: BTreeMap::new(),
            pending: BTreeMap::new(),
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
            if let MessageBody::Tool(results) = &message.body {
                for (index, result) in results.iter().enumerate() {
                    let part = part_ref(message.hash, index);
                    self.result(delta, at, result, part, &mut out);
                }
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
                {
                    let result_part = part_ref(message.hash, *result_index);
                    self.result(delta, at, result, result_part, &mut out);
                }
            }
        }
        for input in out {
            self.flow
                .send(input)
                .map_err(|_| ExtractStepError::FlowStopped)?;
        }
        Ok(())
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
        self.pending.insert(
            (delta.conversation, call.id.0.clone()),
            Pending {
                call: call.clone(),
                writes,
            },
        );
    }

    /// A result in `delta`: its call's held writes released with their
    /// outcome, its reads extracted.
    fn result(
        &mut self,
        delta: &ConversationDelta,
        at: Timestamp,
        result: &ToolResult,
        part: PartRef,
        out: &mut Vec<Extracted>,
    ) {
        let Some(pending) = self
            .pending
            .remove(&(delta.conversation, result.call_id.0.clone()))
        else {
            return;
        };
        let (config, context) = self.extractors_for(delta.conversation);
        let extractors = ToolExtractors::new(&config, &context);
        let accesses = match extractors.extract(&pending.call, Some(result)) {
            Ok(accesses) => accesses,
            Err(error) => {
                tracing::debug!(exchange = %delta.exchange.ulid_text(), call = %result.call_id.0, error = ?error, "result not extracted");
                return;
            }
        };
        let mut held = pending.writes.into_iter();
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

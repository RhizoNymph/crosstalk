//! Building the [`Normalization`] from a protocol's normalized request and
//! response: shared by every normalizer.
//!
//! - Every message body is hashed ([`crate::encoding`]) and kept once in
//!   `messages`, in order of first reference; the exchange references them
//!   by hash, so every hash it holds resolves
//!   (`canonical.exchange.references-resolve`).
//! - The exchange's `meta` is the raw exchange's, and its `continuation`
//!   the decoded request's (`canonical.normalize.continuation-preserved`).
//! - The response (or partial response) is always an `Assistant` message
//!   (`canonical.normalize.response-is-assistant`).
//! - Warnings, in message order, request first: an `UnknownBlock` for every
//!   `Unknown` part, wherever it sits (system, user and assistant parts,
//!   tool result contents; `canonical.normalize.unknown-block-reported`),
//!   and, in a `FullHistory` request only, an `OrphanToolResult` for each
//!   tool result whose call id no earlier tool call in the request has
//!   (`canonical.normalize.orphan-tool-result-reported`). An `Increment`
//!   request answers calls of a response L1 never sees, so it reports no
//!   orphans.
//! - A completed exchange without a first-chunk time (which the proxy
//!   records for every response with a body) takes the end time.

use std::collections::{BTreeMap, BTreeSet};

use crosstalk_spec::ids::MessageHash;
use crosstalk_spec::interfaces::l0_ingress::RawExchange;
use crosstalk_spec::interfaces::l1_canonical::{NormalizeWarning, NormalizedExchange};
use crosstalk_spec::observed::exchange::{
    Continuation, Exchange, ExchangeFailure, ExchangeOutcome, ResponseId, StopReason, TokenUsage,
};
use crosstalk_spec::observed::message::{
    AssistantPart, Message, MessageBody, SystemPart, ToolResult, ToolResultContent, Unknown,
    UserPart,
};

use crate::encoding;

/// A normalized exchange, plus the media bytes its messages reference by
/// hash: what L1 writes to the blob store before announcing the exchange.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Normalization {
    pub exchange: NormalizedExchange,
    /// Each distinct media blob a `Media` part names, in hash order.
    pub media: Vec<MediaBlob>,
}

/// Decoded media bytes and their hash ([`crate::encoding::hash_bytes`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MediaBlob {
    pub hash: MessageHash,
    pub bytes: Vec<u8>,
}

/// Media bytes found while normalizing, by hash.
#[derive(Debug, Default)]
pub(crate) struct MediaSink {
    blobs: BTreeMap<MessageHash, Vec<u8>>,
}

impl MediaSink {
    /// Keeps `bytes` and returns their hash.
    pub(crate) fn add(&mut self, bytes: Vec<u8>) -> MessageHash {
        let hash = encoding::hash_bytes(&bytes);
        self.blobs.entry(hash).or_insert(bytes);
        hash
    }

    fn into_blobs(self) -> Vec<MediaBlob> {
        self.blobs
            .into_iter()
            .map(|(hash, bytes)| MediaBlob { hash, bytes })
            .collect()
    }
}

/// What a response came to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ResponseRead {
    Completed {
        parts: Vec<AssistantPart>,
        response_id: Option<String>,
        stop: StopReason,
        usage: Option<TokenUsage>,
    },
    Failed {
        partial: Option<Vec<AssistantPart>>,
        failure: ExchangeFailure,
    },
}

/// The messages of one exchange, each body kept once.
#[derive(Default)]
struct MessageSet {
    messages: Vec<Message>,
    seen: BTreeSet<MessageHash>,
}

impl MessageSet {
    fn add(&mut self, body: MessageBody) -> MessageHash {
        let message = encoding::message(body);
        let hash = message.hash;
        if self.seen.insert(hash) {
            self.messages.push(message);
        }
        hash
    }
}

pub(crate) fn assemble(
    raw: &RawExchange,
    request: Vec<MessageBody>,
    response: ResponseRead,
    sink: MediaSink,
) -> Normalization {
    let continuation = raw.request.harness.continuation.clone();
    let response_body = match &response {
        ResponseRead::Completed { parts, .. } => Some(parts.as_slice()),
        ResponseRead::Failed { partial, .. } => partial.as_deref(),
    };
    let warnings = warnings(&request, response_body, &continuation);
    let mut set = MessageSet::default();
    let request: Vec<MessageHash> = request.into_iter().map(|body| set.add(body)).collect();
    let outcome = match response {
        ResponseRead::Completed {
            parts,
            response_id,
            stop,
            usage,
        } => ExchangeOutcome::Completed {
            response: set.add(MessageBody::Assistant(parts)),
            response_id: response_id.map(ResponseId),
            first_chunk_at: raw.first_chunk_at.unwrap_or(raw.ended_at),
            finished_at: raw.ended_at,
            stop,
            usage,
        },
        ResponseRead::Failed { partial, failure } => ExchangeOutcome::Failed {
            partial_response: partial.map(|parts| set.add(MessageBody::Assistant(parts))),
            first_chunk_at: raw.first_chunk_at,
            failed_at: raw.ended_at,
            failure,
        },
    };
    Normalization {
        exchange: NormalizedExchange {
            exchange: Exchange {
                meta: raw.meta.clone(),
                continuation,
                request,
                outcome,
            },
            messages: set.messages,
            warnings,
        },
        media: sink.into_blobs(),
    }
}

/// The warnings of a request and its response (module docs).
fn warnings(
    request: &[MessageBody],
    response: Option<&[AssistantPart]>,
    continuation: &Continuation,
) -> Vec<NormalizeWarning> {
    let full_history = matches!(continuation, Continuation::FullHistory);
    let mut calls: BTreeSet<&str> = BTreeSet::new();
    let mut warnings = Vec::new();
    for body in request {
        match body {
            MessageBody::System(parts) => {
                for part in parts {
                    if let SystemPart::Unknown(unknown) = part {
                        warnings.push(unknown_block(unknown));
                    }
                }
            }
            MessageBody::User(parts) => {
                for part in parts {
                    if let UserPart::Unknown(unknown) = part {
                        warnings.push(unknown_block(unknown));
                    }
                }
            }
            MessageBody::Assistant(parts) => {
                assistant_warnings(parts, &mut warnings);
                for part in parts {
                    if let AssistantPart::ToolCall(call) = part {
                        calls.insert(&call.id.0);
                    }
                }
            }
            MessageBody::Tool(results) => {
                for result in results.iter() {
                    result_warnings(result, &mut warnings);
                    if full_history && !calls.contains(result.call_id.0.as_str()) {
                        warnings.push(NormalizeWarning::OrphanToolResult {
                            call_id: result.call_id.0.clone(),
                        });
                    }
                }
            }
        }
    }
    if let Some(parts) = response {
        assistant_warnings(parts, &mut warnings);
    }
    warnings
}

fn unknown_block(unknown: &Unknown) -> NormalizeWarning {
    NormalizeWarning::UnknownBlock {
        kind: unknown.kind.clone(),
    }
}

fn assistant_warnings(parts: &[AssistantPart], warnings: &mut Vec<NormalizeWarning>) {
    for part in parts {
        match part {
            AssistantPart::Unknown(unknown) => warnings.push(unknown_block(unknown)),
            AssistantPart::ServerToolResult(result) => result_warnings(result, warnings),
            AssistantPart::Text(_) | AssistantPart::Reasoning(_) | AssistantPart::ToolCall(_) => {}
        }
    }
}

fn result_warnings(result: &ToolResult, warnings: &mut Vec<NormalizeWarning>) {
    for content in &result.content {
        if let ToolResultContent::Unknown(unknown) = content {
            warnings.push(unknown_block(unknown));
        }
    }
}

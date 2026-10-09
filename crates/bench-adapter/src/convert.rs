//! Bench rows as spec values: the reverse of [`crate::to_bench`]
//! (`to_bench::message`, `to_bench::world`).
//!
//! - **Messages.** Each bench part becomes the spec part at the same index,
//!   in the same message, so a location translates mechanically. What the
//!   bench does not store stays absent: no reasoning or tool-call
//!   signature, an opaque reasoning block with an empty signature, an
//!   unknown block of kind `unknown` with raw `{}`, a media part naming the
//!   empty media blob. Every part's spec text must equal its bench text
//!   ([`check_part_text`]: parity stage P1, run live); a difference fails the
//!   world `part_text_mismatch`. The spec `MessageHash` differs from the
//!   original's by design (signatures and raw blocks are gone); parity is on
//!   part order and text.
//! - **Exchanges.** The bench id is the spec id (same 128 bits). The client
//!   is what a proxy saw ([`client`]): replay ingress under
//!   `eval-<dataset>` (the corpus name ct-eval's converters used, kept so
//!   the composition sees what it saw at parity), the credential `k:<hex>`
//!   as an API key's keyed digest at secret version 0, the session
//!   as the harness session; `client.turn` is bench-only. The protocol, which
//!   the bench does not carry and no detection layer reads, follows the
//!   vendor. A response is one message; a failed call's error text is the
//!   `to_bench::world` writes it, read back.

use std::collections::{BTreeMap, HashMap};

use a2a_bench_format as bench;
use bench::check::WorldInputs;
use bench::message::{
    AssistantPart, Body, MediaKind as BenchMedia, ResultContent, SystemPart, ToolArguments,
    ToolExecution, ToolOutcome, ToolPart, ToolResult, UserPart,
};
use crosstalk_spec::ids::{CredentialHash, ExchangeId, MessageHash, SecretVersion};
use crosstalk_spec::interfaces::l1_canonical::NormalizedExchange;
use crosstalk_spec::observed::client::{
    ClientContext, CorpusId, CredentialRef, CredentialScheme, HarnessIds, InferenceServer,
    IngressMode, RequestClass, Upstream, UpstreamId, UpstreamKind, Vendor,
};
use crosstalk_spec::observed::exchange::{
    Continuation, Exchange, ExchangeFailure, ExchangeMeta, ExchangeOutcome, ModelName, StopReason,
    Transport, WireProtocol,
};
use crosstalk_spec::observed::message::{
    self as spec, CanonicalJson, MediaBlob, MediaKind, Message, MessageBody, Reasoning, Text,
    ToolCallId, ToolName, ToolResultContent,
};
use crosstalk_spec::support::{Blake3, NonEmpty, Timestamp};

use crate::detect::Timed;
use crate::to_bench::world::MessageIndex;
use crate::{FailureCode, WorldFailure};

/// The kind an unknown block is given back (the bench keeps none).
pub const UNKNOWN_KIND: &str = "unknown";

fn conversion(detail: impl std::fmt::Display) -> WorldFailure {
    WorldFailure::new(FailureCode::Conversion, detail)
}

/// The blob every media part names: the bench keeps no media bytes.
pub fn empty_media() -> MediaBlob {
    MediaBlob::new(Vec::new())
}

fn unknown() -> spec::Unknown {
    spec::Unknown {
        kind: UNKNOWN_KIND.to_owned(),
        raw: CanonicalJson("{}".to_owned()),
    }
}

fn media(kind: BenchMedia) -> Result<spec::Media, WorldFailure> {
    let kind = match kind {
        BenchMedia::Image => MediaKind::Image,
        BenchMedia::Audio => MediaKind::Audio,
        BenchMedia::Document => MediaKind::Document,
        BenchMedia::Other => {
            return Err(conversion(
                "a media part of kind other: the spec has no such kind",
            ));
        }
    };
    Ok(spec::Media {
        kind,
        blob: empty_media().hash(),
    })
}

fn tool_result(result: &ToolResult) -> Result<spec::ToolResult, WorldFailure> {
    let mut content = Vec::with_capacity(result.content.len());
    for item in &result.content {
        content.push(match item {
            ResultContent::Text { text } => ToolResultContent::Text(Text(text.clone())),
            ResultContent::Media { kind } => ToolResultContent::Media(media(*kind)?),
            ResultContent::Unknown => ToolResultContent::Unknown(unknown()),
        });
    }
    Ok(spec::ToolResult {
        call_id: ToolCallId(result.call_id.clone()),
        content,
        outcome: match result.outcome {
            ToolOutcome::Success => spec::ToolOutcome::Success,
            ToolOutcome::Error => spec::ToolOutcome::Error,
            ToolOutcome::Unknown => spec::ToolOutcome::Unknown,
        },
    })
}

/// The spec body of a bench message, part for part.
pub fn body(message: &bench::message::Message) -> Result<MessageBody, WorldFailure> {
    Ok(match message.body() {
        Body::System(parts) => MessageBody::System(
            parts
                .iter()
                .map(|part| match part {
                    SystemPart::Text { text } => spec::SystemPart::Text(Text(text.clone())),
                    SystemPart::Unknown => spec::SystemPart::Unknown(unknown()),
                })
                .collect(),
        ),
        Body::User(parts) => {
            let mut out = Vec::with_capacity(parts.len());
            for part in parts {
                out.push(match part {
                    UserPart::Text { text } => spec::UserPart::Text(Text(text.clone())),
                    UserPart::Media { kind } => spec::UserPart::Media(media(*kind)?),
                    UserPart::Unknown => spec::UserPart::Unknown(unknown()),
                });
            }
            MessageBody::User(out)
        }
        Body::Assistant(parts) => {
            let mut out = Vec::with_capacity(parts.len());
            for part in parts {
                out.push(match part {
                    AssistantPart::Text { text } => spec::AssistantPart::Text(Text(text.clone())),
                    AssistantPart::Reasoning { text } => {
                        spec::AssistantPart::Reasoning(Reasoning::Visible {
                            text: Text(text.clone()),
                            signature: None,
                        })
                    }
                    AssistantPart::ReasoningOpaque => {
                        spec::AssistantPart::Reasoning(Reasoning::Opaque {
                            signature: String::new(),
                        })
                    }
                    AssistantPart::ToolCall(call) => {
                        spec::AssistantPart::ToolCall(spec::ToolCall {
                            id: ToolCallId(call.call_id.clone()),
                            name: ToolName(call.name.clone()),
                            arguments: match &call.arguments {
                                ToolArguments::Json(json) => spec::ToolArguments::Json(
                                    CanonicalJson(json.as_str().to_owned()),
                                ),
                                ToolArguments::Invalid(text) => {
                                    spec::ToolArguments::Invalid(text.clone())
                                }
                            },
                            execution: match call.execution {
                                ToolExecution::Client => spec::ToolExecution::Client,
                                ToolExecution::Server => spec::ToolExecution::Server,
                            },
                            signature: None,
                        })
                    }
                    AssistantPart::ServerToolResult(result) => {
                        spec::AssistantPart::ServerToolResult(tool_result(result)?)
                    }
                    AssistantPart::Unknown => spec::AssistantPart::Unknown(unknown()),
                });
            }
            MessageBody::Assistant(out)
        }
        Body::Tool(parts) => {
            let mut out = Vec::with_capacity(parts.len());
            for ToolPart::ToolResult(result) in parts {
                out.push(tool_result(result)?);
            }
            MessageBody::Tool(
                NonEmpty::from_vec(out)
                    .ok_or_else(|| conversion("a tool message without results"))?,
            )
        }
    })
}

/// Checks that every part of `converted` has `message`'s part text: the
/// same text, or no text on both sides.
pub fn check_part_text(
    message: &bench::message::Message,
    converted: &Message,
) -> Result<(), WorldFailure> {
    let parts = u16::try_from(message.part_count())
        .map_err(|_| conversion(format!("message {} has too many parts", message.id())))?;
    for index in 0..parts {
        let bench_text = message.part_text(index).ok();
        let spec_text = converted.part_text(index).ok();
        if bench_text.as_deref() != spec_text.as_deref() {
            return Err(WorldFailure::new(
                FailureCode::PartTextMismatch,
                format!(
                    "message {} part {index}: bench {}, spec {}",
                    message.id(),
                    describe(bench_text.as_deref()),
                    describe(spec_text.as_deref()),
                ),
            ));
        }
    }
    if converted.part_text(parts).is_ok() {
        return Err(WorldFailure::new(
            FailureCode::PartTextMismatch,
            format!("message {}: the spec message has more parts", message.id()),
        ));
    }
    Ok(())
}

/// A part text's shape, never its content.
fn describe(text: Option<&str>) -> String {
    match text {
        Some(text) => format!("{} bytes", text.len()),
        None => "no text".to_owned(),
    }
}

/// The spec message of `message`, its part text checked.
pub fn message(message: &bench::message::Message) -> Result<Message, WorldFailure> {
    let converted = Message::new(body(message)?);
    check_part_text(message, &converted)?;
    Ok(converted)
}

/// The vendor a bench client names (`to_bench::world`'s `vendor`, read back).
pub fn upstream_kind(vendor: Option<&str>) -> UpstreamKind {
    match vendor {
        Some("anthropic") => UpstreamKind::VendorApi(Vendor::Anthropic),
        Some("openai") => UpstreamKind::VendorApi(Vendor::OpenAi),
        Some("google") => UpstreamKind::VendorApi(Vendor::Google),
        Some("github_copilot") => UpstreamKind::VendorApi(Vendor::GithubCopilot),
        Some("vllm") => UpstreamKind::InferenceServer(InferenceServer::Vllm),
        Some("sglang") => UpstreamKind::InferenceServer(InferenceServer::Sglang),
        Some(other) => UpstreamKind::VendorApi(Vendor::Other(other.to_owned())),
        None => UpstreamKind::VendorApi(Vendor::Other(String::new())),
    }
}

/// The wire protocol a vendor speaks. No detection layer reads it.
pub fn protocol(kind: &UpstreamKind) -> WireProtocol {
    match kind {
        UpstreamKind::VendorApi(Vendor::Anthropic)
        | UpstreamKind::Subscription(Vendor::Anthropic) => WireProtocol::AnthropicMessages,
        UpstreamKind::VendorApi(Vendor::Google) | UpstreamKind::Subscription(Vendor::Google) => {
            WireProtocol::GeminiGenerate
        }
        _ => WireProtocol::OpenAiChat,
    }
}

/// The context a proxy in front of the model would have recorded for
/// `client`, replayed under `dataset`'s corpus.
pub fn client(
    dataset: &bench::ids::DatasetId,
    client: &bench::exchange::Client,
) -> Result<ClientContext, WorldFailure> {
    let hex = client
        .credential
        .strip_prefix("k:")
        .ok_or_else(|| conversion("a credential that is not a k:<hex> digest".to_owned()))?;
    let digest = Blake3::from_hex(hex)
        .map_err(|error| conversion(format!("a credential digest: {error:?}")))?;
    Ok(ClientContext {
        ingress: IngressMode::Replay {
            corpus: CorpusId(format!("eval-{dataset}")),
        },
        upstream: Upstream {
            id: UpstreamId(format!("eval-{dataset}")),
            kind: upstream_kind(client.vendor.as_deref()),
        },
        credential: Some(CredentialRef {
            scheme: CredentialScheme::ApiKey,
            hash: CredentialHash::from_keyed_digest(SecretVersion(0), digest),
        }),
        account: None,
        previous_digests: None,
        harness: None,
        ids: HarnessIds {
            session: client.session.clone(),
            agent: None,
            parent_agent: None,
        },
        class: RequestClass::Main,
    })
}

/// A stop reason as `to_bench::world` writes it.
pub fn stop(text: Option<&str>) -> StopReason {
    match text {
        Some("end_turn") => StopReason::EndTurn,
        Some("tool_use") => StopReason::ToolUse,
        Some("max_tokens") => StopReason::MaxTokens,
        Some("stop_sequence") => StopReason::StopSequence,
        Some("refusal") => StopReason::Refusal,
        Some("aborted") => StopReason::Aborted,
        _ => StopReason::Other,
    }
}

/// A failure as `to_bench::world` writes it.
pub fn failure(text: &str) -> Result<ExchangeFailure, WorldFailure> {
    let parsed = match text {
        "upstream_unreachable" => Some(ExchangeFailure::UpstreamUnreachable),
        "stream_truncated" => Some(ExchangeFailure::StreamTruncated),
        "upstream_error_event" => Some(ExchangeFailure::UpstreamErrorEvent),
        "unparseable_response" => Some(ExchangeFailure::UnparseableResponse),
        "client_disconnected" => Some(ExchangeFailure::ClientDisconnected),
        "timeout" => Some(ExchangeFailure::Timeout),
        other => {
            if let Some(status) = other.strip_prefix("upstream ") {
                status
                    .parse()
                    .ok()
                    .map(|status| ExchangeFailure::Upstream { status })
            } else if let Some(offset) = other.strip_prefix("malformed_stream at ") {
                offset
                    .parse()
                    .ok()
                    .map(|offset| ExchangeFailure::MalformedStream { offset })
            } else {
                None
            }
        }
    };
    parsed.ok_or_else(|| conversion(format!("a response error the spec cannot hold: {text:?}")))
}

/// One world, converted: its exchanges in order, ready to ingest, and
/// each spec message hash's bench id.
#[derive(Debug, Clone, Default)]
pub struct ConvertedWorld {
    pub exchanges: Vec<(Timestamp, NormalizedExchange)>,
    pub index: MessageIndex,
    /// Each exchange's position in `exchanges`.
    pub positions: HashMap<ExchangeId, usize>,
}

impl ConvertedWorld {
    pub fn timed(&self) -> Vec<Timed<'_>> {
        self.exchanges
            .iter()
            .map(|(at, exchange)| Timed { exchange, at: *at })
            .collect()
    }

    pub fn exchange(&self, id: ExchangeId) -> Option<&NormalizedExchange> {
        self.positions
            .get(&id)
            .and_then(|at| self.exchanges.get(*at))
            .map(|(_, exchange)| exchange)
    }
}

/// `inputs` (a world of `dataset`) as spec exchanges.
pub fn world(
    dataset: &bench::ids::DatasetId,
    inputs: &WorldInputs,
) -> Result<ConvertedWorld, WorldFailure> {
    let mut converted: BTreeMap<bench::ids::MessageId, Message> = BTreeMap::new();
    for bench_message in inputs.messages() {
        converted.insert(bench_message.id(), message(bench_message)?);
    }
    let mut out = ConvertedWorld::default();
    for bench_exchange in inputs.exchanges() {
        let exchange = exchange(dataset, bench_exchange, &converted)?;
        let id = exchange.exchange.meta.id;
        for (bench_id, spec) in bench_exchange
            .request
            .messages
            .iter()
            .chain(&bench_exchange.response.messages)
            .filter_map(|bench_id| converted.get(bench_id).map(|spec| (bench_id, spec)))
        {
            out.index.insert(id, spec.hash, *bench_id);
        }
        out.positions.insert(id, out.exchanges.len());
        out.exchanges.push((
            Timestamp::from_micros(bench_exchange.at_us.as_micros()),
            exchange,
        ));
    }
    Ok(out)
}

/// One bench exchange, with its messages from `messages`, as a checked
/// normalized exchange.
pub fn exchange(
    dataset: &bench::ids::DatasetId,
    exchange: &bench::exchange::Exchange,
    messages: &BTreeMap<bench::ids::MessageId, Message>,
) -> Result<NormalizedExchange, WorldFailure> {
    let id = ExchangeId::from_ulid(exchange.id.raw());
    let at = Timestamp::from_micros(exchange.at_us.as_micros());
    let lookup = |bench_id: &bench::ids::MessageId| {
        messages
            .get(bench_id)
            .ok_or_else(|| conversion(format!("exchange {} names an unknown message", exchange.id)))
    };
    let mut request: Vec<MessageHash> = Vec::with_capacity(exchange.request.messages.len());
    let mut held: Vec<Message> = Vec::new();
    let keep = |message: &Message, held: &mut Vec<Message>| {
        if !held.iter().any(|kept| kept.hash == message.hash) {
            held.push(message.clone());
        }
    };
    for bench_id in &exchange.request.messages {
        let message = lookup(bench_id)?;
        request.push(message.hash);
        keep(message, &mut held);
    }
    let response = match exchange.response.messages.as_slice() {
        [] => None,
        [one] => {
            let message = lookup(one)?;
            keep(message, &mut held);
            Some(message.hash)
        }
        _ => {
            return Err(conversion(format!(
                "exchange {} has {} response messages; the spec holds one",
                exchange.id,
                exchange.response.messages.len()
            )));
        }
    };
    let outcome = match (&exchange.response.error, response) {
        (None, Some(response)) => ExchangeOutcome::Completed {
            response,
            response_id: None,
            first_chunk_at: at,
            finished_at: at,
            stop: stop(exchange.response.stop.as_deref()),
            usage: None,
        },
        (None, None) => {
            return Err(conversion(format!(
                "exchange {} has neither a response nor an error",
                exchange.id
            )));
        }
        (Some(error), partial_response) => ExchangeOutcome::Failed {
            partial_response,
            first_chunk_at: partial_response.map(|_| at),
            failed_at: at,
            failure: failure(error)?,
        },
    };
    let context = client(dataset, &exchange.client)?;
    let normalized = NormalizedExchange {
        exchange: Exchange {
            meta: ExchangeMeta {
                id,
                protocol: protocol(&context.upstream.kind),
                transport: Transport::Http,
                model: ModelName(exchange.client.model.clone().unwrap_or_default()),
                client: context,
                started_at: at,
            },
            continuation: Continuation::FullHistory,
            request,
            outcome,
        },
        media: if held.iter().any(|message| has_media(&message.body)) {
            vec![empty_media()]
        } else {
            Vec::new()
        },
        messages: held,
        warnings: Vec::new(),
    };
    normalized.check().map_err(|reason| {
        conversion(format!(
            "exchange {} is not a valid normalized exchange: {reason:?}",
            exchange.id
        ))
    })?;
    Ok(normalized)
}

fn has_media(body: &MessageBody) -> bool {
    let result = |result: &spec::ToolResult| {
        result
            .content
            .iter()
            .any(|content| matches!(content, ToolResultContent::Media(_)))
    };
    match body {
        MessageBody::System(_) => false,
        MessageBody::User(parts) => parts
            .iter()
            .any(|part| matches!(part, spec::UserPart::Media(_))),
        MessageBody::Assistant(parts) => parts.iter().any(|part| match part {
            spec::AssistantPart::ServerToolResult(found) => result(found),
            _ => false,
        }),
        MessageBody::Tool(results) => results.iter().any(result),
    }
}

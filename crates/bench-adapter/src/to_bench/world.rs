//! One world in the bench format: its messages (each once), its declared
//! agents and its exchanges in time order (no labels: the bench labels).
//!
//! [`WorldBuilder`] takes exchanges one at a time with their messages
//! (`from-export` reads them from the gateway's exchange log and blobs).
//! It keeps the [`MessageIndex`]: each spec `MessageHash`'s bench
//! `MessageId` (two spec bodies that differ only in what the bench drops,
//! such as a signature, share one), which translates a detection's spec
//! locations into bench locations.

use std::collections::{BTreeSet, HashMap, HashSet};

use a2a_bench_format as bench;
use bench::exchange::{Client, Exchange, Fidelity, Request, Response, WorldDecl};
use bench::ids::{MessageId, SourceRef};
use bench::time::Timestamp;
use crosstalk_spec::ids::{ExchangeId, MessageHash};
use crosstalk_spec::observed::client::{ClientContext, InferenceServer, UpstreamKind, Vendor};
use crosstalk_spec::observed::exchange::{
    Continuation, Exchange as SpecExchange, ExchangeFailure, ExchangeOutcome, StopReason,
};
use crosstalk_spec::observed::message::Message as SpecMessage;
use crosstalk_spec::support::Timestamp as SpecTimestamp;

use super::{Gap, Lossy, ToBenchError, ids, message};

/// A world ready to write: every row of its messages and exchanges
/// sections.
#[derive(Debug, Clone)]
pub struct WorldExport {
    pub key: bench::ids::WorldKey,
    pub decl: WorldDecl,
    pub messages: Vec<bench::message::Message>,
    pub exchanges: Vec<Exchange>,
    pub index: MessageIndex,
    pub lossy: Lossy,
}

/// Spec message hashes as bench message ids, and the world's exchanges.
#[derive(Debug, Clone, Default)]
pub struct MessageIndex {
    ids: HashMap<MessageHash, MessageId>,
    /// The exported exchanges.
    exchanges: HashSet<ExchangeId>,
}

impl MessageIndex {
    /// Records that `exchange` carries the spec message `hash`, whose bench
    /// id is `id`: how a reader of bench files (`bench_detect`), which
    /// converted the bench message to the spec one, builds the index.
    pub fn insert(&mut self, exchange: ExchangeId, hash: MessageHash, id: MessageId) {
        self.ids.insert(hash, id);
        self.exchanges.insert(exchange);
    }

    pub fn id(&self, hash: MessageHash) -> Result<MessageId, ToBenchError> {
        self.ids
            .get(&hash)
            .copied()
            .ok_or(ToBenchError::UnknownMessage(hash))
    }

    /// A spec location in `exchange`, as a bench location.
    pub fn location(
        &self,
        exchange: ExchangeId,
        at: &crosstalk_spec::derived::provenance::span::SpanLocation,
    ) -> Result<bench::location::Location, ToBenchError> {
        if !self.exchanges.contains(&exchange) {
            return Err(ToBenchError::UnknownExchange(exchange));
        }
        Ok(bench::location::Location {
            exchange: ids::exchange(exchange),
            message: self.id(at.part.message)?,
            part: at.part.index,
            range: ids::range(at.range)?,
        })
    }
}

/// What the adapter knows about one exchange beyond the spec record.
#[derive(Debug, Clone, Copy)]
pub struct Draft<'a> {
    pub exchange: &'a SpecExchange,
    pub at: SpecTimestamp,
    pub fidelity: Fidelity,
    pub source: &'a SourceRef,
    /// The request's ordinal in its client session, when known (the demo
    /// swarm's join key).
    pub turn: Option<u32>,
}

/// Builds a [`WorldExport`] one exchange at a time, in time order.
#[derive(Debug, Default)]
pub struct WorldBuilder {
    messages: Vec<bench::message::Message>,
    seen: BTreeSet<MessageId>,
    index: MessageIndex,
    exchanges: Vec<Exchange>,
    lossy: Lossy,
}

impl WorldBuilder {
    /// Adds `draft`, reading each message it names that the world has not
    /// seen yet through `message`.
    pub fn exchange<'m>(
        &mut self,
        draft: Draft<'_>,
        mut message: impl FnMut(MessageHash) -> Option<&'m SpecMessage>,
    ) -> Result<(), ToBenchError> {
        let spec = draft.exchange;
        let id = spec.meta.id;
        if let Continuation::Increment { .. } = spec.continuation {
            return Err(ToBenchError::Unexpressible(Gap::IncrementalRequest {
                exchange: id,
            }));
        }
        let (response, stop, error) = match &spec.outcome {
            ExchangeOutcome::Completed { response, stop, .. } => {
                (Some(*response), Some(stop_text(*stop)), None)
            }
            ExchangeOutcome::Failed {
                partial_response,
                failure,
                ..
            } => (*partial_response, None, Some(failure_text(*failure))),
        };
        let mut request = Vec::with_capacity(spec.request.len());
        for hash in &spec.request {
            request.push(self.message(id, *hash, &mut message)?);
        }
        let response = match response {
            Some(hash) => vec![self.message(id, hash, &mut message)?],
            None => Vec::new(),
        };
        self.exchanges.push(Exchange {
            id: ids::exchange(id),
            at_us: Timestamp::from_micros(draft.at.as_micros()),
            client: client(&spec.meta.client, &spec.meta.model.0, draft.turn, id)?,
            request: Request {
                messages: request,
                tools: None,
            },
            response: Response {
                messages: response,
                stop,
                error,
            },
            fidelity: draft.fidelity,
            source: draft.source.clone(),
        });
        self.index.exchanges.insert(id);
        Ok(())
    }

    fn message<'m>(
        &mut self,
        exchange: ExchangeId,
        hash: MessageHash,
        lookup: &mut impl FnMut(MessageHash) -> Option<&'m SpecMessage>,
    ) -> Result<MessageId, ToBenchError> {
        if let Some(id) = self.index.ids.get(&hash) {
            return Ok(*id);
        }
        let spec = lookup(hash).ok_or(ToBenchError::MissingBody { exchange, hash })?;
        let converted = message::convert(spec, &mut self.lossy)?;
        let id = converted.id();
        self.index.ids.insert(hash, id);
        if self.seen.insert(id) {
            self.messages.push(converted);
        }
        Ok(id)
    }

    pub fn index(&self) -> &MessageIndex {
        &self.index
    }

    pub fn finish(self, key: bench::ids::WorldKey, decl: WorldDecl) -> WorldExport {
        WorldExport {
            key,
            decl,
            messages: self.messages,
            exchanges: self.exchanges,
            index: self.index,
            lossy: self.lossy,
        }
    }
}

/// What a proxy observes: the credential's digest (`k:<hex>`),
/// the harness session, the request's ordinal in it, the vendor and model.
fn client(
    context: &ClientContext,
    model: &str,
    turn: Option<u32>,
    exchange: ExchangeId,
) -> Result<Client, ToBenchError> {
    let credential = context
        .credential
        .as_ref()
        .ok_or(ToBenchError::Unexpressible(Gap::NoCredential { exchange }))?;
    Ok(Client {
        credential: format!("k:{}", credential.hash.digest().to_hex()),
        session: context.ids.session.clone(),
        turn,
        vendor: Some(vendor(&context.upstream.kind)),
        model: Some(model.to_owned()),
    })
}

fn vendor(kind: &UpstreamKind) -> String {
    match kind {
        UpstreamKind::VendorApi(vendor) | UpstreamKind::Subscription(vendor) => match vendor {
            Vendor::Anthropic => "anthropic".to_owned(),
            Vendor::OpenAi => "openai".to_owned(),
            Vendor::Google => "google".to_owned(),
            Vendor::GithubCopilot => "github_copilot".to_owned(),
            Vendor::Other(name) => name.clone(),
        },
        UpstreamKind::InferenceServer(InferenceServer::Vllm) => "vllm".to_owned(),
        UpstreamKind::InferenceServer(InferenceServer::Sglang) => "sglang".to_owned(),
    }
}

/// A failed exchange's failure, as `response.error`.
fn failure_text(failure: ExchangeFailure) -> String {
    match failure {
        ExchangeFailure::Upstream { status } => format!("upstream {status}"),
        ExchangeFailure::UpstreamUnreachable => "upstream_unreachable".to_owned(),
        ExchangeFailure::StreamTruncated => "stream_truncated".to_owned(),
        ExchangeFailure::MalformedStream { offset } => format!("malformed_stream at {offset}"),
        ExchangeFailure::UpstreamErrorEvent => "upstream_error_event".to_owned(),
        ExchangeFailure::UnparseableResponse => "unparseable_response".to_owned(),
        ExchangeFailure::ClientDisconnected => "client_disconnected".to_owned(),
        ExchangeFailure::Timeout => "timeout".to_owned(),
    }
}

fn stop_text(stop: StopReason) -> String {
    match stop {
        StopReason::EndTurn => "end_turn",
        StopReason::ToolUse => "tool_use",
        StopReason::MaxTokens => "max_tokens",
        StopReason::StopSequence => "stop_sequence",
        StopReason::Refusal => "refusal",
        StopReason::Aborted => "aborted",
        StopReason::Other => "other",
    }
    .to_owned()
}

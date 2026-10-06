//! One world in the bench format: its messages (each once), its declared
//! agents, its exchanges in time order, and its labels.
//!
//! [`WorldBuilder`] takes exchanges one at a time with their messages, so
//! both a ct-eval [`World`] (its `CorpusExchange`s hold their messages) and
//! the demo swarm (messages read from the gateway's blobs) build through it.
//! It keeps the [`MessageIndex`]: each spec `MessageHash`'s bench
//! `MessageId` (two spec bodies that differ only in what the bench drops,
//! such as a signature, share one), and the first exchange of each agent
//! that carries each message, which places a ct-eval location that names
//! no exchange.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use a2a_bench_format as bench;
use bench::check::{WorldInputs, check_labels};
use bench::exchange::{
    AgentDecl, Client, Driven, Exchange, Fidelity, Request, Response, WorldDecl,
};
use bench::files::Coverage;
use bench::ids::MessageId;
use bench::labels::Label;
use bench::time::Timestamp;
use crosstalk_spec::ids::{ExchangeId, MessageHash};
use crosstalk_spec::observed::client::{ClientContext, InferenceServer, UpstreamKind, Vendor};
use crosstalk_spec::observed::exchange::{
    Continuation, Exchange as SpecExchange, ExchangeFailure, ExchangeOutcome, StopReason,
};
use crosstalk_spec::observed::message::Message as SpecMessage;
use crosstalk_spec::support::Timestamp as SpecTimestamp;

use super::{Gap, GoldenError, Lossy, ids, message};
use crate::corpus::{self, World};
use crate::keys::SourceRef;

/// A world ready to write: every row of its four files' sections but the
/// predictions.
#[derive(Debug, Clone)]
pub struct WorldExport {
    pub key: bench::ids::WorldKey,
    pub decl: WorldDecl,
    pub messages: Vec<bench::message::Message>,
    pub exchanges: Vec<Exchange>,
    pub labels: Vec<Label>,
    /// Counts for the world's manifest entry (`WorldEntry::notes`): labels
    /// the converter dropped or did not make, by name.
    pub notes: BTreeMap<String, u64>,
    pub coverage: Coverage,
    pub index: MessageIndex,
    pub lossy: Lossy,
}

impl WorldExport {
    /// The world's inputs, checked (`WorldInputs::new`), with its labels
    /// checked against them (`check_labels`).
    pub fn check(&self) -> Result<WorldInputs, GoldenError> {
        let inputs = WorldInputs::new(
            &self.key,
            self.messages.clone(),
            self.decl.clone(),
            self.exchanges.clone(),
        )
        .map_err(|source| GoldenError::Inputs {
            world: self.key.to_string(),
            source,
        })?;
        check_labels(&inputs, &self.labels).map_err(|source| GoldenError::Labels {
            world: self.key.to_string(),
            source,
        })?;
        Ok(inputs)
    }
}

/// Spec message hashes as bench message ids, and where each message is
/// first carried.
#[derive(Debug, Clone, Default)]
pub struct MessageIndex {
    ids: HashMap<MessageHash, MessageId>,
    /// The first exchange of an agent whose request or response carries a
    /// message.
    by_agent: HashMap<(MessageHash, String), ExchangeId>,
    /// The first exchange of any agent that carries it.
    first: HashMap<MessageHash, ExchangeId>,
    /// The exported exchanges.
    exchanges: HashSet<ExchangeId>,
}

impl MessageIndex {
    /// Records that `exchange` carries the spec message `hash`, whose bench
    /// id is `id`: how a reader of bench files (`bench_detect`), which
    /// converted the bench message to the spec one, builds the index.
    pub fn insert(&mut self, exchange: ExchangeId, hash: MessageHash, id: MessageId) {
        self.ids.insert(hash, id);
        self.first.entry(hash).or_insert(exchange);
        self.exchanges.insert(exchange);
    }

    pub fn id(&self, hash: MessageHash) -> Result<MessageId, GoldenError> {
        self.ids
            .get(&hash)
            .copied()
            .ok_or(GoldenError::UnknownMessage(hash))
    }

    /// The first exchange of `agent` that carries `hash`, else the world's
    /// first that does.
    pub fn carrier(&self, hash: MessageHash, agent: &str) -> Result<ExchangeId, GoldenError> {
        self.by_agent
            .get(&(hash, agent.to_owned()))
            .or_else(|| self.first.get(&hash))
            .copied()
            .ok_or(GoldenError::UnknownMessage(hash))
    }

    /// A spec location in `exchange`, as a bench location.
    pub fn location(
        &self,
        exchange: ExchangeId,
        at: &crosstalk_spec::derived::provenance::span::SpanLocation,
    ) -> Result<bench::location::Location, GoldenError> {
        if !self.exchanges.contains(&exchange) {
            return Err(GoldenError::UnknownExchange(exchange));
        }
        Ok(bench::location::Location {
            exchange: ids::exchange(exchange),
            message: self.id(at.part.message)?,
            part: at.part.index,
            range: ids::range(at.range)?,
        })
    }
}

/// What the eval knows about one exchange beyond the spec record.
#[derive(Debug, Clone, Copy)]
pub struct Draft<'a> {
    pub exchange: &'a SpecExchange,
    /// The true agent's name.
    pub agent: &'a str,
    pub at: SpecTimestamp,
    pub fidelity: corpus::Fidelity,
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
    owners: Vec<(ExchangeId, String)>,
    lossy: Lossy,
}

impl WorldBuilder {
    /// Adds `draft`, reading each message it names that the world has not
    /// seen yet through `message`.
    pub fn exchange<'m>(
        &mut self,
        draft: Draft<'_>,
        mut message: impl FnMut(MessageHash) -> Option<&'m SpecMessage>,
    ) -> Result<(), GoldenError> {
        let spec = draft.exchange;
        let id = spec.meta.id;
        if let Continuation::Increment { .. } = spec.continuation {
            return Err(GoldenError::Unexpressible(Gap::IncrementalRequest {
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
            request.push(self.message(id, draft.agent, *hash, &mut message)?);
        }
        let response = match response {
            Some(hash) => vec![self.message(id, draft.agent, hash, &mut message)?],
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
            fidelity: fidelity(draft.fidelity),
            source: ids::source(draft.source),
        });
        self.owners.push((id, draft.agent.to_owned()));
        self.index.exchanges.insert(id);
        Ok(())
    }

    fn message<'m>(
        &mut self,
        exchange: ExchangeId,
        agent: &str,
        hash: MessageHash,
        lookup: &mut impl FnMut(MessageHash) -> Option<&'m SpecMessage>,
    ) -> Result<MessageId, GoldenError> {
        self.index
            .by_agent
            .entry((hash, agent.to_owned()))
            .or_insert(exchange);
        self.index.first.entry(hash).or_insert(exchange);
        if let Some(id) = self.index.ids.get(&hash) {
            return Ok(*id);
        }
        let spec = lookup(hash).ok_or(GoldenError::MissingBody { exchange, hash })?;
        let converted = message::convert(spec, &mut self.lossy)?;
        let id = converted.id();
        self.index.ids.insert(hash, id);
        if self.seen.insert(id) {
            self.messages.push(converted);
        }
        Ok(id)
    }

    /// The exchanges added so far, with their true agents' names.
    pub fn owners(&self) -> &[(ExchangeId, String)] {
        &self.owners
    }

    pub fn index(&self) -> &MessageIndex {
        &self.index
    }

    pub fn finish(
        self,
        key: bench::ids::WorldKey,
        decl: WorldDecl,
        labels: Vec<Label>,
        notes: BTreeMap<String, u64>,
        coverage: Coverage,
    ) -> WorldExport {
        WorldExport {
            key,
            decl,
            messages: self.messages,
            exchanges: self.exchanges,
            labels,
            notes,
            coverage,
            index: self.index,
            lossy: self.lossy,
        }
    }
}

/// A ct-eval world in the bench format.
pub fn export(world: &World) -> Result<WorldExport, GoldenError> {
    let mut builder = WorldBuilder::default();
    for exchange in world.exchanges() {
        builder.exchange(
            Draft {
                exchange: exchange.exchange(),
                agent: &exchange.agent().name,
                at: exchange.at(),
                fidelity: exchange.fidelity(),
                source: exchange.source(),
                turn: None,
            },
            |hash| exchange.message(hash),
        )?;
    }
    let key = ids::world(world.key())?;
    let mut agents = Vec::with_capacity(world.agents().len());
    for agent in world.agents() {
        if agent.key.world != *world.key() {
            return Err(GoldenError::ForeignAgent {
                agent: agent.key.to_string(),
            });
        }
        agents.push(AgentDecl {
            key: ids::agent(&agent.key.name)?,
            driven: match agent.driven {
                corpus::Driven::Model => Driven::Model,
                corpus::Driven::Scripted => Driven::Scripted,
            },
            model: (!agent.model.is_empty()).then(|| agent.model.clone()),
        });
    }
    let decl = WorldDecl {
        key: key.clone(),
        agents,
    };
    let mut labels = super::labels::exchange_agents(builder.owners())?;
    let truth = super::labels::truth(world.key(), world.truth(), builder.index())?;
    labels.extend(truth.labels);
    Ok(builder.finish(
        key,
        decl,
        labels,
        truth.notes,
        super::kinds::coverage(world.coverage()),
    ))
}

fn fidelity(fidelity: corpus::Fidelity) -> Fidelity {
    match fidelity {
        corpus::Fidelity::Exact => Fidelity::Exact,
        corpus::Fidelity::Reconstructed => Fidelity::Reconstructed,
        corpus::Fidelity::Synthetic => Fidelity::Synthetic,
    }
}

/// What a proxy observes: the credential's digest (`k:<hex>`),
/// the harness session, the request's ordinal in it, the vendor and model.
fn client(
    context: &ClientContext,
    model: &str,
    turn: Option<u32>,
    exchange: ExchangeId,
) -> Result<Client, GoldenError> {
    let credential = context
        .credential
        .as_ref()
        .ok_or(GoldenError::Unexpressible(Gap::NoCredential { exchange }))?;
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

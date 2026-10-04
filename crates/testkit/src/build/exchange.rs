//! Exchanges, their client context, and normalized exchanges.
//!
//! The default exchange is a completed streamed Anthropic Messages turn
//! from Claude Code through the reverse proxy, with a stable API key, the
//! shape of the corpus's `text_turn_streaming` case.

use std::time::Duration;

use crosstalk_spec::ids::{ExchangeId, MessageHash};
use crosstalk_spec::interfaces::l1_canonical::{NormalizeWarning, NormalizedExchange};
use crosstalk_spec::observed::client::{
    ClientContext, CredentialRef, CredentialScheme, HarnessClaim, HarnessFamily, HarnessIds,
    IngressMode, RequestClass, RouteName, Upstream, UpstreamId, UpstreamKind, Vendor,
};
use crosstalk_spec::observed::exchange::{
    ConnectionId, Continuation, Exchange, ExchangeFailure, ExchangeMeta, ExchangeOutcome,
    ModelName, ResponseId, StopReason, TokenUsage, Transport, WireProtocol,
};
use crosstalk_spec::observed::message::{Message, MessageBody};
use crosstalk_spec::support::Timestamp;

use crate::build::message::{self, content_hash};
use crate::ids::Ids;
use crate::time::{T0, after};

/// The Claude Code version the corpus and the default client claim.
pub const CLAUDE_CODE_VERSION: &str = "2.1.282";

/// The User-Agent Claude Code sends.
pub const CLAUDE_CODE_USER_AGENT: &str = "claude-cli/2.1.282 (external, cli)";

/// The model the default exchange and the corpus use.
pub const MODEL: &str = "claude-opus-5-5";

/// How long after `started_at` the default response's first chunk arrives.
pub const FIRST_CHUNK_AFTER: Duration = Duration::from_millis(400);

/// How long after `started_at` the default response finishes.
pub const FINISHED_AFTER: Duration = Duration::from_secs(3);

/// A Claude Code caller through the reverse proxy's `anthropic` route, to
/// the Anthropic API, with a fresh stable API key and session id.
pub fn claude_code_client(ids: &mut Ids) -> ClientContext {
    let session = ids.conversation().ulid_text().to_lowercase();
    ClientContext {
        ingress: IngressMode::ReverseProxy {
            route: RouteName("anthropic".to_owned()),
        },
        upstream: anthropic_api(),
        credential: Some(CredentialRef {
            scheme: CredentialScheme::ApiKey,
            hash: ids.credential(),
        }),
        account: None,
        previous_digests: None,
        harness: Some(HarnessClaim {
            family: HarnessFamily::ClaudeCode,
            version: Some(CLAUDE_CODE_VERSION.to_owned()),
            user_agent: CLAUDE_CODE_USER_AGENT.to_owned(),
        }),
        ids: HarnessIds {
            session: Some(session),
            agent: None,
            parent_agent: None,
        },
        class: RequestClass::Main,
    }
}

/// The configured upstream `anthropic`: the Anthropic API.
pub fn anthropic_api() -> Upstream {
    Upstream {
        id: UpstreamId("anthropic".to_owned()),
        kind: UpstreamKind::VendorApi(Vendor::Anthropic),
    }
}

/// How the built exchange ended.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Outcome {
    Completed {
        response: MessageHash,
        response_id: Option<ResponseId>,
        stop: StopReason,
        usage: Option<TokenUsage>,
    },
    Failed {
        partial: Option<MessageHash>,
        failure: ExchangeFailure,
    },
}

/// Builds an [`Exchange`]. Timestamps follow `started_at`: the first chunk
/// [`FIRST_CHUNK_AFTER`] it and the end [`FINISHED_AFTER`] it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExchangeBuilder {
    id: ExchangeId,
    protocol: WireProtocol,
    transport: Transport,
    model: ModelName,
    client: ClientContext,
    started_at: Timestamp,
    continuation: Continuation,
    request: Vec<MessageHash>,
    outcome: Outcome,
}

impl ExchangeBuilder {
    pub fn new(ids: &mut Ids) -> Self {
        let id = ids.exchange();
        let client = claude_code_client(ids);
        let request = vec![ids.message()];
        let response = ids.message();
        Self {
            id,
            protocol: WireProtocol::AnthropicMessages,
            transport: Transport::Sse,
            model: ModelName(MODEL.to_owned()),
            client,
            started_at: T0,
            continuation: Continuation::FullHistory,
            request,
            outcome: Outcome::Completed {
                response,
                response_id: Some(ResponseId(format!("msg_{}", id.ulid_text()))),
                stop: StopReason::EndTurn,
                usage: Some(TokenUsage {
                    input: 1200,
                    output: 48,
                    cache_read: 0,
                    reasoning: None,
                }),
            },
        }
    }

    pub fn id(&self) -> ExchangeId {
        self.id
    }

    pub fn with_id(mut self, id: ExchangeId) -> Self {
        self.id = id;
        self
    }

    pub fn protocol(mut self, protocol: WireProtocol) -> Self {
        self.protocol = protocol;
        self
    }

    pub fn transport(mut self, transport: Transport) -> Self {
        self.transport = transport;
        self
    }

    pub fn model(mut self, model: &str) -> Self {
        self.model = ModelName(model.to_owned());
        self
    }

    pub fn client(mut self, client: ClientContext) -> Self {
        self.client = client;
        self
    }

    /// Replace the caller's credential (`None`: an unauthenticated upstream).
    pub fn credential(mut self, credential: Option<CredentialRef>) -> Self {
        self.client.credential = credential;
        self
    }

    /// The harness session id the caller sends.
    pub fn session(mut self, session: &str) -> Self {
        self.client.ids.session = Some(session.to_owned());
        self
    }

    /// Mark the caller a harness sub-agent: its agent id and, for a nested
    /// agent, its parent's. Sets the request class to `Subagent`.
    pub fn subagent(mut self, agent: &str, parent: Option<&str>) -> Self {
        self.client.ids.agent = Some(agent.to_owned());
        self.client.ids.parent_agent = parent.map(str::to_owned);
        self.client.class = RequestClass::Subagent;
        self
    }

    pub fn class(mut self, class: RequestClass) -> Self {
        self.client.class = class;
        self
    }

    pub fn started_at(mut self, at: Timestamp) -> Self {
        self.started_at = at;
        self
    }

    /// The request's messages, in order.
    pub fn request(mut self, messages: Vec<MessageHash>) -> Self {
        self.request = messages;
        self
    }

    /// An increment continuing `previous`, as on a WebSocket turn.
    pub fn increment(mut self, previous: &str, connection: Option<ConnectionId>) -> Self {
        self.continuation = Continuation::Increment {
            previous: ResponseId(previous.to_owned()),
            connection,
        };
        self
    }

    /// Completed with `response`, keeping the stop reason and usage set so
    /// far (end turn and default usage unless overridden).
    pub fn response(mut self, response: MessageHash) -> Self {
        self.outcome = match self.outcome {
            Outcome::Completed {
                response_id,
                stop,
                usage,
                ..
            } => Outcome::Completed {
                response,
                response_id,
                stop,
                usage,
            },
            Outcome::Failed { .. } => Outcome::Completed {
                response,
                response_id: Some(ResponseId(format!("msg_{}", self.id.ulid_text()))),
                stop: StopReason::EndTurn,
                usage: None,
            },
        };
        self
    }

    /// The stop reason of a completed exchange. No effect on a failed one.
    pub fn stop(mut self, reason: StopReason) -> Self {
        if let Outcome::Completed { stop, .. } = &mut self.outcome {
            *stop = reason;
        }
        self
    }

    /// The usage of a completed exchange. No effect on a failed one.
    pub fn usage(mut self, tokens: Option<TokenUsage>) -> Self {
        if let Outcome::Completed { usage, .. } = &mut self.outcome {
            *usage = tokens;
        }
        self
    }

    /// Failed with `failure`, with no partial response.
    pub fn failed(mut self, failure: ExchangeFailure) -> Self {
        self.outcome = Outcome::Failed {
            partial: None,
            failure,
        };
        self
    }

    /// Failed with `failure` after `partial` had arrived.
    pub fn failed_after(mut self, partial: MessageHash, failure: ExchangeFailure) -> Self {
        self.outcome = Outcome::Failed {
            partial: Some(partial),
            failure,
        };
        self
    }

    pub fn build(self) -> Exchange {
        let first_chunk_at = after(self.started_at, FIRST_CHUNK_AFTER);
        let ended_at = after(self.started_at, FINISHED_AFTER);
        let outcome = match self.outcome {
            Outcome::Completed {
                response,
                response_id,
                stop,
                usage,
            } => ExchangeOutcome::Completed {
                response,
                response_id,
                first_chunk_at,
                finished_at: ended_at,
                stop,
                usage,
            },
            Outcome::Failed { partial, failure } => ExchangeOutcome::Failed {
                first_chunk_at: partial.map(|_| first_chunk_at),
                partial_response: partial,
                failed_at: ended_at,
                failure,
            },
        };
        Exchange {
            meta: ExchangeMeta {
                id: self.id,
                protocol: self.protocol,
                transport: self.transport,
                model: self.model,
                client: self.client,
                started_at: self.started_at,
            },
            continuation: self.continuation,
            request: self.request,
            outcome,
        }
    }
}

/// How the built normalized exchange ended.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Ending {
    Response(MessageBody),
    Failed {
        partial: Option<MessageBody>,
        failure: ExchangeFailure,
    },
}

/// Builds a [`NormalizedExchange`] from message bodies. Every hash in the
/// exchange is the [`content_hash`] of one of its messages, the invariant
/// `NormalizedExchange` documents, by construction.
///
/// The default request is a system prompt and one user turn, answered by
/// one assistant text message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NormalizedExchangeBuilder {
    exchange: ExchangeBuilder,
    request: Vec<MessageBody>,
    ending: Ending,
    warnings: Vec<NormalizeWarning>,
}

impl NormalizedExchangeBuilder {
    pub fn new(ids: &mut Ids) -> Self {
        Self {
            exchange: ExchangeBuilder::new(ids),
            request: vec![
                message::system_text("You are Claude Code, Anthropic's official CLI for Claude."),
                message::user_text("Summarize the plan in notes/plan.md."),
            ],
            ending: Ending::Response(message::assistant_text(
                "The plan has three steps: parse, index, report.",
            )),
            warnings: Vec::new(),
        }
    }

    pub fn id(&self) -> ExchangeId {
        self.exchange.id()
    }

    /// Adjust the exchange's own fields (time, client, transport, stop
    /// reason). Its request and outcome are replaced from the messages at
    /// build time.
    pub fn exchange(mut self, adjust: impl FnOnce(ExchangeBuilder) -> ExchangeBuilder) -> Self {
        self.exchange = adjust(self.exchange);
        self
    }

    /// Replace the request messages.
    pub fn request(mut self, messages: Vec<MessageBody>) -> Self {
        self.request = messages;
        self
    }

    /// Append one request message.
    pub fn then(mut self, message: MessageBody) -> Self {
        self.request.push(message);
        self
    }

    pub fn response(mut self, response: MessageBody) -> Self {
        self.ending = Ending::Response(response);
        self
    }

    /// Failed with `failure`, after `partial` if any arrived.
    pub fn failed(mut self, partial: Option<MessageBody>, failure: ExchangeFailure) -> Self {
        self.ending = Ending::Failed { partial, failure };
        self
    }

    pub fn warning(mut self, warning: NormalizeWarning) -> Self {
        self.warnings.push(warning);
        self
    }

    pub fn build(self) -> NormalizedExchange {
        let mut messages: Vec<Message> = Vec::new();
        let request: Vec<MessageHash> = self
            .request
            .into_iter()
            .map(|body| keep(&mut messages, body))
            .collect();
        let exchange = self.exchange.request(request);
        let exchange = match self.ending {
            Ending::Response(body) => exchange.response(keep(&mut messages, body)),
            Ending::Failed {
                partial: Some(body),
                failure,
            } => exchange.failed_after(keep(&mut messages, body), failure),
            Ending::Failed {
                partial: None,
                failure,
            } => exchange.failed(failure),
        };
        NormalizedExchange {
            exchange: exchange.build(),
            messages,
            warnings: self.warnings,
        }
    }
}

/// Add `body` to `messages` unless an equal one is there; its hash.
fn keep(messages: &mut Vec<Message>, body: MessageBody) -> MessageHash {
    let hash = content_hash(&body);
    if messages.iter().all(|kept| kept.hash != hash) {
        messages.push(Message { hash, body });
    }
    hash
}

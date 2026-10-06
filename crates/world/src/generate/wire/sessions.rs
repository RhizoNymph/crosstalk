//! One agent's needed exchanges as harness sessions: full-history
//! requests that grow turn by turn, a new session after a long pause, a
//! compacted continuation when a session gets long, now and then a fork
//! (the operator rewinds to an earlier reply) and a failed attempt retried.
//!
//! ```text
//! request = [system] ++ history ++ new inputs        (FullHistory)
//! completed: history ++= new inputs ++ [response]
//! pause > SESSION_GAP            → new session: history = []
//! turns ≥ MAX_TURNS              → compaction: history = [summary] ++ last 2 messages
//! chance FORK, ≥ 3 replies       → fork: history cut after an earlier reply
//! ```

use crosstalk_spec::derived::provenance::matching::Carrier;
use crosstalk_spec::ids::{AgentId, ExchangeId, MessageHash};
use crosstalk_spec::observed::client::ClientContext;
use crosstalk_spec::observed::exchange::{
    Continuation, Exchange, ExchangeFailure, ExchangeMeta, ExchangeOutcome, ModelName, StopReason,
    TokenCounts, TokenUsage, Transport, WireProtocol,
};
use crosstalk_spec::support::Timestamp;

use crate::clock::{MINUTE, SECOND, minus, plus};
use crate::error::WorldError;
use crate::mint::Mint;
use crate::rng::Rng;
use crate::text::Theme;

use super::WireExchange;
use super::messages::{self, Bodies};
use super::needs::{Event, Need};

/// A pause longer than this starts a new session.
pub const SESSION_GAP: u64 = 90 * MINUTE;
/// A session this many completed turns long is compacted.
pub const MAX_TURNS: u32 = 12;
/// The chance a turn forks from an earlier reply.
const FORK: f64 = 0.05;
/// The chance an exchange first fails and is retried.
const FAILURE: f64 = 0.03;

/// How an agent's harness calls: everything but the per-exchange fields.
#[derive(Debug, Clone)]
pub struct Caller {
    pub agent: AgentId,
    pub key: &'static str,
    pub harness: &'static str,
    pub protocol: WireProtocol,
    pub transport: Transport,
    pub model: ModelName,
    pub client: ClientContext,
}

#[derive(Debug, Clone)]
struct Session {
    system: MessageHash,
    history: Vec<MessageHash>,
    /// The history's length after each completed turn's response.
    replies: Vec<usize>,
    turns: u32,
    last: Timestamp,
}

/// Builds one agent's exchanges.
pub struct Sessions<'a> {
    caller: &'a Caller,
    rng: &'a mut Rng,
    mint: &'a mut Mint,
    bodies: &'a mut Bodies,
    session: Option<Session>,
    /// Sessions started so far.
    count: u32,
    /// Turns completed so far, across sessions.
    steps: u32,
    out: Vec<WireExchange>,
}

impl<'a> Sessions<'a> {
    pub fn new(
        caller: &'a Caller,
        rng: &'a mut Rng,
        mint: &'a mut Mint,
        bodies: &'a mut Bodies,
    ) -> Self {
        Self {
            caller,
            rng,
            mint,
            bodies,
            session: None,
            count: 0,
            steps: 0,
            out: Vec::new(),
        }
    }

    /// The exchanges, in the order the agent sent them (starts never
    /// decrease).
    pub fn finish(self) -> Vec<WireExchange> {
        self.out
    }

    /// Add the exchanges `event` needs, after whatever opens or reshapes
    /// the session first.
    pub fn add(&mut self, event: &Event) -> Result<(), WorldError> {
        self.shape(event)?;
        match &event.need {
            Need::Send { output } => {
                let prompt = self.prompt(event.theme);
                if self.rng.chance(FAILURE) {
                    let at = self.before(event.at, 2 * SECOND);
                    let id = self.mint.at(at)?;
                    self.failed(id, at, vec![prompt]);
                }
                self.completed(
                    event.exchange,
                    event.at,
                    vec![prompt],
                    *output,
                    StopReason::EndTurn,
                )
            }
            Need::Read {
                reads,
                carrier,
                tool,
            } => match carrier {
                Carrier::ToolResult(call) => {
                    let at = self.before(event.at, 3 * SECOND);
                    let id = self.mint.at(at)?;
                    let prompt = self.prompt(event.theme);
                    let calling = messages::tool_call(call, tool, self.steps + 1);
                    let calling = self.bodies.keep(&calling);
                    self.completed(id, at, vec![prompt], calling, StopReason::ToolUse)?;
                    let reply = self.reply();
                    self.completed(
                        event.exchange,
                        event.at,
                        reads.clone(),
                        reply,
                        StopReason::EndTurn,
                    )
                }
                Carrier::UserTurn => {
                    let reply = self.reply();
                    self.completed(
                        event.exchange,
                        event.at,
                        reads.clone(),
                        reply,
                        StopReason::EndTurn,
                    )
                }
                Carrier::SystemPrompt => {
                    if let (Some(session), Some(system)) = (self.session.as_mut(), reads.first()) {
                        session.system = *system;
                    }
                    let prompt = self.prompt(event.theme);
                    let reply = self.reply();
                    self.completed(
                        event.exchange,
                        event.at,
                        vec![prompt],
                        reply,
                        StopReason::EndTurn,
                    )
                }
                Carrier::ReaderOutput => {
                    let prompt = self.prompt(event.theme);
                    let Some(output) = reads.first().copied() else {
                        return Err(WorldError::missing("the reader's output"));
                    };
                    self.completed(
                        event.exchange,
                        event.at,
                        vec![prompt],
                        output,
                        StopReason::EndTurn,
                    )
                }
            },
        }
    }

    /// Open a session, compact it or fork it, as `event` finds it.
    fn shape(&mut self, event: &Event) -> Result<(), WorldError> {
        let paused = self
            .session
            .as_ref()
            .is_none_or(|s| event.at.as_micros().saturating_sub(s.last.as_micros()) > SESSION_GAP);
        if paused {
            self.count += 1;
            let system = messages::system(self.caller.harness, self.caller.key);
            self.session = Some(Session {
                system: self.bodies.keep(&system),
                history: Vec::new(),
                replies: Vec::new(),
                turns: 0,
                last: event.at,
            });
            return Ok(());
        }
        let summary = messages::summary(event.theme, self.steps, self.count);
        let fork = self.rng.chance(FORK);
        let pick = self.rng.next_u64();
        let Some(session) = self.session.as_mut() else {
            return Ok(());
        };
        if session.turns >= MAX_TURNS {
            self.count += 1;
            let carried: Vec<MessageHash> = session
                .history
                .iter()
                .rev()
                .take(2)
                .rev()
                .copied()
                .collect();
            let mut history = vec![self.bodies.keep(&summary)];
            history.extend(carried);
            session.history = history;
            session.replies.clear();
            session.turns = 0;
        } else if fork && session.replies.len() >= 3 {
            // An earlier reply, never the latest: the turn rewinds.
            let earlier = session.replies.len() - 1;
            let index = usize::try_from(pick % earlier as u64).unwrap_or(0);
            if let Some(cut) = session.replies.get(index).copied() {
                session.history.truncate(cut);
                session.replies.truncate(index + 1);
            }
        }
        Ok(())
    }

    fn prompt(&mut self, theme: Theme) -> MessageHash {
        let body = messages::prompt(self.rng, theme, self.steps + 1);
        self.bodies.keep(&body)
    }

    fn reply(&mut self) -> MessageHash {
        let body = messages::reply(self.rng, self.steps + 1);
        self.bodies.keep(&body)
    }

    /// `step` before `at`, but never before the session's last exchange
    /// nor after `at`: starts never decrease.
    fn before(&self, at: Timestamp, step: u64) -> Timestamp {
        let last = self.session.as_ref().map_or(at, |s| s.last);
        minus(at, step).max(last).min(at)
    }

    fn request(&self, inputs: &[MessageHash]) -> Vec<MessageHash> {
        let Some(session) = &self.session else {
            return inputs.to_vec();
        };
        let mut request = Vec::with_capacity(session.history.len() + inputs.len() + 1);
        request.push(session.system);
        request.extend(&session.history);
        request.extend(inputs);
        request
    }

    fn meta(&self, id: ExchangeId, at: Timestamp) -> ExchangeMeta {
        ExchangeMeta {
            id,
            protocol: self.caller.protocol,
            transport: self.caller.transport,
            model: self.caller.model.clone(),
            client: self.caller.client.clone(),
            started_at: at,
        }
    }

    fn usage(&mut self, request: usize) -> Result<TokenUsage, WorldError> {
        let input = 300
            * u32::try_from(request)
                .unwrap_or(u32::MAX / 300)
                .min(u32::MAX / 300);
        TokenUsage::new(TokenCounts {
            input,
            output: 80 + u32::try_from(self.rng.below(400)).unwrap_or(0),
            cache_read: input / 2,
            cache_write: Some(input / 4),
            reasoning: None,
        })
        .map_err(|e| WorldError::invalid("TokenUsage", e))
    }

    fn completed(
        &mut self,
        id: ExchangeId,
        at: Timestamp,
        inputs: Vec<MessageHash>,
        response: MessageHash,
        stop: StopReason,
    ) -> Result<(), WorldError> {
        let request = self.request(&inputs);
        let usage = self.usage(request.len())?;
        let first_chunk_at = plus(at, 400_000 + self.rng.below(SECOND));
        let finished_at = plus(first_chunk_at, SECOND + self.rng.below(4 * SECOND));
        let exchange = Exchange {
            meta: self.meta(id, at),
            continuation: Continuation::FullHistory,
            request,
            outcome: ExchangeOutcome::Completed {
                response,
                response_id: None,
                first_chunk_at,
                finished_at,
                stop,
                usage: Some(usage),
            },
        };
        self.out.push(WireExchange {
            agent: self.caller.agent,
            exchange,
        });
        self.steps += 1;
        if let Some(session) = self.session.as_mut() {
            session.history.extend(inputs);
            session.history.push(response);
            session.replies.push(session.history.len());
            session.turns += 1;
            session.last = at;
        }
        Ok(())
    }

    /// An attempt the upstream refused: the same request is sent again.
    fn failed(&mut self, id: ExchangeId, at: Timestamp, inputs: Vec<MessageHash>) {
        let request = self.request(&inputs);
        let exchange = Exchange {
            meta: self.meta(id, at),
            continuation: Continuation::FullHistory,
            request,
            outcome: ExchangeOutcome::Failed {
                partial_response: None,
                first_chunk_at: None,
                failed_at: plus(at, 300_000),
                failure: ExchangeFailure::Upstream { status: 529 },
            },
        };
        self.out.push(WireExchange {
            agent: self.caller.agent,
            exchange,
        });
        if let Some(session) = self.session.as_mut() {
            session.last = at;
        }
    }
}

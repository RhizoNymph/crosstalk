//! Threads the generated traffic into conversations ([`build`]).

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use crosstalk_spec::derived::flow::transmission::{DelegationDirection, Route};
use crosstalk_spec::derived::provenance::matching::Carrier;
use crosstalk_spec::derived::provenance::span::{RelaySource, SpanLocation};
use crosstalk_spec::ids::{AgentId, ConversationId, ExchangeId, MessageHash, SpanId};
use crosstalk_spec::observed::client::{CorpusId, HarnessClaim, IngressMode, RouteName};
use crosstalk_spec::observed::conversation::ConversationOrigin;
use crosstalk_spec::observed::exchange::{
    ConnectionId, Continuation, ExchangeFailure, ModelName, ResponseId, StopReason, TokenCounts,
    TokenUsage, Transport, WireProtocol,
};
use crosstalk_spec::observed::message::{Message, PartRef, Role, ToolCallId};
use crosstalk_spec::support::{ByteRange, Timestamp};

use crate::backend::fixture::clock::{HOUR, MINUTE, Mint, NOW, SECOND, minus, plus};
use crate::backend::fixture::rng::Rng;

use super::super::agents::Family;
use super::super::{GenError, World, states};
use super::messages;
use super::{
    Cases, ConversationRecord, Conversations, Ending, Entry, RelayedSpan, SpanRecord, TurnRecord,
};

/// A gap longer than this starts a new conversation.
const SESSION_GAP: u64 = 3 * HOUR;
/// A conversation holds at most this many turns from the traffic.
const MAX_TURNS: usize = 24;
/// The longest quote a reply makes of what the agent read.
const QUOTE_MAX: usize = 160;
/// The corpus the replayed agent's conversations came from.
const CORPUS: &str = "agentdojo-workspace";

/// One message a reader's copy arrived in.
struct Read {
    message: MessageHash,
    carrier: Carrier,
    origin: SpanId,
    /// The matched text as it arrived, when the body is still held.
    quote: Option<String>,
}

/// What one exchange of the traffic is.
enum Kind {
    /// The reader's copies arrived in its request (or, for `ReaderOutput`,
    /// were found in its output).
    Reads {
        reads: Vec<Read>,
        /// A parent's task: starts the child's conversation.
        delegated: bool,
    },
    /// Its output holds an originated span.
    Writes {
        span: SpanId,
        location: SpanLocation,
    },
}

struct Pending {
    exchange: ExchangeId,
    agent: AgentId,
    at: Timestamp,
    kind: Kind,
}

/// Builds every conversation of `world`.
pub fn build(world: &World) -> Result<Conversations, GenError> {
    let mut rng = Rng::fork(world.seed, "conversations");
    let mut mint = Mint::new(world.seed ^ 0x636f_6e76_6572_7361);
    let pending = exchanges(world, &mut rng, &mut mint);
    let mut builder = Builder {
        world,
        rng,
        mint,
        records: BTreeMap::new(),
        spans: HashMap::new(),
        relayed: HashMap::new(),
        bodies: HashMap::new(),
    };
    builder.thread(pending)?;
    let cases = builder.cases()?;
    builder.finish(cases)
}

/// Every exchange the traffic names, as reader or writer, oldest first.
fn exchanges(world: &World, rng: &mut Rng, mint: &mut Mint) -> Vec<Pending> {
    let mut reads: BTreeMap<ExchangeId, Pending> = BTreeMap::new();
    let mut writes: Vec<Pending> = Vec::new();
    let mut first_read: BTreeMap<SpanId, (Timestamp, AgentId, SpanLocation)> = BTreeMap::new();
    for record in &world.transmissions {
        let Some(confirmed) = states::confirmed(&record.transmission.state) else {
            continue;
        };
        let at = record.transmission.opened_at;
        let delegated = matches!(
            record.transmission.route,
            Route::Delegation(DelegationDirection::ParentToChild)
        );
        for content in confirmed.content().iter() {
            let read_at = content.read_at();
            let quote = world.blobs.body(read_at.part.message).and_then(|body| {
                let text = body.part_text(read_at.part.index).ok()?;
                let start = usize::try_from(read_at.range.start()).ok()?;
                let end = usize::try_from(read_at.range.end()).ok()?;
                let cut = text.get(start..end)?;
                Some(clip(cut, QUOTE_MAX).to_owned())
            });
            let entry = reads
                .entry(content.reader_exchange())
                .or_insert_with(|| Pending {
                    exchange: content.reader_exchange(),
                    agent: content.reader(),
                    at,
                    kind: Kind::Reads {
                        reads: Vec::new(),
                        delegated,
                    },
                });
            if let Kind::Reads { reads, .. } = &mut entry.kind {
                reads.push(Read {
                    message: read_at.part.message,
                    carrier: content.carrier().clone(),
                    origin: content.origin(),
                    quote,
                });
            }
            if let Some(location) = world.blobs.span(content.origin()) {
                let slot = first_read.entry(content.origin()).or_insert((
                    at,
                    content.origin_agent(),
                    location,
                ));
                if at < slot.0 {
                    slot.0 = at;
                }
            }
        }
    }
    for (span, (read, author, location)) in first_read {
        let at = minus(read, rng.between(5, 90) * MINUTE);
        writes.push(Pending {
            exchange: ExchangeId::from_ulid(mint.ulid(at)),
            agent: author,
            at,
            kind: Kind::Writes { span, location },
        });
    }
    let mut all: Vec<Pending> = reads.into_values().chain(writes).collect();
    all.sort_by_key(|p| (p.agent, p.at, p.exchange));
    all
}

/// Cuts `text` to at most `max` bytes at a character boundary.
fn clip(text: &str, max: usize) -> &str {
    let mut end = text.len().min(max);
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    &text[..end]
}

struct Builder<'w> {
    world: &'w World,
    rng: Rng,
    mint: Mint,
    records: BTreeMap<ConversationId, ConversationRecord>,
    spans: HashMap<SpanId, SpanRecord>,
    relayed: HashMap<ExchangeId, Vec<RelayedSpan>>,
    bodies: HashMap<MessageHash, Message>,
}

/// How an agent's harness talks to its upstream.
struct Wire {
    family: &'static str,
    protocol: WireProtocol,
    transport: Transport,
    model: &'static str,
    route: &'static str,
}

fn wire(family: Option<Family>) -> Wire {
    match family {
        Some(Family::Codex) => Wire {
            family: "Codex",
            protocol: WireProtocol::OpenAiResponses,
            transport: Transport::WebSocket,
            model: "gpt-5-codex",
            route: "openai",
        },
        Some(Family::Pi) => Wire {
            family: "pi",
            protocol: WireProtocol::OpenAiChat,
            transport: Transport::Sse,
            model: "qwen3-coder",
            route: "openrouter",
        },
        Some(Family::OhMyPi) => Wire {
            family: "oh-my-pi",
            protocol: WireProtocol::AnthropicMessages,
            transport: Transport::Sse,
            model: "claude-opus-4-1",
            route: "anthropic",
        },
        Some(Family::SelfHosted) => Wire {
            family: "self-hosted",
            protocol: WireProtocol::OpenAiChat,
            transport: Transport::Http,
            model: "llama-3.3-70b",
            route: "local",
        },
        Some(Family::ClaudeCode) | None => Wire {
            family: "Claude Code",
            protocol: WireProtocol::AnthropicMessages,
            transport: Transport::Sse,
            model: "claude-sonnet-4-5",
            route: "anthropic",
        },
    }
}

impl Builder<'_> {
    fn store(&mut self, message: Message) -> MessageHash {
        let hash = message.hash;
        self.bodies.insert(hash, message);
        hash
    }

    fn claim(&self, agent: AgentId, turn: usize) -> Option<HarnessClaim> {
        let entries = self.world.claims.get(&agent)?.entries();
        if entries.is_empty() {
            return None;
        }
        entries
            .get(turn % entries.len())
            .map(|seen| seen.claim.clone())
    }

    fn usage(&mut self) -> Result<Option<TokenUsage>, GenError> {
        let input = u32::try_from(self.rng.between(2_000, 60_000)).unwrap_or(2_000);
        let cache_read = input / 2;
        let output = u32::try_from(self.rng.between(80, 1_500)).unwrap_or(80);
        TokenUsage::new(TokenCounts {
            input,
            output,
            cache_read,
            cache_write: Some(input / 10),
            reasoning: None,
        })
        .map(Some)
        .map_err(|e| GenError::invalid("TokenUsage", e))
    }

    /// Groups each agent's exchanges into conversations and builds their
    /// turns.
    fn thread(&mut self, pending: Vec<Pending>) -> Result<(), GenError> {
        let mut groups: Vec<Vec<Pending>> = Vec::new();
        for item in pending {
            let starts = match groups.last().and_then(|g| g.last()) {
                None => true,
                Some(last) => {
                    last.agent != item.agent
                        || item.at.as_micros().saturating_sub(last.at.as_micros()) > SESSION_GAP
                        || groups.last().is_some_and(|g| g.len() >= MAX_TURNS)
                        || matches!(
                            item.kind,
                            Kind::Reads {
                                delegated: true,
                                ..
                            }
                        )
                }
            };
            if starts {
                groups.push(Vec::new());
            }
            if let Some(group) = groups.last_mut() {
                group.push(item);
            }
        }
        for group in groups {
            self.conversation(group)?;
        }
        Ok(())
    }

    fn conversation(&mut self, group: Vec<Pending>) -> Result<(), GenError> {
        let Some(first) = group.first() else {
            return Ok(());
        };
        let agent = first.agent;
        let opened = first.at;
        let id = ConversationId::from_ulid(self.mint.ulid(opened));
        let family = self.world.scenario.cast.families.get(&agent).copied();
        let wire = wire(family);
        let connection =
            (wire.transport == Transport::WebSocket).then(|| ConnectionId(self.mint.ulid(opened)));
        // The tool call each turn's reply makes: the call whose result the
        // next turn's request carries.
        let calls: Vec<Option<ToolCallId>> = (0..group.len())
            .map(|i| {
                group.get(i + 1).and_then(|next| match &next.kind {
                    Kind::Reads { reads, .. } => {
                        reads.iter().find_map(|read| match &read.carrier {
                            Carrier::ToolResult(call) => Some(call.clone()),
                            _ => None,
                        })
                    }
                    Kind::Writes { .. } => None,
                })
            })
            .collect();
        let mut turns = Vec::with_capacity(group.len());
        for (index, item) in group.into_iter().enumerate() {
            let mut inputs = Vec::new();
            if index == 0 {
                let system = self.store(messages::system_prompt(wire.family, id));
                let task = messages::user_task(&mut self.rng, id);
                let task = self.store(task);
                inputs.push(entry(system, Role::System));
                inputs.push(entry(task, Role::User));
            }
            let output = match item.kind {
                Kind::Reads { reads, .. } => {
                    let mut output = None;
                    let mut quote = None;
                    for read in &reads {
                        match &read.carrier {
                            // The generator stores each copy found in a
                            // reader's output as its own message, but an
                            // exchange has one output: the first is it, and
                            // further ones are listed among the inputs as
                            // assistant messages.
                            Carrier::ReaderOutput if output.is_none() => {
                                output = Some(read.message);
                            }
                            carrier => {
                                inputs.push(entry(read.message, role_of(carrier)));
                                if quote.is_none() {
                                    quote = read.quote.clone().map(|q| (read.origin, q));
                                }
                            }
                        }
                    }
                    match output {
                        Some(found) => found,
                        None => {
                            let quoted = quote.filter(|_| self.rng.chance(0.4));
                            let call = calls.get(index).cloned().flatten();
                            let reply = messages::reply(
                                &mut self.rng,
                                item.exchange,
                                quoted.as_ref().map(|(_, q)| q.as_str()),
                                call.as_ref(),
                            );
                            let hash = self.store(reply.message);
                            if let (Some((origin, _)), Some((start, end))) = (quoted, reply.quote) {
                                let range = ByteRange::new(start, end)
                                    .map_err(|e| GenError::invalid("ByteRange", e))?;
                                let span = SpanId::from_ulid(self.mint.ulid(item.at));
                                self.relayed
                                    .entry(item.exchange)
                                    .or_default()
                                    .push(RelayedSpan {
                                        span,
                                        location: SpanLocation {
                                            part: PartRef {
                                                message: hash,
                                                index: 0,
                                            },
                                            range,
                                        },
                                        source: RelaySource::Span(origin),
                                    });
                            }
                            hash
                        }
                    }
                }
                Kind::Writes { span, location } => {
                    let nudge = messages::user_task(&mut self.rng, id);
                    let nudge = self.store(rename(nudge, item.exchange));
                    inputs.push(entry(nudge, Role::User));
                    self.spans.insert(
                        span,
                        SpanRecord {
                            exchange: item.exchange,
                            author: item.agent,
                            location,
                            indexed_at: plus(item.at, 30 * SECOND),
                        },
                    );
                    location.part.message
                }
            };
            let continuation = match connection {
                Some(connection) if index > 0 => Continuation::Increment {
                    previous: ResponseId(format!("resp_{}", item.exchange.ulid_text())),
                    connection: Some(connection),
                },
                _ => Continuation::FullHistory,
            };
            let stop = if calls.get(index).is_some_and(Option::is_some) {
                StopReason::ToolUse
            } else {
                StopReason::EndTurn
            };
            let finished_at = plus(item.at, self.rng.between(2, 40) * SECOND);
            turns.push(TurnRecord {
                exchange: item.exchange,
                agent: item.agent,
                started_at: item.at,
                protocol: wire.protocol,
                transport: wire.transport,
                model: ModelName(wire.model.to_owned()),
                harness: self.claim(item.agent, index),
                continuation,
                ending: Ending::Completed {
                    finished_at: finished_at.min(NOW),
                    stop,
                    usage: self.usage()?,
                },
                inputs,
                output: Some(output),
            });
        }
        self.records.insert(
            id,
            ConversationRecord {
                id,
                agent,
                origin: ConversationOrigin::Root,
                ingress: IngressMode::ReverseProxy {
                    route: RouteName(wire.route.to_owned()),
                },
                turns,
            },
        );
        Ok(())
    }

    /// Picks a conversation for each named case and makes it.
    fn cases(&mut self) -> Result<Cases, GenError> {
        let mut used: HashSet<ConversationId> = HashSet::new();
        let cast = &self.world.scenario.cast;
        let family_of = |agent: AgentId| cast.families.get(&agent).copied();
        let pick = |records: &BTreeMap<ConversationId, ConversationRecord>,
                    used: &HashSet<ConversationId>,
                    want: &dyn Fn(&ConversationRecord) -> bool|
         -> Result<ConversationId, GenError> {
            records
                .values()
                .rev()
                .find(|r| !used.contains(&r.id) && want(r))
                .map(|r| r.id)
                .ok_or_else(|| GenError::Missing("a conversation for a case".to_owned()))
        };
        let generated = |bodies: &HashMap<MessageHash, Message>, turn: &TurnRecord| {
            turn.output.is_some_and(|o| bodies.contains_key(&o))
        };

        let parent = pick(&self.records, &used, &|r| {
            r.turns.len() >= 4
                && family_of(r.agent) == Some(Family::ClaudeCode)
                && r.turns.iter().all(|t| generated(&self.bodies, t))
        })?;
        used.insert(parent);
        let fork = self.fork(parent)?;
        used.insert(fork);

        let old = pick(&self.records, &used, &|r| {
            r.turns.len() >= 8 && family_of(r.agent) == Some(Family::ClaudeCode)
        })?;
        used.insert(old);
        let compacted = self.compact(old)?;
        used.insert(compacted);

        let unseen = pick(&self.records, &used, &|r| {
            r.turns.len() >= 3 && family_of(r.agent) == Some(Family::Codex)
        })?;
        used.insert(unseen);
        self.unseen_increment(unseen)?;

        let mid = pick(&self.records, &used, &|r| r.turns.len() >= 3)?;
        used.insert(mid);
        self.system_turn(mid)?;

        let failed = pick(&self.records, &used, &|r| {
            r.turns.len() >= 2
                && r.turns.last().is_some_and(|t| generated(&self.bodies, t))
                && r.turns
                    .last()
                    .is_some_and(|t| !self.relayed.contains_key(&t.exchange))
        })?;
        used.insert(failed);
        let failed_turn = self.fail_last(failed)?;

        let replayed = self.replay(&used)?;

        let pending_scan = self
            .records
            .values()
            .filter(|r| !used.contains(&r.id))
            .flat_map(|r| r.turns.iter())
            .max_by_key(|t| (t.started_at, t.exchange))
            .map(|t| t.exchange)
            .ok_or_else(|| GenError::Missing("a turn to leave unscanned".to_owned()))?;

        Ok(Cases {
            fork: (fork, parent),
            compaction: (compacted, old),
            unseen_increment: unseen,
            mid_system: (mid, 1),
            failed: (failed, failed_turn),
            replay: replayed,
            pending_scan,
        })
    }

    fn record_mut(&mut self, id: ConversationId) -> Result<&mut ConversationRecord, GenError> {
        self.records
            .get_mut(&id)
            .ok_or_else(|| GenError::Missing(format!("conversation {}", id.ulid_text())))
    }

    /// A retry: a fork sharing the parent's first two turns, then two turns
    /// of its own.
    fn fork(&mut self, parent: ConversationId) -> Result<ConversationId, GenError> {
        let record = self.record_mut(parent)?.clone();
        let base = ConversationRecord {
            turns: record.turns[..2].to_vec(),
            ..record.clone()
        };
        let shared_prefix = u32::try_from(base.history().len())
            .map_err(|e| GenError::invalid("shared prefix", e))?;
        let branch_at = plus(record.turns[1].started_at, 2 * MINUTE);
        let id = ConversationId::from_ulid(self.mint.ulid(branch_at));
        let mut turns = Vec::new();
        for step in 0..2u64 {
            let at = plus(branch_at, step * 4 * MINUTE);
            let exchange = ExchangeId::from_ulid(self.mint.ulid(at));
            let retry = messages::user_task(&mut self.rng, id);
            let retry = self.store(rename(retry, exchange));
            let reply = messages::reply(&mut self.rng, exchange, None, None);
            let reply = self.store(reply.message);
            let mut turn = record.turns[1].clone();
            turn.exchange = exchange;
            turn.started_at = at;
            turn.continuation = Continuation::FullHistory;
            turn.ending = Ending::Completed {
                finished_at: plus(at, 12 * SECOND),
                stop: StopReason::EndTurn,
                usage: self.usage()?,
            };
            turn.inputs = vec![entry(retry, Role::User)];
            turn.output = Some(reply);
            turns.push(turn);
        }
        self.records.insert(
            id,
            ConversationRecord {
                id,
                agent: record.agent,
                origin: ConversationOrigin::Fork {
                    parent,
                    shared_prefix,
                },
                ingress: record.ingress.clone(),
                turns,
            },
        );
        Ok(id)
    }

    /// Splits a long conversation: its later turns continue in a compaction
    /// that opens with a summary and two carried-over messages.
    fn compact(&mut self, old: ConversationId) -> Result<ConversationId, GenError> {
        let record = self.record_mut(old)?;
        let keep = record.turns.len() / 2;
        let later = record.turns.split_off(keep);
        let history = record.history();
        let agent = record.agent;
        let ingress = record.ingress.clone();
        let family = self.world.scenario.cast.families.get(&agent).copied();
        let first_at = later
            .first()
            .map(|t| t.started_at)
            .ok_or_else(|| GenError::Missing("turns to compact".to_owned()))?;
        let id = ConversationId::from_ulid(self.mint.ulid(first_at));
        let system = self.store(messages::system_prompt(wire(family).family, id));
        let summary = self.store(messages::compaction_summary(old));
        let mut turns = later;
        if let Some(first) = turns.first_mut() {
            let mut inputs = vec![entry(system, Role::System), entry(summary, Role::User)];
            for carried in history.iter().rev().take(2).rev() {
                let role = self
                    .bodies
                    .get(carried)
                    .map(|m| m.body.role())
                    .unwrap_or(Role::Assistant);
                inputs.push(Entry {
                    message: *carried,
                    role,
                    carried_over: true,
                });
            }
            inputs.append(&mut first.inputs);
            first.inputs = inputs;
        }
        self.records.insert(
            id,
            ConversationRecord {
                id,
                agent,
                origin: ConversationOrigin::Compaction { predecessor: old },
                ingress,
                turns,
            },
        );
        Ok(id)
    }

    /// The conversation's first turn continues a response the gateway never
    /// saw: it holds only its increment.
    fn unseen_increment(&mut self, id: ConversationId) -> Result<(), GenError> {
        let connection = ConnectionId(self.mint.ulid(NOW));
        let record = self.record_mut(id)?;
        let first = record
            .turns
            .first_mut()
            .ok_or_else(|| GenError::Missing("a first turn".to_owned()))?;
        first.inputs.drain(..2);
        first.continuation = Continuation::Increment {
            previous: ResponseId("resp_never_seen".to_owned()),
            connection: Some(connection),
        };
        for turn in record.turns.iter_mut().skip(1) {
            if let Continuation::Increment { connection: c, .. } = &mut turn.continuation {
                *c = Some(connection);
            }
        }
        if first_inputs_empty(record) {
            return Err(GenError::Missing("an increment's inputs".to_owned()));
        }
        Ok(())
    }

    /// The harness inserts a system turn in the second request.
    fn system_turn(&mut self, id: ConversationId) -> Result<(), GenError> {
        let exchange = self
            .records
            .get(&id)
            .and_then(|r| r.turns.get(1))
            .map(|t| t.exchange)
            .ok_or_else(|| GenError::Missing("a second turn".to_owned()))?;
        let system = self.store(messages::system_turn(exchange));
        let record = self.record_mut(id)?;
        if let Some(turn) = record.turns.get_mut(1) {
            let at = turn.inputs.len().min(1);
            turn.inputs.insert(at, entry(system, Role::System));
        }
        Ok(())
    }

    /// The conversation's last exchange failed mid-stream.
    fn fail_last(&mut self, id: ConversationId) -> Result<u32, GenError> {
        let exchange = self
            .records
            .get(&id)
            .and_then(|r| r.turns.last())
            .map(|t| t.exchange)
            .ok_or_else(|| GenError::Missing("a last turn".to_owned()))?;
        let partial = self.store(messages::partial(exchange));
        let record = self.record_mut(id)?;
        let index = record.turns.len() - 1;
        if let Some(turn) = record.turns.last_mut() {
            turn.ending = Ending::Failed {
                failed_at: plus(turn.started_at, 9 * SECOND),
                failure: ExchangeFailure::StreamTruncated,
            };
            turn.output = Some(partial);
        }
        u32::try_from(index).map_err(|e| GenError::invalid("turn index", e))
    }

    /// Every conversation of one self-hosted agent came from a recorded
    /// corpus.
    fn replay(
        &mut self,
        used: &HashSet<ConversationId>,
    ) -> Result<(CorpusId, ConversationId), GenError> {
        let cast = &self.world.scenario.cast;
        let agent = self
            .records
            .values()
            .filter(|r| !used.contains(&r.id))
            .map(|r| r.agent)
            .find(|agent| cast.families.get(agent) == Some(&Family::SelfHosted))
            .or_else(|| {
                self.records
                    .values()
                    .filter(|r| !used.contains(&r.id))
                    .map(|r| r.agent)
                    .find(|agent| cast.families.get(agent) == Some(&Family::Pi))
            })
            .ok_or_else(|| GenError::Missing("an agent to replay".to_owned()))?;
        let corpus = CorpusId(CORPUS.to_owned());
        let mut one = None;
        for record in self.records.values_mut() {
            if record.agent == agent && !used.contains(&record.id) {
                record.ingress = IngressMode::Replay {
                    corpus: corpus.clone(),
                };
                one.get_or_insert(record.id);
            }
        }
        let one = one.ok_or_else(|| GenError::Missing("a replayed conversation".to_owned()))?;
        Ok((corpus, one))
    }

    fn finish(self, cases: Cases) -> Result<Conversations, GenError> {
        let mut turns = HashMap::new();
        for record in self.records.values() {
            for (index, turn) in record.turns.iter().enumerate() {
                let index = u32::try_from(index).map_err(|e| GenError::invalid("turn index", e))?;
                if turns.insert(turn.exchange, (record.id, index)).is_some() {
                    return Err(GenError::Missing(format!(
                        "exchange {} threaded twice",
                        turn.exchange.ulid_text()
                    )));
                }
            }
        }
        let mut spans_by_exchange: HashMap<ExchangeId, Vec<SpanId>> = HashMap::new();
        let mut ordered: Vec<(&SpanId, &SpanRecord)> = self.spans.iter().collect();
        ordered.sort_by_key(|(id, _)| **id);
        for (id, span) in ordered {
            spans_by_exchange
                .entry(span.exchange)
                .or_default()
                .push(*id);
        }
        let mut pending = HashSet::new();
        pending.insert(cases.pending_scan);
        Ok(Conversations {
            records: self.records,
            turns,
            spans: self.spans,
            spans_by_exchange,
            relayed: self.relayed,
            bodies: Arc::new(self.bodies),
            pending,
            cases: Some(cases),
        })
    }
}

fn first_inputs_empty(record: &ConversationRecord) -> bool {
    record
        .turns
        .first()
        .is_none_or(|t| t.inputs.is_empty() && t.output.is_none())
}

fn entry(message: MessageHash, role: Role) -> Entry {
    Entry {
        message,
        role,
        carried_over: false,
    }
}

/// The role of the message a copy carried by `carrier` arrived in.
fn role_of(carrier: &Carrier) -> Role {
    match carrier {
        Carrier::ToolResult(_) => Role::Tool,
        Carrier::UserTurn => Role::User,
        Carrier::SystemPrompt => Role::System,
        Carrier::ReaderOutput => Role::Assistant,
    }
}

/// A generated user message made unique to `exchange`.
fn rename(message: Message, exchange: ExchangeId) -> Message {
    use crosstalk_spec::observed::message::{MessageBody, Text, UserPart};
    let text = match &message.body {
        MessageBody::User(parts) => parts
            .iter()
            .find_map(|p| match p {
                UserPart::Text(Text(t)) => Some(t.clone()),
                _ => None,
            })
            .unwrap_or_default(),
        _ => String::new(),
    };
    Message::new(MessageBody::User(vec![UserPart::Text(Text(format!(
        "Continue: {text} (step {})",
        exchange.ulid_text()
    )))]))
}

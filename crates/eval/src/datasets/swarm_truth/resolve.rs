//! The resolver: truth rows plus the gateway's exchange log become one
//! eval [`World`] of labels over the gateway's real exchange ids.
//!
//! Join rules:
//!
//! - **Agents.** `AgentKey { world: header.world, name }`. A session id
//!   belongs to the agent the truth rows name for it; an exchange belongs to
//!   its session's agent.
//! - **Reader.** The session's exchange at ordinal `reader_turn`, checked by
//!   finding a tool result for `content.at.tool_use_id` whose text hashes
//!   to `content.blake3`. When that exchange does not hold the tool result,
//!   the session's first exchange that does is used and the row is
//!   reported (`turn_mismatch`). A tool result whose bytes do not hash right
//!   drops the row (`hash_mismatch`).
//! - **Writer.** The session's exchange at `writer_turn`, checked by its
//!   response's `PUT` tool call `writer_tool_use_id` whose `body` hashes to
//!   `content.blake3`; the same fallback. A writer that cannot be joined
//!   leaves the label without a sender exchange.
//! - **Location.** The whole text of the reader's tool result part.
//!
//! Rows become:
//!
//! | Row | Label |
//! | --- | --- |
//! | `transmission` | `ExpectedTransmission`: Channel route (`Locator::Url` of the canonical URL), `ToolResult` carrier, `Construction` tier |
//! | `self_read` | `NegativeControl` `SelfRead`, writer → itself, at the read |
//! | `reread` | `NegativeControl` `Reread`, writer → reader, at the read |
//! | `miss` | `NegativeControl` `Miss` from every other agent of the world, at the read |
//! | `agent_cluster` | none: reported as `key_group_not_a_cluster` |
//!
//! A key group lists agents that share one API key; an eval
//! `AgentCluster` lists keys that are one agent. They are different claims,
//! so key groups are kept on [`Resolved::key_groups`] for identity work and
//! not labelled.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use crosstalk_flow::extract::resource::url_locator;
use crosstalk_spec::derived::flow::resource::Locator;
use crosstalk_spec::derived::provenance::span::SpanLocation;
use crosstalk_spec::ids::ExchangeId;
use crosstalk_spec::observed::exchange::Exchange;

use super::bodies::{Bodies, Cached};
use super::diagnostics::{Diagnostic, Diagnostics, Effect, JoinFailure, RowKind, Side};
use super::exchange_log::{Session, Sessions};
use super::locate::{FoundCall, FoundResult, LocateError, tool_result, write_call};
use super::schema::{Delivery, HexDigest, KeyGroup, Miss, TruthRoute};
use super::truth_file::{DeliveryKind, Row, TruthFile};
use super::{DATASET, MODEL};
use crate::corpus::{CorpusError, Coverage, Driven, World, WorldBuilder};
use crate::keys::{AgentKey, DatasetId, SourceRef, WorldKey};
use crate::truth::{
    CarrierKind, Expectation, ExpectedContent, ExpectedTransmission, MatchNeed, NegativeControl,
    NegativeLabel, NegativeReason, RouteExpectation, Tier, TransmissionLabel,
};

/// Which agent each captured exchange and session belongs to.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct AgentIndex {
    pub sessions: BTreeMap<String, AgentKey>,
    pub exchanges: HashMap<ExchangeId, AgentKey>,
}

impl AgentIndex {
    pub fn exchange(&self, id: ExchangeId) -> Option<&AgentKey> {
        self.exchanges.get(&id)
    }
}

/// How many labels the resolver made.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize)]
pub struct ResolveCounts {
    pub rows: u64,
    pub transmissions: u64,
    pub without_sender: u64,
    pub self_reads: u64,
    pub rereads: u64,
    pub misses: u64,
    /// Miss controls: one per other agent per miss.
    pub miss_controls: u64,
    pub key_groups: u64,
    pub dropped: u64,
}

/// The resolved benchmark world.
#[derive(Debug, Clone)]
pub struct Resolved {
    pub world: World,
    pub agents: AgentIndex,
    pub key_groups: Vec<KeyGroup>,
    pub diagnostics: Diagnostics,
    pub counts: ResolveCounts,
}

#[derive(Debug, thiserror::Error)]
pub enum ResolveError {
    #[error(transparent)]
    Corpus(#[from] CorpusError),
}

/// Where a join looked: the truth line, the row and side, and the session
/// and turn it named.
struct Site<'s> {
    line: usize,
    row: RowKind,
    side: Side,
    session_id: &'s str,
    session: &'s Session,
    turn: u32,
}

/// A joined read: the reader's exchange and the tool result in it.
struct ReadJoin {
    exchange: ExchangeId,
    found: FoundResult,
}

struct Resolver<'a, B> {
    file: &'a str,
    world: WorldKey,
    sessions: &'a Sessions,
    bodies: &'a mut Cached<B>,
    diagnostics: Diagnostics,
}

/// The truth's agents, the session each row names for them, and the
/// sessions two agents claim.
fn agents_and_sessions(
    truth: &TruthFile,
) -> (BTreeSet<String>, BTreeMap<String, BTreeSet<String>>) {
    let mut agents = BTreeSet::new();
    let mut sessions: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    let mut claim = |session: &str, agent: &str| {
        sessions
            .entry(session.to_owned())
            .or_default()
            .insert(agent.to_owned());
    };
    for numbered in &truth.rows {
        match &numbered.row {
            Row::Delivery { row, .. } => {
                agents.insert(row.writer.clone());
                agents.insert(row.reader.clone());
                claim(&row.writer_session, &row.writer);
                claim(&row.reader_session, &row.reader);
            }
            Row::Miss(row) => {
                agents.insert(row.reader.clone());
                claim(&row.reader_session, &row.reader);
            }
            Row::Cluster(row) => agents.extend(row.agents.iter().cloned()),
        }
    }
    (agents, sessions)
}

/// Resolves `truth` against the exchange log's `sessions`, reading bodies
/// from `bodies`. `file` names the truth file in labels' source references.
pub fn resolve<B: Bodies>(
    truth: &TruthFile,
    file: &str,
    sessions: &Sessions,
    bodies: &mut Cached<B>,
) -> Result<Resolved, ResolveError> {
    let dataset = DatasetId::new(DATASET);
    let world = WorldKey::new(truth.header.world.clone());
    let mut builder = WorldBuilder::new(dataset, world.clone());
    let (names, claims) = agents_and_sessions(truth);
    for name in &names {
        builder.agent(name, Driven::Model, MODEL)?;
    }
    let mut resolver = Resolver {
        file,
        world: world.clone(),
        sessions,
        bodies,
        diagnostics: Diagnostics::default(),
    };
    let mut index = AgentIndex::default();
    for (session, agents) in claims {
        if agents.len() > 1 {
            resolver.diagnostics.push(Diagnostic {
                line: None,
                row: None,
                side: Side::Row,
                failure: JoinFailure::SessionConflict {
                    session: session.clone(),
                    agents: agents.iter().cloned().collect(),
                },
                effect: Effect::Noted,
            });
        }
        if let Some(first) = agents.into_iter().next() {
            let key = AgentKey::new(world.clone(), first);
            if let Some(found) = sessions.get(&session) {
                for exchange in &found.exchanges {
                    index.exchanges.insert(exchange.meta.id, key.clone());
                }
            }
            index.sessions.insert(session, key);
        }
    }
    let mut counts = ResolveCounts::default();
    let mut key_groups = Vec::new();
    for numbered in &truth.rows {
        counts.rows += 1;
        let line = numbered.line;
        match &numbered.row {
            Row::Delivery { kind, row } => match resolver.delivery(*kind, row, line) {
                Some(made) => {
                    match *kind {
                        DeliveryKind::Transmission => {
                            counts.transmissions += 1;
                            if let Expectation::Transmission(expected) = &made
                                && expected.label().sender_exchange.is_none()
                            {
                                counts.without_sender += 1;
                            }
                        }
                        DeliveryKind::SelfRead => counts.self_reads += 1,
                        DeliveryKind::Reread => counts.rereads += 1,
                    }
                    builder.expect(made);
                }
                None => counts.dropped += 1,
            },
            Row::Miss(row) => {
                let made = resolver.miss(row, line, &names);
                if made.is_empty() {
                    counts.dropped += 1;
                } else {
                    counts.misses += 1;
                    counts.miss_controls += made.len() as u64;
                    for control in made {
                        builder.expect(control);
                    }
                }
            }
            Row::Cluster(row) => {
                counts.key_groups += 1;
                resolver.diagnostics.push(Diagnostic {
                    line: Some(line),
                    row: Some(RowKind::AgentCluster),
                    side: Side::Row,
                    failure: JoinFailure::KeyGroupNotACluster {
                        key_group: row.key_group,
                        agents: row.agents.len(),
                    },
                    effect: Effect::Noted,
                });
                key_groups.push(row.clone());
            }
        }
    }
    let diagnostics = resolver.diagnostics;
    Ok(Resolved {
        world: builder.finish(Coverage::Complete {
            tier: Tier::Construction,
        }),
        agents: index,
        key_groups,
        diagnostics,
        counts,
    })
}

/// `Normalized` when the page holds a character JSON escapes (the writer's
/// `PUT` carries it escaped inside its arguments), `Exact` otherwise.
pub fn needs(text: &str) -> MatchNeed {
    if text.chars().any(|c| c == '"' || c == '\\' || c < ' ') {
        MatchNeed::Normalized
    } else {
        MatchNeed::Exact
    }
}

impl<B: Bodies> Resolver<'_, B> {
    fn key(&self, name: &str) -> AgentKey {
        AgentKey::new(self.world.clone(), name)
    }

    fn source(&self, line: usize) -> SourceRef {
        SourceRef::new(self.file, format!("line/{line}"))
    }

    fn report(
        &mut self,
        line: usize,
        row: RowKind,
        side: Side,
        failure: JoinFailure,
        effect: Effect,
    ) {
        self.diagnostics.push(Diagnostic {
            line: Some(line),
            row: Some(row),
            side,
            failure,
            effect,
        });
    }

    fn delivery(&mut self, kind: DeliveryKind, row: &Delivery, line: usize) -> Option<Expectation> {
        let row_kind = RowKind::from(kind);
        let TruthRoute::Channel { url } = &row.route;
        let resource = match url_locator(url) {
            Ok(locator) => locator,
            Err(error) => {
                self.report(
                    line,
                    row_kind,
                    Side::Row,
                    JoinFailure::BadUrl {
                        url: url.clone(),
                        reason: error.to_string(),
                    },
                    Effect::Dropped,
                );
                return None;
            }
        };
        let read = self.read(
            line,
            row_kind,
            &row.reader_session,
            row.reader_turn,
            &row.content.at.tool_use_id,
            Some(row.content.blake3),
        )?;
        let from = self.key(&row.writer);
        let to = self.key(&row.reader);
        let made = match kind {
            DeliveryKind::Transmission => {
                let sender_exchange = self.write(line, row);
                self.transmission(from, to, sender_exchange, read, resource, line)
            }
            DeliveryKind::SelfRead => {
                Self::control(from, to, read, NegativeReason::SelfRead, self.source(line))
            }
            DeliveryKind::Reread => {
                Self::control(from, to, read, NegativeReason::Reread, self.source(line))
            }
        };
        match made {
            Ok(expectation) => Some(expectation),
            Err(reason) => {
                self.report(
                    line,
                    row_kind,
                    Side::Row,
                    JoinFailure::InvalidLabel { reason },
                    Effect::Dropped,
                );
                None
            }
        }
    }

    fn transmission(
        &self,
        from: AgentKey,
        to: AgentKey,
        sender_exchange: Option<ExchangeId>,
        read: ReadJoin,
        resource: Locator,
        line: usize,
    ) -> Result<Expectation, String> {
        let needs = needs(&read.found.text);
        ExpectedTransmission::new(TransmissionLabel {
            from,
            to,
            sender_exchange,
            reader_exchange: read.exchange,
            route: RouteExpectation::Channel { resource },
            carrier: CarrierKind::ToolResult,
            content: ExpectedContent {
                text: read.found.text,
                at: read.found.at,
            },
            needs,
            tier: Tier::Construction,
            source: self.source(line),
        })
        .map(Expectation::Transmission)
        .map_err(|error| error.to_string())
    }

    fn control(
        from: AgentKey,
        to: AgentKey,
        read: ReadJoin,
        reason: NegativeReason,
        source: SourceRef,
    ) -> Result<Expectation, String> {
        NegativeControl::new(NegativeLabel {
            from,
            to,
            reader_exchange: Some(read.exchange),
            at: Some(read.found.at),
            origin: None,
            text: Some(read.found.text),
            reason,
            tier: Tier::Construction,
            source,
        })
        .map(Expectation::NoTransmission)
        .map_err(|error| error.to_string())
    }

    fn miss(&mut self, row: &Miss, line: usize, names: &BTreeSet<String>) -> Vec<Expectation> {
        let Some(read) = self.read(
            line,
            RowKind::Miss,
            &row.reader_session,
            row.reader_turn,
            &row.reader_tool_use_id,
            None,
        ) else {
            return Vec::new();
        };
        let to = self.key(&row.reader);
        let at: SpanLocation = read.found.at;
        let mut out = Vec::new();
        for name in names.iter().filter(|name| **name != row.reader) {
            let made = NegativeControl::new(NegativeLabel {
                from: self.key(name),
                to: to.clone(),
                reader_exchange: Some(read.exchange),
                at: Some(at),
                origin: None,
                text: None,
                reason: NegativeReason::Miss,
                tier: Tier::Construction,
                source: self.source(line),
            });
            match made {
                Ok(control) => out.push(Expectation::NoTransmission(control)),
                Err(error) => self.report(
                    line,
                    RowKind::Miss,
                    Side::Row,
                    JoinFailure::InvalidLabel {
                        reason: error.to_string(),
                    },
                    Effect::Dropped,
                ),
            }
        }
        out
    }

    /// Joins a read: the tool result `call` at `turn` of `session`, checked
    /// against `digest` when given.
    fn read(
        &mut self,
        line: usize,
        row: RowKind,
        session_id: &str,
        turn: u32,
        call: &str,
        digest: Option<HexDigest>,
    ) -> Option<ReadJoin> {
        let sessions = self.sessions;
        let Some(session) = sessions.get(session_id) else {
            self.report(
                line,
                row,
                Side::Reader,
                JoinFailure::UnknownSession {
                    session: session_id.to_owned(),
                },
                Effect::Dropped,
            );
            return None;
        };
        let at_turn = session.at(turn);
        let candidates = at_turn.into_iter().chain(
            session
                .exchanges
                .iter()
                .filter(|exchange| Some(exchange.meta.id) != at_turn.map(|at| at.meta.id)),
        );
        for exchange in candidates {
            let found = match tool_result(exchange, call, self.bodies) {
                Ok(Some(found)) => found,
                Ok(None) => continue,
                Err(error) => {
                    self.body_failure(line, row, Side::Reader, exchange, &error, Effect::Dropped);
                    return None;
                }
            };
            let id = exchange.meta.id;
            if digest.is_some_and(|digest| digest != found.digest()) {
                self.report(
                    line,
                    row,
                    Side::Reader,
                    JoinFailure::HashMismatch {
                        session: session_id.to_owned(),
                        turn,
                        tool_use_id: call.to_owned(),
                        exchange: id,
                    },
                    Effect::Dropped,
                );
                return None;
            }
            let site = Site {
                line,
                row,
                side: Side::Reader,
                session_id,
                session,
                turn,
            };
            self.check_turn(&site, id, Effect::Kept);
            return Some(ReadJoin {
                exchange: id,
                found,
            });
        }
        let site = Site {
            line,
            row,
            side: Side::Reader,
            session_id,
            session,
            turn,
        };
        self.not_found(&site, call, Effect::Dropped);
        None
    }

    /// Joins a transmission's write: the sender's exchange, or `None` (and
    /// a diagnostic) when it cannot be joined.
    fn write(&mut self, line: usize, row: &Delivery) -> Option<ExchangeId> {
        let kind = RowKind::Transmission;
        let effect = Effect::KeptWithoutSender;
        let sessions = self.sessions;
        let session_id = &row.writer_session;
        let call = &row.writer_tool_use_id;
        let turn = row.writer_turn;
        let Some(session) = sessions.get(session_id) else {
            self.report(
                line,
                kind,
                Side::Writer,
                JoinFailure::UnknownSession {
                    session: session_id.clone(),
                },
                effect,
            );
            return None;
        };
        let at_turn = session.at(turn);
        let candidates = at_turn.into_iter().chain(
            session
                .exchanges
                .iter()
                .filter(|exchange| Some(exchange.meta.id) != at_turn.map(|at| at.meta.id)),
        );
        for exchange in candidates {
            let id = exchange.meta.id;
            let body = match write_call(exchange, call, self.bodies) {
                Ok(Some(FoundCall::Put { body, .. })) => body,
                Ok(Some(FoundCall::NotAPut)) => {
                    self.report(
                        line,
                        kind,
                        Side::Writer,
                        JoinFailure::NotAPut {
                            session: session_id.clone(),
                            tool_use_id: call.clone(),
                            exchange: id,
                        },
                        effect,
                    );
                    return None;
                }
                Ok(None) => continue,
                Err(error) => {
                    self.body_failure(line, kind, Side::Writer, exchange, &error, effect);
                    return None;
                }
            };
            if HexDigest::blake3_of(body.as_bytes()) != row.content.blake3 {
                self.report(
                    line,
                    kind,
                    Side::Writer,
                    JoinFailure::HashMismatch {
                        session: session_id.clone(),
                        turn,
                        tool_use_id: call.clone(),
                        exchange: id,
                    },
                    effect,
                );
                return None;
            }
            let site = Site {
                line,
                row: kind,
                side: Side::Writer,
                session_id,
                session,
                turn,
            };
            self.check_turn(&site, id, Effect::Kept);
            return Some(id);
        }
        let site = Site {
            line,
            row: kind,
            side: Side::Writer,
            session_id,
            session,
            turn,
        };
        self.not_found(&site, call, effect);
        None
    }

    fn check_turn(&mut self, site: &Site<'_>, found: ExchangeId, effect: Effect) {
        let found_turn = site.session.ordinal(found).unwrap_or(u32::MAX);
        if found_turn != site.turn {
            self.report(
                site.line,
                site.row,
                site.side,
                JoinFailure::TurnMismatch {
                    session: site.session_id.to_owned(),
                    turn: site.turn,
                    found_turn,
                    exchange: found,
                },
                effect,
            );
        }
    }

    fn not_found(&mut self, site: &Site<'_>, call: &str, effect: Effect) {
        let exchanges = site.session.exchanges.len();
        let failure = if usize::try_from(site.turn).map_or(true, |turn| turn >= exchanges) {
            JoinFailure::TurnOutOfRange {
                session: site.session_id.to_owned(),
                turn: site.turn,
                exchanges,
            }
        } else {
            JoinFailure::ToolUseMissing {
                session: site.session_id.to_owned(),
                turn: site.turn,
                tool_use_id: call.to_owned(),
            }
        };
        self.report(site.line, site.row, site.side, failure, effect);
    }

    fn body_failure(
        &mut self,
        line: usize,
        row: RowKind,
        side: Side,
        exchange: &Exchange,
        error: &LocateError,
        effect: Effect,
    ) {
        self.report(
            line,
            row,
            side,
            JoinFailure::Body {
                exchange: exchange.meta.id,
                reason: error.to_string(),
            },
            effect,
        );
    }
}

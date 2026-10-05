//! `WindowedCorrelator`: one shard's correlator.
//!
//! It holds evidence per medium ([`MediumKey`]) and decides every
//! transmission at the close of its window, so the transmission states are
//! the same whatever order the evidence arrived in within the window
//! (`flow.correlator.order-insensitive`):
//!
//! - **Channel.** A pairable write and a later read of one resource by
//!   another agent within the correlation window open a channel
//!   transmission `AwaitingContent` (`OpenChannel`), one per reader
//!   exchange and writer. Tool-result matches carried by the medium's reads
//!   are held. When the window closes (`window_closes_at` of the read), a
//!   transmission one of whose writes explains held matches is confirmed
//!   with all of them (`Confirm`); otherwise it is suspected (`Suspect`),
//!   and it expires at `expires_at(since)` (`Discard`). A match that
//!   arrives while it is suspected confirms it at once (late confirmation);
//!   once confirmed, further matches extend it (`Extend`). A match
//!   explained by a write after its transmission was discarded opens a new
//!   one.
//! - **Content past the window.** A held match a write of its sender
//!   explains pairs that write with the read carrying it within the content
//!   retention, whatever the correlation window
//!   (`flow.correlator.content-confirms-past-window`): the co-access joins
//!   or opens the transmission, which confirms when the read's window
//!   closes. The window bounds access-only pairing alone, so a write with
//!   spans is kept for the retention; one without is kept for the window.
//! - **Not a channel.** A user-turn, system-prompt or reader-output match, a
//!   match between parent and child, and a tool-result match whose call
//!   yielded no read by the window's close (`window_closes_at` of its
//!   reader exchange's start) are collected per identity and opened
//!   confirmed when that window closes (`OpenConfirmed`); later ones extend
//!   it.
//!
//! Input that arrives after its window closed (by the last tick) is
//! decided at once. Time is only ever an input's event time or a tick's
//! `now`; nothing reads a clock (`flow.correlator.no-io`), so a replay with
//! corpus timestamps runs exactly as live traffic does, on the replay's
//! ticks.
//!
//! The correlator needs each reader exchange's start: it learns it from
//! accesses (an access's `at` is its exchange's start) and from
//! [`WindowedCorrelator::exchange`], which the flow consumer feeds from
//! `ExchangeCaptured`. A match whose exchange start is unknown waits for
//! it.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use crosstalk_spec::derived::flow::access::{Access, AccessOp};
use crosstalk_spec::derived::flow::timing::CorrelationTiming;
use crosstalk_spec::derived::flow::transmission::{Confirmed, NonChannelRoute};
use crosstalk_spec::derived::provenance::matching::{Carrier, ContentMatch};
use crosstalk_spec::ids::{AccessId, AgentId, ChannelId, ExchangeId, MessageHash, TransmissionId};
use crosstalk_spec::interfaces::l5_flow::{Correlator, TransmissionUpdate};
use crosstalk_spec::observed::message::{ToolCallId, ToolName};
use crosstalk_spec::support::{NonEmpty, Timestamp};

use super::decide::{pair, settle};
use super::ids;
use super::key::MediumKey;
use super::kinship::{Kin, Kinship};
use super::medium::{Held, MatchKey, Medium, Phase};
use super::pairing::{self, WriteOutcome};
use super::retention::ContentRetention;
use super::route::{self, Carriage, RouteChoice, RouteKey};

/// The tool name a `Direct(ToolResult)` transmission carries when the
/// call's name never reached the correlator.
pub const UNKNOWN_TOOL: &str = "unknown";

/// One update the correlator decided, with the time its transmission
/// opened: the read's time for a channel transmission, the reader
/// exchange's start for one opened confirmed. The flow consumer needs it
/// to store an opened channel transmission (`AwaitingContent`'s
/// `window_closes_at` is `window_closes_at(opened_at)`).
#[derive(Debug, Clone, PartialEq)]
pub struct Decided {
    pub update: TransmissionUpdate,
    pub opened_at: Timestamp,
}

/// A read's tool result part, by reader and exchange: what a tool-result
/// match is carried by.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ReadPart {
    reader: AgentId,
    exchange: ExchangeId,
    message: MessageHash,
    index: u16,
}

impl ReadPart {
    /// The part of a read access; `None` for a write.
    pub fn of_read(access: &Access) -> Option<Self> {
        match access.op {
            AccessOp::Read { result } => Some(Self {
                reader: access.agent,
                exchange: access.exchange,
                message: result.message,
                index: result.index,
            }),
            AccessOp::Write { .. } => None,
        }
    }

    /// The part a match sits in.
    pub fn of_match(content: &ContentMatch) -> Self {
        let part = content.read_at().part;
        Self {
            reader: content.reader(),
            exchange: content.reader_exchange(),
            message: part.message,
            index: part.index,
        }
    }
}

/// A transmission opened confirmed: its identity.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct DirectIdent {
    exchange: ExchangeId,
    sender: AgentId,
    route: RouteKey,
}

/// A transmission opened confirmed: collecting until its window closes,
/// then open.
#[derive(Debug, Clone, PartialEq)]
struct DirectTx {
    to: AgentId,
    /// The reader exchange's start.
    at: Timestamp,
    closes_at: Timestamp,
    route: NonChannelRoute,
    id: Option<TransmissionId>,
    matches: BTreeMap<MatchKey, ContentMatch>,
}

/// One medium's evidence in transit between shards.
#[derive(Debug, Clone, PartialEq)]
pub struct MediumEvidence {
    medium: Medium,
}

/// The correlator of one shard. Feed it accesses, content matches,
/// exchange starts, agent kinship and ticks; it does no I/O.
#[derive(Debug, Clone)]
pub struct WindowedCorrelator {
    timing: CorrelationTiming,
    retention: ContentRetention,
    media: BTreeMap<MediumKey, Medium>,
    /// Every read's part, to find the read carrying a tool-result match.
    reads: BTreeMap<ReadPart, (MediumKey, Timestamp)>,
    /// Tool-result matches whose read has not arrived.
    uncarried: BTreeMap<MatchKey, ContentMatch>,
    /// Matches for a non-channel route, waiting for their exchange's start.
    timeless: BTreeMap<MatchKey, (ContentMatch, NonChannelRoute)>,
    direct: BTreeMap<DirectIdent, DirectTx>,
    exchanges: BTreeMap<ExchangeId, Timestamp>,
    tools: BTreeMap<(AgentId, String), (ToolName, Timestamp)>,
    kin: Kinship,
    seen_accesses: BTreeMap<AccessId, Timestamp>,
    seen_matches: BTreeSet<MatchKey>,
    last_tick: Option<Timestamp>,
}

impl WindowedCorrelator {
    /// A correlator remembering writes for content for the default
    /// retention ([`ContentRetention::default_for`]).
    pub fn new(timing: CorrelationTiming) -> Self {
        Self::with_retention(timing, ContentRetention::default_for(timing))
    }

    /// A correlator whose content-confirmed pairing reaches back
    /// `retention` (`flow.correlator.content-confirms-past-window`).
    pub fn with_retention(timing: CorrelationTiming, retention: ContentRetention) -> Self {
        Self {
            timing,
            retention,
            media: BTreeMap::new(),
            reads: BTreeMap::new(),
            uncarried: BTreeMap::new(),
            timeless: BTreeMap::new(),
            direct: BTreeMap::new(),
            exchanges: BTreeMap::new(),
            tools: BTreeMap::new(),
            kin: Kinship::default(),
            seen_accesses: BTreeMap::new(),
            seen_matches: BTreeSet::new(),
            last_tick: None,
        }
    }

    pub fn timing(&self) -> CorrelationTiming {
        self.timing
    }

    pub fn retention(&self) -> ContentRetention {
        self.retention
    }

    /// The last tick processed: L7 reads it as `PipelineFrontier::ticked_through`.
    pub fn last_tick(&self) -> Option<Timestamp> {
        self.last_tick
    }

    /// Whether this shard holds evidence for `medium`.
    pub fn holds(&self, medium: MediumKey) -> bool {
        self.media.contains_key(&medium)
    }

    /// Record how `agent` resolves (from `AgentReads::cluster`).
    pub fn learn_kin(&mut self, agent: AgentId, kin: Kin) {
        self.kin.learn(agent, kin);
    }

    pub fn knows_agent(&self, agent: AgentId) -> bool {
        self.kin.knows(agent)
    }

    /// Forget every agent's kinship: a merge or an unmerge happened.
    pub fn forget_kin(&mut self) {
        self.kin.forget_all();
    }

    /// The tool `agent` called as `call`, named `name`, at `at`: the name a
    /// `Direct(ToolResult)` transmission for its result carries.
    pub fn tool_named(&mut self, agent: AgentId, call: &ToolCallId, name: ToolName, at: Timestamp) {
        self.tools.insert((agent, call.0.clone()), (name, at));
    }

    /// The reader exchange `exchange` started at `started_at`.
    pub fn exchange(&mut self, exchange: ExchangeId, started_at: Timestamp) -> Vec<Decided> {
        let mut out = Vec::new();
        self.exchanges.entry(exchange).or_insert(started_at);
        let waiting: Vec<MatchKey> = self
            .timeless
            .keys()
            .filter(|key| key.exchange() == exchange)
            .copied()
            .collect();
        for key in waiting {
            if let Some((content, route)) = self.timeless.remove(&key) {
                self.candidate(&content, route, &mut out);
            }
        }
        if let Some(now) = self.last_tick {
            self.resolve_uncarried(Some(exchange), now, &mut out);
        }
        out
    }

    /// An access on a resource on the canonical channel `channel` (`None`:
    /// on no channel).
    pub fn access(&mut self, access: &Access, channel: Option<ChannelId>) -> Vec<Decided> {
        let mut out = Vec::new();
        if self.seen_accesses.contains_key(&access.id) {
            return out;
        }
        self.seen_accesses.insert(access.id, access.at);
        out.extend(self.exchange(access.exchange, access.at));
        let key = MediumKey::of(access, channel);
        let timing = self.timing;
        match &access.op {
            AccessOp::Write { .. } => {
                if !pairing::outcome(access).is_some_and(WriteOutcome::pairs) {
                    tracing::debug!(access = %access.id.ulid_text(), "write does not pair; not correlated");
                    return out;
                }
                let medium = self.media.entry(key).or_default();
                medium.writes.insert(access.id, access.clone());
                let reads: Vec<Access> = medium.reads.values().cloned().collect();
                for read in &reads {
                    pair(medium, key, access, read, timing, &mut out);
                }
            }
            AccessOp::Read { .. } => {
                if let Some(part) = ReadPart::of_read(access) {
                    self.reads.insert(part, (key, access.at));
                }
                let carried: Vec<MatchKey> = self
                    .uncarried
                    .iter()
                    .filter(|(_, content)| pairing::carried_by(content, access))
                    .map(|(key, _)| *key)
                    .collect();
                let medium = self.media.entry(key).or_default();
                medium.reads.insert(access.id, access.clone());
                for match_key in carried {
                    if let Some(content) = self.uncarried.remove(&match_key) {
                        medium.held.insert(
                            match_key,
                            Held {
                                content,
                                read_at: access.at,
                            },
                        );
                    }
                }
                let writes: Vec<Access> = medium.writes.values().cloned().collect();
                for write in &writes {
                    pair(medium, key, write, access, timing, &mut out);
                }
            }
        }
        if let Some(now) = self.last_tick {
            self.settle_medium(key, now, &mut out);
        }
        out
    }

    /// A content match from L4.
    pub fn content(&mut self, content: &ContentMatch) -> Vec<Decided> {
        let mut out = Vec::new();
        let key = MatchKey::of(content);
        if !self.seen_matches.insert(key) {
            return out;
        }
        let name = self.tool_name(content);
        match content.carrier() {
            Carrier::ToolResult(_) => match self.reads.get(&ReadPart::of_match(content)).copied() {
                Some((medium, read_at)) => {
                    match route::choose(content, Carriage::Read, &self.kin, || name) {
                        RouteChoice::Channel => {
                            self.media.entry(medium).or_default().held.insert(
                                key,
                                Held {
                                    content: content.clone(),
                                    read_at,
                                },
                            );
                            if let Some(now) = self.last_tick {
                                self.settle_medium(medium, now, &mut out);
                            }
                        }
                        RouteChoice::NonChannel(route) => {
                            self.candidate(content, route, &mut out);
                        }
                    }
                }
                None => match route::choose(content, Carriage::Read, &self.kin, || name) {
                    RouteChoice::NonChannel(route @ NonChannelRoute::Delegation(_)) => {
                        self.candidate(content, route, &mut out);
                    }
                    RouteChoice::NonChannel(_) | RouteChoice::Channel => {
                        self.uncarried.insert(key, content.clone());
                        if let Some(now) = self.last_tick {
                            self.resolve_uncarried(Some(content.reader_exchange()), now, &mut out);
                        }
                    }
                },
            },
            Carrier::UserTurn | Carrier::SystemPrompt | Carrier::ReaderOutput => {
                match route::choose(content, Carriage::NoAccess, &self.kin, || name) {
                    RouteChoice::NonChannel(route) => self.candidate(content, route, &mut out),
                    // Only a tool result is ever carried by a read.
                    RouteChoice::Channel => {}
                }
            }
        }
        out
    }

    /// Close every window and expire every suspicion up to `now`. A tick
    /// earlier than the last one counts as the last one.
    pub fn tick(&mut self, now: Timestamp) -> Vec<Decided> {
        let now = self.last_tick.map_or(now, |last| last.max(now));
        self.last_tick = Some(now);
        let mut out = Vec::new();
        self.resolve_uncarried(None, now, &mut out);
        let due: Vec<DirectIdent> = self
            .direct
            .iter()
            .filter(|(_, tx)| tx.id.is_none() && tx.closes_at <= now)
            .map(|(ident, _)| ident.clone())
            .collect();
        for ident in &due {
            self.open_direct(ident, &mut out);
        }
        let media: Vec<MediumKey> = self.media.keys().copied().collect();
        for key in media {
            self.settle_medium(key, now, &mut out);
        }
        self.collect_garbage(now);
        out
    }

    /// Remove `medium`'s evidence, to hand it to another shard.
    pub fn take_medium(&mut self, medium: MediumKey) -> Option<MediumEvidence> {
        let taken = self.media.remove(&medium)?;
        self.reads.retain(|_, (key, _)| *key != medium);
        Some(MediumEvidence { medium: taken })
    }

    /// Take over evidence handed from another medium as `medium`'s: a
    /// resource's evidence once a channel was discovered from it, or a
    /// superseded channel's once it was promoted. Writes and reads of one
    /// resource that the two media held apart now pair.
    pub fn absorb(&mut self, medium: MediumKey, evidence: MediumEvidence) -> Vec<Decided> {
        let mut out = Vec::new();
        let incoming = evidence.medium;
        let timing = self.timing;
        let carried: Vec<(MatchKey, Timestamp)> = self
            .uncarried
            .iter()
            .filter_map(|(key, content)| {
                incoming
                    .reads
                    .values()
                    .find(|read| pairing::carried_by(content, read))
                    .map(|read| (*key, read.at))
            })
            .collect();
        for read in incoming.reads.values() {
            if let Some(part) = ReadPart::of_read(read) {
                self.reads.insert(part, (medium, read.at));
            }
        }
        let target = self.media.entry(medium).or_default();
        let old_writes: Vec<Access> = target.writes.values().cloned().collect();
        let old_reads: Vec<Access> = target.reads.values().cloned().collect();
        target.open.extend(incoming.open);
        for (ident, (count, at)) in incoming.retired {
            let entry = target.retired.entry(ident).or_insert((0, at));
            *entry = (entry.0.max(count), entry.1.max(at));
        }
        target.held.extend(incoming.held);
        for (delivery, delivered) in incoming.delivered {
            // Two media delivered the span apart: the earlier transmission
            // (ids are time-ordered by their read) keeps it.
            let entry = target.delivered.entry(delivery).or_insert(delivered);
            entry.transmission = entry.transmission.min(delivered.transmission);
            entry.last = entry.last.max(delivered.last);
        }
        for (key, read_at) in carried {
            if let Some(content) = self.uncarried.remove(&key) {
                target.held.insert(key, Held { content, read_at });
            }
        }
        target.writes.extend(incoming.writes.clone());
        target.reads.extend(incoming.reads.clone());
        for write in incoming.writes.values() {
            for read in &old_reads {
                pair(target, medium, write, read, timing, &mut out);
            }
        }
        for read in incoming.reads.values() {
            for write in &old_writes {
                pair(target, medium, write, read, timing, &mut out);
            }
        }
        if let Some(now) = self.last_tick {
            self.settle_medium(medium, now, &mut out);
        }
        out
    }

    /// Move `from`'s evidence to `to` within this shard.
    pub fn rekey(&mut self, from: MediumKey, to: MediumKey) -> Vec<Decided> {
        match self.take_medium(from) {
            Some(evidence) if from != to => self.absorb(to, evidence),
            Some(evidence) => self.absorb(from, evidence),
            None => Vec::new(),
        }
    }

    /// Remove the tool-result matches waiting here that `read` carries, to
    /// hand them to the shard that holds the read.
    pub fn take_uncarried(&mut self, read: &Access) -> Vec<ContentMatch> {
        let keys: Vec<MatchKey> = self
            .uncarried
            .iter()
            .filter(|(_, content)| pairing::carried_by(content, read))
            .map(|(key, _)| *key)
            .collect();
        keys.iter()
            .filter_map(|key| {
                self.seen_matches.remove(key);
                self.uncarried.remove(key)
            })
            .collect()
    }

    fn tool_name(&self, content: &ContentMatch) -> ToolName {
        match content.carrier() {
            Carrier::ToolResult(call) => self
                .tools
                .get(&(content.reader(), call.0.clone()))
                .map_or_else(
                    || ToolName(UNKNOWN_TOOL.to_owned()),
                    |(name, _)| name.clone(),
                ),
            Carrier::UserTurn | Carrier::SystemPrompt | Carrier::ReaderOutput => {
                ToolName(UNKNOWN_TOOL.to_owned())
            }
        }
    }

    /// A match for a non-channel route: collected into its transmission,
    /// opened now when its window already closed.
    fn candidate(
        &mut self,
        content: &ContentMatch,
        route: NonChannelRoute,
        out: &mut Vec<Decided>,
    ) {
        let Some(at) = self.exchanges.get(&content.reader_exchange()).copied() else {
            self.timeless
                .insert(MatchKey::of(content), (content.clone(), route));
            return;
        };
        let ident = DirectIdent {
            exchange: content.reader_exchange(),
            sender: content.origin_agent(),
            route: RouteKey::of(&route),
        };
        let closes_at = self.timing.window_closes_at(at);
        let tx = self
            .direct
            .entry(ident.clone())
            .or_insert_with(|| DirectTx {
                to: content.reader(),
                at,
                closes_at,
                route,
                id: None,
                matches: BTreeMap::new(),
            });
        let key = MatchKey::of(content);
        if tx.matches.contains_key(&key) {
            return;
        }
        tx.matches.insert(key, content.clone());
        match tx.id {
            Some(id) => out.push(Decided {
                update: TransmissionUpdate::Extend {
                    transmission: id,
                    content: content.clone(),
                },
                opened_at: tx.at,
            }),
            None => {
                if self.last_tick.is_some_and(|now| tx.closes_at <= now) {
                    self.open_direct(&ident, out);
                }
            }
        }
    }

    fn open_direct(&mut self, ident: &DirectIdent, out: &mut Vec<Decided>) {
        let Some(tx) = self.direct.get_mut(ident) else {
            return;
        };
        if tx.id.is_some() {
            return;
        }
        let Some(content) = NonEmpty::from_vec(tx.matches.values().cloned().collect()) else {
            return;
        };
        match Confirmed::new(content, Vec::new(), tx.at) {
            Ok(confirmed) => {
                let id = ids::transmission_id(tx.at, ident.exchange, ident.sender, &ident.route, 0);
                tx.id = Some(id);
                out.push(Decided {
                    update: TransmissionUpdate::OpenConfirmed {
                        transmission: id,
                        to: tx.to,
                        route: tx.route.clone(),
                        confirmed,
                    },
                    opened_at: tx.at,
                });
            }
            Err(mixed) => {
                tracing::warn!(exchange = %ident.exchange.ulid_text(), error = ?mixed, "matches of one identity disagree; not opened");
            }
        }
    }

    /// Tool-result matches with no read whose window closed by `now`
    /// (for one exchange, or all): `Direct(ToolResult)` unless delegated.
    fn resolve_uncarried(
        &mut self,
        only: Option<ExchangeId>,
        now: Timestamp,
        out: &mut Vec<Decided>,
    ) {
        let timing = self.timing;
        let due: Vec<MatchKey> = self
            .uncarried
            .keys()
            .filter(|key| only.is_none_or(|exchange| key.exchange() == exchange))
            .filter(|key| {
                self.exchanges
                    .get(&key.exchange())
                    .is_some_and(|started| timing.window_closes_at(*started) <= now)
            })
            .copied()
            .collect();
        for key in due {
            let Some(content) = self.uncarried.remove(&key) else {
                continue;
            };
            let name = self.tool_name(&content);
            match route::choose(&content, Carriage::NoAccess, &self.kin, || name) {
                RouteChoice::NonChannel(route) => self.candidate(&content, route, out),
                RouteChoice::Channel => {}
            }
        }
    }

    fn settle_medium(&mut self, key: MediumKey, now: Timestamp, out: &mut Vec<Decided>) {
        let (timing, retention) = (self.timing, self.retention);
        if let Some(medium) = self.media.get_mut(&key) {
            settle(medium, key, timing, retention, now, out);
        }
    }

    fn collect_garbage(&mut self, now: Timestamp) {
        let keep = self
            .timing
            .settle_after()
            .saturating_add(self.timing.correlation_window());
        let horizon = before(now, keep);
        // A write holding spans is kept for content past the window: a read
        // still within `keep` of `now` can pair with it by content for up
        // to the retention. One without spans can never explain content.
        let content_horizon = before(now, self.retention.get().saturating_add(keep));
        for medium in self.media.values_mut() {
            medium.writes.retain(|_, write| {
                write.at >= horizon || (write.at >= content_horizon && holds_spans(write))
            });
            medium.reads.retain(|_, read| read.at >= horizon);
            medium.held.retain(|_, held| held.read_at >= horizon);
            medium.open.retain(|_, tx| {
                !(matches!(tx.phase, Phase::Confirmed(_)) && tx.opened_at < horizon)
            });
            medium.retired.retain(|_, (_, at)| *at >= horizon);
            medium
                .delivered
                .retain(|_, delivered| delivered.last >= content_horizon);
        }
        self.media.retain(|_, medium| !medium.is_empty());
        self.reads.retain(|_, (_, at)| *at >= horizon);
        self.direct
            .retain(|_, tx| !(tx.id.is_some() && tx.at < horizon));
        let exchanges = &self.exchanges;
        self.seen_matches.retain(|key| {
            exchanges
                .get(&key.exchange())
                .is_none_or(|started| *started >= horizon)
        });
        self.seen_accesses.retain(|_, at| *at >= horizon);
        self.tools.retain(|_, (_, at)| *at >= horizon);
        self.exchanges.retain(|_, started| *started >= horizon);
    }
}

impl Correlator for WindowedCorrelator {
    fn on_access(
        &mut self,
        access: &Access,
        channel: Option<ChannelId>,
    ) -> Vec<TransmissionUpdate> {
        updates(self.access(access, channel))
    }

    /// The correlator finds the read carrying a tool-result match itself;
    /// `channel` names the shard the match was routed to.
    fn on_match(
        &mut self,
        content: &ContentMatch,
        _channel: Option<ChannelId>,
    ) -> Vec<TransmissionUpdate> {
        updates(self.content(content))
    }

    fn on_tick(&mut self, now: Timestamp) -> Vec<TransmissionUpdate> {
        updates(self.tick(now))
    }
}

fn updates(decided: Vec<Decided>) -> Vec<TransmissionUpdate> {
    decided.into_iter().map(|decided| decided.update).collect()
}

/// Whether `write` carried spans content could match.
fn holds_spans(write: &Access) -> bool {
    matches!(&write.op, AccessOp::Write { spans, .. } if !spans.is_empty())
}

/// `at - by`, saturating at the epoch.
fn before(at: Timestamp, by: Duration) -> Timestamp {
    let micros = u64::try_from(by.as_micros()).unwrap_or(u64::MAX);
    Timestamp::from_micros(at.as_micros().saturating_sub(micros))
}

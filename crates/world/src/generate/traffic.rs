//! Seven days of traffic: about 5,000 transmissions on a diurnal, weekday
//! heavy curve, the accesses behind the channel-routed ones, background
//! accesses that carried nothing, and a key-value entry only one agent
//! (`cc7`) writes and reads.
//!
//! Older transmissions are mostly `Aggregated` (some `Suspected` or
//! `Discarded`); a burst in the last quarter hour holds the in-flight states
//! (`Detected`, `AwaitingContent`, `Confirmed`, `Classified`). See
//! [`super::states`] for how each state is built.

use std::collections::BTreeMap;
use std::time::Duration;

use crosstalk_spec::derived::flow::access::{Access, AccessKind, AccessOp, Extraction};
use crosstalk_spec::derived::flow::evidence::CoAccess;
use crosstalk_spec::derived::flow::resource::{Locator, Resource};
use crosstalk_spec::derived::flow::transmission::{DelegationDirection, DirectCarrier, Route};
use crosstalk_spec::derived::provenance::matching::Carrier;
use crosstalk_spec::ids::{AgentId, ChannelId, ExchangeId, ResourceId, SpanId};
use crosstalk_spec::observed::message::{ToolCallId, ToolName};
use crosstalk_spec::support::Timestamp;

use crate::clock::{DAY, HOUR, MINUTE, SECOND, minus, plus};
use crate::error::WorldError;
use crate::rng::Rng;
use crate::text::Theme;

use super::agents::Cast;
use super::bodies::Blobs;
use super::channels::ChannelPlan;
use super::drafts::lone_locator;
use super::evidence;
use super::states::{self, Builder, Planned, TxRecord, Want};

const TRANSMISSIONS: usize = 5000;
const BURST: usize = 50;

/// The correlation window: a read further than this after a write is not a
/// co-access (the config's `CorrelationTiming::correlation_window`).
const CORRELATION_WINDOW: Duration = Duration::from_secs(24 * 3600);

const DELEGATION_THEMES: &[(Theme, f64)] = &[
    (Theme::CodeReview, 3.0),
    (Theme::Research, 2.0),
    (Theme::Deploy, 2.0),
    (Theme::DataPipeline, 1.0),
    (Theme::Incidents, 1.0),
];
const DIRECT_THEMES: &[(Theme, f64)] = &[
    (Theme::Meetings, 3.0),
    (Theme::Deploy, 2.0),
    (Theme::Support, 2.0),
    (Theme::Incidents, 2.0),
];
const UNOBSERVED_THEMES: &[(Theme, f64)] = &[
    (Theme::Credentials, 2.0),
    (Theme::Scraping, 2.0),
    (Theme::Injection, 1.0),
];

enum DirectKind {
    User,
    System,
    Tool(&'static str),
}

/// Who relays to whom without a channel: orchestrators through user turns,
/// configured context through system prompts, and chat tools.
const DIRECT: &[(&str, &str, DirectKind)] = &[
    ("omp0", "cc5", DirectKind::User),
    ("omp0", "omp1", DirectKind::User),
    ("cc0", "cc1", DirectKind::System),
    ("cx0", "cc2", DirectKind::Tool("slack_read")),
    ("cc2", "cx4", DirectKind::Tool("read_inbox")),
    ("sh0", "cx3", DirectKind::User),
    ("pi2", "cc4", DirectKind::Tool("slack_read")),
    ("al3", "pi3", DirectKind::User),
    ("omp3", "omp1", DirectKind::User),
];

/// Pairs whose text crosses through something the gateway cannot see.
const UNOBSERVED: &[(&str, &str)] = &[
    ("pi0", "cc6"),
    ("omp2", "cx3"),
    ("sh1", "cc5"),
    ("pi1", "cx2"),
];

/// The agent that uses [`lone_locator`].
pub const LONE_AGENT: &str = "cc7";

/// The generated traffic.
#[derive(Debug, Clone)]
pub struct Traffic {
    /// Every resource with its first sighting, by channel plan order then
    /// the lone entry.
    pub resources: Vec<Resource>,
    /// The planned channel of each resource a channel's traffic uses: a
    /// declared channel's resources are on it from their first sighting, a
    /// discovered channel's seed from its first cross-agent transmission.
    /// The lone entry, on no channel, has none.
    pub resource_channel: BTreeMap<ResourceId, ChannelId>,
    /// `cc7`'s key-value entry: a resource only one agent uses, so never a
    /// channel.
    pub lone: ResourceId,
    /// The id minted for a channel of the lone entry, which no write ever
    /// creates: `ChannelKey::Scratch`, kept so readers can check that no
    /// such channel exists.
    pub scratch: ChannelId,
    /// Oldest first.
    pub accesses: Vec<Access>,
    /// Oldest first.
    pub transmissions: Vec<TxRecord>,
    /// The spans and bodies behind the transmissions' content matches.
    pub blobs: Blobs,
}

struct Gen<'a> {
    rng: Rng,
    mint: &'a mut crate::mint::Mint,
    times: &'a super::times::Times,
    cast: &'a Cast,
    plan: &'a ChannelPlan,
    topics: &'a super::topics::TopicModel,
    accesses: Vec<Access>,
    transmissions: Vec<TxRecord>,
    blobs: Blobs,
    resource_channel: BTreeMap<ResourceId, ChannelId>,
    lone: (ResourceId, Locator),
    scratch: ChannelId,
}

/// Everything generation reads.
pub struct Inputs<'a> {
    pub seed: u64,
    pub mint: &'a mut crate::mint::Mint,
    pub times: &'a super::times::Times,
    pub cast: &'a Cast,
    pub plan: &'a ChannelPlan,
    pub topics: &'a super::topics::TopicModel,
}

pub fn generate(inputs: Inputs<'_>) -> Result<Traffic, WorldError> {
    let Inputs {
        seed,
        mint,
        times,
        cast,
        plan,
        topics,
    } = inputs;
    let lone: ResourceId = mint.at(times.lone_from)?;
    let scratch: ChannelId = mint.at(times.lone_from)?;
    let mut g = Gen {
        rng: Rng::fork(seed, "traffic"),
        mint,
        times,
        cast,
        plan,
        topics,
        accesses: Vec::new(),
        transmissions: Vec::new(),
        blobs: Blobs::default(),
        resource_channel: BTreeMap::new(),
        lone: (lone, lone_locator()),
        scratch,
    };
    for _ in 0..TRANSMISSIONS {
        g.one(None)?;
    }
    for i in 0..BURST {
        g.one(Some(i))?;
    }
    g.background()?;
    Ok(g.finish())
}

/// Activity weight of an instant: a daytime peak around 14:00 UTC and
/// quieter weekends.
fn activity(at: Timestamp) -> f64 {
    let micros = at.as_micros();
    let hour = ((micros / HOUR) % 24) as f64;
    let weekday = (micros / DAY + 4) % 7; // 0 = Sunday
    let day = if weekday == 0 || weekday == 6 {
        0.45
    } else {
        1.0
    };
    (0.15 + 0.85 * (-(hour - 14.0).powi(2) / 32.0).exp()) * day
}

impl Gen<'_> {
    /// A time in `[from, until)`, following the activity curve.
    fn diurnal(&mut self, from: Timestamp, until: Timestamp) -> Timestamp {
        let span = until.as_micros().saturating_sub(from.as_micros()).max(1);
        let mut at = from;
        for _ in 0..64 {
            at = plus(from, self.rng.below(span));
            if self.rng.chance(activity(at)) {
                break;
            }
        }
        at
    }

    /// A time in the last quarter hour, for the in-flight burst.
    fn recent(&mut self) -> Timestamp {
        minus(self.times.now, self.rng.between(MINUTE, 14 * MINUTE))
    }

    fn when(&mut self, burst: Option<usize>) -> Timestamp {
        match burst {
            Some(_) => self.recent(),
            None => self.diurnal(self.times.start, self.times.now),
        }
    }

    fn theme(&mut self, themes: &[(Theme, f64)]) -> Theme {
        let weights: Vec<f64> = themes.iter().map(|(_, w)| *w).collect();
        self.rng
            .weighted(&weights)
            .and_then(|i| themes.get(i))
            .map_or(Theme::Meetings, |(t, _)| *t)
    }

    fn pick_active(
        &mut self,
        pool: &[AgentId],
        at: Timestamp,
        not: Option<AgentId>,
    ) -> Option<AgentId> {
        let candidates: Vec<AgentId> = pool
            .iter()
            .copied()
            .filter(|a| self.cast.active_at(*a, at) && Some(*a) != not)
            .collect();
        self.rng.pick(&candidates).copied()
    }

    fn tool_call(&mut self) -> ToolCallId {
        ToolCallId(format!("toolu_{:016x}", self.rng.next_u64()))
    }

    fn one(&mut self, burst: Option<usize>) -> Result<(), WorldError> {
        match self.rng.weighted(&[0.6, 0.2, 0.15, 0.05]) {
            Some(0) => self.channel_tx(burst),
            Some(1) => self.delegation_tx(burst),
            Some(2) => self.direct_tx(burst),
            _ => self.unobserved_tx(burst),
        }
    }

    fn push(&mut self, planned: Planned) -> Result<(), WorldError> {
        let mut builder = Builder {
            rng: &mut self.rng,
            mint: self.mint,
            times: self.times,
            topics: self.topics,
            blobs: &mut self.blobs,
        };
        let record = states::build(&mut builder, planned)?;
        self.transmissions.push(record);
        Ok(())
    }

    /// Records an access of `resource`, which `channel`'s traffic uses
    /// (`None` for a resource no channel's traffic uses).
    fn access(
        &mut self,
        agent: AgentId,
        resource: (ResourceId, &Locator),
        channel: Option<ChannelId>,
        at: Timestamp,
        kind: AccessKind,
    ) -> Result<Access, WorldError> {
        let via = match resource.1 {
            Locator::Url { .. } if self.rng.chance(0.3) => Extraction::Parsed,
            Locator::Url { .. } => Extraction::Scanned,
            Locator::Opaque { .. } => Extraction::Parsed,
            Locator::File { .. } | Locator::Mcp { .. } => Extraction::Structured,
        };
        let op = match kind {
            AccessKind::Write => AccessOp::Write {
                call: evidence::part(&mut self.rng),
                spans: vec![self.mint.at::<SpanId>(at)?],
            },
            AccessKind::Read => AccessOp::Read {
                result: evidence::part(&mut self.rng),
            },
        };
        let access = Access {
            id: self.mint.at(at)?,
            agent,
            exchange: self.mint.at(at)?,
            resource: resource.0,
            at,
            via,
            op,
        };
        if let Some(channel) = channel {
            self.resource_channel.insert(resource.0, channel);
        }
        self.accesses.push(access.clone());
        Ok(access)
    }

    /// A write by one of the channel's writers, a later read by one of its
    /// readers, and the transmission the pair opens. Suspected ones
    /// sometimes have a second earlier write.
    fn channel_tx(&mut self, burst: Option<usize>) -> Result<(), WorldError> {
        let plan = self.plan;
        let weights: Vec<f64> = plan.specs.iter().map(|s| s.weight).collect();
        let Some(spec) = self.rng.weighted(&weights).and_then(|i| plan.specs.get(i)) else {
            return Ok(());
        };
        let at = match burst {
            Some(_) if spec.until >= self.times.now => self.recent(),
            _ => self.diurnal(spec.from, spec.until),
        };
        let earliest = plus(spec.from, SECOND);
        let Some(reader) = self.pick_active(&spec.readers, at, None) else {
            return Ok(());
        };
        let Some(writer) = self.pick_active(&spec.writers, at, Some(reader)) else {
            return Ok(());
        };
        let Some((rid, locator)) = self.rng.pick(&spec.resources).cloned() else {
            return Ok(());
        };
        if at <= earliest {
            return Ok(());
        }
        let resource = (rid, &locator);
        let write_at = minus(at, self.rng.between(2 * MINUTE, 20 * HOUR)).max(earliest);
        let write = self.access(writer, resource, Some(spec.id), write_at, AccessKind::Write)?;
        let read = self.access(reader, resource, Some(spec.id), at, AccessKind::Read)?;
        let co_access = |w: &Access| {
            CoAccess::new(w, &read, CORRELATION_WINDOW)
                .map_err(|e| WorldError::invalid("CoAccess", e))
        };
        let mut co = vec![co_access(&write)?];
        let mut accesses = vec![write.id, read.id];
        let want = states::want_channel(&mut self.rng, self.times, at, spec.confirms, burst);
        if matches!(want, Want::Suspected | Want::Discarded)
            && self.rng.chance(0.25)
            && let Some(second) = self.pick_active(&spec.writers, write_at, Some(reader))
        {
            let second_at = minus(write_at, self.rng.between(MINUTE, 2 * HOUR)).max(earliest);
            if second_at < write_at {
                let w2 = self.access(
                    second,
                    resource,
                    Some(spec.id),
                    second_at,
                    AccessKind::Write,
                )?;
                co.push(co_access(&w2)?);
                accesses.push(w2.id);
            }
        }
        if want == Want::Detected {
            co.clear();
            accesses.clear();
        }
        let theme = self.theme(&spec.themes);
        let carrier = Carrier::ToolResult(self.tool_call());
        self.push(Planned {
            at,
            from: writer,
            to: reader,
            route: Route::Channel(spec.id),
            theme,
            co,
            accesses,
            exchange: read.exchange,
            carrier,
            want,
        })
    }

    fn delegation_tx(&mut self, burst: Option<usize>) -> Result<(), WorldError> {
        let at = self.when(burst);
        let pairs: Vec<(AgentId, AgentId)> = self
            .cast
            .delegations
            .iter()
            .copied()
            .filter(|(p, c)| self.cast.active_at(*p, at) && self.cast.active_at(*c, at))
            .collect();
        let Some((parent, child)) = self.rng.pick(&pairs).copied() else {
            return Ok(());
        };
        let (from, to, direction, carrier) = if self.rng.chance(0.5) {
            let direction = DelegationDirection::ParentToChild;
            (parent, child, direction, Carrier::UserTurn)
        } else {
            let direction = DelegationDirection::ChildToParent;
            (
                child,
                parent,
                direction,
                Carrier::ToolResult(self.tool_call()),
            )
        };
        let theme = self.theme(DELEGATION_THEMES);
        self.non_channel(
            at,
            (from, to),
            Route::Delegation(direction),
            theme,
            carrier,
            burst,
        )
    }

    fn direct_tx(&mut self, burst: Option<usize>) -> Result<(), WorldError> {
        let at = self.when(burst);
        let Some((from, to, kind)) = self.rng.pick(DIRECT) else {
            return Ok(());
        };
        let (from, to) = (self.cast.id(from)?, self.cast.id(to)?);
        if !self.cast.active_at(from, at) || !self.cast.active_at(to, at) {
            return Ok(());
        }
        let (route, carrier) = match kind {
            DirectKind::User => (DirectCarrier::UserTurn, Carrier::UserTurn),
            DirectKind::System => (DirectCarrier::SystemPrompt, Carrier::SystemPrompt),
            DirectKind::Tool(name) => (
                DirectCarrier::ToolResult(ToolName((*name).to_owned())),
                Carrier::ToolResult(self.tool_call()),
            ),
        };
        let theme = self.theme(DIRECT_THEMES);
        self.non_channel(at, (from, to), Route::Direct(route), theme, carrier, burst)
    }

    fn unobserved_tx(&mut self, burst: Option<usize>) -> Result<(), WorldError> {
        let at = self.when(burst);
        let Some((from, to)) = self.rng.pick(UNOBSERVED) else {
            return Ok(());
        };
        let (from, to) = (self.cast.id(from)?, self.cast.id(to)?);
        let theme = self.theme(UNOBSERVED_THEMES);
        let carrier = Carrier::ReaderOutput;
        self.non_channel(at, (from, to), Route::Unobserved, theme, carrier, burst)
    }

    fn non_channel(
        &mut self,
        at: Timestamp,
        (from, to): (AgentId, AgentId),
        route: Route,
        theme: Theme,
        carrier: Carrier,
        burst: Option<usize>,
    ) -> Result<(), WorldError> {
        let exchange: ExchangeId = self.mint.at(at)?;
        let want = states::want_direct(&mut self.rng, self.times, at, burst);
        self.push(Planned {
            at,
            from,
            to,
            route,
            theme,
            co: Vec::new(),
            accesses: Vec::new(),
            exchange,
            carrier,
            want,
        })
    }

    /// Accesses that carried nothing: reads and writes by each channel's
    /// agents, and the key-value entry only one agent uses.
    fn background(&mut self) -> Result<(), WorldError> {
        let plan = self.plan;
        for spec in &plan.specs {
            let count = (spec.weight * 40.0) as usize;
            for _ in 0..count {
                let at = self.diurnal(spec.from, spec.until);
                let kind = if self.rng.chance(0.4) {
                    AccessKind::Write
                } else {
                    AccessKind::Read
                };
                let pool = match kind {
                    AccessKind::Write => &spec.writers,
                    AccessKind::Read => &spec.readers,
                };
                let Some(agent) = self.pick_active(pool, at, None) else {
                    continue;
                };
                let Some((rid, locator)) = self.rng.pick(&spec.resources).cloned() else {
                    continue;
                };
                self.access(agent, (rid, &locator), Some(spec.id), at, kind)?;
            }
        }
        self.lone()
    }

    /// `cc7` writing and reading its key-value scratch entry: one agent, so
    /// never a co-access and never a transmission.
    fn lone(&mut self) -> Result<(), WorldError> {
        let agent = self.cast.id(LONE_AGENT)?;
        let (id, locator) = self.lone.clone();
        let (from, now) = (self.times.lone_from, self.times.now);
        for _ in 0..40 {
            let at = self.diurnal(from, now);
            let kind = if self.rng.chance(0.4) {
                AccessKind::Write
            } else {
                AccessKind::Read
            };
            self.access(agent, (id, &locator), None, at, kind)?;
        }
        Ok(())
    }

    /// Sorts everything by time and fixes each resource's first sighting.
    fn finish(mut self) -> Traffic {
        self.accesses.sort_by_key(|a| (a.at, a.id));
        self.transmissions
            .sort_by_key(|t| (t.transmission.opened_at, t.transmission.id));
        let mut first_seen: BTreeMap<ResourceId, Timestamp> = BTreeMap::new();
        for access in &self.accesses {
            first_seen.entry(access.resource).or_insert(access.at);
        }
        let mut resources = Vec::new();
        for spec in &self.plan.specs {
            for (id, locator) in &spec.resources {
                // A resource nobody accessed is never seen: the registry
                // stores resources as traffic reveals them.
                let Some(seen) = first_seen.get(id) else {
                    continue;
                };
                resources.push(Resource {
                    id: *id,
                    locator: locator.clone(),
                    first_seen: *seen,
                });
            }
        }
        let (lone, lone_locator) = self.lone;
        if let Some(seen) = first_seen.get(&lone) {
            resources.push(Resource {
                id: lone,
                locator: lone_locator,
                first_seen: *seen,
            });
        }
        Traffic {
            resources,
            resource_channel: self.resource_channel,
            lone,
            scratch: self.scratch,
            accesses: self.accesses,
            transmissions: self.transmissions,
            blobs: self.blobs,
        }
    }
}

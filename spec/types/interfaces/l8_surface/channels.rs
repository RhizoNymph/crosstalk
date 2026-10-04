//! Channel read models: the rows of `QueryApi::channels` (also the head of
//! a channel page, `QueryApi::channel`), channel names in batches
//! (`QueryApi::channel_names`), and what promoting a channel would do
//! (`QueryApi::promotion_preview`).
//!
//! **Rows.** A [`ChannelRow`] is the stored channel, its seed resource, and
//! its standing: in force with its activity, or superseded with who
//! superseded it into which channel. Activity belongs to the channel in
//! force: accesses to a superseded channel's resources and transmissions
//! routed through it resolve to its superseding channel at read time and are
//! counted there. A superseded row therefore carries no counts and no last
//! activity, so summing a list's counts never counts an access twice; its
//! supersession names the row that does carry them.
//!
//! ```text
//! in force:   ChannelActivity::Seen { last, counts } over the channel and every channel it superseded
//!             counts = ChannelCounts::tally(full channel_resources(channel, window),
//!                                           ChannelCounts::routed(graph(window, default filter))[channel])
//! superseded: SupersededInto { into, by, at }, no counts
//! ```
//!
//! **Rows and the overview.** A row's `transmissions` is what the topology
//! graph counts on the channel for the same window under
//! `TopologyFilter::default()` ([`ChannelCounts::routed`]), as an agent
//! row's traffic is its node's counts in that graph. The overview's
//! `active_channels` (`EdgeTotals::of` the graph for its window and
//! filter) counts the channels with a non-zero entry there, so under the
//! default filter and the same window it is the number of rows in force
//! whose `transmissions` is non-zero. "Active" in the overview is that
//! count; a row's [`ChannelActivity::Seen`] is wider: any access or
//! confirmation ever, so a channel written to and never read is `Seen` but
//! not active.
//!
//! **Names.** [`ChannelName`] is what the UI shows for a channel id: the
//! channel in force and its pattern (declared) or seed locator (discovered).
//! [`resolve_names`] is the reference for `channel_names`, which takes an
//! [`IdBatch`] like `agent_names`, so the batch's bound is the type's.
//!
//! **Promotion preview.** [`PromotionPreview::from_registry`] turns the
//! registry's [`promotion::coverage`] into what the operator sees, mapping a
//! refusal exactly as `PromoteChannel` does (`ActionError::from`): a
//! conflict is an answer (`conflict()`), anything else an error. Covered and
//! uncovered resources are [`CappedResources`]s: the newest
//! `COVERAGE_CAP` (200) of each with exact totals, while the superseded
//! channels are always complete.
//!
//! [`promotion::coverage`]: crate::derived::flow::channel::promotion::coverage

use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};

use crate::aggregates::access::ResourceUse;
use crate::aggregates::edge::TopologyGraph;
use crate::batch::IdBatch;
use crate::derived::flow::channel::policy::PolicyAuthor;
use crate::derived::flow::channel::promotion::{CappedResources, PromotionCoverage, Registered};
use crate::derived::flow::channel::{Channel, ChannelOrigin, DeclaredHistory, Supersession};
use crate::derived::flow::resource::{Locator, Resource, ResourcePattern};
use crate::derived::flow::transmission::Route;
use crate::ids::{ChannelId, OperatorId};
use crate::interfaces::l5_flow::PromoteError;
use crate::support::Timestamp;
use crate::wire::Rejected;

use super::actions::SupersededChannels;
use super::{ActionError, ConflictKind, QueryError};

/// One channel as the channel list and the channel page show it.
///
/// Built only through [`ChannelRow::new`], which checks that:
/// - `seed` is the channel's seed resource, present exactly when the
///   channel has a seed ([`ChannelOrigin::seed`]);
/// - the standing is superseded exactly when the channel is, with the
///   channel's own supersession (`into` its superseding channel, `at` its
///   time);
/// - a channel whose detection shows traffic is not listed as never active.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", try_from = "RawChannelRow")]
pub struct ChannelRow {
    channel: Channel,
    seed: Option<Resource>,
    standing: ChannelStanding,
}

/// Whether a channel is in force, with its activity, or superseded, with
/// no activity of its own.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "data",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum ChannelStanding {
    InForce(ChannelActivity),
    Superseded(SupersededInto),
}

/// The activity of a channel in force: its own and that of every channel it
/// superseded.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "data",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum ChannelActivity {
    /// No access to any of its resources and no transmission routed
    /// through it, ever: a channel declared before traffic that has seen
    /// none.
    Never,
    Seen {
        /// The latest `Access::at` of any of its resources or
        /// `Confirmed::at` of a transmission routed through it, over all
        /// time, whatever the filter's window.
        last: Timestamp,
        /// Counted in the filter's window (all time when it has none).
        counts: ChannelCounts,
    },
}

/// What a channel in force carried within a window.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct ChannelCounts {
    /// Distinct canonical agents that wrote any of its resources.
    pub writers: u64,
    /// Distinct canonical agents that read any of its resources.
    pub readers: u64,
    /// The transmissions the topology graph counts on the channel in the
    /// window under `TopologyFilter::default()`: confirmed, routed through
    /// it or a channel it superseded, by `Confirmed::at`, under the active
    /// topic version, false detections included (the list has no verdict
    /// choice), and none whose sender and reader resolve to one agent
    /// ([`ChannelCounts::routed`]).
    pub transmissions: u64,
}

impl ChannelCounts {
    /// The reference definition: writers and readers are the distinct
    /// agents across every [`ResourceUse`] of a full `channel_resources`
    /// traversal of the channel and window (whose agents are already
    /// canonical, aliases summed); `transmissions` is passed through, and is
    /// the channel's entry of [`ChannelCounts::routed`] (zero when absent).
    pub fn tally<'a>(uses: impl IntoIterator<Item = &'a ResourceUse>, transmissions: u64) -> Self {
        let mut writers = HashSet::new();
        let mut readers = HashSet::new();
        for resource in uses {
            writers.extend(resource.writers().iter().map(|entry| entry.agent));
            readers.extend(resource.readers().iter().map(|entry| entry.agent));
        }
        Self {
            writers: count(writers.len()),
            readers: count(readers.len()),
            transmissions,
        }
    }

    /// The transmissions each channel carried in `graph`: for every
    /// channel some edge's route names (`Route::Channel`, already
    /// canonical), the sum of those edges' transmissions. Rows read it from
    /// `EdgeStore::graph` for their window under `TopologyFilter::default()`.
    /// Its keys are exactly the channels [`EdgeTotals::of`] counts as active
    /// in the same graph, so the overview and the rows agree.
    ///
    /// [`EdgeTotals::of`]: crate::aggregates::edge::EdgeTotals::of
    pub fn routed(graph: &TopologyGraph) -> HashMap<ChannelId, u64> {
        let mut routed: HashMap<ChannelId, u64> = HashMap::new();
        for edge in &graph.edges {
            if let Route::Channel(channel) = edge.route {
                let sum = routed.entry(channel).or_default();
                *sum = sum.saturating_add(edge.stats.transmissions.get());
            }
        }
        routed
    }
}

fn count(n: usize) -> u64 {
    u64::try_from(n).unwrap_or(u64::MAX)
}

/// A superseded channel's supersession as a row shows it: the channel in
/// force it resolves to, the operator whose promotion superseded it, and
/// when.
///
/// Built only through [`SupersededInto::of`], which takes the operator from
/// the superseding channel's declaration, so it cannot name anyone but the
/// promoting operator.
///
/// A response, never a request: `by` and `at` are the promotion's stamps.
/// Decoding cannot rerun [`SupersededInto::of`], which reads the
/// superseding channel; `ChannelRow::new` checks `into` and `at` against
/// the row's own channel.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct SupersededInto {
    into: ChannelId,
    by: OperatorId,
    at: Timestamp,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidSupersededInto {
    /// `superseding` is not the channel the supersession names.
    WrongChannel { expected: ChannelId, got: ChannelId },
    /// The superseding channel is not a promoted channel.
    NotPromoted,
    /// The superseding channel's declaration was not made by an operator.
    /// `Promotion::new` always authors one, so this is a store fault.
    NotByOperator,
    /// The supersession time is not the superseding declaration's time.
    TimeMismatch,
}

impl SupersededInto {
    pub fn of(
        supersession: Supersession,
        superseding: &Channel,
    ) -> Result<Self, InvalidSupersededInto> {
        if superseding.id != supersession.by {
            return Err(InvalidSupersededInto::WrongChannel {
                expected: supersession.by,
                got: superseding.id,
            });
        }
        let ChannelOrigin::Declared {
            declaration,
            history: DeclaredHistory::Promoted { .. },
        } = &superseding.origin
        else {
            return Err(InvalidSupersededInto::NotPromoted);
        };
        let PolicyAuthor::Operator(by) = declaration.by else {
            return Err(InvalidSupersededInto::NotByOperator);
        };
        if declaration.at != supersession.at {
            return Err(InvalidSupersededInto::TimeMismatch);
        }
        Ok(Self {
            into: supersession.by,
            by,
            at: supersession.at,
        })
    }

    /// The channel in force this one resolves to.
    pub fn into(self) -> ChannelId {
        self.into
    }

    /// The operator who promoted `into`.
    pub fn by(self) -> OperatorId {
        self.by
    }

    pub fn at(self) -> Timestamp {
        self.at
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidChannelRow {
    /// The seed resource is missing, present for a channel without a seed,
    /// or not the channel's seed resource.
    SeedMismatch,
    /// A superseded standing for a channel in force or the reverse, or a
    /// supersession that is not the channel's own.
    StandingMismatch,
    /// A channel whose detection shows traffic, listed as never active.
    TrafficWithoutActivity,
}

/// [`ChannelRow`]'s fields, decoded without the checks. Decoding goes
/// through [`ChannelRow::new`].
#[derive(Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
struct RawChannelRow {
    channel: Channel,
    seed: Option<Resource>,
    standing: ChannelStanding,
}

impl TryFrom<RawChannelRow> for ChannelRow {
    type Error = Rejected<InvalidChannelRow>;

    fn try_from(raw: RawChannelRow) -> Result<Self, Self::Error> {
        Self::new(raw.channel, raw.seed, raw.standing)
            .map_err(|error| Rejected::new("channel row", error))
    }
}

impl ChannelRow {
    pub fn new(
        channel: Channel,
        seed: Option<Resource>,
        standing: ChannelStanding,
    ) -> Result<Self, InvalidChannelRow> {
        let expected_seed = channel.origin.seed().map(|seed| seed.resource);
        if expected_seed != seed.as_ref().map(|resource| resource.id) {
            return Err(InvalidChannelRow::SeedMismatch);
        }
        match (channel.origin.supersession(), standing) {
            (Some(own), ChannelStanding::Superseded(shown))
                if shown.into == own.by && shown.at == own.at => {}
            (None, ChannelStanding::InForce(activity)) => {
                if channel.origin.traffic().is_some() && activity == ChannelActivity::Never {
                    return Err(InvalidChannelRow::TrafficWithoutActivity);
                }
            }
            (Some(_), _) | (None, ChannelStanding::Superseded(_)) => {
                return Err(InvalidChannelRow::StandingMismatch);
            }
        }
        Ok(Self {
            channel,
            seed,
            standing,
        })
    }

    /// The stored channel, as recorded under its own id.
    pub fn channel(&self) -> &Channel {
        &self.channel
    }

    /// The resource the channel was discovered from; `None` only for a
    /// channel declared before traffic.
    pub fn seed(&self) -> Option<&Resource> {
        self.seed.as_ref()
    }

    pub fn standing(&self) -> ChannelStanding {
        self.standing
    }

    /// `None` for a channel in force.
    pub fn supersession(&self) -> Option<SupersededInto> {
        match self.standing {
            ChannelStanding::Superseded(supersession) => Some(supersession),
            ChannelStanding::InForce(_) => None,
        }
    }

    /// `None` for a superseded channel (its activity is its superseding
    /// channel's) and for a channel never active.
    pub fn counts(&self) -> Option<ChannelCounts> {
        match self.standing {
            ChannelStanding::InForce(ChannelActivity::Seen { counts, .. }) => Some(counts),
            ChannelStanding::InForce(ChannelActivity::Never) | ChannelStanding::Superseded(_) => {
                None
            }
        }
    }

    /// `None` for a superseded channel and for a channel never active.
    pub fn last_activity(&self) -> Option<Timestamp> {
        match self.standing {
            ChannelStanding::InForce(ChannelActivity::Seen { last, .. }) => Some(last),
            ChannelStanding::InForce(ChannelActivity::Never) | ChannelStanding::Superseded(_) => {
                None
            }
        }
    }
}

/// What a channel looks like where it is named.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "data",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum ChannelShape {
    /// A declared channel's pattern (declared before traffic, or promoted).
    Pattern(ResourcePattern),
    /// A discovered channel's seed locator.
    Seed(Locator),
}

/// The name of the channel in force for an id: what `channel_names`
/// returns for each id it knows.
///
/// Built only through [`ChannelName::of`], from a channel in force, so `id`
/// is never superseded. Decoding cannot rerun [`ChannelName::of`], which
/// reads the registry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct ChannelName {
    id: ChannelId,
    shape: ChannelShape,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidChannelName {
    /// The channel is superseded; name the channel it resolves to.
    Superseded { channel: ChannelId, by: ChannelId },
    /// A discovered channel whose seed locator could not be read.
    SeedUnreadable(ChannelId),
}

impl ChannelName {
    /// The name of a channel in force: its pattern when declared, its seed
    /// locator when discovered.
    pub fn of(entry: &Registered<'_>) -> Result<Self, InvalidChannelName> {
        let id = entry.channel.id;
        let shape = match &entry.channel.origin {
            ChannelOrigin::Superseded { supersession, .. } => {
                return Err(InvalidChannelName::Superseded {
                    channel: id,
                    by: supersession.by,
                });
            }
            ChannelOrigin::Declared { declaration, .. } => {
                ChannelShape::Pattern(declaration.pattern.clone())
            }
            ChannelOrigin::Discovered { .. } => ChannelShape::Seed(
                entry
                    .seed
                    .cloned()
                    .ok_or(InvalidChannelName::SeedUnreadable(id))?,
            ),
        };
        Ok(Self { id, shape })
    }

    /// The channel in force.
    pub fn id(&self) -> ChannelId {
        self.id
    }

    pub fn shape(&self) -> &ChannelShape {
        &self.shape
    }
}

/// The reference for `QueryApi::channel_names` over the registered
/// channels: for each id of the batch that the registry knows, keyed by
/// that id, the [`ChannelName`] of the channel it resolves to
/// (`Channel::canonical`, what `ChannelDirectory::canonical` returns).
/// Unknown ids are left out. The batch holds each id once and at most
/// [`IdBatch::MAX`] of them, so no batch is refused here. A known id whose
/// channel in force is missing or unnamable is a store fault.
pub fn resolve_names(
    ids: &IdBatch<ChannelId>,
    registry: &[Registered<'_>],
) -> Result<HashMap<ChannelId, ChannelName>, QueryError> {
    let entry = |id: ChannelId| registry.iter().find(|entry| entry.channel.id == id);
    let mut names = HashMap::new();
    for &id in ids.ids() {
        let Some(asked) = entry(id) else { continue };
        let in_force = asked.channel.canonical();
        let current = entry(in_force).ok_or_else(|| QueryError::Store {
            reason: format!("channel {id:?} resolves to unknown channel {in_force:?}"),
        })?;
        let name = ChannelName::of(current).map_err(|error| QueryError::Store {
            reason: format!("channel {in_force:?} has no name: {error:?}"),
        })?;
        names.insert(id, name);
    }
    Ok(names)
}

/// What `PromoteChannel` would do now with a channel and a pattern.
///
/// Built only through [`PromotionPreview::from_registry`], so it either
/// carries the registry's [`PromotionCoverage`] or the conflict a promotion
/// in the same state would be refused with, never both.
///
/// On the wire, `{"type": "promotes", "data": <PromotionCoverage>}` or
/// `{"type": "refused", "data": <ConflictKind>}`. A response, never a
/// request. Decoding cannot rerun [`PromotionPreview::from_registry`], which
/// takes the registry's answer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PromotionPreview(Outcome);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "data",
    rename_all = "snake_case",
    deny_unknown_fields
)]
enum Outcome {
    Promotes(PromotionCoverage),
    Refused(ConflictKind),
}

impl PromotionPreview {
    /// The preview of `ChannelRegistry::promotion_coverage`'s result. Its
    /// error goes through the same `ActionError::from` as `PromoteChannel`'s:
    /// a `Conflict` (superseded, not discovered, overlapping pattern) is a
    /// preview with that conflict; any other (`NotFound` for an unknown
    /// channel, `InvalidInput(PatternMissesSeed)`, `Store`) is returned as
    /// the query's error.
    pub fn from_registry(
        result: Result<PromotionCoverage, PromoteError>,
    ) -> Result<Self, QueryError> {
        match result {
            Ok(coverage) => Ok(Self(Outcome::Promotes(coverage))),
            Err(error) => match ActionError::from(error) {
                ActionError::Conflict(kind) => Ok(Self(Outcome::Refused(kind))),
                other => Err(other.into()),
            },
        }
    }

    /// Why `PromoteChannel` would be refused now; `None` when it would
    /// succeed.
    pub fn conflict(&self) -> Option<&ConflictKind> {
        match &self.0 {
            Outcome::Refused(kind) => Some(kind),
            Outcome::Promotes(_) => None,
        }
    }

    /// The resources the declared channel would hold that its pattern
    /// matches: the newest [`COVERAGE_CAP`] and the exact total. `None`
    /// when refused: a refused promotion covers nothing, which an empty
    /// sample would misstate as a pattern that matches nothing.
    ///
    /// [`COVERAGE_CAP`]: crate::derived::flow::channel::promotion::COVERAGE_CAP
    pub fn covered_resources(&self) -> Option<&CappedResources> {
        match &self.0 {
            Outcome::Promotes(coverage) => Some(coverage.covered()),
            Outcome::Refused(_) => None,
        }
    }

    /// The resources of the channel and of the channels it would supersede
    /// that the pattern does not match, sampled the same way. `None` when
    /// refused.
    pub fn uncovered_resources(&self) -> Option<&CappedResources> {
        match &self.0 {
            Outcome::Promotes(coverage) => Some(coverage.uncovered()),
            Outcome::Refused(_) => None,
        }
    }

    /// The other channels the promotion would supersede, complete (the
    /// action records every one), as `ActionOutcome::ChannelPromoted` would
    /// report them. Empty when refused.
    pub fn superseded_channels(&self) -> SupersededChannels {
        match &self.0 {
            Outcome::Promotes(coverage) => {
                SupersededChannels::new(coverage.superseded().iter().copied())
            }
            Outcome::Refused(_) => SupersededChannels::default(),
        }
    }
}

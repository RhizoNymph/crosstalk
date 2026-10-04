//! Promotion: an operator turns a discovered channel into a declared one,
//! sets its policy, and absorbs the discovered channels its pattern covers.
//!
//! ```text
//! OperatorAction::PromoteChannel { channel, pattern, policy, note }
//!   ─(surface stamps operator, time)─▶ Promotion
//!   ─▶ ChannelRegistry::promote: plan, then in one transaction:
//!        the channel's origin  := promoted (same id)
//!        its policy history    += the operator's decision
//!        each covered channel  := Superseded { by: channel, at }
//!   ─▶ DetectEvent::ChannelPromoted
//! ```
//!
//! [`plan`] is the reference for what a promotion does and refuses, and
//! [`coverage`] for what an operator is shown before promoting
//! (`QueryApi::promotion_preview`): it runs the same `plan` and adds which
//! resources the pattern covers, so a preview and a promotion in the same
//! state never disagree. `plan` reads only the declaration (pattern, author,
//! time), never the policy, so a preview needs no policy to ask. Checks
//! run in this order and the first failure is the refusal: the channel is
//! known; it is not superseded; it is discovered (not declared); the pattern
//! matches its seed's locator; the pattern overlaps no other declared
//! channel's pattern. A refused promotion changes nothing.
//!
//! Supersession is decided by seeds. A discovered channel is created by its
//! seed resource, and lookups add later accesses of that resource to it, so
//! its seed is what it stands for. Every other discovered channel whose
//! seed the pattern matches is superseded; already superseded channels are
//! never superseded again (their superseding channel's pattern matches
//! their seed, so the overlap check refuses a pattern that would).

use serde::{Deserialize, Serialize};

use crate::derived::flow::channel::policy::{Decision, PolicyAuthor, PolicyDecision, PolicyKind};
use crate::derived::flow::channel::{
    Channel, ChannelOrigin, Declaration, NotPromotable, Supersession,
};
use std::collections::HashSet;

use crate::derived::flow::resource::{Locator, Resource, ResourcePattern};
use crate::ids::{ChannelId, OperatorId, ResourceId};
use crate::support::{Capped, Timestamp};
use crate::wire::Rejected;

/// One promotion as the surface hands it to flow detection: the pattern and
/// the policy decision, both authored by the calling operator at the time
/// the surface accepted the action.
///
/// Built only through [`Promotion::new`], so the declaration and the policy
/// decision always share one operator and one time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Promotion {
    declaration: Declaration,
    decision: PolicyDecision,
}

impl Promotion {
    pub fn new(
        pattern: ResourcePattern,
        policy: PolicyKind,
        by: OperatorId,
        at: Timestamp,
        note: Option<String>,
    ) -> Self {
        let author = PolicyAuthor::Operator(by);
        Self {
            declaration: Declaration {
                pattern,
                by: author,
                at,
            },
            decision: PolicyDecision {
                kind: policy,
                decision: Decision {
                    by: author,
                    at,
                    note,
                },
            },
        }
    }

    /// The pattern, attached by the operator at the promotion time.
    pub fn declaration(&self) -> &Declaration {
        &self.declaration
    }

    /// The policy decision recorded in the channel's policy history.
    pub fn decision(&self) -> &PolicyDecision {
        &self.decision
    }

    pub fn pattern(&self) -> &ResourcePattern {
        &self.declaration.pattern
    }

    pub fn at(&self) -> Timestamp {
        self.declaration.at
    }
}

/// What promotion reads about one registered channel.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Registered<'a> {
    pub channel: &'a Channel,
    /// The locator of the channel's seed resource. `Some` exactly when the
    /// channel has a seed (`ChannelOrigin::seed`).
    pub seed: Option<&'a Locator>,
}

/// Why a promotion was refused. Each maps to one surface error (see
/// `ActionError::from`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PromotionRefusal {
    UnknownChannel(ChannelId),
    /// The channel was superseded by `by`; promote or edit `by` instead.
    Superseded {
        channel: ChannelId,
        by: ChannelId,
    },
    /// The channel is already declared.
    NotDiscovered(ChannelId),
    /// The pattern does not match the channel's own seed locator (or the
    /// seed's locator could not be read).
    PatternMissesSeed,
    /// The pattern overlaps the pattern of the declared channel `existing`.
    PatternOverlaps {
        existing: ChannelId,
    },
}

/// The changes an accepted promotion makes, applied in one transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PromotionPlan {
    /// The promoted channel's new origin. Its id is unchanged.
    pub origin: ChannelOrigin,
    /// Every discovered channel whose seed the pattern matches, with its
    /// superseded origin, in registry order.
    pub superseded: Vec<(ChannelId, ChannelOrigin)>,
}

impl PromotionPlan {
    pub fn superseded_ids(&self) -> impl Iterator<Item = ChannelId> + '_ {
        self.superseded.iter().map(|(id, _)| *id)
    }
}

/// Plan promoting `target` with `declaration` against every channel in the
/// registry (`target` included). `ChannelRegistry::promote` passes
/// [`Promotion::declaration`]; the promotion's policy decision is recorded
/// separately with `PolicyHistory::record`, in the same transaction.
pub fn plan(
    target: ChannelId,
    declaration: &Declaration,
    registry: &[Registered<'_>],
) -> Result<PromotionPlan, PromotionRefusal> {
    let entry = registry
        .iter()
        .find(|entry| entry.channel.id == target)
        .ok_or(PromotionRefusal::UnknownChannel(target))?;
    let origin =
        entry
            .channel
            .origin
            .promoted(declaration.clone())
            .map_err(|refusal| match refusal {
                NotPromotable::AlreadyDeclared => PromotionRefusal::NotDiscovered(target),
                NotPromotable::Superseded(supersession) => PromotionRefusal::Superseded {
                    channel: target,
                    by: supersession.by,
                },
            })?;
    let pattern = &declaration.pattern;
    if !entry.seed.is_some_and(|seed| pattern.matches(seed)) {
        return Err(PromotionRefusal::PatternMissesSeed);
    }
    let overlapping = registry.iter().find(|other| {
        other.channel.id != target
            && other
                .channel
                .origin
                .pattern()
                .is_some_and(|declared| declared.overlaps(pattern))
    });
    if let Some(other) = overlapping {
        return Err(PromotionRefusal::PatternOverlaps {
            existing: other.channel.id,
        });
    }
    let supersession = Supersession {
        by: target,
        at: declaration.at,
    };
    let superseded = registry
        .iter()
        .filter(|other| other.channel.id != target)
        .filter(|other| other.seed.is_some_and(|seed| pattern.matches(seed)))
        // Only a discovered channel can be superseded. A declared or already
        // superseded one whose seed matched was refused above as an overlap.
        .filter_map(|other| {
            other
                .channel
                .origin
                .superseded(supersession)
                .ok()
                .map(|origin| (other.channel.id, origin))
        })
        .collect();
    Ok(PromotionPlan { origin, superseded })
}

/// How many covered, and how many uncovered, resources a coverage shows.
/// A broad pattern can bring thousands of resources together; the totals
/// are always exact.
pub const COVERAGE_CAP: usize = 200;

/// A coverage's resources: the newest [`COVERAGE_CAP`] and the exact total.
pub type CappedResources = Capped<Resource, COVERAGE_CAP>;

/// What an accepted promotion would take in: the channels [`plan`]
/// supersedes and every resource held by the promoted channel or by one of
/// them, split by whether the pattern matches its locator.
///
/// Built only by [`coverage`], so `superseded` is always the plan's (and
/// complete: it is what the promotion records), and the two resource
/// samples always partition the held resources by the pattern: their
/// totals add up to the number of distinct held resources, and each shows
/// the newest of its side.
/// Uncovered resources are not dropped: they stay stored on their channel
/// (the promoted one, or a superseded one that resolves to it), but no new
/// resource outside the pattern joins.
///
/// A response (inside `PromotionPreview`). Decoding cannot rerun
/// [`coverage`]: it reads the registry, which the value does not hold. It
/// checks what the value can know about itself ([`InvalidCoverage`]): no
/// channel is superseded twice, each sample is newest first with no
/// resource twice, and no resource is shown on both sides.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", try_from = "RawPromotionCoverage")]
pub struct PromotionCoverage {
    superseded: Vec<ChannelId>,
    covered: CappedResources,
    uncovered: CappedResources,
}

/// Why a decoded coverage is not one [`coverage`] could have built.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidCoverage {
    /// `superseded[index]` repeats an earlier channel.
    RepeatedChannel { index: usize },
    /// The covered sample is not in strictly descending id order (newest
    /// first, each resource once).
    CoveredNotNewestFirst,
    /// The same, for the uncovered sample.
    UncoveredNotNewestFirst,
    /// A resource shown as both covered and uncovered.
    CoveredAndUncovered(ResourceId),
}

/// [`PromotionCoverage`]'s fields, decoded without the checks.
#[derive(Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
struct RawPromotionCoverage {
    superseded: Vec<ChannelId>,
    covered: CappedResources,
    uncovered: CappedResources,
}

impl TryFrom<RawPromotionCoverage> for PromotionCoverage {
    type Error = Rejected<InvalidCoverage>;

    fn try_from(raw: RawPromotionCoverage) -> Result<Self, Self::Error> {
        let rejected = |error| Rejected::new("promotion coverage", error);
        let mut channels = HashSet::new();
        if let Some(index) = raw.superseded.iter().position(|id| !channels.insert(*id)) {
            return Err(rejected(InvalidCoverage::RepeatedChannel { index }));
        }
        let newest_first =
            |sample: &CappedResources| sample.shown().windows(2).all(|w| w[0].id > w[1].id);
        if !newest_first(&raw.covered) {
            return Err(rejected(InvalidCoverage::CoveredNotNewestFirst));
        }
        if !newest_first(&raw.uncovered) {
            return Err(rejected(InvalidCoverage::UncoveredNotNewestFirst));
        }
        let covered: HashSet<ResourceId> = raw.covered.shown().iter().map(|r| r.id).collect();
        if let Some(both) = raw
            .uncovered
            .shown()
            .iter()
            .find(|r| covered.contains(&r.id))
        {
            return Err(rejected(InvalidCoverage::CoveredAndUncovered(both.id)));
        }
        Ok(Self {
            superseded: raw.superseded,
            covered: raw.covered,
            uncovered: raw.uncovered,
        })
    }
}

impl PromotionCoverage {
    /// The channels the promotion would supersede, as in
    /// [`PromotionPlan::superseded`] (registry order). Never the promoted
    /// channel.
    pub fn superseded(&self) -> &[ChannelId] {
        &self.superseded
    }

    /// Held resources the pattern matches: the newest (highest id) first,
    /// at most [`COVERAGE_CAP`] of them, and how many there are.
    pub fn covered(&self) -> &CappedResources {
        &self.covered
    }

    /// Held resources the pattern does not match, sampled the same way.
    pub fn uncovered(&self) -> &CappedResources {
        &self.uncovered
    }
}

/// [`plan`], plus the resources the promotion would cover.
///
/// `held(c)` lists every resource stored on channel `c`: its seed resource
/// and `Channel::resources`, whenever seen. The held resources are those of
/// `target` and of every channel the plan supersedes (a discovered target
/// has superseded nothing, so that is every resource the promotion brings
/// together). Each resource counts once, under `covered` when
/// `declaration.pattern` matches its locator and under `uncovered`
/// otherwise; each side shows its newest [`COVERAGE_CAP`] and counts all
/// of them. A refusal is exactly `plan`'s refusal.
pub fn coverage(
    target: ChannelId,
    declaration: &Declaration,
    registry: &[Registered<'_>],
    held: impl Fn(ChannelId) -> Vec<Resource>,
) -> Result<PromotionCoverage, PromotionRefusal> {
    let plan = plan(target, declaration, registry)?;
    let superseded: Vec<ChannelId> = plan.superseded_ids().collect();
    let mut seen = HashSet::new();
    let (mut covered, mut uncovered): (Vec<Resource>, Vec<Resource>) = std::iter::once(target)
        .chain(superseded.iter().copied())
        .flat_map(held)
        .filter(|resource| seen.insert(resource.id))
        .partition(|resource| declaration.pattern.matches(&resource.locator));
    let newest_first = |a: &Resource, b: &Resource| b.id.cmp(&a.id);
    covered.sort_by(newest_first);
    uncovered.sort_by(newest_first);
    Ok(PromotionCoverage {
        superseded,
        covered: Capped::first(covered),
        uncovered: Capped::first(uncovered),
    })
}

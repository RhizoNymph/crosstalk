//! Resources and channels as L5 recorded them.
//!
//! - **Resources.** Each resource is stored at its first access
//!   (`ChannelTraffic::add_resource`): on a declared channel when its
//!   pattern matches, otherwise on no channel. Its accesses are recorded on
//!   it whether it is on a channel or not, and L7 buckets them by resource.
//! - **Discovery.** A discovered channel is created by the first
//!   cross-agent transmission through its seed resource, when that
//!   transmission opens (`ChannelTraffic::discover`), under the id the
//!   plan minted for it. A discovered channel holds exactly its seed.
//!   `cc7`'s scratch entry, which no other agent touches, carries no
//!   transmission, so it stays a resource on no channel.
//! - **Detection.** Recording a channel transmission that opens or is
//!   confirmed keeps its canonical channel `Active` (see
//!   [`super::transmissions`]); a declared channel goes `InUse` at its
//!   first. Only the idle moves are written here: `Dormant` a day after a
//!   dormant channel's last opened or confirmed transmission, and `Unused`
//!   for the declared channel that never saw traffic.
//! - **Policy.** The researcher's decisions, the promotion, and the on-call
//!   operator's refused attempt to sanction the hijacked wiki.

use std::collections::{BTreeMap, BTreeSet};

use crosstalk_spec::derived::flow::channel::detection::TrafficDetection;
use crosstalk_spec::derived::flow::channel::policy::PolicyKind;
use crosstalk_spec::derived::flow::resource::Resource;
use crosstalk_spec::derived::flow::transmission::Route;
use crosstalk_spec::ids::{ChannelId, ResourceId, TransmissionId};
use crosstalk_spec::interfaces::l5_flow::channels::DetectionUpdate;
use crosstalk_spec::support::Timestamp;

use crate::clock::{DAY, minus, plus};
use crate::config::OPERATOR_ONCALL;
use crate::error::WorldError;
use crate::generate::Generated;
use crate::generate::drafts::{Target, operator_decisions, team_notes_promotion};
use crate::generate::states::co_accesses;
use crate::scenario::ChannelKey;
use crate::script::{Op, Script};

pub fn assemble(
    generated: &Generated,
    placement: &Placement,
    script: &mut Script,
) -> Result<(), WorldError> {
    resources_and_accesses(generated, placement, script)?;
    discoveries(placement, script);
    detections(generated, script)?;
    policies(generated, script)
}

/// The promoted channel, the channel it superseded, and when.
pub struct Promotion {
    pub target: ChannelId,
    pub superseded: ChannelId,
    pub at: Timestamp,
}

pub fn promotion(generated: &Generated) -> Result<Promotion, WorldError> {
    Ok(Promotion {
        target: generated.plan.id(ChannelKey::TeamNotes)?,
        superseded: generated.plan.id(ChannelKey::OldTeamNotes)?,
        at: generated.times.promote_at,
    })
}

impl Promotion {
    /// The channel a lookup of `channel`'s resources names at `at`.
    pub fn canonical(&self, channel: ChannelId, at: Timestamp) -> ChannelId {
        if channel == self.superseded && at >= self.at {
            self.target
        } else {
            channel
        }
    }
}

/// What discovered a channel: its seed resource and the first cross-agent
/// transmission through it, which opened at `at`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Seeding {
    pub resource: ResourceId,
    pub transmission: TransmissionId,
    pub at: Timestamp,
}

/// Where each resource is over the week: the discovered channels' seedings,
/// and the promotion that moved the standup page's lookups to the promoted
/// channel.
pub struct Placement {
    resource_channel: BTreeMap<ResourceId, ChannelId>,
    discovered: BTreeMap<ChannelId, Seeding>,
    promotion: Promotion,
}

impl Placement {
    /// Each discovered channel of the plan is seeded by the oldest channel
    /// transmission routed through it that a co-access opened (a write by
    /// one agent, a later read by another). A discovered channel with none
    /// would never exist: a world bug.
    pub fn of(generated: &Generated) -> Result<Self, WorldError> {
        let mut discovered = BTreeMap::new();
        for spec in generated
            .plan
            .specs
            .iter()
            .filter(|s| s.origin.is_discovered())
        {
            let (resource, _) = spec
                .resources
                .first()
                .ok_or_else(|| WorldError::missing(format!("the seed of {:?}", spec.key)))?;
            // Transmissions are sorted by (opened_at, id).
            let first = generated
                .traffic
                .transmissions
                .iter()
                .find(|record| {
                    record.transmission.route == Route::Channel(spec.id)
                        && !co_accesses(&record.transmission.state).is_empty()
                })
                .ok_or_else(|| {
                    WorldError::missing(format!("a cross-agent transmission on {:?}", spec.key))
                })?;
            discovered.insert(
                spec.id,
                Seeding {
                    resource: *resource,
                    transmission: first.id(),
                    at: first.transmission.opened_at,
                },
            );
        }
        Ok(Self {
            resource_channel: generated.traffic.resource_channel.clone(),
            discovered,
            promotion: promotion(generated)?,
        })
    }

    /// Each discovered channel and what seeded it.
    pub fn discovered(&self) -> &BTreeMap<ChannelId, Seeding> {
        &self.discovered
    }

    /// When each discovered channel was created.
    pub fn created(&self) -> BTreeMap<ChannelId, Timestamp> {
        self.discovered
            .iter()
            .map(|(channel, seeding)| (*channel, seeding.at))
            .collect()
    }

    /// The canonical channel `resource` is on at `at`, as a lookup then
    /// names it; `None` while it is on no channel. A discovered channel's
    /// seed is on no channel until the discovery, which follows the access
    /// that opened the discovering transmission.
    pub fn channel_at(&self, resource: ResourceId, at: Timestamp) -> Option<ChannelId> {
        let planned = *self.resource_channel.get(&resource)?;
        if let Some(seeding) = self.discovered.get(&planned)
            && at <= seeding.at
        {
            return None;
        }
        Some(self.promotion.canonical(planned, at))
    }
}

fn resources_and_accesses(
    generated: &Generated,
    placement: &Placement,
    script: &mut Script,
) -> Result<(), WorldError> {
    let traffic = &generated.traffic;
    let resources: BTreeMap<ResourceId, &Resource> =
        traffic.resources.iter().map(|r| (r.id, r)).collect();
    let mut stored: BTreeSet<ResourceId> = BTreeSet::new();
    for access in &traffic.accesses {
        if stored.insert(access.resource) {
            let resource = (*resources
                .get(&access.resource)
                .ok_or_else(|| WorldError::missing(format!("resource {:?}", access.resource)))?)
            .clone();
            let on = placement.channel_at(access.resource, access.at);
            script.push(access.at, Op::AddResource { resource, on });
        }
        script.push(access.at, Op::Access(access.clone()));
    }
    Ok(())
}

/// Each discovery, after the accesses at its instant (the read that opened
/// the discovering transmission among them).
fn discoveries(placement: &Placement, script: &mut Script) {
    for (channel, seeding) in placement.discovered() {
        script.push(
            seeding.at,
            Op::Discover {
                channel: *channel,
                resource: seeding.resource,
                transmission: seeding.transmission,
            },
        );
    }
}

/// The idle moves: `Dormant` a day after a dormant channel's last
/// transmission that opened or was confirmed (what keeps a channel active),
/// `Unused` for the declared channel that never saw traffic.
fn detections(generated: &Generated, script: &mut Script) -> Result<(), WorldError> {
    let promotion = promotion(generated)?;
    let mut last: BTreeMap<ChannelId, (Timestamp, TransmissionId)> = BTreeMap::new();
    for record in &generated.traffic.transmissions {
        let Route::Channel(routed) = record.transmission.route else {
            continue;
        };
        let opened = (!co_accesses(&record.transmission.state).is_empty())
            .then_some(record.transmission.opened_at);
        let confirmed = record.confirmed().map(|c| c.at());
        // A superseded channel's detection is frozen at its supersession;
        // later traffic moves its superseding channel's.
        for at in opened.into_iter().chain(confirmed) {
            let entry = last
                .entry(promotion.canonical(routed, at))
                .or_insert((at, record.id()));
            if at >= entry.0 {
                *entry = (at, record.id());
            }
        }
    }
    for spec in &generated.plan.specs {
        match spec.target {
            Target::Dormant => {
                let (last_at, last) = *last
                    .get(&spec.id)
                    .ok_or_else(|| WorldError::missing(format!("traffic on {:?}", spec.key)))?;
                let since = plus(last_at, DAY);
                if since <= generated.times.now {
                    script.push(
                        since,
                        Op::Detection {
                            channel: spec.id,
                            update: DetectionUpdate::Traffic(TrafficDetection::Dormant {
                                since,
                                last_transmission: last,
                            }),
                        },
                    );
                }
            }
            Target::Unused => {
                let since = minus(generated.times.now, 6 * DAY);
                script.push(
                    since,
                    Op::Detection {
                        channel: spec.id,
                        update: DetectionUpdate::Unused { since },
                    },
                );
            }
            Target::Active | Target::Awaiting => {}
        }
    }
    Ok(())
}

fn policies(generated: &Generated, script: &mut Script) -> Result<(), WorldError> {
    let times = &generated.times;
    for (key, decision) in operator_decisions(times) {
        script.push(
            decision.decision.at,
            Op::Policy {
                channel: generated.plan.id(key)?,
                decision,
            },
        );
    }
    let promotion = team_notes_promotion(times);
    script.push(
        promotion.at(),
        Op::Promote {
            channel: generated.plan.id(ChannelKey::TeamNotes)?,
            promotion: Box::new(promotion),
        },
    );
    script.push(
        minus(times.now, DAY),
        Op::ForbiddenPolicy {
            channel: generated.plan.id(ChannelKey::HijackedWiki)?,
            policy: PolicyKind::Sanctioned,
            note: Some("looks like a normal wiki".to_owned()),
            by: OPERATOR_ONCALL,
        },
    );
    Ok(())
}

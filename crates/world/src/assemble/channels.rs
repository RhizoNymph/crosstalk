//! Channels as L5 recorded them, on today's spec.
//!
//! - **Discovery.** A discovered channel is created at the first access to
//!   any of its resources, with that resource as its seed
//!   (`ChannelTraffic::discover`); its other resources join it at their
//!   first access (`add_resource`), as do a declared channel's. The UI
//!   fixture seeded each discovered channel at its first planned locator
//!   and created it at its first cross-agent transmission: that is the
//!   channel-semantics port's rule, not today's.
//! - **Accesses.** Every access is recorded on its resource and counted
//!   into its bucket under the channel its lookup named: after the
//!   promotion, the superseded standup page's accesses count on the
//!   promoted channel.
//! - **Detection.** `Candidate` at a channel's first co-access when that
//!   comes before its first confirmation; `Active` on each confirmation
//!   (written with the transmission, see [`super::transmissions`]);
//!   `Dormant` a day after a dormant channel's last confirmation; `Unused`
//!   for the declared channel that never saw traffic.
//! - **Policy.** The researcher's decisions, the promotion, and the on-call
//!   operator's refused attempt to sanction the hijacked wiki.

use std::collections::{BTreeMap, BTreeSet};

use crosstalk_spec::derived::flow::channel::detection::TrafficDetection;
use crosstalk_spec::derived::flow::channel::policy::PolicyKind;
use crosstalk_spec::derived::flow::evidence::CoAccess;
use crosstalk_spec::derived::flow::resource::Resource;
use crosstalk_spec::derived::flow::transmission::Route;
use crosstalk_spec::ids::{ChannelId, ResourceId, TransmissionId};
use crosstalk_spec::interfaces::l5_flow::channels::DetectionUpdate;
use crosstalk_spec::support::Timestamp;

use crate::clock::{DAY, minus, plus};
use crate::config::OPERATOR_ONCALL;
use crate::error::WorldError;
use crate::generate::Generated;
use crate::generate::drafts::{DraftOrigin, Target, operator_decisions, team_notes_promotion};
use crate::generate::states::co_accesses;
use crate::scenario::ChannelKey;
use crate::script::{Op, Script};

pub fn assemble(generated: &Generated, script: &mut Script) -> Result<(), WorldError> {
    resources_and_accesses(generated, script)?;
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

/// Whether `channel` is created by discovery (not declared in config).
fn discovered(generated: &Generated, channel: ChannelId) -> bool {
    channel == generated.traffic.scratch
        || generated
            .plan
            .by_id(channel)
            .is_some_and(|spec| spec.origin == DraftOrigin::Discovered)
}

fn resources_and_accesses(generated: &Generated, script: &mut Script) -> Result<(), WorldError> {
    let traffic = &generated.traffic;
    let promotion = promotion(generated)?;
    let resources: BTreeMap<ResourceId, &Resource> =
        traffic.resources.iter().map(|r| (r.id, r)).collect();
    let mut stored: BTreeSet<ResourceId> = BTreeSet::new();
    let mut created: BTreeSet<ChannelId> = BTreeSet::new();
    for access in &traffic.accesses {
        let channel = *traffic
            .resource_channel
            .get(&access.resource)
            .ok_or_else(|| WorldError::missing(format!("channel of {:?}", access.resource)))?;
        if stored.insert(access.resource) {
            let resource = (*resources
                .get(&access.resource)
                .ok_or_else(|| WorldError::missing(format!("resource {:?}", access.resource)))?)
            .clone();
            let op = if discovered(generated, channel) && created.insert(channel) {
                Op::Discover {
                    channel,
                    resource,
                    first_access: access.id,
                }
            } else {
                Op::AddResource {
                    // A resource first seen after the promotion joins the
                    // promoted channel, whose pattern now claims it.
                    channel: promotion.canonical(channel, access.at),
                    resource,
                }
            };
            script.push(access.at, op);
        }
        script.push(
            access.at,
            Op::Access {
                access: access.clone(),
                channel: promotion.canonical(channel, access.at),
            },
        );
    }
    Ok(())
}

/// What moves one channel's detection: its first co-access and its
/// confirmations, in time order.
#[derive(Default)]
struct Moves {
    first_co: Option<(Timestamp, CoAccess)>,
    confirmations: Vec<(Timestamp, TransmissionId)>,
}

fn detections(generated: &Generated, script: &mut Script) -> Result<(), WorldError> {
    let promotion = promotion(generated)?;
    let mut moves: BTreeMap<ChannelId, Moves> = BTreeMap::new();
    for record in &generated.traffic.transmissions {
        let Route::Channel(routed) = record.transmission.route else {
            continue;
        };
        let opened = record.transmission.opened_at;
        // A superseded channel's detection is frozen at its supersession;
        // later traffic moves its superseding channel's.
        if let Some(co) = co_accesses(&record.transmission.state).first() {
            let own = moves
                .entry(promotion.canonical(routed, opened))
                .or_default();
            if own.first_co.is_none_or(|(at, _)| opened < at) {
                own.first_co = Some((opened, *co));
            }
        }
        if let Some(confirmed) = record.confirmed() {
            let at = confirmed.at();
            moves
                .entry(promotion.canonical(routed, at))
                .or_default()
                .confirmations
                .push((at, record.id()));
        }
    }
    for spec in &generated.plan.specs {
        let own = moves.remove(&spec.id).unwrap_or_default();
        let mut confirmations = own.confirmations;
        confirmations.sort();
        let first_confirm = confirmations.first().map(|(at, _)| *at);
        if let Some((at, co)) = own.first_co
            && first_confirm.is_none_or(|confirmed| at < confirmed)
        {
            script.push(
                at,
                Op::Detection {
                    channel: spec.id,
                    update: DetectionUpdate::Traffic(TrafficDetection::Candidate {
                        first_cross_access: co,
                    }),
                },
            );
        }
        match spec.target {
            Target::Dormant => {
                let (last_at, last) = *confirmations
                    .last()
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

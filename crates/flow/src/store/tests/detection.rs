//! Property tests of the registry's detection transitions: the pure
//! functions `record_transmission` and `set_detection` apply to a stored
//! origin (`registry::detection`), over generated origins and transmission
//! states.

use crosstalk_memory::flow::registry::model::{access_agent, pattern};
use crosstalk_memory::flow::verdicts::model::state_between;
use crosstalk_spec::derived::flow::channel::detection::{DeclaredDetection, TrafficDetection};
use crosstalk_spec::derived::flow::channel::policy::PolicyAuthor;
use crosstalk_spec::derived::flow::channel::{
    ChannelOrigin, Declaration, DeclaredHistory, Seed, Supersession,
};
use crosstalk_spec::derived::flow::transmission::{Route, Transmission, TransmissionState};
use crosstalk_spec::ids::{ChannelId, ResourceId, TransmissionId};
use crosstalk_spec::interfaces::l5_flow::channels::{DetectionUpdate, TrafficError};
use crosstalk_spec::support::Timestamp;
use proptest::prelude::*;

use crate::store::registry::{advanced, next_origin};

fn at(micros: u64) -> Timestamp {
    Timestamp::from_micros(micros)
}

fn traffic(active: bool, since: u64, last: u8) -> TrafficDetection {
    let last_transmission = TransmissionId::from_ulid(0x7A00 | u128::from(last));
    if active {
        TrafficDetection::Active {
            since: at(since),
            last_transmission,
        }
    } else {
        TrafficDetection::Dormant {
            since: at(since),
            last_transmission,
        }
    }
}

fn seed() -> Seed {
    Seed {
        resource: ResourceId::from_ulid(0x4E50),
        first_transmission: TransmissionId::from_ulid(0x7A01),
        opened_at: at(1),
    }
}

/// Every origin shape: declared before traffic (awaiting, unused, in use),
/// promoted, discovered and superseded.
fn origin() -> impl Strategy<Value = ChannelOrigin> {
    let declaration = Declaration {
        pattern: pattern(0),
        by: PolicyAuthor::Config,
        at: at(0),
    };
    (0u8..6, any::<bool>(), 0u64..50, 0u8..4).prop_map(move |(shape, active, since, last)| {
        let detection = traffic(active, since, last);
        let declared = |history| ChannelOrigin::Declared {
            declaration: declaration.clone(),
            history,
        };
        match shape {
            0 => declared(DeclaredHistory::BeforeTraffic(
                DeclaredDetection::AwaitingTraffic,
            )),
            1 => declared(DeclaredHistory::BeforeTraffic(DeclaredDetection::Unused {
                since: at(since),
            })),
            2 => declared(DeclaredHistory::BeforeTraffic(DeclaredDetection::InUse(
                detection,
            ))),
            3 => declared(DeclaredHistory::Promoted {
                from: seed(),
                detection,
            }),
            4 => ChannelOrigin::Discovered {
                seed: seed(),
                detection,
            },
            _ => ChannelOrigin::Superseded {
                seed: seed(),
                detection,
                supersession: Supersession {
                    by: ChannelId::from_ulid(0x0C4B),
                    at: at(2),
                },
            },
        }
    })
}

/// A cross-agent transmission in state `n` (every state), opened at
/// `opened`, routed through the channel.
fn transmission() -> impl Strategy<Value = Option<Transmission>> {
    (0u8..7, 0u64..60, 0u8..4).prop_map(|(n, opened, id)| {
        Some(Transmission {
            id: TransmissionId::from_ulid(0x7B00 | u128::from(id)),
            to: access_agent(2),
            route: Route::Channel(ChannelId::from_ulid(0x0C4A)),
            opened_at: at(opened),
            state: state_between(n, &[0], access_agent(1), access_agent(2))?,
        })
    })
}

/// When the recorded state advances detection, by the statement: opened at
/// `opened_at`, confirmed at `Confirmed::at`; never otherwise.
fn expected_at(transmission: &Transmission) -> Option<Timestamp> {
    match &transmission.state {
        TransmissionState::AwaitingContent { .. } => Some(transmission.opened_at),
        state => state.confirmed().map(|confirmed| confirmed.at()),
    }
}

fn declared_detection(origin: &ChannelOrigin) -> Option<&DeclaredDetection> {
    match origin {
        ChannelOrigin::Declared {
            history: DeclaredHistory::BeforeTraffic(detection),
            ..
        } => Some(detection),
        _ => None,
    }
}

proptest! {
    /// INV-1030 `flow.channel.declared-detection-on-cross-agent-transmission`:
    /// a declared channel's detection moves AwaitingTraffic to Unused only
    /// by `set_detection(Unused)`, and AwaitingTraffic or Unused to
    /// InUse(Active since the transmission opened or was confirmed) on a
    /// recorded cross-agent transmission; InUse never goes back; a state
    /// that is not opened or confirmed moves nothing.
    #[test]
    fn declared_detection_transitions(origin in origin(), recorded in transmission(), since in 0u64..60) {
        let id = ChannelId::from_ulid(0x0C4A);
        let Some(before) = declared_detection(&origin).cloned() else {
            return Ok(());
        };
        let Some(recorded) = recorded else {
            return Err(TestCaseError::fail("state fixture"));
        };
        // On a recorded transmission.
        let after = match advanced(&origin, id, &recorded) {
            Ok(next) => next.unwrap_or_else(|| origin.clone()),
            Err(error) => return Err(TestCaseError::fail(format!("refused: {error:?}"))),
        };
        let after = declared_detection(&after).cloned();
        match expected_at(&recorded) {
            None => prop_assert_eq!(after, Some(before.clone())),
            Some(at) => match &before {
                DeclaredDetection::AwaitingTraffic | DeclaredDetection::Unused { .. } => {
                    prop_assert_eq!(after, Some(DeclaredDetection::InUse(TrafficDetection::Active {
                        since: at,
                        last_transmission: recorded.id,
                    })));
                }
                DeclaredDetection::InUse(_) => {
                    let active = matches!(
                        after,
                        Some(DeclaredDetection::InUse(TrafficDetection::Active { .. }))
                    );
                    prop_assert!(active, "InUse stays InUse(Active): {:?}", after);
                }
            },
        }
        // On the idle timeout.
        let unused = next_origin(&origin, id, DetectionUpdate::Unused { since: at(since) });
        match before {
            DeclaredDetection::AwaitingTraffic => prop_assert_eq!(
                unused.ok().as_ref().and_then(declared_detection).cloned(),
                Some(DeclaredDetection::Unused { since: at(since) })
            ),
            DeclaredDetection::Unused { .. } | DeclaredDetection::InUse(_) => {
                prop_assert_eq!(unused, Err(TrafficError::NotAwaitingTraffic(id)));
            }
        }
    }

    /// INV-1031 `flow.channel.traffic-detection-transitions`: a recorded
    /// opened or confirmed cross-agent transmission makes a traffic
    /// detection Active naming it, keeping `since` when it was Active and
    /// otherwise since the transmission opened or was confirmed; a
    /// suspected, discarded or detected state moves nothing; a superseded
    /// channel's detection never changes.
    #[test]
    fn traffic_detection_transitions(origin in origin(), recorded in transmission()) {
        let id = ChannelId::from_ulid(0x0C4A);
        let Some(recorded) = recorded else {
            return Err(TestCaseError::fail("state fixture"));
        };
        if let Some(supersession) = origin.supersession() {
            let superseded = TrafficError::Superseded { channel: id, by: supersession.by };
            if expected_at(&recorded).is_some() {
                prop_assert_eq!(advanced(&origin, id, &recorded), Err(superseded.clone()));
            }
            prop_assert_eq!(
                next_origin(&origin, id, DetectionUpdate::Traffic(traffic(true, 0, 0))),
                Err(superseded)
            );
            return Ok(());
        }
        let Some(before) = origin.traffic().cloned() else {
            return Ok(());
        };
        let after = match advanced(&origin, id, &recorded) {
            Ok(next) => next.unwrap_or_else(|| origin.clone()),
            Err(error) => return Err(TestCaseError::fail(format!("refused: {error:?}"))),
        };
        match expected_at(&recorded) {
            None => prop_assert_eq!(&after, &origin),
            Some(at) => {
                let since = match before {
                    TrafficDetection::Active { since, .. } => since,
                    TrafficDetection::Dormant { .. } => at,
                };
                prop_assert_eq!(after.traffic(), Some(&TrafficDetection::Active {
                    since,
                    last_transmission: recorded.id,
                }));
                // Nothing else about the origin changes.
                prop_assert_eq!(after.seed(), origin.seed());
                prop_assert_eq!(after.pattern(), origin.pattern());
                prop_assert_eq!(after.created_at(), origin.created_at());
            }
        }
    }
}

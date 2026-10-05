//! How the registry moves a channel's detection: pure functions of the
//! stored origin, shared by `ChannelTraffic::set_detection` and
//! `record_transmission`.
//!
//! - A cross-agent transmission recorded opened (`AwaitingContent`, at its
//!   `opened_at`) or confirmed (at `Confirmed::at`) makes the canonical
//!   channel `Active` naming it, keeping `since` when it was already
//!   active ([`advanced`]).
//! - `set_detection` replaces a traffic detection, or turns a declared
//!   channel awaiting traffic `Unused` ([`next_origin`]).
//! - A superseded channel's detection is frozen.

use crosstalk_spec::derived::flow::channel::detection::{DeclaredDetection, TrafficDetection};
use crosstalk_spec::derived::flow::channel::{ChannelOrigin, DeclaredHistory};
use crosstalk_spec::derived::flow::transmission::{Transmission, TransmissionState};
use crosstalk_spec::ids::ChannelId;
use crosstalk_spec::interfaces::l5_flow::channels::{DetectionUpdate, TrafficError};
use crosstalk_spec::support::Timestamp;

/// When a recorded state advances its channel's detection: opened by a
/// co-access (at `opened_at`) or confirmed by content (at `Confirmed::at`).
/// Detected, suspected and discarded states do not.
pub(crate) fn advances_at(transmission: &Transmission) -> Option<Timestamp> {
    match &transmission.state {
        TransmissionState::AwaitingContent { .. } => Some(transmission.opened_at),
        TransmissionState::Confirmed(confirmed)
        | TransmissionState::Classified { confirmed, .. }
        | TransmissionState::Aggregated { confirmed, .. } => Some(confirmed.at()),
        TransmissionState::Detected
        | TransmissionState::Suspected { .. }
        | TransmissionState::Discarded { .. } => None,
    }
}

/// The canonical channel's origin once `transmission` is recorded through
/// it; `None` when the state does not advance detection or the origin
/// already says so.
pub(crate) fn advanced(
    origin: &ChannelOrigin,
    canonical: ChannelId,
    transmission: &Transmission,
) -> Result<Option<ChannelOrigin>, TrafficError> {
    let Some(at) = advances_at(transmission) else {
        return Ok(None);
    };
    let since = match origin.traffic() {
        Some(TrafficDetection::Active { since, .. }) => *since,
        Some(TrafficDetection::Dormant { .. }) | None => at,
    };
    let detection = TrafficDetection::Active {
        since,
        last_transmission: transmission.id,
    };
    let next = next_origin(origin, canonical, DetectionUpdate::Traffic(detection))?;
    Ok((next != *origin).then_some(next))
}

/// `origin` with its detection set by `update`. Refuses a superseded
/// channel and `Unused` on anything but a declared channel awaiting
/// traffic.
pub(crate) fn next_origin(
    origin: &ChannelOrigin,
    id: ChannelId,
    update: DetectionUpdate,
) -> Result<ChannelOrigin, TrafficError> {
    match (origin, update) {
        (ChannelOrigin::Superseded { supersession, .. }, _) => Err(TrafficError::Superseded {
            channel: id,
            by: supersession.by,
        }),
        (ChannelOrigin::Discovered { seed, .. }, DetectionUpdate::Traffic(detection)) => {
            Ok(ChannelOrigin::Discovered {
                seed: *seed,
                detection,
            })
        }
        (
            ChannelOrigin::Declared {
                declaration,
                history,
            },
            DetectionUpdate::Traffic(detection),
        ) => {
            let history = match history {
                DeclaredHistory::Promoted { from, .. } => DeclaredHistory::Promoted {
                    from: *from,
                    detection,
                },
                DeclaredHistory::BeforeTraffic(_) => {
                    DeclaredHistory::BeforeTraffic(DeclaredDetection::InUse(detection))
                }
            };
            Ok(ChannelOrigin::Declared {
                declaration: declaration.clone(),
                history,
            })
        }
        (
            ChannelOrigin::Declared {
                declaration,
                history: DeclaredHistory::BeforeTraffic(DeclaredDetection::AwaitingTraffic),
            },
            DetectionUpdate::Unused { since },
        ) => Ok(ChannelOrigin::Declared {
            declaration: declaration.clone(),
            history: DeclaredHistory::BeforeTraffic(DeclaredDetection::Unused { since }),
        }),
        (
            ChannelOrigin::Declared { .. } | ChannelOrigin::Discovered { .. },
            DetectionUpdate::Unused { .. },
        ) => Err(TrafficError::NotAwaitingTraffic(id)),
    }
}

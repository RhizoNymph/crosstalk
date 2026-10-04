//! Channel data as the pages show it: what a channel is matched by, its
//! detection in words, and its policy decision.

use std::time::Duration;

use crosstalk_spec::derived::flow::channel::ChannelOrigin;
use crosstalk_spec::derived::flow::channel::detection::{DeclaredDetection, TrafficDetection};
use crosstalk_spec::derived::flow::channel::policy::{Decision, Policy};
use crosstalk_spec::derived::flow::resource::{Locator, ResourcePattern};
use crosstalk_spec::ids::TransmissionId;

use crate::components::format_time;
use crate::components::locator::{format_locator, format_pattern};
use crate::contract::channels::ChannelSummary;

/// What identifies a channel to a reader: a declared channel's pattern, or a
/// discovered channel's seed resource.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Shape<'a> {
    Pattern(&'a ResourcePattern),
    Seed(&'a Locator),
    /// A discovered channel whose seed resource the backend did not return.
    UnknownSeed,
}

pub fn shape(summary: &ChannelSummary) -> Shape<'_> {
    match (&summary.channel.origin, &summary.seed) {
        (ChannelOrigin::Declared { pattern, .. }, _) => Shape::Pattern(pattern),
        (ChannelOrigin::Discovered { .. }, Some(seed)) => Shape::Seed(&seed.locator),
        (ChannelOrigin::Discovered { .. }, None) => Shape::UnknownSeed,
    }
}

/// The channel's name in titles.
pub fn title(summary: &ChannelSummary) -> String {
    match shape(summary) {
        Shape::Pattern(pattern) => format_pattern(pattern),
        Shape::Seed(locator) => format_locator(locator),
        Shape::UnknownSeed => "discovered channel".to_owned(),
    }
}

/// The detection state in words, and the latest transmission if it has one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DetectionDetail {
    pub text: String,
    pub last_transmission: Option<TransmissionId>,
}

pub fn detection_detail(origin: &ChannelOrigin) -> DetectionDetail {
    let traffic = match origin {
        ChannelOrigin::Declared { detection, .. } => match detection {
            DeclaredDetection::AwaitingTraffic => {
                return plain("Declared; no traffic yet.".to_owned());
            }
            DeclaredDetection::Unused { since } => {
                return plain(format!(
                    "Declared; no traffic by the end of its idle window ({}).",
                    format_time(*since)
                ));
            }
            DeclaredDetection::InUse(traffic) => traffic,
        },
        ChannelOrigin::Discovered { detection, .. } => detection,
    };
    match traffic {
        TrafficDetection::Observed { .. } => {
            plain("Accessed, but not yet written by one agent and read by another.".to_owned())
        }
        TrafficDetection::Candidate { first_cross_access } => plain(format!(
            "Written by one agent and read by another ({} later); no content match yet.",
            format_lag(first_cross_access.lag())
        )),
        TrafficDetection::Active {
            since,
            last_transmission,
        } => DetectionDetail {
            text: format!(
                "Carrying confirmed transmissions since {}.",
                format_time(*since)
            ),
            last_transmission: Some(*last_transmission),
        },
        TrafficDetection::Dormant {
            since,
            last_transmission,
        } => DetectionDetail {
            text: format!("No confirmed transmission since {}.", format_time(*since)),
            last_transmission: Some(*last_transmission),
        },
    }
}

fn plain(text: String) -> DetectionDetail {
    DetectionDetail {
        text,
        last_transmission: None,
    }
}

/// A lag in the largest whole unit that keeps it readable.
pub fn format_lag(lag: Duration) -> String {
    let secs = lag.as_secs();
    if secs >= 3600 {
        format!("{}h {}m", secs / 3600, (secs % 3600) / 60)
    } else if secs >= 60 {
        format!("{}m {}s", secs / 60, secs % 60)
    } else if secs > 0 {
        format!("{secs}s")
    } else {
        format!("{}ms", lag.as_millis())
    }
}

/// The decision behind a policy; `None` for a channel never reviewed.
pub fn decision(policy: &Policy) -> Option<&Decision> {
    match policy {
        Policy::Unreviewed(decision) => decision.as_ref(),
        Policy::Sanctioned(decision) | Policy::Unsanctioned(decision) => Some(decision),
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use crosstalk_spec::derived::flow::channel::Channel;
    use crosstalk_spec::derived::flow::channel::policy::PolicyAuthor;
    use crosstalk_spec::derived::flow::resource::{Host, Resource};
    use crosstalk_spec::ids::{AccessId, ChannelId, ResourceId};
    use crosstalk_spec::support::Timestamp;

    use super::*;

    pub fn wiki() -> Locator {
        Locator::Url {
            scheme: "https".into(),
            host: Host("wiki.example.org".into()),
            path: "/team/agents/notes".into(),
            query: None,
        }
    }

    /// A discovered, active, unreviewed channel seeded by [`wiki`].
    pub fn discovered(id: u128) -> ChannelSummary {
        ChannelSummary {
            channel: Channel {
                id: ChannelId::from_ulid(id),
                origin: ChannelOrigin::Discovered {
                    seed: ResourceId::from_ulid(id),
                    first_access: AccessId::from_ulid(1),
                    detection: TrafficDetection::Active {
                        since: Timestamp::from_micros(1_790_985_600_000_000),
                        last_transmission: TransmissionId::from_ulid(9),
                    },
                },
                resources: Vec::new(),
                policy: Policy::Unreviewed(None),
            },
            seed: Some(Resource {
                id: ResourceId::from_ulid(id),
                locator: wiki(),
                first_seen: Timestamp::from_micros(1_790_900_000_000_000),
            }),
            superseded: None,
            writers: 2,
            readers: 3,
            transmissions: 14,
            last_activity: Some(Timestamp::from_micros(1_790_985_000_000_000)),
        }
    }

    #[test]
    fn discovered_channels_are_named_by_their_seed() {
        let summary = discovered(1);
        assert_eq!(shape(&summary), Shape::Seed(&wiki()));
        assert_eq!(
            title(&summary),
            "https://wiki.example.org/team/agents/notes"
        );
        let mut unknown = discovered(1);
        unknown.seed = None;
        assert_eq!(shape(&unknown), Shape::UnknownSeed);
    }

    #[test]
    fn declared_channels_are_named_by_their_pattern() {
        let mut summary = discovered(1);
        summary.channel.origin = ChannelOrigin::Declared {
            pattern: ResourcePattern::Host(Host("wiki.example.org".into())),
            by: PolicyAuthor::Config,
            at: Timestamp::from_micros(0),
            detection: DeclaredDetection::AwaitingTraffic,
        };
        assert_eq!(title(&summary), "wiki.example.org/…");
        assert_eq!(
            detection_detail(&summary.channel.origin).text,
            "Declared; no traffic yet."
        );
    }

    #[test]
    fn active_detection_points_at_its_last_transmission() {
        let detail = detection_detail(&discovered(1).channel.origin);
        assert_eq!(detail.last_transmission, Some(TransmissionId::from_ulid(9)));
        assert!(
            detail
                .text
                .starts_with("Carrying confirmed transmissions since 2026-10-03")
        );
    }

    #[test]
    fn lags_use_readable_units() {
        assert_eq!(format_lag(Duration::from_millis(250)), "250ms");
        assert_eq!(format_lag(Duration::from_secs(42)), "42s");
        assert_eq!(format_lag(Duration::from_secs(125)), "2m 5s");
        assert_eq!(format_lag(Duration::from_secs(7260)), "2h 1m");
    }

    #[test]
    fn unreviewed_without_decision_has_none() {
        assert_eq!(decision(&Policy::Unreviewed(None)), None);
        let made = Decision {
            by: PolicyAuthor::Config,
            at: Timestamp::from_micros(0),
            note: None,
        };
        assert_eq!(decision(&Policy::Sanctioned(made.clone())), Some(&made));
    }
}

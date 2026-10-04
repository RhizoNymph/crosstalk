//! Channel data as the pages show it: what a channel is matched by, its
//! origin and detection in words, and its policy decisions.

use crosstalk_spec::aggregates::node::CanonicalOriginKind;
use crosstalk_spec::derived::flow::channel::confirmation::{Confirmation, Listing};
use crosstalk_spec::derived::flow::channel::detection::{DeclaredDetection, TrafficDetection};
use crosstalk_spec::derived::flow::channel::policy::{PolicyDecision, PolicyKind};
use crosstalk_spec::derived::flow::channel::{ChannelOrigin, DeclaredHistory};
use crosstalk_spec::derived::flow::resource::{Locator, ResourcePattern};
use crosstalk_spec::ids::TransmissionId;
use crosstalk_spec::interfaces::l8_surface::channels::ChannelRow;

use crate::components::format_time;
use crate::components::locator::{format_locator, format_pattern};

/// What identifies a channel to a reader: a declared channel's pattern, or a
/// discovered channel's seed resource.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Shape<'a> {
    Pattern(&'a ResourcePattern),
    Seed(&'a Locator),
    /// A discovered channel whose seed resource the backend did not return.
    UnknownSeed,
}

pub fn shape(row: &ChannelRow) -> Shape<'_> {
    match (row.channel().origin.pattern(), row.seed()) {
        (Some(pattern), _) => Shape::Pattern(pattern),
        (None, Some(seed)) => Shape::Seed(&seed.locator),
        (None, None) => Shape::UnknownSeed,
    }
}

/// The channel's name in titles.
pub fn title(row: &ChannelRow) -> String {
    match shape(row) {
        Shape::Pattern(pattern) => format_pattern(pattern),
        Shape::Seed(locator) => format_locator(locator),
        Shape::UnknownSeed => "discovered channel".to_owned(),
    }
}

/// How a channel came to be, as its origin badge shows it. A superseded
/// channel was discovered; pages show its supersession beside the badge.
pub fn origin_kind(origin: &ChannelOrigin) -> CanonicalOriginKind {
    CanonicalOriginKind::of(origin).unwrap_or(CanonicalOriginKind::Discovered)
}

/// The origin in words: declared before traffic, promoted from its seed,
/// discovered, or superseded.
pub fn origin_text(origin: &ChannelOrigin) -> &'static str {
    match origin {
        ChannelOrigin::Declared {
            history: DeclaredHistory::BeforeTraffic(_),
            ..
        } => "Declared before any traffic.",
        ChannelOrigin::Declared {
            history: DeclaredHistory::Promoted { .. },
            ..
        } => "Discovered from its seed resource, then promoted to a declared channel.",
        ChannelOrigin::Discovered { .. } => "Discovered from its seed resource.",
        ChannelOrigin::Superseded { .. } => {
            "Discovered from its seed resource; superseded by a promotion."
        }
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
        ChannelOrigin::Declared {
            history: DeclaredHistory::BeforeTraffic(detection),
            ..
        } => match detection {
            DeclaredDetection::AwaitingTraffic => {
                return plain("Declared; no cross-agent traffic yet.".to_owned());
            }
            DeclaredDetection::Unused { since } => {
                return plain(format!(
                    "Declared; no cross-agent traffic by the end of its idle window ({}).",
                    format_time(*since)
                ));
            }
            DeclaredDetection::InUse(traffic) => traffic,
        },
        ChannelOrigin::Declared {
            history: DeclaredHistory::Promoted { detection, .. },
            ..
        }
        | ChannelOrigin::Discovered { detection, .. }
        | ChannelOrigin::Superseded { detection, .. } => detection,
    };
    match traffic {
        TrafficDetection::Active {
            since,
            last_transmission,
        } => DetectionDetail {
            text: format!(
                "Carrying cross-agent traffic since {}.",
                format_time(*since)
            ),
            last_transmission: Some(*last_transmission),
        },
        TrafficDetection::Dormant {
            since,
            last_transmission,
        } => DetectionDetail {
            text: format!("No cross-agent transmission since {}.", format_time(*since)),
            last_transmission: Some(*last_transmission),
        },
    }
}

/// What a channel's listing means, as its page's banner says it; `None`
/// for a confirmed channel, which needs no explanation.
pub fn listing_text(listing: Listing) -> Option<&'static str> {
    match listing {
        Listing::Channel(Confirmation::Confirmed) => None,
        Listing::Channel(Confirmation::Unconfirmed) => Some(
            "Unconfirmed: every transmission between agents through this channel is suspected (one agent wrote, another read, and no content match confirms it yet). Review them below; the confirmed-only filter leaves this channel out.",
        ),
        Listing::Declaration => Some(
            "Declared, no traffic yet: no transmission between two agents has gone through it. It is listed as a declaration and counts as no channel until one does.",
        ),
        Listing::Hidden => Some(
            "Hidden: every transmission through this channel is now between ids of one agent, merged since. It is in no list, graph or count; unmerging those agents lists it again.",
        ),
    }
}

fn plain(text: String) -> DetectionDetail {
    DetectionDetail {
        text,
        last_transmission: None,
    }
}

/// One policy history entry in words: what the decision set.
pub fn decision_text(entry: &PolicyDecision) -> &'static str {
    match entry.kind {
        PolicyKind::Unreviewed => "reset to unreviewed",
        PolicyKind::Sanctioned => "sanctioned",
        PolicyKind::Unsanctioned => "unsanctioned",
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use crosstalk_spec::derived::flow::channel::confirmation::CrossTraffic;
    use crosstalk_spec::derived::flow::channel::detection::TrafficDetection;
    use crosstalk_spec::derived::flow::channel::policy::{Decision, Policy, PolicyAuthor};
    use crosstalk_spec::derived::flow::channel::{Channel, Declaration, Seed, Supersession};
    use crosstalk_spec::derived::flow::resource::{Host, Resource};
    use crosstalk_spec::ids::{ChannelId, OperatorId, ResourceId};
    use crosstalk_spec::interfaces::l8_surface::channels::{
        ChannelActivity, ChannelCounts, ChannelStanding, SupersededInto,
    };
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

    fn seed(id: u128) -> Seed {
        Seed {
            resource: ResourceId::from_ulid(id),
            first_transmission: TransmissionId::from_ulid(1),
        }
    }

    fn active() -> TrafficDetection {
        TrafficDetection::Active {
            since: Timestamp::from_micros(1_790_985_600_000_000),
            last_transmission: TransmissionId::from_ulid(9),
        }
    }

    fn seed_resource(id: u128) -> Resource {
        Resource {
            id: ResourceId::from_ulid(id),
            locator: wiki(),
            first_seen: Timestamp::from_micros(1_790_900_000_000_000),
        }
    }

    /// A discovered, active, unreviewed channel seeded by [`wiki`], with
    /// two writers, three readers and 14 transmissions in the window.
    pub fn discovered(id: u128) -> ChannelRow {
        let channel = Channel {
            id: ChannelId::from_ulid(id),
            origin: ChannelOrigin::Discovered {
                seed: seed(id),
                detection: active(),
            },
            resources: Vec::new(),
            policy: Policy::Unreviewed(None),
        };
        let standing = ChannelStanding::InForce {
            traffic: CrossTraffic {
                confirmed: 14,
                unconfirmed: 0,
            },
            activity: ChannelActivity::Seen {
                last: Timestamp::from_micros(1_790_985_000_000_000),
                counts: ChannelCounts {
                    writers: 2,
                    readers: 3,
                    transmissions: 14,
                },
            },
        };
        ChannelRow::new(channel, Some(seed_resource(id)), standing).expect("row")
    }

    /// `discovered(id)` with `policy`.
    pub fn with_policy(id: u128, policy: Policy) -> ChannelRow {
        let row = discovered(id);
        let mut channel = row.channel().clone();
        channel.policy = policy;
        ChannelRow::new(channel, row.seed().cloned(), row.standing()).expect("row")
    }

    /// The channel `promoted`, promoted by operator 3, and `id`, a
    /// discovered channel it superseded.
    pub fn superseded(id: u128, promoted: u128) -> ChannelRow {
        let at = Timestamp::from_micros(1_790_985_600_000_000);
        let by = Channel {
            id: ChannelId::from_ulid(promoted),
            origin: ChannelOrigin::Declared {
                declaration: Declaration {
                    pattern: ResourcePattern::Host(Host("wiki.example.org".into())),
                    by: PolicyAuthor::Operator(OperatorId::from_ulid(3)),
                    at,
                },
                history: DeclaredHistory::Promoted {
                    from: seed(promoted),
                    detection: active(),
                },
            },
            resources: Vec::new(),
            policy: Policy::Unreviewed(None),
        };
        let supersession = Supersession { by: by.id, at };
        let channel = Channel {
            id: ChannelId::from_ulid(id),
            origin: ChannelOrigin::Superseded {
                seed: seed(id),
                detection: active(),
                supersession,
            },
            resources: Vec::new(),
            policy: Policy::Unreviewed(None),
        };
        let into = SupersededInto::of(supersession, &by).expect("supersession");
        ChannelRow::new(
            channel,
            Some(seed_resource(id)),
            ChannelStanding::Superseded(into),
        )
        .expect("row")
    }

    /// A channel declared in config before any traffic, never active.
    pub fn declared(id: u128) -> ChannelRow {
        let channel = Channel {
            id: ChannelId::from_ulid(id),
            origin: ChannelOrigin::Declared {
                declaration: Declaration {
                    pattern: ResourcePattern::Host(Host("wiki.example.org".into())),
                    by: PolicyAuthor::Config,
                    at: Timestamp::from_micros(0),
                },
                history: DeclaredHistory::BeforeTraffic(DeclaredDetection::AwaitingTraffic),
            },
            resources: Vec::new(),
            policy: Policy::Unreviewed(None),
        };
        ChannelRow::new(
            channel,
            None,
            ChannelStanding::InForce {
                traffic: CrossTraffic::NONE,
                activity: ChannelActivity::Never,
            },
        )
        .expect("row")
    }

    #[test]
    fn discovered_channels_are_named_by_their_seed() {
        let row = discovered(1);
        assert_eq!(shape(&row), Shape::Seed(&wiki()));
        assert_eq!(title(&row), "https://wiki.example.org/team/agents/notes");
        assert_eq!(
            origin_kind(&row.channel().origin),
            CanonicalOriginKind::Discovered
        );
    }

    #[test]
    fn declared_channels_are_named_by_their_pattern() {
        let row = declared(1);
        assert_eq!(title(&row), "wiki.example.org/…");
        assert_eq!(
            detection_detail(&row.channel().origin).text,
            "Declared; no cross-agent traffic yet."
        );
        assert_eq!(
            origin_kind(&row.channel().origin),
            CanonicalOriginKind::DeclaredBeforeTraffic
        );
    }

    #[test]
    fn superseded_channels_read_as_discovered_and_say_so() {
        let row = superseded(1, 2);
        assert_eq!(
            origin_kind(&row.channel().origin),
            CanonicalOriginKind::Discovered
        );
        assert!(origin_text(&row.channel().origin).contains("superseded"));
        assert_eq!(row.counts(), None, "a superseded row has no counts");
    }

    #[test]
    fn active_detection_points_at_its_last_transmission() {
        let detail = detection_detail(&discovered(1).channel().origin);
        assert_eq!(detail.last_transmission, Some(TransmissionId::from_ulid(9)));
        assert!(
            detail
                .text
                .starts_with("Carrying cross-agent traffic since 2026-10-03")
        );
    }

    /// `discovered(id)` whose only cross-agent traffic is `suspected`
    /// suspected transmissions.
    pub fn unconfirmed(id: u128, suspected: u64) -> ChannelRow {
        let row = discovered(id);
        let ChannelStanding::InForce { activity, .. } = row.standing() else {
            unreachable!("discovered rows are in force")
        };
        let standing = ChannelStanding::InForce {
            traffic: CrossTraffic {
                confirmed: 0,
                unconfirmed: suspected,
            },
            activity,
        };
        ChannelRow::new(row.channel().clone(), row.seed().cloned(), standing).expect("row")
    }

    #[test]
    fn listings_explain_themselves_except_confirmed_channels() {
        assert_eq!(
            listing_text(Listing::Channel(Confirmation::Confirmed)),
            None
        );
        for listing in [
            Listing::Channel(Confirmation::Unconfirmed),
            Listing::Declaration,
            Listing::Hidden,
        ] {
            assert!(listing_text(listing).is_some(), "{listing:?}");
        }
        assert_eq!(
            unconfirmed(1, 3).listing(),
            Some(Listing::Channel(Confirmation::Unconfirmed))
        );
        assert_eq!(declared(2).listing(), Some(Listing::Declaration));
    }

    #[test]
    fn decisions_read_as_what_they_set() {
        let entry = |kind| PolicyDecision {
            kind,
            decision: Decision {
                by: PolicyAuthor::Config,
                at: Timestamp::from_micros(0),
                note: None,
            },
        };
        assert_eq!(decision_text(&entry(PolicyKind::Sanctioned)), "sanctioned");
        assert_eq!(
            decision_text(&entry(PolicyKind::Unreviewed)),
            "reset to unreviewed"
        );
    }
}

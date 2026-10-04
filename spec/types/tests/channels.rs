//! Supersession, promotion planning, pattern overlap and read-time channel
//! resolution.

use crate::aggregates::alert::AlertSubject;
use crate::aggregates::edge::{RouteKind, TopologyFilter};
use crate::aggregates::filter::{AccessSubject, FilterSubject};
use crate::aliases::{Aliases, NoAliases, Resolve};
use crate::derived::flow::channel::detection::{
    DeclaredDetection, DetectionKind, TrafficDetection,
};
use crate::derived::flow::channel::policy::{Decision, Policy, PolicyAuthor, PolicyKind};
use crate::derived::flow::channel::promotion::{Promotion, PromotionRefusal, Registered, plan};
use crate::derived::flow::channel::{
    AlreadyDeclared, Channel, ChannelOrigin, Declaration, DeclaredHistory, NotPromotable,
    NotSupersedable, Seed, Supersession,
};
use crate::derived::flow::resource::{Host, Locator, ResourcePattern};
use crate::derived::flow::transmission::{DelegationDirection, Route};
use crate::ids::{AgentId, ChannelId, OperatorId, TopicId};
use crate::interfaces::l5_flow::{ChannelDirectory, PromoteError, RegistryError};
use crate::interfaces::l8_surface::{ActionError, ConflictKind, InputError, QueryError};
use crate::observed::message::ToolName;
use crate::tests::fixtures::{access, agent, at, channel, resource, transmission};

fn url(host: &str, path: &str) -> Locator {
    Locator::Url {
        scheme: "https".into(),
        host: Host(host.into()),
        path: path.into(),
        query: None,
    }
}

fn file(host: Option<&str>, path: &str) -> Locator {
    Locator::File {
        host: host.map(|h| Host(h.into())),
        path: path.into(),
    }
}

fn mcp(server: &str) -> Locator {
    Locator::Mcp {
        server: server.into(),
        tool: ToolName("read".into()),
        target: None,
    }
}

fn host(name: &str) -> Host {
    Host(name.into())
}

fn seed(n: u128) -> Seed {
    Seed {
        resource: resource(n),
        first_access: access(n),
    }
}

fn observed(n: u128) -> TrafficDetection {
    TrafficDetection::Observed {
        first_access: access(n),
    }
}

fn discovered(id: u128) -> Channel {
    Channel {
        id: channel(id),
        origin: ChannelOrigin::Discovered {
            seed: seed(id),
            detection: observed(id),
        },
        resources: Vec::new(),
        policy: Policy::Unreviewed(None),
    }
}

fn declared(id: u128, pattern: ResourcePattern) -> Channel {
    Channel {
        id: channel(id),
        origin: ChannelOrigin::Declared {
            declaration: Declaration {
                pattern,
                by: PolicyAuthor::Config,
                at: at(0),
            },
            history: DeclaredHistory::BeforeTraffic(DeclaredDetection::AwaitingTraffic),
        },
        resources: Vec::new(),
        policy: Policy::Unreviewed(None),
    }
}

fn superseded_by(id: u128, by: u128) -> Channel {
    let mut channel = discovered(id);
    channel.origin = channel
        .origin
        .superseded(supersession(by))
        .expect("discovered channels can be superseded");
    channel
}

fn supersession(by: u128) -> Supersession {
    Supersession {
        by: channel(by),
        at: at(50),
    }
}

fn operator() -> OperatorId {
    OperatorId::from_ulid(7)
}

fn team_pattern() -> ResourcePattern {
    ResourcePattern::UrlPrefix {
        host: host("wiki.example"),
        path_prefix: "/team".into(),
    }
}

fn promotion(pattern: ResourcePattern) -> Promotion {
    Promotion::new(
        pattern,
        PolicyKind::Sanctioned,
        operator(),
        at(100),
        Some("team wiki".into()),
    )
}

/// The supersession table over a set of channels.
struct Directory(Vec<Channel>);

impl ChannelDirectory for Directory {
    fn canonical(&self, id: ChannelId) -> ChannelId {
        self.0
            .iter()
            .find(|channel| channel.id == id)
            .map_or(id, Channel::canonical)
    }
}

/// Channel 2 was superseded by channel 1; no merges.
fn two_into_one(id: ChannelId) -> ChannelId {
    if id == channel(2) { channel(1) } else { id }
}

fn no_merges(id: AgentId) -> AgentId {
    id
}

// Supersession on the channel origin.

#[test]
fn superseding_keeps_seed_and_detection() {
    let origin = discovered(2).origin;
    let superseded = origin
        .superseded(supersession(1))
        .expect("discovered channels can be superseded");
    assert_eq!(
        superseded,
        ChannelOrigin::Superseded {
            seed: seed(2),
            detection: observed(2),
            supersession: supersession(1),
        }
    );
    assert_eq!(superseded.traffic(), origin.traffic());
    assert_eq!(superseded.seed(), Some(seed(2)));
    assert_eq!(superseded.pattern(), None);
    assert_eq!(superseded.supersession(), Some(supersession(1)));
    assert_eq!(origin.supersession(), None);
}

#[test]
fn only_discovered_channels_can_be_superseded() {
    let wiki = declared(3, team_pattern()).origin;
    assert_eq!(
        wiki.superseded(supersession(1)),
        Err(NotSupersedable::Declared)
    );
    let promoted = discovered(4)
        .origin
        .promoted(promotion(team_pattern()).declaration().clone())
        .expect("discovered");
    assert_eq!(
        promoted.superseded(supersession(1)),
        Err(NotSupersedable::Declared)
    );
    let superseded = superseded_by(2, 1).origin;
    assert_eq!(
        superseded.superseded(supersession(5)),
        Err(NotSupersedable::AlreadySuperseded(supersession(1)))
    );
}

#[test]
fn superseded_channels_cannot_be_promoted() {
    let superseded = superseded_by(2, 1).origin;
    assert_eq!(
        superseded.promoted(promotion(team_pattern()).declaration().clone()),
        Err(NotPromotable::Superseded(supersession(1)))
    );
    assert_eq!(
        declared(3, team_pattern())
            .origin
            .promoted(promotion(team_pattern()).declaration().clone()),
        Err(AlreadyDeclared)
    );
}

#[test]
fn channel_canonical_is_its_superseding_channel() {
    assert_eq!(superseded_by(2, 1).canonical(), channel(1));
    assert_eq!(discovered(2).canonical(), channel(2));
    assert_eq!(declared(3, team_pattern()).canonical(), channel(3));
}

#[test]
fn seed_is_absent_only_before_traffic() {
    assert_eq!(declared(3, team_pattern()).origin.seed(), None);
    assert_eq!(discovered(2).origin.seed(), Some(seed(2)));
    let promoted = discovered(4)
        .origin
        .promoted(promotion(team_pattern()).declaration().clone())
        .expect("discovered");
    assert_eq!(promoted.seed(), Some(seed(4)));
}

#[test]
fn detection_kind_follows_each_origin() {
    let active = TrafficDetection::Active {
        since: at(1),
        last_transmission: transmission(1),
    };
    let before = |detection| ChannelOrigin::Declared {
        declaration: Declaration {
            pattern: team_pattern(),
            by: PolicyAuthor::Config,
            at: at(0),
        },
        history: DeclaredHistory::BeforeTraffic(detection),
    };
    let cases = [
        (
            before(DeclaredDetection::AwaitingTraffic),
            DetectionKind::AwaitingTraffic,
        ),
        (
            before(DeclaredDetection::Unused { since: at(2) }),
            DetectionKind::Unused,
        ),
        (
            before(DeclaredDetection::InUse(active.clone())),
            DetectionKind::Active,
        ),
        (discovered(2).origin, DetectionKind::Observed),
        (superseded_by(2, 1).origin, DetectionKind::Observed),
        (
            ChannelOrigin::Discovered {
                seed: seed(2),
                detection: TrafficDetection::Dormant {
                    since: at(3),
                    last_transmission: transmission(1),
                },
            },
            DetectionKind::Dormant,
        ),
    ];
    for (origin, kind) in cases {
        assert_eq!(origin.detection_kind(), kind);
    }
}

// Promotion.

#[test]
fn promotion_declaration_and_decision_share_operator_and_time() {
    let promotion = promotion(team_pattern());
    let author = PolicyAuthor::Operator(operator());
    assert_eq!(promotion.declaration().by, author);
    assert_eq!(promotion.decision().decision.by, author);
    assert_eq!(promotion.declaration().at, at(100));
    assert_eq!(promotion.decision().decision.at, at(100));
    assert_eq!(promotion.at(), at(100));
    assert_eq!(promotion.pattern(), &team_pattern());
    assert_eq!(promotion.decision().kind, PolicyKind::Sanctioned);
    assert_eq!(
        promotion.decision().policy(),
        Policy::Sanctioned(Decision {
            by: author,
            at: at(100),
            note: Some("team wiki".into()),
        })
    );
}

#[test]
fn plan_promotes_in_place_and_supersedes_matching_discovered_channels() {
    let target = discovered(1);
    let sibling = discovered(2);
    let elsewhere = discovered(3);
    let mcp_wiki = declared(4, ResourcePattern::McpServer("wiki".into()));
    let (a, b, c) = (
        url("wiki.example", "/team/a"),
        url("wiki.example", "/team/b"),
        url("wiki.example", "/other/c"),
    );
    let registry = [
        Registered {
            channel: &target,
            seed: Some(&a),
        },
        Registered {
            channel: &sibling,
            seed: Some(&b),
        },
        Registered {
            channel: &elsewhere,
            seed: Some(&c),
        },
        Registered {
            channel: &mcp_wiki,
            seed: None,
        },
    ];
    let promotion = promotion(team_pattern());
    let plan = plan(channel(1), &promotion, &registry).expect("valid promotion");
    assert_eq!(
        plan.origin,
        target
            .origin
            .promoted(promotion.declaration().clone())
            .expect("discovered")
    );
    let expected = sibling
        .origin
        .superseded(Supersession {
            by: channel(1),
            at: at(100),
        })
        .expect("discovered");
    assert_eq!(plan.superseded, vec![(channel(2), expected)]);
    assert_eq!(plan.superseded_ids().collect::<Vec<_>>(), vec![channel(2)]);
}

#[test]
fn plan_refuses_an_unknown_channel() {
    let only = discovered(2);
    let seed = url("wiki.example", "/team/b");
    let registry = [Registered {
        channel: &only,
        seed: Some(&seed),
    }];
    assert_eq!(
        plan(channel(9), &promotion(team_pattern()), &registry),
        Err(PromotionRefusal::UnknownChannel(channel(9)))
    );
}

#[test]
fn plan_refuses_a_superseded_channel_naming_its_superseder() {
    let target = superseded_by(2, 1);
    let seed = url("wiki.example", "/team/b");
    let registry = [Registered {
        channel: &target,
        seed: Some(&seed),
    }];
    assert_eq!(
        plan(channel(2), &promotion(team_pattern()), &registry),
        Err(PromotionRefusal::Superseded {
            channel: channel(2),
            by: channel(1),
        })
    );
}

#[test]
fn plan_refuses_a_declared_channel() {
    let target = declared(3, ResourcePattern::McpServer("wiki".into()));
    let registry = [Registered {
        channel: &target,
        seed: None,
    }];
    assert_eq!(
        plan(channel(3), &promotion(team_pattern()), &registry),
        Err(PromotionRefusal::NotDiscovered(channel(3)))
    );
}

#[test]
fn plan_refuses_a_pattern_that_misses_the_seed() {
    let target = discovered(1);
    let outside = url("wiki.example", "/other/a");
    let registry = [Registered {
        channel: &target,
        seed: Some(&outside),
    }];
    assert_eq!(
        plan(channel(1), &promotion(team_pattern()), &registry),
        Err(PromotionRefusal::PatternMissesSeed)
    );
    let unreadable = [Registered {
        channel: &target,
        seed: None,
    }];
    assert_eq!(
        plan(channel(1), &promotion(team_pattern()), &unreadable),
        Err(PromotionRefusal::PatternMissesSeed)
    );
}

#[test]
fn plan_refuses_a_pattern_overlapping_a_declared_channel() {
    let target = discovered(1);
    let whole_host = declared(5, ResourcePattern::Host(host("wiki.example")));
    let seed = url("wiki.example", "/team/a");
    let registry = [
        Registered {
            channel: &target,
            seed: Some(&seed),
        },
        Registered {
            channel: &whole_host,
            seed: None,
        },
    ];
    assert_eq!(
        plan(channel(1), &promotion(team_pattern()), &registry),
        Err(PromotionRefusal::PatternOverlaps {
            existing: channel(5),
        })
    );
}

#[test]
fn plan_checks_in_order() {
    let whole_host = declared(5, ResourcePattern::Host(host("wiki.example")));
    let outside = url("elsewhere.example", "/x");
    let overlapping = promotion(team_pattern());
    // Superseded before the seed and overlap checks.
    let superseded = superseded_by(2, 1);
    let registry = [
        Registered {
            channel: &superseded,
            seed: Some(&outside),
        },
        Registered {
            channel: &whole_host,
            seed: None,
        },
    ];
    assert!(matches!(
        plan(channel(2), &overlapping, &registry),
        Err(PromotionRefusal::Superseded { .. })
    ));
    // A missed seed before an overlap.
    let target = discovered(1);
    let registry = [
        Registered {
            channel: &target,
            seed: Some(&outside),
        },
        Registered {
            channel: &whole_host,
            seed: None,
        },
    ];
    assert_eq!(
        plan(channel(1), &overlapping, &registry),
        Err(PromotionRefusal::PatternMissesSeed)
    );
}

#[test]
fn plan_supersedes_only_discovered_channels() {
    // A registry that breaks the overlap rule on purpose (channel 3 was
    // superseded by a channel that is not listed), to show that even then
    // a superseded or declared channel is never superseded again.
    let target = discovered(1);
    let already = superseded_by(3, 8);
    let seed_a = url("wiki.example", "/team/a");
    let seed_c = url("wiki.example", "/team/c");
    let registry = [
        Registered {
            channel: &target,
            seed: Some(&seed_a),
        },
        Registered {
            channel: &already,
            seed: Some(&seed_c),
        },
    ];
    let plan = plan(channel(1), &promotion(team_pattern()), &registry).expect("valid");
    assert!(plan.superseded.is_empty());
}

#[test]
fn supersession_resolves_in_one_step() {
    let target = discovered(1);
    let sibling = discovered(2);
    let (a, b) = (
        url("wiki.example", "/team/a"),
        url("wiki.example", "/team/b"),
    );
    let registry = [
        Registered {
            channel: &target,
            seed: Some(&a),
        },
        Registered {
            channel: &sibling,
            seed: Some(&b),
        },
    ];
    let plan = plan(channel(1), &promotion(team_pattern()), &registry).expect("valid");
    let mut channels = vec![Channel {
        origin: plan.origin.clone(),
        ..target.clone()
    }];
    channels.extend(plan.superseded.iter().map(|(id, origin)| Channel {
        id: *id,
        origin: origin.clone(),
        ..sibling.clone()
    }));
    let directory = Directory(channels);
    assert_eq!(directory.canonical(channel(2)), channel(1));
    assert_eq!(directory.canonical(channel(1)), channel(1));
    for id in [channel(1), channel(2), channel(9)] {
        let once = directory.canonical(id);
        assert_eq!(directory.canonical(once), once);
    }
}

// Pattern overlap.

#[test]
fn pattern_overlap_cases() {
    let url_prefix = |h: &str, p: &str| ResourcePattern::UrlPrefix {
        host: host(h),
        path_prefix: p.into(),
    };
    let path_prefix = |h: Option<&str>, p: &str| ResourcePattern::PathPrefix {
        host: h.map(host),
        prefix: p.into(),
    };
    let cases = [
        (
            ResourcePattern::Host(host("a")),
            ResourcePattern::Host(host("a")),
            true,
        ),
        (
            ResourcePattern::Host(host("a")),
            ResourcePattern::Host(host("b")),
            false,
        ),
        (
            ResourcePattern::Host(host("a")),
            url_prefix("a", "/x"),
            true,
        ),
        (
            ResourcePattern::Host(host("a")),
            url_prefix("b", "/x"),
            false,
        ),
        (url_prefix("a", "/x"), url_prefix("a", "/x/y"), true),
        (url_prefix("a", "/x"), url_prefix("a", "/xy"), false),
        (url_prefix("a", "/x/"), url_prefix("a", "/x"), true),
        (url_prefix("a", "/x"), url_prefix("b", "/x"), false),
        (
            path_prefix(None, "/srv"),
            path_prefix(None, "/srv/shared"),
            true,
        ),
        (path_prefix(None, "/srv"), path_prefix(None, "/srv2"), false),
        (
            path_prefix(None, "/srv"),
            path_prefix(Some("h"), "/srv"),
            false,
        ),
        (
            ResourcePattern::McpServer("wiki".into()),
            ResourcePattern::McpServer("wiki".into()),
            true,
        ),
        (
            ResourcePattern::McpServer("wiki".into()),
            ResourcePattern::McpServer("mail".into()),
            false,
        ),
        (
            ResourcePattern::Exact(url("a", "/x/1")),
            url_prefix("a", "/x"),
            true,
        ),
        (
            ResourcePattern::Exact(url("a", "/y/1")),
            url_prefix("a", "/x"),
            false,
        ),
        (
            ResourcePattern::Exact(url("a", "/x")),
            ResourcePattern::Exact(url("a", "/x")),
            true,
        ),
        (
            ResourcePattern::Exact(url("a", "/x")),
            ResourcePattern::Exact(url("a", "/y")),
            false,
        ),
        (
            ResourcePattern::Host(host("a")),
            path_prefix(Some("a"), "/"),
            false,
        ),
        (
            url_prefix("a", "/"),
            ResourcePattern::McpServer("a".into()),
            false,
        ),
        (
            path_prefix(None, "/"),
            ResourcePattern::McpServer("a".into()),
            false,
        ),
    ];
    for (left, right, expected) in cases {
        assert_eq!(left.overlaps(&right), expected, "{left:?} vs {right:?}");
        assert_eq!(right.overlaps(&left), expected, "{right:?} vs {left:?}");
    }
}

#[test]
fn overlap_is_implied_by_a_shared_match() {
    let patterns = [
        ResourcePattern::Host(host("a")),
        ResourcePattern::UrlPrefix {
            host: host("a"),
            path_prefix: "/x".into(),
        },
        ResourcePattern::UrlPrefix {
            host: host("a"),
            path_prefix: "/x/y".into(),
        },
        ResourcePattern::UrlPrefix {
            host: host("a"),
            path_prefix: "/xy".into(),
        },
        ResourcePattern::PathPrefix {
            host: None,
            prefix: "/srv".into(),
        },
        ResourcePattern::McpServer("wiki".into()),
        ResourcePattern::Exact(url("a", "/x/y/z")),
        ResourcePattern::Exact(file(None, "/srv/a")),
    ];
    let locators = [
        url("a", "/x"),
        url("a", "/x/y"),
        url("a", "/x/y/z"),
        url("a", "/xy"),
        url("b", "/x"),
        file(None, "/srv/a"),
        file(Some("h"), "/srv/a"),
        mcp("wiki"),
        mcp("mail"),
    ];
    for left in &patterns {
        for right in &patterns {
            let shared = locators
                .iter()
                .any(|locator| left.matches(locator) && right.matches(locator));
            if shared {
                assert!(left.overlaps(right), "{left:?} vs {right:?}");
            }
        }
    }
}

// Read-time resolution.

#[test]
fn route_resolution_changes_only_channels() {
    let aliases = Resolve {
        agents: no_merges,
        channels: two_into_one,
    };
    assert_eq!(
        Route::Channel(channel(2)).resolved(aliases),
        Route::Channel(channel(1))
    );
    assert_eq!(
        Route::Channel(channel(3)).resolved(aliases),
        Route::Channel(channel(3))
    );
    let delegated = Route::Delegation(DelegationDirection::ParentToChild);
    assert_eq!(delegated.resolved(aliases), delegated);
    assert_eq!(Route::Unobserved.resolved(aliases), Route::Unobserved);
}

#[test]
fn alias_sources_resolve_as_documented() {
    let merged = |id: AgentId| if id == agent(9) { agent(1) } else { id };
    assert_eq!(merged.agent(agent(9)), agent(1));
    assert_eq!(merged.channel(channel(2)), channel(2));
    assert_eq!(NoAliases.agent(agent(9)), agent(9));
    assert_eq!(NoAliases.channel(channel(2)), channel(2));
    let both = Resolve {
        agents: merged,
        channels: two_into_one,
    };
    assert_eq!(both.agent(agent(9)), agent(1));
    assert_eq!(both.channel(channel(2)), channel(1));
}

#[test]
fn alert_subjects_resolve_channels_and_agents() {
    let aliases = Resolve {
        agents: |id: AgentId| if id == agent(9) { agent(1) } else { id },
        channels: two_into_one,
    };
    assert_eq!(
        AlertSubject::Channel(channel(2)).resolved(aliases),
        AlertSubject::Channel(channel(1))
    );
    assert_eq!(
        AlertSubject::Agent(agent(9)).resolved(aliases),
        AlertSubject::Agent(agent(1))
    );
    assert_eq!(
        AlertSubject::Transmission(transmission(4)).resolved(aliases),
        AlertSubject::Transmission(transmission(4))
    );
}

#[test]
fn filter_listing_a_superseded_channel_selects_its_superseder() {
    let canonical_route = Route::Channel(channel(1));
    let subject = FilterSubject {
        from: agent(1),
        to: agent(2),
        route: &canonical_route,
        topic: None,
        false_detection: false,
    };
    let listing_old = TopologyFilter {
        channels: vec![channel(2)],
        ..TopologyFilter::default()
    };
    let resolve = Resolve {
        agents: no_merges,
        channels: two_into_one,
    };
    assert!(listing_old.admits(&subject, resolve));
    assert!(!listing_old.admits(&subject, no_merges));
    let listing_new = TopologyFilter {
        channels: vec![channel(1)],
        ..TopologyFilter::default()
    };
    assert!(listing_new.admits(&subject, resolve));
}

fn topic(n: u128) -> TopicId {
    TopicId::from_ulid(n)
}

#[test]
fn access_filter_applies_each_field() {
    let topics = [topic(5)];
    let subject = AccessSubject {
        agent: agent(1),
        channel: channel(1),
        channel_topics: &topics,
    };
    let resolve = Resolve {
        agents: |id: AgentId| if id == agent(9) { agent(1) } else { id },
        channels: two_into_one,
    };
    let admits = |filter: TopologyFilter| filter.admits_access(&subject, resolve);
    assert!(admits(TopologyFilter::default()));
    assert!(admits(TopologyFilter {
        agents: vec![agent(9)],
        ..TopologyFilter::default()
    }));
    assert!(!admits(TopologyFilter {
        agents: vec![agent(2)],
        ..TopologyFilter::default()
    }));
    assert!(admits(TopologyFilter {
        channels: vec![channel(2)],
        ..TopologyFilter::default()
    }));
    assert!(!admits(TopologyFilter {
        channels: vec![channel(3)],
        ..TopologyFilter::default()
    }));
    assert!(admits(TopologyFilter {
        route_kinds: vec![RouteKind::Channel, RouteKind::Direct],
        ..TopologyFilter::default()
    }));
    assert!(!admits(TopologyFilter {
        route_kinds: vec![RouteKind::Delegation],
        ..TopologyFilter::default()
    }));
    assert!(admits(TopologyFilter {
        topics: vec![topic(5)],
        ..TopologyFilter::default()
    }));
    assert!(!admits(TopologyFilter {
        topics: vec![topic(6)],
        ..TopologyFilter::default()
    }));
}

#[test]
fn access_on_a_channel_without_topics_fails_any_topic_filter() {
    let subject = AccessSubject {
        agent: agent(1),
        channel: channel(1),
        channel_topics: &[],
    };
    let filter = TopologyFilter {
        topics: vec![topic(5)],
        ..TopologyFilter::default()
    };
    assert!(!filter.admits_access(&subject, NoAliases));
    assert!(TopologyFilter::default().admits_access(&subject, NoAliases));
}

// Surface errors.

#[test]
fn promotion_refusals_map_to_action_errors() {
    let cases = [
        (
            PromotionRefusal::UnknownChannel(channel(1)),
            ActionError::NotFound,
        ),
        (
            PromotionRefusal::Superseded {
                channel: channel(2),
                by: channel(1),
            },
            ActionError::Conflict(ConflictKind::ChannelSuperseded {
                channel: channel(2),
                by: channel(1),
            }),
        ),
        (
            PromotionRefusal::NotDiscovered(channel(3)),
            ActionError::Conflict(ConflictKind::ChannelNotDiscovered {
                channel: channel(3),
            }),
        ),
        (
            PromotionRefusal::PatternMissesSeed,
            ActionError::InvalidInput(InputError::PatternMissesSeed),
        ),
        (
            PromotionRefusal::PatternOverlaps {
                existing: channel(5),
            },
            ActionError::Conflict(ConflictKind::PatternOverlaps {
                existing: channel(5),
            }),
        ),
    ];
    for (refusal, expected) in cases {
        assert_eq!(ActionError::from(refusal), expected);
        assert_eq!(ActionError::from(PromoteError::Refused(refusal)), expected);
    }
    assert_eq!(
        ActionError::from(PromoteError::Store {
            reason: "down".into()
        }),
        ActionError::Store {
            reason: "down".into()
        }
    );
}

#[test]
fn registry_errors_map_to_query_errors() {
    let cases = [
        (
            RegistryError::Store {
                reason: "down".into(),
            },
            QueryError::Store {
                reason: "down".into(),
            },
        ),
        (
            RegistryError::UnknownChannel(channel(1)),
            QueryError::NotFound,
        ),
        (
            RegistryError::OverlappingDeclaration {
                existing: channel(5),
            },
            QueryError::Conflict(ConflictKind::PatternOverlaps {
                existing: channel(5),
            }),
        ),
        (
            RegistryError::Superseded {
                channel: channel(2),
                by: channel(1),
            },
            QueryError::Conflict(ConflictKind::ChannelSuperseded {
                channel: channel(2),
                by: channel(1),
            }),
        ),
        (RegistryError::InvalidCursor, QueryError::InvalidCursor),
    ];
    for (error, expected) in cases {
        assert_eq!(QueryError::from(error), expected);
    }
}

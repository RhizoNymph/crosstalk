//! The overview's counts: activity from the graph, queues from the stores.

use std::num::NonZeroU64;

use crate::aggregates::alert::{Alert, AlertState, AlertSubject, SuppressReason};
use crate::aggregates::edge::{EdgeStats, EdgeTotals, TopologyGraph, WeightedEdge, Weighting};
use crate::aggregates::topic::TopicModelVersion;
use crate::derived::flow::channel::detection::{DeclaredDetection, TrafficDetection};
use crate::derived::flow::channel::policy::{Decision, Policy, PolicyAuthor};
use crate::derived::flow::channel::{
    Channel, ChannelOrigin, Declaration, DeclaredHistory, Seed, Supersession,
};
use crate::derived::flow::resource::{Host, ResourcePattern};
use crate::derived::flow::transmission::{DelegationDirection, Route};
use crate::ids::{AlertId, AlertRuleId, OperatorId};
use crate::interfaces::l8_surface::overview::QueueCounts;
use crate::support::{Share, TimeWindow};
use crate::tests::fixtures::{access, agent, at, channel, resource};

fn count(n: u64) -> NonZeroU64 {
    NonZeroU64::new(n).expect("non-zero")
}

fn edge(from: u128, to: u128, route: Route, transmissions: u64, bytes: u64) -> WeightedEdge {
    WeightedEdge {
        from: agent(from),
        to: agent(to),
        route,
        stats: EdgeStats {
            transmissions: count(transmissions),
            matched_bytes: count(bytes),
        },
        share: Share::new(0.25).expect("in range"),
    }
}

fn graph(edges: Vec<WeightedEdge>) -> TopologyGraph {
    TopologyGraph {
        window: TimeWindow::new(at(0), at(60)).expect("non-empty"),
        weighting: Weighting::Transmissions,
        topic_version: TopicModelVersion(3),
        nodes: Vec::new(),
        edges,
    }
}

#[test]
fn totals_sum_the_edges_and_count_distinct_channels() {
    let totals = EdgeTotals::of(&graph(vec![
        edge(1, 2, Route::Channel(channel(7)), 3, 300),
        edge(2, 1, Route::Channel(channel(7)), 1, 10),
        edge(1, 3, Route::Channel(channel(8)), 2, 20),
        edge(
            1,
            3,
            Route::Delegation(DelegationDirection::ParentToChild),
            4,
            40,
        ),
        edge(3, 2, Route::Unobserved, 1, 5),
    ]));
    assert_eq!(
        totals,
        EdgeTotals {
            topic_version: TopicModelVersion(3),
            transmissions: 11,
            matched_bytes: 375,
            active_channels: 2,
        }
    );
}

#[test]
fn totals_do_not_depend_on_weighting() {
    let edges = vec![edge(1, 2, Route::Channel(channel(7)), 3, 300)];
    let by_count = graph(edges.clone());
    let mut by_bytes = graph(edges);
    by_bytes.weighting = Weighting::MatchedBytes;
    assert_eq!(EdgeTotals::of(&by_count), EdgeTotals::of(&by_bytes));
}

#[test]
fn an_empty_graph_counts_nothing() {
    let totals = EdgeTotals::of(&graph(Vec::new()));
    assert_eq!(
        (
            totals.transmissions,
            totals.matched_bytes,
            totals.active_channels
        ),
        (0, 0, 0)
    );
    assert_eq!(totals.topic_version, TopicModelVersion(3));
}

fn alert(n: u128, state: AlertState) -> Alert {
    Alert {
        id: AlertId::from_ulid(n),
        rule: AlertRuleId::from_ulid(1),
        subject: AlertSubject::Channel(channel(1)),
        raised_at: at(1),
        occurrences: 1,
        state,
    }
}

fn decision() -> Decision {
    Decision {
        by: PolicyAuthor::Operator(OperatorId::from_ulid(1)),
        at: at(2),
        note: None,
    }
}

fn seed() -> Seed {
    Seed {
        resource: resource(1),
        first_access: access(1),
    }
}

fn observed() -> TrafficDetection {
    TrafficDetection::Observed {
        first_access: access(1),
    }
}

fn discovered(id: u128, policy: Policy) -> Channel {
    Channel {
        id: channel(id),
        origin: ChannelOrigin::Discovered {
            seed: seed(),
            detection: observed(),
        },
        resources: Vec::new(),
        policy,
    }
}

#[test]
fn queues_count_open_alerts_only() {
    let operator = OperatorId::from_ulid(1);
    let alerts = [
        alert(1, AlertState::Open),
        alert(2, AlertState::Open),
        alert(
            3,
            AlertState::Acknowledged {
                by: operator,
                at: at(2),
            },
        ),
        alert(
            4,
            AlertState::Resolved {
                by: operator,
                at: at(3),
                note: None,
            },
        ),
        alert(
            5,
            AlertState::Suppressed {
                at: at(3),
                reason: SuppressReason::OperatorRejected,
            },
        ),
    ];
    let counts = QueueCounts::tally(&alerts, &[]);
    assert_eq!(counts.open_alerts, 2);
    assert_eq!(counts.unreviewed_channels, 0);
}

#[test]
fn queues_count_unreviewed_channels_that_are_not_superseded() {
    let declared = Channel {
        id: channel(4),
        origin: ChannelOrigin::Declared {
            declaration: Declaration {
                pattern: ResourcePattern::Host(Host("wiki.internal".into())),
                by: PolicyAuthor::Config,
                at: at(0),
            },
            history: DeclaredHistory::BeforeTraffic(DeclaredDetection::AwaitingTraffic),
        },
        resources: Vec::new(),
        policy: Policy::Unreviewed(None),
    };
    let superseded = Channel {
        id: channel(5),
        origin: ChannelOrigin::Superseded {
            seed: seed(),
            detection: observed(),
            supersession: Supersession {
                by: channel(4),
                at: at(3),
            },
        },
        resources: Vec::new(),
        policy: Policy::Unreviewed(None),
    };
    let channels = [
        discovered(1, Policy::Unreviewed(None)),
        discovered(2, Policy::Unreviewed(Some(decision()))),
        discovered(3, Policy::Sanctioned(decision())),
        discovered(6, Policy::Unsanctioned(decision())),
        declared,
        superseded,
    ];
    let counts = QueueCounts::tally(&[], &channels);
    assert_eq!(
        counts.unreviewed_channels, 3,
        "never reviewed, reset, declared"
    );
    assert_eq!(counts.open_alerts, 0);
}

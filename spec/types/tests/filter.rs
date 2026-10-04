use crate::aggregates::alert::{AlertRule, AlertRuleDef, RuleStatus, TopicWatch, WatchedTopics};
use crate::aggregates::edge::{RouteKind, TopologyFilter};
use crate::aggregates::filter::{FalseDetections, FilterSubject, TopicVersionSelector};
use crate::derived::flow::channel::detection::DeclaredDetection;
use crate::derived::flow::channel::policy::{Decision, Policy, PolicyAuthor};
use crate::derived::flow::channel::{Channel, ChannelOrigin, Declaration, DeclaredHistory};
use crate::derived::flow::resource::ResourcePattern;
use crate::derived::flow::transmission::{DelegationDirection, DirectCarrier, Route};
use crate::ids::PromptHash;
use crate::ids::{AgentId, AlertRuleId, TopicId};
use crate::interfaces::l8_surface::PolicyKind;
use crate::interfaces::l8_surface::lists::{
    AgentFilter, AgentStateKind, AlertRuleFilter, ChannelFilter,
};
use crate::observed::agent::{
    Agent, AgentState, IdentityEvidence, LabelLog, MergeAuthor, MergeableState, Merged,
};
use crate::support::{Blake3, NonEmpty};
use crate::tests::fixtures::{agent, at, channel};

fn topic(n: u128) -> TopicId {
    TopicId::from_ulid(n)
}

/// No merges.
fn identity(id: AgentId) -> AgentId {
    id
}

fn subject(from: u128, to: u128, route: &Route, topic: Option<TopicId>) -> FilterSubject<'_> {
    FilterSubject {
        from: agent(from),
        to: agent(to),
        route,
        topic,
        false_detection: false,
    }
}

#[test]
fn route_kind_follows_route_variant() {
    let cases = [
        (Route::Channel(channel(1)), RouteKind::Channel),
        (
            Route::Delegation(DelegationDirection::ParentToChild),
            RouteKind::Delegation,
        ),
        (Route::Direct(DirectCarrier::UserTurn), RouteKind::Direct),
        (Route::Unobserved, RouteKind::Unobserved),
    ];
    for (route, kind) in cases {
        assert_eq!(RouteKind::from(&route), kind);
    }
}

#[test]
fn default_filter_admits_everything() {
    let route = Route::Unobserved;
    assert!(TopologyFilter::default().admits(&subject(1, 2, &route, None), identity));
}

#[test]
fn agent_filter_matches_sender_or_reader() {
    let route = Route::Unobserved;
    let filter = TopologyFilter {
        agents: vec![agent(2)],
        ..TopologyFilter::default()
    };
    assert!(filter.admits(&subject(2, 3, &route, None), identity));
    assert!(filter.admits(&subject(1, 2, &route, None), identity));
    assert!(!filter.admits(&subject(1, 3, &route, None), identity));
}

#[test]
fn agent_filter_resolves_listed_ids_through_merges() {
    let route = Route::Unobserved;
    let filter = TopologyFilter {
        agents: vec![agent(9)],
        ..TopologyFilter::default()
    };
    let merged_into_two = |id: AgentId| if id == agent(9) { agent(2) } else { id };
    assert!(filter.admits(&subject(1, 2, &route, None), merged_into_two));
    assert!(!filter.admits(&subject(1, 2, &route, None), identity));
}

#[test]
fn channel_filter_excludes_other_channels_and_non_channel_routes() {
    let filter = TopologyFilter {
        channels: vec![channel(1)],
        ..TopologyFilter::default()
    };
    let on_listed = Route::Channel(channel(1));
    let on_other = Route::Channel(channel(2));
    let delegated = Route::Delegation(DelegationDirection::ChildToParent);
    assert!(filter.admits(&subject(1, 2, &on_listed, None), identity));
    assert!(!filter.admits(&subject(1, 2, &on_other, None), identity));
    assert!(!filter.admits(&subject(1, 2, &delegated, None), identity));
}

#[test]
fn route_kind_filter_keeps_listed_kinds() {
    let filter = TopologyFilter {
        route_kinds: vec![RouteKind::Direct, RouteKind::Unobserved],
        ..TopologyFilter::default()
    };
    let direct = Route::Direct(DirectCarrier::SystemPrompt);
    let on_channel = Route::Channel(channel(1));
    assert!(filter.admits(&subject(1, 2, &direct, None), identity));
    assert!(filter.admits(&subject(1, 2, &Route::Unobserved, None), identity));
    assert!(!filter.admits(&subject(1, 2, &on_channel, None), identity));
}

#[test]
fn topic_filter_never_matches_outliers_or_unclassified() {
    let route = Route::Unobserved;
    let filter = TopologyFilter {
        topics: vec![topic(5)],
        ..TopologyFilter::default()
    };
    assert!(filter.admits(&subject(1, 2, &route, Some(topic(5))), identity));
    assert!(!filter.admits(&subject(1, 2, &route, Some(topic(6))), identity));
    assert!(!filter.admits(&subject(1, 2, &route, None), identity));
}

#[test]
fn non_empty_fields_combine_with_and() {
    let filter = TopologyFilter {
        agents: vec![agent(1)],
        channels: vec![channel(1)],
        route_kinds: vec![RouteKind::Channel],
        topics: vec![topic(5)],
        ..TopologyFilter::default()
    };
    let on_listed = Route::Channel(channel(1));
    assert!(filter.admits(&subject(1, 2, &on_listed, Some(topic(5))), identity));
    // Each case fails exactly one field.
    assert!(!filter.admits(&subject(3, 2, &on_listed, Some(topic(5))), identity));
    let on_other = Route::Channel(channel(2));
    assert!(!filter.admits(&subject(1, 2, &on_other, Some(topic(5))), identity));
    assert!(!filter.admits(&subject(1, 2, &on_listed, Some(topic(6))), identity));
}

fn declared_channel(policy: Policy) -> Channel {
    Channel {
        id: channel(1),
        origin: ChannelOrigin::Declared {
            declaration: Declaration {
                pattern: ResourcePattern::McpServer("wiki".into()),
                by: PolicyAuthor::Config,
                at: at(0),
            },
            history: DeclaredHistory::BeforeTraffic(DeclaredDetection::AwaitingTraffic),
        },
        resources: Vec::new(),
        policy,
    }
}

#[test]
fn channel_filter_matches_policy_kind() {
    let decision = Decision {
        by: PolicyAuthor::Config,
        at: at(1),
        note: None,
    };
    let sanctioned = declared_channel(Policy::Sanctioned(decision.clone()));
    let reset = declared_channel(Policy::Unreviewed(Some(decision)));
    let filter = ChannelFilter {
        policies: vec![PolicyKind::Unreviewed],
    };
    assert!(filter.matches(&reset));
    assert!(!filter.matches(&sanctioned));
    assert!(ChannelFilter::default().matches(&sanctioned));
}

fn agent_in(state: AgentState) -> Agent {
    Agent {
        id: agent(1),
        evidence: NonEmpty::new(IdentityEvidence::PromptFingerprint(
            PromptHash::from_digest(Blake3::from_bytes([7; 32])),
        )),
        parent: None,
        state,
        labels: LabelLog::default(),
    }
}

#[test]
fn agent_filter_matches_state_kind() {
    let merged = agent_in(AgentState::Merged(Merged::new(
        agent(2),
        at(5),
        MergeAuthor::Resolver,
        MergeableState::Provisional { first_seen: at(1) },
    )));
    let live = agent_in(AgentState::Provisional { first_seen: at(1) });
    let canonical = AgentFilter {
        states: vec![
            AgentStateKind::Registered,
            AgentStateKind::Provisional,
            AgentStateKind::Established,
        ],
    };
    assert!(canonical.matches(&live));
    assert!(!canonical.matches(&merged));
    assert!(AgentFilter::default().matches(&merged));
}

#[test]
fn alert_rule_filter_matches_status() {
    let rule = |status| AlertRuleDef {
        id: AlertRuleId::from_ulid(1),
        rule: AlertRule::NewChannel,
        status,
    };
    let disabled_only = AlertRuleFilter {
        statuses: vec![RuleStatus::Disabled],
        stale: None,
    };
    assert!(disabled_only.matches(&rule(RuleStatus::Disabled)));
    assert!(!disabled_only.matches(&rule(RuleStatus::Enabled)));
    assert!(AlertRuleFilter::default().matches(&rule(RuleStatus::Disabled)));
}

#[test]
fn alert_rule_filter_matches_staleness_apart_from_status() {
    let last = WatchedTopics {
        version: crate::aggregates::topic::TopicModelVersion(1),
        topics: NonEmpty::new(topic(1)),
    };
    let watched = |watch, status| AlertRuleDef {
        id: AlertRuleId::from_ulid(2),
        rule: AlertRule::WatchedTopic {
            watch,
            remap_threshold: crate::support::Similarity::new(0.8).expect("in range"),
        },
        status,
    };
    let stale = TopicWatch::Stale {
        last: last.clone(),
        unmapped_in: crate::aggregates::topic::TopicModelVersion(2),
        unmapped: NonEmpty::new(topic(1)),
    };
    let stale_only = AlertRuleFilter {
        statuses: Vec::new(),
        stale: Some(true),
    };
    // A disabled stale rule is still stale.
    assert!(stale_only.matches(&watched(stale.clone(), RuleStatus::Disabled)));
    assert!(stale_only.matches(&watched(stale.clone(), RuleStatus::Enabled)));
    assert!(!stale_only.matches(&watched(
        TopicWatch::Current(last.clone()),
        RuleStatus::Enabled
    )));
    let evaluating = AlertRuleFilter {
        statuses: vec![RuleStatus::Enabled],
        stale: Some(false),
    };
    assert!(evaluating.matches(&watched(TopicWatch::Current(last), RuleStatus::Enabled)));
    assert!(!evaluating.matches(&watched(stale, RuleStatus::Enabled)));
}

#[test]
fn false_detections_are_kept_unless_excluded() {
    let route = Route::Channel(channel(1));
    let mut judged = subject(1, 2, &route, None);
    judged.false_detection = true;
    let unjudged = subject(1, 2, &route, None);
    let include = TopologyFilter::default();
    assert!(include.admits(&judged, identity));
    let exclude = TopologyFilter {
        false_detections: FalseDetections::Exclude,
        ..TopologyFilter::default()
    };
    assert!(!exclude.admits(&judged, identity));
    assert!(exclude.admits(&unjudged, identity));
}

#[test]
fn default_filter_reads_the_current_topic_version() {
    assert_eq!(
        TopologyFilter::default().topic_version,
        TopicVersionSelector::Current
    );
}

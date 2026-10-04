//! The scenarios the world contains: the cast, merges, channels, topics,
//! alerts, rules and operator history.

use std::collections::HashSet;

use crosstalk_spec::aggregates::alert::AlertSubject;
use crosstalk_spec::aggregates::topic::{Assignment, TopicModelVersion};
use crosstalk_spec::derived::flow::channel::detection::{DeclaredDetection, TrafficDetection};
use crosstalk_spec::derived::flow::channel::policy::Policy;
use crosstalk_spec::derived::flow::channel::{ChannelOrigin, DeclaredHistory};
use crosstalk_spec::derived::flow::resource::Locator;
use crosstalk_spec::observed::agent::MergeAuthor;
use crosstalk_spec::observed::client::HarnessFamily;

use super::super::FixtureBackend;
use super::super::world::ChannelKey;
use super::shared;
use crosstalk_spec::aggregates::alert::{
    AlertRule, AlertRuleKind, AlertState, BuiltinRule, RuleStatus, StaleReason, SuppressReason,
    TopicWatch, WatchedTopics,
};
use crosstalk_spec::derived::flow::verdict::Verdict;
use crosstalk_spec::observed::agent::AgentState;

fn state_of(b: &FixtureBackend) -> tokio::sync::RwLockReadGuard<'_, super::super::store::State> {
    b.state.blocking_read()
}

#[test]
fn cast_covers_every_family_state_and_scenario() {
    let b = shared();
    let state = state_of(b);
    let canonical = state
        .identity
        .agents()
        .filter(|a| !matches!(a.state, AgentState::Merged(_)))
        .count();
    assert_eq!(canonical, 40);
    for kind in ["Registered", "Provisional", "Established"] {
        assert!(
            state
                .identity
                .agents()
                .any(|a| format!("{:?}", a.state).starts_with(kind)),
            "{kind}"
        );
    }
    assert!(state.identity.agents().any(|a| a.label.is_some()));
    assert!(
        state
            .identity
            .agents()
            .filter(|a| a.parent.is_some())
            .count()
            >= 10
    );

    // Impersonation: pi and oh-my-pi agents claiming Claude Code.
    let cast = &b.world.scenario.cast;
    for key in ["pi0", "pi1", "omp0", "omp2"] {
        let id = cast.get(key).expect("agent");
        let claims = b.world.claims.get(&id).expect("claims");
        assert!(
            claims
                .entries()
                .iter()
                .any(|c| c.claim.family == HarnessFamily::ClaudeCode),
            "{key}"
        );
        assert!(
            claims
                .entries()
                .iter()
                .any(|c| matches!(c.claim.family, HarnessFamily::Pi | HarnessFamily::OhMyPi)),
            "{key}"
        );
    }
    let families: HashSet<String> = b
        .world
        .claims
        .values()
        .flat_map(|set| set.entries())
        .map(|c| format!("{:?}", c.claim.family))
        .collect();
    assert_eq!(families.len(), 5, "{families:?}");
}

#[test]
fn merge_history_has_resolver_operator_repointed_reverted_and_veto() {
    let b = shared();
    let state = state_of(b);
    let cast = &b.world.scenario.cast;
    let merges = state.identity.merges();
    assert!(merges.iter().any(|m| m.by() == MergeAuthor::Resolver));
    assert!(
        merges
            .iter()
            .any(|m| matches!(m.by(), MergeAuthor::Operator(_)))
    );
    let repointing = merges
        .iter()
        .find(|m| !m.repointed().is_empty())
        .expect("repointing merge");
    assert_eq!(repointing.repointed(), [cast.get("al2").expect("al2")]);
    let reverted = merges
        .iter()
        .find(|m| m.reverted().is_some())
        .expect("reverted merge");
    assert!(
        !state.identity.is_merged(reverted.source()),
        "a reverted merge's agent is active again"
    );
    let (from, into) = (reverted.source(), reverted.target());
    assert!(
        state
            .identity
            .vetoes()
            .iter()
            .any(|v| (v.a(), v.b()) == (from.min(into), from.max(into)))
    );
    // Every alias resolves to a canonical agent that is not merged, and
    // is merged exactly by the one unreverted record naming it as source.
    for agent in state.identity.agents() {
        let canonical = state.identity.canonical(agent.id);
        assert!(!state.identity.is_merged(canonical));
        let merged_by: Vec<_> = merges
            .iter()
            .filter(|m| m.source() == agent.id && m.reverted().is_none())
            .map(|m| m.id())
            .collect();
        match &agent.state {
            AgentState::Merged(merged) => assert_eq!(merged_by, [merged.merge]),
            _ => assert!(merged_by.is_empty()),
        }
    }
    assert_eq!(
        state.identity.canonical(cast.get("al2").expect("al2")),
        cast.get("pi2").expect("pi2")
    );
}

#[test]
fn channels_cover_every_origin_detection_and_policy() {
    let b = shared();
    let state = state_of(b);
    let ch = |key| {
        let id = b.world.scenario.channel(key).expect("channel");
        state.channels.get(&id).expect("record").channel().clone()
    };
    use ChannelKey as K;
    for key in [K::InternalWiki, K::Monorepo, K::IssueTracker] {
        let r = ch(key);
        assert!(matches!(r.policy, Policy::Sanctioned(_)), "{key:?}");
        assert!(matches!(
            r.origin,
            ChannelOrigin::Declared {
                history: DeclaredHistory::BeforeTraffic(DeclaredDetection::InUse(_)),
                ..
            }
        ));
    }
    assert!(matches!(
        ch(K::DesignDocs).origin,
        ChannelOrigin::Declared {
            history: DeclaredHistory::BeforeTraffic(DeclaredDetection::AwaitingTraffic),
            ..
        }
    ));
    let unused = ch(K::ReleaseBucket);
    assert!(matches!(
        unused.origin,
        ChannelOrigin::Declared {
            history: DeclaredHistory::BeforeTraffic(DeclaredDetection::Unused { .. }),
            ..
        }
    ));
    assert!(matches!(unused.policy, Policy::Sanctioned(_)));
    let discovered = |key| match ch(key).origin {
        ChannelOrigin::Discovered { detection, .. } => detection,
        ChannelOrigin::Declared { .. } | ChannelOrigin::Superseded { .. } => {
            panic!("{key:?} is not discovered")
        }
    };
    assert!(matches!(
        discovered(K::HijackedWiki),
        TrafficDetection::Active { .. }
    ));
    assert!(matches!(
        ch(K::HijackedWiki).policy,
        Policy::Unreviewed(None)
    ));
    assert!(matches!(ch(K::Pastebin).policy, Policy::Unsanctioned(_)));
    assert!(matches!(
        ch(K::McpMemory).policy,
        Policy::Unreviewed(Some(_))
    ));
    assert!(matches!(ch(K::SharedFile).policy, Policy::Sanctioned(_)));
    assert!(matches!(
        discovered(K::Gist),
        TrafficDetection::Dormant { .. }
    ));
    assert!(matches!(
        discovered(K::S3Handoff),
        TrafficDetection::Active { .. }
    ));
    assert!(matches!(
        discovered(K::SelfNotes),
        TrafficDetection::Dormant { .. }
    ));
    // The key-value entry only cc7 uses is a resource on no channel.
    let lone = b.world.scenario.lone_resource;
    assert!(b.world.resource(lone).is_some());
    assert!(!b.world.resource_channel.contains_key(&lone));
    let old = ch(K::OldTeamNotes);
    let notes = ch(K::TeamNotes);
    assert_eq!(old.origin.supersession().map(|s| s.by), Some(notes.id));
    assert!(matches!(
        notes.origin,
        ChannelOrigin::Declared {
            history: DeclaredHistory::Promoted { .. },
            ..
        }
    ));
    assert!(matches!(notes.policy, Policy::Sanctioned(_)));

    // Every locator variant appears among resources.
    let w = &b.world;
    assert!(
        w.resources
            .iter()
            .any(|r| matches!(r.locator, Locator::Url { .. }))
    );
    assert!(
        w.resources
            .iter()
            .any(|r| matches!(r.locator, Locator::File { .. }))
    );
    assert!(
        w.resources
            .iter()
            .any(|r| matches!(r.locator, Locator::Mcp { .. }))
    );
    assert!(
        w.resources
            .iter()
            .any(|r| matches!(r.locator, Locator::Opaque { .. }))
    );
    // The hijacked wiki's seed is on wiki.example.org.
    let seed = match ch(K::HijackedWiki).origin {
        ChannelOrigin::Discovered { seed, .. } => seed.resource,
        ChannelOrigin::Declared { .. } | ChannelOrigin::Superseded { .. } => unreachable!(),
    };
    assert!(matches!(
        &w.resource(seed).expect("seed").locator,
        Locator::Url { host, .. } if host.0 == "wiki.example.org"
    ));
}

#[test]
fn topic_versions_and_remaps() {
    use crosstalk_spec::aggregates::topic_history::TopicVersionStatusKind;
    use crosstalk_spec::support::Similarity;

    let w = &shared().world;
    let state = shared().state.try_read().expect("no action runs");
    let history = &state.catalog;
    let versions: Vec<u32> = history.versions().iter().map(|v| v.version().0).collect();
    assert_eq!(versions, vec![0, 1, 2]);
    assert_eq!(history.active().version(), TopicModelVersion(2));
    let v0 = history.get(TopicModelVersion(0)).expect("v0");
    assert_eq!(v0.status().kind(), TopicVersionStatusKind::Superseded);
    assert!(!v0.retention().is_retained(), "retention dropped v0");
    let v1 = history.get(TopicModelVersion(1)).expect("v1");
    assert!(v1.retention().pin().is_some(), "v1 is pinned");
    assert_eq!(w.topics.topics_of(TopicModelVersion(0)).count(), 0);
    assert_eq!(w.topics.topics_of(TopicModelVersion(1)).count(), 6);
    assert_eq!(w.topics.topics_of(TopicModelVersion(2)).count(), 10);
    for topic in &w.topics.topics {
        let info = history.get(topic.version).expect("version");
        assert_eq!(Some(topic.fitted_at), info.fitted_at());
    }
    let lineage = w.topics.lineage(TopicModelVersion(1)).expect("v1 lineage");
    assert_eq!(lineage.to(), TopicModelVersion(2));
    assert_eq!(lineage.entries().len(), 6);
    let threshold = Similarity::new(super::super::world::topics::REMAP_THRESHOLD).expect("t");
    let unmapped: Vec<_> = lineage
        .entries()
        .iter()
        .filter(|e| e.best().is_none_or(|best| best.similarity < threshold))
        .collect();
    assert_eq!(unmapped.len(), 1, "exactly one v1 topic has no v2 match");
    let chatter = w
        .topics
        .topics_of(TopicModelVersion(1))
        .find(|t| t.label == "Engineering chatter")
        .expect("chatter");
    assert_eq!(unmapped[0].topic(), chatter.id);
    assert!(
        !unmapped[0].others().is_empty(),
        "its near misses are listed above the floor"
    );
    let v0 = w.topics.lineage(TopicModelVersion(0)).expect("v0 lineage");
    assert!(v0.entries().is_empty(), "v0 had no topics to carry over");
    for topic in &w.topics.topics {
        assert!(!topic.terms.is_empty());
        assert_eq!(topic.centroid.model(), &w.topics.model);
    }
    // Outliers exist under every fitted version.
    for v in [1, 2] {
        assert!(
            w.transmissions
                .iter()
                .any(|t| t.assignment(TopicModelVersion(v)) == Some(Assignment::Outlier))
        );
    }
}

#[test]
fn alerts_cover_every_state_reason_subject_and_dedup() {
    let state = state_of(shared());
    let alerts = &state.alerts;
    assert!(alerts.iter().any(|a| a.state == AlertState::Open));
    assert!(
        alerts
            .iter()
            .any(|a| matches!(a.state, AlertState::Acknowledged { .. }))
    );
    assert!(
        alerts
            .iter()
            .any(|a| matches!(a.state, AlertState::Resolved { .. }))
    );
    for reason in [
        SuppressReason::ChannelSanctioned,
        SuppressReason::RuleDisabled,
        SuppressReason::OperatorRejected,
    ] {
        assert!(
            alerts.iter().any(
                |a| matches!(a.state, AlertState::Suppressed { reason: r, .. } if r == reason)
            ),
            "{reason:?}"
        );
    }
    assert!(alerts.iter().any(|a| a.occurrences > 1));
    assert!(
        alerts
            .iter()
            .any(|a| matches!(a.subject, AlertSubject::Channel(_)))
    );
    assert!(
        alerts
            .iter()
            .any(|a| matches!(a.subject, AlertSubject::Transmission(_)))
    );
    assert!(
        alerts
            .iter()
            .any(|a| matches!(a.subject, AlertSubject::Agent(_)))
    );
    // Every alert references a rule that exists.
    let rules: HashSet<_> = state.rules.iter().map(|r| r.id()).collect();
    assert!(alerts.iter().all(|a| rules.contains(&a.rule)));
}

#[test]
fn rules_and_sinks() {
    let b = shared();
    let state = state_of(b);
    let builtins: Vec<BuiltinRule> = state
        .rules
        .iter()
        .filter_map(|r| match r.rule() {
            AlertRule::Builtin(b) => Some(*b),
            AlertRule::User { .. } => None,
        })
        .collect();
    assert_eq!(builtins, BuiltinRule::ALL.to_vec(), "each once, in order");
    assert!(
        state
            .rules
            .iter()
            .filter(|r| matches!(r.rule(), AlertRule::Builtin(_)))
            .all(|r| r.sinks.len() == b.world.sinks.len()),
        "built-ins deliver to every sink"
    );
    let v2 = TopicModelVersion(2);
    assert!(state.rules.iter().any(|r| r.status == RuleStatus::Enabled
        && r.stale_reason().is_none()
        && watched_topics(r).is_some_and(|w| w.version == v2)));
    assert!(
        state
            .rules
            .iter()
            .any(|r| r.kind() == AlertRuleKind::SemanticQuery)
    );
    assert!(state.rules.iter().any(|r| r.status == RuleStatus::Disabled));
    let sinks = &b.world.sinks;
    assert_eq!(sinks.len(), 3);
    assert!(
        sinks
            .iter()
            .any(|s| matches!(s.last_delivery, Some(Err(_))))
    );
    assert!(
        sinks
            .iter()
            .filter(|s| matches!(s.last_delivery, Some(Ok(_))))
            .count()
            == 2
    );
}

/// The v1 rule is stale exactly as `TopicLineage::remap` over the stored
/// lineage leaves it: "Engineering chatter" unmapped in v2, while it is
/// still enabled (it went stale while enabled).
#[test]
fn the_v1_rule_is_stale_as_its_lineage_says() {
    use crosstalk_spec::aggregates::alert::ContentRule;

    let b = shared();
    let state = state_of(b);
    let stale: Vec<_> = state
        .rules
        .iter()
        .filter(|r| r.stale_reason().is_some())
        .collect();
    assert_eq!(stale.len(), 1);
    let rule = stale[0];
    assert_eq!(rule.status, RuleStatus::Enabled, "staleness is separate");
    let AlertRule::User {
        content:
            ContentRule::WatchedTopic {
                watch: TopicWatch::Stale { last, .. },
                remap_threshold,
            },
        ..
    } = rule.rule()
    else {
        panic!("a stale watched-topic rule: {rule:?}")
    };
    let lineage = b
        .world
        .topics
        .lineage(TopicModelVersion(1))
        .expect("v1 lineage");
    let remapped = lineage.remap(last, *remap_threshold).expect("remap");
    let chatter = b
        .world
        .topics
        .topics_of(TopicModelVersion(1))
        .find(|t| t.label == "Engineering chatter")
        .expect("chatter")
        .id;
    assert_eq!(
        Some(remapped.clone()),
        match rule.rule() {
            AlertRule::User {
                content: ContentRule::WatchedTopic { watch, .. },
                ..
            } => Some(watch.clone()),
            AlertRule::User { .. } | AlertRule::Builtin(_) => None,
        }
    );
    assert_eq!(
        *last,
        WatchedTopics {
            version: TopicModelVersion(1),
            topics: crosstalk_spec::support::NonEmpty::new(chatter),
        }
    );
    assert!(matches!(
        rule.stale_reason(),
        Some(StaleReason::TopicsUnmapped { version, topics })
            if version == TopicModelVersion(2) && topics.iter().eq([&chatter])
    ));
}

#[test]
fn verdicts_audit_and_dead_letters() {
    let b = shared();
    let state = state_of(b);
    let records = || state.verdicts.values().flat_map(|log| log.records());
    assert!(records().any(|v| v.verdict() == Some(Verdict::Genuine)));
    assert!(records().any(|v| v.verdict() == Some(Verdict::FalseDetection)));
    assert!(
        records().any(|v| v.verdict().is_none()),
        "one verdict is withdrawn"
    );
    assert!(
        state
            .verdicts
            .iter()
            .all(|(id, log)| log.transmission() == *id && log.revision().is_some()),
        "each log is its transmission's, and holds a record"
    );
    use crosstalk_spec::interfaces::l8_surface::audit::{
        AuditAuthor, AuditBody, AuditOutcome, ConfigChange,
    };
    use crosstalk_spec::interfaces::l8_surface::operators::AccessMode;

    let entries = state.audit.entries();
    assert!(
        (200..1_000).contains(&entries.len()),
        "a few hundred entries"
    );
    let operators: HashSet<_> = entries
        .iter()
        .filter_map(|e| match e.by() {
            AuditAuthor::Operator(op) => Some(op),
            AuditAuthor::Config => None,
        })
        .collect();
    assert_eq!(operators.len(), 2);
    let changes: Vec<&ConfigChange> = entries
        .iter()
        .filter_map(|e| match &e.body {
            AuditBody::Config(record) => Some(&record.change),
            _ => None,
        })
        .collect();
    assert!(changes.contains(&&ConfigChange::SetAccessMode(AccessMode::Authenticated)));
    let defined: Vec<&str> = changes
        .iter()
        .filter_map(|c| match c {
            ConfigChange::SetOperator { name, .. } => Some(name.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(defined, ["researcher", "oncall"]);
    assert_eq!(
        changes
            .iter()
            .filter(|c| matches!(c, ConfigChange::DeclareChannel { .. }))
            .count(),
        5
    );
    let refused: Vec<&AuditOutcome> = entries
        .iter()
        .filter_map(|e| match &e.body {
            AuditBody::Operator(record) => Some(record.outcome()),
            _ => None,
        })
        .filter(|o| {
            matches!(
                o,
                AuditOutcome::Rejected(_) | AuditOutcome::Forbidden { .. }
            )
        })
        .collect();
    assert_eq!(refused.len(), 2, "the two refused actions");
    let ids: HashSet<_> = entries.iter().map(|e| e.id).collect();
    assert_eq!(ids.len(), entries.len(), "ids are unique");
    assert_eq!(state.dead_letters.len(), 4);
}

/// A watched-topic rule's topics: its current ones, or the ones it last
/// held before it went stale (as the UI's rule list reads them).
fn watched_topics(
    rule: &crosstalk_spec::aggregates::alert::AlertRuleDef,
) -> Option<&crosstalk_spec::aggregates::alert::WatchedTopics> {
    use crosstalk_spec::aggregates::alert::{AlertRule, ContentRule, TopicWatch};
    match rule.rule() {
        AlertRule::User {
            content: ContentRule::WatchedTopic { watch, .. },
            ..
        } => Some(match watch {
            TopicWatch::Current(topics) | TopicWatch::Stale { last: topics, .. } => topics,
        }),
        _ => None,
    }
}

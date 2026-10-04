//! The scenarios the world contains: the cast, merges, channels, topics,
//! alerts, rules and operator history.

use std::collections::HashSet;

use crosstalk_spec::aggregates::alert::{AlertState, AlertSubject, SuppressReason};
use crosstalk_spec::aggregates::topic::{Assignment, TopicModelVersion};
use crosstalk_spec::derived::flow::channel::ChannelOrigin;
use crosstalk_spec::derived::flow::channel::detection::{DeclaredDetection, TrafficDetection};
use crosstalk_spec::derived::flow::channel::policy::Policy;
use crosstalk_spec::derived::flow::resource::Locator;
use crosstalk_spec::observed::agent::{AgentState, MergeAuthor};
use crosstalk_spec::observed::client::HarnessFamily;

use super::super::FixtureBackend;
use super::super::world::ChannelKey;
use super::shared;
use crate::contract::rules::{BuiltinRule, RuleKind, RuleStatus, StaleReason, UserRule};
use crate::contract::verdict::Verdict;

fn state_of(b: &FixtureBackend) -> tokio::sync::RwLockReadGuard<'_, super::super::store::State> {
    b.state.blocking_read()
}

#[test]
fn cast_covers_every_family_state_and_scenario() {
    let b = shared();
    let state = state_of(b);
    let canonical = state
        .agents
        .values()
        .filter(|r| !matches!(r.agent.state, AgentState::Merged { .. }))
        .count();
    assert_eq!(canonical, 40);
    for kind in ["Registered", "Provisional", "Established"] {
        assert!(
            state
                .agents
                .values()
                .any(|r| format!("{:?}", r.agent.state).starts_with(kind)),
            "{kind}"
        );
    }
    assert!(state.agents.values().any(|r| r.label.is_some()));
    assert!(
        state
            .agents
            .values()
            .filter(|r| r.agent.parent.is_some())
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
                .iter()
                .any(|c| c.claim.family == HarnessFamily::ClaudeCode),
            "{key}"
        );
        assert!(
            claims
                .iter()
                .any(|c| matches!(c.claim.family, HarnessFamily::Pi | HarnessFamily::OhMyPi)),
            "{key}"
        );
    }
    let families: HashSet<String> = b
        .world
        .claims
        .values()
        .flatten()
        .map(|c| format!("{:?}", c.claim.family))
        .collect();
    assert_eq!(families.len(), 5, "{families:?}");
}

#[test]
fn merge_history_has_resolver_operator_repointed_reverted_and_veto() {
    let b = shared();
    let state = state_of(b);
    let cast = &b.world.scenario.cast;
    assert!(state.merges.iter().any(|m| m.by == MergeAuthor::Resolver));
    assert!(
        state
            .merges
            .iter()
            .any(|m| matches!(m.by, MergeAuthor::Operator(_)))
    );
    let repointing = state
        .merges
        .iter()
        .find(|m| !m.repointed.is_empty())
        .expect("repointing merge");
    assert_eq!(repointing.repointed, vec![cast.get("al2").expect("al2")]);
    let reverted = state
        .merges
        .iter()
        .find(|m| m.reverted.is_some())
        .expect("reverted merge");
    assert!(
        !state.is_merged(reverted.from),
        "a reverted merge's agent is active again"
    );
    assert!(
        state
            .vetoes
            .iter()
            .any(|v| v.a == reverted.from && v.b == reverted.into)
    );
    // Every alias resolves to a canonical agent that is not merged.
    for record in state.agents.values() {
        let canonical = state.canonical_agent(record.agent.id);
        assert!(!state.is_merged(canonical));
    }
    assert_eq!(
        state.canonical_agent(cast.get("al2").expect("al2")),
        cast.get("pi2").expect("pi2")
    );
}

#[test]
fn channels_cover_every_origin_detection_and_policy() {
    let b = shared();
    let state = state_of(b);
    let ch = |key| {
        let id = b.world.scenario.channel(key).expect("channel");
        state.channels.get(&id).expect("record").clone()
    };
    use ChannelKey as K;
    for key in [K::InternalWiki, K::Monorepo, K::IssueTracker] {
        let r = ch(key);
        assert!(matches!(r.channel.policy, Policy::Sanctioned(_)), "{key:?}");
        assert!(matches!(
            r.channel.origin,
            ChannelOrigin::Declared {
                detection: DeclaredDetection::InUse(_),
                ..
            }
        ));
    }
    assert!(matches!(
        ch(K::DesignDocs).channel.origin,
        ChannelOrigin::Declared {
            detection: DeclaredDetection::AwaitingTraffic,
            ..
        }
    ));
    let unused = ch(K::ReleaseBucket);
    assert!(matches!(
        unused.channel.origin,
        ChannelOrigin::Declared {
            detection: DeclaredDetection::Unused { .. },
            ..
        }
    ));
    assert!(matches!(unused.channel.policy, Policy::Sanctioned(_)));
    let discovered = |key| match ch(key).channel.origin {
        ChannelOrigin::Discovered { detection, .. } => detection,
        ChannelOrigin::Declared { .. } => panic!("{key:?} is declared"),
    };
    assert!(matches!(
        discovered(K::HijackedWiki),
        TrafficDetection::Active { .. }
    ));
    assert!(matches!(
        ch(K::HijackedWiki).channel.policy,
        Policy::Unreviewed(None)
    ));
    assert!(matches!(
        ch(K::Pastebin).channel.policy,
        Policy::Unsanctioned(_)
    ));
    assert!(matches!(
        ch(K::McpMemory).channel.policy,
        Policy::Unreviewed(Some(_))
    ));
    assert!(matches!(
        ch(K::SharedFile).channel.policy,
        Policy::Sanctioned(_)
    ));
    assert!(matches!(
        discovered(K::Gist),
        TrafficDetection::Dormant { .. }
    ));
    assert!(matches!(
        discovered(K::KvScratch),
        TrafficDetection::Observed { .. }
    ));
    assert!(matches!(
        discovered(K::S3Handoff),
        TrafficDetection::Candidate { .. }
    ));
    let old = ch(K::OldTeamNotes);
    let notes = b.world.scenario.channel(K::TeamNotes).expect("notes");
    assert_eq!(old.superseded.map(|s| s.into), Some(notes));

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
    let seed = match ch(K::HijackedWiki).channel.origin {
        ChannelOrigin::Discovered { seed, .. } => seed,
        ChannelOrigin::Declared { .. } => unreachable!(),
    };
    assert!(matches!(
        &w.resource(seed).expect("seed").locator,
        Locator::Url { host, .. } if host.0 == "wiki.example.org"
    ));
}

#[test]
fn topic_versions_and_remaps() {
    let w = &shared().world;
    let versions: Vec<u32> = w.topics.versions.iter().map(|v| v.version.0).collect();
    assert_eq!(versions, vec![0, 1, 2]);
    assert_eq!(w.topics.topics_of(TopicModelVersion(0)).count(), 0);
    assert_eq!(w.topics.topics_of(TopicModelVersion(1)).count(), 6);
    assert_eq!(w.topics.topics_of(TopicModelVersion(2)).count(), 10);
    let remap = w
        .topics
        .remaps
        .iter()
        .find(|r| r.from == TopicModelVersion(1))
        .expect("v1 remap");
    let unmapped: Vec<_> = remap.remaps.iter().filter(|r| r.to.is_none()).collect();
    assert_eq!(unmapped.len(), 1, "exactly one v1 topic has no v2 match");
    assert_eq!(remap.remaps.len(), 6);
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
    let rules: HashSet<_> = state.rules.iter().map(|r| r.id).collect();
    assert!(alerts.iter().all(|a| rules.contains(&a.rule)));
}

#[test]
fn rules_and_sinks() {
    let b = shared();
    let state = state_of(b);
    let builtins: HashSet<BuiltinRule> = state
        .rules
        .iter()
        .filter_map(|r| match r.rule {
            RuleKind::Builtin(b) => Some(b),
            RuleKind::User(_) => None,
        })
        .collect();
    assert_eq!(builtins, BuiltinRule::ALL.into_iter().collect());
    assert!(state.rules.iter().any(|r| matches!(
        (&r.rule, &r.status),
        (RuleKind::User(UserRule::WatchedTopic { version, .. }), RuleStatus::Enabled) if version.0 == 2
    )));
    assert!(state.rules.iter().any(|r| matches!(
        r.status,
        RuleStatus::Stale(StaleReason::TopicsUnmapped { .. })
    )));
    assert!(
        state
            .rules
            .iter()
            .any(|r| matches!(r.rule, RuleKind::User(UserRule::SemanticQuery { .. })))
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

#[test]
fn verdicts_audit_and_dead_letters() {
    let b = shared();
    let state = state_of(b);
    assert!(
        state
            .verdicts
            .iter()
            .any(|v| v.verdict == Some(Verdict::Genuine))
    );
    assert!(
        state
            .verdicts
            .iter()
            .any(|v| v.verdict == Some(Verdict::FalseDetection))
    );
    assert!(
        state.verdicts.iter().any(|v| v.verdict.is_none()),
        "one verdict is withdrawn"
    );
    let operators: HashSet<_> = state
        .audit
        .iter()
        .filter_map(|r| match r.entry.by {
            crate::contract::research::Actor::Operator(op) => Some(op),
            crate::contract::research::Actor::Config => None,
        })
        .collect();
    assert_eq!(operators.len(), 2);
    assert!(
        state
            .audit
            .iter()
            .any(|r| matches!(r.entry.by, crate::contract::research::Actor::Config))
    );
    assert!(state.audit.iter().any(|r| matches!(
        r.entry.outcome,
        crate::contract::research::AuditOutcome::Rejected(_)
    )));
    assert!(
        state
            .audit
            .windows(2)
            .all(|w| (w[0].entry.at, w[0].entry.id) <= (w[1].entry.at, w[1].entry.id))
    );
    assert!((3..=5).contains(&state.dead_letters.len()));
}

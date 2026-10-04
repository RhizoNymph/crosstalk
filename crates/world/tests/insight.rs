//! Topics, rules and alerts, read back through L6's read traits: three
//! topic versions with their stored fits and lineage, v0 dropped by
//! retention and v1 pinned, the stale rule the re-fit left, every rule
//! state, alerts in every state and suppress reason with deduplicated
//! occurrences, and the projection jobs.

mod support;

use std::collections::BTreeSet;

use crosstalk_spec::aggregates::alert::{
    AlertState, AlertSubject, BuiltinRule, RuleStatus, StaleReason, SuppressReason,
};
use crosstalk_spec::aggregates::projection::ProjectionStatusKind;
use crosstalk_spec::aggregates::retention::Retention;
use crosstalk_spec::aggregates::topic::TopicModelVersion;
use crosstalk_spec::aggregates::topic_history::TopicVersionStatusKind;
use crosstalk_spec::interfaces::l6_analysis::TopicCatalog;
use crosstalk_spec::interfaces::l6_analysis::alerts::AlertReads;
use crosstalk_spec::interfaces::l7_topology::EdgeStore;
use crosstalk_world::{JobKey, RuleKey};

use support::{read, run, shared};

type Result<T = ()> = std::result::Result<T, String>;

const V0: TopicModelVersion = TopicModelVersion(0);
const V1: TopicModelVersion = TopicModelVersion(1);
const V2: TopicModelVersion = TopicModelVersion(2);

#[test]
fn three_topic_versions_v0_dropped_v1_pinned_v2_active() -> Result {
    let seeded = shared();
    let history = run(seeded.stores.catalog.versions()).map_err(|e| format!("{e:?}"))?;
    let versions: Vec<_> = history.versions().iter().map(|v| v.version()).collect();
    assert_eq!(versions, [V0, V1, V2]);
    let info = |v| history.get(v).ok_or(format!("{v:?}"));
    assert!(matches!(info(V0)?.retention(), Retention::Dropped { .. }));
    assert!(info(V1)?.retention().pin().is_some(), "v1 pinned");
    assert_eq!(
        info(V1)?.status().kind(),
        TopicVersionStatusKind::Superseded
    );
    assert_eq!(info(V2)?.status().kind(), TopicVersionStatusKind::Active);
    assert_eq!(history.active().version(), V2);
    assert_eq!(seeded.scenario.topics(V1).len(), 6);
    assert_eq!(seeded.scenario.topics(V2).len(), 10);
    Ok(())
}

#[test]
fn the_lineage_from_v1_leaves_engineering_chatter_unmapped() -> Result {
    let seeded = shared();
    let lineage = run(seeded.stores.catalog.lineage(V1))
        .map_err(|e| format!("{e:?}"))?
        .ok_or("a lineage from v1")?;
    assert_eq!(lineage.to(), V2);
    let threshold = seeded.world.config().rules.default_remap_threshold;
    let unmapped = seeded.scenario.unmapped_topic().ok_or("unmapped topic")?;
    for entry in lineage.entries() {
        let best = entry.best().ok_or("a best link")?;
        if entry.topic() == unmapped {
            assert!(best.similarity < threshold, "below the remap threshold");
        } else {
            assert!(
                best.similarity >= threshold,
                "{:?} carries over",
                entry.topic()
            );
        }
    }
    Ok(())
}

#[test]
fn sizes_are_stored_for_retained_versions_and_frozen_for_the_dropped_one() -> Result {
    let seeded = shared();
    run(async {
        let v2 = seeded
            .stores
            .catalog
            .sizes(V2, None)
            .await
            .map_err(|e| format!("{e:?}"))?;
        assert_eq!(v2.topics().len(), 10);
        let v1 = seeded
            .stores
            .catalog
            .sizes(V1, None)
            .await
            .map_err(|e| format!("{e:?}"))?;
        assert_eq!(v1.topics().len(), 6);
        // v0 (unfitted) assigned every transmission as an outlier; its
        // all-time sizes were frozen when retention dropped it.
        let v0 = seeded
            .stores
            .catalog
            .sizes(V0, None)
            .await
            .map_err(|e| format!("{e:?}"))?;
        assert!(v0.topics().is_empty());
        assert!(v0.outliers().is_some());
        Ok(())
    })
}

#[test]
fn rules_every_state_built_ins_first_and_the_stale_one_still_enabled() -> Result {
    let seeded = shared();
    let rules = run(read::rules(seeded))?;
    assert_eq!(rules.len(), 9, "five built-ins and four user rules");
    let builtins: Vec<_> = rules.iter().take(5).map(|r| r.id()).collect();
    assert_eq!(builtins, BuiltinRule::ALL.map(|r| r.id()));
    let rule = |key| {
        let id = seeded.scenario.rule(key).ok_or(format!("{key:?}"))?;
        rules
            .iter()
            .find(|r| r.id() == id)
            .ok_or(format!("{key:?} listed"))
    };
    let stale = rule(RuleKey::Stale)?;
    assert_eq!(stale.status, RuleStatus::Enabled, "still enabled");
    let Some(StaleReason::TopicsUnmapped { version, topics }) = stale.stale_reason() else {
        return Err("the v1 rule is stale with unmapped topics".to_owned());
    };
    assert_eq!(version, V2);
    assert_eq!(Some(*topics.first()), seeded.scenario.unmapped_topic());
    assert!(rule(RuleKey::Watch)?.stale_reason().is_none());
    assert!(rule(RuleKey::Semantic)?.stale_reason().is_none());
    assert_eq!(rule(RuleKey::Refunds)?.status, RuleStatus::Disabled);
    let version = run(seeded.stores.alerts.rule_version()).map_err(|e| format!("{e:?}"))?;
    assert_eq!(version, V2);
    Ok(())
}

#[test]
fn alerts_cover_every_state_suppress_reason_subject_and_dedup() -> Result {
    let seeded = shared();
    let alerts = run(read::alerts(seeded))?;
    assert!(alerts.len() >= 300, "{} alerts", alerts.len());
    let states: BTreeSet<&str> = alerts
        .iter()
        .map(|a| match a.state {
            AlertState::Open => "open",
            AlertState::Acknowledged { .. } => "acknowledged",
            AlertState::Resolved { .. } => "resolved",
            AlertState::Suppressed { .. } => "suppressed",
        })
        .collect();
    assert_eq!(states.len(), 4, "{states:?}");
    let reasons: BTreeSet<String> = alerts
        .iter()
        .filter_map(|a| match a.state {
            AlertState::Suppressed { reason, .. } => Some(format!("{reason:?}")),
            _ => None,
        })
        .collect();
    for reason in [
        SuppressReason::ChannelSanctioned,
        SuppressReason::RuleDisabled,
        SuppressReason::OperatorRejected,
    ] {
        assert!(
            reasons.contains(&format!("{reason:?}")),
            "{reason:?}: {reasons:?}"
        );
    }
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
    assert!(
        alerts.iter().any(|a| a.occurrences > 1),
        "deduplicated occurrences"
    );
    let rules: BTreeSet<_> = alerts.iter().map(|a| a.rule).collect();
    for builtin in BuiltinRule::ALL {
        assert!(rules.contains(&builtin.id()), "{builtin:?}");
    }
    for key in RuleKey::ALL {
        let id = seeded.scenario.rule(key).ok_or(format!("{key:?}"))?;
        assert!(rules.contains(&id), "{key:?}");
    }
    Ok(())
}

#[test]
fn the_pi_agent_keeps_matching_the_semantic_rule() -> Result {
    let seeded = shared();
    let alerts = run(read::alerts(seeded))?;
    let pi1 = seeded.scenario.agent("pi1").ok_or("pi1")?;
    let semantic = seeded.scenario.rule(RuleKey::Semantic).ok_or("rule")?;
    let alert = alerts
        .iter()
        .find(|a| a.subject == AlertSubject::Agent(pi1))
        .ok_or("an agent alert")?;
    assert_eq!(alert.rule, semantic);
    assert_eq!(alert.occurrences, 7);
    assert_eq!(alert.state, AlertState::Open);
    Ok(())
}

#[test]
fn the_unused_sanctioned_channel_has_its_open_alert() -> Result {
    let seeded = shared();
    let alerts = run(read::alerts(seeded))?;
    let bucket = seeded
        .scenario
        .channel(crosstalk_world::ChannelKey::ReleaseBucket)
        .ok_or("bucket")?;
    assert!(alerts.iter().any(|a| {
        a.rule == BuiltinRule::SanctionedUnused.id()
            && a.subject == AlertSubject::Channel(bucket)
            && a.state == AlertState::Open
    }));
    Ok(())
}

#[test]
fn projection_jobs_one_per_resting_status() -> Result {
    let seeded = shared();
    let jobs = run(read::jobs(seeded))?;
    assert_eq!(jobs.len(), 4);
    for (key, status) in [
        (JobKey::Expired, ProjectionStatusKind::Expired),
        (JobKey::Failed, ProjectionStatusKind::Failed),
        (JobKey::Fitting, ProjectionStatusKind::Fitting),
        (JobKey::Queued, ProjectionStatusKind::Queued),
    ] {
        let id = seeded.scenario.job(key).ok_or(format!("{key:?}"))?;
        let job = jobs
            .iter()
            .find(|j| j.id() == id)
            .ok_or(format!("{key:?} listed"))?;
        assert_eq!(job.status().kind(), status, "{key:?}");
    }
    Ok(())
}

#[test]
fn the_watermark_trails_the_present_by_the_settle_delay() -> Result {
    let seeded = shared();
    let watermark = run(seeded.stores.edges.watermark()).map_err(|e| format!("{e:?}"))?;
    let config = seeded.world.config();
    let settle =
        u64::try_from(config.timing.settle_after().as_micros()).map_err(|e| e.to_string())?;
    let now = seeded.world.anchor().now();
    assert!(watermark.at() <= now);
    assert!(now.as_micros() - watermark.at().as_micros() >= settle);
    assert!(config.bucket_width.is_boundary(watermark.at()));
    Ok(())
}

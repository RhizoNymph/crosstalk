//! Alerts, rules, operators, the present and agents, as the surface reads
//! them from the stores.

use std::collections::BTreeSet;

use crosstalk_spec::aggregates::agents::AgentLookup;
use crosstalk_spec::aggregates::alert::{AlertStateKind, AlertSubject, BuiltinRule};
use crosstalk_spec::aggregates::projection::FrameRetention;
use crosstalk_spec::batch::IdBatch;
use crosstalk_spec::derived::flow::channel::policy::PolicyKind;
use crosstalk_spec::derived::flow::resource::{Host, ResourcePattern};
use crosstalk_spec::ids::{AgentId, AlertId, AlertRuleId};
use crosstalk_spec::interfaces::l7_topology::EdgeStore;
use crosstalk_spec::interfaces::l8_surface::audit::{AuditFilter, AuditSubject};
use crosstalk_spec::interfaces::l8_surface::lists::{AgentFilter, AlertRuleFilter};
use crosstalk_spec::interfaces::l8_surface::{
    ActionOutcome, ActionRequest, AlertFilter, OperatorAction, OperatorActions, QueryApi,
};
use crosstalk_spec::paging::{AlertList, AuditList, PageRequest};
use crosstalk_spec::support::Timestamp;
use crosstalk_testkit::build::ResourceBuilder;

use super::page;
use super::world::{Fixture, Who, config, minute, minutes};

/// INV-379: the alerts filter keeps the listed states, and a channel's
/// alerts.
#[tokio::test]
async fn alert_filter_cases() {
    let fixture = Fixture::new().await;
    let scene = fixture.scene().await;
    let caller = fixture.caller(Who::Admin).await;
    let other = fixture
        .alert(
            BuiltinRule::NewChannel,
            AlertSubject::Agent(scene.a1),
            minute(1),
        )
        .await;
    let on_transmission = fixture
        .alert(
            BuiltinRule::UnreviewedTraffic,
            AlertSubject::Transmission(scene.t1.transmission.id),
            minute(1),
        )
        .await;
    assert!(
        fixture
            .surface
            .act(&caller, OperatorAction::Acknowledge { alert: other })
            .await
            .is_ok()
    );
    let ids = |filter: AlertFilter| {
        let surface = &fixture.surface;
        let caller = caller.clone();
        async move {
            match surface.alerts(&caller, &filter, &page(100)).await {
                Ok(page) => page
                    .items()
                    .iter()
                    .map(|alert| alert.id)
                    .collect::<BTreeSet<_>>(),
                Err(error) => panic!("alerts: {error:?}"),
            }
        }
    };
    let all: BTreeSet<AlertId> = [scene.alert, other, on_transmission].into();
    assert_eq!(ids(AlertFilter::default()).await, all);
    assert_eq!(
        ids(AlertFilter {
            states: vec![AlertStateKind::Open],
            channel: None
        })
        .await,
        [scene.alert, on_transmission].into()
    );
    assert_eq!(
        ids(AlertFilter {
            states: vec![AlertStateKind::Acknowledged],
            channel: None
        })
        .await,
        [other].into()
    );
    // The channel's own alert and the transmission routed through it.
    assert_eq!(
        ids(AlertFilter {
            states: Vec::new(),
            channel: Some(scene.c1)
        })
        .await,
        [scene.alert, on_transmission].into()
    );
    assert!(
        ids(AlertFilter {
            states: vec![AlertStateKind::Resolved],
            channel: None
        })
        .await
        .is_empty()
    );
}

/// INV-667: filtering on a promoted channel finds the alerts stored under
/// the channel it superseded.
#[tokio::test]
async fn alert_filter_resolves_superseded_channels() {
    let fixture = Fixture::new().await;
    let mut scene = fixture.scene().await;
    let resource = ResourceBuilder::new(&mut scene.ids)
        .url("https", "wiki.example", "/b", None)
        .first_seen(minute(1))
        .build();
    let c2 = scene.ids.channel();
    fixture
        .channel(&mut scene.ids, c2, &resource, scene.a2, scene.a3, minute(1))
        .await;
    let on_c2 = fixture
        .alert(
            BuiltinRule::NewChannel,
            AlertSubject::Channel(c2),
            minute(1),
        )
        .await;
    let caller = fixture.caller(Who::Admin).await;
    let promote = OperatorAction::PromoteChannel {
        channel: scene.c1,
        pattern: ResourcePattern::Host(Host("wiki.example".to_owned())),
        policy: PolicyKind::Unreviewed,
        note: None,
    };
    assert!(fixture.surface.act(&caller, promote).await.is_ok());
    let filter = |channel| AlertFilter {
        states: Vec::new(),
        channel: Some(channel),
    };
    let listed = |channel| {
        let surface = &fixture.surface;
        let caller = caller.clone();
        async move {
            match surface.alerts(&caller, &filter(channel), &page(100)).await {
                Ok(page) => page
                    .items()
                    .iter()
                    .map(|alert| alert.id)
                    .collect::<BTreeSet<_>>(),
                Err(error) => panic!("alerts: {error:?}"),
            }
        }
    };
    let both: BTreeSet<AlertId> = [scene.alert, on_c2].into();
    assert_eq!(listed(scene.c1).await, both);
    assert_eq!(listed(c2).await, both);
}

/// INV-702: one alert by id is the listed alert; an unknown id is `None`.
#[tokio::test]
async fn alert_by_id_matches_the_list() {
    let fixture = Fixture::new().await;
    let scene = fixture.scene().await;
    let caller = fixture.caller(Who::Viewer).await;
    let Ok(listed) = fixture
        .surface
        .alerts(&caller, &AlertFilter::default(), &page(100))
        .await
    else {
        panic!("alerts");
    };
    for alert in listed.items() {
        assert_eq!(
            fixture.surface.alert(&caller, alert.id).await,
            Ok(Some(alert.clone()))
        );
    }
    assert!(listed.items().iter().any(|alert| alert.id == scene.alert));
    assert_eq!(
        fixture
            .surface
            .alert(&caller, AlertId::from_ulid(0xABCD))
            .await,
        Ok(None)
    );
}

/// INV-794: one rule by id is the listed rule, built in or user; `None`
/// only for an id no rule had.
#[tokio::test]
async fn alert_rule_is_the_listed_rule() {
    let fixture = Fixture::new().await;
    let caller = fixture.caller(Who::Admin).await;
    let text = match crosstalk_spec::aggregates::alert::RuleQueryText::new("tokens") {
        Ok(text) => text,
        Err(error) => panic!("{error:?}"),
    };
    let (Ok(name), Ok(threshold)) = (
        crosstalk_spec::aggregates::alert::RuleName::new("tokens"),
        crosstalk_spec::support::Similarity::new(0.5),
    ) else {
        panic!("rule");
    };
    let created = fixture
        .surface
        .act(
            &caller,
            OperatorAction::CreateRule {
                name,
                rule: crosstalk_spec::aggregates::alert::UserRule::SemanticQuery {
                    text,
                    threshold,
                },
                sinks: Vec::new(),
            },
        )
        .await;
    assert!(matches!(created, Ok(ActionOutcome::RuleCreated(_))));
    let Ok(listed) = fixture
        .surface
        .alert_rules(&caller, &AlertRuleFilter::default(), &page(100))
        .await
    else {
        panic!("rules");
    };
    assert_eq!(listed.items().len(), BuiltinRule::ALL.len() + 1);
    for rule in listed.items() {
        assert_eq!(
            fixture.surface.alert_rule(&caller, rule.id()).await,
            Ok(Some(rule.clone()))
        );
    }
    assert_eq!(
        fixture
            .surface
            .alert_rule(&caller, AlertRuleId::from_ulid(1 << 101))
            .await,
        Ok(None)
    );
}

/// INV-557: the operator directory as stored, by id.
#[tokio::test]
async fn operators_query_returns_directory() {
    let fixture = Fixture::new().await;
    let caller = fixture.caller(Who::Viewer).await;
    let Ok(operators) = fixture.surface.operators(&caller).await else {
        panic!("operators");
    };
    let Some(directory) = fixture.world.operators.directory() else {
        panic!("no directory");
    };
    let stored: Vec<_> = directory.operators().cloned().collect();
    assert_eq!(operators, stored);
    let ids: Vec<_> = operators.iter().map(|operator| operator.id).collect();
    let mut sorted = ids.clone();
    sorted.sort();
    assert_eq!(ids, sorted);
    assert_eq!(ids.len(), super::world::Who::ALL.len());
}

/// INV-1077: every caller reads its own operator, whatever its
/// permissions (an auditor without View too): the directory's id and name
/// with the permissions it was authenticated with.
#[tokio::test]
async fn me_is_the_callers_own_operator_for_every_caller() {
    let fixture = Fixture::new().await;
    let Some(directory) = fixture.world.operators.directory() else {
        panic!("no directory");
    };
    for who in Who::ALL {
        let caller = fixture.caller(who).await;
        let me = fixture.surface.me(&caller).await;
        let Ok(me) = me else {
            panic!("me as {who:?}: {me:?}");
        };
        let Some(listed) = directory.get(caller.operator()) else {
            panic!("{who:?} is not in the directory");
        };
        assert_eq!(me.id, caller.operator(), "{who:?}");
        assert_eq!(me.name, listed.name, "{who:?}");
        assert_eq!(me.permissions, caller.permissions(), "{who:?}");
        assert_eq!(&me, listed, "{who:?}");
    }
}

/// INV-1077: in trusted mode every request's caller is the configured
/// operator, so `me` answers with it and every permission, whatever the
/// request carried.
#[tokio::test]
async fn me_in_trusted_mode_is_the_trusted_operator() {
    use crosstalk_spec::ids::{ConfigHash, OperatorId};
    use crosstalk_spec::interfaces::l8_surface::PermissionSet;
    use crosstalk_spec::interfaces::l8_surface::operators::{
        AccessConfig, OperatorName, OperatorStore, RequestIdentity, TrustedOperator,
    };
    use crosstalk_spec::support::Blake3;

    let fixture = Fixture::new().await;
    let Ok(name) = OperatorName::new("solo") else {
        panic!("name");
    };
    let trusted = TrustedOperator {
        id: OperatorId::from_ulid(9_999),
        name: name.clone(),
    };
    let mut store = fixture.world.operators.clone();
    let loaded = store
        .load(
            &AccessConfig::Trusted(trusted.clone()),
            ConfigHash::from_digest(Blake3::of(b"trusted")),
            minute(1),
        )
        .await;
    assert!(loaded.is_ok(), "{loaded:?}");
    for identity in [
        RequestIdentity::Anonymous,
        RequestIdentity::Verified(Who::Viewer.id()),
    ] {
        let caller = store.caller(identity).await;
        let Ok(caller) = caller else {
            panic!("caller for {identity:?}: {caller:?}");
        };
        let me = fixture.surface.me(&caller).await;
        let Ok(me) = me else {
            panic!("me for {identity:?}: {me:?}");
        };
        assert_eq!(me.id, trusted.id, "{identity:?}");
        assert_eq!(me.name, name, "{identity:?}");
        assert_eq!(me.permissions, PermissionSet::ALL, "{identity:?}");
    }
}

/// INV-789: the present's bucket width is the edge store's.
#[tokio::test]
async fn present_reports_the_edge_store_bucket_width() {
    let fixture = Fixture::new().await;
    let caller = fixture.caller(Who::Viewer).await;
    let Ok(present) = fixture.surface.present(&caller).await else {
        panic!("present");
    };
    assert_eq!(present.bucket_width, fixture.world.edges.bucket_width());
    // A window aligned to it is never refused as unaligned.
    let window = minutes(0, 3);
    assert!(
        fixture
            .surface
            .topology(
                &caller,
                window,
                crosstalk_spec::aggregates::edge::Weighting::Transmissions,
                &Default::default()
            )
            .await
            .is_ok()
    );
}

/// INV-790: the present's remap threshold is the alert store's default.
#[tokio::test]
async fn present_reports_the_default_remap_threshold() {
    let fixture = Fixture::new().await;
    let caller = fixture.caller(Who::Viewer).await;
    let Ok(present) = fixture.surface.present(&caller).await else {
        panic!("present");
    };
    assert_eq!(
        present.default_remap_threshold,
        config().default_remap_threshold
    );
    assert_eq!(present.export_formats, config().export_formats);
}

/// INV-790: the present's frame retention is the projection store's.
#[tokio::test]
async fn present_reports_the_frame_retention() {
    let fixture = Fixture::new().await;
    let caller = fixture.caller(Who::Viewer).await;
    let Ok(present) = fixture.surface.present(&caller).await else {
        panic!("present");
    };
    assert_eq!(present.frame_retention_micros, FrameRetention::default());
    assert_eq!(
        present.frame_retention_micros.as_duration(),
        fixture.world.projections.config().frame_retention
    );
}

/// INV-792: the present's `now` is the clock, never before the watermark.
#[tokio::test]
async fn present_reports_the_wall_clock() {
    let fixture = Fixture::new().await;
    let caller = fixture.caller(Who::Viewer).await;
    let now = Timestamp::from_micros(minute(9).as_micros() + 5);
    fixture.clock.set(now);
    let Ok(present) = fixture.surface.present(&caller).await else {
        panic!("present");
    };
    assert_eq!(present.now, now);
    // A clock behind the exposed watermark answers with the watermark.
    let watermark = fixture.watermark(minute(20)).await;
    let Ok(present) = fixture.surface.present(&caller).await else {
        panic!("present");
    };
    assert_eq!(present.now, watermark.at());
}

/// INV-793: the present's rule version is the one rules are checked
/// against: a rule naming it and its topics is accepted.
#[tokio::test]
async fn present_reports_the_rule_store_version() {
    let fixture = Fixture::new().await;
    let caller = fixture.caller(Who::Admin).await;
    let Ok(present) = fixture.surface.present(&caller).await else {
        panic!("present");
    };
    let mut probe = fixture.world.alerts.clone();
    use crosstalk_spec::interfaces::l6_analysis::alerts::AlertReads;
    assert_eq!(probe.rule_version().await, Ok(present.current_rule_version));
    let _ = &mut probe;
}

/// INV-714: a merged id answers for its canonical agent, redirected; an
/// unknown id is `None`.
#[tokio::test]
async fn agent_detail_redirects_merged_ids() {
    let fixture = Fixture::new().await;
    let scene = fixture.scene().await;
    let caller = fixture.caller(Who::Admin).await;
    let merged = fixture
        .surface
        .request(
            &caller,
            ActionRequest::MergeAgents {
                from: scene.a3,
                into: scene.a1,
            },
        )
        .await;
    assert!(matches!(merged, Ok(ActionOutcome::Merged(_))));
    let window = minutes(0, 10);
    let Ok(Some(direct)) = fixture.surface.agent(&caller, scene.a1, window).await else {
        panic!("agent");
    };
    assert_eq!(direct.value.cluster.lookup(), AgentLookup::Canonical);
    let Ok(Some(redirected)) = fixture.surface.agent(&caller, scene.a3, window).await else {
        panic!("agent");
    };
    assert_eq!(redirected.value.cluster.profile().id(), scene.a1);
    assert_eq!(
        redirected.value.cluster.lookup(),
        AgentLookup::Redirected { from: scene.a3 }
    );
    assert_eq!(
        fixture
            .surface
            .agent(&caller, AgentId::from_ulid(0xFACE), window)
            .await,
        Ok(None)
    );
    // The detail's traffic is the canonical agent's node in the graph.
    assert_eq!(direct.value.traffic.transmissions_out, 1);
}

/// INV-716: names follow merges, keyed by the id asked for; unknown ids are
/// left out.
#[tokio::test]
async fn agent_names_resolve_aliases_and_skip_unknown() {
    let fixture = Fixture::new().await;
    let scene = fixture.scene().await;
    let caller = fixture.caller(Who::Admin).await;
    let merged = fixture
        .surface
        .request(
            &caller,
            ActionRequest::MergeAgents {
                from: scene.a3,
                into: scene.a1,
            },
        )
        .await;
    assert!(merged.is_ok());
    let unknown = AgentId::from_ulid(0xFACE);
    let Ok(batch) = IdBatch::new([scene.a1, scene.a3, unknown]) else {
        panic!("batch");
    };
    let Ok(names) = fixture.surface.agent_names(&caller, &batch).await else {
        panic!("names");
    };
    assert_eq!(names.len(), 2);
    assert_eq!(names.get(&scene.a1).map(|name| name.id), Some(scene.a1));
    assert_eq!(names.get(&scene.a3).map(|name| name.id), Some(scene.a1));
    assert!(!names.contains_key(&unknown));
}

/// INV-719: every row is a canonical agent with its aliases; no merged
/// agent is a row.
#[tokio::test]
async fn agent_rows_are_canonical() {
    let fixture = Fixture::new().await;
    let scene = fixture.scene().await;
    let caller = fixture.caller(Who::Admin).await;
    let merged = fixture
        .surface
        .request(
            &caller,
            ActionRequest::MergeAgents {
                from: scene.a3,
                into: scene.a1,
            },
        )
        .await;
    assert!(merged.is_ok());
    let Ok(rows) = fixture
        .surface
        .agents(&caller, &AgentFilter::default(), minutes(0, 10), &page(50))
        .await
    else {
        panic!("agents");
    };
    let ids: Vec<AgentId> = rows
        .value
        .items()
        .iter()
        .map(|row| row.profile.id())
        .collect();
    assert!(ids.contains(&scene.a1));
    assert!(ids.contains(&scene.a2));
    assert!(!ids.contains(&scene.a3));
    let Some(a1) = rows
        .value
        .items()
        .iter()
        .find(|row| row.profile.id() == scene.a1)
    else {
        panic!("a1");
    };
    assert_eq!(a1.profile.aliases(), [scene.a3]);
    assert_eq!(a1.traffic.transmissions_out, 1);
    let Some(a2) = rows
        .value
        .items()
        .iter()
        .find(|row| row.profile.id() == scene.a2)
    else {
        panic!("a2");
    };
    assert_eq!(a2.traffic.transmissions_in, 1);
    // The page carries the watermark the traffic was read under.
    assert_eq!(
        rows.watermark,
        fixture
            .world
            .edges
            .watermark()
            .await
            .unwrap_or(rows.watermark)
    );
}

/// INV-408: the audit log pages newest first, with no gaps or repeats.
#[tokio::test]
async fn audit_pages_newest_first_without_gaps() {
    let fixture = Fixture::new().await;
    let scene = fixture.scene().await;
    let admin = fixture.caller(Who::Admin).await;
    for n in 0..7_u64 {
        fixture.clock.set(minute(n + 1));
        let rename = OperatorAction::RenameAgent {
            agent: scene.a2,
            label: crosstalk_spec::observed::agent::AgentLabel::new(&format!("label {n}")).ok(),
        };
        assert!(fixture.surface.act(&admin, rename).await.is_ok());
    }
    let auditor = fixture.caller(Who::Auditor).await;
    let filter = AuditFilter {
        by: Vec::new(),
        subject: Some(AuditSubject::Agent(scene.a2)),
        window: None,
    };
    let mut request: PageRequest<AuditList> = page(3);
    let mut seen = Vec::new();
    loop {
        let Ok(page) = fixture.surface.audit(&auditor, &filter, &request).await else {
            panic!("audit");
        };
        assert!(page.items().iter().all(|entry| filter.matches(entry)));
        seen.extend(page.items().iter().map(|entry| (entry.at, entry.id)));
        match page.next() {
            Some(next) => request.after = Some(next.clone()),
            None => break,
        }
    }
    assert_eq!(seen.len(), 7);
    let mut sorted = seen.clone();
    sorted.sort_by(|a, b| b.cmp(a));
    assert_eq!(seen, sorted);
}

/// INV-408: every listed alert, rule and agent satisfies its filter.
#[tokio::test]
async fn list_filter_cases() {
    let fixture = Fixture::new().await;
    let scene = fixture.scene().await;
    let caller = fixture.caller(Who::Admin).await;
    let disabled = OperatorAction::SetRuleEnabled {
        id: BuiltinRule::SanctionedUnused.id(),
        enabled: false,
    };
    assert!(fixture.surface.act(&caller, disabled).await.is_ok());
    let rules = AlertRuleFilter {
        statuses: vec![crosstalk_spec::aggregates::alert::RuleStatus::Disabled],
        stale: None,
    };
    let Ok(listed) = fixture
        .surface
        .alert_rules(&caller, &rules, &page(50))
        .await
    else {
        panic!("rules");
    };
    assert_eq!(listed.items().len(), 1);
    assert!(listed.items().iter().all(|rule| rules.matches(rule)));
    let alerts = AlertFilter {
        states: vec![AlertStateKind::Open],
        channel: Some(scene.c1),
    };
    let Ok(listed) = fixture.surface.alerts(&caller, &alerts, &page(50)).await else {
        panic!("alerts");
    };
    assert!(
        listed
            .items()
            .iter()
            .all(|alert| alert.state.kind() == AlertStateKind::Open)
    );
    let page_request: PageRequest<AlertList> = page(1);
    let Ok(first) = fixture
        .surface
        .alerts(&caller, &AlertFilter::default(), &page_request)
        .await
    else {
        panic!("alerts");
    };
    assert!(first.items().len() <= 1);
}

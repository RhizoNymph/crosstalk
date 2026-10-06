//! The audit log, the operator directory and dead letters as `QueryApi`
//! defines them: `audit` (Audit) pages exactly what `AuditFilter::matches`
//! admits, newest first; every `act` call leaves one entry whose outcome is
//! what it returned; `operators` (View) is the directory; `dead_letters`
//! (Operate) lists one group or all, newest envelope first.

use std::collections::HashSet;

use crosstalk_spec::aggregates::topic::TopicModelVersion;
use crosstalk_spec::derived::flow::resource::{Host, ResourcePattern};
use crosstalk_spec::interfaces::l2_transport::ConsumerGroup;
use crosstalk_spec::interfaces::l8_surface::audit::{
    AuditAuthor, AuditBody, AuditEntry, AuditFilter, AuditOutcome, AuditSubject,
};
use crosstalk_spec::interfaces::l8_surface::{
    ActionOutcome, OperatorAction, Permission, PermissionSet, PolicyKind, QueryError,
};

use super::super::FixtureBackend;
use super::super::clock::{DAY, NOW, ago};
use super::super::world::{ChannelKey, OPERATOR_ONCALL, OPERATOR_RESEARCHER};
use super::actions_support::channel;
use super::{caller, collect, first, fresh, researcher, shared, window};
use crosstalk_spec::interfaces::l8_surface::CallerSnapshot;
use crosstalk_spec::interfaces::l8_surface::{OperatorActions, QueryApi};

/// `audit` followed to its last page.
async fn audited(b: &FixtureBackend, filter: &AuditFilter) -> Vec<AuditEntry> {
    let c = researcher();
    collect(40, async |p| b.audit(&c, filter, &p).await).await
}

/// What `AuditFilter::matches` admits from the whole log, newest first.
async fn expected(b: &FixtureBackend, filter: &AuditFilter) -> Vec<AuditEntry> {
    let state = b.state.read().await;
    let mut entries: Vec<AuditEntry> = state
        .audit
        .entries()
        .iter()
        .filter(|e| filter.matches(e))
        .cloned()
        .collect();
    entries.sort_by_key(|e| std::cmp::Reverse((e.at, e.id)));
    entries
}

#[tokio::test]
async fn the_log_needs_audit_and_pages_exactly_what_the_filter_admits() {
    let b = shared();
    let without = caller(&[
        Permission::View,
        Permission::Content,
        Permission::Govern,
        Permission::Triage,
        Permission::Operate,
    ]);
    assert_eq!(
        b.audit(&without, &AuditFilter::default(), &first(5))
            .await
            .err(),
        Some(QueryError::Forbidden {
            missing: Permission::Audit
        })
    );
    let wiki = channel(b, ChannelKey::HijackedWiki);
    let filters = [
        AuditFilter::default(),
        AuditFilter {
            by: vec![AuditAuthor::Config],
            ..AuditFilter::default()
        },
        AuditFilter {
            by: vec![AuditAuthor::Operator(OPERATOR_ONCALL)],
            window: Some(window(ago(DAY))),
            ..AuditFilter::default()
        },
        AuditFilter {
            subject: Some(AuditSubject::Channel(wiki)),
            ..AuditFilter::default()
        },
        AuditFilter {
            subject: Some(AuditSubject::TopicVersion(TopicModelVersion(1))),
            ..AuditFilter::default()
        },
    ];
    for filter in filters {
        let listed = audited(b, &filter).await;
        assert_eq!(listed, expected(b, &filter).await, "{filter:?}");
    }
    let config = audited(
        b,
        &AuditFilter {
            by: vec![AuditAuthor::Config],
            ..AuditFilter::default()
        },
    )
    .await;
    assert!(!config.is_empty());
    assert!(
        config
            .iter()
            .all(|e| matches!(e.body, AuditBody::Config(_)))
    );
}

#[tokio::test]
async fn every_call_leaves_one_entry_naming_what_it_touched_and_created() {
    let b = fresh();
    let c = researcher();
    let (wiki, talk) = (
        channel(&b, ChannelKey::HijackedWiki),
        channel(&b, ChannelKey::WikiTalk),
    );
    let promote = OperatorAction::PromoteChannel {
        channel: wiki,
        pattern: ResourcePattern::UrlPrefix {
            host: Host("wiki.example.org".to_owned()),
            path_prefix: "/wiki".to_owned(),
        },
        policy: PolicyKind::Sanctioned,
        note: None,
    };
    let pin = OperatorAction::PinTopicVersion {
        version: TopicModelVersion(2),
    };
    let calls = [
        (c.clone(), promote),
        (c.clone(), pin.clone()),
        (c.clone(), pin),
        (
            caller(&[Permission::View]),
            OperatorAction::Unmerge {
                merge: crosstalk_spec::ids::MergeId::from_ulid(1),
            },
        ),
    ];
    for (who, action) in calls {
        let before = b.state.read().await.audit.entries().len();
        let result = b.act(&who, action.clone()).await;
        let state = b.state.read().await;
        let entries = state.audit.entries();
        assert_eq!(entries.len(), before + 1, "{action:?}");
        let entry = entries.last().expect("entry");
        let AuditBody::Operator(record) = &entry.body else {
            panic!("an operator entry");
        };
        assert_eq!(
            (entry.at, record.caller(), record.action()),
            (NOW, &CallerSnapshot::of(&who), &action)
        );
        assert_eq!(record.outcome(), &AuditOutcome::of(&result));
        assert_eq!(entry.by(), AuditAuthor::Operator(who.operator()));
    }
    // The promotion is found from the channel it superseded; the repeated
    // pin is recorded as unchanged; the forbidden unmerge as forbidden.
    let superseded = audited(
        &b,
        &AuditFilter {
            subject: Some(AuditSubject::Channel(talk)),
            ..AuditFilter::default()
        },
    )
    .await;
    assert_eq!(superseded.iter().filter(|e| e.at == NOW).count(), 1);
    let pins = audited(
        &b,
        &AuditFilter {
            subject: Some(AuditSubject::TopicVersion(TopicModelVersion(2))),
            ..AuditFilter::default()
        },
    )
    .await;
    let outcomes: Vec<_> = pins
        .iter()
        .filter_map(|e| match &e.body {
            AuditBody::Operator(record) => Some(record.outcome().clone()),
            _ => None,
        })
        .collect();
    assert_eq!(
        outcomes,
        [
            AuditOutcome::Succeeded(ActionOutcome::Unchanged),
            AuditOutcome::Succeeded(ActionOutcome::Applied)
        ],
        "newest first"
    );
    let forbidden = audited(
        &b,
        &AuditFilter {
            by: vec![AuditAuthor::Operator(OPERATOR_ONCALL)],
            subject: Some(AuditSubject::Merge(
                crosstalk_spec::ids::MergeId::from_ulid(1),
            )),
            ..AuditFilter::default()
        },
    )
    .await;
    assert!(matches!(
        &forbidden[..],
        [entry] if matches!(&entry.body, AuditBody::Operator(record)
            if record.outcome() == &AuditOutcome::Forbidden { missing: Permission::Govern })
    ));
}

/// `me` needs no permission: a caller holding only Audit reads its own
/// operator, named by the directory, with the permissions it holds.
#[tokio::test]
async fn me_is_the_callers_own_operator() {
    let b = shared();
    let auditor = caller(&[Permission::Audit]);
    let me = b.me(&auditor).await.expect("me");
    assert_eq!(
        (me.id, me.name.as_str(), me.permissions),
        (
            OPERATOR_ONCALL,
            "oncall",
            PermissionSet::of([Permission::Audit])
        )
    );
}

#[tokio::test]
async fn operators_are_the_directory() {
    let b = shared();
    let viewer = caller(&[Permission::View]);
    let operators = b.operators(&viewer).await.expect("operators");
    let named: Vec<(_, &str, PermissionSet)> = operators
        .iter()
        .map(|o| (o.id, o.name.as_str(), o.permissions))
        .collect();
    assert_eq!(
        named,
        [
            (OPERATOR_RESEARCHER, "researcher", PermissionSet::ALL),
            (
                OPERATOR_ONCALL,
                "oncall",
                PermissionSet::of([Permission::View, Permission::Content, Permission::Triage])
            ),
        ]
    );
    assert_eq!(
        b.operators(&caller(&[Permission::Audit])).await.err(),
        Some(QueryError::Forbidden {
            missing: Permission::View
        })
    );
}

#[tokio::test]
async fn dead_letters_list_one_group_or_all_newest_first() {
    let b = shared();
    let c = researcher();
    let all = collect(2, async |p| b.dead_letters(&c, None, &p).await).await;
    assert_eq!(all.len(), 4);
    assert!(
        all.windows(2)
            .all(|w| (w[0].envelope.at, w[0].envelope.id) >= (w[1].envelope.at, w[1].envelope.id)),
        "newest envelope first"
    );
    let groups: HashSet<&ConsumerGroup> = all.iter().map(|l| &l.group).collect();
    assert_eq!(groups.len(), 4);
    for group in groups {
        let one = collect(2, async |p| b.dead_letters(&c, Some(group), &p).await).await;
        assert!(!one.is_empty() && one.iter().all(|l| &l.group == group));
    }
    let nobody = ConsumerGroup("nobody".to_owned());
    assert!(
        b.dead_letters(&c, Some(&nobody), &first(5))
            .await
            .expect("empty")
            .items()
            .is_empty()
    );
    // A cursor is bound to the group it was issued for.
    let page = b.dead_letters(&c, None, &first(1)).await.expect("page");
    let other = crosstalk_spec::paging::PageRequest {
        size: first::<crosstalk_spec::paging::DeadLetterList>(1).size,
        after: page.next().cloned(),
    };
    assert_eq!(
        b.dead_letters(&c, Some(&nobody), &other).await.err(),
        Some(QueryError::InvalidCursor)
    );
    assert_eq!(
        b.dead_letters(&caller(&[Permission::View]), None, &first(5))
            .await
            .err(),
        Some(QueryError::Forbidden {
            missing: Permission::Operate
        })
    );
}

#[tokio::test]
async fn a_live_clock_stamps_actions_after_the_data_and_reports_the_present() {
    let b = FixtureBackend::try_live(super::SEED).expect("fixture generates");
    let c = researcher();
    let before = b.present(&c).await.expect("now").now;
    assert!(before >= NOW);
    let pin = OperatorAction::PinTopicVersion {
        version: TopicModelVersion(2),
    };
    b.act(&c, pin).await.expect("pin");
    let at = b
        .state
        .read()
        .await
        .audit
        .entries()
        .last()
        .expect("entry")
        .at;
    let after = b.present(&c).await.expect("now").now;
    assert!(
        before <= at && at <= after,
        "{before:?} <= {at:?} <= {after:?}"
    );
}

#[tokio::test]
async fn the_log_records_one_interrupted_call_whose_effect_did_not_apply() {
    use crosstalk_spec::aggregates::alert::AlertState;
    use crosstalk_spec::interfaces::l8_surface::OperatorAction;
    let b = &shared();
    let interrupted: Vec<_> = audited(b, &AuditFilter::default())
        .await
        .into_iter()
        .filter_map(|entry| match entry.body {
            AuditBody::Operator(record) if record.outcome() == &AuditOutcome::Interrupted => {
                Some((entry.at, record))
            }
            _ => None,
        })
        .collect();
    let [(at, record)] = interrupted.as_slice() else {
        panic!("one interrupted call, not {}", interrupted.len());
    };
    assert_eq!(*at, ago(6 * super::super::clock::HOUR));
    assert_eq!(record.caller().operator(), OPERATOR_RESEARCHER);
    let OperatorAction::Acknowledge { alert } = record.action() else {
        panic!("an acknowledgement: {:?}", record.action());
    };
    let alert = b
        .alert(&researcher(), *alert)
        .await
        .expect("alert")
        .expect("the alert exists");
    assert_eq!(
        alert.state,
        AlertState::Open,
        "the acknowledgement did not take"
    );
}

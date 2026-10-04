use crate::aggregates::alert::AlertRuleKind;
use crate::aggregates::topic::TopicModelVersion;
use crate::derived::flow::channel::policy::{PolicyAuthor, PolicyKind};
use crate::derived::flow::resource::{Host, ResourcePattern};
use crate::derived::flow::verdict::Verdict;
use crate::ids::{
    AccountHash, AlertId, AlertRuleId, AuditId, ConfigHash, EventId, MergeId, SecretVersion,
};
use crate::interfaces::l2_transport::ConsumerGroup;
use crate::interfaces::l8_surface::audit::{
    AuditBody, AuditEntry, AuditFilter, AuditOutcome, AuditSubject, ConfigChange, ConfigOutcome,
    ConfigRecord, InvalidOperatorRecord, OperatorRecord, OutcomeKind, Rejection,
};
use crate::interfaces::l8_surface::operators::{AccessMode, OperatorName};
use crate::interfaces::l8_surface::{
    ActionError, ActionKind, ActionOutcome, ConflictKind, InputError, OperatorAction, Permission,
    PermissionSet, QueryError,
};
use crate::observed::agent::{AgentLabel, IdentityEvidence, MergeAuthor, MergeRequest};
use crate::support::{Blake3, NonEmpty, TimeWindow};
use crate::tests::fixtures::{agent, at, channel, transmission};
use crate::tests::operators::{caller, operator};

fn set_policy() -> OperatorAction {
    OperatorAction::SetPolicy {
        channel: channel(1),
        policy: PolicyKind::Sanctioned,
        note: Some("expected traffic".into()),
    }
}

fn merge() -> MergeRequest {
    MergeRequest::new(agent(1), agent(2), MergeAuthor::Operator(operator(1)))
        .expect("different agents")
}

fn wiki() -> ResourcePattern {
    ResourcePattern::Host(Host("wiki.example".into()))
}

/// One action of every variant whose arguments need no fixtures beyond ids,
/// with its kind, required permission and subjects. Rule creation and
/// update are covered by `tests::surface`; an update's subject is the rule
/// id, and a creation's is the id its outcome carries.
fn every_action() -> Vec<(OperatorAction, ActionKind, Permission, Vec<AuditSubject>)> {
    let rule = AlertRuleId::from_ulid(4);
    let alert = AlertId::from_ulid(3);
    vec![
        (
            set_policy(),
            ActionKind::SetPolicy,
            Permission::Govern,
            vec![AuditSubject::Channel(channel(1))],
        ),
        (
            OperatorAction::MergeAgents(merge()),
            ActionKind::MergeAgents,
            Permission::Govern,
            vec![AuditSubject::Agent(agent(1)), AuditSubject::Agent(agent(2))],
        ),
        (
            OperatorAction::Unmerge {
                merge: MergeId::from_ulid(5),
            },
            ActionKind::Unmerge,
            Permission::Govern,
            vec![AuditSubject::Merge(MergeId::from_ulid(5))],
        ),
        (
            OperatorAction::RenameAgent {
                agent: agent(6),
                label: Some(AgentLabel::new("planner").expect("valid")),
            },
            ActionKind::RenameAgent,
            Permission::Govern,
            vec![AuditSubject::Agent(agent(6))],
        ),
        (
            OperatorAction::PromoteChannel {
                channel: channel(2),
                pattern: wiki(),
                policy: PolicyKind::Sanctioned,
                note: None,
            },
            ActionKind::PromoteChannel,
            Permission::Govern,
            vec![AuditSubject::Channel(channel(2))],
        ),
        (
            OperatorAction::SetRuleEnabled {
                id: rule,
                enabled: false,
            },
            ActionKind::SetRuleEnabled,
            Permission::Govern,
            vec![AuditSubject::Rule(rule)],
        ),
        (
            OperatorAction::Acknowledge { alert },
            ActionKind::Acknowledge,
            Permission::Triage,
            vec![AuditSubject::Alert(alert)],
        ),
        (
            OperatorAction::Resolve { alert, note: None },
            ActionKind::Resolve,
            Permission::Triage,
            vec![AuditSubject::Alert(alert)],
        ),
        (
            OperatorAction::SetVerdict {
                transmission: transmission(8),
                verdict: Some(Verdict::FalseDetection),
                note: None,
            },
            ActionKind::SetVerdict,
            Permission::Triage,
            vec![AuditSubject::Transmission(transmission(8))],
        ),
        (
            OperatorAction::ReplayDeadLetter {
                group: ConsumerGroup("flow".into()),
                id: EventId::from_ulid(1),
            },
            ActionKind::ReplayDeadLetter,
            Permission::Operate,
            Vec::new(),
        ),
        (
            OperatorAction::PinTopicVersion {
                version: TopicModelVersion(3),
            },
            ActionKind::PinTopicVersion,
            Permission::Govern,
            vec![AuditSubject::TopicVersion(TopicModelVersion(3))],
        ),
    ]
}

#[test]
fn every_action_reports_its_kind_permission_and_subjects() {
    for (action, kind, permission, subjects) in every_action() {
        assert_eq!(action.kind(), kind);
        assert_eq!(action.required_permission(), permission);
        assert_eq!(action.subjects(), subjects, "{action:?}");
    }
}

#[test]
fn outcomes_name_the_ids_they_created() {
    let rule = AlertRuleId::from_ulid(1);
    let merge = MergeId::from_ulid(2);
    assert_eq!(ActionOutcome::Applied.subject(), None);
    assert_eq!(ActionOutcome::Unchanged.subject(), None);
    assert_eq!(
        ActionOutcome::RuleCreated(rule).subject(),
        Some(AuditSubject::Rule(rule))
    );
    assert_eq!(
        ActionOutcome::ChannelPromoted(channel(1)).subject(),
        Some(AuditSubject::Channel(channel(1)))
    );
    assert_eq!(
        ActionOutcome::Merged(merge).subject(),
        Some(AuditSubject::Merge(merge))
    );
}

#[test]
fn forbidden_record_requires_missing_permission() {
    let auditor = caller(1, &[Permission::Audit]);
    for (action, _, required, _) in every_action() {
        assert_eq!(
            OperatorRecord::new(
                caller(1, &[required]),
                action.clone(),
                AuditOutcome::Forbidden { missing: required }
            ),
            Err(InvalidOperatorRecord::ForbiddenButPermitted { required })
        );
        let record = OperatorRecord::new(
            auditor.clone(),
            action.clone(),
            AuditOutcome::Forbidden { missing: required },
        )
        .expect("an auditor holds no action permission");
        assert_eq!(record.caller(), &auditor);
        assert_eq!(record.action(), &action);
        let other = if required == Permission::Govern {
            Permission::Triage
        } else {
            Permission::Govern
        };
        assert_eq!(
            OperatorRecord::new(
                auditor.clone(),
                action.clone(),
                AuditOutcome::Forbidden { missing: other }
            ),
            Err(InvalidOperatorRecord::WrongMissingPermission { required })
        );
    }
}

#[test]
fn attempted_record_requires_permission() {
    let attempted = [
        AuditOutcome::Succeeded(ActionOutcome::Applied),
        AuditOutcome::Succeeded(ActionOutcome::Unchanged),
        AuditOutcome::Rejected(Rejection::NotFound),
    ];
    for (action, _, required, _) in every_action() {
        for outcome in attempted.clone() {
            assert_eq!(
                OperatorRecord::new(
                    caller(1, &[Permission::View]),
                    action.clone(),
                    outcome.clone()
                ),
                Err(InvalidOperatorRecord::AttemptedWithoutPermission { required })
            );
            let record =
                OperatorRecord::new(caller(1, &[required]), action.clone(), outcome.clone())
                    .expect("caller holds the permission");
            assert_eq!(record.outcome(), &outcome);
        }
    }
}

#[test]
fn outcome_maps_every_result_and_back() {
    let results = [
        Ok(ActionOutcome::Applied),
        Ok(ActionOutcome::Unchanged),
        Ok(ActionOutcome::RuleCreated(AlertRuleId::from_ulid(1))),
        Ok(ActionOutcome::Merged(MergeId::from_ulid(1))),
        Err(ActionError::Forbidden {
            missing: Permission::Govern,
        }),
        Err(ActionError::NotFound),
        Err(ActionError::Conflict(ConflictKind::AlertNotActive {
            alert: AlertId::from_ulid(1),
        })),
        Err(ActionError::InvalidInput(InputError::PatternMissesSeed)),
        Err(ActionError::Store {
            reason: "connection reset".into(),
        }),
    ];
    let kinds = [
        OutcomeKind::Applied,
        OutcomeKind::Unchanged,
        OutcomeKind::Applied,
        OutcomeKind::Applied,
        OutcomeKind::Forbidden,
        OutcomeKind::Rejected,
        OutcomeKind::Rejected,
        OutcomeKind::Rejected,
        OutcomeKind::Rejected,
    ];
    for (result, kind) in results.into_iter().zip(kinds) {
        let outcome = AuditOutcome::of(&result);
        assert_eq!(outcome.kind(), kind);
        assert_eq!(outcome.result(), result);
    }
}

#[test]
fn outcome_keeps_rejection_detail() {
    let result = Err(ActionError::Conflict(ConflictKind::AlertNotActive {
        alert: AlertId::from_ulid(7),
    }));
    assert_eq!(
        AuditOutcome::of(&result),
        AuditOutcome::Rejected(Rejection::Conflict(ConflictKind::AlertNotActive {
            alert: AlertId::from_ulid(7)
        }))
    );
}

#[test]
fn every_action_error_is_a_query_error() {
    let missing = Permission::Operate;
    assert_eq!(
        QueryError::from(ActionError::Forbidden { missing }),
        QueryError::Forbidden { missing }
    );
    assert_eq!(
        QueryError::from(ActionError::InvalidInput(InputError::UnalignedWindow)),
        QueryError::InvalidInput(InputError::UnalignedWindow)
    );
}

fn operator_entry(
    n: u128,
    when: u64,
    by: u128,
    action: OperatorAction,
    outcome: AuditOutcome,
) -> AuditEntry {
    AuditEntry {
        id: AuditId::from_ulid(n),
        at: at(when),
        body: AuditBody::Operator(
            OperatorRecord::new(caller(by, &[Permission::Govern]), action, outcome)
                .expect("caller holds Govern"),
        ),
    }
}

fn config_entry(n: u128, when: u64, change: ConfigChange) -> AuditEntry {
    AuditEntry {
        id: AuditId::from_ulid(n),
        at: at(when),
        body: AuditBody::Config(ConfigRecord {
            config: ConfigHash::from_digest(Blake3::from_bytes([1; 32])),
            change,
            outcome: ConfigOutcome::Applied,
        }),
    }
}

fn declare(n: u128) -> ConfigChange {
    ConfigChange::DeclareChannel {
        channel: channel(n),
        pattern: wiki(),
        policy: PolicyKind::Sanctioned,
        note: None,
    }
}

fn applied() -> AuditOutcome {
    AuditOutcome::Succeeded(ActionOutcome::Applied)
}

#[test]
fn entry_author_follows_its_body() {
    assert_eq!(
        operator_entry(1, 1, 9, set_policy(), applied()).by(),
        PolicyAuthor::Operator(operator(9))
    );
    assert_eq!(config_entry(2, 1, declare(1)).by(), PolicyAuthor::Config);
}

#[test]
fn operator_entry_subjects_include_created_ids() {
    let merged = MergeId::from_ulid(5);
    let entry = operator_entry(
        1,
        1,
        1,
        OperatorAction::MergeAgents(merge()),
        AuditOutcome::Succeeded(ActionOutcome::Merged(merged)),
    );
    assert_eq!(
        entry.subjects(),
        vec![
            AuditSubject::Agent(agent(1)),
            AuditSubject::Agent(agent(2)),
            AuditSubject::Merge(merged),
        ]
    );
    let promoted = operator_entry(
        2,
        1,
        1,
        OperatorAction::PromoteChannel {
            channel: channel(3),
            pattern: wiki(),
            policy: PolicyKind::Sanctioned,
            note: None,
        },
        AuditOutcome::Succeeded(ActionOutcome::ChannelPromoted(channel(3))),
    );
    assert_eq!(promoted.subjects(), vec![AuditSubject::Channel(channel(3))]);
    let refused = operator_entry(
        3,
        1,
        1,
        OperatorAction::MergeAgents(merge()),
        AuditOutcome::Rejected(Rejection::NotFound),
    );
    assert_eq!(
        refused.subjects(),
        vec![AuditSubject::Agent(agent(1)), AuditSubject::Agent(agent(2))]
    );
}

#[test]
fn every_config_change_names_its_subjects() {
    let rule = AlertRuleId::from_ulid(2);
    let evidence = NonEmpty::new(IdentityEvidence::Account(AccountHash::from_keyed_digest(
        SecretVersion(1),
        Blake3::from_bytes([2; 32]),
    )));
    let cases = [
        (declare(1), vec![AuditSubject::Channel(channel(1))]),
        (
            ConfigChange::SetPolicy {
                channel: channel(2),
                policy: PolicyKind::Unsanctioned,
                note: None,
            },
            vec![AuditSubject::Channel(channel(2))],
        ),
        (
            ConfigChange::RegisterAgent {
                agent: agent(3),
                evidence,
            },
            vec![AuditSubject::Agent(agent(3))],
        ),
        (
            ConfigChange::ProvisionRule {
                rule,
                kind: AlertRuleKind::NewChannel,
            },
            vec![AuditSubject::Rule(rule)],
        ),
        (ConfigChange::SetAccessMode(AccessMode::Trusted), Vec::new()),
        (
            ConfigChange::SetOperator {
                operator: operator(4),
                name: OperatorName::new("Ada").expect("valid"),
                permissions: PermissionSet::ALL,
            },
            vec![AuditSubject::Operator(operator(4))],
        ),
        (
            ConfigChange::RemoveOperator {
                operator: operator(5),
            },
            vec![AuditSubject::Operator(operator(5))],
        ),
    ];
    for (change, subjects) in cases {
        assert_eq!(change.subjects(), subjects, "{change:?}");
        assert_eq!(config_entry(1, 1, change).subjects(), subjects);
    }
}

#[test]
fn empty_audit_filter_matches_everything() {
    let filter = AuditFilter::default();
    assert!(filter.matches(&operator_entry(
        1,
        1,
        1,
        set_policy(),
        AuditOutcome::Rejected(Rejection::NotFound)
    )));
    assert!(filter.matches(&config_entry(2, 2, declare(1))));
}

#[test]
fn audit_filter_selects_by_author() {
    let by_operator = operator_entry(1, 10, 1, set_policy(), applied());
    let by_config = config_entry(2, 10, declare(1));
    let config_only = AuditFilter {
        by: vec![PolicyAuthor::Config],
        ..AuditFilter::default()
    };
    assert!(config_only.matches(&by_config));
    assert!(!config_only.matches(&by_operator));
    let operator_one = AuditFilter {
        by: vec![PolicyAuthor::Operator(operator(1))],
        ..AuditFilter::default()
    };
    assert!(operator_one.matches(&by_operator));
    assert!(!operator_one.matches(&by_config));
    let operator_two = AuditFilter {
        by: vec![PolicyAuthor::Operator(operator(2))],
        ..AuditFilter::default()
    };
    assert!(!operator_two.matches(&by_operator));
}

#[test]
fn audit_filter_selects_by_subject() {
    let merged = MergeId::from_ulid(5);
    let entry = operator_entry(
        1,
        1,
        1,
        OperatorAction::MergeAgents(merge()),
        AuditOutcome::Succeeded(ActionOutcome::Merged(merged)),
    );
    for subject in [
        AuditSubject::Agent(agent(1)),
        AuditSubject::Agent(agent(2)),
        AuditSubject::Merge(merged),
    ] {
        let filter = AuditFilter {
            subject: Some(subject),
            ..AuditFilter::default()
        };
        assert!(filter.matches(&entry), "{subject:?}");
    }
    let other = AuditFilter {
        subject: Some(AuditSubject::Agent(agent(3))),
        ..AuditFilter::default()
    };
    assert!(!other.matches(&entry));
    let channel_filter = AuditFilter {
        subject: Some(AuditSubject::Channel(channel(1))),
        ..AuditFilter::default()
    };
    assert!(channel_filter.matches(&config_entry(2, 1, declare(1))));
    assert!(!channel_filter.matches(&config_entry(3, 1, declare(2))));
}

#[test]
fn audit_filter_combines_fields_with_and() {
    let filter = AuditFilter {
        by: vec![PolicyAuthor::Operator(operator(1))],
        subject: Some(AuditSubject::Channel(channel(1))),
        window: Some(TimeWindow::new(at(5), at(15)).expect("non-empty")),
    };
    assert!(filter.matches(&operator_entry(1, 10, 1, set_policy(), applied())));
    assert!(!filter.matches(&operator_entry(2, 20, 1, set_policy(), applied())));
    assert!(!filter.matches(&operator_entry(3, 10, 2, set_policy(), applied())));
    let elsewhere = OperatorAction::SetPolicy {
        channel: channel(2),
        policy: PolicyKind::Sanctioned,
        note: None,
    };
    assert!(!filter.matches(&operator_entry(4, 10, 1, elsewhere, applied())));
}

use crate::derived::flow::channel::policy::PolicyKind;
use crate::ids::{AlertId, AuditId, EventId, OperatorId};
use crate::interfaces::l2_transport::ConsumerGroup;
use crate::interfaces::l8_surface::audit::{
    AuditFilter, AuditOutcome, AuditRecord, InvalidAuditRecord, OutcomeKind, Rejection,
};
use crate::interfaces::l8_surface::{
    ActionError, ActionKind, ActionOutcome, Caller, ConflictKind, InputError, OperatorAction,
    Permission,
};
use crate::observed::agent::{MergeAuthor, MergeRequest};
use crate::paging::PageSize;
use crate::support::TimeWindow;
use crate::tests::fixtures::{agent, at, channel};

fn operator(n: u128) -> OperatorId {
    OperatorId::from_ulid(n)
}

fn caller(n: u128, permissions: &[Permission]) -> Caller {
    Caller {
        operator: operator(n),
        permissions: permissions.to_vec(),
    }
}

fn set_policy() -> OperatorAction {
    OperatorAction::SetPolicy {
        channel: channel(1),
        policy: PolicyKind::Sanctioned,
        note: Some("expected traffic".into()),
    }
}

/// One action of every variant, with its kind and required permission.
fn every_action() -> Vec<(OperatorAction, ActionKind, Permission)> {
    let merge = MergeRequest::new(agent(1), agent(2), MergeAuthor::Operator(operator(1)))
        .expect("different agents");
    vec![
        (set_policy(), ActionKind::SetPolicy, Permission::Govern),
        (
            OperatorAction::MergeAgents(merge),
            ActionKind::MergeAgents,
            Permission::Govern,
        ),
        (
            OperatorAction::Acknowledge {
                alert: AlertId::from_ulid(1),
            },
            ActionKind::Acknowledge,
            Permission::Triage,
        ),
        (
            OperatorAction::Resolve {
                alert: AlertId::from_ulid(1),
                note: None,
            },
            ActionKind::Resolve,
            Permission::Triage,
        ),
        (
            OperatorAction::ReplayDeadLetter {
                group: ConsumerGroup("flow".into()),
                id: EventId::from_ulid(1),
            },
            ActionKind::ReplayDeadLetter,
            Permission::Operate,
        ),
    ]
}

#[test]
fn every_action_reports_its_kind_and_permission() {
    for (action, kind, permission) in every_action() {
        assert_eq!(action.kind(), kind);
        assert_eq!(action.required_permission(), permission);
    }
}

#[test]
fn caller_has_only_its_permissions() {
    let viewer = caller(1, &[Permission::View]);
    assert!(viewer.has(Permission::View));
    assert!(!viewer.has(Permission::Audit));
}

#[test]
fn forbidden_record_requires_missing_permission() {
    for (action, _, required) in every_action() {
        let permitted = caller(1, &[required]);
        assert_eq!(
            AuditRecord::new(
                AuditId::from_ulid(1),
                at(1),
                permitted,
                action.clone(),
                AuditOutcome::Forbidden { missing: required }
            ),
            Err(InvalidAuditRecord::ForbiddenButPermitted { required })
        );
        let viewer = caller(1, &[Permission::View]);
        let record = AuditRecord::new(
            AuditId::from_ulid(1),
            at(1),
            viewer.clone(),
            action.clone(),
            AuditOutcome::Forbidden { missing: required },
        )
        .expect("viewer lacks every action permission");
        assert_eq!(record.caller(), &viewer);
        assert_eq!(record.action(), &action);
        assert_eq!(
            record.outcome(),
            &AuditOutcome::Forbidden { missing: required }
        );
        let other = if required == Permission::Govern {
            Permission::Triage
        } else {
            Permission::Govern
        };
        assert_eq!(
            AuditRecord::new(
                AuditId::from_ulid(1),
                at(1),
                viewer.clone(),
                action.clone(),
                AuditOutcome::Forbidden { missing: other }
            ),
            Err(InvalidAuditRecord::WrongMissingPermission { required })
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
    for (action, _, required) in every_action() {
        for outcome in attempted.clone() {
            assert_eq!(
                AuditRecord::new(
                    AuditId::from_ulid(1),
                    at(1),
                    caller(1, &[Permission::View]),
                    action.clone(),
                    outcome.clone()
                ),
                Err(InvalidAuditRecord::AttemptedWithoutPermission { required })
            );
            let record = AuditRecord::new(
                AuditId::from_ulid(2),
                at(2),
                caller(1, &[required]),
                action.clone(),
                outcome.clone(),
            )
            .expect("caller holds the permission");
            assert_eq!(record.id(), AuditId::from_ulid(2));
            assert_eq!(record.at(), at(2));
            assert_eq!(record.outcome(), &outcome);
        }
    }
}

#[test]
fn outcome_maps_every_result_and_back() {
    let results = [
        Ok(ActionOutcome::Applied),
        Ok(ActionOutcome::Unchanged),
        Ok(ActionOutcome::RuleCreated(
            crate::ids::AlertRuleId::from_ulid(1),
        )),
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

fn record(n: u128, when: u64, by: u128, outcome: AuditOutcome) -> AuditRecord {
    AuditRecord::new(
        AuditId::from_ulid(n),
        at(when),
        caller(by, &[Permission::Govern]),
        set_policy(),
        outcome,
    )
    .expect("caller holds Govern")
}

#[test]
fn empty_audit_filter_matches_everything() {
    let filter = AuditFilter::default();
    assert!(filter.matches(&record(
        1,
        1,
        1,
        AuditOutcome::Succeeded(ActionOutcome::Applied)
    )));
    assert!(filter.matches(&record(
        2,
        2,
        2,
        AuditOutcome::Rejected(Rejection::NotFound)
    )));
}

#[test]
fn audit_filter_combines_fields_with_and() {
    let applied = record(1, 10, 1, AuditOutcome::Succeeded(ActionOutcome::Applied));
    let filter = AuditFilter {
        window: Some(TimeWindow::new(at(5), at(15)).expect("non-empty")),
        operators: vec![operator(1)],
        actions: vec![ActionKind::SetPolicy],
        outcomes: vec![OutcomeKind::Applied],
    };
    assert!(filter.matches(&applied));
    assert!(!filter.matches(&record(
        2,
        20,
        1,
        AuditOutcome::Succeeded(ActionOutcome::Applied)
    )));
    assert!(!filter.matches(&record(
        3,
        10,
        2,
        AuditOutcome::Succeeded(ActionOutcome::Applied)
    )));
    assert!(!filter.matches(&record(
        4,
        10,
        1,
        AuditOutcome::Succeeded(ActionOutcome::Unchanged)
    )));
    let other_action = AuditFilter {
        actions: vec![ActionKind::Resolve],
        ..filter
    };
    assert!(!other_action.matches(&applied));
}

#[test]
fn audit_page_is_capped() {
    // The audit log pages like every other list.
    assert_eq!(PageSize::MAX, 500);
}

#[test]
fn every_action_error_is_a_query_error() {
    use crate::interfaces::l8_surface::QueryError;
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

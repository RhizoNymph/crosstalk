//! Acknowledge and resolve against the alert lifecycle.

use crosstalk_spec::aggregates::alert::{AlertState, AlertSubject, BuiltinRule, SuppressReason};
use crosstalk_spec::ids::{AgentId, AlertId};
use crosstalk_spec::interfaces::l8_surface::{
    ActionError, ActionOutcome, ConflictKind, OperatorAction, OperatorActions, QueryApi,
};
use crosstalk_spec::support::Timestamp;

use super::world::{Fixture, Who, minute};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Start {
    Open,
    Acknowledged,
    Resolved,
    Suppressed,
}

/// An alert in `start`, the actions that put it there taken at minute 1.
async fn alert_in(fixture: &Fixture, start: Start, n: u128) -> AlertId {
    let rule = match start {
        Start::Suppressed => BuiltinRule::SuspectedTransmission,
        _ => BuiltinRule::NewChannel,
    };
    let subject = AlertSubject::Agent(AgentId::from_ulid(0xA100 + n));
    let alert = fixture.alert(rule, subject, minute(0)).await;
    let admin = fixture.caller(Who::Admin).await;
    fixture.clock.set(minute(1));
    let steps: &[OperatorAction] = match start {
        Start::Open => &[],
        Start::Acknowledged => &[OperatorAction::Acknowledge { alert }],
        Start::Resolved => &[
            OperatorAction::Acknowledge { alert },
            OperatorAction::Resolve {
                alert,
                note: Some("first".to_owned()),
            },
        ],
        Start::Suppressed => &[OperatorAction::SetRuleEnabled {
            id: rule.id(),
            enabled: false,
        }],
    };
    for step in steps {
        let result = fixture.surface.act(&admin, step.clone()).await;
        assert!(result.is_ok(), "{step:?}: {result:?}");
    }
    alert
}

/// INV-363 (`surface.action.alert-lifecycle-model`): for every state an
/// alert can be in, what acknowledging and resolving it return and the
/// state they leave, stamped with the caller, the acceptance time and the
/// note.
#[tokio::test]
async fn alert_action_transition_table() {
    for (n, start) in [
        Start::Open,
        Start::Acknowledged,
        Start::Resolved,
        Start::Suppressed,
    ]
    .into_iter()
    .enumerate()
    {
        for resolve in [false, true] {
            let fixture = Fixture::new().await;
            let alert = alert_in(&fixture, start, n as u128).await;
            let triager = fixture.caller(Who::Triager).await;
            let at = Timestamp::from_micros(minute(5).as_micros() + 7);
            fixture.clock.set(at);
            let before = match fixture.surface.alert(&triager, alert).await {
                Ok(Some(alert)) => alert.state,
                other => panic!("alert: {other:?}"),
            };
            let note = Some("checked".to_owned());
            let action = if resolve {
                OperatorAction::Resolve {
                    alert,
                    note: note.clone(),
                }
            } else {
                OperatorAction::Acknowledge { alert }
            };
            let result = fixture.surface.act(&triager, action).await;
            let by = triager.operator();
            let (expected, after) = match (start, resolve) {
                (Start::Open, false) => (
                    Ok(ActionOutcome::Applied),
                    AlertState::Acknowledged { by, at },
                ),
                (Start::Open, true) => (
                    Err(ActionError::Conflict(ConflictKind::AlertNotAcknowledged {
                        alert,
                    })),
                    before.clone(),
                ),
                (Start::Acknowledged, false) => (Ok(ActionOutcome::Unchanged), before.clone()),
                (Start::Acknowledged, true) => (
                    Ok(ActionOutcome::Applied),
                    AlertState::Resolved {
                        by,
                        at,
                        note: note.clone(),
                    },
                ),
                (Start::Resolved | Start::Suppressed, _) => (
                    Err(ActionError::Conflict(ConflictKind::AlertNotActive {
                        alert,
                    })),
                    before.clone(),
                ),
            };
            assert_eq!(result, expected, "{start:?}, resolve {resolve}");
            let state = match fixture.surface.alert(&triager, alert).await {
                Ok(Some(alert)) => alert.state,
                other => panic!("alert: {other:?}"),
            };
            assert_eq!(state, after, "{start:?}, resolve {resolve}");
            if start == Start::Suppressed {
                assert!(matches!(
                    state,
                    AlertState::Suppressed {
                        reason: SuppressReason::RuleDisabled,
                        ..
                    }
                ));
            }
        }
    }
    let fixture = Fixture::new().await;
    let triager = fixture.caller(Who::Triager).await;
    assert_eq!(
        fixture
            .surface
            .act(
                &triager,
                OperatorAction::Acknowledge {
                    alert: AlertId::from_ulid(0xFFFF)
                }
            )
            .await,
        Err(ActionError::NotFound)
    );
}

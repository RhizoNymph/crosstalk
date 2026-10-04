//! Triage and pipeline actions, and the audit entry every action leaves.

use crosstalk_spec::aggregates::alert::{AlertState, AlertSubject};
use crosstalk_spec::derived::flow::transmission::TransmissionState;
use crosstalk_spec::ids::{AlertId, ChannelId, TransmissionId};
use crosstalk_spec::interfaces::l8_surface::{Permission, PolicyKind};

use super::super::actions::effects::FALSE_DETECTION_NOTE;
use super::super::clock::NOW;
use super::super::world::ChannelKey;
use super::{caller, first, fresh, researcher, scope_with, week};
use crate::backend::Backend;
use crate::contract::actions::{ActionOutcome, OperatorAction};
use crate::contract::agents::AgentLabel;
use crate::contract::errors::{ConflictKind, QueryError};
use crate::contract::graph::TransmissionSelector;
use crate::contract::research::{AuditOutcome, AuditedAction};
use crate::contract::scope::{TopologyFilter, VerdictFilter};
use crate::contract::verdict::Verdict;

use super::actions_support::*;

#[tokio::test]
async fn every_action_appends_one_audit_entry() {
    let b = fresh();
    let c = researcher();
    let wiki = channel(&b, ChannelKey::HijackedWiki);
    let actions = [
        (
            OperatorAction::SetPolicy {
                channel: wiki,
                policy: PolicyKind::Unsanctioned,
                note: None,
            },
            true,
        ),
        (
            OperatorAction::SetPolicy {
                channel: ChannelId::from_ulid(1),
                policy: PolicyKind::Sanctioned,
                note: None,
            },
            false,
        ),
        (
            OperatorAction::RenameAgent {
                agent: agent(&b, "cc1"),
                label: Some(AgentLabel::new("lead").expect("label")),
            },
            true,
        ),
        (
            OperatorAction::RenameAgent {
                agent: agent(&b, "al0"),
                label: None,
            },
            false,
        ),
    ];
    for (action, ok) in actions {
        let before = audit_len(&b).await;
        let result = b.act(&c, action.clone()).await;
        assert_eq!(result.is_ok(), ok, "{action:?}: {result:?}");
        let state = b.state.read().await;
        assert_eq!(state.audit.len(), before + 1);
        let last = &state.audit.last().expect("entry").entry;
        assert_eq!(last.at, NOW);
        assert_eq!(last.action, AuditedAction::Operator(action));
        assert_eq!(matches!(last.outcome, AuditOutcome::Applied), ok);
    }
    // A forbidden action is audited as rejected too.
    let before = audit_len(&b).await;
    let viewer = caller(&[Permission::View]);
    let denied = b
        .act(
            &viewer,
            OperatorAction::SetPolicy {
                channel: wiki,
                policy: PolicyKind::Sanctioned,
                note: None,
            },
        )
        .await;
    assert_eq!(
        denied.err(),
        Some(QueryError::Forbidden {
            missing: Permission::Govern
        })
    );
    assert_eq!(audit_len(&b).await, before + 1);
    let state = b.state.read().await;
    assert!(matches!(
        state.audit.last().expect("entry").entry.outcome,
        AuditOutcome::Rejected(QueryError::Forbidden { .. })
    ));
}

#[tokio::test]
async fn verdicts_judge_only_what_has_evidence() {
    let b = fresh();
    let c = researcher();
    let pick = |f: fn(&TransmissionState) -> bool| -> TransmissionId {
        b.world
            .transmissions
            .iter()
            .find(|t| f(&t.transmission.state))
            .expect("transmission")
            .transmission
            .id
    };
    let detected = pick(|s| matches!(s, TransmissionState::Detected));
    let awaiting = pick(|s| matches!(s, TransmissionState::AwaitingContent { .. }));
    for id in [detected, awaiting] {
        let result = b
            .act(
                &c,
                OperatorAction::SetVerdict {
                    transmission: id,
                    verdict: Some(Verdict::Genuine),
                    note: None,
                },
            )
            .await;
        assert_eq!(result.err(), conflict(ConflictKind::NotJudgeable));
    }
    // A false detection resolves the transmission's active alerts.
    let (alert, tx) = {
        let state = b.state.read().await;
        let a = state
            .alerts
            .iter()
            .find(|a| {
                a.state == AlertState::Open && matches!(a.subject, AlertSubject::Transmission(_))
            })
            .expect("open transmission alert");
        let AlertSubject::Transmission(tx) = a.subject else {
            unreachable!()
        };
        (a.id, tx)
    };
    let verdict = OperatorAction::SetVerdict {
        transmission: tx,
        verdict: Some(Verdict::FalseDetection),
        note: Some("noise".into()),
    };
    assert_eq!(b.act(&c, verdict.clone()).await, Ok(ActionOutcome::Applied));
    assert!(matches!(
        alert_state(&b, alert).await,
        AlertState::Resolved { note: Some(n), at, .. } if n == FALSE_DETECTION_NOTE && at == NOW
    ));
    let excluded = scope_with(
        week().window,
        TopologyFilter {
            verdicts: VerdictFilter::ExcludeFalseDetections,
            ..Default::default()
        },
    );
    let ids = TransmissionSelector::Ids(vec![tx]);
    assert!(
        b.transmissions(&c, &excluded, &ids, &first(5))
            .await
            .expect("rows")
            .items
            .is_empty()
    );
    // Withdrawing it puts the transmission back.
    b.act(
        &c,
        OperatorAction::SetVerdict {
            transmission: tx,
            verdict: None,
            note: None,
        },
    )
    .await
    .expect("withdraw");
    let rows = b
        .transmissions(&c, &excluded, &ids, &first(5))
        .await
        .expect("rows")
        .items;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].verdict, None);
    // Judging needs Content as well as Triage.
    let triage = caller(&[Permission::View, Permission::Triage]);
    assert_eq!(
        b.act(&triage, verdict).await.err(),
        Some(QueryError::Forbidden {
            missing: Permission::Content
        })
    );
}

#[tokio::test]
async fn acknowledge_and_resolve_follow_the_alert_lifecycle() {
    let b = fresh();
    let c = researcher();
    let open = find_alert(&b, |a| a.state == AlertState::Open).await;
    assert_eq!(
        b.act(&c, OperatorAction::Acknowledge { alert: open }).await,
        Ok(ActionOutcome::Applied)
    );
    assert_eq!(
        alert_state(&b, open).await,
        AlertState::Acknowledged {
            by: c.operator,
            at: NOW
        }
    );
    assert_eq!(
        b.act(&c, OperatorAction::Acknowledge { alert: open })
            .await
            .err(),
        conflict(ConflictKind::AlertState)
    );
    b.act(
        &c,
        OperatorAction::Resolve {
            alert: open,
            note: Some("done".into()),
        },
    )
    .await
    .expect("resolve");
    assert_eq!(
        alert_state(&b, open).await,
        AlertState::Resolved {
            by: c.operator,
            at: NOW,
            note: Some("done".into())
        }
    );
    assert_eq!(
        b.act(
            &c,
            OperatorAction::Resolve {
                alert: open,
                note: None
            }
        )
        .await
        .err(),
        conflict(ConflictKind::AlertState)
    );
    let suppressed = find_alert(&b, |a| matches!(a.state, AlertState::Suppressed { .. })).await;
    assert_eq!(
        b.act(&c, OperatorAction::Acknowledge { alert: suppressed })
            .await
            .err(),
        conflict(ConflictKind::AlertState)
    );
    let other_open = find_alert(&b, |a| a.state == AlertState::Open).await;
    b.act(
        &c,
        OperatorAction::Resolve {
            alert: other_open,
            note: None,
        },
    )
    .await
    .expect("open alerts resolve directly");
    assert_eq!(
        b.act(
            &c,
            OperatorAction::Acknowledge {
                alert: AlertId::from_ulid(3)
            }
        )
        .await
        .err(),
        Some(QueryError::NotFound)
    );
}

#[tokio::test]
async fn replaying_a_dead_letter_removes_it() {
    let b = fresh();
    let c = researcher();
    let letter = b
        .dead_letters(&c, &first(1))
        .await
        .expect("letters")
        .items
        .remove(0);
    let replay = OperatorAction::ReplayDeadLetter {
        group: letter.group.clone(),
        id: letter.envelope.id,
    };
    let before = b.state.read().await.dead_letters.len();
    let triage = caller(&[Permission::View, Permission::Triage]);
    assert_eq!(
        b.act(&triage, replay.clone()).await.err(),
        Some(QueryError::Forbidden {
            missing: Permission::Operate
        })
    );
    assert_eq!(b.act(&c, replay.clone()).await, Ok(ActionOutcome::Applied));
    assert_eq!(b.state.read().await.dead_letters.len(), before - 1);
    assert_eq!(b.act(&c, replay).await.err(), Some(QueryError::NotFound));
}

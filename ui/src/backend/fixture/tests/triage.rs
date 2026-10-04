//! Triage and pipeline actions, and the audit entry every action leaves.

use crosstalk_spec::aggregates::alert::AlertSubject;
use crosstalk_spec::derived::flow::transmission::TransmissionState;
use crosstalk_spec::ids::{AlertId, ChannelId, TransmissionId};
use crosstalk_spec::interfaces::l8_surface::{Permission, PolicyKind};

use super::super::clock::NOW;
use super::super::world::ChannelKey;
use super::{caller, first, fresh, researcher, scope_with, week};
use crate::backend::Backend;
use crate::url::scope::ViewFilter;
use crosstalk_spec::aggregates::alert::{AlertState, SuppressReason};
use crosstalk_spec::aggregates::filter::{FalseDetections, TopicVersionSelector};
use crosstalk_spec::derived::flow::verdict::Verdict;
use crosstalk_spec::interfaces::l8_surface::ConflictKind;
use crosstalk_spec::interfaces::l8_surface::audit::{
    AuditBody, AuditEntry, AuditOutcome, OperatorRecord,
};
use crosstalk_spec::interfaces::l8_surface::summary::TransmissionSelection;
use crosstalk_spec::interfaces::l8_surface::{ActionError, ActionOutcome, OperatorAction};
use crosstalk_spec::observed::agent::AgentLabel;

use super::actions_support::*;

/// The operator record of an entry `act` appended.
fn record(entry: &AuditEntry) -> &OperatorRecord {
    match &entry.body {
        AuditBody::Operator(record) => record,
        other => panic!("an operator entry, got {other:?}"),
    }
}

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
        let last = state.audit.entries().last().expect("entry");
        assert_eq!(last.at, NOW);
        assert_eq!(record(last).action(), &action);
        assert_eq!(record(last).caller(), &c, "the caller as authenticated");
        assert_eq!(record(last).outcome().result(), result, "what act returned");
        assert_eq!(
            matches!(record(last).outcome(), AuditOutcome::Succeeded(_)),
            ok
        );
    }
    // A forbidden action is audited as forbidden, naming the permission.
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
        Some(ActionError::Forbidden {
            missing: Permission::Govern
        })
    );
    assert_eq!(audit_len(&b).await, before + 1);
    let state = b.state.read().await;
    assert_eq!(
        record(state.audit.entries().last().expect("entry")).outcome(),
        &AuditOutcome::Forbidden {
            missing: Permission::Govern
        }
    );
}

#[tokio::test]
async fn audit_entries_name_their_subject_and_what_they_created() {
    use crosstalk_spec::interfaces::l8_surface::audit::{AuditFilter, AuditSubject};

    let b = fresh();
    let c = researcher();
    let (cc6, cc5) = (agent(&b, "cc6"), agent(&b, "cc5"));
    let ActionOutcome::Merged(id) = b.act(&c, merge(&b, "cc6", "cc5")).await.expect("merge") else {
        panic!("a merge")
    };
    {
        let state = b.state.read().await;
        let last = state.audit.entries().last().expect("entry");
        assert_eq!(
            last.subjects(),
            [
                AuditSubject::Agent(cc6),
                AuditSubject::Agent(cc5),
                AuditSubject::Merge(id)
            ],
            "both agents as requested, then the record it created"
        );
        assert_eq!(
            record(last).outcome(),
            &AuditOutcome::Succeeded(ActionOutcome::Merged(id))
        );
    }
    b.act(&c, OperatorAction::Unmerge { merge: id })
        .await
        .expect("unmerge");
    {
        let state = b.state.read().await;
        let last = state.audit.entries().last().expect("entry");
        assert_eq!(last.subjects(), [AuditSubject::Merge(id)]);
        assert_eq!(
            record(last).outcome(),
            &AuditOutcome::Succeeded(ActionOutcome::Applied)
        );
    }
    // The merge record finds both entries; each agent finds the merge it
    // took part in (ids are matched as recorded: the unmerge names only
    // the record).
    for (subject, entries) in [
        (AuditSubject::Merge(id), 2),
        (AuditSubject::Agent(cc6), 1),
        (AuditSubject::Agent(cc5), 1),
    ] {
        let filter = AuditFilter {
            subject: Some(subject),
            ..AuditFilter::default()
        };
        let page = b.audit(&c, &filter, &first(50)).await.expect("audit");
        let ours: Vec<_> = page.items().iter().filter(|e| e.at == NOW).collect();
        assert_eq!(ours.len(), entries, "{subject:?}");
        assert!(
            ours.iter().all(|e| e.subjects().contains(&subject)),
            "{subject:?}"
        );
    }
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
        assert!(matches!(
            result.err(),
            Some(ActionError::Conflict(
                ConflictKind::TransmissionNotJudgeable { .. }
            ))
        ));
    }
    // A false detection suppresses the transmission's active alerts.
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
        AlertState::Suppressed { reason: SuppressReason::OperatorRejected, at } if at == NOW
    ));
    let excluded = scope_with(
        week().window,
        ViewFilter {
            false_detections: FalseDetections::Exclude,
            ..Default::default()
        },
    );
    // The verdict already in force appends nothing.
    let records = |b: &super::super::FixtureBackend| {
        let state = b.state.try_read().expect("unlocked");
        state.verdicts.get(&tx).map_or(0, |log| log.records().len())
    };
    let before = records(&b);
    assert_eq!(
        b.act(&c, verdict.clone()).await,
        Ok(ActionOutcome::Unchanged)
    );
    assert_eq!(records(&b), before);
    let one = TransmissionSelection::new(vec![tx]).expect("selection");
    let row = b
        .transmissions_by_id(&c, &one, TopicVersionSelector::Current, &first(5))
        .await
        .expect("rows")
        .page
        .into_parts()
        .0;
    assert_eq!(
        row.first().and_then(|t| t.state.verdict()),
        Some(Verdict::FalseDetection)
    );
    assert!(
        super::reads_support::rows_in(&b, &excluded)
            .await
            .iter()
            .all(|t| t.id != tx)
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
    let rows: Vec<_> = super::reads_support::rows_in(&b, &excluded)
        .await
        .into_iter()
        .filter(|t| t.id == tx)
        .collect();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].state.verdict(), None);
    let log = b.verdicts(&c, tx).await.expect("ok").expect("found");
    assert_eq!(log.current(), None);
    assert_eq!(
        log.records().len(),
        before + 1,
        "the withdrawal is appended"
    );
    // Judging needs Triage alone (a verdict reveals no content); a viewer
    // is forbidden and nothing is appended.
    let viewer = caller(&[Permission::View, Permission::Content]);
    assert_eq!(
        b.act(&viewer, verdict.clone()).await.err(),
        Some(ActionError::Forbidden {
            missing: Permission::Triage
        })
    );
    assert_eq!(records(&b), before + 1);
    let triage = caller(&[Permission::View, Permission::Triage]);
    assert_eq!(b.act(&triage, verdict).await, Ok(ActionOutcome::Applied));
    let log = b.verdicts(&c, tx).await.expect("ok").expect("found");
    assert_eq!(
        log.records().last().map(|r| r.by()),
        Some(triage.operator())
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
            by: c.operator(),
            at: NOW
        }
    );
    // Acknowledging it again matches the state already there: accepted as
    // `Unchanged`, and the first acknowledgement stays.
    let oncall = caller(&[Permission::View, Permission::Triage]);
    assert_eq!(
        b.act(&oncall, OperatorAction::Acknowledge { alert: open })
            .await,
        Ok(ActionOutcome::Unchanged)
    );
    assert_eq!(
        alert_state(&b, open).await,
        AlertState::Acknowledged {
            by: c.operator(),
            at: NOW
        }
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
            by: c.operator(),
            at: NOW,
            note: Some("done".into())
        }
    );
    // Resolving it again is a repeat of the transition: `Unchanged`, and
    // who resolved it stays.
    assert_eq!(
        b.act(
            &c,
            OperatorAction::Resolve {
                alert: open,
                note: None
            }
        )
        .await,
        Ok(ActionOutcome::Unchanged)
    );
    assert!(matches!(
        alert_state(&b, open).await,
        AlertState::Resolved { note: Some(_), .. }
    ));
    assert!(matches!(
        b.act(&c, OperatorAction::Acknowledge { alert: open })
            .await
            .err(),
        Some(ActionError::Conflict(ConflictKind::AlertNotActive { .. }))
    ));
    let suppressed = find_alert(&b, |a| matches!(a.state, AlertState::Suppressed { .. })).await;
    assert!(matches!(
        b.act(&c, OperatorAction::Acknowledge { alert: suppressed })
            .await
            .err(),
        Some(ActionError::Conflict(ConflictKind::AlertNotActive { .. }))
    ));
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
        Some(ActionError::NotFound)
    );
}

#[tokio::test]
async fn replaying_a_dead_letter_removes_it() {
    let b = fresh();
    let c = researcher();
    let letter = b
        .dead_letters(&c, None, &first(1))
        .await
        .expect("letters")
        .into_parts()
        .0
        .remove(0);
    let replay = OperatorAction::ReplayDeadLetter {
        group: letter.group.clone(),
        id: letter.envelope.id,
    };
    let before = b.state.read().await.dead_letters.len();
    let triage = caller(&[Permission::View, Permission::Triage]);
    assert_eq!(
        b.act(&triage, replay.clone()).await.err(),
        Some(ActionError::Forbidden {
            missing: Permission::Operate
        })
    );
    assert_eq!(b.act(&c, replay.clone()).await, Ok(ActionOutcome::Applied));
    assert_eq!(b.state.read().await.dead_letters.len(), before - 1);
    assert_eq!(b.act(&c, replay).await.err(), Some(ActionError::NotFound));
}

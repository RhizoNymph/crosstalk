//! Properties of operator actions: what `SetPolicy` publishes, and one audit
//! entry per call whose outcome inverts to the call's result.

use crosstalk_spec::derived::flow::channel::policy::{PolicyAuthor, PolicyKind};
use crosstalk_spec::derived::flow::verdict::Verdict;
use crosstalk_spec::events::BusEvent;
use crosstalk_spec::events::Envelope;
use crosstalk_spec::events::insight::InsightEvent;
use crosstalk_spec::ids::{EventId, MergeId};
use crosstalk_spec::interfaces::l2_transport::ConsumerGroup;
use crosstalk_spec::interfaces::l8_surface::{
    ActionError, CallerSnapshot, OperatorAction, OperatorActions,
};
use crosstalk_spec::observed::agent::AgentLabel;
use crosstalk_spec::support::Timestamp;
use proptest::collection::vec;
use proptest::option;
use proptest::sample::select;
use proptest::strategy::Strategy;

use super::{ensure, equal, property};
use crate::tests::world::{Fixture, Who, minute};

const KINDS: [PolicyKind; 3] = [
    PolicyKind::Unreviewed,
    PolicyKind::Sanctioned,
    PolicyKind::Unsanctioned,
];

/// A `SetPolicy` request: who sends it, the kind, the note.
fn set_policies() -> impl Strategy<Value = Vec<(Who, PolicyKind, Option<String>)>> {
    vec(
        (
            select(vec![Who::Admin, Who::Governor]),
            select(KINDS.to_vec()),
            option::of("[a-z]{1,8}"),
        ),
        1..6,
    )
}

fn policy_events(
    envelopes: &[Envelope],
) -> Vec<(
    crosstalk_spec::ids::ChannelId,
    crosstalk_spec::derived::flow::channel::policy::Policy,
)> {
    envelopes
        .iter()
        .filter_map(|envelope| match &envelope.event {
            BusEvent::Insight(InsightEvent::PolicyChanged { channel, policy }) => {
                Some((*channel, policy.clone()))
            }
            _ => None,
        })
        .collect()
}

/// INV-367: for every kind and caller, the published decision is authored
/// by the caller's operator at the acceptance time.
#[test]
fn prop_set_policy_decision_author_is_caller() {
    property(24, set_policies(), |requests| async move {
        let fixture = Fixture::new().await;
        let scene = fixture.scene().await;
        for (step, (who, kind, note)) in requests.into_iter().enumerate() {
            let at = Timestamp::from_micros(minute(10).as_micros() + step as u64 * 1_000);
            fixture.clock.set(at);
            let caller = fixture.caller(who).await;
            let action = OperatorAction::SetPolicy {
                channel: scene.c1,
                policy: kind,
                note,
            };
            let result = fixture.surface.act(&caller, action).await;
            ensure(result.is_ok(), || format!("step {step}: {result:?}"))?;
            let events = policy_events(&fixture.world.bus.published());
            let Some((_, policy)) = events.last() else {
                return Err("nothing published".to_owned());
            };
            let Some(decision) = policy.decision() else {
                return Err(format!("{policy:?} carries no decision"));
            };
            equal(
                "author",
                &decision.by,
                &PolicyAuthor::Operator(caller.operator()),
            )?;
            equal("time", &decision.at, &at)?;
        }
        Ok(())
    });
}

/// INV-368: the published event names the channel, has the requested
/// kind's variant and the note, `Unreviewed` included.
#[test]
fn prop_set_policy_event_matches_request() {
    property(24, set_policies(), |requests| async move {
        let fixture = Fixture::new().await;
        let scene = fixture.scene().await;
        let count = requests.len();
        for (step, (who, kind, note)) in requests.into_iter().enumerate() {
            fixture.clock.set(minute(10 + step as u64));
            let caller = fixture.caller(who).await;
            let action = OperatorAction::SetPolicy {
                channel: scene.c1,
                policy: kind,
                note: note.clone(),
            };
            ensure(fixture.surface.act(&caller, action).await.is_ok(), || {
                format!("step {step}")
            })?;
            let events = policy_events(&fixture.world.bus.published());
            let Some((channel, policy)) = events.last() else {
                return Err("nothing published".to_owned());
            };
            equal("channel", channel, &scene.c1)?;
            equal("kind", &policy.kind(), &kind)?;
            let carried = policy.decision().and_then(|decision| decision.note.clone());
            equal("note", &carried, &note)?;
        }
        equal(
            "published",
            &policy_events(&fixture.world.bus.published()).len(),
            &count,
        )
    });
}

/// The action pool the audit property draws from.
fn action(
    scene: &crate::tests::world::Scene,
    caller: &crosstalk_spec::interfaces::l8_surface::Caller,
    pick: u8,
    step: usize,
) -> OperatorAction {
    match pick % 9 {
        0 => OperatorAction::Acknowledge { alert: scene.alert },
        1 => OperatorAction::Resolve {
            alert: scene.alert,
            note: Some(format!("step {step}")),
        },
        2 => OperatorAction::RenameAgent {
            agent: scene.a2,
            label: AgentLabel::new(&format!("label {}", step % 2)).ok(),
        },
        3 => OperatorAction::SetPolicy {
            channel: scene.c1,
            policy: KINDS[step % 3],
            note: None,
        },
        4 => OperatorAction::SetVerdict {
            transmission: scene.t1.transmission.id,
            verdict: Some(if step.is_multiple_of(2) {
                Verdict::Genuine
            } else {
                Verdict::FalseDetection
            }),
            note: None,
        },
        5 => OperatorAction::Unmerge {
            merge: MergeId::from_ulid(0x77),
        },
        6 => OperatorAction::ReplayDeadLetter {
            group: ConsumerGroup("flow".to_owned()),
            id: EventId::from_ulid(0x99),
        },
        7 => OperatorAction::PinTopicVersion {
            version: crosstalk_spec::aggregates::topic::TopicModelVersion(0),
        },
        _ => OperatorAction::merge_agents(caller, scene.a3, scene.a1)
            .unwrap_or(OperatorAction::Acknowledge { alert: scene.alert }),
    }
}

/// INV-458: every call leaves exactly one operator entry (at most one for
/// `Store`), with the caller's snapshot, the action and an outcome whose
/// `result` is what the call returned.
#[test]
fn prop_one_audit_record_per_act_call() {
    let calls = vec((select(Who::ALL.to_vec()), 0_u8..9), 1..10);
    property(24, calls, |calls| async move {
        let fixture = Fixture::new().await;
        let scene = fixture.scene().await;
        for (step, (who, pick)) in calls.into_iter().enumerate() {
            fixture.clock.set(minute(10 + step as u64));
            let caller = fixture.caller(who).await;
            let action = action(&scene, &caller, pick, step);
            let before = fixture.operator_entries().await.len();
            let result = fixture.surface.act(&caller, action.clone()).await;
            let entries = fixture.operator_entries().await;
            let added = entries.len() - before;
            if matches!(result, Err(ActionError::Store { .. })) {
                ensure(added <= 1, || {
                    format!("step {step}: {added} entries for a store failure")
                })?;
                continue;
            }
            equal("entries added", &added, &1)?;
            let (at, record) = &entries[0];
            equal("time", at, &minute(10 + step as u64))?;
            equal("caller", record.caller(), &CallerSnapshot::of(&caller))?;
            equal("action", record.action(), &action)?;
            equal("outcome", &record.outcome().result(), &result)?;
        }
        Ok(())
    });
}

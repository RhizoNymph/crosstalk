//! What `act` returns as the spec's `OperatorActions::act` defines it: the
//! one permission checked before any effect, `Unchanged` where the state
//! already matched, topic-version pins and the authors and times stamped
//! from the caller and the acceptance time.

use crosstalk_spec::aggregates::retention::{Pin, Retention};
use crosstalk_spec::aggregates::topic::TopicModelVersion;
use crosstalk_spec::derived::flow::verdict::Verdict;
use crosstalk_spec::ids::EventId;
use crosstalk_spec::interfaces::l2_transport::ConsumerGroup;
use crosstalk_spec::interfaces::l8_surface::{
    ActionError, ActionOutcome, ConflictKind, OperatorAction, Permission, PolicyKind,
};
use crosstalk_spec::observed::agent::AgentLabel;

use super::super::FixtureBackend;
use super::super::clock::NOW;
use super::super::world::ChannelKey;
use super::actions_support::{agent, audit_len, channel, find_alert, merge};
use super::{caller, fresh, researcher};
use crate::backend::Backend;
use crosstalk_spec::aggregates::alert::{AlertState, BuiltinRule};

/// Everything an action can change, the audit log and the id mint aside.
async fn effects(b: &FixtureBackend) -> String {
    let state = b.state.read().await;
    format!(
        "{:?}{:?}{:?}{:?}{:?}{:?}{:?}{:?}",
        state.identity,
        state.channels,
        state.catalog,
        state.verdicts,
        state.alerts,
        state.rules,
        state.dead_letters,
        state.projections
    )
}

/// One action of every kind, each one a caller with every permission could
/// apply to the generated world.
async fn one_of_each(b: &FixtureBackend) -> Vec<OperatorAction> {
    let pastebin = channel(b, ChannelKey::Pastebin);
    let wiki = channel(b, ChannelKey::HijackedWiki);
    let open = find_alert(b, |a| a.state == AlertState::Open).await;
    let (letter, group) = {
        let state = b.state.read().await;
        let letter = state.dead_letters.first().expect("a dead letter");
        (letter.envelope.id, letter.group.clone())
    };
    let judged = b.transmission_ids().into_iter().find(|id| {
        b.world
            .tx(*id)
            .is_some_and(|record| record.transmission.state.judgeable().is_ok())
    });
    let mut actions = vec![
        OperatorAction::SetPolicy {
            channel: pastebin,
            policy: PolicyKind::Sanctioned,
            note: None,
        },
        merge(b, "cc6", "cc5"),
        OperatorAction::RenameAgent {
            agent: agent(b, "cc1"),
            label: AgentLabel::new("probe").ok(),
        },
        OperatorAction::PromoteChannel {
            channel: wiki,
            pattern: crosstalk_spec::derived::flow::resource::ResourcePattern::Host(
                crosstalk_spec::derived::flow::resource::Host("wiki.example.org".into()),
            ),
            policy: PolicyKind::Unsanctioned,
            note: None,
        },
        OperatorAction::Acknowledge { alert: open },
        OperatorAction::Resolve {
            alert: open,
            note: None,
        },
        OperatorAction::SetRuleEnabled {
            id: BuiltinRule::UnreviewedTraffic.id(),
            enabled: false,
        },
        OperatorAction::ReplayDeadLetter { group, id: letter },
        OperatorAction::PinTopicVersion {
            version: TopicModelVersion(2),
        },
        OperatorAction::UnpinTopicVersion {
            version: TopicModelVersion(1),
        },
    ];
    if let Some(transmission) = judged {
        actions.push(OperatorAction::SetVerdict {
            transmission,
            verdict: Some(Verdict::Genuine),
            note: None,
        });
    }
    actions
}

#[tokio::test]
async fn the_one_permission_is_checked_before_any_effect() {
    let b = fresh();
    let viewer = caller(&[Permission::View, Permission::Content, Permission::Audit]);
    let before = effects(&b).await;
    let actions = one_of_each(&b).await;
    let mut audited = audit_len(&b).await;
    for action in actions {
        let required = action.required_permission();
        assert_eq!(
            b.act(&viewer, action.clone()).await,
            Err(ActionError::Forbidden { missing: required }),
            "{action:?}"
        );
        audited += 1;
        assert_eq!(audit_len(&b).await, audited, "one entry per call");
    }
    assert_eq!(effects(&b).await, before, "nothing but the log changed");
}

#[tokio::test]
async fn set_verdict_needs_triage_alone() {
    let b = fresh();
    let action = one_of_each(&b)
        .await
        .into_iter()
        .find(|a| matches!(a, OperatorAction::SetVerdict { .. }))
        .expect("a judgeable transmission");
    assert_eq!(action.required_permission(), Permission::Triage);
    let triage = caller(&[Permission::Triage]);
    assert_eq!(b.act(&triage, action).await, Ok(ActionOutcome::Applied));
}

#[tokio::test]
async fn pins_follow_the_catalog_and_stamp_the_caller() {
    let b = fresh();
    let c = researcher();
    let retention = async |version: u32| {
        b.state
            .read()
            .await
            .catalog
            .get(TopicModelVersion(version))
            .expect("version")
            .retention()
    };
    let pin = |version: u32| OperatorAction::PinTopicVersion {
        version: TopicModelVersion(version),
    };
    let unpin = |version: u32| OperatorAction::UnpinTopicVersion {
        version: TopicModelVersion(version),
    };
    assert_eq!(b.act(&c, pin(2)).await, Ok(ActionOutcome::Applied));
    assert_eq!(
        retention(2).await,
        Retention::Retained {
            pin: Some(Pin {
                by: c.operator(),
                at: NOW
            })
        }
    );
    // An existing pin keeps its author.
    let other = caller(&[Permission::Govern]);
    assert_eq!(b.act(&other, pin(2)).await, Ok(ActionOutcome::Unchanged));
    assert_eq!(retention(2).await.pin().map(|p| p.by), Some(c.operator()));
    assert_eq!(
        b.act(&c, pin(0)).await,
        Err(ActionError::Conflict(ConflictKind::TopicVersionDropped {
            version: TopicModelVersion(0)
        }))
    );
    assert_eq!(b.act(&c, pin(9)).await, Err(ActionError::NotFound));
    assert_eq!(b.act(&c, unpin(9)).await, Err(ActionError::NotFound));
    // v1 is pinned, and the last two activated versions are kept anyway.
    assert_eq!(b.act(&c, unpin(1)).await, Ok(ActionOutcome::Applied));
    assert_eq!(retention(1).await, Retention::UNPINNED);
    assert_eq!(b.act(&c, unpin(1)).await, Ok(ActionOutcome::Unchanged));
    assert_eq!(
        b.act(&c, unpin(0)).await,
        Ok(ActionOutcome::Unchanged),
        "a dropped version has no pin"
    );
    let versions = b.topic_versions(&c).await.expect("history");
    assert_eq!(
        versions
            .get(TopicModelVersion(2))
            .and_then(|info| info.retention().pin())
            .map(|p| p.by),
        Some(c.operator()),
        "reads see the pin"
    );
}

#[tokio::test]
async fn repeats_report_unchanged() {
    let b = fresh();
    let c = researcher();
    let rename = OperatorAction::RenameAgent {
        agent: agent(&b, "cc1"),
        label: AgentLabel::new("planner").ok(),
    };
    assert_eq!(b.act(&c, rename.clone()).await, Ok(ActionOutcome::Applied));
    assert_eq!(b.act(&c, rename).await, Ok(ActionOutcome::Unchanged));

    let disable = OperatorAction::SetRuleEnabled {
        id: BuiltinRule::UnreviewedTraffic.id(),
        enabled: false,
    };
    assert_eq!(b.act(&c, disable.clone()).await, Ok(ActionOutcome::Applied));
    assert_eq!(b.act(&c, disable).await, Ok(ActionOutcome::Unchanged));

    // The same decision at the same time is already in the history.
    let policy = OperatorAction::SetPolicy {
        channel: channel(&b, ChannelKey::Pastebin),
        policy: PolicyKind::Sanctioned,
        note: Some("vendor".into()),
    };
    assert_eq!(b.act(&c, policy.clone()).await, Ok(ActionOutcome::Applied));
    assert_eq!(b.act(&c, policy).await, Ok(ActionOutcome::Unchanged));

    let replay = OperatorAction::ReplayDeadLetter {
        group: ConsumerGroup("nobody".to_owned()),
        id: EventId::from_ulid(1),
    };
    assert_eq!(b.act(&c, replay).await, Err(ActionError::NotFound));
}

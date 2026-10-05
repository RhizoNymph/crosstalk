//! Action outcomes and refusals: verdicts, topic-version pins, and every
//! layer's refusal mapped through its one `ActionError::from`.

use crosstalk_spec::aggregates::alert::{BuiltinRule, UserRule};
use crosstalk_spec::aggregates::retention::Pin;
use crosstalk_spec::aggregates::topic::TopicModelVersion;
use crosstalk_spec::derived::flow::channel::policy::PolicyKind;
use crosstalk_spec::derived::flow::resource::{Host, ResourcePattern};
use crosstalk_spec::derived::flow::verdict::Verdict;
use crosstalk_spec::ids::{AgentId, AlertRuleId, ChannelId, MergeId, TransmissionId};
use crosstalk_spec::interfaces::l5_flow::verdicts::TransmissionVerdicts;
use crosstalk_spec::interfaces::l6_analysis::TopicCatalog;
use crosstalk_spec::interfaces::l8_surface::{
    ActionError, ActionOutcome, ActionRequest, ConflictKind, InputError, OperatorAction,
    OperatorActions,
};
use crosstalk_spec::support::NonEmpty;
use crosstalk_testkit::build::TransmissionBuilder;

use super::actions::{accepted, fixture_at_accepted, rule_name, semantic, wiki};
use super::world::{Who, minute, sink};

/// INV-509: a verdict is recorded with the caller, the acceptance time and
/// the note.
#[tokio::test]
async fn set_verdict_stamps_caller() {
    let fixture = fixture_at_accepted().await;
    let scene = fixture.scene().await;
    let caller = fixture.caller(Who::Triager).await;
    let id = scene.t1.transmission.id;
    let action = OperatorAction::SetVerdict {
        transmission: id,
        verdict: Some(Verdict::FalseDetection),
        note: Some("echo".to_owned()),
    };
    assert_eq!(
        fixture.surface.act(&caller, action).await,
        Ok(ActionOutcome::Applied)
    );
    let Ok(log) = fixture.world.transmissions.log(id).await else {
        panic!("log");
    };
    let Some(record) = log.records().last() else {
        panic!("no record");
    };
    assert_eq!(record.by(), caller.operator());
    assert_eq!(record.at(), accepted());
    assert_eq!(record.note(), Some("echo"));
    assert_eq!(record.verdict(), Some(Verdict::FalseDetection));
}

/// INV-577 and INV-509: pin and unpin outcomes, the pin stamped with the
/// caller and the acceptance time.
#[tokio::test]
async fn pin_and_unpin_outcomes() {
    let fixture = fixture_at_accepted().await;
    let caller = fixture.caller(Who::Governor).await;
    let pin = |version| OperatorAction::PinTopicVersion { version };
    let unpin = |version| OperatorAction::UnpinTopicVersion { version };
    let unknown = TopicModelVersion(99);
    assert_eq!(
        fixture.surface.act(&caller, pin(unknown)).await,
        Err(ActionError::NotFound)
    );
    assert_eq!(
        fixture.surface.act(&caller, unpin(unknown)).await,
        Err(ActionError::NotFound)
    );

    let ready = fixture.fit(minute(1), &[11, 12], false).await;
    assert_eq!(
        fixture.surface.act(&caller, pin(ready)).await,
        Ok(ActionOutcome::Applied)
    );
    assert_eq!(
        fixture.surface.act(&caller, pin(ready)).await,
        Ok(ActionOutcome::Unchanged)
    );
    let Ok(history) = fixture.world.catalog.versions().await else {
        panic!("history");
    };
    let Some(info) = history.get(ready) else {
        panic!("version");
    };
    assert_eq!(
        info.retention().pin(),
        Some(Pin {
            by: caller.operator(),
            at: accepted()
        })
    );
    assert_eq!(
        fixture.surface.act(&caller, unpin(ready)).await,
        Ok(ActionOutcome::Applied)
    );
    assert_eq!(
        fixture.surface.act(&caller, unpin(ready)).await,
        Ok(ActionOutcome::Unchanged)
    );

    // A version whose fit is running is fitting.
    let mut catalog = fixture.world.catalog.clone();
    use crosstalk_spec::interfaces::l6_analysis::lifecycle::TopicLifecycle;
    let Ok(fitting) = catalog.begin_fit(minute(2)).await else {
        panic!("begin fit");
    };
    assert_eq!(
        fixture.surface.act(&caller, pin(fitting)).await,
        Err(ActionError::Conflict(ConflictKind::TopicVersionFitting {
            version: fitting
        }))
    );
    assert!(catalog.fail_fit(fitting).await.is_ok());

    // Retention keeps three activated versions: the fourth activation
    // drops version 0.
    for (n, at) in [(20, 4), (30, 5), (40, 6)] {
        fixture.fit(minute(at), &[n], true).await;
    }
    let dropped = TopicModelVersion(0);
    assert_eq!(
        fixture.surface.act(&caller, pin(dropped)).await,
        Err(ActionError::Conflict(ConflictKind::TopicVersionDropped {
            version: dropped
        }))
    );
    assert_eq!(
        fixture.surface.act(&caller, unpin(dropped)).await,
        Ok(ActionOutcome::Unchanged)
    );
}

/// INV-512: each layer's refusal reaches the caller as the one
/// `ActionError::from` of its error.
#[tokio::test]
async fn layer_rejections_map_to_action_errors() {
    let fixture = fixture_at_accepted().await;
    let mut scene = fixture.scene().await;
    let admin = fixture.caller(Who::Admin).await;
    let act = |action| fixture.surface.act(&admin, action);

    // L5 registry.
    let unknown_channel = ChannelId::from_ulid(0xDEAD);
    assert_eq!(
        act(OperatorAction::SetPolicy {
            channel: unknown_channel,
            policy: PolicyKind::Sanctioned,
            note: None
        })
        .await,
        Err(ActionError::NotFound)
    );
    assert_eq!(
        act(OperatorAction::PromoteChannel {
            channel: unknown_channel,
            pattern: wiki(),
            policy: PolicyKind::Sanctioned,
            note: None
        })
        .await,
        Err(ActionError::NotFound)
    );
    let elsewhere = ResourcePattern::Host(Host("elsewhere.example".to_owned()));
    assert_eq!(
        act(OperatorAction::PromoteChannel {
            channel: scene.c1,
            pattern: elsewhere,
            policy: PolicyKind::Sanctioned,
            note: None
        })
        .await,
        Err(ActionError::InvalidInput(InputError::PatternMissesSeed))
    );
    assert!(
        act(OperatorAction::PromoteChannel {
            channel: scene.c1,
            pattern: wiki(),
            policy: PolicyKind::Sanctioned,
            note: None
        })
        .await
        .is_ok()
    );
    assert_eq!(
        act(OperatorAction::PromoteChannel {
            channel: scene.c1,
            pattern: wiki(),
            policy: PolicyKind::Sanctioned,
            note: None
        })
        .await,
        Err(ActionError::Conflict(ConflictKind::ChannelNotDiscovered {
            channel: scene.c1
        }))
    );

    // L3 resolver.
    let unknown_agent = AgentId::from_ulid(0xBEEF);
    assert_eq!(
        act(OperatorAction::RenameAgent {
            agent: unknown_agent,
            label: None
        })
        .await,
        Err(ActionError::NotFound)
    );
    assert_eq!(
        act(OperatorAction::Unmerge {
            merge: MergeId::from_ulid(1)
        })
        .await,
        Err(ActionError::NotFound)
    );
    let merge = match fixture
        .surface
        .request(
            &admin,
            ActionRequest::MergeAgents {
                from: scene.a3,
                into: scene.a1,
            },
        )
        .await
    {
        Ok(ActionOutcome::Merged(merge)) => merge,
        other => panic!("merge: {other:?}"),
    };
    assert_eq!(
        fixture
            .surface
            .request(
                &admin,
                ActionRequest::MergeAgents {
                    from: scene.a3,
                    into: scene.a1,
                },
            )
            .await,
        Err(ActionError::Conflict(ConflictKind::MergeIntoSelf {
            from: scene.a3,
            into: scene.a1,
            canonical: scene.a1
        }))
    );
    assert_eq!(
        fixture
            .surface
            .request(
                &admin,
                ActionRequest::MergeAgents {
                    from: scene.a3,
                    into: scene.a2,
                },
            )
            .await,
        Err(ActionError::Conflict(ConflictKind::AgentMerged {
            agent: scene.a3,
            into: scene.a1
        }))
    );
    assert_eq!(
        act(OperatorAction::RenameAgent {
            agent: scene.a3,
            label: None
        })
        .await,
        Err(ActionError::Conflict(ConflictKind::AgentMerged {
            agent: scene.a3,
            into: scene.a1
        }))
    );
    assert!(act(OperatorAction::Unmerge { merge }).await.is_ok());
    assert_eq!(
        act(OperatorAction::Unmerge { merge }).await,
        Err(ActionError::Conflict(ConflictKind::MergeAlreadyReverted {
            merge
        }))
    );

    // L5 verdicts.
    assert_eq!(
        act(OperatorAction::SetVerdict {
            transmission: TransmissionId::from_ulid(0xFEED),
            verdict: Some(Verdict::Genuine),
            note: None
        })
        .await,
        Err(ActionError::NotFound)
    );
    let detected = match TransmissionBuilder::new(&mut scene.ids).detected().build() {
        Ok(transmission) => transmission,
        Err(error) => panic!("{error:?}"),
    };
    fixture.transmission(&detected).await;
    assert_eq!(
        act(OperatorAction::SetVerdict {
            transmission: detected.id,
            verdict: Some(Verdict::Genuine),
            note: None
        })
        .await,
        Err(ActionError::Conflict(
            ConflictKind::TransmissionNotJudgeable {
                transmission: detected.id
            }
        ))
    );

    // L6 rules.
    assert_eq!(
        act(OperatorAction::SetRuleEnabled {
            id: AlertRuleId::from_ulid(1 << 100),
            enabled: false
        })
        .await,
        Err(ActionError::NotFound)
    );
    assert_eq!(
        act(OperatorAction::UpdateRule {
            id: BuiltinRule::NewChannel.id(),
            name: rule_name("mine"),
            rule: semantic("x"),
            sinks: Vec::new()
        })
        .await,
        Err(ActionError::Conflict(ConflictKind::RuleNotEditable {
            rule: BuiltinRule::NewChannel.id()
        }))
    );
    assert_eq!(
        act(OperatorAction::CreateRule {
            name: rule_name("sinkless"),
            rule: semantic("x"),
            sinks: vec![sink(9)]
        })
        .await,
        Err(ActionError::InvalidInput(InputError::UnknownSink {
            sink: sink(9)
        }))
    );
    assert_eq!(
        act(OperatorAction::CreateRule {
            name: rule_name("future"),
            rule: UserRule::watch_topic(
                TopicModelVersion(5),
                crosstalk_memory::model::build::topic_id(1)
            ),
            sinks: Vec::new()
        })
        .await,
        Err(ActionError::Conflict(
            ConflictKind::TopicVersionNotCurrent {
                requested: TopicModelVersion(5),
                current: TopicModelVersion(0)
            }
        ))
    );
    assert_eq!(
        act(OperatorAction::CreateRule {
            name: rule_name("ghost topics"),
            rule: UserRule::WatchedTopic {
                topics: crosstalk_spec::aggregates::alert::WatchedTopics {
                    version: TopicModelVersion(0),
                    topics: NonEmpty::new(crosstalk_memory::model::build::topic_id(404)),
                },
                remap_threshold: None,
            },
            sinks: Vec::new()
        })
        .await,
        Err(ActionError::InvalidInput(InputError::UnknownTopics))
    );
    let long = "word ".repeat(150);
    assert_eq!(
        act(OperatorAction::CreateRule {
            name: rule_name("long"),
            rule: semantic(&long),
            sinks: Vec::new()
        })
        .await,
        Err(ActionError::InvalidInput(InputError::QueryTooLong))
    );
}

/// INV-531: `SetVerdict`'s outcome for each case.
#[tokio::test]
async fn set_verdict_maps_outcomes() {
    let fixture = fixture_at_accepted().await;
    let mut scene = fixture.scene().await;
    let caller = fixture.caller(Who::Triager).await;
    let set = |transmission, verdict| OperatorAction::SetVerdict {
        transmission,
        verdict,
        note: None,
    };
    let id = scene.t1.transmission.id;
    assert_eq!(
        fixture
            .surface
            .act(&caller, set(id, Some(Verdict::Genuine)))
            .await,
        Ok(ActionOutcome::Applied)
    );
    assert_eq!(
        fixture
            .surface
            .act(&caller, set(id, Some(Verdict::Genuine)))
            .await,
        Ok(ActionOutcome::Unchanged)
    );
    assert_eq!(
        fixture.surface.act(&caller, set(id, None)).await,
        Ok(ActionOutcome::Applied)
    );
    assert_eq!(
        fixture
            .surface
            .act(
                &caller,
                set(TransmissionId::from_ulid(3), Some(Verdict::Genuine))
            )
            .await,
        Err(ActionError::NotFound)
    );
    for builder in [
        TransmissionBuilder::new(&mut scene.ids).detected(),
        TransmissionBuilder::new(&mut scene.ids).awaiting_content(),
    ] {
        let Ok(transmission) = builder.build() else {
            panic!("transmission");
        };
        fixture.transmission(&transmission).await;
        assert_eq!(
            fixture
                .surface
                .act(&caller, set(transmission.id, Some(Verdict::FalseDetection)))
                .await,
            Err(ActionError::Conflict(
                ConflictKind::TransmissionNotJudgeable {
                    transmission: transmission.id
                }
            ))
        );
    }
}

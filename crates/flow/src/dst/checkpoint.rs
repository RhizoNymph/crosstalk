//! The checkpoint and restore rules one at a time: the snapshot restores
//! the shards exactly, an incompatible one refuses to start, a busy
//! consumer takes none, held writes and tool calls survive a restart, and
//! a durable run acks a delivery only once a checkpoint covers it.

use std::num::NonZeroU32;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use crosstalk_spec::derived::provenance::matching::Carrier;
use crosstalk_spec::events::{BusEvent, Envelope};
use crosstalk_spec::ids::{AccessId, ChannelId, EventId};
use crosstalk_spec::interfaces::l2_transport::{BusError, Delivery, DeliveryId, Subscription};
use crosstalk_spec::observed::message::{ToolCallId, ToolName};
use crosstalk_spec::support::Timestamp;
use crosstalk_testkit::time::{T0, after};
use tokio::sync::mpsc;

use super::world::{Item, World, scenario};
use crate::consumer::tests::harness::{matched, settings, wiki_page, write};
use crate::consumer::{
    Checkpoint, CheckpointError, DurabilityError, Extracted, FlowDurability, FlowRestoreError,
    Incompatible, Observed, Recovered, SNAPSHOT_FORMAT, Shards, StoredCheckpoint, ToolCalled,
    WriteCall,
};
use crate::correlate::tests::fixtures::Scene;

fn secs(n: u64) -> Timestamp {
    after(T0, Duration::from_secs(n))
}

/// Every prefix of a scenario's run, checkpointed and restored, gives back
/// the very shards it was taken from.
#[tokio::test(flavor = "current_thread")]
async fn a_checkpoint_restores_the_shards_exactly() {
    for seed in 0..12 {
        let plan = scenario(seed, false);
        let world = World::new(&plan).await;
        let mut consumer = world.start().await;
        for (index, item) in plan.items.iter().enumerate() {
            match item {
                Item::Batch(inputs) => {
                    let _ = consumer.handle_batch(inputs.clone()).await;
                }
                Item::Deliver(event) => consumer.handle_event(event).await,
                Item::Tick(now) => consumer.tick(*now).await,
                Item::Checkpoint => {}
            }
            if index % 7 != 0 {
                continue;
            }
            let checkpoint = match consumer.shards().checkpoint() {
                Ok(checkpoint) => checkpoint,
                Err(error) => panic!("seed {seed}: checkpoint: {error}"),
            };
            let stored = StoredCheckpoint {
                format: checkpoint.format,
                count: 3,
                recorded_through: 0,
                shards: checkpoint.shards,
            };
            let config = settings(3);
            let restored = Shards::restore(
                config.timing,
                config.content_retention,
                config.shards,
                &stored,
            );
            assert_eq!(
                restored.as_ref(),
                Ok(consumer.shards()),
                "seed {seed}, item {index}"
            );
        }
    }
}

/// A durability port that hands back one recovered state.
#[derive(Debug, Clone)]
struct Canned(Recovered);

impl FlowDurability for Canned {
    fn survives_restart(&self) -> bool {
        true
    }

    async fn hold(&self, _: &Observed<WriteCall>, _: Timestamp) -> Result<(), DurabilityError> {
        Ok(())
    }

    async fn release(&self, _: AccessId) -> Result<(), DurabilityError> {
        Ok(())
    }

    async fn access_recorded(
        &self,
        _: &crosstalk_spec::derived::flow::access::Access,
        _: &crosstalk_spec::derived::flow::resource::Locator,
        _: Option<ChannelId>,
    ) -> Result<(), DurabilityError> {
        Ok(())
    }

    async fn tool_called(&self, _: &ToolCalled) -> Result<(), DurabilityError> {
        Ok(())
    }

    async fn save(&self, _: &Checkpoint, _: Timestamp) -> Result<(), DurabilityError> {
        Ok(())
    }

    async fn load(&self) -> Result<Recovered, DurabilityError> {
        Ok(self.0.clone())
    }
}

async fn restore_from(stored: StoredCheckpoint) -> Result<(), FlowRestoreError> {
    let stores = crate::consumer::tests::harness::Stores::new();
    let mut consumer = crate::consumer::FlowConsumer::with_durability(
        settings(3),
        crate::consumer::FlowDeps {
            registry: stores.registry.clone(),
            transmissions: stores.transmissions.clone(),
            agents: stores.agents.clone(),
            bus: crate::consumer::tests::harness::RecordingBus::default(),
            clock: Arc::new(crosstalk_memory::support::ManualClock::at(T0)),
        },
        Canned(Recovered {
            checkpoint: Some(stored),
            ..Recovered::default()
        }),
    );
    consumer.restore().await.map(|_| ())
}

/// A checkpoint in a format this binary does not read, of another shard
/// count, missing a shard or not decoding, is a start error that names
/// the reset (decision Q2), never an empty or partial correlator.
#[tokio::test(flavor = "current_thread")]
async fn an_incompatible_checkpoint_refuses_to_restore() {
    let config = settings(3);
    let empty = Shards::new(config.timing, config.content_retention, config.shards);
    let Ok(checkpoint) = empty.checkpoint() else {
        panic!("checkpoint of empty shards");
    };
    let good = StoredCheckpoint {
        format: SNAPSHOT_FORMAT,
        count: 3,
        recorded_through: 0,
        shards: checkpoint.shards.clone(),
    };
    assert_eq!(restore_from(good.clone()).await, Ok(()));
    let cases = [
        (
            StoredCheckpoint {
                format: SNAPSHOT_FORMAT + 1,
                ..good.clone()
            },
            Incompatible::Format {
                found: SNAPSHOT_FORMAT + 1,
                reads: SNAPSHOT_FORMAT,
            },
        ),
        (
            StoredCheckpoint {
                count: 2,
                shards: checkpoint.shards[..2].to_vec(),
                ..good.clone()
            },
            Incompatible::ShardCount {
                found: 2,
                configured: 3,
            },
        ),
        (
            StoredCheckpoint {
                shards: checkpoint.shards[..2].to_vec(),
                ..good.clone()
            },
            Incompatible::MissingShard { shard: 2 },
        ),
    ];
    for (stored, why) in cases {
        let refused = restore_from(stored).await;
        assert_eq!(refused, Err(FlowRestoreError::IncompatibleSnapshot(why)));
        if let Err(error) = refused {
            assert!(error.to_string().contains("--reset-correlator"), "{error}");
        }
    }
    let mut garbled = good;
    garbled.shards[1].state = b"{not json".to_vec();
    assert!(matches!(
        restore_from(garbled).await,
        Err(FlowRestoreError::IncompatibleSnapshot(
            Incompatible::Undecodable { shard: 1, .. }
        ))
    ));
}

/// While a step waits in the queue no checkpoint is taken: it would cover
/// an input whose effects are not stored.
#[tokio::test(flavor = "current_thread")]
async fn a_checkpoint_waits_for_an_empty_queue() {
    let plan = scenario(3, false);
    let world = World::new(&plan).await;
    let mut consumer = world.start().await;
    let mut scene = Scene::new(77);
    let (a, b) = (scene.agent(), scene.agent());
    let exchange = scene.exchange();
    let span = scene.span();
    let content = scene.found(a, b, exchange, span, Carrier::UserTurn);
    let mut captured = crosstalk_testkit::build::exchange::ExchangeBuilder::new(&mut scene.ids)
        .started_at(secs(10))
        .build();
    captured.meta.id = exchange;
    consumer
        .handle_event(&BusEvent::Ingest(
            crosstalk_spec::events::ingest::IngestEvent::ExchangeCaptured(Box::new(captured)),
        ))
        .await;
    consumer.handle_event(&matched(&content)).await;
    world.set_outage(true);
    // The window closes: the transmission is stored, its event not
    // published.
    consumer.tick(secs(600)).await;
    assert!(consumer.backlog() > 0);
    assert!(matches!(
        consumer.checkpoint().await,
        Err(CheckpointError::Busy { .. })
    ));
    world.set_outage(false);
    consumer.tick(secs(601)).await;
    assert_eq!(consumer.backlog(), 0);
    assert!(consumer.checkpoint().await.is_ok());
}

/// A held write and a tool call taken after the last checkpoint are still
/// there after a restart: the write settles as `Unknown` and its stored
/// hold goes once its access is recorded; the tool call names the
/// `Direct(ToolResult)` transmission of its result.
#[tokio::test(flavor = "current_thread")]
async fn held_writes_and_tool_calls_survive_a_restart() {
    let plan = scenario(5, false);
    let world = World::new(&plan).await;
    let mut consumer = world.start().await;
    assert!(consumer.checkpoint().await.is_ok());
    let mut scene = Scene::new(78);
    let (a, b) = (scene.agent(), scene.agent());
    let span = scene.span();
    let edit = write(&mut scene, a, &wiki_page("Held"), secs(5), vec![span]);
    let id = edit.id;
    let call = ToolCallId("toolu_survives".to_owned());
    assert_eq!(
        consumer
            .handle_batch(vec![
                Extracted::Write {
                    write: edit,
                    outcome: None,
                },
                Extracted::ToolCall {
                    agent: b,
                    call: call.clone(),
                    name: ToolName("WebFetch".to_owned()),
                    at: secs(5),
                },
            ])
            .await,
        Ok(())
    );
    assert_eq!(world.durability.inner.held(), 1);
    // The process dies; a new one restores.
    let mut consumer = world.start().await;
    assert!(consumer.held_writes().contains(id));
    // The tool call's name reached the restored shards: a tool-result
    // match without a read opens `Direct(ToolResult(WebFetch))`.
    let exchange = scene.exchange();
    let mut captured = crosstalk_testkit::build::exchange::ExchangeBuilder::new(&mut scene.ids)
        .started_at(secs(5))
        .build();
    captured.meta.id = exchange;
    consumer
        .handle_event(&BusEvent::Ingest(
            crosstalk_spec::events::ingest::IngestEvent::ExchangeCaptured(Box::new(captured)),
        ))
        .await;
    let span = scene.span();
    let content = scene.found(a, b, exchange, span, Carrier::ToolResult(call));
    consumer.handle_event(&matched(&content)).await;
    consumer.tick(secs(3_600)).await;
    assert!(!consumer.held_writes().contains(id));
    assert_eq!(world.durability.inner.held(), 0);
    let named = world.log.envelopes().values().any(|envelope| {
        matches!(
            &envelope.event,
            BusEvent::Detect(crosstalk_spec::events::detect::DetectEvent::TransmissionConfirmed {
                route: crosstalk_spec::derived::flow::transmission::Route::Direct(
                    crosstalk_spec::derived::flow::transmission::DirectCarrier::ToolResult(name)
                ),
                ..
            }) if name.0 == "WebFetch"
        )
    });
    assert!(named, "{:?}", world.log.envelopes());
}

/// A subscription fed by the test, recording acks.
struct Fed {
    deliveries: mpsc::UnboundedReceiver<Delivery>,
    acked: Arc<Mutex<Vec<DeliveryId>>>,
}

impl Subscription for Fed {
    async fn next(&mut self) -> Option<Result<Delivery, BusError>> {
        self.deliveries.recv().await.map(Ok)
    }

    async fn ack(&mut self, id: DeliveryId) -> Result<(), BusError> {
        self.acked
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(id);
        Ok(())
    }

    async fn nack(&mut self, _: DeliveryId, _: Duration, _: String) -> Result<(), BusError> {
        Ok(())
    }
}

/// A durable consumer's loop acks a delivery only once a checkpoint that
/// covers it is stored (every `checkpoint_every`), never when its steps
/// ran.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn a_durable_run_acks_only_checkpointed_deliveries() {
    let plan = scenario(7, false);
    let world = World::new(&plan).await;
    let mut consumer = world.start().await;
    let (feed, deliveries) = mpsc::unbounded_channel();
    let acked = Arc::new(Mutex::new(Vec::new()));
    let subscription = Fed {
        deliveries,
        acked: Arc::clone(&acked),
    };
    let (_inputs, extracted) = mpsc::channel::<Extracted>(4);
    let acks = || acked.lock().unwrap_or_else(PoisonError::into_inner).clone();
    let mut scene = Scene::new(79);
    let (a, b, exchange, span) = (scene.agent(), scene.agent(), scene.exchange(), scene.span());
    let content = scene.found(a, b, exchange, span, Carrier::UserTurn);
    let driver = async {
        // Let the loop take its first (immediate) checkpoint.
        tokio::time::sleep(Duration::from_millis(10)).await;
        let sent = feed.send(Delivery {
            id: DeliveryId(1),
            attempt: NonZeroU32::MIN,
            envelope: Envelope {
                id: EventId::from_ulid(1),
                at: T0,
                event: matched(&content),
            },
        });
        assert!(sent.is_ok());
        tokio::time::sleep(Duration::from_secs(5)).await;
        let early = acks();
        tokio::time::sleep(Duration::from_secs(6)).await;
        let late = acks();
        drop(feed);
        (early, late)
    };
    let ((), (early, late)) = tokio::join!(consumer.run(subscription, extracted), driver);
    assert_eq!(early, Vec::<DeliveryId>::new(), "acked before a checkpoint");
    assert_eq!(late, vec![DeliveryId(1)]);
}

/// `checkpoint_ms` and `checkpoint_unacked` default to ten seconds and 512
/// and refuse zero.
#[test]
fn checkpoint_settings_are_checked() {
    use crate::consumer::{FlowConfig, InvalidFlowConfig, Settings};

    let parsed: Result<FlowConfig, _> =
        serde_json::from_str(r#"{"checkpoint_ms": 2500, "checkpoint_unacked": 8}"#);
    let Ok(parsed) = parsed else {
        panic!("{parsed:?}");
    };
    let Ok(configured) = Settings::try_from(parsed) else {
        panic!("settings");
    };
    assert_eq!(configured.checkpoint_every, Duration::from_millis(2_500));
    assert_eq!(configured.max_unacked.get(), 8);
    let Ok(defaults) = Settings::try_from(FlowConfig::default()) else {
        panic!("defaults");
    };
    assert_eq!(defaults.checkpoint_every, Duration::from_secs(10));
    assert_eq!(defaults.max_unacked.get(), 512);
    for (config, error) in [
        (
            FlowConfig {
                checkpoint_ms: 0,
                ..FlowConfig::default()
            },
            InvalidFlowConfig::ZeroCheckpoint,
        ),
        (
            FlowConfig {
                checkpoint_unacked: 0,
                ..FlowConfig::default()
            },
            InvalidFlowConfig::ZeroUnacked,
        ),
    ] {
        assert_eq!(Settings::try_from(config), Err(error));
    }
}

//! The consumer's rules over the reference stores: held writes, rejected
//! writes, declared channels, supersession, delegation, retries and its
//! configuration.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use crosstalk_memory::flow::MemoryVerdicts;
use crosstalk_memory::support::ManualClock;
use crosstalk_spec::derived::flow::channel::detection::TrafficDetection;
use crosstalk_spec::derived::flow::channel::policy::{Policy, PolicyAuthor, PolicyKind};
use crosstalk_spec::derived::flow::channel::promotion::Promotion;
use crosstalk_spec::derived::flow::resource::{Host, ResourcePattern};
use crosstalk_spec::derived::flow::transmission::{
    DelegationDirection, NonChannelRoute, Route, Transmission, TransmissionState,
};
use crosstalk_spec::derived::provenance::matching::Carrier;
use crosstalk_spec::events::changed::Changed;
use crosstalk_spec::events::detect::DetectEvent;
use crosstalk_spec::events::{BusEvent, Subject};
use crosstalk_spec::ids::{OperatorId, TransmissionId};
use crosstalk_spec::interfaces::l5_flow::ChannelRegistry;
use crosstalk_spec::interfaces::l5_flow::channels::ChannelReads;
use crosstalk_spec::interfaces::l5_flow::transmissions::{
    TransmissionStore, TransmissionStoreError,
};
use crosstalk_spec::support::Timestamp;
use crosstalk_testkit::time::T0;

use super::harness::{
    RecordingBus, Stores, confirmations, consumer, found_in, matched, read, wiki_page, write,
};
use crate::consumer::{Extracted, FlowConfig, FlowConsumer, FlowDeps, InvalidFlowConfig, Settings};
use crate::correlate::pairing::{self, WriteOutcome};
use crate::correlate::tests::fixtures::{Scene, timing};

fn after(at: Timestamp, seconds: u64) -> Timestamp {
    Timestamp::from_micros(at.as_micros() + seconds * 1_000_000)
}

fn recorded(bus: &RecordingBus) -> usize {
    bus.subjects()
        .iter()
        .filter(|subject| **subject == Subject::AccessRecorded)
        .count()
}

/// A write whose result never arrives is held, then recorded `Unknown` at
/// the first tick at or after its settle time, and pairs; a result arriving
/// after that changes nothing (`flow.correlator.write-held-until-outcome`,
/// `flow.correlator.unknown-write-pairs`).
#[tokio::test]
async fn write_without_result_settles_unknown() {
    let mut scene = Scene::new(100);
    let stores = Stores::new();
    let (a, b) = (scene.agent(), scene.agent());
    let page = wiki_page("Unanswered");
    let span = scene.span();
    let bus = RecordingBus::default();
    let mut flow = consumer(&stores, bus.clone(), Arc::new(ManualClock::at(T0)), 1);
    let edit = write(&mut scene, a, &page, T0, vec![span]);
    let edit_id = edit.id;
    flow.handle_extracted(Extracted::Write {
        write: edit,
        outcome: None,
    })
    .await;
    let fetch = read(&mut scene, b, &page, after(T0, 30));
    flow.handle_extracted(Extracted::Read(fetch.clone())).await;
    flow.handle_event(&matched(&found_in(&mut scene, &fetch, a, span)))
        .await;
    assert_eq!(flow.held_writes().len(), 1);
    assert_eq!(
        recorded(&bus),
        1,
        "only the read is recorded while the write is held"
    );

    let settles = pairing::write_settles_at(timing(), T0);
    flow.tick(Timestamp::from_micros(settles.as_micros() - 1))
        .await;
    assert_eq!(flow.held_writes().len(), 1);
    flow.tick(settles).await;
    assert!(flow.held_writes().is_empty());
    assert_eq!(recorded(&bus), 2);
    // Released after the read's window closed: still within its
    // suspicion, so the held match confirms it at once.
    assert_eq!(
        confirmations(&bus.events()).len(),
        1,
        "{:?}",
        bus.subjects()
    );

    flow.handle_extracted(Extracted::WriteResult {
        access: edit_id,
        outcome: WriteOutcome::Rejected,
    })
    .await;
    assert_eq!(recorded(&bus), 2, "a late result records nothing");
}

/// A rejected write is recorded and announced, and never paired: no
/// transmission, no channel (`flow.access.rejected-write-recorded`).
#[tokio::test]
async fn rejected_write_is_recorded_not_paired() {
    let mut scene = Scene::new(101);
    let mut stores = Stores::new();
    let (a, b) = (scene.agent(), scene.agent());
    let page = wiki_page("Refused");
    let span = scene.span();
    let bus = RecordingBus::default();
    let mut flow = consumer(&stores, bus.clone(), Arc::new(ManualClock::at(T0)), 1);
    let edit = write(&mut scene, a, &page, T0, vec![span]);
    let edit_id = edit.id;
    flow.handle_extracted(Extracted::Write {
        write: edit,
        outcome: None,
    })
    .await;
    flow.handle_extracted(Extracted::WriteResult {
        access: edit_id,
        outcome: WriteOutcome::Rejected,
    })
    .await;
    let fetch = read(&mut scene, b, &page, after(T0, 30));
    flow.handle_extracted(Extracted::Read(fetch.clone())).await;
    flow.handle_event(&matched(&found_in(&mut scene, &fetch, a, span)))
        .await;
    flow.tick(after(T0, 10_000)).await;
    assert_eq!(recorded(&bus), 2);
    assert_eq!(
        bus.subjects(),
        vec![Subject::AccessRecorded, Subject::AccessRecorded]
    );
    assert!(stores.channels().await.is_empty());
    assert!(stores.registry_events().is_empty());
}

/// On a declared channel a co-access opens on the channel, no discovery
/// happens, and the declared channel goes into use.
#[tokio::test]
async fn a_declared_channel_carries_its_transmissions() {
    let mut scene = Scene::new(102);
    let mut stores = Stores::new();
    let declared = stores
        .registry
        .declare(
            ResourcePattern::Host(Host("wiki.example".to_owned())),
            Policy::Unreviewed(None),
            PolicyAuthor::Config,
            T0,
        )
        .await;
    let Ok(declared) = declared else {
        panic!("declare: {declared:?}");
    };
    stores.registry_events();
    let (a, b) = (scene.agent(), scene.agent());
    let page = wiki_page("Sanctioned");
    let span = scene.span();
    let bus = RecordingBus::default();
    let mut flow = consumer(&stores, bus.clone(), Arc::new(ManualClock::at(T0)), 3);
    flow.handle_extracted(Extracted::Write {
        write: write(&mut scene, a, &page, after(T0, 1), vec![span]),
        outcome: Some(WriteOutcome::Delivered),
    })
    .await;
    let fetch = read(&mut scene, b, &page, after(T0, 30));
    flow.handle_extracted(Extracted::Read(fetch.clone())).await;
    flow.handle_event(&matched(&found_in(&mut scene, &fetch, a, span)))
        .await;
    flow.tick(after(T0, 100)).await;
    assert!(super::harness::discoveries(&stores.registry_events()).is_empty());
    let routed = stores.transmissions_of(declared).await;
    assert_eq!(routed.len(), 1);
    assert_eq!(routed[0].route, Route::Channel(declared));
    assert!(matches!(routed[0].state, TransmissionState::Confirmed(_)));
    let channel = stores.registry.channel(declared).await;
    let Ok(Some(channel)) = channel else {
        panic!("{channel:?}");
    };
    assert!(
        matches!(channel.channel().origin.traffic(), Some(TrafficDetection::Active { last_transmission, .. }) if *last_transmission == routed[0].id)
    );
}

/// A transmission opened on a discovered channel that a promotion
/// superseded before its content arrived is confirmed after the
/// promotion: the superseding channel's detection records it and is
/// announced, the superseded channel's stays as it was
/// (`flow.channel.confirmation-advances-canonical-detection`).
#[tokio::test]
async fn late_confirmation_on_a_superseded_channel_advances_its_superseder() {
    let mut scene = Scene::new(103);
    let mut stores = Stores::new();
    let (a, b, d) = (scene.agent(), scene.agent(), scene.agent());
    let (first_page, second_page) = (wiki_page("One"), wiki_page("Two"));
    let span = scene.span();
    let bus = RecordingBus::default();
    let mut flow = consumer(&stores, bus.clone(), Arc::new(ManualClock::at(T0)), 4);

    flow.handle_extracted(Extracted::Write {
        write: write(&mut scene, a, &first_page, T0, vec![span]),
        outcome: Some(WriteOutcome::Delivered),
    })
    .await;
    let fetch = read(&mut scene, b, &first_page, after(T0, 30));
    flow.handle_extracted(Extracted::Read(fetch.clone())).await;
    flow.handle_extracted(Extracted::Write {
        write: write(&mut scene, a, &second_page, after(T0, 2), vec![]),
        outcome: Some(WriteOutcome::Delivered),
    })
    .await;
    flow.handle_extracted(Extracted::Read(read(
        &mut scene,
        d,
        &second_page,
        after(T0, 35),
    )))
    .await;
    let channels = stores.channels().await;
    assert_eq!(channels.len(), 2);
    let superseded = channels
        .iter()
        .map(|listed| listed.channel().clone())
        .find(|channel| {
            matches!(&channel.origin, crosstalk_spec::derived::flow::channel::ChannelOrigin::Discovered { seed, .. }
                if seed.opened_at == fetch.at)
        });
    let Some(superseded) = superseded else {
        panic!("{channels:?}");
    };
    let Some(promoted) = channels
        .iter()
        .map(|listed| listed.channel().id)
        .find(|id| *id != superseded.id)
    else {
        panic!("{channels:?}");
    };
    let late: TransmissionId = match superseded.origin.traffic() {
        Some(TrafficDetection::Active {
            last_transmission, ..
        }) => *last_transmission,
        other => panic!("{other:?}"),
    };

    let promotion = Promotion::new(
        ResourcePattern::Host(Host("wiki.example".to_owned())),
        PolicyKind::Sanctioned,
        OperatorId::from_ulid(9),
        after(T0, 40),
        None,
    );
    let promoted_result = stores.registry.promote(promoted, promotion).await;
    let Ok(result) = promoted_result else {
        panic!("{promoted_result:?}");
    };
    assert_eq!(result.superseded, vec![superseded.id]);
    let promotion_events = stores.registry_events();
    let announced = promotion_events.iter().find_map(|event| match event {
        BusEvent::Detect(event @ DetectEvent::ChannelPromoted { .. }) => {
            Some(BusEvent::Detect(event.clone()))
        }
        _ => None,
    });
    let Some(announced) = announced else {
        panic!("{promotion_events:?}");
    };
    flow.handle_event(&announced).await;
    let frozen = stores.registry.channel(superseded.id).await;

    flow.handle_event(&matched(&found_in(&mut scene, &fetch, a, span)))
        .await;
    flow.tick(after(T0, 100)).await;

    let stored = stores.transmissions.transmission(late).await;
    let Ok(Some(stored)) = stored else {
        panic!("{stored:?}");
    };
    assert_eq!(
        stored.route,
        Route::Channel(superseded.id),
        "the stored route is kept"
    );
    assert!(matches!(stored.state, TransmissionState::Confirmed(_)));
    let canonical = stores.registry.channel(promoted).await;
    let Ok(Some(canonical)) = canonical else {
        panic!("{canonical:?}");
    };
    assert!(
        matches!(canonical.channel().origin.traffic(), Some(TrafficDetection::Active { last_transmission, .. }) if *last_transmission == late),
        "{canonical:?}"
    );
    assert_eq!(stores.registry.channel(superseded.id).await, frozen);
    assert!(
        stores
            .registry_events()
            .contains(&BusEvent::Changed(Changed::Channel(promoted)))
    );
    assert_eq!(confirmations(&bus.events()), vec![late]);
}

/// A match between parent and child, read through `AgentReads`, opens a
/// delegation transmission.
#[tokio::test]
async fn delegation_is_read_from_the_agent_store() {
    let mut scene = Scene::new(104);
    let mut stores = Stores::new();
    let parent = stores.agent(&mut scene, None).await;
    let child = stores.agent(&mut scene, Some(parent)).await;
    let bus = RecordingBus::default();
    let mut flow = consumer(&stores, bus.clone(), Arc::new(ManualClock::at(T0)), 2);
    let exchange = scene.exchange();
    let span = scene.span();
    let task = scene.found(parent, child, exchange, span, Carrier::UserTurn);
    flow.handle_event(&matched(&task)).await;
    let mut exchange_seen =
        crosstalk_testkit::build::exchange::ExchangeBuilder::new(&mut scene.ids)
            .with_id(exchange)
            .started_at(after(T0, 5))
            .build();
    exchange_seen.meta.id = exchange;
    flow.handle_event(&BusEvent::Ingest(
        crosstalk_spec::events::ingest::IngestEvent::ExchangeCaptured(Box::new(exchange_seen)),
    ))
    .await;
    flow.tick(after(T0, 100)).await;
    let routes: Vec<_> = bus
        .events()
        .into_iter()
        .filter_map(|event| match event {
            BusEvent::Detect(DetectEvent::TransmissionConfirmed { route, at, .. }) => {
                Some((route, at))
            }
            _ => None,
        })
        .collect();
    assert_eq!(
        routes,
        vec![(
            Route::from(NonChannelRoute::Delegation(
                DelegationDirection::ParentToChild
            )),
            after(T0, 5)
        )]
    );
}

/// A transmission store that fails its first `failures` saves.
#[derive(Clone)]
struct Flaky {
    inner: MemoryVerdicts,
    failures: Arc<AtomicU32>,
}

impl TransmissionStore for Flaky {
    async fn save(&mut self, transmission: Transmission) -> Result<(), TransmissionStoreError> {
        if self
            .failures
            .try_update(Ordering::SeqCst, Ordering::SeqCst, |left| {
                left.checked_sub(1)
            })
            .is_ok()
        {
            return Err(TransmissionStoreError::Store {
                reason: "injected".to_owned(),
            });
        }
        self.inner.save(transmission).await
    }

    async fn transmission(
        &self,
        id: TransmissionId,
    ) -> Result<Option<Transmission>, TransmissionStoreError> {
        self.inner.transmission(id).await
    }

    async fn list(
        &self,
        query: &crosstalk_spec::interfaces::l5_flow::transmissions::TransmissionQuery,
        page: &crosstalk_spec::paging::PageRequest<crosstalk_spec::paging::TransmissionList>,
    ) -> Result<
        crosstalk_spec::paging::Page<Transmission, crosstalk_spec::paging::TransmissionList>,
        TransmissionStoreError,
    > {
        self.inner.list(query, page).await
    }
}

/// A failed save waits at the head of the queue and is retried with the
/// next input; nothing after it runs first, and nothing is lost.
#[tokio::test]
async fn a_failed_step_is_retried_in_order() {
    let mut scene = Scene::new(105);
    let stores = Stores::new();
    let (a, b) = (scene.agent(), scene.agent());
    let page = wiki_page("Flaky");
    let span = scene.span();
    let bus = RecordingBus::default();
    let failures = Arc::new(AtomicU32::new(2));
    let mut flow = FlowConsumer::new(
        super::harness::settings(1),
        FlowDeps {
            registry: stores.registry.clone(),
            transmissions: Flaky {
                inner: stores.transmissions.clone(),
                failures: Arc::clone(&failures),
            },
            agents: stores.agents.clone(),
            bus: bus.clone(),
            clock: Arc::new(ManualClock::at(T0)),
            entropy: crosstalk_spec::ids::SeededRandom::new(1),
        },
    );
    flow.handle_extracted(Extracted::Write {
        write: write(&mut scene, a, &page, T0, vec![span]),
        outcome: Some(WriteOutcome::Delivered),
    })
    .await;
    let fetch = read(&mut scene, b, &page, after(T0, 30));
    flow.handle_extracted(Extracted::Read(fetch.clone())).await;
    assert!(flow.backlog() > 0, "the open's save failed");
    flow.handle_event(&matched(&found_in(&mut scene, &fetch, a, span)))
        .await;
    assert!(flow.backlog() > 0, "the second attempt failed too");
    flow.tick(after(T0, 100)).await;
    assert_eq!(flow.backlog(), 0);
    assert_eq!(
        bus.subjects(),
        vec![
            Subject::AccessRecorded,
            Subject::AccessRecorded,
            Subject::ChannelCrossAccessed,
            Subject::TransmissionConfirmed,
        ]
    );
}

/// The `flow` config section: defaults, checks and unknown fields.
#[test]
fn flow_config_is_checked() {
    let parsed: Result<FlowConfig, _> = serde_json::from_str("{}");
    assert_eq!(parsed.ok(), Some(FlowConfig::default()));
    let settings = Settings::try_from(FlowConfig::default());
    assert!(settings.is_ok(), "{settings:?}");
    let unknown: Result<FlowConfig, _> = serde_json::from_str(r#"{"settle_ms": 5}"#);
    assert!(unknown.is_err());
    for (config, error) in [
        (
            FlowConfig {
                evidence_window_ms: 0,
                ..FlowConfig::default()
            },
            InvalidFlowConfig::Timing(
                crosstalk_spec::derived::flow::timing::InvalidTiming::ZeroEvidenceWindow,
            ),
        ),
        (
            FlowConfig {
                shards: 0,
                ..FlowConfig::default()
            },
            InvalidFlowConfig::ZeroShards,
        ),
        (
            FlowConfig {
                tick_ms: 0,
                ..FlowConfig::default()
            },
            InvalidFlowConfig::ZeroTick,
        ),
    ] {
        assert_eq!(Settings::try_from(config), Err(error));
    }
}

/// Updates the stored state does not admit (a stale suspicion, a
/// discard, a repeated opening) change nothing and announce nothing: a
/// stored transmission never moves back.
#[tokio::test]
async fn stale_updates_never_move_a_stored_transmission_back() {
    let mut scene = Scene::new(106);
    let stores = Stores::new();
    let (a, b) = (scene.agent(), scene.agent());
    let page = wiki_page("Settled");
    let span = scene.span();
    let bus = RecordingBus::default();
    let mut flow = consumer(&stores, bus.clone(), Arc::new(ManualClock::at(T0)), 1);
    flow.handle_extracted(Extracted::Write {
        write: write(&mut scene, a, &page, T0, vec![span]),
        outcome: Some(WriteOutcome::Delivered),
    })
    .await;
    let fetch = read(&mut scene, b, &page, after(T0, 30));
    flow.handle_extracted(Extracted::Read(fetch.clone())).await;
    flow.handle_event(&matched(&found_in(&mut scene, &fetch, a, span)))
        .await;
    flow.tick(after(T0, 100)).await;
    let Some(channel) = stores.only_channel().await else {
        panic!("no channel");
    };
    let routed = stores.transmissions_of(channel.id).await;
    let [confirmed] = routed.as_slice() else {
        panic!("{routed:?}");
    };
    let co_access = confirmed.state.co_accesses()[0];
    let published = bus.events().len();
    for update in [
        crosstalk_spec::interfaces::l5_flow::TransmissionUpdate::Suspect {
            transmission: confirmed.id,
            co_access: crosstalk_spec::support::NonEmpty::new(co_access),
        },
        crosstalk_spec::interfaces::l5_flow::TransmissionUpdate::Discard {
            transmission: confirmed.id,
        },
        crosstalk_spec::interfaces::l5_flow::TransmissionUpdate::OpenChannel {
            transmission: confirmed.id,
            to: b,
            on: crosstalk_spec::interfaces::l5_flow::OpensOn::Channel(channel.id),
            co_access,
        },
    ] {
        let steps = flow
            .decide(crate::correlate::Decided {
                update,
                opened_at: confirmed.opened_at,
            })
            .await;
        assert!(steps.as_ref().is_ok_and(Vec::is_empty), "{steps:?}");
    }
    let stored = stores.transmissions.transmission(confirmed.id).await;
    assert_eq!(stored, Ok(Some(confirmed.clone())));
    assert_eq!(bus.events().len(), published);
}

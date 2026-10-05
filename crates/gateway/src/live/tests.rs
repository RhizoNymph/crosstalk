//! A live process's own machinery, without traffic: every slot runs, the
//! outbox reaches the bus, the surface relay, the L6 classifier, the
//! evidence feeder, settling and the blob store choice. Traffic end to end
//! is crosstalk-e2e's smoke.

use std::num::{NonZeroU32, NonZeroU64};
use std::sync::Arc;
use std::time::Duration;

use crosstalk_api::InProcessOptions;
use crosstalk_flow::consumer::FlowConfig;
use crosstalk_memory::analysis::catalog::RetentionPolicy;
use crosstalk_memory::model::build::test_model;
use crosstalk_memory::support::ManualClock;
use crosstalk_provenance::config::ProvenanceConfig;
use crosstalk_reconstruct::thread::ThreadConfig;
use crosstalk_spec::aggregates::projection::FrameRetention;
use crosstalk_spec::aggregates::series::BucketWidth;
use crosstalk_spec::aggregates::topic::TopicModelVersion;
use crosstalk_spec::derived::flow::timing::CorrelationTiming;
use crosstalk_spec::derived::flow::transmission::{Classification, Route, TransmissionState};
use crosstalk_spec::events::changed::Changed;
use crosstalk_spec::events::detect::DetectEvent;
use crosstalk_spec::events::insight::{ClassificationCause, InsightEvent};
use crosstalk_spec::events::{BusEvent, Envelope, Subject};
use crosstalk_spec::ids::OperatorId;
use crosstalk_spec::interfaces::l2_transport::{BlobStore, ConsumerGroup, EventBus, Subscription};
use crosstalk_spec::interfaces::l5_flow::Discovery;
use crosstalk_spec::interfaces::l5_flow::channels::ChannelTraffic;
use crosstalk_spec::interfaces::l5_flow::transmissions::TransmissionStore;
use crosstalk_spec::interfaces::l8_surface::export::{
    ExportFormat, ExportFormats, ExportLimits, GatewayVersion,
};
use crosstalk_spec::interfaces::l8_surface::live::{
    LiveConfig as FeedConfig, LiveFeed, LiveItem, LiveStream, Resume, UiEvent,
};
use crosstalk_spec::interfaces::l8_surface::operators::{
    AccessConfig, OperatorName, RequestIdentity, TrustedOperator,
};
use crosstalk_spec::support::{Similarity, Timestamp};
use crosstalk_surface::{EvidenceRecords, SurfaceConfig};
use crosstalk_testkit::build::{AccessBuilder, ResourceBuilder, TransmissionBuilder};
use crosstalk_testkit::ids::Ids;
use crosstalk_testkit::time::T0;
use crosstalk_transport::BusConfig;
use crosstalk_transport::MpscSubscription;
use tokio::time::Instant;

use super::{
    BlobConfig, Live, LiveBlobs, LiveClock, LiveConfig, LiveDrained, Slot, SlotTaken, Stages,
    Ticking,
};
use crate::live::classify::Classifier;
use crate::pipeline::Settings;

const MINUTE: u64 = 60_000_000;
const PATIENCE: Duration = Duration::from_secs(5);

fn surface_options(clock: ManualClock) -> InProcessOptions {
    let (
        Ok(formats),
        Ok(live),
        Ok(gateway),
        Ok(threshold),
        Ok(retention),
        Ok(floor),
        Ok(name),
        Ok(timing),
    ) = (
        ExportFormats::new(vec![ExportFormat::Jsonl]),
        FeedConfig::new(
            NonZeroU32::MIN.saturating_add(31),
            Duration::from_secs(15),
            Duration::from_secs(600),
        ),
        GatewayVersion::new("0.1.0-live"),
        Similarity::new(0.8),
        RetentionPolicy::new(3),
        Similarity::new(0.5),
        OperatorName::new("Live"),
        CorrelationTiming::new(
            Duration::from_secs(60),
            Duration::from_secs(60),
            Duration::from_secs(60),
        ),
    )
    else {
        panic!("options");
    };
    InProcessOptions {
        clock: Arc::new(clock),
        seed: 7,
        surface: SurfaceConfig {
            export_formats: formats,
            export_limits: ExportLimits::default(),
            gateway,
            default_remap_threshold: threshold,
            frame_retention: FrameRetention::default(),
            live,
        },
        access: AccessConfig::Trusted(TrustedOperator {
            id: OperatorId::from_ulid(0x11FE),
            name,
        }),
        bucket_width: BucketWidth::from_micros(NonZeroU64::new(MINUTE).unwrap_or(NonZeroU64::MIN)),
        timing,
        retention,
        lineage_floor: floor,
        embedding_model: test_model("live"),
        sinks: Vec::new(),
        projection_lease: Duration::from_secs(60),
        projection_fitting: crosstalk_api::ProjectionFitting::External,
    }
}

fn config(blobs: BlobConfig) -> LiveConfig {
    let clock = ManualClock::at(T0);
    LiveConfig {
        surface: surface_options(clock.clone()),
        clock: LiveClock::Manual(clock),
        blobs,
        bus: BusConfig::default(),
        pipeline: Settings::default(),
        flow: FlowConfig::default(),
        provenance: ProvenanceConfig::default(),
        threading: ThreadConfig::default(),
        ticking: Ticking::OnSettle,
        seed: 7,
        capture: None,
        exchange_log: None,
    }
}

async fn start() -> Live {
    match Live::start(config(BlobConfig::Memory)).await {
        Ok(live) => live,
        Err(error) => panic!("start: {error}"),
    }
}

async fn observe(live: &Live, subjects: &[Subject]) -> MpscSubscription {
    match live
        .stores()
        .bus
        .subscribe(
            subjects,
            ConsumerGroup("live-test-observer".to_owned()),
            Settings::default().consumer_retry,
        )
        .await
    {
        Ok(subscription) => subscription,
        Err(error) => panic!("subscribe: {error:?}"),
    }
}

/// The first envelope on `subscription` that `wanted` accepts.
async fn first<T>(
    subscription: &mut MpscSubscription,
    mut wanted: impl FnMut(&Envelope) -> Option<T>,
) -> T {
    let deadline = Instant::now() + PATIENCE;
    loop {
        let delivery = match tokio::time::timeout_at(deadline, subscription.next()).await {
            Ok(Some(Ok(delivery))) => delivery,
            other => panic!("no matching envelope: {other:?}"),
        };
        let found = wanted(&delivery.envelope);
        if let Err(error) = subscription.ack(delivery.id).await {
            panic!("ack: {error:?}");
        }
        if let Some(found) = found {
            return found;
        }
    }
}

#[tokio::test]
async fn every_slot_runs_and_shutdown_drains() {
    let live = start().await;
    assert_eq!(live.filled(), Slot::ALL);
    let drained = live.shutdown(Instant::now() + PATIENCE).await;
    assert_eq!(
        drained,
        LiveDrained {
            capture: true,
            stages: true,
            log: true,
        }
    );
}

/// `settle` moves a manual clock forwards only, and returns once a pass
/// changed nothing.
#[tokio::test]
async fn settle_moves_the_clock_forwards_and_reaches_a_fixed_point() {
    let live = start().await;
    let later = Timestamp::from_micros(T0.as_micros() + MINUTE);
    let settled = live.settle(later).await;
    let Ok(settled) = settled else {
        panic!("settle: {settled:?}");
    };
    assert_eq!(settled.at, later);
    assert!(settled.passes >= 1);
    assert_eq!(live.clock().now(), later);
    let again = live.settle(T0).await;
    assert_eq!(again.map(|settled| settled.at), Ok(later));
    live.shutdown(Instant::now() + PATIENCE).await;
}

#[tokio::test]
async fn a_slot_is_filled_once() {
    let live = start().await;
    let publisher = live.context().publisher.clone();
    let catalog = live.stores().catalog.clone();
    let transmissions = live.stores().transmissions.clone();
    let classifier = || Classifier::new(catalog.clone(), transmissions.clone(), publisher.clone());
    let mut stages = Stages::default();
    assert_eq!(stages.fill(Slot::L6Classify, classifier()), Ok(()));
    assert_eq!(
        stages.fill(Slot::L6Classify, classifier()),
        Err(SlotTaken(Slot::L6Classify))
    );
    assert_eq!(stages.filled(), [Slot::L6Classify]);
    assert!(!stages.unfilled().contains(&Slot::L6Classify));
    live.shutdown(Instant::now() + PATIENCE).await;
}

/// A store-decided event goes outbox, bus, surface relay, live feed.
#[tokio::test]
async fn a_store_event_reaches_the_bus_and_the_live_feed() {
    let live = start().await;
    let mut observer = observe(&live, &[Subject::Changed]).await;
    let caller = match live.caller(RequestIdentity::Anonymous).await {
        Ok(caller) => caller,
        Err(error) => panic!("caller: {error:?}"),
    };
    let mut stream = match live.surface().subscribe(&caller, Resume::Fresh).await {
        Ok(stream) => stream,
        Err(error) => panic!("live feed: {error:?}"),
    };

    let mut ids = Ids::seeded(11);
    let resource = ResourceBuilder::new(&mut ids)
        .url("https", "wiki.example", "/live", None)
        .first_seen(T0)
        .build();
    let access = AccessBuilder::new(&mut ids)
        .by(ids.agent())
        .on(resource.id)
        .at(T0)
        .write()
        .build();
    let channel = ids.channel();
    let mut registry = live.stores().channels.clone();
    assert_eq!(registry.add_resource(resource.clone()).await, Ok(None));
    assert_eq!(registry.record_access(access).await, Ok(()));
    assert_eq!(
        registry
            .discover(channel, resource.id, ids.transmission(), T0)
            .await,
        Ok(Discovery::Created(channel))
    );

    first(&mut observer, |envelope| match &envelope.event {
        BusEvent::Changed(Changed::Channel(id)) if *id == channel => Some(()),
        _ => None,
    })
    .await;
    let deadline = Instant::now() + PATIENCE;
    loop {
        match tokio::time::timeout_at(deadline, stream.next()).await {
            Ok(Ok(LiveItem::Event {
                event: UiEvent::ChannelChanged { id },
                ..
            })) if id == channel => break,
            Ok(Ok(_)) => {}
            other => panic!("live feed: {other:?}"),
        }
    }
    live.shutdown(Instant::now() + PATIENCE).await;
}

#[tokio::test]
async fn a_confirmed_transmission_is_classified_under_the_active_version() {
    let live = start().await;
    let mut observer = observe(&live, &[Subject::TransmissionClassified]).await;
    let mut ids = Ids::seeded(12);
    let (from, to, channel) = (ids.agent(), ids.agent(), ids.channel());
    let Ok(stored) = TransmissionBuilder::new(&mut ids)
        .between(from, to)
        .channel(channel)
        .opened_at(T0)
        .confirmed()
        .build()
    else {
        panic!("transmission fixture");
    };
    let transmission = stored.id;
    let Some(at) = stored.state.confirmed().map(|confirmed| confirmed.at()) else {
        panic!("not confirmed");
    };
    let mut transmissions = live.stores().transmissions.clone();
    assert_eq!(transmissions.save(stored).await, Ok(()));
    let matched_bytes = NonZeroU64::new(42).unwrap_or(NonZeroU64::MIN);
    let confirmed = DetectEvent::TransmissionConfirmed {
        transmission,
        from,
        to,
        route: Route::Channel(channel),
        at,
        matched_bytes,
    };
    let published = live
        .context()
        .publisher
        .publish(BusEvent::Detect(confirmed))
        .await;
    assert!(published.is_ok(), "{published:?}");

    let classified = first(&mut observer, |envelope| match &envelope.event {
        BusEvent::Insight(event @ InsightEvent::TransmissionClassified { .. }) => {
            Some(event.clone())
        }
        _ => None,
    })
    .await;
    assert_eq!(
        classified,
        InsightEvent::TransmissionClassified {
            cause: ClassificationCause::Confirmation,
            transmission,
            from,
            to,
            route: Route::Channel(channel),
            at,
            matched_bytes,
            classification: Classification {
                version: TopicModelVersion(0),
                topic: None,
                watched: false,
            },
        }
    );
    let stored = live
        .stores()
        .catalog
        .assignment(TopicModelVersion(0), transmission);
    assert_eq!(stored.map(|stored| stored.topic), Some(None));
    let saved = transmissions.transmission(transmission).await;
    assert!(
        matches!(
            &saved,
            Ok(Some(saved)) if matches!(saved.state, TransmissionState::Classified { .. })
        ),
        "{saved:?}"
    );
    live.shutdown(Instant::now() + PATIENCE).await;
}

#[tokio::test]
async fn an_access_and_its_resource_reach_the_evidence_records() {
    let live = start().await;
    let mut ids = Ids::seeded(13);
    let resource = ResourceBuilder::new(&mut ids)
        .url("https", "wiki.example", "/evidence", None)
        .first_seen(T0)
        .build();
    let access = AccessBuilder::new(&mut ids)
        .by(ids.agent())
        .on(resource.id)
        .at(T0)
        .write()
        .build();
    let mut registry = live.stores().channels.clone();
    assert_eq!(registry.add_resource(resource.clone()).await, Ok(None));
    assert_eq!(registry.record_access(access.clone()).await, Ok(()));
    let published = live
        .context()
        .publisher
        .publish(BusEvent::Detect(DetectEvent::AccessRecorded {
            access: access.clone(),
            channel: None,
        }))
        .await;
    assert!(published.is_ok(), "{published:?}");

    let evidence = live.stores().evidence.clone();
    let deadline = Instant::now() + PATIENCE;
    loop {
        let (Ok(found_access), Ok(found_resource)) = (
            evidence.access(access.id).await,
            evidence.resource(resource.id).await,
        ) else {
            panic!("evidence read failed");
        };
        if let (Some(found_access), Some(found_resource)) = (found_access, found_resource) {
            assert_eq!(found_access, access);
            assert_eq!(found_resource, resource);
            break;
        }
        assert!(Instant::now() < deadline, "the evidence was not fed");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    live.shutdown(Instant::now() + PATIENCE).await;
}

/// The public defaults start on a read clock with eval's short windows.
#[tokio::test]
async fn the_defaults_start_on_any_clock() {
    let clock: Arc<dyn crosstalk_spec::support::Clock> = Arc::new(ManualClock::at(T0));
    let flow = FlowConfig {
        evidence_window_ms: 10_000,
        suspected_ttl_ms: 60_000,
        ..FlowConfig::default()
    };
    let config = LiveConfig::new(LiveClock::Read(clock), flow, 3);
    let Ok(config) = config else {
        panic!("defaults: {config:?}", config = config.err());
    };
    let live = match Live::start(config).await {
        Ok(live) => live,
        Err(error) => panic!("start: {error}"),
    };
    assert_eq!(live.filled(), Slot::ALL);
    let settled = live.settle(T0).await;
    assert!(settled.is_ok(), "{settled:?}");
    live.shutdown(Instant::now() + PATIENCE).await;
}

#[tokio::test]
async fn bodies_can_live_on_the_filesystem() {
    let Ok(root) = tempfile::tempdir() else {
        panic!("tempdir");
    };
    let live = match Live::start(config(BlobConfig::Fs {
        root: root.path().join("blobs"),
    }))
    .await
    {
        Ok(live) => live,
        Err(error) => panic!("start: {error}"),
    };
    assert!(matches!(live.stores().blobs, LiveBlobs::Fs(_)));
    let blobs = live.pipeline().blobs().clone();
    let Ok(hash) = blobs.put(b"a body").await else {
        panic!("put");
    };
    let read = live.stores().blobs.get(hash).await;
    assert_eq!(read, Ok(Some(b"a body".to_vec())));
    live.shutdown(Instant::now() + PATIENCE).await;
}

/// INV-1062 (`surface.export.verdicts-from-the-store`): the verdicts export
/// holds one row per verdict record of every judgeable transmission
/// (suspected, discarded, confirmed or later) opened in the settled window,
/// unconfirmed ones included, ordered by (transmission, revision); a
/// transmission without a verdict, or opened outside the window, has none.
#[tokio::test]
async fn the_verdicts_export_covers_unconfirmed_transmissions() {
    use crosstalk_spec::derived::flow::verdict::Verdict;
    use crosstalk_spec::interfaces::l5_flow::verdicts::TransmissionVerdicts;
    use crosstalk_spec::interfaces::l8_surface::QueryApi;
    use crosstalk_spec::interfaces::l8_surface::export::{
        ExportDataset, ExportRequest, ExportRow, ExportStep, ExportStream,
    };
    use crosstalk_spec::support::TimeWindow;

    let live = start().await;
    let mut ids = Ids::seeded(21);
    let (a, b) = (ids.agent(), ids.agent());
    let at = |minutes: u64| Timestamp::from_micros(T0.as_micros() + minutes * MINUTE);
    let build = |ids: &mut Ids,
                 opened: Timestamp,
                 state: fn(TransmissionBuilder) -> TransmissionBuilder| {
        let channel = ids.channel();
        match state(
            TransmissionBuilder::new(ids)
                .between(a, b)
                .channel(channel)
                .opened_at(opened),
        )
        .build()
        {
            Ok(transmission) => transmission,
            Err(error) => panic!("fixture: {error:?}"),
        }
    };
    let suspected = build(&mut ids, at(10), TransmissionBuilder::suspected);
    let confirmed = build(&mut ids, at(20), TransmissionBuilder::confirmed);
    let awaiting = build(&mut ids, at(30), TransmissionBuilder::awaiting_content);
    let unjudged = build(&mut ids, at(40), TransmissionBuilder::discarded);
    let outside = build(&mut ids, at(200), TransmissionBuilder::confirmed);
    let mut store = live.stores().transmissions.clone();
    for transmission in [&suspected, &confirmed, &awaiting, &unjudged, &outside] {
        assert_eq!(store.save(transmission.clone()).await, Ok(()));
    }
    let operator = OperatorId::from_ulid(0x11FE);
    for (id, verdict, minute) in [
        (suspected.id, Some(Verdict::FalseDetection), 50),
        (suspected.id, None, 51),
        (confirmed.id, Some(Verdict::Genuine), 52),
        (outside.id, Some(Verdict::Genuine), 210),
    ] {
        let set = store.set(id, verdict, operator, at(minute), None).await;
        assert!(set.is_ok(), "{set:?}");
    }
    // An awaiting transmission takes no verdict.
    assert!(
        store
            .set(awaiting.id, Some(Verdict::Genuine), operator, at(53), None)
            .await
            .is_err()
    );

    // Past the settle bound (120 s + 1800 s by default) of the window's end.
    let settled = live.settle(at(180)).await;
    assert!(settled.is_ok(), "{settled:?}");
    let caller = match live.caller(RequestIdentity::Anonymous).await {
        Ok(caller) => caller,
        Err(error) => panic!("caller: {error:?}"),
    };
    let Ok(window) = TimeWindow::new(T0, at(60)) else {
        panic!("window");
    };
    let Ok(request) =
        ExportRequest::new(ExportDataset::Verdicts(window), ExportFormat::Jsonl, false)
    else {
        panic!("request");
    };
    let export = match live.surface().export(&caller, &request).await {
        Ok(export) => export,
        Err(error) => panic!("export: {error:?}"),
    };
    let mut rows = Vec::new();
    let mut stream = export.rows;
    let trailer = loop {
        match stream.next().await {
            ExportStep::Row(row, rest) => {
                rows.push(row);
                stream = rest;
            }
            ExportStep::End(trailer) => break trailer,
        }
    };
    assert!(trailer.is_complete(), "{trailer:?}");
    let seen: Vec<_> = rows
        .iter()
        .map(|row| match row {
            ExportRow::Verdict(row) => (row.transmission, row.revision.get().get(), row.verdict),
            other => panic!("not a verdict row: {other:?}"),
        })
        .collect();
    let mut expected = vec![
        (suspected.id, 1, Some(Verdict::FalseDetection)),
        (suspected.id, 2, None),
        (confirmed.id, 1, Some(Verdict::Genuine)),
    ];
    expected.sort_by_key(|(id, revision, _)| (*id, *revision));
    assert_eq!(seen, expected);
    live.shutdown(Instant::now() + PATIENCE).await;
}

/// INV-1070, from the store: a stored suspected transmission (a co-access
/// with no content match) is a row of a transmissions export exactly when
/// its states include `suspected`, carrying its state and timed by its
/// opening; the default export holds only the confirmed one.
#[tokio::test]
async fn a_suspected_transmission_is_exported_only_in_its_state() {
    use crosstalk_spec::interfaces::l8_surface::QueryApi;
    use crosstalk_spec::interfaces::l8_surface::export::{
        ExportDataset, ExportRequest, ExportRow, ExportStates, ExportStep, ExportStream,
        TransmissionScope,
    };
    use crosstalk_spec::interfaces::l8_surface::summary::TransmissionStateKind as Kind;
    use crosstalk_spec::support::TimeWindow;

    let live = start().await;
    let mut ids = Ids::seeded(31);
    let (a, b) = (ids.agent(), ids.agent());
    let at = |minutes: u64| Timestamp::from_micros(T0.as_micros() + minutes * MINUTE);
    let build = |ids: &mut Ids, state: fn(TransmissionBuilder) -> TransmissionBuilder| {
        let channel = ids.channel();
        match state(
            TransmissionBuilder::new(ids)
                .between(a, b)
                .channel(channel)
                .opened_at(at(10)),
        )
        .build()
        {
            Ok(transmission) => transmission,
            Err(error) => panic!("fixture: {error:?}"),
        }
    };
    let suspected = build(&mut ids, TransmissionBuilder::suspected);
    let confirmed = build(&mut ids, TransmissionBuilder::confirmed);
    let mut store = live.stores().transmissions.clone();
    for transmission in [&suspected, &confirmed] {
        assert_eq!(store.save(transmission.clone()).await, Ok(()));
    }
    // Past the default settle bound (120 s + 1800 s) of the window's end.
    let settled = live.settle(at(180)).await;
    assert!(settled.is_ok(), "{settled:?}");
    let caller = match live.caller(RequestIdentity::Anonymous).await {
        Ok(caller) => caller,
        Err(error) => panic!("caller: {error:?}"),
    };
    let Ok(window) = TimeWindow::new(T0, at(60)) else {
        panic!("window");
    };
    let export = |states: ExportStates| {
        let live = &live;
        let caller = caller.clone();
        async move {
            let request = match ExportRequest::new(
                ExportDataset::Transmissions(TransmissionScope {
                    window,
                    filter: Default::default(),
                    states,
                }),
                ExportFormat::Jsonl,
                false,
            ) {
                Ok(request) => request,
                Err(error) => panic!("request: {error:?}"),
            };
            let export = match live.surface().export(&caller, &request).await {
                Ok(export) => export,
                Err(error) => panic!("export: {error:?}"),
            };
            let mut rows = Vec::new();
            let mut stream = export.rows;
            loop {
                match stream.next().await {
                    ExportStep::Row(ExportRow::Transmission(row), rest) => {
                        rows.push((row.summary().id, row.state()));
                        stream = rest;
                    }
                    ExportStep::Row(other, _) => panic!("not a transmission row: {other:?}"),
                    ExportStep::End(trailer) => {
                        assert!(trailer.is_complete(), "{trailer:?}");
                        return rows;
                    }
                }
            }
        }
    };
    assert_eq!(
        export(ExportStates::confirmed()).await,
        vec![(confirmed.id, None)]
    );
    let Ok(with_suspected) = ExportStates::new(vec![Kind::Suspected, Kind::Confirmed]) else {
        panic!("states");
    };
    let mut rows = export(with_suspected).await;
    rows.sort();
    let mut expected = vec![
        (suspected.id, Some(Kind::Suspected)),
        (confirmed.id, Some(Kind::Confirmed)),
    ];
    expected.sort();
    assert_eq!(rows, expected);
    let Ok(discarded_only) = ExportStates::new(vec![Kind::Discarded]) else {
        panic!("states");
    };
    assert_eq!(export(discarded_only).await, Vec::new());
    live.shutdown(Instant::now() + PATIENCE).await;
}

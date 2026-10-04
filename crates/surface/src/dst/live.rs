//! The live feed under simulation: paused time, seeded event sequences,
//! streams that read slowly or not at all, and sessions that end.

use std::num::NonZeroU64;
use std::time::Duration;

use crosstalk_sim::{CheckFailed, DurationRange, Probability, SimRng};
use crosstalk_spec::events::changed::Changed;
use crosstalk_spec::events::{BusEvent, Envelope, Subject};
use crosstalk_spec::ids::{AgentId, EventId, ProjectionId};
use crosstalk_spec::interfaces::l2_transport::{
    BusError, ConsumerGroup, Delivery, DeliveryId, EventBus, RetryPolicy, Subscription,
};
use crosstalk_spec::interfaces::l8_surface::live::{
    LiveCursor, LiveEnd, LiveFeed, LiveItem, LiveStream, Resume, UiEvent,
};
use crosstalk_spec::interfaces::l8_surface::operators::{
    AccessConfig, OperatorConfig, OperatorName, OperatorStore,
};
use crosstalk_spec::interfaces::l8_surface::{Permission, PermissionSet};
use crosstalk_spec::support::{Blake3, Timestamp};
use crosstalk_transport::{BusConfig, MpscBus};
use tokio::time::Instant;

use super::check;
use crate::live::{FeedHandle, FeedStream, FeedWriter};
use crate::tests::world::{Fixture, Who, config};

fn agent_changed(n: u64) -> Changed {
    Changed::Agent(AgentId::from_ulid(0xA000 + u128::from(n)))
}

fn projection_changed(n: u64) -> Changed {
    Changed::Projection(ProjectionId::from_ulid(0xB000 + u128::from(n)))
}

/// A random change: a projection one time in four, an agent otherwise.
fn random_change(rng: &mut SimRng, n: u64) -> Changed {
    let projection = Probability::percent(25).is_ok_and(|p| rng.chance(p));
    if projection {
        projection_changed(n)
    } else {
        agent_changed(n)
    }
}

async fn subscribe(fixture: &Fixture, who: Who, resume: Resume) -> Result<FeedStream, CheckFailed> {
    let caller = fixture.caller(who).await;
    fixture
        .surface
        .subscribe(&caller, resume)
        .await
        .map_err(|error| CheckFailed::new(format!("subscribe: {error:?}")))
}

async fn append(feed: &FeedHandle, changed: Changed) -> Result<LiveCursor, CheckFailed> {
    feed.append(changed)
        .await
        .map_err(|error| CheckFailed::new(format!("append: {error}")))
}

fn heartbeat() -> Duration {
    config().live.heartbeat()
}

crosstalk_sim::sim_test! {
    /// INV-463 (`surface.live.cursor-monotonic`): a viewer's stream passes
    /// over projection events it may not see; every item's cursor has the
    /// feed's epoch and a seq that never goes down, events strictly up, and
    /// a heartbeat carries the newest entry delivered or passed over.
    fn stream_cursors_monotonic_when_passing_over(ctx) {
        let fixture = Fixture::new().await;
        let feed = fixture.feed.clone();
        let mut stream = subscribe(&fixture, Who::Viewer, Resume::Fresh).await?;
        let mut rng = ctx.rng();
        let gaps = DurationRange::new(Duration::ZERO, heartbeat() * 2)
            .map_err(|error| CheckFailed::new(format!("{error:?}")))?;
        let count = 3 + rng.below(NonZeroU64::MIN.saturating_add(12));
        let writer = {
            let feed = feed.clone();
            let mut rng = rng.fork();
            ctx.spawn("writer", async move {
                let mut appended = Vec::new();
                for n in 0..count {
                    tokio::time::sleep(rng.duration_in(gaps)).await;
                    let changed = random_change(&mut rng, n);
                    if let Ok(cursor) = feed.append(changed).await {
                        appended.push((cursor, UiEvent::from(changed)));
                    }
                }
                appended
            })
        };
        let mut items = Vec::new();
        let deadline = Instant::now() + heartbeat() * (2 * u32::try_from(count).unwrap_or(1) + 4);
        while Instant::now() < deadline {
            match tokio::time::timeout_at(deadline, stream.next()).await {
                Ok(Ok(item)) => items.push(item),
                Ok(Err(end)) => return Err(CheckFailed::new(format!("ended {end:?}"))),
                Err(_) => break,
            }
        }
        let appended = writer.join().await.map_err(|error| CheckFailed::new(format!("{error:?}")))?;
        let epoch = feed.head().epoch;
        let mut last_seq = 0;
        let mut last_event = 0;
        for item in &items {
            let cursor = item.cursor();
            check(&ctx, cursor.epoch == epoch, || format!("epoch of {item:?}"))?;
            check(&ctx, cursor.seq >= last_seq, || format!("seq went down at {item:?}"))?;
            if let LiveItem::Event { event, .. } = item {
                check(&ctx, cursor.seq > last_event, || format!("event seq not up at {item:?}"))?;
                check(&ctx, event.visible_to_permissions(Who::Viewer.permissions()), || {
                    format!("{event:?} reached a viewer")
                })?;
                last_event = cursor.seq;
            }
            if let LiveItem::Heartbeat { cursor } = item {
                // Every visible entry up to it was delivered before it.
                let owed = appended
                    .iter()
                    .filter(|(at, event)| {
                        at.seq <= cursor.seq && event.visible_to_permissions(Who::Viewer.permissions())
                    })
                    .count();
                let delivered = items
                    .iter()
                    .take_while(|other| !std::ptr::eq(*other, item))
                    .filter(|other| matches!(other, LiveItem::Event { .. }))
                    .count();
                check(&ctx, delivered == owed, || {
                    format!("heartbeat at {} after {delivered} events, owed {owed}", cursor.seq)
                })?;
            }
            last_seq = cursor.seq;
        }
        let visible = appended
            .iter()
            .filter(|(_, event)| event.visible_to_permissions(Who::Viewer.permissions()))
            .count();
        let delivered = items.iter().filter(|item| matches!(item, LiveItem::Event { .. })).count();
        check(&ctx, visible == delivered, || format!("{delivered} of {visible} delivered"))
    }
}

/// `UiEvent::visible_to` for a permission set, without a `Caller`.
trait VisibleTo {
    fn visible_to_permissions(self, permissions: PermissionSet) -> bool;
}

impl VisibleTo for UiEvent {
    fn visible_to_permissions(self, permissions: PermissionSet) -> bool {
        permissions.contains(self.required_permission())
    }
}

crosstalk_sim::sim_test! {
    /// INV-466 (`surface.live.heartbeat-interval`): a stream that has sent
    /// nothing for the heartbeat interval sends a heartbeat.
    fn idle_stream_sends_heartbeat(ctx) {
        let fixture = Fixture::new().await;
        let mut stream = subscribe(&fixture, Who::Viewer, Resume::Fresh).await?;
        let mut rng = ctx.rng();
        for round in 0..4 {
            let sent = Instant::now();
            if round % 2 == 1 {
                let quiet = rng.duration_in(
                    DurationRange::new(Duration::ZERO, heartbeat() / 2)
                        .map_err(|error| CheckFailed::new(format!("{error:?}")))?,
                );
                tokio::time::sleep(quiet).await;
                append(&fixture.feed, agent_changed(round)).await?;
                let item = stream.next().await;
                check(&ctx, matches!(item, Ok(LiveItem::Event { .. })), || format!("{item:?}"))?;
                continue;
            }
            let item = stream.next().await;
            check(&ctx, matches!(item, Ok(LiveItem::Heartbeat { .. })), || format!("{item:?}"))?;
            let waited = sent.elapsed();
            check(&ctx, waited <= heartbeat(), || format!("heartbeat after {waited:?}"))?;
        }
        Ok(())
    }
}

crosstalk_sim::sim_test! {
    /// INV-469 (`surface.live.resume-no-gap`): resuming after entry n
    /// replays every later entry the caller may see, in order and once
    /// each, then goes live with no entry skipped at the hand-over.
    fn resume_replays_then_goes_live_without_gap(ctx) {
        let fixture = Fixture::new().await;
        let feed = fixture.feed.clone();
        let mut rng = ctx.rng();
        let before = 2 + rng.below(NonZeroU64::MIN.saturating_add(8));
        let mut entries: Vec<(LiveCursor, UiEvent)> = Vec::new();
        for n in 0..before {
            let changed = random_change(&mut rng, n);
            entries.push((append(&feed, changed).await?, UiEvent::from(changed)));
        }
        let pick = rng.index(entries.len()).unwrap_or(0);
        let (from, _) = entries[pick];
        let mut stream = subscribe(&fixture, Who::Viewer, Resume::From(from)).await?;
        let after = 1 + rng.below(NonZeroU64::MIN.saturating_add(6));
        for n in 0..after {
            let changed = random_change(&mut rng, before + n);
            entries.push((append(&feed, changed).await?, UiEvent::from(changed)));
        }
        let mut received = Vec::new();
        loop {
            match tokio::time::timeout(heartbeat() / 2, stream.next()).await {
                Ok(Ok(LiveItem::Event { cursor, event })) => received.push((cursor.seq, event)),
                Ok(Ok(other)) => return Err(CheckFailed::new(format!("unexpected {other:?}"))),
                Ok(Err(end)) => return Err(CheckFailed::new(format!("ended {end:?}"))),
                Err(_) => break,
            }
        }
        let owed: Vec<(u64, UiEvent)> = entries
            .iter()
            .filter(|(cursor, event)| {
                cursor.seq > from.seq && event.visible_to_permissions(Who::Viewer.permissions())
            })
            .map(|(cursor, event)| (cursor.seq, *event))
            .collect();
        check(&ctx, received == owed, || format!("got {received:?}, owed {owed:?}"))
    }
}

crosstalk_sim::sim_test! {
    /// INV-473 (`surface.live.session-end-closes-stream`): after its
    /// session is revoked a stream delivers nothing more, buffered items
    /// included, and ends with `SessionEnded`.
    fn revoked_session_ends_live_stream(ctx) {
        let fixture = Fixture::new().await;
        let feed = fixture.feed.clone();
        let mut stream = subscribe(&fixture, Who::Viewer, Resume::Fresh).await?;
        let mut rng = ctx.rng();
        let buffered = rng.below(NonZeroU64::MIN.saturating_add(5));
        for n in 0..buffered {
            append(&feed, agent_changed(n)).await?;
        }
        let ended = feed
            .end_sessions(Who::Viewer.id())
            .await
            .map_err(|error| CheckFailed::new(error.to_string()))?;
        check(&ctx, ended == 1, || format!("ended {ended} streams"))?;
        append(&feed, agent_changed(99)).await?;
        let item = stream.next().await;
        check(&ctx, item == Err(LiveEnd::SessionEnded), || format!("{item:?}"))?;
        let again = stream.next().await;
        check(&ctx, again == Err(LiveEnd::SessionEnded), || format!("{again:?}"))
    }
}

crosstalk_sim::sim_test! {
    /// INV-474 (`surface.live.slow-subscriber-isolated`): a stream nobody
    /// reads never holds up the writer or another stream; when its buffer
    /// is full it ends with `Lagged`.
    fn stalled_stream_lags_without_blocking_others(ctx) {
        let fixture = Fixture::new().await;
        let feed = fixture.feed.clone();
        let mut stalled = subscribe(&fixture, Who::Viewer, Resume::Fresh).await?;
        let mut reader = subscribe(&fixture, Who::Reader, Resume::Fresh).await?;
        let buffer = u64::from(config().live.buffer().get());
        let mut rng = ctx.rng();
        let extra = 1 + rng.below(NonZeroU64::MIN.saturating_add(4));
        let started = Instant::now();
        let mut cursors = Vec::new();
        for n in 0..buffer + extra {
            cursors.push(append(&feed, agent_changed(n)).await?);
            // The other stream keeps up.
            let item = reader.next().await;
            check(&ctx, matches!(item, Ok(LiveItem::Event { .. })), || format!("reader got {item:?}"))?;
        }
        check(&ctx, started.elapsed() == Duration::ZERO, || {
            format!("appends waited {:?}", started.elapsed())
        })?;
        let item = stalled.next().await;
        check(&ctx, item == Err(LiveEnd::Lagged), || format!("stalled got {item:?}"))?;
        // A reconnect from the stalled stream's start replays what it missed.
        let first = cursors.first().copied().ok_or_else(|| CheckFailed::new("no appends"))?;
        let resume = LiveCursor { seq: first.seq - 1, ..first };
        let mut again = subscribe(&fixture, Who::Viewer, Resume::From(resume)).await?;
        for cursor in &cursors {
            let item = again.next().await;
            check(&ctx, item.map(|item| item.cursor()) == Ok(*cursor), || format!("replay {item:?}"))?;
        }
        Ok(())
    }
}

crosstalk_sim::sim_test! {
    /// INV-547 (`surface.live.events-name-only-queryable-ids`): a stream
    /// delivers only events its caller may query: a viewer passes over
    /// projection events, a reader with Content gets them.
    fn stream_passes_over_events_caller_cannot_query(ctx) {
        let fixture = Fixture::new().await;
        let feed = fixture.feed.clone();
        let mut viewer = subscribe(&fixture, Who::Viewer, Resume::Fresh).await?;
        let mut reader = subscribe(&fixture, Who::Reader, Resume::Fresh).await?;
        let mut rng = ctx.rng();
        // At most the buffer, so neither stream lags before it reads.
        let count = 2 + rng.below(NonZeroU64::MIN.saturating_add(5));
        let mut appended = Vec::new();
        for n in 0..count {
            let changed = random_change(&mut rng, n);
            appended.push(UiEvent::from(changed));
            append(&feed, changed).await?;
        }
        let mut seen_viewer = Vec::new();
        while let Ok(Ok(LiveItem::Event { event, .. })) =
            tokio::time::timeout(heartbeat() / 2, viewer.next()).await
        {
            seen_viewer.push(event);
        }
        let mut seen_reader = Vec::new();
        while let Ok(Ok(LiveItem::Event { event, .. })) =
            tokio::time::timeout(heartbeat() / 2, reader.next()).await
        {
            seen_reader.push(event);
        }
        let for_viewer: Vec<UiEvent> = appended
            .iter()
            .copied()
            .filter(|event| event.required_permission() == Permission::View)
            .collect();
        check(&ctx, seen_viewer == for_viewer, || format!("viewer {seen_viewer:?}"))?;
        check(&ctx, seen_reader == appended, || format!("reader {seen_reader:?}"))
    }
}

/// A subscription that hands out what the bus delivers and records, at
/// each ack, the feed's head and how many `Changed` it delivered by then.
struct Watched<S> {
    inner: S,
    feed: FeedHandle,
    changed: u64,
    acks: std::sync::Arc<std::sync::Mutex<Vec<(u64, u64)>>>,
}

impl<S: Subscription + Send> Subscription for Watched<S> {
    async fn next(&mut self) -> Option<Result<Delivery, BusError>> {
        let next = self.inner.next().await;
        if let Some(Ok(delivery)) = &next
            && matches!(delivery.envelope.event, BusEvent::Changed(_))
        {
            self.changed += 1;
        }
        next
    }

    async fn ack(&mut self, id: DeliveryId) -> Result<(), BusError> {
        if let Ok(mut acks) = self.acks.lock() {
            acks.push((self.changed, self.feed.head().seq));
        }
        self.inner.ack(id).await
    }

    async fn nack(
        &mut self,
        id: DeliveryId,
        retry_after: Duration,
        reason: String,
    ) -> Result<(), BusError> {
        self.inner.nack(id, retry_after, reason).await
    }
}

crosstalk_sim::sim_test! {
    /// INV-548 (`surface.live.feed-appends-every-change`): every `Changed`
    /// delivered to group `live` is in the feed log before its delivery is
    /// acked, and every one is appended.
    fn feed_writer_appends_before_ack(ctx) {
        let bus = MpscBus::start(BusConfig::default())
            .map_err(|error| CheckFailed::new(format!("bus: {error:?}")))?;
        let retry = RetryPolicy::new(
            std::num::NonZeroU32::MIN.saturating_add(4),
            Duration::from_millis(10),
            Duration::from_secs(1),
        )
        .map_err(|error| CheckFailed::new(format!("{error:?}")))?;
        let subscription = bus
            .subscribe(&[Subject::Changed], ConsumerGroup("live".to_owned()), retry)
            .await
            .map_err(|error| CheckFailed::new(format!("subscribe: {error:?}")))?;
        let feed = FeedWriter::spawn(config().live, crosstalk_spec::interfaces::l8_surface::live::FeedEpoch(1));
        let acks = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let watched = Watched {
            inner: subscription,
            feed: feed.clone(),
            changed: 0,
            acks: std::sync::Arc::clone(&acks),
        };
        let consumer = FeedWriter::consume(feed.clone(), watched);
        let mut rng = ctx.rng();
        let count = 3 + rng.below(NonZeroU64::MIN.saturating_add(10));
        for n in 0..count {
            let envelope = Envelope {
                id: EventId::from_ulid(0xE000 + u128::from(n)),
                at: Timestamp::from_micros(n),
                event: BusEvent::Changed(random_change(&mut rng, n)),
            };
            bus.publish(envelope)
                .await
                .map_err(|error| CheckFailed::new(format!("publish: {error:?}")))?;
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
        let acks = acks.lock().map(|acks| acks.clone()).unwrap_or_default();
        check(&ctx, acks.len() as u64 == count, || format!("{} acks of {count}", acks.len()))?;
        for (delivered, head) in &acks {
            check(&ctx, head >= delivered, || format!("acked delivery {delivered} at head {head}"))?;
        }
        check(&ctx, feed.head().seq == count, || format!("head {}", feed.head().seq))?;
        consumer.abort();
        Ok(())
    }
}

crosstalk_sim::sim_test! {
    /// INV-550 (`surface.live.permission-change-ends-stream`): a config
    /// load that changes an operator ends that operator's streams before
    /// anything appended after the load reaches them; other streams carry
    /// on.
    fn operator_change_ends_its_streams(ctx) {
        let fixture = Fixture::new().await;
        let feed = fixture.feed.clone();
        let mut changed = subscribe(&fixture, Who::Viewer, Resume::Fresh).await?;
        let mut kept = subscribe(&fixture, Who::Reader, Resume::Fresh).await?;
        let mut rng = ctx.rng();
        let before = rng.below(NonZeroU64::MIN.saturating_add(3));
        for n in 0..before {
            append(&feed, agent_changed(n)).await?;
        }
        // Config now gives the viewer Audit as well.
        let operators = crate::tests::world::Who::ALL
            .iter()
            .map(|who| {
                let permissions = if *who == Who::Viewer {
                    PermissionSet::of([Permission::View, Permission::Audit])
                } else {
                    who.permissions()
                };
                OperatorConfig {
                    id: who.id(),
                    name: OperatorName::new(&format!("{who:?}")).unwrap_or_else(|_| unreachable_name()),
                    permissions,
                }
            })
            .collect();
        let mut store = fixture.world.operators.clone();
        let changes = store
            .load(
                &AccessConfig::Authenticated(operators),
                crosstalk_spec::ids::ConfigHash::from_digest(Blake3::of(b"reload")),
                Timestamp::from_micros(1),
            )
            .await
            .map_err(|error| CheckFailed::new(format!("load: {error:?}")))?;
        let ended = feed
            .config_loaded(&changes)
            .await
            .map_err(|error| CheckFailed::new(error.to_string()))?;
        check(&ctx, ended == 1, || format!("ended {ended}"))?;
        append(&feed, agent_changed(50)).await?;
        let item = changed.next().await;
        check(&ctx, item == Err(LiveEnd::SessionEnded), || format!("changed operator got {item:?}"))?;
        for _ in 0..before + 1 {
            let item = kept.next().await;
            check(&ctx, matches!(item, Ok(LiveItem::Event { .. })), || format!("kept got {item:?}"))?;
        }
        Ok(())
    }
}

fn unreachable_name() -> OperatorName {
    match OperatorName::new("operator") {
        Ok(name) => name,
        Err(error) => panic!("{error:?}"),
    }
}

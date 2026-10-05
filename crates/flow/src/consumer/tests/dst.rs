//! Deterministic simulations of the flow consumer under paused time: the
//! consumer runs its loop over the in-process bus, content matches and
//! exchanges reach it reordered, duplicated, dropped and redelivered, and
//! late, and its ticks read the simulation's clock.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use crosstalk_sim::{
    BusFaults, CheckFailed, DropFault, DurationRange, FaultyBus, Probability, Redelivery, Reorder,
    SimConfig, SimCtx, SubjectFaults, Timed, sim_test,
};
use crosstalk_spec::derived::flow::channel::policy::PolicyKind;
use crosstalk_spec::derived::flow::channel::promotion::Promotion;
use crosstalk_spec::derived::flow::resource::{Host, Locator, ResourcePattern};
use crosstalk_spec::derived::flow::transmission::{
    DirectCarrier, NonChannelRoute, Route, TransmissionState,
};
use crosstalk_spec::derived::provenance::matching::{Carrier, ContentMatch};
use crosstalk_spec::events::detect::DetectEvent;
use crosstalk_spec::events::ingest::IngestEvent;
use crosstalk_spec::events::{BusEvent, Envelope, Subject};
use crosstalk_spec::ids::{AgentId, ChannelId, OperatorId, TransmissionId};
use crosstalk_spec::interfaces::l2_transport::{ConsumerGroup, EventBus, Subscription};
use crosstalk_spec::interfaces::l5_flow::ChannelRegistry;
use crosstalk_spec::interfaces::l5_flow::transmissions::TransmissionStore;
use crosstalk_spec::observed::message::{ToolCallId, ToolName};
use crosstalk_spec::support::{Clock, Timestamp};
use crosstalk_testkit::build::exchange::ExchangeBuilder;
use crosstalk_testkit::ids::Ids;
use crosstalk_transport::{BusConfig, MpscBus};
use tokio::sync::mpsc;
use tokio::time::Instant;

use super::harness::{
    Consumer, RecordingBus, Stores, confirmations, consumer, discoveries, found_in, matched, read,
    wiki_page, write,
};
use crate::consumer::apply::discovered_channel_id;
use crate::consumer::resources::resource_id;
use crate::consumer::{Extracted, Observed, ReadResult, SUBJECTS, WriteCall, group};
use crate::correlate::MediumKey;
use crate::correlate::pairing::WriteOutcome;
use crate::correlate::tests::fixtures::Scene;

/// Every simulation sweeps this many seeds by default.
fn config() -> SimConfig {
    SimConfig {
        default_seeds: std::num::NonZeroU32::new(24).unwrap_or(std::num::NonZeroU32::MIN),
        ..SimConfig::default()
    }
}

fn failed(what: &str, error: impl std::fmt::Debug) -> CheckFailed {
    CheckFailed::new(format!("{what}: {error:?}"))
}

fn ms(millis: u64) -> Duration {
    Duration::from_millis(millis)
}

fn secs(seconds: u64) -> Duration {
    Duration::from_secs(seconds)
}

fn at(base: Timestamp, offset: Duration) -> Timestamp {
    Timestamp::from_micros(base.as_micros() + u64::try_from(offset.as_micros()).unwrap_or(u64::MAX))
}

fn probability(percent: u8) -> Probability {
    Probability::percent(percent).unwrap_or(Probability::NEVER)
}

/// Content matches and exchanges reordered, duplicated, dropped and
/// redelivered, and some late by up to 100 s a delivery: past the
/// evidence window (60 s), well within the suspicion (300 s).
fn late_and_unruly() -> SubjectFaults {
    SubjectFaults {
        delay: Some(Timed::new(
            probability(40),
            DurationRange::new(ms(1), secs(100)).unwrap_or(DurationRange::exactly(ms(1))),
        )),
        reorder: Reorder::new(probability(30), 4, ms(5)).ok(),
        duplicate: probability(30),
        drop: Some(DropFault {
            chance: probability(10),
            redelivery: Redelivery::Nack {
                after: DurationRange::new(ms(10), ms(500))
                    .unwrap_or(DurationRange::exactly(ms(10))),
            },
        }),
        crash_on_publish: Probability::NEVER,
        crash_before_ack: Probability::NEVER,
    }
}

/// One thing the scenario does at a point in simulated time.
enum Action {
    Extract(Extracted),
    Publish(BusEvent),
    /// Promote `channel` over every page of the wiki, and publish the
    /// registry's `ChannelPromoted`.
    Promote {
        channel: ChannelId,
    },
}

struct Script {
    shards: usize,
    faults: SubjectFaults,
    steps: Vec<(Duration, Action)>,
    until: Duration,
}

struct Outcome {
    stores: Stores,
    flow: Consumer<FaultyBus<MpscBus>>,
    /// What the consumer published, in delivery order.
    events: Vec<BusEvent>,
}

/// Run `script` against a consumer over `stores`.
async fn simulate(
    ctx: &SimCtx,
    mut stores: Stores,
    script: Script,
) -> Result<Outcome, CheckFailed> {
    let config = BusConfig::default();
    let bus = Arc::new(MpscBus::start(config.clone()).map_err(|error| failed("bus", error))?);
    let faults = BusFaults::none()
        .with_subject(Subject::ContentMatched, script.faults.clone())
        .with_subject(Subject::ExchangeCaptured, script.faults);
    let flow_node = ctx.node("flow");
    let source_node = ctx.node("upstream-layers");
    let flow_bus = ctx.faulty_bus(Arc::clone(&bus), faults.clone(), &flow_node.handle());
    let source_bus = ctx.faulty_bus(Arc::clone(&bus), faults, &source_node.handle());
    let subscription = flow_bus
        .subscribe(&SUBJECTS, group(), config.retry)
        .await
        .map_err(|error| failed("subscribe", error))?;
    let mut observer = bus
        .subscribe(
            &[
                Subject::AccessRecorded,
                Subject::ChannelCrossAccessed,
                Subject::TransmissionConfirmed,
                Subject::TransmissionSuspected,
            ],
            ConsumerGroup("observer".to_owned()),
            config.retry,
        )
        .await
        .map_err(|error| failed("observer", error))?;
    let clock = ctx.clock();
    let mut flow = consumer(&stores, flow_bus, Arc::new(clock.clone()), script.shards);
    let (inputs, extracted) = mpsc::channel(256);
    let start = Instant::now();
    let mut ids = Ids::seeded(4242);
    let registry = stores.registry.clone();
    let registry_events = &mut stores.registry_events;
    let steps = script.steps;
    let until = script.until;
    let driver = async move {
        let mut registry = registry;
        for (offset, action) in steps {
            tokio::time::sleep_until(start + offset).await;
            match action {
                Action::Extract(input) => {
                    if inputs.send(input).await.is_err() {
                        return Err(CheckFailed::new("the consumer stopped taking inputs"));
                    }
                }
                Action::Publish(event) => {
                    let envelope = Envelope {
                        id: ids.event(),
                        at: clock.now(),
                        event,
                    };
                    source_bus
                        .publish(envelope)
                        .await
                        .map_err(|error| failed("publish", error))?;
                }
                Action::Promote { channel } => {
                    let promotion = Promotion::new(
                        ResourcePattern::Host(Host("wiki.example".to_owned())),
                        PolicyKind::Sanctioned,
                        OperatorId::from_ulid(9),
                        clock.now(),
                        None,
                    );
                    registry
                        .promote(channel, promotion)
                        .await
                        .map_err(|error| failed("promote", error))?;
                    let promoted: Vec<BusEvent> = crosstalk_memory::support::drain(registry_events)
                        .into_iter()
                        .filter(|event| {
                            matches!(event, BusEvent::Detect(DetectEvent::ChannelPromoted { .. }))
                        })
                        .collect();
                    for event in promoted {
                        let envelope = Envelope {
                            id: ids.event(),
                            at: clock.now(),
                            event,
                        };
                        bus.publish(envelope)
                            .await
                            .map_err(|error| failed("publish", error))?;
                    }
                }
            }
        }
        tokio::time::sleep_until(start + until).await;
        drop(inputs);
        bus.shutdown().await;
        Ok::<(), CheckFailed>(())
    };
    let observe = async {
        let mut events = Vec::new();
        while let Some(Ok(delivery)) = observer.next().await {
            events.push(delivery.envelope.event.clone());
            if observer.ack(delivery.id).await.is_err() {
                break;
            }
        }
        events
    };
    let ((), driven, events) = tokio::join!(flow.run(subscription, extracted), driver, observe);
    driven?;
    Ok(Outcome {
        stores,
        flow,
        events,
    })
}

/// A writes `page` holding a span; B reads it 30 s later; A's span is
/// found in B's tool result. Returns the agents, the span, the read and
/// the match, and the base time.
struct DeadDrop {
    a: AgentId,
    b: AgentId,
    page: Locator,
    content: ContentMatch,
    steps: Vec<(Duration, Action)>,
}

fn dead_drop(scene: &mut Scene, base: Timestamp, page: &str, match_at: Duration) -> DeadDrop {
    let (a, b) = (scene.agent(), scene.agent());
    let page = wiki_page(page);
    let span = scene.span();
    let edit = write(scene, a, &page, base, vec![span]);
    let fetch = read(scene, b, &page, at(base, secs(30)));
    let content = found_in(scene, &fetch, a, span);
    let steps = vec![
        (
            secs(0),
            Action::Extract(Extracted::Write {
                write: edit,
                outcome: Some(WriteOutcome::Delivered),
            }),
        ),
        (secs(30), Action::Extract(Extracted::Read(fetch))),
        (match_at, Action::Publish(matched(&content))),
    ];
    DeadDrop {
        a,
        b,
        page,
        content,
        steps,
    }
}

fn sorted(mut steps: Vec<(Duration, Action)>) -> Vec<(Duration, Action)> {
    steps.sort_by_key(|(offset, _)| *offset);
    steps
}

fn confirmed_count(events: &[BusEvent]) -> BTreeMap<TransmissionId, usize> {
    let mut counts = BTreeMap::new();
    for id in confirmations(events) {
        *counts.entry(id).or_insert(0) += 1;
    }
    counts
}

fn suspected(events: &[BusEvent]) -> Vec<TransmissionId> {
    events
        .iter()
        .filter_map(|event| match event {
            BusEvent::Detect(DetectEvent::TransmissionSuspected { transmission, .. }) => {
                Some(*transmission)
            }
            _ => None,
        })
        .collect()
}

async fn stored_state(outcome: &Outcome, id: TransmissionId) -> Option<TransmissionState> {
    match outcome.stores.transmissions.transmission(id).await {
        Ok(Some(transmission)) => Some(transmission.state),
        _ => None,
    }
}

sim_test! {
    /// The match is published before the read is even extracted, and
    /// reaches the consumer in any order: the transmission is confirmed
    /// (`flow.correlator.order-insensitive`).
    fn match_before_access_confirms(ctx) with config() => {
        let base = ctx.clock().now();
        let mut scene = Scene::new(200);
        let drop = dead_drop(&mut scene, base, "Before", secs(1));
        let outcome = simulate(&ctx, Stores::new(), Script {
            shards: 3,
            faults: late_and_unruly(),
            steps: sorted(drop.steps),
            until: secs(300),
        }).await?;
        let confirmed = confirmed_count(&outcome.events);
        ctx.check(confirmed.len() == 1, || format!("confirmed {confirmed:?}"))?;
        let channels = outcome.stores.channels().await;
        ctx.check(channels.len() == 1, || format!("channels {channels:?}"))?;
        let routed = outcome.stores.transmissions_of(channels[0].channel().id).await;
        ctx.check(
            routed.iter().all(|transmission| matches!(transmission.state, TransmissionState::Confirmed(_))) && routed.len() == 1,
            || format!("{routed:?}"),
        )?;
        let _ = (drop.a, drop.b, drop.page, drop.content);
        Ok(())
    }
}

sim_test! {
    /// Evidence on a channel before a promotion superseded it and after
    /// meets on the promoted channel's shard: a read after the promotion
    /// pairs with a write before it, and nothing is left on another shard
    /// for the superseded channel or the resources
    /// (`flow.correlator.shard-affinity`).
    fn channel_evidence_reaches_one_shard(ctx) with config() => {
        let base = ctx.clock().now();
        let mut scene = Scene::new(201);
        let first = dead_drop(&mut scene, base, "First", secs(31));
        let (c, d) = (scene.agent(), scene.agent());
        let second_page = wiki_page("Second");
        let span = scene.span();
        let second_write = write(&mut scene, c, &second_page, at(base, secs(5)), vec![span]);
        let second_read = read(&mut scene, d, &second_page, at(base, secs(40)));
        let second_match = found_in(&mut scene, &second_read, c, span);
        let r1 = resource_id(&first.page, base);
        let c1 = discovered_channel_id(r1, at(base, secs(30)));
        let r2 = resource_id(&second_page, at(base, secs(5)));
        let c2 = discovered_channel_id(r2, at(base, secs(40)));
        let e = scene.agent();
        let late_read = read(&mut scene, e, &first.page, at(base, secs(60)));
        let first_span = first.content.origin();
        let late_match = found_in(&mut scene, &late_read, first.a, first_span);
        let mut steps = first.steps;
        steps.extend([
            (secs(5), Action::Extract(Extracted::Write { write: second_write, outcome: Some(WriteOutcome::Delivered) })),
            (secs(40), Action::Extract(Extracted::Read(second_read))),
            (secs(41), Action::Publish(matched(&second_match))),
            (secs(50), Action::Promote { channel: c2 }),
            (secs(60), Action::Extract(Extracted::Read(late_read))),
            (secs(61), Action::Publish(matched(&late_match))),
        ]);
        let outcome = simulate(&ctx, Stores::new(), Script {
            shards: 4,
            faults: late_and_unruly(),
            steps: sorted(steps),
            until: secs(440),
        }).await?;
        let confirmed = confirmed_count(&outcome.events);
        ctx.check(confirmed.len() == 3, || format!("confirmed {confirmed:?}"))?;
        ctx.check(confirmed.values().all(|count| *count == 1), || format!("confirmed {confirmed:?}"))?;
        let routed = outcome.stores.transmissions_of(c2).await;
        let to_e = routed.iter().find(|transmission| transmission.to == e);
        ctx.check(
            to_e.is_some_and(|transmission| transmission.route == Route::Channel(c2)
                && matches!(transmission.state, TransmissionState::Confirmed(_))),
            || format!("{routed:?}"),
        )?;
        let shards = outcome.flow.shards();
        let home = shards.medium_shard(MediumKey::Channel(c2));
        for index in 0..shards.count() {
            let Some(shard) = shards.shard(index) else { continue };
            ctx.check(!shard.holds(MediumKey::Channel(c1)), || format!("shard {index} holds the superseded channel"))?;
            ctx.check(!shard.holds(MediumKey::Resource(r1)) && !shard.holds(MediumKey::Resource(r2)), || format!("shard {index} holds a resource"))?;
            ctx.check(shard.holds(MediumKey::Channel(c2)) == (index == home), || format!("shard {index}, home {home}"))?;
        }
        Ok(())
    }
}

sim_test! {
    /// A tool-result match whose call yielded no access opens one
    /// `Direct(ToolResult)` transmission named after the tool, at the
    /// exchange's start (`flow.route.tool-result-without-access`).
    fn tool_result_without_access_opens_direct(ctx) with config() => {
        let base = ctx.clock().now();
        let mut scene = Scene::new(202);
        let (a, b) = (scene.agent(), scene.agent());
        let exchange = scene.exchange();
        let span = scene.span();
        let call = ToolCallId("toolu_curl".to_owned());
        let content = scene.found(a, b, exchange, span, Carrier::ToolResult(call.clone()));
        let mut captured = ExchangeBuilder::new(&mut scene.ids).started_at(at(base, secs(10))).build();
        captured.meta.id = exchange;
        let steps = vec![
            (secs(9), Action::Extract(Extracted::ToolCall { agent: b, call, name: ToolName("Bash".to_owned()), at: at(base, secs(9)) })),
            (secs(10), Action::Publish(BusEvent::Ingest(IngestEvent::ExchangeCaptured(Box::new(captured))))),
            (secs(12), Action::Publish(matched(&content))),
        ];
        let outcome = simulate(&ctx, Stores::new(), Script { shards: 2, faults: late_and_unruly(), steps, until: secs(400) }).await?;
        let opened: Vec<(Route, Timestamp)> = outcome.events.iter().filter_map(|event| match event {
            BusEvent::Detect(DetectEvent::TransmissionConfirmed { route, at, .. }) => Some((route.clone(), *at)),
            _ => None,
        }).collect();
        let expected = vec![(Route::from(NonChannelRoute::Direct(DirectCarrier::ToolResult(ToolName("Bash".to_owned())))), at(base, secs(10)))];
        ctx.check(opened == expected, || format!("{opened:?}"))?;
        ctx.check(outcome.stores.channels().await.is_empty(), || "a channel".to_owned())
    }
}

sim_test! {
    /// Duplicated, redelivered and late matches of one span confirm the
    /// transmission once and extend it with each distinct match once
    /// (`flow.transmission.confirm-once`, `flow.transmission.identity`).
    fn confirm_once_per_transmission(ctx) with config() => {
        let base = ctx.clock().now();
        let mut scene = Scene::new(203);
        let drop = dead_drop(&mut scene, base, "Once", secs(31));
        let second = scene.again(&drop.content, 100);
        let third = scene.again(&drop.content, 200);
        let mut steps = drop.steps;
        steps.extend([
            (secs(32), Action::Publish(matched(&second))),
            (secs(150), Action::Publish(matched(&third))),
            (secs(151), Action::Publish(matched(&drop.content))),
        ]);
        let outcome = simulate(&ctx, Stores::new(), Script { shards: 2, faults: late_and_unruly(), steps: sorted(steps), until: secs(400) }).await?;
        let confirmed = confirmed_count(&outcome.events);
        ctx.check(confirmed.len() == 1 && confirmed.values().all(|count| *count == 1), || format!("{confirmed:?}"))?;
        let Some(id) = confirmed.keys().next().copied() else { return Err(CheckFailed::new("nothing confirmed")) };
        let Some(TransmissionState::Confirmed(stored)) = stored_state(&outcome, id).await else {
            return Err(CheckFailed::new("not stored confirmed"));
        };
        let distinct: BTreeSet<String> = stored.content().iter().map(|content| format!("{content:?}")).collect();
        ctx.check(stored.content().iter().count() == 3 && distinct.len() == 3, || format!("{stored:?}"))
    }
}

/// A dead drop whose content arrives only after its suspicion expired.
async fn discarded_then_found(
    ctx: &SimCtx,
    seed: u32,
) -> Result<(Outcome, TransmissionId, TransmissionId), CheckFailed> {
    let base = ctx.clock().now();
    let mut scene = Scene::new(seed);
    // The read at 30 s closes at 90 s and expires at 390 s.
    let drop = dead_drop(&mut scene, base, "Expired", secs(420));
    let faults = SubjectFaults {
        duplicate: probability(50),
        ..SubjectFaults::none()
    };
    let outcome = simulate(
        ctx,
        Stores::new(),
        Script {
            shards: 2,
            faults,
            steps: drop.steps,
            until: secs(600),
        },
    )
    .await?;
    let first = suspected(&outcome.events);
    let confirmed = confirmations(&outcome.events);
    let ([first], [second, ..]) = (first.as_slice(), confirmed.as_slice()) else {
        return Err(CheckFailed::new(format!(
            "suspected {first:?}, confirmed {confirmed:?}"
        )));
    };
    Ok((outcome, *first, *second))
}

sim_test! {
    /// Content arriving after the suspicion was discarded opens a new
    /// transmission, confirmed on the same channel
    /// (`flow.transmission.content-after-discard-opens-new`).
    fn content_after_discard_opens_new(ctx) with config() => {
        let (outcome, discarded, opened) = discarded_then_found(&ctx, 204).await?;
        ctx.check(discarded != opened, || "the discarded transmission was revived".to_owned())?;
        let channels = outcome.stores.channels().await;
        ctx.check(channels.len() == 1, || format!("{channels:?}"))?;
        let routed = outcome.stores.transmissions_of(channels[0].channel().id).await;
        let states: BTreeMap<TransmissionId, bool> = routed.iter().map(|transmission| (transmission.id, matches!(transmission.state, TransmissionState::Confirmed(_)))).collect();
        ctx.check(states.get(&opened) == Some(&true) && states.get(&discarded) == Some(&false), || format!("{routed:?}"))
    }
}

sim_test! {
    /// Nothing happens to a transmission once discarded: it stays
    /// discarded, and no event names it again
    /// (`flow.transmission.discarded-final`).
    fn no_update_after_discard(ctx) with config() => {
        let (outcome, discarded, _) = discarded_then_found(&ctx, 205).await?;
        let state = stored_state(&outcome, discarded).await;
        ctx.check(matches!(state, Some(TransmissionState::Discarded { .. })), || format!("{state:?}"))?;
        ctx.check(!confirmations(&outcome.events).contains(&discarded), || "a discarded transmission was confirmed".to_owned())
    }
}

sim_test! {
    /// Redelivered and duplicated matches from two senders in one reader
    /// exchange keep one transmission per sender
    /// (`flow.transmission.identity`).
    fn redelivered_matches_keep_identity(ctx) with config() => {
        let base = ctx.clock().now();
        let mut scene = Scene::new(206);
        let drop = dead_drop(&mut scene, base, "Two_Senders", secs(31));
        let c = scene.agent();
        let span = scene.span();
        let other_write = write(&mut scene, c, &drop.page, at(base, secs(3)), vec![span]);
        let reader_part = drop.content.read_at().part;
        let from_c = scene.matched(c, drop.b, drop.content.reader_exchange(), span, reader_part, drop.content.carrier().clone());
        let mut steps = drop.steps;
        steps.extend([
            (secs(3), Action::Extract(Extracted::Write { write: other_write, outcome: Some(WriteOutcome::Delivered) })),
            (secs(33), Action::Publish(matched(&from_c))),
            (secs(34), Action::Publish(matched(&from_c))),
            (secs(35), Action::Publish(matched(&drop.content))),
        ]);
        let outcome = simulate(&ctx, Stores::new(), Script { shards: 3, faults: late_and_unruly(), steps: sorted(steps), until: secs(400) }).await?;
        let channels = outcome.stores.channels().await;
        ctx.check(channels.len() == 1, || format!("{channels:?}"))?;
        let routed = outcome.stores.transmissions_of(channels[0].channel().id).await;
        let mut senders = BTreeMap::new();
        for transmission in &routed {
            if let TransmissionState::Confirmed(confirmed) = &transmission.state {
                *senders.entry(confirmed.from()).or_insert(0) += 1;
                ctx.check(confirmed.content().iter().count() == 1, || format!("{confirmed:?}"))?;
            }
        }
        ctx.check(senders.len() == 2 && senders.values().all(|count| *count == 1), || format!("{routed:?}"))?;
        ctx.check(confirmed_count(&outcome.events).values().all(|count| *count == 1), || "confirmed twice".to_owned())
    }
}

sim_test! {
    /// A window that closes without content, ticked every second, suspects
    /// once (`flow.transmission.suspect-once`).
    fn window_close_suspects_once(ctx) with config() => {
        let base = ctx.clock().now();
        let mut scene = Scene::new(207);
        let mut drop = dead_drop(&mut scene, base, "Quiet", secs(0));
        drop.steps.retain(|(_, action)| !matches!(action, Action::Publish(_)));
        let outcome = simulate(&ctx, Stores::new(), Script { shards: 1, faults: SubjectFaults::none(), steps: sorted(drop.steps), until: secs(380) }).await?;
        let suspected = suspected(&outcome.events);
        ctx.check(suspected.len() == 1, || format!("{suspected:?}"))?;
        ctx.check(confirmations(&outcome.events).is_empty(), || "confirmed without content".to_owned())?;
        let state = stored_state(&outcome, suspected[0]).await;
        ctx.check(matches!(state, Some(TransmissionState::Suspected { .. })), || format!("{state:?}"))
    }
}

sim_test! {
    /// One agent's own writes and reads, writes nobody reads, and reads of
    /// pages nobody wrote create no channel; every access is recorded on
    /// no channel (`flow.channel.resource-only-until-cross-agent`).
    fn writes_without_cross_reads_create_no_channel(ctx) with config() => {
        let base = ctx.clock().now();
        let mut scene = Scene::new(208);
        let (a, b, c, d) = (scene.agent(), scene.agent(), scene.agent(), scene.agent());
        let (own, unread, unwritten) = (wiki_page("Scratch"), wiki_page("Unread"), wiki_page("Unwritten"));
        let span = scene.span();
        let steps = vec![
            (secs(0), Action::Extract(Extracted::Write { write: write(&mut scene, a, &own, base, vec![span]), outcome: Some(WriteOutcome::Delivered) })),
            (secs(5), Action::Extract(Extracted::Write { write: write(&mut scene, b, &unread, at(base, secs(5)), vec![]), outcome: Some(WriteOutcome::Delivered) })),
            (secs(10), Action::Extract(Extracted::Read(read(&mut scene, a, &own, at(base, secs(10)))))),
            (secs(20), Action::Extract(Extracted::Read(read(&mut scene, c, &unwritten, at(base, secs(20)))))),
            // Outside the ten-minute correlation window of B's write.
            (secs(700), Action::Extract(Extracted::Read(read(&mut scene, d, &unread, at(base, secs(700)))))),
        ];
        let mut stores = Stores::new();
        let registry_before = discoveries(&stores.registry_events());
        let mut outcome = simulate(&ctx, stores, Script { shards: 2, faults: SubjectFaults::none(), steps, until: secs(1_000) }).await?;
        ctx.check(registry_before.is_empty(), || "discoveries before".to_owned())?;
        ctx.check(outcome.stores.channels().await.is_empty(), || "a channel".to_owned())?;
        ctx.check(discoveries(&outcome.stores.registry_events()).is_empty(), || "ChannelDiscovered".to_owned())?;
        let recorded: Vec<Option<ChannelId>> = outcome.events.iter().filter_map(|event| match event {
            BusEvent::Detect(DetectEvent::AccessRecorded { channel, .. }) => Some(*channel),
            _ => None,
        }).collect();
        ctx.check(recorded.len() == 5 && recorded.iter().all(Option::is_none), || format!("{recorded:?}"))?;
        ctx.check(outcome.events.len() == 5, || format!("{:?}", outcome.events))
    }
}

sim_test! {
    /// When a channel is discovered from a resource, the resource's
    /// evidence moves to the channel's shard before the next input: a read
    /// after the discovery, on the channel, pairs with the write made
    /// before it, on the resource (`flow.correlator.resource-shard-handoff`).
    fn discovery_moves_resource_evidence(ctx) with config() => {
        let base = ctx.clock().now();
        let shards = 4;
        let probe = crate::consumer::Shards::new(super::harness::settings(shards).timing, super::harness::settings(shards).content_retention, super::harness::settings(shards).shards);
        // A page whose resource and channel hash to different shards.
        let name = (0..64).map(|n| format!("Handoff_{n}")).find(|name| {
            let resource = resource_id(&wiki_page(name), base);
            let channel = discovered_channel_id(resource, at(base, secs(30)));
            probe.medium_shard(MediumKey::Resource(resource)) != probe.medium_shard(MediumKey::Channel(channel))
        });
        let Some(name) = name else { return Err(CheckFailed::new("no page splits across shards")) };
        let mut scene = Scene::new(209);
        let drop = dead_drop(&mut scene, base, &name, secs(31));
        let resource = resource_id(&drop.page, base);
        let channel = discovered_channel_id(resource, at(base, secs(30)));
        let e = scene.agent();
        let later = read(&mut scene, e, &drop.page, at(base, secs(45)));
        let later_match = found_in(&mut scene, &later, drop.a, drop.content.origin());
        let mut steps = drop.steps;
        steps.extend([
            (secs(45), Action::Extract(Extracted::Read(later))),
            (secs(46), Action::Publish(matched(&later_match))),
        ]);
        let outcome = simulate(&ctx, Stores::new(), Script { shards, faults: late_and_unruly(), steps: sorted(steps), until: secs(300) }).await?;
        let routed = outcome.stores.transmissions_of(channel).await;
        ctx.check(routed.len() == 2, || format!("{routed:?}"))?;
        ctx.check(routed.iter().all(|transmission| matches!(transmission.state, TransmissionState::Confirmed(_)) && transmission.route == Route::Channel(channel)), || format!("{routed:?}"))?;
        let flow_shards = outcome.flow.shards();
        let (from, to) = (flow_shards.medium_shard(MediumKey::Resource(resource)), flow_shards.medium_shard(MediumKey::Channel(channel)));
        ctx.check(from != to, || "same shard".to_owned())?;
        ctx.check(flow_shards.shard(from).is_some_and(|shard| !shard.holds(MediumKey::Resource(resource))), || "evidence left behind".to_owned())?;
        ctx.check(flow_shards.shard(to).is_some_and(|shard| shard.holds(MediumKey::Channel(channel))), || "evidence not moved".to_owned())
    }
}

/// One consumer's share of a contended discovery: a write, then a read.
async fn write_then_read(
    flow: &mut Consumer<RecordingBus>,
    (edit, fetch): (Observed<WriteCall>, Observed<ReadResult>),
    before: Duration,
    between: Duration,
) {
    tokio::time::sleep(before).await;
    flow.handle_extracted(Extracted::Write {
        write: edit,
        outcome: Some(WriteOutcome::Delivered),
    })
    .await;
    tokio::time::sleep(between).await;
    flow.handle_extracted(Extracted::Read(fetch)).await;
}

sim_test! {
    /// Two consumers discovering a channel from one resource at once make
    /// one channel and one `ChannelDiscovered`, and route both
    /// transmissions through it (`flow.registry.at-most-one-channel-per-resource`).
    fn concurrent_discovery_one_channel(ctx) with config() => {
        let base = ctx.clock().now();
        let mut scene = Scene::new(210);
        let mut stores = Stores::new();
        let page = wiki_page("Contended");
        let mut evidence = Vec::new();
        for _ in 0..2 {
            let (writer, reader) = (scene.agent(), scene.agent());
            let span = scene.span();
            evidence.push((
                write(&mut scene, writer, &page, base, vec![span]),
                read(&mut scene, reader, &page, at(base, secs(30))),
            ));
        }
        let clock: Arc<dyn Clock> = Arc::new(ctx.clock());
        let mut flows: Vec<_> = (0..2).map(|_| consumer(&stores, RecordingBus::default(), Arc::clone(&clock), 2)).collect();
        let mut rng = ctx.rng();
        let jitter = DurationRange::new(ms(0), ms(20)).unwrap_or(DurationRange::exactly(ms(0)));
        let delays: Vec<Duration> = (0..4).map(|_| rng.duration_in(jitter)).collect();
        let mut flows_iter = flows.iter_mut();
        let (Some(first), Some(second)) = (flows_iter.next(), flows_iter.next()) else {
            return Err(CheckFailed::new("two consumers"));
        };
        let mut pairs = evidence.into_iter();
        let (Some(one), Some(two)) = (pairs.next(), pairs.next()) else {
            return Err(CheckFailed::new("two pairs"));
        };
        tokio::join!(
            write_then_read(first, one, delays[0], delays[1]),
            write_then_read(second, two, delays[2], delays[3]),
        );
        let discovered = discoveries(&stores.registry_events());
        ctx.check(discovered.len() == 1, || format!("{discovered:?}"))?;
        let channels = stores.channels().await;
        ctx.check(channels.len() == 1, || format!("{channels:?}"))?;
        let routed = stores.transmissions_of(channels[0].channel().id).await;
        let backlogs: Vec<usize> = flows.iter().map(|flow| flow.backlog()).collect();
        ctx.check(routed.len() == 2, || format!("{routed:?} backlogs {backlogs:?}"))
    }
}

sim_test! {
    /// A write whose result reports a rejection is recorded but never
    /// pairs, whatever order its result, the read and the match arrive in
    /// (`flow.correlator.write-held-until-outcome`).
    fn rejected_write_never_pairs(ctx) with config() => {
        let base = ctx.clock().now();
        let mut scene = Scene::new(211);
        let (a, b) = (scene.agent(), scene.agent());
        let page = wiki_page("Rejected");
        let span = scene.span();
        let edit = write(&mut scene, a, &page, base, vec![span]);
        let edit_id = edit.id;
        let fetch = read(&mut scene, b, &page, at(base, secs(30)));
        let content = found_in(&mut scene, &fetch, a, span);
        let mut rng = ctx.rng();
        let result_at = rng.duration_in(DurationRange::new(secs(1), secs(120)).unwrap_or(DurationRange::exactly(secs(1))));
        let steps = sorted(vec![
            (secs(0), Action::Extract(Extracted::Write { write: edit, outcome: None })),
            (result_at, Action::Extract(Extracted::WriteResult { access: edit_id, outcome: WriteOutcome::Rejected })),
            (secs(30), Action::Extract(Extracted::Read(fetch))),
            (secs(31), Action::Publish(matched(&content))),
        ]);
        let mut outcome = simulate(&ctx, Stores::new(), Script { shards: 2, faults: late_and_unruly(), steps, until: secs(800) }).await?;
        ctx.check(outcome.stores.channels().await.is_empty(), || "a channel".to_owned())?;
        ctx.check(discoveries(&outcome.stores.registry_events()).is_empty(), || "discovered".to_owned())?;
        let subjects: Vec<Subject> = outcome.events.iter().map(BusEvent::subject).collect();
        ctx.check(subjects == vec![Subject::AccessRecorded, Subject::AccessRecorded], || format!("{subjects:?}"))
    }
}

sim_test! {
    /// A write whose result never arrives is released `Unknown` when its
    /// settle window closes and pairs: the transmission it opens is
    /// confirmed by the match held meanwhile
    /// (`flow.correlator.write-held-until-outcome`,
    /// `flow.correlator.unknown-write-pairs`).
    fn write_without_result_settles_unknown(ctx) with config() => {
        let base = ctx.clock().now();
        let mut scene = Scene::new(212);
        let (a, b) = (scene.agent(), scene.agent());
        let page = wiki_page("No_Result");
        let span = scene.span();
        let edit = write(&mut scene, a, &page, base, vec![span]);
        let fetch = read(&mut scene, b, &page, at(base, secs(30)));
        let content = found_in(&mut scene, &fetch, a, span);
        let steps = vec![
            (secs(0), Action::Extract(Extracted::Write { write: edit, outcome: None })),
            (secs(30), Action::Extract(Extracted::Read(fetch))),
            (secs(31), Action::Publish(matched(&content))),
        ];
        // Settles at 360 s; the read's suspicion lasts until 390 s.
        let outcome = simulate(&ctx, Stores::new(), Script { shards: 2, faults: late_and_unruly(), steps, until: secs(450) }).await?;
        let confirmed = confirmed_count(&outcome.events);
        ctx.check(confirmed.len() == 1, || format!("{:?}", outcome.events))?;
        ctx.check(outcome.flow.held_writes().is_empty(), || "still held".to_owned())
    }
}

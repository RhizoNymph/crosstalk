//! The M2 acceptance shape: two agents share a wiki page. A writes it, B
//! reads it, the content match confirms, and the memory stores hold one
//! discovered channel and one confirmed transmission routed through it.
//! Replayed corpora carry timestamps far in the past; windows settle on
//! the replay's clock.

use std::sync::Arc;
use std::time::Duration;

use crosstalk_memory::support::ManualClock;
use crosstalk_spec::derived::flow::channel::ChannelOrigin;
use crosstalk_spec::derived::flow::channel::confirmation::{Confirmation, Listing};
use crosstalk_spec::derived::flow::transmission::{Route, TransmissionState};
use crosstalk_spec::events::detect::DetectEvent;
use crosstalk_spec::events::{BusEvent, Subject};
use crosstalk_spec::interfaces::l5_flow::transmissions::TransmissionStore;
use crosstalk_spec::support::Timestamp;

use super::harness::{
    RecordingBus, Stores, confirmations, consumer, discoveries, found_in, matched, read, wiki_page,
    write,
};
use crate::consumer::Extracted;
use crate::correlate::pairing::WriteOutcome;
use crate::correlate::tests::fixtures::{Scene, timing};

fn after(at: Timestamp, seconds: u64) -> Timestamp {
    Timestamp::from_micros(at.as_micros() + seconds * 1_000_000)
}

/// Runs the scenario from `start`, ticking on a clock that reads the
/// replay's time.
async fn wiki_dead_drop(start: Timestamp) {
    let mut scene = Scene::new(90);
    let mut stores = Stores::new();
    let a = stores.agent(&mut scene, None).await;
    let b = stores.agent(&mut scene, None).await;
    let page = wiki_page("Dead_Drop");
    let span = scene.span();
    let clock = ManualClock::at(start);
    let bus = RecordingBus::default();
    let mut flow = consumer(&stores, bus.clone(), Arc::new(clock.clone()), 2);

    // A edits the page; its result arrives in A's next request.
    let edit = write(&mut scene, a, &page, start, vec![span]);
    let edit_id = edit.id;
    flow.handle_extracted(Extracted::Write {
        write: edit,
        outcome: None,
    })
    .await;
    flow.handle_extracted(Extracted::WriteResult {
        access: edit_id,
        outcome: WriteOutcome::Delivered,
    })
    .await;
    // B reads it 30 s later; provenance finds A's span in B's tool result.
    let fetch = read(&mut scene, b, &page, after(start, 30));
    let content = found_in(&mut scene, &fetch, a, span);
    flow.handle_extracted(Extracted::Read(fetch.clone())).await;
    flow.handle_event(&matched(&content)).await;
    for second in 31..=(30 + 61) {
        clock.set(after(start, second));
        flow.tick(after(start, second)).await;
    }

    // One discovered channel, seeded by the page, listed confirmed.
    let channels = stores.channels().await;
    assert_eq!(channels.len(), 1, "{channels:?}");
    let listed = &channels[0];
    assert_eq!(
        listed.listing(),
        Some(Listing::Channel(Confirmation::Confirmed))
    );
    let channel = listed.channel().id;
    let ChannelOrigin::Discovered { seed, .. } = &listed.channel().origin else {
        panic!("not discovered: {listed:?}");
    };
    assert_eq!(seed.opened_at, fetch.at);

    // One confirmed transmission, A to B, routed through it.
    let routed = stores.transmissions_of(channel).await;
    assert_eq!(routed.len(), 1, "{routed:?}");
    let transmission = &routed[0];
    assert_eq!(transmission.route, Route::Channel(channel));
    assert_eq!(transmission.to, b);
    assert_eq!(seed.first_transmission, transmission.id);
    let stored = stores.transmissions.transmission(transmission.id).await;
    let Ok(Some(stored)) = stored else {
        panic!("not stored: {stored:?}");
    };
    let TransmissionState::Confirmed(confirmed) = &stored.state else {
        panic!("not confirmed: {stored:?}");
    };
    assert_eq!(confirmed.from(), a);
    assert_eq!(confirmed.at(), fetch.at);
    assert_eq!(confirmed.content().first(), &content);

    // The registry announced the discovery; the consumer its own events,
    // in commit order.
    assert_eq!(discoveries(&stores.registry_events()), vec![channel]);
    let events = bus.events();
    assert_eq!(confirmations(&events), vec![transmission.id]);
    assert_eq!(
        bus.subjects(),
        vec![
            Subject::AccessRecorded,
            Subject::AccessRecorded,
            Subject::ChannelCrossAccessed,
            Subject::TransmissionConfirmed,
        ]
    );
    let recorded: Vec<_> = events
        .iter()
        .filter_map(|event| match event {
            BusEvent::Detect(DetectEvent::AccessRecorded { channel, .. }) => Some(*channel),
            _ => None,
        })
        .collect();
    assert_eq!(
        recorded,
        vec![None, None],
        "both accesses predate the channel"
    );
    assert_eq!(flow.backlog(), 0);
}

#[tokio::test]
async fn two_agents_share_a_wiki_page() {
    wiki_dead_drop(crosstalk_testkit::time::T0).await;
}

/// The same exchange replayed from a corpus recorded years ago: the
/// windows close on the replay clock's ticks, so the transmission is
/// confirmed exactly as live, never suspected or discarded against wall
/// time.
#[tokio::test]
async fn a_replayed_corpus_settles_on_the_replay_clock() {
    // 2019-03-01T00:00:00Z.
    wiki_dead_drop(Timestamp::from_micros(1_551_398_400_000_000)).await;
}

/// The page read by a third agent after the channel exists pairs with the
/// write made before it: the evidence moved with the discovery.
#[tokio::test]
async fn a_later_reader_meets_the_earlier_write() {
    let mut scene = Scene::new(91);
    let mut stores = Stores::new();
    let a = stores.agent(&mut scene, None).await;
    let b = stores.agent(&mut scene, None).await;
    let c = stores.agent(&mut scene, None).await;
    let page = wiki_page("Shared_Notes");
    let span = scene.span();
    let start = crosstalk_testkit::time::T0;
    let clock = ManualClock::at(start);
    let mut flow = consumer(&stores, RecordingBus::default(), Arc::new(clock), 4);
    flow.handle_extracted(Extracted::Write {
        write: write(&mut scene, a, &page, start, vec![span]),
        outcome: Some(WriteOutcome::Delivered),
    })
    .await;
    flow.handle_extracted(Extracted::Read(read(
        &mut scene,
        b,
        &page,
        after(start, 30),
    )))
    .await;
    let Some(channel) = stores.only_channel().await else {
        panic!("no channel discovered");
    };
    let later = read(&mut scene, c, &page, after(start, 60));
    let content = found_in(&mut scene, &later, a, span);
    flow.handle_extracted(Extracted::Read(later)).await;
    flow.handle_event(&matched(&content)).await;
    flow.tick(after(start, 200)).await;
    let routed = stores.transmissions_of(channel.id).await;
    assert_eq!(routed.len(), 2, "{routed:?}");
    let to_c = routed.iter().find(|transmission| transmission.to == c);
    assert!(
        to_c.is_some_and(|transmission| matches!(
            transmission.state,
            TransmissionState::Confirmed(_)
        )),
        "{routed:?}"
    );
    let _ = (Duration::ZERO, timing());
}

/// A dead drop read a day after it was written, far past the correlation
/// window, with the writer's span in the reader's tool result: the
/// consumer discovers the channel and stores the transmission confirmed,
/// on a clock that ticked through the day
/// (`flow.correlator.content-confirms-past-window`).
#[tokio::test]
async fn a_dead_drop_read_a_day_later_confirms() {
    let mut scene = Scene::new(92);
    let mut stores = Stores::new();
    let a = stores.agent(&mut scene, None).await;
    let b = stores.agent(&mut scene, None).await;
    let page = wiki_page("Dead_Drop");
    let span = scene.span();
    let start = crosstalk_testkit::time::T0;
    let bus = RecordingBus::default();
    let mut flow = consumer(&stores, bus.clone(), Arc::new(ManualClock::at(start)), 4);
    flow.handle_extracted(Extracted::Write {
        write: write(&mut scene, a, &page, start, vec![span]),
        outcome: Some(WriteOutcome::Delivered),
    })
    .await;
    let day = 24 * 3_600;
    for hour in 1..24 {
        flow.tick(after(start, hour * 3_600)).await;
    }
    let fetch = read(&mut scene, b, &page, after(start, day));
    let content = found_in(&mut scene, &fetch, a, span);
    flow.handle_extracted(Extracted::Read(fetch)).await;
    flow.handle_event(&matched(&content)).await;
    flow.tick(after(start, day + 200)).await;
    let Some(channel) = stores.only_channel().await else {
        panic!("no channel discovered");
    };
    let routed = stores.transmissions_of(channel.id).await;
    assert_eq!(routed.len(), 1, "{routed:?}");
    assert_eq!(routed[0].to, b);
    assert!(
        matches!(routed[0].state, TransmissionState::Confirmed(_)),
        "{routed:?}"
    );
    assert_eq!(confirmations(&bus.events()).len(), 1);
}

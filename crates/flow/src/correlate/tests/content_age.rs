//! Content confirms a channel transmission whatever the write-to-read age,
//! within the content retention; the correlation window bounds
//! access-only pairing alone (`flow.correlator.content-confirms-past-window`).

use std::time::Duration;

use crosstalk_spec::derived::flow::access::Access;
use crosstalk_spec::derived::provenance::matching::ContentMatch;
use crosstalk_spec::ids::AgentId;
use crosstalk_spec::interfaces::l5_flow::TransmissionUpdate;
use crosstalk_spec::support::Timestamp;

use super::fixtures::{Scene, secs, timing};
use super::{fold, kinds};
use crate::correlate::lifecycle::{Stage, UpdateKind};
use crate::correlate::{
    ContentRetention, DEFAULT_CONTENT_RETENTION, Decided, InvalidRetention, WindowedCorrelator,
};

const HOUR: u64 = 3_600;
const DAY: u64 = 24 * HOUR;

/// A writes a page at 0 s holding a span; B reads it `lag` seconds later
/// and A's span is in B's tool result.
struct DeadDrop {
    a: AgentId,
    write: Access,
    read: Access,
    content: ContentMatch,
}

fn dead_drop(seed: u32, lag: u64) -> DeadDrop {
    let mut scene = Scene::new(seed);
    let (a, b) = (scene.agent(), scene.agent());
    let resource = scene.resource();
    let span = scene.span();
    let write = scene.write(a, resource, secs(0), vec![span]);
    let read = scene.read(b, resource, secs(lag));
    let content = scene.carried(&read, a, span);
    DeadDrop {
        a,
        write,
        read,
        content,
    }
}

fn after(at: Timestamp, by: Duration) -> Timestamp {
    Timestamp::from_micros(at.as_micros() + u64::try_from(by.as_micros()).unwrap_or(u64::MAX))
}

/// One input of a dead drop.
#[derive(Clone, Copy)]
enum Input {
    Write,
    Read,
    Match,
}

fn feed(correlator: &mut WindowedCorrelator, drop: &DeadDrop, input: Input) -> Vec<Decided> {
    match input {
        Input::Write => correlator.access(&drop.write, None),
        Input::Read => correlator.access(&drop.read, None),
        Input::Match => correlator.content(&drop.content),
    }
}

/// The write delivered at its time, ticks every hour up to the read (so
/// the shard collects garbage in between), then the read and the match,
/// then a tick past the read's window.
fn live(correlator: &mut WindowedCorrelator, drop: &DeadDrop) -> Vec<Decided> {
    let mut out = correlator.tick(drop.write.at);
    out.extend(correlator.access(&drop.write, None));
    let mut now = drop.write.at;
    loop {
        now = after(now, Duration::from_secs(HOUR));
        if now >= drop.read.at {
            break;
        }
        out.extend(correlator.tick(now));
    }
    out.extend(correlator.access(&drop.read, None));
    out.extend(correlator.content(&drop.content));
    out.extend(correlator.tick(timing().window_closes_at(drop.read.at)));
    out
}

fn confirmed_lag(out: &[Decided]) -> Option<Duration> {
    out.iter().find_map(|decided| match &decided.update {
        TransmissionUpdate::Confirm { confirmed, .. } => {
            confirmed.co_access().first().map(|co| co.lag())
        }
        _ => None,
    })
}

/// A read with the writer's content an hour, a day and 29 days after the
/// write confirms: the correlation window (10 minutes) does not bound it.
#[test]
fn content_confirms_long_after_the_write() {
    for (seed, lag) in [(1, HOUR), (2, DAY), (3, 29 * DAY)] {
        let drop = dead_drop(seed, lag);
        let mut correlator = WindowedCorrelator::new(timing());
        let out = live(&mut correlator, &drop);
        assert_eq!(
            kinds(&out),
            vec![UpdateKind::OpenChannel, UpdateKind::Confirm],
            "lag {lag} s: {out:?}"
        );
        assert_eq!(out[0].opened_at, drop.read.at);
        assert_eq!(confirmed_lag(&out), Some(Duration::from_secs(lag)));
        let TransmissionUpdate::Confirm { confirmed, .. } = &out[1].update else {
            panic!("not confirmed: {out:?}");
        };
        assert_eq!(confirmed.from(), drop.a);
        assert_eq!(confirmed.content().first(), &drop.content);
    }
}

/// Past the retention (31 days against L4's 30) nothing opens, whether
/// the write was collected in between or is still held.
#[test]
fn content_past_retention_does_not_confirm() {
    let drop = dead_drop(4, 31 * DAY);
    let mut correlator = WindowedCorrelator::new(timing());
    let out = live(&mut correlator, &drop);
    assert!(out.is_empty(), "{out:?}");

    let mut correlator = WindowedCorrelator::new(timing());
    let mut out = Vec::new();
    for input in [Input::Write, Input::Read, Input::Match] {
        out.extend(feed(&mut correlator, &drop, input));
    }
    out.extend(correlator.tick(timing().window_closes_at(drop.read.at)));
    assert!(out.is_empty(), "{out:?}");
}

/// The configured retention is the bound: with two hours, a read an hour
/// later confirms and one three hours later does not.
#[test]
fn configured_retention_bounds_content() {
    let retention = ContentRetention::new(Duration::from_secs(2 * HOUR), timing());
    let Ok(retention) = retention else {
        panic!("{retention:?}");
    };
    let within = dead_drop(5, HOUR);
    let mut correlator = WindowedCorrelator::with_retention(timing(), retention);
    assert_eq!(
        kinds(&live(&mut correlator, &within)),
        vec![UpdateKind::OpenChannel, UpdateKind::Confirm]
    );
    let beyond = dead_drop(6, 3 * HOUR);
    let mut correlator = WindowedCorrelator::with_retention(timing(), retention);
    assert!(live(&mut correlator, &beyond).is_empty());
}

/// Access-only pairing still respects the window: a read just past it with
/// no content opens nothing, one just within it opens and is suspected.
#[test]
fn access_only_pairing_respects_the_window() {
    let window = timing().correlation_window().as_secs();
    let mut scene = Scene::new(7);
    let (a, b) = (scene.agent(), scene.agent());
    let resource = scene.resource();
    let span = scene.span();
    let write = scene.write(a, resource, secs(0), vec![span]);

    let late = scene.read(b, resource, secs(window + 1));
    let mut correlator = WindowedCorrelator::new(timing());
    let mut out = correlator.access(&write, None);
    out.extend(correlator.access(&late, None));
    out.extend(correlator.tick(timing().expires_at(timing().window_closes_at(late.at))));
    assert!(out.is_empty(), "{out:?}");

    let timely = scene.read(b, resource, secs(window - 1));
    let mut correlator = WindowedCorrelator::new(timing());
    let mut out = correlator.access(&write, None);
    out.extend(correlator.access(&timely, None));
    out.extend(correlator.tick(timing().window_closes_at(timely.at)));
    assert_eq!(
        kinds(&out),
        vec![UpdateKind::OpenChannel, UpdateKind::Suspect]
    );
}

/// Content from an old write and a recent access-only pair of the same
/// reader exchange and writer make one transmission with both
/// co-accesses, confirmed, in every delivery order.
#[test]
fn old_content_joins_a_recent_co_access_in_any_order() {
    let mut scene = Scene::new(8);
    let (a, b) = (scene.agent(), scene.agent());
    let resource = scene.resource();
    let (old_span, recent_span) = (scene.span(), scene.span());
    let read_at = secs(DAY);
    let old = scene.write(a, resource, secs(0), vec![old_span]);
    let recent = scene.write(a, resource, secs(DAY - 60), vec![recent_span]);
    let read = scene.read(b, resource, read_at);
    let content = scene.carried(&read, a, old_span);
    let inputs = [0_usize, 1, 2, 3];
    let mut expected = None;
    for order in permutations(&inputs) {
        let mut correlator = WindowedCorrelator::new(timing());
        let mut out = Vec::new();
        for index in order {
            out.extend(match index {
                0 => correlator.access(&old, None),
                1 => correlator.access(&recent, None),
                2 => correlator.access(&read, None),
                _ => correlator.content(&content),
            });
        }
        out.extend(correlator.tick(timing().window_closes_at(read_at)));
        let finals = fold(&out);
        let Ok(finals) = finals else {
            panic!("{finals:?}");
        };
        assert_eq!(finals.len(), 1, "{finals:?}");
        let only = finals
            .values()
            .next()
            .map(|state| (state.stage, state.co_access.len()));
        assert_eq!(only, Some((Stage::Confirmed, 2)), "{finals:?}");
        match &expected {
            None => expected = Some(finals),
            Some(first) => assert_eq!(first, &finals),
        }
    }
}

/// Every delivery order of a day-old dead drop gives the same states
/// (`flow.correlator.order-insensitive`).
#[test]
fn content_past_the_window_is_order_insensitive() {
    let drop = dead_drop(9, DAY);
    let inputs = [Input::Write, Input::Read, Input::Match];
    let indices = [0_usize, 1, 2];
    let mut expected = None;
    for order in permutations(&indices) {
        for tick_first in [false, true] {
            let mut correlator = WindowedCorrelator::new(timing());
            let mut out = Vec::new();
            if tick_first {
                out.extend(correlator.tick(drop.read.at));
            }
            for index in &order {
                out.extend(feed(&mut correlator, &drop, inputs[*index]));
            }
            out.extend(correlator.tick(timing().window_closes_at(drop.read.at)));
            let finals = fold(&out);
            let Ok(finals) = finals else {
                panic!("{finals:?}");
            };
            assert_eq!(finals.len(), 1, "{finals:?}");
            match &expected {
                None => expected = Some(finals),
                Some(first) => assert_eq!(first, &finals),
            }
        }
    }
}

/// The default is L4's index retention, and a retention shorter than the
/// correlation window is refused.
#[test]
fn retention_defaults_to_the_index_and_covers_the_window() {
    assert_eq!(DEFAULT_CONTENT_RETENTION, Duration::from_secs(30 * DAY));
    assert_eq!(
        ContentRetention::default_for(timing()).get(),
        DEFAULT_CONTENT_RETENTION
    );
    let window = timing().correlation_window();
    assert_eq!(
        ContentRetention::new(window - Duration::from_secs(1), timing()),
        Err(InvalidRetention::ShorterThanWindow {
            retention: window - Duration::from_secs(1),
            window,
        })
    );
    assert!(ContentRetention::new(window, timing()).is_ok());
}

fn permutations(items: &[usize]) -> Vec<Vec<usize>> {
    if items.len() <= 1 {
        return vec![items.to_vec()];
    }
    let mut all = Vec::new();
    for (index, item) in items.iter().enumerate() {
        let mut rest = items.to_vec();
        rest.remove(index);
        for mut tail in permutations(&rest) {
            tail.insert(0, *item);
            all.push(tail);
        }
    }
    all
}

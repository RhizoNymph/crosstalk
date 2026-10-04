//! Channel transmissions: opening, the evidence window, suspicion, late
//! confirmation, extension and expiry.

use std::time::Duration;

use crosstalk_spec::derived::flow::access::Access;
use crosstalk_spec::derived::provenance::matching::{Carrier, ContentMatch};
use crosstalk_spec::ids::{AgentId, ResourceId, SpanId};
use crosstalk_spec::interfaces::l5_flow::{OpensOn, TransmissionUpdate};
use crosstalk_spec::support::Timestamp;

use super::fixtures::{Scene, secs, timing};
use super::kinds;
use crate::correlate::lifecycle::UpdateKind;
use crate::correlate::{Decided, WindowedCorrelator};

/// A writes `resource` at 0s holding `span`; B reads it at 30s.
struct Pair {
    a: AgentId,
    b: AgentId,
    resource: ResourceId,
    span: SpanId,
    write: Access,
    read: Access,
}

fn pair(scene: &mut Scene) -> Pair {
    let (a, b) = (scene.agent(), scene.agent());
    let resource = scene.resource();
    let span = scene.span();
    let write = scene.write(a, resource, secs(0), vec![span]);
    let read = scene.read(b, resource, secs(30));
    Pair {
        a,
        b,
        resource,
        span,
        write,
        read,
    }
}

fn fed(pair: &Pair) -> (WindowedCorrelator, Vec<Decided>) {
    let mut correlator = WindowedCorrelator::new(timing());
    let mut out = correlator.access(&pair.write, None);
    out.extend(correlator.access(&pair.read, None));
    (correlator, out)
}

fn after(at: Timestamp, by: Duration) -> Timestamp {
    Timestamp::from_micros(at.as_micros() + u64::try_from(by.as_micros()).unwrap_or(u64::MAX))
}

/// Opens on the read; suspected exactly at `window_closes_at(read.at)`,
/// discarded exactly at `expires_at(since)`; a tool-result match without
/// an access opens `Direct(ToolResult)` exactly at `window_closes_at` of
/// its exchange's start (`flow.timing.lifecycle-times`).
#[test]
fn windows_follow_correlation_timing() {
    let mut scene = Scene::new(1);
    let pair = pair(&mut scene);
    let (mut correlator, opened) = fed(&pair);
    assert_eq!(kinds(&opened), vec![UpdateKind::OpenChannel]);
    assert_eq!(opened[0].opened_at, pair.read.at);
    let TransmissionUpdate::OpenChannel { on, to, .. } = &opened[0].update else {
        panic!("not opened: {opened:?}");
    };
    assert_eq!(*on, OpensOn::Resource(pair.resource));
    assert_eq!(*to, pair.b);

    let closes = timing().window_closes_at(pair.read.at);
    let just_before = Timestamp::from_micros(closes.as_micros() - 1);
    assert!(correlator.tick(just_before).is_empty());
    assert_eq!(kinds(&correlator.tick(closes)), vec![UpdateKind::Suspect]);
    let expires = timing().expires_at(closes);
    assert!(
        correlator
            .tick(Timestamp::from_micros(expires.as_micros() - 1))
            .is_empty()
    );
    assert_eq!(kinds(&correlator.tick(expires)), vec![UpdateKind::Discard]);

    let mut correlator = WindowedCorrelator::new(timing());
    let (c, d) = (scene.agent(), scene.agent());
    let exchange = scene.exchange();
    let span = scene.span();
    let content = scene.found(c, d, exchange, span, super::fixtures::tool_result(exchange));
    assert!(correlator.exchange(exchange, secs(100)).is_empty());
    assert!(correlator.content(&content).is_empty());
    let closes = timing().window_closes_at(secs(100));
    assert!(
        correlator
            .tick(Timestamp::from_micros(closes.as_micros() - 1))
            .is_empty()
    );
    let out = correlator.tick(closes);
    assert_eq!(kinds(&out), vec![UpdateKind::OpenConfirmed]);
    assert_eq!(out[0].opened_at, secs(100));
}

/// A match processed while the transmission is suspected confirms it
/// (`flow.transmission.late-match-confirms`).
#[test]
fn late_match_confirms_suspected() {
    let mut scene = Scene::new(2);
    let pair = pair(&mut scene);
    let (mut correlator, _) = fed(&pair);
    let closes = timing().window_closes_at(pair.read.at);
    assert_eq!(kinds(&correlator.tick(closes)), vec![UpdateKind::Suspect]);
    let content = scene.carried(&pair.read, pair.a, pair.span);
    let out = correlator.content(&content);
    assert_eq!(kinds(&out), vec![UpdateKind::Confirm]);
    let TransmissionUpdate::Confirm { confirmed, .. } = &out[0].update else {
        panic!("not confirmed: {out:?}");
    };
    assert_eq!(confirmed.from(), pair.a);
    assert_eq!(confirmed.content().first(), &content);
}

/// A match within the window confirms at the close, with every match held;
/// a later one extends (`flow.transmission.later-match-extends`).
#[test]
fn later_match_extends_confirmed() {
    let mut scene = Scene::new(3);
    let pair = pair(&mut scene);
    let (mut correlator, _) = fed(&pair);
    let first = scene.carried(&pair.read, pair.a, pair.span);
    assert!(correlator.content(&first).is_empty());
    let out = correlator.tick(timing().window_closes_at(pair.read.at));
    assert_eq!(kinds(&out), vec![UpdateKind::Confirm]);
    let later = scene.again(&first, 100);
    let out = correlator.content(&later);
    assert_eq!(kinds(&out), vec![UpdateKind::Extend]);
    let TransmissionUpdate::Extend { content, .. } = &out[0].update else {
        panic!("not extended: {out:?}");
    };
    assert_eq!(content, &later);
    // A redelivery of either changes nothing.
    assert!(correlator.content(&later).is_empty());
    assert!(correlator.content(&first).is_empty());
}

/// A tick at or after a suspicion's expiry discards it
/// (`flow.transmission.expiry-discards`), keeping nothing open.
#[test]
fn expiry_discards_suspected() {
    let mut scene = Scene::new(4);
    let pair = pair(&mut scene);
    let (mut correlator, opened) = fed(&pair);
    let id = super::subject(&opened[0].update);
    let far = after(pair.read.at, Duration::from_secs(10_000));
    let out = correlator.tick(far);
    assert_eq!(kinds(&out), vec![UpdateKind::Suspect, UpdateKind::Discard]);
    assert!(
        out.iter()
            .all(|decided| super::subject(&decided.update) == id)
    );
    assert!(
        correlator
            .tick(after(far, Duration::from_secs(1)))
            .is_empty()
    );
}

/// Repeated ticks between the close and the expiry suspect once
/// (`flow.transmission.suspect-once`).
#[test]
fn repeated_ticks_suspect_once() {
    let mut scene = Scene::new(5);
    let pair = pair(&mut scene);
    let (mut correlator, _) = fed(&pair);
    let closes = timing().window_closes_at(pair.read.at);
    let mut suspects = 0;
    for step in 0..50 {
        let out = correlator.tick(after(closes, Duration::from_secs(step)));
        suspects += kinds(&out)
            .iter()
            .filter(|kind| **kind == UpdateKind::Suspect)
            .count();
    }
    assert_eq!(suspects, 1);
}

/// Confirming a suspected transmission keeps every co-access it held
/// (`flow.transmission.upgrade-keeps-coaccess`).
#[test]
fn upgrade_keeps_coaccess() {
    let mut scene = Scene::new(6);
    let pair = pair(&mut scene);
    let second = scene.write(pair.a, pair.resource, secs(10), vec![pair.span]);
    let (mut correlator, _) = fed(&pair);
    assert!(
        correlator.access(&second, None).is_empty(),
        "joins, opens nothing"
    );
    let out = correlator.tick(timing().window_closes_at(pair.read.at));
    let TransmissionUpdate::Suspect { co_access, .. } = &out[0].update else {
        panic!("not suspected: {out:?}");
    };
    let suspected: Vec<_> = co_access.iter().copied().collect();
    assert_eq!(suspected.len(), 2);
    let content = scene.carried(&pair.read, pair.a, pair.span);
    let out = correlator.content(&content);
    let TransmissionUpdate::Confirm { confirmed, .. } = &out[0].update else {
        panic!("not confirmed: {out:?}");
    };
    assert_eq!(confirmed.co_access(), suspected.as_slice());
}

/// `Confirmed::at` is the reader exchange's start: the read's time for a
/// channel transmission, the exchange's for one opened confirmed
/// (`flow.transmission.at-is-reader-exchange`).
#[test]
fn confirmed_at_is_reader_exchange_start() {
    let mut scene = Scene::new(7);
    let pair = pair(&mut scene);
    let (mut correlator, _) = fed(&pair);
    let content = scene.carried(&pair.read, pair.a, pair.span);
    correlator.content(&content);
    // Confirmed long after the read: still at the read's time.
    let out = correlator.tick(after(pair.read.at, Duration::from_secs(61)));
    let TransmissionUpdate::Confirm { confirmed, .. } = &out[0].update else {
        panic!("not confirmed: {out:?}");
    };
    assert_eq!(confirmed.at(), pair.read.at);

    let exchange = scene.exchange();
    let span = scene.span();
    let user_turn = scene.found(pair.a, pair.b, exchange, span, Carrier::UserTurn);
    correlator.content(&user_turn);
    correlator.exchange(exchange, secs(500));
    let out = correlator.tick(secs(10_000));
    let opened: Vec<&Decided> = out
        .iter()
        .filter(|decided| matches!(decided.update, TransmissionUpdate::OpenConfirmed { .. }))
        .collect();
    let [
        Decided {
            update: TransmissionUpdate::OpenConfirmed { confirmed, .. },
            ..
        },
    ] = opened.as_slice()
    else {
        panic!("not opened: {out:?}");
    };
    assert_eq!(confirmed.at(), secs(500));
}

/// A tool-result match in B's read whose origin agent never wrote the
/// resource is a shared upstream source: the transmission from the writer
/// stays suspected, and the match confirms nothing
/// (`flow.route.shared-upstream-stays-suspected`).
#[test]
fn shared_upstream_match_keeps_transmission_suspected() {
    let mut scene = Scene::new(8);
    let pair = pair(&mut scene);
    let upstream = scene.agent();
    let quoted = scene.span();
    let (mut correlator, _) = fed(&pair);
    let content = scene.carried(&pair.read, upstream, quoted);
    let mut out = correlator.content(&content);
    out.extend(correlator.tick(timing().window_closes_at(pair.read.at)));
    out.extend(correlator.tick(secs(100_000)));
    assert_eq!(kinds(&out), vec![UpdateKind::Suspect, UpdateKind::Discard]);
}

/// A match whose span is not among the writer's written spans confirms
/// nothing either: the write did not carry it.
#[test]
fn span_outside_the_write_confirms_nothing() {
    let mut scene = Scene::new(9);
    let pair = pair(&mut scene);
    let other = scene.span();
    let (mut correlator, _) = fed(&pair);
    let content = scene.carried(&pair.read, pair.a, other);
    correlator.content(&content);
    let out = correlator.tick(timing().window_closes_at(pair.read.at));
    assert_eq!(kinds(&out), vec![UpdateKind::Suspect]);
}

/// A write and a read by one agent, a read before the write, and a read
/// outside the correlation window open nothing.
#[test]
fn only_cross_agent_reads_after_writes_open() {
    let mut scene = Scene::new(10);
    let (a, b) = (scene.agent(), scene.agent());
    let resource = scene.resource();
    let span = scene.span();
    let mut correlator = WindowedCorrelator::new(timing());
    let early = scene.read(b, resource, secs(0));
    let write = scene.write(a, resource, secs(10), vec![span]);
    let own = scene.read(a, resource, secs(20));
    let late = scene.read(b, resource, secs(10 + 601));
    for access in [&early, &write, &own, &late] {
        assert!(correlator.access(access, None).is_empty(), "{access:?}");
    }
}

/// Two writers read in one exchange are two transmissions, each confirmed
/// only by its own sender's matches (`flow.transmission.one-per-sender`).
#[test]
fn each_writer_is_its_own_transmission() {
    let mut scene = Scene::new(11);
    let (a, c, b) = (scene.agent(), scene.agent(), scene.agent());
    let resource = scene.resource();
    let (sa, sc) = (scene.span(), scene.span());
    let mut correlator = WindowedCorrelator::new(timing());
    correlator.access(&scene.write(a, resource, secs(0), vec![sa]), None);
    correlator.access(&scene.write(c, resource, secs(5), vec![sc]), None);
    let read = scene.read(b, resource, secs(30));
    let opened = correlator.access(&read, None);
    assert_eq!(
        kinds(&opened),
        vec![UpdateKind::OpenChannel, UpdateKind::OpenChannel]
    );
    let from_a: ContentMatch = scene.carried(&read, a, sa);
    let from_c: ContentMatch = scene.carried(&read, c, sc);
    correlator.content(&from_a);
    correlator.content(&from_c);
    let out = correlator.tick(timing().window_closes_at(read.at));
    let senders: Vec<AgentId> = out
        .iter()
        .filter_map(|decided| match &decided.update {
            TransmissionUpdate::Confirm { confirmed, .. } => Some(confirmed.from()),
            _ => None,
        })
        .collect();
    assert_eq!(senders.len(), 2);
    assert!(senders.contains(&a) && senders.contains(&c));
}

/// Content explained by a write after the transmission was discarded
/// opens a new transmission (a new id), confirmed at once
/// (`flow.transmission.content-after-discard-opens-new`); the discarded
/// one gets nothing more (`flow.transmission.discarded-final`).
#[test]
fn content_after_discard_opens_a_new_transmission() {
    let mut scene = Scene::new(12);
    let pair = pair(&mut scene);
    let (mut correlator, opened) = fed(&pair);
    let first = super::subject(&opened[0].update);
    // Past the expiry (60 s + 300 s after the read), before the evidence
    // is collected.
    correlator.tick(after(pair.read.at, Duration::from_secs(400)));
    let content = scene.carried(&pair.read, pair.a, pair.span);
    let out = correlator.content(&content);
    assert_eq!(
        kinds(&out),
        vec![UpdateKind::OpenChannel, UpdateKind::Confirm]
    );
    let second = super::subject(&out[0].update);
    assert_ne!(first, second);
    assert!(
        out.iter()
            .all(|decided| super::subject(&decided.update) == second)
    );
}

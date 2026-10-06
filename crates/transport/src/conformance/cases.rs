//! The conformance cases, each generic over a [`Kit`].

use std::collections::{HashMap, HashSet};
use std::num::NonZeroU32;
use std::time::Duration;

use crosstalk_spec::events::Subject;
use crosstalk_spec::interfaces::l2_transport::{
    BusError, ConsumerGroup, DeadLetter, DeadLetterStore, DeliveryId, EventBus, Subscription,
};
use crosstalk_spec::observed::agent::AgentLabel;
use crosstalk_spec::paging::{Cursor, DeadLetterList, PageRequest, PageSize};

use super::{Kit, Settings};
use crate::testing::{
    changed, default_retry, event_id, group, next_within, renamed, retry, watermark,
};

const SECOND: Duration = Duration::from_secs(1);

/// Within a generous bound, so a slow database fails loudly, not forever.
async fn next_soon<S: Subscription>(
    sub: &mut S,
) -> crosstalk_spec::interfaces::l2_transport::Delivery {
    match next_within(sub, 20 * SECOND).await {
        Some(Ok(delivery)) => delivery,
        other => panic!("expected a delivery, got {other:?}"),
    }
}

pub(super) async fn each_group_gets_every_envelope_and_members_share_them<K: Kit>(kit: &K) {
    let (bus, _) = kit.start(Settings::default()).await;
    let (flow, analysis) = (group("flow"), group("analysis"));
    let mut flow_a = bus
        .subscribe(&[Subject::Changed], flow.clone(), default_retry())
        .await
        .expect("subscribe");
    let mut flow_b = bus
        .subscribe(&[Subject::Changed], flow.clone(), default_retry())
        .await
        .expect("subscribe");
    let mut analysis_sub = bus
        .subscribe(&[Subject::Changed], analysis, default_retry())
        .await
        .expect("subscribe");
    for n in 1..=10 {
        bus.publish(changed(n)).await.expect("publish");
    }
    let mut flow_seen = Vec::new();
    for round in 0..10 {
        let sub = if round % 2 == 0 {
            &mut flow_a
        } else {
            &mut flow_b
        };
        let delivery = next_soon(sub).await;
        flow_seen.push(delivery.envelope.id.as_ulid());
        sub.ack(delivery.id).await.expect("ack");
    }
    let mut analysis_seen = Vec::new();
    for _ in 0..10 {
        let delivery = next_soon(&mut analysis_sub).await;
        analysis_seen.push(delivery.envelope.id.as_ulid());
        analysis_sub.ack(delivery.id).await.expect("ack");
    }
    let every: HashSet<u128> = (1..=10).collect();
    assert_eq!(flow_seen.iter().copied().collect::<HashSet<_>>(), every);
    assert_eq!(
        flow_seen.len(),
        10,
        "members of a group share each envelope once"
    );
    assert_eq!(analysis_seen.iter().copied().collect::<HashSet<_>>(), every);
    assert!(next_within(&mut flow_a, kit.quiet()).await.is_none());
    assert!(next_within(&mut flow_b, kit.quiet()).await.is_none());
}

pub(super) async fn a_group_keeps_envelopes_while_no_consumer_is_connected<K: Kit>(kit: &K) {
    let (bus, _) = kit.start(Settings::default()).await;
    let flow = group("flow");
    let first = bus
        .subscribe(&[Subject::Changed], flow.clone(), default_retry())
        .await
        .expect("subscribe");
    drop(first);
    bus.publish(changed(1)).await.expect("publish");
    let mut again = bus
        .subscribe(&[Subject::Changed], flow, default_retry())
        .await
        .expect("subscribe");
    assert_eq!(next_soon(&mut again).await.envelope, changed(1));
}

pub(super) async fn a_group_gets_nothing_published_before_it_subscribed<K: Kit>(kit: &K) {
    let (bus, _) = kit.start(Settings::default()).await;
    bus.publish(changed(1)).await.expect("publish");
    let mut late = bus
        .subscribe(&[Subject::Changed], group("late"), default_retry())
        .await
        .expect("subscribe");
    assert!(
        next_within(&mut late, kit.quiet()).await.is_none(),
        "no backfill"
    );
    bus.publish(changed(2)).await.expect("publish");
    assert_eq!(next_soon(&mut late).await.envelope, changed(2));
}

/// `transport.delivery.subject-filter` and
/// `transport.delivery.envelope-unchanged`: a group gets exactly its
/// subjects, each envelope equal to the published one (free text
/// included), first delivery or redelivery.
pub(super) async fn a_subscription_yields_only_its_subjects_unchanged<K: Kit>(kit: &K) {
    let (bus, _) = kit.start(Settings::default()).await;
    let mut sub = bus
        .subscribe(
            &[Subject::Changed, Subject::AgentRenamed],
            group("flow"),
            default_retry(),
        )
        .await
        .expect("subscribe");
    let label = AgentLabel::new("a \"quoted\" label / with ünïcode").expect("label");
    let wanted = vec![changed(1), renamed(3, label), changed(5)];
    for envelope in [
        changed(1),
        watermark(2),
        wanted[1].clone(),
        watermark(4),
        changed(5),
    ] {
        bus.publish(envelope).await.expect("publish");
    }
    let mut got = HashMap::new();
    let mut redelivered = false;
    while got.len() < wanted.len() {
        let delivery = next_soon(&mut sub).await;
        assert!(
            matches!(
                delivery.envelope.event.subject(),
                Subject::Changed | Subject::AgentRenamed
            ),
            "unsubscribed subject"
        );
        if delivery.attempt.get() == 1 && !redelivered {
            redelivered = true;
            sub.nack(delivery.id, Duration::ZERO, "once".into())
                .await
                .expect("nack");
            continue;
        }
        sub.ack(delivery.id).await.expect("ack");
        got.insert(delivery.envelope.id, delivery.envelope);
    }
    for envelope in &wanted {
        assert_eq!(got.get(&envelope.id), Some(envelope));
    }
    assert!(next_within(&mut sub, kit.quiet()).await.is_none());
}

/// `transport.ack.unknown-delivery`.
pub(super) async fn ack_of_unheld_delivery_is_unknown<K: Kit>(kit: &K) {
    let (bus, _) = kit.start(Settings::default()).await;
    let flow = group("flow");
    let mut a = bus
        .subscribe(&[Subject::Changed], flow.clone(), default_retry())
        .await
        .expect("subscribe");
    let mut b = bus
        .subscribe(&[Subject::Changed], flow.clone(), default_retry())
        .await
        .expect("subscribe");
    let unknown = |id| Err(BusError::UnknownDelivery(id));
    let never = DeliveryId(9_999_999);
    assert_eq!(a.ack(never).await, unknown(never));
    assert_eq!(a.nack(never, SECOND, "x".into()).await, unknown(never));

    bus.publish(changed(1)).await.expect("publish");
    bus.publish(changed(2)).await.expect("publish");
    let first = next_soon(&mut a).await;
    // Held by another subscription of the group.
    assert_eq!(b.ack(first.id).await, unknown(first.id));
    assert_eq!(
        b.nack(first.id, SECOND, "x".into()).await,
        unknown(first.id)
    );
    assert_eq!(a.ack(first.id).await, Ok(()));
    // Already acked.
    assert_eq!(a.ack(first.id).await, unknown(first.id));
    // Already nacked.
    let second = next_soon(&mut b).await;
    assert_eq!(b.nack(second.id, SECOND, "x".into()).await, Ok(()));
    assert_eq!(
        b.nack(second.id, SECOND, "x".into()).await,
        unknown(second.id)
    );
    assert_eq!(b.ack(second.id).await, unknown(second.id));
    // The nacked one comes back; the acked one does not.
    let again = next_soon(&mut a).await;
    assert_eq!((again.envelope.id, again.attempt.get()), (changed(2).id, 2));
}

/// `transport.delivery.attempt-counts-deliveries` and
/// `transport.deadletter.stored-before-release`: each nack counts an
/// attempt; the last one stores the dead letter, with the consumer's
/// reason, and the envelope is not delivered again.
pub(super) async fn nacks_count_attempts_then_dead_letter<K: Kit>(kit: &K) {
    let (bus, letters) = kit.start(Settings::default()).await;
    let (flow, analysis) = (group("flow"), group("analysis"));
    let mut flow_sub = bus
        .subscribe(&[Subject::Changed], flow.clone(), default_retry())
        .await
        .expect("subscribe");
    let mut analysis_sub = bus
        .subscribe(&[Subject::Changed], analysis.clone(), default_retry())
        .await
        .expect("subscribe");
    bus.publish(changed(1)).await.expect("publish");
    let mut attempts = Vec::new();
    for _ in 0..3 {
        let delivery = next_soon(&mut flow_sub).await;
        attempts.push(delivery.attempt.get());
        let reason = format!("failure {}", delivery.attempt);
        flow_sub
            .nack(delivery.id, Duration::ZERO, reason)
            .await
            .expect("nack");
    }
    assert_eq!(attempts, vec![1, 2, 3]);
    let page = letters
        .list(Some(&flow), &request(10, None))
        .await
        .expect("lists");
    let (items, _) = page.into_parts();
    assert_eq!(
        items,
        vec![DeadLetter {
            group: flow.clone(),
            envelope: changed(1),
            attempts: NonZeroU32::new(3).expect("non-zero"),
            last_error: "failure 3".to_owned(),
        }]
    );
    assert!(next_within(&mut flow_sub, kit.quiet()).await.is_none());
    // The other group is unaffected and counts its own attempts.
    let other = next_soon(&mut analysis_sub).await;
    assert_eq!(other.attempt.get(), 1);
}

/// `transport.delivery.redelivered-until-acked` on a timeout: a delivery
/// not settled within the ack timeout comes back with its attempt counted,
/// and the late ack is refused.
pub(super) async fn an_ack_timeout_redelivers_and_refuses_the_late_ack<K: Kit>(kit: &K) {
    let ack_timeout = kit.ack_timeout();
    let settings = Settings {
        ack_timeout,
        ..Settings::default()
    };
    let (bus, _) = kit.start(settings).await;
    let mut sub = bus
        .subscribe(&[Subject::Changed], group("flow"), default_retry())
        .await
        .expect("subscribe");
    bus.publish(changed(1)).await.expect("publish");
    let first = next_soon(&mut sub).await;
    assert_eq!(first.attempt.get(), 1);
    tokio::time::sleep(ack_timeout + Duration::from_millis(100)).await;
    assert_eq!(
        sub.ack(first.id).await,
        Err(BusError::UnknownDelivery(first.id))
    );
    let second = next_soon(&mut sub).await;
    assert_eq!((second.envelope, second.attempt.get()), (changed(1), 2));
    sub.ack(second.id).await.expect("ack");
    assert!(next_within(&mut sub, kit.quiet()).await.is_none());
}

/// A consumer crash: the subscription holding a delivery is dropped, and
/// the group gets it again with the attempt counted.
pub(super) async fn a_dropped_holder_is_redelivered<K: Kit>(kit: &K) {
    let (bus, _) = kit.start(Settings::default()).await;
    let flow = group("flow");
    let mut holder = bus
        .subscribe(&[Subject::Changed], flow.clone(), default_retry())
        .await
        .expect("subscribe");
    let mut other = bus
        .subscribe(&[Subject::Changed], flow.clone(), default_retry())
        .await
        .expect("subscribe");
    bus.publish(changed(1)).await.expect("publish");
    let held = next_soon(&mut holder).await;
    assert_eq!(held.attempt.get(), 1);
    drop(holder);
    let again = next_soon(&mut other).await;
    assert_eq!((again.envelope, again.attempt.get()), (changed(1), 2));
}

/// `transport.deadletter.replay-consumes` and
/// `transport.deadletter.replay-unknown`: a replay goes to the letter's
/// group alone, at attempt 1, and removes the letter; replaying it again,
/// or one never stored, or one of another group, is unknown; a group that
/// never subscribed is refused and the letter stays.
pub(super) async fn replay_delivers_to_its_group_alone_at_attempt_one<K: Kit>(kit: &K) {
    let (bus, letters) = kit.start(Settings::default()).await;
    let (flow, analysis) = (group("flow"), group("analysis"));
    let mut flow_sub = bus
        .subscribe(&[Subject::Changed], flow.clone(), default_retry())
        .await
        .expect("subscribe");
    let mut analysis_sub = bus
        .subscribe(&[Subject::Changed], analysis.clone(), default_retry())
        .await
        .expect("subscribe");
    let unknown = |g: &ConsumerGroup, n| {
        Err(BusError::UnknownDeadLetter {
            group: g.clone(),
            id: event_id(n),
        })
    };
    assert_eq!(letters.replay(&flow, event_id(1)).await, unknown(&flow, 1));

    // A letter from real exhaustion.
    bus.publish(changed(1)).await.expect("publish");
    for _ in 0..3 {
        let delivery = next_soon(&mut flow_sub).await;
        flow_sub
            .nack(delivery.id, Duration::ZERO, "fails".into())
            .await
            .expect("nack");
    }
    let analysis_first = next_soon(&mut analysis_sub).await;
    analysis_sub.ack(analysis_first.id).await.expect("ack");
    wait_for_letter(&letters, &flow, 1).await;

    assert_eq!(
        letters.replay(&analysis, event_id(1)).await,
        unknown(&analysis, 1)
    );
    let nobody = group("nobody");
    letters
        .put(DeadLetter {
            group: nobody.clone(),
            envelope: changed(7),
            attempts: NonZeroU32::new(2).expect("non-zero"),
            last_error: "x".to_owned(),
        })
        .await
        .expect("put");
    assert!(matches!(
        letters.replay(&nobody, event_id(7)).await,
        Err(BusError::PublishRejected { .. })
    ));

    assert_eq!(letters.replay(&flow, event_id(1)).await, Ok(()));
    assert_eq!(letters.replay(&flow, event_id(1)).await, unknown(&flow, 1));
    let replayed = next_soon(&mut flow_sub).await;
    assert_eq!((replayed.envelope, replayed.attempt.get()), (changed(1), 1));
    flow_sub.ack(replayed.id).await.expect("ack");
    assert!(next_within(&mut flow_sub, kit.quiet()).await.is_none());
    assert!(
        next_within(&mut analysis_sub, kit.quiet()).await.is_none(),
        "the other group gets nothing"
    );
    let left = letters.list(None, &request(10, None)).await.expect("lists");
    let groups: Vec<String> = left.items().iter().map(|l| l.group.0.clone()).collect();
    assert_eq!(
        groups,
        vec!["nobody".to_owned()],
        "the refused letter stays"
    );
}

async fn wait_for_letter<L: DeadLetterStore>(letters: &L, group: &ConsumerGroup, n: u128) {
    for _ in 0..200 {
        let page = letters
            .list(Some(group), &request(10, None))
            .await
            .expect("lists");
        if page.items().iter().any(|l| l.envelope.id == event_id(n)) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("no dead letter for {n}");
}

/// `transport.subscribe.group-subject-mismatch` and
/// `transport.subscribe.group-retry-mismatch`: subject sets compare as
/// sets; another set or policy is refused.
pub(super) async fn subscribe_rejects_another_subject_set_or_policy<K: Kit>(kit: &K) {
    let (bus, _) = kit.start(Settings::default()).await;
    let flow = group("flow");
    let _first = bus
        .subscribe(
            &[Subject::Changed, Subject::WatermarkAdvanced],
            flow.clone(),
            default_retry(),
        )
        .await
        .expect("subscribe");
    let refused_sets: [&[Subject]; 3] = [
        &[Subject::Changed],
        &[
            Subject::Changed,
            Subject::WatermarkAdvanced,
            Subject::AgentRenamed,
        ],
        &[Subject::AgentRenamed, Subject::WatermarkAdvanced],
    ];
    for subjects in refused_sets {
        assert_eq!(
            bus.subscribe(subjects, flow.clone(), default_retry())
                .await
                .err(),
            Some(BusError::GroupSubjectMismatch {
                group: flow.clone()
            })
        );
    }
    for policy in [
        retry(4, Duration::from_millis(10), Duration::from_millis(80)),
        retry(3, Duration::from_millis(20), Duration::from_millis(80)),
        retry(3, Duration::from_millis(10), Duration::from_millis(90)),
    ] {
        assert_eq!(
            bus.subscribe(
                &[Subject::Changed, Subject::WatermarkAdvanced],
                flow.clone(),
                policy
            )
            .await
            .err(),
            Some(BusError::GroupRetryMismatch {
                group: flow.clone()
            })
        );
    }
    let mut same = bus
        .subscribe(
            &[
                Subject::WatermarkAdvanced,
                Subject::Changed,
                Subject::Changed,
            ],
            flow,
            default_retry(),
        )
        .await
        .expect("the same set in another order joins");
    bus.publish(watermark(1)).await.expect("publish");
    assert_eq!(next_soon(&mut same).await.envelope, watermark(1));
}

fn request(size: u16, after: Option<Cursor<DeadLetterList>>) -> PageRequest<DeadLetterList> {
    PageRequest {
        size: PageSize::new(size).expect("valid size"),
        after,
    }
}

fn letter(group: &ConsumerGroup, n: u128) -> DeadLetter {
    DeadLetter {
        group: group.clone(),
        envelope: changed(n),
        attempts: NonZeroU32::new(3).expect("non-zero"),
        last_error: format!("failure {n}"),
    }
}

async fn traverse<L: DeadLetterStore>(
    letters: &L,
    group: Option<&ConsumerGroup>,
    size: u16,
) -> Vec<(u128, String)> {
    let mut all = Vec::new();
    let mut after = None;
    loop {
        let page = letters
            .list(group, &request(size, after))
            .await
            .expect("lists");
        all.extend(
            page.items()
                .iter()
                .map(|l| (l.envelope.id.as_ulid(), l.group.0.clone())),
        );
        match page.next() {
            Some(next) => after = Some(next.clone()),
            None => return all,
        }
    }
}

/// `DeadLetterStore::list`: newest envelope first, ties by group, one group
/// or all, through cursors; a second letter for one envelope and group
/// replaces the first; a cursor for another filter, or tampered with, is
/// `InvalidCursor`.
pub(super) async fn dead_letters_list_newest_first_with_cursors<K: Kit>(kit: &K) {
    let (_bus, letters) = kit.start(Settings::default()).await;
    let (a, b) = (group("a-group"), group("b-group"));
    for n in [3, 1, 2] {
        letters.put(letter(&a, n)).await.expect("put");
    }
    for n in [2, 4] {
        letters.put(letter(&b, n)).await.expect("put");
    }
    let mut again = letter(&a, 1);
    again.last_error = "again".to_owned();
    letters.put(again.clone()).await.expect("put");

    assert_eq!(
        traverse(&letters, None, 2).await,
        vec![
            (4, "b-group".into()),
            (3, "a-group".into()),
            (2, "b-group".into()),
            (2, "a-group".into()),
            (1, "a-group".into()),
        ]
    );
    assert_eq!(
        traverse(&letters, Some(&a), 1).await,
        vec![
            (3, "a-group".into()),
            (2, "a-group".into()),
            (1, "a-group".into())
        ]
    );
    assert!(
        traverse(&letters, Some(&group("nobody")), 5)
            .await
            .is_empty()
    );
    let replaced = letters
        .list(Some(&a), &request(10, None))
        .await
        .expect("lists");
    assert_eq!(replaced.items().last(), Some(&again));

    let all_cursor = letters
        .list(None, &request(1, None))
        .await
        .expect("lists")
        .next()
        .cloned()
        .expect("more");
    let a_cursor = letters
        .list(Some(&a), &request(1, None))
        .await
        .expect("lists")
        .next()
        .cloned()
        .expect("more");
    for (filter, cursor) in [
        (Some(&b), a_cursor.clone()),
        (Some(&a), all_cursor.clone()),
        (None, a_cursor.clone()),
    ] {
        assert_eq!(
            letters.list(filter, &request(1, Some(cursor))).await,
            Err(BusError::InvalidCursor)
        );
    }
    let token = a_cursor.token();
    let flipped = format!(
        "{}{}",
        if token.starts_with('0') { "1" } else { "0" },
        &token[1..]
    );
    for forged in [flipped, "A".repeat(token.len()), "short".to_owned()] {
        let cursor = Cursor::from_token(forged).expect("token text");
        assert_eq!(
            letters.list(Some(&a), &request(1, Some(cursor))).await,
            Err(BusError::InvalidCursor)
        );
    }
}

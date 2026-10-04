//! Unit and property tests. The functions named by transport invariants'
//! `unit` and `property` evidence live here, at
//! `crosstalk_transport::tests::<name>`; the rest are in submodules.

mod bus;
mod config;
mod letters;

use std::collections::{HashMap, HashSet};
use std::num::NonZeroU32;
use std::time::Duration;

use crosstalk_spec::events::{Envelope, Subject};
use crosstalk_spec::interfaces::l2_transport::{
    BusError, DeadLetter, DeadLetterStore, DeliveryId, EventBus, Subscription,
};
use proptest::prelude::*;

use crate::MpscBus;
use crate::testing::{
    GENERATED_SUBJECTS, arb_envelopes, changed, config, default_retry, event_id, group, next_ok,
    next_within, paused_runtime, renamed, retry, watermark,
};

const SECOND: Duration = Duration::from_secs(1);

/// `transport.ack.unknown-delivery`: ids never issued, already acked,
/// already nacked, or held by another subscription of the group are
/// unknown to ack and nack, and the attempt changes nothing.
#[tokio::test(start_paused = true)]
async fn ack_of_unheld_delivery_is_unknown() {
    let bus = MpscBus::start(config()).expect("bus starts");
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

    // Never issued.
    let never = DeliveryId(9_999);
    assert_eq!(a.ack(never).await, unknown(never));
    assert_eq!(a.nack(never, SECOND, "x".into()).await, unknown(never));

    bus.publish(changed(1)).await.expect("publish");
    bus.publish(changed(2)).await.expect("publish");
    let first = next_ok(&mut a).await;

    // Held by another subscription of the group: refused, and `a` still
    // holds it.
    assert_eq!(b.ack(first.id).await, unknown(first.id));
    assert_eq!(
        b.nack(first.id, SECOND, "x".into()).await,
        unknown(first.id)
    );
    let depth = bus.depth(&flow).await.expect("depth").expect("group");
    assert_eq!((depth.held, depth.ready), (1, 1));
    assert_eq!(a.ack(first.id).await, Ok(()));

    // Already acked.
    assert_eq!(a.ack(first.id).await, unknown(first.id));
    assert_eq!(
        a.nack(first.id, SECOND, "x".into()).await,
        unknown(first.id)
    );

    // Already nacked: the delivery stays scheduled for redelivery.
    let second = next_ok(&mut b).await;
    assert_eq!(b.nack(second.id, SECOND, "x".into()).await, Ok(()));
    assert_eq!(
        b.nack(second.id, SECOND, "x".into()).await,
        unknown(second.id)
    );
    assert_eq!(b.ack(second.id).await, unknown(second.id));
    let depth = bus.depth(&flow).await.expect("depth").expect("group");
    assert_eq!((depth.delayed, depth.held, depth.tracked()), (1, 0, 1));
}

/// `transport.deadletter.replay-unknown`: replaying a letter never stored,
/// stored for another group, or already replayed is `UnknownDeadLetter`,
/// and delivers nothing.
#[tokio::test(start_paused = true)]
async fn replay_of_unknown_dead_letter_is_rejected() {
    let bus = MpscBus::start(config()).expect("bus starts");
    let letters = bus.dead_letters();
    let flow = group("flow");
    let analysis = group("analysis");
    let mut flow_sub = bus
        .subscribe(&[Subject::Changed], flow.clone(), default_retry())
        .await
        .expect("subscribe");
    let mut analysis_sub = bus
        .subscribe(&[Subject::Changed], analysis.clone(), default_retry())
        .await
        .expect("subscribe");
    let unknown = |group: &crosstalk_spec::interfaces::l2_transport::ConsumerGroup, n| {
        Err(BusError::UnknownDeadLetter {
            group: group.clone(),
            id: event_id(n),
        })
    };

    // Never stored.
    assert_eq!(letters.replay(&flow, event_id(1)).await, unknown(&flow, 1));

    let letter = DeadLetter {
        group: flow.clone(),
        envelope: changed(1),
        attempts: NonZeroU32::new(3).expect("non-zero"),
        last_error: "constraint violated".to_owned(),
    };
    letters.put(letter).await.expect("put");

    // Stored for another group.
    assert_eq!(
        letters.replay(&analysis, event_id(1)).await,
        unknown(&analysis, 1)
    );
    assert!(next_within(&mut analysis_sub, SECOND).await.is_none());

    // Replayed once, then unknown.
    assert_eq!(letters.replay(&flow, event_id(1)).await, Ok(()));
    assert_eq!(letters.replay(&flow, event_id(1)).await, unknown(&flow, 1));

    // Exactly the one replay is delivered.
    let delivery = next_ok(&mut flow_sub).await;
    assert_eq!(delivery.envelope, changed(1));
    flow_sub.ack(delivery.id).await.expect("ack");
    assert!(next_within(&mut flow_sub, 10 * SECOND).await.is_none());
}

/// `transport.subscribe.group-retry-mismatch`.
#[tokio::test(start_paused = true)]
async fn subscribe_rejects_different_retry_policy() {
    let bus = MpscBus::start(config()).expect("bus starts");
    let flow = group("flow");
    let _first = bus
        .subscribe(&[Subject::Changed], flow.clone(), default_retry())
        .await
        .expect("subscribe");
    let other_policies = [
        retry(4, Duration::from_millis(10), Duration::from_millis(80)),
        retry(3, Duration::from_millis(20), Duration::from_millis(80)),
        retry(3, Duration::from_millis(10), Duration::from_millis(90)),
    ];
    for policy in other_policies {
        let refused = bus
            .subscribe(&[Subject::Changed], flow.clone(), policy)
            .await;
        assert_eq!(
            refused.err(),
            Some(BusError::GroupRetryMismatch {
                group: flow.clone()
            })
        );
    }
    let depth = bus.depth(&flow).await.expect("depth").expect("group");
    assert_eq!(
        depth.subscriptions, 1,
        "a refused subscribe creates nothing"
    );

    // The same policy joins.
    let _second = bus
        .subscribe(&[Subject::Changed], flow.clone(), default_retry())
        .await
        .expect("same policy joins");
    let depth = bus.depth(&flow).await.expect("depth").expect("group");
    assert_eq!(depth.subscriptions, 2);
}

/// `transport.subscribe.group-subject-mismatch`: compared as a set, so order
/// and duplicates do not matter; a subset or a superset is refused.
#[tokio::test(start_paused = true)]
async fn subscribe_rejects_different_subject_set() {
    let bus = MpscBus::start(config()).expect("bus starts");
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
        let refused = bus.subscribe(subjects, flow.clone(), default_retry()).await;
        assert_eq!(
            refused.err(),
            Some(BusError::GroupSubjectMismatch {
                group: flow.clone()
            })
        );
    }
    let depth = bus.depth(&flow).await.expect("depth").expect("group");
    assert_eq!(
        depth.subscriptions, 1,
        "a refused subscribe creates nothing"
    );

    let _same = bus
        .subscribe(
            &[
                Subject::WatermarkAdvanced,
                Subject::Changed,
                Subject::Changed,
            ],
            flow.clone(),
            default_retry(),
        )
        .await
        .expect("the same set in another order joins");
    let depth = bus.depth(&flow).await.expect("depth").expect("group");
    assert_eq!(depth.subscriptions, 2);

    // A watermark still reaches the group, and only once.
    bus.publish(watermark(1)).await.expect("publish");
    assert_eq!(
        bus.depth(&flow).await.expect("depth").map(|d| d.ready),
        Some(1)
    );
}

/// Publish every envelope to one group subscribed to `subjects`, then take
/// deliveries until `expected` envelopes are acked, nacking each envelope's
/// first delivery when `nack_first` holds for its index. Returns every
/// delivered envelope with its attempt.
async fn publish_and_drain(
    envelopes: &[Envelope],
    subjects: &[Subject],
    expected: usize,
    nack_first: impl Fn(usize) -> bool,
) -> Vec<(Envelope, u32)> {
    let bus = MpscBus::start(config()).expect("bus starts");
    let flow = group("flow");
    let mut sub = bus
        .subscribe(subjects, flow.clone(), default_retry())
        .await
        .expect("subscribe");
    for envelope in envelopes {
        bus.publish(envelope.clone()).await.expect("publish");
    }
    let index: HashMap<_, _> = envelopes
        .iter()
        .enumerate()
        .map(|(i, e)| (e.id, i))
        .collect();
    let mut delivered = Vec::new();
    let mut acked = HashSet::new();
    while acked.len() < expected {
        let delivery = next_ok(&mut sub).await;
        let i = index[&delivery.envelope.id];
        delivered.push((delivery.envelope.clone(), delivery.attempt.get()));
        if delivery.attempt.get() == 1 && nack_first(i) {
            sub.nack(delivery.id, Duration::ZERO, "first try".into())
                .await
                .expect("nack");
        } else {
            sub.ack(delivery.id).await.expect("ack");
            acked.insert(delivery.envelope.id);
        }
    }
    // Nothing else arrives.
    assert!(next_within(&mut sub, 10 * SECOND).await.is_none());
    delivered
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(48))]

    /// `transport.delivery.envelope-unchanged`: every delivery, first or
    /// redelivered after a nack, carries an envelope equal to the published
    /// one, through the bus codec's encode and strict decode.
    #[test]
    fn delivered_envelope_equals_published(envelopes in arb_envelopes(16)) {
        let delivered = paused_runtime().block_on(publish_and_drain(
            &envelopes,
            &GENERATED_SUBJECTS,
            envelopes.len(),
            |i| i % 2 == 1,
        ));
        let published: HashMap<_, _> = envelopes.iter().map(|e| (e.id, e)).collect();
        for (envelope, attempt) in &delivered {
            prop_assert_eq!(Some(&envelope), published.get(&envelope.id));
            prop_assert!(*attempt <= 2);
        }
        let redelivered = delivered.iter().filter(|(_, attempt)| *attempt == 2).count();
        prop_assert_eq!(redelivered, envelopes.len() / 2);
    }

    /// `transport.delivery.subject-filter`: a subscription yields exactly the
    /// envelopes whose event's subject it subscribed to.
    #[test]
    fn subscription_yields_only_subscribed_subjects(
        envelopes in arb_envelopes(24),
        subjects in proptest::sample::subsequence(GENERATED_SUBJECTS.to_vec(), 1..=GENERATED_SUBJECTS.len()),
    ) {
        let wanted: HashSet<Subject> = subjects.iter().copied().collect();
        let expected = envelopes
            .iter()
            .filter(|e| wanted.contains(&e.event.subject()))
            .count();
        let delivered = paused_runtime().block_on(publish_and_drain(
            &envelopes,
            &subjects,
            expected,
            |_| false,
        ));
        prop_assert_eq!(delivered.len(), expected);
        for (envelope, _) in &delivered {
            prop_assert!(wanted.contains(&envelope.event.subject()));
        }
    }

    /// `transport.confidentiality.no-payload-in-logs`: a decode error's
    /// reason, and the error value's debug form, never contain the
    /// payload's text, wherever in the payload it is.
    #[test]
    fn error_values_omit_payload_bytes(secret in "zq[a-z]{10,30}") {
        let id = event_id(7).ulid_text();
        let at = "2026-10-04T12:34:56.789012Z";
        let payloads: Vec<(Subject, Vec<u8>)> = vec![
            // An unknown layer.
            (Subject::Changed, format!(
                r#"{{"id":"{id}","at":"{at}","event":{{"type":"{secret}","data":{{}}}}}}"#
            ).into_bytes()),
            // An unknown event of a known layer.
            (Subject::Changed, format!(
                r#"{{"id":"{id}","at":"{at}","event":{{"type":"insight","data":{{"type":"{secret}"}}}}}}"#
            ).into_bytes()),
            // An unknown field.
            (Subject::Changed, format!(
                r#"{{"id":"{id}","at":"{at}","event":{{"type":"changed","data":{{"type":"agent","data":"{id}"}}}},"{secret}":1}}"#
            ).into_bytes()),
            // A malformed id and a malformed timestamp.
            (Subject::Changed, format!(
                r#"{{"id":"{secret}","at":"{at}","event":{{"type":"changed","data":{{"type":"agent","data":"{id}"}}}}}}"#
            ).into_bytes()),
            (Subject::Changed, format!(
                r#"{{"id":"{id}","at":"{secret}","event":{{"type":"changed","data":{{"type":"agent","data":"{id}"}}}}}}"#
            ).into_bytes()),
            // A string where a number belongs.
            (Subject::TopicVersionDropped, format!(
                r#"{{"id":"{id}","at":"{at}","event":{{"type":"insight","data":{{"type":"topic_version_dropped","data":{{"version":"{secret}"}}}}}}}}"#
            ).into_bytes()),
            // Not JSON at all.
            (Subject::Changed, secret.clone().into_bytes()),
            // A valid envelope carrying the text, routed under another
            // subject.
            (Subject::Changed, serde_json::to_vec(&renamed(
                7,
                crosstalk_spec::observed::agent::AgentLabel::new(&secret).expect("label"),
            )).expect("encodes")),
        ];
        let errors = paused_runtime().block_on(async {
            let bus = MpscBus::start(config()).expect("bus starts");
            let flow = group("flow");
            let all = [Subject::Changed, Subject::TopicVersionDropped];
            let mut sub = bus.subscribe(&all, flow, default_retry()).await.expect("subscribe");
            let mut errors = Vec::new();
            for (subject, bytes) in payloads {
                bus.publish_encoded(subject, bytes).await.expect("publish");
                match sub.next().await {
                    Some(Err(error)) => errors.push(error),
                    other => panic!("expected a decode error, got {other:?}"),
                }
            }
            errors
        });
        for error in &errors {
            let BusError::Decode { reason } = error else {
                return Err(TestCaseError::fail(format!("not a decode error: {error:?}")));
            };
            prop_assert!(!reason.contains(&secret), "reason quotes the payload: {}", reason);
            let debug = format!("{error:?}");
            prop_assert!(!debug.contains(&secret), "debug form quotes the payload");
        }
    }
}

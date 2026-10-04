//! `DeadLetters::list`: order, group filter, cursors.

use std::num::NonZeroU32;

use crosstalk_spec::events::Subject;
use crosstalk_spec::interfaces::l2_transport::{
    BusError, ConsumerGroup, DeadLetter, DeadLetterStore, EventBus,
};
use crosstalk_spec::paging::{Cursor, DeadLetterList, PageRequest, PageSize};

use crate::MpscBus;
use crate::testing::{changed, config, default_retry, event_id, group};

fn letter(group: &ConsumerGroup, n: u128) -> DeadLetter {
    DeadLetter {
        group: group.clone(),
        envelope: changed(n),
        attempts: NonZeroU32::new(3).expect("non-zero"),
        last_error: format!("failure {n}"),
    }
}

fn request(size: u16, after: Option<Cursor<DeadLetterList>>) -> PageRequest<DeadLetterList> {
    PageRequest {
        size: PageSize::new(size).expect("valid size"),
        after,
    }
}

fn keys(letters: &[DeadLetter]) -> Vec<(u128, String)> {
    letters
        .iter()
        .map(|l| (l.envelope.id.as_ulid(), l.group.0.clone()))
        .collect()
}

/// Every letter of `group` (or of all), following cursors `size` at a time.
async fn traverse(
    store: &impl DeadLetterStore,
    group: Option<&ConsumerGroup>,
    size: u16,
) -> Vec<DeadLetter> {
    let mut all = Vec::new();
    let mut after = None;
    loop {
        let page = store
            .list(group, &request(size, after))
            .await
            .expect("lists");
        all.extend(page.items().iter().cloned());
        match page.next() {
            Some(next) => after = Some(next.clone()),
            None => return all,
        }
    }
}

#[tokio::test(start_paused = true)]
async fn letters_list_newest_envelope_first_by_group() {
    let bus = MpscBus::start(config()).expect("bus starts");
    let store = bus.dead_letters();
    let (a, b) = (group("a-group"), group("b-group"));
    for n in [3, 1, 2] {
        store.put(letter(&a, n)).await.expect("put");
    }
    for n in [2, 4] {
        store.put(letter(&b, n)).await.expect("put");
    }

    let every = traverse(&store, None, 2).await;
    assert_eq!(
        keys(&every),
        vec![
            (4, "b-group".into()),
            (3, "a-group".into()),
            (2, "b-group".into()),
            (2, "a-group".into()),
            (1, "a-group".into()),
        ]
    );
    let only_a = traverse(&store, Some(&a), 1).await;
    assert_eq!(
        keys(&only_a),
        vec![
            (3, "a-group".into()),
            (2, "a-group".into()),
            (1, "a-group".into())
        ]
    );
    let none = traverse(&store, Some(&group("nobody")), 5).await;
    assert!(none.is_empty());
}

#[tokio::test(start_paused = true)]
async fn a_second_letter_for_one_envelope_and_group_replaces_the_first() {
    let bus = MpscBus::start(config()).expect("bus starts");
    let store = bus.dead_letters();
    let a = group("a");
    store.put(letter(&a, 1)).await.expect("put");
    let mut newer = letter(&a, 1);
    newer.last_error = "again".into();
    store.put(newer.clone()).await.expect("put");
    assert_eq!(traverse(&store, None, 10).await, vec![newer]);
}

#[tokio::test(start_paused = true)]
async fn a_replay_during_a_traversal_skips_nothing_else() {
    let bus = MpscBus::start(config()).expect("bus starts");
    let store = bus.dead_letters();
    let a = group("a");
    let _sub = bus
        .subscribe(&[Subject::Changed], a.clone(), default_retry())
        .await
        .expect("subscribe");
    for n in 1..=5 {
        store.put(letter(&a, n)).await.expect("put");
    }
    let first = store
        .list(Some(&a), &request(2, None))
        .await
        .expect("lists");
    assert_eq!(keys(first.items()), vec![(5, "a".into()), (4, "a".into())]);
    // Replay one on this page and one on the next.
    store.replay(&a, event_id(4)).await.expect("replays");
    store.replay(&a, event_id(2)).await.expect("replays");
    let rest = store
        .list(Some(&a), &request(10, first.next().cloned()))
        .await
        .expect("lists");
    assert_eq!(keys(rest.items()), vec![(3, "a".into()), (1, "a".into())]);
    assert!(rest.next().is_none());
}

#[tokio::test(start_paused = true)]
async fn foreign_and_misused_cursors_are_invalid() {
    let bus = MpscBus::start(config()).expect("bus starts");
    let store = bus.dead_letters();
    let (a, b) = (group("a"), group("b"));
    for n in 1..=3 {
        store.put(letter(&a, n)).await.expect("put");
        store.put(letter(&b, n)).await.expect("put");
    }
    let all_page = store.list(None, &request(1, None)).await.expect("lists");
    let a_page = store
        .list(Some(&a), &request(1, None))
        .await
        .expect("lists");
    let all_cursor = all_page.next().cloned().expect("more");
    let a_cursor = a_page.next().cloned().expect("more");

    // Issued for another filter.
    for (filter, cursor) in [
        (Some(&b), a_cursor.clone()),
        (Some(&a), all_cursor.clone()),
        (None, a_cursor.clone()),
    ] {
        assert_eq!(
            store.list(filter, &request(1, Some(cursor))).await,
            Err(BusError::InvalidCursor)
        );
    }

    // Tampered with, or never issued.
    let token = a_cursor.token();
    let flipped = format!(
        "{}{}",
        if token.starts_with('0') { "1" } else { "0" },
        &token[1..]
    );
    for forged in [flipped, "A".repeat(token.len()), "short".to_owned()] {
        let cursor = Cursor::from_token(forged).expect("token text");
        assert_eq!(
            store.list(Some(&a), &request(1, Some(cursor))).await,
            Err(BusError::InvalidCursor)
        );
    }

    // Another bus's cursor.
    let other = MpscBus::start(config()).expect("bus starts");
    let other_store = other.dead_letters();
    for n in 1..=3 {
        other_store.put(letter(&a, n)).await.expect("put");
    }
    assert_eq!(
        other_store
            .list(Some(&a), &request(1, Some(a_cursor.clone())))
            .await,
        Err(BusError::InvalidCursor)
    );

    // The right cursor works.
    let next = store
        .list(Some(&a), &request(5, Some(a_cursor)))
        .await
        .expect("lists");
    assert_eq!(keys(next.items()), vec![(2, "a".into()), (1, "a".into())]);
}

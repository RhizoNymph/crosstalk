//! `ProvenanceStore::started_at`: the start the L4 stage stamps a delta's
//! extracted accesses with, read from the records rather than kept in the
//! stage's memory. Each scenario runs on any store; the Postgres store runs
//! the same one (`integration::restart`).

use crosstalk_spec::support::Timestamp;
use crosstalk_testkit::ids::Ids;

use crate::store::{ExchangeRecord, ProvenanceStore};

fn at(seconds: u64) -> Timestamp {
    Timestamp::from_micros(seconds * 1_000_000)
}

/// A recorded exchange's start is read back, an unknown one is `None`, a
/// second record of the same exchange keeps the first start, and pruning
/// the request lists keeps every start.
pub(crate) async fn started_at_is_the_recorded_start<S: ProvenanceStore>(store: &mut S) {
    let mut ids = Ids::seeded(11);
    let (first, second, unknown) = (ids.exchange(), ids.exchange(), ids.exchange());
    let message = ids.message();
    for (exchange, seconds) in [(first, 10), (second, 20)] {
        store
            .record_exchange(ExchangeRecord {
                id: exchange,
                started_at: at(seconds),
                request: vec![message],
                output: None,
            })
            .await
            .expect("recorded");
    }
    store
        .record_exchange(ExchangeRecord {
            id: first,
            started_at: at(99),
            request: Vec::new(),
            output: None,
        })
        .await
        .expect("recorded again");
    assert_eq!(store.started_at(first).await.expect("read"), Some(at(10)));
    assert_eq!(store.started_at(second).await.expect("read"), Some(at(20)));
    assert_eq!(store.started_at(unknown).await.expect("read"), None);

    store.prune(at(30)).await.expect("pruned");
    let (record, _) = store
        .exchange(first)
        .await
        .expect("read")
        .expect("recorded");
    assert!(record.request.is_empty(), "the request list was pruned");
    assert_eq!(store.started_at(first).await.expect("read"), Some(at(10)));
    assert_eq!(store.started_at(second).await.expect("read"), Some(at(20)));
}

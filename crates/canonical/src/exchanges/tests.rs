//! `canonical.exchange.store-read` on both exchange stores: `put` keeps
//! the first record, `exchanges` reads a batch back, `list` traverses a
//! window newest first, each exchange once, with cursors bound to it.

use std::collections::BTreeMap;

use crosstalk_spec::batch::IdBatch;
use crosstalk_spec::ids::ExchangeId;
use crosstalk_spec::interfaces::l1_canonical::NormalizeWarning;
use crosstalk_spec::interfaces::l1_canonical::exchanges::{
    ExchangeQuery, ExchangeReads, ExchangeStore, ExchangeStoreError, StoredExchange,
};
use crosstalk_spec::paging::{PageRequest, PageSize};
use crosstalk_spec::support::{TimeWindow, Timestamp};
use crosstalk_store::{Layer, Migrations, TestDb};
use crosstalk_testkit::build::ExchangeBuilder;
use crosstalk_testkit::ids::Ids;

use super::{MIGRATIONS, MemoryExchanges, PgExchanges};

fn at(seconds: u64) -> Timestamp {
    Timestamp::from_micros(seconds * 1_000_000)
}

fn exchange(ids: &mut Ids, started: u64) -> StoredExchange {
    let response = ids.message();
    let request = vec![ids.message()];
    StoredExchange {
        exchange: ExchangeBuilder::new(ids)
            .started_at(at(started))
            .request(request)
            .response(response)
            .build(),
        warnings: vec![NormalizeWarning::UnknownBlock {
            kind: "container_upload".into(),
        }],
    }
}

/// Every exchange `query` admits, page by page of `size`.
async fn traverse<S: ExchangeReads>(
    store: &S,
    query: &ExchangeQuery,
    size: u16,
) -> Vec<ExchangeId> {
    let size = PageSize::new(size).unwrap_or_else(|error| panic!("{error:?}"));
    let mut request = PageRequest { size, after: None };
    let mut seen = Vec::new();
    loop {
        let page = store
            .list(query, &request)
            .await
            .unwrap_or_else(|error| panic!("list: {error:?}"));
        let (items, next) = page.into_parts();
        seen.extend(items.iter().map(StoredExchange::id));
        match next {
            Some(cursor) => request.after = Some(cursor),
            None => return seen,
        }
    }
}

/// Puts seven exchanges (two at one instant), one twice with another
/// record, and checks every read.
async fn scenario<S: ExchangeStore + ExchangeReads>(store: &mut S) {
    let mut ids = Ids::new();
    let exchanges: Vec<StoredExchange> = [10, 20, 20, 30, 40, 50, 60]
        .into_iter()
        .map(|started| exchange(&mut ids, started))
        .collect();
    for stored in &exchanges {
        store
            .put(stored.clone())
            .await
            .unwrap_or_else(|error| panic!("put: {error:?}"));
    }
    let mut changed = exchanges[3].clone();
    changed.warnings.clear();
    store
        .put(changed)
        .await
        .unwrap_or_else(|error| panic!("put again: {error:?}"));
    let unknown = ids.exchange();
    let batch = IdBatch::new(exchanges.iter().map(StoredExchange::id).chain([unknown]))
        .unwrap_or_else(|error| panic!("{error:?}"));
    let read = store
        .exchanges(&batch)
        .await
        .unwrap_or_else(|error| panic!("read: {error:?}"));
    let want: BTreeMap<ExchangeId, StoredExchange> = exchanges
        .iter()
        .map(|stored| (stored.id(), stored.clone()))
        .collect();
    assert_eq!(read, want, "each as first put; the unknown id absent");
    let mut newest_first: Vec<StoredExchange> = exchanges.clone();
    newest_first
        .sort_by_key(|stored| std::cmp::Reverse((stored.exchange.meta.started_at, stored.id())));
    let every = ExchangeQuery::default();
    for size in [1, 2, 3, 500] {
        assert_eq!(
            traverse(store, &every, size).await,
            newest_first
                .iter()
                .map(StoredExchange::id)
                .collect::<Vec<_>>(),
            "page size {size}"
        );
    }
    let window = ExchangeQuery {
        window: Some(TimeWindow::new(at(20), at(50)).unwrap_or_else(|error| panic!("{error:?}"))),
    };
    let in_window: Vec<ExchangeId> = newest_first
        .iter()
        .filter(|stored| window.admits(stored))
        .map(StoredExchange::id)
        .collect();
    assert_eq!(in_window.len(), 4);
    assert_eq!(traverse(store, &window, 1).await, in_window);
    let size = PageSize::new(1).unwrap_or_else(|error| panic!("{error:?}"));
    let first = store
        .list(&window, &PageRequest { size, after: None })
        .await
        .unwrap_or_else(|error| panic!("{error:?}"));
    let cursor = first
        .next()
        .cloned()
        .unwrap_or_else(|| panic!("more to follow"));
    assert_eq!(
        store
            .list(
                &every,
                &PageRequest {
                    size,
                    after: Some(cursor)
                }
            )
            .await
            .map(|_| ()),
        Err(ExchangeStoreError::InvalidCursor),
        "a cursor is bound to its window"
    );
}

/// INV-1024 `canonical.exchange.store-read` on the in-memory store.
#[tokio::test]
async fn exchange_store_keeps_the_first_put_and_lists_newest_first() {
    scenario(&mut MemoryExchanges::new()).await;
}

/// INV-1024 on Postgres: the same reads as the in-memory store.
#[tokio::test(flavor = "multi_thread")]
async fn pg_exchange_store_agrees_with_memory() {
    let db = match TestDb::new_or_skip("pg_exchange_store_agrees_with_memory").await {
        Ok(Some(db)) => db,
        Ok(None) => return,
        Err(error) => panic!("test database: {error:?}"),
    };
    db.migrate(Layer::Canonical, Migrations::Embedded(&MIGRATIONS))
        .await
        .unwrap_or_else(|error| panic!("L1 migrations: {error:?}"));
    scenario(&mut PgExchanges::new(db.pool().clone())).await;
    db.close()
        .await
        .unwrap_or_else(|error| panic!("close: {error:?}"));
}

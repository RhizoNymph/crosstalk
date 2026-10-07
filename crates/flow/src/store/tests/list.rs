//! `PgTransmissionStore::list` against the memory reference
//! (`flow.transmission-store.list-matches-query`): the same transmissions
//! saved into both, the same query matrix (windows, state sets, channels
//! including a superseded one and an unknown one, page sizes), every page
//! followed to the end; the two listings are equal. A cursor presented with
//! another query is `InvalidCursor`.

use std::collections::BTreeSet;

use crosstalk_memory::analysis::aliases::StaticDirectory;
use crosstalk_memory::flow::MemoryVerdicts;
use crosstalk_memory::flow::registry::model::{access_agent, channel, resource};
use crosstalk_memory::flow::verdicts::model::state_between;
use crosstalk_memory::support::Outbox;
use crosstalk_spec::derived::flow::channel::policy::{PolicyKind, Recorded};
use crosstalk_spec::derived::flow::channel::promotion::Promotion;
use crosstalk_spec::derived::flow::transmission::{
    DirectCarrier, Route, Transmission, TransmissionState,
};
use crosstalk_spec::ids::{ChannelId, OperatorId, TransmissionId};
use crosstalk_spec::interfaces::l5_flow::channels::ChannelTraffic;
use crosstalk_spec::interfaces::l5_flow::transmissions::{
    TransmissionQuery, TransmissionStore, TransmissionStoreError,
};
use crosstalk_spec::interfaces::l5_flow::{ChannelRegistry, Discovery};
use crosstalk_spec::interfaces::l8_surface::summary::TransmissionStateKind;
use crosstalk_spec::paging::{PageRequest, PageSize, TransmissionList};
use crosstalk_spec::support::TimeWindow;

use super::support::{Failure, TestResult, at, db, registry, same, transmissions};

fn unexpected(what: impl std::fmt::Debug) -> Failure {
    Failure::Unexpected(format!("{what:?}"))
}

/// Twelve transmissions: routes over channels 0, 1 (superseded by 0 below),
/// 2, and a direct route; every state; opened 50 µs apart from 1000 µs.
fn fixtures() -> Vec<Transmission> {
    (0u8..12)
        .map(|n| Transmission {
            id: TransmissionId::from_ulid(0x7B00_0000 | u128::from(n)),
            to: access_agent(2),
            route: match n % 4 {
                3 => Route::Direct(DirectCarrier::UserTurn),
                c => Route::Channel(channel(c)),
            },
            opened_at: at(1_000 + 50 * u64::from(n)),
            state: state_between(n % 7, &[0], access_agent(1), access_agent(2))
                .unwrap_or(TransmissionState::Detected),
        })
        .collect()
}

fn queries() -> Result<Vec<TransmissionQuery>, Failure> {
    use TransmissionStateKind as Kind;
    let windows = [
        TimeWindow::new(at(0), crosstalk_spec::wire::time::MAX).map_err(unexpected)?,
        TimeWindow::new(at(1_000), at(1_300)).map_err(unexpected)?,
        TimeWindow::new(at(1_200), at(1_201)).map_err(unexpected)?,
        TimeWindow::new(at(1_300), at(1_600)).map_err(unexpected)?,
    ];
    let states = [
        None,
        Some(BTreeSet::from([Kind::Confirmed, Kind::Classified])),
        Some(BTreeSet::from([Kind::Detected])),
        Some(BTreeSet::from([
            Kind::AwaitingContent,
            Kind::Suspected,
            Kind::Aggregated,
            Kind::Discarded,
        ])),
    ];
    let channels = [
        None,
        Some(channel(0)),
        Some(channel(1)),
        Some(channel(2)),
        Some(ChannelId::from_ulid(0xDEAD)),
    ];
    let mut all = Vec::new();
    for window in windows {
        for state in &states {
            for wanted in channels {
                all.push(TransmissionQuery {
                    window,
                    states: state.clone(),
                    channel: wanted,
                });
            }
        }
    }
    Ok(all)
}

/// Every page of `query` at `size`, in order.
async fn traverse<T: TransmissionStore + Sync>(
    store: &T,
    query: &TransmissionQuery,
    size: u16,
) -> Result<Vec<Transmission>, Failure> {
    let size = PageSize::new(size).map_err(unexpected)?;
    let mut request: PageRequest<TransmissionList> = PageRequest { size, after: None };
    let mut listed = Vec::new();
    for _ in 0..100 {
        let page = store.list(query, &request).await.map_err(unexpected)?;
        let (items, next) = page.into_parts();
        listed.extend(items);
        match next {
            Some(next) => request.after = Some(next),
            None => return Ok(listed),
        }
    }
    Err(Failure::Unexpected("the pages never end".to_owned()))
}

#[tokio::test(flavor = "multi_thread")]
async fn pg_list_agrees_with_the_memory_reference() -> TestResult {
    let Some(db) = db("pg_list_agrees_with_the_memory_reference").await? else {
        return Ok(());
    };
    // Channels 0, 1, 2 discovered; channel 1 superseded by channel 0.
    let (mut channels, _events) = registry(db.pool()).await?;
    for (c, r) in [(0u8, 0u8), (1, 2), (2, 4)] {
        let _ = channels.add_resource(resource(r)).await;
        let found = channels
            .discover(
                channel(c),
                resource(r).id,
                TransmissionId::from_ulid(0xF1A0_0000 | u128::from(r)),
                at(u64::from(r)),
            )
            .await;
        same("discover", &found, &Ok(Discovery::Created(channel(c))))?;
    }
    let promoted = channels
        .promote(
            channel(0),
            Promotion::new(
                crosstalk_memory::flow::registry::model::pattern(0),
                PolicyKind::Sanctioned,
                OperatorId::from_ulid(0x0B0B_0001),
                at(100),
                None,
            ),
        )
        .await
        .map_err(unexpected)?;
    same("superseded", &promoted.superseded, &vec![channel(1)])?;
    same("policy", &promoted.policy, &Recorded::Current)?;

    let directory = StaticDirectory::new();
    directory
        .supersede(channel(1), channel(0))
        .map_err(unexpected)?;
    let mut memory =
        MemoryVerdicts::with_directories(StaticDirectory::new(), directory, Outbox::none());
    let (mut pg, _events) = transmissions(db.pool()).await?;
    // Whatever the registry stored for its seed transmissions, the
    // reference holds too.
    let everything = TransmissionQuery {
        window: TimeWindow::new(at(0), crosstalk_spec::wire::time::MAX).map_err(unexpected)?,
        states: None,
        channel: None,
    };
    for stored in traverse(&pg, &everything, 100).await? {
        memory.save(stored).await.map_err(unexpected)?;
    }
    for transmission in fixtures() {
        pg.save(transmission.clone()).await.map_err(unexpected)?;
        memory.save(transmission).await.map_err(unexpected)?;
    }
    for query in queries()? {
        for size in [1u16, 3, 100] {
            let want = traverse(&memory, &query, size).await?;
            let got = traverse(&pg, &query, size).await?;
            same(&format!("{query:?} at page size {size}"), &got, &want)?;
        }
    }
    let all = traverse(&pg, &everything, 100).await?;
    for transmission in fixtures() {
        crate::store::tests::support::ensure(all.contains(&transmission), || {
            format!("{} not listed", transmission.id.ulid_text())
        })?;
    }

    // A cursor binds its query.
    let size = PageSize::new(1).map_err(unexpected)?;
    let first = pg
        .list(&everything, &PageRequest { size, after: None })
        .await
        .map_err(unexpected)?;
    let (_, next) = first.into_parts();
    let other = TransmissionQuery {
        channel: Some(channel(0)),
        ..everything.clone()
    };
    same(
        "a cursor with another query",
        &pg.list(&other, &PageRequest { size, after: next })
            .await
            .map(|_| ()),
        &Err(TransmissionStoreError::InvalidCursor),
    )?;
    db.close().await?;
    Ok(())
}

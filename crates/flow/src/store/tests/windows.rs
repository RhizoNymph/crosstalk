//! Windows past the `bigint` time columns (`crosstalk_store::ColumnWindow`):
//! a window ending past `i64::MAX` reads as unbounded above, the same rows
//! as a bounded window covering them, its cursors still binding it; one
//! starting past it reads empty, not an error. For `list`, `quality` and
//! `resource_use`; and `channels`, whose filter's window binds its cursors
//! but never restricts its rows.

use crosstalk_memory::flow::registry::model::{access_agent, channel, resource};
use crosstalk_memory::flow::verdicts::model::state_between;
use crosstalk_spec::aggregates::access::ResourceUse;
use crosstalk_spec::derived::flow::access::{Access, AccessOp, Extraction};
use crosstalk_spec::derived::flow::transmission::{Route, Transmission, TransmissionState};
use crosstalk_spec::ids::{AccessId, ChannelId, ExchangeId, MessageHash, TransmissionId};
use crosstalk_spec::interfaces::l5_flow::channels::ChannelTraffic;
use crosstalk_spec::interfaces::l5_flow::transmissions::{TransmissionQuery, TransmissionStore};
use crosstalk_spec::interfaces::l5_flow::verdicts::TransmissionVerdicts;
use crosstalk_spec::interfaces::l5_flow::{ChannelRegistry, Discovery};
use crosstalk_spec::interfaces::l8_surface::lists::{ChannelFilter, OriginFilter};
use crosstalk_spec::observed::message::PartRef;
use crosstalk_spec::paging::{PageRequest, PageSize, TransmissionList};
use crosstalk_spec::support::{Blake3, TimeWindow};

use super::support::{Failure, TestResult, at, db, ensure, registry, same, transmissions};

/// The first time a `bigint` column cannot hold.
const PAST: u64 = i64::MAX.unsigned_abs() + 1;

fn unexpected(what: impl std::fmt::Debug) -> Failure {
    Failure::Unexpected(format!("{what:?}"))
}

fn window(start: u64, end: u64) -> Result<TimeWindow, Failure> {
    TimeWindow::new(at(start), at(end)).map_err(unexpected)
}

/// Ten transmissions over channel 0 and a direct route, in every state,
/// opened 50 µs apart from 1000 µs.
fn fixtures() -> Vec<Transmission> {
    (0u8..10)
        .map(|n| Transmission {
            id: TransmissionId::from_ulid(0x7C00_0000 | u128::from(n)),
            to: access_agent(2),
            route: if n % 3 == 0 {
                Route::Direct(crosstalk_spec::derived::flow::transmission::DirectCarrier::UserTurn)
            } else {
                Route::Channel(channel(0))
            },
            opened_at: at(1_000 + 50 * u64::from(n)),
            state: state_between(n % 7, &[0], access_agent(1), access_agent(2))
                .unwrap_or(TransmissionState::Detected),
        })
        .collect()
}

/// Every page of `query` at `size`, in order.
async fn listed<T: TransmissionStore + Sync>(
    store: &T,
    query: &TransmissionQuery,
    size: u16,
) -> Result<Vec<Transmission>, Failure> {
    let size = PageSize::new(size).map_err(unexpected)?;
    let mut request: PageRequest<TransmissionList> = PageRequest { size, after: None };
    let mut all = Vec::new();
    for _ in 0..100 {
        let (items, next) = store
            .list(query, &request)
            .await
            .map_err(unexpected)?
            .into_parts();
        all.extend(items);
        match next {
            Some(next) => request.after = Some(next),
            None => return Ok(all),
        }
    }
    Err(Failure::Unexpected("the pages never end".to_owned()))
}

fn query(window: TimeWindow) -> TransmissionQuery {
    TransmissionQuery {
        window,
        states: None,
        channel: None,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn pg_list_and_quality_read_an_end_past_the_column_as_unbounded() -> TestResult {
    let Some(db) = db("pg_list_and_quality_read_an_end_past_the_column_as_unbounded").await? else {
        return Ok(());
    };
    let (mut store, _events) = transmissions(db.pool()).await?;
    for transmission in fixtures() {
        store.save(transmission).await.map_err(unexpected)?;
    }
    let max = crosstalk_spec::wire::time::MAX.as_micros();
    for start in [0, 1_200] {
        let bounded = query(window(start, max)?);
        let want = listed(&store, &bounded, 100).await?;
        ensure(!want.is_empty(), || format!("rows from {start}"))?;
        for end in [PAST, u64::MAX] {
            let unbounded = query(window(start, end)?);
            for size in [1u16, 3, 100] {
                same(
                    &format!("{start}..{end} at page size {size}"),
                    &listed(&store, &unbounded, size).await?,
                    &want,
                )?;
            }
            let got = store.quality(unbounded.window).await.map_err(unexpected)?;
            let tallied = store.quality(bounded.window).await.map_err(unexpected)?;
            ensure(!tallied.rows().is_empty(), || {
                format!("quality from {start}")
            })?;
            same(
                &format!("quality {start}..{end}"),
                &got.rows(),
                &tallied.rows(),
            )?;
            same("quality window", &got.window(), &unbounded.window)?;
        }
    }
    for beyond in [window(PAST, u64::MAX)?, window(u64::MAX - 1, u64::MAX)?] {
        let size = PageSize::new(100).map_err(unexpected)?;
        let (items, next) = store
            .list(&query(beyond), &PageRequest { size, after: None })
            .await
            .map_err(unexpected)?
            .into_parts();
        same(
            &format!("{beyond:?} listed"),
            &(items, next),
            &(Vec::new(), None),
        )?;
        let quality = store.quality(beyond).await.map_err(unexpected)?;
        ensure(quality.rows().is_empty(), || format!("{quality:?}"))?;
        same("quality window", &quality.window(), &beyond)?;
    }
    db.close().await?;
    Ok(())
}

fn access(n: u8, at_micros: u64) -> Access {
    let part = PartRef {
        message: MessageHash::from_digest(Blake3::from_bytes([0; 32])),
        index: 0,
    };
    Access {
        id: AccessId::from_ulid(0xACC1_0000 | u128::from(n)),
        agent: access_agent(1 + n % 2),
        exchange: ExchangeId::from_ulid(0xE100 | u128::from(n)),
        resource: resource(0).id,
        at: at(at_micros),
        via: Extraction::Structured,
        op: AccessOp::Read { result: part },
    }
}

/// Every page of channel 0's resource use in `window` at `size`.
async fn used(
    store: &impl ChannelRegistry,
    window: TimeWindow,
    size: u16,
) -> Result<Vec<(ChannelId, Vec<ResourceUse>)>, Failure> {
    crosstalk_memory::flow::registry::model::traverse(store, channel(0), window, size)
        .await
        .map_err(unexpected)
}

#[tokio::test(flavor = "multi_thread")]
async fn pg_resource_use_and_channels_read_an_end_past_the_column_as_unbounded() -> TestResult {
    let Some(db) =
        db("pg_resource_use_and_channels_read_an_end_past_the_column_as_unbounded").await?
    else {
        return Ok(());
    };
    let (mut channels, _events) = registry(db.pool()).await?;
    // Channels 0, 1 and 2, from resources 0, 2 and 4.
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
        // Its seed transmission, awaiting content, as the flow consumer
        // records it: a channel with no traffic is in no list.
        let seed = Transmission {
            id: TransmissionId::from_ulid(0xF1A0_0000 | u128::from(r)),
            to: access_agent(2),
            route: Route::Channel(channel(c)),
            opened_at: at(u64::from(r)),
            state: state_between(1, &[0], access_agent(1), access_agent(2))
                .unwrap_or(TransmissionState::Detected),
        };
        channels
            .record_transmission(&seed)
            .await
            .map_err(unexpected)?;
    }
    for n in 0u8..6 {
        channels
            .record_access(access(n, 1_000 + 100 * u64::from(n)))
            .await
            .map_err(unexpected)?;
    }
    let max = crosstalk_spec::wire::time::MAX.as_micros();
    for start in [0, 1_250] {
        let want = used(&channels, window(start, max)?, 100).await?;
        ensure(want.iter().any(|(_, rows)| !rows.is_empty()), || {
            format!("resource use from {start}")
        })?;
        for end in [PAST, u64::MAX] {
            for size in [1u16, 100] {
                let got = used(&channels, window(start, end)?, size).await?;
                same(
                    &format!("{start}..{end} at page size {size}"),
                    &got.into_iter()
                        .flat_map(|(_, rows)| rows)
                        .collect::<Vec<_>>(),
                    &want
                        .iter()
                        .flat_map(|(_, rows)| rows.clone())
                        .collect::<Vec<_>>(),
                )?;
            }
        }
    }
    for beyond in [window(PAST, u64::MAX)?, window(u64::MAX - 1, u64::MAX)?] {
        same(
            &format!("{beyond:?}"),
            &used(&channels, beyond, 100).await?,
            &vec![(channel(0), Vec::new())],
        )?;
    }
    // The channel list, in pages of 2 (cursors bound to the filter).
    let listed = |window: Option<TimeWindow>| ChannelFilter {
        origin: OriginFilter::WithSuperseded(Vec::new()),
        window,
        ..ChannelFilter::default()
    };
    let every = crosstalk_memory::flow::registry::model::channels_under(&channels, &listed(None))
        .await
        .map_err(unexpected)?;
    same("channels listed", &every.len(), &3)?;
    for window in [
        window(0, max)?,
        window(0, u64::MAX)?,
        window(PAST, u64::MAX)?,
    ] {
        same(
            &format!("channels in {window:?}"),
            &crosstalk_memory::flow::registry::model::channels_under(
                &channels,
                &listed(Some(window)),
            )
            .await
            .map_err(unexpected)?,
            &every,
        )?;
    }
    db.close().await?;
    Ok(())
}

//! Reference tests for the verdict store, one or more per invariant that
//! names `TransmissionVerdicts`. Each test's doc names the invariant.

use std::num::NonZeroU32;

use crosstalk_spec::aggregates::quality::DetectionQuality;
use crosstalk_spec::derived::flow::transmission::{Transmission, TransmissionState};
use crosstalk_spec::derived::flow::verdict::{
    Verdict, VerdictLog, VerdictRecorded, VerdictRevision,
};
use crosstalk_spec::events::BusEvent;
use crosstalk_spec::events::changed::Changed;
use crosstalk_spec::events::detect::DetectEvent;
use crosstalk_spec::ids::{AgentId, OperatorId, TransmissionId};
use crosstalk_spec::interfaces::l5_flow::transmissions::TransmissionStore;
use crosstalk_spec::interfaces::l5_flow::verdicts::{TransmissionVerdicts, VerdictError};
use crosstalk_spec::support::{TimeWindow, Timestamp};
use tokio::sync::mpsc::UnboundedReceiver;

use super::MemoryVerdicts;
use super::model::{self, route, state, transmission_id, unknown_transmission};
use crate::model::{HarnessConfig, ModelMismatch};
use crate::support::{Outbox, drain};

/// The case count the pipeline harnesses have always run with.
fn pipeline_harness() -> HarnessConfig {
    HarnessConfig {
        cases: 64,
        ..HarnessConfig::default()
    }
}

fn at(micros: u64) -> Timestamp {
    Timestamp::from_micros(micros)
}

fn operator() -> OperatorId {
    OperatorId::from_ulid(0x0B0B)
}

fn transmission(
    n: u8,
    state_kind: u8,
    classes: &[u8],
    route_kind: u8,
    opened: u64,
) -> Transmission {
    Transmission {
        id: transmission_id(n),
        to: AgentId::from_ulid(0x0A6E_0002),
        route: route(route_kind),
        opened_at: at(opened),
        state: state(state_kind, classes).unwrap_or_else(|| panic!("state fixture")),
    }
}

async fn store_with(
    transmissions: Vec<Transmission>,
) -> (MemoryVerdicts, UnboundedReceiver<BusEvent>) {
    let (outbox, events) = Outbox::channel();
    let mut store = MemoryVerdicts::new(outbox);
    for transmission in transmissions {
        assert_eq!(store.save(transmission).await, Ok(()));
    }
    (store, events)
}

fn revision(n: u32) -> VerdictRevision {
    VerdictRevision::new(NonZeroU32::new(n).unwrap_or(NonZeroU32::MIN))
}

/// `flow.verdict.set-rejects-without-effect`.
#[tokio::test]
async fn set_verdict_rejects_unknown_and_unjudgeable() {
    let (mut store, mut events) = store_with(vec![
        transmission(0, 0, &[0], 0, 5),
        transmission(1, 1, &[0], 0, 5),
    ])
    .await;
    assert_eq!(
        store
            .set(
                unknown_transmission(),
                Some(Verdict::Genuine),
                operator(),
                at(10),
                None
            )
            .await,
        Err(VerdictError::UnknownTransmission(unknown_transmission()))
    );
    for n in [0, 1] {
        assert_eq!(
            store
                .set(
                    transmission_id(n),
                    Some(Verdict::Genuine),
                    operator(),
                    at(10),
                    None
                )
                .await,
            Err(VerdictError::NotJudgeable(transmission_id(n)))
        );
        assert_eq!(
            store.log(transmission_id(n)).await,
            Ok(VerdictLog::new(transmission_id(n)))
        );
    }
    assert!(drain(&mut events).is_empty());
}

/// `flow.verdict.set-event-once` and `flow.verdict.change-announced`: an
/// append publishes one `VerdictSet` with its revision and one
/// `Changed::Verdict`; a repeat publishes nothing.
#[tokio::test]
async fn verdict_set_once() {
    let (mut store, mut events) = store_with(vec![transmission(0, 3, &[0], 0, 5)]).await;
    let id = transmission_id(0);
    assert_eq!(
        store
            .set(
                id,
                Some(Verdict::FalseDetection),
                operator(),
                at(10),
                Some("noise".to_owned())
            )
            .await,
        Ok(VerdictRecorded::Appended(revision(1)))
    );
    assert_eq!(
        drain(&mut events),
        vec![
            BusEvent::Detect(DetectEvent::VerdictSet {
                transmission: id,
                verdict: Some(Verdict::FalseDetection),
                revision: revision(1),
                by: operator(),
                at: at(10),
            }),
            BusEvent::Changed(Changed::Verdict(id)),
        ]
    );
    assert_eq!(
        store
            .set(id, Some(Verdict::FalseDetection), operator(), at(11), None)
            .await,
        Ok(VerdictRecorded::Unchanged)
    );
    assert!(drain(&mut events).is_empty());
    assert_eq!(
        store.set(id, None, operator(), at(12), None).await,
        Ok(VerdictRecorded::Appended(revision(2)))
    );
    assert_eq!(drain(&mut events).len(), 2);
}

/// `flow.verdict.repeat-appends-nothing`, through the store: withdrawing
/// with nothing in force appends nothing.
#[tokio::test]
async fn repeating_the_current_verdict_appends_nothing() {
    let (mut store, _events) = store_with(vec![transmission(0, 2, &[0], 1, 5)]).await;
    let id = transmission_id(0);
    assert_eq!(
        store.set(id, None, operator(), at(10), None).await,
        Ok(VerdictRecorded::Unchanged)
    );
    assert_eq!(store.log(id).await, Ok(VerdictLog::new(id)));
}

/// `flow.verdict.state-untouched`.
#[tokio::test]
async fn set_verdict_keeps_transmission_state() {
    let stored = transmission(0, 4, &[1, 2], 2, 5);
    let (mut store, _events) = store_with(vec![stored.clone()]).await;
    for verdict in [Some(Verdict::Genuine), Some(Verdict::FalseDetection), None] {
        assert!(
            store
                .set(transmission_id(0), verdict, operator(), at(10), None)
                .await
                .is_ok()
        );
        assert_eq!(
            store.transmission(transmission_id(0)).await,
            Ok(Some(stored.clone()))
        );
    }
}

/// A transmission that becomes judgeable takes a verdict, and its log
/// survives later state changes.
#[tokio::test]
async fn verdicts_follow_the_transmission_forward() {
    let (mut store, _events) = store_with(vec![transmission(0, 1, &[0], 0, 5)]).await;
    let id = transmission_id(0);
    assert!(
        store
            .set(id, Some(Verdict::Genuine), operator(), at(10), None)
            .await
            .is_err()
    );
    assert_eq!(store.save(transmission(0, 2, &[0], 0, 5)).await, Ok(()));
    assert!(
        store
            .set(id, Some(Verdict::Genuine), operator(), at(11), None)
            .await
            .is_ok()
    );
    assert_eq!(store.save(transmission(0, 3, &[0], 0, 5)).await, Ok(()));
    let Ok(log) = store.log(id).await else {
        panic!("log");
    };
    assert_eq!(log.current(), Some(Verdict::Genuine));
    assert!(matches!(
        store.transmission(id).await.map(|t| t.map(|t| t.state)),
        Ok(Some(TransmissionState::Confirmed(_)))
    ));
}

/// `surface.query.detection-quality-matches-tally`, at L5: `quality` is
/// `DetectionQuality::tally` over every stored transmission with its
/// current verdict.
#[tokio::test]
async fn detection_quality_matches_tally() {
    let stored = vec![
        transmission(0, 3, &[2, 0], 0, 5),
        transmission(1, 2, &[0], 0, 6),
        transmission(2, 6, &[0], 1, 7),
        transmission(3, 0, &[0], 2, 8),
        transmission(4, 5, &[3], 3, 90),
    ];
    let (mut store, _events) = store_with(stored.clone()).await;
    let verdicts = [
        (0, Some(Verdict::Genuine)),
        (1, Some(Verdict::FalseDetection)),
        (2, Some(Verdict::Genuine)),
        (4, Some(Verdict::FalseDetection)),
    ];
    for (n, verdict) in verdicts {
        assert!(
            store
                .set(transmission_id(n), verdict, operator(), at(20), None)
                .await
                .is_ok()
        );
    }
    assert!(
        store
            .set(transmission_id(1), None, operator(), at(21), None)
            .await
            .is_ok()
    );
    let Ok(window) = TimeWindow::new(at(0), at(50)) else {
        panic!("window");
    };
    let current = |id: TransmissionId| match id {
        id if id == transmission_id(0) || id == transmission_id(2) => Some(Verdict::Genuine),
        id if id == transmission_id(4) => Some(Verdict::FalseDetection),
        _ => None,
    };
    let expected = DetectionQuality::tally(
        window,
        stored
            .iter()
            .map(|transmission| (transmission, current(transmission.id))),
        crosstalk_spec::aliases::NoAliases,
    );
    assert_eq!(store.quality(window).await, Ok(expected));
}

/// The log of an unknown transmission is refused; of a stored one never
/// judged, empty.
#[tokio::test]
async fn logs_of_unknown_and_unjudged_transmissions() {
    let (store, _events) = store_with(vec![transmission(0, 3, &[0], 0, 5)]).await;
    assert_eq!(
        store.log(unknown_transmission()).await,
        Err(VerdictError::UnknownTransmission(unknown_transmission()))
    );
    assert_eq!(
        store.log(transmission_id(0)).await,
        Ok(VerdictLog::new(transmission_id(0)))
    );
}

/// The reference agrees with itself under the harness.
#[test]
fn reference_agrees_with_itself_under_the_harness() {
    let outcome =
        model::check_transmission_verdicts(pipeline_harness(), MemoryVerdicts::with_agents);
    assert_eq!(outcome, Ok(()));
}

/// The harness catches a store that publishes nothing.
#[test]
fn harness_rejects_a_store_that_publishes_nothing() {
    let outcome = model::check_transmission_verdicts(pipeline_harness(), |agents, _outbox| {
        MemoryVerdicts::with_agents(agents, Outbox::none())
    });
    assert!(
        matches!(outcome, Err(ModelMismatch::Failed { .. })),
        "{outcome:?}"
    );
}

/// The store is `Send + Sync`, so its futures are `Send`.
#[test]
fn the_store_is_send_and_sync() {
    fn assert_send_sync<T: Send + Sync + 'static>() {}
    assert_send_sync::<MemoryVerdicts>();
}

/// `flow.quality.cross-agent-only`, at the store: a
/// transmission whose sender has since been merged into its reader is not
/// counted, and the unmerge counts it again.
#[tokio::test]
async fn quality_leaves_out_transmissions_within_one_agent() {
    use crate::analysis::aliases::StaticDirectory;
    use crosstalk_spec::aliases::NoAliases;

    let directory = StaticDirectory::new();
    let mut store = MemoryVerdicts::with_agents(directory.clone(), Outbox::none());
    let crossing = transmission(0, 3, &[0], 0, 5);
    assert_eq!(store.save(crossing.clone()).await, Ok(()));
    let Some(state) = model::state_between(
        3,
        &[0],
        AgentId::from_ulid(0x0A6E_0003),
        AgentId::from_ulid(0x0A6E_0002),
    ) else {
        panic!("state fixture");
    };
    let merged = Transmission {
        id: transmission_id(1),
        state,
        ..crossing.clone()
    };
    assert_eq!(store.save(merged.clone()).await, Ok(()));
    let Ok(window) = TimeWindow::new(at(0), at(50)) else {
        panic!("window");
    };
    let both = DetectionQuality::tally(window, [(&crossing, None), (&merged, None)], NoAliases);
    assert_eq!(store.quality(window).await, Ok(both.clone()));
    let Ok(()) = directory.merge(
        AgentId::from_ulid(0x0A6E_0003),
        AgentId::from_ulid(0x0A6E_0002),
    ) else {
        panic!("merge");
    };
    let one = DetectionQuality::tally(window, [(&crossing, None)], NoAliases);
    assert_eq!(store.quality(window).await, Ok(one));
    directory.unmerge(AgentId::from_ulid(0x0A6E_0003));
    assert_eq!(store.quality(window).await, Ok(both));
}

/// `flow.transmission-store.list-matches-query`: `list` returns exactly the
/// stored transmissions the query matches (window on `opened_at`, state
/// kinds, channel resolved through the directory), newest id first, each
/// once across pages, and refuses a cursor presented with another query.
#[tokio::test]
async fn list_returns_what_the_query_matches_newest_first() {
    use std::collections::BTreeSet;

    use crosstalk_spec::ids::ChannelId;
    use crosstalk_spec::interfaces::l5_flow::transmissions::{
        TransmissionQuery, TransmissionStoreError,
    };
    use crosstalk_spec::interfaces::l8_surface::summary::TransmissionStateKind;
    use crosstalk_spec::paging::{PageRequest, PageSize};

    use crate::analysis::aliases::StaticDirectory;

    let old = ChannelId::from_ulid(0x0C4A);
    let new = ChannelId::from_ulid(0x0C4B);
    let directory = StaticDirectory::new();
    assert!(directory.supersede(old, new).is_ok());
    let mut store =
        MemoryVerdicts::with_directories(StaticDirectory::new(), directory, Outbox::none());
    // n: id; state kind; route kind (0 is a channel route on `old`); opened.
    let rows = [
        (0, 3, 0, 100),
        (1, 1, 0, 200),
        (2, 3, 1, 300),
        (3, 4, 0, 500),
        (4, 3, 0, 900),
    ];
    for (n, kind, route_kind, opened) in rows {
        let saved = store
            .save(transmission(n, kind, &[0], route_kind, opened))
            .await;
        assert_eq!(saved, Ok(()));
    }
    let window = TimeWindow::new(at(100), at(600)).unwrap_or_else(|_| panic!("window"));
    let query = TransmissionQuery {
        window,
        states: Some(BTreeSet::from([
            TransmissionStateKind::Confirmed,
            TransmissionStateKind::Classified,
        ])),
        // Asked for by the superseding channel: the routes name `old`.
        channel: Some(new),
    };
    let size = PageSize::new(1).unwrap_or_else(|_| panic!("page size"));
    let mut request = PageRequest { size, after: None };
    let mut listed = Vec::new();
    loop {
        let page = store
            .list(&query, &request)
            .await
            .unwrap_or_else(|error| panic!("list: {error:?}"));
        let (items, next) = page.into_parts();
        listed.extend(items.into_iter().map(|transmission| transmission.id));
        match next {
            Some(next) => request.after = Some(next),
            None => break,
        }
    }
    // 1 is awaiting content, 2 is not a channel route, 4 opened after the
    // window.
    assert_eq!(listed, vec![transmission_id(3), transmission_id(0)]);

    let everything = TransmissionQuery {
        window: TimeWindow::new(at(0), at(1_000)).unwrap_or_else(|_| panic!("window")),
        states: None,
        channel: None,
    };
    let first = store
        .list(&query, &PageRequest { size, after: None })
        .await
        .unwrap_or_else(|error| panic!("list: {error:?}"));
    let (_, next) = first.into_parts();
    let stale = PageRequest { size, after: next };
    assert_eq!(
        store.list(&everything, &stale).await.map(|_| ()),
        Err(TransmissionStoreError::InvalidCursor)
    );
    let all = store
        .list(
            &everything,
            &PageRequest {
                size: PageSize::new(10).unwrap_or_else(|_| panic!("page size")),
                after: None,
            },
        )
        .await
        .unwrap_or_else(|error| panic!("list: {error:?}"));
    let ids: Vec<_> = all
        .items()
        .iter()
        .map(|transmission| transmission.id)
        .collect();
    assert_eq!(ids, (0..5).rev().map(transmission_id).collect::<Vec<_>>());
}

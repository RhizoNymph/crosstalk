//! Integration tests of `PgTransmissionStore` and the outbox, against
//! Postgres.

use crosstalk_memory::flow::verdicts::model::{route, state, transmission_id};
use crosstalk_spec::derived::flow::transmission::Transmission;
use crosstalk_spec::derived::flow::verdict::{Verdict, VerdictRecorded, VerdictRevision};
use crosstalk_spec::events::BusEvent;
use crosstalk_spec::events::changed::Changed;
use crosstalk_spec::events::detect::DetectEvent;
use crosstalk_spec::ids::{AgentId, OperatorId};
use crosstalk_spec::interfaces::l5_flow::transmissions::TransmissionStore;
use crosstalk_spec::interfaces::l5_flow::verdicts::{TransmissionVerdicts, VerdictError};
use tokio::sync::mpsc::unbounded_channel;

use super::support::{Failure, TestResult, agents, at, db, drain, ensure, same, transmissions};
use crate::store::{EventSink, PgTransmissionStore};

fn stored(n: u8, state_number: u8) -> Result<Transmission, Failure> {
    Ok(Transmission {
        id: transmission_id(n),
        to: AgentId::from_ulid(0x0A6E_0002),
        route: route(0),
        opened_at: at(10),
        state: state(state_number, &[0])
            .ok_or_else(|| Failure::Unexpected("state fixture".to_owned()))?,
    })
}

fn operator() -> OperatorId {
    OperatorId::from_ulid(0x0B0B_0000)
}

async fn staged(pool: &sqlx::PgPool) -> Result<i64, sqlx::Error> {
    sqlx::query_scalar("SELECT count(*) FROM flow.outbox")
        .fetch_one(pool)
        .await
}

/// INV-527 `flow.verdict.set-event-once`: an appended verdict publishes one
/// VerdictSet with its revision and one Changed::Verdict, through the
/// outbox in the append's transaction; a repeat appends and publishes
/// nothing; a refusal stages nothing.
#[tokio::test(flavor = "multi_thread")]
async fn pg_verdict_set_outbox() -> TestResult {
    let Some(db) = db("pg_verdict_set_outbox").await? else {
        return Ok(());
    };
    let (mut store, mut events) = transmissions(db.pool()).await?;
    store
        .save(stored(0, 3)?)
        .await
        .map_err(|e| Failure::Unexpected(format!("{e:?}")))?;
    let set = store
        .set(
            transmission_id(0),
            Some(Verdict::Genuine),
            operator(),
            at(20),
            None,
        )
        .await;
    same(
        "appended",
        &set,
        &Ok(VerdictRecorded::Appended(VerdictRevision::FIRST)),
    )?;
    same(
        "events",
        &drain(&mut events),
        &vec![
            BusEvent::Detect(DetectEvent::VerdictSet {
                transmission: transmission_id(0),
                verdict: Some(Verdict::Genuine),
                revision: VerdictRevision::FIRST,
                by: operator(),
                at: at(20),
            }),
            BusEvent::Changed(Changed::Verdict(transmission_id(0))),
        ],
    )?;
    same("relayed and removed", &staged(db.pool()).await?, &0)?;
    let repeat = store
        .set(
            transmission_id(0),
            Some(Verdict::Genuine),
            operator(),
            at(30),
            None,
        )
        .await;
    same("repeat", &repeat, &Ok(VerdictRecorded::Unchanged))?;
    same("repeat publishes nothing", &drain(&mut events), &Vec::new())?;
    store
        .save(stored(1, 1)?)
        .await
        .map_err(|e| Failure::Unexpected(format!("{e:?}")))?;
    same(
        "awaiting content takes no verdict",
        &store
            .set(
                transmission_id(1),
                Some(Verdict::Genuine),
                operator(),
                at(20),
                None,
            )
            .await,
        &Err(VerdictError::NotJudgeable(transmission_id(1))),
    )?;
    same(
        "unknown",
        &store
            .set(transmission_id(4), None, operator(), at(20), None)
            .await,
        &Err(VerdictError::UnknownTransmission(transmission_id(4))),
    )?;
    same("refusals publish nothing", &drain(&mut events), &Vec::new())?;
    same("refusals stage nothing", &staged(db.pool()).await?, &0)?;
    db.close().await?;
    Ok(())
}

/// INV-811 `flow.transmission-store.save-keeps-verdicts` and INV-529
/// `flow.verdict.state-untouched`, against Postgres: saving a new state
/// keeps the log, and a verdict leaves the stored transmission as saved.
#[tokio::test(flavor = "multi_thread")]
async fn pg_save_keeps_the_verdict_log() -> TestResult {
    let Some(db) = db("pg_save_keeps_the_verdict_log").await? else {
        return Ok(());
    };
    let (mut store, _events) = transmissions(db.pool()).await?;
    let suspected = stored(0, 2)?;
    store
        .save(suspected.clone())
        .await
        .map_err(|e| Failure::Unexpected(format!("{e:?}")))?;
    store
        .set(
            transmission_id(0),
            Some(Verdict::FalseDetection),
            operator(),
            at(20),
            Some("noise".to_owned()),
        )
        .await
        .map_err(|e| Failure::Unexpected(format!("{e:?}")))?;
    same(
        "a verdict leaves the transmission",
        &store.transmission(transmission_id(0)).await,
        &Ok(Some(suspected)),
    )?;
    let log = store.log(transmission_id(0)).await;
    let confirmed = stored(0, 3)?;
    store
        .save(confirmed.clone())
        .await
        .map_err(|e| Failure::Unexpected(format!("{e:?}")))?;
    same("log kept", &store.log(transmission_id(0)).await, &log)?;
    same(
        "new state stored",
        &store.transmission(transmission_id(0)).await,
        &Ok(Some(confirmed)),
    )?;
    db.close().await?;
    Ok(())
}

/// The outbox delivers at least once: events staged by a write whose relay
/// found the receiver gone stay staged, and a later relay by another store
/// on the database sends them.
#[tokio::test(flavor = "multi_thread")]
async fn pg_outbox_keeps_events_until_relayed() -> TestResult {
    let Some(db) = db("pg_outbox_keeps_events_until_relayed").await? else {
        return Ok(());
    };
    let (sender, receiver) = unbounded_channel();
    drop(receiver);
    let mut lost =
        PgTransmissionStore::new(db.pool().clone(), agents().await?, EventSink::new(sender));
    lost.save(stored(0, 3)?)
        .await
        .map_err(|e| Failure::Unexpected(format!("{e:?}")))?;
    let set = lost
        .set(
            transmission_id(0),
            Some(Verdict::Genuine),
            operator(),
            at(20),
            None,
        )
        .await;
    ensure(set.is_ok(), || format!("the write succeeds: {set:?}"))?;
    same("still staged", &staged(db.pool()).await?, &2)?;
    let (other, mut events) = transmissions(db.pool()).await?;
    same("relayed", &other.relay().relay().await?, &2)?;
    let relayed = drain(&mut events);
    same("in staging order", &relayed.len(), &2)?;
    ensure(
        matches!(relayed[0], BusEvent::Detect(DetectEvent::VerdictSet { .. })),
        || format!("{relayed:?}"),
    )?;
    same("none left", &staged(db.pool()).await?, &0)?;
    db.close().await?;
    Ok(())
}

//! A race on one confirmed transmission, for any `TransmissionStore`: its
//! content extended from several tasks while a classification of it is
//! written, each through `TransmissionStore::modify`, as the flow consumer
//! and analysis write it (`flow.transmission-store.modify-atomic`).
//!
//! ```text
//! save Confirmed { content: [m0] }
//! classify: modify(Confirmed ─▶ Classified), holding the read for HOLD
//!   extenders 1..=EXTENSIONS/2 start before it, the rest while it holds
//! every extender: modify(add m_i to the confirmation)
//! read back: Classified, content m0..=m_EXTENSIONS, nothing lost
//! ```
//!
//! A classifier that read with `transmission` and wrote with `save` would
//! write back the content it read, dropping every match an extender added
//! while it held: the 20261007T150139Z bench run lost two that way.

use std::num::NonZeroU32;
use std::time::Duration;

use crosstalk_spec::aggregates::topic::TopicModelVersion;
use crosstalk_spec::derived::flow::transmission::{
    Classification, Confirmed, Route, Transmission, TransmissionState,
};
use crosstalk_spec::derived::provenance::matching::{Carrier, ContentMatch, MatchKind};
use crosstalk_spec::derived::provenance::span::SpanLocation;
use crosstalk_spec::ids::{AgentId, ChannelId, ExchangeId, MessageHash, SpanId, TransmissionId};
use crosstalk_spec::interfaces::l5_flow::transmissions::TransmissionStore;
use crosstalk_spec::observed::message::{PartRef, ToolCallId};
use crosstalk_spec::support::{Blake3, ByteRange, NonEmpty, Timestamp};
use tokio::sync::oneshot;

/// How many matches extend the transmission during the race.
pub const EXTENSIONS: u32 = 8;

/// How long the classification holds what it read before it writes: the
/// classifier's read of the topic catalog between the two.
pub const HOLD: Duration = Duration::from_millis(50);

/// Why a race did not end as `modify` promises.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RaceError {
    #[error("fixture: {0}")]
    Fixture(String),
    #[error("store: {0}")]
    Store(String),
    #[error("task: {0}")]
    Task(String),
    #[error("after the race: {0}")]
    Lost(String),
}

fn sender() -> AgentId {
    AgentId::from_ulid(0x0A6E_0001)
}

fn reader() -> AgentId {
    AgentId::from_ulid(0x0A6E_0002)
}

/// The race's transmission in round `round`.
pub fn race_transmission(round: u8) -> TransmissionId {
    TransmissionId::from_ulid(0x7B00_0000 | u128::from(round))
}

/// Match `n` of the race: one span of the sender read at its own range of
/// one tool result of the reader's exchange.
pub fn race_match(n: u32) -> Result<ContentMatch, RaceError> {
    let fixture = |what: &str| RaceError::Fixture(what.to_owned());
    let start = n.saturating_mul(40);
    ContentMatch::new(
        SpanId::from_ulid(0x5DB0_0000 | u128::from(n)),
        sender(),
        reader(),
        ExchangeId::from_ulid(0xE2),
        SpanLocation {
            part: PartRef {
                message: MessageHash::from_digest(Blake3::from_bytes([5; 32])),
                index: 0,
            },
            range: ByteRange::new(start, start.saturating_add(32)).map_err(|_| fixture("range"))?,
        },
        Carrier::ToolResult(ToolCallId("call-race".to_owned())),
        MatchKind::Exact,
        NonZeroU32::new(32).ok_or_else(|| fixture("bytes"))?,
    )
    .map_err(|error| RaceError::Fixture(format!("match {n}: {error:?}")))
}

fn classification() -> Classification {
    Classification {
        version: TopicModelVersion(1),
        topic: None,
        watched: false,
    }
}

/// `stored` with `content` added to its confirmation (unchanged when it
/// holds none), as the flow consumer extends one.
fn extended(stored: Transmission, content: ContentMatch) -> (TransmissionState, bool) {
    let mut state = stored.state;
    let added = match &mut state {
        TransmissionState::Confirmed(confirmed)
        | TransmissionState::Classified { confirmed, .. }
        | TransmissionState::Aggregated { confirmed, .. } => confirmed.extend(content).is_ok(),
        _ => false,
    };
    (state, added)
}

/// `stored` classified when confirmed, as analysis classifies one.
fn classified(stored: Transmission) -> (TransmissionState, bool) {
    match stored.state {
        TransmissionState::Confirmed(confirmed) => (
            TransmissionState::Classified {
                confirmed,
                classification: classification(),
            },
            true,
        ),
        other => (other, false),
    }
}

async fn extend<S>(mut store: S, id: TransmissionId, n: u32) -> Result<(), RaceError>
where
    S: TransmissionStore + Send,
{
    let content = race_match(n)?;
    match store
        .modify(id, |stored| extended(stored, content))
        .await
        .map_err(|error| RaceError::Store(format!("{error:?}")))?
    {
        Some(true) => Ok(()),
        other => Err(RaceError::Lost(format!(
            "extension {n} not applied: {other:?}"
        ))),
    }
}

/// Run one round of the race on `store` and check what it stored.
pub async fn race<S>(store: S, round: u8) -> Result<(), RaceError>
where
    S: TransmissionStore + Clone + Send + Sync + 'static,
{
    let id = race_transmission(round);
    let confirmed = Confirmed::new(
        NonEmpty::new(race_match(0)?),
        Vec::new(),
        Timestamp::from_micros(20),
    )
    .map_err(|error| RaceError::Fixture(format!("{error:?}")))?;
    let mut writer = store.clone();
    writer
        .save(Transmission {
            id,
            to: reader(),
            route: Route::Channel(ChannelId::from_ulid(0xC4A1)),
            opened_at: Timestamp::from_micros(15),
            state: TransmissionState::Confirmed(confirmed),
        })
        .await
        .map_err(|error| RaceError::Store(format!("{error:?}")))?;
    let half = EXTENSIONS / 2;
    let mut tasks = Vec::new();
    for n in 1..=half {
        tasks.push(tokio::spawn(extend(store.clone(), id, n)));
    }
    let (holding, held) = oneshot::channel::<()>();
    let mut classifier = store.clone();
    let classify = tokio::spawn(async move {
        classifier
            .modify(id, move |stored| {
                // The receiver waits for this; a dropped one ends the race anyway.
                let _ = holding.send(());
                std::thread::sleep(HOLD);
                classified(stored)
            })
            .await
    });
    held.await
        .map_err(|_| RaceError::Task("the classification never read".to_owned()))?;
    for n in half + 1..=EXTENSIONS {
        tasks.push(tokio::spawn(extend(store.clone(), id, n)));
    }
    match classify
        .await
        .map_err(|error| RaceError::Task(error.to_string()))?
        .map_err(|error| RaceError::Store(format!("{error:?}")))?
    {
        Some(true) => {}
        other => {
            return Err(RaceError::Lost(format!(
                "classification not applied: {other:?}"
            )));
        }
    }
    for task in tasks {
        task.await
            .map_err(|error| RaceError::Task(error.to_string()))??;
    }
    let stored = store
        .transmission(id)
        .await
        .map_err(|error| RaceError::Store(format!("{error:?}")))?
        .ok_or_else(|| RaceError::Lost("the transmission".to_owned()))?;
    let TransmissionState::Classified { confirmed, .. } = &stored.state else {
        return Err(RaceError::Lost(format!(
            "the classification: stored {:?}",
            stored.state
        )));
    };
    for n in 0..=EXTENSIONS {
        let want = race_match(n)?;
        if !confirmed.content().iter().any(|held| *held == want) {
            return Err(RaceError::Lost(format!("match {n} from the content")));
        }
    }
    Ok(())
}

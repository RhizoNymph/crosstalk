//! `PgTransmissionStore::holding` against Postgres.

use std::collections::BTreeSet;
use std::num::NonZeroU32;

use crosstalk_memory::flow::verdicts::model::transmission_id;
use crosstalk_spec::derived::flow::transmission::{
    Confirmed, DirectCarrier, Route, Transmission, TransmissionState,
};
use crosstalk_spec::derived::provenance::matching::{Carrier, ContentMatch, MatchKind};
use crosstalk_spec::derived::provenance::span::SpanLocation;
use crosstalk_spec::ids::{AgentId, ChannelId, ExchangeId, MessageHash, SpanId};
use crosstalk_spec::interfaces::l5_flow::transmissions::{MatchKey, TransmissionStore};
use crosstalk_spec::observed::message::{PartRef, ToolCallId};
use crosstalk_spec::support::{Blake3, ByteRange, NonEmpty};

use super::support::{Failure, TestResult, at, db, same, transmissions};

fn content(span: u128, part: u16, start: u32, carrier: Carrier) -> Result<ContentMatch, Failure> {
    let range = ByteRange::new(start, start + 10)
        .map_err(|_| Failure::Unexpected("range fixture".to_owned()))?;
    ContentMatch::new(
        SpanId::from_ulid(span),
        AgentId::from_ulid(0x0A6E_0001),
        AgentId::from_ulid(0x0A6E_0002),
        ExchangeId::from_ulid(0xE1),
        SpanLocation {
            part: PartRef {
                message: MessageHash::from_digest(Blake3::from_bytes([9; 32])),
                index: part,
            },
            range,
        },
        carrier,
        MatchKind::Exact,
        NonZeroU32::MIN.saturating_add(9),
    )
    .map_err(|error| Failure::Unexpected(format!("match fixture: {error:?}")))
}

fn confirmed(matches: Vec<ContentMatch>) -> Result<Confirmed, Failure> {
    let mut matches = matches.into_iter();
    let first = matches
        .next()
        .ok_or_else(|| Failure::Unexpected("one match".to_owned()))?;
    let mut confirmed = Confirmed::new(NonEmpty::new(first), Vec::new(), at(10))
        .map_err(|error| Failure::Unexpected(format!("{error:?}")))?;
    for content in matches {
        confirmed
            .extend(content)
            .map_err(|error| Failure::Unexpected(format!("{error:?}")))?;
    }
    Ok(confirmed)
}

/// INV-1013 `surface.conversation.inbound-transmission`: `holding` names
/// the transmission holding each key, follows a later save of the same
/// transmission, and leaves out keys nothing holds.
#[tokio::test(flavor = "multi_thread")]
async fn pg_holding_finds_the_transmission_holding_each_match() -> TestResult {
    let Some(db) = db("pg_holding_finds_the_transmission_holding_each_match").await? else {
        return Ok(());
    };
    let (mut store, _events) = transmissions(db.pool()).await?;
    let tool = || Carrier::ToolResult(ToolCallId("call_1".into()));
    let in_tool = content(1, 0, 0, tool())?;
    let in_tool_again = content(1, 0, 20, tool())?;
    let in_user = content(1, 1, 0, Carrier::UserTurn)?;
    let unheld = content(2, 2, 0, Carrier::UserTurn)?;
    let channel = Transmission {
        id: transmission_id(1),
        to: AgentId::from_ulid(0x0A6E_0002),
        route: Route::Channel(ChannelId::from_ulid(7)),
        opened_at: at(10),
        state: TransmissionState::Confirmed(confirmed(vec![in_tool.clone()])?),
    };
    let direct = Transmission {
        id: transmission_id(2),
        to: AgentId::from_ulid(0x0A6E_0002),
        route: Route::Direct(DirectCarrier::UserTurn),
        opened_at: at(10),
        state: TransmissionState::Confirmed(confirmed(vec![in_user.clone()])?),
    };
    let save = |error| Failure::Unexpected(format!("save: {error:?}"));
    store.save(channel.clone()).await.map_err(save)?;
    store.save(direct).await.map_err(save)?;
    let keys: BTreeSet<MatchKey> = [&in_tool, &in_tool_again, &in_user, &unheld]
        .into_iter()
        .map(MatchKey::of)
        .collect();
    let read = |error| Failure::Unexpected(format!("holding: {error:?}"));
    let held = store.holding(&keys).await.map_err(read)?;
    same("held", &held.len(), &2)?;
    same(
        "tool",
        &held.get(&MatchKey::of(&in_tool)),
        &Some(&transmission_id(1)),
    )?;
    same(
        "user",
        &held.get(&MatchKey::of(&in_user)),
        &Some(&transmission_id(2)),
    )?;
    // The channel transmission is extended by a second match: saving it
    // again keys both.
    let extended = Transmission {
        state: TransmissionState::Confirmed(confirmed(vec![
            in_tool.clone(),
            in_tool_again.clone(),
        ])?),
        ..channel
    };
    store.save(extended).await.map_err(save)?;
    let held = store.holding(&keys).await.map_err(read)?;
    same("held after extend", &held.len(), &3)?;
    same(
        "second tool match",
        &held.get(&MatchKey::of(&in_tool_again)),
        &Some(&transmission_id(1)),
    )?;
    same("unheld", &held.get(&MatchKey::of(&unheld)), &None)?;
    let none = store.holding(&BTreeSet::new()).await.map_err(read)?;
    same("no keys", &none.len(), &0)?;
    Ok(())
}

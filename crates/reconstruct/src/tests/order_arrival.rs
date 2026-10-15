//! The rewind plan (`thread::order::arrival`) and the reorder window: a
//! late exchange rewinds only when every turn to undo is the newest of its
//! conversation and has its call kept; otherwise it is threaded where it
//! arrived, and a store keeps working.

use std::collections::HashMap;
use std::time::Duration;

use crosstalk_spec::ids::{ConversationId, ExchangeId};
use crosstalk_spec::interfaces::l3_reconstruction::Threader;
use crosstalk_spec::observed::client::TrafficSource;
use crosstalk_spec::support::Timestamp;
use crosstalk_testkit::ids::Ids;

use super::support::Scene;
use crate::thread::config::ThreadConfigError;
use crate::thread::order::{Arrival, LaterTurn, Stranded, ThreadCall, arrival};
use crate::thread::store::{RequestKind, TurnKey};
use crate::thread::{MemoryConversations, ReorderWindow};

fn later(
    ids: &mut Ids,
    seq: u64,
    ended: u64,
    conversation: ConversationId,
    turn: u32,
) -> LaterTurn {
    LaterTurn {
        seq,
        key: TurnKey {
            ended_at: Timestamp::from_micros(ended),
            exchange: ids.exchange(),
        },
        conversation,
        turn,
        first_ordinal: turn * 2,
    }
}

fn call(ids: &mut Ids, turn: &LaterTurn) -> ThreadCall {
    ThreadCall {
        exchange: turn.key.exchange,
        at: turn.key.ended_at,
        ended_at: turn.key.ended_at,
        agent: ids.agent(),
        request: Vec::new(),
        kind: RequestKind::FullHistory,
        output: None,
        response: None,
        summary: None,
        conversation: turn.conversation,
        source: TrafficSource::Live,
    }
}

#[test]
fn nothing_later_is_in_order() {
    assert_eq!(
        arrival(Vec::new(), &HashMap::new(), HashMap::new()),
        Arrival::InOrder
    );
}

/// Undone newest first, threaded again in key order.
#[test]
fn late_arrival_undoes_newest_first_and_replays_by_key() {
    let mut ids = Ids::new();
    let (first, second) = (ids.conversation(), ids.conversation());
    let turns = vec![
        later(&mut ids, 10, 300, first, 1),
        later(&mut ids, 11, 200, second, 0),
        later(&mut ids, 12, 100, first, 2),
    ];
    let calls: HashMap<ExchangeId, ThreadCall> = turns
        .iter()
        .map(|turn| (turn.key.exchange, call(&mut ids, turn)))
        .collect();
    let counts = HashMap::from([(first, 3), (second, 1)]);
    let Arrival::Late(rewind) = arrival(turns.clone(), &counts, calls) else {
        panic!("not a rewind");
    };
    let undone: Vec<u64> = rewind.undo().iter().map(|turn| turn.seq).collect();
    assert_eq!(undone, vec![12, 11, 10]);
    let replayed: Vec<u64> = rewind
        .replay()
        .iter()
        .map(|call| call.ended_at.as_micros())
        .collect();
    assert_eq!(replayed, vec![100, 200, 300]);
}

/// A conversation with a turn after the later ones that is not among
/// them (another cluster's, after an unmerge) is not rewound.
#[test]
fn a_turn_outside_the_cluster_strands_the_arrival() {
    let mut ids = Ids::new();
    let conversation = ids.conversation();
    let turns = vec![later(&mut ids, 10, 300, conversation, 1)];
    let calls = turns
        .iter()
        .map(|turn| (turn.key.exchange, call(&mut ids, turn)))
        .collect();
    let counts = HashMap::from([(conversation, 3)]);
    assert_eq!(
        arrival(turns, &counts, calls),
        Arrival::Stranded(Stranded::Interleaved { conversation })
    );
}

/// A later turn whose call was pruned is not rewound.
#[test]
fn a_forgotten_call_strands_the_arrival() {
    let mut ids = Ids::new();
    let conversation = ids.conversation();
    let turns = vec![
        later(&mut ids, 10, 300, conversation, 0),
        later(&mut ids, 11, 400, conversation, 1),
    ];
    let calls = turns
        .iter()
        .skip(1)
        .map(|turn| (turn.key.exchange, call(&mut ids, turn)))
        .collect();
    let counts = HashMap::from([(conversation, 2)]);
    assert_eq!(
        arrival(turns, &counts, calls),
        Arrival::Stranded(Stranded::Forgotten { missing: 1 })
    );
}

#[test]
fn reorder_window_checks() {
    assert_eq!(
        ReorderWindow::default().as_duration(),
        Duration::from_secs(24 * 60 * 60)
    );
    assert_eq!(
        ReorderWindow::new(Duration::ZERO),
        Err(ThreadConfigError::ZeroReorderWindow)
    );
    assert_eq!(
        ReorderWindow::new(Duration::from_secs(u64::MAX)),
        Err(ThreadConfigError::ReorderWindowTooLong)
    );
    let window = ReorderWindow::new(Duration::from_secs(1)).expect("a window");
    assert_eq!(
        window.before(Timestamp::from_micros(3_000_000)),
        Timestamp::from_micros(2_000_000)
    );
    assert_eq!(
        window.before(Timestamp::from_micros(5)),
        Timestamp::from_micros(0)
    );
}

/// With a one-second window, an exchange arriving after one that ended
/// more than a second after a kept call's exchange finds that call pruned
/// and is threaded where it arrived; every exchange is still threaded and
/// recorded, and a later late arrival still rewinds what it can.
#[tokio::test]
async fn beyond_the_window_threads_where_it_arrived() {
    let mut scene = Scene::new();
    let agent = scene.ids.agent();
    let mut history = vec![scene.user("start").await];
    let mut exchanges = Vec::new();
    for step in 0..4 {
        let output = scene.assistant(&format!("step {step}")).await;
        exchanges.push(scene.exchange(history.clone(), output));
        history.push(output);
        history.push(scene.tool("c", &format!("result {step}")).await);
    }
    let window = ReorderWindow::new(Duration::from_secs(1)).expect("a window");
    let store = MemoryConversations::new().with_reorder_window(window);
    let mut threader = scene.threader(store.clone());
    for index in [1, 3, 0, 2] {
        threader
            .thread(&exchanges[index], agent)
            .await
            .expect("threads");
    }
    for exchange in &exchanges {
        let again = threader.thread(exchange, agent).await.expect("recorded");
        assert_eq!(again.delta().exchange, exchange.meta.id);
    }
}

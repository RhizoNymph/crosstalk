//! Delivery order end to end: the wiki relay with B's first exchange
//! (`b1-read`, the third) delivered after its second (`b2-repeat`, the
//! fourth), as the node0 forensic run saw it when an outage cut the third
//! mid-delivery and it was redelivered after the fourth. L2 promises no
//! order (`transport.ordering.unconstrained`); L3's conversation stores
//! thread the two in the order they ended
//! (`reconstruct.thread.order-independent`), so B's exchanges form one
//! conversation and every detection answer is the uninterrupted run's.
//!
//! Each run is compared on its answers and on its threads: every stored
//! conversation through the surface (`QueryApi::conversations`,
//! `conversation_turns`), by agent role, with each turn's exchange, new
//! inputs and output. Before order-tolerant threading the answers already
//! agreed here (B's second exchange scanned B's page either way) but B had
//! two conversations.
//!
//! One difference stays: B's agent is created by the first of its
//! exchanges L3 handles, so its id carries the fourth exchange's
//! millisecond instead of the third's (and so does the id of the
//! conversation the third opens). The comparisons here name the two
//! agents by their role (A, B) and conversations by their turns, and
//! compare everything else exactly;
//! `exact_answers_need_agent_ids_from_the_earliest_exchange` (ignored)
//! states the exact comparison that holds once agent ids are re-anchored.
//!
//! Memory mode needs no database; the Postgres-mode test needs the test
//! database (`TEST_DATABASE_URL`) and skips, passing, without one.
//! Evidence paths: `crosstalk_e2e::reorder::*`.

#[path = "../common/mod.rs"]
mod common;

use std::time::Duration;

use crosstalk_e2e::answers::{Answers, answers};
use crosstalk_e2e::compose_with;
use crosstalk_e2e::read;
use crosstalk_e2e::scenario::{DEFAULT_START, Scenario};
use crosstalk_gateway::live::Ticking;
use crosstalk_memory::support::ManualClock;
use crosstalk_spec::interfaces::l3_reconstruction::conversations::{TurnIndex, TurnWindow};
use crosstalk_spec::interfaces::l8_surface::conversation::ConversationFilter;
use crosstalk_spec::interfaces::l8_surface::{Caller, QueryApi};
use crosstalk_spec::paging::{PageRequest, PageSize};
use crosstalk_spec::support::{Clock, Timestamp};

use common::{
    Volume, assert_same, caller, migrated, passes, read as read_answers, settle, start_generation,
    stop_pg, uninterrupted, within,
};

/// The relay's exchanges in `order`.
fn delivered_in(scenario: &Scenario, order: [usize; 4]) -> Scenario {
    Scenario {
        exchanges: order
            .iter()
            .map(|index| scenario.exchanges[*index].clone())
            .collect(),
        ..scenario.clone()
    }
}

/// B's first exchange after its second.
const THIRD_AFTER_FOURTH: [usize; 4] = [0, 1, 3, 2];

/// Move `clock` to `at` unless it is already later: a redelivered
/// exchange arrives late, the clock does not go back.
fn forward(clock: &ManualClock) -> impl FnMut(Timestamp) + '_ {
    move |at| {
        if at > clock.now() {
            clock.set(at);
        }
    }
}

/// The id token `role: AgentId(<name>)` names in renamed agent answers.
fn role_token(agents: &str, role: &str) -> Option<String> {
    let start = agents.find(&format!("{role}: AgentId("))? + role.len() + 2;
    let end = start + agents[start..].find(')')? + 1;
    Some(agents[start..end].to_owned())
}

/// `answers` renamed (`Answers::renamed`), then the scenario's two agents
/// named by their role.
fn by_role(answers: &Answers) -> Answers {
    let renamed = answers.renamed();
    let roles: Vec<(String, &str)> = [("a", "AgentId(A)"), ("b", "AgentId(B)")]
        .into_iter()
        .filter_map(|(role, name)| role_token(&renamed.agents, role).map(|token| (token, name)))
        .collect();
    let named = |text: &str| {
        roles.iter().fold(text.to_owned(), |text, (token, name)| {
            text.replace(token, name)
        })
    };
    Answers {
        agents: named(&renamed.agents),
        edges: named(&renamed.edges),
        edge_transmissions: named(&renamed.edge_transmissions),
        summaries: named(&renamed.summaries),
        evidence: named(&renamed.evidence),
        channels: named(&renamed.channels),
        transmissions: named(&renamed.transmissions),
        watermark: renamed.watermark,
    }
}

/// Every stored conversation as the surface reads it, one line each: its
/// agent's role, then each turn's index, exchange, how many new inputs it
/// listed and whether it had an output; lines sorted.
async fn threads<Q: QueryApi + Sync>(surface: &Q, caller: &Caller, scenario: &Scenario) -> String {
    let window = read::window(scenario, Duration::from_secs(300)).expect("a window");
    let agents = read::agents(surface, caller, scenario, window)
        .await
        .expect("agents")
        .agents
        .expect("A and B");
    let size = PageSize::new(100).expect("a page size");
    let page = surface
        .conversations(
            caller,
            &ConversationFilter::default(),
            &PageRequest { size, after: None },
        )
        .await
        .expect("conversations");
    let mut lines = Vec::new();
    for row in page.items() {
        let role = if row.agent == agents.a {
            "a"
        } else if row.agent == agents.b {
            "b"
        } else {
            "?"
        };
        let turns = surface
            .conversation_turns(
                caller,
                row.id,
                &TurnWindow {
                    from: TurnIndex(0),
                    size,
                },
            )
            .await
            .expect("turns")
            .expect("a listed conversation is stored");
        let turns: Vec<String> = turns
            .turns
            .iter()
            .map(|turn| {
                format!(
                    "{} {:?} inputs {} output {}",
                    turn.index.0,
                    turn.exchange,
                    turn.inputs.len(),
                    turn.output.is_some()
                )
            })
            .collect();
        lines.push(format!("{role}: {turns:?}"));
    }
    lines.sort();
    lines.join("\n")
}

/// What a run is compared on at one pass.
struct Run {
    answers: Answers,
    threads: String,
}

/// The memory composition fed `order`, read at each of `passes`.
async fn memory_run(scenario: &Scenario, order: [usize; 4]) -> Vec<Run> {
    let memory = compose_with(scenario.start, Ticking::OnSettle)
        .await
        .expect("the memory composition");
    crosstalk_e2e::feed(
        &delivered_in(scenario, order),
        &memory.pipeline,
        forward(&memory.clock),
    )
    .await
    .expect("fed");
    let mut out = Vec::new();
    for until in passes(scenario) {
        memory.live().settle(until).await.expect("settles");
        let answers = answers(
            memory.surface.as_ref(),
            &memory.caller,
            &memory.stores.transmissions,
            scenario,
            memory.live().watermark(),
        )
        .await
        .expect("answers");
        let threads = threads(memory.surface.as_ref(), &memory.caller, scenario).await;
        out.push(Run { answers, threads });
    }
    memory.shutdown().await;
    out
}

/// The third exchange delivered after the fourth, in memory mode: the
/// threads (one conversation per session, B's opened by its first
/// exchange) and the answers (agents by role, the topology graph, the
/// channel edge's transmissions, every transmission with its summary and
/// evidence, the channels and the watermark) are the uninterrupted run's,
/// at the scenario's end and an hour after.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn third_exchange_after_the_fourth_answers_as_uninterrupted() {
    let scenario = Scenario::wiki_relay(DEFAULT_START);
    let expected = uninterrupted(&scenario).await;
    let in_order = memory_run(&scenario, [0, 1, 2, 3]).await;
    let observed = memory_run(&scenario, THIRD_AFTER_FOURTH).await;
    assert_eq!(
        in_order[0].threads.lines().count(),
        2,
        "one conversation per session: {}",
        in_order[0].threads
    );
    for (pass, ((observed, in_order), expected)) in
        observed.iter().zip(&in_order).zip(&expected).enumerate()
    {
        assert!(
            observed.answers.detected(),
            "pass {pass}: the relay is detected: {}",
            observed.answers.transmissions
        );
        assert_eq!(
            observed.threads, in_order.threads,
            "pass {pass}: the reordered run threads differently"
        );
        assert_eq!(
            by_role(&observed.answers),
            by_role(expected),
            "pass {pass}: the reordered run differs from the uninterrupted one"
        );
    }
}

/// The exact comparison, agent ids included: B's id would have to carry
/// its earliest exchange's millisecond, which needs agent ids re-anchored
/// when an earlier exchange arrives (not done: the later exchange's delta
/// and B's id were published before the earlier one arrived).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "agent ids are minted from the first exchange L3 handles, not the earliest"]
async fn exact_answers_need_agent_ids_from_the_earliest_exchange() {
    let scenario = Scenario::wiki_relay(DEFAULT_START);
    let expected = uninterrupted(&scenario).await;
    let observed = memory_run(&scenario, THIRD_AFTER_FOURTH).await;
    for (pass, (observed, expected)) in observed.iter().zip(&expected).enumerate() {
        assert_same(
            &observed.answers,
            expected,
            &format!("pass {pass}: the reordered run differs from the uninterrupted one"),
        );
    }
}

/// The same in Postgres mode (`Live::<PgSet>::start_pg`): B's first
/// exchange published after its second gives the uninterrupted answers,
/// agents by role.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pg_third_exchange_after_the_fourth_answers_as_uninterrupted() {
    let Some(db) = migrated("pg_third_exchange_after_the_fourth_answers_as_uninterrupted").await
    else {
        return;
    };
    let scenario = Scenario::wiki_relay(DEFAULT_START);
    let expected = uninterrupted(&scenario).await;
    let in_order = memory_run(&scenario, [0, 1, 2, 3]).await;
    let volume = Volume::new(scenario.start);
    let live = start_generation(&db, &volume, 0).await;
    let clock = volume.clock.clone();
    within(
        &live,
        "feeding the reordered scenario",
        crosstalk_e2e::feed(
            &delivered_in(&scenario, THIRD_AFTER_FOURTH),
            live.pipeline(),
            forward(&clock),
        ),
    )
    .await
    .expect("fed");
    for (pass, until) in passes(&scenario).into_iter().enumerate() {
        settle(&live, until).await;
        let observed = read_answers(&live, &scenario).await;
        let caller = caller(&live).await;
        let threaded = within(
            &live,
            "the threads",
            threads(live.surface().as_ref(), &caller, &scenario),
        )
        .await;
        assert_eq!(
            threaded, in_order[pass].threads,
            "pass {pass}: the reordered Postgres-mode run threads differently"
        );
        assert_eq!(
            by_role(&observed),
            by_role(&expected[pass]),
            "pass {pass}: the reordered Postgres-mode run differs from the uninterrupted one"
        );
    }
    stop_pg(live).await;
    db.close().await.expect("drops");
}

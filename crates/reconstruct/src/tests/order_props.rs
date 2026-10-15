//! `reconstruct.thread.order-independent`: what a conversation store holds
//! for a set of exchanges does not depend on the order they were delivered
//! in. The transport promises no order (a retry, a nack, a second take or
//! a delivery cut by an outage and redelivered after later ones), so the
//! same exchanges in any order must leave the same conversations, turns,
//! transcripts and recorded outcomes as delivering them in the order they
//! ended.
//!
//! Generated harness scripts on the in-memory store, each delivered in
//! several seeded permutations (and redelivered), against the same script
//! delivered in order; and the node0 forensic case (an outage cut a
//! session's first exchange, redelivered after its second) through the
//! threader and through the whole consumer.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use crosstalk_spec::ids::{AgentId, ExchangeId};
use crosstalk_spec::interfaces::l3_reconstruction::Threader;
use crosstalk_spec::interfaces::l3_reconstruction::conversations::ThreadOutcomeKind;
use crosstalk_spec::observed::client::ClientContext;
use crosstalk_spec::observed::exchange::Exchange;
use crosstalk_testkit::build::ExchangeBuilder;
use proptest::prelude::*;
use proptest::test_runner::TestCaseError;

use super::order_view::{L3View, view};
use super::rig::Rig;
use super::support::{Scene, ulids};
use super::thread_props::property;
use super::thread_props::script::{Harness, Mix, script};
use crate::consumer::Handled;
use crate::thread::{ConversationThreader, MemoryConversations, ReadsMembers};

/// A permutation of `0..len` drawn from `seed` (xorshift, Fisher-Yates).
pub(crate) fn permutation(len: usize, seed: u64) -> Vec<usize> {
    let mut order: Vec<usize> = (0..len).collect();
    let mut state = seed | 1;
    for last in (1..len).rev() {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        let pick = usize::try_from(state % (last as u64 + 1)).unwrap_or(0);
        order.swap(last, pick);
    }
    order
}

fn fail(what: impl std::fmt::Debug) -> TestCaseError {
    TestCaseError::fail(format!("{what:?}"))
}

/// `exchanges` threaded in `order` on a fresh memory store, then read back.
async fn delivered(
    harness: &Harness,
    exchanges: &[(Exchange, AgentId)],
    order: &[usize],
) -> Result<L3View, TestCaseError> {
    let store = MemoryConversations::new();
    let mut threader = harness.scene.threader_in(store.clone(), harness.clusters());
    for index in order {
        let (exchange, agent) = &exchanges[*index];
        threader
            .thread(exchange, *agent)
            .await
            .map_err(|error| fail((index, error)))?;
    }
    view(&store, &mut threader, exchanges).await.map_err(fail)
}

/// Every permutation drawn for a script stores what the in-order delivery
/// stores.
fn permutations(mix: Mix, cases: u32) {
    let strategy = (script(mix, 7), proptest::collection::vec(any::<u64>(), 3));
    property(cases, strategy, |(ops, seeds)| async move {
        let mut harness = Harness::new();
        let mut exchanges = Vec::new();
        for op in &ops {
            let step = harness.step(op).await;
            exchanges.push((step.exchange, step.agent));
        }
        let in_order: Vec<usize> = (0..exchanges.len()).collect();
        let expected = delivered(&harness, &exchanges, &in_order).await?;
        for seed in seeds {
            let order = permutation(exchanges.len(), seed);
            let got = delivered(&harness, &exchanges, &order).await?;
            if got != expected {
                return Err(TestCaseError::fail(format!(
                    "delivered in {order:?}:\n  got      {got:#?}\n  expected {expected:#?}"
                )));
            }
        }
        Ok(())
    });
}

/// `reconstruct.thread.order-independent`: continuations, retries,
/// branches, rewrites, system changes, failures and compactions.
#[test]
fn any_delivery_order_stores_the_in_order_conversations() {
    permutations(Mix::GENERAL, 64);
}

/// `reconstruct.thread.order-independent`: forks (retries, branches and
/// rewrites) delivered out of order.
#[test]
fn any_delivery_order_stores_the_in_order_forks() {
    permutations(Mix::FORKS, 64);
}

/// `reconstruct.thread.order-independent`: compactions (chained ones
/// included) delivered out of order.
#[test]
fn any_delivery_order_stores_the_in_order_compactions() {
    permutations(Mix::COMPACTIONS, 64);
}

/// The permutations cover every order of a short script, the identity
/// included.
#[test]
fn permutation_draws_every_order_of_three() {
    let orders: BTreeSet<Vec<usize>> = (0..512u64).map(|seed| permutation(3, seed)).collect();
    assert_eq!(orders.len(), 6, "{orders:?}");
    for order in &orders {
        let mut sorted = order.clone();
        sorted.sort_unstable();
        assert_eq!(sorted, vec![0, 1, 2]);
    }
}

/// The wiki relay's four exchanges as the node0 forensic run saw them: A
/// writes the page (two exchanges), B reads it and answers (two
/// exchanges), each later exchange of a session carrying the history
/// before it.
pub(crate) async fn relay(
    scene: &mut Scene,
    a: &ClientContext,
    b: &ClientContext,
) -> Vec<Exchange> {
    let a_ask = scene.user("publish the rollback runbook").await;
    let a_call = scene.assistant("I'll publish it").await;
    let a_saved = scene.tool("write", "saved").await;
    let a_done = scene.assistant("published").await;
    let b_ask = scene.user("what has to happen before a rollback?").await;
    let b_call = scene.assistant("let me read the runbook").await;
    let b_page = scene.tool("read", "the runbook page").await;
    let b_answer = scene.assistant("freeze the ledger first").await;
    let mut exchanges = Vec::new();
    for (client, request, output) in [
        (a, vec![a_ask], a_call),
        (a, vec![a_ask, a_call, a_saved], a_done),
        (b, vec![b_ask], b_call),
        (b, vec![b_ask, b_call, b_page], b_answer),
    ] {
        let at = scene.tick();
        exchanges.push(
            ExchangeBuilder::new(&mut scene.ids)
                .started_at(at)
                .client(client.clone())
                .request(request)
                .response(output)
                .build(),
        );
    }
    exchanges
}

/// The forensic case on the threader: B's first exchange delivered after
/// its second stores what in-order delivery stores, one conversation per
/// session, B's opened by its first exchange.
#[tokio::test]
async fn third_exchange_after_the_fourth_stores_the_in_order_conversations() {
    let mut scene = Scene::new();
    let a = ExchangeBuilder::new(&mut scene.ids).build().meta.client;
    let b = ExchangeBuilder::new(&mut scene.ids).build().meta.client;
    let agents = [scene.ids.agent(), scene.ids.agent()];
    let exchanges: Vec<(Exchange, AgentId)> = relay(&mut scene, &a, &b)
        .await
        .into_iter()
        .zip([agents[0], agents[0], agents[1], agents[1]])
        .collect();
    let mut views = Vec::new();
    for order in [[0, 1, 2, 3], [0, 1, 3, 2]] {
        let store = MemoryConversations::new();
        let mut threader = scene.threader(store.clone());
        for index in order {
            let (exchange, agent) = &exchanges[index];
            threader.thread(exchange, *agent).await.expect("threads");
        }
        views.push(
            view(&store, &mut threader, &exchanges)
                .await
                .expect("reads"),
        );
    }
    assert_eq!(views[1], views[0]);
    let in_order = &views[0];
    assert_eq!(in_order.conversation_count(), 2);
    let b_first = exchanges[2].0.meta.id;
    let b_second = exchanges[3].0.meta.id;
    let b_conversation = in_order
        .conversations
        .get(&b_first)
        .expect("B's first exchange opens its conversation");
    let turns: Vec<ExchangeId> = b_conversation
        .turns
        .iter()
        .map(|turn| turn.exchange)
        .collect();
    assert_eq!(turns, vec![b_first, b_second]);
    assert_eq!(
        in_order.outcomes.get(&b_second).map(|outcome| outcome.kind),
        Some(ThreadOutcomeKind::Extends)
    );
}

/// A linear session whose middle exchange arrives last: it joins the
/// conversation between the turns around it, and the last turn's delta
/// lists only what it added.
#[tokio::test]
async fn late_middle_turn_joins_between_its_neighbours() {
    let mut scene = Scene::new();
    let agent = scene.ids.agent();
    let u1 = scene.user("one").await;
    let a1 = scene.assistant("reply one").await;
    let u2 = scene.tool("c", "two").await;
    let a2 = scene.assistant("reply two").await;
    let u3 = scene.tool("c", "three").await;
    let a3 = scene.assistant("reply three").await;
    let exchanges: Vec<(Exchange, AgentId)> = vec![
        (scene.exchange(vec![u1], a1), agent),
        (scene.exchange(vec![u1, a1, u2], a2), agent),
        (scene.exchange(vec![u1, a1, u2, a2, u3], a3), agent),
    ];
    let store = MemoryConversations::new();
    let mut threader = scene.threader(store.clone());
    for index in [0, 2, 1] {
        let (exchange, agent) = &exchanges[index];
        threader.thread(exchange, *agent).await.expect("threads");
    }
    let got = view(&store, &mut threader, &exchanges)
        .await
        .expect("reads");
    assert_eq!(got.conversation_count(), 1);
    let last = got
        .outcomes
        .get(&exchanges[2].0.meta.id)
        .expect("the last exchange is recorded");
    assert_eq!(last.kind, ThreadOutcomeKind::Extends);
    assert_eq!(last.new_inputs, vec![u3]);
    let middle = got
        .outcomes
        .get(&exchanges[1].0.meta.id)
        .expect("the middle exchange is recorded");
    assert_eq!(middle.new_inputs, vec![u2]);
}

/// Each agent named by the first exchange (in id order) attributed to
/// it, so two runs that minted different agent ids compare.
fn agents_by_first_exchange(view: &L3View) -> BTreeMap<AgentId, ExchangeId> {
    let mut first: BTreeMap<AgentId, ExchangeId> = BTreeMap::new();
    for (exchange, outcome) in &view.outcomes {
        first
            .entry(outcome.agent)
            .and_modify(|seen| *seen = (*seen).min(*exchange))
            .or_insert(*exchange);
    }
    first
}

/// A conversation as (opening exchange, each turn's exchange and outcome,
/// its agent named by the agent's first exchange).
type Shape = (ExchangeId, Vec<(ExchangeId, ThreadOutcomeKind)>, ExchangeId);

/// The view's conversations as [`Shape`]s.
fn shape(view: &L3View) -> Vec<Shape> {
    let agents = agents_by_first_exchange(view);
    view.conversations
        .iter()
        .map(|(opening, conversation)| {
            (
                *opening,
                conversation
                    .turns
                    .iter()
                    .map(|turn| (turn.exchange, turn.outcome))
                    .collect(),
                agents.get(&conversation.agent).copied().unwrap_or(*opening),
            )
        })
        .collect()
}

/// The forensic case through the whole consumer (attribution, then
/// threading): the third exchange delivered after the fourth leaves the
/// in-order conversations and turns, and B's exchanges are attributed to
/// one agent.
#[tokio::test]
async fn consumer_threads_a_late_first_exchange_as_in_order() {
    let mut shapes = Vec::new();
    for order in [[0, 1, 2, 3], [0, 1, 3, 2]] {
        let mut rig = Rig::new();
        let a = ExchangeBuilder::new(&mut rig.scene.ids).build().meta.client;
        let b = ExchangeBuilder::new(&mut rig.scene.ids).build().meta.client;
        let exchanges = relay(&mut rig.scene, &a, &b).await;
        let mut attributed = Vec::new();
        for index in order {
            match rig.deliver(&exchanges[index]).await.expect("handled") {
                Handled::Threaded { agent, .. } => attributed.push((index, agent)),
                other => panic!("exchange {index}: {other:?}"),
            }
        }
        attributed.sort_by_key(|(index, _)| *index);
        let placed: Vec<(Exchange, AgentId)> = attributed
            .iter()
            .map(|(index, agent)| (exchanges[*index].clone(), *agent))
            .collect();
        let mut reader = ConversationThreader::new(
            rig.conversations.clone(),
            Arc::clone(&rig.scene.messages),
            ReadsMembers(rig.agents.clone()),
            ulids(99),
        );
        let got = view(&rig.conversations, &mut reader, &placed)
            .await
            .expect("reads");
        assert_eq!(got.conversation_count(), 2, "order {order:?}: {got:#?}");
        shapes.push(shape(&got));
    }
    assert_eq!(shapes[1], shapes[0]);
}

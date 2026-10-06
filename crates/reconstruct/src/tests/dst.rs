//! Deterministic simulations of the consumer and the threader
//! (`crosstalk-sim`): each seed draws a delivery schedule (which exchanges
//! are delivered how many times, in what order), and concurrent threaders
//! interleave on the simulation's single-threaded, paused-time runtime.
//! A failure names its seed (`CROSSTALK_SIM_SEED=<n>` reruns it).

use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroU64;

use crosstalk_sim::{CheckFailed, SimConfig, SimCtx, SimRng, sim_test};
use crosstalk_spec::events::BusEvent;
use crosstalk_spec::events::ingest::IngestEvent;
use crosstalk_spec::ids::{AgentId, EventId, ExchangeId, SecretVersion};
use crosstalk_spec::interfaces::l3_reconstruction::{ThreadOutcome, Threader};
use crosstalk_spec::observed::client::{ClientContext, PreviousDigests};
use crosstalk_spec::observed::exchange::Exchange;
use crosstalk_testkit::build::ExchangeBuilder;

use super::rig::Rig;
use super::support::Scene;
use super::thread_props::oracle::snapshot;
use crate::consumer::Handled;
use crate::thread::{MemoryConversations, outcome_kind};

fn config() -> SimConfig {
    SimConfig::default()
}

fn fail(message: impl Into<String>) -> CheckFailed {
    CheckFailed::new(message)
}

fn below(rng: &mut SimRng, bound: u64) -> u64 {
    NonZeroU64::new(bound).map_or(0, |bound| rng.below(bound))
}

/// Exchanges from two callers, each continuing its own history.
async fn traffic(rig: &mut Rig, count: usize) -> Vec<Exchange> {
    let callers: Vec<ClientContext> = (0..2)
        .map(|_| ExchangeBuilder::new(&mut rig.scene.ids).build().meta.client)
        .collect();
    let mut histories = [Vec::new(), Vec::new()];
    let mut exchanges = Vec::new();
    for n in 0..count {
        let who = n % 2;
        let user = rig.scene.user(&format!("turn {n}")).await;
        let output = rig.scene.assistant(&format!("reply {n}")).await;
        histories[who].push(user);
        let at = rig.scene.tick();
        exchanges.push(
            ExchangeBuilder::new(&mut rig.scene.ids)
                .started_at(at)
                .client(callers[who].clone())
                .request(histories[who].clone())
                .response(output)
                .build(),
        );
        histories[who].push(output);
    }
    exchanges
}

/// A seeded schedule: every exchange delivered one to three times, the
/// first deliveries in order (a harness sends in order), the duplicates
/// anywhere after their original.
fn schedule(rng: &mut SimRng, count: usize) -> Vec<usize> {
    let mut order: Vec<usize> = (0..count).collect();
    for original in 0..count {
        for _ in 0..below(rng, 3) {
            let after = order.iter().position(|n| *n == original).unwrap_or(0) + 1;
            let at = after + below(rng, (order.len() - after + 1) as u64) as usize;
            order.insert(at.min(order.len()), original);
        }
    }
    order
}

/// `reconstruct.agent-seen.once-per-evidence`: however often exchanges are
/// redelivered, each piece of evidence is announced once per agent, and
/// every piece an agent holds was announced.
#[test]
fn dst_agent_seen_once_per_evidence() {
    sim_test(
        "dst_agent_seen_once_per_evidence",
        &config(),
        |ctx: SimCtx| async move {
            let mut rig = Rig::new();
            let exchanges = traffic(&mut rig, 8).await;
            let mut rng = ctx.rng();
            for n in schedule(&mut rng, exchanges.len()) {
                rig.deliver(&exchanges[n])
                    .await
                    .map_err(|e| fail(format!("{e}")))?;
            }
            let mut announced: BTreeMap<(AgentId, String), usize> = BTreeMap::new();
            for event in rig.store_events() {
                if let BusEvent::Ingest(IngestEvent::AgentSeen { agent, evidence }) = event {
                    *announced
                        .entry((agent, format!("{evidence:?}")))
                        .or_default() += 1;
                }
            }
            ctx.check(announced.values().all(|count| *count == 1), || {
                format!("announced more than once: {announced:?}")
            })?;
            let agents: BTreeSet<AgentId> = announced.keys().map(|(agent, _)| *agent).collect();
            for agent in agents {
                let cluster =
                    crosstalk_spec::interfaces::l3_reconstruction::agents::AgentReads::cluster(
                        &rig.agents,
                        agent,
                    )
                    .await
                    .map_err(|e| fail(format!("{e:?}")))?
                    .ok_or_else(|| fail("agent missing"))?;
                for item in cluster.agent().evidence.iter() {
                    let key = (agent, format!("{item:?}"));
                    ctx.check(announced.contains_key(&key), || {
                        format!("{item:?} never announced")
                    })?;
                }
            }
            Ok(())
        },
    );
}

/// `reconstruct.delta.single-envelope-per-exchange`: every delivery of an
/// exchange publishes its delta under one envelope id, with one content.
#[test]
fn dst_one_delta_envelope_per_exchange() {
    sim_test(
        "dst_one_delta_envelope_per_exchange",
        &config(),
        |ctx: SimCtx| async move {
            let mut rig = Rig::new();
            let exchanges = traffic(&mut rig, 8).await;
            let mut rng = ctx.rng();
            for n in schedule(&mut rng, exchanges.len()) {
                rig.deliver(&exchanges[n])
                    .await
                    .map_err(|e| fail(format!("{e}")))?;
            }
            let mut by_exchange: BTreeMap<ExchangeId, BTreeSet<(EventId, String)>> =
                BTreeMap::new();
            for (id, delta) in rig.bus.deltas() {
                by_exchange
                    .entry(delta.exchange)
                    .or_default()
                    .insert((id, format!("{delta:?}")));
            }
            ctx.check(by_exchange.len() == exchanges.len(), || {
                "an exchange has no delta".to_owned()
            })?;
            for (exchange, envelopes) in &by_exchange {
                ctx.check(envelopes.len() == 1, || {
                    format!(
                        "{exchange:?} published under {} envelopes or contents",
                        envelopes.len()
                    )
                })?;
            }
            Ok(())
        },
    );
}

/// `reconstruct.thread.rethread-idempotent`: a redelivered exchange is
/// threaded as the first time and changes no stored conversation.
#[test]
fn dst_rethreading_is_idempotent() {
    sim_test(
        "dst_rethreading_is_idempotent",
        &config(),
        |ctx: SimCtx| async move {
            let mut rig = Rig::new();
            let exchanges = traffic(&mut rig, 8).await;
            let mut rng = ctx.rng();
            let mut first: BTreeMap<usize, ThreadOutcome> = BTreeMap::new();
            for n in schedule(&mut rng, exchanges.len()) {
                let before = snapshot(&rig.conversations)
                    .await
                    .map_err(|e| fail(format!("{e:?}")))?;
                let handled = rig
                    .deliver(&exchanges[n])
                    .await
                    .map_err(|e| fail(format!("{e}")))?;
                let Handled::Threaded { outcome, .. } = handled else {
                    return Err(fail(format!("not threaded: {handled:?}")));
                };
                match first.get(&n) {
                    None => {
                        first.insert(n, outcome);
                    }
                    Some(original) => {
                        ctx.check(*original == outcome, || {
                            format!("exchange {n} rethreaded differently")
                        })?;
                        let after = snapshot(&rig.conversations)
                            .await
                            .map_err(|e| fail(format!("{e:?}")))?;
                        ctx.check(before == after, || {
                            format!("redelivering {n} changed conversations")
                        })?;
                    }
                }
            }
            Ok(())
        },
    );
}

/// `reconstruct.resolve.secret-rotation-keeps-agent`: an agent seen before
/// a secret rotation, then during its overlap (current and previous
/// digests), is the same agent after the overlap ends, in any delivery
/// order within each phase.
#[test]
fn dst_secret_rotation_keeps_agent() {
    sim_test(
        "dst_secret_rotation_keeps_agent",
        &config(),
        |ctx: SimCtx| async move {
            let mut rig = Rig::new();
            let old = ExchangeBuilder::new(&mut rig.scene.ids).build().meta.client;
            let mut overlap = old.clone();
            let mut after = old.clone();
            let rotated = crosstalk_spec::ids::CredentialHash::from_keyed_digest(
                SecretVersion(2),
                rig.scene.ids.digest(),
            );
            if let (Some(mut credential), Some(previous)) = (old.credential, old.credential) {
                credential.hash = rotated;
                overlap.credential = Some(credential);
                overlap.previous_digests = Some(PreviousDigests {
                    credential: Some(previous.hash),
                    account: None,
                });
                after.credential = Some(credential);
            }
            let mut rng = ctx.rng();
            let mut agents = BTreeSet::new();
            for (phase, client) in [old, overlap, after].iter().enumerate() {
                let mut batch = Vec::new();
                for n in 0..3 {
                    let user = rig.scene.user(&format!("phase {phase} turn {n}")).await;
                    let output = rig
                        .scene
                        .assistant(&format!("phase {phase} reply {n}"))
                        .await;
                    let at = rig.scene.tick();
                    batch.push(
                        ExchangeBuilder::new(&mut rig.scene.ids)
                            .started_at(at)
                            .client(client.clone())
                            .request(vec![user])
                            .response(output)
                            .build(),
                    );
                }
                rng.shuffle(&mut batch);
                for exchange in &batch {
                    match rig
                        .deliver(exchange)
                        .await
                        .map_err(|e| fail(format!("{e}")))?
                    {
                        Handled::Threaded { agent, .. } => {
                            agents.insert(agent);
                        }
                        other => return Err(fail(format!("phase {phase}: {other:?}"))),
                    }
                }
            }
            ctx.check(agents.len() == 1, || {
                format!("the rotation split the agent: {agents:?}")
            })?;
            Ok(())
        },
    );
}

/// `reconstruct.thread.serializable`: threaders sharing one store, their
/// calls interleaved by the seed, give the outcomes of threading the same
/// exchanges one at a time in the order the store took them.
#[test]
fn dst_concurrent_threading_is_serializable() {
    sim_test(
        "dst_concurrent_threading_is_serializable",
        &config(),
        |ctx: SimCtx| async move {
            // Build the exchanges once: two harness lines and a fork of one.
            let mut scene = Scene::new();
            let agent = scene.ids.agent();
            let mut exchanges = Vec::new();
            for line in 0..2 {
                let mut history = vec![scene.user(&format!("line {line}")).await];
                for step in 0..3 {
                    let output = scene.assistant(&format!("line {line} step {step}")).await;
                    exchanges.push(scene.exchange(history.clone(), output));
                    history.push(output);
                    history.push(scene.tool("c", &format!("line {line} result {step}")).await);
                }
            }
            let mut rng = ctx.rng();
            let store = MemoryConversations::new();
            let committed = std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
            let mut tasks = Vec::new();
            for (index, exchange) in exchanges.iter().cloned().enumerate() {
                let mut threader = scene.threader(store.clone());
                let delay = std::time::Duration::from_millis(below(&mut rng, 20));
                let committed = std::rc::Rc::clone(&committed);
                tasks.push(async move {
                    tokio::time::sleep(delay).await;
                    let outcome = threader.thread(&exchange, agent).await;
                    committed.borrow_mut().push((index, outcome));
                });
            }
            futures_join(tasks).await;
            let committed = committed.borrow().clone();
            // Serial replay in commit order, on a fresh store.
            let serial = MemoryConversations::new();
            for (index, concurrent) in &committed {
                let mut threader = scene.threader(serial.clone());
                let concurrent = concurrent.as_ref().map_err(|e| fail(format!("{e:?}")))?;
                let replayed = threader
                    .thread(&exchanges[*index], agent)
                    .await
                    .map_err(|e| fail(format!("{e:?}")))?;
                let shape = |outcome: &ThreadOutcome| {
                    let delta = outcome.delta();
                    (
                        outcome_kind(outcome),
                        delta.new_inputs.clone(),
                        delta.new_system,
                        delta.output,
                    )
                };
                ctx.check(shape(concurrent) == shape(&replayed), || {
                    format!("exchange {index}: concurrent {concurrent:?}, serial {replayed:?}")
                })?;
            }
            let stored = snapshot(&store).await.map_err(|e| fail(format!("{e:?}")))?;
            let replayed = snapshot(&serial)
                .await
                .map_err(|e| fail(format!("{e:?}")))?;
            let histories = |snapshot: &super::thread_props::oracle::Snapshot| -> BTreeSet<Vec<_>> {
                snapshot
                    .values()
                    .map(|conversation| conversation.messages.clone())
                    .collect()
            };
            ctx.check(histories(&stored) == histories(&replayed), || {
                "stored histories differ from the serial replay".to_owned()
            })?;
            Ok(())
        },
    );
}

/// Run every future of `tasks` to completion on the current task,
/// interleaving them.
async fn futures_join<F: Future<Output = ()> + 'static>(tasks: Vec<F>) {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async move {
            let handles: Vec<_> = tasks.into_iter().map(tokio::task::spawn_local).collect();
            for handle in handles {
                let _ = handle.await;
            }
        })
        .await;
}

/// A bus that fails the publishes a seeded plan names, as a consumer that
/// stops between its store writes and its publishes sees it, and records
/// every envelope it was handed.
#[derive(Debug, Clone, Default)]
struct FlakyBus {
    /// Every envelope handed over and accepted, duplicates included.
    published: std::sync::Arc<std::sync::Mutex<Vec<crosstalk_spec::events::Envelope>>>,
    /// Whether each next publish fails, in order; publishes past the plan
    /// land.
    plan: std::sync::Arc<std::sync::Mutex<std::collections::VecDeque<bool>>>,
}

impl FlakyBus {
    fn published(&self) -> Vec<crosstalk_spec::events::Envelope> {
        self.published
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

impl crosstalk_spec::interfaces::l2_transport::EventBus for FlakyBus {
    type Subscription = super::rig::Silent;

    async fn publish(
        &self,
        envelope: crosstalk_spec::events::Envelope,
    ) -> Result<(), crosstalk_spec::interfaces::l2_transport::BusError> {
        let fails = self
            .plan
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .pop_front()
            .unwrap_or(false);
        if fails {
            return Err(crosstalk_spec::interfaces::l2_transport::BusError::Disconnected);
        }
        self.published
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(envelope);
        Ok(())
    }

    async fn subscribe(
        &self,
        _subjects: &[crosstalk_spec::events::Subject],
        _group: crosstalk_spec::interfaces::l2_transport::ConsumerGroup,
        _retry: crosstalk_spec::interfaces::l2_transport::RetryPolicy,
    ) -> Result<super::rig::Silent, crosstalk_spec::interfaces::l2_transport::BusError> {
        Ok(super::rig::Silent)
    }
}

type FlakyConsumer = crate::consumer::ReconstructConsumer<
    crosstalk_memory::reconstruct::MemoryAgents,
    super::rig::Threader,
    crosstalk_transport::blob::MemoryBlobStore,
    crate::evidence::ChainEvidence,
    crate::ids::UlidSource<crosstalk_spec::ids::mint::SeededRandom>,
    FlakyBus,
>;

/// A consumer over fresh reference stores, publishing on `bus`, with the
/// rig's id seeds and `rig`'s message bodies, and the receiver of what its
/// agent store publishes.
fn flaky_consumer(
    rig: &Rig,
    bus: &FlakyBus,
) -> (
    FlakyConsumer,
    tokio::sync::mpsc::UnboundedReceiver<BusEvent>,
) {
    use crosstalk_memory::support::{IdSequence, Outbox};
    let (outbox, store_events) = Outbox::channel();
    let agents = crosstalk_memory::reconstruct::MemoryAgents::new(IdSequence::default(), outbox);
    let threader = crate::thread::ConversationThreader::new(
        MemoryConversations::new(),
        std::sync::Arc::clone(&rig.scene.messages),
        crate::thread::ReadsMembers(agents.clone()),
        super::support::ulids(11),
    );
    let consumer = crate::consumer::ReconstructConsumer::new(crate::consumer::ConsumerParts {
        agents,
        threader,
        messages: std::sync::Arc::clone(&rig.scene.messages),
        deriver: crate::evidence::ChainEvidence::default(),
        agent_ids: super::support::ulids(13),
        bus: std::sync::Arc::new(bus.clone()),
    });
    (consumer, store_events)
}

/// Deliver `order` to `consumer`; a delivery that fails is delivered again
/// at a seeded later point, as the bus redelivers a nacked delivery.
/// Returns every delivery made, in order, failed ones included.
async fn deliver_until_handled(
    rig: &mut Rig,
    consumer: &mut FlakyConsumer,
    exchanges: &[Exchange],
    order: Vec<usize>,
    rng: &mut SimRng,
) -> Result<Vec<usize>, CheckFailed> {
    let mut queue: std::collections::VecDeque<usize> = order.into();
    let mut delivered = Vec::new();
    while let Some(n) = queue.pop_front() {
        delivered.push(n);
        let envelope = rig.captured(&exchanges[n]);
        match consumer.handle(&envelope).await {
            Ok(_) => {}
            Err(crate::consumer::ConsumeError::Publish(_)) => {
                let at = below(rng, queue.len() as u64 + 1) as usize;
                queue.insert(at, n);
            }
            Err(error) => return Err(fail(format!("delivery failed: {error}"))),
        }
    }
    Ok(delivered)
}

/// The bus's log of `envelopes` as `PgBus` keeps it: the first event per
/// envelope id. Fails when an id carries two different events.
fn log_of(
    envelopes: &[crosstalk_spec::events::Envelope],
) -> Result<BTreeMap<EventId, String>, CheckFailed> {
    let mut log = BTreeMap::new();
    for envelope in envelopes {
        let event = format!("{:?}", envelope.event);
        match log.get(&envelope.id) {
            Some(held) if *held != event => {
                return Err(fail(format!(
                    "{} carries two events",
                    envelope.id.ulid_text()
                )));
            }
            Some(_) => {}
            None => {
                log.insert(envelope.id, event);
            }
        }
    }
    Ok(log)
}

/// `transport.consumer.derived-envelope-ids` for L3: deliveries whose
/// publishes fail at seeded points (the consumer stopped after its writes)
/// are redelivered until handled. The same deliveries made with every
/// publish landing give exactly the same bus log (each envelope id with
/// its one event) and exactly the same agent store events (`AgentSeen`
/// included: it is staged with the write, so no announcement is lost to a
/// failed publish).
#[test]
fn redelivery_republishes_the_same_envelope_ids() {
    sim_test(
        "redelivery_republishes_the_same_envelope_ids",
        &config(),
        |ctx: SimCtx| async move {
            let mut rig = Rig::new();
            let exchanges = traffic(&mut rig, 8).await;
            let mut rng = ctx.rng();
            let order = schedule(&mut rng, exchanges.len());

            let flaky = FlakyBus::default();
            {
                let mut plan = flaky
                    .plan
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                for _ in 0..64 {
                    plan.push_back(below(&mut rng, 3) == 0);
                }
            }
            let (mut interrupted, mut interrupted_events) = flaky_consumer(&rig, &flaky);
            let delivered =
                deliver_until_handled(&mut rig, &mut interrupted, &exchanges, order, &mut rng)
                    .await?;

            let calm = FlakyBus::default();
            let (mut uninterrupted, mut uninterrupted_events) = flaky_consumer(&rig, &calm);
            let failed = deliver_until_handled(
                &mut rig,
                &mut uninterrupted,
                &exchanges,
                delivered.clone(),
                &mut rng,
            )
            .await?;
            ctx.check(failed == delivered, || {
                "the calm bus failed a publish".to_owned()
            })?;

            let expected = log_of(&calm.published())?;
            let got = log_of(&flaky.published())?;
            ctx.check(got == expected, || {
                format!("the interrupted run's log differs:\n  got:      {got:?}\n  expected: {expected:?}")
            })?;
            let deltas = got
                .values()
                .filter(|event| event.contains("ConversationDelta"))
                .count();
            ctx.check(deltas == exchanges.len(), || {
                format!("{deltas} deltas for {} exchanges", exchanges.len())
            })?;
            let store_events = |receiver: &mut tokio::sync::mpsc::UnboundedReceiver<BusEvent>| {
                crosstalk_memory::support::drain(receiver)
            };
            let got = store_events(&mut interrupted_events);
            let expected = store_events(&mut uninterrupted_events);
            ctx.check(
                got.iter()
                    .any(|event| matches!(event, BusEvent::Ingest(IngestEvent::AgentSeen { .. }))),
                || "no AgentSeen was staged".to_owned(),
            )?;
            ctx.check(got == expected, || {
                "the interrupted run's store events differ".to_owned()
            })?;
            Ok(())
        },
    );
}

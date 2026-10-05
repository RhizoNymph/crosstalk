//! The corpus model: the world builder's checks, the clock, client contexts,
//! new inputs.

mod common;

use common::{calls, dataset, draft, result, says, system, tick, user};
use crosstalk_eval::corpus::client::{corpus_id, synthetic_client, vendor_of};
use crosstalk_eval::corpus::delta::new_inputs;
use crosstalk_eval::corpus::exchange::normalized;
use crosstalk_eval::corpus::{
    CorpusError, Coverage, Driven, HashedMessage, InMemory, TraceSource, WorldBuilder, clock,
};
use crosstalk_eval::corpus::{CorpusExchange, Fidelity};
use crosstalk_eval::keys::{AgentKey, SourceRef, WorldKey};
use crosstalk_eval::truth::Tier;
use crosstalk_spec::interfaces::l1_canonical::InvalidNormalizedExchange;
use crosstalk_spec::observed::client::{CorpusId, CredentialScheme, IngressMode, Vendor};
use crosstalk_spec::observed::exchange::{
    Continuation, Exchange, ExchangeMeta, ExchangeOutcome, ModelName, StopReason, Transport,
    WireProtocol,
};

fn builder() -> (WorldBuilder, AgentKey, AgentKey) {
    let mut world = WorldBuilder::new(dataset(), WorldKey::new("w"));
    let alice = world
        .agent("alice", Driven::Model, "gemini/flash")
        .unwrap_or_else(|e| panic!("{e}"));
    let bob = world
        .agent("bob", Driven::Scripted, "script")
        .unwrap_or_else(|e| panic!("{e}"));
    (world, alice, bob)
}

#[test]
fn exchanges_carry_their_messages_and_time() {
    let (mut world, alice, _) = builder();
    let request = vec![system("sys"), user("hello")];
    let id = world
        .exchange(draft(&alice, 3, request, says("hi there")))
        .unwrap_or_else(|e| panic!("{e}"));
    let world = world.finish(Coverage::Complete {
        tier: Tier::Construction,
    });
    let exchange = world
        .exchange(id)
        .unwrap_or_else(|| panic!("exchange missing"));
    assert_eq!(exchange.at(), tick(3));
    assert_eq!(exchange.exchange().meta.started_at, tick(3));
    assert_eq!(exchange.request().count(), 2);
    assert_eq!(
        exchange.response().map(|m| m.hash),
        Some(says("hi there").hash())
    );
    assert_eq!(exchange.agent(), &alice);
    assert_eq!(exchange.exchange().meta.client, world.agents()[0].client);
}

#[test]
fn repeated_messages_are_stored_once() {
    let (mut world, alice, _) = builder();
    let request = vec![user("same"), user("same"), user("other")];
    let id = world
        .exchange(draft(&alice, 1, request, says("ok")))
        .unwrap_or_else(|e| panic!("{e}"));
    let world = world.finish(Coverage::Partial);
    let exchange = world.exchange(id).unwrap_or_else(|| panic!("missing"));
    assert_eq!(exchange.normalized().messages.len(), 3);
    assert_eq!(exchange.request().count(), 3);
}

#[test]
fn scripted_and_unknown_agents_make_no_exchanges() {
    let (mut world, _, bob) = builder();
    assert!(matches!(
        world.exchange(draft(&bob, 1, vec![], says("x"))),
        Err(CorpusError::ScriptedAgent(_))
    ));
    let carol = AgentKey::new(WorldKey::new("w"), "carol");
    assert!(matches!(
        world.exchange(draft(&carol, 1, vec![], says("x"))),
        Err(CorpusError::UnknownAgent(_))
    ));
    let foreign = AgentKey::new(WorldKey::new("other"), "alice");
    assert!(matches!(
        world.exchange(draft(&foreign, 1, vec![], says("x"))),
        Err(CorpusError::ForeignAgent { .. })
    ));
    assert!(matches!(
        world.agent("bob", Driven::Model, "m"),
        Err(CorpusError::DuplicateAgent(_))
    ));
}

#[test]
fn an_agents_exchanges_move_forward_in_time() {
    let (mut world, alice, _) = builder();
    assert!(
        world
            .exchange(draft(&alice, 5, vec![user("a")], says("b")))
            .is_ok()
    );
    assert!(matches!(
        world.exchange(draft(&alice, 5, vec![user("c")], says("d"))),
        Err(CorpusError::OutOfOrder { .. })
    ));
    assert!(matches!(
        world.exchange(draft(&alice, 4, vec![user("c")], says("d"))),
        Err(CorpusError::OutOfOrder { .. })
    ));
}

#[test]
fn finished_worlds_order_exchanges_by_time() {
    let mut world = WorldBuilder::new(dataset(), WorldKey::new("w"));
    let a = world
        .agent("a", Driven::Model, "m")
        .unwrap_or_else(|e| panic!("{e}"));
    let b = world
        .agent("b", Driven::Model, "m")
        .unwrap_or_else(|e| panic!("{e}"));
    for (agent, at) in [(&a, 9), (&b, 2), (&a, 12), (&b, 7)] {
        assert!(
            world
                .exchange(draft(agent, at, vec![user("q")], says("r")))
                .is_ok()
        );
    }
    let world = world.finish(Coverage::Partial);
    let times: Vec<_> = world.exchanges().iter().map(|e| e.at()).collect();
    assert_eq!(times, vec![tick(2), tick(7), tick(9), tick(12)]);
}

#[test]
fn normalized_rejects_a_missing_message() {
    let hello = user("hello");
    let exchange = Exchange {
        meta: ExchangeMeta {
            id: crosstalk_spec::ids::ExchangeId::from_ulid(1),
            protocol: WireProtocol::OpenAiChat,
            transport: Transport::Http,
            model: ModelName("m".into()),
            client: synthetic_client(&dataset(), &AgentKey::new(WorldKey::new("w"), "a"), "m"),
            started_at: tick(1),
        },
        continuation: Continuation::FullHistory,
        request: vec![hello.hash(), user("absent").hash()],
        outcome: ExchangeOutcome::Completed {
            response: says("r").hash(),
            response_id: None,
            first_chunk_at: tick(1),
            finished_at: tick(1),
            stop: StopReason::EndTurn,
            usage: None,
        },
    };
    let normalized = normalized(exchange, vec![hello, says("r")]).unwrap_or_else(|e| panic!("{e}"));
    let error = CorpusExchange::new(
        AgentKey::new(WorldKey::new("w"), "a"),
        normalized,
        Fidelity::Reconstructed,
        SourceRef::new("f", "/x"),
    );
    assert!(matches!(
        error,
        Err(CorpusError::Invalid {
            reason: InvalidNormalizedExchange::UnresolvedMessage { .. },
            ..
        })
    ));
}

#[test]
fn hashed_messages_hash_their_body() {
    let message = user("x");
    assert_eq!(
        message.hash(),
        crosstalk_spec::observed::message::encoding::hash(&message.message().body)
    );
    let rebuilt = HashedMessage::new(message.message().body.clone());
    assert_eq!(rebuilt, message);
}

#[test]
fn clock_orders_components_and_bounds_them() {
    let ok =
        |major, minor, sub| clock::compose(major, minor, sub).unwrap_or_else(|e| panic!("{e}"));
    assert!(ok(0, clock::MINOR_LIMIT - 1, clock::SUB_LIMIT - 1) < ok(1, 0, 0));
    assert!(ok(3, 5, 999) < ok(3, 6, 0));
    assert_eq!(ok(0, 0, 0).as_micros(), clock::EPOCH_MICROS);
    assert!(clock::compose(0, clock::MINOR_LIMIT, 0).is_err());
    assert!(clock::compose(0, 0, clock::SUB_LIMIT).is_err());
    assert!(clock::compose(u64::MAX, 0, 0).is_err());
    assert!(
        clock::ordinal(1).unwrap_or_else(|e| panic!("{e}"))
            < clock::ordinal(2).unwrap_or_else(|e| panic!("{e}"))
    );
}

#[test]
fn corpus_exchanges_are_replayed_under_their_dataset() {
    let alice = AgentKey::new(WorldKey::new("w1"), "alice");
    let other_world = AgentKey::new(WorldKey::new("w2"), "alice");
    let client = synthetic_client(&dataset(), &alice, "m");
    assert_eq!(
        client.ingress,
        IngressMode::Replay {
            corpus: corpus_id(&dataset())
        }
    );
    assert_eq!(corpus_id(&dataset()), CorpusId("eval-synthetic".into()));
    // One corpus per dataset, whatever the world; credentials stay per agent.
    let elsewhere = synthetic_client(&dataset(), &other_world, "m");
    assert_eq!(client.ingress, elsewhere.ingress);
    assert_ne!(client.credential, elsewhere.credential);
}

#[test]
fn clients_are_stable_per_agent_and_claim_nothing() {
    let alice = AgentKey::new(WorldKey::new("w"), "alice");
    let bob = AgentKey::new(WorldKey::new("w"), "bob");
    let a1 = synthetic_client(&dataset(), &alice, "gemini/x");
    let a2 = synthetic_client(&dataset(), &alice, "gemini/x");
    let b = synthetic_client(&dataset(), &bob, "gemini/x");
    assert_eq!(a1, a2);
    assert_ne!(a1.credential, b.credential);
    assert_eq!(
        a1.credential.map(|c| c.scheme),
        Some(CredentialScheme::ApiKey)
    );
    assert!(a1.harness.is_none());
    assert!(a1.ids.session.is_none() && a1.ids.agent.is_none());
    assert_eq!(
        vendor_of("bedrock/converse/global.anthropic.claude-opus-4-6-v1"),
        Vendor::Anthropic
    );
    assert_eq!(vendor_of("gemini/gemini-3.7-flash"), Vendor::Google);
    assert_eq!(vendor_of("openai/gpt-5.6-luna"), Vendor::OpenAi);
    assert_eq!(
        vendor_of("deepseek/deepseek-v4-flash"),
        Vendor::Other("deepseek".into())
    );
}

#[test]
fn new_inputs_are_the_multiset_difference() {
    let (mut world, alice, _) = builder();
    let first = vec![system("s"), user("go"), user("round")];
    let second = vec![
        system("s"),
        user("go"),
        user("round"),
        calls("c1", "send", r#"{"x":1}"#),
        result("c1", "sent"),
        user("round"),
        user("peer said hi"),
    ];
    let a = world.exchange(draft(&alice, 1, first, calls("c1", "send", r#"{"x":1}"#)));
    let b = world.exchange(draft(&alice, 2, second, says("done")));
    let (Ok(a), Ok(b)) = (a, b) else {
        panic!("exchanges")
    };
    let world = world.finish(Coverage::Partial);
    let (Some(a), Some(b)) = (world.exchange(a), world.exchange(b)) else {
        panic!("missing")
    };
    let first_new: Vec<usize> = new_inputs(None, a).into_iter().map(|(at, _)| at).collect();
    assert_eq!(first_new, vec![0, 1, 2]);
    // The echoed call is the previous response; the second "round" is new.
    let second_new: Vec<usize> = new_inputs(Some(a), b)
        .into_iter()
        .map(|(at, _)| at)
        .collect();
    assert_eq!(second_new, vec![4, 5, 6]);
}

#[test]
fn truncated_histories_do_not_count_kept_messages_as_new() {
    let (mut world, alice, _) = builder();
    let long = vec![system("s"), user("old 1"), user("old 2"), user("recent")];
    let truncated = vec![system("s"), user("old 2"), user("recent"), user("fresh")];
    let a = world.exchange(draft(&alice, 1, long, says("r1")));
    let b = world.exchange(draft(&alice, 2, truncated, says("r2")));
    let (Ok(a), Ok(b)) = (a, b) else {
        panic!("exchanges")
    };
    let world = world.finish(Coverage::Partial);
    let (Some(a), Some(b)) = (world.exchange(a), world.exchange(b)) else {
        panic!("missing")
    };
    let fresh: Vec<usize> = new_inputs(Some(a), b)
        .into_iter()
        .map(|(at, _)| at)
        .collect();
    assert_eq!(fresh, vec![3]);
}

#[test]
fn in_memory_sources_stream_their_worlds() {
    let (world, _, _) = builder();
    let mut source = InMemory::new(dataset(), vec![world.finish(Coverage::Partial)]);
    assert_eq!(source.id(), dataset());
    assert_eq!(source.worlds().count(), 1);
    assert_eq!(source.worlds().count(), 0);
}

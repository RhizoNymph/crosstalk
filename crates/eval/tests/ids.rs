//! Deterministic ids, and messages hashed the spec's way.

mod common;

use crosstalk_eval::corpus::HashedMessage;
use crosstalk_eval::ids::{agent_id, derive, exchange_id};
use crosstalk_eval::keys::{AgentKey, DatasetId, SourceRef, WorldKey};
use crosstalk_spec::observed::message::{MessageBody, Text, UserPart, encoding};

#[test]
fn ids_are_deterministic_and_distinct() {
    let dataset = DatasetId::new("d");
    let alice = AgentKey::new(WorldKey::new("w1"), "alice");
    let bob = AgentKey::new(WorldKey::new("w1"), "bob");
    let alice_elsewhere = AgentKey::new(WorldKey::new("w2"), "alice");
    assert_eq!(agent_id(&dataset, &alice), agent_id(&dataset, &alice));
    assert_ne!(agent_id(&dataset, &alice), agent_id(&dataset, &bob));
    assert_ne!(
        agent_id(&dataset, &alice),
        agent_id(&dataset, &alice_elsewhere)
    );
    assert_ne!(
        agent_id(&dataset, &alice),
        agent_id(&DatasetId::new("e"), &alice)
    );
    // Field boundaries are unambiguous: ("ab", "c") is not ("a", "bc").
    assert_ne!(
        derive("k", &dataset, &["ab", "c"], None),
        derive("k", &dataset, &["a", "bc"], None)
    );
}

#[test]
fn exchange_ids_sort_by_time() {
    let dataset = DatasetId::new("d");
    let early = exchange_id(&dataset, &SourceRef::new("f", "/z"), common::tick(1));
    let late = exchange_id(&dataset, &SourceRef::new("f", "/a"), common::tick(5_000));
    assert!(early < late);
}

#[test]
fn hashed_messages_use_the_spec_encoding() {
    let body = MessageBody::User(vec![UserPart::Text(Text("hello".into()))]);
    let message = HashedMessage::new(body.clone());
    assert_eq!(message.hash(), encoding::hash(&body));
    assert_eq!(message.message(), &encoding::message(body));
}

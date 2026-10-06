use std::collections::HashSet;
use std::sync::OnceLock;

use crosstalk_spec::derived::provenance::matching::Carrier;
use crosstalk_spec::derived::provenance::span::RelaySource;
use crosstalk_spec::observed::client::IngressMode;
use crosstalk_spec::observed::conversation::ConversationOrigin;
use crosstalk_spec::observed::exchange::{Continuation, Transport};
use crosstalk_spec::observed::message::Role;

use super::super::{World, generate, states};
use super::*;
use crate::backend::fixture::clock::{HOUR, NOW, minus};

fn world() -> &'static World {
    static WORLD: OnceLock<World> = OnceLock::new();
    WORLD.get_or_init(|| generate(7).expect("world").0)
}

fn conversations() -> &'static Conversations {
    static CONVERSATIONS: OnceLock<Conversations> = OnceLock::new();
    CONVERSATIONS.get_or_init(|| build(world()).expect("conversations"))
}

fn cases() -> &'static Cases {
    conversations().cases().expect("cases")
}

/// Every message a conversation's turns add, in order.
fn messages(record: &ConversationRecord) -> Vec<MessageHash> {
    record
        .turns
        .iter()
        .flat_map(|t| t.inputs.iter().map(|e| e.message).chain(t.output))
        .collect()
}

#[test]
fn every_reader_exchange_is_a_turn_of_the_readers_conversation() {
    let world = world();
    let mut seen = 0;
    for record in &world.transmissions {
        let Some(confirmed) = states::confirmed(&record.transmission.state) else {
            continue;
        };
        for content in confirmed.content().iter() {
            seen += 1;
            let turn = conversations()
                .turn(content.reader_exchange())
                .expect("the reader exchange is threaded");
            assert_eq!(turn.agent, content.reader());
            let message = content.read_at().part.message;
            let found = match content.carrier() {
                Carrier::ReaderOutput => {
                    turn.output == Some(message)
                        || turn
                            .inputs
                            .iter()
                            .any(|e| e.message == message && e.role == Role::Assistant)
                }
                _ => turn.inputs.iter().any(|e| e.message == message),
            };
            assert!(found, "the copy's message is in the reader's turn");
        }
    }
    assert!(seen > 100, "the world has confirmed matches ({seen})");
}

#[test]
fn every_origin_span_sits_in_its_authors_output() {
    let world = world();
    for record in &world.transmissions {
        let Some(confirmed) = states::confirmed(&record.transmission.state) else {
            continue;
        };
        for content in confirmed.content().iter() {
            let span = conversations()
                .span(content.origin())
                .expect("the origin span is recorded");
            assert_eq!(span.author, content.origin_agent());
            assert_eq!(Some(span.location), world.blobs.span(content.origin()));
            let turn = conversations().turn(span.exchange).expect("threaded");
            assert_eq!(turn.agent, content.origin_agent());
            assert_eq!(turn.output, Some(span.location.part.message));
            assert!(
                turn.started_at <= record.transmission.opened_at,
                "written before it was read"
            );
            assert!(
                conversations()
                    .spans_of(span.exchange)
                    .any(|(id, _)| id == content.origin())
            );
        }
    }
}

#[test]
fn turns_are_in_time_order_and_never_repeat_a_message() {
    for record in conversations().records() {
        assert!(!record.turns.is_empty());
        for pair in record.turns.windows(2) {
            assert!(pair[0].started_at <= pair[1].started_at);
        }
        let all = messages(record);
        let distinct: HashSet<_> = all.iter().collect();
        assert_eq!(
            distinct.len(),
            all.len(),
            "no message twice in one conversation"
        );
        for turn in &record.turns {
            assert!(
                !turn.inputs.is_empty() || turn.output.is_some(),
                "a turn adds something"
            );
        }
    }
}

#[test]
fn every_exchange_locates_to_its_own_turn() {
    let mut exchanges = HashSet::new();
    for record in conversations().records() {
        for (index, turn) in record.turns.iter().enumerate() {
            assert!(exchanges.insert(turn.exchange), "one turn per exchange");
            assert_eq!(
                conversations().locate(turn.exchange),
                Some((record.id, u32::try_from(index).expect("index")))
            );
        }
    }
}

#[test]
fn every_generated_message_has_a_body_and_the_rest_come_from_blobs() {
    let world = world();
    let dropped: HashSet<MessageHash> = world
        .transmissions
        .iter()
        .filter_map(|r| states::confirmed(&r.transmission.state))
        .flat_map(|c| c.content().iter().cloned().collect::<Vec<_>>())
        .flat_map(|m| {
            let origin = world.blobs.span(m.origin()).map(|l| l.part.message);
            [Some(m.read_at().part.message), origin]
        })
        .flatten()
        .filter(|hash| world.blobs.body(*hash).is_none())
        .collect();
    let mut missing = 0;
    for record in conversations().records() {
        for hash in messages(record) {
            let generated = conversations().body(hash).is_some();
            let stored = world.blobs.body(hash).is_some();
            if !generated && !stored {
                assert!(dropped.contains(&hash), "only retention drops bodies");
                missing += 1;
            }
        }
    }
    assert!(missing > 0, "some dropped bodies show in conversations");
}

#[test]
fn each_conversation_opens_with_a_system_prompt_and_a_task() {
    let unseen = cases().unseen_increment;
    for record in conversations().records() {
        if record.id == unseen || !matches!(record.origin, ConversationOrigin::Root) {
            continue;
        }
        let first = &record.turns[0].inputs;
        assert_eq!(first[0].role, Role::System);
        assert_eq!(first[1].role, Role::User);
    }
}

#[test]
fn the_fork_shares_a_prefix_with_an_assistant_message() {
    let (fork, parent) = cases().fork;
    let record = conversations().get(fork).expect("fork");
    let ConversationOrigin::Fork {
        parent: named,
        shared_prefix,
    } = record.origin
    else {
        panic!("a fork");
    };
    assert_eq!(named, parent);
    let parent = conversations().get(parent).expect("parent");
    let history = parent.history();
    let prefix = usize::try_from(shared_prefix).expect("prefix");
    assert!(prefix > 0 && prefix < history.len());
    let has_assistant = parent.turns.iter().take(2).any(|t| t.output.is_some());
    assert!(has_assistant);
    assert!(
        conversations()
            .successors(parent.id)
            .iter()
            .any(|s| s.id == fork)
    );
    assert!(record.turns[0].started_at >= parent.turns[1].started_at);
}

#[test]
fn the_compaction_carries_over_messages_of_its_predecessor() {
    let (compacted, old) = cases().compaction;
    let record = conversations().get(compacted).expect("compaction");
    assert_eq!(
        record.origin,
        ConversationOrigin::Compaction { predecessor: old }
    );
    let predecessor = conversations().get(old).expect("predecessor").history();
    let first = &record.turns[0];
    let carried: Vec<_> = first.inputs.iter().filter(|e| e.carried_over).collect();
    assert_eq!(carried.len(), 2);
    for entry in &carried {
        assert!(predecessor.contains(&entry.message));
    }
    for turn in record.turns.iter().skip(1) {
        assert!(turn.inputs.iter().all(|e| !e.carried_over));
    }
    for other in conversations().records() {
        if other.id != compacted {
            assert!(
                other
                    .turns
                    .iter()
                    .all(|t| t.inputs.iter().all(|e| !e.carried_over))
            );
        }
    }
}

#[test]
fn the_unseen_increment_starts_a_root_on_a_websocket() {
    let record = conversations()
        .get(cases().unseen_increment)
        .expect("conversation");
    assert_eq!(record.origin, ConversationOrigin::Root);
    let first = &record.turns[0];
    assert_eq!(first.transport, Transport::WebSocket);
    assert!(matches!(first.continuation, Continuation::Increment { .. }));
    assert!(first.inputs.iter().all(|e| e.role != Role::System));
}

#[test]
fn a_system_turn_appears_after_the_first_turn() {
    let (id, index) = cases().mid_system;
    let record = conversations().get(id).expect("conversation");
    let turn = &record.turns[usize::try_from(index).expect("index")];
    assert!(index > 0);
    assert!(turn.inputs.iter().any(|e| e.role == Role::System));
}

#[test]
fn the_failed_turn_keeps_a_partial_output() {
    let (id, index) = cases().failed;
    let record = conversations().get(id).expect("conversation");
    let turn = &record.turns[usize::try_from(index).expect("index")];
    assert!(matches!(turn.ending, Ending::Failed { .. }));
    let output = turn.output.expect("a partial output");
    assert!(conversations().body(output).is_some());
}

#[test]
fn one_agents_conversations_are_all_replayed() {
    let (corpus, one) = &cases().replay;
    let agent = conversations().get(*one).expect("conversation").agent;
    let replayed = |r: &ConversationRecord| matches!(&r.ingress, IngressMode::Replay { corpus: c } if c == corpus);
    let mut count = 0;
    for record in conversations().records() {
        if replayed(record) {
            assert_eq!(record.agent, agent, "only that agent was replayed");
            count += 1;
        }
    }
    assert!(count >= 1);
}

#[test]
fn delegated_tasks_open_the_childs_conversation() {
    let world = world();
    let mut seen = 0;
    for record in &world.transmissions {
        let Route::Delegation(DelegationDirection::ParentToChild) = record.transmission.route
        else {
            continue;
        };
        let Some(confirmed) = states::confirmed(&record.transmission.state) else {
            continue;
        };
        let exchange = confirmed.content().first().reader_exchange();
        let (id, index) = conversations().locate(exchange).expect("threaded");
        if conversations()
            .get(id)
            .is_some_and(|r| r.origin == ConversationOrigin::Root)
            && cases().compaction.0 != id
        {
            assert_eq!(index, 0, "the task opens the child's conversation");
        }
        seen += 1;
    }
    assert!(seen > 0, "the world has delegations");
}

#[test]
fn relayed_spans_quote_their_source_in_the_reply() {
    let mut seen = 0;
    for record in conversations().records() {
        for turn in &record.turns {
            for relayed in conversations().relayed_in(turn.exchange) {
                seen += 1;
                assert_eq!(turn.output, Some(relayed.location.part.message));
                let body = conversations()
                    .body(relayed.location.part.message)
                    .expect("a generated reply");
                let text = body.part_text(relayed.location.part.index).expect("text");
                let start = relayed.location.range.start() as usize;
                let end = relayed.location.range.end() as usize;
                assert!(text.get(start..end).is_some());
                let RelaySource::Span(origin) = relayed.source else {
                    panic!("relayed from a span");
                };
                assert!(conversations().span(origin).is_some());
            }
        }
    }
    assert!(seen > 0, "some replies quote what they read");
}

#[test]
fn one_turn_waits_for_its_scan() {
    let pending = cases().pending_scan;
    assert!(conversations().is_pending(pending));
    assert!(conversations().turn(pending).is_some());
}

#[test]
fn a_replay_keeps_only_turns_started_by_its_cutoff() {
    let cutoff = minus(NOW, 24 * HOUR);
    let earlier = conversations().at(cutoff);
    assert!(earlier.records().count() < conversations().records().count());
    for record in earlier.records() {
        for (index, turn) in record.turns.iter().enumerate() {
            assert!(turn.started_at <= cutoff);
            assert_eq!(
                earlier.locate(turn.exchange),
                Some((record.id, u32::try_from(index).expect("index")))
            );
        }
    }
    for record in conversations().records() {
        for turn in &record.turns {
            if turn.started_at > cutoff {
                assert_eq!(earlier.locate(turn.exchange), None);
            }
        }
    }
}

#[test]
fn the_same_seed_builds_the_same_conversations() {
    let again = build(world()).expect("build");
    assert_eq!(&again, conversations());
}

use crosstalk_spec::derived::flow::transmission::{DelegationDirection, Route};

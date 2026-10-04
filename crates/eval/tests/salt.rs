//! The SALT converter on synthetic fixtures shaped like the dataset.

use std::collections::BTreeMap;
use std::io::Write;
use std::path::{Path, PathBuf};

use crosstalk_eval::corpus::{Coverage, Driven, Fidelity, TraceSource, World};
use crosstalk_eval::datasets::salt::files::{discover, world_name};
use crosstalk_eval::datasets::salt::schema::Trace;
use crosstalk_eval::datasets::salt::{SaltSource, Selection, convert_trace, load_world};
use crosstalk_eval::keys::AgentKey;
use crosstalk_eval::location::SpanLocationExt;
use crosstalk_eval::truth::{
    Expectation, ExpectedTransmission, MatchNeed, NegativeControl, NegativeReason, Tier, jsonl,
};
use crosstalk_spec::observed::message::{AssistantPart, MessageBody, Reasoning};

const MAIN: &str = "traces/main/main__fixture-model/rep001.json";
const MEMORY: &str =
    "traces/memory_scope/memory_scope__fixture-model__communication-onward/rep001.json";
const CONTROLLED: &str =
    "traces/controlled_peer/controlled_peer__fixture-model__summary/rep001.json";

fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/salt")
}

fn world(file: &str) -> World {
    load_world(&root(), Path::new(file)).unwrap_or_else(|e| panic!("{file}: {e}"))
}

fn positives(world: &World) -> Vec<&ExpectedTransmission> {
    world
        .truth()
        .iter()
        .filter_map(|e| match e {
            Expectation::Transmission(t) => Some(t),
            _ => None,
        })
        .collect()
}

fn negatives(world: &World, reason: NegativeReason) -> Vec<&NegativeControl> {
    world
        .truth()
        .iter()
        .filter_map(|e| match e {
            Expectation::NoTransmission(c) if c.label().reason == reason => Some(c),
            _ => None,
        })
        .collect()
}

fn key(world: &World, name: &str) -> AgentKey {
    AgentKey::new(world.key().clone(), name)
}

#[test]
fn files_are_stratified_filtered_and_limited() {
    let all = discover(&root(), &Selection::default()).unwrap_or_else(|e| panic!("{e}"));
    let names: Vec<String> = all
        .iter()
        .map(|p| p.to_string_lossy().into_owned())
        .collect();
    assert_eq!(
        names,
        vec![CONTROLLED.to_owned(), MAIN.to_owned(), MEMORY.to_owned()]
    );
    let limited = discover(
        &root(),
        &Selection {
            limit: Some(1),
            include: vec![],
        },
    )
    .unwrap_or_default();
    assert_eq!(limited.len(), 1);
    let filtered = discover(
        &root(),
        &Selection {
            limit: None,
            include: vec!["main/".into()],
        },
    )
    .unwrap_or_default();
    assert_eq!(filtered.len(), 1);
    assert_eq!(
        world_name(Path::new("traces/main/c/rep001.json.gz")),
        "main/c/rep001"
    );
    assert!(discover(Path::new("/nonexistent"), &Selection::default()).is_err());
}

#[test]
fn main_world_reconstructs_every_call() {
    let world = world(MAIN);
    assert_eq!(world.key().as_str(), "main/main__fixture-model/rep001");
    assert_eq!(
        world.coverage(),
        Coverage::Complete {
            tier: Tier::Construction
        }
    );
    let count = |name: &str| {
        world
            .exchanges()
            .iter()
            .filter(|e| e.agent().name == name)
            .count()
    };
    assert_eq!((count("alice"), count("bob")), (9, 8));
    assert!(world.agents().iter().all(|a| a.driven == Driven::Model));
    assert!(
        world
            .exchanges()
            .iter()
            .all(|e| e.fidelity() == Fidelity::Reconstructed)
    );
    let times: Vec<_> = world.exchanges().iter().map(|e| e.at()).collect();
    let mut sorted = times.clone();
    sorted.sort();
    assert_eq!(times, sorted);
    for agent in ["alice", "bob"] {
        let mine: Vec<_> = world
            .exchanges()
            .iter()
            .filter(|e| e.agent().name == agent)
            .collect();
        assert!(mine.windows(2).all(|w| w[0].at() < w[1].at()));
        for exchange in &mine {
            assert!(matches!(
                exchange.response().map(|m| &m.body),
                Some(MessageBody::Assistant(_))
            ));
        }
    }
}

#[test]
fn episodes_carry_their_history_but_only_their_own_calls() {
    let world = world(MAIN);
    let alice: Vec<_> = world
        .exchanges()
        .iter()
        .filter(|e| e.agent().name == "alice")
        .collect();
    // Episode 2's first call sees episode 1's whole list, then its own turns.
    let first_of_second = alice[6];
    assert_eq!(first_of_second.request().count(), 22);
    assert_eq!(
        first_of_second.source().path,
        "/results/1/agents/alice/messages/22"
    );
}

#[test]
fn memory_rewritten_contexts_read_their_own_list() {
    let world = world(MEMORY);
    let alice: Vec<_> = world
        .exchanges()
        .iter()
        .filter(|e| e.agent().name == "alice")
        .collect();
    assert_eq!(alice.len(), 9);
    let first_of_second = alice[6];
    let request: Vec<String> = first_of_second
        .request()
        .filter_map(|m| m.part_text(0).ok().map(|t| t.into_owned()))
        .collect();
    assert!(request[1].starts_with("## Episode 1: communication phase"));
    assert!(
        !request
            .iter()
            .any(|t| t.starts_with("## Episode 1: task phase"))
    );
    assert!(
        request
            .last()
            .is_some_and(|t| t.starts_with("## Episode 2: task phase"))
    );
    assert_eq!(positives(&world).len(), 6);
}

#[test]
fn delivered_messages_are_labelled_where_they_arrive() {
    let world = world(MAIN);
    let labels = positives(&world);
    assert_eq!(labels.len(), 6);
    for expected in &labels {
        let label = expected.label();
        assert_eq!(label.tier, Tier::Construction);
        let reader = world
            .exchange(label.reader_exchange)
            .unwrap_or_else(|| panic!("reader exchange"));
        assert_eq!(reader.agent(), &label.to);
        let message = reader
            .message(label.content.at.message())
            .unwrap_or_else(|| panic!("the reader exchange carries the message"));
        assert_eq!(
            label.content.at.text(message).ok().as_deref(),
            Some(label.content.text.as_str())
        );
        // It is the first call to carry it: the reader's previous call did not.
        let previous = world
            .exchanges()
            .iter()
            .rev()
            .find(|e| e.agent() == reader.agent() && e.at() < reader.at())
            .unwrap_or_else(|| panic!("an earlier call"));
        assert!(previous.message(label.content.at.message()).is_none());
        // The sender's exchange came first.
        let sender = world
            .exchange(
                label
                    .sender_exchange
                    .unwrap_or_else(|| panic!("sender exchange")),
            )
            .unwrap_or_else(|| panic!("sender exchange in world"));
        assert_eq!(sender.agent(), &label.from);
        assert!(sender.at() < reader.at());
        let call_text: Vec<String> = (0..sender.response().map_or(0, |m| m.part_count()))
            .filter_map(|p| {
                sender
                    .response()?
                    .part_text(u16::try_from(p).ok()?)
                    .ok()
                    .map(|t| t.into_owned())
            })
            .collect();
        assert!(
            call_text
                .iter()
                .any(|t| t.contains("send_message") || t.contains("content"))
        );
    }
    let needs: BTreeMap<&str, &MatchNeed> = labels
        .iter()
        .map(|l| (l.label().content.text.as_str(), &l.label().needs))
        .collect();
    assert_eq!(
        needs.get("Bob reports the clinical query returned \"21 rows\"\nwith severity three or higher in every row."),
        Some(&&MatchNeed::Normalized)
    );
    assert_eq!(needs.get("OK."), Some(&&MatchNeed::Exact));
}

#[test]
fn rejected_sends_point_at_the_failed_call() {
    let world = world(MAIN);
    let rejected = negatives(&world, NegativeReason::RejectedSend);
    assert_eq!(rejected.len(), 1);
    let label = rejected[0].label();
    assert_eq!(
        (&label.from, &label.to),
        (&key(&world, "alice"), &key(&world, "bob"))
    );
    assert!(
        label
            .text
            .as_deref()
            .is_some_and(|t| t.starts_with("Raw log follows:"))
    );
    let origin = label.origin.unwrap_or_else(|| panic!("origin"));
    let sender = world
        .exchanges()
        .iter()
        .find(|e| e.response().is_some_and(|m| m.hash == origin.message()))
        .unwrap_or_else(|| panic!("the failed call's exchange"));
    let text = sender
        .response()
        .and_then(|m| origin.text(m).ok())
        .unwrap_or_default();
    assert!(text.contains("Raw log follows:"));
}

#[test]
fn shared_sources_and_boilerplate_are_negative_controls() {
    let world = world(MAIN);
    let shared = negatives(&world, NegativeReason::SharedSource);
    let system = shared
        .iter()
        .filter(|c| c.label().reader_exchange.is_none())
        .count();
    assert_eq!(system, 2, "one per reader, deduplicated across episodes");
    assert_eq!(
        shared.len() - system,
        4,
        "two shared database results per agent"
    );
    assert_eq!(negatives(&world, NegativeReason::Boilerplate).len(), 22);
    assert!(shared.iter().all(|c| c.label().tier == Tier::Structural));
}

#[test]
fn scripted_peers_make_no_exchanges_and_send_from_nowhere() {
    let world = world(CONTROLLED);
    let bob = world
        .agent(&key(&world, "bob"))
        .unwrap_or_else(|| panic!("bob"));
    assert_eq!(bob.driven, Driven::Scripted);
    assert!(world.exchanges().iter().all(|e| e.agent().name == "alice"));
    assert!(positives(&world).is_empty());
    let scripted = negatives(&world, NegativeReason::NoSenderExchange);
    assert_eq!(scripted.len(), 1);
    let label = scripted[0].label();
    assert_eq!(
        (&label.from, &label.to),
        (&key(&world, "bob"), &key(&world, "alice"))
    );
    assert!(label.at.is_some() && label.reader_exchange.is_some());
}

#[test]
fn gemini_thought_signatures_stay_opaque() {
    let world = world(MAIN);
    let mut opaque = 0;
    for exchange in world
        .exchanges()
        .iter()
        .filter(|e| e.agent().name == "alice")
    {
        let Some(MessageBody::Assistant(parts)) = exchange.response().map(|m| &m.body) else {
            continue;
        };
        for (index, part) in parts.iter().enumerate() {
            match part {
                AssistantPart::Reasoning(Reasoning::Opaque { .. }) => opaque += 1,
                AssistantPart::ToolCall(call) => {
                    assert!(call.id.0.contains("__thought__"), "the id is kept whole");
                    let text = exchange
                        .response()
                        .and_then(|m| m.part_text(u16::try_from(index).ok()?).ok())
                        .unwrap_or_default();
                    assert!(!text.contains("__thought__"), "no id in part text");
                }
                _ => {}
            }
        }
    }
    assert!(opaque > 0);
}

#[test]
fn accepted_call_mismatch_makes_exchanges_synthetic() {
    let text = std::fs::read_to_string(root().join(MAIN)).unwrap_or_default();
    let mut trace: Trace = serde_json::from_str(&text).unwrap_or_else(|e| panic!("{e}"));
    trace.results[0]
        .llm_usage
        .retain(|u| !(u.actor == "bob" && u.accepted()));
    let world = convert_trace(&trace, MAIN).unwrap_or_else(|e| panic!("{e}"));
    let bob_first: Vec<_> = world
        .exchanges()
        .iter()
        .filter(|e| e.agent().name == "bob" && e.source().path.starts_with("/results/0/"))
        .collect();
    assert!(!bob_first.is_empty());
    assert!(
        bob_first
            .iter()
            .all(|e| e.fidelity() == Fidelity::Synthetic)
    );
}

#[test]
fn gzipped_traces_read_the_same() {
    let dir = std::env::temp_dir().join(format!("ct-eval-gz-{}", std::process::id()));
    let target = dir.join("traces/main/main__fixture-model");
    std::fs::create_dir_all(&target).unwrap_or_else(|e| panic!("{e}"));
    let raw = std::fs::read(root().join(MAIN)).unwrap_or_default();
    let file =
        std::fs::File::create(target.join("rep001.json.gz")).unwrap_or_else(|e| panic!("{e}"));
    let mut encoder = flate2::write::GzEncoder::new(file, flate2::Compression::fast());
    encoder.write_all(&raw).unwrap_or_else(|e| panic!("{e}"));
    encoder.finish().unwrap_or_else(|e| panic!("{e}"));
    let gz = load_world(
        &dir,
        Path::new("traces/main/main__fixture-model/rep001.json.gz"),
    )
    .unwrap_or_else(|e| panic!("{e}"));
    let plain = world(MAIN);
    assert_eq!(gz.key(), plain.key());
    assert_eq!(gz.exchanges().len(), plain.exchanges().len());
    let texts = |w: &World| {
        positives(w)
            .iter()
            .map(|t| t.label().content.text.clone())
            .collect::<Vec<_>>()
    };
    assert_eq!(texts(&gz), texts(&plain));
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn conversion_is_deterministic() {
    let dump = |world: &World| {
        let mut out = Vec::new();
        jsonl::write(&mut out, world.truth()).unwrap_or_else(|e| panic!("{e}"));
        let ids: Vec<String> = world
            .exchanges()
            .iter()
            .map(|e| e.id().ulid_text())
            .collect();
        (out, ids)
    };
    assert_eq!(dump(&world(MAIN)), dump(&world(MAIN)));
}

#[test]
fn the_source_streams_one_world_per_file() {
    let mut source =
        SaltSource::open(&root(), &Selection::default()).unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(source.id().as_str(), "salt");
    assert_eq!(source.files().len(), 3);
    let worlds: Vec<_> = source.worlds().collect();
    assert_eq!(worlds.len(), 3);
    assert!(worlds.iter().all(Result::is_ok));
}

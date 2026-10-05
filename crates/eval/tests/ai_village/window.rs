//! The window mode on a synthetic two-day village.

use std::collections::BTreeMap;

use crosstalk_eval::corpus::{Coverage, Fidelity, TraceSource, World};
use crosstalk_eval::datasets::ai_village::time::Day;
use crosstalk_eval::datasets::ai_village::{AiVillageSource, Mode, Stats};
use crosstalk_eval::location::SpanLocationExt;
use crosstalk_eval::pipeline::{ReferenceDetector, run};
use crosstalk_eval::truth::kinds::locator_key;
use crosstalk_eval::truth::{CarrierKind, Expectation, RouteExpectation, Tier, TransmissionLabel};
use crosstalk_spec::observed::message::{MessageBody, SystemPart, UserPart};
use serde_json::json;

use super::fixture::{
    self, ALICE, BOB, CAROL, GENERAL, SIDE, anthropic, bash, gemini, responses, send, table, turn,
};

const PUSHED: &str = "Pushed the tracker fix, please pull and review it";
const SECOND: &str = "Second message for the general room only, after Carol left";
const LATE: &str = "Late note that nobody reads within four hours of posting";
const COMMENT: &str = "The parser now handles nested quotes in every field";

fn dataset() -> tempfile::TempDir {
    let dir = fixture::dir();
    let root = dir.path();
    fixture::base(root);
    table(
        root,
        "computer_use_sessions",
        &[
            json!({"id": "sA1", "agent_id": ALICE, "session_goal": "g"}),
            json!({"id": "sB1", "agent_id": BOB, "session_goal": "g"}),
            json!({"id": "sB2", "agent_id": BOB, "session_goal": "g"}),
            json!({"id": "sB3", "agent_id": BOB, "session_goal": "g"}),
            json!({"id": "sC1", "agent_id": CAROL, "session_goal": "g"}),
        ],
    );
    let alice = |text: &str, id: &str, tool: &str, input: serde_json::Value| {
        anthropic(text, id, tool, input)
    };
    let bob = |text: &str, id: &str, command: &str| {
        responses(text, id, "bash", json!({"command": command}))
    };
    let push = "cd /home/computeruse/tracker && git push origin main";
    let comment = format!("gh issue comment 5 -R ai-village-agents/tracker --body \"{COMMENT}\"");
    let pull = "cd ~/tracker && git pull";
    let view = "gh issue view 5 --repo ai-village-agents/tracker --comments";
    table(
        root,
        "computer_use_turns",
        &[
            turn(
                "A1",
                "sA1",
                "2026-07-13 16:10:00.0",
                bash(push),
                alice(
                    "Pushing my fix",
                    "toolu_a1",
                    "bash",
                    json!({"command": push}),
                ),
                None,
                Some(
                    "To https://github.com/ai-village-agents/tracker.git\n   a1..b2  main -> main",
                ),
            ),
            turn(
                "A2",
                "sA1",
                "2026-07-13 16:20:00.0",
                send(PUSHED),
                alice(
                    "Telling everyone",
                    "toolu_a2",
                    "send_message_to_chat",
                    json!({"content": PUSHED}),
                ),
                None,
                None,
            ),
            turn(
                "A3",
                "sA1",
                "2026-07-13 16:40:00.0",
                bash(&comment),
                alice(
                    "Commenting on the issue",
                    "toolu_a3",
                    "bash",
                    json!({"command": comment}),
                ),
                Some("https://github.com/ai-village-agents/tracker/issues/5#issuecomment-1"),
                None,
            ),
            turn(
                "A4",
                "sA1",
                "2026-07-13 17:44:59.9",
                send(SECOND),
                alice(
                    "Another note",
                    "toolu_a4",
                    "send_message_to_chat",
                    json!({"content": SECOND}),
                ),
                None,
                None,
            ),
            turn(
                "A5",
                "sA1",
                "2026-07-13 18:19:59.9",
                send(LATE),
                alice(
                    "A late note",
                    "toolu_a5",
                    "send_message_to_chat",
                    json!({"content": LATE}),
                ),
                None,
                None,
            ),
            turn(
                "B1",
                "sB1",
                "2026-07-13 16:30:00.0",
                bash(pull),
                bob("Pulling", "call_b1", pull),
                None,
                Some(
                    "From https://github.com/ai-village-agents/tracker\n * branch main -> FETCH_HEAD",
                ),
            ),
            turn(
                "B2",
                "sB1",
                "2026-07-13 16:50:00.0",
                bash(view),
                bob("Reading the issue", "call_b2", view),
                Some(&format!(
                    "title: Parser bug\n--\nalice commented:\n{COMMENT}\n"
                )),
                None,
            ),
            turn(
                "B3",
                "sB1",
                "2026-07-13 17:00:00.0",
                json!({"action": "type", "text": "https://docs.google.com/document/d/abc/edit"}),
                responses(
                    "Opening the Google Doc",
                    "call_b3",
                    "computer",
                    json!({"action": "type"}),
                ),
                None,
                None,
            ),
            turn(
                "B4",
                "sB2",
                "2026-07-13 18:10:00.0",
                bash("ls"),
                bob("Listing", "call_b4", "ls"),
                Some("README.md"),
                None,
            ),
            turn(
                "B5",
                "sB2",
                "2026-07-13 22:30:00.0",
                bash("ls"),
                bob("Listing again", "call_b5", "ls"),
                Some("README.md"),
                None,
            ),
            turn(
                "C1",
                "sC1",
                "2026-07-13 16:25:00.0",
                json!({"action": "screenshot"}),
                gemini(
                    "Looking around",
                    "use_computer",
                    json!({"action": "screenshot"}),
                ),
                None,
                None,
            ),
            turn(
                "C2",
                "sC1",
                "2026-07-13 18:00:00.0",
                json!({"action": "screenshot"}),
                gemini(
                    "Still looking",
                    "use_computer",
                    json!({"action": "screenshot"}),
                ),
                None,
                None,
            ),
            turn(
                "B6",
                "sB3",
                "2026-07-14 16:10:00.0",
                bash(pull),
                bob("Pulling again", "call_b6", pull),
                None,
                Some("Already up to date."),
            ),
            // Outside the window.
            turn(
                "Z1",
                "sB1",
                "2026-07-20 16:10:00.0",
                bash("ls"),
                bob("Later", "call_z", "ls"),
                None,
                None,
            ),
        ],
    );
    table(
        root,
        "events",
        &[
            fixture::talk(
                "t1",
                10,
                "2026-07-13 16:20:00.1",
                ALICE,
                GENERAL,
                "c1",
                PUSHED,
            ),
            fixture::talk(
                "t2",
                20,
                "2026-07-13 17:45:00.0",
                ALICE,
                GENERAL,
                "c2",
                SECOND,
            ),
            fixture::talk(
                "t3",
                30,
                "2026-07-13 18:20:00.0",
                ALICE,
                GENERAL,
                "c3",
                LATE,
            ),
            json!({"id": "u1", "event_index": 5, "data": {"actionType": "USER_TALK", "roomId": GENERAL, "messageId": "c4", "speakerName": "automated", "content": "nudge"}, "created_at": "2026-07-13 16:05:00.0"}),
            json!({"id": "m1", "event_index": 15, "data": {"actionType": "ENTER_ROOM", "agentId": CAROL, "roomId": SIDE, "previousRoomId": GENERAL}, "created_at": "2026-07-13 17:30:00.0"}),
            json!({"id": "p0", "event_index": 1, "data": {"actionType": "PAUSE", "agentId": ALICE, "roomId": GENERAL}, "created_at": "2026-07-01 16:00:00.0"}),
        ],
    );
    table(
        root,
        "chat_messages",
        &[
            fixture::chat("c1", "2026-07-13 16:20:00.1", Some(ALICE), GENERAL, PUSHED),
            fixture::chat("c2", "2026-07-13 17:45:00.0", Some(ALICE), GENERAL, SECOND),
            fixture::chat("c3", "2026-07-13 18:20:00.0", Some(ALICE), GENERAL, LATE),
            fixture::chat("c4", "2026-07-13 16:05:00.0", None, GENERAL, "nudge"),
            fixture::chat(
                "c9",
                "2026-06-01 16:05:00.0",
                Some(BOB),
                GENERAL,
                "long before the window",
            ),
        ],
    );
    table(
        root,
        "agent_memories",
        &[
            json!({"id": "mem1", "agent_id": ALICE, "content": "Alice remembers the tracker repo", "created_at": "2026-07-10 23:00:00.0"}),
            json!({"id": "mem0", "agent_id": ALICE, "content": "Alice's older memory", "created_at": "2026-07-01 23:00:00.0"}),
            json!({"id": "mem2", "agent_id": BOB, "content": "Bob remembers his tracker goal", "created_at": "2026-07-13 16:00:00.0"}),
            json!({"id": "mem3", "agent_id": BOB, "content": "Bob's memory from the future", "created_at": "2026-07-25 16:00:00.0"}),
        ],
    );
    dir
}

fn open(dir: &tempfile::TempDir) -> AiVillageSource {
    let from = Day::parse("2026-07-13").unwrap_or_else(|e| panic!("{e}"));
    let to = Day::parse("2026-07-14").unwrap_or_else(|e| panic!("{e}"));
    AiVillageSource::open(dir.path(), Mode::Window { from, to }).unwrap_or_else(|e| panic!("{e}"))
}

fn labels(world: &World) -> Vec<&TransmissionLabel> {
    world
        .truth()
        .iter()
        .filter_map(|e| match e {
            Expectation::Transmission(t) => Some(t.label()),
            _ => None,
        })
        .collect()
}

fn text(message: &crosstalk_spec::observed::message::Message) -> String {
    match &message.body {
        MessageBody::System(parts) => parts
            .iter()
            .map(|p| match p {
                SystemPart::Text(t) => t.0.clone(),
                SystemPart::Unknown(_) => String::new(),
            })
            .collect(),
        MessageBody::User(parts) => parts
            .iter()
            .map(|p| match p {
                UserPart::Text(t) => t.0.clone(),
                _ => String::new(),
            })
            .collect(),
        _ => String::new(),
    }
}

#[test]
fn days_become_worlds_with_rebuilt_requests() {
    let dir = dataset();
    let mut source = open(&dir);
    let worlds: Vec<World> = source
        .worlds()
        .map(|w| w.unwrap_or_else(|e| panic!("{e}")))
        .collect();
    assert_eq!(worlds.len(), 2);
    let world = &worlds[0];
    assert_eq!(world.key().as_str(), "window/2026-07-13");
    assert_eq!(world.coverage(), Coverage::Partial);
    let mut per_agent: BTreeMap<&str, Vec<_>> = BTreeMap::new();
    for exchange in world.exchanges() {
        per_agent
            .entry(exchange.agent().name.as_str())
            .or_default()
            .push(exchange);
    }
    let counts: Vec<(&str, usize)> = per_agent.iter().map(|(n, e)| (*n, e.len())).collect();
    assert_eq!(counts, vec![("Alice", 5), ("Bob", 5), ("Carol", 2)]);
    assert!(
        world
            .exchanges()
            .iter()
            .all(|e| e.fidelity() == Fidelity::Reconstructed)
    );
    // Alice's first call: her system prompt (village goal, latest memory
    // before the call) and the human's nudge.
    let alice = &per_agent["Alice"];
    let request: Vec<_> = alice[0].request().collect();
    assert_eq!(request.len(), 2);
    let system = text(request[0]);
    assert!(system.contains("You are Alice"));
    assert!(system.contains("Build something together"));
    assert!(system.contains("Alice remembers the tracker repo"));
    assert!(!system.contains("older memory"));
    assert!(text(request[1]).ends_with("#general automated: nudge"));
    // Bob's second call carries his first call and its output; his goal
    // and memory are in his system prompt.
    let bob = &per_agent["Bob"];
    let request: Vec<_> = bob[1].request().collect();
    assert_eq!(request.len(), 4);
    let system = text(request[0]);
    assert!(system.contains("Ship the tracker: keep it live"));
    assert!(system.contains("Bob remembers his tracker goal"));
    assert!(text(request[1]).contains(PUSHED));
    assert!(matches!(&request[2].body, MessageBody::Assistant(_)));
    assert!(matches!(&request[3].body, MessageBody::Tool(_)));
    // A new session starts from its system prompt again.
    let request: Vec<_> = bob[3].request().collect();
    assert_eq!(request.len(), 2);
    assert!(text(request[1]).contains(SECOND));
    assert_eq!(worlds[1].exchanges().len(), 1);
}

#[test]
fn chat_reaches_room_members_within_the_horizon() {
    let dir = dataset();
    let mut source = open(&dir);
    let world = source
        .worlds()
        .next()
        .unwrap_or_else(|| panic!("no world"))
        .unwrap_or_else(|e| panic!("{e}"));
    let chat: Vec<_> = labels(&world)
        .into_iter()
        .filter(|l| l.carrier == CarrierKind::UserTurn)
        .collect();
    let pairs: Vec<(String, String, String)> = chat
        .iter()
        .map(|l| {
            (
                l.from.name.clone(),
                l.to.name.clone(),
                l.content.text.clone(),
            )
        })
        .collect();
    assert_eq!(
        pairs,
        vec![
            ("Alice".to_owned(), "Bob".to_owned(), PUSHED.to_owned()),
            ("Alice".to_owned(), "Carol".to_owned(), PUSHED.to_owned()),
            ("Alice".to_owned(), "Bob".to_owned(), SECOND.to_owned()),
        ]
    );
    for label in &chat {
        assert_eq!(label.route, RouteExpectation::Direct);
        assert_eq!(label.tier, Tier::Structural);
        let reader = world
            .exchange(label.reader_exchange)
            .unwrap_or_else(|| panic!("reader"));
        let message = reader
            .message(label.content.at.message())
            .unwrap_or_else(|| panic!("message"));
        assert_eq!(
            label.content.at.text(message).ok().as_deref(),
            Some(label.content.text.as_str())
        );
        let sender = world
            .exchange(label.sender_exchange.unwrap_or_else(|| panic!("sender")))
            .unwrap_or_else(|| panic!("sender exchange"));
        assert!(sender.at() < reader.at());
    }
    let Stats::Window(stats) = source.stats() else {
        panic!("not a window");
    };
    assert_eq!(stats.talks, 3);
    assert_eq!(stats.talks_without_sender, 0);
    assert_eq!(stats.chat_labels, 3);
    assert_eq!(stats.chat_late, 1);
}

#[test]
fn repository_pairs_become_channel_labels_or_co_accesses() {
    let dir = dataset();
    let mut source = open(&dir);
    let worlds: Vec<World> = source
        .worlds()
        .map(|w| w.unwrap_or_else(|e| panic!("{e}")))
        .collect();
    let repo: Vec<_> = labels(&worlds[0])
        .into_iter()
        .filter(|l| l.carrier == CarrierKind::ToolResult)
        .collect();
    assert_eq!(repo.len(), 1);
    let label = repo[0];
    assert_eq!(
        (label.from.name.as_str(), label.to.name.as_str()),
        ("Alice", "Bob")
    );
    assert_eq!(label.tier, Tier::Heuristic);
    assert_eq!(label.content.text, COMMENT);
    match &label.route {
        RouteExpectation::Channel { resource } => {
            assert_eq!(
                locator_key(resource),
                "https://github.com/ai-village-agents/tracker"
            );
        }
        other => panic!("{other:?}"),
    }
    // The label sits at Bob's call after the read: the one carrying its output.
    let reader = worlds[0]
        .exchange(label.reader_exchange)
        .unwrap_or_else(|| panic!("reader"));
    assert_eq!(reader.source().path, "/B3");
    let Stats::Window(stats) = source.stats() else {
        panic!("not a window");
    };
    assert_eq!(stats.repo_labels, 1);
    assert_eq!(stats.repo_co_access, 1);
    assert_eq!(stats.repo_cross_day, 1);
    assert_eq!(stats.access.pairs, 3);
    assert_eq!(stats.access.writes, 2);
    assert_eq!(stats.access.reads, 3);
    assert_eq!(stats.gui.google_docs, 1);
    assert_eq!(stats.agents, 3);
}

#[test]
fn the_reference_matcher_finds_the_chat() {
    let dir = dataset();
    let mut source = open(&dir);
    let mut detector = ReferenceDetector::default();
    let summary = run(&mut source, &mut detector, 10, |_, _| {});
    assert!(summary.failures.is_empty(), "{:?}", summary.failures);
    let mut chat = (0, 0);
    let mut repo = (0, 0);
    for row in &summary.score.rows {
        match row.key.carrier {
            CarrierKind::UserTurn => {
                chat.0 += row.counts.expected;
                chat.1 += row.counts.found;
            }
            CarrierKind::ToolResult => {
                repo.0 += row.counts.expected;
                repo.1 += row.counts.found;
            }
            _ => {}
        }
    }
    assert_eq!(chat, (3, 3));
    // The reference routes a bash result as Direct, so a channel label is
    // not aligned: the eval's reference cannot see bash channels.
    assert_eq!(repo, (1, 0));
}

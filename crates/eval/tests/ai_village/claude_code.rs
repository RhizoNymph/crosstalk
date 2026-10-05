//! The Claude Code stream on a synthetic SDK log.

use std::path::Path;

use crosstalk_eval::corpus::{Coverage, Fidelity, TraceSource, World};
use crosstalk_eval::datasets::ai_village::claude_code::{calls, entries};
use crosstalk_eval::datasets::ai_village::{AiVillageSource, Mode, Stats};
use crosstalk_eval::location::SpanLocationExt;
use crosstalk_eval::pipeline::{ReferenceDetector, run};
use crosstalk_eval::truth::{CarrierKind, Expectation, MatchNeed, RouteExpectation, Tier};
use crosstalk_spec::observed::message::MessageBody;
use serde_json::{Value, json};

use super::fixture::{self, ALICE, BOB, CAROL, CLAUDE_CODE, GENERAL, anthropic, responses, table};

const SESSION: &str = "sdk-session-1";
const E1: &str = "e0000001-0000-4000-8000-000000000001";
const E2: &str = "e0000001-0000-4000-8000-000000000002";
const E3: &str = "e0000001-0000-4000-8000-000000000003";
const ALICE_SAYS: &str = "Please review my \"parser\" fix in the tracker repo today";
const BOB_SAYS: &str = "Bob's deploy finished: the tracker is live at the new URL now";
const CC_SAYS: &str = "Thanks Alice, I am reviewing the parser change right now";

fn row(id: &str, at: &str, kind: &str, subtype: Option<&str>, content: Value) -> Value {
    json!({"id": id, "agent_id": CLAUDE_CODE, "sdk_session_id": SESSION, "message_uuid": null,
           "message_type": kind, "message_subtype": subtype, "content": content, "created_at": at})
}

fn block(id: &str, at: &str, message: &str, block: Value) -> Value {
    row(
        id,
        at,
        "assistant",
        None,
        json!({"type": "assistant", "message": {"id": message, "role": "assistant", "model": "claude-test-1",
               "content": [block], "stop_reason": null,
               "usage": {"input_tokens": 3, "output_tokens": 5, "cache_read_input_tokens": 100, "cache_creation_input_tokens": 10}}}),
    )
}

fn result(id: &str, at: &str, call: &str, text: &str) -> Value {
    row(
        id,
        at,
        "user",
        None,
        json!({"type": "user", "message": {"role": "user", "content": [
            {"type": "tool_result", "tool_use_id": call, "content": [{"type": "text", "text": text}]}
        ]}}),
    )
}

fn events_text(events: Value) -> String {
    serde_json::to_string_pretty(
        &json!({"events": events, "hasMore": false, "agentStatus": {"unseenEventsCount": 0}}),
    )
    .unwrap_or_default()
}

fn talk(id: &str, speaker: &str, content: &str) -> Value {
    json!({"actionType": "AGENT_TALK", "agentName": speaker, "content": content, "createdAt": "3/2/2026, 10:00:00 AM PST", "id": id})
}

fn get_events(id: &str) -> Value {
    json!({"type": "tool_use", "id": id, "name": "mcp__village__get_events", "input": {}})
}

/// Writes the dataset; returns its directory.
fn dataset() -> tempfile::TempDir {
    let dir = fixture::dir();
    let root = dir.path();
    fixture::base(root);
    let first = events_text(json!([
        talk(E1, "Alice", ALICE_SAYS),
        talk("e-own", "Opus (Claude Code)", "my own words, which are not a transmission"),
        talk("e-unknown", "Nobody", "a speaker the directory does not know at all"),
        {"actionType": "START_USING_COMPUTER", "agentName": "Bob", "createdAt": "x", "id": "e-start"},
    ]));
    let second = events_text(json!([
        talk(E1, "Alice", ALICE_SAYS),
        talk(E2, "Bob", BOB_SAYS)
    ]));
    let third = events_text(json!([talk(
        E3,
        "Carol",
        "Carol says something nobody will read in time"
    )]));
    table(
        root,
        "claude_code_messages",
        &[
            // Rows are stored out of order, as the real table is.
            block(
                "r05",
                "2026-03-02 18:00:05",
                "msg_2",
                json!({"type": "tool_use", "id": "tu_2", "name": "mcp__village__chat_message", "input": {"content": CC_SAYS}}),
            ),
            row(
                "r01",
                "2026-03-02 18:00:00",
                "system",
                Some("init"),
                json!({"type": "system", "subtype": "init"}),
            ),
            block(
                "r02",
                "2026-03-02 18:00:01",
                "msg_1",
                json!({"type": "thinking", "thinking": "check events", "signature": "[BLOB_REMOVED]"}),
            ),
            block("r03", "2026-03-02 18:00:02", "msg_1", get_events("tu_1")),
            result("r04", "2026-03-02 18:00:03", "tu_1", &first),
            block(
                "r04b",
                "2026-03-02 18:00:04",
                "msg_2",
                json!({"type": "text", "text": "Replying and checking again"}),
            ),
            result("r06", "2026-03-02 18:00:06", "tu_2", "sent"),
            block("r07", "2026-03-02 18:00:07", "msg_2", get_events("tu_3")),
            result("r08", "2026-03-02 18:00:08", "tu_3", &second),
            block(
                "r09",
                "2026-03-02 18:00:09",
                "msg_3",
                json!({"type": "text", "text": "All caught up."}),
            ),
            row(
                "r10",
                "2026-03-02 18:00:10",
                "result",
                Some("success"),
                json!({"type": "result"}),
            ),
            row(
                "r11",
                "2026-03-02 18:00:11",
                "system",
                Some("compact_boundary"),
                json!({"type": "system"}),
            ),
            row(
                "r12",
                "2026-03-02 18:00:12",
                "user",
                None,
                json!({"type": "user", "message": {"role": "user", "content": [{"type": "text", "text": "This session is being continued: summary"}]}}),
            ),
            block("r13", "2026-03-02 18:00:13", "msg_4", get_events("tu_4")),
            result("r14", "2026-03-02 18:00:14", "tu_4", &third),
            row(
                "r15",
                "2026-03-02 18:00:15",
                "system",
                Some("compact_boundary"),
                json!({"type": "system"}),
            ),
            block(
                "r16",
                "2026-03-02 18:00:16",
                "msg_5",
                json!({"type": "text", "text": "Fresh context."}),
            ),
        ],
    );
    let output = |content: &str| {
        anthropic(
            "Posting to chat",
            "toolu_x",
            "send_message_to_chat",
            json!({"message": content}),
        )
    };
    table(
        root,
        "events",
        &[
            json!({"id": E1, "event_index": 1, "data": {"actionType": "AGENT_TALK", "speakerId": ALICE, "roomId": GENERAL, "content": ALICE_SAYS, "output": output(ALICE_SAYS)}, "created_at": "2026-03-02 17:59:00.000001"}),
            json!({"id": E2, "event_index": 2, "data": {"actionType": "AGENT_TALK", "speakerId": BOB, "roomId": GENERAL, "content": BOB_SAYS,
                   "output": responses("Telling the others", "call_b", "send_message_to_chat", json!({"message": BOB_SAYS}))}, "created_at": "2026-03-02 18:00:07.5"}),
            json!({"id": E3, "event_index": 3, "data": {"actionType": "AGENT_TALK", "speakerId": CAROL, "roomId": GENERAL, "content": "x"}, "created_at": "2026-03-02 18:00:13.5"}),
            json!({"id": "e-other", "event_index": 4, "data": {"actionType": "WAIT", "agentId": ALICE}, "created_at": "2026-03-02 18:00:00"}),
        ],
    );
    table(
        root,
        "chat_messages",
        &[
            fixture::chat(
                "c1",
                "2026-03-02 18:00:05.5",
                Some(CLAUDE_CODE),
                GENERAL,
                CC_SAYS,
            ),
            fixture::chat(
                "c2",
                "2026-03-02 17:59:00",
                Some(ALICE),
                GENERAL,
                ALICE_SAYS,
            ),
        ],
    );
    dir
}

fn worlds(root: &Path) -> (Vec<World>, Stats) {
    let mut source = AiVillageSource::open(root, Mode::ClaudeCode { limit: None })
        .unwrap_or_else(|e| panic!("{e}"));
    let worlds: Vec<World> = source
        .worlds()
        .map(|w| w.unwrap_or_else(|e| panic!("{e}")))
        .collect();
    (worlds, source.stats())
}

#[test]
fn entries_split_into_contexts_and_calls() {
    let dir = dataset();
    let all = entries::load(dir.path()).unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(all.len(), 17);
    assert!(all.windows(2).all(|w| w[0].at <= w[1].at));
    let ranges = entries::contexts(&all);
    assert_eq!(ranges.len(), 3);
    let first = calls::context(&all[ranges[0].clone()]);
    let ids: Vec<&str> = first.calls.iter().map(|c| c.message_id.as_str()).collect();
    assert_eq!(ids, vec!["msg_1", "msg_2", "msg_3"]);
    assert!(first.calls[0].request.is_empty());
    // msg_2 sees msg_1 and its result; msg_3 sees msg_2 whole, then both of
    // its results, though they arrived between its blocks.
    assert_eq!(first.calls[1].request.len(), 2);
    let third = &first.calls[2].request;
    assert_eq!(third.len(), 5);
    assert!(matches!(&third[2].message().body, MessageBody::Assistant(parts) if parts.len() == 3));
    assert!(matches!(&third[3].message().body, MessageBody::Tool(_)));
    assert!(matches!(&third[4].message().body, MessageBody::Tool(_)));
    let readers: Vec<(String, Option<usize>)> = first
        .results
        .iter()
        .map(|r| (r.tool.clone(), r.reader))
        .collect();
    assert_eq!(
        readers,
        vec![
            ("mcp__village__get_events".to_owned(), Some(1)),
            ("mcp__village__chat_message".to_owned(), Some(2)),
            ("mcp__village__get_events".to_owned(), Some(2)),
        ]
    );
    let usage = first.calls[0].usage.unwrap_or_else(|| panic!("usage"));
    assert_eq!((usage.input(), usage.cache_read()), (113, 100));
    let second = calls::context(&all[ranges[1].clone()]);
    assert_eq!(second.calls.len(), 1);
    assert!(matches!(
        &second.calls[0].request[0].message().body,
        MessageBody::User(_)
    ));
    assert_eq!(second.results[0].reader, None);
}

#[test]
fn get_events_deliveries_are_construction_labels() {
    let dir = dataset();
    let (worlds, stats) = worlds(dir.path());
    assert_eq!(worlds.len(), 3);
    let world = &worlds[0];
    assert!(
        world
            .key()
            .as_str()
            .starts_with("claude-code/0000-2026-03-02")
    );
    assert_eq!(world.coverage(), Coverage::Partial);
    let cc: Vec<_> = world
        .exchanges()
        .iter()
        .filter(|e| e.agent().name == "Opus (Claude Code)")
        .collect();
    assert_eq!(cc.len(), 3);
    assert!(cc.iter().all(|e| e.fidelity() == Fidelity::Reconstructed));
    let labels: Vec<_> = world
        .truth()
        .iter()
        .filter_map(|e| match e {
            Expectation::Transmission(t) => Some(t.label()),
            _ => None,
        })
        .collect();
    assert_eq!(labels.len(), 2);
    let alice = labels[0];
    assert_eq!(alice.from.name, "Alice");
    assert_eq!(alice.to.name, "Opus (Claude Code)");
    assert_eq!(alice.reader_exchange, cc[1].id());
    assert_eq!(alice.route, RouteExpectation::Direct);
    assert_eq!(alice.carrier, CarrierKind::ToolResult);
    assert_eq!(alice.tier, Tier::Construction);
    // The content sits JSON-escaped in the result, and the sender's tool
    // arguments escape it the same way.
    assert_eq!(alice.content.text, ALICE_SAYS.replace('"', "\\\""));
    assert_eq!(alice.needs, MatchNeed::Exact);
    let reader = world
        .exchange(alice.reader_exchange)
        .unwrap_or_else(|| panic!("reader"));
    let message = reader
        .message(alice.content.at.message())
        .unwrap_or_else(|| panic!("message"));
    assert_eq!(
        alice.content.at.text(message).ok().as_deref(),
        Some(alice.content.text.as_str())
    );
    let sender = world
        .exchange(alice.sender_exchange.unwrap_or_else(|| panic!("sender")))
        .unwrap_or_else(|| panic!("sender exchange"));
    assert_eq!(sender.fidelity(), Fidelity::Synthetic);
    assert!(sender.at() < reader.at());
    let bob = labels[1];
    assert_eq!(bob.from.name, "Bob");
    assert_eq!(bob.reader_exchange, cc[2].id());
    assert_eq!(bob.content.text, BOB_SAYS);
    match &stats {
        Stats::ClaudeCode(stats) => {
            assert_eq!(stats.contexts, 3);
            assert_eq!(stats.calls, 5);
            assert_eq!(stats.get_events_results, 3);
            assert_eq!(stats.get_events_unread, 1);
            assert_eq!(stats.labels, 2);
            assert_eq!(stats.redeliveries, 1);
            assert_eq!(stats.own_talks, 1);
            assert_eq!(stats.unknown_speaker, 1);
            assert_eq!(stats.chat_writes, 1);
            assert_eq!(stats.chat_writes_matched, 1);
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn the_reference_matcher_finds_every_delivery() {
    let dir = dataset();
    let mut source = AiVillageSource::open(dir.path(), Mode::ClaudeCode { limit: Some(1) })
        .unwrap_or_else(|e| panic!("{e}"));
    let mut detector = ReferenceDetector::default();
    let summary = run(&mut source, &mut detector, 10, |_, _| {});
    assert!(summary.failures.is_empty(), "{:?}", summary.failures);
    let mut expected = 0;
    let mut found = 0;
    for row in &summary.score.rows {
        expected += row.counts.expected;
        found += row.counts.found;
    }
    assert_eq!((expected, found), (2, 2));
}

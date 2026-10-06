//! The extraction step over hand-built deltas, on the memory ledger:
//! which tool results pair with which calls
//! (`flow.extract.result-pairs-with-history-call`), replayed results
//! (`flow.extract.replayed-result-read-once`) and the configured
//! extractors. Carried over unchanged from the gateway's tests of the
//! step.

use crosstalk_spec::derived::flow::resource::{Host, Locator};
use crosstalk_spec::ids::{AgentId, ExchangeId};
use crosstalk_spec::observed::message::{
    AssistantPart, MessageBody, Text, ToolCallId, ToolName, ToolOutcome, ToolResult,
};
use crosstalk_spec::support::NonEmpty;
use serde_json::json;

use super::support::{
    AGENT, Delta, Harness, PAGE, bash, calls, get, held_locators, held_writes, page, post, reads,
    result, tool_call, write_results,
};
use crate::extract::step::MemoryExtractionLedger;
use crate::extract::{ExtractConfig, WriteOutcome};

fn memory(config: ExtractConfig) -> Harness<MemoryExtractionLedger> {
    Harness::new(MemoryExtractionLedger::new(), config)
}

/// The wiki converter's shape: a new conversation whose request carries
/// the GET and its result.
#[tokio::test]
async fn a_result_pairs_with_its_call_in_the_same_request() {
    let step = memory(ExtractConfig::default());
    let out = step
        .delta(Delta::new(
            10,
            100,
            vec![
                calls(vec![get("call_1")]),
                result("call_1", "the relay index"),
            ],
            Some(MessageBody::Assistant(vec![AssistantPart::Text(Text(
                "Read it.".to_owned(),
            ))])),
        ))
        .await;
    assert_eq!(reads(&out), vec![(ExchangeId::from_ulid(10), page())]);
}

/// A call carried by an earlier delta's inputs of the same conversation
/// (a compacted history) pairs with a later result.
#[tokio::test]
async fn a_result_pairs_with_a_history_call_of_an_earlier_delta() {
    let step = memory(ExtractConfig::default());
    let first = step
        .delta(Delta::new(10, 100, vec![calls(vec![get("call_1")])], None))
        .await;
    assert!(reads(&first).is_empty());
    let second = step
        .delta(Delta::new(
            11,
            100,
            vec![result("call_1", "the relay index")],
            None,
        ))
        .await;
    assert_eq!(reads(&second), vec![(ExchangeId::from_ulid(11), page())]);
}

/// The call in an output, its result in the next delta: as before.
#[tokio::test]
async fn a_result_pairs_with_the_call_of_an_earlier_output() {
    let step = memory(ExtractConfig::default());
    let first = step
        .delta(Delta::new(
            10,
            100,
            vec![],
            Some(calls(vec![get("call_1")])),
        ))
        .await;
    assert!(reads(&first).is_empty());
    let second = step
        .delta(Delta::new(
            11,
            100,
            vec![result("call_1", "the relay index")],
            None,
        ))
        .await;
    assert_eq!(reads(&second), vec![(ExchangeId::from_ulid(11), page())]);
}

/// A POST made in one conversation's output, whose result arrives in a new
/// conversation's request together with the same call: the held write is
/// released with its outcome.
#[tokio::test]
async fn a_held_write_is_released_by_its_result_in_another_conversation() {
    let step = memory(ExtractConfig::default());
    let first = step
        .delta(Delta::new(
            10,
            100,
            vec![],
            Some(calls(vec![post("call_1")])),
        ))
        .await;
    let held = held_writes(&first);
    assert_eq!(held.len(), 1);
    let second = step
        .delta(Delta::new(
            11,
            200,
            vec![calls(vec![post("call_1")]), result("call_1", "saved")],
            None,
        ))
        .await;
    assert_eq!(
        write_results(&second),
        vec![(held[0], WriteOutcome::Delivered)]
    );
    // Not extracted again as a fresh write.
    assert!(held_writes(&second).is_empty());
}

/// Another call that reuses the id in another conversation (different
/// arguments) does not take the held write; its own reads are extracted.
#[tokio::test]
async fn a_reused_call_id_does_not_release_another_calls_write() {
    let step = memory(ExtractConfig::default());
    let first = step
        .delta(Delta::new(
            10,
            100,
            vec![],
            Some(calls(vec![post("call_1")])),
        ))
        .await;
    assert_eq!(held_writes(&first).len(), 1);
    let second = step
        .delta(Delta::new(
            11,
            200,
            vec![
                calls(vec![get("call_1")]),
                result("call_1", "the relay index"),
            ],
            None,
        ))
        .await;
    assert!(write_results(&second).is_empty());
    assert_eq!(reads(&second), vec![(ExchangeId::from_ulid(11), page())]);
    // The held write still takes its own result in its conversation.
    let third = step
        .delta(Delta::new(12, 100, vec![result("call_1", "saved")], None))
        .await;
    assert_eq!(
        write_results(&third),
        vec![(held_writes(&first)[0], WriteOutcome::Delivered)]
    );
}

/// A new conversation that replays a transcript the agent already received
/// reads nothing again.
#[tokio::test]
async fn a_replayed_result_is_read_once() {
    let step = memory(ExtractConfig::default());
    let first = step
        .delta(Delta::new(
            10,
            100,
            vec![],
            Some(calls(vec![get("call_1")])),
        ))
        .await;
    assert!(reads(&first).is_empty());
    let second = step
        .delta(Delta::new(
            11,
            100,
            vec![result("call_1", "the relay index")],
            None,
        ))
        .await;
    assert_eq!(reads(&second).len(), 1);
    let replay = step
        .delta(Delta::new(
            12,
            200,
            vec![
                calls(vec![get("call_1")]),
                result("call_1", "the relay index"),
            ],
            None,
        ))
        .await;
    assert!(reads(&replay).is_empty());
    // Replayed twice in one request, or again later: still once.
    let again = step
        .delta(Delta::new(
            13,
            300,
            vec![
                calls(vec![get("call_2")]),
                result("call_2", "v2"),
                calls(vec![get("call_2")]),
                result("call_2", "v2"),
            ],
            None,
        ))
        .await;
    assert_eq!(reads(&again).len(), 1);
    // The same call with another result is a new delivery.
    let changed = step
        .delta(Delta::new(
            14,
            400,
            vec![calls(vec![get("call_1")]), result("call_1", "edited")],
            None,
        ))
        .await;
    assert_eq!(reads(&changed), vec![(ExchangeId::from_ulid(14), page())]);
}

/// A result whose call the step never saw is dropped.
#[tokio::test]
async fn a_result_without_a_call_is_dropped() {
    let step = memory(ExtractConfig::default());
    let out = step
        .delta(Delta::new(
            10,
            100,
            vec![result("call_1", "the relay index")],
            None,
        ))
        .await;
    assert!(out.is_empty());
}

/// The step extracts with its configuration: a fetch tool configured by
/// name reads its URL.
#[tokio::test]
async fn the_configured_fetch_tools_are_extracted() {
    let get_webpage = |id: &str| tool_call(id, "get_webpage", json!({ "url": PAGE }));
    let step = memory(ExtractConfig::default());
    let out = step
        .delta(Delta::new(
            10,
            100,
            vec![calls(vec![get_webpage("call_1")]), result("call_1", "page")],
            None,
        ))
        .await;
    assert!(reads(&out).is_empty());
    let config = ExtractConfig::from_json(r#"{"fetch_tools": ["get_webpage"]}"#)
        .unwrap_or_else(|error| panic!("config: {error}"));
    let step = memory(config);
    let out = step
        .delta(Delta::new(
            10,
            100,
            vec![calls(vec![get_webpage("call_1")]), result("call_1", "page")],
            None,
        ))
        .await;
    assert_eq!(reads(&out), vec![(ExchangeId::from_ulid(10), page())]);
}

/// `01KXE46805TY443EM5HE3VE2YF`: the result of a `glab api … | jq` that
/// printed nothing (and the empty result of a turn's second call) has no
/// text to locate a read at, so no read is recorded; the same call with
/// text is read (`flow.extract.read-locates-its-result`).
#[tokio::test]
async fn a_result_without_text_is_no_read() {
    let step = memory(ExtractConfig::default());
    step.delta(Delta::new(
        10,
        100,
        vec![],
        Some(calls(vec![get("call_1"), get("call_2")])),
    ))
    .await;
    let empty = MessageBody::Tool(NonEmpty::new(ToolResult {
        call_id: ToolCallId("call_2".to_owned()),
        content: Vec::new(),
        outcome: ToolOutcome::Success,
    }));
    let out = step
        .delta(Delta::new(11, 100, vec![result("call_1", ""), empty], None))
        .await;
    assert_eq!(reads(&out), vec![]);
    step.delta(Delta::new(
        12,
        100,
        vec![],
        Some(calls(vec![get("call_3")])),
    ))
    .await;
    let out = step
        .delta(Delta::new(
            13,
            100,
            vec![result("call_3", "the relay index")],
            None,
        ))
        .await;
    assert_eq!(reads(&out), vec![(ExchangeId::from_ulid(13), page())]);
}

/// Each result teaches its agent's context in its conversation: the clone
/// and directory one call made name the next call's file, for that agent
/// only (`flow.extract.shell-state-from-observed`).
#[tokio::test]
async fn results_teach_the_agents_own_context() {
    let config = ExtractConfig::from_json(r#"{ "persistent_shells": ["bash"] }"#)
        .unwrap_or_else(|error| panic!("config: {error}"));
    let step = memory(config);
    let other = AgentId::from_ulid(2);
    let clone =
        "git clone https://gitlab.com/ai-village-agents/village/atlas.git /w/atlas && cd /w/atlas";
    step.delta(Delta::of(
        AGENT,
        10,
        100,
        vec![],
        Some(calls(vec![bash("c1", clone)])),
    ))
    .await;
    let out = step
        .delta(Delta::of(
            AGENT,
            11,
            100,
            vec![result("c1", "Cloning into '/w/atlas'...")],
            Some(calls(vec![bash("c2", "echo done >> NOTES.md")])),
        ))
        .await;
    assert_eq!(
        held_locators(&out),
        vec![Locator::File {
            host: Some(Host(
                "gitlab.com/ai-village-agents/village/atlas".to_owned()
            )),
            path: "/NOTES.md".to_owned(),
        }]
    );
    let out = step
        .delta(Delta::of(
            other,
            12,
            100,
            vec![],
            Some(calls(vec![bash("c3", "echo done >> NOTES.md")])),
        ))
        .await;
    assert_eq!(
        held_locators(&out),
        vec![Locator::Opaque {
            tool: ToolName("bash".to_owned()),
            key: "NOTES.md".to_owned(),
        }],
        "another agent's shell knows no directory"
    );
    let out = step
        .delta(Delta::of(
            AGENT,
            13,
            100,
            vec![result("c2", "")],
            Some(calls(vec![bash("c4", "git push")])),
        ))
        .await;
    assert_eq!(
        held_locators(&out),
        vec![Locator::Repository {
            host: Host("gitlab.com".to_owned()),
            owner: "ai-village-agents/village".to_owned(),
            name: "atlas".to_owned(),
        }]
    );
}

//! AI Village's Claude Code stream (`ai-village/claude_code_messages.jsonl.gz`)
//! replayed through the reconstruct consumer.
//!
//! The rows of the main SDK session are put in time order and grouped into
//! API calls by `content.message.id`. Each call becomes one replayed
//! exchange: its request is the history Claude Code held then (a fixed
//! system prompt, the user prompt, every earlier call's response and every
//! tool result), its response the call's assistant message. A
//! `compact_boundary` record empties the history; the synthetic summary
//! turn that follows opens the next one. A synthetic error message (an
//! authentication failure Claude Code wrote itself) is a failed exchange
//! that adds nothing to the history. Every exchange carries the corpus's
//! stable synthetic credential and the session id, as a replay does.

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader};

use crosstalk_spec::ids::MessageHash;
use crosstalk_spec::interfaces::l3_reconstruction::ThreadOutcome;
use crosstalk_spec::interfaces::l3_reconstruction::agents::AgentReads;
use crosstalk_spec::observed::client::ClientContext;
use crosstalk_spec::observed::exchange::{Exchange, ExchangeFailure};
use crosstalk_spec::observed::message::json::canonicalize;
use crosstalk_spec::observed::message::{
    AssistantPart, MessageBody, Reasoning, Text, ToolArguments, ToolCall, ToolCallId,
    ToolExecution, ToolName, ToolOutcome, ToolResult, ToolResultContent, UserPart,
};
use crosstalk_spec::paging::{PageRequest, PageSize};
use crosstalk_spec::support::NonEmpty;
use crosstalk_testkit::build::ExchangeBuilder;
use serde_json::Value;

use super::{dataset, parse_time, replay_client};
use crate::consumer::Handled;
use crate::tests::rig::Rig;
use crate::thread::outcome_kind;

const SESSION: &str = "0a15c7c2-01ad-49cc-9765-9c4425204407";

/// One row of the stream.
struct Row {
    created_at: String,
    id: String,
    kind: String,
    subtype: Option<String>,
    content: Value,
}

fn text(value: &Value) -> String {
    value.as_str().unwrap_or_default().to_owned()
}

fn assistant_part(block: &Value) -> AssistantPart {
    match block.get("type").and_then(Value::as_str) {
        Some("thinking") => AssistantPart::Reasoning(Reasoning::Visible {
            text: Text(text(&block["thinking"])),
            signature: block
                .get("signature")
                .and_then(Value::as_str)
                .filter(|signature| !signature.is_empty())
                .map(str::to_owned),
        }),
        Some("tool_use") => {
            let arguments = block.get("input").map(Value::to_string).unwrap_or_default();
            AssistantPart::ToolCall(ToolCall {
                id: ToolCallId(text(&block["id"])),
                name: ToolName(text(&block["name"])),
                arguments: match canonicalize(&arguments) {
                    Ok(json) => ToolArguments::Json(json),
                    Err(_) => ToolArguments::Invalid(arguments),
                },
                execution: ToolExecution::Client,
                signature: None,
            })
        }
        _ => AssistantPart::Text(Text(text(&block["text"]))),
    }
}

fn tool_content(content: &Value) -> Vec<ToolResultContent> {
    match content {
        Value::String(text) => vec![ToolResultContent::Text(Text(text.clone()))],
        Value::Array(blocks) => blocks
            .iter()
            .map(|block| match block.get("type").and_then(Value::as_str) {
                Some("text") => ToolResultContent::Text(Text(text(&block["text"]))),
                _ => ToolResultContent::Text(Text("[image]".to_owned())),
            })
            .collect(),
        _ => Vec::new(),
    }
}

/// A user row as messages: its tool results as one tool message, or its
/// text as a user message.
fn user_body(content: &Value) -> Option<MessageBody> {
    match &content["message"]["content"] {
        Value::String(text) => Some(MessageBody::User(vec![UserPart::Text(Text(text.clone()))])),
        Value::Array(blocks) => {
            let results: Vec<ToolResult> = blocks
                .iter()
                .filter(|block| block.get("type").and_then(Value::as_str) == Some("tool_result"))
                .map(|block| ToolResult {
                    call_id: ToolCallId(text(&block["tool_use_id"])),
                    content: tool_content(&block["content"]),
                    outcome: if block.get("is_error").and_then(Value::as_bool) == Some(true) {
                        ToolOutcome::Error
                    } else {
                        ToolOutcome::Success
                    },
                })
                .collect();
            match NonEmpty::from_vec(results) {
                Some(results) => Some(MessageBody::Tool(results)),
                None => Some(MessageBody::User(
                    blocks
                        .iter()
                        .map(|block| UserPart::Text(Text(text(&block["text"]))))
                        .collect(),
                )),
            }
        }
        _ => None,
    }
}

/// The main session's rows, in time order.
fn rows(path: &std::path::Path) -> Vec<Row> {
    let file = std::fs::File::open(path).expect("dataset opens");
    let reader = BufReader::new(flate2::read::GzDecoder::new(file));
    let mut rows = Vec::new();
    for line in reader.lines() {
        let line = line.expect("dataset line");
        let row: Value = serde_json::from_str(&line).expect("dataset JSON");
        if row["sdk_session_id"] != SESSION {
            continue;
        }
        rows.push(Row {
            created_at: text(&row["created_at"]),
            id: text(&row["id"]),
            kind: text(&row["message_type"]),
            subtype: row["message_subtype"].as_str().map(str::to_owned),
            content: row["content"].clone(),
        });
    }
    rows.sort_by(|a, b| (&a.created_at, &a.id).cmp(&(&b.created_at, &b.id)));
    rows
}

/// One API call: the history sent, and the response (or the failure).
struct Call {
    request: Vec<MessageHash>,
    response: Option<MessageHash>,
    at: String,
    /// The first call after a compact boundary.
    after_boundary: bool,
}

/// What the replay saw, per outcome.
#[derive(Debug, Default)]
struct Tally {
    outcomes: BTreeMap<&'static str, usize>,
    calls: usize,
    failed: usize,
    boundaries: usize,
    compactions_where_expected: usize,
    unexpected: Vec<String>,
}

fn exchange(rig: &mut Rig, client: &ClientContext, call: &Call) -> Exchange {
    let at = parse_time(&call.at).expect("dataset time");
    let builder = ExchangeBuilder::new(&mut rig.scene.ids)
        .started_at(at)
        .client(client.clone())
        .request(call.request.clone());
    match call.response {
        Some(response) => builder.response(response),
        None => builder.failed(ExchangeFailure::Upstream { status: 401 }),
    }
    .build()
}

/// Replay every call of the main session through the consumer and check
/// each outcome: the first call starts the conversation, the first call
/// after each compact boundary compacts the conversation before it, and
/// every other call (resumed queries included) extends the current one.
#[tokio::test]
#[ignore = "reads the AI Village dataset from ~/Data"]
async fn ai_village_claude_code_stream_threads_as_one_compacting_conversation() {
    let Some(path) = dataset("ai-village/claude_code_messages.jsonl.gz") else {
        return;
    };
    let rows = rows(&path);
    let mut rig = Rig::new();
    let client = replay_client(
        "ai-village",
        "opus-4.5-claude-code",
        Some(SESSION.to_owned()),
    );
    let system = rig
        .scene
        .system("You are Claude Code, Anthropic's official CLI for Claude.")
        .await;
    let mut history = vec![system];
    let mut tally = Tally::default();
    let mut pending: Option<(String, String, Vec<AssistantPart>, bool)> = None;
    let mut boundary = false;
    let mut current = None;
    let mut agent = None;
    let mut index = 0;
    let total = rows.len();
    loop {
        let row = rows.get(index);
        index += 1;
        let continues_call = matches!(
            (row, &pending),
            (Some(row), Some((id, ..))) if row.kind == "assistant"
                && row.content["message"]["id"].as_str() == Some(id.as_str())
        );
        if !continues_call && let Some((_, at, parts, failed)) = pending.take() {
            let response = if failed {
                None
            } else {
                Some(rig.scene.put(&MessageBody::Assistant(parts)).await)
            };
            let call = Call {
                request: history.clone(),
                response,
                at,
                after_boundary: std::mem::take(&mut boundary),
            };
            let exchange = exchange(&mut rig, &client, &call);
            let handled = rig.deliver(&exchange).await.expect("reconstructed");
            let Handled::Threaded {
                agent: attributed,
                outcome,
            } = handled
            else {
                panic!("call {} not threaded: {handled:?}", tally.calls);
            };
            assert_eq!(*agent.get_or_insert(attributed), attributed, "one agent");
            tally.calls += 1;
            *tally.outcomes.entry(outcome_kind(&outcome)).or_default() += 1;
            let conversation = outcome.delta().conversation;
            let expected = match (&outcome, call.after_boundary, current) {
                (ThreadOutcome::Starts { .. }, _, None) => true,
                (ThreadOutcome::Compacts { predecessor, .. }, true, Some(before)) => {
                    tally.compactions_where_expected += 1;
                    *predecessor == before
                }
                (ThreadOutcome::Extends { .. }, false, Some(before)) => conversation == before,
                _ => false,
            };
            if !expected && tally.unexpected.len() < 10 {
                tally.unexpected.push(format!(
                    "call {} at {}: {} (after boundary: {})",
                    tally.calls,
                    call.at,
                    outcome_kind(&outcome),
                    call.after_boundary
                ));
            }
            current = Some(conversation);
            match call.response {
                Some(response) => history.push(response),
                None => tally.failed += 1,
            }
        }
        let Some(row) = row else {
            break;
        };
        match (row.kind.as_str(), row.subtype.as_deref()) {
            ("assistant", _) => {
                let id = text(&row.content["message"]["id"]);
                let failed = row.content.get("error").is_some();
                let blocks = row.content["message"]["content"]
                    .as_array()
                    .cloned()
                    .unwrap_or_default();
                let entry =
                    pending.get_or_insert_with(|| (id, row.created_at.clone(), Vec::new(), failed));
                entry.2.extend(blocks.iter().map(assistant_part));
            }
            ("system", Some("compact_boundary")) => {
                tally.boundaries += 1;
                boundary = true;
                history = vec![system];
            }
            ("user", _) => {
                if let Some(body) = user_body(&row.content) {
                    history.push(rig.scene.put(&body).await);
                }
            }
            _ => {}
        }
        if index.is_multiple_of(20_000) {
            eprintln!("ai village: {index}/{total} rows, {} calls", tally.calls);
        }
    }
    eprintln!("ai village: {tally:?}");
    assert!(tally.unexpected.is_empty(), "{:#?}", tally.unexpected);
    assert_eq!(tally.outcomes.get("starts"), Some(&1));
    assert_eq!(tally.outcomes.get("forks"), None);
    assert_eq!(tally.compactions_where_expected, tally.boundaries);
    assert_eq!(tally.outcomes.get("compacts"), Some(&tally.boundaries));
    assert_eq!(
        tally.outcomes.get("extends").copied().unwrap_or_default(),
        tally.calls - 1 - tally.boundaries
    );
    // One agent, holding the corpus's credential and the session.
    let size = PageSize::new(10).expect("page size");
    let page = rig
        .agents
        .list(&Default::default(), &PageRequest { size, after: None })
        .await
        .expect("agents");
    assert_eq!(page.items().len(), 1);
}

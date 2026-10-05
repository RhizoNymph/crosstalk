//! lmcache's agentic traces (`lmcache/data/train-*.parquet`): the WildClaw
//! arxiv-digest session, where two re-runs of the task interleave under
//! one session id, replayed through the threader.
//!
//! Each row is one LLM call with its full OpenAI-format input. A call's
//! response is not in its row: it is the message at the call's input
//! length in the next call of the same run (runs told apart by their
//! system prompt and first user turn); a run's last call gets a synthetic
//! response.

use std::collections::BTreeMap;

use crosstalk_spec::ids::MessageHash;
use crosstalk_spec::interfaces::l3_reconstruction::{ThreadOutcome, Threader};
use crosstalk_spec::observed::message::json::canonicalize;
use crosstalk_spec::observed::message::{
    AssistantPart, MessageBody, SystemPart, Text, ToolArguments, ToolCall, ToolCallId,
    ToolExecution, ToolName, ToolOutcome, ToolResult, ToolResultContent, UserPart,
};
use crosstalk_spec::support::NonEmpty;
use crosstalk_testkit::build::ExchangeBuilder;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::{Field, Row};

use super::{dataset, replay_client};
use crate::tests::support::Scene;
use crate::thread::{MemoryConversations, outcome_kind};

const SESSION: &str = "wildclaw__01_Productivity_Flow_task_1_arxiv_digest__claude";

/// One OpenAI-format message.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct Turn {
    role: String,
    content: String,
    calls: Vec<(String, String, String)>,
    call_id: String,
}

fn field<'a>(row: &'a Row, name: &str) -> Option<&'a Field> {
    row.get_column_iter()
        .find(|(column, _)| column.as_str() == name)
        .map(|(_, value)| value)
}

fn string(row: &Row, name: &str) -> String {
    match field(row, name) {
        Some(Field::Str(text)) => text.clone(),
        _ => String::new(),
    }
}

fn list<'a>(row: &'a Row, name: &str) -> &'a [Field] {
    match field(row, name) {
        Some(Field::ListInternal(list)) => list.elements(),
        _ => &[],
    }
}

fn turn(element: &Field) -> Option<Turn> {
    let Field::Group(message) = element else {
        return None;
    };
    let calls = list(message, "tool_calls")
        .iter()
        .filter_map(|call| match call {
            Field::Group(call) => {
                let (name, arguments) = match field(call, "function") {
                    Some(Field::Group(function)) => {
                        (string(function, "name"), string(function, "arguments"))
                    }
                    _ => (String::new(), String::new()),
                };
                Some((string(call, "id"), name, arguments))
            }
            _ => None,
        })
        .collect();
    Some(Turn {
        role: string(message, "role"),
        content: string(message, "content"),
        calls,
        call_id: string(message, "tool_call_id"),
    })
}

fn body(turn: &Turn) -> MessageBody {
    match turn.role.as_str() {
        "system" => MessageBody::System(vec![SystemPart::Text(Text(turn.content.clone()))]),
        "assistant" => {
            let mut parts = Vec::new();
            if !turn.content.is_empty() {
                parts.push(AssistantPart::Text(Text(turn.content.clone())));
            }
            for (id, name, arguments) in &turn.calls {
                parts.push(AssistantPart::ToolCall(ToolCall {
                    id: ToolCallId(id.clone()),
                    name: ToolName(name.clone()),
                    arguments: match canonicalize(arguments) {
                        Ok(json) => ToolArguments::Json(json),
                        Err(_) => ToolArguments::Invalid(arguments.clone()),
                    },
                    execution: ToolExecution::Client,
                    signature: None,
                }));
            }
            MessageBody::Assistant(parts)
        }
        "tool" => MessageBody::Tool(NonEmpty::new(ToolResult {
            call_id: ToolCallId(turn.call_id.clone()),
            content: vec![ToolResultContent::Text(Text(turn.content.clone()))],
            outcome: ToolOutcome::Success,
        })),
        _ => MessageBody::User(vec![UserPart::Text(Text(turn.content.clone()))]),
    }
}

/// The session's calls, in file order.
fn calls() -> Option<Vec<Vec<Turn>>> {
    let mut calls = Vec::new();
    for shard in 0..5 {
        let path = dataset(&format!("lmcache/data/train-0000{shard}-of-00005.parquet"))?;
        let file = std::fs::File::open(&path).expect("parquet opens");
        let reader = SerializedFileReader::new(file).expect("parquet reads");
        for row in reader.get_row_iter(None).expect("rows") {
            let row = row.expect("row");
            if string(&row, "session_id") != SESSION {
                continue;
            }
            calls.push(list(&row, "input").iter().filter_map(turn).collect());
        }
    }
    Some(calls)
}

/// The run a call belongs to: its system prompt and first user turn.
fn run_of(call: &[Turn]) -> (Option<Turn>, Option<Turn>) {
    (
        call.iter().find(|turn| turn.role == "system").cloned(),
        call.iter().find(|turn| turn.role == "user").cloned(),
    )
}

/// Two interleaved runs each thread as one conversation; the last call,
/// whose harness rewrote two early tool results, forks its run where the
/// first rewrite is.
#[tokio::test]
#[ignore = "reads the lmcache dataset from ~/Data"]
async fn lmcache_interleaved_reruns_thread_as_two_conversations() {
    let Some(calls) = calls() else {
        return;
    };
    assert_eq!(calls.len(), 34, "the session's calls");
    let mut scene = Scene::new();
    let mut threader = scene.threader(MemoryConversations::new());
    let agent = scene.ids.agent();
    let client = replay_client("lmcache", SESSION, Some(SESSION.to_owned()));
    let mut outcomes: Vec<ThreadOutcome> = Vec::new();
    let mut roots: BTreeMap<(Option<Turn>, Option<Turn>), crosstalk_spec::ids::ConversationId> =
        BTreeMap::new();
    for (index, call) in calls.iter().enumerate() {
        let run = run_of(call);
        let response = calls[index + 1..]
            .iter()
            .find(|later| run_of(later) == run)
            .and_then(|later| later.get(call.len()))
            .map(body)
            .unwrap_or_else(|| {
                MessageBody::Assistant(vec![AssistantPart::Text(Text(format!(
                    "final answer {index}"
                )))])
            });
        let mut request: Vec<MessageHash> = Vec::new();
        for turn in call {
            request.push(scene.put(&body(turn)).await);
        }
        let response = scene.put(&response).await;
        let at = scene.tick();
        let exchange = ExchangeBuilder::new(&mut scene.ids)
            .started_at(at)
            .client(client.clone())
            .request(request)
            .response(response)
            .build();
        let outcome = threader.thread(&exchange, agent).await.expect("threaded");
        let conversation = outcome.delta().conversation;
        match &outcome {
            ThreadOutcome::Starts { .. } => {
                assert!(roots.insert(run, conversation).is_none(), "call {index}");
            }
            ThreadOutcome::Extends { .. } => {
                assert_eq!(roots.get(&run), Some(&conversation), "call {index}");
            }
            ThreadOutcome::Forks {
                parent,
                shared_prefix,
                ..
            } => {
                assert_eq!(index, 33, "only the rewritten call forks");
                assert_eq!(roots.get(&run), Some(parent));
                assert_eq!(*shared_prefix, 2);
            }
            ThreadOutcome::Compacts { .. } => panic!("call {index} compacts"),
        }
        outcomes.push(outcome);
    }
    let mut tally: BTreeMap<&str, usize> = BTreeMap::new();
    for outcome in &outcomes {
        *tally.entry(outcome_kind(outcome)).or_default() += 1;
    }
    assert_eq!(roots.len(), 2);
    assert_eq!(tally.get("starts"), Some(&2));
    assert_eq!(tally.get("forks"), Some(&1));
    assert_eq!(tally.get("extends"), Some(&31));
}

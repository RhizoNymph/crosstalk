//! A synthetic AI Village dataset written to a temporary directory: every
//! row is made up here, shaped like the real tables.

use std::fs::File;
use std::io::Write;
use std::path::Path;

use flate2::Compression;
use flate2::write::GzEncoder;
use serde_json::{Value, json};
use tempfile::TempDir;

pub const ALICE: &str = "00000000-0000-4000-8000-00000000000a";
pub const BOB: &str = "00000000-0000-4000-8000-00000000000b";
pub const CAROL: &str = "00000000-0000-4000-8000-00000000000c";
pub const CLAUDE_CODE: &str = "00000000-0000-4000-8000-0000000000cc";
pub const GENERAL: &str = "00000000-0000-4000-8000-0000000000f1";
pub const SIDE: &str = "00000000-0000-4000-8000-0000000000f2";

/// Writes `rows` as `<name>.jsonl.gz` under `root`.
pub fn table(root: &Path, name: &str, rows: &[Value]) {
    let path = root.join(format!("{name}.jsonl.gz"));
    let file = File::create(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    let mut encoder = GzEncoder::new(file, Compression::fast());
    for row in rows {
        writeln!(encoder, "{row}").unwrap_or_else(|e| panic!("{e}"));
    }
    encoder.finish().unwrap_or_else(|e| panic!("{e}"));
}

pub fn dir() -> TempDir {
    tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"))
}

/// Agents (Alice on Anthropic, Bob on OpenAI Responses, Carol on Gemini,
/// the Claude Code agent) and rooms.
pub fn base(root: &Path) {
    table(
        root,
        "agents",
        &[
            json!({"id": ALICE, "name": "Alice", "model_string": "claude-test-1", "created_at": "2026-01-01 00:00:00.0"}),
            json!({"id": BOB, "name": "Bob", "model_string": "gpt-test-1", "created_at": "2026-01-01 00:00:00.0"}),
            json!({"id": CAROL, "name": "Carol", "model_string": "gemini-test-1", "created_at": "2026-01-01 00:00:00.0"}),
            json!({"id": CLAUDE_CODE, "name": "Opus (Claude Code)", "model_string": "claude-code::claude-test-1", "created_at": "2026-01-01 00:00:00.0"}),
        ],
    );
    table(
        root,
        "chat_rooms",
        &[
            json!({"id": GENERAL, "name": "general", "deleted_at": null, "created_at": "2025-04-02 00:00:00.0"}),
            json!({"id": SIDE, "name": "side", "deleted_at": null, "created_at": "2026-03-01 00:00:00.0"}),
        ],
    );
    table(
        root,
        "village_goals",
        &[
            json!({"goal": "Old goal", "start_time": "2026-01-01 00:00:00.0", "end_time": "2026-07-01 00:00:00.0"}),
            json!({"goal": "Build something together", "start_time": "2026-07-01 00:00:00.0", "end_time": null}),
        ],
    );
    table(
        root,
        "agent_goals",
        &[
            json!({"agent_id": BOB, "name": "Ship the tracker", "description": "keep it live", "start_time": null, "end_time": null}),
        ],
    );
}

/// An Anthropic response with text and one tool call.
pub fn anthropic(text: &str, call_id: &str, tool: &str, input: Value) -> Value {
    json!({
        "id": format!("msg_{call_id}"), "role": "assistant", "type": "message", "model": "claude-test-1",
        "content": [
            {"type": "thinking", "thinking": format!("thinking about {tool}"), "signature": "[BLOB_REMOVED]"},
            {"type": "text", "text": text},
            {"type": "tool_use", "id": call_id, "name": tool, "input": input}
        ],
        "stop_reason": "tool_use", "usage": {"input_tokens": 1, "output_tokens": 1}
    })
}

/// An OpenAI Responses item list with reasoning, a message and a function
/// call.
pub fn responses(text: &str, call_id: &str, tool: &str, arguments: Value) -> Value {
    json!([
        {"id": "rs_1", "type": "reasoning", "summary": [{"type": "summary_text", "text": "Reasoning summary here"}], "encrypted_content": "gAAAAopaque"},
        {"id": "msg_1", "type": "message", "role": "assistant", "content": [{"type": "output_text", "text": text}]},
        {"id": "fc_1", "type": "function_call", "call_id": call_id, "name": tool, "arguments": arguments.to_string()}
    ])
}

/// A Gemini response with a thought and a function call without an id.
pub fn gemini(text: &str, tool: &str, args: Value) -> Value {
    json!({"candidates": [{"content": {"role": "model", "parts": [
        {"text": "a thought of mine", "thought": true},
        {"text": text},
        {"functionCall": {"name": tool, "args": args}, "thoughtSignature": "[BLOB_REMOVED]"}
    ]}, "finishReason": "STOP"}]})
}

pub fn turn(
    id: &str,
    session: &str,
    at: &str,
    action: Value,
    messages: Value,
    output: Option<&str>,
    error: Option<&str>,
) -> Value {
    json!({
        "id": id, "session_id": session, "agent_action": action, "agent_messages": messages,
        "output": output, "error": error, "system": null, "screenshot_is_redacted": false,
        "has_redaction_been_overruled": null, "created_at": at, "updated_at": at
    })
}

pub fn talk(
    id: &str,
    index: u64,
    at: &str,
    speaker: &str,
    room: &str,
    message: &str,
    content: &str,
) -> Value {
    json!({"id": id, "event_index": index, "data": {
        "actionType": "AGENT_TALK", "speakerId": speaker, "roomId": room, "messageId": message, "content": content
    }, "village_id": "v", "created_at": at, "updated_at": at})
}

pub fn chat(id: &str, at: &str, speaker: Option<&str>, room: &str, content: &str) -> Value {
    json!({
        "id": id, "agent_speaker_id": speaker, "user_speaker_id": null,
        "speaker_type": if speaker.is_some() { "agent" } else { "user" },
        "content": content, "room_id": room, "created_at": at, "updated_at": at, "has_been_approved": true
    })
}

pub fn send(content: &str) -> Value {
    json!({"action": "send_message_back_to_chat", "content": content})
}

pub fn bash(command: &str) -> Value {
    json!({"command": command})
}

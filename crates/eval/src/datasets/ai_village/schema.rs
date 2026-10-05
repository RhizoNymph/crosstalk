//! The rows the converter reads, as the tables dump them (SCHEMA.md). Only
//! the columns the converter uses are named; the rest are ignored.

use serde::Deserialize;
use serde_json::Value;

/// `agents.jsonl.gz`.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct AgentRow {
    pub id: String,
    pub name: String,
    #[serde(default)]
    pub model_string: Option<String>,
}

/// `chat_rooms.jsonl.gz`.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct RoomRow {
    pub id: String,
    pub name: String,
}

/// `computer_use_sessions.jsonl.gz`.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct SessionRow {
    pub id: String,
    pub agent_id: String,
}

/// `computer_use_turns.jsonl.gz`: one model call of the standard
/// scaffolding, its executed action and the tool's output.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct TurnRow {
    pub id: String,
    pub session_id: String,
    #[serde(default)]
    pub agent_action: Option<Value>,
    /// The raw model response, provider-shaped.
    #[serde(default)]
    pub agent_messages: Value,
    #[serde(default)]
    pub output: Option<String>,
    #[serde(default)]
    pub error: Option<String>,
    pub created_at: String,
}

/// `events.jsonl.gz`.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct EventRow {
    pub id: String,
    #[serde(default)]
    pub event_index: Option<u64>,
    pub data: Value,
    pub created_at: String,
}

impl EventRow {
    pub fn action(&self) -> Option<&str> {
        self.data.get("actionType").and_then(Value::as_str)
    }

    pub fn str(&self, name: &str) -> Option<&str> {
        self.data.get(name).and_then(Value::as_str)
    }

    /// The agent an event is about: `agentId`, else `speakerId`.
    pub fn agent(&self) -> Option<&str> {
        self.str("agentId").or_else(|| self.str("speakerId"))
    }
}

/// `chat_messages.jsonl.gz`.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct ChatRow {
    pub id: String,
    pub speaker_type: String,
    #[serde(default)]
    pub agent_speaker_id: Option<String>,
    pub content: String,
    pub room_id: String,
    pub created_at: String,
}

/// `agent_memories.jsonl.gz`.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct MemoryRow {
    pub agent_id: String,
    pub content: String,
    pub created_at: String,
}

/// `village_goals.jsonl.gz`.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct VillageGoalRow {
    pub goal: String,
    #[serde(default)]
    pub start_time: Option<String>,
    #[serde(default)]
    pub end_time: Option<String>,
}

/// `agent_goals.jsonl.gz`.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct AgentGoalRow {
    pub agent_id: String,
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub start_time: Option<String>,
    #[serde(default)]
    pub end_time: Option<String>,
}

/// `claude_code_messages.jsonl.gz`: one raw Agent SDK entry.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct ClaudeCodeRow {
    pub id: String,
    pub sdk_session_id: String,
    pub message_type: String,
    #[serde(default)]
    pub message_subtype: Option<String>,
    pub content: Value,
    pub created_at: String,
}

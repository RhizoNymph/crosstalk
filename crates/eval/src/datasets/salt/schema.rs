//! The parts of a SALT-NLP trace file (`traces/<exp>/<cond>/repNNN.json.gz`)
//! the converter reads. Unknown fields are ignored.

use std::collections::BTreeMap;

use serde::Deserialize;
use serde_json::Value;

#[derive(Debug, Deserialize)]
pub struct Trace {
    #[serde(default)]
    pub condition_id: Option<String>,
    pub run_config: RunConfig,
    pub results: Vec<Episode>,
}

#[derive(Debug, Deserialize)]
pub struct RunConfig {
    /// Model route per agent (`alice`, `bob`).
    #[serde(default)]
    pub models: BTreeMap<String, String>,
}

#[derive(Debug, Deserialize)]
pub struct Episode {
    pub episode_index: u64,
    pub agents: BTreeMap<String, AgentRecord>,
    /// Delivered messages, in recorded order.
    #[serde(default)]
    pub channel_transcript: Vec<Delivery>,
    /// Every tool event of the episode, with episode-global ids.
    #[serde(default)]
    pub events: Vec<Event>,
    /// One entry per LLM call attempt.
    #[serde(default)]
    pub llm_usage: Vec<Usage>,
}

#[derive(Debug, Deserialize)]
pub struct AgentRecord {
    /// The agent's context as replayed to the model, in OpenAI chat format.
    #[serde(default)]
    pub messages: Vec<RawMessage>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RawMessage {
    pub role: String,
    #[serde(default)]
    pub content: Value,
    #[serde(default)]
    pub tool_calls: Option<Vec<RawToolCall>>,
    #[serde(default)]
    pub tool_call_id: Option<String>,
    #[serde(default)]
    pub thinking_blocks: Option<Vec<Value>>,
    #[serde(default)]
    pub reasoning_content: Option<Value>,
    #[serde(default)]
    pub reasoning: Option<Value>,
    #[serde(default)]
    pub reasoning_items: Option<Vec<Value>>,
    #[serde(default)]
    pub provider_specific_fields: Option<Value>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RawToolCall {
    pub id: String,
    pub function: RawFunction,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RawFunction {
    pub name: String,
    /// A JSON string as the model wrote it (sometimes an object).
    #[serde(default)]
    pub arguments: Value,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Delivery {
    pub event_id: u64,
    pub sender: String,
    pub receiver: String,
    pub content: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Event {
    pub event_id: u64,
    pub actor: String,
    #[serde(default)]
    pub tool: Option<String>,
    #[serde(default)]
    pub success: Option<bool>,
    #[serde(default)]
    pub recipient: Option<String>,
    #[serde(default)]
    pub error: Option<String>,
    /// For `send_message`: the content, cut to 120 characters.
    #[serde(default)]
    pub content: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Usage {
    pub actor: String,
    pub response_status: String,
    #[serde(default)]
    pub finish_reason: Option<String>,
    #[serde(default)]
    pub input_tokens: Option<u64>,
    #[serde(default)]
    pub output_tokens: Option<u64>,
    #[serde(default)]
    pub cached_input_tokens: Option<u64>,
    #[serde(default)]
    pub reasoning_tokens: Option<u64>,
    #[serde(default)]
    pub requested_model: Option<String>,
}

impl Usage {
    pub fn accepted(&self) -> bool {
        self.response_status == "accepted"
    }
}

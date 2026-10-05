//! The results JSON τ²-bench writes (`data/tau2/results/final/*.json`).
//!
//! Only what the converter reads is modelled; unknown members are ignored.

use serde::Deserialize;
use serde_json::{Map, Value};

/// One results file: one agent model on one domain, every task × trial.
#[derive(Debug, Clone, Deserialize)]
pub struct Results {
    pub info: Info,
    #[serde(default)]
    pub tasks: Vec<Task>,
    #[serde(default)]
    pub simulations: Vec<Simulation>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Info {
    pub user_info: UserInfo,
    pub agent_info: AgentInfo,
    pub environment_info: EnvironmentInfo,
}

#[derive(Debug, Clone, Deserialize)]
pub struct UserInfo {
    /// `user_simulator`, or `dummy_user` when the agent works alone.
    pub implementation: String,
    #[serde(default)]
    pub llm: Option<String>,
    #[serde(default)]
    pub global_simulation_guidelines: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AgentInfo {
    /// `llm_agent`, `llm_agent_gt` (given the resolution steps) or
    /// `llm_agent_solo` (given a ticket, no user).
    pub implementation: String,
    #[serde(default)]
    pub llm: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct EnvironmentInfo {
    pub domain_name: String,
    #[serde(default)]
    pub policy: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Task {
    pub id: String,
    #[serde(default)]
    pub user_scenario: Option<UserScenario>,
    #[serde(default)]
    pub ticket: Option<String>,
    #[serde(default)]
    pub evaluation_criteria: Option<EvaluationCriteria>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct UserScenario {
    #[serde(default)]
    pub persona: Option<String>,
    pub instructions: Instructions,
}

/// A scenario's instructions: structured, or plain text.
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum Instructions {
    Structured(StructuredInstructions),
    Text(String),
}

#[derive(Debug, Clone, Deserialize)]
pub struct StructuredInstructions {
    pub domain: String,
    pub reason_for_call: String,
    #[serde(default)]
    pub known_info: Option<String>,
    #[serde(default)]
    pub unknown_info: Option<String>,
    pub task_instructions: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct EvaluationCriteria {
    #[serde(default)]
    pub actions: Option<Vec<Action>>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Action {
    pub requestor: String,
    pub name: String,
    #[serde(default)]
    pub arguments: Map<String, Value>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Simulation {
    pub id: String,
    pub task_id: String,
    #[serde(default)]
    pub trial: Option<u32>,
    #[serde(default)]
    pub messages: Vec<RawMessage>,
}

/// One message. Assistant and user messages made by a model carry
/// `raw_data` (the provider's response); the agent's hard-coded greeting
/// does not.
#[derive(Debug, Clone, Deserialize)]
pub struct RawMessage {
    pub role: String,
    #[serde(default)]
    pub content: Option<String>,
    #[serde(default)]
    pub tool_calls: Option<Vec<RawCall>>,
    #[serde(default)]
    pub timestamp: Option<String>,
    #[serde(default)]
    pub raw_data: Option<Value>,
    #[serde(default)]
    pub usage: Option<Usage>,
    /// A tool message's requestor: `assistant` (the agent) or `user`.
    #[serde(default)]
    pub requestor: Option<String>,
    /// A tool message's call id.
    #[serde(default)]
    pub id: Option<String>,
    #[serde(default)]
    pub error: Option<bool>,
}

impl RawMessage {
    /// The text, when there is any.
    pub fn text(&self) -> Option<&str> {
        self.content.as_deref().filter(|text| !text.is_empty())
    }

    pub fn calls(&self) -> &[RawCall] {
        self.tool_calls.as_deref().unwrap_or_default()
    }

    /// Whether a model produced it.
    pub fn is_model_call(&self) -> bool {
        self.raw_data.as_ref().is_some_and(|raw| !raw.is_null())
    }

    /// The provider's finish reason.
    pub fn finish_reason(&self) -> Option<&str> {
        self.raw_data
            .as_ref()
            .and_then(|raw| raw.get("finish_reason"))
            .and_then(Value::as_str)
    }
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct RawCall {
    pub id: String,
    pub name: String,
    #[serde(default)]
    pub arguments: Value,
    #[serde(default)]
    pub requestor: Option<String>,
}

#[derive(Debug, Clone, Copy, Deserialize)]
pub struct Usage {
    #[serde(default)]
    pub prompt_tokens: Option<u64>,
    #[serde(default)]
    pub completion_tokens: Option<u64>,
}

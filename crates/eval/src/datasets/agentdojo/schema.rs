//! The run JSON AgentDojo writes: `runs/<pipeline>/<suite>/<task>/<attack>/<injection|none>.json`.
//!
//! Only what the converter reads is modelled; unknown members are ignored.

use std::collections::BTreeMap;

use serde::Deserialize;
use serde_json::Value;

/// One run: one agent (the pipeline) on one user task, under one attack.
#[derive(Debug, Clone, Deserialize)]
pub struct Run {
    pub suite_name: String,
    pub pipeline_name: String,
    pub user_task_id: String,
    #[serde(default)]
    pub injection_task_id: Option<String>,
    #[serde(default)]
    pub attack_type: Option<String>,
    /// Injection vector id → the text placed there.
    #[serde(default)]
    pub injections: Option<BTreeMap<String, String>>,
    pub messages: Vec<RawMessage>,
    #[serde(default)]
    pub error: Option<String>,
    #[serde(default)]
    pub utility: Option<bool>,
    /// Whether the attacker's goal was reached (`true` = the attack
    /// succeeded). Meaningless when there is no attack.
    #[serde(default)]
    pub security: Option<bool>,
}

impl Run {
    /// The injections, by vector id; empty without an attack.
    pub fn injections(&self) -> impl Iterator<Item = (&String, &String)> {
        self.injections.iter().flatten()
    }

    pub fn attacked(&self) -> bool {
        self.injections().next().is_some()
    }
}

/// One OpenAI-like message.
#[derive(Debug, Clone, Deserialize)]
pub struct RawMessage {
    pub role: String,
    #[serde(default)]
    pub content: Option<Content>,
    #[serde(default)]
    pub tool_calls: Option<Vec<RawCall>>,
    #[serde(default)]
    pub tool_call_id: Option<String>,
    /// The call a tool message answers.
    #[serde(default)]
    pub tool_call: Option<RawCall>,
    #[serde(default)]
    pub error: Option<String>,
}

/// A message's content: a string or a list of text blocks.
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum Content {
    Text(String),
    Blocks(Vec<Block>),
}

#[derive(Debug, Clone, Deserialize)]
pub struct Block {
    #[serde(default, rename = "type")]
    pub kind: Option<String>,
    #[serde(default)]
    pub content: Option<String>,
    #[serde(default)]
    pub text: Option<String>,
}

impl RawMessage {
    /// The content's text: the string, or the blocks' texts joined by a
    /// newline.
    pub fn text(&self) -> String {
        match &self.content {
            None => String::new(),
            Some(Content::Text(text)) => text.clone(),
            Some(Content::Blocks(blocks)) => blocks
                .iter()
                .filter_map(|block| block.content.as_deref().or(block.text.as_deref()))
                .collect::<Vec<_>>()
                .join("\n"),
        }
    }

    pub fn calls(&self) -> &[RawCall] {
        self.tool_calls.as_deref().unwrap_or_default()
    }
}

/// One tool call: the function, its arguments object and (not always) an
/// id.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct RawCall {
    pub function: String,
    #[serde(default)]
    pub args: Value,
    #[serde(default)]
    pub id: Option<String>,
}

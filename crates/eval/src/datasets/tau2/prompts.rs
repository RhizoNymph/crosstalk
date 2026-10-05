//! System prompts, rebuilt from τ²-bench's templates (`tau2/agent/llm_agent.py`,
//! `tau2/user/user_simulator.py`) and what the results file records. The
//! results file stores neither prompt, so both are reconstructions.
//!
//! - `llm_agent`: the instruction and the domain policy.
//! - `llm_agent_solo`: the solo instruction, the policy and the ticket.
//! - `llm_agent_gt`: the ground-truth instruction, the policy and the
//!   resolution steps rendered from the task's expected actions. Python
//!   formats argument values with `str()`, which is only approximated here,
//!   so this prompt is `Synthetic`.
//! - The user simulator: the global guidelines (persona placeholder
//!   removed) and the scenario as `str(UserScenario)` renders it.

use serde_json::Value;

use super::schema::{Action, Info, Instructions, Task, UserScenario};
use crate::corpus::Fidelity;

pub const AGENT_INSTRUCTION: &str =
    "You are a customer service agent that helps the user according to the <policy> provided below.
In each turn you can either:
- Send a message to the user.
- Make a tool call.
You cannot do both at the same time.

Try to be helpful and always follow the policy. Always make sure you generate valid JSON only.";

pub const AGENT_GT_INSTRUCTION: &str = "You are testing that our user simulator is working correctly.
User simulator will have an issue for you to solve.
You must behave according to the <policy> provided below.
To make following the policy easier, we give you the list of resolution steps you are expected to take.
These steps involve either taking an action or asking the user to take an action.

In each turn you can either:
- Send a message to the user.
- Make a tool call.
You cannot do both at the same time.

Try to be helpful and always follow the policy. Always make sure you generate valid JSON only.";

pub const AGENT_SOLO_INSTRUCTION: &str = "You are a customer service agent that helps the user according to the <policy> provided below.
You will be provided with a ticket that contains the user's request.
You will need to plan and call the appropriate tools to solve the ticket.

You cannot communicate with the user, only make tool calls.
Stop when you consider that you have solved the ticket.
To do so, send a message containing a single tool call to the `done` tool. Do not include any other tool calls in this last message.

Always follow the policy. Always make sure you generate valid JSON only.";

/// The agent's system prompt and how faithful it is.
pub fn agent_system_prompt(info: &Info, task: &Task) -> (String, Fidelity) {
    let policy = &info.environment_info.policy;
    match info.agent_info.implementation.as_str() {
        "llm_agent_gt" => (
            format!(
                "<instructions>\n{AGENT_GT_INSTRUCTION}\n</instructions>\n<policy>\n{policy}\n</policy>\n<resolution_steps>\n{}\n</resolution_steps>",
                resolution_steps(task)
            ),
            Fidelity::Synthetic,
        ),
        "llm_agent_solo" => (
            format!(
                "<instructions>\n{AGENT_SOLO_INSTRUCTION}\n</instructions>\n<policy>\n{policy}\n</policy>\n<ticket>\n{}\n</ticket>",
                task.ticket.as_deref().unwrap_or("None")
            ),
            Fidelity::Reconstructed,
        ),
        "llm_agent" => (
            format!(
                "<instructions>\n{AGENT_INSTRUCTION}\n</instructions>\n<policy>\n{policy}\n</policy>"
            ),
            Fidelity::Reconstructed,
        ),
        _ => (
            format!(
                "<instructions>\n{AGENT_INSTRUCTION}\n</instructions>\n<policy>\n{policy}\n</policy>"
            ),
            Fidelity::Synthetic,
        ),
    }
}

/// The user simulator's system prompt.
pub fn user_system_prompt(info: &Info, task: &Task) -> String {
    let guidelines = info
        .user_info
        .global_simulation_guidelines
        .as_deref()
        .unwrap_or_default()
        .replace("<PERSONA_GUIDELINES>", "");
    let scenario = task
        .user_scenario
        .as_ref()
        .map_or_else(|| "None".to_owned(), scenario_text);
    format!("{guidelines}\n\n<scenario>\n{scenario}\n</scenario>")
}

/// `str(UserScenario)`.
pub fn scenario_text(scenario: &UserScenario) -> String {
    let mut lines = Vec::new();
    if let Some(persona) = &scenario.persona {
        lines.push("Persona:".to_owned());
        lines.push(indent(persona));
    }
    lines.push("Instructions:".to_owned());
    lines.push(indent(&instructions_text(&scenario.instructions)));
    lines.join("\n")
}

/// `str(StructuredUserInstructions)`, or the plain text.
fn instructions_text(instructions: &Instructions) -> String {
    match instructions {
        Instructions::Text(text) => text.clone(),
        Instructions::Structured(structured) => {
            let mut lines = vec![
                format!("Domain: {}", structured.domain),
                format!("Reason for call:\n{}", indent(&structured.reason_for_call)),
            ];
            if let Some(known) = &structured.known_info {
                lines.push(format!("Known info:\n{}", indent(known)));
            }
            if let Some(unknown) = &structured.unknown_info {
                lines.push(format!("Unknown info:\n{}", indent(unknown)));
            }
            lines.push(format!(
                "Task instructions:\n{}",
                indent(&structured.task_instructions)
            ));
            lines.join("\n")
        }
    }
}

/// Python's `textwrap.indent(text, "\t")`: a tab before every line that is
/// not only whitespace.
fn indent(text: &str) -> String {
    text.split_inclusive('\n')
        .map(|line| {
            if line.trim().is_empty() {
                line.to_owned()
            } else {
                format!("\t{line}")
            }
        })
        .collect()
}

/// `LLMGTAgent.make_agent_instructions_from_actions`, with arguments
/// formatted as JSON values.
fn resolution_steps(task: &Task) -> String {
    let actions: &[Action] = task
        .evaluation_criteria
        .as_ref()
        .and_then(|criteria| criteria.actions.as_deref())
        .unwrap_or_default();
    actions
        .iter()
        .enumerate()
        .map(|(step, action)| {
            let call = format!(
                "{}({})",
                action.name,
                action
                    .arguments
                    .iter()
                    .map(|(name, value)| format!("{name}={}", python_str(value)))
                    .collect::<Vec<_>>()
                    .join(", ")
            );
            let instruction = if action.requestor == "user" {
                format!("Instruct the user to perform the following action: {call}.")
            } else {
                format!("Perform the following action: {call}.")
            };
            format!("[Step {}] {instruction}", step + 1)
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// Python's `str()` of a JSON value, for the common cases.
fn python_str(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        Value::Bool(true) => "True".to_owned(),
        Value::Bool(false) => "False".to_owned(),
        Value::Null => "None".to_owned(),
        other => other.to_string(),
    }
}

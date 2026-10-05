//! Where a detector's unlabelled predictions sit.
//!
//! AI Village worlds have partial coverage, so a prediction no label
//! explains is unjudged, not false. [`Unlabelled`] sorts those predictions
//! by what the reader was reading when the text arrived: a chat turn, a
//! `get_events` result, a web read (`curl`, `wget`, a script fetching a
//! URL), a repository command, a local file, a GUI action. Most text that
//! two agents share without one telling the other comes from the same web
//! page or repository, so this is the false-positive picture to look at.

use std::collections::BTreeMap;

use crosstalk_spec::observed::message::{MessageBody, ToolArguments};
use serde::{Deserialize, Serialize};

use super::access::shell::commands;
use crate::corpus::World;
use crate::predict::Prediction;
use crate::reference::route::find_call;
use crate::score::align::aligns;
use crate::truth::Expectation;

/// Unlabelled predictions by reader carrier and source kind, with a few
/// examples of each.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Unlabelled {
    pub total: u64,
    pub labelled: u64,
    pub by_source: BTreeMap<String, u64>,
    pub examples: BTreeMap<String, Vec<String>>,
}

const EXAMPLES: usize = 3;

impl Unlabelled {
    /// Counts one world's predictions.
    pub fn observe(&mut self, world: &World, predictions: &[Prediction]) {
        let labels: Vec<_> = world
            .truth()
            .iter()
            .filter_map(|e| match e {
                Expectation::Transmission(t) => Some(t),
                _ => None,
            })
            .collect();
        for prediction in predictions {
            self.total += 1;
            if labels.iter().any(|label| aligns(prediction, label)) {
                self.labelled += 1;
                continue;
            }
            let (source, example) = classify(world, prediction);
            *self.by_source.entry(source.clone()).or_default() += 1;
            let examples = self.examples.entry(source).or_default();
            if examples.len() < EXAMPLES {
                examples.push(example);
            }
        }
    }
}

fn snippet(text: &str) -> String {
    let mut out: String = text.chars().take(120).collect();
    if out.len() < text.len() {
        out.push('…');
    }
    out.replace('\n', " ")
}

/// What the reader was reading where the prediction sits.
fn classify(world: &World, prediction: &Prediction) -> (String, String) {
    let Some(exchange) = world.exchange(prediction.reader_exchange) else {
        return ("unknown".to_owned(), String::new());
    };
    let Some(message) = exchange.message(prediction.read_at.part.message) else {
        return ("unknown".to_owned(), String::new());
    };
    let read = message
        .part_text(prediction.read_at.part.index)
        .ok()
        .and_then(|text| {
            text.get(
                prediction.read_at.range.start() as usize..prediction.read_at.range.end() as usize,
            )
            .map(snippet)
        })
        .unwrap_or_default();
    let source = match &message.body {
        MessageBody::System(_) => "system prompt".to_owned(),
        MessageBody::User(_) => "user turn".to_owned(),
        MessageBody::Assistant(_) => "assistant".to_owned(),
        MessageBody::Tool(results) => {
            let call = results
                .iter()
                .nth(usize::from(prediction.read_at.part.index))
                .and_then(|result| find_call(exchange.request(), &result.call_id));
            match call {
                None => "tool result: unknown call".to_owned(),
                Some(call) => {
                    let arguments = match &call.arguments {
                        ToolArguments::Json(json) => json.0.as_str(),
                        ToolArguments::Invalid(raw) => raw.as_str(),
                    };
                    format!("tool result: {}", tool_kind(&call.name.0, arguments))
                }
            }
        }
    };
    (
        source,
        format!("{} -> {}: {read}", prediction.from.name, prediction.to.name),
    )
}

/// A tool call's kind, from its name and (for bash) its command.
pub fn tool_kind(name: &str, arguments: &str) -> String {
    if name.contains("get_events") {
        return "get_events".to_owned();
    }
    let command = serde_json::from_str::<serde_json::Value>(arguments)
        .ok()
        .and_then(|v| v.get("command").and_then(|c| c.as_str()).map(str::to_owned));
    let Some(command) = command else {
        return match name {
            "computer" | "use_computer" => "gui".to_owned(),
            other => other.to_owned(),
        };
    };
    let mut kinds: Vec<&str> = Vec::new();
    for simple in commands(&command) {
        let Some(program) = simple.words.first() else {
            continue;
        };
        let program = program.rsplit('/').next().unwrap_or(program);
        kinds.push(match program {
            "curl" | "wget" => "web read",
            "git" | "gh" | "glab" => "repository",
            "python" | "python3" | "node" if command.contains("http") => "script fetching a URL",
            "cat" | "head" | "tail" | "sed" | "grep" | "rg" | "ls" | "find" | "wc" | "diff"
            | "jq" => "local file",
            _ => continue,
        });
    }
    for preferred in [
        "web read",
        "script fetching a URL",
        "repository",
        "local file",
    ] {
        if kinds.contains(&preferred) {
            return format!("bash {preferred}");
        }
    }
    "bash other".to_owned()
}

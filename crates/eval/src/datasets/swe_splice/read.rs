//! The synthetic read spliced into the reader's trajectory, in its
//! harness's own format.
//!
//! - **Editor view** (OpenHands and SWE-agent, which have
//!   `str_replace_editor`): the call is `{"command": "view", "path": P}` and
//!   the result is `Here's the result of running `cat -n` on P:` followed by
//!   the file's lines, each prefixed with its number right-aligned in six
//!   columns and a tab. SWE-agent prefixes every result with
//!   `OBSERVATION:`.
//! - **Shell cat** (mini-swe-agent, which has only `bash`): the call is
//!   `{"command": "cat -n P"}` and the result is the harness's JSON,
//!   `{"returncode": 0, "output": "…"}`, with the same numbered lines
//!   escaped inside the `output` string.
//!
//! The result's content location covers the numbered lines (escaped, for a
//! shell read), not the banner.

use std::fmt;

use crate::datasets::chat::{ChatFunction, ChatMessage, ChatToolCall};

/// The banner OpenHands and SWE-agent put before a viewed file.
pub fn view_banner(path: &str) -> String {
    format!("Here's the result of running `cat -n` on {path}:\n")
}

/// SWE-agent's prefix on every tool result.
pub const OBSERVATION: &str = "OBSERVATION:\n";

/// How the reader's harness reads a file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ReadForm {
    EditorView { observation: bool },
    ShellCat,
}

impl ReadForm {
    /// The form a trajectory's own tool use implies: an editor view when it
    /// ever calls `str_replace_editor`, with SWE-agent's prefix when most of
    /// its tool results carry it; a shell cat otherwise.
    pub fn of(messages: &[ChatMessage]) -> Self {
        let editor = messages
            .iter()
            .flat_map(ChatMessage::calls)
            .any(|call| call.function.name == "str_replace_editor");
        if !editor {
            return Self::ShellCat;
        }
        let tools: Vec<&ChatMessage> = messages.iter().filter(|m| m.role == "tool").collect();
        let prefixed = tools
            .iter()
            .filter(|m| m.text().starts_with(OBSERVATION))
            .count();
        Self::EditorView {
            observation: !tools.is_empty() && prefixed * 2 > tools.len(),
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::EditorView { .. } => "editor_view",
            Self::ShellCat => "shell_cat",
        }
    }

    /// The assistant message calling for the read, with call id `id`.
    pub fn call(self, path: &str, id: &str) -> ChatMessage {
        let (name, arguments) = match self {
            Self::EditorView { .. } => (
                "str_replace_editor",
                serde_json::json!({"command": "view", "path": path}),
            ),
            Self::ShellCat => (
                "bash",
                serde_json::json!({"command": format!("cat -n {path}")}),
            ),
        };
        ChatMessage {
            role: "assistant".into(),
            content: Some(String::new()),
            reasoning_content: Some(String::new()),
            tool_calls: Some(vec![ChatToolCall {
                id: Some(id.to_owned()),
                function: ChatFunction {
                    name: name.into(),
                    arguments: Some(arguments.to_string()),
                },
            }]),
            tool_call_id: None,
        }
    }

    /// The tool message answering call `id` with the file `body`, and the
    /// byte range of the numbered lines in its text.
    pub fn result(self, path: &str, id: &str, body: &str) -> (ChatMessage, (usize, usize)) {
        let lines = numbered(body);
        let (text, range) = match self {
            Self::EditorView { observation } => {
                let mut text = String::new();
                if observation {
                    text.push_str(OBSERVATION);
                }
                text.push_str(&view_banner(path));
                let start = text.len();
                text.push_str(&lines);
                let end = text.len();
                (text, (start, end))
            }
            Self::ShellCat => {
                let escaped =
                    serde_json::to_string(&lines).unwrap_or_else(|_| format!("{lines:?}"));
                let head = "{\n  \"returncode\": 0,\n  \"output\": ";
                let text = format!("{head}{escaped}\n}}");
                // Inside the quotes.
                let start = head.len() + 1;
                let end = head.len() + escaped.len() - 1;
                (text, (start, end))
            }
        };
        (
            ChatMessage {
                role: "tool".into(),
                content: Some(text),
                reasoning_content: None,
                tool_calls: None,
                tool_call_id: Some(id.to_owned()),
            },
            range,
        )
    }
}

impl fmt::Display for ReadForm {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// `cat -n`: each line of `body` as its number right-aligned in six
/// columns, a tab, the line and a newline.
pub fn numbered(body: &str) -> String {
    let mut out = String::with_capacity(body.len() + body.len() / 4);
    for (at, line) in body.lines().enumerate() {
        out.push_str(&format!("{:>6}\t{line}\n", at + 1));
    }
    out
}

//! Whether a bash access went through, judged from the command's output.
//!
//! The spec's `WriteOutcome`: a write is `Delivered` when the output shows
//! the tool's success, `Rejected` when it shows a failure, `Unknown` when it
//! shows neither (curl prints whatever the server answered). A rejected
//! write is recorded but never paired. A read needs a delivered result: a
//! read whose output shows a failure is no access.
//!
//! The village's bash turns carry no exit status, and git and gh write
//! progress to stderr, so the judgement is the output's text alone.

use crosstalk_spec::derived::flow::access::WriteOutcome;

use super::Tool;

/// Lines that open a failed command's output, any tool.
const FAILURE_LINES: &[&str] = &[
    "fatal:",
    "error:",
    "ERROR:",
    "curl: (",
    "HTTP 4",
    "HTTP 5",
    "GraphQL:",
    "gh: ",
    "glab: ",
    "wget: ",
    "remote: Permission",
    "remote: Invalid",
    "! [rejected]",
    "! [remote rejected]",
    "Permission denied",
    "could not",
    "Could not",
];

/// API error bodies (GitHub `message`, GitLab `message`/`error`).
const FAILURE_MESSAGES: &[&str] = &[
    "Bad credentials",
    "Not Found",
    "Requires authentication",
    "Validation Failed",
    "Resource not accessible",
    "401 Unauthorized",
    "403 Forbidden",
    "404 Not Found",
    "insufficient_scope",
    "invalid_token",
];

/// Whether `output` reports a failure.
pub fn failed(output: &str) -> bool {
    let lines = || output.lines().map(str::trim).filter(|l| !l.is_empty());
    // git push reports a rejection after its `To` line.
    if lines().any(|line| {
        line.starts_with("! [rejected]")
            || line.starts_with("! [remote rejected]")
            || line.starts_with("error: failed to push")
    }) {
        return true;
    }
    if lines()
        .next()
        .is_some_and(|first| FAILURE_LINES.iter().any(|m| first.starts_with(m)))
    {
        return true;
    }
    api_error(output)
}

/// A JSON error body: a top-level `"message"` or `"error"` naming a known
/// failure.
fn api_error(output: &str) -> bool {
    let head = output.trim_start();
    if !head.starts_with('{') {
        return false;
    }
    let Ok(value) = serde_json::from_str::<serde_json::Value>(head) else {
        return FAILURE_MESSAGES
            .iter()
            .any(|m| head.contains(&format!("\"message\": \"{m}")));
    };
    ["message", "error"].iter().any(|key| {
        value
            .get(key)
            .and_then(serde_json::Value::as_str)
            .is_some_and(|text| FAILURE_MESSAGES.iter().any(|m| text.contains(m)))
    })
}

/// A write's outcome from its tool and output.
pub fn write_outcome(tool: Tool, output: &str) -> WriteOutcome {
    if failed(output) {
        return WriteOutcome::Rejected;
    }
    let delivered = match tool {
        Tool::Git => output.lines().any(|line| {
            let line = line.trim();
            (line.contains("->") && !line.starts_with('!')) || line == "Everything up-to-date"
        }),
        Tool::GitHubCli | Tool::GitLabCli => {
            output.contains("https://")
                || output.trim_start().starts_with('✓')
                || created_body(output)
        }
        Tool::Curl | Tool::Wget => created_body(output),
    };
    if delivered {
        WriteOutcome::Delivered
    } else {
        WriteOutcome::Unknown
    }
}

/// A forge API's answer to a created or updated object.
fn created_body(output: &str) -> bool {
    let head = output.trim_start();
    head.starts_with('{')
        && ["\"html_url\"", "\"web_url\"", "\"created_at\""]
            .iter()
            .any(|key| head.contains(key))
}

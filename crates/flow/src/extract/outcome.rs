//! Write outcomes and delivered reads: the one place a tool's result is
//! judged.
//!
//! The rule (`flow.extract.write-outcome-classified`, from the eval spec
//! PR): without a result a write is `Unknown`; a result the wire flags as
//! an error is `Rejected`; otherwise the tool's content rule reads the
//! result's text (`Delivered` or `Rejected`), and a tool with no content
//! rule keeps the wire's word (`Success` is `Delivered`,
//! `ToolOutcome::Unknown` is `Unknown`). [`content_rule`] is the per-tool
//! table; [`write_outcome`] applies it.
//!
//! A read is recorded only with a result (`flow.extract.read-requires-result`)
//! that delivered the content: never for a result the same rule judges
//! `Rejected`, since an error message is not the resource's content.
//!
//! **Shell commands.** A shell tool has no content rule of its own, but an
//! access a known command makes is judged by that command's output
//! ([`CommandRule`], `flow.extract.shell-outcome-from-known-output`), after
//! the wire's error flag:
//!
//! | Command | `Rejected` | `Delivered` | otherwise |
//! | --- | --- | --- | --- |
//! | `git push` | a line opening with `! [rejected]`, `! [remote rejected]`, `error:`, `fatal:`, `remote: Permission`, `remote: Invalid`, `Permission denied` | a ref update (`a..b  main -> main`, `* [new branch]`), `Everything up-to-date` | `Unknown` |
//! | `git pull`, `fetch`, `clone` | a line opening with `fatal:` or `error:` | | the wire's word |
//! | `curl`, `wget` | the last HTTP status shown is 4xx/5xx (a `HTTP/x 404` status line from `-i`/`-I`/`-v`/`-S`, wget's `… 404 Not Found` and `ERROR 404:`, the code a `-w '%{http_code}'` printed last), or `curl: (N)` | the last status shown is 2xx/3xx | the wire's word |
//! | `gh`, `glab` | a line opening with `gh: `, `glab: `, `GraphQL:`, `error:`, `ERROR:`, `HTTP 4`/`HTTP 5`, `could not`, `failed to`, `X `, or ending in `(HTTP 4xx)`/`(HTTP 5xx)` | a printed `https://` URL or a `✓` line | the wire's word |
//!
//! The output is the whole call's (a script's commands share one result),
//! so a command's rule reads every line of it.

use crosstalk_spec::observed::message::{ToolOutcome, ToolResult, ToolResultContent};

use crate::extract::catalog::KnownTool;
use crate::extract::mcp::config::RefusalMarker;
use crate::extract::op::WriteOutcome;

/// How a known tool reports a refusal in its result text.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContentRule<'c> {
    /// Claude Code's refusals: a `<tool_use_error>` (a failed validation,
    /// a missing path, a file not read before editing) or the user
    /// declining the call.
    ClaudeCodeRefusal,
    /// A configured MCP tool's markers.
    Markers(&'c [RefusalMarker]),
}

const CLAUDE_CODE_REFUSALS: [&str; 2] = [
    "<tool_use_error>",
    "The user doesn't want to proceed with this tool use",
];

impl ContentRule<'_> {
    /// Whether `text` reports a refusal.
    pub fn refuses(self, text: &str) -> bool {
        match self {
            Self::ClaudeCodeRefusal => {
                let text = text.trim_start();
                CLAUDE_CODE_REFUSALS
                    .iter()
                    .any(|marker| text.starts_with(marker))
            }
            Self::Markers(markers) => markers.iter().any(|marker| marker.matches(text)),
        }
    }
}

/// The command an access of a shell call came from, whose output judges it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CommandRule {
    /// `git push`.
    GitPush,
    /// `git pull`, `git fetch`, `git clone`, `gh repo clone`.
    GitTransfer,
    /// `curl`, `wget`. `status_written`: a `-w` format prints the status
    /// code (`%{http_code}`, `%{response_code}`).
    Http { status_written: bool },
    /// `gh`, `glab`.
    ForgeCli,
}

impl CommandRule {
    /// What `text`, the command's output, says: `None` when it says
    /// neither, and the wire's word stands.
    pub fn judge(self, text: &str) -> Option<WriteOutcome> {
        let lines = || text.lines().map(str::trim).filter(|line| !line.is_empty());
        let opens = |markers: &[&str]| {
            lines().any(|line| markers.iter().any(|marker| line.starts_with(marker)))
        };
        match self {
            Self::GitPush => {
                if opens(&GIT_PUSH_FAILURES) {
                    Some(WriteOutcome::Rejected)
                } else if lines().any(ref_updated) {
                    Some(WriteOutcome::Delivered)
                } else {
                    Some(WriteOutcome::Unknown)
                }
            }
            Self::GitTransfer => opens(&["fatal:", "error:"]).then_some(WriteOutcome::Rejected),
            Self::Http { status_written } => {
                if opens(&["curl: ("]) {
                    return Some(WriteOutcome::Rejected);
                }
                let mut status = lines().rev().find_map(shown_status);
                if status_written && let Some(written) = lines().next_back().and_then(trailing_code)
                {
                    status = Some(written);
                }
                status.map(|code| {
                    if code >= 400 {
                        WriteOutcome::Rejected
                    } else {
                        WriteOutcome::Delivered
                    }
                })
            }
            Self::ForgeCli => {
                let failed = opens(&FORGE_CLI_FAILURES)
                    || lines().any(|line| {
                        line.ends_with(')')
                            && line
                                .rsplit_once("(HTTP ")
                                .and_then(|(_, code)| code.strip_suffix(')'))
                                .and_then(|code| code.parse::<u16>().ok())
                                .is_some_and(|code| code >= 400)
                    });
                if failed {
                    Some(WriteOutcome::Rejected)
                } else if lines().any(|line| line.starts_with("https://") || line.starts_with('✓'))
                {
                    Some(WriteOutcome::Delivered)
                } else {
                    None
                }
            }
        }
    }
}

const GIT_PUSH_FAILURES: [&str; 7] = [
    "! [rejected]",
    "! [remote rejected]",
    "error:",
    "fatal:",
    "remote: Permission",
    "remote: Invalid",
    "Permission denied",
];

const FORGE_CLI_FAILURES: [&str; 10] = [
    "gh: ",
    "glab: ",
    "GraphQL:",
    "error:",
    "ERROR:",
    "HTTP 4",
    "HTTP 5",
    "could not",
    "failed to",
    "X ",
];

/// A git push ref-update line: `a1b2c3d..e4f5a6b  main -> main`, a forced
/// `+ a...b main -> main (forced update)`, `* [new branch]  x -> x`.
fn ref_updated(line: &str) -> bool {
    if line == "Everything up-to-date" {
        return true;
    }
    if !line.contains(" -> ") || line.starts_with('!') {
        return false;
    }
    let first = line.trim_start_matches(['+', ' ']);
    first.starts_with("* [new ")
        || first
            .split_whitespace()
            .next()
            .and_then(|range| range.split_once(".."))
            .is_some_and(|(from, to)| {
                let to = to.trim_start_matches('.');
                hex(from) && hex(to)
            })
}

fn hex(text: &str) -> bool {
    text.len() >= 4 && text.bytes().all(|b| b.is_ascii_hexdigit())
}

/// The status a line shows: `HTTP/1.1 404 Not Found`, `HTTP/2 200`, a
/// `< HTTP/2 200` from `-v`, wget's `HTTP request sent, awaiting
/// response... 404 Not Found` and `ERROR 404: Not Found.`.
fn shown_status(line: &str) -> Option<u16> {
    let line = line.trim_start_matches(['<', ' ']);
    let code = if let Some(rest) = line.strip_prefix("HTTP/") {
        rest.split_whitespace().nth(1)?
    } else if let Some(rest) = line.strip_prefix("ERROR ") {
        rest.split(':').next()?
    } else if let Some((_, rest)) = line.split_once("awaiting response... ") {
        rest.split_whitespace().next()?
    } else {
        return None;
    };
    status_code(code)
}

/// A three-digit status code at the end of a line (`404`, `HTTP 404`,
/// `{"ok":true}200`).
fn trailing_code(line: &str) -> Option<u16> {
    let digits = line.len() - line.trim_end_matches(|c: char| c.is_ascii_digit()).len();
    if digits != 3 {
        return None;
    }
    status_code(&line[line.len() - 3..])
}

fn status_code(text: &str) -> Option<u16> {
    let code: u16 = text.parse().ok()?;
    (text.len() == 3 && (100..600).contains(&code)).then_some(code)
}

/// Each known tool's content rule, `None` for a tool whose result text the
/// extractor does not read.
pub fn content_rule<'c>(tool: &KnownTool<'c>) -> Option<ContentRule<'c>> {
    match tool {
        // File tools report refusals in text (Claude Code also flags them).
        KnownTool::File(_) => Some(ContentRule::ClaudeCodeRefusal),
        // A fetch's or a command's output is the page's or the program's;
        // only the wire's flag says whether it failed.
        KnownTool::Fetch(_) | KnownTool::Shell(_) | KnownTool::Http(_) => None,
        KnownTool::Mcp(mcp) if mcp.rule.refusal.is_empty() => None,
        KnownTool::Mcp(mcp) => Some(ContentRule::Markers(&mcp.rule.refusal)),
    }
}

/// What became of a write by `tool`, given its result if one arrived.
pub fn write_outcome(tool: &KnownTool<'_>, result: Option<&ToolResult>) -> WriteOutcome {
    let text = result.map(result_text);
    judge(tool, None, result, text.as_deref())
}

/// Whether `result` delivered the content a read by `tool` asked for.
pub fn read_delivered(tool: &KnownTool<'_>, result: Option<&ToolResult>) -> bool {
    result.is_some() && write_outcome(tool, result) != WriteOutcome::Rejected
}

/// The judgement of one access of a call by `tool`, made by the shell
/// command `command` (`None` for the tool's own rule), given the result
/// and its text ([`result_text`]) if one arrived. A read is kept when
/// this is not `Rejected`.
pub fn judge(
    tool: &KnownTool<'_>,
    command: Option<CommandRule>,
    result: Option<&ToolResult>,
    text: Option<&str>,
) -> WriteOutcome {
    let Some(result) = result else {
        return WriteOutcome::Unknown;
    };
    let text = text.unwrap_or_default();
    match result.outcome {
        ToolOutcome::Error => WriteOutcome::Rejected,
        outcome @ (ToolOutcome::Success | ToolOutcome::Unknown) => {
            if let Some(judged) = command.and_then(|rule| rule.judge(text)) {
                return judged;
            }
            match content_rule(tool) {
                Some(rule) if rule.refuses(text) => WriteOutcome::Rejected,
                Some(_) => WriteOutcome::Delivered,
                None if outcome == ToolOutcome::Success => WriteOutcome::Delivered,
                None => WriteOutcome::Unknown,
            }
        }
    }
}

/// The result's text parts, joined by newlines.
pub fn result_text(result: &ToolResult) -> String {
    let texts: Vec<&str> = result
        .content
        .iter()
        .filter_map(|part| match part {
            ToolResultContent::Text(text) => Some(text.0.as_str()),
            ToolResultContent::Media(_) | ToolResultContent::Unknown(_) => None,
        })
        .collect();
    texts.join("\n")
}

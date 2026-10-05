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
    let Some(result) = result else {
        return WriteOutcome::Unknown;
    };
    match result.outcome {
        ToolOutcome::Error => WriteOutcome::Rejected,
        outcome @ (ToolOutcome::Success | ToolOutcome::Unknown) => match content_rule(tool) {
            Some(rule) if rule.refuses(&result_text(result)) => WriteOutcome::Rejected,
            Some(_) => WriteOutcome::Delivered,
            None if outcome == ToolOutcome::Success => WriteOutcome::Delivered,
            None => WriteOutcome::Unknown,
        },
    }
}

/// Whether `result` delivered the content a read by `tool` asked for.
pub fn read_delivered(tool: &KnownTool<'_>, result: Option<&ToolResult>) -> bool {
    result.is_some() && write_outcome(tool, result) != WriteOutcome::Rejected
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

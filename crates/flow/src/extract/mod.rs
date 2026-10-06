//! L5 resource extraction: an assistant's tool call and its result, as the
//! resource accesses they imply.
//!
//! [`ToolExtractors`] implements the spec's
//! [`ResourceExtractor`] over every tool the extractors know
//! ([`catalog`]):
//!
//! | Tools | Module | Extraction |
//! | --- | --- | --- |
//! | file tools (Claude Code `Read`/`Write`/`Edit`/`MultiEdit`/`NotebookEdit`, OpenCode, pi, Gemini CLI, the text editor tool) | [`mod@file`] | `Structured` |
//! | fetch tools (`WebFetch`, the `web_fetch` server tool, OpenCode `webfetch`) | [`fetch`] | `Structured`, `Scanned` for a prompt argument |
//! | HTTP tools (`http_request {method, url, body?}`, configured names) | [`http`] | `Structured` |
//! | shell tools (`Bash`, Codex `shell`, Gemini CLI `run_shell_command`) | [`bash`] | `Parsed` |
//! | configured MCP tools (`mcp__<server>__<tool>`) | [`mcp`] | `Structured` |
//!
//! Every locator is canonical ([`resource`]), so two agents touching one
//! thing get one resource. Each family names the accesses a call implies
//! (candidates); [`outcome`] then judges the result once for all of them:
//! a write carries its [`WriteOutcome`], a read is kept only when the
//! result arrived and delivered the content
//! (`flow.extract.read-requires-result`). [`spans`] builds the stored
//! `AccessOp` with the spans a write carries.
//!
//! Extraction is pure and total: arbitrary arguments and results give
//! accesses or a typed `ExtractError`, never a panic
//! (`flow.extract.no-panic`).

pub mod args;
pub mod bash;
pub mod catalog;
pub mod context;
mod error;
pub mod fetch;
pub mod file;
pub mod http;
pub mod mcp;
pub mod op;
pub mod outcome;
pub mod resource;
pub mod sites;
pub mod spans;
pub mod step;

#[cfg(test)]
mod fuzz;
#[cfg(test)]
pub(crate) mod tests;

use crosstalk_spec::derived::flow::access::AccessKind;
use crosstalk_spec::interfaces::l5_flow::{ExtractError, ExtractedAccess, ResourceExtractor};
use crosstalk_spec::observed::message::{ToolCall, ToolResult};

pub use args::{ArgPath, InvalidArgPath};
pub use catalog::KnownTool;
pub use context::{ConversationContext, stated_cwd};
pub use mcp::config::{
    ConfigError, ExtractConfig, McpAccessRule, McpResource, McpServerConfig, McpToolRule,
    RefusalMarker, RuleOp,
};
pub use op::{Classified, ExtractedOp, WriteOutcome, WritePayload};
pub use resource::{AbsolutePath, KeyCanon, RepoId};
pub use sites::{HostPattern, MediaWikiSite, SitePath, SitesConfig};
pub use spans::{AccessOpError, access_op, write_spans};

use args::Args;
use op::Candidate;

/// Every known tool's extractor, for one conversation.
#[derive(Debug, Clone, Copy)]
pub struct ToolExtractors<'a> {
    config: &'a ExtractConfig,
    context: &'a ConversationContext,
}

impl<'a> ToolExtractors<'a> {
    pub fn new(config: &'a ExtractConfig, context: &'a ConversationContext) -> Self {
        Self { config, context }
    }

    /// The known tool `call` invokes, if any.
    pub fn identify(&self, call: &ToolCall) -> Option<KnownTool<'a>> {
        catalog::identify(&call.name, self.config)
    }

    /// The accesses `call` implies, each write with its outcome. A result
    /// for another call is ignored, as if none had arrived.
    pub fn extract_classified(
        &self,
        call: &ToolCall,
        result: Option<&ToolResult>,
    ) -> Result<Vec<Classified>, ExtractError> {
        let Some(tool) = self.identify(call) else {
            return Ok(Vec::new());
        };
        let result = result.filter(|result| result.call_id == call.id);
        let args = Args::parse(&call.arguments)?;
        let text = result.map(outcome::result_text);
        let candidates = match tool {
            KnownTool::File(file) => file::candidates(file, &call.name, &args, self.context)?,
            KnownTool::Fetch(fetch) => fetch::candidates(fetch, &args, self.config.sites())?,
            KnownTool::Shell(shell) => bash::candidates(
                shell,
                &call.name,
                &args,
                self.context,
                self.config.sites(),
                text.as_deref(),
            )?,
            KnownTool::Mcp(mcp) => {
                mcp::candidates(mcp, &call.name, &args, self.context, self.config.sites())?
            }
            KnownTool::Http(http) => http::tool_candidates(http, &args, self.config.sites())?,
        };
        let mut classified: Vec<Classified> = Vec::with_capacity(candidates.len());
        for Candidate {
            kind,
            locator,
            via,
            payload,
            rule,
            refuted,
        } in candidates
        {
            let judged = if refuted && result.is_some() {
                WriteOutcome::Rejected
            } else {
                outcome::judge(&tool, rule, result, text.as_deref())
            };
            let op = match kind {
                AccessKind::Write => ExtractedOp::Write {
                    outcome: judged,
                    payload,
                },
                AccessKind::Read if result.is_some() && judged != WriteOutcome::Rejected => {
                    ExtractedOp::Read
                }
                AccessKind::Read => continue,
            };
            let access = Classified { op, locator, via };
            if !classified.contains(&access) {
                classified.push(access);
            }
        }
        Ok(classified)
    }
}

impl ResourceExtractor for ToolExtractors<'_> {
    fn handles(&self, call: &ToolCall) -> bool {
        self.identify(call).is_some()
    }

    fn extract(
        &self,
        call: &ToolCall,
        result: Option<&ToolResult>,
    ) -> Result<Vec<ExtractedAccess>, ExtractError> {
        Ok(self
            .extract_classified(call, result)?
            .into_iter()
            .map(Classified::into_spec)
            .collect())
    }
}

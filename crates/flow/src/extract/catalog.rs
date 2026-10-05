//! The tools the extractors know, by name.
//!
//! File, fetch and shell tools are the built-in tables below, covering
//! Claude Code and the harnesses with the same kind of tools (OpenCode, pi,
//! Gemini CLI, Codex, OpenHands, the Anthropic text editor tool). MCP tools come from
//! the configuration ([`ExtractConfig`]), named as Claude Code names them:
//! `mcp__<server>__<tool>`. HTTP tools (`http_request {method, url,
//! body?}`) are the configured names ([`ExtractConfig::http_tools`]); a
//! call of one without a `method` is the fetch tool of that name, if any.
//! Further fetch tools are configured by name
//! ([`ExtractConfig::fetch_tools`]): each reads the URL in its `url`
//! argument ([`CONFIGURED_FETCH`]).

use crosstalk_spec::observed::message::ToolName;

use crate::extract::mcp::config::{ExtractConfig, McpServerConfig, McpToolRule};

/// A tool the extractors understand, with its schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KnownTool<'c> {
    File(&'static FileTool),
    Fetch(&'static FetchTool),
    Shell(&'static ShellTool),
    Mcp(McpTool<'c>),
    Http(HttpTool),
}

/// A configured HTTP tool: arguments `url` and `method`, and an optional
/// body. `fallback` is the fetch tool of the same name, which a call
/// without a `method` is (`web_fetch`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HttpTool {
    pub fallback: Option<&'static FetchTool>,
}

/// A configured MCP tool.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct McpTool<'c> {
    pub server: &'c McpServerConfig,
    pub rule: &'c McpToolRule,
}

/// A tool that reads or writes one file named by an argument.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FileTool {
    pub name: &'static str,
    pub op: FileOp,
    /// The path argument: the first of these present.
    pub path_keys: &'static [&'static str],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileOp {
    Read,
    Write,
    /// The op is chosen by the `key` argument: `reads` read, `writes` write,
    /// anything else is not a valid call.
    ByCommand {
        key: &'static str,
        reads: &'static [&'static str],
        writes: &'static [&'static str],
    },
}

/// A tool that fetches a URL and returns its content.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FetchTool {
    pub name: &'static str,
    /// The URL argument, read as `Structured`.
    pub url_key: Option<&'static str>,
    /// A free-text argument URLs are scanned out of, as `Scanned`, when
    /// there is no URL argument.
    pub scan_key: Option<&'static str>,
}

/// A tool that runs a shell command.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShellTool {
    pub name: &'static str,
    /// The command: a script string, or an argv array.
    pub command_key: &'static str,
    /// The directory the command runs in, when the tool takes one.
    pub workdir_key: Option<&'static str>,
    /// The harness keeps one shell across calls, so a `cd` moves where the
    /// next call starts (Claude Code's `Bash`).
    pub persists_cwd: bool,
}

const EDITOR: FileOp = FileOp::ByCommand {
    key: "command",
    reads: &["view"],
    writes: &["create", "str_replace", "insert", "undo_edit"],
};

pub static FILE_TOOLS: &[FileTool] = &[
    // Claude Code.
    file("Read", FileOp::Read, &["file_path"]),
    file("Write", FileOp::Write, &["file_path"]),
    file("Edit", FileOp::Write, &["file_path"]),
    file("MultiEdit", FileOp::Write, &["file_path"]),
    file("NotebookEdit", FileOp::Write, &["notebook_path"]),
    file("NotebookRead", FileOp::Read, &["notebook_path"]),
    // OpenCode (`filePath`) and pi (`path`).
    file("read", FileOp::Read, &["filePath", "path", "file_path"]),
    file("write", FileOp::Write, &["filePath", "path", "file_path"]),
    file("edit", FileOp::Write, &["filePath", "path", "file_path"]),
    // Gemini CLI.
    file(
        "read_file",
        FileOp::Read,
        &["absolute_path", "file_path", "path"],
    ),
    file("write_file", FileOp::Write, &["file_path", "path"]),
    file("replace", FileOp::Write, &["file_path", "path"]),
    // The Anthropic text editor tool.
    file("str_replace_based_edit_tool", EDITOR, &["path"]),
    file("str_replace_editor", EDITOR, &["path"]),
];

pub static FETCH_TOOLS: &[FetchTool] = &[
    // Claude Code.
    fetch("WebFetch", Some("url"), None),
    // The Anthropic server tool (`url`) and Gemini CLI (`prompt`).
    fetch("web_fetch", Some("url"), Some("prompt")),
    // OpenCode.
    fetch("webfetch", Some("url"), None),
];

/// The schema of every configured fetch tool: the URL in `url`, read as
/// `Structured`; no free-text argument.
pub static CONFIGURED_FETCH: FetchTool = fetch("<configured>", Some("url"), None);

pub static SHELL_TOOLS: &[ShellTool] = &[
    // Claude Code, OpenCode, pi.
    shell("Bash", "command", None, true),
    shell("bash", "command", None, false),
    // Gemini CLI.
    shell("run_shell_command", "command", Some("directory"), false),
    // OpenHands: one persistent bash session per conversation.
    shell("execute_bash", "command", None, true),
    // Codex.
    shell("shell", "command", Some("workdir"), false),
    shell("exec_command", "cmd", Some("workdir"), false),
];

const fn file(name: &'static str, op: FileOp, path_keys: &'static [&'static str]) -> FileTool {
    FileTool {
        name,
        op,
        path_keys,
    }
}

const fn fetch(
    name: &'static str,
    url_key: Option<&'static str>,
    scan_key: Option<&'static str>,
) -> FetchTool {
    FetchTool {
        name,
        url_key,
        scan_key,
    }
}

const fn shell(
    name: &'static str,
    command_key: &'static str,
    workdir_key: Option<&'static str>,
    persists_cwd: bool,
) -> ShellTool {
    ShellTool {
        name,
        command_key,
        workdir_key,
        persists_cwd,
    }
}

/// The server and tool of a Claude Code MCP tool name,
/// `mcp__<server>__<tool>`. The server is everything up to the first `__`
/// after the prefix.
pub fn mcp_name(name: &str) -> Option<(&str, &str)> {
    let (server, tool) = name.strip_prefix("mcp__")?.split_once("__")?;
    (!server.is_empty() && !tool.is_empty()).then_some((server, tool))
}

/// The known tool `name` names, if any.
pub fn identify<'c>(name: &ToolName, config: &'c ExtractConfig) -> Option<KnownTool<'c>> {
    let name = name.0.as_str();
    if let Some((server, tool)) = mcp_name(name) {
        let (server, rule) = config.rule(server, tool)?;
        return Some(KnownTool::Mcp(McpTool { server, rule }));
    }
    let fetch = FETCH_TOOLS.iter().find(|tool| tool.name == name);
    if config.http_tools().iter().any(|tool| tool == name) {
        return Some(KnownTool::Http(HttpTool { fallback: fetch }));
    }
    if config.fetch_tools().iter().any(|tool| tool == name) {
        return Some(KnownTool::Fetch(&CONFIGURED_FETCH));
    }
    FILE_TOOLS
        .iter()
        .find(|tool| tool.name == name)
        .map(KnownTool::File)
        .or_else(|| fetch.map(KnownTool::Fetch))
        .or_else(|| {
            SHELL_TOOLS
                .iter()
                .find(|tool| tool.name == name)
                .map(KnownTool::Shell)
        })
}

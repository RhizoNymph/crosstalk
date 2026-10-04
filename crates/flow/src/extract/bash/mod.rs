//! Shell tools: the command is lexed ([`lex`]) and its simple commands
//! interpreted ([`commands`]). Every access is `Parsed`.
//!
//! The command is a script string (Claude Code's `Bash`), or an argv array
//! (Codex's `shell`), where `[sh|bash|zsh, -c|-lc, script]` runs the script
//! and any other array is one command. A tool's working-directory argument
//! (Codex `workdir`, Gemini CLI `directory`) is where the command starts;
//! otherwise it starts in the conversation's working directory, which
//! [`ConversationContext::observe`] moves after each call of a tool whose
//! shell persists (Claude Code's `Bash`), and with the clones the
//! conversation knows.

pub mod commands;
mod git;
pub mod lex;
mod net;
mod options;

use serde_json::Value;

use crosstalk_spec::interfaces::l5_flow::ExtractError;
use crosstalk_spec::observed::message::ToolName;

use crate::extract::args::{ArgError, Args};
use crate::extract::catalog::ShellTool;
use crate::extract::context::ConversationContext;
use crate::extract::op::Candidate;
use crate::extract::resource::AbsolutePath;
use crate::extract::sites::SitesConfig;

use commands::{Shell, ShellRun, ShellState};
use lex::Script;

pub(crate) fn candidates(
    tool: &ShellTool,
    name: &ToolName,
    args: &Args,
    context: &ConversationContext,
    sites: &SitesConfig,
) -> Result<Vec<Candidate>, ExtractError> {
    Ok(run(tool, name, args, context, sites)?.candidates)
}

/// Run the call's command from where it starts: the tool's directory
/// argument or the conversation's working directory, with the clones the
/// conversation knows.
pub(crate) fn run(
    tool: &ShellTool,
    name: &ToolName,
    args: &Args,
    context: &ConversationContext,
    sites: &SitesConfig,
) -> Result<ShellRun, ExtractError> {
    let script = script(tool, args)?;
    let start = ShellState {
        cwd: start_directory(tool, args, context)?,
        repos: context.repos().clone(),
    };
    Ok(Shell::new(name, context.file_host(), sites, start).run(&script))
}

fn script(tool: &ShellTool, args: &Args) -> Result<Script, ExtractError> {
    let key = tool.command_key;
    match args.get(key) {
        Some(Value::String(text)) => Ok(Script::lex(text)?),
        Some(Value::Array(items)) => {
            let argv = items
                .iter()
                .map(|item| item.as_str().map(str::to_owned))
                .collect::<Option<Vec<String>>>()
                .ok_or_else(|| ArgError::invalid(key, "an argv item is not a string"))?;
            match argv.as_slice() {
                [shell, flag, script, ..]
                    if matches!(base(shell), "sh" | "bash" | "zsh")
                        && matches!(flag.as_str(), "-c" | "-lc" | "-ic") =>
                {
                    Ok(Script::lex(script)?)
                }
                _ => Ok(Script::from_argv(argv)),
            }
        }
        Some(_) => Err(ArgError::NotAString(key.to_owned()).into()),
        None => Err(ArgError::Missing(key.to_owned()).into()),
    }
}

fn base(program: &str) -> &str {
    program.rsplit('/').next().unwrap_or(program)
}

fn start_directory(
    tool: &ShellTool,
    args: &Args,
    context: &ConversationContext,
) -> Result<Option<AbsolutePath>, ExtractError> {
    let Some(key) = tool.workdir_key else {
        return Ok(context.cwd().cloned());
    };
    let Some(dir) = args.opt_str(key)? else {
        return Ok(context.cwd().cloned());
    };
    if dir.starts_with('/') {
        return AbsolutePath::parse(dir)
            .map(Some)
            .map_err(|error| ArgError::invalid(key, error).into());
    }
    match context.cwd() {
        Some(cwd) => cwd
            .join(dir)
            .map(Some)
            .map_err(|error| ArgError::invalid(key, error).into()),
        None => Ok(None),
    }
}

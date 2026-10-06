//! Shell tools: the command is lexed ([`lex`]) and its simple commands
//! interpreted ([`commands`]). Every access is `Parsed`.
//!
//! The command is a script string (Claude Code's `Bash`), or an argv array
//! (Codex's `shell`), where `[sh|bash|zsh, -c|-lc, script]` runs the script
//! and any other array is one command. A tool's working-directory argument
//! (Codex `workdir`, Gemini CLI `directory`) is where the command starts;
//! otherwise it starts where the conversation's shell is ([`state`]),
//! which [`ConversationContext::observe`] moves after each call of a tool
//! whose shell persists, with the clones the conversation knows.
//!
//! Accesses are named from the call alone; the output then refutes those
//! of commands it shows skipped or failed ([`evidence`]) and corrects a
//! push's or pull's repository by the remote it prints (`transfers`).
//! `after` is the shell the call leaves for the next one.
//!
//! [`ConversationContext::observe`]: crate::extract::context::ConversationContext::observe

pub mod commands;
pub mod evidence;
mod forge;
mod git;
pub mod lex;
mod net;
mod options;
pub mod state;
mod transfers;

use serde_json::Value;

use crosstalk_spec::interfaces::l5_flow::ExtractError;
use crosstalk_spec::observed::message::ToolName;

use crate::extract::args::{ArgError, Args};
use crate::extract::catalog::ShellTool;
use crate::extract::context::ConversationContext;
use crate::extract::op::Candidate;
use crate::extract::resource::{AbsolutePath, Place};
use crate::extract::sites::SitesConfig;

use commands::{Print, RemoteQuery, Shell};
use evidence::Evidence;
use lex::Script;
use state::ShellState;
use transfers::Printed;

/// The accesses a call names, from the call alone; with its output, those
/// the output refutes are marked and a pull's repository is the one it
/// printed.
pub(crate) fn candidates(
    tool: &ShellTool,
    name: &ToolName,
    args: &Args,
    context: &ConversationContext,
    sites: &SitesConfig,
    output: Option<&str>,
) -> Result<Vec<Candidate>, ExtractError> {
    let script = script(tool, args)?;
    let start = start(tool, args, context)?;
    let assumed = Evidence::assumed(&script);
    let run = Shell::new(name, context.file_host(), sites, &assumed, start).run(&script);
    let mut found = run.found;
    if let Some(output) = output {
        let evidence = Evidence::read(&script, output);
        for access in &mut found {
            if evidence.refutes(access.step, access.operand.as_deref()) {
                access.candidate.refuted = true;
            }
        }
        transfers::correct(
            &run.transfers,
            &mut found,
            &Printed::scan(output),
            &evidence,
        );
    }
    Ok(found.into_iter().map(|access| access.candidate).collect())
}

/// The shell a call leaves, given its output: the commands the output
/// shows skipped change nothing and a failed `cd` stays put; what the
/// output prints teaches the home directory (a `pwd` or `echo ~`, a failed
/// `cd ~/x`'s message), the directory (a `pwd` at the end, when it was not
/// known), the remotes `git remote -v`, `get-url` and `config` printed, and
/// the remote a push or pull printed.
pub(crate) fn after(
    tool: &ShellTool,
    name: &ToolName,
    args: &Args,
    context: &ConversationContext,
    sites: &SitesConfig,
    output: &str,
) -> Result<ShellState, ExtractError> {
    let script = script(tool, args)?;
    let start = start(tool, args, context)?;
    let evidence = Evidence::read(&script, output);
    let run = Shell::new(name, context.file_host(), sites, &evidence, start).run(&script);
    let mut end = run.end;
    if let Some(home) = evidence.home() {
        end.learn_home(home.clone());
    }
    learn_from_prints(&mut end, &run.prints, output);
    if let [query] = run.remote_queries.as_slice() {
        learn_remotes(&mut end, query, output);
    }
    transfers::learn(&mut end, &run.transfers, &Printed::scan(output), &evidence);
    Ok(end)
}

/// Lines of `output` that are one absolute path each.
fn path_lines(output: &str) -> Vec<AbsolutePath> {
    let mut paths: Vec<AbsolutePath> = Vec::new();
    for line in output.lines().map(str::trim) {
        if line.starts_with('/')
            && !line.contains(char::is_whitespace)
            && let Ok(path) = AbsolutePath::parse(line)
            && !paths.contains(&path)
        {
            paths.push(path);
        }
    }
    paths
}

/// A lone `pwd` or `echo ~` and the one path line that answers it.
fn learn_from_prints(state: &mut ShellState, prints: &[Print], output: &str) {
    let [print] = prints else {
        return;
    };
    let paths = path_lines(output);
    match print {
        Print::Home => {
            if let [home] = paths.as_slice() {
                state.learn_home(home.clone());
            }
        }
        Print::Pwd { at, moved_after } => match at {
            Some(Place::Home(inside)) => {
                let homes: Vec<AbsolutePath> = paths
                    .iter()
                    .filter_map(|path| {
                        if inside.as_str() == "/" {
                            Some(path.clone())
                        } else {
                            path.strip_suffix(inside)
                        }
                    })
                    .collect();
                if let [home] = homes.as_slice() {
                    state.learn_home(home.clone());
                }
            }
            None if !moved_after && state.cwd().is_none() => {
                if let [cwd] = paths.as_slice() {
                    state.set_cwd(Some(Place::Absolute(cwd.clone())));
                }
            }
            Some(Place::Absolute(_)) | None => {}
        },
    }
}

/// Bind the remotes a lone `git remote -v`, `get-url` or `config` printed
/// to the clone it ran in.
fn learn_remotes(state: &mut ShellState, query: &RemoteQuery, output: &str) {
    let Some(dir) = query.dir.clone().map(|dir| state.normalized(dir)) else {
        return;
    };
    let cwd = state.absolute_cwd().cloned();
    let printed = |word: &str| crate::extract::resource::RepoId::parse(word, cwd.as_ref());
    match &query.name {
        Some(name) => {
            let repo = output.lines().find_map(|line| {
                line.split_whitespace()
                    .filter(|word| {
                        word.contains("://") || word.contains(':') || word.starts_with('/')
                    })
                    .find_map(printed)
            });
            if let Some(repo) = repo {
                state.bind(dir, name, repo);
            }
        }
        None => {
            // `origin\thttps://… (fetch)`, one line per remote and use.
            let mut bound: Vec<String> = Vec::new();
            for line in output.lines() {
                let mut words = line.split_whitespace();
                let (Some(name), Some(url)) = (words.next(), words.next()) else {
                    continue;
                };
                if bound.iter().any(|seen| seen == name) {
                    continue;
                }
                if let Some(repo) = printed(url) {
                    bound.push(name.to_owned());
                    state.bind(dir.clone(), name, repo);
                }
            }
        }
    }
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

/// Where the call's shell starts: the conversation's shell, moved to the
/// tool's directory argument when it has one.
fn start(
    tool: &ShellTool,
    args: &Args,
    context: &ConversationContext,
) -> Result<ShellState, ExtractError> {
    let mut state = context.shell().clone();
    let Some(key) = tool.workdir_key else {
        return Ok(state);
    };
    let Some(dir) = args.opt_str(key)? else {
        return Ok(state);
    };
    let place = if dir.starts_with('/') {
        Some(Place::Absolute(
            AbsolutePath::parse(dir).map_err(|error| ArgError::invalid(key, error))?,
        ))
    } else {
        AbsolutePath::root()
            .join(dir)
            .map_err(|error| ArgError::invalid(key, error))?;
        state.cwd().and_then(|cwd| cwd.join(dir))
    };
    state.set_cwd(place);
    Ok(state)
}

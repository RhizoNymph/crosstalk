//! What a lexed command line reads and writes, and what it teaches about
//! where later commands run.
//!
//! - Output redirections (`>`, `>>`, `>|`, `&>`, `1>`) write their file;
//!   stderr-only ones (`2>`) and `/dev/*` targets are not resources.
//! - `cat`, `head`, `tail`, `tac` and `bat` read their file operands (or
//!   their `<` input) when their output reaches the result, that is, is not
//!   redirected to a file. `tee` writes its operands.
//! - `curl` and `wget` are HTTP requests (`net`).
//! - `git` and `gh` clone repositories, name remotes and show files of a
//!   repository (`git`).
//! - `cd` moves the working directory later commands resolve against; one
//!   it cannot follow makes it unknown, so relative paths after it are
//!   keyed as written.
//!
//! Every access is `Parsed`. A word the shell would expand names nothing.

use crosstalk_spec::derived::flow::access::Extraction;
use crosstalk_spec::derived::flow::resource::{Host, Locator};
use crosstalk_spec::observed::message::ToolName;

use crate::extract::op::Candidate;
use crate::extract::resource::{AbsolutePath, FileScope, RepoBindings, file_locator};
use crate::extract::sites::SitesConfig;

use super::lex::{Command, RedirectOp, Script, Word};
use super::options::{NO_VALUES, OptSpec, Options};

/// Where a shell is: its working directory and the clones it knows.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ShellState {
    pub cwd: Option<AbsolutePath>,
    pub repos: RepoBindings,
}

/// What running a script's commands found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ShellRun {
    pub candidates: Vec<Candidate>,
    /// Where the shell is after the last command.
    pub end: ShellState,
    /// The directory of each command that prints a remote's URL
    /// (`git remote -v`), `None` where it is unknown.
    pub remote_queries: Vec<Option<AbsolutePath>>,
}

/// The interpreter: the tool keying a relative path when the working
/// directory is unknown, the host files live on, the site rules, and the
/// shell's state as it runs.
pub(crate) struct Shell<'a> {
    pub(super) tool: &'a ToolName,
    pub(super) host: Option<&'a Host>,
    pub(super) sites: &'a SitesConfig,
    pub(super) state: ShellState,
    pub(super) remote_queries: Vec<Option<AbsolutePath>>,
}

impl<'a> Shell<'a> {
    pub fn new(
        tool: &'a ToolName,
        host: Option<&'a Host>,
        sites: &'a SitesConfig,
        start: ShellState,
    ) -> Self {
        Self {
            tool,
            host,
            sites,
            state: start,
            remote_queries: Vec::new(),
        }
    }

    pub fn run(mut self, script: &Script) -> ShellRun {
        let mut found = Vec::new();
        for command in &script.commands {
            self.command(command, &mut found);
        }
        ShellRun {
            candidates: found,
            end: self.state,
            remote_queries: self.remote_queries,
        }
    }

    fn command(&mut self, command: &Command, found: &mut Vec<Candidate>) {
        let mut stdout_to_file = false;
        for redirect in &command.redirects {
            let to_stdout = matches!(redirect.fd, None | Some(1));
            let file = match redirect.op {
                RedirectOp::Out | RedirectOp::Append if to_stdout => Some(&redirect.target),
                RedirectOp::Both => Some(&redirect.target),
                RedirectOp::DupOut if to_stdout && !is_descriptor(&redirect.target.text) => {
                    Some(&redirect.target)
                }
                _ => None,
            };
            if let Some(target) = file {
                stdout_to_file = true;
                if let Some(locator) = self.file(target) {
                    found.push(Candidate::write(locator, Extraction::Parsed));
                }
            }
        }
        let Some((name, args)) = program(&command.words) else {
            return;
        };
        match name {
            "cd" => self.state.cwd = change_directory(self.state.cwd.as_ref(), args),
            "cat" | "head" | "tail" | "tac" | "bat" if !stdout_to_file => {
                let spec = match name {
                    "head" | "tail" => HEAD_TAIL,
                    "bat" => BAT,
                    _ => NO_VALUES,
                };
                let parsed = Options::parse(args, &spec);
                let mut operands: Vec<&Word> = parsed
                    .operands
                    .into_iter()
                    .filter(|word| word.text != "-")
                    .collect();
                if operands.is_empty() {
                    operands = command
                        .redirects
                        .iter()
                        .filter(|r| r.op == RedirectOp::In && matches!(r.fd, None | Some(0)))
                        .map(|r| &r.target)
                        .collect();
                }
                for word in operands {
                    if let Some(locator) = self.file(word) {
                        found.push(Candidate::read(locator, Extraction::Parsed));
                    }
                }
            }
            "tee" => {
                for word in Options::parse(args, &NO_VALUES).operands {
                    if let Some(locator) = self.file(word) {
                        found.push(Candidate::write(locator, Extraction::Parsed));
                    }
                }
            }
            "curl" => self.curl(args, stdout_to_file, found),
            "wget" => self.wget(args, stdout_to_file, found),
            "git" => self.git(args, stdout_to_file, found),
            "gh" => self.gh(args),
            _ => {}
        }
    }

    pub(super) fn scope(&self) -> FileScope<'_> {
        FileScope {
            cwd: self.state.cwd.as_ref(),
            host: self.host,
            repos: &self.state.repos,
        }
    }

    /// The file a literal word names, unless it is a device.
    pub(super) fn file(&self, word: &Word) -> Option<Locator> {
        let path = word.as_literal()?;
        if path.starts_with("/dev/") || path == "/dev" {
            return None;
        }
        file_locator(path, self.tool, self.scope()).ok()
    }

    /// The directory a literal word names, from `from`.
    pub(super) fn directory(
        &self,
        from: Option<&AbsolutePath>,
        word: &Word,
    ) -> Option<AbsolutePath> {
        let path = word.as_literal()?;
        if path.starts_with('/') {
            return AbsolutePath::parse(path).ok();
        }
        if path.starts_with('~') {
            return None;
        }
        from?.join(path).ok()
    }
}

/// The program a command runs and its arguments, past variable
/// assignments and wrappers (`sudo`, `env`, `command`, `exec`, `nohup`,
/// `time`, `timeout`). `None` when it is not a literal word.
fn program(words: &[Word]) -> Option<(&str, &[Word])> {
    let mut rest = words;
    loop {
        let (first, tail) = rest.split_first()?;
        if is_assignment(&first.text) || first.text == "{" || first.text == "}" {
            rest = tail;
            continue;
        }
        let name = first.as_literal()?;
        let name = name.rsplit('/').next().unwrap_or(name);
        let skip_values: &[&str] = match name {
            "sudo" => &["-u", "-g", "-C", "-h", "-p", "-U", "-r", "-t"],
            "env" => &["-u", "-C", "-S"],
            "timeout" => &["-s", "-k"],
            "command" | "builtin" | "exec" | "nohup" | "time" => &[],
            _ => return Some((name, tail)),
        };
        rest = skip_wrapper(tail, skip_values);
        if name == "timeout" {
            // The duration.
            rest = rest.split_first().map_or(rest, |(_, tail)| tail);
        }
    }
}

fn skip_wrapper<'w>(mut words: &'w [Word], value_options: &[&str]) -> &'w [Word] {
    while let Some((first, tail)) = words.split_first() {
        if value_options.contains(&first.text.as_str()) {
            words = tail.split_first().map_or(tail, |(_, rest)| rest);
        } else if first.text.starts_with('-') || is_assignment(&first.text) {
            words = tail;
        } else {
            break;
        }
    }
    words
}

fn is_assignment(text: &str) -> bool {
    match text.split_once('=') {
        Some((name, _)) => {
            let mut chars = name.chars();
            chars
                .next()
                .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
                && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
        }
        None => false,
    }
}

fn is_descriptor(text: &str) -> bool {
    text == "-" || (!text.is_empty() && text.bytes().all(|b| b.is_ascii_digit()))
}

/// `cd`'s new directory: `None` (unknown) for a target it cannot follow
/// (none, `-`, `~`, an expansion, a relative one from an unknown
/// directory).
fn change_directory(cwd: Option<&AbsolutePath>, args: &[Word]) -> Option<AbsolutePath> {
    let target = args
        .iter()
        .find(|word| !matches!(word.text.as_str(), "-L" | "-P" | "-e" | "-@"))?;
    let target = target.as_literal()?;
    if target == "-" || target.starts_with('~') {
        return None;
    }
    if target.starts_with('/') {
        return AbsolutePath::parse(target).ok();
    }
    cwd?.join(target).ok()
}

const HEAD_TAIL: OptSpec = OptSpec {
    short_values: "nc",
    long_values: &["--lines", "--bytes"],
};

const BAT: OptSpec = OptSpec {
    short_values: "lrHm",
    long_values: &[
        "--language",
        "--line-range",
        "--highlight-line",
        "--map-syntax",
        "--style",
        "--theme",
    ],
};

//! What a lexed command line reads and writes, and what it teaches about
//! where later commands run.
//!
//! - Output redirections (`>`, `>>`, `>|`, `&>`, `1>`) write their file;
//!   stderr-only ones (`2>`) and `/dev/*` targets are not resources.
//! - `cat`, `head`, `tail`, `tac` and `bat` read their file operands (or
//!   their `<` input) when their output reaches the result, that is, is not
//!   redirected to a file. `tee` writes its operands.
//! - `sed -n` printing line numbers (`sed -n '3,10p' f`, `sed -n 5p f`,
//!   `'$p'`) reads its file operands, like `cat`, when its output reaches
//!   the result. Any other `sed` script is no access.
//! - `curl` and `wget` are HTTP requests (`net`).
//! - `git` clones repositories, names remotes, shows files of a
//!   repository, pushes to and pulls from it (`git`); `gh` and `glab`
//!   clone, read and write issues and pull/merge requests and make API
//!   requests (`forge`).
//! - `cd` moves the working directory later commands resolve against
//!   (`cd` and `cd ~` to the home directory, `cd -` back to the previous
//!   one, a path under the home directory while the home's own path is not
//!   known yet as a home-relative place); one it cannot follow makes it
//!   unknown, so relative paths after it are keyed as written. A `cd`
//!   inside parentheses moves only its subshell.
//! - `pwd`, and `echo` of the home directory, print where the shell is:
//!   the run records them (`ShellRun::prints`) for the context to read
//!   the answer from the output.
//!
//! The interpreter runs under [`Evidence`]: a command the output shows
//! skipped changes nothing, and a `cd` the output shows failing leaves the
//! directory where it was. Without an output every command is taken as
//! run ([`Evidence::assumed`]), which is how accesses are named: a
//! write's locator never depends on the result
//! (`flow.extract.write-locators-from-call`).
//!
//! Every access is `Parsed` and remembers the command that made it
//! (`Found`), so the output can refute it. A word the shell would expand
//! names nothing, except a path under the home directory.

use crosstalk_spec::derived::flow::access::Extraction;
use crosstalk_spec::derived::flow::resource::{Host, Locator};
use crosstalk_spec::observed::message::ToolName;

use crate::extract::op::Candidate;
use crate::extract::resource::{FileScope, Place, RepoId, file_locator};
use crate::extract::sites::SitesConfig;

use super::evidence::{Evidence, Ran, Status};
use super::forge::Cli;
use super::lex::{Command, RedirectOp, Script, Word};
use super::options::{NO_VALUES, OptSpec, Options};
use super::state::{CdTarget, ShellState};

/// One access a command of the script made.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Found {
    /// The command's index in the script.
    pub step: usize,
    /// The file operand it opens, as written, for a file reader's read:
    /// the output can show that one operand failed.
    pub operand: Option<String>,
    pub candidate: Candidate,
}

/// A remote a `git remote` or `git config` command prints.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RemoteQuery {
    /// The directory it ran in, when known.
    pub dir: Option<Place>,
    /// The remote it prints (`get-url <name>`, `remote.<name>.url`);
    /// `None` for `git remote -v`, which prints every remote by name.
    pub name: Option<String>,
}

/// What a `git push`, `pull` or `fetch` moves.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TransferKind {
    Push,
    /// `pull` or `fetch`.
    Fetch,
}

/// How a transfer named its remote.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RemoteRef {
    /// A URL or path operand: the call names the repository itself.
    Explicit,
    /// A remote of the clone: by name, or the default (`None`).
    Clone(Option<String>),
}

/// One `git push`, `pull` or `fetch` of the script.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Transfer {
    pub step: usize,
    pub kind: TransferKind,
    pub remote: RemoteRef,
    /// The directory it ran in, when known.
    pub dir: Option<Place>,
    /// The repository the call resolved it to, if any.
    pub repo: Option<RepoId>,
}

/// A command that prints where the shell is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Print {
    /// `pwd`, run where the shell was then (`None`: unknown), and whether a
    /// `cd` came after it.
    Pwd {
        at: Option<Place>,
        moved_after: bool,
    },
    /// `echo ~`, `echo $HOME`.
    Home,
}

/// What running a script's commands found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ShellRun {
    pub found: Vec<Found>,
    /// Where the shell is after the last command.
    pub end: ShellState,
    pub remote_queries: Vec<RemoteQuery>,
    pub transfers: Vec<Transfer>,
    pub prints: Vec<Print>,
}

/// The interpreter: the tool keying a relative path when the working
/// directory is unknown, the host files live on, the site rules, what the
/// output shows of each command, and the shell's state as it runs.
pub(crate) struct Shell<'a> {
    pub(super) tool: &'a ToolName,
    pub(super) host: Option<&'a Host>,
    pub(super) sites: &'a SitesConfig,
    evidence: &'a Evidence,
    pub(super) state: ShellState,
    pub(super) remote_queries: Vec<RemoteQuery>,
    pub(super) transfers: Vec<Transfer>,
    prints: Vec<Print>,
    /// The command running now.
    pub(super) step: usize,
    /// The directory to restore at each open subshell's closing
    /// parenthesis, innermost last.
    subshells: Vec<Option<Place>>,
}

impl<'a> Shell<'a> {
    pub fn new(
        tool: &'a ToolName,
        host: Option<&'a Host>,
        sites: &'a SitesConfig,
        evidence: &'a Evidence,
        start: ShellState,
    ) -> Self {
        Self {
            tool,
            host,
            sites,
            evidence,
            state: start,
            remote_queries: Vec::new(),
            transfers: Vec::new(),
            prints: Vec::new(),
            step: 0,
            subshells: Vec::new(),
        }
    }

    pub fn run(mut self, script: &Script) -> ShellRun {
        let mut found = Vec::new();
        for (step, command) in script.commands.iter().enumerate() {
            self.step = step;
            self.enter(command.depth);
            let ran = self.evidence.step(step).map_or(Ran::Yes, |step| step.ran);
            if ran == Ran::No {
                continue;
            }
            let mut accesses = Vec::new();
            self.command(command, &mut accesses);
            found.extend(accesses);
        }
        self.enter(0);
        ShellRun {
            found,
            end: self.state,
            remote_queries: self.remote_queries,
            transfers: self.transfers,
            prints: self.prints,
        }
    }

    /// Open or close subshells up to `depth`: a closing one restores the
    /// directory it was entered from.
    fn enter(&mut self, depth: u16) {
        let depth = usize::from(depth);
        while self.subshells.len() > depth {
            if let Some(cwd) = self.subshells.pop() {
                self.state.set_cwd(cwd);
            }
        }
        while self.subshells.len() < depth {
            self.subshells.push(self.state.cwd().cloned());
        }
    }

    fn command(&mut self, command: &Command, found: &mut Vec<Found>) {
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
                    self.found(found, Candidate::write(locator, Extraction::Parsed));
                }
            }
        }
        let Some((name, args)) = program(&command.words) else {
            return;
        };
        match name {
            "cd" => {
                let failed = self
                    .evidence
                    .step(self.step)
                    .is_some_and(|step| step.status == Status::Failed);
                if !failed {
                    let target = self.cd_target(args);
                    self.state.cd(target);
                    for print in &mut self.prints {
                        if let Print::Pwd { moved_after, .. } = print {
                            *moved_after = true;
                        }
                    }
                }
            }
            "pwd" if !stdout_to_file => self.prints.push(Print::Pwd {
                at: self.state.cwd().cloned(),
                moved_after: false,
            }),
            "echo" if !stdout_to_file => {
                if let [word] = args
                    && word.home() == Some("")
                {
                    self.prints.push(Print::Home);
                }
            }
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
                        self.found_operand(
                            found,
                            word,
                            Candidate::read(locator, Extraction::Parsed),
                        );
                    }
                }
            }
            "tee" => {
                for word in Options::parse(args, &NO_VALUES).operands {
                    if let Some(locator) = self.file(word) {
                        self.found(found, Candidate::write(locator, Extraction::Parsed));
                    }
                }
            }
            "curl" => self.curl(args, stdout_to_file, found),
            "wget" => self.wget(args, stdout_to_file, found),
            "git" => self.git(args, stdout_to_file, found),
            "gh" => self.forge_cli(Cli::Gh, args, stdout_to_file, found),
            "glab" => self.forge_cli(Cli::Glab, args, stdout_to_file, found),
            "sed" if !stdout_to_file => self.sed(args, found),
            _ => {}
        }
    }

    /// Record an access of the command running now.
    pub(super) fn found(&self, found: &mut Vec<Found>, candidate: Candidate) {
        found.push(Found {
            step: self.step,
            operand: None,
            candidate,
        });
    }

    /// Record a file reader's read of `operand`.
    fn found_operand(&self, found: &mut Vec<Found>, operand: &Word, candidate: Candidate) {
        found.push(Found {
            step: self.step,
            operand: Some(operand.text.clone()),
            candidate,
        });
    }

    pub(super) fn scope(&self) -> FileScope<'_> {
        FileScope {
            cwd: self.state.absolute_cwd(),
            host: self.host,
            repos: self.state.repos(),
        }
    }

    /// The file a word names, unless it is a device: a literal path, or
    /// one under the home directory. Under a home whose own path is not
    /// known, a path names a file only inside a known clone; outside one, a
    /// `~/x` word names nothing (as before the home was tracked) and a
    /// relative path is keyed as written, as from an unknown directory
    /// (`flow.resource.relative-path-opaque`).
    pub(super) fn file(&self, word: &Word) -> Option<Locator> {
        if word.home().is_some() {
            return self.place_locator(self.state.place(None, word)?);
        }
        let path = word.as_literal()?;
        if path.starts_with("/dev/") || path == "/dev" {
            return None;
        }
        if let Some(Place::Home(dir)) = self.state.cwd()
            && !path.starts_with('/')
        {
            let place = Place::Home(dir.clone()).join(path);
            return place
                .and_then(|place| self.place_locator(place))
                .or_else(|| {
                    Some(Locator::Opaque {
                        tool: self.tool.clone(),
                        key: path.to_owned(),
                    })
                });
        }
        file_locator(path, self.tool, self.scope()).ok()
    }

    /// The locator of a place: a file of a known clone or of the machine;
    /// `None` for a place under a home whose path is unknown, outside every
    /// known clone.
    pub(super) fn place_locator(&self, place: Place) -> Option<Locator> {
        match place {
            Place::Absolute(path) => Some(crate::extract::resource::absolute_locator(
                path,
                self.scope(),
            )),
            Place::Home(_) => {
                let (repo, inside) = self.state.repos().locate_place(&place)?;
                Some(repo.file(&inside))
            }
        }
    }

    /// The directory a word names, from `from`.
    pub(super) fn directory(&self, from: Option<&Place>, word: &Word) -> Option<Place> {
        self.state.place(from, word)
    }

    /// What a `cd` with `args` asks for.
    fn cd_target(&self, args: &[Word]) -> CdTarget {
        let Some(target) = args
            .iter()
            .find(|word| !matches!(word.text.as_str(), "-L" | "-P" | "-e" | "-@"))
        else {
            return CdTarget::Home;
        };
        if target.home() == Some("") {
            return CdTarget::Home;
        }
        if target.as_literal() == Some("-") {
            return CdTarget::Previous;
        }
        match self.state.place(self.state.cwd(), target) {
            Some(place) => CdTarget::Place(place),
            None => CdTarget::Unknown,
        }
    }
}

/// The program a command runs and its arguments, past variable
/// assignments and wrappers (`sudo`, `env`, `command`, `exec`, `nohup`,
/// `time`, `timeout`). `None` when it is not a literal word.
pub(super) fn program(words: &[Word]) -> Option<(&str, &[Word])> {
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

impl Shell<'_> {
    /// `sed -n '<lines>p' <file>…`: a read of each file.
    fn sed(&self, args: &[Word], found: &mut Vec<Found>) {
        let parsed = Options::parse(args, &SED);
        if !parsed.has(&["-n", "--quiet", "--silent"])
            || parsed.has(&["-i", "--in-place"])
            || parsed
                .options
                .iter()
                .any(|(name, _)| name.starts_with("--in-place"))
        {
            return;
        }
        let mut operands = parsed.operands.iter();
        let script = match parsed.last(&["-e", "--expression"]) {
            Some(script) => script,
            None => match operands.next() {
                Some(script) => *script,
                None => return,
            },
        };
        if !script.as_literal().is_some_and(prints_lines) {
            return;
        }
        for word in operands.filter(|word| word.text != "-") {
            if let Some(locator) = self.file(word) {
                self.found_operand(found, word, Candidate::read(locator, Extraction::Parsed));
            }
        }
    }
}

/// A sed script that only prints a line or a range of lines: `5p`,
/// `3,10p`, `10,$p`, `$p`.
fn prints_lines(script: &str) -> bool {
    let Some(address) = script.trim().strip_suffix('p') else {
        return false;
    };
    let line = |text: &str| {
        let text = text.trim();
        text == "$" || (!text.is_empty() && text.bytes().all(|b| b.is_ascii_digit()))
    };
    match address.split_once(',') {
        Some((from, to)) => line(from) && line(to),
        None => line(address),
    }
}

const SED: OptSpec = OptSpec {
    short_values: "elf",
    long_values: &["--expression", "--file", "--line-length"],
};

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

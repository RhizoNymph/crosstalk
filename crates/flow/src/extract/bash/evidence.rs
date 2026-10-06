//! What a call's output shows about which of its commands ran.
//!
//! A shell result rarely carries exit statuses, but bash prints its own
//! failures, and so do the file readers: `cd: <dir>: No such file or
//! directory`, `<name>: command not found`, `cat: <file>: No such file or
//! directory`, `sed: can't read <file>: …`, `head: cannot open '<file>'
//! for reading: …`. From those lines and the script's `&&`, `||`, `;` and
//! `|` joins, each command is judged ([`Step`]):
//!
//! - a command whose failure the output shows **failed** (a reader only
//!   for the operands it names; a `cd` leaves the directory where it was);
//! - a `cd` with no failure shown, whose stderr reaches the output and
//!   which surely ran, **succeeded**; `true` and `:` succeed, `false` fails;
//!   any other command's status is **unknown**: the output shows failures,
//!   not successes;
//! - the list's status is that of the last command that ran (of a
//!   pipeline, its last command's); a command after `&&` runs when it
//!   succeeded, is skipped when it failed and may have run when it is
//!   unknown; after `||` the reverse; after `;`, `&`, a newline or a
//!   parenthesis it runs; in a pipeline, as its pipeline does.
//!
//! Only a skipped command and a failure the output names change what is
//! extracted (`flow.extract.shell-skipped-command-no-access`): a command
//! that may have run is taken as run, as before.

use crate::extract::resource::AbsolutePath;

use super::commands::program;
use super::lex::{Command, Join, RedirectOp, Script, Word};

/// Whether a command ran.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Ran {
    Yes,
    No,
    /// It ran if the command before it succeeded (or failed, after `||`),
    /// which the output does not show.
    Maybe,
}

/// A command's exit status as the output shows it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Status {
    Succeeded,
    Failed,
    Unknown,
}

/// One command, judged.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Step {
    pub ran: Ran,
    pub status: Status,
    /// The operands a file reader could not open, as written.
    pub unread: Vec<String>,
}

impl Step {
    fn assumed() -> Self {
        Self {
            ran: Ran::Yes,
            status: Status::Unknown,
            unread: Vec::new(),
        }
    }

    /// Whether the command's effects happened: it ran (or may have) and
    /// did not fail.
    pub fn took_effect(&self) -> bool {
        self.ran != Ran::No && self.status != Status::Failed
    }
}

/// Every command of a script, judged by its output.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Evidence {
    steps: Vec<Step>,
    /// The home directory a failed `cd ~/…` printed in full.
    home: Option<AbsolutePath>,
}

impl Evidence {
    /// No output: every command is taken as run, its status unknown.
    pub fn assumed(script: &Script) -> Self {
        Self {
            steps: script.commands.iter().map(|_| Step::assumed()).collect(),
            home: None,
        }
    }

    /// What `output` shows of `script`'s commands.
    pub fn read(script: &Script, output: &str) -> Self {
        let mut failures = Failures::scan(output);
        let cds = script
            .commands
            .iter()
            .filter(|command| matches!(program(&command.words), Some(("cd", _))))
            .count();
        let mut steps: Vec<Step> = Vec::with_capacity(script.commands.len());
        let mut home = None;
        let mut list = Status::Succeeded;
        for command in &script.commands {
            let mut ran = match command.join {
                Join::Sequence => Ran::Yes,
                Join::Pipe => steps.last().map_or(Ran::Yes, |step| step.ran),
                Join::And => match list {
                    Status::Succeeded => Ran::Yes,
                    Status::Failed => Ran::No,
                    Status::Unknown => Ran::Maybe,
                },
                Join::Or => match list {
                    Status::Succeeded => Ran::No,
                    Status::Failed => Ran::Yes,
                    Status::Unknown => Ran::Maybe,
                },
            };
            if ran == Ran::No {
                steps.push(Step {
                    ran,
                    status: Status::Unknown,
                    unread: Vec::new(),
                });
                continue;
            }
            let (status, unread) = failures.shown(command, ran, cds == 1, &mut home);
            if ran == Ran::Maybe && status == Status::Failed {
                // Its failure shows it ran.
                ran = Ran::Yes;
            }
            list = match (ran, status) {
                (Ran::Maybe, Status::Succeeded) => Status::Unknown,
                (_, status) => status,
            };
            steps.push(Step {
                ran,
                status,
                unread,
            });
        }
        Self { steps, home }
    }

    /// Command `index`'s step.
    pub fn step(&self, index: usize) -> Option<&Step> {
        self.steps.get(index)
    }

    /// Whether the output shows that the access a command made on
    /// `operand` (the file a reader opens; `None` for the command's other
    /// accesses) did not happen: the command was skipped, or it failed on
    /// that operand, or the shell could not find it.
    pub fn refutes(&self, index: usize, operand: Option<&str>) -> bool {
        let Some(step) = self.steps.get(index) else {
            return false;
        };
        step.ran == Ran::No
            || operand.is_some_and(|operand| step.unread.iter().any(|unread| unread == operand))
            || (operand.is_none() && step.status == Status::Failed && step.unread.is_empty())
    }

    /// The home directory the output showed, if any.
    pub fn home(&self) -> Option<&AbsolutePath> {
        self.home.as_ref()
    }
}

/// The failure lines of an output, consumed as commands claim them.
#[derive(Debug, Default)]
struct Failures {
    /// `cd` failures with the directory printed.
    cd: Vec<String>,
    /// `cd` failures that name no directory (`too many arguments`).
    cd_untargeted: usize,
    /// Names the shell could not find.
    not_found: Vec<String>,
    /// File readers' failures: the program and the operand.
    unread: Vec<(String, String)>,
}

const CD_REASONS: [&str; 4] = [
    "No such file or directory",
    "Not a directory",
    "Permission denied",
    "File name too long",
];

const CD_UNTARGETED: [&str; 3] = ["too many arguments", "OLDPWD not set", "HOME not set"];

const READ_REASONS: [&str; 4] = [
    "No such file or directory",
    "Is a directory",
    "Permission denied",
    "Not a directory",
];

const READERS: [&str; 6] = ["cat", "tac", "head", "tail", "sed", "bat"];

impl Failures {
    fn scan(output: &str) -> Self {
        let mut failures = Self::default();
        for line in output.lines().map(str::trim) {
            if let Some(rest) = after(line, "cd: ") {
                if let Some(target) = cd_target(rest) {
                    failures.cd.push(target);
                } else if CD_UNTARGETED.iter().any(|reason| rest.starts_with(reason)) {
                    failures.cd_untargeted += 1;
                }
                continue;
            }
            if let Some(name) = not_found(line) {
                failures.not_found.push(name);
                continue;
            }
            if let Some(read) = reader_failure(line) {
                failures.unread.push(read);
            }
        }
        failures
    }

    /// The status `command` shows, and the operands it could not read.
    /// `only_cd`: the script has one `cd`, which an untargeted `cd`
    /// failure must be.
    fn shown(
        &mut self,
        command: &Command,
        ran: Ran,
        only_cd: bool,
        home: &mut Option<AbsolutePath>,
    ) -> (Status, Vec<String>) {
        let Some((name, args)) = program(&command.words) else {
            return (Status::Unknown, Vec::new());
        };
        if let Some(at) = self.not_found.iter().position(|missing| missing == name) {
            self.not_found.remove(at);
            return (Status::Failed, Vec::new());
        }
        match name {
            "cd" => {
                let target = args
                    .iter()
                    .find(|word| !matches!(word.text.as_str(), "-L" | "-P" | "-e" | "-@"));
                if let Some(at) = target.and_then(|word| self.cd_failure(word, home)) {
                    self.cd.remove(at);
                    return (Status::Failed, Vec::new());
                }
                if only_cd && self.cd_untargeted > 0 {
                    self.cd_untargeted -= 1;
                    return (Status::Failed, Vec::new());
                }
                if ran == Ran::Yes && stderr_shown(command) {
                    (Status::Succeeded, Vec::new())
                } else {
                    (Status::Unknown, Vec::new())
                }
            }
            "true" | ":" => (Status::Succeeded, Vec::new()),
            "false" => (Status::Failed, Vec::new()),
            reader if READERS.contains(&reader) => {
                let mut unread = Vec::new();
                for word in args.iter().filter(|word| !word.text.starts_with('-')) {
                    if let Some(at) = self
                        .unread
                        .iter()
                        .position(|(program, operand)| program == reader && *operand == word.text)
                    {
                        self.unread.remove(at);
                        unread.push(word.text.clone());
                    }
                }
                let status = if unread.is_empty() {
                    Status::Unknown
                } else {
                    Status::Failed
                };
                (status, unread)
            }
            _ => (Status::Unknown, Vec::new()),
        }
    }

    /// The failure line a `cd` to `word` printed, if any. bash prints a
    /// `~/x` target expanded (`/home/u/x`): that also shows the home
    /// directory.
    fn cd_failure(&self, word: &Word, home: &mut Option<AbsolutePath>) -> Option<usize> {
        if let Some(text) = word.as_literal() {
            return self.cd.iter().position(|target| target == text);
        }
        let rest = word.home()?;
        let rest = AbsolutePath::parse(if rest.is_empty() { "/" } else { rest }).ok()?;
        self.cd.iter().position(|target| {
            let printed = AbsolutePath::parse(target).ok();
            let Some(found) = printed.and_then(|printed| {
                if rest.as_str() == "/" {
                    None
                } else {
                    printed.strip_suffix(&rest)
                }
            }) else {
                return false;
            };
            if home.is_none() {
                *home = Some(found);
            }
            true
        })
    }
}

/// `line` after the first `marker`, where the marker opens the line or
/// follows `: ` (bash's `bash: line 3: cd: x: …`).
fn after<'l>(line: &'l str, marker: &str) -> Option<&'l str> {
    if let Some(rest) = line.strip_prefix(marker) {
        return Some(rest);
    }
    let at = line.find(&format!(": {marker}"))?;
    Some(&line[at + 2 + marker.len()..])
}

/// The directory of a `cd` failure: bash's `<dir>: <reason>`, zsh's
/// `<reason, lower case>: <dir>`, dash's `can't cd to <dir>`.
fn cd_target(rest: &str) -> Option<String> {
    if let Some(dir) = rest.strip_prefix("can't cd to ") {
        return Some(dir.trim().to_owned());
    }
    if let Some((dir, reason)) = rest.rsplit_once(": ")
        && CD_REASONS.contains(&reason.trim())
    {
        return Some(dir.to_owned());
    }
    CD_REASONS.iter().find_map(|reason| {
        rest.strip_prefix(&reason.to_lowercase())
            .and_then(|tail| tail.strip_prefix(": "))
            .map(|dir| dir.trim().to_owned())
    })
}

/// The name of a `command not found` line: bash's `[bash: [line N: ]]<name>:
/// command not found`, zsh's `zsh: command not found: <name>`.
fn not_found(line: &str) -> Option<String> {
    if let Some(name) = line.strip_prefix("zsh: command not found: ") {
        return Some(name.trim().to_owned());
    }
    let head = line.strip_suffix(": command not found")?;
    let name = head.rsplit(": ").next()?.trim();
    (!name.is_empty() && !name.contains(char::is_whitespace)).then(|| name.to_owned())
}

/// A file reader's failure: `cat: <f>: <reason>`, `head: cannot open '<f>'
/// for reading: …`, `sed: can't read <f>: …`, `[bat error]: '<f>': …`.
fn reader_failure(line: &str) -> Option<(String, String)> {
    if let Some(rest) = line.strip_prefix("[bat error]: ") {
        let (file, reason) = rest.rsplit_once(": ")?;
        return READ_REASONS
            .iter()
            .any(|known| reason.starts_with(known))
            .then(|| ("bat".to_owned(), unquote(file)));
    }
    let (program, rest) = line.split_once(": ")?;
    let program = program.rsplit('/').next()?;
    if !READERS.contains(&program) {
        return None;
    }
    if let Some(rest) = rest.strip_prefix("cannot open ") {
        let (file, _) = rest.split_once(" for reading")?;
        return Some((program.to_owned(), unquote(file)));
    }
    if let Some(rest) = rest.strip_prefix("can't read ") {
        let (file, _) = rest.rsplit_once(": ")?;
        return Some((program.to_owned(), unquote(file)));
    }
    let (file, reason) = rest.rsplit_once(": ")?;
    READ_REASONS
        .contains(&reason.trim())
        .then(|| (program.to_owned(), unquote(file)))
}

/// A name as coreutils quote it: `'a b'` or `"it's"`.
fn unquote(name: &str) -> String {
    let name = name.trim();
    for quote in ['\'', '"'] {
        if let Some(inner) = name
            .strip_prefix(quote)
            .and_then(|rest| rest.strip_suffix(quote))
        {
            return inner.to_owned();
        }
    }
    name.to_owned()
}

/// Whether a command's stderr reaches the output: no redirection of
/// descriptor 2 (`2>`, `2>&1` keeps it), `&>` or `>&` file.
fn stderr_shown(command: &Command) -> bool {
    !command.redirects.iter().any(|redirect| match redirect.op {
        RedirectOp::Both => true,
        RedirectOp::Out | RedirectOp::Append => redirect.fd == Some(2),
        RedirectOp::DupOut => match redirect.fd {
            Some(2) => redirect.target.text != "1",
            None => !redirect.target.text.bytes().all(|b| b.is_ascii_digit()),
            Some(_) => false,
        },
        RedirectOp::In
        | RedirectOp::DupIn
        | RedirectOp::ReadWrite
        | RedirectOp::HereDoc
        | RedirectOp::HereString => false,
    })
}

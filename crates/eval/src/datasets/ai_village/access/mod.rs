//! Reads and writes of shared resources, from bash commands, as L5's
//! extractor sees them.
//!
//! Every command goes through `crosstalk_flow`'s [`ToolExtractors`] as the
//! `bash` tool call the agent made, with its output as the result: the
//! locators, the ops and each write's [`WriteOutcome`] (judged from the
//! output: git, curl, wget and the forge CLIs print their failures) are
//! the extractor's own. So are the commands it reads:
//!
//! | Command | Access | Locator |
//! | --- | --- | --- |
//! | `git push` | write, no content in the call (`WritePayload::Unseen`) | the repository (`Locator::Repository`) |
//! | `git pull`, `git fetch`, `git clone`, `gh repo clone` | read | the repository |
//! | `gh`/`glab` `issue`/`pr`/`mr` `create` | write | the collection (`/issues`, `/pulls`, `/-/issues`, `/-/merge_requests`) |
//! | `gh`/`glab` `comment`, `note`, `edit`, `review` | write | the thread (`/issues/<N>`, `/-/merge_requests/<N>`) |
//! | `gh`/`glab` `view` / `list` | read | the thread / the collection |
//! | `gh api`, `glab api`, `curl`, `wget` | the HTTP method's | the URL's site locator: a repository, its file (`File { host: "<host>/<o>/<n>" }`), a thread, or the URL |
//! | `cat`, `head`, `tail`, `sed -n '<lines>p'` of a file; `>`, `>>`, `tee` into one | read; write | the file: in a clone whose remote is known, the repository's file |
//!
//! Only shared resources are kept ([`kind`]): every village agent has its
//! own computer, so a file outside a known clone is its alone.
//!
//! **What the converter adds** is the shell's true state, which the
//! gateway's context cannot know (see the feature doc's findings):
//!
//! - the village's bash tool is one persistent shell per agent, so the
//!   working directory carries over between calls (L5's `bash` does not
//!   persist it; the context is moved as Claude Code's persistent `Bash`
//!   would be);
//! - the home directory is [`HOME`], so `~` and `$HOME` are expanded
//!   before extraction (L5 cannot follow `~`);
//! - a clone made before the window is learnt from the remote a push or
//!   pull prints (`To <remote>`, `From <remote>`): the directory the
//!   command ended in is bound to it, and the command is extracted again
//!   with the binding.
//!
//! A write keeps the text its author typed into the command ([`payload`]),
//! unless L5 says the call carries none of its content (`git push`).

pub mod payload;
pub mod shell;

use std::sync::LazyLock;

use crosstalk_flow::extract::resource::RepoId;
use crosstalk_flow::extract::{
    AbsolutePath, Classified, ConversationContext, ExtractConfig, ExtractedOp, ToolExtractors,
    WritePayload,
};
use crosstalk_spec::derived::flow::access::WriteOutcome;
use crosstalk_spec::derived::flow::resource::Locator;
use crosstalk_spec::observed::message::{
    CanonicalJson, Text, ToolArguments, ToolCall, ToolCallId, ToolExecution, ToolName, ToolOutcome,
    ToolResult, ToolResultContent,
};

use super::resource::{ResourceKind, kind};

/// The bash tool's home directory in the village's computers.
pub const HOME: &str = "/home/computeruse";

/// The village's shell tool, as the agents call it.
pub const BASH_TOOL: &str = "bash";

/// A shell tool whose working directory persists across calls in L5's
/// catalog (Claude Code's), used to move the context as the village's
/// persistent shell moves.
const PERSISTENT_SHELL: &str = "Bash";

/// The extractor's default configuration, which the gateway runs with.
static CONFIG: LazyLock<ExtractConfig> = LazyLock::new(ExtractConfig::default);

/// What a write carried.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Payload {
    /// The content is not in the call (`git push`): the gateway records
    /// the write without spans, so only co-access can link it.
    Unseen,
    /// The texts the author typed (here-document bodies, body flags,
    /// data); may be empty.
    Authored(Vec<String>),
}

/// A read, or a write with its outcome and payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Op {
    Read,
    Write {
        outcome: WriteOutcome,
        payload: Payload,
    },
}

impl Op {
    pub fn is_write(&self) -> bool {
        matches!(self, Self::Write { .. })
    }

    /// Whether this access can be one side of a pair: every read, and
    /// every write the spec pairs (`WriteOutcome::pairs`).
    pub fn pairs(&self) -> bool {
        match self {
            Self::Read => true,
            Self::Write { outcome, .. } => outcome.pairs(),
        }
    }
}

/// One read or write of a shared resource.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Access {
    pub op: Op,
    /// The extractor's locator.
    pub resource: Locator,
    pub kind: ResourceKind,
}

impl Access {
    /// What a write carried; `None` for a read.
    pub fn payload(&self) -> Option<&Payload> {
        match &self.op {
            Op::Read => None,
            Op::Write { payload, .. } => Some(payload),
        }
    }

    /// `<read|write> <kind>`, for counts.
    pub fn label(&self) -> String {
        let op = if self.op.is_write() { "write" } else { "read" };
        format!("{op} {}", self.kind.as_str())
    }
}

/// One agent's persistent shell, as the extractor's conversation context.
#[derive(Debug, Clone)]
pub struct Shell {
    context: ConversationContext,
    /// Commands the extractor refused (an unterminated quote, arguments
    /// that are not a command): no access, as in the gateway.
    unextracted: u64,
}

impl Default for Shell {
    fn default() -> Self {
        Self {
            context: ConversationContext::new(AbsolutePath::parse(HOME).ok(), None),
            unextracted: 0,
        }
    }
}

impl Shell {
    /// The shell's working directory, when it is known.
    pub fn cwd(&self) -> Option<&str> {
        self.context.cwd().map(AbsolutePath::as_str)
    }

    /// How many commands the extractor refused.
    pub fn unextracted(&self) -> u64 {
        self.unextracted
    }

    /// The shared-resource accesses of one executed command, given its
    /// output (stdout and stderr together). Moves the shell and learns
    /// its clones.
    pub fn accesses(&mut self, command: &str, output: &str) -> Vec<Access> {
        let expanded = expand_home(command);
        let result = result(output);
        let mut extracted = match extract(&self.context, &expanded, &result) {
            Some(found) => found,
            None => {
                self.unextracted += 1;
                Vec::new()
            }
        };
        let mut after = self.context.clone();
        after.observe(&CONFIG, &call(PERSISTENT_SHELL, &expanded), Some(&result));
        if let Some(dir) = after.cwd().cloned()
            && after.repos().locate(&dir).is_none()
            && let Some(repo) = printed_remote(&expanded, output)
        {
            let mut before = self.context.clone();
            before.bind_repo(dir.clone(), repo.clone());
            after.bind_repo(dir, repo);
            extracted = extract(&before, &expanded, &result).unwrap_or_default();
        }
        self.context = after;
        let authored = payload::authored(command);
        extracted
            .into_iter()
            .filter_map(|classified| {
                let kind = kind(&classified.locator)?;
                let op = match classified.op {
                    ExtractedOp::Read => Op::Read,
                    ExtractedOp::Write {
                        outcome,
                        payload: WritePayload::Unseen,
                    } => Op::Write {
                        outcome,
                        payload: Payload::Unseen,
                    },
                    ExtractedOp::Write {
                        outcome,
                        payload: WritePayload::CallArguments,
                    } => Op::Write {
                        outcome,
                        payload: Payload::Authored(authored.clone()),
                    },
                };
                Some(Access {
                    op,
                    resource: classified.locator,
                    kind,
                })
            })
            .collect()
    }
}

/// The call id every converted command gets; the extractor only pairs it
/// with its result.
const CALL_ID: &str = "village-bash";

/// The `bash` call of `command`, as `name`.
fn call(name: &str, command: &str) -> ToolCall {
    ToolCall {
        id: ToolCallId(CALL_ID.to_owned()),
        name: ToolName(name.to_owned()),
        arguments: ToolArguments::Json(CanonicalJson(
            serde_json::json!({ "command": command }).to_string(),
        )),
        execution: ToolExecution::Client,
        signature: None,
    }
}

/// The village records no exit status: every result is a success, judged
/// by its text.
fn result(output: &str) -> ToolResult {
    ToolResult {
        call_id: ToolCallId(CALL_ID.to_owned()),
        content: vec![ToolResultContent::Text(Text(output.to_owned()))],
        outcome: ToolOutcome::Success,
    }
}

/// The extractor's accesses; `None` when it refuses the command.
fn extract(
    context: &ConversationContext,
    command: &str,
    result: &ToolResult,
) -> Option<Vec<Classified>> {
    let extractors = ToolExtractors::new(&CONFIG, context);
    match extractors.extract_classified(&call(BASH_TOOL, command), Some(result)) {
        Ok(found) => Some(found),
        Err(error) => {
            tracing::debug!(?error, "bash command not extracted");
            None
        }
    }
}

/// The forge repository a `git push`, `pull` or `fetch` printed as its
/// remote (`To <remote>`, `From <remote>`).
fn printed_remote(command: &str, output: &str) -> Option<RepoId> {
    let git = command.contains("git");
    let markers: Vec<&str> = [
        (git && command.contains("push"), "To "),
        (
            git && (command.contains("pull") || command.contains("fetch")),
            "From ",
        ),
    ]
    .into_iter()
    .filter_map(|(ran, marker)| ran.then_some(marker))
    .collect();
    output.lines().find_map(|line| {
        let line = line.trim();
        let remote = markers
            .iter()
            .find_map(|marker| line.strip_prefix(marker))?
            .split_whitespace()
            .next()?;
        let repo = RepoId::parse(remote, None)?;
        repo.forge_parts().map(|_| repo.clone())
    })
}

/// `command` with `~` (a word's leading `~` before `/` or the word's end)
/// and `$HOME` / `${HOME}` replaced by [`HOME`].
pub fn expand_home(command: &str) -> String {
    let command = command.replace("${HOME}", HOME).replace("$HOME", HOME);
    let mut out = String::with_capacity(command.len());
    let mut previous: Option<char> = None;
    let mut chars = command.chars().peekable();
    while let Some(c) = chars.next() {
        let word_start = previous.is_none_or(|p| {
            p.is_whitespace() || matches!(p, '=' | ';' | '&' | '|' | '(' | '"' | '\'')
        });
        let ends = chars.peek().is_none_or(|next| {
            *next == '/'
                || next.is_whitespace()
                || matches!(next, ';' | '&' | '|' | ')' | '"' | '\'')
        });
        if c == '~' && word_start && ends {
            out.push_str(HOME);
        } else {
            out.push(c);
        }
        previous = Some(c);
    }
    out
}

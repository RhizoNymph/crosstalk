//! What the extractors know about the conversation a call belongs to: its
//! working directory, the host its files live on and the clones of shared
//! repositories it has made or named.
//!
//! Harness system prompts state the working directory: Claude Code writes
//! `Working directory: /path` (or `Primary working directory: /path`) in its
//! environment block, Codex `<cwd>/path</cwd>` in its environment context.
//! [`stated_cwd`] reads it, and relative file paths resolve against it
//! (`flow.resource.relative-path-cwd`). Without one they are keyed as
//! written (`flow.resource.relative-path-opaque`): guessing would merge
//! unrelated files.
//!
//! The context learns as the conversation goes ([`ConversationContext::observe`]):
//! the flow consumer extracts each call against the context as it was
//! before the call, then lets the context observe the call and its result,
//! in conversation order.

use crosstalk_spec::derived::flow::resource::Host;
use crosstalk_spec::observed::message::{ToolCall, ToolOutcome, ToolResult};

use crate::extract::args::Args;
use crate::extract::bash;
use crate::extract::catalog::{self, KnownTool};
use crate::extract::mcp::config::ExtractConfig;
use crate::extract::outcome::result_text;
use crate::extract::resource::{AbsolutePath, FileScope, RepoBindings, RepoId};

/// The conversation's working directory, the host its files live on and
/// its known clones. Built once per conversation by the flow consumer and
/// kept with it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ConversationContext {
    cwd: Option<AbsolutePath>,
    file_host: Option<Host>,
    repos: RepoBindings,
}

/// Claude Code's note when it moved its shell back into the project.
const CWD_RESET: &str = "Shell cwd was reset to ";

impl ConversationContext {
    pub fn new(cwd: Option<AbsolutePath>, file_host: Option<Host>) -> Self {
        Self {
            cwd,
            file_host,
            repos: RepoBindings::default(),
        }
    }

    /// The context a system prompt states: its working directory, no host.
    pub fn from_system_prompt(text: &str) -> Self {
        Self::new(stated_cwd(text), None)
    }

    pub fn cwd(&self) -> Option<&AbsolutePath> {
        self.cwd.as_ref()
    }

    /// The host file locators carry: `None` for files on the agent's own
    /// machine, which is all a gateway sees of a local harness.
    pub fn file_host(&self) -> Option<&Host> {
        self.file_host.as_ref()
    }

    pub fn repos(&self) -> &RepoBindings {
        &self.repos
    }

    /// What a file path in this conversation resolves against.
    pub fn scope(&self) -> FileScope<'_> {
        FileScope {
            cwd: self.cwd.as_ref(),
            host: self.file_host.as_ref(),
            repos: &self.repos,
        }
    }

    /// Record that `root` is a clone of `repo` (an operator's
    /// configuration, or a binding learned elsewhere).
    pub fn bind_repo(&mut self, root: AbsolutePath, repo: RepoId) {
        self.repos.bind(root, repo);
    }

    /// Learn from one call and its result, after extracting it. Only a
    /// shell call whose result arrived without an error teaches anything:
    /// - the clones its commands made or named (`git clone`, `gh repo
    ///   clone`, `git remote add`), and the remote a `git remote -v` it ran
    ///   printed, for the directory it ran in;
    /// - for a tool whose shell persists, the directory its last command
    ///   left the shell in (unknown when a `cd` could not be followed), or
    ///   the one Claude Code says it reset the shell to.
    pub fn observe(
        &mut self,
        config: &ExtractConfig,
        call: &ToolCall,
        result: Option<&ToolResult>,
    ) {
        let Some(result) = result.filter(|result| result.call_id == call.id) else {
            return;
        };
        if result.outcome == ToolOutcome::Error {
            return;
        }
        let Some(KnownTool::Shell(tool)) = catalog::identify(&call.name, config) else {
            return;
        };
        let Ok(args) = Args::parse(&call.arguments) else {
            return;
        };
        let Ok(run) = bash::run(tool, &call.name, &args, self, config.sites()) else {
            return;
        };
        let text = result_text(result);
        self.repos = run.end.repos;
        if let [Some(dir)] = run.remote_queries.as_slice()
            && let Some(repo) = printed_remote(&text, self.cwd.as_ref())
        {
            self.repos.bind(dir.clone(), repo);
        }
        if tool.persists_cwd {
            self.cwd = run.end.cwd;
        }
        if let Some(reset) = text
            .lines()
            .find_map(|line| line.trim().strip_prefix(CWD_RESET))
            .and_then(|path| AbsolutePath::parse(path.trim()).ok())
        {
            self.cwd = Some(reset);
        }
    }
}

/// The first remote a `git remote -v` (or `get-url`) output shows.
fn printed_remote(text: &str, cwd: Option<&AbsolutePath>) -> Option<RepoId> {
    text.lines().find_map(|line| {
        line.split_whitespace()
            .filter(|word| word.contains("://") || word.contains(':') || word.starts_with('/'))
            .find_map(|word| RepoId::parse(word, cwd))
    })
}

const CWD_LABELS: [&str; 2] = ["Primary working directory:", "Working directory:"];

/// The working directory a harness system prompt states, if it states an
/// absolute one. The first statement wins.
pub fn stated_cwd(text: &str) -> Option<AbsolutePath> {
    text.lines().find_map(|line| {
        let line = line.trim().trim_start_matches(['-', '*', ' ']);
        let labelled = CWD_LABELS
            .iter()
            .find_map(|label| line.strip_prefix(label))
            .or_else(|| {
                line.strip_prefix("<cwd>")
                    .and_then(|rest| rest.strip_suffix("</cwd>"))
            })?;
        AbsolutePath::parse(labelled.trim()).ok()
    })
}

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
//! in conversation order. What it learns is the conversation's shell
//! ([`ShellState`]): its directory (for a tool whose shell persists), its
//! home directory once an output shows it, and the clones and remotes its
//! commands made or printed. It is a function of the calls and results
//! observed, in order, and bounded.

use crosstalk_spec::derived::flow::resource::Host;
use crosstalk_spec::observed::message::{ToolCall, ToolOutcome, ToolResult};

use crate::extract::args::Args;
use crate::extract::bash;
use crate::extract::bash::state::ShellState;
use crate::extract::catalog::{self, KnownTool};
use crate::extract::mcp::config::ExtractConfig;
use crate::extract::outcome::result_text;
use crate::extract::resource::{AbsolutePath, FileScope, Place, RepoBindings, RepoId};

/// The conversation's shell, and the host its files live on. Built once
/// per conversation by the flow consumer and kept with it.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConversationContext {
    shell: ShellState,
    file_host: Option<Host>,
}

/// Claude Code's note when it moved its shell back into the project.
const CWD_RESET: &str = "Shell cwd was reset to ";

impl ConversationContext {
    pub fn new(cwd: Option<AbsolutePath>, file_host: Option<Host>) -> Self {
        Self {
            shell: ShellState::new(cwd),
            file_host,
        }
    }

    /// The context a system prompt states: its working directory, no host.
    pub fn from_system_prompt(text: &str) -> Self {
        Self::new(stated_cwd(text), None)
    }

    /// Take a changed system prompt's context (`stated`) into this one:
    /// what this conversation's calls taught is kept, and only a working
    /// directory this context does not know is taken from it.
    pub fn restate(&mut self, stated: &ConversationContext) {
        if self.shell.cwd().is_none() {
            self.shell.set_cwd(stated.shell.cwd().cloned());
        }
        if self.file_host.is_none() {
            self.file_host = stated.file_host.clone();
        }
    }

    /// The working directory, when it is known as an absolute path.
    pub fn cwd(&self) -> Option<&AbsolutePath> {
        self.shell.absolute_cwd()
    }

    /// The conversation's shell: where it is, its home and its clones.
    pub fn shell(&self) -> &ShellState {
        &self.shell
    }

    /// The host file locators carry: `None` for files on the agent's own
    /// machine, which is all a gateway sees of a local harness.
    pub fn file_host(&self) -> Option<&Host> {
        self.file_host.as_ref()
    }

    pub fn repos(&self) -> &RepoBindings {
        self.shell.repos()
    }

    /// What a file path in this conversation resolves against.
    pub fn scope(&self) -> FileScope<'_> {
        FileScope {
            cwd: self.cwd(),
            host: self.file_host.as_ref(),
            repos: self.shell.repos(),
        }
    }

    /// Record that `root` is a clone of `repo` (an operator's
    /// configuration, or a binding learned elsewhere), as its `origin`.
    pub fn bind_repo(&mut self, root: AbsolutePath, repo: RepoId) {
        self.shell.bind(
            Place::Absolute(root),
            crate::extract::resource::repo::ORIGIN,
            repo,
        );
    }

    /// Learn from one call and its result, after extracting it. Only a
    /// shell call whose result arrived without an error teaches anything
    /// (`bash::after`): the clones and remotes its commands made or
    /// printed, the home directory its output showed, and, for a tool
    /// whose shell persists (built in, or configured
    /// `persistent_shells`), the directory its commands left the shell in
    /// (a skipped command or a failed `cd` does not move it; one it cannot
    /// follow makes it unknown) or the one Claude Code says it reset the
    /// shell to.
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
        let text = result_text(result);
        let Ok(mut end) = bash::after(tool, &call.name, &args, self, config.sites(), &text) else {
            return;
        };
        let persists = tool.persists_cwd || config.persistent_shell(&call.name.0);
        if !persists {
            end.set_cwd(self.shell.cwd().cloned());
        }
        if let Some(reset) = text
            .lines()
            .find_map(|line| line.trim().strip_prefix(CWD_RESET))
            .and_then(|path| AbsolutePath::parse(path.trim()).ok())
        {
            end.set_cwd(Some(Place::Absolute(reset)));
        }
        self.shell = end;
    }
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

//! `git`: which directories are clones of which repositories, the
//! repository files `git show` prints, and the repository itself, which
//! `git push` writes and `git pull`, `fetch` and `clone` read.
//!
//! - `git clone <remote> [<dir>]` (and `gh repo clone <repo> [<dir>]`,
//!   [`super::forge`]) binds the directory it makes (`<dir>`, else the
//!   repository's name, from the working directory) to the repository,
//!   and reads the repository (`RepoId::locator`).
//! - `git push [<remote>]` writes the repository, with no content of its
//!   own in the call (`WritePayload::Unseen`); `--dry-run` writes nothing.
//!   `git pull [<remote>]` and `git fetch [<remote>]` read it. The
//!   repository is the remote operand when it is a URL or a path, else
//!   (a remote name, or none) the one the clone the command runs in is
//!   bound to. The output judges each (`CommandRule::GitPush`,
//!   `CommandRule::GitTransfer`).
//! - `git remote add|set-url <name> <url>` binds the working directory, as
//!   the clone's root.
//! - `git remote -v`, `git remote get-url` and `git config --get
//!   remote.<name>.url` print a remote: the context binds the working
//!   directory to the one the result shows
//!   ([`ConversationContext::observe`]).
//! - `git show <rev>:<path>` (and `git cat-file -p`) reads the repository's
//!   file at `<path>`, from the root of the clone the working directory is
//!   in (from the working directory for `./` and `../` paths).
//!
//! `git -C <dir>` runs any of these in `<dir>`.
//!
//! [`ConversationContext::observe`]: crate::extract::context::ConversationContext::observe

use crosstalk_spec::derived::flow::access::Extraction;

use crate::extract::op::Candidate;
use crate::extract::outcome::CommandRule;
use crate::extract::resource::{AbsolutePath, RepoId, absolute_locator};

use super::commands::Shell;
use super::lex::Word;
use super::options::{OptSpec, Options};

const CLONE: OptSpec = OptSpec {
    short_values: "bocju",
    long_values: &[
        "--branch",
        "--origin",
        "--config",
        "--depth",
        "--reference",
        "--reference-if-able",
        "--separate-git-dir",
        "--jobs",
        "--filter",
        "--template",
        "--upload-pack",
        "--shallow-since",
        "--shallow-exclude",
        "--server-option",
        "--bundle-uri",
    ],
};

const PUSH: OptSpec = OptSpec {
    short_values: "o",
    long_values: &["--repo", "--push-option", "--receive-pack", "--exec"],
};

const PULL_FETCH: OptSpec = OptSpec {
    short_values: "jsXo",
    long_values: &[
        "--depth",
        "--deepen",
        "--shallow-since",
        "--shallow-exclude",
        "--jobs",
        "--upload-pack",
        "--strategy",
        "--strategy-option",
        "--server-option",
        "--negotiation-tip",
    ],
};

impl Shell<'_> {
    pub(super) fn git(&mut self, args: &[Word], stdout_to_file: bool, found: &mut Vec<Candidate>) {
        let mut dir = self.state.cwd.clone();
        let mut rest = args;
        while let Some((first, tail)) = rest.split_first() {
            match first.text.as_str() {
                "-C" => {
                    let Some((target, tail)) = tail.split_first() else {
                        return;
                    };
                    dir = self.directory(dir.as_ref(), target);
                    rest = tail;
                }
                "-c" | "--git-dir" | "--work-tree" | "--namespace" | "--exec-path" => {
                    rest = tail.split_first().map_or(tail, |(_, tail)| tail);
                }
                option if option.starts_with('-') => rest = tail,
                _ => break,
            }
        }
        let Some((subcommand, args)) = rest.split_first() else {
            return;
        };
        match subcommand.text.as_str() {
            "clone" => {
                let parsed = Options::parse(args, &CLONE);
                if let Some(repo) =
                    self.clone_into(dir.as_ref(), &parsed.operands, |remote, cwd| {
                        RepoId::parse(remote, cwd)
                    })
                {
                    found.push(
                        Candidate::read(repo.locator().clone(), Extraction::Parsed)
                            .judged_by(CommandRule::GitTransfer),
                    );
                }
            }
            "push" => {
                let parsed = Options::parse(args, &PUSH);
                if parsed.has(&["-n", "--dry-run"]) {
                    return;
                }
                let named = parsed
                    .last(&["--repo"])
                    .or_else(|| parsed.operands.first().copied());
                if let Some(repo) = self.remote_repo(dir.as_ref(), named) {
                    found.push(
                        Candidate::write(repo.locator().clone(), Extraction::Parsed)
                            .unseen()
                            .judged_by(CommandRule::GitPush),
                    );
                }
            }
            "pull" | "fetch" => {
                let parsed = Options::parse(args, &PULL_FETCH);
                if parsed.has(&["--dry-run"]) {
                    return;
                }
                let named = if parsed.has(&["--all", "--multiple"]) {
                    None
                } else {
                    parsed.operands.first().copied()
                };
                if let Some(repo) = self.remote_repo(dir.as_ref(), named) {
                    found.push(
                        Candidate::read(repo.locator().clone(), Extraction::Parsed)
                            .judged_by(CommandRule::GitTransfer),
                    );
                }
            }
            "remote" => match args {
                [verb, _name, url, ..] if matches!(verb.text.as_str(), "add" | "set-url") => {
                    if let (Some(dir), Some(repo)) = (
                        dir,
                        url.as_literal()
                            .and_then(|url| RepoId::parse(url, self.state.cwd.as_ref())),
                    ) {
                        self.state.repos.bind(dir, repo);
                    }
                }
                [verb, ..] if matches!(verb.text.as_str(), "-v" | "--verbose" | "get-url") => {
                    self.remote_queries.push(dir);
                }
                _ => {}
            },
            "config" => {
                let prints_url =
                    args.iter().any(|word| {
                        word.text.starts_with("remote.") && word.text.ends_with(".url")
                    }) && !args.iter().any(|word| {
                        matches!(word.text.as_str(), "--add" | "--unset" | "--replace-all")
                    }) && args.len() <= 3;
                if prints_url {
                    self.remote_queries.push(dir);
                }
            }
            "show" | "cat-file" if !stdout_to_file => {
                for word in args.iter().filter(|word| !word.text.starts_with('-')) {
                    if let Some(candidate) = self.revision_file(dir.as_ref(), word) {
                        found.push(candidate);
                    }
                }
            }
            _ => {}
        }
    }

    /// Bind the directory a clone makes to the repository it clones, which
    /// is returned.
    pub(super) fn clone_into(
        &mut self,
        dir: Option<&AbsolutePath>,
        operands: &[&Word],
        repo_of: impl Fn(&str, Option<&AbsolutePath>) -> Option<RepoId>,
    ) -> Option<RepoId> {
        let remote = operands.first().and_then(|word| word.as_literal())?;
        let repo = repo_of(remote, dir)?;
        let target = match operands.get(1) {
            Some(word) => self.directory(dir, word),
            None => dir
                .zip(clone_directory(remote))
                .and_then(|(dir, name)| dir.join(name).ok()),
        };
        if let Some(target) = target {
            self.state.repos.bind(target, repo.clone());
        }
        Some(repo)
    }

    /// The repository a remote operand names: a URL or path names it
    /// directly; a remote name, or none, is the remote the clone `dir` is
    /// in is bound to.
    pub(super) fn remote_repo(
        &self,
        dir: Option<&AbsolutePath>,
        named: Option<&Word>,
    ) -> Option<RepoId> {
        if let Some(word) = named {
            let text = word.as_literal()?;
            if let Some(repo) = RepoId::parse(text, dir) {
                return Some(repo);
            }
        }
        self.bound_repo(dir)
    }

    /// The repository the clone `dir` is in is bound to.
    pub(super) fn bound_repo(&self, dir: Option<&AbsolutePath>) -> Option<RepoId> {
        let (repo, _) = self.state.repos.locate(dir?)?;
        Some(repo.clone())
    }

    /// `<rev>:<path>`: the repository file it names.
    fn revision_file(&self, dir: Option<&AbsolutePath>, word: &Word) -> Option<Candidate> {
        let (_rev, path) = word.as_literal()?.split_once(':')?;
        if path.is_empty() {
            return None;
        }
        let dir = dir?;
        let locator = if path.starts_with("./") || path.starts_with("../") {
            absolute_locator(dir.join(path).ok()?, self.scope())
        } else {
            let (repo, _) = self.state.repos.locate(dir)?;
            repo.file(&AbsolutePath::parse(&format!("/{path}")).ok()?)
        };
        Some(Candidate::read(locator, Extraction::Parsed))
    }
}

/// The directory `git clone` makes when none is given: the remote's last
/// path segment as written, without `.git`.
fn clone_directory(remote: &str) -> Option<&str> {
    let trimmed = remote.trim_end_matches('/');
    let trimmed = trimmed.strip_suffix(".git").unwrap_or(trimmed);
    let name = trimmed.rsplit(['/', ':']).next()?;
    (!name.is_empty() && name != "." && name != "..").then_some(name)
}

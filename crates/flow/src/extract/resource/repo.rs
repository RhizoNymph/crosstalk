//! Git repositories: one file of a shared repository is one resource,
//! whichever clone an agent touches it in.
//!
//! Agents that share a repository work in their own clones, at paths that
//! differ by machine and user (`/home/a/atlas/src/x.py`,
//! `/workspace/atlas/src/x.py`). A file inside a clone whose remote is known
//! is keyed by the repository and its path in it:
//! `Locator::File { host: Some(Host(<repo id>)), path: "/src/x.py" }`. The
//! same file reached through the forge's URLs (a GitHub blob, raw or
//! contents URL) gets the same locator ([`crate::extract::sites`]).
//!
//! A [`RepoId`] is the remote in canonical form: `host/owner/name` for a
//! network remote (lowercase, without `.git`, scheme, user or port), the
//! normalized absolute path for a repository on the local filesystem.
//!
//! The repository itself (what `git push`, `git pull` and `git clone`
//! touch) is [`RepoId::locator`]: the spec's canonical
//! `Locator::Repository { host, owner, name }` for a forge repository,
//! `Locator::File { host: None, path }` of its directory for a local one.
//! Every spelling of a remote meets on it, and so do the forge URLs that
//! name the repository ([`crate::extract::sites`]).
//!
//! An issue or a pull/merge request of a forge repository is a `Url` of its
//! canonical web page ([`ForgeRepo::thread`], [`ForgeRepo::collection`]).

use std::cmp::Ordering;
use std::fmt;

use crosstalk_spec::derived::flow::resource::{Host, Locator};

use super::path::AbsolutePath;

/// A repository's canonical identity. Only [`RepoId::parse`] and
/// [`RepoId::forge`] make one.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RepoId {
    /// `host/owner/name`, or the local path.
    id: String,
    /// The repository's own locator; a function of `id`.
    locator: Locator,
}

impl PartialOrd for RepoId {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for RepoId {
    fn cmp(&self, other: &Self) -> Ordering {
        // The locator is a function of the id.
        self.id.cmp(&other.id)
    }
}

/// How a forge spells its issue and change-request pages.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ForgeStyle {
    /// GitHub (and GitHub Enterprise; the `gh` CLI).
    GitHub,
    /// GitLab (the `glab` CLI).
    GitLab,
}

/// What a forge thread is: an issue, or a pull (GitHub) or merge (GitLab)
/// request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ThreadKind {
    Issue,
    Change,
}

/// The parts of a forge repository, canonical.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ForgeRepo<'a> {
    pub host: &'a str,
    pub owner: &'a str,
    pub name: &'a str,
}

impl ForgeRepo<'_> {
    /// The canonical locator of issue or change request `number`: the
    /// `https` URL of its web page, with the repository's canonical
    /// (lower-case) path.
    ///
    /// - GitHub: `/<owner>/<name>/issues/<n>` for an issue and a pull
    ///   request alike. They share one number space and one conversation
    ///   (`/issues/<n>` of a pull request redirects to `/pull/<n>`, and the
    ///   REST API comments on both under `/issues/<n>/comments`), so a
    ///   comment made either way and a read either way meet.
    /// - GitLab: `/<owner>/<name>/-/issues/<n>` and
    ///   `/<owner>/<name>/-/merge_requests/<n>`, separate number spaces.
    pub fn thread(&self, style: ForgeStyle, kind: ThreadKind, number: u64) -> Locator {
        let page = match (style, kind) {
            (ForgeStyle::GitHub, _) => format!("issues/{number}"),
            (ForgeStyle::GitLab, ThreadKind::Issue) => format!("-/issues/{number}"),
            (ForgeStyle::GitLab, ThreadKind::Change) => format!("-/merge_requests/{number}"),
        };
        self.page(&page)
    }

    /// The canonical locator of the repository's issues or change
    /// requests as a collection, for an access whose number is not known
    /// from the call (a `create`, a `list`): GitHub `/<owner>/<name>/issues`
    /// and `/<owner>/<name>/pulls`, GitLab `/<owner>/<name>/-/issues` and
    /// `/<owner>/<name>/-/merge_requests`.
    pub fn collection(&self, style: ForgeStyle, kind: ThreadKind) -> Locator {
        let page = match (style, kind) {
            (ForgeStyle::GitHub, ThreadKind::Issue) => "issues",
            (ForgeStyle::GitHub, ThreadKind::Change) => "pulls",
            (ForgeStyle::GitLab, ThreadKind::Issue) => "-/issues",
            (ForgeStyle::GitLab, ThreadKind::Change) => "-/merge_requests",
        };
        self.page(page)
    }

    fn page(&self, page: &str) -> Locator {
        Locator::Url {
            scheme: "https".to_owned(),
            host: Host(self.host.to_owned()),
            path: format!("/{}/{}/{page}", self.owner, self.name),
            query: None,
        }
    }
}

impl RepoId {
    /// The repository a git remote names: `https://host/owner/name(.git)`,
    /// `ssh://[user@]host[:port]/owner/name`, `git://…`,
    /// `[user@]host:owner/name` (scp form), `file:///path` or an absolute
    /// path. A relative local path resolves against `cwd`. `None` for
    /// anything else.
    pub fn parse(remote: &str, cwd: Option<&AbsolutePath>) -> Option<Self> {
        let remote = remote.trim();
        if remote.is_empty() || remote.contains(char::is_whitespace) {
            return None;
        }
        if let Some(path) = remote.strip_prefix("file://") {
            return Self::local(AbsolutePath::parse(path).ok()?);
        }
        if let Some((scheme, rest)) = remote.split_once("://") {
            if !matches!(
                scheme,
                "http" | "https" | "ssh" | "git" | "git+ssh" | "ssh+git"
            ) {
                return None;
            }
            let (authority, path) = rest.split_once('/')?;
            let host = authority.rsplit('@').next()?;
            let host = host.split(':').next()?;
            return Self::forge(host, path);
        }
        if remote.starts_with('/') {
            return Self::local(AbsolutePath::parse(remote).ok()?);
        }
        if let Some((authority, path)) = remote.split_once(':')
            && !authority.contains('/')
            && !path.starts_with("//")
        {
            let host = authority.rsplit('@').next()?;
            // A host has a dot (`github.com`) or is `localhost`; a drive
            // letter (`C:/x`) is not one.
            if host.contains('.') || host == "localhost" {
                return Self::forge(host, path);
            }
            return None;
        }
        if remote.starts_with('.') {
            return Self::local(cwd?.join(remote).ok()?);
        }
        None
    }

    /// The repository at `path` (`owner/name`, or `group/subgroup/name`)
    /// on `host`. `path` may carry `.git` and slashes around it.
    pub fn forge(host: &str, path: &str) -> Option<Self> {
        let path = path.trim_matches('/');
        let path = path.strip_suffix(".git").unwrap_or(path);
        let (owner, name) = path.rsplit_once('/')?;
        let locator = Locator::repository(host, owner, name).ok()?;
        let id = locator.repository_file_host()?.0;
        Some(Self { id, locator })
    }

    fn local(path: AbsolutePath) -> Option<Self> {
        let text = path.into_string();
        let text = text
            .strip_suffix(".git")
            .unwrap_or(&text)
            .trim_end_matches('/');
        (!text.is_empty()).then(|| Self {
            id: text.to_owned(),
            locator: Locator::File {
                host: None,
                path: text.to_owned(),
            },
        })
    }

    pub fn as_str(&self) -> &str {
        &self.id
    }

    /// The repository's own locator: `Locator::Repository` on a forge,
    /// the `File` of its directory on the local filesystem.
    pub fn locator(&self) -> &Locator {
        &self.locator
    }

    /// The forge repository's parts; `None` for a local one.
    pub fn forge_parts(&self) -> Option<ForgeRepo<'_>> {
        match &self.locator {
            Locator::Repository { host, owner, name } => Some(ForgeRepo {
                host: &host.0,
                owner,
                name,
            }),
            Locator::Url { .. }
            | Locator::File { .. }
            | Locator::Mcp { .. }
            | Locator::Opaque { .. } => None,
        }
    }

    /// The locator of the file at `path` (absolute within the repository)
    /// in this repository.
    pub fn file(&self, path: &AbsolutePath) -> Locator {
        Locator::File {
            host: Some(Host(self.id.clone())),
            path: path.as_str().to_owned(),
        }
    }
}

impl fmt::Display for RepoId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.id)
    }
}

/// Which local directories are clones of which repositories. A later
/// binding of the same directory replaces the earlier one; a path is in
/// the clone whose root is its longest ancestor.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RepoBindings(Vec<(AbsolutePath, RepoId)>);

impl RepoBindings {
    pub fn bind(&mut self, root: AbsolutePath, repo: RepoId) {
        self.0.retain(|(bound, _)| *bound != root);
        self.0.push((root, repo));
    }

    pub fn iter(&self) -> impl Iterator<Item = (&AbsolutePath, &RepoId)> {
        self.0.iter().map(|(root, repo)| (root, repo))
    }

    /// The repository `path` is in and its path inside it.
    pub fn locate(&self, path: &AbsolutePath) -> Option<(&RepoId, AbsolutePath)> {
        self.0
            .iter()
            .filter_map(|(root, repo)| Some((root, repo, within(path, root)?)))
            .max_by_key(|(root, _, _)| root.as_str().len())
            .map(|(_, repo, inside)| (repo, inside))
    }
}

/// `path` relative to `root`, as an absolute path inside it.
fn within(path: &AbsolutePath, root: &AbsolutePath) -> Option<AbsolutePath> {
    if root.as_str() == "/" {
        return Some(path.clone());
    }
    let rest = path.as_str().strip_prefix(root.as_str())?;
    if rest.is_empty() {
        return Some(AbsolutePath::root());
    }
    rest.starts_with('/')
        .then(|| AbsolutePath::parse(rest).ok())
        .flatten()
}

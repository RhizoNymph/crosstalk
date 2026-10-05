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

use std::fmt;

use crosstalk_spec::derived::flow::resource::{Host, Locator};

use super::path::AbsolutePath;

/// A repository's canonical identity. Only [`RepoId::parse`] and
/// [`RepoId::forge`] make one.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RepoId(String);

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

    /// `host/owner/name` from a host and a repository path, which may carry
    /// `.git` and a trailing slash.
    pub fn forge(host: &str, path: &str) -> Option<Self> {
        let host = host.trim_end_matches('.').to_ascii_lowercase();
        let host = host.strip_prefix("www.").unwrap_or(&host);
        let segments: Vec<&str> = path
            .trim_end_matches('/')
            .trim_end_matches(".git")
            .split('/')
            .filter(|segment| !segment.is_empty())
            .collect();
        let valid = |text: &str| {
            !text.is_empty()
                && text != "."
                && text != ".."
                && text
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'))
        };
        if host.is_empty()
            || !valid(host)
            || segments.len() < 2
            || !segments.iter().all(|s| valid(s))
        {
            return None;
        }
        Some(Self(
            format!("{host}/{}", segments.join("/")).to_ascii_lowercase(),
        ))
    }

    fn local(path: AbsolutePath) -> Option<Self> {
        let text = path.into_string();
        let text = text
            .strip_suffix(".git")
            .unwrap_or(&text)
            .trim_end_matches('/');
        (!text.is_empty()).then(|| Self(text.to_owned()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// The locator of the file at `path` (absolute within the repository)
    /// in this repository.
    pub fn file(&self, path: &AbsolutePath) -> Locator {
        Locator::File {
            host: Some(Host(self.0.clone())),
            path: path.as_str().to_owned(),
        }
    }
}

impl fmt::Display for RepoId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
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

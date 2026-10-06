//! Shared resources: the locator L5's extractor gives every form of a
//! repository, its files, threads and other web pages.
//!
//! Nothing here normalizes on its own: every locator is
//! `crosstalk_flow::extract`'s, so a label's resource is exactly what the
//! gateway records for the same access.
//!
//! | Form | Locator |
//! | --- | --- |
//! | a git remote (`https://github.com/o/n(.git)`, `git@github.com:o/n.git`, `ssh://…`), `github.com/o/n`, `…/tree/<ref>`, `codeload.github.com/o/n/…`, `api.github.com/repos/o/n/…`, Pages `o.github.io/n/…` (`o.github.io/` is `o/o.github.io`), `gitlab.com/g/s/p(/-/…)`, `gitlab.com/api/v4/projects/g%2Fs%2Fp/…`, `g.gitlab.io/p/…` | `Locator::Repository { host, owner, name }`, lower case, no `.git`, nested GitLab groups joined with `/` ([`RepoId`]) |
//! | `github.com/o/n/blob\|raw/<ref>/<path>`, `raw.githubusercontent.com/o/n/<ref>/<path>`, `api.github.com/repos/o/n/contents/<path>`, `gitlab.com/…/-/blob\|raw/<ref>/<path>`, a file inside a clone whose remote is known | `File { host: "<host>/<owner>/<name>", path }` |
//! | `github.com/o/n/issues\|pull/<N>`, the API's `issues\|pulls/<N>` | `https://github.com/o/n/issues/<N>` |
//! | GitLab `…/-/issues/<N>`, `…/-/merge_requests/<N>` (web or API) | that page |
//! | an issue or change collection (`gh issue create`, `…/issues`) | `/issues`, `/pulls`, `/-/issues`, `/-/merge_requests` |
//! | any other http(s) URL (a numeric GitLab project id, a unique Pages domain, any site) | L5's URL locator ([`tool_url_locator`]): normalized, query sorted, fragment and credentials dropped; a scheme-less host is `https` |
//!
//! [`kind`] says which of these a locator is, and whether it is shared at
//! all: a file on an agent's own computer (`File` without a host, a local
//! bare repository) is not, since every village agent has its own machine.

use std::sync::LazyLock;

use crosstalk_flow::extract::SitesConfig;
use crosstalk_flow::extract::http::HttpRequest;
use crosstalk_flow::extract::resource::{RepoId, tool_url_locator};
use crosstalk_spec::derived::flow::resource::Locator;
use serde::{Deserialize, Serialize};

/// The extractor's default site rules, which the gateway runs with.
static SITES: LazyLock<SitesConfig> = LazyLock::new(SitesConfig::default);

/// What kind of shared resource a locator is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResourceKind {
    /// A forge repository itself (`Locator::Repository`): what `git push`,
    /// `pull`, `fetch` and `clone` touch.
    Repository,
    /// A file of a forge repository (`File { host: "<host>/<o>/<n>" }`).
    RepoFile,
    /// A web page: an issue or change thread or collection, an API URL,
    /// any other site.
    Url,
}

impl ResourceKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Repository => "repository",
            Self::RepoFile => "repo_file",
            Self::Url => "url",
        }
    }
}

/// The kind of a shared resource; `None` for one only its agent's own
/// computer holds (a local file or bare repository), an MCP resource or
/// an opaque key.
pub fn kind(locator: &Locator) -> Option<ResourceKind> {
    match locator {
        Locator::Repository { .. } => Some(ResourceKind::Repository),
        Locator::File {
            host: Some(host), ..
        } if host.0.contains('/') && !host.0.starts_with('/') => Some(ResourceKind::RepoFile),
        Locator::Url { .. } => Some(ResourceKind::Url),
        Locator::File { .. } | Locator::Mcp { .. } | Locator::Opaque { .. } => None,
    }
}

/// The locator L5 gives a `GET` of `text`: the site rules' (a forge
/// repository, file or thread), else the URL's own.
pub fn from_url(text: &str) -> Option<Locator> {
    let url = tool_url_locator(text).ok()?;
    let site = SITES
        .apply(&HttpRequest::get(url.clone()))
        .and_then(|access| access.locators.into_iter().next());
    Some(site.unwrap_or(url))
}

/// The repository a git remote names (`RepoId::parse`), when it is a
/// forge's; `None` for a local path or anything else.
pub fn from_remote(text: &str) -> Option<Locator> {
    let repo = RepoId::parse(text, None)?;
    repo.forge_parts()?;
    Some(repo.locator().clone())
}

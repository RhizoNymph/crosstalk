//! GitHub: the repository, its files and its issues and pull requests,
//! whatever GitHub URL reaches them.
//!
//! | URL | Locator |
//! | --- | --- |
//! | `github.com/<o>/<n>(.git)`, `…/tree/<ref>`, `codeload.github.com/<o>/<n>/…`, `api.github.com/repos/<o>/<n>` and its other subpaths | the repository (`Locator::Repository`) |
//! | `github.com/<o>/<n>/blob\|raw/<ref>/<path>`, `raw.githubusercontent.com/<o>/<n>/<ref>/<path>`, `api.github.com/repos/<o>/<n>/contents/<path>` | the repository's file (`RepoId::file`), the locator the same file gets in any clone whose remote is known |
//! | `github.com/<o>/<n>/issues\|pull/<N>/…`, `api.github.com/repos/<o>/<n>/issues\|pulls/<N>/…` | the thread `https://github.com/<o>/<n>/issues/<N>` ([`ForgeRepo::thread`]) |
//! | `github.com/<o>/<n>/issues\|pulls`, `api.github.com/repos/<o>/<n>/issues\|pulls` | the collection ([`ForgeRepo::collection`]) |
//! | `<o>.github.io/<n>/…` (Pages) | the repository `<o>/<n>`; `<o>.github.io/` with no path is `<o>/<o>.github.io` |
//!
//! Web and Pages URLs are read only: a writing request to one is left to
//! the plain URL rule. The API's op is the method's. The ref (branch, tag
//! or commit) is dropped: the resource is the file at that path, whose
//! content moves between agents across commits. A ref with a slash in it
//! (`feature/x`) cannot be told from the path in a `blob` URL; the first
//! segment is taken as the ref. A Pages path's first segment is taken as
//! the project repository's name, which a user site's subdirectory is
//! not; nothing in the URL tells them apart.
//!
//! [`ForgeRepo::thread`]: crate::extract::resource::ForgeRepo::thread
//! [`ForgeRepo::collection`]: crate::extract::resource::ForgeRepo::collection

use crosstalk_spec::derived::flow::access::AccessKind;
use crosstalk_spec::derived::flow::resource::Locator;

use crate::extract::http::HttpRequest;
use crate::extract::resource::repo::{ForgeStyle, ThreadKind};
use crate::extract::resource::{AbsolutePath, RepoId};

use super::SiteAccess;

/// First path segments of `github.com` that are not an owner.
const NOT_OWNERS: [&str; 22] = [
    "about",
    "apps",
    "collections",
    "contact",
    "customer-stories",
    "enterprise",
    "events",
    "explore",
    "features",
    "issues",
    "login",
    "marketplace",
    "new",
    "notifications",
    "orgs",
    "pricing",
    "pulls",
    "search",
    "settings",
    "sponsors",
    "topics",
    "users",
];

pub fn apply(request: &HttpRequest) -> Option<SiteAccess> {
    let Locator::Url { host, path, .. } = &request.url else {
        return None;
    };
    let segments = super::segments(path);
    let segments: Vec<&str> = segments.iter().map(String::as_str).collect();
    let read_only = request.method.reads().then_some(AccessKind::Read);
    let by_method = method_kind(request);
    match (host.0.as_str(), segments.as_slice()) {
        ("github.com" | "www.github.com", [owner, ..]) if NOT_OWNERS.contains(owner) => None,
        ("github.com" | "www.github.com", [owner, repo, "blob" | "raw", _ref, file @ ..]) => {
            file_access(owner, repo, file, AccessKind::Read)
        }
        ("github.com" | "www.github.com", [owner, repo, "issues" | "pull", number, ..]) => {
            thread(owner, repo, number, read_only?)
        }
        ("github.com" | "www.github.com", [owner, repo, page @ ("issues" | "pulls")]) => {
            collection(owner, repo, page, read_only?)
        }
        ("github.com" | "www.github.com", [owner, repo])
        | ("github.com" | "www.github.com", [owner, repo, "tree", _]) => {
            repository("github.com", owner, repo, read_only?)
        }
        ("codeload.github.com", [owner, repo, ..]) => {
            repository("github.com", owner, repo, read_only?)
        }
        ("raw.githubusercontent.com", [owner, repo, "refs", "heads" | "tags", _ref, file @ ..])
        | ("raw.githubusercontent.com", [owner, repo, _ref, file @ ..]) => {
            file_access(owner, repo, file, AccessKind::Read)
        }
        ("api.github.com", ["repos", owner, repo, "contents", file @ ..]) => {
            file_access(owner, repo, file, by_method?)
        }
        ("api.github.com", ["repos", owner, repo, "issues" | "pulls", number, ..])
            if number.bytes().all(|b| b.is_ascii_digit()) =>
        {
            thread(owner, repo, number, by_method?)
        }
        ("api.github.com", ["repos", owner, repo, page @ ("issues" | "pulls")]) => {
            collection(owner, repo, page, by_method?)
        }
        ("api.github.com", ["repos", owner, repo, ..]) => {
            repository("github.com", owner, repo, by_method?)
        }
        (pages, rest) => {
            let user = pages.strip_suffix(".github.io")?;
            if user.is_empty() || user.contains('.') {
                return None;
            }
            match rest {
                [] => repository("github.com", user, pages, read_only?),
                [repo, ..] => repository("github.com", user, repo, read_only?),
            }
        }
    }
}

/// The access a request's method makes through an API: `GET`/`HEAD` read,
/// a writing method writes, any other none.
pub(super) fn method_kind(request: &HttpRequest) -> Option<AccessKind> {
    if request.method.reads() {
        Some(AccessKind::Read)
    } else if request.method.writes() {
        Some(AccessKind::Write)
    } else {
        None
    }
}

fn repo_id(owner: &str, repo: &str) -> Option<RepoId> {
    RepoId::forge("github.com", &format!("{owner}/{repo}"))
}

fn file_access(owner: &str, repo: &str, file: &[&str], kind: AccessKind) -> Option<SiteAccess> {
    if file.is_empty() {
        return None;
    }
    let repo = repo_id(owner, repo)?;
    let path = AbsolutePath::parse(&format!("/{}", file.join("/"))).ok()?;
    Some(SiteAccess {
        kind,
        locators: vec![repo.file(&path)],
    })
}

fn thread(owner: &str, repo: &str, number: &str, kind: AccessKind) -> Option<SiteAccess> {
    let number: u64 = number.parse().ok()?;
    let repo = repo_id(owner, repo)?;
    let forge = repo.forge_parts()?;
    Some(SiteAccess {
        kind,
        locators: vec![forge.thread(ForgeStyle::GitHub, ThreadKind::Issue, number)],
    })
}

fn collection(owner: &str, repo: &str, page: &str, kind: AccessKind) -> Option<SiteAccess> {
    let repo = repo_id(owner, repo)?;
    let forge = repo.forge_parts()?;
    let thread = if page == "issues" {
        ThreadKind::Issue
    } else {
        ThreadKind::Change
    };
    Some(SiteAccess {
        kind,
        locators: vec![forge.collection(ForgeStyle::GitHub, thread)],
    })
}

pub(super) fn repository(
    host: &str,
    owner: &str,
    repo: &str,
    kind: AccessKind,
) -> Option<SiteAccess> {
    let repo = RepoId::forge(host, &format!("{owner}/{repo}"))?;
    Some(SiteAccess {
        kind,
        locators: vec![repo.locator().clone()],
    })
}

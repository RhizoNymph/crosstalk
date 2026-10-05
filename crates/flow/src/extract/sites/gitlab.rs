//! GitLab (`gitlab.com`): the project, its files and its issues and merge
//! requests, whatever GitLab URL reaches them.
//!
//! | URL | Locator |
//! | --- | --- |
//! | `gitlab.com/<path…>/<n>(.git)`, `gitlab.com/<path…>/<n>/-/<other>`, `gitlab.com/api/v4/projects/<url-encoded path>` and its other subpaths | the repository (`Locator::Repository`, owner `<path…>`) |
//! | `gitlab.com/<path…>/<n>/-/blob\|raw/<ref>/<file>`, `…/api/v4/projects/<p>/repository/files/<url-encoded file>(/raw)` | the repository's file |
//! | `…/-/issues/<N>`, `…/-/merge_requests/<N>`, `…/api/v4/projects/<p>/issues\|merge_requests/<N>/…` | the thread ([`ForgeRepo::thread`]) |
//! | `…/-/issues`, `…/-/merge_requests`, `…/api/v4/projects/<p>/issues\|merge_requests` | the collection ([`ForgeRepo::collection`]) |
//! | `<g>.gitlab.io/<p>/…` (Pages) | the repository `<g>/<p>` |
//!
//! A numeric project id (`/api/v4/projects/123`) names no path and stays
//! its URL; so does a unique Pages domain (`<name>-<6 hex>.gitlab.io`).
//! Web and Pages URLs are read only; the API's op is the method's.
//!
//! [`ForgeRepo::thread`]: crate::extract::resource::repo::ForgeRepo::thread
//! [`ForgeRepo::collection`]: crate::extract::resource::repo::ForgeRepo::collection

use crosstalk_spec::derived::flow::access::AccessKind;
use crosstalk_spec::derived::flow::resource::Locator;

use crate::extract::http::HttpRequest;
use crate::extract::resource::repo::{ForgeStyle, ThreadKind};
use crate::extract::resource::{AbsolutePath, RepoId};

use super::SiteAccess;
use super::github::{method_kind, repository};

const HOST: &str = "gitlab.com";

/// First path segments of `gitlab.com` that are not a namespace.
const NOT_NAMESPACES: [&str; 8] = [
    "-",
    "api",
    "dashboard",
    "explore",
    "groups",
    "help",
    "users",
    "search",
];

pub fn apply(request: &HttpRequest) -> Option<SiteAccess> {
    let Locator::Url { host, path, .. } = &request.url else {
        return None;
    };
    let segments = super::segments(path);
    let segments: Vec<&str> = segments.iter().map(String::as_str).collect();
    let read_only = request.method.reads().then_some(AccessKind::Read);
    match (host.0.as_str(), segments.as_slice()) {
        ("gitlab.com" | "www.gitlab.com", ["api", "v4", "projects", project, rest @ ..]) => {
            let repo = project_path(project)?;
            api(&repo, rest, method_kind(request)?)
        }
        ("gitlab.com" | "www.gitlab.com", [first, ..]) if NOT_NAMESPACES.contains(first) => None,
        ("gitlab.com" | "www.gitlab.com", all) => {
            let (project, rest) = match all.iter().position(|segment| *segment == "-") {
                Some(at) => (&all[..at], &all[at + 1..]),
                None => (all, &[][..]),
            };
            let repo = RepoId::forge(HOST, &project.join("/"))?;
            web(&repo, rest, read_only?)
        }
        (pages, rest) => {
            let group = pages.strip_suffix(".gitlab.io")?;
            if group.is_empty() || group.contains('.') || unique_domain(group) {
                return None;
            }
            let [project, ..] = rest else {
                return None;
            };
            repository(HOST, group, project, read_only?)
        }
    }
}

/// `<name>-<6 hex>`: a unique Pages domain, which names no project path.
fn unique_domain(label: &str) -> bool {
    label
        .rsplit_once('-')
        .is_some_and(|(_, hex)| hex.len() == 6 && hex.bytes().all(|b| b.is_ascii_hexdigit()))
}

/// The project an API path's id names, when it is a URL-encoded path
/// (already decoded by the segment split); `None` for a numeric id.
fn project_path(project: &str) -> Option<RepoId> {
    if project.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    RepoId::forge(HOST, project)
}

fn web(repo: &RepoId, rest: &[&str], kind: AccessKind) -> Option<SiteAccess> {
    match rest {
        ["blob" | "raw", _ref, file @ ..] => file_access(repo, file, kind),
        [page @ ("issues" | "merge_requests"), number, ..] => thread(repo, page, number, kind),
        [page @ ("issues" | "merge_requests")] => collection(repo, page, kind),
        _ => whole(repo, kind),
    }
}

fn api(repo: &RepoId, rest: &[&str], kind: AccessKind) -> Option<SiteAccess> {
    match rest {
        ["repository", "files", file] | ["repository", "files", file, "raw"] => {
            file_access(repo, &[file], kind)
        }
        [page @ ("issues" | "merge_requests"), number, ..]
            if number.bytes().all(|b| b.is_ascii_digit()) =>
        {
            thread(repo, page, number, kind)
        }
        [page @ ("issues" | "merge_requests")] => collection(repo, page, kind),
        _ => whole(repo, kind),
    }
}

fn whole(repo: &RepoId, kind: AccessKind) -> Option<SiteAccess> {
    Some(SiteAccess {
        kind,
        locators: vec![repo.locator().clone()],
    })
}

fn file_access(repo: &RepoId, file: &[&str], kind: AccessKind) -> Option<SiteAccess> {
    if file.is_empty() {
        return None;
    }
    let path = AbsolutePath::parse(&format!("/{}", file.join("/"))).ok()?;
    Some(SiteAccess {
        kind,
        locators: vec![repo.file(&path)],
    })
}

fn thread_kind(page: &str) -> ThreadKind {
    if page == "issues" {
        ThreadKind::Issue
    } else {
        ThreadKind::Change
    }
}

fn thread(repo: &RepoId, page: &str, number: &str, kind: AccessKind) -> Option<SiteAccess> {
    let number: u64 = number.parse().ok()?;
    let forge = repo.forge_parts()?;
    Some(SiteAccess {
        kind,
        locators: vec![forge.thread(ForgeStyle::GitLab, thread_kind(page), number)],
    })
}

fn collection(repo: &RepoId, page: &str, kind: AccessKind) -> Option<SiteAccess> {
    let forge = repo.forge_parts()?;
    Some(SiteAccess {
        kind,
        locators: vec![forge.collection(ForgeStyle::GitLab, thread_kind(page))],
    })
}

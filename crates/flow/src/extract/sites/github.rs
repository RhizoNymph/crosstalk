//! GitHub: a repository file reached through the web, raw or contents API
//! URLs, as the repository's file (`RepoId::file`), the locator the same
//! file gets in any clone whose remote is known. The ref (branch, tag or
//! commit) is dropped: the resource is the file at that path, whose content
//! moves between agents across commits. A ref with a slash in it (`feature/x`)
//! cannot be told from the path in a `blob` URL; the first segment is taken
//! as the ref.

use crosstalk_spec::derived::flow::access::AccessKind;
use crosstalk_spec::derived::flow::resource::Locator;

use crate::extract::http::HttpRequest;
use crate::extract::resource::{AbsolutePath, RepoId};

use super::{SiteAccess, percent_decode};

pub fn apply(request: &HttpRequest) -> Option<SiteAccess> {
    let Locator::Url { host, path, .. } = &request.url else {
        return None;
    };
    let segments: Vec<String> = path
        .split('/')
        .filter(|segment| !segment.is_empty())
        .map(percent_decode)
        .collect();
    let segments: Vec<&str> = segments.iter().map(String::as_str).collect();
    let (owner, repo, file, kind) = match (host.0.as_str(), segments.as_slice()) {
        ("github.com" | "www.github.com", [owner, repo, "blob" | "raw", _ref, file @ ..]) => {
            (*owner, *repo, file, AccessKind::Read)
        }
        ("raw.githubusercontent.com", [owner, repo, "refs", "heads" | "tags", _ref, file @ ..])
        | ("raw.githubusercontent.com", [owner, repo, _ref, file @ ..]) => {
            (*owner, *repo, file, AccessKind::Read)
        }
        ("api.github.com", ["repos", owner, repo, "contents", file @ ..]) => {
            let kind = if request.method.writes() {
                AccessKind::Write
            } else {
                AccessKind::Read
            };
            (*owner, *repo, file, kind)
        }
        _ => return None,
    };
    if file.is_empty() {
        return None;
    }
    let repo = RepoId::forge("github.com", &format!("{owner}/{repo}"))?;
    let path = AbsolutePath::parse(&format!("/{}", file.join("/"))).ok()?;
    Some(SiteAccess {
        kind,
        locators: vec![repo.file(&path)],
    })
}

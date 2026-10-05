//! Canonical shared resources: one `Locator` per repository or site, however
//! an agent named it.
//!
//! Agents reach the same repository as a git remote, a web URL, a REST API
//! path, raw file URLs or its Pages site. [`from_url`] (and
//! [`from_remote`] for git remotes) maps every such form to one
//! `Locator::Url { scheme: "https", host: <forge>, path: "/<owner>/<repo>" }`
//! with a lower-cased path, so accesses through any of them meet on one
//! channel:
//!
//! | Form | Resource |
//! | --- | --- |
//! | `https://github.com/o/r(.git)(/…)`, `git@github.com:o/r.git`, `ssh://git@github.com/o/r` | `github.com/o/r` |
//! | `https://api.github.com/repos/o/r/…`, `https://raw.githubusercontent.com/o/r/…` | `github.com/o/r` |
//! | `https://o.github.io/r/…` | `github.com/o/r` (`github.com/o/o.github.io` for root files) |
//! | `https://gitlab.com/g/s/p(.git)(/-/…)` | `gitlab.com/g/s/p` |
//! | `https://gitlab.com/api/v4/projects/g%2Fs%2Fp/…` | `gitlab.com/g/s/p` |
//! | `https://gitlab.com/api/v4/projects/<id>/…` | `gitlab.com/api/v4/projects/<id>` (the id is all there is) |
//! | `https://g.gitlab.io/p/…` | `gitlab.com/g/p` |
//! | `https://<name>-<6 hex>.gitlab.io/…` (a unique Pages domain) | `https://<that host>/` (the site; its project is not in the name) |
//! | any other http(s) URL | the URL with its query and fragment dropped |
//!
//! Credentials in the authority (`https://oauth2:[REDACTED]@gitlab.com/…`)
//! and a leading `www.` are dropped; hosts are lower-cased.

use crosstalk_spec::derived::flow::resource::{Host, Locator};

/// A code forge with a canonical repository form.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Forge {
    GitHub,
    GitLab,
}

impl Forge {
    pub fn host(self) -> &'static str {
        match self {
            Self::GitHub => "github.com",
            Self::GitLab => "gitlab.com",
        }
    }
}

/// The repository `slug` (`owner/repo`, or a GitLab group path) on `forge`.
/// `None` when the slug has fewer than two segments.
pub fn repo(forge: Forge, slug: &str) -> Option<Locator> {
    let slug = slug.trim().trim_matches('/');
    let slug = slug.strip_suffix(".git").unwrap_or(slug);
    let segments: Vec<&str> = slug.split('/').filter(|s| !s.is_empty()).collect();
    if segments.len() < 2 || segments.iter().any(|s| !valid_segment(s)) {
        return None;
    }
    let segments = match forge {
        Forge::GitHub => &segments[..2],
        Forge::GitLab => &segments[..],
    };
    Some(site(
        forge.host(),
        &format!("/{}", segments.join("/").to_ascii_lowercase()),
    ))
}

fn valid_segment(segment: &str) -> bool {
    !segment.is_empty()
        && segment
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'))
}

fn site(host: &str, path: &str) -> Locator {
    Locator::Url {
        scheme: "https".to_owned(),
        host: Host(host.to_owned()),
        path: path.to_owned(),
        query: None,
    }
}

/// A git remote: an http(s) URL, `git@host:path` or `ssh://git@host/path`.
pub fn from_remote(text: &str) -> Option<Locator> {
    let text = text.trim();
    if let Some(rest) = text.strip_prefix("git@") {
        let (host, path) = rest.split_once(':')?;
        return forge_path(&host.to_ascii_lowercase(), path);
    }
    if let Some(rest) = text.strip_prefix("ssh://") {
        let rest = rest.split_once('@').map_or(rest, |(_, rest)| rest);
        let (host, path) = rest.split_once('/')?;
        let host = host.split(':').next().unwrap_or(host);
        return forge_path(&host.to_ascii_lowercase(), path);
    }
    from_url(text)
}

fn forge_path(host: &str, path: &str) -> Option<Locator> {
    match host {
        "github.com" => repo(Forge::GitHub, path),
        "gitlab.com" => repo(Forge::GitLab, path),
        _ => None,
    }
}

/// The canonical resource of an http(s) URL.
pub fn from_url(text: &str) -> Option<Locator> {
    let text = text
        .trim()
        .trim_end_matches(['.', ',', ')', ';', '\'', '"']);
    let (scheme, rest) = text.split_once("://")?;
    let scheme = scheme.to_ascii_lowercase();
    if scheme != "http" && scheme != "https" {
        return None;
    }
    let rest = rest.split(['#', '?']).next().unwrap_or(rest);
    let (authority, path) = match rest.find('/') {
        Some(at) => (&rest[..at], &rest[at..]),
        None => (rest, "/"),
    };
    let authority = authority
        .rsplit_once('@')
        .map_or(authority, |(_, host)| host);
    let host = authority.to_ascii_lowercase();
    let host = host.strip_prefix("www.").unwrap_or(&host);
    let host = match host.rsplit_once(':') {
        Some((name, "80" | "443")) => name,
        _ => host,
    };
    if host.is_empty() || host.chars().any(|c| c.is_whitespace() || c == '`') {
        return None;
    }
    let segments: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
    if let Some(locator) = forge_resource(host, &segments) {
        return Some(locator);
    }
    Some(Locator::Url {
        scheme,
        host: Host(host.to_owned()),
        path: crate::reference::route::normalize_path(path),
        query: None,
    })
}

fn forge_resource(host: &str, segments: &[&str]) -> Option<Locator> {
    match host {
        "github.com" => repo(Forge::GitHub, &segments.get(..2)?.join("/")),
        "raw.githubusercontent.com" => repo(Forge::GitHub, &segments.get(..2)?.join("/")),
        "api.github.com" => match segments {
            ["repos", owner, name, ..] => repo(Forge::GitHub, &format!("{owner}/{name}")),
            _ => None,
        },
        "gitlab.com" => gitlab(segments),
        _ => {
            if let Some(owner) = host.strip_suffix(".github.io") {
                return pages(Forge::GitHub, host, owner, segments);
            }
            if let Some(group) = host.strip_suffix(".gitlab.io") {
                if unique_pages_domain(group) {
                    return Some(site(host, "/"));
                }
                return pages(Forge::GitLab, host, group, segments);
            }
            None
        }
    }
}

fn gitlab(segments: &[&str]) -> Option<Locator> {
    match segments {
        ["api", "v4", "projects", project, ..] => {
            let decoded = project.replace("%2F", "/").replace("%2f", "/");
            if decoded.contains('/') {
                repo(Forge::GitLab, &decoded)
            } else {
                Some(site(
                    "gitlab.com",
                    &format!("/api/v4/projects/{}", project.to_ascii_lowercase()),
                ))
            }
        }
        _ => {
            let end = segments
                .iter()
                .position(|s| *s == "-")
                .unwrap_or(segments.len());
            repo(Forge::GitLab, &segments[..end].join("/"))
        }
    }
}

/// A Pages site of `owner` (a user or group): the project is the first path
/// segment, or the `<owner>.<pages host>` project for root files.
fn pages(forge: Forge, host: &str, owner: &str, segments: &[&str]) -> Option<Locator> {
    match segments.first() {
        Some(first) if !first.contains('.') => repo(forge, &format!("{owner}/{first}")),
        _ => repo(forge, &format!("{owner}/{host}")),
    }
}

/// GitLab's unique Pages domains: `<project>-<6 hex>`.
fn unique_pages_domain(name: &str) -> bool {
    name.rsplit_once('-').is_some_and(|(_, suffix)| {
        suffix.len() == 6 && suffix.chars().all(|c| c.is_ascii_hexdigit())
    })
}

/// Every http(s) URL in `text`, in order (for command lines and outputs).
pub fn urls(text: &str) -> Vec<&str> {
    let mut out = Vec::new();
    let mut from = 0;
    while let Some(at) = text[from..].find("http") {
        let start = from + at;
        let rest = &text[start..];
        if rest.starts_with("http://") || rest.starts_with("https://") {
            let end = rest
                .find(|c: char| {
                    c.is_whitespace() || matches!(c, '"' | '\'' | '<' | '>' | '`' | '|' | '\\')
                })
                .unwrap_or(rest.len());
            out.push(&rest[..end]);
            from = start + end.max(1);
        } else {
            from = start + 4;
        }
    }
    out
}

//! MediaWiki: every way to read or edit a page, as the page's canonical
//! article URL.
//!
//! A title is canonical as MediaWiki makes it: `_` and whitespace runs are
//! one space, surrounding space is dropped, a `#section` is dropped, and on
//! a `capital_links` site the first letter is upper-cased. The page's
//! locator is the article URL of that title (`/wiki/Dead_drop`), normalized
//! like any URL, on the request's scheme and host (the mobile host
//! `en.m.wikipedia.org` folded into `en.wikipedia.org` when the site says
//! so). Namespaces are not folded (`talk:` and `Talk:` stay apart).

use crosstalk_spec::derived::flow::access::AccessKind;
use crosstalk_spec::derived::flow::resource::Locator;

use crate::extract::http::HttpRequest;
use crate::extract::resource::url_locator;

use super::{MediaWikiSite, SiteAccess, percent_decode};

const REST_PAGE: &str = "rest.php/v1/page/";
const WIKIMEDIA_REST_PAGE: &str = "/api/rest_v1/page/";

/// The pages `request` reads or edits on `site`, `None` when it is not a
/// page request this rule knows.
pub fn apply(site: &MediaWikiSite, request: &HttpRequest) -> Option<SiteAccess> {
    let Locator::Url {
        scheme, host, path, ..
    } = &request.url
    else {
        return None;
    };
    let host = if site.fold_mobile_host {
        fold_mobile(&host.0)
    } else {
        host.0.clone()
    };
    let page = |title: &str| page_locator(site, scheme, &host, title);
    let params = request.params();
    let param = |name: &str| {
        params
            .iter()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value.as_str())
    };
    let page_kind = || match param("action") {
        Some("submit") => AccessKind::Write,
        Some("edit") if request.method.writes() => AccessKind::Write,
        _ => AccessKind::Read,
    };
    let access = |kind, locators: Vec<Locator>| Some(SiteAccess { kind, locators });

    if let Some(title) = path.strip_prefix(site.article_path.as_str()) {
        return access(page_kind(), vec![page(&percent_decode(title))?]);
    }
    if let Some(rest) = path.strip_prefix(WIKIMEDIA_REST_PAGE) {
        let (_endpoint, rest) = rest.split_once('/')?;
        let title = rest.split('/').next()?;
        return access(AccessKind::Read, vec![page(&percent_decode(title))?]);
    }
    let script = path.strip_prefix(site.script_path.as_str())?;
    if let Some(rest) = script.strip_prefix(REST_PAGE) {
        let title = rest.split('/').next()?;
        let kind = if request.method.writes() {
            AccessKind::Write
        } else {
            AccessKind::Read
        };
        return access(kind, vec![page(&percent_decode(title))?]);
    }
    match script {
        "index.php" => access(page_kind(), vec![page(param("title")?)?]),
        "api.php" => match param("action")? {
            "edit" | "delete" | "protect" | "undelete" => {
                access(AccessKind::Write, vec![page(param("title")?)?])
            }
            "move" => access(
                AccessKind::Write,
                vec![page(param("from")?)?, page(param("to")?)?],
            ),
            "query" => match param("titles") {
                Some(titles) => access(
                    AccessKind::Read,
                    titles.split('|').filter_map(page).collect(),
                ),
                None => access(AccessKind::Read, vec![request.url.clone()]),
            },
            "parse" => match param("page") {
                Some(title) => access(AccessKind::Read, vec![page(title)?]),
                None => access(AccessKind::Read, vec![request.url.clone()]),
            },
            _ if request.method.writes() => None,
            _ => access(AccessKind::Read, vec![request.url.clone()]),
        },
        _ => None,
    }
}

/// `raw` as MediaWiki canonicalizes a title, `None` when nothing is left.
pub fn canonical_title(raw: &str, capital_links: bool) -> Option<String> {
    let raw = raw.split('#').next().unwrap_or_default();
    let words: Vec<&str> = raw
        .split(|c: char| c == '_' || c.is_whitespace())
        .filter(|word| !word.is_empty())
        .collect();
    if words.is_empty() {
        return None;
    }
    let title = words.join(" ");
    if !capital_links {
        return Some(title);
    }
    let mut chars = title.chars();
    let first = chars.next()?;
    Some(first.to_uppercase().chain(chars).collect())
}

/// The canonical article URL of `title` on `site`.
pub fn page_locator(
    site: &MediaWikiSite,
    scheme: &str,
    host: &str,
    title: &str,
) -> Option<Locator> {
    let title = canonical_title(title, site.capital_links)?;
    let mut path = String::from(site.article_path.as_str());
    for c in title.chars() {
        match c {
            ' ' => path.push('_'),
            '%' => path.push_str("%25"),
            '?' => path.push_str("%3F"),
            '#' => path.push_str("%23"),
            '\\' => path.push_str("%5C"),
            c => path.push(c),
        }
    }
    url_locator(&format!("{scheme}://{host}{path}")).ok()
}

/// `en.m.wikipedia.org` as `en.wikipedia.org`: an `m` label after the
/// first is the mobile site.
fn fold_mobile(host: &str) -> String {
    let labels: Vec<&str> = host.split('.').collect();
    match labels.as_slice() {
        [first, "m", rest @ ..] if !rest.is_empty() => std::iter::once(*first)
            .chain(rest.iter().copied())
            .collect::<Vec<_>>()
            .join("."),
        ["m", rest @ ..] if rest.len() > 1 => rest.join("."),
        _ => host.to_owned(),
    }
}

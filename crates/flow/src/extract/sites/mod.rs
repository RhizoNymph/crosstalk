//! Site rules: the pages of known wikis and forges get one locator,
//! whatever URL or API reaches them.
//!
//! A MediaWiki page is read at `/wiki/Title`, `/w/index.php?title=Title`,
//! through `api.php` (`action=query&titles=…`, `action=parse&page=…`) or
//! the REST APIs, and edited through `api.php` (`action=edit`) or
//! `index.php` (`action=submit`), with the title in the query or the form
//! body. Every one of these names the page as its canonical article URL
//! ([`mediawiki`]), so an agent editing a page through the API and another
//! fetching its article URL meet on one resource.
//!
//! A GitHub file is read at its `blob`, `raw` or `raw.githubusercontent.com`
//! URL and read or written through the contents API; each names the file
//! as the repository's file (`RepoId::file`), the locator a clone's file
//! gets ([`github`]).
//!
//! Which hosts are MediaWiki sites is configured ([`SitesConfig`]); the
//! default covers the Wikimedia projects and Fandom.

pub mod github;
pub mod mediawiki;

use serde::{Deserialize, Serialize};

use crosstalk_spec::derived::flow::access::AccessKind;
use crosstalk_spec::derived::flow::resource::Locator;

use crate::extract::http::HttpRequest;

/// What a site rule decided for a request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SiteAccess {
    pub kind: AccessKind,
    pub locators: Vec<Locator>,
}

/// The site rules in force.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct SitesConfig {
    #[serde(default = "default_mediawiki")]
    pub mediawiki: Vec<MediaWikiSite>,
    /// Whether GitHub's URLs are read as repository files.
    #[serde(default = "enabled")]
    pub github: bool,
}

impl Default for SitesConfig {
    fn default() -> Self {
        Self {
            mediawiki: default_mediawiki(),
            github: true,
        }
    }
}

fn enabled() -> bool {
    true
}

/// MediaWiki sites, by host.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct MediaWikiSite {
    pub hosts: Vec<HostPattern>,
    /// `$wgArticlePath` without `$1`: `/wiki/`.
    #[serde(default = "article_path")]
    pub article_path: SitePath,
    /// `$wgScriptPath` with a trailing slash: `/w/`.
    #[serde(default = "script_path")]
    pub script_path: SitePath,
    /// `$wgCapitalLinks`: the first letter of a title is upper-cased.
    #[serde(default = "enabled")]
    pub capital_links: bool,
    /// The mobile host (`en.m.wikipedia.org`) is the same site as the
    /// desktop one (`en.wikipedia.org`).
    #[serde(default)]
    pub fold_mobile_host: bool,
}

fn article_path() -> SitePath {
    SitePath("/wiki/".to_owned())
}

fn script_path() -> SitePath {
    SitePath("/w/".to_owned())
}

fn default_mediawiki() -> Vec<MediaWikiSite> {
    let site =
        |hosts: &[&str], script: &str, capital_links: bool, fold_mobile_host: bool| MediaWikiSite {
            hosts: hosts
                .iter()
                .map(|host| HostPattern((*host).to_owned()))
                .collect(),
            article_path: article_path(),
            script_path: SitePath(script.to_owned()),
            capital_links,
            fold_mobile_host,
        };
    vec![
        site(
            &[
                "*.wikipedia.org",
                "*.wikimedia.org",
                "*.wikibooks.org",
                "*.wikiquote.org",
                "*.wikisource.org",
                "*.wikiversity.org",
                "*.wikivoyage.org",
                "*.wikinews.org",
                "*.wikidata.org",
                "*.mediawiki.org",
            ],
            "/w/",
            true,
            true,
        ),
        site(&["*.wiktionary.org"], "/w/", false, true),
        site(&["*.fandom.com"], "/", true, false),
    ]
}

impl SitesConfig {
    /// No site rules at all.
    pub fn none() -> Self {
        Self {
            mediawiki: Vec::new(),
            github: false,
        }
    }

    /// The first rule that recognizes `request`.
    pub fn apply(&self, request: &HttpRequest) -> Option<SiteAccess> {
        let Locator::Url { host, .. } = &request.url else {
            return None;
        };
        let name = host_name(&host.0);
        let wiki = self
            .mediawiki
            .iter()
            .filter(|site| site.hosts.iter().any(|pattern| pattern.matches(name)))
            .find_map(|site| mediawiki::apply(site, request));
        wiki.or_else(|| self.github.then(|| github::apply(request)).flatten())
    }
}

/// A host without its port.
fn host_name(host: &str) -> &str {
    if host.starts_with('[') {
        return host;
    }
    match host.rsplit_once(':') {
        Some((name, port)) if port.bytes().all(|b| b.is_ascii_digit()) => name,
        _ => host,
    }
}

/// A host, or `*.suffix` for the suffix and every subdomain of it. Lower
/// case. Decoded only through [`HostPattern::new`].
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct HostPattern(String);

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum InvalidSiteConfig {
    #[error("`{0}` is not a host or `*.suffix` in lower case")]
    Host(String),
    #[error("`{0}` is not a path starting and ending with `/`")]
    Path(String),
}

impl HostPattern {
    pub fn new(text: impl Into<String>) -> Result<Self, InvalidSiteConfig> {
        let text = text.into();
        let host = text.strip_prefix("*.").unwrap_or(&text);
        let valid = !host.is_empty()
            && host
                .chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || matches!(c, '.' | '-'));
        if valid {
            Ok(Self(text))
        } else {
            Err(InvalidSiteConfig::Host(text))
        }
    }

    pub fn matches(&self, host: &str) -> bool {
        match self.0.strip_prefix("*.") {
            Some(suffix) => {
                host == suffix
                    || host
                        .strip_suffix(suffix)
                        .is_some_and(|rest| rest.ends_with('.'))
            }
            None => host == self.0,
        }
    }
}

impl TryFrom<String> for HostPattern {
    type Error = InvalidSiteConfig;

    fn try_from(text: String) -> Result<Self, Self::Error> {
        Self::new(text)
    }
}

impl From<HostPattern> for String {
    fn from(pattern: HostPattern) -> Self {
        pattern.0
    }
}

/// A URL path prefix that starts and ends with `/`. Decoded only through
/// [`SitePath::new`].
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct SitePath(String);

impl SitePath {
    pub fn new(text: impl Into<String>) -> Result<Self, InvalidSiteConfig> {
        let text = text.into();
        if text.starts_with('/') && text.ends_with('/') {
            Ok(Self(text))
        } else {
            Err(InvalidSiteConfig::Path(text))
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for SitePath {
    type Error = InvalidSiteConfig;

    fn try_from(text: String) -> Result<Self, Self::Error> {
        Self::new(text)
    }
}

impl From<SitePath> for String {
    fn from(path: SitePath) -> Self {
        path.0
    }
}

/// `text` with every `%XX` escape decoded; an undecodable result keeps the
/// text as it was.
pub(crate) fn percent_decode(text: &str) -> String {
    let bytes = text.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        let hex = |at: usize| bytes.get(at).and_then(|b| char::from(*b).to_digit(16));
        match (bytes[index], hex(index + 1), hex(index + 2)) {
            (b'%', Some(high), Some(low)) => {
                // Two hex digits are at most 255.
                out.push((high * 16 + low) as u8);
                index += 3;
            }
            (byte, _, _) => {
                out.push(byte);
                index += 1;
            }
        }
    }
    String::from_utf8(out).unwrap_or_else(|_| text.to_owned())
}

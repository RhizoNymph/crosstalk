//! The public URL of a wiki page, the channel resource agents read and
//! write through.
//!
//! Each wiki the export names is hosted somewhere public; a page's canonical
//! resource is the `https` URL a browser (or an agent's `GET`) would
//! fetch. The same string goes into both synthetic `http_request` calls'
//! `url` argument (`GET` to read, `POST` to write), so an L5 extractor
//! parses the identical locator the label expects. The URL is built, then
//! run through the reference matcher's own `parse_url`
//! (`crate::reference::route`), so the label's [`Locator`] is byte-for-byte
//! what the matcher extracts.

use crosstalk_spec::derived::flow::resource::Locator;

use crate::reference::route::parse_url;

/// The `https` URL of page `name` on `wiki`.
///
/// Hosts follow the export's own site list (`site-coverage.csv`): the `dse`,
/// `fractal` and `wiki4d` wikis live under `prowiki.org`, the others under
/// `wikiservice.at`, except `dorfwiki` on `dorfwiki.org`.
pub fn page_url(wiki: &str, name: &str) -> String {
    let (host, prefix) = match wiki {
        "dse" => ("www.prowiki.org", "/dse"),
        "fractal" => ("www.prowiki.org", "/fractal"),
        "wiki4d" => ("www.prowiki.org", "/wiki4d"),
        "dorfwiki" => ("www.dorfwiki.org", ""),
        other => {
            // wikiservice.at hosts named wikis under /<wiki>/.
            return format!("https://www.wikiservice.at/{other}/{}", escape(name));
        }
    };
    format!("https://{host}{prefix}/{}", escape(name))
}

/// The canonical [`Locator`] of page `name` on `wiki`: the reference
/// matcher's parse of [`page_url`], so a label and a prediction name one
/// resource.
pub fn page_locator(wiki: &str, name: &str) -> Option<Locator> {
    parse_url(&page_url(wiki, name))
}

/// Percent-encode the characters a wiki page name may hold that would
/// otherwise break the path or the URL parse (spaces and `#`, `?`, `%`).
fn escape(name: &str) -> String {
    let mut out = String::with_capacity(name.len());
    for ch in name.chars() {
        match ch {
            ' ' => out.push_str("%20"),
            '#' => out.push_str("%23"),
            '?' => out.push_str("%3F"),
            '%' => out.push_str("%25"),
            other => out.push(other),
        }
    }
    out
}

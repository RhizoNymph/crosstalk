//! Fetch tools: one read of the URL argument (`Structured`), or of every
//! URL in a free-text argument (`Scanned`) for a tool that takes a prompt.

use crosstalk_spec::derived::flow::access::Extraction;

use crate::extract::args::{ArgError, Args};
use crate::extract::catalog::FetchTool;
use crate::extract::http::{self, HttpRequest};
use crate::extract::op::Candidate;
use crate::extract::resource::{scan_urls, url_locator};
use crate::extract::sites::SitesConfig;

/// A fetch is a `GET` whose body reaches the result, so site rules name
/// the page or file it reads.
pub(crate) fn candidates(
    tool: &FetchTool,
    args: &Args,
    sites: &SitesConfig,
) -> Result<Vec<Candidate>, ArgError> {
    let get = |locator, via| http::candidates(&HttpRequest::get(locator), true, via, sites);
    if let Some(key) = tool.url_key
        && let Some(url) = args.opt_str(key)?
    {
        let locator = url_locator(url).map_err(|error| ArgError::invalid(key, error))?;
        return Ok(get(locator, Extraction::Structured));
    }
    if let Some(key) = tool.scan_key
        && let Some(text) = args.opt_str(key)?
    {
        return Ok(scan_urls(text)
            .into_iter()
            .filter_map(|url| url_locator(url).ok())
            .flat_map(|locator| get(locator, Extraction::Scanned))
            .collect());
    }
    let keys: Vec<&str> = tool.url_key.into_iter().chain(tool.scan_key).collect();
    Err(ArgError::Missing(keys.join(" or ")))
}

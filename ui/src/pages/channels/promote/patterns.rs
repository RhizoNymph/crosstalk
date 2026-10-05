//! The pattern builder: patterns derived from a discovered channel's seed
//! locator. What each would cover is the backend's `promotion_preview`.
//!
//! Candidates run from most to least specific. Every candidate covers the
//! seed, which `PromoteChannel` requires, so the operator picks a scope
//! rather than writing a pattern.

use crosstalk_spec::derived::flow::resource::{Locator, ResourcePattern};

/// Patterns that cover `seed`, most specific first.
pub fn candidates(seed: &Locator) -> Vec<ResourcePattern> {
    let mut out = vec![ResourcePattern::Exact(seed.clone())];
    match seed {
        Locator::Url { host, path, .. } => {
            out.extend(segment_prefixes(path).into_iter().rev().map(|path_prefix| {
                ResourcePattern::UrlPrefix {
                    host: host.clone(),
                    path_prefix,
                }
            }));
            out.push(ResourcePattern::Host(host.clone()));
        }
        Locator::File { host, path } => {
            out.extend(segment_prefixes(path).into_iter().rev().map(|prefix| {
                ResourcePattern::PathPrefix {
                    host: host.clone(),
                    prefix,
                }
            }));
        }
        Locator::Mcp { server, .. } => out.push(ResourcePattern::McpServer(server.clone())),
        Locator::Opaque { .. } => {}
    }
    out.retain(|pattern| pattern.matches(seed));
    out
}

/// Every whole-segment prefix of an absolute path, shortest first, without
/// the root: `/a/b/c` gives `/a`, `/a/b`, `/a/b/c`.
pub fn segment_prefixes(path: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut prefix = String::new();
    for segment in path.split('/').filter(|s| !s.is_empty()) {
        prefix.push('/');
        prefix.push_str(segment);
        out.push(prefix.clone());
    }
    out
}

/// The candidate at a query index, if there is one.
pub fn pick(candidates: &[ResourcePattern], index: Option<&str>) -> Result<Option<usize>, String> {
    let Some(text) = index else {
        return Ok(None);
    };
    let index: usize = text
        .parse()
        .map_err(|_| format!("{text:?} is not a pattern number"))?;
    if index < candidates.len() {
        Ok(Some(index))
    } else {
        Err(format!("there is no pattern {index}"))
    }
}

#[cfg(test)]
mod tests {
    use crosstalk_spec::derived::flow::resource::Host;
    use crosstalk_spec::observed::message::ToolName;

    use super::*;

    fn url(path: &str) -> Locator {
        Locator::Url {
            scheme: "https".into(),
            host: Host("wiki.example.org".into()),
            path: path.into(),
            query: Some("rev=3".into()),
        }
    }

    #[test]
    fn prefixes_follow_whole_segments() {
        assert_eq!(segment_prefixes("/a/b/c"), ["/a", "/a/b", "/a/b/c"]);
        assert_eq!(segment_prefixes("/a//b/"), ["/a", "/a/b"]);
        assert!(segment_prefixes("/").is_empty());
    }

    #[test]
    fn url_seeds_offer_exact_path_prefixes_then_host() {
        let seed = url("/team/agents");
        let patterns = candidates(&seed);
        let host = Host("wiki.example.org".into());
        assert_eq!(
            patterns,
            vec![
                ResourcePattern::Exact(seed.clone()),
                ResourcePattern::UrlPrefix {
                    host: host.clone(),
                    path_prefix: "/team/agents".into()
                },
                ResourcePattern::UrlPrefix {
                    host: host.clone(),
                    path_prefix: "/team".into()
                },
                ResourcePattern::Host(host),
            ]
        );
        assert!(patterns.iter().all(|p| p.matches(&seed)));
    }

    #[test]
    fn file_and_mcp_seeds() {
        let file = Locator::File {
            host: None,
            path: "/srv/shared/notes.md".into(),
        };
        assert_eq!(candidates(&file).len(), 4);
        let mcp = Locator::Mcp {
            server: "notion".into(),
            tool: ToolName("read".into()),
            target: None,
        };
        assert_eq!(
            candidates(&mcp).last(),
            Some(&ResourcePattern::McpServer("notion".into()))
        );
        let opaque = Locator::Opaque {
            tool: ToolName("bash".into()),
            key: "x".into(),
        };
        assert_eq!(candidates(&opaque).len(), 1);
    }

    #[test]
    fn picks_are_bounded_indexes() {
        let patterns = candidates(&url("/a"));
        assert_eq!(pick(&patterns, None), Ok(None));
        assert_eq!(pick(&patterns, Some("1")), Ok(Some(1)));
        assert!(pick(&patterns, Some("9")).is_err());
        assert!(pick(&patterns, Some("-1")).is_err());
    }
}

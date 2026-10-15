//! Pattern overlap: two declared patterns overlap exactly when some
//! locator matches both, so declaring and promoting can refuse overlaps.

use crate::derived::flow::resource::ResourcePattern;
use crate::tests::channels::{file, host, mcp, url};

#[test]
fn pattern_overlap_cases() {
    let url_prefix = |h: &str, p: &str| ResourcePattern::UrlPrefix {
        host: host(h),
        path_prefix: p.into(),
    };
    let path_prefix = |h: Option<&str>, p: &str| ResourcePattern::PathPrefix {
        host: h.map(host),
        prefix: p.into(),
    };
    let cases = [
        (
            ResourcePattern::Host(host("a")),
            ResourcePattern::Host(host("a")),
            true,
        ),
        (
            ResourcePattern::Host(host("a")),
            ResourcePattern::Host(host("b")),
            false,
        ),
        (
            ResourcePattern::Host(host("a")),
            url_prefix("a", "/x"),
            true,
        ),
        (
            ResourcePattern::Host(host("a")),
            url_prefix("b", "/x"),
            false,
        ),
        (url_prefix("a", "/x"), url_prefix("a", "/x/y"), true),
        (url_prefix("a", "/x"), url_prefix("a", "/xy"), false),
        (url_prefix("a", "/x/"), url_prefix("a", "/x"), true),
        (url_prefix("a", "/x"), url_prefix("b", "/x"), false),
        (
            path_prefix(None, "/srv"),
            path_prefix(None, "/srv/shared"),
            true,
        ),
        (path_prefix(None, "/srv"), path_prefix(None, "/srv2"), false),
        (
            path_prefix(None, "/srv"),
            path_prefix(Some("h"), "/srv"),
            false,
        ),
        (
            ResourcePattern::McpServer("wiki".into()),
            ResourcePattern::McpServer("wiki".into()),
            true,
        ),
        (
            ResourcePattern::McpServer("wiki".into()),
            ResourcePattern::McpServer("mail".into()),
            false,
        ),
        (
            ResourcePattern::Exact(url("a", "/x/1")),
            url_prefix("a", "/x"),
            true,
        ),
        (
            ResourcePattern::Exact(url("a", "/y/1")),
            url_prefix("a", "/x"),
            false,
        ),
        (
            ResourcePattern::Exact(url("a", "/x")),
            ResourcePattern::Exact(url("a", "/x")),
            true,
        ),
        (
            ResourcePattern::Exact(url("a", "/x")),
            ResourcePattern::Exact(url("a", "/y")),
            false,
        ),
        (
            ResourcePattern::Host(host("a")),
            path_prefix(Some("a"), "/"),
            false,
        ),
        (
            url_prefix("a", "/"),
            ResourcePattern::McpServer("a".into()),
            false,
        ),
        (
            path_prefix(None, "/"),
            ResourcePattern::McpServer("a".into()),
            false,
        ),
    ];
    for (left, right, expected) in cases {
        assert_eq!(left.overlaps(&right), expected, "{left:?} vs {right:?}");
        assert_eq!(right.overlaps(&left), expected, "{right:?} vs {left:?}");
    }
}

#[test]
fn overlap_is_implied_by_a_shared_match() {
    let patterns = [
        ResourcePattern::Host(host("a")),
        ResourcePattern::UrlPrefix {
            host: host("a"),
            path_prefix: "/x".into(),
        },
        ResourcePattern::UrlPrefix {
            host: host("a"),
            path_prefix: "/x/y".into(),
        },
        ResourcePattern::UrlPrefix {
            host: host("a"),
            path_prefix: "/xy".into(),
        },
        ResourcePattern::PathPrefix {
            host: None,
            prefix: "/srv".into(),
        },
        ResourcePattern::McpServer("wiki".into()),
        ResourcePattern::Exact(url("a", "/x/y/z")),
        ResourcePattern::Exact(file(None, "/srv/a")),
    ];
    let locators = [
        url("a", "/x"),
        url("a", "/x/y"),
        url("a", "/x/y/z"),
        url("a", "/xy"),
        url("b", "/x"),
        file(None, "/srv/a"),
        file(Some("h"), "/srv/a"),
        mcp("wiki"),
        mcp("mail"),
    ];
    for left in &patterns {
        for right in &patterns {
            let shared = locators
                .iter()
                .any(|locator| left.matches(locator) && right.matches(locator));
            if shared {
                assert!(left.overlaps(right), "{left:?} vs {right:?}");
            }
        }
    }
}

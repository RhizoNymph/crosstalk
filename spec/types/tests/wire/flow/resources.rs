//! Resources, their locators and patterns, and accesses.

use super::super::harness::{assert_golden, assert_rejected, assert_request_golden};
use super::{AREA, ULID_F, page, read_access, wiki_locator, write_access};
use crate::derived::flow::access::{Access, AccessKind, AccessOp, Extraction};
use crate::derived::flow::resource::{Host, Locator, Resource, ResourcePattern};
use crate::observed::message::ToolName;
use crate::tests::wire::ts;

/// One locator of every variant.
fn every_locator() -> Vec<Locator> {
    fn declared(locator: Locator) -> Locator {
        match locator {
            Locator::Url { .. }
            | Locator::File { .. }
            | Locator::Mcp { .. }
            | Locator::Opaque { .. } => locator,
        }
    }
    [
        Locator::Url {
            scheme: "https".into(),
            host: Host("wiki.internal.example".into()),
            path: "/projects/crosstalk/plan".into(),
            query: Some("rev=12&view=raw".into()),
        },
        Locator::File {
            host: Some(Host("build-01".into())),
            path: "/srv/shared/handoff.md".into(),
        },
        Locator::Mcp {
            server: "linear".into(),
            tool: ToolName("get_issue".into()),
            target: Some("ENG-1423".into()),
        },
        Locator::Opaque {
            tool: ToolName("kv_put".into()),
            key: "handoff/latest".into(),
        },
    ]
    .into_iter()
    .map(declared)
    .collect()
}

#[test]
fn resources_and_locators_golden() {
    assert_golden(AREA, "locators", &every_locator());
    let resource = Resource {
        id: page(),
        locator: wiki_locator("/projects/crosstalk/plan"),
        first_seen: ts("2026-10-04T11:58:12.000000Z"),
    };
    assert_golden(AREA, "resource", &resource);
}

/// One pattern of every variant: a client sends each with a promotion
/// (`PromoteChannel`) or a promotion preview.
fn every_pattern() -> Vec<(&'static str, ResourcePattern)> {
    fn declared(pattern: ResourcePattern) -> ResourcePattern {
        match pattern {
            ResourcePattern::Exact(_)
            | ResourcePattern::Host(_)
            | ResourcePattern::UrlPrefix { .. }
            | ResourcePattern::PathPrefix { .. }
            | ResourcePattern::McpServer(_) => pattern,
        }
    }
    [
        (
            "resource_pattern_exact",
            ResourcePattern::Exact(wiki_locator("/projects/crosstalk/plan")),
        ),
        (
            "resource_pattern_host",
            ResourcePattern::Host(Host("wiki.internal.example".into())),
        ),
        (
            "resource_pattern_url_prefix",
            ResourcePattern::UrlPrefix {
                host: Host("wiki.internal.example".into()),
                path_prefix: "/projects/crosstalk".into(),
            },
        ),
        (
            "resource_pattern_path_prefix",
            ResourcePattern::PathPrefix {
                host: None,
                prefix: "/srv/shared".into(),
            },
        ),
        (
            "resource_pattern_mcp_server",
            ResourcePattern::McpServer("linear".into()),
        ),
    ]
    .into_iter()
    .map(|(name, pattern)| (name, declared(pattern)))
    .collect()
}

#[test]
fn resource_patterns_are_request_goldens() {
    for (name, pattern) in every_pattern() {
        assert_request_golden(AREA, name, &pattern);
    }
}

#[test]
fn accesses_golden() {
    assert_golden(AREA, "access_write", &write_access());
    assert_golden(AREA, "access_read", &read_access());
    fn kind(kind: AccessKind) -> AccessKind {
        match kind {
            AccessKind::Write | AccessKind::Read => kind,
        }
    }
    assert_golden(
        AREA,
        "access_kinds",
        &[AccessKind::Write, AccessKind::Read].map(kind).to_vec(),
    );
    fn via(via: Extraction) -> Extraction {
        match via {
            Extraction::Scanned | Extraction::Parsed | Extraction::Structured => via,
        }
    }
    assert_golden(
        AREA,
        "extractions",
        &[
            Extraction::Scanned,
            Extraction::Parsed,
            Extraction::Structured,
        ]
        .map(via)
        .to_vec(),
    );
}

#[test]
fn resources_refuse_unknown_fields_and_variants() {
    assert_rejected::<Locator>(
        r#"{"type": "socket", "data": {"path": "/tmp/s"}}"#,
        "unknown variant `socket`",
    );
    assert_rejected::<Locator>(
        r#"{"type": "file", "data": {"host": null, "path": "/a", "mode": "rw"}}"#,
        "unknown field `mode`",
    );
    assert_rejected::<ResourcePattern>(
        r#"{"type": "glob", "data": "/srv/*"}"#,
        "unknown variant `glob`",
    );
    assert_rejected::<ResourcePattern>(
        r#"{"type": "url_prefix", "data": {"host": "wiki", "path_prefix": "/a", "exact": true}}"#,
        "unknown field `exact`",
    );
    assert_rejected::<ResourcePattern>(
        r#"{"type": "host", "data": {"name": "wiki"}}"#,
        "invalid type",
    );
    assert_rejected::<Resource>(
        &format!(
            r#"{{"id": "{ULID_F}", "locator": {{"type": "opaque", "data": {{"tool": "kv", "key": "k"}}}},
                "first_seen": "2026-10-04T11:58:12.000000Z", "last_seen": null}}"#
        ),
        "unknown field `last_seen`",
    );
}

#[test]
fn accesses_refuse_unknown_fields_and_variants() {
    let read = serde_json::to_value(read_access()).expect("an access encodes");
    let mut extra = read.clone();
    extra["confidence"] = serde_json::json!(0.9);
    assert_rejected::<Access>(&extra.to_string(), "unknown field `confidence`");
    let mut op = read;
    op["op"] = serde_json::json!({"type": "delete", "data": {"result": null}});
    assert_rejected::<Access>(&op.to_string(), "unknown variant `delete`");
    assert_rejected::<AccessOp>(
        r#"{"type": "read", "data": {"result": {"message": "af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262", "index": 2}, "spans": []}}"#,
        "unknown field `spans`",
    );
    assert_rejected::<AccessKind>(r#""append""#, "unknown variant `append`");
    assert_rejected::<Extraction>(r#""guessed""#, "unknown variant `guessed`");
}

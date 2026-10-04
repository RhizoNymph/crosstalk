//! Configured MCP tools: the M2 wiki server, a fetch server and a
//! filesystem server (`wiki.json`), and the configuration's checks.

use serde_json::json;

use crosstalk_spec::derived::flow::access::Extraction::Structured;
use crosstalk_spec::derived::flow::resource::Locator;
use crosstalk_spec::interfaces::l5_flow::{ExtractError, ResourceExtractor};
use crosstalk_spec::observed::message::ToolName;

use super::support::*;
use crate::extract::catalog::mcp_name;
use crate::extract::{
    ArgPath, Classified, ConfigError, ExtractConfig, KeyCanon, McpAccessRule, McpResource,
    McpServerConfig, McpToolRule, RuleOp, ToolExtractors, WriteOutcome,
};

use WriteOutcome::{Delivered, Unknown};

fn wiki(
    name: &str,
    args: serde_json::Value,
    result: Option<&str>,
) -> Result<Vec<Classified>, ExtractError> {
    let result = result.map(ok);
    extract(
        &wiki_config(),
        &context(),
        &call(name, args),
        result.as_ref(),
    )
}

#[test]
fn wiki_tools() {
    let cases: Vec<(&str, serde_json::Value, Option<&str>, Vec<Classified>)> = vec![
        (
            "mcp__wiki__write_page",
            json!({ "title": "Release Notes", "content": "Ship on Friday." }),
            Some("Saved page Release Notes (revision 3)."),
            vec![write(page("release notes"), Delivered, Structured)],
        ),
        (
            "mcp__wiki__write_page",
            json!({ "title": "Release Notes", "content": "Ship on Friday." }),
            None,
            vec![write(page("release notes"), Unknown, Structured)],
        ),
        (
            "mcp__wiki__read_page",
            json!({ "title": "  release_notes " }),
            Some("# Release Notes\nShip on Friday."),
            vec![read(page("release notes"), Structured)],
        ),
        (
            "mcp__wiki__read_page",
            json!({ "title": "Release Notes" }),
            None,
            vec![],
        ),
        (
            "mcp__team-wiki__read_page",
            json!({ "title": "/Projects//Atlas/" }),
            Some("..."),
            vec![read(page("projects/atlas"), Structured)],
        ),
        (
            "mcp__wiki__edit_page",
            json!({ "page": { "title": "Projects/Atlas" }, "find": "a", "replace": "b" }),
            Some("Edited."),
            vec![write(page("projects/atlas"), Delivered, Structured)],
        ),
        (
            "mcp__wiki__list_pages",
            json!({}),
            Some("Release Notes\nProjects/Atlas"),
            vec![read(
                Locator::Mcp {
                    server: "wiki".to_owned(),
                    tool: ToolName("page".to_owned()),
                    target: None,
                },
                Structured,
            )],
        ),
        (
            "mcp__wiki__move_page",
            json!({ "from": "Draft", "to": "Final" }),
            Some("Moved."),
            vec![
                write(page("draft"), Delivered, Structured),
                write(page("final"), Delivered, Structured),
            ],
        ),
        (
            "mcp__fetch__fetch",
            json!({ "url": "https://Example.com/a/../b" }),
            Some("<html>"),
            vec![read(https("example.com", "/b"), Structured)],
        ),
        (
            "mcp__filesystem__write_file",
            json!({ "path": "shared/plan.md", "content": "x" }),
            Some("Successfully wrote to shared/plan.md"),
            vec![write(
                file("/home/alice/project/shared/plan.md"),
                Delivered,
                Structured,
            )],
        ),
        (
            "mcp__filesystem__read_text_file",
            json!({ "path": "/home/alice/project/shared/plan.md" }),
            Some("x"),
            vec![read(file("/home/alice/project/shared/plan.md"), Structured)],
        ),
        // A tool the configuration does not map, on a mapped server.
        (
            "mcp__wiki__delete_page",
            json!({ "title": "x" }),
            Some("ok"),
            vec![],
        ),
        // A server the configuration does not know.
        (
            "mcp__linear__get_issue",
            json!({ "id": "ENG-1" }),
            Some("{}"),
            vec![],
        ),
    ];
    for (name, args, result, expected) in cases {
        assert_eq!(
            wiki(name, args.clone(), result),
            Ok(expected),
            "{name} {args}"
        );
    }
}

#[test]
fn two_agents_meet_on_one_wiki_page() {
    let config = wiki_config();
    let alice = context_in("/home/alice/project");
    let bob = context_in("/home/bob/work");
    let written = ToolExtractors::new(&config, &alice)
        .extract_classified(
            &call(
                "mcp__wiki__write_page",
                json!({ "title": "Hand-off Notes", "content": "x" }),
            ),
            Some(&ok("Saved.")),
        )
        .expect("extracts");
    let read = ToolExtractors::new(&config, &bob)
        .extract_classified(
            &call(
                "mcp__wiki_local__read_page",
                json!({ "title": "hand-off_notes" }),
            ),
            Some(&ok("x")),
        )
        .expect("extracts");
    assert_eq!(written.len(), 1);
    assert_eq!(read.len(), 1);
    assert_eq!(written[0].locator, read[0].locator);
}

#[test]
fn wiki_argument_errors() {
    for (name, args) in [
        ("mcp__wiki__read_page", json!({})),
        ("mcp__wiki__read_page", json!({ "title": "   " })),
        ("mcp__wiki__read_page", json!({ "title": ["a"] })),
        ("mcp__wiki__edit_page", json!({ "page": "Home" })),
        ("mcp__fetch__fetch", json!({ "url": "nope" })),
        ("mcp__filesystem__write_file", json!({ "path": "" })),
    ] {
        assert!(
            matches!(
                wiki(name, args.clone(), Some("ok")),
                Err(ExtractError::Arguments { .. })
            ),
            "{name} {args}",
        );
    }
}

#[test]
fn numeric_targets_key_by_their_text() {
    let config = ExtractConfig::new(vec![server(
        "tracker",
        vec![],
        vec![tool("get_issue", RuleOp::Read, keyed("issue", "/id"))],
    )])
    .expect("valid");
    let got = extract(
        &config,
        &context(),
        &call("mcp__tracker__get_issue", json!({ "id": 1423 })),
        Some(&ok("{}")),
    );
    assert_eq!(
        got,
        Ok(vec![read(
            Locator::Mcp {
                server: "tracker".to_owned(),
                tool: ToolName("issue".to_owned()),
                target: Some("1423".to_owned()),
            },
            Structured,
        )])
    );
}

#[test]
fn mcp_names_split_at_the_first_separator() {
    assert_eq!(
        mcp_name("mcp__wiki__read_page"),
        Some(("wiki", "read_page"))
    );
    assert_eq!(
        mcp_name("mcp__team-wiki__read__page"),
        Some(("team-wiki", "read__page"))
    );
    assert_eq!(mcp_name("mcp__wiki"), None);
    assert_eq!(mcp_name("mcp____x"), None);
    assert_eq!(mcp_name("Read"), None);
}

fn keyed(collection: &str, target: &str) -> McpResource {
    McpResource::Keyed {
        collection: collection.to_owned(),
        target: Some(ArgPath::new(target).expect("valid")),
        canon: KeyCanon::default(),
    }
}

fn tool(name: &str, op: RuleOp, resource: McpResource) -> McpToolRule {
    McpToolRule {
        tool: name.to_owned(),
        accesses: vec![McpAccessRule { op, resource }],
        refusal: vec![],
    }
}

fn server(name: &str, aliases: Vec<&str>, tools: Vec<McpToolRule>) -> McpServerConfig {
    McpServerConfig {
        server: name.to_owned(),
        aliases: aliases.into_iter().map(str::to_owned).collect(),
        tools,
    }
}

#[test]
fn configuration_checks() {
    let page = || keyed("page", "/title");
    let read_page = || tool("read_page", RuleOp::Read, page());
    assert_eq!(
        ExtractConfig::new(vec![
            server("wiki", vec![], vec![read_page()]),
            server("wiki", vec![], vec![])
        ]),
        Err(ConfigError::DuplicateServer("wiki".to_owned())),
    );
    assert_eq!(
        ExtractConfig::new(vec![
            server("a", vec!["b"], vec![]),
            server("b", vec![], vec![])
        ]),
        Err(ConfigError::DuplicateServer("b".to_owned())),
    );
    assert_eq!(
        ExtractConfig::new(vec![server("a", vec!["a"], vec![])]),
        Err(ConfigError::DuplicateServer("a".to_owned())),
    );
    assert_eq!(
        ExtractConfig::new(vec![server("wiki", vec![], vec![read_page(), read_page()])]),
        Err(ConfigError::DuplicateTool {
            server: "wiki".to_owned(),
            tool: "read_page".to_owned(),
        }),
    );
    let mut empty = read_page();
    empty.accesses.clear();
    assert_eq!(
        ExtractConfig::new(vec![server("wiki", vec![], vec![empty])]),
        Err(ConfigError::NoAccess {
            server: "wiki".to_owned(),
            tool: "read_page".to_owned(),
        }),
    );
    assert_eq!(
        ExtractConfig::new(vec![server("", vec![], vec![])]),
        Err(ConfigError::EmptyName),
    );
    assert_eq!(
        ExtractConfig::new(vec![server(
            "w",
            vec![],
            vec![tool("t", RuleOp::Read, keyed("", "/x"))]
        )]),
        Err(ConfigError::EmptyName),
    );
    let mut marked = read_page();
    marked.refusal = vec![crate::extract::RefusalMarker::Contains(String::new())];
    assert!(matches!(
        ExtractConfig::new(vec![server("wiki", vec![], vec![marked])]),
        Err(ConfigError::EmptyMarker { .. })
    ));
}

#[test]
fn configuration_json_is_strict() {
    assert_eq!(ExtractConfig::from_json("{}"), Ok(ExtractConfig::default()));
    for text in [
        r#"{"mcp_servers": [], "extra": 1}"#,
        r#"{"mcp_servers": [{"server": "w", "tools": [], "color": "red"}]}"#,
        r#"{"mcp_servers": [{"server": "w", "tools": [{"tool": "t", "accesses": [{"op": "delete", "resource": {"type": "url", "data": {"arg": "/u"}}}]}]}]}"#,
        r#"{"mcp_servers": [{"server": "w", "tools": [{"tool": "t", "accesses": [{"op": "read", "resource": {"type": "url", "data": {"arg": "u"}}}]}]}]}"#,
        r#"{"mcp_servers": [{"server": "w", "tools": [{"tool": "t", "accesses": [{"op": "read", "resource": {"type": "url", "data": {"arg": "/a~2"}}}]}]}]}"#,
        r#"{"mcp_servers": [{"server": "w", "tools": [{"tool": "t", "accesses": []}]}]}"#,
    ] {
        assert!(ExtractConfig::from_json(text).is_err(), "{text}");
    }
    let config = wiki_config();
    let text = serde_json::to_string(&config).expect("serializes");
    assert_eq!(ExtractConfig::from_json(&text), Ok(config), "round trip");
}

#[test]
fn arg_paths_are_json_pointers() {
    assert!(ArgPath::new("/title").is_ok());
    assert!(ArgPath::new("/a~1b/c~0d").is_ok());
    assert!(ArgPath::new("title").is_err());
    assert!(ArgPath::new("/a~").is_err());
    let config = ExtractConfig::new(vec![server(
        "s",
        vec![],
        vec![tool("t", RuleOp::Write, keyed("doc", "/a~1b/c~0d"))],
    )])
    .expect("valid");
    let got = extract(
        &config,
        &context(),
        &call("mcp__s__t", json!({ "a/b": { "c~d": "Key" } })),
        None,
    );
    assert_eq!(
        got,
        Ok(vec![write(
            Locator::Mcp {
                server: "s".to_owned(),
                tool: ToolName("doc".to_owned()),
                target: Some("Key".to_owned()),
            },
            Unknown,
            Structured,
        )])
    );
}

#[test]
fn handles_only_configured_mcp_tools() {
    let config = wiki_config();
    let context = context();
    let extractors = ToolExtractors::new(&config, &context);
    assert!(extractors.handles(&call("mcp__wiki__read_page", json!({}))));
    assert!(extractors.handles(&call("mcp__team-wiki__write_page", json!({}))));
    assert!(!extractors.handles(&call("mcp__wiki__delete_page", json!({}))));
    assert!(!extractors.handles(&call("mcp__other__read_page", json!({}))));
}

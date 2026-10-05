//! Generators of realistic tool calls and results, for the properties and
//! the fuzz test.

use proptest::prelude::*;
use serde_json::{Value, json};

use crosstalk_spec::observed::message::{ToolCall, ToolOutcome, ToolResult};

use super::support::{call, result};

const SEGMENTS: [&str; 8] = [
    "home", "alice", "shared", "notes.md", ".", "..", "src", "a b",
];

/// A path as an agent might write it: absolute, relative, home-relative or
/// junk.
pub fn path() -> impl Strategy<Value = String> {
    let segments = prop::collection::vec(prop::sample::select(&SEGMENTS[..]), 0..5);
    prop_oneof![
        segments.clone().prop_map(|s| format!("/{}", s.join("/"))),
        segments.clone().prop_map(|s| s.join("/")),
        segments.prop_map(|s| format!("~/{}", s.join("/"))),
        "\\PC{0,16}",
    ]
}

/// A URL, well formed or not.
pub fn url() -> impl Strategy<Value = String> {
    let host = prop::sample::select(
        &[
            "Example.com",
            "en.wikipedia.org",
            "en.m.wikipedia.org",
            "github.com",
            "raw.githubusercontent.com",
            "api.github.com",
            "x.fandom.com",
        ][..],
    );
    let path = prop::sample::select(
        &[
            "/",
            "/wiki/dead_drop",
            "/w/api.php",
            "/w/index.php",
            "/a/b/blob/main/c.py",
            "/repos/a/b/contents/c",
            "/a/../b",
            "",
        ][..],
    );
    let query = prop::sample::select(
        &[
            "",
            "?action=edit&title=X",
            "?titles=A|b_c&action=query",
            "?b=2&a=1",
            "#frag",
        ][..],
    );
    prop_oneof![
        (
            prop::sample::select(&["http", "https", "HTTPS"][..]),
            host,
            path,
            query
        )
            .prop_map(|(scheme, host, path, query)| format!("{scheme}://{host}{path}{query}")),
        "\\PC{0,24}",
    ]
}

/// A shell command an agent might run, with a path and a URL in it.
pub fn command() -> impl Strategy<Value = String> {
    let template = prop::sample::select(
        &[
            "cat {p}",
            "echo x > {p}",
            "head -n 3 {p} | tee {p}",
            "cd {p} && cat {p}",
            "curl -s {u}",
            "curl -X POST -d 'action=edit&title=T' {u}",
            "wget -qO- {u}",
            "git clone {u} {p}",
            "git show HEAD:{p}",
            "cat <<EOF > {p}\nbody\nEOF",
            "{p} {u} 'unterminated",
        ][..],
    );
    prop_oneof![
        (template, path(), url())
            .prop_map(|(template, path, url)| template.replace("{p}", &path).replace("{u}", &url)),
        "\\PC{0,40}",
    ]
}

/// A wiki page title.
pub fn title() -> impl Strategy<Value = String> {
    prop_oneof![
        prop::sample::select(&["Home", "release_notes", "  Release Notes ", "a/b", "", "_"][..])
            .prop_map(str::to_owned),
        "\\PC{0,12}",
    ]
}

/// A call of a known tool, with arguments shaped like its schema.
pub fn known_call() -> impl Strategy<Value = ToolCall> {
    prop_oneof![
        path().prop_map(|p| call("Read", json!({ "file_path": p }))),
        path().prop_map(|p| call("Write", json!({ "file_path": p, "content": "x" }))),
        path().prop_map(|p| call(
            "Edit",
            json!({ "file_path": p, "old_string": "a", "new_string": "b" })
        )),
        path().prop_map(|p| call("read", json!({ "path": p }))),
        path().prop_map(|p| call(
            "str_replace_editor",
            json!({ "command": "create", "path": p })
        )),
        url().prop_map(|u| call("WebFetch", json!({ "url": u, "prompt": "x" }))),
        url().prop_map(|u| call("web_fetch", json!({ "prompt": format!("see {u}") }))),
        command().prop_map(|c| call("Bash", json!({ "command": c }))),
        (command(), path()).prop_map(|(c, p)| call(
            "shell",
            json!({ "command": ["bash", "-lc", c], "workdir": p })
        )),
        title().prop_map(|t| call("mcp__wiki__read_page", json!({ "title": t }))),
        title().prop_map(|t| call(
            "mcp__wiki__write_page",
            json!({ "title": t, "content": "x" })
        )),
        title().prop_map(|t| call(
            "mcp__team-wiki__edit_page",
            json!({ "page": { "title": t } })
        )),
        (title(), title())
            .prop_map(|(a, b)| call("mcp__wiki__move_page", json!({ "from": a, "to": b }))),
        url().prop_map(|u| call("mcp__fetch__fetch", json!({ "url": u }))),
        (
            prop::sample::select(&["http_request", "fetch", "web_fetch", "curl"][..]),
            prop::sample::select(&["GET", "head", "POST", "put", "PATCH", "DELETE", "OPTIONS"][..]),
            url(),
            prop::option::of(json_value()),
        )
            .prop_map(|(name, method, url, body)| match body {
                Some(body) => call(name, json!({ "method": method, "url": url, "body": body })),
                None => call(name, json!({ "method": method, "url": url })),
            }),
        path().prop_map(|p| call("mcp__filesystem__write_file", json!({ "path": p }))),
    ]
}

/// Arbitrary JSON, nested a little.
pub fn json_value() -> impl Strategy<Value = Value> {
    let leaf = prop_oneof![
        Just(Value::Null),
        any::<bool>().prop_map(Value::Bool),
        any::<i64>().prop_map(Value::from),
        "\\PC{0,24}".prop_map(Value::String),
    ];
    leaf.prop_recursive(3, 24, 4, |inner| {
        prop_oneof![
            prop::collection::vec(inner.clone(), 0..4).prop_map(Value::Array),
            prop::collection::btree_map(
                prop::sample::select(
                    &[
                        "file_path",
                        "path",
                        "url",
                        "command",
                        "title",
                        "prompt",
                        "cmd",
                        "workdir",
                        "page",
                        "from",
                        "to",
                        "x",
                    ][..]
                ),
                inner,
                0..4,
            )
            .prop_map(|map| Value::Object(
                map.into_iter().map(|(k, v)| (k.to_owned(), v)).collect()
            )),
        ]
    })
}

/// A result's text: success texts, each tool's refusals, and junk.
pub fn result_text() -> impl Strategy<Value = String> {
    prop_oneof![
        prop::sample::select(
            &[
                "File created successfully at: /x",
                "<tool_use_error>File has not been read yet.</tool_use_error>",
                "  <tool_use_error>x</tool_use_error>",
                "The user doesn't want to proceed with this tool use. The tool use was rejected.",
                "Error: page title is reserved",
                "Saved page (revision 3).",
                "Rejected: content exceeds the maximum page length of 4096 bytes",
                "origin\thttps://github.com/a/b.git (fetch)",
                "Shell cwd was reset to /home/alice",
                "",
            ][..]
        )
        .prop_map(str::to_owned),
        "\\PC{0,40}",
    ]
}

pub fn tool_result() -> impl Strategy<Value = Option<ToolResult>> {
    prop::option::of(
        (
            prop::sample::select(&[ToolOutcome::Success, ToolOutcome::Error][..]),
            result_text(),
        )
            .prop_map(|(outcome, text)| result(outcome, &text)),
    )
}

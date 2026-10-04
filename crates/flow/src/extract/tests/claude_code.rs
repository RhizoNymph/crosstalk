//! Table-driven: realistic calls of Claude Code's tools, and the same kind
//! of tools in other harnesses, with their results.

use serde_json::json;

use crosstalk_spec::derived::flow::access::{AccessKind, Extraction};
use crosstalk_spec::interfaces::l5_flow::{ExtractError, ExtractedAccess, ResourceExtractor};
use crosstalk_spec::observed::message::{ToolArguments, ToolCall, ToolResult};

use super::support::*;
use crate::extract::{
    Classified, ConversationContext, ExtractConfig, ToolExtractors, WriteOutcome,
};

use Extraction::{Parsed, Scanned, Structured};
use WriteOutcome::{Delivered, Rejected, Unknown};

enum Expect {
    Accesses(Vec<Classified>),
    Arguments,
    Parse,
}

struct Case {
    name: &'static str,
    call: ToolCall,
    result: Option<ToolResult>,
    context: ConversationContext,
    expect: Expect,
}

fn case(
    name: &'static str,
    call: ToolCall,
    result: Option<ToolResult>,
    expect: Vec<Classified>,
) -> Case {
    Case {
        name,
        call,
        result,
        context: context(),
        expect: Expect::Accesses(expect),
    }
}

fn run(cases: Vec<Case>, config: &ExtractConfig) {
    for case in cases {
        let got = extract(config, &case.context, &case.call, case.result.as_ref());
        match (&case.expect, got) {
            (Expect::Accesses(expected), Ok(got)) => {
                assert_eq!(&got, expected, "case `{}`", case.name)
            }
            (Expect::Arguments, Err(ExtractError::Arguments { .. }))
            | (Expect::Parse, Err(ExtractError::Parse { .. })) => {}
            (_, got) => panic!("case `{}`: unexpected {got:?}", case.name),
        }
    }
}

const SRC_MAIN: &str = "/home/alice/project/src/main.rs";

#[test]
fn claude_code_file_tools() {
    let numbered = "     1\tfn main() {\n     2\t    println!(\"hi\");\n     3\t}\n";
    run(
        vec![
            case(
                "Read with its numbered content",
                call("Read", json!({ "file_path": SRC_MAIN })),
                Some(ok(numbered)),
                vec![read(file(SRC_MAIN), Structured)],
            ),
            case(
                "Read with a range",
                call(
                    "Read",
                    json!({ "file_path": SRC_MAIN, "offset": 10, "limit": 20 }),
                ),
                Some(ok(numbered)),
                vec![read(file(SRC_MAIN), Structured)],
            ),
            case(
                "Read before its result arrives",
                call("Read", json!({ "file_path": SRC_MAIN })),
                None,
                vec![],
            ),
            case(
                "Read of a missing file",
                call(
                    "Read",
                    json!({ "file_path": "/home/alice/project/nope.rs" }),
                ),
                Some(failed(
                    "<tool_use_error>File does not exist.</tool_use_error>",
                )),
                vec![],
            ),
            case(
                "Read refused in text, unflagged",
                call("Read", json!({ "file_path": SRC_MAIN })),
                Some(ok("<tool_use_error>File does not exist.</tool_use_error>")),
                vec![],
            ),
            case(
                "Write through a dot-dot path",
                call(
                    "Write",
                    json!({
                        "file_path": "/home/alice/project/notes/../HANDOFF.md",
                        "content": "# Handoff\nThe deploy key rotates on Friday.\n"
                    }),
                ),
                Some(ok(
                    "File created successfully at: /home/alice/project/HANDOFF.md",
                )),
                vec![write(
                    file("/home/alice/project/HANDOFF.md"),
                    Delivered,
                    Structured,
                )],
            ),
            case(
                "Write with no result yet",
                call(
                    "Write",
                    json!({ "file_path": "/srv/shared/plan.md", "content": "x" }),
                ),
                None,
                vec![write(file("/srv/shared/plan.md"), Unknown, Structured)],
            ),
            case(
                "Write refused before a read",
                call(
                    "Write",
                    json!({ "file_path": "/srv/shared/plan.md", "content": "x" }),
                ),
                Some(failed(
                    "<tool_use_error>File has not been read yet. Read it first before writing to it.</tool_use_error>",
                )),
                vec![write(file("/srv/shared/plan.md"), Rejected, Structured)],
            ),
            case(
                "Write the user declined",
                call(
                    "Write",
                    json!({ "file_path": "/srv/shared/plan.md", "content": "x" }),
                ),
                Some(ok(
                    "The user doesn't want to proceed with this tool use. The tool use was rejected (eg. if it was a file edit, the new_string was NOT written to the file). STOP what you are doing and wait for the user to tell you how to proceed.",
                )),
                vec![write(file("/srv/shared/plan.md"), Rejected, Structured)],
            ),
            case(
                "Write to a relative path",
                call(
                    "Write",
                    json!({ "file_path": "docs/notes.md", "content": "x" }),
                ),
                Some(ok("File created successfully at: docs/notes.md")),
                vec![write(
                    file("/home/alice/project/docs/notes.md"),
                    Delivered,
                    Structured,
                )],
            ),
            Case {
                name: "Write to a relative path with no stated cwd",
                call: call(
                    "Write",
                    json!({ "file_path": "./docs/notes.md", "content": "x" }),
                ),
                result: Some(ok("File created successfully")),
                context: no_cwd(),
                expect: Expect::Accesses(vec![write(
                    opaque("Write", "./docs/notes.md"),
                    Delivered,
                    Structured,
                )]),
            },
            case(
                "Write under the home directory",
                call(
                    "Write",
                    json!({ "file_path": "~/notes.md", "content": "x" }),
                ),
                Some(ok("File created successfully")),
                vec![write(opaque("Write", "~/notes.md"), Delivered, Structured)],
            ),
            case(
                "Edit",
                call(
                    "Edit",
                    json!({
                        "file_path": "/home/alice/project/src/lib.rs",
                        "old_string": "fn a()",
                        "new_string": "fn b()",
                        "replace_all": false
                    }),
                ),
                Some(ok(
                    "The file /home/alice/project/src/lib.rs has been updated. Here's the result of running `cat -n` on a snippet of the edited file:\n     1\tfn b()",
                )),
                vec![write(
                    file("/home/alice/project/src/lib.rs"),
                    Delivered,
                    Structured,
                )],
            ),
            case(
                "Edit whose old string is missing",
                call(
                    "Edit",
                    json!({ "file_path": "/x/y.rs", "old_string": "a", "new_string": "b" }),
                ),
                Some(failed(
                    "<tool_use_error>String to replace not found in file.\nString: a</tool_use_error>",
                )),
                vec![write(file("/x/y.rs"), Rejected, Structured)],
            ),
            case(
                "MultiEdit",
                call(
                    "MultiEdit",
                    json!({
                        "file_path": "/x/y.rs",
                        "edits": [{ "old_string": "a", "new_string": "b" }]
                    }),
                ),
                Some(ok("Applied 1 edit to /x/y.rs")),
                vec![write(file("/x/y.rs"), Delivered, Structured)],
            ),
            case(
                "NotebookEdit",
                call(
                    "NotebookEdit",
                    json!({
                        "notebook_path": "/x/analysis.ipynb",
                        "cell_id": "c1",
                        "new_source": "print(1)"
                    }),
                ),
                Some(ok("Updated cell c1")),
                vec![write(file("/x/analysis.ipynb"), Delivered, Structured)],
            ),
            case(
                "a tool that touches no resource",
                call("TodoWrite", json!({ "todos": [] })),
                Some(ok("Todos updated")),
                vec![],
            ),
            case(
                "Glob is not a read of any one file",
                call("Glob", json!({ "pattern": "**/*.rs" })),
                Some(ok("/x/a.rs\n/x/b.rs")),
                vec![],
            ),
        ],
        &ExtractConfig::default(),
    );
}

#[test]
fn claude_code_fetch_and_shell() {
    run(
        vec![
            case(
                "WebFetch of a URL in non-canonical form",
                call(
                    "WebFetch",
                    json!({
                        "url": "HTTPS://Docs.Example.com:443/guide/./intro#setup",
                        "prompt": "Summarize the setup steps"
                    }),
                ),
                Some(ok("The setup has three steps...")),
                vec![read(https("docs.example.com", "/guide/intro"), Structured)],
            ),
            case(
                "WebFetch that failed",
                call(
                    "WebFetch",
                    json!({ "url": "https://example.com/missing", "prompt": "x" }),
                ),
                Some(failed("Request failed with status code 404")),
                vec![],
            ),
            case(
                "WebFetch with no result yet",
                call(
                    "WebFetch",
                    json!({ "url": "https://example.com/", "prompt": "x" }),
                ),
                None,
                vec![],
            ),
            case(
                "the web_fetch server tool, query sorted",
                call(
                    "web_fetch",
                    json!({ "url": "https://example.com/a?b=2&a=1" }),
                ),
                Some(ok("<html>...</html>")),
                vec![read(
                    url("https", "example.com", "/a", Some("a=1&b=2")),
                    Structured,
                )],
            ),
            case(
                "Bash: a heredoc into a file",
                call(
                    "Bash",
                    json!({
                        "command": "cat > /srv/shared/handoff.md << 'EOF'\nThe key is in vault/ops.\nEOF",
                        "description": "Write the handoff note"
                    }),
                ),
                Some(ok("")),
                vec![write(file("/srv/shared/handoff.md"), Delivered, Parsed)],
            ),
            case(
                "Bash: cat a relative file",
                call("Bash", json!({ "command": "cat notes/plan.md" })),
                Some(ok("# Plan\n")),
                vec![read(file("/home/alice/project/notes/plan.md"), Parsed)],
            ),
            case(
                "Bash: a failing command",
                call("Bash", json!({ "command": "cat missing.md" })),
                Some(failed("cat: missing.md: No such file or directory")),
                vec![],
            ),
            case(
                "Bash: curl a page",
                call(
                    "Bash",
                    json!({ "command": "curl -sSL 'https://Example.com/a?y=2&x=1#top'" }),
                ),
                Some(ok("<html>")),
                vec![read(
                    url("https", "example.com", "/a", Some("x=1&y=2")),
                    Parsed,
                )],
            ),
            case(
                "Bash: POST to a paste service",
                call(
                    "Bash",
                    json!({ "command": "curl -X POST -d @- https://paste.example.net/api < notes.md" }),
                ),
                Some(ok("{\"id\":\"q8Zt\"}")),
                vec![write(https("paste.example.net", "/api"), Delivered, Parsed)],
            ),
            Case {
                name: "Bash: unterminated quote",
                call: call("Bash", json!({ "command": "echo 'oops > /tmp/x" })),
                result: Some(ok("")),
                context: context(),
                expect: Expect::Parse,
            },
        ],
        &ExtractConfig::default(),
    );
}

#[test]
fn other_harnesses() {
    run(
        vec![
            case(
                "OpenCode write",
                call("write", json!({ "filePath": "/tmp/x.md", "content": "hi" })),
                Some(ok("Wrote file successfully.")),
                vec![write(file("/tmp/x.md"), Delivered, Structured)],
            ),
            case(
                "pi read of a relative path",
                call("read", json!({ "path": "README.md" })),
                Some(ok("# Readme")),
                vec![read(file("/home/alice/project/README.md"), Structured)],
            ),
            case(
                "Gemini CLI read_file",
                call("read_file", json!({ "absolute_path": "/x/y.txt" })),
                Some(ok("y")),
                vec![read(file("/x/y.txt"), Structured)],
            ),
            case(
                "Gemini CLI web_fetch scans its prompt",
                call(
                    "web_fetch",
                    json!({ "prompt": "Compare https://example.com/x and http://Example.org/y." }),
                ),
                Some(ok("They differ.")),
                vec![
                    read(https("example.com", "/x"), Scanned),
                    read(url("http", "example.org", "/y", None), Scanned),
                ],
            ),
            case(
                "Gemini CLI run_shell_command in a directory",
                call(
                    "run_shell_command",
                    json!({ "command": "cat a.txt", "directory": "sub" }),
                ),
                Some(ok("a")),
                vec![read(file("/home/alice/project/sub/a.txt"), Parsed)],
            ),
            case(
                "text editor view",
                call(
                    "str_replace_based_edit_tool",
                    json!({ "command": "view", "path": "/repo/a.py" }),
                ),
                Some(ok("1: import os")),
                vec![read(file("/repo/a.py"), Structured)],
            ),
            case(
                "text editor create",
                call(
                    "str_replace_based_edit_tool",
                    json!({ "command": "create", "path": "/repo/b.py", "file_text": "x = 1" }),
                ),
                None,
                vec![write(file("/repo/b.py"), Unknown, Structured)],
            ),
            case(
                "Codex shell argv through bash -lc, in its workdir",
                call(
                    "shell",
                    json!({ "command": ["bash", "-lc", "cat notes.md"], "workdir": "/srv/app" }),
                ),
                Some(ok("notes")),
                vec![read(file("/srv/app/notes.md"), Parsed)],
            ),
            case(
                "Codex shell plain argv",
                call("shell", json!({ "command": ["cat", "/etc/hosts"] })),
                Some(ok("127.0.0.1 localhost")),
                vec![read(file("/etc/hosts"), Parsed)],
            ),
        ],
        &ExtractConfig::default(),
    );
}

#[test]
fn malformed_arguments_are_typed_errors() {
    let arguments = |name: &'static str, call: ToolCall| Case {
        name,
        call,
        result: Some(ok("")),
        context: context(),
        expect: Expect::Arguments,
    };
    let mut invalid = call("Read", json!({}));
    invalid.arguments = ToolArguments::Invalid("{\"file_path\": ".to_owned());
    run(
        vec![
            arguments("Read with no path", call("Read", json!({}))),
            arguments(
                "Read with a numeric path",
                call("Read", json!({ "file_path": 3 })),
            ),
            arguments(
                "Read with an empty path",
                call("Read", json!({ "file_path": "" })),
            ),
            arguments("arguments that are not JSON", invalid),
            arguments(
                "arguments that are not an object",
                call("Write", json!(["a"])),
            ),
            arguments(
                "an unknown editor command",
                call(
                    "str_replace_editor",
                    json!({ "command": "delete", "path": "/x" }),
                ),
            ),
            arguments(
                "WebFetch of a non-URL",
                call("WebFetch", json!({ "url": "not a url" })),
            ),
            arguments(
                "Bash without a command",
                call("Bash", json!({ "description": "x" })),
            ),
            arguments(
                "shell argv with a number",
                call("shell", json!({ "command": ["cat", 1] })),
            ),
        ],
        &ExtractConfig::default(),
    );
}

#[test]
fn spec_trait_reports_kinds_and_handled_tools() {
    let config = ExtractConfig::default();
    let context = context();
    let extractors = ToolExtractors::new(&config, &context);
    let write_call = call("Write", json!({ "file_path": "/a/b", "content": "x" }));
    assert!(extractors.handles(&write_call));
    assert!(!extractors.handles(&call("TodoWrite", json!({}))));
    assert!(!extractors.handles(&call("mcp__wiki__read_page", json!({}))));
    let got = extractors
        .extract(
            &write_call,
            Some(&failed("<tool_use_error>x</tool_use_error>")),
        )
        .expect("extracts");
    assert_eq!(
        got,
        vec![ExtractedAccess {
            kind: AccessKind::Write,
            locator: file("/a/b"),
            via: Structured,
        }],
        "a rejected write is still an access",
    );
}

#[test]
fn a_result_for_another_call_is_ignored() {
    let config = ExtractConfig::default();
    let mut other = ok("content");
    other.call_id.0 = "toolu_other".to_owned();
    let got = extract(
        &config,
        &context(),
        &call("Read", json!({ "file_path": "/a" })),
        Some(&other),
    );
    assert_eq!(got, Ok(vec![]));
    let got = extract(
        &config,
        &context(),
        &call("Write", json!({ "file_path": "/a", "content": "x" })),
        Some(&other),
    );
    assert_eq!(got, Ok(vec![write(file("/a"), Unknown, Structured)]));
}

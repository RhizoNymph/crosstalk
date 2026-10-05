//! The tool calls that touch the shared page, as Claude Code makes them.
//!
//! The page is a file on the team wiki's shared mount, written with
//! Claude Code's `Write {file_path, content}` and read with
//! `Read {file_path}`. That is the resource whose write and read the spec
//! pins down best (`FileToolExtractor`): both calls name one absolute path,
//! so they give one `Locator::File` and one resource, needing no working
//! directory. An MCP wiki tool would not do: an `Mcp` locator includes the
//! tool, so a `write_page` and a `read_page` on the same page are two
//! resources and never co-access.

use serde_json::{Value, json};

use super::SENTENCE;
use super::wire::ToolSpec;

/// The page both agents touch: an absolute path on the shared wiki mount.
pub const WIKI_PAGE: &str = "/srv/team-wiki/runbooks/ledger-rollback.md";

/// What Claude Code's `Write` tool answers once the file is written.
pub const WRITE_RESULT: &str =
    "File created successfully at: /srv/team-wiki/runbooks/ledger-rollback.md";

/// The tools every scenario request declares.
pub fn declared() -> Vec<ToolSpec> {
    vec![
        ToolSpec {
            name: "Read",
            description: "Reads a file from the local filesystem.",
            fields: &["file_path"],
        },
        ToolSpec {
            name: "Write",
            description: "Writes a file to the local filesystem.",
            fields: &["file_path", "content"],
        },
        ToolSpec {
            name: "Bash",
            description: "Executes a given bash command and returns its output.",
            fields: &["command", "description"],
        },
    ]
}

/// The page's lines, as A writes them. [`SENTENCE`] is a line of its own,
/// so it survives the `Read` tool's line numbering intact.
fn page_lines() -> [&'static str; 9] {
    [
        "# Ledger service rollback",
        "",
        "Owner: ledger team. Applies to every region.",
        "",
        SENTENCE,
        "",
        "1. Announce the rollback in #ledger-ops.",
        "2. Deploy the previous release tag.",
        "3. Watch settlement lag until it is back under a minute.",
    ]
}

/// A's `Write` call: id, tool name, input.
pub fn write_call() -> (String, String, Value) {
    let mut content = page_lines().join("\n");
    content.push('\n');
    (
        "toolu_01E2EWriteLedgerRunbook01".to_owned(),
        "Write".to_owned(),
        json!({ "file_path": WIKI_PAGE, "content": content }),
    )
}

/// B's `Read` call: id, tool name, input.
pub fn read_call() -> (String, String, Value) {
    (
        "toolu_01E2EReadLedgerRunbook002".to_owned(),
        "Read".to_owned(),
        json!({ "file_path": WIKI_PAGE }),
    )
}

/// The page as Claude Code's `Read` tool returns it: each line numbered,
/// `cat -n` style.
pub fn page_as_read() -> String {
    page_lines()
        .iter()
        .enumerate()
        .map(|(index, line)| format!("{:>6}\t{line}\n", index + 1))
        .collect()
}

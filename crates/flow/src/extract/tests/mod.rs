//! The extractors' tests: table-driven over realistic calls per harness and
//! tool family (`claude_code`, `bash`, `mcp`, `sites`, `repos`), the write
//! spans (`spans`), and here the invariants' evidence.

mod bash;
mod claude_code;
mod fetch_config;
pub(crate) mod generate;
mod http;
mod mcp;
mod repos;
mod sites;
mod spans;
pub(crate) mod support;

use proptest::prelude::*;
use serde_json::json;

use crosstalk_spec::derived::flow::access::Extraction::Structured;
use crosstalk_spec::derived::provenance::span::Span;
use crosstalk_spec::ids::SpanId;
use crosstalk_spec::observed::message::{ToolCall, ToolOutcome, ToolResult};

use crate::extract::outcome::result_text;
use crate::extract::{ExtractConfig, ExtractedOp, RefusalMarker, WriteOutcome, write_spans};

use support::{call, context, extract, ok, page, wiki_config, write};

/// What the eval spec PR's rule says a write's outcome is, restated
/// independently of `outcome.rs` for the tools `generate::known_call` makes.
fn expected_outcome(call: &ToolCall, result: Option<&ToolResult>) -> WriteOutcome {
    let Some(result) = result else {
        return WriteOutcome::Unknown;
    };
    if result.outcome == ToolOutcome::Error {
        return WriteOutcome::Rejected;
    }
    let text = result_text(result);
    let trimmed = text.trim_start();
    let refused = match call.name.0.as_str() {
        "Read" | "Write" | "Edit" | "read" | "str_replace_editor" => {
            trimmed.starts_with("<tool_use_error>")
                || trimmed.starts_with("The user doesn't want to proceed with this tool use")
        }
        "mcp__wiki__write_page" => {
            trimmed.starts_with("Error:") || text.contains("exceeds the maximum page length")
        }
        "mcp__team-wiki__edit_page" => trimmed.starts_with("Error:"),
        _ => false,
    };
    if refused {
        WriteOutcome::Rejected
    } else {
        WriteOutcome::Delivered
    }
}

proptest! {
    /// `flow.extract.read-requires-result`.
    #[test]
    fn no_read_without_result(call in generate::known_call()) {
        let config = wiki_config();
        if let Ok(accesses) = extract(&config, &context(), &call, None) {
            prop_assert!(accesses.iter().all(|access| access.op != ExtractedOp::Read));
        }
    }

    /// `flow.extract.write-outcome-classified` (eval spec PR).
    #[test]
    fn write_outcome_follows_result(call in generate::known_call(), result in generate::tool_result()) {
        let config = wiki_config();
        if let Ok(accesses) = extract(&config, &context(), &call, result.as_ref()) {
            let expected = expected_outcome(&call, result.as_ref());
            for access in accesses {
                if let ExtractedOp::Write(outcome) = access.op {
                    prop_assert_eq!(outcome, expected, "{:?} {:?}", call, result);
                }
            }
        }
    }

    /// `flow.access.write-spans-include-self-relay` (eval spec PR) and
    /// `flow.access.write-spans-include-forwarded-input`: the originated
    /// and input-relayed spans in the call's part, and the writer's own
    /// sources of the spans relayed there, each once.
    #[test]
    fn write_spans_include_self_relayed_sources(
        drafts in prop::collection::vec(spans::span_draft(), 0..12),
    ) {
        let spans: Vec<Span> = drafts.iter().map(spans::DraftSpan::span).collect();
        let got = write_spans(spans::call_part(), spans::writer(), &spans, spans::agent_of);
        let mut expected: Vec<SpanId> = Vec::new();
        for draft in &drafts {
            if !draft.in_call || !draft.by_writer {
                continue;
            }
            let carried = match draft.kind {
                spans::Kind::Originated | spans::Kind::Indexed | spans::Kind::RelayedInput => {
                    Some(draft.id())
                }
                spans::Kind::RelayedSpan { source, source_by_writer: true } => Some(source),
                _ => None,
            };
            if let Some(id) = carried && !expected.contains(&id) {
                expected.push(id);
            }
        }
        prop_assert_eq!(got, expected);
    }
}

/// `flow.extract.write-outcome-classified` (eval spec PR): a message tool
/// that reports refusal only in its text.
#[test]
fn message_tool_refusal_text_is_rejected() {
    let wiki = |name: &str, args, text: &str| {
        extract(
            &wiki_config(),
            &context(),
            &call(name, args),
            Some(&ok(text)),
        )
    };
    for text in [
        "Error: page title is reserved",
        "  Error: the wiki is read-only",
        "Rejected: content exceeds the maximum page length of 4096 bytes",
    ] {
        assert_eq!(
            wiki(
                "mcp__wiki__write_page",
                json!({ "title": "Home", "content": "x" }),
                text
            ),
            Ok(vec![write(
                page("home"),
                WriteOutcome::Rejected,
                Structured
            )]),
            "{text}",
        );
    }
    assert_eq!(
        wiki(
            "mcp__wiki__write_page",
            json!({ "title": "Home", "content": "x" }),
            "Saved."
        ),
        Ok(vec![write(
            page("home"),
            WriteOutcome::Delivered,
            Structured
        )]),
    );
    // A tool without refusal markers keeps the wire's word.
    assert_eq!(
        wiki(
            "mcp__wiki__move_page",
            json!({ "from": "a", "to": "b" }),
            "Error: no such page"
        ),
        Ok(vec![
            write(page("a"), WriteOutcome::Delivered, Structured),
            write(page("b"), WriteOutcome::Delivered, Structured),
        ]),
    );
    // A read whose result is a refusal delivered no content.
    let mut servers = wiki_config().servers().to_vec();
    servers[0].tools[0].refusal = vec![RefusalMarker::Prefix("Error:".to_owned())];
    let config = ExtractConfig::new(servers).expect("valid");
    assert_eq!(
        extract(
            &config,
            &context(),
            &call("mcp__wiki__read_page", json!({ "title": "Home" })),
            Some(&ok("Error: no such page")),
        ),
        Ok(vec![]),
    );
}

/// A flagged error rejects a write whatever the text says; the write is
/// still an access (`flow.access.rejected-write-recorded`, eval spec PR).
#[test]
fn rejected_writes_are_still_extracted() {
    let got = extract(
        &ExtractConfig::default(),
        &context(),
        &call("Write", json!({ "file_path": "/a", "content": "x" })),
        Some(&support::failed("File created successfully")),
    );
    assert_eq!(
        got,
        Ok(vec![write(
            support::file("/a"),
            WriteOutcome::Rejected,
            Structured
        )])
    );
    assert!(!WriteOutcome::Rejected.pairs());
    assert!(WriteOutcome::Delivered.pairs() && WriteOutcome::Unknown.pairs());
}

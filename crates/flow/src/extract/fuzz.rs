//! `flow.extract.no-panic`: extraction and context learning over arbitrary
//! tool names, arguments and results return accesses or a typed error, and
//! never panic. Property-based fuzzing over model-shaped and arbitrary
//! input.

use proptest::prelude::*;

use crosstalk_spec::observed::message::{
    CanonicalJson, ToolArguments, ToolCall, ToolCallId, ToolExecution, ToolName,
};

use crate::extract::bash::lex::Script;
use crate::extract::tests::generate;
use crate::extract::tests::support::{context, no_cwd, wiki_config};
use crate::extract::{ConversationContext, ToolExtractors};

fn tool_name() -> impl Strategy<Value = String> {
    prop_oneof![
        prop::sample::select(
            &[
                "Read",
                "Write",
                "Edit",
                "MultiEdit",
                "NotebookEdit",
                "WebFetch",
                "web_fetch",
                "Bash",
                "bash",
                "shell",
                "exec_command",
                "run_shell_command",
                "str_replace_editor",
                "read",
                "write",
                "mcp__wiki__read_page",
                "mcp__wiki__move_page",
                "mcp__fetch__fetch",
                "mcp__filesystem__read_text_file",
                "http_request",
                "fetch",
                "curl",
            ][..]
        )
        .prop_map(str::to_owned),
        "\\PC{0,24}",
    ]
}

fn arguments() -> impl Strategy<Value = ToolArguments> {
    prop_oneof![
        generate::json_value().prop_map(|v| ToolArguments::Json(CanonicalJson(v.to_string()))),
        "\\PC{0,40}".prop_map(ToolArguments::Invalid),
        "\\PC{0,40}".prop_map(|text| ToolArguments::Json(CanonicalJson(text))),
    ]
}

fn arbitrary_call() -> impl Strategy<Value = ToolCall> {
    prop_oneof![
        generate::known_call(),
        (tool_name(), arguments()).prop_map(|(name, arguments)| ToolCall {
            id: ToolCallId("toolu_01".to_owned()),
            name: ToolName(name),
            arguments,
            execution: ToolExecution::Client,
        }),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(2048))]

    #[test]
    fn extract_arbitrary_call(
        call in arbitrary_call(),
        result in generate::tool_result(),
        with_cwd in any::<bool>(),
    ) {
        let config = wiki_config();
        let mut context: ConversationContext = if with_cwd { context() } else { no_cwd() };
        let extractors = ToolExtractors::new(&config, &context);
        let _ = extractors.extract_classified(&call, result.as_ref());
        let _ = extractors.extract_classified(&call, None);
        context.observe(&config, &call, result.as_ref());
    }

    #[test]
    fn lex_arbitrary_command(text in "\\PC{0,80}") {
        let _ = Script::lex(&text);
    }

    #[test]
    fn lex_shell_like_command(
        parts in prop::collection::vec(
            prop::sample::select(&[
                "cat", "x", " ", "'", "\"", "\\", "$(", ")", "`", "<<", "EOF", "\n", ">", ">>",
                "2>&1", "&>", "|", "&&", ";", "#", "${", "}", "*", "~", "<", "<<<", "-",
            ][..]),
            0..24,
        ),
    ) {
        let _ = Script::lex(&parts.concat());
    }
}

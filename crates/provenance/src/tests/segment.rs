//! The segmenter on fixed outputs.

use crosstalk_spec::derived::provenance::span::{Origin, RelaySource};
use crosstalk_spec::interfaces::l4_provenance::Segmenter;
use crosstalk_spec::observed::message::{
    AssistantPart, Message, Text, ToolCallId, ToolExecution, ToolOutcome, ToolResult,
    ToolResultContent,
};
use crosstalk_testkit::build::message::{
    assistant, assistant_text, message, tool_call, tool_result, user_text,
};

use super::fixtures::{config, sentence};
use crate::decode::DecodePipeline;
use crate::fingerprint::Winnowing;
use crate::segment::NovelRunSegmenter;

fn segmenter() -> NovelRunSegmenter {
    let config = config();
    NovelRunSegmenter::new(
        Winnowing::new(config.winnow()),
        DecodePipeline::new(config.decode()),
    )
}

fn slice(
    message: &Message,
    draft: &crosstalk_spec::interfaces::l4_provenance::SpanDraft,
) -> String {
    let text = message.part_text(draft.location.part.index).expect("text");
    text[draft.location.range.start() as usize..draft.location.range.end() as usize].to_owned()
}

#[test]
fn copied_and_written_text_split() {
    let copied = sentence("garnet");
    let written = "my own conclusion is entirely different from anything above";
    let input = message(tool_result("call_1", &format!("page: {copied}")));
    let output = message(assistant_text(&format!("{written}. {copied}")));
    let drafts = segmenter().segment(&output, std::slice::from_ref(&input));
    assert_eq!(
        drafts.len(),
        2,
        "{}",
        super::fixtures::brief_drafts(&drafts)
    );
    assert_eq!(drafts[0].origin, Origin::Originated);
    assert!(slice(&output, &drafts[0]).starts_with(written));
    assert_eq!(
        drafts[1].origin,
        Origin::Relayed(RelaySource::Input(input.hash))
    );
    assert!(copied.contains(slice(&output, &drafts[1]).trim()));
}

#[test]
fn escaped_input_still_explains_the_copy() {
    let copied = "line one of the note\nline \"two\" of the note";
    let escaped = serde_json::to_string(copied).expect("json");
    let input = message(tool_result("call_1", &format!("{{\"note\": {escaped}}}")));
    let output = message(assistant_text(copied));
    let drafts = segmenter().segment(&output, std::slice::from_ref(&input));
    assert!(
        drafts
            .iter()
            .all(|d| d.origin == Origin::Relayed(RelaySource::Input(input.hash))),
        "{}",
        super::fixtures::brief_drafts(&drafts)
    );
}

#[test]
fn tool_call_arguments_are_segmented_unescaped() {
    let copied = "first line of the copied body\nsecond \"quoted\" line of it";
    let input = message(user_text(copied));
    let call = tool_call("call_1", "write", &serde_json::json!({ "body": copied }));
    let output = message(assistant(vec![call]));
    let drafts = segmenter().segment(&output, std::slice::from_ref(&input));
    let relayed: Vec<_> = drafts
        .iter()
        .filter(|d| d.origin == Origin::Relayed(RelaySource::Input(input.hash)))
        .collect();
    assert_eq!(
        relayed.len(),
        1,
        "{}",
        super::fixtures::brief_drafts(&drafts)
    );
    let text = slice(&output, relayed[0]);
    assert!(text.contains("second \\\"quoted\\\" line"), "{text}");
}

#[test]
fn server_tool_results_get_no_span() {
    let fetched = sentence("basalt");
    let result = AssistantPart::ServerToolResult(ToolResult {
        call_id: ToolCallId("srv_1".to_owned()),
        content: vec![ToolResultContent::Text(Text(fetched.clone()))],
        outcome: ToolOutcome::Success,
    });
    let call = match tool_call(
        "srv_1",
        "web_fetch",
        &serde_json::json!({"url": "https://example.com"}),
    ) {
        AssistantPart::ToolCall(mut call) => {
            call.execution = ToolExecution::Server;
            AssistantPart::ToolCall(call)
        }
        other => other,
    };
    let output = message(assistant(vec![
        call,
        result,
        AssistantPart::Text(Text(format!("The page says: {fetched}"))),
    ]));
    let drafts = segmenter().segment(&output, &[]);
    for draft in &drafts {
        assert_ne!(
            draft.location.part.index, 1,
            "the server result itself is not segmented"
        );
        if draft.location.part.index == 2 {
            assert!(
                !slice(&output, draft).contains(&fetched[..20]),
                "{}",
                super::fixtures::brief_drafts(&drafts)
            );
        }
    }
}

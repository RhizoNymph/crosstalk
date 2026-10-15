//! The text a span location indexes, per part kind.

use crate::observed::message::text::{NoPartText, TOOL_RESULT_SEPARATOR};
use crate::observed::message::{
    AssistantPart, CanonicalJson, Media, MediaKind, Message, MessageBody, Reasoning, SystemPart,
    Text, ToolArguments, ToolCall, ToolCallId, ToolExecution, ToolName, ToolOutcome, ToolResult,
    ToolResultContent, Unknown, UserPart,
};
use crate::support::NonEmpty;
use crate::tests::fixtures::message;

fn text(s: &str) -> Text {
    Text(s.to_owned())
}

fn unknown() -> Unknown {
    Unknown {
        kind: "mystery".into(),
        raw: CanonicalJson("{}".into()),
    }
}

fn media() -> Media {
    Media {
        kind: MediaKind::Image,
        blob: message(9),
    }
}

fn result(content: Vec<ToolResultContent>) -> ToolResult {
    ToolResult {
        call_id: ToolCallId("call_1".into()),
        content,
        outcome: ToolOutcome::Success,
    }
}

fn call(arguments: ToolArguments) -> AssistantPart {
    AssistantPart::ToolCall(ToolCall {
        id: ToolCallId("call_1".into()),
        name: ToolName("fetch".into()),
        arguments,
        execution: ToolExecution::Client,
    })
}

fn of(body: MessageBody) -> Message {
    Message {
        hash: message(1),
        body,
    }
}

#[test]
fn text_parts_are_their_text() {
    let system = of(MessageBody::System(vec![
        SystemPart::Text(text("be brief")),
        SystemPart::Unknown(unknown()),
    ]));
    assert_eq!(system.part_text(0).as_deref(), Ok("be brief"));
    assert_eq!(system.part_text(1), Err(NoPartText::NotText { index: 1 }));

    let user = of(MessageBody::User(vec![
        UserPart::Media(media()),
        UserPart::Text(text("look")),
        UserPart::Unknown(unknown()),
    ]));
    assert_eq!(user.part_text(0), Err(NoPartText::NotText { index: 0 }));
    assert_eq!(user.part_text(1).as_deref(), Ok("look"));
    assert_eq!(user.part_text(2), Err(NoPartText::NotText { index: 2 }));
}

#[test]
fn assistant_parts_index_text_reasoning_and_arguments() {
    let assistant = of(MessageBody::Assistant(vec![
        AssistantPart::Text(text("done")),
        AssistantPart::Reasoning(Reasoning::Visible(text("thinking"))),
        AssistantPart::Reasoning(Reasoning::Opaque {
            signature: "sig".into(),
        }),
        call(ToolArguments::Json(CanonicalJson("{\"url\":\"x\"}".into()))),
        call(ToolArguments::Invalid("{url: x".into())),
        AssistantPart::ServerToolResult(result(vec![ToolResultContent::Text(text("page"))])),
        AssistantPart::Unknown(unknown()),
    ]));
    assert_eq!(assistant.part_text(0).as_deref(), Ok("done"));
    assert_eq!(assistant.part_text(1).as_deref(), Ok("thinking"));
    assert_eq!(
        assistant.part_text(2),
        Err(NoPartText::NotText { index: 2 })
    );
    assert_eq!(assistant.part_text(3).as_deref(), Ok("{\"url\":\"x\"}"));
    assert_eq!(assistant.part_text(4).as_deref(), Ok("{url: x"));
    assert_eq!(assistant.part_text(5).as_deref(), Ok("page"));
    assert_eq!(
        assistant.part_text(6),
        Err(NoPartText::NotText { index: 6 })
    );
}

#[test]
fn tool_results_join_their_text_contents_and_skip_the_rest() {
    let first = result(vec![
        ToolResultContent::Text(text("one")),
        ToolResultContent::Media(media()),
        ToolResultContent::Text(text("two")),
        ToolResultContent::Unknown(unknown()),
        ToolResultContent::Text(text("three")),
    ]);
    let textless = result(vec![ToolResultContent::Media(media())]);
    let tool = of(MessageBody::Tool(
        NonEmpty::from_vec(vec![first, textless]).expect("two results"),
    ));
    let joined = ["one", "two", "three"].join(TOOL_RESULT_SEPARATOR);
    assert_eq!(tool.part_text(0).as_deref(), Ok(joined.as_str()));
    assert_eq!(tool.part_text(1), Err(NoPartText::NotText { index: 1 }));
}

#[test]
fn an_index_past_the_parts_names_no_part() {
    let user = of(MessageBody::User(vec![UserPart::Text(text("hi"))]));
    assert_eq!(user.part_count(), 1);
    assert_eq!(
        user.part_text(1),
        Err(NoPartText::NoSuchPart { index: 1, parts: 1 })
    );
    let tool = of(MessageBody::Tool(NonEmpty::new(result(Vec::new()))));
    assert_eq!(tool.part_count(), 1);
    assert_eq!(
        tool.part_text(3),
        Err(NoPartText::NoSuchPart { index: 3, parts: 1 })
    );
}

//! Spec messages as bench messages (`to_bench::message`, which
//! `from-export` writes the gateway's logged bodies through), and back
//! (`convert::message`, which `ct-bench-detect` reads them with): the bench
//! keeps every part in its place and its text, and drops only what it does
//! not store (signatures, a reasoning block's hidden text).

use a2a_bench_format::message::{AssistantPart, Body, ResultContent, ToolArguments, UserPart};
use crosstalk_bench_adapter::convert;
use crosstalk_bench_adapter::to_bench::{Lossy, message};
use crosstalk_spec::observed::message::{
    AssistantPart as SpecAssistant, Media, MediaKind, Message, MessageBody, Reasoning, Text,
    ToolCall, ToolCallId, ToolExecution, ToolName, ToolOutcome, ToolResult, ToolResultContent,
    UserPart as SpecUser,
};
use crosstalk_spec::support::NonEmpty;
use crosstalk_testkit::build::message::content_hash;

#[test]
fn message_parts_keep_what_the_bench_stores() {
    let body = MessageBody::Assistant(vec![
        SpecAssistant::Reasoning(Reasoning::Visible {
            text: Text("Let me think.".into()),
            signature: Some("sig-visible".into()),
        }),
        SpecAssistant::Reasoning(Reasoning::Opaque {
            signature: "sig-opaque".into(),
        }),
        SpecAssistant::ToolCall(ToolCall {
            id: ToolCallId("call_7".into()),
            name: ToolName("lookup".into()),
            arguments: crosstalk_spec::observed::message::ToolArguments::Json(
                crosstalk_spec::observed::message::json::canonicalize(r#"{"b":1,"a":"x"}"#)
                    .unwrap_or_else(|e| panic!("{e:?}")),
            ),
            execution: ToolExecution::Client,
            signature: Some("call-sig".into()),
        }),
    ]);
    let spec = Message::new(body);
    let mut lossy = Lossy::default();
    let bench = message::convert(&spec, &mut lossy).unwrap_or_else(|e| panic!("{e}"));
    let Body::Assistant(parts) = bench.body() else {
        panic!("an assistant message");
    };
    assert_eq!(
        parts[0],
        AssistantPart::Reasoning {
            text: "Let me think.".into()
        }
    );
    assert_eq!(parts[1], AssistantPart::ReasoningOpaque);
    let AssistantPart::ToolCall(call) = &parts[2] else {
        panic!("a tool call");
    };
    assert_eq!(call.call_id, "call_7");
    match &call.arguments {
        ToolArguments::Json(json) => assert_eq!(json.as_str(), r#"{"a":"x","b":1}"#),
        ToolArguments::Invalid(text) => panic!("invalid arguments {text}"),
    }

    let media = Message::new(MessageBody::User(vec![
        SpecUser::Text(Text("see attached".into())),
        SpecUser::Media(Media {
            kind: MediaKind::Image,
            blob: content_hash(&MessageBody::User(Vec::new())),
        }),
    ]));
    let bench = message::convert(&media, &mut lossy).unwrap_or_else(|e| panic!("{e}"));
    let Body::User(parts) = bench.body() else {
        panic!("a user message");
    };
    assert_eq!(
        parts[1],
        UserPart::Media {
            kind: a2a_bench_format::message::MediaKind::Image
        }
    );

    let results = Message::new(MessageBody::Tool(NonEmpty::new(ToolResult {
        call_id: ToolCallId("call_7".into()),
        content: vec![
            ToolResultContent::Text(Text("one".into())),
            ToolResultContent::Text(Text("two".into())),
        ],
        outcome: ToolOutcome::Error,
    })));
    let bench = message::convert(&results, &mut lossy).unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(bench.part_text(0).as_deref(), Ok("one\ntwo"));
    let Body::Tool(parts) = bench.body() else {
        panic!("a tool message");
    };
    let a2a_bench_format::message::ToolPart::ToolResult(result) = &parts[0];
    assert_eq!(
        result.content,
        vec![
            ResultContent::Text { text: "one".into() },
            ResultContent::Text { text: "two".into() }
        ]
    );
    assert_eq!(
        result.outcome,
        a2a_bench_format::message::ToolOutcome::Error
    );
}

#[test]
fn a_message_reads_back_with_every_part_text() {
    let messages = [
        Message::new(MessageBody::System(vec![
            crosstalk_spec::observed::message::SystemPart::Text(Text("You are Bob.".into())),
        ])),
        Message::new(MessageBody::User(vec![
            SpecUser::Text(Text("first".into())),
            SpecUser::Text(Text("second".into())),
        ])),
        Message::new(MessageBody::Assistant(vec![
            SpecAssistant::Reasoning(Reasoning::Visible {
                text: Text("Let me think.".into()),
                signature: Some("sig".into()),
            }),
            SpecAssistant::Text(Text("Posted.".into())),
        ])),
        Message::new(MessageBody::Tool(NonEmpty::new(ToolResult {
            call_id: ToolCallId("call_1".into()),
            content: vec![ToolResultContent::Text(Text("SECRET".into()))],
            outcome: ToolOutcome::Success,
        }))),
    ];
    let mut lossy = Lossy::default();
    for spec in &messages {
        let bench = message::convert(spec, &mut lossy).unwrap_or_else(|e| panic!("{e}"));
        let back = convert::message(&bench).unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(bench.part_count(), spec.part_count());
        for part in 0..u16::try_from(spec.part_count()).unwrap_or(u16::MAX) {
            assert_eq!(
                back.part_text(part).ok().map(|t| t.into_owned()),
                spec.part_text(part).ok().map(|t| t.into_owned()),
            );
        }
    }
}

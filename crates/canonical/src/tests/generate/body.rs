//! Generated message bodies, of every variant, as the spec types.

use crosstalk_spec::ids::MessageHash;
use crosstalk_spec::observed::message::{
    AssistantPart, Media, MediaKind, MessageBody, Reasoning, SystemPart, Text, ToolArguments,
    ToolCall, ToolCallId, ToolExecution, ToolName, ToolOutcome, ToolResult, ToolResultContent,
    Unknown, UserPart,
};
use crosstalk_spec::support::{Blake3, NonEmpty};
use proptest::prelude::*;

use super::json::{arb_json, arb_text};

fn arb_unknown() -> impl Strategy<Value = Unknown> {
    ("[a-z_]{0,12}", arb_json()).prop_map(|(kind, value)| Unknown {
        kind,
        raw: value.value().canonical(),
    })
}

fn arb_media() -> impl Strategy<Value = Media> {
    (
        prop_oneof![
            Just(MediaKind::Image),
            Just(MediaKind::Audio),
            Just(MediaKind::Document)
        ],
        any::<[u8; 32]>(),
    )
        .prop_map(|(kind, digest)| Media {
            kind,
            blob: MessageHash::from_digest(Blake3::from_bytes(digest)),
        })
}

fn arb_result() -> impl Strategy<Value = ToolResult> {
    (
        arb_text(),
        proptest::collection::vec(
            prop_oneof![
                arb_text().prop_map(|text| ToolResultContent::Text(Text(text))),
                arb_media().prop_map(ToolResultContent::Media),
                arb_unknown().prop_map(ToolResultContent::Unknown),
            ],
            0..4,
        ),
        any::<bool>(),
    )
        .prop_map(|(id, content, error)| ToolResult {
            call_id: ToolCallId(id),
            content,
            outcome: if error {
                ToolOutcome::Error
            } else {
                ToolOutcome::Success
            },
        })
}

fn arb_assistant_part() -> impl Strategy<Value = AssistantPart> {
    prop_oneof![
        arb_text().prop_map(|text| AssistantPart::Text(Text(text))),
        arb_text().prop_map(|text| AssistantPart::Reasoning(Reasoning::Visible(Text(text)))),
        any::<String>()
            .prop_map(|signature| AssistantPart::Reasoning(Reasoning::Opaque { signature })),
        (
            arb_text(),
            arb_text(),
            prop_oneof![
                arb_json().prop_map(|value| ToolArguments::Json(value.value().canonical())),
                any::<String>().prop_map(ToolArguments::Invalid),
            ],
            any::<bool>()
        )
            .prop_map(
                |(id, name, arguments, server)| AssistantPart::ToolCall(ToolCall {
                    id: ToolCallId(id),
                    name: ToolName(name),
                    arguments,
                    execution: if server {
                        ToolExecution::Server
                    } else {
                        ToolExecution::Client
                    },
                })
            ),
        arb_result().prop_map(AssistantPart::ServerToolResult),
        arb_unknown().prop_map(AssistantPart::Unknown),
    ]
}

pub fn arb_body() -> impl Strategy<Value = MessageBody> {
    prop_oneof![
        proptest::collection::vec(
            prop_oneof![
                arb_text().prop_map(|text| SystemPart::Text(Text(text))),
                arb_unknown().prop_map(SystemPart::Unknown),
            ],
            0..4
        )
        .prop_map(MessageBody::System),
        proptest::collection::vec(
            prop_oneof![
                arb_text().prop_map(|text| UserPart::Text(Text(text))),
                arb_media().prop_map(UserPart::Media),
                arb_unknown().prop_map(UserPart::Unknown),
            ],
            0..4
        )
        .prop_map(MessageBody::User),
        proptest::collection::vec(arb_assistant_part(), 0..5).prop_map(MessageBody::Assistant),
        proptest::collection::vec(arb_result(), 1..4).prop_map(|results| {
            MessageBody::Tool(
                NonEmpty::from_vec(results).unwrap_or_else(|| panic!("at least one result")),
            )
        }),
    ]
}

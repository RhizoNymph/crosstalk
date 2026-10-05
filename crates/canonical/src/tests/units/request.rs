//! Requests on fixed inputs: system prompts, role splitting, unknown
//! blocks, orphans, verbatim text and ids.

use crosstalk_spec::interfaces::l0_ingress::RawResponse;
use crosstalk_spec::interfaces::l1_canonical::{NormalizeWarning, Normalizer};
use crosstalk_spec::observed::client::Dialect;
use crosstalk_spec::observed::exchange::{ConnectionId, Continuation, ResponseId, Transport};
use crosstalk_spec::observed::message::{
    AssistantPart, CanonicalJson, MessageBody, SystemPart, Text, ToolCallId, ToolOutcome,
    ToolResult, ToolResultContent, Unknown, UserPart,
};
use crosstalk_spec::support::NonEmpty;

use crate::AnthropicMessages;
use crate::tests::support::{START, normalize, ok, raw, request_bodies, response_parts, sse, stop};

fn text(value: &str) -> Text {
    Text(value.to_owned())
}

fn request(system: &str, messages: &str) -> String {
    format!(r#"{{"model":"claude-opus-5-5","max_tokens":1024{system},"messages":{messages}}}"#)
}

fn bodies(system: &str, messages: &str) -> Vec<MessageBody> {
    request_bodies(&normalize(&raw(
        &request(system, messages),
        Transport::Http,
        ok("{}"),
    )))
}

fn result(id: &str, content: &str) -> ToolResult {
    ToolResult {
        call_id: ToolCallId(id.to_owned()),
        content: vec![ToolResultContent::Text(text(content))],
        outcome: ToolOutcome::Success,
    }
}

/// `system` as a string or as blocks becomes one System message first.
pub fn top_level_system_prompt_becomes_first_message() {
    let turn = r#"[{"role":"user","content":"hi"}]"#;
    assert_eq!(
        bodies(r#","system":"Be brief.""#, turn),
        vec![
            MessageBody::System(vec![SystemPart::Text(text("Be brief."))]),
            MessageBody::User(vec![UserPart::Text(text("hi"))]),
        ]
    );
    let blocks = r#","system":[{"type":"text","text":"billing"},{"type":"text","text":"You are Claude Code.","cache_control":{"type":"ephemeral","ttl":"1h"}}]"#;
    assert_eq!(
        bodies(blocks, turn)[0],
        MessageBody::System(vec![
            SystemPart::Text(text("billing")),
            SystemPart::Text(text("You are Claude Code.")),
        ])
    );
    // The system field may follow the messages in the body.
    let late = r#"{"messages":[{"role":"user","content":"hi"}],"system":"Be brief.","model":"m"}"#;
    let late = request_bodies(&normalize(&raw(late, Transport::Http, ok("{}"))));
    assert_eq!(
        late[0],
        MessageBody::System(vec![SystemPart::Text(text("Be brief."))])
    );
    assert_eq!(
        bodies("", turn),
        vec![MessageBody::User(vec![UserPart::Text(text("hi"))])]
    );
    assert_eq!(bodies(r#","system":null"#, turn).len(), 1);
}

/// A `system` turn inside `messages` (Claude Code sends one) is a System
/// message at its own position, its content mapped like the top-level
/// `system`: a string is one text part, blocks one part each, cache
/// markers dropped and unknown blocks kept and reported. The top-level
/// prompt stays first.
pub fn system_turn_stays_in_place() {
    // A string turn after the first user turn, with a top-level prompt.
    let messages = r#"[
        {"role":"user","content":"hi"},
        {"role":"system","content":"Context: the repo is clean."},
        {"role":"assistant","content":"ok"},
        {"role":"user","content":"go"}
    ]"#;
    assert_eq!(
        bodies(r#","system":"Be brief.""#, messages),
        vec![
            MessageBody::System(vec![SystemPart::Text(text("Be brief."))]),
            MessageBody::User(vec![UserPart::Text(text("hi"))]),
            MessageBody::System(vec![SystemPart::Text(text("Context: the repo is clean."))]),
            MessageBody::Assistant(vec![AssistantPart::Text(text("ok"))]),
            MessageBody::User(vec![UserPart::Text(text("go"))]),
        ]
    );

    // A block turn: the same mapping as the top-level `system` array.
    let blocks = r#"[{"type":"text","text":"billing"},{"type":"text","text":"Reminder.","cache_control":{"type":"ephemeral"}},{"type":"zz_sys","v":1}]"#;
    let turn =
        format!(r#"[{{"role":"user","content":"hi"}},{{"role":"system","content":{blocks}}}]"#);
    let normalization = normalize(&raw(
        &request(&format!(r#","system":{blocks}"#), &turn),
        Transport::Http,
        ok("{}"),
    ));
    let got = request_bodies(&normalization);
    let expected = MessageBody::System(vec![
        SystemPart::Text(text("billing")),
        SystemPart::Text(text("Reminder.")),
        SystemPart::Unknown(Unknown {
            kind: "zz_sys".to_owned(),
            raw: CanonicalJson(r#"{"type":"zz_sys","v":1}"#.to_owned()),
        }),
    ]);
    assert_eq!(got.len(), 3, "{got:?}");
    assert_eq!(got[0], expected, "the top-level prompt");
    assert_eq!(got[2], expected, "the turn maps like the top-level prompt");
    assert_eq!(
        normalization.warnings,
        vec![
            NormalizeWarning::UnknownBlock {
                kind: "zz_sys".to_owned()
            };
            2
        ],
        "an unknown block in either is reported"
    );
    // Same content, same message: one stored body for both.
    assert_eq!(
        normalization.exchange.request[0],
        normalization.exchange.request[2]
    );
    // `{}` is no Messages response, so the exchange has no response message.
    assert_eq!(
        normalization.messages.len(),
        2,
        "the system body once, and the user turn"
    );

    // No top-level `system`: the turn is the only System message, where
    // it sits, even first.
    let first = r#"[{"role":"system","content":"s"},{"role":"user","content":"hi"},{"role":"system","content":[]}]"#;
    assert_eq!(
        bodies("", first),
        vec![
            MessageBody::System(vec![SystemPart::Text(text("s"))]),
            MessageBody::User(vec![UserPart::Text(text("hi"))]),
            MessageBody::System(Vec::new()),
        ]
    );
}

/// A refused body's shape names its keys, roles and content kinds, and
/// none of its values.
pub fn request_shape_holds_no_content() {
    use crate::anthropic::RequestShape;
    let secret = "sk-ant-api03-SECRET";
    let body = format!(
        r#"{{"model":"m","system":[{{"type":"text","text":"{secret}"}}],"messages":[{{"role":"user","content":"{secret}"}},{{"role":"system","content":[{{"type":"text","text":"{secret}"}},{{"v":1}}]}},{{"role":"{secret} weird","content":7}},{{"content":"x"}},3],"{secret} key":1}}"#
    );
    let shape = RequestShape::of(body.as_bytes()).to_string();
    assert_eq!(
        shape,
        "keys=[model,system,messages,<23 bytes>] system=array \
         messages=[user:string,system:array(text,<untyped>),<25 bytes>:number,\
         <no role>:string,<number>]"
    );
    assert!(!shape.contains("SECRET"), "{shape}");
    // A token-shaped name is withheld by its length.
    let token = format!("sk-ant-oat01-{}", "A".repeat(80));
    let body = format!(r#"{{"messages":[{{"role":"{token}","content":""}}]}}"#);
    assert_eq!(
        RequestShape::of(body.as_bytes()).to_string(),
        "keys=[messages] system=absent messages=[<93 bytes>:string]"
    );
    assert_eq!(RequestShape::of(b"not json").to_string(), "not json");
    assert_eq!(
        RequestShape::of(b"[1]").to_string(),
        "not an object (array)"
    );
    assert_eq!(
        RequestShape::of(br#"{"messages":{}}"#).to_string(),
        "keys=[messages] system=absent messages=object"
    );
    assert_eq!(
        RequestShape::of(br#"{"model":"m"}"#).to_string(),
        "keys=[model] system=absent messages=absent"
    );
}

/// A user turn mixing tool results and text becomes one message per
/// maximal run of one role, in block order.
pub fn anthropic_user_turn_with_tool_results_splits() {
    let history = r#"[
        {"role":"assistant","content":[{"type":"tool_use","id":"a","name":"Read","input":{}},{"type":"tool_use","id":"b","name":"Read","input":{}}]},
        {"role":"user","content":[
            {"type":"tool_result","tool_use_id":"a","content":"one"},
            {"type":"tool_result","tool_use_id":"b","content":[{"type":"text","text":"two"}],"cache_control":{"type":"ephemeral"}},
            {"type":"text","text":"Now summarize."}
        ]},
        {"role":"user","content":[
            {"type":"text","text":"before"},
            {"type":"tool_result","tool_use_id":"a","content":"again"},
            {"type":"text","text":"after"}
        ]}
    ]"#;
    let bodies = bodies("", history);
    assert_eq!(bodies.len(), 6);
    assert_eq!(
        bodies[1],
        MessageBody::Tool(
            NonEmpty::from_vec(vec![result("a", "one"), result("b", "two")])
                .unwrap_or_else(|| panic!("two"))
        )
    );
    assert_eq!(
        bodies[2],
        MessageBody::User(vec![UserPart::Text(text("Now summarize."))])
    );
    assert_eq!(
        bodies[3],
        MessageBody::User(vec![UserPart::Text(text("before"))])
    );
    assert_eq!(
        bodies[4],
        MessageBody::Tool(NonEmpty::new(result("a", "again")))
    );
    assert_eq!(
        bodies[5],
        MessageBody::User(vec![UserPart::Text(text("after"))])
    );
}

/// An unrecognized block stays at its position as `Unknown`, with its type
/// and canonical JSON (less its cache marker), in every part list, and is
/// reported.
pub fn unknown_block_kept_in_place() {
    let messages = r#"[
        {"role":"user","content":[
            {"type":"text","text":"a"},
            {"z":[1, 2.0],"type":"zz_new", "cache_control":{"type":"ephemeral"}},
            {"type":"text","text":"b"},
            {"type":"tool_result","tool_use_id":"t","content":[{"type":"text","text":"r"},{"type":"zz_item","n":10e-1}]}
        ]},
        {"role":"assistant","content":[{"type":"text","text":"c"},{"type":"zz_out"},{"type":"text","text":"d"}]}
    ]"#;
    let normalization = normalize(&raw(
        &request(
            r#","system":[{"type":"zz_sys","v":true},{"type":"text","text":"s"}]"#,
            messages,
        ),
        Transport::Http,
        ok("{}"),
    ));
    let bodies = request_bodies(&normalization);
    let unknown = |kind: &str, raw: &str| Unknown {
        kind: kind.to_owned(),
        raw: CanonicalJson(raw.to_owned()),
    };
    assert_eq!(
        bodies[0],
        MessageBody::System(vec![
            SystemPart::Unknown(unknown("zz_sys", r#"{"type":"zz_sys","v":true}"#)),
            SystemPart::Text(text("s")),
        ])
    );
    assert_eq!(
        bodies[1],
        MessageBody::User(vec![
            UserPart::Text(text("a")),
            UserPart::Unknown(unknown("zz_new", r#"{"type":"zz_new","z":[1,2]}"#)),
            UserPart::Text(text("b")),
        ])
    );
    let MessageBody::Tool(results) = &bodies[2] else {
        panic!("a tool message: {bodies:?}");
    };
    assert_eq!(
        results.first().content[1],
        ToolResultContent::Unknown(unknown("zz_item", r#"{"n":1,"type":"zz_item"}"#))
    );
    assert_eq!(
        bodies[3],
        MessageBody::Assistant(vec![
            AssistantPart::Text(text("c")),
            AssistantPart::Unknown(unknown("zz_out", r#"{"type":"zz_out"}"#)),
            AssistantPart::Text(text("d")),
        ])
    );
    let kinds: Vec<&NormalizeWarning> = normalization
        .warnings
        .iter()
        .filter(|warning| matches!(warning, NormalizeWarning::UnknownBlock { .. }))
        .collect();
    let expected: Vec<NormalizeWarning> = ["zz_sys", "zz_new", "zz_item", "zz_out"]
        .into_iter()
        .map(|kind| NormalizeWarning::UnknownBlock {
            kind: kind.to_owned(),
        })
        .collect();
    assert_eq!(kinds, expected.iter().collect::<Vec<_>>());
}

const ORPHAN: &str = r#"[
    {"role":"assistant","content":[{"type":"tool_use","id":"toolu_known","name":"Read","input":{}}]},
    {"role":"user","content":[
        {"type":"tool_result","tool_use_id":"toolu_known","content":"ok"},
        {"type":"tool_result","tool_use_id":"toolu_compacted","content":"kept"}
    ]}
]"#;

/// A tool result with no earlier call in a full history is kept and
/// reported, never an error.
pub fn orphan_tool_result_warns_and_is_kept() {
    let normalization = normalize(&raw(&request("", ORPHAN), Transport::Http, ok("{}")));
    let bodies = request_bodies(&normalization);
    assert_eq!(
        bodies[1],
        MessageBody::Tool(
            NonEmpty::from_vec(vec![
                result("toolu_known", "ok"),
                result("toolu_compacted", "kept")
            ])
            .unwrap_or_else(|| panic!("two"))
        )
    );
    assert_eq!(
        normalization.warnings,
        vec![NormalizeWarning::OrphanToolResult {
            call_id: "toolu_compacted".to_owned()
        }]
    );
    // A call after its result does not count: "earlier" only.
    let later = r#"[
        {"role":"user","content":[{"type":"tool_result","tool_use_id":"x","content":"r"}]},
        {"role":"assistant","content":[{"type":"tool_use","id":"x","name":"Read","input":{}}]}
    ]"#;
    let warnings = normalize(&raw(&request("", later), Transport::Http, ok("{}"))).warnings;
    assert_eq!(
        warnings,
        vec![NormalizeWarning::OrphanToolResult {
            call_id: "x".to_owned()
        }]
    );
}

/// An increment's results answer calls L1 never saw: no orphan warnings,
/// and the continuation is carried as it came.
pub fn increment_tool_results_do_not_warn() {
    let mut increment = raw(&request("", ORPHAN), Transport::Http, ok("{}"));
    let continuation = Continuation::Increment {
        previous: ResponseId("msg_prev".to_owned()),
        connection: Some(ConnectionId(7)),
    };
    increment.request.harness.continuation = continuation.clone();
    let normalization = normalize(&increment);
    assert!(normalization.warnings.is_empty());
    assert_eq!(normalization.exchange.continuation, continuation);
    assert_eq!(request_bodies(&normalization).len(), 2);
}

/// Text keeps edge whitespace, NFD sequences, controls and escapes
/// exactly, in every place text appears.
pub fn text_with_edge_whitespace_and_nfd_kept() {
    let decoded = "  e\u{301}\u{e9} \t\r\n\u{1}\u{feff}\u{1F600}\"\\/  ";
    let wire = r#"  éé \t\r\n\u0001﻿😀\"\\\/  "#;
    let messages = format!(
        r#"[{{"role":"user","content":[{{"type":"text","text":"{wire}"}},{{"type":"tool_result","tool_use_id":"t","content":"{wire}"}}]}},{{"role":"assistant","content":"{wire}"}}]"#
    );
    let normalization = normalize(&raw(
        &request(&format!(r#","system":"{wire}""#), &messages),
        Transport::Http,
        ok("{}"),
    ));
    let bodies = request_bodies(&normalization);
    assert_eq!(
        bodies[0],
        MessageBody::System(vec![SystemPart::Text(text(decoded))])
    );
    assert_eq!(
        bodies[1],
        MessageBody::User(vec![UserPart::Text(text(decoded))])
    );
    assert_eq!(
        bodies[2],
        MessageBody::Tool(NonEmpty::new(result("t", decoded)))
    );
    assert_eq!(
        bodies[3],
        MessageBody::Assistant(vec![AssistantPart::Text(text(decoded))])
    );
    let stream = sse(&[
        START,
        (
            "content_block_start",
            r#"{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}"#,
        ),
        (
            "content_block_delta",
            &format!(
                r#"{{"type":"content_block_delta","index":0,"delta":{{"type":"text_delta","text":"{wire}"}}}}"#
            ),
        ),
        (stop("end_turn")[0].0, &stop("end_turn")[0].1),
        (stop("end_turn")[1].0, &stop("end_turn")[1].1),
    ]);
    let streamed = normalize(&raw(&request("", "[]"), Transport::Sse, ok(&stream)));
    assert_eq!(
        response_parts(&streamed),
        vec![AssistantPart::Text(text(decoded))]
    );
}

/// Tool call ids are kept exactly as the wire carried them, whatever the
/// dialect minted, in calls and in the results that answer them.
pub fn dialect_tool_call_ids_kept_verbatim() {
    let ids = [
        "toolu_016FdvOnXciSbMA2YzruhO7J",
        "chatcmpl-tool-3f2a9c1e-7b4d-4e8a-9c2f-1a2b3c4d5e6f",
        "call_0123456789abcdef01234567",
        " spaced id ",
    ];
    for dialect in [
        Dialect::Reference,
        Dialect::Vllm,
        Dialect::Sglang,
        Dialect::Copilot,
    ] {
        assert!(AnthropicMessages.handles(dialect));
        for id in ids {
            let messages = format!(
                r#"[{{"role":"assistant","content":[{{"type":"tool_use","id":"{id}","name":"Bash","input":{{}}}}]}},{{"role":"user","content":[{{"type":"tool_result","tool_use_id":"{id}","content":"x"}}]}}]"#
            );
            let response = format!(
                r#"{{"id":"msg_1","type":"message","role":"assistant","content":[{{"type":"tool_use","id":"{id}","name":"Bash","input":{{}}}}],"stop_reason":"tool_use","usage":null}}"#
            );
            let mut exchange = raw(&request("", &messages), Transport::Http, ok(&response));
            exchange.request.harness.dialect = dialect;
            let normalization = normalize(&exchange);
            let bodies = request_bodies(&normalization);
            let MessageBody::Assistant(parts) = &bodies[0] else {
                panic!("assistant first");
            };
            let AssistantPart::ToolCall(call) = &parts[0] else {
                panic!("a call");
            };
            assert_eq!(call.id, ToolCallId(id.to_owned()));
            assert_eq!(bodies[1], MessageBody::Tool(NonEmpty::new(result(id, "x"))));
            let AssistantPart::ToolCall(answer) = &response_parts(&normalization)[0] else {
                panic!("a call in the response");
            };
            assert_eq!(answer.id, ToolCallId(id.to_owned()));
            assert!(normalization.warnings.is_empty());
        }
    }
}

/// A request body that is not an Anthropic Messages request is the only
/// normalization error; content inside a valid one never is.
pub fn invalid_request_bodies_are_errors() {
    for body in [
        "not json",
        "[]",
        r#"{"model":"m"}"#,
        r#"{"messages":{}}"#,
        r#"{"messages":[{"content":"x"}]}"#,
        r#"{"messages":[{"role":"developer","content":"x"}]}"#,
        r#"{"messages":[{"role":"system","content":7}]}"#,
        r#"{"messages":[{"role":"user","content":7}]}"#,
        r#"{"messages":[],"system":7}"#,
    ] {
        let result = crate::anthropic::normalize(&raw(body, Transport::Http, ok("{}")));
        assert!(result.is_err(), "{body} is refused");
    }
    let failed = raw(
        r#"{"messages":[]}"#,
        Transport::Sse,
        RawResponse::Failed {
            failure: crosstalk_spec::observed::exchange::ExchangeFailure::ClientDisconnected,
            partial_body: vec![0xff, 0xfe],
        },
    );
    assert!(crate::anthropic::normalize(&failed).is_ok());
}

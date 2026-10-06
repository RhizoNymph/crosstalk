//! The fake model: deterministic, obedient to the task marker, tolerant of
//! what it does not need.

use std::time::Duration;

use bytes::BytesMut;
use serde_json::{Value, json};

use crate::anthropic::sse::{Split, encode};
use crate::anthropic::{ResponseBlock, StopReason};
use crate::http::BaseUrl;
use crate::knobs::Span;
use crate::protocol::{HTTP_TOOL, PageSlug, Task, tool_definitions};
use crate::upstream::generate::{GenConfig, LastTurn, RequestError, generate, parse_request};

fn config() -> GenConfig {
    GenConfig {
        seed: 11,
        words: Span::ordered(30, 60),
        first_byte_ms: Span::ordered(100, 200),
        stream_ms: Span::ordered(1000, 2000),
    }
}

fn body(messages: Value, stream: bool) -> Vec<u8> {
    serde_json::to_vec(&json!({
        "model": "claude-opus-5-5",
        "max_tokens": 4096,
        "system": [{"type": "text", "text": "You are agent-001."}],
        "tools": tool_definitions(),
        "messages": messages,
        "stream": stream,
    }))
    .expect("encode")
}

fn user_task(task: &Task) -> Value {
    json!([{"role": "user", "content": [{"type": "text", "text": task.prompt()}]}])
}

fn page(name: &str) -> PageSlug {
    name.parse().expect("slug")
}

fn wiki() -> BaseUrl {
    "http://wiki:8090".parse().expect("url")
}

fn sse(body: &[u8]) -> Vec<u8> {
    let request = parse_request(body).expect("request");
    let reply = generate(&config(), &request, body);
    let mut out = BytesMut::new();
    for frame in encode(&reply.message, Split::default()) {
        out.extend_from_slice(&frame.to_bytes());
    }
    out.to_vec()
}

#[test]
fn same_seed_and_body_give_identical_bytes() {
    let body = body(user_task(&Task::Chat { topic: 2 }), true);
    assert_eq!(sse(&body), sse(&body));
    let request = parse_request(&body).expect("request");
    assert_eq!(
        generate(&config(), &request, &body),
        generate(&config(), &request, &body)
    );
}

#[test]
fn seed_and_body_change_the_answer() {
    let body_a = body(user_task(&Task::Chat { topic: 2 }), true);
    let body_b = body(user_task(&Task::Chat { topic: 3 }), true);
    let request = parse_request(&body_a).expect("request");
    let other_seed = GenConfig {
        seed: 12,
        ..config()
    };
    assert_ne!(
        generate(&config(), &request, &body_a).message,
        generate(&other_seed, &request, &body_a).message
    );
    assert_ne!(sse(&body_a), sse(&body_b));
}

#[test]
fn write_task_gets_an_http_put() {
    let task = Task::Write {
        page: page("vacuum-tuning-1"),
        topic: 1,
        base: wiki(),
    };
    let body = body(user_task(&task), true);
    let request = parse_request(&body).expect("request");
    assert_eq!(request.last, LastTurn::Task(task));
    let reply = generate(&config(), &request, &body);
    assert_eq!(reply.message.stop_reason, StopReason::ToolUse);
    assert!(matches!(
        reply.message.content[0],
        ResponseBlock::Text { .. }
    ));
    let ResponseBlock::ToolUse { id, name, input } = &reply.message.content[1] else {
        panic!("a tool call")
    };
    assert!(id.starts_with("toolu_01"));
    assert_eq!(name, HTTP_TOOL);
    assert_eq!(input["method"], "PUT");
    assert_eq!(input["url"], "http://wiki:8090/pages/vacuum-tuning-1");
    assert_eq!(
        input.as_object().map(|o| o.len()),
        Some(3),
        "method, url, body"
    );
    let content = input["body"].as_str().expect("body");
    let words = content.split_whitespace().count();
    assert!(words >= 30, "{words} words");
    assert!(reply.message.id.starts_with("msg_01"));
}

#[test]
fn read_task_gets_an_http_get() {
    let task = Task::Read {
        page: page("cache-invalidation-2"),
        base: wiki(),
    };
    let body = body(user_task(&task), false);
    let request = parse_request(&body).expect("request");
    assert!(!request.stream);
    let reply = generate(&config(), &request, &body);
    assert!(!reply.stream);
    assert_eq!(reply.message.stop_reason, StopReason::ToolUse);
    let ResponseBlock::ToolUse { name, input, .. } = &reply.message.content[1] else {
        panic!("a tool call")
    };
    assert_eq!(name, HTTP_TOOL);
    assert_eq!(
        input,
        &json!({"method": "GET", "url": "http://wiki:8090/pages/cache-invalidation-2"})
    );
}

#[test]
fn undeclared_tools_are_never_called() {
    let task = Task::Write {
        page: page("vacuum-tuning-1"),
        topic: 1,
        base: wiki(),
    };
    let body = serde_json::to_vec(&json!({
        "model": "m", "max_tokens": 100, "messages": user_task(&task),
    }))
    .expect("encode");
    let request = parse_request(&body).expect("request");
    let reply = generate(&config(), &request, &body);
    assert_eq!(reply.message.stop_reason, StopReason::EndTurn);
    assert!(
        reply
            .message
            .content
            .iter()
            .all(|b| matches!(b, ResponseBlock::Text { .. }))
    );
}

#[test]
fn tool_results_get_a_closing_answer() {
    let messages = json!([
        {"role": "user", "content": [{"type": "text", "text": Task::Read { page: page("x-1"), base: wiki() }.prompt()}]},
        {"role": "assistant", "content": [{"type": "tool_use", "id": "toolu_1", "name": HTTP_TOOL, "input": {"method": "GET", "url": "http://wiki:8090/pages/x-1"}}]},
        {"role": "user", "content": [{"type": "tool_result", "tool_use_id": "toolu_1", "content": "page text"}]},
    ]);
    let body = body(messages, true);
    let request = parse_request(&body).expect("request");
    assert_eq!(
        request.last,
        LastTurn::ToolResults {
            errors: 0,
            total: 1
        }
    );
    let reply = generate(&config(), &request, &body);
    assert_eq!(reply.message.stop_reason, StopReason::EndTurn);
    assert_eq!(reply.message.content.len(), 1);
}

#[test]
fn failed_tool_results_are_acknowledged() {
    let messages = json!([
        {"role": "user", "content": [{"type": "tool_result", "tool_use_id": "t", "content": "nope", "is_error": true}]},
    ]);
    let body = body(messages, true);
    let request = parse_request(&body).expect("request");
    assert_eq!(
        request.last,
        LastTurn::ToolResults {
            errors: 1,
            total: 1
        }
    );
    let reply = generate(&config(), &request, &body);
    let ResponseBlock::Text { text } = &reply.message.content[0] else {
        panic!("text")
    };
    assert!(text.starts_with("That did not work"));
}

#[test]
fn system_turns_inside_messages_are_skipped() {
    let task = Task::Read {
        page: page("x-1"),
        base: wiki(),
    };
    let messages = json!([
        {"role": "system", "content": "<system-reminder>hi</system-reminder>"},
        {"role": "user", "content": task.prompt()},
        {"role": "system", "content": "<system-reminder>again</system-reminder>"},
    ]);
    let request = parse_request(&body(messages, true)).expect("request");
    assert_eq!(request.last, LastTurn::Task(task));
}

#[test]
fn prompts_without_a_marker_get_prose() {
    let body = body(json!([{"role": "user", "content": "hello there"}]), true);
    let request = parse_request(&body).expect("request");
    assert_eq!(request.last, LastTurn::Other);
    let reply = generate(&config(), &request, &body);
    assert_eq!(reply.message.stop_reason, StopReason::EndTurn);
}

#[test]
fn malformed_requests_are_refused() {
    assert!(matches!(parse_request(b"nope"), Err(RequestError::Json(_))));
    assert_eq!(parse_request(b"[]"), Err(RequestError::NotObject));
    let without = |field: &str| {
        let mut value =
            json!({"model": "m", "max_tokens": 10, "messages": [{"role": "user", "content": "x"}]});
        value.as_object_mut().expect("object").remove(field);
        parse_request(&serde_json::to_vec(&value).expect("encode"))
    };
    assert_eq!(without("model"), Err(RequestError::Field("model")));
    assert_eq!(
        without("max_tokens"),
        Err(RequestError::Field("max_tokens"))
    );
    assert_eq!(without("messages"), Err(RequestError::Field("messages")));
    assert_eq!(
        parse_request(br#"{"model":"m","max_tokens":1,"messages":[{"role":"user","content":"x"}],"stream":"yes"}"#),
        Err(RequestError::Field("stream"))
    );
}

#[test]
fn timing_and_length_stay_in_their_ranges() {
    for topic in 0..50 {
        let body = body(user_task(&Task::Chat { topic }), true);
        let request = parse_request(&body).expect("request");
        let reply = generate(&config(), &request, &body);
        assert!(
            (Duration::from_millis(100)..=Duration::from_millis(200)).contains(&reply.first_byte)
        );
        assert!(
            (Duration::from_millis(1000)..=Duration::from_millis(2000))
                .contains(&reply.stream_time)
        );
        let ResponseBlock::Text { text } = &reply.message.content[0] else {
            panic!("text")
        };
        let words = text.split_whitespace().count();
        // Whole sentences: at least the drawn count, at most one sentence more.
        assert!((30..=60 + 20).contains(&words), "{words} words");
    }
}

#[test]
fn max_tokens_caps_the_length() {
    let body = serde_json::to_vec(&json!({
        "model": "m", "max_tokens": 4, "messages": user_task(&Task::Chat { topic: 0 }),
    }))
    .expect("encode");
    let request = parse_request(&body).expect("request");
    let reply = generate(&config(), &request, &body);
    let ResponseBlock::Text { text } = &reply.message.content[0] else {
        panic!("text")
    };
    // Three words asked for; one whole sentence at most.
    assert!(text.split_whitespace().count() <= 20);
}

#[test]
fn usage_reflects_the_request_and_answer() {
    let body = body(user_task(&Task::Chat { topic: 4 }), true);
    let request = parse_request(&body).expect("request");
    let reply = generate(&config(), &request, &body);
    assert_eq!(reply.message.usage.input_tokens, body.len() as u64 / 4);
    assert!(reply.message.usage.output_tokens > 0);
    assert_eq!(reply.message.model, "claude-opus-5-5");
}

/// The lead-in text of a write reply for agent `agent` in `style`.
fn write_lead_in(agent: &str, style: &str) -> String {
    let task = Task::Write {
        page: page("shared-1"),
        topic: 3,
        base: wiki(),
    };
    let body = serde_json::to_vec(&json!({
        "model": "claude-opus-5-5",
        "max_tokens": 4096,
        "system": format!("You are {agent}.\n\n[style:{style}]"),
        "tools": tool_definitions(),
        "messages": user_task(&task),
        "stream": false,
    }))
    .expect("encode");
    let request = parse_request(&body).expect("request");
    let reply = generate(&config(), &request, &body);
    let ResponseBlock::Text { text } = &reply.message.content[0] else {
        panic!("a lead-in")
    };
    text.clone()
}

/// Two agents writing the same page share their lead-in in the boilerplate
/// style and share nothing but the page name in the headline style.
#[test]
fn headline_lead_ins_differ_between_agents() {
    let (a, b) = (
        write_lead_in("agent-001", "boilerplate"),
        write_lead_in("agent-002", "boilerplate"),
    );
    assert_eq!(a, b);
    assert!(a.starts_with("I'll update the wiki page `shared-1`"));
    let (a, b) = (
        write_lead_in("agent-001", "headline"),
        write_lead_in("agent-002", "headline"),
    );
    assert!(a.ends_with("`shared-1`.") && b.ends_with("`shared-1`."));
    let common = a
        .as_bytes()
        .windows(16)
        .filter(|w| !b"`shared-1`.".windows(w.len()).any(|p| p == *w))
        .any(|w| b.as_bytes().windows(16).any(|v| v == w));
    assert!(!common, "headline lead-ins share 16 bytes: {a:?} / {b:?}");
}

//! Conversation growth and the tool-result flow, including one agent's
//! model output reaching another agent's input through the wiki.

use serde_json::{Value, json};

use crate::anthropic::{AssistantMessage, ResponseBlock, Role, StopReason, Usage};
use crate::knobs::Span;
use crate::protocol::{PageSlug, Task, WIKI_READ, WIKI_WRITE};
use crate::swarm::conversation::{Conversation, OrderError, Profile, SYSTEM_TURN, Step};
use crate::upstream::generate::{GenConfig, generate, parse_request};
use crate::wiki::store::{Author, Wiki};

fn profile(agent: &str) -> Profile {
    Profile {
        agent: agent.to_owned(),
        system: format!("You are {agent}."),
        model: "claude-opus-5-5".to_owned(),
        max_tokens: 4096,
    }
}

fn text_answer(text: &str) -> AssistantMessage {
    AssistantMessage {
        id: "msg_1".to_owned(),
        model: "m".to_owned(),
        content: vec![ResponseBlock::Text {
            text: text.to_owned(),
        }],
        stop_reason: StopReason::EndTurn,
        usage: Usage {
            input_tokens: 1,
            output_tokens: 1,
        },
    }
}

fn tool_answer(id: &str) -> AssistantMessage {
    AssistantMessage {
        content: vec![
            ResponseBlock::Text {
                text: "checking".to_owned(),
            },
            ResponseBlock::ToolUse {
                id: id.to_owned(),
                name: WIKI_READ.to_owned(),
                input: json!({"page": "a-1"}),
            },
        ],
        stop_reason: StopReason::ToolUse,
        ..text_answer("")
    }
}

#[test]
fn every_turn_resends_the_whole_conversation() {
    let mut conversation = Conversation::new("s-1".to_owned(), false);
    let profile = profile("agent-001");
    let mut previous: Vec<Value> = Vec::new();
    for turn in 0..4 {
        conversation.ask(format!("prompt {turn}")).expect("idle");
        let body = conversation.body(&profile, true);
        let messages = body["messages"].as_array().expect("messages").clone();
        // The previous body's messages are a prefix of this one's.
        assert_eq!(&messages[..previous.len()], &previous[..]);
        assert_eq!(messages.len(), previous.len() + 1);
        assert_eq!(body["system"][0]["text"], "You are agent-001.");
        assert_eq!(body["tools"].as_array().map(Vec::len), Some(2));
        assert_eq!(body["stream"], true);
        assert_eq!(body["max_tokens"], 4096);
        assert_eq!(
            conversation.receive(text_answer(&format!("answer {turn}"))),
            Ok(Step::Answered)
        );
        previous = conversation.body(&profile, true)["messages"]
            .as_array()
            .expect("messages")
            .clone();
    }
    assert_eq!(conversation.prompts(), 4);
    assert_eq!(conversation.messages().len(), 8);
    let roles: Vec<Role> = conversation.messages().iter().map(|m| m.role).collect();
    assert!(
        roles
            .chunks(2)
            .all(|pair| pair == [Role::User, Role::Assistant])
    );
}

#[test]
fn tool_calls_are_answered_with_their_results() {
    let mut conversation = Conversation::new("s".to_owned(), false);
    conversation.ask("read a-1".to_owned()).expect("idle");
    let Ok(Step::Tools(pending)) = conversation.receive(tool_answer("toolu_A")) else {
        panic!("a tool call")
    };
    assert_eq!(pending.calls().len(), 1);
    assert_eq!(pending.calls()[0].id, "toolu_A");
    // A prompt now would leave the call unanswered.
    assert_eq!(
        conversation.ask("next".to_owned()),
        Err(OrderError::NotIdle)
    );
    let result = pending.calls()[0].result("page text".to_owned(), false);
    conversation
        .resolve(pending, vec![result])
        .expect("matches");
    assert!(conversation.is_followup());
    let body = conversation.body(&profile("agent-001"), false);
    let last = &body["messages"][2];
    assert_eq!(last["role"], "user");
    assert_eq!(
        last["content"],
        json!([{"type": "tool_result", "tool_use_id": "toolu_A", "content": "page text"}])
    );
    // The assistant's tool call was kept as sent.
    assert_eq!(body["messages"][1]["content"][1]["type"], "tool_use");
    assert_eq!(body["messages"][1]["content"][1]["id"], "toolu_A");
    assert_eq!(
        conversation.receive(text_answer("done")),
        Ok(Step::Answered)
    );
    assert!(!conversation.is_followup());
    assert_eq!(conversation.prompts(), 1);
}

#[test]
fn errors_are_marked_on_tool_results() {
    let mut conversation = Conversation::new("s".to_owned(), false);
    conversation.ask("read".to_owned()).expect("idle");
    let Ok(Step::Tools(pending)) = conversation.receive(tool_answer("t1")) else {
        panic!("a tool call")
    };
    let result = pending.calls()[0].result("missing".to_owned(), true);
    assert!(result.is_error());
    conversation
        .resolve(pending, vec![result])
        .expect("matches");
    let body = conversation.body(&profile("a"), true);
    assert_eq!(body["messages"][2]["content"][0]["is_error"], true);
}

#[test]
fn out_of_order_calls_are_refused() {
    let mut conversation = Conversation::new("s".to_owned(), false);
    assert_eq!(
        conversation.receive(text_answer("x")),
        Err(OrderError::NoRequest)
    );
    conversation.ask("p".to_owned()).expect("idle");
    let Ok(Step::Tools(pending)) = conversation.receive(tool_answer("t1")) else {
        panic!("a tool call")
    };
    // Results of another call do not resolve this one.
    let mut other = Conversation::new("o".to_owned(), false);
    other.ask("p".to_owned()).expect("idle");
    let Ok(Step::Tools(foreign)) = other.receive(tool_answer("t2")) else {
        panic!("a tool call")
    };
    let wrong = foreign.calls()[0].result("x".to_owned(), false);
    assert_eq!(
        conversation.resolve(pending, vec![wrong]),
        Err(OrderError::ResultMismatch)
    );
    let mut idle = Conversation::new("i".to_owned(), false);
    assert_eq!(
        idle.resolve(foreign, Vec::new()),
        Err(OrderError::NoPendingTools)
    );
}

#[test]
fn claude_code_shape_adds_a_system_turn_before_each_prompt() {
    let mut conversation = Conversation::new("s".to_owned(), true);
    conversation.ask("one".to_owned()).expect("idle");
    conversation.receive(text_answer("a")).expect("answer");
    conversation.ask("two".to_owned()).expect("idle");
    let body = conversation.body(&profile("a"), true);
    let roles: Vec<&str> = body["messages"]
        .as_array()
        .expect("messages")
        .iter()
        .map(|m| m["role"].as_str().expect("role"))
        .collect();
    assert_eq!(roles, ["system", "user", "assistant", "system", "user"]);
    assert_eq!(body["messages"][0]["content"], SYSTEM_TURN);
    assert_eq!(conversation.prompts(), 2);
}

/// Agent A writes a page with the fake model's words; agent B reads it;
/// B's next request carries A's model output verbatim in a tool result.
#[test]
fn one_agents_output_reaches_anothers_input_through_the_wiki() {
    let config = GenConfig {
        seed: 3,
        words: Span::ordered(60, 80),
        ..GenConfig::default()
    };
    let mut wiki = Wiki::new(1 << 20, 100);
    let page: PageSlug = "rate-limiting-0".parse().expect("slug");

    // Agent A: the write task, the model's wiki_write call, the wiki.
    let a = profile("agent-000");
    let mut writer = Conversation::new("sa".to_owned(), false);
    writer
        .ask(
            Task::Write {
                page: page.clone(),
                topic: 0,
            }
            .prompt(),
        )
        .expect("idle");
    let body = serde_json::to_vec(&writer.body(&a, true)).expect("encode");
    let reply = generate(&config, &parse_request(&body).expect("request"), &body);
    let Ok(Step::Tools(pending)) = writer.receive(reply.message.clone()) else {
        panic!("a write call")
    };
    let call = &pending.calls()[0];
    assert_eq!(call.name, WIKI_WRITE);
    let written = call.input["content"].as_str().expect("content").to_owned();
    wiki.put(
        page.clone(),
        written.clone(),
        Author::new(&a.agent).expect("author"),
    )
    .expect("put");
    let ack = call.result("Saved.".to_owned(), false);
    writer.resolve(pending, vec![ack]).expect("resolve");

    // Agent B: the read task, the model's wiki_read call, the page.
    let b = profile("agent-001");
    let mut reader = Conversation::new("sb".to_owned(), false);
    reader
        .ask(Task::Read { page: page.clone() }.prompt())
        .expect("idle");
    let body = serde_json::to_vec(&reader.body(&b, true)).expect("encode");
    let reply = generate(&config, &parse_request(&body).expect("request"), &body);
    let Ok(Step::Tools(pending)) = reader.receive(reply.message) else {
        panic!("a read call")
    };
    assert_eq!(pending.calls()[0].name, WIKI_READ);
    let stored = wiki.get(&page).expect("page");
    assert_eq!(stored.author.as_str(), "agent-000");
    let result = pending.calls()[0].result(stored.text.clone(), false);
    reader.resolve(pending, vec![result]).expect("resolve");

    // A's model output (in A's answer) is in B's next request body.
    let b_body = serde_json::to_string(&reader.body(&b, true)).expect("encode");
    let a_output = serde_json::to_string(&written).expect("encode");
    let a_output = a_output.trim_matches('"');
    assert!(written.split_whitespace().count() >= 60);
    assert!(b_body.contains(a_output), "B's input carries A's output");
    let b_messages = reader.body(&b, true)["messages"].clone();
    assert_eq!(b_messages[2]["content"][0]["type"], "tool_result");
    assert_eq!(b_messages[2]["content"][0]["content"], written);
}

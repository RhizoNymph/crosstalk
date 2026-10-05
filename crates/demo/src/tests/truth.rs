//! The `http_request` tool on both sides (the fake model's calls, the
//! agent running them against a real wiki), and the ground-truth book:
//! each row kind, pairing reads with writes, and the v2 schema's keys.

use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::time::Duration;

use serde_json::{Value, json};
use tokio::sync::mpsc;

use crate::anthropic::{AssistantMessage, ResponseBlock, StopReason, Usage};
use crate::http::BaseUrl;
use crate::knobs::Span;
use crate::protocol::{
    CallRefused, HTTP_TOOL, PageSlug, Task, WikiCall, page_url, read_input, tool_definitions,
    write_input,
};
use crate::swarm::agent::{Agent, Shared};
use crate::swarm::config::SwarmConfig;
use crate::swarm::conversation::{Conversation, Step, locate_result};
use crate::swarm::stats::Event;
use crate::swarm::tools::execute;
use crate::swarm::truth::{
    At, Content, EXCERPT_CHARS, ReadOutcome, ReadRecord, Reader, Row, RunInfo, TruthBook,
    WriteRecord, excerpt,
};
use crate::swarm::{RunClock, run_id, ulid_text};
use crate::upstream::generate::{GenConfig, generate, parse_request};

use super::servers::wiki;

fn slug(name: &str) -> PageSlug {
    name.parse().expect("slug")
}

fn base(text: &str) -> BaseUrl {
    text.parse().expect("url")
}

#[test]
fn page_urls_come_from_one_function() {
    let page = slug("rate-limiting-3");
    assert_eq!(
        page_url(&base("http://wiki:8090"), &page),
        "http://wiki:8090/pages/rate-limiting-3"
    );
    // A trailing slash on the base, an upper-case host and port 80 all give
    // the one canonical spelling.
    assert_eq!(
        page_url(&base("http://Wiki:80/team/"), &page),
        "http://wiki/team/pages/rate-limiting-3"
    );
    assert_eq!(
        page_url(&base("http://[::1]:9"), &page),
        "http://[::1]:9/pages/rate-limiting-3"
    );
    let wiki = base("http://wiki:8090/");
    assert_eq!(
        read_input(&wiki, &page),
        json!({"method": "GET", "url": "http://wiki:8090/pages/rate-limiting-3"})
    );
    assert_eq!(
        write_input(&wiki, &page, "text"),
        json!({"method": "PUT", "url": "http://wiki:8090/pages/rate-limiting-3", "body": "text"})
    );
    // The URL round-trips through the marker unchanged.
    for text in ["http://wiki:8090", "http://wiki/team", "http://[::1]:9/w"] {
        let url = base(text);
        assert_eq!(url.url(), text);
        assert_eq!(base(&url.url()), url);
    }
}

#[test]
fn the_one_declared_tool_is_http_request() {
    let tools = tool_definitions();
    let tools = tools.as_array().expect("tools");
    assert_eq!(tools.len(), 1);
    assert_eq!(tools[0]["name"], HTTP_TOOL);
    assert_eq!(HTTP_TOOL, "http_request");
    let schema = &tools[0]["input_schema"];
    let properties: BTreeSet<&str> = schema["properties"]
        .as_object()
        .expect("properties")
        .keys()
        .map(String::as_str)
        .collect();
    assert_eq!(properties, BTreeSet::from(["body", "method", "url"]));
    assert_eq!(schema["required"], json!(["method", "url"]));
}

#[test]
fn wiki_calls_are_read_from_http_requests() {
    let wiki = base("http://wiki:8090");
    let url = "http://wiki:8090/pages/a-1";
    assert_eq!(
        WikiCall::parse(HTTP_TOOL, &json!({"method": "GET", "url": url}), &wiki),
        Ok(WikiCall::Read { page: slug("a-1") })
    );
    assert_eq!(
        WikiCall::parse(
            HTTP_TOOL,
            &json!({"method": "put", "url": url, "body": "x"}),
            &wiki
        ),
        Ok(WikiCall::Write {
            page: slug("a-1"),
            body: "x".to_owned()
        })
    );
    let refused = |name: &str, input: Value| WikiCall::parse(name, &input, &wiki);
    assert_eq!(
        refused("wiki_read", json!({"page": "a-1"})),
        Err(CallRefused::UnknownTool("wiki_read".to_owned()))
    );
    assert_eq!(
        refused(HTTP_TOOL, json!({"method": "DELETE", "url": url})),
        Err(CallRefused::Method("DELETE".to_owned()))
    );
    assert_eq!(
        refused(HTTP_TOOL, json!({"url": url})),
        Err(CallRefused::NoMethod)
    );
    assert_eq!(
        refused(HTTP_TOOL, json!({"method": "GET"})),
        Err(CallRefused::NoUrl)
    );
    assert_eq!(
        refused(HTTP_TOOL, json!({"method": "PUT", "url": url})),
        Err(CallRefused::NoBody)
    );
    for other in [
        "http://elsewhere:8090/pages/a-1",
        "http://wiki:8090/pages/A",
        "http://wiki:8090/pages/a-1/",
        "http://wiki:8090/pages/a-1?x=1",
        "http://wiki:8090/other/a-1",
    ] {
        assert!(
            matches!(
                refused(HTTP_TOOL, json!({"method": "GET", "url": other})),
                Err(CallRefused::NotWiki { .. })
            ),
            "{other}"
        );
    }
}

fn model_config() -> GenConfig {
    GenConfig {
        seed: 4,
        words: Span::ordered(30, 50),
        first_byte_ms: Span::ordered(0, 0),
        stream_ms: Span::ordered(0, 0),
    }
}

fn ask_model(task: &Task) -> AssistantMessage {
    let body = serde_json::to_vec(&json!({
        "model": "claude-opus-5-5",
        "max_tokens": 4096,
        "tools": tool_definitions(),
        "messages": [{"role": "user", "content": [{"type": "text", "text": task.prompt()}]}],
    }))
    .expect("encode");
    generate(
        &model_config(),
        &parse_request(&body).expect("request"),
        &body,
    )
    .message
}

fn tool_call(message: &AssistantMessage) -> (&str, &Value) {
    match &message.content[1] {
        ResponseBlock::ToolUse { name, input, .. } => (name, input),
        ResponseBlock::Text { .. } => panic!("a tool call"),
    }
}

#[test]
fn the_model_echoes_the_marker_base_in_its_http_calls() {
    for wiki in ["http://wiki:8090", "http://127.0.0.1:41234/team"] {
        let wiki = base(wiki);
        let page = slug("cache-invalidation-2");
        let write = ask_model(&Task::Write {
            page: page.clone(),
            topic: 2,
            base: wiki.clone(),
        });
        assert_eq!(write.stop_reason, StopReason::ToolUse);
        let (name, input) = tool_call(&write);
        assert_eq!(name, HTTP_TOOL);
        assert_eq!(input["method"], "PUT");
        assert_eq!(input["url"], page_url(&wiki, &page));
        let body = input["body"].as_str().expect("body");
        assert!(body.split_whitespace().count() >= 30);
        assert_eq!(
            WikiCall::parse(name, input, &wiki),
            Ok(WikiCall::Write {
                page: page.clone(),
                body: body.to_owned()
            })
        );
        let read = ask_model(&Task::Read {
            page: page.clone(),
            base: wiki.clone(),
        });
        let (name, input) = tool_call(&read);
        assert_eq!(name, HTTP_TOOL);
        assert_eq!(input, &read_input(&wiki, &page));
        // Deterministic from the seed and the request.
        assert_eq!(
            read,
            ask_model(&Task::Read {
                page: page.clone(),
                base: wiki.clone()
            })
        );
    }
    // Another base changes the request, so the URL in the answer follows it.
    let a = ask_model(&Task::Read {
        page: slug("x-1"),
        base: base("http://a:1"),
    });
    let b = ask_model(&Task::Read {
        page: slug("x-1"),
        base: base("http://b:2"),
    });
    assert_eq!(tool_call(&a).1["url"], "http://a:1/pages/x-1");
    assert_eq!(tool_call(&b).1["url"], "http://b:2/pages/x-1");
}

fn answer_with_calls(calls: &[(&str, &str, Value)]) -> AssistantMessage {
    AssistantMessage {
        id: "msg_1".to_owned(),
        model: "m".to_owned(),
        content: calls
            .iter()
            .map(|(id, name, input)| ResponseBlock::ToolUse {
                id: (*id).to_owned(),
                name: (*name).to_owned(),
                input: input.clone(),
            })
            .collect(),
        stop_reason: StopReason::ToolUse,
        usage: Usage {
            input_tokens: 1,
            output_tokens: 1,
        },
    }
}

fn shared_for(wiki_addr: SocketAddr) -> Shared {
    let wiki: BaseUrl = format!("http://{wiki_addr}").parse().expect("url");
    let config = SwarmConfig::new(wiki.clone(), wiki.clone());
    Shared {
        gateway: wiki.client(wiki_addr, Duration::from_secs(5)),
        wiki: wiki.client(wiki_addr, Duration::from_secs(5)),
        config,
        clock: RunClock::start(&crosstalk_spec::support::SystemClock),
    }
}

#[tokio::test]
async fn the_agent_runs_http_requests_against_the_wiki() {
    let server = wiki().await;
    let shared = shared_for(server.addr);
    let wiki = shared.config.wiki.clone();
    let agent = Agent::new(&shared.config, 3);
    let (events, mut inbox) = mpsc::channel(64);
    let page = slug("release-plan-8");
    let url = page_url(&wiki, &page);

    let mut conversation = Conversation::new("s-1".to_owned(), false);
    conversation.ask("go".to_owned()).expect("idle");
    let answer = answer_with_calls(&[
        ("t-missing", HTTP_TOOL, read_input(&wiki, &page)),
        (
            "t-put",
            HTTP_TOOL,
            write_input(&wiki, &page, "page text v1"),
        ),
        ("t-get", HTTP_TOOL, read_input(&wiki, &page)),
        ("t-tool", "wiki_read", json!({"page": "release-plan-8"})),
        (
            "t-method",
            HTTP_TOOL,
            json!({"method": "DELETE", "url": url}),
        ),
        (
            "t-elsewhere",
            HTTP_TOOL,
            json!({"method": "GET", "url": "http://example.com/pages/release-plan-8"}),
        ),
        ("t-nobody", HTTP_TOOL, json!({"method": "PUT", "url": url})),
    ]);
    let Ok(Step::Tools(pending)) = conversation.receive(answer) else {
        panic!("tool calls")
    };
    let (results, reads) = execute(&agent, &shared, &events, "s-1", 7, &pending).await;
    drop(events);

    let shown: Vec<(&str, bool)> = results
        .iter()
        .map(|r| (r.content(), r.is_error()))
        .collect();
    assert_eq!(
        shown[0],
        ("Page `release-plan-8` does not exist yet.", true)
    );
    assert_eq!(
        shown[1],
        ("Saved `release-plan-8` (version 1, 12 bytes).", false)
    );
    assert_eq!(shown[2], ("page text v1", false));
    assert_eq!(shown[3], ("Error: no tool named wiki_read.", true));
    assert!(
        shown[4].1 && shown[4].0.contains("DELETE"),
        "{:?}",
        shown[4]
    );
    assert!(
        shown[5].1 && shown[5].0.contains("not a page"),
        "{:?}",
        shown[5]
    );
    assert_eq!(shown[6], ("Error: a PUT needs a text `body`.", true));

    // The write is reported at once, with the turn of the answer that made it.
    let mut writes = Vec::new();
    while let Some(event) = inbox.recv().await {
        match event {
            Event::WikiWrite(write) => writes.push(write),
            other => panic!("unexpected event {other:?}"),
        }
    }
    assert_eq!(writes.len(), 1);
    let write = &writes[0];
    assert_eq!(
        (
            write.writer.as_str(),
            write.key_group,
            write.session.as_str(),
            write.turn,
            write.tool_use_id.as_str(),
            write.page.as_str(),
            write.version
        ),
        ("agent-003", 3, "s-1", 7, "t-put", "release-plan-8", 1)
    );
    assert!(write.written_at_unix_ms >= shared.clock.started_at_unix_ms());

    // The reads wait for the request that carries them.
    assert_eq!(reads.len(), 2);
    assert_eq!(reads[0].tool_use_id, "t-missing");
    assert_eq!(reads[0].found, None);
    assert_eq!(reads[1].tool_use_id, "t-get");
    assert_eq!(reads[1].found, Some(("agent-003".to_owned(), 1)));
    assert_eq!(reads[1].url, url);
    assert_eq!(reads[1].input, read_input(&wiki, &page));
    assert_eq!(
        reads[1].read_at_unix_ms,
        shared.clock.started_at_unix_ms() + reads[1].at_ms
    );
}

#[test]
fn results_are_located_in_the_body_as_sent() {
    for shape in [false, true] {
        let mut conversation = Conversation::new("s".to_owned(), shape);
        conversation.ask("one".to_owned()).expect("idle");
        let Ok(Step::Tools(pending)) = conversation.receive(answer_with_calls(&[
            ("a", HTTP_TOOL, json!({})),
            ("b", HTTP_TOOL, json!({})),
        ])) else {
            panic!("tool calls")
        };
        let results = vec![
            pending.calls()[0].result("first".to_owned(), true),
            pending.calls()[1].result("second ü".to_owned(), false),
        ];
        conversation.resolve(pending, results).expect("resolve");
        let profile = crate::swarm::conversation::Profile {
            agent: "a".to_owned(),
            system: "s".to_owned(),
            model: "m".to_owned(),
            max_tokens: 10,
        };
        let body = conversation.body(&profile, true);
        let found = locate_result(&body, "b").expect("located");
        // The system turn shifts every index when present.
        let message = if shape { 3 } else { 2 };
        assert_eq!((found.message, found.block), (message, 1));
        assert_eq!(found.content, "second ü");
        assert_eq!(body["messages"][message]["content"][1]["tool_use_id"], "b");
        assert!(locate_result(&body, "zzz").is_none());
    }
}

#[test]
fn turns_count_every_request_claimed() {
    let mut conversation = Conversation::new("s".to_owned(), false);
    assert_eq!(conversation.turns_sent(), 0);
    assert_eq!(conversation.claim_turn(), 0);
    assert_eq!(conversation.claim_turn(), 1);
    assert_eq!(conversation.claim_turn(), 2);
    assert_eq!(conversation.turns_sent(), 3);
}

#[test]
fn excerpts_are_short_distinct_substrings() {
    assert_eq!(excerpt("short page"), "short page");
    let long: String = (0..60).map(|i| format!("word{i} ")).collect();
    let piece = excerpt(&long);
    assert!(piece.chars().count() <= EXCERPT_CHARS);
    assert!(piece.chars().count() >= EXCERPT_CHARS - 8);
    assert!(long.contains(&piece));
    assert!(!long.starts_with(&piece), "skips the opening");
    assert!(piece.starts_with("word"), "starts at a word: {piece}");
    let wide = "ü".repeat(300);
    assert_eq!(excerpt(&wide).chars().count(), EXCERPT_CHARS);
}

#[test]
fn content_digests_cover_the_exact_bytes() {
    let at = At {
        message: 2,
        block: 0,
        tool_use_id: "t".to_owned(),
    };
    let content = Content::of("abc", at.clone());
    assert_eq!(
        content.sha256,
        "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
    );
    assert_eq!(content.blake3, blake3::hash(b"abc").to_hex().to_string());
    assert_eq!(content.at, at);
}

#[test]
fn run_ids_are_ulids_of_the_start_and_seed() {
    let id = run_id(42, 1_790_000_000_000).expect("id");
    assert_eq!(id.len(), 26);
    assert_eq!(id, run_id(42, 1_790_000_000_000).expect("id"));
    assert_ne!(id, run_id(43, 1_790_000_000_000).expect("id"));
    assert_ne!(id, run_id(42, 1_790_000_000_001).expect("id"));
    // The time part is the start in milliseconds.
    let time = &id[..10];
    assert_eq!(
        time,
        &ulid_text(u128::from(1_790_000_000_000u64) << 80)[..10]
    );
    assert!(
        id.chars()
            .all(|c| c.is_ascii_digit() || c.is_ascii_uppercase())
    );
}

fn info() -> RunInfo {
    RunInfo {
        run: "01J0000000000000000000000A".to_owned(),
        seed: 42,
        agents: 5,
        keys: 3,
        agents_per_key: 2,
        claude_code_shape: true,
        started_at_unix_ms: 1_000,
        gateway_url: "http://crosstalk:8080/anthropic".to_owned(),
        wiki_url: "http://wiki:8090".to_owned(),
    }
}

fn write(writer: &str, group: u32, page: &str, version: u64) -> WriteRecord {
    WriteRecord {
        writer: writer.to_owned(),
        key_group: group,
        session: format!("w-{writer}"),
        turn: 3,
        tool_use_id: format!("toolu_w{version}"),
        page: slug(page),
        version,
        written_at_unix_ms: 1_100,
    }
}

fn read(reader: &str, session: &str, page: &str, found: Option<(&str, u64)>) -> ReadRecord {
    let wiki = base("http://wiki:8090");
    ReadRecord {
        by: Reader {
            reader: reader.to_owned(),
            key_group: 1,
            session: session.to_owned(),
            turn: 5,
            tool_use_id: "toolu_r".to_owned(),
            page: slug(page),
            url: page_url(&wiki, &slug(page)),
            input: read_input(&wiki, &slug(page)),
            at_ms: 250,
            read_at_unix_ms: 1_250,
        },
        outcome: match found {
            None => ReadOutcome::Missing,
            Some((author, version)) => ReadOutcome::Found {
                author: author.to_owned(),
                version,
                content: Content::of(
                    "the page",
                    At {
                        message: 7,
                        block: 1,
                        tool_use_id: "toolu_r".to_owned(),
                    },
                ),
            },
        },
    }
}

fn kind(row: &Row) -> &'static str {
    match row {
        Row::Header(_) => "header",
        Row::Transmission(_) => "transmission",
        Row::SelfRead(_) => "self_read",
        Row::Reread(_) => "reread",
        Row::Miss(_) => "miss",
        Row::UnattributedRead(_) => "unattributed_read",
        Row::AgentCluster(_) => "agent_cluster",
    }
}

#[test]
fn the_book_classifies_every_read() {
    let mut book = TruthBook::new(info().world());
    assert!(book.write(write("agent-000", 0, "p-1", 1)).is_empty());
    let first = book.read(read("agent-002", "s-a", "p-1", Some(("agent-000", 1))));
    assert_eq!(first.as_ref().map(kind), Some("transmission"));
    // The same version again in the same session: a reread.
    let again = book.read(read("agent-002", "s-a", "p-1", Some(("agent-000", 1))));
    assert_eq!(again.as_ref().map(kind), Some("reread"));
    // In a new session it is a transmission again.
    let fresh = book.read(read("agent-002", "s-b", "p-1", Some(("agent-000", 1))));
    assert_eq!(fresh.as_ref().map(kind), Some("transmission"));
    // The writer reading its own version.
    let own = book.read(read("agent-000", "s-c", "p-1", Some(("agent-000", 1))));
    assert_eq!(own.as_ref().map(kind), Some("self_read"));
    let own_again = book.read(read("agent-000", "s-c", "p-1", Some(("agent-000", 1))));
    assert_eq!(own_again.as_ref().map(kind), Some("self_read"));
    let missing = book.read(read("agent-002", "s-a", "p-9", None));
    assert_eq!(missing.as_ref().map(kind), Some("miss"));
    // A read that arrives before its write waits for it, keeping its order.
    assert!(
        book.read(read("agent-001", "s-d", "p-1", Some(("agent-004", 2))))
            .is_none()
    );
    assert!(
        book.read(read("agent-001", "s-d", "p-1", Some(("agent-004", 2))))
            .is_none()
    );
    let released = book.write(write("agent-004", 2, "p-1", 2));
    assert_eq!(
        released.iter().map(kind).collect::<Vec<_>>(),
        ["transmission", "reread"]
    );
    // A version never reported written stays unattributed.
    assert!(
        book.read(read("agent-001", "s-d", "p-1", Some(("agent-003", 9))))
            .is_none()
    );
    book.end_session("s-a");
    let after_end = book.read(read("agent-002", "s-a", "p-1", Some(("agent-000", 1))));
    assert_eq!(after_end.as_ref().map(kind), Some("transmission"));
    let unattributed = book.finish();
    assert_eq!(
        unattributed.iter().map(kind).collect::<Vec<_>>(),
        ["unattributed_read"]
    );
    let Some(Row::UnattributedRead(left)) = unattributed.first() else {
        panic!("an unattributed read")
    };
    assert_eq!(
        (left.reader.as_str(), left.page.as_str(), left.version),
        ("agent-001", "p-1", 9)
    );
    assert_eq!(left.reader_session, "s-d");
    assert_eq!(left.at_unix_ms, 1_250);
    // Finishing again finds nothing left.
    assert!(book.finish().is_empty());
    let counts = book.counts();
    assert_eq!(
        (
            counts.transmissions,
            counts.rereads,
            counts.self_reads,
            counts.misses,
            counts.unattributed
        ),
        (4, 2, 2, 1, 1)
    );
    assert_eq!(book.pairs(), 2);

    let Some(Row::Transmission(row)) = first else {
        panic!("a transmission")
    };
    assert_eq!(row.writer_session, "w-agent-000");
    assert_eq!(row.writer_turn, 3);
    assert_eq!(row.writer_tool_use_id, "toolu_w1");
    assert_eq!(row.writer_key_group, 0);
    assert_eq!(row.reader_key_group, 1);
    assert_eq!(row.at_unix_ms, row.read_at_unix_ms);
    assert_eq!(row.written_at_unix_ms, 1_100);
}

fn keys(value: &Value) -> BTreeSet<&str> {
    value
        .as_object()
        .expect("object")
        .keys()
        .map(String::as_str)
        .collect()
}

/// Every kind serialises with exactly the agreed keys, `kind` first, in the
/// agreed order.
#[test]
fn rows_have_exactly_the_v2_keys() {
    let delivered_keys = [
        "kind",
        "world",
        "writer",
        "reader",
        "page",
        "version",
        "writer_key_group",
        "reader_key_group",
        "writer_session",
        "writer_turn",
        "writer_tool_use_id",
        "reader_session",
        "reader_turn",
        "reader_tool_use_id",
        "route",
        "carrier",
        "read_tool",
        "content",
        "at_ms",
        "at_unix_ms",
        "written_at_unix_ms",
        "read_at_unix_ms",
    ];
    let opening = Row::opening(&info(), |i| format!("agent-{i:03}"));
    let encode = |row: &Row| -> Value {
        let text = serde_json::to_string(row).expect("encode");
        // serde_json keeps insertion order only with preserve_order; check the
        // text order directly.
        let value: Value = serde_json::from_str(&text).expect("json");
        let mut order: Vec<(usize, String)> = value
            .as_object()
            .expect("object")
            .keys()
            .map(|k| (text.find(&format!("\"{k}\":")).expect("key"), k.clone()))
            .collect();
        order.sort();
        let ordered: Vec<String> = order.into_iter().map(|(_, k)| k).collect();
        json!({"value": value, "order": ordered})
    };
    let header = encode(&opening[0]);
    assert_eq!(
        header["order"],
        json!([
            "kind",
            "version",
            "world",
            "run",
            "seed",
            "agents",
            "keys",
            "agents_per_key",
            "claude_code_shape",
            "started_at_unix_ms",
            "gateway_url",
            "wiki_url"
        ])
    );
    assert_eq!(header["value"]["kind"], "header");
    assert_eq!(header["value"]["version"], 2);
    assert_eq!(header["value"]["world"], "swarm-01J0000000000000000000000A");
    // One cluster per key group, singletons included.
    let clusters: Vec<Value> = opening[1..]
        .iter()
        .map(|r| encode(r)["value"].clone())
        .collect();
    assert_eq!(
        clusters,
        [
            json!({"kind": "agent_cluster", "world": "swarm-01J0000000000000000000000A", "key_group": 0, "agents": ["agent-000", "agent-001"]}),
            json!({"kind": "agent_cluster", "world": "swarm-01J0000000000000000000000A", "key_group": 1, "agents": ["agent-002", "agent-003"]}),
            json!({"kind": "agent_cluster", "world": "swarm-01J0000000000000000000000A", "key_group": 2, "agents": ["agent-004"]}),
        ]
    );

    let mut book = TruthBook::new(info().world());
    book.write(write("agent-000", 0, "p-1", 1));
    let rows = [
        book.read(read("agent-002", "s", "p-1", Some(("agent-000", 1)))),
        book.read(read("agent-002", "s", "p-1", Some(("agent-000", 1)))),
        book.read(read("agent-000", "s2", "p-1", Some(("agent-000", 1)))),
    ];
    for (row, name) in rows.iter().zip(["transmission", "reread", "self_read"]) {
        let encoded = encode(row.as_ref().expect("row"));
        assert_eq!(encoded["order"], json!(delivered_keys), "{name}");
        let value = &encoded["value"];
        assert_eq!(value["kind"], name);
        assert_eq!(
            value["route"],
            json!({"kind": "channel", "url": "http://wiki:8090/pages/p-1"})
        );
        assert_eq!(value["carrier"], "tool_result");
        assert_eq!(
            value["read_tool"],
            json!({"name": "http_request", "input": {"method": "GET", "url": "http://wiki:8090/pages/p-1"}})
        );
        assert_eq!(
            keys(&value["content"]),
            BTreeSet::from(["at", "blake3", "excerpt", "sha256"])
        );
        assert_eq!(
            value["content"]["at"],
            json!({"message": 7, "block": 1, "tool_use_id": "toolu_r"})
        );
    }
    let miss = book.read(read("agent-002", "s", "p-2", None)).expect("row");
    let encoded = encode(&miss);
    assert_eq!(
        encoded["order"],
        json!([
            "kind",
            "world",
            "reader",
            "reader_key_group",
            "page",
            "reader_session",
            "reader_turn",
            "reader_tool_use_id",
            "read_tool",
            "at_ms",
            "at_unix_ms"
        ])
    );
    assert_eq!(encoded["value"]["kind"], "miss");

    // A read whose write this run never saw.
    assert!(
        book.read(read("agent-002", "s", "p-3", Some(("agent-009", 5))))
            .is_none()
    );
    let left = book.finish();
    let encoded = encode(left.first().expect("unattributed row"));
    assert_eq!(
        encoded["order"],
        json!([
            "kind",
            "world",
            "reader",
            "reader_key_group",
            "page",
            "version",
            "reader_session",
            "reader_turn",
            "reader_tool_use_id",
            "read_tool",
            "content",
            "at_ms",
            "at_unix_ms"
        ])
    );
    let value = &encoded["value"];
    assert_eq!(value["kind"], "unattributed_read");
    assert_eq!(value["version"], 5);
    assert_eq!(
        keys(&value["content"]),
        BTreeSet::from(["at", "blake3", "excerpt", "sha256"])
    );
}

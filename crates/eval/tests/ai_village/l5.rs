//! The agreed L5 `HttpTool` contract, write outcomes and the JSON string
//! codec: what an extractor would make of the village's raw bash accesses.

use crosstalk_eval::datasets::ai_village::access::{
    Access, HttpMethod, HttpRequest, Op, Shell, Tool,
};
use crosstalk_eval::datasets::ai_village::resource::{canonical, from_url};
use crosstalk_eval::datasets::ai_village::text::need;
use crosstalk_eval::datasets::ai_village::time::{Day, parse_timestamp};
use crosstalk_eval::datasets::ai_village::window::repo::{AccessLog, TurnRef};
use crosstalk_eval::truth::MatchNeed;
use crosstalk_eval::truth::kinds::locator_key;
use crosstalk_flow::extract::{
    ConversationContext, ExtractConfig, ExtractedOp, ToolExtractors, WriteOutcome,
};
use crosstalk_spec::derived::provenance::matching::Codec;
use crosstalk_spec::observed::message::{
    AssistantPart, CanonicalJson, Message, MessageBody, Text, ToolArguments, ToolCall, ToolCallId,
    ToolExecution, ToolName, ToolOutcome, ToolResult, ToolResultContent,
};

fn one(shell: &mut Shell, command: &str, output: &str) -> Access {
    let mut accesses = shell.accesses(command, output);
    assert_eq!(accesses.len(), 1, "{command}: {accesses:?}");
    accesses.remove(0)
}

fn request(access: &Access) -> &HttpRequest {
    access
        .http
        .as_ref()
        .unwrap_or_else(|| panic!("no HTTP equivalent: {access:?}"))
}

#[test]
fn curl_and_wget_follow_the_http_tool_contract() {
    let mut shell = Shell::default();
    let get = one(
        &mut shell,
        "curl -sL 'https://pages.example/notes?b=2&a=1'",
        "notes",
    );
    assert_eq!(get.op, Op::Read);
    assert_eq!(request(&get).method, HttpMethod::Get);
    assert_eq!(request(&get).url, "https://pages.example/notes?b=2&a=1");
    assert_eq!(request(&get).body, None);

    let head = one(
        &mut shell,
        "curl -I https://pages.example/notes",
        "HTTP/2 200",
    );
    assert_eq!(head.op, Op::Read);
    assert_eq!(request(&head).method, HttpMethod::Head);

    // Data without a method is a POST; the body is what was written.
    let post = one(
        &mut shell,
        "curl -s https://api.example.com/notes -d '{\"text\":\"meet at the old mill at nine\"}'",
        "{\"id\": 4}",
    );
    assert!(matches!(post.op, Op::Write(_)));
    assert_eq!(request(&post).method, HttpMethod::Post);
    assert_eq!(
        request(&post).body.as_deref(),
        Some("{\"text\":\"meet at the old mill at nine\"}")
    );
    // Several data flags are joined the way curl joins them.
    let form = one(
        &mut shell,
        "curl -X PUT https://api.example.com/notes/4 -d a=1 -d b=2",
        "",
    );
    assert_eq!(request(&form).method, HttpMethod::Put);
    assert_eq!(request(&form).body.as_deref(), Some("a=1&b=2"));
    let patch = one(
        &mut shell,
        "curl --request patch https://api.example.com/notes/4 --data-raw x=1",
        "",
    );
    assert_eq!(request(&patch).method, HttpMethod::Patch);
    let delete = one(
        &mut shell,
        "curl -X DELETE https://api.example.com/notes/4",
        "",
    );
    assert!(matches!(delete.op, Op::Write(_)));
    assert_eq!(request(&delete).method, HttpMethod::Delete);
    assert_eq!(request(&delete).body, None);
    // -G sends the data as a query: a read.
    let query = one(
        &mut shell,
        "curl -G https://api.example.com/notes -d q=1",
        "",
    );
    assert_eq!(query.op, Op::Read);
    assert_eq!(request(&query).method, HttpMethod::Get);
    // A method outside the contract is no access.
    assert!(
        shell
            .accesses("curl -X OPTIONS https://api.example.com/notes", "")
            .is_empty()
    );

    let fetched = one(&mut shell, "wget -qO- https://pages.example/a.txt", "text");
    assert_eq!(fetched.tool, Tool::Wget);
    assert_eq!(request(&fetched).method, HttpMethod::Get);
    let posted = one(
        &mut shell,
        "wget --post-data='note=left under the bridge' https://api.example.com/notes",
        "",
    );
    assert_eq!(request(&posted).method, HttpMethod::Post);
    assert_eq!(
        request(&posted).body.as_deref(),
        Some("note=left under the bridge")
    );
    assert_eq!(
        posted.payload,
        vec!["note=left under the bridge".to_owned()]
    );
}

#[test]
fn forge_api_commands_have_http_equivalents_and_repository_commands_do_not() {
    let mut shell = Shell::default();
    let api = one(
        &mut shell,
        "gh api repos/o/r/issues/5/comments -f body='The parser now handles nested quotes'",
        "{\"html_url\": \"https://github.com/o/r/issues/5#issuecomment-1\"}",
    );
    assert!(matches!(api.op, Op::Write(_)));
    assert_eq!(request(&api).method, HttpMethod::Post);
    assert_eq!(
        request(&api).url,
        "https://api.github.com/repos/o/r/issues/5/comments"
    );
    assert_eq!(
        api.payload,
        vec!["The parser now handles nested quotes".to_owned()]
    );
    assert_eq!(locator_key(&api.resource), "https://github.com/o/r");
    let read = one(&mut shell, "glab api projects/g%2Fp/issues", "[]");
    assert_eq!(read.op, Op::Read);
    assert_eq!(
        request(&read).url,
        "https://gitlab.com/api/v4/projects/g%2Fp/issues"
    );

    // git and the issue commands go through their own protocols: only a
    // Bash extractor sees them.
    let pushed = one(
        &mut shell,
        "git push https://github.com/o/r.git main",
        "To https://github.com/o/r.git\n   1..2  main -> main",
    );
    assert_eq!(pushed.http, None);
    let commented = one(
        &mut shell,
        "gh issue comment 5 -R o/r --body 'Fixed in the latest push'",
        "https://github.com/o/r/issues/5#issuecomment-2",
    );
    assert_eq!(commented.http, None);
}

fn http_call(request: &HttpRequest) -> ToolCall {
    request.tool_call(ToolCallId("toolu_1".to_owned()))
}

fn ok(text: &str) -> ToolResult {
    ToolResult {
        call_id: ToolCallId("toolu_1".to_owned()),
        content: vec![ToolResultContent::Text(Text(text.to_owned()))],
        outcome: ToolOutcome::Success,
    }
}

#[test]
fn the_l5_extractor_meets_the_converter_on_one_resource() {
    let config = ExtractConfig::default();
    let context = ConversationContext::default();
    let extractors = ToolExtractors::new(&config, &context);
    let mut shell = Shell::default();
    for (command, write) in [
        ("curl -s 'https://pages.example/notes?b=2&a=1'", false),
        (
            "curl -s https://WWW.Pages.Example:443/x/./y/../z#top",
            false,
        ),
        ("curl -X POST https://api.example.com/notes -d 'x=1'", true),
        (
            "curl https://raw.githubusercontent.com/O/R/main/README.md",
            false,
        ),
        ("curl https://github.com/o/r/blob/main/src/lib.rs", false),
        ("curl https://o.github.io/r/index.html", false),
        ("curl https://api.github.com/repos/o/r/issues/5", false),
        ("gh api -X PATCH repos/o/r/issues/5 -f state=closed", true),
        ("curl https://gitlab.com/g/s/p/-/raw/main/a.md", false),
    ] {
        let access = one(&mut shell, command, "{}");
        let call = http_call(request(&access));
        assert_eq!(call.name, ToolName("http_request".to_owned()));
        let extracted = extractors
            .extract_classified(&call, Some(&ok("{}")))
            .unwrap_or_else(|e| panic!("{command}: {e:?}"));
        assert_eq!(extracted.len(), 1, "{command}: {extracted:?}");
        assert_eq!(
            matches!(extracted[0].op, ExtractedOp::Write(_)),
            write,
            "{command}"
        );
        // The label's resource is the converter's canonical form of the
        // extractor's locator.
        assert_eq!(
            canonical(&extracted[0].locator).as_ref(),
            Some(&access.resource),
            "{command}: {:?}",
            extracted[0].locator
        );
    }
    // A URL off the forges is the extractor's locator itself.
    let access = one(&mut shell, "curl 'https://Pages.example/a?b=2&a=1#x'", "");
    let extracted = extractors
        .extract_classified(&http_call(request(&access)), Some(&ok("")))
        .unwrap_or_else(|e| panic!("{e:?}"));
    assert_eq!(extracted[0].locator, access.resource);
    assert_eq!(
        from_url("https://pages.example/a?a=1&b=2"),
        Some(access.resource.clone())
    );
}

#[test]
fn write_outcomes_are_judged_from_the_output() {
    let mut shell = Shell::default();
    let rejected = one(
        &mut shell,
        "git push https://github.com/o/r.git main",
        "To https://github.com/o/r.git\n ! [rejected]        main -> main (fetch first)\nerror: failed to push some refs",
    );
    assert_eq!(rejected.op, Op::Write(WriteOutcome::Rejected));
    let delivered = one(
        &mut shell,
        "git push https://github.com/o/r.git main",
        "To https://github.com/o/r.git\n   1a2b3c4..5d6e7f8  main -> main",
    );
    assert_eq!(delivered.op, Op::Write(WriteOutcome::Delivered));
    let denied = one(
        &mut shell,
        "gh issue comment 5 -R o/r --body 'This one did not go through at all'",
        "GraphQL: Could not resolve to an issue or pull request with the number of 5.",
    );
    assert_eq!(denied.op, Op::Write(WriteOutcome::Rejected));
    let commented = one(
        &mut shell,
        "gh issue comment 5 -R o/r --body 'This one went through just fine'",
        "https://github.com/o/r/issues/5#issuecomment-3",
    );
    assert_eq!(commented.op, Op::Write(WriteOutcome::Delivered));
    let unknown = one(
        &mut shell,
        "curl -s -X POST https://api.example.com/notes -d x=1",
        "",
    );
    assert_eq!(unknown.op, Op::Write(WriteOutcome::Unknown));
    let refused = one(
        &mut shell,
        "curl -s -X POST https://api.github.com/repos/o/r/issues -d '{}'",
        "{\n  \"message\": \"Bad credentials\",\n  \"status\": \"401\"\n}",
    );
    assert_eq!(refused.op, Op::Write(WriteOutcome::Rejected));
    let curl_error = one(
        &mut shell,
        "curl -X POST https://down.example/notes -d x=1",
        "curl: (6) Could not resolve host: down.example",
    );
    assert_eq!(curl_error.op, Op::Write(WriteOutcome::Rejected));
    // A read needs a delivered result.
    assert!(
        shell
            .accesses(
                "git clone https://github.com/o/missing.git",
                "fatal: repository 'https://github.com/o/missing.git/' not found"
            )
            .is_empty()
    );
    assert!(
        shell
            .accesses(
                "curl https://down.example/x",
                "curl: (7) Failed to connect to down.example port 443"
            )
            .is_empty()
    );
}

#[test]
fn rejected_writes_never_pair() {
    let day = Day::parse("2026-07-13").unwrap_or_else(|e| panic!("{e}"));
    let at = |text: &str| parse_timestamp(text).unwrap_or_else(|e| panic!("{e}"));
    let mut log = AccessLog::default();
    let turn = |agent: &'static str, id: &'static str, when: &str| TurnRef {
        agent,
        turn: id,
        session: agent,
        at: at(when),
        day,
    };
    log.command(
        turn("alice", "t1", "2026-07-13 16:00:00"),
        "gh issue comment 5 -R o/r --body 'An update that was refused by the forge'",
        "HTTP 403: Resource not accessible by integration",
    );
    log.command(
        turn("bob", "t2", "2026-07-13 16:05:00"),
        "gh issue view 5 -R o/r",
        "title: Parser\nAn update that was refused by the forge",
    );
    log.command(
        turn("alice", "t3", "2026-07-13 16:10:00"),
        "gh issue comment 6 -R o/r --body 'A second update that the forge accepted'",
        "https://github.com/o/r/issues/6#issuecomment-1",
    );
    log.command(
        turn("bob", "t4", "2026-07-13 16:15:00"),
        "gh issue view 6 -R o/r",
        "A second update that the forge accepted",
    );
    log.finish();
    assert_eq!(log.stats.writes, 2);
    assert_eq!(log.stats.rejected_writes, 1);
    assert_eq!(log.pairs.len(), 1);
    assert_eq!(log.records[log.pairs[0].write].turn, "t3");
    assert_eq!(log.records[log.pairs[0].read].turn, "t4");
    assert_eq!(log.stats.http_visible, 0);
    assert_eq!(log.stats.bash_only, 4);
}

fn assistant(parts: Vec<AssistantPart>) -> Message {
    Message::new(MessageBody::Assistant(parts))
}

#[test]
fn escaped_reads_need_the_json_string_codec() {
    let content = "He said \"meet at nine\"\nby the mill";
    let escaped = "He said \\\"meet at nine\\\"\\nby the mill";
    // The sender wrote the text itself: the reader's escaped bytes decode
    // to it.
    let text = assistant(vec![AssistantPart::Text(Text(content.to_owned()))]);
    assert_eq!(
        need(&text, escaped),
        MatchNeed::Decoded {
            codecs: vec![Codec::JsonString]
        }
    );
    // The sender's tool call carries the same escaped bytes: exact.
    let call = assistant(vec![AssistantPart::ToolCall(ToolCall {
        id: ToolCallId("c".to_owned()),
        name: ToolName("send_message_back_to_chat".to_owned()),
        arguments: ToolArguments::Json(CanonicalJson(format!("{{\"message\":\"{escaped}\"}}"))),
        execution: ToolExecution::Client,
        signature: None,
    })]);
    assert_eq!(need(&call, escaped), MatchNeed::Exact);
    // Nothing to unescape: exact or not, never the codec.
    let plain = assistant(vec![AssistantPart::Text(Text("meet at nine".to_owned()))]);
    assert_eq!(need(&plain, "meet at nine"), MatchNeed::Exact);
    assert_eq!(need(&plain, "MEET  at nine"), MatchNeed::Normalized);
    assert_eq!(need(&plain, "an unrelated sentence"), MatchNeed::Semantic);
}

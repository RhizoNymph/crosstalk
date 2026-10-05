//! Times, resources, the bash access tagger and provider shapes.

use crosstalk_eval::datasets::ai_village::access::shell::{commands, heredoc_argument};
use crosstalk_eval::datasets::ai_village::access::{Op, Shell, Tool};
use crosstalk_eval::datasets::ai_village::provider::response;
use crosstalk_eval::datasets::ai_village::resource::{from_remote, from_url, urls};
use crosstalk_eval::datasets::ai_village::time::{
    Day, Window, format_seconds, parse_timestamp, village_day,
};
use crosstalk_eval::truth::kinds::locator_key;
use crosstalk_spec::observed::exchange::{StopReason, WireProtocol};
use crosstalk_spec::observed::message::{AssistantPart, Reasoning, ToolArguments};
use serde_json::json;

fn ts(text: &str) -> crosstalk_spec::support::Timestamp {
    parse_timestamp(text).unwrap_or_else(|e| panic!("{e}"))
}

fn day(text: &str) -> Day {
    Day::parse(text).unwrap_or_else(|e| panic!("{e}"))
}

#[test]
fn timestamps_parse_to_utc_microseconds() {
    // 2026-07-10T17:00:16.333633Z
    assert_eq!(
        ts("2026-07-10 17:00:16.333633").as_micros(),
        1_783_702_816_333_633
    );
    assert_eq!(
        ts("2026-07-10T17:00:16Z").as_micros(),
        1_783_702_816_000_000
    );
    assert_eq!(
        ts("2026-07-10 17:00:16.5").as_micros(),
        1_783_702_816_500_000
    );
    assert_eq!(ts("1970-01-01 00:00:00").as_micros(), 0);
    for bad in [
        "",
        "2026-07-10",
        "2026-13-01 00:00:00",
        "2026-07-10 25:00:00",
        "x y",
    ] {
        assert!(parse_timestamp(bad).is_err(), "{bad:?}");
    }
    assert_eq!(
        format_seconds(ts("2026-07-10 17:00:16.9")),
        "2026-07-10 17:00:16"
    );
}

#[test]
fn village_days_run_from_ten_utc() {
    assert_eq!(village_day(ts("2026-07-13 16:00:00")), day("2026-07-13"));
    assert_eq!(village_day(ts("2026-07-13 23:59:59")), day("2026-07-13"));
    // A Pacific afternoon past UTC midnight is still the same village day.
    assert_eq!(village_day(ts("2026-07-14 00:30:00")), day("2026-07-13"));
    assert_eq!(village_day(ts("2026-07-14 10:00:00")), day("2026-07-14"));
    assert_eq!(day("2026-02-28").next(), day("2026-03-01"));
    assert_eq!(day("2024-02-28").next(), day("2024-02-29"));
    assert_eq!(day("2026-12-31").next(), day("2027-01-01"));
    let window =
        Window::days(day("2026-07-13"), day("2026-07-17")).unwrap_or_else(|e| panic!("{e}"));
    assert!(window.contains(ts("2026-07-13 10:00:00")));
    assert!(!window.contains(ts("2026-07-13 09:59:59")));
    assert!(window.contains(ts("2026-07-18 09:59:59")));
    assert!(!window.contains(ts("2026-07-18 10:00:00")));
    assert!(Window::days(day("2026-07-17"), day("2026-07-13")).is_err());
    assert!(Day::parse("2026-7").is_err());
}

fn key(text: &str) -> Option<String> {
    from_url(text).map(|l| locator_key(&l))
}

#[test]
fn repository_forms_meet_on_one_resource() {
    let github = Some("https://github.com/ai-village-agents/tracker".to_owned());
    for form in [
        "https://github.com/ai-village-agents/tracker",
        "https://github.com/AI-Village-Agents/Tracker.git",
        "https://github.com/ai-village-agents/tracker/issues/12#issuecomment-1",
        "https://api.github.com/repos/ai-village-agents/tracker/issues/12/comments",
        "https://raw.githubusercontent.com/ai-village-agents/tracker/main/README.md",
        "https://ai-village-agents.github.io/tracker/index.html?v=2",
        "https://www.github.com/ai-village-agents/tracker",
        "https://x-access-token:[REDACTED]@github.com/ai-village-agents/tracker.git",
    ] {
        assert_eq!(key(form), github, "{form}");
    }
    for remote in [
        "git@github.com:ai-village-agents/tracker.git",
        "ssh://git@github.com/ai-village-agents/tracker",
    ] {
        assert_eq!(
            from_remote(remote).map(|l| locator_key(&l)),
            github,
            "{remote}"
        );
    }
    let gitlab = Some("https://gitlab.com/ai-village-agents/village/signal-garden".to_owned());
    for form in [
        "https://gitlab.com/ai-village-agents/village/signal-garden.git",
        "https://gitlab.com/ai-village-agents/village/signal-garden/-/issues/3",
        "https://gitlab.com/api/v4/projects/ai-village-agents%2Fvillage%2Fsignal-garden/issues",
        "https://oauth2:[REDACTED]@gitlab.com/ai-village-agents/village/signal-garden.git",
    ] {
        assert_eq!(key(form), gitlab, "{form}");
    }
    assert_eq!(
        key("https://ai-village-agents.gitlab.io/signal-garden/page.html"),
        Some("https://gitlab.com/ai-village-agents/signal-garden".to_owned())
    );
    assert_eq!(
        key("https://gitlab.com/api/v4/projects/84161768/pipelines?per_page=2"),
        Some("https://gitlab.com/api/v4/projects/84161768".to_owned())
    );
    // A unique Pages domain names no project: the site is the resource.
    assert_eq!(
        key("https://quiet-rooms-gallery-83555a.gitlab.io/start.html"),
        Some("https://quiet-rooms-gallery-83555a.gitlab.io/".to_owned())
    );
    assert_eq!(
        key("https://Example.com:443/a/./b/../c?x=1#top"),
        Some("https://example.com/a/c".to_owned())
    );
    assert_eq!(key("ftp://example.com/x"), None);
    // A forge page that names no repository is just its URL.
    assert_eq!(
        key("https://github.com/only-owner"),
        Some("https://github.com/only-owner".to_owned())
    );
    assert_eq!(
        urls("see https://a.com/x and 'http://b.org/y?z=1' `https://c.io`"),
        vec!["https://a.com/x", "http://b.org/y?z=1", "https://c.io"]
    );
}

#[test]
fn the_shell_splitter_unquotes_and_skips_data() {
    let words = |script: &str| -> Vec<Vec<String>> {
        commands(script).into_iter().map(|c| c.words).collect()
    };
    assert_eq!(
        words("# a comment\ncd ~/repo && git push origin main 2>&1 | tail -3"),
        vec![
            vec!["cd".to_owned(), "~/repo".to_owned()],
            vec![
                "git".to_owned(),
                "push".to_owned(),
                "origin".to_owned(),
                "main".to_owned()
            ],
            vec!["tail".to_owned(), "-3".to_owned()],
        ]
    );
    assert_eq!(
        words(r#"gh issue comment 5 --body "it's \"done\"" > /tmp/out; echo 'a b'"#),
        vec![
            vec![
                "gh".to_owned(),
                "issue".to_owned(),
                "comment".to_owned(),
                "5".to_owned(),
                "--body".to_owned(),
                "it's \"done\"".to_owned()
            ],
            vec!["echo".to_owned(), "a b".to_owned()],
        ]
    );
    // Here-document bodies are data.
    assert_eq!(
        words("python3 - <<'PY'\nimport os; os.system('git push')\nPY\ngit pull"),
        vec![
            vec!["python3".to_owned(), "-".to_owned()],
            vec!["git".to_owned(), "pull".to_owned()],
        ]
    );
    let substituted = words(
        "glab issue create --description \"$(cat <<'EOF'\nLine one of the body here\nEOF\n)\"",
    );
    assert_eq!(substituted.len(), 1);
    assert_eq!(
        heredoc_argument(&substituted[0][4]).as_deref(),
        Some("Line one of the body here")
    );
    assert_eq!(heredoc_argument("plain"), None);
}

#[test]
fn bash_commands_become_accesses() {
    let mut shell = Shell::default();
    // A push names its remote in the output; the directory learns it.
    let pushed = shell.accesses(
        "cd /home/computeruse/tracker && git push origin main",
        "To https://github.com/ai-village-agents/tracker.git\n   1..2  main -> main",
    );
    assert_eq!(pushed.len(), 1);
    assert_eq!(pushed[0].op, Op::Write);
    assert_eq!(pushed[0].tool, Tool::Git);
    assert_eq!(
        locator_key(&pushed[0].resource),
        "https://github.com/ai-village-agents/tracker"
    );
    // A pull with no `From` line resolves through the learnt directory.
    let pulled = shell.accesses("git pull", "Already up to date.");
    assert_eq!(pulled.len(), 1);
    assert_eq!(pulled[0].op, Op::Read);
    assert_eq!(pulled[0].resource, pushed[0].resource);
    // A clone teaches its directory.
    let cloned = shell.accesses(
        "cd ~ && git clone https://gitlab.com/g/proj.git && cd proj && git fetch",
        "",
    );
    assert_eq!(cloned.len(), 2);
    assert!(cloned.iter().all(|a| a.op == Op::Read));
    assert_eq!(
        locator_key(&cloned[1].resource),
        "https://gitlab.com/g/proj"
    );
    assert_eq!(shell.cwd(), "/home/computeruse/proj");
    // Issue commands: writes keep their payload.
    let commented = shell.accesses(
        "gh issue comment 7 -R ai-village-agents/tracker --body 'Fixed the parser bug in tracker'",
        "https://github.com/ai-village-agents/tracker/issues/7#issuecomment-9",
    );
    assert_eq!(commented.len(), 1);
    assert_eq!(commented[0].op, Op::Write);
    assert_eq!(commented[0].verb, "gh issue comment");
    assert_eq!(
        commented[0].payload,
        vec!["Fixed the parser bug in tracker".to_owned()]
    );
    let viewed = shell.accesses(
        "glab issue view 3 --repo ai-village-agents/village/signal-garden",
        "",
    );
    assert_eq!(viewed[0].op, Op::Read);
    assert_eq!(
        locator_key(&viewed[0].resource),
        "https://gitlab.com/ai-village-agents/village/signal-garden"
    );
    let noted = shell.accesses("glab mr note 4 -m 'Looks good to me, merging'", "");
    // The directory's remote names the repository.
    assert_eq!(noted[0].op, Op::Write);
    assert_eq!(locator_key(&noted[0].resource), "https://gitlab.com/g/proj");
    // curl: data makes a write, -G keeps a read.
    let posted = shell.accesses(
        "curl -s -X POST https://api.example.com/notes -H 'Content-Type: application/json' -d '{\"text\":\"hello there\"}'",
        "",
    );
    assert_eq!(posted[0].op, Op::Write);
    assert_eq!(
        posted[0].payload,
        vec!["{\"text\":\"hello there\"}".to_owned()]
    );
    let got = shell.accesses("curl -sG https://api.example.com/notes -d q=1", "");
    assert_eq!(got[0].op, Op::Read);
    assert!(got[0].payload.is_empty());
    let api = shell.accesses("gh api repos/o/r/issues -f title=x", "");
    assert_eq!(api[0].op, Op::Write);
    assert_eq!(locator_key(&api[0].resource), "https://github.com/o/r");
    assert!(shell.accesses("ls -la && echo done", "").is_empty());
    assert!(
        shell
            .accesses("timeout 30 git push", "fatal: no remote")
            .len()
            <= 1
    );
}

#[test]
fn provider_shapes_become_assistant_parts() {
    let anthropic = response(
        &json!({"type": "message", "role": "assistant", "content": [
            {"type": "thinking", "thinking": "hmm", "signature": "[BLOB_REMOVED]"},
            {"type": "text", "text": "Doing it"},
            {"type": "tool_use", "id": "toolu_1", "name": "bash", "input": {"command": "ls"}}
        ], "stop_reason": "tool_use"}),
        "turn",
    );
    assert_eq!(anthropic.protocol, WireProtocol::AnthropicMessages);
    assert_eq!(anthropic.stop, StopReason::ToolUse);
    assert_eq!(anthropic.parts.len(), 3);
    assert!(matches!(
        &anthropic.parts[0],
        AssistantPart::Reasoning(Reasoning::Visible {
            signature: None,
            ..
        })
    ));
    assert_eq!(anthropic.call_ids()[0].0, "toolu_1");

    let responses = response(
        &json!([
            {"type": "reasoning", "summary": [{"type": "summary_text", "text": "plan"}], "encrypted_content": "gAAAA"},
            {"type": "message", "content": [{"type": "output_text", "text": "Hi"}, {"type": "output_text", "text": ""}]},
            {"type": "computer_call", "call_id": "call_9", "actions": [{"type": "click", "x": 1, "y": 2}]}
        ]),
        "turn",
    );
    assert_eq!(responses.protocol, WireProtocol::OpenAiResponses);
    assert_eq!(responses.parts.len(), 4);
    assert!(matches!(
        &responses.parts[1],
        AssistantPart::Reasoning(Reasoning::Opaque { .. })
    ));
    match &responses.parts[3] {
        AssistantPart::ToolCall(call) => {
            assert_eq!(call.id.0, "call_9");
            assert_eq!(call.name.0, "computer");
            assert!(
                matches!(&call.arguments, ToolArguments::Json(json) if json.0.contains("\"actions\""))
            );
        }
        other => panic!("{other:?}"),
    }

    let chat = response(
        &json!({"role": "assistant", "content": null, "reasoning_content": "think", "tool_calls": [
            {"id": "c1", "type": "function", "function": {"name": "bash", "arguments": "{\"command\": \"ls\"}"}},
            {"id": "c2", "type": "function", "function": {"name": "bash", "arguments": "not json"}}
        ]}),
        "turn",
    );
    assert_eq!(chat.protocol, WireProtocol::OpenAiChat);
    assert_eq!(chat.call_ids().len(), 2);
    assert!(
        matches!(&chat.parts[2], AssistantPart::ToolCall(call) if call.arguments == ToolArguments::Invalid("not json".into()))
    );

    let gemini = response(
        &json!({"candidates": [{"content": {"parts": [
            {"text": "thought", "thought": true},
            {"functionCall": {"name": "use_computer", "args": {"action": "screenshot"}}, "thoughtSignature": "[BLOB_REMOVED]"},
            {"functionCall": {"name": "bash", "args": {}}}
        ]}, "finishReason": "STOP"}]}),
        "turn-7",
    );
    assert_eq!(gemini.protocol, WireProtocol::GeminiGenerate);
    assert_eq!(
        gemini
            .call_ids()
            .iter()
            .map(|id| id.0.as_str())
            .collect::<Vec<_>>(),
        vec!["turn-7-1", "turn-7-2"]
    );
    assert_eq!(gemini.stop, StopReason::ToolUse);
    assert!(response(&json!(null), "t").parts.is_empty());
    assert!(response(&json!([]), "t").parts.is_empty());
}

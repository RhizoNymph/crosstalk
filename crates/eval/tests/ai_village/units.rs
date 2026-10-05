//! Times, resources, the bash accesses (through L5's extractor) and
//! provider shapes.

use crosstalk_eval::datasets::ai_village::access::payload::authored;
use crosstalk_eval::datasets::ai_village::access::shell::{commands, heredoc_argument};
use crosstalk_eval::datasets::ai_village::access::{Access, Op, Payload, Shell, expand_home};
use crosstalk_eval::datasets::ai_village::provider::response;
use crosstalk_eval::datasets::ai_village::resource::{ResourceKind, from_remote, from_url};
use crosstalk_eval::datasets::ai_village::time::{
    Day, Window, format_seconds, parse_timestamp, village_day,
};
use crosstalk_eval::truth::kinds::locator_key;
use crosstalk_spec::derived::flow::access::WriteOutcome;
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
    // The repository itself: every remote, web, API and Pages form.
    let github = Some("repo://github.com/ai-village-agents/tracker".to_owned());
    for form in [
        "https://github.com/ai-village-agents/tracker",
        "https://github.com/AI-Village-Agents/Tracker.git",
        "https://github.com/ai-village-agents/tracker/tree/main",
        "https://api.github.com/repos/ai-village-agents/tracker",
        "https://api.github.com/repos/ai-village-agents/tracker/commits?per_page=5",
        "https://codeload.github.com/ai-village-agents/tracker/zip/refs/heads/main",
        "https://ai-village-agents.github.io/tracker/index.html?v=2",
        "https://www.github.com/ai-village-agents/tracker",
        "https://x-access-token:[REDACTED]@github.com/ai-village-agents/tracker.git",
    ] {
        assert_eq!(key(form), github, "{form}");
    }
    for remote in [
        "git@github.com:ai-village-agents/tracker.git",
        "ssh://git@github.com/ai-village-agents/tracker",
        "https://github.com/ai-village-agents/tracker.git",
    ] {
        assert_eq!(
            from_remote(remote).map(|l| locator_key(&l)),
            github,
            "{remote}"
        );
    }
    // A local bare repository is no forge's.
    assert_eq!(from_remote("/srv/git/tracker.git"), None);
    // A user site's root is its `<o>.github.io` repository.
    assert_eq!(
        key("https://ai-village-agents.github.io/"),
        Some("repo://github.com/ai-village-agents/ai-village-agents.github.io".to_owned())
    );
    // Files of the repository, however they are reached.
    let readme = Some("file://github.com/ai-village-agents/tracker/README.md".to_owned());
    for form in [
        "https://raw.githubusercontent.com/ai-village-agents/tracker/main/README.md",
        "https://github.com/ai-village-agents/tracker/blob/main/README.md",
        "https://api.github.com/repos/ai-village-agents/tracker/contents/README.md",
    ] {
        assert_eq!(key(form), readme, "{form}");
    }
    // Threads: an issue and a pull request share one GitHub page.
    let thread = Some("https://github.com/ai-village-agents/tracker/issues/12".to_owned());
    for form in [
        "https://github.com/ai-village-agents/tracker/issues/12#issuecomment-1",
        "https://github.com/ai-village-agents/tracker/pull/12",
        "https://api.github.com/repos/ai-village-agents/tracker/issues/12/comments",
    ] {
        assert_eq!(key(form), thread, "{form}");
    }
    // GitLab: nested groups joined with `/`.
    let gitlab = Some("repo://gitlab.com/ai-village-agents/village/signal-garden".to_owned());
    for form in [
        "https://gitlab.com/ai-village-agents/village/signal-garden.git",
        "https://gitlab.com/ai-village-agents/village/signal-garden/-/tree/main",
        "https://gitlab.com/api/v4/projects/ai-village-agents%2Fvillage%2Fsignal-garden",
        "https://oauth2:[REDACTED]@gitlab.com/ai-village-agents/village/signal-garden.git",
    ] {
        assert_eq!(key(form), gitlab, "{form}");
    }
    assert_eq!(
        key("https://gitlab.com/ai-village-agents/village/signal-garden/-/issues/3"),
        Some("https://gitlab.com/ai-village-agents/village/signal-garden/-/issues/3".to_owned())
    );
    assert_eq!(
        key("https://ai-village-agents.gitlab.io/signal-garden/page.html"),
        Some("repo://gitlab.com/ai-village-agents/signal-garden".to_owned())
    );
    // A numeric project id and a unique Pages domain name no project path.
    assert_eq!(
        key("https://gitlab.com/api/v4/projects/84161768/pipelines?per_page=2"),
        Some("https://gitlab.com/api/v4/projects/84161768/pipelines?per_page=2".to_owned())
    );
    assert_eq!(
        key("https://quiet-rooms-gallery-83555a.gitlab.io/start.html"),
        Some("https://quiet-rooms-gallery-83555a.gitlab.io/start.html".to_owned())
    );
    assert_eq!(
        key("https://Example.com:443/a/./b/../c?y=2&x=1#top"),
        Some("https://example.com/a/c?x=1&y=2".to_owned())
    );
    // A scheme-less host is https.
    assert_eq!(
        key("www.informations.com"),
        Some("https://www.informations.com/".to_owned())
    );
    // Whatever L5 makes of the rest: another scheme is its URL, a bare
    // file name is no URL.
    assert_eq!(
        key("ftp://example.com/x"),
        Some("ftp://example.com/x".to_owned())
    );
    assert_eq!(key("README.md"), None);
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

fn keys(accesses: &[Access]) -> Vec<(bool, String)> {
    accesses
        .iter()
        .map(|a| (a.op.is_write(), locator_key(&a.resource)))
        .collect()
}

#[test]
fn bash_commands_become_accesses() {
    let mut shell = Shell::default();
    // A push names its remote in the output; the directory learns it.
    let pushed = shell.accesses(
        "cd /home/computeruse/tracker && git push origin main",
        "To https://github.com/ai-village-agents/tracker.git\n   1a2b3c4..5d6e7f8  main -> main",
    );
    assert_eq!(
        keys(&pushed),
        vec![(
            true,
            "repo://github.com/ai-village-agents/tracker".to_owned()
        )]
    );
    assert_eq!(pushed[0].kind, ResourceKind::Repository);
    assert_eq!(
        pushed[0].op,
        Op::Write {
            outcome: WriteOutcome::Delivered,
            payload: Payload::Unseen
        }
    );
    assert_eq!(shell.cwd(), Some("/home/computeruse/tracker"));
    // The shell persists: a later pull runs in the learnt clone.
    let pulled = shell.accesses("git pull", "Already up to date.");
    assert_eq!(
        keys(&pulled),
        keys(&pushed)
            .into_iter()
            .map(|(_, k)| (false, k))
            .collect::<Vec<_>>()
    );
    // A file of the clone is the repository's file; `~` is the home.
    let read = shell.accesses(
        "cat ~/tracker/docs/plan.md && sed -n '1,20p' README.md",
        "# Plan",
    );
    assert_eq!(
        keys(&read),
        vec![
            (
                false,
                "file://github.com/ai-village-agents/tracker/docs/plan.md".to_owned()
            ),
            (
                false,
                "file://github.com/ai-village-agents/tracker/README.md".to_owned()
            ),
        ]
    );
    // A write into the clone carries what the author typed.
    let written = shell.accesses(
        "cat > notes.md <<'EOF'\nMeet at the old mill at nine tonight, bring the ledger\nEOF",
        "",
    );
    assert_eq!(
        keys(&written),
        vec![(
            true,
            "file://github.com/ai-village-agents/tracker/notes.md".to_owned()
        )]
    );
    assert_eq!(
        written[0].payload(),
        Some(&Payload::Authored(vec![
            "Meet at the old mill at nine tonight, bring the ledger".to_owned()
        ]))
    );
    // A clone teaches its directory; files outside a clone are the agent's own.
    let cloned = shell.accesses(
        "cd ~ && git clone https://gitlab.com/g/proj.git && cd proj && git fetch && cat /etc/hosts",
        "",
    );
    assert_eq!(
        keys(&cloned),
        vec![(false, "repo://gitlab.com/g/proj".to_owned())]
    );
    assert_eq!(shell.cwd(), Some("/home/computeruse/proj"));
    // Issue commands: a comment writes the thread, a view reads it.
    let commented = shell.accesses(
        "gh issue comment 7 -R ai-village-agents/tracker --body 'Fixed the parser bug in tracker'",
        "https://github.com/ai-village-agents/tracker/issues/7#issuecomment-9",
    );
    assert_eq!(
        keys(&commented),
        vec![(
            true,
            "https://github.com/ai-village-agents/tracker/issues/7".to_owned()
        )]
    );
    assert_eq!(
        commented[0].payload(),
        Some(&Payload::Authored(vec![
            "Fixed the parser bug in tracker".to_owned()
        ]))
    );
    let created = shell.accesses(
        "glab issue create --title 'Signal garden: broken link on the start page'",
        "https://gitlab.com/g/proj/-/issues/4",
    );
    assert_eq!(
        keys(&created),
        vec![(true, "https://gitlab.com/g/proj/-/issues".to_owned())]
    );
    let viewed = shell.accesses(
        "glab issue view 3 --repo ai-village-agents/village/signal-garden",
        "title: x",
    );
    assert_eq!(
        keys(&viewed),
        vec![(
            false,
            "https://gitlab.com/ai-village-agents/village/signal-garden/-/issues/3".to_owned()
        )]
    );
    // curl: the site rules name the repository's file; data makes a write.
    let raw = shell.accesses(
        "curl -sL https://raw.githubusercontent.com/o/r/main/a.md",
        "text",
    );
    assert_eq!(
        keys(&raw),
        vec![(false, "file://github.com/o/r/a.md".to_owned())]
    );
    let posted = shell.accesses(
        "curl -s -X POST https://api.example.com/notes -H 'Content-Type: application/json' -d '{\"text\":\"hello there\"}'",
        "",
    );
    assert!(posted[0].op.is_write());
    assert_eq!(
        posted[0].payload(),
        Some(&Payload::Authored(vec![
            "{\"text\":\"hello there\"}".to_owned()
        ]))
    );
    assert!(shell.accesses("ls -la && echo done", "").is_empty());
    // A failed push is rejected; a failed read is no access.
    let refused = shell.accesses(
        "git push",
        "To https://gitlab.com/g/proj.git\n ! [rejected]        main -> main (fetch first)\nerror: failed to push some refs",
    );
    assert!(matches!(
        refused[0].op,
        Op::Write {
            outcome: WriteOutcome::Rejected,
            ..
        }
    ));
    assert!(
        shell
            .accesses(
                "git clone https://github.com/o/missing.git",
                "fatal: repository 'https://github.com/o/missing.git/' not found"
            )
            .is_empty()
    );
}

#[test]
fn home_is_expanded_where_the_shell_would() {
    assert_eq!(
        expand_home("cd ~ && cat ~/a.txt \"$HOME/b\" ${HOME}/c x~y"),
        "cd /home/computeruse && cat /home/computeruse/a.txt \"/home/computeruse/b\" /home/computeruse/c x~y"
    );
}

#[test]
fn authored_text_is_bodies_flags_and_data() {
    let script = "cd x && cat > a.md <<'EOF'\nline one\nline two\nEOF\ngh pr create -t 'The title' --body \"$(cat <<'BODY'\nBody here\nBODY\n)\" && echo hi > b";
    assert_eq!(
        authored(script),
        vec![
            "line one\nline two".to_owned(),
            "Body here".to_owned(),
            "The title".to_owned(),
            "hi".to_owned(),
        ]
    );
    assert_eq!(
        authored("gh api repos/o/r/issues -f title=Broken -F body='It fails'"),
        vec!["Broken".to_owned(), "It fails".to_owned()]
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
    // An event's `data.output` can be a bare array of Anthropic blocks.
    let blocks = response(
        &json!([
            {"type": "thinking", "thinking": "plan", "signature": "[BLOB_REMOVED]"},
            {"type": "text", "text": "Good morning, village"},
            {"type": "tool_use", "id": "toolu_2", "name": "start_using_computer", "input": {}}
        ]),
        "turn",
    );
    assert_eq!(blocks.protocol, WireProtocol::AnthropicMessages);
    assert_eq!(blocks.parts.len(), 3);
    assert!(
        matches!(&blocks.parts[1], AssistantPart::Text(text) if text.0 == "Good morning, village")
    );
    assert_eq!(blocks.stop, StopReason::ToolUse);
    assert!(response(&json!(null), "t").parts.is_empty());
    assert!(response(&json!([]), "t").parts.is_empty());
}

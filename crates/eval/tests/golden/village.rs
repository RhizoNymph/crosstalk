//! The AI Village converter (window mode) on a small synthetic village
//! (the shapes of `tests/ai_village/fixture.rs`): Alice pushes, posts to
//! chat and comments on an issue; Bob pulls and reads the issue.

use std::path::Path;

use crosstalk_eval::datasets::ai_village::time::Day;
use crosstalk_eval::datasets::ai_village::{AiVillageSource, Mode};
use serde_json::json;

use super::common::{dir, export_reference};
use super::village_fixture::{
    self as fixture, ALICE, BOB, GENERAL, anthropic, bash, responses, send, table, turn,
};

const PUSHED: &str = "Pushed the tracker fix, please pull and review it";
const COMMENT: &str = "The parser now handles nested quotes in every field";

fn village(root: &Path) {
    fixture::base(root);
    table(
        root,
        "computer_use_sessions",
        &[
            json!({"id": "sA1", "agent_id": ALICE, "session_goal": "g"}),
            json!({"id": "sB1", "agent_id": BOB, "session_goal": "g"}),
        ],
    );
    let push = "cd /home/computeruse/tracker && git push origin main";
    let comment = format!("gh issue comment 5 -R ai-village-agents/tracker --body \"{COMMENT}\"");
    let pull = "cd ~/tracker && git pull";
    let view = "gh issue view 5 --repo ai-village-agents/tracker --comments";
    let bob = |text: &str, id: &str, command: &str| {
        responses(text, id, "bash", json!({"command": command}))
    };
    table(
        root,
        "computer_use_turns",
        &[
            turn(
                "A1",
                "sA1",
                "2026-07-13 16:10:00.0",
                bash(push),
                anthropic(
                    "Pushing my fix",
                    "toolu_a1",
                    "bash",
                    json!({"command": push}),
                ),
                None,
                Some(
                    "To https://github.com/ai-village-agents/tracker.git\n   a1..b2  main -> main",
                ),
            ),
            turn(
                "A2",
                "sA1",
                "2026-07-13 16:20:00.0",
                send(PUSHED),
                anthropic(
                    "Telling everyone",
                    "toolu_a2",
                    "send_message_to_chat",
                    json!({"content": PUSHED}),
                ),
                None,
                None,
            ),
            turn(
                "A3",
                "sA1",
                "2026-07-13 16:40:00.0",
                bash(&comment),
                anthropic(
                    "Commenting on the issue",
                    "toolu_a3",
                    "bash",
                    json!({"command": comment}),
                ),
                Some("https://github.com/ai-village-agents/tracker/issues/5#issuecomment-1"),
                None,
            ),
            turn(
                "B1",
                "sB1",
                "2026-07-13 16:30:00.0",
                bash(pull),
                bob("Pulling", "call_b1", pull),
                None,
                Some(
                    "From https://github.com/ai-village-agents/tracker\n * branch main -> FETCH_HEAD",
                ),
            ),
            turn(
                "B2",
                "sB1",
                "2026-07-13 16:50:00.0",
                bash(view),
                bob("Reading the issue", "call_b2", view),
                Some(&format!(
                    "title: Parser bug\n--\nalice commented:\n{COMMENT}\n"
                )),
                None,
            ),
        ],
    );
    table(
        root,
        "events",
        &[fixture::talk(
            "t1",
            10,
            "2026-07-13 16:20:00.1",
            ALICE,
            GENERAL,
            "c1",
            PUSHED,
        )],
    );
    table(
        root,
        "chat_messages",
        &[fixture::chat(
            "c1",
            "2026-07-13 16:20:00.1",
            Some(ALICE),
            GENERAL,
            PUSHED,
        )],
    );
    table(
        root,
        "agent_memories",
        &[
            json!({"id": "mem1", "agent_id": ALICE, "content": "Alice remembers the tracker repo", "created_at": "2026-07-10 23:00:00.0"}),
        ],
    );
}

#[test]
fn ai_village_window_exports_and_checks() {
    let data = dir("ai-village-data");
    village(&data);
    let day = Day::parse("2026-07-13").unwrap_or_else(|e| panic!("{e}"));
    let mut source = AiVillageSource::open(&data, Mode::Window { from: day, to: day })
        .unwrap_or_else(|e| panic!("{e}"));
    let out = dir("ai-village");
    let (_, verified) = export_reference(&mut source, &out);
    assert!(verified.worlds > 0);
    assert!(
        verified.labels > verified.exchanges,
        "labels beside exchange_agent rows"
    );
}

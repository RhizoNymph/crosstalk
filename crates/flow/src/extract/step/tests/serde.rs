//! A conversation context survives the ledger's JSON: every context the
//! step can build reads back equal, and stored text that breaks a
//! context's invariants is refused.

use crosstalk_spec::observed::message::{
    Text, ToolCallId, ToolOutcome, ToolResult, ToolResultContent,
};
use proptest::prelude::*;

use super::support::bash;
use crate::extract::resource::{AbsolutePath, RepoId};
use crate::extract::{ConversationContext, ExtractConfig};

const PROMPTS: [&str; 3] = [
    "Working directory: /w",
    "Primary working directory: /home/u/proj",
    "no directory stated",
];

/// A shell call and its output, teaching the context.
const LESSONS: [(&str, &str); 7] = [
    (
        "git clone https://gitlab.com/ai-village-agents/village/atlas.git /w/atlas && cd /w/atlas",
        "Cloning into '/w/atlas'...",
    ),
    ("cd ~/notes", ""),
    ("cd ..", ""),
    ("echo $HOME", "/home/u"),
    ("pwd", "/w/elsewhere"),
    (
        "git clone git@github.com:Org/Repo.git",
        "Cloning into 'Repo'...",
    ),
    (
        "git remote -v",
        "upstream\thttps://github.com/up/stream.git (fetch)",
    ),
];

fn config() -> ExtractConfig {
    ExtractConfig::from_json(r#"{ "persistent_shells": ["bash"] }"#)
        .unwrap_or_else(|error| panic!("config: {error}"))
}

fn context(prompt: usize, lessons: &[usize], bind: bool) -> ConversationContext {
    let config = config();
    let mut context = ConversationContext::from_system_prompt(PROMPTS[prompt]);
    if bind
        && let (Ok(root), Some(repo)) = (
            AbsolutePath::parse("/srv/clone"),
            RepoId::parse("https://github.com/a/b.git", None),
        )
    {
        context.bind_repo(root, repo);
    }
    for (index, lesson) in lessons.iter().enumerate() {
        let (command, output) = LESSONS[*lesson];
        let id = format!("c{index}");
        let call = bash(&id, command);
        let result = ToolResult {
            call_id: ToolCallId(id),
            content: vec![ToolResultContent::Text(Text(output.to_owned()))],
            outcome: ToolOutcome::Success,
        };
        context.observe(&config, &call, Some(&result));
    }
    context
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 128, failure_persistence: None, ..ProptestConfig::default() })]

    #[test]
    fn a_context_reads_back_equal(
        prompt in 0..PROMPTS.len(),
        lessons in prop::collection::vec(0..LESSONS.len(), 0..8),
        bind in any::<bool>(),
    ) {
        let context = context(prompt, &lessons, bind);
        let text = serde_json::to_string(&context)
            .map_err(|error| TestCaseError::fail(error.to_string()))?;
        let back: ConversationContext = serde_json::from_str(&text)
            .map_err(|error| TestCaseError::fail(format!("{error}: {text}")))?;
        prop_assert_eq!(back, context);
    }
}

/// The contexts the property builds do hold what the checks guard: a
/// known home, home-relative places and clone bindings.
#[test]
fn the_generated_contexts_cover_home_and_clones() {
    let learnt = context(0, &[0, 1, 3], true);
    assert!(learnt.shell().home().is_some());
    assert!(learnt.repos().iter().count() >= 2);
    let relative = context(2, &[1], false);
    assert!(relative.shell().home().is_none());
    assert!(relative.shell().cwd().is_some());
}

fn refused(text: &str) -> bool {
    serde_json::from_str::<ConversationContext>(text).is_err()
}

#[test]
fn stored_text_breaking_an_invariant_is_refused() {
    let shell = |cwd: &str, home: &str, repos: &str| {
        format!(
            r#"{{"shell":{{"cwd":{cwd},"previous":null,"home":{home},"repos":{repos}}},"file_host":null}}"#
        )
    };
    // Accepted: a canonical absolute directory.
    assert!(!refused(&shell(r#"{"absolute":"/w"}"#, "null", "[]")));
    // A path that is not canonical.
    assert!(refused(&shell(r#"{"absolute":"/w/../x/"}"#, "null", "[]")));
    // A relative path.
    assert!(refused(&shell(r#"{"absolute":"w"}"#, "null", "[]")));
    // A home-relative place once the home is known.
    assert!(refused(&shell(
        r#"{"home":"/notes"}"#,
        r#""/home/u""#,
        "[]"
    )));
    // A repository whose id is not the one its locator names.
    let bound = |id: &str| {
        format!(
            r#"[{{"root":{{"absolute":"/w"}},"remote":"origin","repo":{{"id":"{id}","locator":{{"type":"repository","data":{{"host":"github.com","owner":"a","name":"b"}}}}}}}}]"#
        )
    };
    assert!(!refused(&shell("null", "null", &bound("github.com/a/b"))));
    assert!(refused(&shell("null", "null", &bound("github.com/x/y"))));
    // A root and remote bound twice.
    let twice = r#"[{"root":{"absolute":"/w"},"remote":"origin","repo":{"id":"/r","locator":{"type":"file","data":{"host":null,"path":"/r"}}}},{"root":{"absolute":"/w"},"remote":"origin","repo":{"id":"/r","locator":{"type":"file","data":{"host":null,"path":"/r"}}}}]"#;
    assert!(refused(&shell("null", "null", twice)));
}

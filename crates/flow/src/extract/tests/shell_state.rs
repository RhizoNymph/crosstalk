//! The shell a conversation's calls run in, as the AI Village's persistent
//! `bash` runs them: which commands of a call ran (the output shows
//! failures, `&&`/`||`/`;`/`|` decide the rest), the remote a push or pull
//! prints, and the state that carries over (directory, home, remotes).
//! The commands and outputs are the shapes of the village's own turns
//! (exchange ids in the test names' comments).

use proptest::prelude::*;
use serde_json::json;

use crosstalk_spec::derived::flow::access::Extraction::{Parsed, Structured};
use crosstalk_spec::derived::flow::resource::{Host, Locator};
use crosstalk_spec::interfaces::l5_flow::WritePayload;
use crosstalk_spec::observed::message::{ToolCall, ToolResult};

use super::support::*;
use crate::extract::resource::repo::MAX_BINDINGS;
use crate::extract::resource::{Place, RepoBindings};
use crate::extract::{
    AbsolutePath, Classified, ConversationContext, ExtractConfig, ExtractedOp, RepoId,
    ToolExtractors, WriteOutcome,
};

use WriteOutcome::{Delivered, Rejected, Unknown};

const HOME: &str = "/home/computeruse";

fn repository(name: &str) -> Locator {
    Locator::Repository {
        host: Host("gitlab.com".to_owned()),
        owner: "ai-village-agents/village".to_owned(),
        name: name.to_owned(),
    }
}

fn repo_file(name: &str, path: &str) -> Locator {
    Locator::File {
        host: Some(Host(format!("gitlab.com/ai-village-agents/village/{name}"))),
        path: path.to_owned(),
    }
}

fn remote(name: &str) -> String {
    format!("https://gitlab.com/ai-village-agents/village/{name}.git")
}

fn pushed(locator: Locator, outcome: WriteOutcome) -> Classified {
    Classified {
        op: ExtractedOp::Write {
            outcome,
            payload: WritePayload::Unseen,
        },
        locator,
        via: Parsed,
    }
}

fn r(locator: Locator) -> Classified {
    read(locator, Parsed)
}

fn w(locator: Locator, outcome: WriteOutcome) -> Classified {
    write(locator, outcome, Parsed)
}

fn place(path: &str) -> Place {
    Place::Absolute(AbsolutePath::parse(path).expect("absolute"))
}

fn home_place(path: &str) -> Place {
    Place::Home(AbsolutePath::parse(path).expect("absolute"))
}

/// The village's configuration: its `bash` keeps one shell per agent.
fn village() -> ExtractConfig {
    ExtractConfig::from_json(r#"{ "persistent_shells": ["bash"] }"#).expect("valid")
}

/// One village agent's conversation: each call extracted against the
/// context as it was, then observed, as the gateway does.
struct Agent {
    config: ExtractConfig,
    context: ConversationContext,
}

impl Agent {
    /// A session that starts where nothing is known: no stated directory.
    fn new() -> Self {
        Self {
            config: village(),
            context: ConversationContext::default(),
        }
    }

    fn at(cwd: &str) -> Self {
        Self {
            config: village(),
            context: context_in(cwd),
        }
    }

    fn bash(&mut self, command: &str, output: &str) -> Vec<Classified> {
        let call = call("bash", json!({ "command": command }));
        let result = ok(output);
        let got = ToolExtractors::new(&self.config, &self.context)
            .extract_classified(&call, Some(&result))
            .expect("extracts");
        self.context.observe(&self.config, &call, Some(&result));
        got
    }

    fn cwd(&self) -> Option<&Place> {
        self.context.shell().cwd()
    }

    fn home(&self) -> Option<&AbsolutePath> {
        self.context.shell().home()
    }
}

fn once(
    context: &ConversationContext,
    command: &str,
    result: Option<&ToolResult>,
) -> Vec<Classified> {
    extract(
        &village(),
        context,
        &call("bash", json!({ "command": command })),
        result,
    )
    .expect("extracts")
}

// 1. A command the output shows never ran makes no access.

/// `01KXED5HV034DD2CSEZCVNJ0FV`: the `cd` failed, so the `sed` after `&&`
/// never ran and read nothing.
#[test]
fn a_read_after_a_failed_cd_in_an_and_list_never_happened() {
    let mut agent = Agent::at(HOME);
    let got = agent.bash(
        "# View key sections of the Wave 2 visualization page to audit metric framing\ncd ai-wellbeing && sed -n '1,220p' wave2-visualization.html",
        "/bin/bash: line 90: cd: ai-wellbeing: No such file or directory",
    );
    assert_eq!(got, vec![]);
    // The failed `cd` left the shell where it was.
    assert_eq!(agent.cwd(), Some(&place(HOME)));
}

#[test]
fn list_semantics_decide_which_commands_ran() {
    let context = context_in("/w");
    let cd_failed = "bash: cd: x: No such file or directory";
    let cd_failed_then_read = format!("{cd_failed}\n# A");
    let cases: Vec<(&str, &str, Vec<Classified>)> = vec![
        // `&&` after a failure: skipped, writes refuted, reads gone, also
        // down a pipeline.
        (
            "cd x && echo hi > note.txt && cat a.md",
            cd_failed,
            vec![w(file("/w/x/note.txt"), Rejected)],
        ),
        ("cd x && cat a.md | head -5", cd_failed, vec![]),
        // `;` runs whatever came before: the read stays (its locator is
        // the call's, `flow.extract.write-locators-from-call`).
        (
            "cd x; cat a.md",
            &cd_failed_then_read,
            vec![r(file("/w/x/a.md"))],
        ),
        // `||` runs after a failure...
        (
            "cd atlas || git clone https://github.com/agentvillage/atlas atlas",
            "bash: cd: atlas: No such file or directory\nCloning into 'atlas'...",
            vec![r(Locator::Repository {
                host: Host("github.com".to_owned()),
                owner: "agentvillage".to_owned(),
                name: "atlas".to_owned(),
            })],
        ),
        // ...and is skipped after a `cd` that surely succeeded (no error,
        // its stderr shown).
        (
            "cd atlas || git clone https://github.com/agentvillage/atlas atlas",
            "",
            vec![],
        ),
        // A `cd` whose errors are hidden may have failed: what follows may
        // have run, and is kept.
        (
            "cd x 2>/dev/null && cat a.md",
            "# A",
            vec![r(file("/w/x/a.md"))],
        ),
        // A reader that could not open one operand still read the other.
        (
            "cat a.md b.md",
            "cat: a.md: No such file or directory\n# B",
            vec![r(file("/w/b.md"))],
        ),
        (
            "head -n 5 'my notes.md' c.md",
            "head: cannot open 'my notes.md' for reading: No such file or directory\n# C",
            vec![r(file("/w/c.md"))],
        ),
        (
            "sed -n '1,20p' gone.md",
            "sed: can't read gone.md: No such file or directory",
            vec![],
        ),
        // A program the shell could not find did nothing.
        (
            "glab issue view 3 -R ai-village-agents/village/ai-wellbeing",
            "bash: line 1: glab: command not found",
            vec![],
        ),
        // The error of a command that may have run shows it ran.
        (
            "test -d x && cat a.md && cat b.md",
            "cat: a.md: No such file or directory",
            vec![],
        ),
    ];
    for (command, output, expected) in cases {
        assert_eq!(
            once(&context, command, Some(&ok(output))),
            expected,
            "{command}"
        );
    }
}

#[test]
fn a_failed_cd_teaches_nothing_and_a_skipped_one_does_not_move() {
    let mut agent = Agent::at("/w");
    agent.bash(
        "cd nope && cd /srv",
        "bash: cd: nope: No such file or directory",
    );
    assert_eq!(agent.cwd(), Some(&place("/w")));
    agent.bash(
        "cd nope; cd /srv",
        "bash: cd: nope: No such file or directory",
    );
    assert_eq!(agent.cwd(), Some(&place("/srv")));
    // A subshell's `cd` stays in it.
    agent.bash("(cd /tmp && ls); cat a.md", "x");
    assert_eq!(agent.cwd(), Some(&place("/srv")));
    // `cd -` goes back.
    agent.bash("cd /opt", "");
    agent.bash("cd -", "/srv");
    assert_eq!(agent.cwd(), Some(&place("/srv")));
}

// 2. The remote a push or pull prints wins over the binding.

/// `01KXESQPE17KBP4TKRAK0HCY59` then `01KXESRHVM2S1265BP82GN2RCR`: a stale
/// binding said `daily-signal-garden-gpt55`; git printed
/// `constraint-dashboard`.
#[test]
fn a_printed_remote_overrides_a_stale_binding() {
    let mut agent = Agent::at("/home/computeruse/work");
    agent.context.bind_repo(
        AbsolutePath::parse("/home/computeruse/work").expect("absolute"),
        RepoId::parse(&remote("daily-signal-garden-gpt55"), None).expect("a remote"),
    );
    let first = agent.bash(
        "# Commit with a clear ethics-focused message and push\ngit commit -m \"docs: add ethics summary and clarify optional monitoring framing\" && git push origin main",
        &format!(
            "[main ef7e509] docs: add ethics summary\n 14 files changed, 226 insertions(+), 49 deletions(-)\nTo {}\n ! [rejected]        main -> main (fetch first)\nerror: failed to push some refs to '{}'",
            remote("constraint-dashboard"),
            remote("constraint-dashboard"),
        ),
    );
    // The bound repository was never pushed to.
    assert_eq!(
        first,
        vec![pushed(repository("daily-signal-garden-gpt55"), Rejected)]
    );
    let second = agent.bash(
        "# Push rebased main branch\ngit push origin main\n",
        &format!(
            "To {}\n   70484afb..07675fb2  main -> main",
            remote("constraint-dashboard")
        ),
    );
    assert_eq!(
        second,
        vec![pushed(repository("constraint-dashboard"), Delivered)]
    );
    // Files of the clone are the printed repository's now.
    assert_eq!(
        agent.bash("cat README.md", "# Constraint dashboard"),
        vec![r(repo_file("constraint-dashboard", "/README.md"))]
    );
}

/// `01KXHB5HQQHV7CBSMV31T798NT`: a pull read the repository its `From`
/// line printed.
#[test]
fn a_pull_reads_the_repository_it_printed() {
    let mut agent = Agent::at("/home/computeruse/workspace");
    agent.context.bind_repo(
        AbsolutePath::parse("/home/computeruse/workspace").expect("absolute"),
        RepoId::parse(&remote("deepseek-pattern-archive"), None).expect("a remote"),
    );
    let got = agent.bash(
        "cd /home/computeruse/workspace/relationship-goal-tracker && git pull --ff-only",
        &format!(
            "Updating 0e7ea78..ea6da7d\nFast-forward\n 3 files changed\nFrom {}\n   0e7ea78..ea6da7d  master     -> origin/master",
            "https://gitlab.com/ai-village-agents/village/relationship-goal-tracker"
        ),
    );
    assert_eq!(got, vec![r(repository("relationship-goal-tracker"))]);
    // With nothing bound, the printed remote still names it.
    let mut fresh = Agent::at("/srv/clone");
    assert_eq!(
        fresh.bash(
            "git fetch",
            &format!(
                "From {}\n * [new branch]  x -> origin/x",
                remote("village-ci-tools")
            )
        ),
        vec![r(repository("village-ci-tools"))]
    );
    // ...and is learnt: `Already up to date.` prints none.
    assert_eq!(
        fresh.bash("git pull", "Already up to date."),
        vec![r(repository("village-ci-tools"))]
    );
}

#[test]
fn printed_remotes_never_correct_an_explicit_remote_or_an_ambiguous_script() {
    let mut agent = Agent::at("/w");
    agent.context.bind_repo(
        AbsolutePath::parse("/w").expect("absolute"),
        RepoId::parse(&remote("a"), None).expect("a remote"),
    );
    // A URL operand names the repository itself.
    assert_eq!(
        once(
            &agent.context,
            &format!("git push {} main", remote("b")),
            Some(&ok(&format!(
                "To {}\n   1a2b..3c4d  main -> main",
                remote("c")
            ))),
        ),
        vec![pushed(repository("b"), Delivered)]
    );
    // Two pushes: the lines cannot be told apart.
    let two = format!(
        "To {}\n   1a2b..3c4d  main -> main\nTo {}\n   1a2b..3c4d  main -> main",
        remote("c"),
        remote("d")
    );
    assert_eq!(
        once(
            &agent.context,
            "git push && git -C /x push",
            Some(&ok(&two))
        ),
        vec![pushed(repository("a"), Delivered)]
    );
    // A remote name the clone has no binding for names nothing.
    assert_eq!(
        once(
            &agent.context,
            "git push upstream main",
            Some(&ok("Everything up-to-date"))
        ),
        vec![]
    );
    // A named remote is learnt under its name, and files stay origin's.
    agent.bash(
        "git push upstream main",
        &format!("To {}\n   1a2b..3c4d  main -> main", remote("u")),
    );
    assert_eq!(
        agent.bash("git push upstream main", "Everything up-to-date"),
        vec![pushed(repository("u"), Delivered)]
    );
    assert_eq!(
        agent.bash("cat x.md", "x"),
        vec![r(repo_file("a", "/x.md"))]
    );
}

#[test]
fn write_locators_still_come_from_the_call() {
    let mut context = context_in("/w");
    context.bind_repo(
        AbsolutePath::parse("/w").expect("absolute"),
        RepoId::parse(&remote("a"), None).expect("a remote"),
    );
    let command = "cd x && echo hi > n.txt; git push";
    let outputs = [
        None,
        Some(ok("")),
        Some(ok("bash: cd: x: No such file or directory")),
        Some(ok(&format!(
            "To {}\n   1a2b..3c4d  main -> main",
            remote("z")
        ))),
        Some(failed("boom")),
    ];
    let writes = |result: Option<&ToolResult>| -> Vec<Locator> {
        once(&context, command, result)
            .into_iter()
            .filter(|access| {
                access.op.kind() == crosstalk_spec::derived::flow::access::AccessKind::Write
            })
            .map(|access| access.locator)
            .collect()
    };
    let expected = writes(None);
    assert_eq!(expected.len(), 2);
    for output in &outputs {
        assert_eq!(writes(output.as_ref()), expected);
    }
}

// 4. The shell persists: directory, home and remotes carry over.

/// `01KXE3J8NFPA8TNQ3T8JDP9N5C`: `cd ~/wellbeing-compass && … git push`,
/// with the home directory not known yet.
#[test]
fn a_clone_under_an_unknown_home_is_learnt_from_its_push() {
    let mut agent = Agent::new();
    let push = "cd ~/wellbeing-compass && git add claude-skill/human-wellbeing-coach/SKILL.md && git commit -m \"Incorporate Sonnet 4.6 feedback\" && git push 2>&1 | tail -10";
    let output = format!(
        "[main e4cb54f] Incorporate Sonnet 4.6 feedback\n 1 file changed, 53 insertions(+), 7 deletions(-)\nTo {}\n   101187d..e4cb54f  main -> main",
        remote("wellbeing-compass")
    );
    // Nothing known: the push names no repository yet.
    assert_eq!(agent.bash(push, &output), vec![]);
    assert_eq!(agent.cwd(), Some(&home_place("/wellbeing-compass")));
    // Learnt from what it printed: the next push and the clone's files.
    assert_eq!(
        agent.bash("git push", "Everything up-to-date"),
        vec![pushed(repository("wellbeing-compass"), Delivered)]
    );
    assert_eq!(
        agent.bash(
            "cd ~ && cat ~/wellbeing-compass/claude-skill/SKILL.md",
            "# Skill"
        ),
        vec![r(repo_file("wellbeing-compass", "/claude-skill/SKILL.md"))]
    );
    assert_eq!(agent.cwd(), Some(&home_place("/")));
    // A `pwd` in the home shows where it is: every place becomes
    // absolute, and absolute paths meet the clone.
    agent.bash("pwd", HOME);
    assert_eq!(agent.home().map(AbsolutePath::as_str), Some(HOME));
    assert_eq!(agent.cwd(), Some(&place(HOME)));
    assert_eq!(
        agent.bash(
            "cat /home/computeruse/wellbeing-compass/README.md",
            "# Wellbeing compass"
        ),
        vec![r(repo_file("wellbeing-compass", "/README.md"))]
    );
}

/// `01KXEAYNKYNVQVQBSHD22HR3WW`: a `git push` in a clone entered in an
/// earlier call.
#[test]
fn a_push_runs_in_the_clone_an_earlier_call_entered() {
    let mut agent = Agent::new();
    agent.bash(
        "cd /home/computeruse/ai-wellbeing && git remote -v",
        &format!(
            "origin\t{} (fetch)\norigin\t{} (push)",
            remote("ai-wellbeing"),
            remote("ai-wellbeing")
        ),
    );
    assert_eq!(
        agent.bash(
            "# Push the updated Wave 2 visualization guardrail to origin\ngit push",
            &format!(
                "To {}\n   bae041a..1d802f3  main -> main",
                remote("ai-wellbeing")
            ),
        ),
        vec![pushed(repository("ai-wellbeing"), Delivered)]
    );
}

/// `01KXEB6MG9ZY9FBDHM15SB025E`: a pull in a clone made before the
/// conversation, known from an earlier fetch's `From` line.
#[test]
fn a_pull_in_a_clone_made_before_the_conversation() {
    let mut agent = Agent::new();
    assert_eq!(
        agent.bash(
            "cd /home/computeruse/ai-wellbeing/village-ci-tools\ngit pull",
            "Already up to date.",
        ),
        vec![]
    );
    agent.bash(
        "git fetch",
        &format!(
            "From {}\n   1a2b..3c4d  master -> origin/master",
            remote("village-ci-tools")
        ),
    );
    assert_eq!(
        agent.bash(
            "# Update village-ci-tools to latest master\ncd /home/computeruse/ai-wellbeing/village-ci-tools\ngit pull",
            "Already up to date.",
        ),
        vec![r(repository("village-ci-tools"))]
    );
}

#[test]
fn the_home_directory_is_learnt_only_from_output() {
    // `echo ~` prints it.
    let mut agent = Agent::new();
    agent.bash("echo ~", HOME);
    assert_eq!(agent.home().map(AbsolutePath::as_str), Some(HOME));
    // A failed `cd ~/x` prints it expanded.
    let mut agent = Agent::new();
    agent.bash(
        "cd ~/nope && ls",
        "bash: cd: /home/computeruse/nope: No such file or directory",
    );
    assert_eq!(agent.home().map(AbsolutePath::as_str), Some(HOME));
    assert_eq!(agent.cwd(), None);
    // A `pwd` under the home: the printed path less the place in it.
    let mut agent = Agent::new();
    agent.bash("cd ~/a/b && pwd", "/home/computeruse/a/b");
    assert_eq!(agent.home().map(AbsolutePath::as_str), Some(HOME));
    assert_eq!(agent.cwd(), Some(&place("/home/computeruse/a/b")));
    // A `pwd` from an unknown directory teaches the directory.
    let mut agent = Agent::new();
    agent.bash("pwd", "/srv/app");
    assert_eq!(agent.cwd(), Some(&place("/srv/app")));
    // Nothing is invented: `cd ~` alone teaches only a home-relative place,
    // and an ambiguous output teaches nothing.
    let mut agent = Agent::new();
    agent.bash("cd ~", "");
    assert_eq!(agent.home(), None);
    assert_eq!(agent.cwd(), Some(&home_place("/")));
    agent.bash("pwd; ls /", "/home/computeruse\n/bin");
    assert_eq!(agent.home(), None);
}

#[test]
fn a_shell_that_does_not_persist_keeps_its_directory_but_learns_its_remotes() {
    let config = ExtractConfig::default();
    let mut context = context_in("/w");
    let push = call("bash", json!({ "command": "cd /w/atlas && git push" }));
    context.observe(
        &config,
        &push,
        Some(&ok(&format!(
            "To {}\n   1a2b..3c4d  main -> main",
            remote("atlas")
        ))),
    );
    assert_eq!(context.cwd(), AbsolutePath::parse("/w").ok().as_ref());
    assert_eq!(
        once(&context, "cat /w/atlas/README.md", Some(&ok("# Atlas"))),
        vec![r(repo_file("atlas", "/README.md"))]
    );
}

#[test]
fn the_persistent_shells_configuration() {
    assert_eq!(
        ExtractConfig::from_json(r#"{ "persistent_shells": [""] }"#),
        Err(crate::extract::ConfigError::EmptyName)
    );
    let config = village();
    assert!(config.persistent_shell("bash"));
    assert!(!ExtractConfig::default().persistent_shell("bash"));
    // A file tool's `~` path stays as written.
    assert_eq!(
        extract(
            &config,
            &context(),
            &call("Read", json!({ "file_path": "~/x.md" })),
            Some(&ok("x"))
        ),
        Ok(vec![read(opaque("Read", "~/x.md"), Structured)])
    );
}

#[test]
fn clone_bindings_are_bounded() {
    let mut repos = RepoBindings::default();
    for at in 0..(MAX_BINDINGS + 10) {
        repos.bind(
            AbsolutePath::parse(&format!("/c/{at}")).expect("absolute"),
            RepoId::parse(&remote(&format!("r{at}")), None).expect("a remote"),
        );
    }
    assert_eq!(repos.iter().count(), MAX_BINDINGS);
    // The oldest went first.
    assert!(
        repos
            .locate(&AbsolutePath::parse("/c/0/x").expect("absolute"))
            .is_none()
    );
    assert!(
        repos
            .locate(&AbsolutePath::parse(&format!("/c/{}/x", MAX_BINDINGS + 9)).expect("absolute"))
            .is_some()
    );
}

// The state is a function of what was observed, and keeps its invariant.

fn step() -> impl Strategy<Value = (String, String)> {
    let dir = prop_oneof![
        Just("~".to_owned()),
        Just("~/repo".to_owned()),
        Just("~/repo/sub".to_owned()),
        Just("/home/u/repo".to_owned()),
        Just("/srv".to_owned()),
        Just("..".to_owned()),
        Just("sub".to_owned()),
        Just("-".to_owned()),
        Just("\"$(mktemp -d)\"".to_owned()),
    ];
    let command = prop_oneof![
        dir.clone().prop_map(|dir| format!("cd {dir}")),
        dir.clone().prop_map(|dir| format!("cd {dir} && pwd")),
        dir.prop_map(|dir| format!("(cd {dir}); cat a")),
        Just("echo ~".to_owned()),
        Just("git push".to_owned()),
        Just("git pull origin main".to_owned()),
        Just("git remote -v".to_owned()),
        Just("cat x && cat ~/repo/y".to_owned()),
    ];
    let output = prop_oneof![
        Just(String::new()),
        Just("/home/u".to_owned()),
        Just("/home/u/repo".to_owned()),
        Just("bash: cd: sub: No such file or directory".to_owned()),
        Just("bash: cd: /home/u/repo: No such file or directory".to_owned()),
        Just(format!("To {}\n   1a2b..3c4d  main -> main", remote("a"))),
        Just(format!(
            "From {}\n   1a2b..3c4d  main -> origin/main",
            remote("b")
        )),
        Just(format!("origin\t{} (fetch)", remote("c"))),
    ];
    (command, output)
}

fn replay(steps: &[(String, String)]) -> (ConversationContext, Vec<Vec<Classified>>) {
    let config = village();
    let mut context = ConversationContext::default();
    let mut found = Vec::new();
    for (command, output) in steps {
        let call: ToolCall = call("bash", json!({ "command": command }));
        let result = ok(output);
        found.push(
            ToolExtractors::new(&config, &context)
                .extract_classified(&call, Some(&result))
                .expect("extracts"),
        );
        context.observe(&config, &call, Some(&result));
    }
    (context, found)
}

proptest! {
    /// `flow.extract.shell-state-from-observed`: the same calls and
    /// results, in the same order, give the same state and accesses; once
    /// the home directory is known no place is home-relative; the clone
    /// bindings stay bounded.
    #[test]
    fn shell_state_is_a_function_of_what_was_observed(
        steps in prop::collection::vec(step(), 0..24),
    ) {
        let (first, found) = replay(&steps);
        let (second, again) = replay(&steps);
        prop_assert_eq!(&first, &second);
        prop_assert_eq!(found, again);
        let shell = first.shell();
        if shell.home().is_some() {
            prop_assert!(!matches!(shell.cwd(), Some(Place::Home(_))));
            prop_assert!(!matches!(shell.previous(), Some(Place::Home(_))));
            prop_assert!(shell.repos().iter().all(|(root, _)| matches!(root, Place::Absolute(_))));
        }
        prop_assert!(shell.repos().iter().count() <= MAX_BINDINGS);
    }
}

#[test]
fn a_write_with_no_result_is_unknown_and_unrefuted() {
    let context = context_in("/w");
    assert_eq!(
        once(&context, "cd x && echo hi > n.txt", None),
        vec![w(file("/w/x/n.txt"), Unknown)]
    );
}

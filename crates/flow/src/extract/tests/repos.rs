//! Shared git repositories: agents in their own clones meet on the
//! repository's files, as the context learns which directories are clones.

use serde_json::json;

use crosstalk_spec::derived::flow::access::Extraction::{Parsed, Structured};
use crosstalk_spec::derived::flow::resource::{Host, Locator};
use crosstalk_spec::observed::message::{ToolCall, ToolResult};

use super::support::*;
use crate::extract::{
    AbsolutePath, Classified, ConversationContext, ExtractConfig, RepoId, ToolExtractors,
    WriteOutcome,
};

fn repo_file(repo: &str, path: &str) -> Locator {
    Locator::File {
        host: Some(Host(repo.to_owned())),
        path: path.to_owned(),
    }
}

fn atlas(path: &str) -> Locator {
    repo_file("github.com/agentvillage/atlas", path)
}

/// One agent's conversation: each call extracted against the context as it
/// was, then observed.
struct Agent {
    config: ExtractConfig,
    context: ConversationContext,
}

impl Agent {
    fn in_dir(cwd: &str) -> Self {
        Self {
            config: ExtractConfig::default(),
            context: context_in(cwd),
        }
    }

    fn step(&mut self, call: ToolCall, result: ToolResult) -> Vec<Classified> {
        let got = ToolExtractors::new(&self.config, &self.context)
            .extract_classified(&call, Some(&result))
            .expect("extracts");
        self.context.observe(&self.config, &call, Some(&result));
        got
    }

    fn bash(&mut self, command: &str, output: &str) -> Vec<Classified> {
        self.step(call("Bash", json!({ "command": command })), ok(output))
    }

    fn read(&mut self, path: &str) -> Vec<Classified> {
        self.step(call("Read", json!({ "file_path": path })), ok("content"))
    }

    fn edit(&mut self, path: &str) -> Vec<Classified> {
        self.step(
            call(
                "Edit",
                json!({ "file_path": path, "old_string": "a", "new_string": "b" }),
            ),
            ok("The file has been updated."),
        )
    }
}

#[test]
fn clones_in_different_places_share_files() {
    let mut alice = Agent::in_dir("/home/alice");
    alice.bash(
        "git clone https://github.com/AgentVillage/Atlas.git",
        "Cloning into 'Atlas'...",
    );
    assert_eq!(
        alice.edit("/home/alice/Atlas/src/app.py"),
        vec![write(
            atlas("/src/app.py"),
            WriteOutcome::Delivered,
            Structured
        )],
    );

    let mut bob = Agent::in_dir("/workspace");
    bob.bash(
        "git clone --depth 1 -b main git@github.com:agentvillage/atlas.git work",
        "",
    );
    assert_eq!(
        bob.read("/workspace/work/src/app.py"),
        vec![read(atlas("/src/app.py"), Structured)],
    );
    // Outside the clone, a file is still a file of the machine.
    assert_eq!(
        bob.read("/workspace/notes.md"),
        vec![read(file("/workspace/notes.md"), Structured)]
    );
}

#[test]
fn a_clone_counts_within_the_same_command() {
    let mut carol = Agent::in_dir("/tmp/carol");
    assert_eq!(
        carol.bash(
            "git clone https://github.com/agentvillage/atlas && cat atlas/README.md && echo done >> atlas/LOG",
            "# Atlas",
        ),
        vec![read(atlas("/README.md"), Parsed), write(atlas("/LOG"), WriteOutcome::Delivered, Parsed)],
    );
}

#[test]
fn claude_code_shell_keeps_its_directory() {
    let mut alice = Agent::in_dir("/home/alice");
    alice.bash("gh repo clone AgentVillage/Atlas", "");
    alice.bash("cd Atlas", "");
    assert_eq!(
        alice.context.cwd(),
        AbsolutePath::parse("/home/alice/Atlas").ok().as_ref()
    );
    assert_eq!(
        alice.bash("cat src/app.py", "print(1)"),
        vec![read(atlas("/src/app.py"), Parsed)],
    );
    assert_eq!(
        alice.bash("git show HEAD~1:src/app.py", "print(0)"),
        vec![read(atlas("/src/app.py"), Parsed)],
    );
    // Claude Code moved the shell back.
    alice.bash("cd /etc", "Shell cwd was reset to /home/alice");
    assert_eq!(
        alice.context.cwd(),
        AbsolutePath::parse("/home/alice").ok().as_ref()
    );
    // A cd it cannot follow leaves the directory unknown.
    alice.bash("cd \"$(mktemp -d)\"", "");
    assert_eq!(alice.context.cwd(), None);
    assert_eq!(
        alice.bash("cat x", "x"),
        vec![read(opaque("Bash", "x"), Parsed)]
    );
}

#[test]
fn a_shell_that_does_not_persist_keeps_the_stated_directory() {
    let config = ExtractConfig::default();
    let mut context = context_in("/srv/app");
    let cd = call("shell", json!({ "command": ["bash", "-lc", "cd /tmp"] }));
    context.observe(&config, &cd, Some(&ok("")));
    assert_eq!(context.cwd(), AbsolutePath::parse("/srv/app").ok().as_ref());
}

#[test]
fn a_printed_remote_binds_the_directory() {
    let mut dave = Agent::in_dir("/home/dave/proj");
    dave.bash(
        "git remote -v",
        "origin\thttps://github.com/AgentVillage/Atlas.git (fetch)\norigin\thttps://github.com/AgentVillage/Atlas.git (push)\n",
    );
    assert_eq!(
        dave.read("/home/dave/proj/src/app.py"),
        vec![read(atlas("/src/app.py"), Structured)]
    );

    let mut erin = Agent::in_dir("/home/erin/x");
    erin.bash(
        "git config --get remote.origin.url",
        "git@github.com:agentvillage/atlas.git\n",
    );
    assert_eq!(
        erin.read("/home/erin/x/src/app.py"),
        vec![read(atlas("/src/app.py"), Structured)]
    );

    let mut fay = Agent::in_dir("/home/fay/y");
    fay.bash(
        "git remote add origin https://github.com/agentvillage/atlas",
        "",
    );
    assert_eq!(
        fay.read("/home/fay/y/a"),
        vec![read(atlas("/a"), Structured)]
    );
}

#[test]
fn failed_or_absent_results_teach_nothing() {
    let config = ExtractConfig::default();
    let mut context = context_in("/home/alice");
    let clone = call(
        "Bash",
        json!({ "command": "git clone https://github.com/a/b && cd b" }),
    );
    context.observe(
        &config,
        &clone,
        Some(&failed("fatal: repository not found")),
    );
    context.observe(&config, &clone, None);
    assert_eq!(context.repos().iter().count(), 0);
    assert_eq!(
        context.cwd(),
        AbsolutePath::parse("/home/alice").ok().as_ref()
    );
}

#[test]
fn a_local_shared_repository() {
    let mut alice = Agent::in_dir("/home/alice");
    alice.bash("git clone /srv/shared/atlas.git", "");
    let mut bob = Agent::in_dir("/home/bob");
    bob.bash(
        "git -C /home/bob/src clone file:///srv/shared/atlas atlas-copy",
        "",
    );
    let shared = repo_file("/srv/shared/atlas", "/plan.md");
    assert_eq!(
        alice.read("/home/alice/atlas/plan.md"),
        vec![read(shared.clone(), Structured)]
    );
    assert_eq!(
        bob.read("/home/bob/src/atlas-copy/plan.md"),
        vec![read(shared, Structured)]
    );
}

#[test]
fn repository_ids_are_canonical() {
    let atlas = Some("github.com/agentvillage/atlas");
    let cases = [
        ("https://github.com/AgentVillage/Atlas.git", atlas),
        ("https://github.com/AgentVillage/Atlas/", atlas),
        ("http://www.github.com/agentvillage/atlas", atlas),
        ("https://token@github.com/agentvillage/atlas.git", atlas),
        ("ssh://git@github.com:22/agentvillage/atlas.git", atlas),
        ("git@github.com:AgentVillage/Atlas.git", atlas),
        ("git://github.com/agentvillage/atlas", atlas),
        (
            "https://gitlab.com/group/sub/proj.git",
            Some("gitlab.com/group/sub/proj"),
        ),
        ("/srv/shared/atlas.git", Some("/srv/shared/atlas")),
        ("file:///srv/shared/./atlas/", Some("/srv/shared/atlas")),
        ("../atlas", Some("/home/atlas")),
        ("https://github.com/only-owner", None),
        ("ftp://github.com/a/b", None),
        ("not a remote", None),
        ("", None),
        ("C:/x/y", None),
    ];
    let cwd = AbsolutePath::parse("/home/alice").ok();
    for (remote, expected) in cases {
        assert_eq!(
            RepoId::parse(remote, cwd.as_ref()).map(|repo| repo.as_str().to_owned()),
            expected.map(str::to_owned),
            "{remote}",
        );
    }
}

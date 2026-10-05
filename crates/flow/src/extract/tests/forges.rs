//! Repositories and forges: `Locator::Repository` from every spelling of a
//! repository, `git push`/`pull`/`fetch`/`clone`, the `gh` and `glab` CLIs,
//! shell outcomes judged from a known command's output, `sed -n`, and
//! OpenHands' tools.

use proptest::prelude::*;
use serde_json::json;

use crosstalk_spec::derived::flow::access::{AccessKind, AccessOp, Extraction};
use crosstalk_spec::derived::flow::resource::{Host, Locator};
use crosstalk_spec::ids::SpanId;
use crosstalk_spec::observed::message::{ToolCall, ToolResult};

use super::support::*;
use crate::extract::{
    AbsolutePath, Classified, ConversationContext, ExtractConfig, ExtractedOp, RepoId,
    ToolExtractors, WriteOutcome, WritePayload, access_op,
};

use Extraction::{Parsed, Structured};
use WriteOutcome::{Delivered, Rejected, Unknown};

fn atlas() -> Locator {
    Locator::Repository {
        host: Host("github.com".to_owned()),
        owner: "agentvillage".to_owned(),
        name: "atlas".to_owned(),
    }
}

fn atlas_file(path: &str) -> Locator {
    Locator::File {
        host: Some(Host("github.com/agentvillage/atlas".to_owned())),
        path: path.to_owned(),
    }
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

fn w(locator: Locator, outcome: WriteOutcome) -> Classified {
    write(locator, outcome, Parsed)
}

fn r(locator: Locator) -> Classified {
    read(locator, Parsed)
}

/// A context in `/home/alice/atlas`, a clone of `agentvillage/atlas`.
fn in_clone() -> ConversationContext {
    let mut context = context_in("/home/alice/atlas");
    context.bind_repo(
        AbsolutePath::parse("/home/alice/atlas").expect("absolute"),
        RepoId::parse("git@github.com:AgentVillage/Atlas.git", None).expect("a remote"),
    );
    context
}

/// A context in `/home/alice/infra`, a clone of `village/ops/infra` on
/// GitLab.
fn in_gitlab_clone() -> ConversationContext {
    let mut context = context_in("/home/alice/infra");
    context.bind_repo(
        AbsolutePath::parse("/home/alice/infra").expect("absolute"),
        RepoId::parse("https://gitlab.com/Village/Ops/Infra.git", None).expect("a remote"),
    );
    context
}

fn run(context: &ConversationContext, command: &str, output: &str) -> Vec<Classified> {
    extract(
        &ExtractConfig::default(),
        context,
        &call("Bash", json!({ "command": command })),
        Some(&ok(output)),
    )
    .expect("extracts")
}

fn github(path: &str) -> Locator {
    https("github.com", path)
}

fn gitlab(path: &str) -> Locator {
    https("gitlab.com", path)
}

// Every spelling of a repository meets on one resource.

fn segment() -> impl Strategy<Value = String> {
    "[a-z0-9][a-z0-9_-]{0,7}"
}

/// `s` with its letters' case flipped where `mask` says.
fn spell(text: &str, mask: u64) -> String {
    text.chars()
        .enumerate()
        .map(|(at, c)| {
            if mask >> (at % 64) & 1 == 1 {
                c.to_ascii_uppercase()
            } else {
                c
            }
        })
        .collect()
}

fn fetched(url: &str) -> Vec<Classified> {
    extract(
        &ExtractConfig::default(),
        &context(),
        &call("WebFetch", json!({ "url": url, "prompt": "x" })),
        Some(&ok("page")),
    )
    .expect("extracts")
}

proptest! {
    /// `flow.resource.repository-forms-meet`: the remote URLs (https, ssh,
    /// scp form, with or without `.git`, in any case), the web and API
    /// URLs, `codeload` and Pages, and the remotes `git clone`, `git push`
    /// and `git pull` name all give one `Locator::Repository`; the raw,
    /// blob and contents URLs of a file in it give the file whose host is
    /// that repository's.
    #[test]
    fn repository_forms_meet(
        owner in segment(),
        name in segment(),
        mask in any::<u64>(),
        file in prop::collection::vec(segment(), 1..3),
    ) {
        let repository = Locator::repository("github.com", &owner, &name).expect("valid");
        let (o, n) = (spell(&owner, mask), spell(&name, mask.rotate_left(7)));
        let remotes = [
            format!("https://github.com/{o}/{n}"),
            format!("https://github.com/{o}/{n}.git"),
            format!("https://user@GitHub.com/{o}/{n}.git/"),
            format!("ssh://git@github.com:22/{o}/{n}.git"),
            format!("git@github.com:{o}/{n}.git"),
            format!("git@github.com:{o}/{n}"),
        ];
        for remote in &remotes {
            let repo = RepoId::parse(remote, None);
            prop_assert_eq!(repo.as_ref().map(RepoId::locator), Some(&repository), "{}", remote);
            let clone = run(&context_in("/w"), &format!("git clone {remote} work"), "Cloning into 'work'...");
            prop_assert_eq!(&clone, &vec![r(repository.clone())], "{}", remote);
            let push = run(&context_in("/w"), &format!("git push {remote} main"), "Everything up-to-date");
            prop_assert_eq!(&push, &vec![pushed(repository.clone(), Delivered)], "{}", remote);
            let pull = run(&context_in("/w"), &format!("git pull {remote}"), "Already up to date.");
            prop_assert_eq!(&pull, &vec![r(repository.clone())], "{}", remote);
        }
        let urls = [
            format!("https://github.com/{o}/{n}"),
            format!("https://www.github.com/{o}/{n}.git"),
            format!("https://github.com/{o}/{n}/tree/main"),
            format!("https://api.github.com/repos/{o}/{n}"),
            format!("https://api.github.com/repos/{o}/{n}/commits?per_page=5"),
            format!("https://codeload.github.com/{o}/{n}/zip/refs/heads/main"),
            format!("https://{o}.github.io/{n}/index.html"),
        ];
        for url in &urls {
            prop_assert_eq!(fetched(url), vec![read(repository.clone(), Structured)], "{}", url);
        }
        let path = format!("/{}", file.join("/"));
        let inside = Locator::File {
            host: repository.repository_file_host(),
            path: path.clone(),
        };
        let file_urls = [
            format!("https://github.com/{o}/{n}/blob/main{path}"),
            format!("https://raw.githubusercontent.com/{o}/{n}/main{path}"),
            format!("https://api.github.com/repos/{o}/{n}/contents{path}"),
        ];
        for url in &file_urls {
            prop_assert_eq!(fetched(url), vec![read(inside.clone(), Structured)], "{}", url);
        }
    }

    /// `flow.extract.write-locators-from-call`: a call's writes, their
    /// locators and payloads, are the same with or without its result, so
    /// writes held at the call are released in order with it.
    #[test]
    fn write_locators_never_depend_on_the_result(
        command in forge_command(),
        output in forge_output(),
        clone in any::<bool>(),
    ) {
        let context = if clone { in_clone() } else { context() };
        let call = call("Bash", json!({ "command": command }));
        let config = ExtractConfig::default();
        let writes = |result: Option<&ToolResult>| -> Vec<(Locator, Option<WritePayload>)> {
            extract(&config, &context, &call, result)
                .unwrap_or_default()
                .into_iter()
                .filter_map(|access| match access.op {
                    ExtractedOp::Write { payload, .. } => Some((access.locator, Some(payload))),
                    ExtractedOp::Read => None,
                })
                .collect()
        };
        let without = writes(None);
        prop_assert_eq!(writes(Some(&ok(&output))), without.clone());
        prop_assert_eq!(writes(Some(&failed(&output))), without);
    }
}

proptest! {
    /// `flow.extract.write-locators-from-call`, over every known tool.
    #[test]
    fn known_calls_write_the_same_locators_with_any_result(
        call in super::generate::known_call(),
        result in super::generate::tool_result(),
    ) {
        let config = wiki_config();
        let writes = |result: Option<&ToolResult>| -> Vec<(Locator, WritePayload)> {
            extract(&config, &in_clone(), &call, result)
                .unwrap_or_default()
                .into_iter()
                .filter_map(|access| match access.op {
                    ExtractedOp::Write { payload, .. } => Some((access.locator, payload)),
                    ExtractedOp::Read => None,
                })
                .collect()
        };
        prop_assert_eq!(writes(result.as_ref()), writes(None));
    }
}

fn forge_command() -> impl Strategy<Value = String> {
    prop::sample::select(
        &[
            "git push",
            "git push origin main",
            "git push https://github.com/a/b.git",
            "git pull && git push",
            "gh issue create --title T --body 'B is ready'",
            "gh issue comment 12 --body 'done'",
            "gh pr comment --body 'lgtm'",
            "gh pr create -t T -b B -R agentvillage/atlas",
            "glab mr note 3 -m 'merged'",
            "glab issue create -t T -d D -R village/ops/infra",
            "gh api repos/{owner}/{repo}/issues -f title=T -f body=B",
            "gh api -X PATCH repos/a/b/issues/3 -f state=closed",
            "curl -X POST -d x=1 https://example.com/api -w '%{http_code}'",
            "echo x > notes.md && git push",
        ][..],
    )
    .prop_map(str::to_owned)
}

fn forge_output() -> impl Strategy<Value = String> {
    prop_oneof![
        prop::sample::select(
            &[
                "To github.com:a/b.git\n   1a2b3c4..5d6e7f8  main -> main",
                " ! [rejected]        main -> main (fetch first)\nerror: failed to push some refs",
                "https://github.com/agentvillage/atlas/issues/13",
                "gh: Not Found (HTTP 404)",
                "HTTP/2 404\n",
                "201",
                "",
            ][..]
        )
        .prop_map(str::to_owned),
        "\\PC{0,40}",
    ]
}

// git.

#[test]
fn git_push_pull_fetch_clone() {
    let clone = in_clone();
    let tmp = context_in("/tmp");
    let bob = context_in("/home/bob");
    let work = context_in("/w");
    let cases: Vec<(&ConversationContext, &str, &str, Vec<Classified>)> = vec![
        (
            &clone,
            "git push",
            "To github.com:agentvillage/atlas.git\n   1a2b3c4..5d6e7f8  main -> main",
            vec![pushed(atlas(), Delivered)],
        ),
        (
            &clone,
            "git push -u origin feature",
            " * [new branch]      feature -> feature",
            vec![pushed(atlas(), Delivered)],
        ),
        (
            &clone,
            "git push --force-with-lease origin main",
            " + 1a2b3c4...5d6e7f8 main -> main (forced update)",
            vec![pushed(atlas(), Delivered)],
        ),
        (
            &clone,
            "git push origin main",
            "To github.com:agentvillage/atlas.git\n ! [rejected]        main -> main (fetch first)\nerror: failed to push some refs to 'github.com:agentvillage/atlas.git'",
            vec![pushed(atlas(), Rejected)],
        ),
        (
            &clone,
            "git push",
            "remote: Permission to agentvillage/atlas.git denied to bob.\nfatal: unable to access 'https://github.com/agentvillage/atlas.git/': The requested URL returned error: 403",
            vec![pushed(atlas(), Rejected)],
        ),
        // Neither a success nor a failure shows: unknown.
        (
            &clone,
            "git push 2>/dev/null",
            "",
            vec![pushed(atlas(), Unknown)],
        ),
        (&clone, "git push --dry-run", "", vec![]),
        // No known remote, no repository.
        (&tmp, "git push", "", vec![]),
        (
            &clone,
            "git pull --rebase origin main",
            "Updating 1a2b3c4..5d6e7f8\nFast-forward\n README.md | 2 +-",
            vec![r(atlas())],
        ),
        (&clone, "git fetch --all", "", vec![r(atlas())]),
        // A failed pull read nothing.
        (
            &clone,
            "git pull",
            "fatal: Could not read from remote repository.",
            vec![],
        ),
        (
            &bob,
            "git -C /home/alice/atlas push",
            "Everything up-to-date",
            vec![],
        ),
        (
            &clone,
            "cd /tmp && git -C /home/alice/atlas push",
            "Everything up-to-date",
            vec![pushed(atlas(), Delivered)],
        ),
        // A local bare repository is the file of its directory.
        (
            &work,
            "git push /srv/shared/atlas.git main",
            "To /srv/shared/atlas.git\n   1a2b3c4..5d6e7f8  main -> main",
            vec![pushed(file("/srv/shared/atlas"), Delivered)],
        ),
        // The redirect write carries the call's spans; the push does not.
        (
            &clone,
            "echo 'v2 is out' >> NEWS && git commit -am news && git push",
            "",
            vec![w(atlas_file("/NEWS"), Delivered), pushed(atlas(), Unknown)],
        ),
    ];
    for (context, command, output, expected) in cases {
        assert_eq!(run(context, command, output), expected, "{command}");
    }
}

#[test]
fn a_push_and_a_clone_meet_across_agents() {
    // Alice pushes from her clone, Bob clones by URL into another
    // directory and later pulls by remote name.
    let alice = run(&in_clone(), "git push origin main", "Everything up-to-date");
    let config = ExtractConfig::default();
    let mut bob = context_in("/workspace");
    let clone = call(
        "Bash",
        json!({ "command": "git clone https://github.com/agentvillage/atlas.git" }),
    );
    let cloned = ToolExtractors::new(&config, &bob)
        .extract_classified(&clone, Some(&ok("Cloning into 'atlas'...")))
        .expect("extracts");
    bob.observe(&config, &clone, Some(&ok("Cloning into 'atlas'...")));
    let pulled = run(
        &bob,
        "cd atlas && git pull origin main",
        "Already up to date.",
    );
    assert_eq!(alice, vec![pushed(atlas(), Delivered)]);
    assert_eq!(cloned, vec![r(atlas())]);
    assert_eq!(pulled, vec![r(atlas())]);
}

#[test]
fn an_unseen_write_carries_no_spans() {
    let part = super::spans::call_part();
    let spans = vec![SpanId::from_ulid(1)];
    let op = |payload| ExtractedOp::Write {
        outcome: Delivered,
        payload,
    };
    assert_eq!(
        access_op(op(WritePayload::Unseen), part, None, spans.clone()),
        Ok(AccessOp::Write {
            call: part,
            spans: vec![],
            outcome: Delivered
        })
    );
    assert_eq!(
        access_op(op(WritePayload::CallArguments), part, None, spans.clone()),
        Ok(AccessOp::Write {
            call: part,
            spans,
            outcome: Delivered
        })
    );
}

// gh and glab.

#[test]
fn forge_cli_threads() {
    let clone = in_clone();
    let tmp = context_in("/tmp");
    let lab = in_gitlab_clone();
    let issues = github("/agentvillage/atlas/issues");
    let pulls = github("/agentvillage/atlas/pulls");
    let issue = |n: u64| github(&format!("/agentvillage/atlas/issues/{n}"));
    let cases: Vec<(&ConversationContext, &str, &str, Vec<Classified>)> = vec![
        (
            &clone,
            "gh issue create --title 'Deploy plan' --body 'Ship v2 on Friday at 17:00'",
            "Creating issue in agentvillage/atlas\n\nhttps://github.com/agentvillage/atlas/issues/13",
            vec![w(issues.clone(), Delivered)],
        ),
        (
            &clone,
            "gh issue comment 13 --body 'Moved to Monday'",
            "https://github.com/agentvillage/atlas/issues/13#issuecomment-1",
            vec![w(issue(13), Delivered)],
        ),
        // A pull request's conversation is its issue's.
        (
            &clone,
            "gh pr comment '#7' -b 'lgtm'",
            "",
            vec![w(issue(7), Delivered)],
        ),
        (
            &clone,
            "gh pr comment https://github.com/AgentVillage/Atlas/pull/7 --body lgtm",
            "",
            vec![w(issue(7), Delivered)],
        ),
        // No number: the collection.
        (
            &clone,
            "gh pr comment --body 'lgtm'",
            "",
            vec![w(pulls.clone(), Delivered)],
        ),
        (
            &tmp,
            "gh pr create -R AgentVillage/Atlas --title T --body B",
            "https://github.com/agentvillage/atlas/pull/8",
            vec![w(pulls.clone(), Delivered)],
        ),
        (
            &tmp,
            "gh issue view 13 --repo github.com/agentvillage/atlas --comments",
            "Deploy plan\n...",
            vec![r(issue(13))],
        ),
        (
            &clone,
            "gh issue list --state open -L 50",
            "13  Deploy plan",
            vec![r(issues.clone())],
        ),
        (&clone, "gh pr list", "", vec![r(pulls.clone())]),
        // Errors reject a write and drop a read.
        (
            &clone,
            "gh issue comment 13 --body x",
            "GraphQL: Could not resolve to an issue or pull request with the number of 13. (repository.issue)",
            vec![w(issue(13), Rejected)],
        ),
        (
            &clone,
            "gh issue view 99",
            "gh: Not Found (HTTP 404)",
            vec![],
        ),
        // No repository known: no access.
        (&tmp, "gh issue list", "", vec![]),
        (&clone, "gh pr checkout 7", "", vec![]),
        // glab.
        (
            &lab,
            "glab issue create -t 'Rotate keys' -d 'Keys rotate at 9'",
            "https://gitlab.com/village/ops/infra/-/issues/4",
            vec![w(gitlab("/village/ops/infra/-/issues"), Delivered)],
        ),
        (
            &lab,
            "glab mr note 3 -m 'Rebased'",
            "",
            vec![w(
                gitlab("/village/ops/infra/-/merge_requests/3"),
                Delivered,
            )],
        ),
        (
            &lab,
            "glab issue view 4",
            "Rotate keys",
            vec![r(gitlab("/village/ops/infra/-/issues/4"))],
        ),
        (
            &tmp,
            "glab mr list -R village/ops/infra",
            "",
            vec![r(gitlab("/village/ops/infra/-/merge_requests"))],
        ),
        (
            &tmp,
            "glab repo clone village/ops/infra",
            "",
            vec![r(
                Locator::repository("gitlab.com", "village/ops", "infra").expect("valid")
            )],
        ),
        (
            &tmp,
            "gh repo clone AgentVillage/Atlas",
            "Cloning into 'Atlas'...",
            vec![r(atlas())],
        ),
    ];
    for (context, command, output, expected) in cases {
        assert_eq!(run(context, command, output), expected, "{command}");
    }
}

#[test]
fn forge_api_commands_keep_the_http_contract() {
    let clone = in_clone();
    let tmp = context_in("/tmp");
    let lab = in_gitlab_clone();
    let cases: Vec<(&ConversationContext, &str, &str, Vec<Classified>)> = vec![
        (
            &clone,
            "gh api repos/{owner}/{repo}/issues/13/comments -f body='Moved to Monday'",
            "{\"html_url\": \"https://github.com/agentvillage/atlas/issues/13#issuecomment-2\"}",
            vec![w(github("/agentvillage/atlas/issues/13"), Delivered)],
        ),
        (
            &clone,
            "gh api /repos/agentvillage/atlas/pulls/7/comments",
            "[]",
            vec![r(github("/agentvillage/atlas/issues/7"))],
        ),
        (
            &clone,
            "gh api repos/AgentVillage/Atlas/contents/README.md",
            "{}",
            vec![r(atlas_file("/README.md"))],
        ),
        (
            &clone,
            "gh api -X PUT repos/agentvillage/atlas/contents/docs/plan.md -f message=m -f content=eA==",
            "{\"content\": {}}",
            vec![w(atlas_file("/docs/plan.md"), Delivered)],
        ),
        (
            &clone,
            "gh api user",
            "{}",
            vec![r(https("api.github.com", "/user"))],
        ),
        (
            &clone,
            "gh api repos/agentvillage/atlas/issues -f title=T",
            "gh: Validation Failed (HTTP 422)",
            vec![w(github("/agentvillage/atlas/issues"), Rejected)],
        ),
        (
            &clone,
            "gh api graphql -f query='{viewer{login}}'",
            "{}",
            vec![],
        ),
        // A placeholder with no repository to fill it: no access.
        (&tmp, "gh api repos/{owner}/{repo}", "{}", vec![]),
        (
            &lab,
            "glab api projects/:fullpath/issues/4/notes -f body=hi",
            "{}",
            vec![w(gitlab("/village/ops/infra/-/issues/4"), Delivered)],
        ),
        (
            &lab,
            "glab api projects/village%2Fops%2Finfra/repository/files/src%2Fapp.py/raw",
            "print()",
            vec![r(Locator::File {
                host: Some(Host("gitlab.com/village/ops/infra".to_owned())),
                path: "/src/app.py".to_owned(),
            })],
        ),
    ];
    for (context, command, output, expected) in cases {
        assert_eq!(run(context, command, output), expected, "{command}");
    }
}

#[test]
fn gitlab_urls() {
    let infra = Locator::repository("gitlab.com", "village/ops", "infra").expect("valid");
    for url in [
        "https://gitlab.com/Village/Ops/Infra",
        "https://gitlab.com/village/ops/infra.git",
        "https://gitlab.com/village/ops/infra/-/tree/main",
        "https://gitlab.com/api/v4/projects/village%2Fops%2Finfra",
        "https://village.gitlab.io/ops/index.html",
    ] {
        let expected = if url.contains("gitlab.io") {
            Locator::repository("gitlab.com", "village", "ops").expect("valid")
        } else {
            infra.clone()
        };
        assert_eq!(fetched(url), vec![read(expected, Structured)], "{url}");
    }
    assert_eq!(
        fetched("https://gitlab.com/village/ops/infra/-/merge_requests/3/diffs"),
        vec![read(
            gitlab("/village/ops/infra/-/merge_requests/3"),
            Structured
        )]
    );
    // A numeric project id and a unique Pages domain stay URLs.
    assert_eq!(
        fetched("https://gitlab.com/api/v4/projects/123"),
        vec![read(gitlab("/api/v4/projects/123"), Structured)]
    );
    assert_eq!(
        fetched("https://ops-1a2b3c.gitlab.io/x"),
        vec![read(https("ops-1a2b3c.gitlab.io", "/x"), Structured)]
    );
    // A group page names no repository.
    assert_eq!(
        fetched("https://gitlab.com/village"),
        vec![read(gitlab("/village"), Structured)]
    );
}

#[test]
fn github_threads_and_non_repositories() {
    assert_eq!(
        fetched("https://github.com/AgentVillage/Atlas/pull/7/files"),
        vec![read(github("/agentvillage/atlas/issues/7"), Structured)]
    );
    assert_eq!(
        fetched("https://github.com/agentvillage/atlas/issues"),
        vec![read(github("/agentvillage/atlas/issues"), Structured)]
    );
    for url in [
        "https://github.com/orgs/agentvillage",
        "https://github.com/settings/tokens",
    ] {
        let Ok(Locator::Url { path, .. }) = crate::extract::resource::url_locator(url) else {
            panic!("a URL");
        };
        assert_eq!(fetched(url), vec![read(github(&path), Structured)], "{url}");
    }
}

// Shell outcomes from known commands' output.

#[test]
fn http_commands_judged_by_the_status_they_show() {
    let post =
        |flags: &str| format!("curl {flags} -X POST -d msg=hi https://relay.example.com/inbox");
    let inbox = https("relay.example.com", "/inbox");
    let cases: Vec<(String, &str, Vec<Classified>)> = vec![
        (
            post("-i"),
            "HTTP/1.1 403 Forbidden\r\n\r\nnope",
            vec![w(inbox.clone(), Rejected)],
        ),
        (
            post("-si"),
            "HTTP/1.1 100 Continue\n\nHTTP/2 201\ncontent-type: text/plain\n\nok",
            vec![w(inbox.clone(), Delivered)],
        ),
        (
            post("-s -w '%{http_code}'"),
            "{\"error\":\"x\"}500",
            vec![w(inbox.clone(), Rejected)],
        ),
        (
            post("-s -w '\\nHTTP %{http_code}\\n'"),
            "ok\nHTTP 200",
            vec![w(inbox.clone(), Delivered)],
        ),
        (
            post("-sf"),
            "curl: (22) The requested URL returned error: 404",
            vec![w(inbox.clone(), Rejected)],
        ),
        // No status shown: the wire's word.
        (post("-s"), "ok", vec![w(inbox.clone(), Delivered)]),
        // A `-w` that prints no status is not read as one.
        (
            post("-s -w '%{time_total}'"),
            "0.512",
            vec![w(inbox.clone(), Delivered)],
        ),
        // A read of an error page is no read.
        (
            "curl -sI https://relay.example.com/inbox".to_owned(),
            "HTTP/2 404\n",
            vec![],
        ),
        (
            "wget -qO- https://relay.example.com/inbox".to_owned(),
            "ERROR 404: Not Found.",
            vec![],
        ),
        (
            "wget -O- https://relay.example.com/inbox".to_owned(),
            "HTTP request sent, awaiting response... 200 OK\nhello",
            vec![r(inbox.clone())],
        ),
    ];
    for (command, output, expected) in cases {
        assert_eq!(run(&context(), &command, output), expected, "{command}");
    }
    // A command with no rule keeps the wire's word whatever its output.
    assert_eq!(
        run(
            &context(),
            "echo 'HTTP/1.1 500' > status.txt",
            "HTTP/1.1 500"
        ),
        vec![w(file("/home/alice/project/status.txt"), Delivered)]
    );
}

// sed.

#[test]
fn sed_prints_are_reads() {
    let here = |path: &str| file(&format!("{CWD}/{path}"));
    let cases: Vec<(&str, Vec<Classified>)> = vec![
        ("sed -n '10,40p' src/app.py", vec![r(here("src/app.py"))]),
        ("sed -n 5p /etc/hosts", vec![r(file("/etc/hosts"))]),
        (
            "sed -n '$p' a.log b.log",
            vec![r(here("a.log")), r(here("b.log"))],
        ),
        ("sed -n -e '3,$p' notes.md", vec![r(here("notes.md"))]),
        (
            "sed -n '1,20p' notes.md > head.md",
            vec![w(here("head.md"), Delivered)],
        ),
        ("sed -n '/TODO/p' notes.md", vec![]),
        ("sed 's/a/b/' notes.md", vec![]),
        ("sed -i -n '1p' notes.md", vec![]),
        ("sed -n 1p", vec![]),
    ];
    for (command, expected) in cases {
        assert_eq!(run(&context(), command, "x"), expected, "{command}");
    }
}

// OpenHands.

#[test]
fn openhands_writes() {
    let heredoc = "cat > /tmp/test_indent.py << 'EOF'\ndef f():\n    return 1\nEOF";
    let cases: Vec<(&str, ToolCall, ToolResult, Vec<Classified>)> = vec![
        (
            "execute_bash: a heredoc into /tmp",
            call(
                "execute_bash",
                json!({ "command": heredoc, "is_input": "false" }),
            ),
            ok("\n[The command completed with exit code 0.]"),
            vec![write(file("/tmp/test_indent.py"), Delivered, Parsed)],
        ),
        (
            "execute_bash: cat the file back",
            call(
                "execute_bash",
                json!({ "command": "cat /tmp/test_indent.py" }),
            ),
            ok("def f():\n    return 1"),
            vec![read(file("/tmp/test_indent.py"), Parsed)],
        ),
        (
            "str_replace_editor: create",
            call(
                "str_replace_editor",
                json!({ "command": "create", "path": "/tmp/test_indent.py", "file_text": "def f():\n    return 1\n" }),
            ),
            ok("File created successfully at: /tmp/test_indent.py"),
            vec![write(file("/tmp/test_indent.py"), Delivered, Structured)],
        ),
        (
            "str_replace_editor: view",
            call(
                "str_replace_editor",
                json!({ "command": "view", "path": "/tmp/test_indent.py" }),
            ),
            ok("Here's the result of running `cat -n` on /tmp/test_indent.py:\n     1\tdef f():"),
            vec![read(file("/tmp/test_indent.py"), Structured)],
        ),
    ];
    for (name, call, result, expected) in cases {
        let got =
            extract(&ExtractConfig::default(), &context(), &call, Some(&result)).expect("extracts");
        assert_eq!(got, expected, "{name}");
    }
    // OpenHands' shell persists: a `cd` moves where the next call runs.
    let config = ExtractConfig::default();
    let mut context = context();
    let cd = call("execute_bash", json!({ "command": "cd /workspace/repo" }));
    context.observe(&config, &cd, Some(&ok("")));
    assert_eq!(
        context.cwd(),
        AbsolutePath::parse("/workspace/repo").ok().as_ref()
    );
    let kinds: Vec<AccessKind> = extract(
        &config,
        &context,
        &call("execute_bash", json!({ "command": "echo x > out.txt" })),
        Some(&ok("")),
    )
    .expect("extracts")
    .iter()
    .map(|access| access.op.kind())
    .collect();
    assert_eq!(kinds, vec![AccessKind::Write]);
}

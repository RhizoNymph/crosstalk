//! Table-driven: shell commands agents run through Claude Code's `Bash`.

use serde_json::json;

use crosstalk_spec::derived::flow::access::Extraction;
use crosstalk_spec::derived::flow::resource::Locator;
use crosstalk_spec::interfaces::l5_flow::ExtractError;

use super::support::*;
use crate::extract::{Classified, ExtractConfig, WriteOutcome};

fn w(locator: Locator) -> Classified {
    write(locator, WriteOutcome::Delivered, Extraction::Parsed)
}

fn r(locator: Locator) -> Classified {
    read(locator, Extraction::Parsed)
}

fn here(path: &str) -> Locator {
    file(&format!("{CWD}/{path}"))
}

fn bash(command: &str) -> Result<Vec<Classified>, ExtractError> {
    extract(
        &ExtractConfig::default(),
        &context(),
        &call("Bash", json!({ "command": command })),
        Some(&ok("")),
    )
}

#[test]
fn shell_commands() {
    let cases: Vec<(&str, Vec<Classified>)> = vec![
        // Redirections.
        (
            "echo \"deploy at 5\" > /srv/shared/status.txt",
            vec![w(file("/srv/shared/status.txt"))],
        ),
        ("echo x >> log.txt", vec![w(here("log.txt"))]),
        ("echo x >| /tmp/f", vec![w(file("/tmp/f"))]),
        (
            "echo x > ../shared/./n.md",
            vec![w(file("/home/alice/shared/n.md"))],
        ),
        ("ls 2> err.log", vec![]),
        ("make 2>&1 > build.log", vec![w(here("build.log"))]),
        ("make &> all.log", vec![w(here("all.log"))]),
        ("make >& all.log", vec![w(here("all.log"))]),
        ("echo hi > \"$OUT\"", vec![]),
        ("echo hi > /dev/null", vec![]),
        ("git commit -m \"write > notes.md\"", vec![]),
        // Reading commands.
        ("cat a.md b.md", vec![r(here("a.md")), r(here("b.md"))]),
        (
            "head -n 20 /var/log/app.log",
            vec![r(file("/var/log/app.log"))],
        ),
        ("tail -f -n5 x.log", vec![r(here("x.log"))]),
        ("cat -- -weird.md", vec![r(here("-weird.md"))]),
        ("cat a.md > b.md", vec![w(here("b.md"))]),
        ("cat < input.txt", vec![r(here("input.txt"))]),
        ("grep foo a.md", vec![]),
        ("cat $FILE", vec![]),
        ("cat *.md", vec![]),
        ("cat ~/notes.md", vec![]),
        ("cat \"$(ls | head -1)\"", vec![]),
        (
            "printf 'a' | tee -a /tmp/a.txt /tmp/b.txt > /dev/null",
            vec![w(file("/tmp/a.txt")), w(file("/tmp/b.txt"))],
        ),
        // Here-documents.
        (
            "cat <<EOF > out.md\nhello $USER\ncat secret.md\nEOF\ncat out.md",
            vec![w(here("out.md")), r(here("out.md"))],
        ),
        ("# cat secret.md\ncat real.md", vec![r(here("real.md"))]),
        // Working directory.
        (
            "cd /srv/shared && cat notes.md",
            vec![r(file("/srv/shared/notes.md"))],
        ),
        ("cd sub; cat ../x.md", vec![r(here("x.md"))]),
        ("(cd /tmp && cat a)", vec![r(file("/tmp/a"))]),
        (
            "cd \"$HOME\" && cat notes.md",
            vec![r(opaque("Bash", "notes.md"))],
        ),
        ("cd - && cat notes.md", vec![r(opaque("Bash", "notes.md"))]),
        // Wrappers and assignments.
        ("sudo -u bob cat /etc/shadow", vec![r(file("/etc/shadow"))]),
        ("FOO=1 env BAR=2 cat x", vec![r(here("x"))]),
        (
            "timeout 10 /usr/bin/curl https://example.com/",
            vec![r(https("example.com", "/"))],
        ),
        // HTTP.
        (
            "curl -s https://api.example.com/v1/items | jq .",
            vec![r(https("api.example.com", "/v1/items"))],
        ),
        (
            "curl example.com/x",
            vec![r(url("http", "example.com", "/x", None))],
        ),
        (
            "curl -o page.html https://example.com/p",
            vec![w(here("page.html"))],
        ),
        (
            "curl https://example.com/p > page.html",
            vec![w(here("page.html"))],
        ),
        ("curl -O https://example.com/p.zip", vec![]),
        (
            "curl --json '{\"a\":1}' https://api.example.com/v1/items",
            vec![w(https("api.example.com", "/v1/items"))],
        ),
        (
            "curl -G -d q=1 https://api.example.com/search",
            vec![r(https("api.example.com", "/search"))],
        ),
        (
            "curl -XPUT https://api.example.com/x -T file.txt",
            vec![w(https("api.example.com", "/x"))],
        ),
        (
            "curl --request=DELETE https://api.example.com/x",
            vec![w(https("api.example.com", "/x"))],
        ),
        (
            "cat a.md | curl -X POST --data-binary @- https://paste.example.net/",
            vec![r(here("a.md")), w(https("paste.example.net", "/"))],
        ),
        (
            "wget -qO- https://example.com/feed",
            vec![r(https("example.com", "/feed"))],
        ),
        ("wget https://example.com/file.zip", vec![]),
        (
            "wget -O out.html https://example.com/",
            vec![w(here("out.html"))],
        ),
        (
            "wget --post-data='a=1' https://example.com/form",
            vec![w(https("example.com", "/form"))],
        ),
    ];
    for (command, expected) in cases {
        assert_eq!(bash(command), Ok(expected), "command `{command}`");
    }
}

#[test]
fn shell_parse_errors() {
    for command in [
        "echo $(date",
        "echo \"x",
        "echo 'x",
        "cat >",
        "echo `date",
        "echo ${HOME",
    ] {
        assert!(
            matches!(bash(command), Err(ExtractError::Parse { .. })),
            "command `{command}`",
        );
    }
}

#[test]
fn shell_without_a_result_writes_unknown_and_reads_nothing() {
    let got = extract(
        &ExtractConfig::default(),
        &context(),
        &call("Bash", json!({ "command": "cat a.md > b.md; cat c.md" })),
        None,
    );
    assert_eq!(
        got,
        Ok(vec![write(
            here("b.md"),
            WriteOutcome::Unknown,
            Extraction::Parsed
        )])
    );
}

#[test]
fn shell_repeated_access_is_reported_once() {
    assert_eq!(bash("cat a.md; cat ./a.md"), Ok(vec![r(here("a.md"))]));
}

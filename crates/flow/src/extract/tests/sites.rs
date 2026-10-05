//! Table-driven: wiki pages and forge files reached over HTTP, through
//! fetch tools and `curl`/`wget`, each as one canonical resource.

use serde_json::json;

use crosstalk_spec::derived::flow::access::Extraction::{self, Parsed, Structured};
use crosstalk_spec::derived::flow::resource::{Host, Locator};

use super::support::*;
use crate::extract::{Classified, ExtractConfig, SitesConfig, ToolExtractors, WriteOutcome};

use WriteOutcome::Delivered;

fn wiki_page(host: &str, path: &str) -> Locator {
    https(host, path)
}

fn dead_drop() -> Locator {
    wiki_page("en.wikipedia.org", "/wiki/Dead_drop")
}

fn repo_file(repo: &str, path: &str) -> Locator {
    Locator::File {
        host: Some(Host(repo.to_owned())),
        path: path.to_owned(),
    }
}

fn atlas_app() -> Locator {
    repo_file("github.com/agentvillage/atlas", "/src/app.py")
}

fn fetch(url: &str) -> Vec<Classified> {
    extract(
        &ExtractConfig::default(),
        &context(),
        &call("WebFetch", json!({ "url": url, "prompt": "Read it" })),
        Some(&ok("page text")),
    )
    .expect("extracts")
}

fn bash(command: &str) -> Vec<Classified> {
    extract(
        &ExtractConfig::default(),
        &context(),
        &call("Bash", json!({ "command": command })),
        Some(&ok("{}")),
    )
    .expect("extracts")
}

fn r(locator: Locator, via: Extraction) -> Classified {
    read(locator, via)
}

fn w(locator: Locator) -> Classified {
    write(locator, Delivered, Parsed)
}

#[test]
fn wiki_pages_over_http() {
    let fetched: Vec<(&str, Vec<Classified>)> = vec![
        (
            "https://en.wikipedia.org/wiki/dead_drop",
            vec![r(dead_drop(), Structured)],
        ),
        (
            "https://en.wikipedia.org/wiki/Dead_drop",
            vec![r(dead_drop(), Structured)],
        ),
        (
            "https://EN.Wikipedia.org/wiki/Dead%20drop#History",
            vec![r(dead_drop(), Structured)],
        ),
        (
            "https://en.m.wikipedia.org/wiki/Dead_drop",
            vec![r(dead_drop(), Structured)],
        ),
        (
            "https://en.wikipedia.org/w/index.php?title=Dead_drop&action=raw",
            vec![r(dead_drop(), Structured)],
        ),
        (
            "https://en.wikipedia.org/w/index.php?action=edit&title=dead+drop",
            vec![r(dead_drop(), Structured)],
        ),
        (
            "https://en.wikipedia.org/api/rest_v1/page/html/Dead_drop",
            vec![r(dead_drop(), Structured)],
        ),
        (
            "https://en.wikipedia.org/w/api.php?action=parse&page=dead_drop&format=json",
            vec![r(dead_drop(), Structured)],
        ),
        (
            "https://community.fandom.com/wiki/help:Contents",
            vec![r(
                wiki_page("community.fandom.com", "/wiki/Help:Contents"),
                Structured,
            )],
        ),
        (
            "https://en.wiktionary.org/wiki/dead_drop",
            vec![r(
                wiki_page("en.wiktionary.org", "/wiki/dead_drop"),
                Structured,
            )],
        ),
        (
            "https://de.wikipedia.org/wiki/Caf%c3%a9",
            vec![r(
                wiki_page("de.wikipedia.org", "/wiki/Caf%C3%A9"),
                Structured,
            )],
        ),
        (
            "https://de.wikipedia.org/wiki/café",
            vec![r(
                wiki_page("de.wikipedia.org", "/wiki/Caf%C3%A9"),
                Structured,
            )],
        ),
        (
            "https://en.wikipedia.org/wiki/What%3F",
            vec![r(
                wiki_page("en.wikipedia.org", "/wiki/What%3F"),
                Structured,
            )],
        ),
        // Not a configured wiki: plain URL normalization only.
        (
            "https://wiki.example.org/wiki/dead_drop",
            vec![r(
                wiki_page("wiki.example.org", "/wiki/dead_drop"),
                Structured,
            )],
        ),
    ];
    for (url, expected) in fetched {
        assert_eq!(fetch(url), expected, "{url}");
    }
    let commands: Vec<(&str, Vec<Classified>)> = vec![
        (
            "curl -s 'https://en.wikipedia.org/w/api.php?action=query&prop=revisions&rvprop=content&format=json&titles=Dead_drop|steganography'",
            vec![
                r(dead_drop(), Parsed),
                r(wiki_page("en.wikipedia.org", "/wiki/Steganography"), Parsed),
            ],
        ),
        (
            "curl -X POST https://en.wikipedia.org/w/api.php -d action=edit -d title=dead_drop --data-urlencode 'text=meet at the fountain' -d token=abc%2B%5C",
            vec![w(dead_drop())],
        ),
        (
            "curl -d 'action=edit&format=json&title=Dead%20drop&appendtext=x' https://en.wikipedia.org/w/api.php",
            vec![w(dead_drop())],
        ),
        (
            "curl -F action=edit -F title=dead_drop -F text=x https://en.wikipedia.org/w/api.php",
            vec![w(dead_drop())],
        ),
        (
            "curl -d 'action=query&meta=tokens&type=csrf&format=json' https://en.wikipedia.org/w/api.php",
            vec![r(https("en.wikipedia.org", "/w/api.php"), Parsed)],
        ),
        (
            "wget -qO- --post-data='action=edit&title=Dead drop&text=x' https://en.wikipedia.org/w/api.php",
            vec![w(dead_drop())],
        ),
        (
            "curl -X PUT https://en.wikipedia.org/w/rest.php/v1/page/Dead_drop --json '{\"source\":\"x\"}'",
            vec![w(dead_drop())],
        ),
        (
            "curl -s https://en.wikipedia.org/w/rest.php/v1/page/dead_drop",
            vec![r(dead_drop(), Parsed)],
        ),
        (
            "curl -d 'action=move&from=Dead drop&to=Dead drops' https://en.wikipedia.org/w/api.php",
            vec![
                w(dead_drop()),
                w(wiki_page("en.wikipedia.org", "/wiki/Dead_drops")),
            ],
        ),
        (
            "curl -d 'action=edit&title=Notes&text=x' https://community.fandom.com/api.php",
            vec![w(wiki_page("community.fandom.com", "/wiki/Notes"))],
        ),
        // The page body saved to a file: the page is not read.
        (
            "curl -s -o page.json 'https://en.wikipedia.org/w/api.php?action=parse&page=Dead_drop'",
            vec![w(here("page.json"))],
        ),
    ];
    for (command, expected) in commands {
        assert_eq!(bash(command), expected, "{command}");
    }
}

fn atlas_repository() -> Locator {
    Locator::Repository {
        host: Host("github.com".to_owned()),
        owner: "agentvillage".to_owned(),
        name: "atlas".to_owned(),
    }
}

fn here(path: &str) -> Locator {
    file(&format!("{CWD}/{path}"))
}

#[test]
fn forge_files_over_http() {
    let fetched: Vec<(&str, Vec<Classified>)> = vec![
        (
            "https://github.com/AgentVillage/Atlas/blob/main/src/app.py",
            vec![r(atlas_app(), Structured)],
        ),
        (
            "https://github.com/agentvillage/atlas/raw/3f2c1a9/src/app.py",
            vec![r(atlas_app(), Structured)],
        ),
        (
            "https://raw.githubusercontent.com/AgentVillage/Atlas/refs/heads/main/src/app.py",
            vec![r(atlas_app(), Structured)],
        ),
        (
            "https://raw.githubusercontent.com/AgentVillage/Atlas/main/src/app.py",
            vec![r(atlas_app(), Structured)],
        ),
        (
            "https://api.github.com/repos/AgentVillage/Atlas/contents/src/app.py?ref=main",
            vec![r(atlas_app(), Structured)],
        ),
        // The repository itself, since `Locator::Repository`.
        (
            "https://github.com/AgentVillage/Atlas",
            vec![r(atlas_repository(), Structured)],
        ),
    ];
    for (url, expected) in fetched {
        assert_eq!(fetch(url), expected, "{url}");
    }
    assert_eq!(
        bash(
            "curl -X PUT -H 'Authorization: token x' https://api.github.com/repos/agentvillage/atlas/contents/src/app.py -d '{\"message\":\"m\",\"content\":\"eA==\"}'"
        ),
        vec![w(atlas_app())],
    );
}

#[test]
fn two_agents_meet_on_one_wiki_page() {
    let config = ExtractConfig::default();
    let alice = context_in("/home/alice");
    let bob = context_in("/home/bob");
    let edit = ToolExtractors::new(&config, &alice)
        .extract_classified(
            &call(
                "Bash",
                json!({ "command": "curl -s -X POST https://en.wikipedia.org/w/api.php --data-urlencode 'title=dead drop' --data-urlencode 'appendtext=see you at 9' -d action=edit -d token=t" }),
            ),
            Some(&ok("{\"edit\":{\"result\":\"Success\"}}")),
        )
        .expect("extracts");
    let fetch = ToolExtractors::new(&config, &bob)
        .extract_classified(
            &call(
                "WebFetch",
                json!({ "url": "https://en.m.wikipedia.org/wiki/Dead_drop", "prompt": "x" }),
            ),
            Some(&ok("see you at 9")),
        )
        .expect("extracts");
    assert_eq!(edit, vec![w(dead_drop())]);
    assert_eq!(fetch, vec![r(dead_drop(), Structured)]);
}

#[test]
fn configured_wiki_sites() {
    let config = ExtractConfig::from_json(
        r#"{"sites": {"mediawiki": [{"hosts": ["wiki.example.org"], "script_path": "/"}], "github": false}}"#,
    )
    .expect("valid");
    let context = context();
    let extractors = ToolExtractors::new(&config, &context);
    let got = extractors
        .extract_classified(
            &call(
                "WebFetch",
                json!({ "url": "https://wiki.example.org/wiki/dead_drop" }),
            ),
            Some(&ok("x")),
        )
        .expect("extracts");
    assert_eq!(
        got,
        vec![r(
            wiki_page("wiki.example.org", "/wiki/Dead_drop"),
            Structured
        )]
    );
    // The built-in wikis are replaced, and GitHub is off.
    let got = extractors
        .extract_classified(
            &call(
                "WebFetch",
                json!({ "url": "https://en.wikipedia.org/wiki/dead_drop" }),
            ),
            Some(&ok("x")),
        )
        .expect("extracts");
    assert_eq!(
        got,
        vec![r(
            wiki_page("en.wikipedia.org", "/wiki/dead_drop"),
            Structured
        )]
    );
    let none = ExtractConfig::default().with_sites(SitesConfig::none());
    assert!(none.sites().mediawiki.is_empty());
    for text in [
        r#"{"sites": {"mediawiki": [{"hosts": ["Wiki.Example.org"]}]}}"#,
        r#"{"sites": {"mediawiki": [{"hosts": ["*."]}]}}"#,
        r#"{"sites": {"mediawiki": [{"hosts": ["w.org"], "article_path": "/wiki"}]}}"#,
        r#"{"sites": {"github": true, "bitbucket": true}}"#,
    ] {
        assert!(ExtractConfig::from_json(text).is_err(), "{text}");
    }
}

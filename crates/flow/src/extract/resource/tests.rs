//! Canonical identity: equivalent spellings of one file, URL, wiki page,
//! repository file or MCP key give one locator; different ones stay apart.

use proptest::prelude::*;
use serde_json::json;

use crosstalk_spec::derived::flow::resource::{Host, Locator, ResourcePattern};
use crosstalk_spec::observed::message::ToolName;

use super::*;
use crate::extract::ExtractConfig;
use crate::extract::context::ConversationContext;
use crate::extract::http::HttpRequest;
use crate::extract::sites::SitesConfig;
use crate::extract::tests::support::{call, context_in, extract, ok};

fn tool() -> ToolName {
    ToolName("Read".to_owned())
}

fn no_repos() -> RepoBindings {
    RepoBindings::default()
}

fn scope<'a>(cwd: Option<&'a AbsolutePath>, repos: &'a RepoBindings) -> FileScope<'a> {
    FileScope {
        cwd,
        host: None,
        repos,
    }
}

fn file_at(path: &str) -> Locator {
    Locator::File {
        host: None,
        path: path.to_owned(),
    }
}

/// A path segment: never a dot segment, and without surrounding space (a
/// working directory is stated on a line, which is trimmed).
fn name() -> impl Strategy<Value = String> {
    "[a-zA-Z0-9_-]([a-zA-Z0-9._ -]{0,6}[a-zA-Z0-9_-])?"
}

/// One canonical segment written noisily: `./` before it, a `zz/..`
/// detour, a doubled slash.
fn noisy(segment: String) -> impl Strategy<Value = String> {
    (any::<bool>(), any::<bool>(), any::<bool>()).prop_map(move |(dot, detour, double)| {
        let mut out = String::new();
        if dot {
            out.push_str("./");
        }
        if detour {
            out.push_str("zz/../");
        }
        if double {
            out.push('/');
        }
        out.push_str(&segment);
        out
    })
}

fn noisy_path(segments: Vec<String>) -> impl Strategy<Value = String> {
    let parts: Vec<_> = segments.into_iter().map(noisy).collect();
    (parts, any::<bool>()).prop_map(|(parts, trailing)| {
        let mut path = format!("/{}", parts.join("/"));
        if trailing {
            path.push('/');
        }
        path
    })
}

/// The lexical resolution of `relative` under `base`, computed apart from
/// the code under test.
fn resolve(base: &[String], relative: &str) -> String {
    let mut stack: Vec<String> = base.to_vec();
    for segment in relative.split('/') {
        match segment {
            "" | "." => {}
            ".." => {
                stack.pop();
            }
            s => stack.push(s.to_owned()),
        }
    }
    format!("/{}", stack.join("/"))
}

proptest! {
    /// `flow.resource.path-normalization`.
    #[test]
    fn equivalent_paths_share_resource_id(
        (canonical, a, b) in prop::collection::vec(name(), 0..6).prop_flat_map(|segments| {
            let canonical = format!("/{}", segments.join("/"));
            (Just(canonical), noisy_path(segments.clone()), noisy_path(segments))
        }),
    ) {
        let repos = no_repos();
        let la = file_locator(&a, &tool(), scope(None, &repos));
        let lb = file_locator(&b, &ToolName("Write".to_owned()), scope(None, &repos));
        prop_assert_eq!(&la, &lb);
        prop_assert_eq!(la, Ok(file_at(&canonical)));
        // Idempotent: the canonical form is its own form.
        prop_assert_eq!(AbsolutePath::parse(&canonical).map(AbsolutePath::into_string), Ok(canonical));
    }

    /// Paths that differ once resolved stay apart.
    #[test]
    fn different_paths_stay_apart(
        a in prop::collection::vec(name(), 0..4),
        b in prop::collection::vec(name(), 0..4),
    ) {
        let repos = no_repos();
        let la = file_locator(&format!("/{}", a.join("/")), &tool(), scope(None, &repos));
        let lb = file_locator(&format!("/{}", b.join("/")), &tool(), scope(None, &repos));
        prop_assert_eq!(a == b, la == lb);
    }

    /// `flow.resource.relative-path-cwd`.
    #[test]
    fn relative_path_resolves_against_stated_cwd(
        cwd in prop::collection::vec(name(), 0..4),
        relative in prop::collection::vec(
            prop_oneof![name(), Just(".".to_owned()), Just("..".to_owned())], 1..5,
        ),
    ) {
        let relative = relative.join("/");
        let cwd_text = format!("/{}", cwd.join("/"));
        let stated = ConversationContext::from_system_prompt(&format!(
            "You are Claude Code.\n<env>\nWorking directory: {cwd_text}\nIs directory a git repo: Yes\n</env>"
        ));
        let from_relative = file_locator(&relative, &tool(), stated.scope());
        let expected = resolve(&cwd, &relative);
        let from_absolute = file_locator(&expected, &tool(), stated.scope());
        prop_assert_eq!(&from_relative, &from_absolute);
        prop_assert_eq!(from_relative, Ok(file_at(&expected)));
    }

    /// `flow.resource.url-normalization`.
    #[test]
    fn equivalent_urls_share_resource_id(
        https in any::<bool>(),
        labels in prop::collection::vec("[a-z][a-z0-9-]{0,6}", 1..4),
        path in prop::collection::vec(
            "[a-zA-Z0-9_.~-]{1,6}".prop_filter("not a dot segment", |s| s != "." && s != ".."),
            0..4,
        ),
        params in prop::collection::btree_map("[a-z]{1,4}", "[a-zA-Z0-9]{0,4}", 0..4),
        upper in prop::collection::vec(any::<bool>(), 16),
        explicit_port in any::<bool>(),
        fragment in prop::option::of("[a-z]{0,6}"),
        shuffle in any::<prop::sample::Index>(),
    ) {
        let scheme = if https { "https" } else { "http" };
        let host = format!("{}.example", labels.join("."));
        let path = format!("/{}", path.join("/"));
        let pairs: Vec<String> = params.iter().map(|(k, v)| format!("{k}={v}")).collect();
        let canonical_query = (!pairs.is_empty()).then(|| pairs.join("&"));
        let plain = match &canonical_query {
            Some(q) => format!("{scheme}://{host}{path}?{q}"),
            None => format!("{scheme}://{host}{path}"),
        };
        // Variant: mixed case scheme and host, the default port, the
        // parameters rotated, a fragment.
        let mixed = |text: &str| -> String {
            text.chars()
                .zip(upper.iter().cycle())
                .map(|(c, up)| if *up { c.to_ascii_uppercase() } else { c })
                .collect()
        };
        let mut rotated = pairs.clone();
        if !rotated.is_empty() {
            let at = shuffle.index(rotated.len());
            rotated.rotate_left(at);
        }
        let port = if explicit_port { if https { ":443" } else { ":80" } } else { "" };
        let mut variant = format!("{}://{}{port}{path}", mixed(scheme), mixed(&host));
        if !rotated.is_empty() {
            variant.push('?');
            variant.push_str(&rotated.join("&"));
        }
        if let Some(fragment) = &fragment {
            variant.push('#');
            variant.push_str(fragment);
        }
        let a = url_locator(&plain);
        let b = url_locator(&variant);
        prop_assert_eq!(&a, &b);
        let expected = Locator::Url {
            scheme: scheme.to_owned(),
            host: Host(host),
            path,
            query: canonical_query,
        };
        prop_assert_eq!(&a, &Ok(expected.clone()));
        // Idempotent through its own text.
        let text = url_text(&expected).expect("a URL");
        prop_assert_eq!(url_locator(&text), Ok(expected));
    }

    /// Configured key folding is idempotent and folds case, separators and
    /// slashes when asked.
    #[test]
    fn keys_fold_to_one_form(
        words in prop::collection::vec("[a-zA-Z0-9]{1,6}", 1..4),
        seps in prop::collection::vec(prop::sample::select(&[" ", "_", "  ", " _ ", "\t"][..]), 3),
        upper in any::<bool>(),
    ) {
        let canon = KeyCanon { fold_case: true, fold_separators: true, path_like: true };
        let plain = words.join(" ").to_lowercase();
        let mut noisy = String::from("  /");
        for (i, word) in words.iter().enumerate() {
            if i > 0 {
                noisy.push_str(seps[i % seps.len()]);
            }
            noisy.push_str(&if upper { word.to_uppercase() } else { word.clone() });
        }
        noisy.push_str("// ");
        let folded = canon.canonical(&noisy);
        prop_assert_eq!(&folded, &Ok(plain.clone()));
        prop_assert_eq!(canon.canonical(&plain), Ok(plain));
    }

    /// Every way to reach a MediaWiki page names one locator: the article
    /// URL with underscores or spaces, a lower-case first letter, extra
    /// whitespace, percent-encoding, `index.php`, `api.php` reads and edits.
    #[test]
    fn wiki_page_spellings_share_resource_id(
        words in prop::collection::vec("[a-z][a-z0-9]{0,5}", 1..4),
        mobile in any::<bool>(),
    ) {
        let config = ExtractConfig::default();
        let title_spaced = words.join(" ");
        let title_under = words.join("_");
        let encoded = words.join("%20");
        let host = if mobile { "en.m.wikipedia.org" } else { "en.wikipedia.org" };
        let urls = [
            format!("https://{host}/wiki/{title_under}"),
            format!("https://en.wikipedia.org/wiki/{encoded}"),
            format!("https://en.wikipedia.org/w/index.php?title={title_under}&action=raw"),
            format!("https://en.wikipedia.org/w/api.php?action=query&titles=__{title_under}__&prop=revisions"),
        ];
        let mut first = title_spaced.chars();
        let capital: String = first
            .next()
            .map(|c| c.to_uppercase().chain(first).collect())
            .unwrap_or_default();
        let expected = Locator::Url {
            scheme: "https".to_owned(),
            host: Host("en.wikipedia.org".to_owned()),
            path: format!("/wiki/{}", capital.replace(' ', "_")),
            query: None,
        };
        for url in urls {
            let request = HttpRequest::get(url_locator(&url).expect("a URL"));
            let access = config.sites().apply(&request).expect("a wiki page");
            prop_assert_eq!(access.locators, vec![expected.clone()], "{}", url);
        }
        let edit = extract(
            &config,
            &context_in("/w"),
            &call("Bash", json!({ "command": format!(
                "curl -X POST https://en.wikipedia.org/w/api.php -d action=edit --data-urlencode 'title= {title_spaced} ' -d text=x"
            ) })),
            Some(&ok("{}")),
        )
        .expect("extracts");
        prop_assert_eq!(edit.len(), 1);
        prop_assert_eq!(&edit[0].locator, &expected);
    }

    /// A repository's file is one locator in every clone, whatever its root.
    #[test]
    fn repo_files_share_resource_id(
        root_a in prop::collection::vec(name(), 1..4),
        root_b in prop::collection::vec(name(), 1..4),
        inside in prop::collection::vec(name(), 1..4),
    ) {
        let repo = RepoId::parse("https://github.com/AgentVillage/Atlas.git", None).expect("a remote");
        let locate = |root: &[String]| {
            let mut repos = RepoBindings::default();
            let root = format!("/{}", root.join("/"));
            repos.bind(AbsolutePath::parse(&root).expect("absolute"), repo.clone());
            file_locator(&format!("{root}/./{}", inside.join("/")), &tool(), scope(None, &repos))
        };
        let expected = Locator::File {
            host: Some(Host("github.com/agentvillage/atlas".to_owned())),
            path: format!("/{}", inside.join("/")),
        };
        prop_assert_eq!(locate(&root_a), Ok(expected.clone()));
        prop_assert_eq!(locate(&root_b), Ok(expected));
    }

    /// `flow.resource.pattern-overlap-exact`: two patterns overlap exactly
    /// when some locator matches both, checked over a universe of locators
    /// that holds a witness for every overlapping pair the generator makes.
    #[test]
    fn overlap_iff_shared_locator(p in pattern(), q in pattern()) {
        let shared = universe().iter().any(|l| p.matches(l) && q.matches(l));
        prop_assert_eq!(p.overlaps(&q), shared);
        prop_assert_eq!(p.overlaps(&q), q.overlaps(&p));
    }
}

const PATHS: [&str; 6] = ["/", "/x", "/x/", "/x/y", "/xy", "/x/y/z"];

fn universe() -> Vec<Locator> {
    let mut all = Vec::new();
    for path in PATHS {
        for host in ["a", "b"] {
            all.push(Locator::Url {
                scheme: "https".to_owned(),
                host: Host(host.to_owned()),
                path: path.to_owned(),
                query: None,
            });
        }
        for host in [None, Some("a"), Some("b")] {
            all.push(Locator::File {
                host: host.map(|h| Host(h.to_owned())),
                path: path.to_owned(),
            });
        }
    }
    for server in ["s", "t"] {
        all.push(Locator::Mcp {
            server: server.to_owned(),
            tool: ToolName("page".to_owned()),
            target: None,
        });
    }
    all.push(Locator::Opaque {
        tool: ToolName("Bash".to_owned()),
        key: "x".to_owned(),
    });
    all
}

fn pattern() -> impl Strategy<Value = ResourcePattern> {
    let host = prop::sample::select(&["a", "b"][..]).prop_map(|h| Host(h.to_owned()));
    let path = prop::sample::select(&PATHS[..]).prop_map(str::to_owned);
    prop_oneof![
        prop::sample::select(universe()).prop_map(ResourcePattern::Exact),
        host.clone().prop_map(ResourcePattern::Host),
        (host.clone(), path.clone())
            .prop_map(|(host, path_prefix)| ResourcePattern::UrlPrefix { host, path_prefix }),
        (prop::option::of(host), path)
            .prop_map(|(host, prefix)| ResourcePattern::PathPrefix { host, prefix }),
        prop::sample::select(&["s", "t"][..])
            .prop_map(|s| ResourcePattern::McpServer(s.to_owned())),
    ]
}

/// `flow.resource.relative-path-opaque`.
#[test]
fn relative_path_without_cwd_is_opaque() {
    let repos = no_repos();
    for written in [
        "notes.md",
        "./notes.md",
        "../shared/notes.md",
        "~/notes.md",
        "~bob/x",
        "C:\\x\\y",
    ] {
        assert_eq!(
            file_locator(written, &tool(), scope(None, &repos)),
            Ok(Locator::Opaque {
                tool: tool(),
                key: written.to_owned(),
            }),
            "{written}",
        );
    }
    // A home or drive path stays opaque even with a working directory.
    let cwd = AbsolutePath::parse("/w").ok();
    assert_eq!(
        file_locator("~/notes.md", &tool(), scope(cwd.as_ref(), &repos)),
        Ok(Locator::Opaque {
            tool: tool(),
            key: "~/notes.md".to_owned(),
        }),
    );
    // An opaque key never matches a declared path prefix.
    let pattern = ResourcePattern::PathPrefix {
        host: None,
        prefix: "/".to_owned(),
    };
    let opaque = file_locator("notes.md", &tool(), scope(None, &repos)).expect("opaque");
    assert!(!pattern.matches(&opaque));
}

#[test]
fn path_errors() {
    let repos = no_repos();
    assert_eq!(
        file_locator("", &tool(), scope(None, &repos)),
        Err(PathError::Empty)
    );
    assert_eq!(
        file_locator("/a\0b", &tool(), scope(None, &repos)),
        Err(PathError::Nul)
    );
    assert_eq!(AbsolutePath::parse("a"), Err(PathError::NotAbsolute));
    assert_eq!(
        AbsolutePath::parse("/../../a/./b/").map(AbsolutePath::into_string),
        Ok("/a/b".to_owned())
    );
}

#[test]
fn url_details() {
    let cases = [
        (
            "HTTP://Example.COM:80",
            Some(("http", "example.com", "/", None)),
        ),
        (
            "https://example.com:8443/a",
            Some(("https", "example.com:8443", "/a", None)),
        ),
        (
            "https://user:secret@example.com/a",
            Some(("https", "example.com", "/a", None)),
        ),
        (
            "https://example.com./a/./b/../c",
            Some(("https", "example.com", "/a/c", None)),
        ),
        (
            "https://example.com/%7euser/%2fx",
            Some(("https", "example.com", "/~user/%2Fx", None)),
        ),
        (
            "https://example.com/a?",
            Some(("https", "example.com", "/a", None)),
        ),
        (
            "https://example.com/a?&&b=1&",
            Some(("https", "example.com", "/a", Some("b=1"))),
        ),
        (
            "https://example.com/a?b=%7e&a=1",
            Some(("https", "example.com", "/a", Some("a=1&b=~"))),
        ),
        (
            "https://[::1]:8080/x",
            Some(("https", "[::1]:8080", "/x", None)),
        ),
        (
            "https://bücher.example/",
            Some(("https", "xn--bcher-kva.example", "/", None)),
        ),
        ("mailto:a@example.com", None),
        ("not a url", None),
        ("", None),
    ];
    for (text, expected) in cases {
        let expected = expected.map(|(scheme, host, path, query)| Locator::Url {
            scheme: scheme.to_owned(),
            host: Host(host.to_owned()),
            path: path.to_owned(),
            query: query.map(str::to_owned),
        });
        assert_eq!(url_locator(text).ok(), expected, "{text}");
    }
}

#[test]
fn scanned_urls() {
    assert_eq!(
        scan_urls(
            "See https://a.example/x, and (http://b.example/y). Also https://c.example/z?q=1."
        ),
        vec![
            "https://a.example/x",
            "http://b.example/y",
            "https://c.example/z?q=1"
        ],
    );
    assert!(scan_urls("no links here: http:// alone").is_empty());
}

#[test]
fn key_folding_options() {
    let exact = KeyCanon::default();
    assert_eq!(
        exact.canonical("  Release_Notes "),
        Ok("Release_Notes".to_owned())
    );
    assert_eq!(exact.canonical("   "), Err(KeyError::Empty));
    let path = KeyCanon {
        path_like: true,
        ..KeyCanon::default()
    };
    assert_eq!(path.canonical("/a//b/./c/../d/"), Ok("a/b/d".to_owned()));
    assert_eq!(path.canonical("/./"), Err(KeyError::Empty));
}

#[test]
fn site_rules_can_be_turned_off() {
    let request = HttpRequest::get(url_locator("https://en.wikipedia.org/wiki/x").expect("a URL"));
    assert!(SitesConfig::none().apply(&request).is_none());
    assert!(SitesConfig::default().apply(&request).is_some());
}

//! Knobs, the task protocol, the wiki store, percentiles and the CLI.

use std::time::Duration;

use crate::cli::{Command, UsageError};
use crate::http::BaseUrl;
use crate::knobs::{Fraction, KnobError, PositiveSpan, Rng, Span, parse_duration};
use crate::protocol::{PageSlug, TOPICS, Task, Topic};
use crate::swarm::stats::{Percentiles, percentile};
use crate::wiki::store::{Author, Wiki, WriteError};

fn args(text: &str) -> Vec<String> {
    text.split_whitespace().map(str::to_owned).collect()
}

#[test]
fn spans_parse() {
    assert_eq!("3..7".parse::<Span>(), Ok(Span::ordered(3, 7)));
    assert_eq!("3..=7".parse::<Span>(), Ok(Span::ordered(3, 7)));
    assert_eq!("5".parse::<Span>(), Ok(Span::ordered(5, 5)));
    assert!(matches!(
        "7..3".parse::<Span>(),
        Err(KnobError::Inverted(_))
    ));
    assert!(matches!("a..3".parse::<Span>(), Err(KnobError::Number(_))));
    assert!(matches!(
        "0..3".parse::<PositiveSpan>(),
        Err(KnobError::Zero(_))
    ));
    assert_eq!(Span::ordered(9, 2), Span::ordered(2, 9));
    assert_eq!(Span::ordered(2, 9).to_string(), "2..9");
}

#[test]
fn spans_draw_inside() {
    let mut rng = Rng::new(1);
    let span = Span::ordered(10, 13);
    let mut seen = [false; 4];
    for _ in 0..500 {
        let v = span.draw(&mut rng);
        assert!((10..=13).contains(&v));
        seen[(v - 10) as usize] = true;
    }
    assert!(seen.iter().all(|s| *s), "every value drawn");
    assert_eq!(Span::ordered(4, 4).draw(&mut rng), 4);
}

#[test]
fn fractions_and_durations_parse() {
    assert_eq!("0.25".parse::<Fraction>().map(Fraction::get), Ok(0.25));
    assert!("1.5".parse::<Fraction>().is_err());
    assert!("-0.1".parse::<Fraction>().is_err());
    assert!("NaN".parse::<Fraction>().is_err());
    assert_eq!(parse_duration("500ms"), Ok(Duration::from_millis(500)));
    assert_eq!(parse_duration("90s"), Ok(Duration::from_secs(90)));
    assert_eq!(parse_duration("5m"), Ok(Duration::from_secs(300)));
    assert_eq!(parse_duration("1h"), Ok(Duration::from_secs(3600)));
    assert_eq!(parse_duration("12"), Ok(Duration::from_secs(12)));
    assert!(parse_duration("soon").is_err());
}

#[test]
fn rng_is_deterministic_and_derived_streams_differ() {
    let a: Vec<u64> = (0..5)
        .map({
            let mut r = Rng::new(9);
            move |_| r.next_u64()
        })
        .collect();
    let b: Vec<u64> = (0..5)
        .map({
            let mut r = Rng::new(9);
            move |_| r.next_u64()
        })
        .collect();
    assert_eq!(a, b);
    let mut x = Rng::derive(9, &[b"agent", &1u32.to_le_bytes()]);
    let mut y = Rng::derive(9, &[b"agent", &2u32.to_le_bytes()]);
    assert_ne!(x.next_u64(), y.next_u64());
    // Part boundaries count: ("ab","c") is not ("a","bc").
    assert_ne!(
        Rng::derive(1, &[b"ab", b"c"]).next_u64(),
        Rng::derive(1, &[b"a", b"bc"]).next_u64()
    );
    let mut r = Rng::new(3);
    assert_eq!(r.below(0), 0);
    assert!((0..1000).all(|_| r.unit() < 1.0));
    assert!(!r.chance(Fraction::ZERO));
    assert!(r.chance(Fraction::ONE));
    assert_eq!(r.hex(12).len(), 12);
    assert!(r.base62(22).chars().all(|c| c.is_ascii_alphanumeric()));
}

#[test]
fn task_markers_round_trip() {
    let tasks = [
        Task::Chat { topic: 3 },
        Task::Write {
            page: "rate-limiting-0".parse().expect("slug"),
            topic: 0,
        },
        Task::Read {
            page: "x".parse().expect("slug"),
        },
    ];
    for task in tasks {
        assert_eq!(Task::parse(&task.marker()), Some(task.clone()));
        assert_eq!(Task::find(&task.prompt()), Some(task.clone()));
    }
    for junk in [
        "[task:write page=x]",
        "[task:read]",
        "[task:fly page=x]",
        "[task:read page=Bad]",
        "[task:chat topic=-1]",
        "[task:chat topic=1 extra=2]",
        "task:chat topic=1",
    ] {
        assert_eq!(Task::parse(junk), None, "{junk}");
    }
    // The last marker wins.
    let text = "[task:chat topic=1]\nthen\n[task:read page=a-1]";
    assert_eq!(
        Task::find(text),
        Some(Task::Read {
            page: "a-1".parse().expect("slug")
        })
    );
}

#[test]
fn page_slugs_are_checked() {
    assert!("a-b-9".parse::<PageSlug>().is_ok());
    for bad in ["", "A", "a/b", "a b", "ü", &"x".repeat(97)] {
        assert!(bad.parse::<PageSlug>().is_err(), "{bad}");
    }
    assert_eq!(PageSlug::of_page(17, 8).as_str(), "vacuum-tuning-17");
    let n = TOPICS.len() as u32;
    assert_eq!(Topic::of(n + 2).slug, format!("{}1", TOPICS[2].0));
    assert_ne!(Topic::of(2).label, Topic::of(n + 2).label);
}

#[test]
fn wiki_store_versions_pages() {
    let mut wiki = Wiki::new(10, 2);
    let a: PageSlug = "a".parse().expect("slug");
    let b: PageSlug = "b".parse().expect("slug");
    let alice = Author::new("agent-001").expect("author");
    let bob = Author::new("agent-002").expect("author");
    let first = wiki
        .put(a.clone(), "one".to_owned(), alice.clone())
        .expect("put");
    assert!(first.created && first.version == 1);
    let second = wiki
        .put(a.clone(), "two".to_owned(), bob.clone())
        .expect("put");
    assert!(!second.created && second.version == 2);
    let page = wiki.get(&a).expect("page");
    assert_eq!(
        (page.text.as_str(), page.version, &page.author),
        ("two", 2, &bob)
    );
    assert_eq!(
        wiki.put(b.clone(), "x".repeat(11), alice.clone()),
        Err(WriteError::TooLarge {
            size: 11,
            limit: 10
        })
    );
    wiki.put(b, "x".to_owned(), alice.clone()).expect("put");
    assert_eq!(
        wiki.put("c".parse().expect("slug"), "y".to_owned(), alice),
        Err(WriteError::Full { limit: 2 })
    );
    let list = wiki.list();
    assert_eq!(list.len(), 2);
    assert_eq!(list[0].page, "a");
    assert_eq!(list[0].bytes, 3);
    assert!(Author::new("").is_none());
    assert!(Author::new("has space").is_none());
}

#[test]
fn percentiles_are_nearest_rank() {
    let values: Vec<u64> = (1..=100).collect();
    assert_eq!(percentile(&values, 50.0), Some(50));
    assert_eq!(percentile(&values, 95.0), Some(95));
    assert_eq!(percentile(&values, 99.0), Some(99));
    assert_eq!(percentile(&values, 100.0), Some(100));
    assert_eq!(percentile(&[7], 50.0), Some(7));
    assert_eq!(percentile(&[], 50.0), None);
    let p = Percentiles::of(vec![3000, 1000, 2000]).expect("percentiles");
    assert_eq!((p.p50, p.p95, p.p99, p.max), (2, 3, 3, 3));
    assert!(Percentiles::of(Vec::new()).is_none());
}

#[test]
fn base_urls_parse() {
    let url: BaseUrl = "http://crosstalk:8080/anthropic/".parse().expect("url");
    assert_eq!(
        (url.host.as_str(), url.port, url.prefix.as_str()),
        ("crosstalk", 8080, "/anthropic")
    );
    let url: BaseUrl = "http://wiki".parse().expect("url");
    assert_eq!((url.port, url.prefix.as_str()), (80, ""));
    let url: BaseUrl = "http://[::1]:9/x".parse().expect("url");
    assert_eq!((url.host.as_str(), url.port), ("::1", 9));
    for bad in [
        "https://a",
        "http://",
        "http://a:b",
        "http://a/x?q=1",
        "a:80",
    ] {
        assert!(bad.parse::<BaseUrl>().is_err(), "{bad}");
    }
}

#[test]
fn cli_parses_each_subcommand() {
    let Ok(Command::Swarm { config, json }) = Command::parse(&args(
        "swarm --agents 200 --agents-per-key 10 --think-ms 100..200 --turns 3 \
         --write-fraction 0.5 --read-fraction 0.5 --pages 9 --topics 3 --duration 90s \
         --seed 5 --stream-fraction 0.5 --claude-code-shape --json \
         --gateway http://gw:1/anthropic --ground-truth /tmp/t.jsonl",
    )) else {
        panic!("swarm parses")
    };
    assert!(json);
    assert_eq!(config.agents.get(), 200);
    assert_eq!(config.agents_per_key.get(), 10);
    assert_eq!(config.think_ms, Span::ordered(100, 200));
    assert_eq!(config.turns.get(), Span::ordered(3, 3));
    assert_eq!(config.duration, Duration::from_secs(90));
    assert!(config.claude_code_shape);
    assert_eq!(config.gateway.host, "gw");
    assert_eq!(config.wiki.port, 8090);
    assert!(config.ground_truth.is_some());

    let Ok(Command::Swarm { config, .. }) = Command::parse(&args("swarm")) else {
        panic!("defaults parse")
    };
    assert_eq!(config.agents.get(), 150);
    assert!(!config.claude_code_shape);
    assert_eq!(config.stream_fraction, Fraction::ONE);

    let Ok(Command::Upstream(up)) =
        Command::parse(&args("upstream --listen=127.0.0.1:1 --stream-ms 5..9"))
    else {
        panic!("upstream parses")
    };
    assert_eq!(up.listen.port(), 1);
    assert_eq!(up.generation.stream_ms, Span::ordered(5, 9));
    assert!(
        matches!(Command::parse(&args("wiki --max-pages 3")), Ok(Command::Wiki(w)) if w.max_pages == 3)
    );
    assert!(matches!(
        Command::parse(&args("healthcheck --url http://127.0.0.1:1/healthz")),
        Ok(Command::Healthcheck { .. })
    ));
    assert_eq!(Command::parse(&args("help")), Ok(Command::Help));
}

#[test]
fn cli_refuses_bad_lines() {
    assert_eq!(Command::parse(&[]), Err(UsageError::Missing));
    assert!(matches!(
        Command::parse(&args("fly")),
        Err(UsageError::UnknownCommand(_))
    ));
    assert!(matches!(
        Command::parse(&args("swarm --nope 1")),
        Err(UsageError::UnknownOption(_))
    ));
    assert!(matches!(
        Command::parse(&args("swarm --agents")),
        Err(UsageError::NoValue(_))
    ));
    assert!(matches!(
        Command::parse(&args("swarm --agents 0")),
        Err(UsageError::Value { .. })
    ));
    assert!(matches!(
        Command::parse(&args("swarm --agents 1 --agents 2")),
        Err(UsageError::Twice(_))
    ));
    assert!(matches!(
        Command::parse(&args("swarm --write-fraction 0.7 --read-fraction 0.7")),
        Err(UsageError::Value { .. })
    ));
    assert!(matches!(
        Command::parse(&args("swarm stray")),
        Err(UsageError::Positional(_))
    ));
    assert!(matches!(
        Command::parse(&args("healthcheck")),
        Err(UsageError::Required(_))
    ));
}

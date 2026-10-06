//! The two prose generators and how the fake model picks one: the templated
//! generator is frozen byte for byte, the high-entropy one shares no 32-byte
//! run between unrelated paragraphs, and the system prompt's style marker
//! selects between them on every path that writes prose.

use std::collections::{BTreeSet, HashMap};

use serde_json::{Value, json};

use crate::anthropic::ResponseBlock;
use crate::http::BaseUrl;
use crate::knobs::{Rng, Span};
use crate::protocol::{HTTP_TOOL, PageSlug, Scenario, TOPICS, Task, Topic, tool_definitions};
use crate::swarm::agent::Agent;
use crate::swarm::config::SwarmConfig;
use crate::upstream::generate::{GenConfig, generate, parse_request};
use crate::upstream::text::{TEMPLATES, high_entropy_paragraph, prose, templated_paragraph};

/// The window no two unrelated high-entropy paragraphs may share.
const WINDOW: usize = 32;

/// Pinned before the high-entropy generator existed: the templated
/// generator's bytes, and where it leaves the stream, never change.
#[test]
fn templated_paragraph_is_frozen() {
    let mut rng = Rng::new(1);
    assert_eq!(
        templated_paragraph(&mut rng, &Topic::of(3), 50),
        "The main risk with root cause is that the dashboard hides it until load peaks. \
         We tried rollback in the staging cluster and rolled it back after 505 minutes. \
         One option is to pair timeline with blast radius, which last week's numbers already \
         supports. Open question: does pager alert interact with blast radius under the \
         staging cluster?"
    );
    let mut rng = Rng::derive(9, &[b"body"]);
    assert_eq!(
        templated_paragraph(&mut rng, &Topic::of(21), 30),
        "The main risk with BM25 is that the open question hides it until load peaks. \
         I would keep query rewrite as is and revisit click model after the next release."
    );
    assert_eq!(rng.next_u64(), 5_994_565_010_603_501_570);
}

#[test]
fn prose_picks_the_generator_by_scenario() {
    let topic = Topic::of(5);
    let run = |scenario| prose(scenario, &mut Rng::new(77), &topic, 60);
    assert_eq!(
        run(Scenario::Boilerplate),
        templated_paragraph(&mut Rng::new(77), &topic, 60)
    );
    assert_eq!(
        run(Scenario::Headline),
        high_entropy_paragraph(&mut Rng::new(77), &topic, 60)
    );
    assert_ne!(run(Scenario::Headline), run(Scenario::Boilerplate));
}

#[test]
fn high_entropy_paragraph_is_deterministic() {
    for seed in 0..20 {
        let topic = Topic::of(seed as u32);
        let a = high_entropy_paragraph(&mut Rng::new(seed), &topic, 80);
        let b = high_entropy_paragraph(&mut Rng::new(seed), &topic, 80);
        assert_eq!(a, b);
        assert_ne!(
            a,
            high_entropy_paragraph(&mut Rng::new(seed + 1000), &topic, 80)
        );
    }
}

/// Whitespace words of `text` that are an invented word: not a number and
/// not a word of any template, topic term, label or common phrase.
fn invented_share(text: &str) -> f64 {
    let vocabulary = vocabulary();
    let words: Vec<String> = text.split_whitespace().map(normal).collect();
    let words: Vec<&String> = words.iter().filter(|w| !is_number(w)).collect();
    let invented = words.iter().filter(|w| !vocabulary.contains(**w)).count();
    invented as f64 / words.len().max(1) as f64
}

fn normal(word: &str) -> String {
    word.trim_matches(|c: char| !c.is_ascii_alphanumeric())
        .to_lowercase()
}

fn is_number(word: &str) -> bool {
    !word.is_empty() && word.chars().all(|c| c.is_ascii_digit())
}

/// Every word the templated generator can write.
fn vocabulary() -> BTreeSet<String> {
    let mut words = BTreeSet::new();
    let mut add = |text: &str| {
        words.extend(text.split_whitespace().map(normal));
    };
    TEMPLATES.iter().for_each(|t| add(t));
    Topic::common().iter().for_each(|c| add(c));
    for (_, label, terms) in TOPICS {
        add(label);
        add("(track)");
        terms.iter().for_each(|t| add(t));
    }
    words
}

#[test]
fn high_entropy_paragraph_keeps_the_shape() {
    let mut all = Vec::new();
    for seed in 0..200u64 {
        let mut rng = Rng::derive(seed, &[b"shape"]);
        let topic = Topic::of(u32::try_from(seed % 40).expect("small"));
        let words = 1 + rng.index(160);
        let text = high_entropy_paragraph(&mut rng, &topic, words);
        assert!(text.is_ascii(), "{text}");
        let count = text.split_whitespace().count();
        // Whole sentences: at least the budget, at most one sentence more.
        assert!((words..=words + 16).contains(&count), "{count} for {words}");
        assert!(text.ends_with(['.', '?']), "{text}");
        assert!(
            text.chars().next().is_some_and(|c| c.is_ascii_uppercase()),
            "{text}"
        );
        for sentence in text.split_inclusive(['.', '?']) {
            let n = sentence.split_whitespace().count();
            assert!((8..=16).contains(&n), "{n} words: {sentence:?}");
        }
        // Mostly invented words (a term counts once per word it has).
        assert!(invented_share(&text) > 0.4, "{text}");
        all.push(text);
    }
    assert!(invented_share(&all.join(" ")) > 0.6);
    let text = high_entropy_paragraph(&mut Rng::new(3), &Topic::of(1), 400);
    assert!(
        Topic::of(1).terms.iter().any(|term| text.contains(term)),
        "{text}"
    );
    assert!(text.chars().any(|c| c.is_ascii_digit()), "{text}");
}

/// The first pair of paragraphs (by index) sharing a `WINDOW`-byte run,
/// with that run.
fn shared_window(paragraphs: &[String]) -> Option<(usize, usize, String)> {
    let mut seen: HashMap<&[u8], usize> = HashMap::new();
    for (index, text) in paragraphs.iter().enumerate() {
        for window in text.as_bytes().windows(WINDOW) {
            match seen.get(window) {
                Some(&other) if other != index => {
                    return Some((other, index, String::from_utf8_lossy(window).into_owned()));
                }
                Some(_) => {}
                None => {
                    seen.insert(window, index);
                }
            }
        }
    }
    None
}

/// 2000 paragraphs, one per seed, each from its own body, cycling through
/// 40 topics (track topics included) and 40..160 words.
fn corpus(generator: fn(&mut Rng, &Topic, usize) -> String) -> Vec<String> {
    (0..2000u64)
        .map(|seed| {
            let body = format!("{{\"agent\":{},\"turn\":{}}}", seed % 37, seed / 37);
            let mut rng = Rng::derive(seed, &[body.as_bytes()]);
            let topic = Topic::of(u32::try_from(seed % 40).expect("small"));
            let words = usize::try_from(Span::ordered(40, 160).draw(&mut rng)).expect("small");
            generator(&mut rng, &topic, words)
        })
        .collect()
}

#[test]
fn high_entropy_paragraphs_from_different_seeds_share_no_32_bytes() {
    let paragraphs = corpus(high_entropy_paragraph);
    let bytes: usize = paragraphs.iter().map(String::len).sum();
    assert!(bytes > 1_000_000, "{bytes} bytes");
    assert_eq!(shared_window(&paragraphs), None);
}

/// Why the boilerplate scenario exists: the same corpus from the templated
/// generator does share runs of 32 bytes between unrelated paragraphs.
#[test]
fn templated_paragraphs_from_different_seeds_do_share_32_bytes() {
    let paragraphs = corpus(templated_paragraph);
    let shared = shared_window(&paragraphs);
    assert!(shared.is_some());
}

fn wiki() -> BaseUrl {
    "http://wiki:8090".parse().expect("url")
}

fn request(system: Option<Value>, messages: Value) -> Vec<u8> {
    let mut body = json!({
        "model": "claude-opus-5-5",
        "max_tokens": 4096,
        "tools": tool_definitions(),
        "messages": messages,
        "stream": true,
    });
    if let (Some(system), Some(object)) = (system, body.as_object_mut()) {
        object.insert("system".to_owned(), system);
    }
    serde_json::to_vec(&body).expect("encode")
}

fn blocks(text: &str) -> Value {
    json!([{"type": "text", "text": "You are agent-001."}, {"type": "text", "text": text}])
}

fn user(task: &Task) -> Value {
    json!([{"role": "user", "content": [{"type": "text", "text": task.prompt()}]}])
}

#[test]
fn the_style_comes_from_the_system_prompt() {
    let chat = user(&Task::Chat { topic: 1 });
    let style = |system: Option<Value>| {
        parse_request(&request(system, chat.clone()))
            .expect("request")
            .style
    };
    let boilerplate = Scenario::Boilerplate.marker();
    assert_eq!(boilerplate, "[style:boilerplate]");
    assert_eq!(Scenario::Headline.marker(), "[style:headline]");
    assert_eq!(
        style(Some(json!(format!("Be concise.\n\n{boilerplate}")))),
        Scenario::Boilerplate
    );
    assert_eq!(style(Some(blocks(&boilerplate))), Scenario::Boilerplate);
    assert_eq!(
        style(Some(json!("Be concise.\n\n[style:headline]"))),
        Scenario::Headline
    );
    assert_eq!(style(Some(blocks("[style:headline]"))), Scenario::Headline);
    // No marker, no system prompt, an unknown style or a malformed system
    // field: headline.
    assert_eq!(style(Some(json!("Be concise."))), Scenario::Headline);
    assert_eq!(style(None), Scenario::Headline);
    assert_eq!(style(Some(json!("[style:other]"))), Scenario::Headline);
    assert_eq!(style(Some(json!(7))), Scenario::Headline);
    // A marker in a user turn is not the system prompt's.
    let body = request(
        None,
        json!([{"role": "user", "content": format!("{boilerplate}\n\n[task:chat topic=1]")}]),
    );
    assert_eq!(
        parse_request(&body).expect("request").style,
        Scenario::Headline
    );
}

#[test]
fn scenario_names_round_trip() {
    for scenario in Scenario::ALL {
        assert_eq!(scenario.name().parse::<Scenario>(), Ok(scenario));
        assert_eq!(scenario.to_string(), scenario.name());
        assert_eq!(
            serde_json::to_value(scenario).expect("encode"),
            json!(scenario.name())
        );
        assert_eq!(
            Scenario::of_system(&format!("x {} y", scenario.marker())),
            scenario
        );
    }
    assert_eq!(Scenario::default(), Scenario::Headline);
    assert!("Boilerplate".parse::<Scenario>().is_err());
}

/// The prose of every answer kind for `system`: page text, chat, a
/// closing answer, a failed closing answer and an unmarked prompt.
fn answers(system: &str) -> Vec<String> {
    let config = GenConfig {
        seed: 5,
        words: Span::ordered(40, 80),
        ..GenConfig::default()
    };
    let page: PageSlug = "vacuum-tuning-1".parse().expect("slug");
    let read = Task::Read {
        page: page.clone(),
        base: wiki(),
    };
    let after = |result: Value| {
        json!([
            {"role": "user", "content": [{"type": "text", "text": read.prompt()}]},
            {"role": "assistant", "content": [{"type": "tool_use", "id": "toolu_1", "name": HTTP_TOOL, "input": {"method": "GET", "url": "http://wiki:8090/pages/vacuum-tuning-1"}}]},
            {"role": "user", "content": [result]},
        ])
    };
    let conversations = [
        user(&Task::Write {
            page,
            topic: 1,
            base: wiki(),
        }),
        user(&Task::Chat { topic: 2 }),
        after(json!({"type": "tool_result", "tool_use_id": "toolu_1", "content": "page text"})),
        after(
            json!({"type": "tool_result", "tool_use_id": "toolu_1", "content": "missing", "is_error": true}),
        ),
        json!([{"role": "user", "content": "hello there"}]),
    ];
    conversations
        .into_iter()
        .map(|messages| {
            let body = request(Some(blocks(system)), messages);
            let reply = generate(&config, &parse_request(&body).expect("request"), &body);
            match reply.message.content.as_slice() {
                [
                    ResponseBlock::Text { .. },
                    ResponseBlock::ToolUse { input, .. },
                ] => input["body"].as_str().expect("a PUT body").to_owned(),
                [ResponseBlock::Text { text }] => text
                    .strip_prefix("That did not work, so I'll continue from what I know. ")
                    .unwrap_or(text)
                    .to_owned(),
                other => panic!("unexpected answer {other:?}"),
            }
        })
        .collect()
}

#[test]
fn every_prose_path_honours_the_style() {
    for text in answers(&Scenario::Boilerplate.marker()) {
        assert_eq!(invented_share(&text), 0.0, "templated: {text}");
    }
    for system in [Scenario::Headline.marker(), "no marker".to_owned()] {
        for text in answers(&system) {
            assert!(invented_share(&text) > 0.4, "high-entropy: {text}");
        }
    }
}

#[test]
fn headline_answers_to_different_bodies_and_seeds_share_no_32_bytes() {
    let system = blocks(&Scenario::Headline.marker());
    let mut texts = Vec::new();
    for seed in 1..=4 {
        let config = GenConfig {
            seed,
            ..GenConfig::default()
        };
        for topic in 0..100 {
            let body = request(Some(system.clone()), user(&Task::Chat { topic }));
            let reply = generate(&config, &parse_request(&body).expect("request"), &body);
            let [ResponseBlock::Text { text }] = reply.message.content.as_slice() else {
                panic!("one text block")
            };
            texts.push(text.clone());
        }
    }
    assert_eq!(shared_window(&texts), None);
}

#[test]
fn agents_carry_the_scenario_marker() {
    for scenario in Scenario::ALL {
        let mut config = SwarmConfig::new(wiki(), wiki());
        config.scenario = scenario;
        let agent = Agent::new(&config, "01J0000000000000000000000A", 3);
        assert!(
            agent.profile.system.ends_with(&scenario.marker()),
            "{}",
            agent.profile.system
        );
        assert_eq!(Scenario::of_system(&agent.profile.system), scenario);
    }
}

/// Prints one paragraph of each generator (`--nocapture` to see them).
#[test]
fn sample_paragraphs() {
    let topic = Topic::of(7);
    for scenario in Scenario::ALL {
        println!(
            "{scenario}: {}",
            prose(scenario, &mut Rng::new(2026), &topic, 30)
        );
    }
}

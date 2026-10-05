//! The short-span exact path's hashing and token runs, and the new
//! configuration.

use proptest::prelude::*;

use crate::config::{ConfigError, ProvenanceConfig, ReaderOutputRules, ShortSpans, SpreadRule};
use crate::fingerprint::hash::{self, Prefix};
use crate::fingerprint::short::{token_runs, whole};
use crate::text::normalize;
use crate::text::normalize::normalized_string;

fn chars(text: &str) -> Vec<char> {
    text.chars().collect()
}

/// The texts every token run of `text` covers, normalized.
fn run_texts(text: &str, spans: ShortSpans) -> Vec<String> {
    token_runs(&normalize(text), spans)
        .into_iter()
        .map(|run| normalized_string(&text[run.start as usize..run.end as usize]))
        .collect()
}

#[test]
fn short_hash_is_pinned_and_never_a_kgram_fingerprint() {
    let value = chars("note for my partner: irxsbdcnmr");
    let window = hash::kgram(&value);
    assert_ne!(hash::short(window), window);
    // A change here is a migration of every stored short-span posting.
    assert_eq!(hash::short(window), 13_650_572_856_992_088_287);
    let found = whole(
        &normalize("  Note for my   partner: IRxSBdcNMr \n"),
        ShortSpans::default(),
    )
    .expect("admitted");
    assert_eq!(found.fingerprint.0, hash::short(window));
    assert_eq!(found.start, 2);
}

#[test]
fn whole_values_outside_the_range_have_no_hash() {
    let spans = ShortSpans::default();
    assert!(whole(&normalize("twenty-three characters"), spans).is_none());
    assert!(whole(&normalize("twenty-four characters!!"), spans).is_some());
    assert!(whole(&normalize(&"x".repeat(46)), spans).is_some());
    assert!(whole(&normalize(&"x".repeat(47)), spans).is_none());
    assert!(whole(&normalize("   "), spans).is_none());
}

#[test]
fn token_runs_start_and_end_on_boundaries() {
    let spans = ShortSpans::new(4, 13).expect("a range");
    let runs = run_texts("say \"hello there\" now", spans);
    assert!(runs.contains(&"hello there".to_owned()), "{runs:?}");
    assert!(runs.contains(&"\"hello there\"".to_owned()), "{runs:?}");
    assert!(runs.contains(&"say \"hello".to_owned()), "{runs:?}");
    // Never inside a word, never at a space.
    assert!(!runs.iter().any(|run| run.starts_with("ello")), "{runs:?}");
    assert!(
        !runs
            .iter()
            .any(|run| run.starts_with(' ') || run.ends_with(' '))
    );
    assert!(!runs.contains(&"hello ther".to_owned()), "{runs:?}");
}

#[test]
fn short_config_decodes_and_refuses_bad_ranges() {
    let config: ProvenanceConfig = serde_json::from_str(
        r#"{"short_spans": {"min_chars": 20, "max_chars": 40},
            "reader_output": {"min_chars": 80},
            "spread": {"agents": 5, "window_secs": 30, "distinctive_chars": 90}}"#,
    )
    .expect("decodes");
    assert_eq!(
        config.short_spans(),
        ShortSpans::new(20, 40).expect("range")
    );
    assert_eq!(config.reader_output(), ReaderOutputRules::new(80));
    assert_eq!(
        config.spread(),
        SpreadRule::new(5, std::time::Duration::from_secs(30), 90).expect("a rule")
    );
    let defaults = ProvenanceConfig::default();
    assert_eq!(defaults.short_spans().min_chars(), 24);
    assert_eq!(defaults.short_spans().max_chars(), 46);
    assert_eq!(defaults.reader_output().min_chars(), 64);
    assert!(!defaults.forwarding(), "forwarding is off by default");
    let on: ProvenanceConfig = serde_json::from_str(r#"{"forwarding": true}"#).expect("decodes");
    assert!(on.forwarding());
    assert_eq!(defaults.spread().agents(), 4);
    assert_eq!(defaults.spread().window().as_secs(), 60);
    assert_eq!(defaults.spread().distinctive_chars(), 64);
    for agents in [0, 1] {
        assert_eq!(
            SpreadRule::new(agents, std::time::Duration::from_secs(60), 64),
            Err(ConfigError::SpreadAgents { agents })
        );
    }
    assert!(serde_json::from_str::<ProvenanceConfig>(r#"{"spread": {"agents": 1}}"#).is_err());
    assert!(
        serde_json::from_str::<ProvenanceConfig>(r#"{"reader_output": {"cutoff": 5}}"#).is_err()
    );
    assert_eq!(
        ShortSpans::new(3, 40),
        Err(ConfigError::ShortSpanRange { min: 3, max: 40 })
    );
    assert_eq!(
        ShortSpans::new(30, 20),
        Err(ConfigError::ShortSpanRange { min: 30, max: 20 })
    );
    for json in [
        r#"{"short_spans": {"min_chars": 2}}"#,
        r#"{"short_spans": {"min_chars": 50}}"#,
        r#"{"short_spans": {"unknown": 1}}"#,
        r#"{"reader_output": {"unknown": 1}}"#,
    ] {
        assert!(
            serde_json::from_str::<ProvenanceConfig>(json).is_err(),
            "{json}"
        );
    }
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 256, failure_persistence: None, ..ProptestConfig::default() })]

    /// Prefix windows hash as `kgram` does, for every window.
    #[test]
    fn prefix_windows_equal_kgram_hashes(text in "[a-zß一 ]{0,40}", start in 0usize..40, len in 1usize..40) {
        let all = chars(&text);
        let prefix = Prefix::new(&all);
        let end = start + len;
        match all.get(start..end) {
            Some(window) => prop_assert_eq!(prefix.window(start, end), Some(hash::kgram(window))),
            None => prop_assert_eq!(prefix.window(start, end), None),
        }
    }

    /// `provenance.match.short-span-exact`: a whole value of admitted
    /// length is found among the token runs of any text holding it between
    /// boundaries, however it is cased and spaced there.
    #[test]
    fn whole_value_is_a_token_run_of_any_text_holding_it(
        words in proptest::collection::vec("[a-z]{2,6}", 3..8),
        before in "[a-z ]{0,12}",
        after in "[a-z ]{0,12}",
        upper in any::<bool>(),
    ) {
        let spans = ShortSpans::default();
        let value = words.join(" ");
        let Some(origin) = whole(&normalize(&value), spans) else {
            return Ok(());
        };
        let shown = if upper { value.to_uppercase().replace(' ', "  ") } else { value.clone() };
        let read = format!("{before}: {shown}. {after}");
        let runs = token_runs(&normalize(&read), spans);
        prop_assert!(runs.iter().any(|run| run.fingerprint == origin.fingerprint));
    }
}

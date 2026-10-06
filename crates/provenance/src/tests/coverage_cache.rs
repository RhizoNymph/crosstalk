//! The scanner's coverage cache: a coverage extended from a kept prefix is
//! the coverage built from scratch.

use crosstalk_spec::observed::message::Message;
use crosstalk_testkit::build::message::{assistant_text, message, tool_result, user_text};

use super::fixtures::{config, sentence};
use crate::scan::Scanner;
use crate::scan::cache::CoverageCache;
use crate::segment::{Coverage, message_kgrams};

/// Every message of a conversation-like history, with text repeated across
/// messages more often than a coverage keeps occurrences of one k-gram.
fn history() -> Vec<Message> {
    let shared = sentence("shared");
    let mut messages = Vec::new();
    for turn in 0..12 {
        let own = sentence(&format!("turn {turn}"));
        messages.push(match turn % 3 {
            0 => message(user_text(&format!("{shared} {own}"))),
            1 => message(assistant_text(&format!("{own}. {shared}"))),
            _ => message(tool_result(
                &format!("call_{turn}"),
                &format!("{{\"page\": \"{own} {shared}\"}}"),
            )),
        });
    }
    messages
}

/// Whether two coverages agree on every input and on every k-gram of
/// `messages`.
fn assert_same(scanner: &Scanner, messages: &[&Message], got: &Coverage, want: &Coverage) {
    assert_eq!(got.inputs(), want.inputs());
    assert_eq!(got.positions(), want.positions());
    for input in 0..want.inputs() {
        assert_eq!(got.input(input), want.input(input));
    }
    for message in messages {
        let layers = message_kgrams(scanner.winnowing(), scanner.pipeline(), message, |_| true);
        for fingerprint in layers.iter().flatten() {
            assert_eq!(got.get(*fingerprint), want.get(*fingerprint));
        }
    }
}

#[test]
fn extended_coverage_is_the_coverage_from_scratch() {
    let scanner = Scanner::new(&config());
    let messages = history();
    let all: Vec<&Message> = messages.iter().collect();
    // A conversation growing by a few messages each scan, the coverage kept
    // after each, then lists that are not extensions of what is kept.
    let lists: Vec<Vec<&Message>> = vec![
        all[..2].to_vec(),
        all[..5].to_vec(),
        all[..5].to_vec(),
        all[..9].to_vec(),
        all.clone(),
        vec![all[0], all[3], all[1]],
        all[1..].to_vec(),
        all[..4].to_vec(),
    ];
    for inputs in lists {
        let got = scanner.coverage(&inputs);
        let want = scanner.segmenter().coverage(&inputs);
        assert_same(&scanner, &all, &got, &want);
        scanner.keep_coverage(&inputs, got);
    }
}

#[test]
fn the_cache_takes_the_longest_kept_prefix() {
    let scanner = Scanner::new(&config());
    let messages = history();
    let hashes: Vec<_> = messages.iter().map(|message| message.hash).collect();
    let inputs: Vec<&Message> = messages.iter().collect();
    let mut cache = CoverageCache::new(usize::MAX, 8);
    cache.keep(hashes[..2].to_vec(), scanner.coverage(&inputs[..2]));
    cache.keep(hashes[..6].to_vec(), scanner.coverage(&inputs[..6]));
    cache.keep(vec![hashes[1]], scanner.coverage(&inputs[1..2]));
    let Some((length, coverage)) = cache.take_prefix(&hashes[..8]) else {
        panic!("a kept prefix");
    };
    assert_eq!(length, 6);
    assert_eq!(coverage.inputs(), 6);
    // Taken out: the next longest is the two-message prefix.
    assert_eq!(
        cache.take_prefix(&hashes[..8]).map(|(length, _)| length),
        Some(2)
    );
    assert_eq!(
        cache.take_prefix(&hashes[..8]).map(|(length, _)| length),
        None
    );
    assert_eq!(cache.len(), 1);
}

#[test]
fn the_cache_stays_within_its_budget() {
    let scanner = Scanner::new(&config());
    let messages = history();
    let hashes: Vec<_> = messages.iter().map(|message| message.hash).collect();
    let inputs: Vec<&Message> = messages.iter().collect();
    let one = scanner.coverage(&inputs[..1]);
    let weight = one.positions();
    assert!(weight > 0);
    let mut cache = CoverageCache::new(weight * 2, 8);
    cache.keep(hashes[..1].to_vec(), one);
    cache.keep(vec![hashes[1]], scanner.coverage(&inputs[1..2]));
    cache.keep(vec![hashes[2]], scanner.coverage(&inputs[2..3]));
    // The oldest went first.
    assert!(cache.take_prefix(&hashes[..1]).is_none());
    // A coverage over the whole budget is not kept.
    let mut small = CoverageCache::new(weight - 1, 8);
    small.keep(hashes[..1].to_vec(), scanner.coverage(&inputs[..1]));
    assert!(small.is_empty());
    // Nor more entries than the cap.
    let mut capped = CoverageCache::new(usize::MAX, 2);
    for (hash, message) in hashes.iter().zip(&inputs).take(3) {
        capped.keep(vec![*hash], scanner.coverage(std::slice::from_ref(message)));
    }
    assert_eq!(capped.len(), 2);
    assert!(capped.take_prefix(&hashes[..1]).is_none());
}

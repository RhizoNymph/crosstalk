//! Deterministic prose, two generators, both drawing only from the `rng`
//! they are given, so the same seed and request body give the same bytes.
//!
//! - [`high_entropy_paragraph`] (the `headline` scenario): sentences of
//!   invented words built from a space of 300 consonant-vowel syllables,
//!   mixed with the topic's real terms and numbers. Unrelated paragraphs
//!   share no run of 32 bytes, so a shared span is a real copy.
//! - [`templated_paragraph`] (the `boilerplate` scenario): 16 sentence
//!   templates filled with the topic's terms, generic phrases and numbers.
//!   Unrelated paragraphs share template fragments of 30-50 bytes, as real
//!   agents share boilerplate. Its output is frozen: the same rng state
//!   gives the same bytes as every earlier version, so boilerplate runs stay
//!   comparable.
//!
//! [`prose`] picks one by [`Scenario`]. Both stop at the first sentence end
//! at or past the word budget.

use crate::knobs::Rng;
use crate::protocol::{Scenario, Topic};

/// About `words` words of prose on `topic` in `scenario`'s style.
pub fn prose(scenario: Scenario, rng: &mut Rng, topic: &Topic, words: usize) -> String {
    match scenario {
        Scenario::Headline => high_entropy_paragraph(rng, topic, words),
        Scenario::Boilerplate => templated_paragraph(rng, topic, words),
    }
}

/// Syllable onsets: 30.
const ONSETS: &[&str] = &[
    "b", "c", "d", "f", "g", "h", "j", "k", "l", "m", "n", "p", "r", "s", "t", "v", "w", "y", "z",
    "br", "dr", "gr", "kl", "pl", "st", "tr", "sh", "ch", "th", "sk",
];
/// Syllable nuclei: 10, so 300 consonant-vowel syllables.
const NUCLEI: &[&str] = &["a", "e", "i", "o", "u", "ai", "ea", "io", "ou", "ue"];
/// How a word may end, after its last syllable (empty twice as likely).
const CODAS: &[&str] = &["", "", "n", "r", "l", "s", "t", "m", "k", "x"];

/// About `words` words of high-entropy prose on `topic`, ending at a
/// sentence end. Each sentence is 8 to 16 words: invented words of 2 to 4
/// syllables, with a topic term or a number after some of them (never two
/// in a row), ending in `.` or, one time in eight, `?`.
pub fn high_entropy_paragraph(rng: &mut Rng, topic: &Topic, words: usize) -> String {
    let mut out = String::new();
    let mut count = 0;
    while count < words.max(1) {
        let sentence = high_entropy_sentence(rng, topic);
        count += sentence.split_whitespace().count();
        if !out.is_empty() {
            out.push(' ');
        }
        out.push_str(&sentence);
    }
    out
}

fn high_entropy_sentence(rng: &mut Rng, topic: &Topic) -> String {
    let target = 8 + rng.index(7);
    let mut tokens: Vec<String> = Vec::with_capacity(target);
    let mut count = 0;
    // A fixed token (term or number) only ever follows an invented word, so
    // no two sit side by side.
    let mut after_word = false;
    while count < target {
        let roll = rng.below(100);
        let token = match rng.pick(topic.terms) {
            Some(term) if after_word && roll < 22 => {
                after_word = false;
                (*term).to_owned()
            }
            _ if after_word && roll < 32 => {
                after_word = false;
                number(rng)
            }
            _ => {
                after_word = true;
                invented_word(rng)
            }
        };
        count += token.split_whitespace().count();
        tokens.push(token);
    }
    let ending = if rng.below(8) == 0 { '?' } else { '.' };
    let mut sentence = capitalize(&tokens.join(" "));
    sentence.push(ending);
    sentence
}

/// A number as the templates write them: a count or a percentage.
fn number(rng: &mut Rng) -> String {
    if rng.below(2) == 0 {
        (rng.below(990) + 10).to_string()
    } else {
        format!("{}%", rng.below(60) + 5)
    }
}

/// A word of 2 to 4 consonant-vowel syllables and an optional coda.
fn invented_word(rng: &mut Rng) -> String {
    let syllables = 2 + rng.index(3);
    let mut word = String::with_capacity(syllables * 3 + 1);
    for _ in 0..syllables {
        word.push_str(rng.pick(ONSETS).copied().unwrap_or("t"));
        word.push_str(rng.pick(NUCLEI).copied().unwrap_or("a"));
    }
    word.push_str(rng.pick(CODAS).copied().unwrap_or(""));
    word
}

/// Sentence templates. `{t}` is a topic term, `{c}` a common phrase, `{l}`
/// the topic's label, `{n}` a number, `{p}` a percentage.
pub const TEMPLATES: &[&str] = &[
    "We measured {t} on {c} and saw roughly {n} events per minute.",
    "The main risk with {t} is that {c} hides it until load peaks.",
    "I would keep {t} as is and revisit {t} after the next release.",
    "For {l}, {t} matters more than {t} at our current scale.",
    "{c} suggests that {t} accounts for about {p} of the problem.",
    "One option is to pair {t} with {t}, which {c} already supports.",
    "Nobody owns {t} yet, so I propose we track it with {c}.",
    "The data from {c} contradicts the earlier claim about {t}.",
    "If we change {t}, expect {n} follow-up tickets around {t}.",
    "Our notes on {l} still say {t} is fine; that is no longer true.",
    "Reducing {t} by {p} should be enough for the next quarter.",
    "We tried {t} in {c} and rolled it back after {n} minutes.",
    "The cheapest fix is to document {t} and alert on {t}.",
    "Compared with last month, {t} improved while {t} regressed by {p}.",
    "Open question: does {t} interact with {t} under {c}?",
    "I recommend a short spike on {t} before committing to {l}.",
];

/// About `words` words of templated prose on `topic`, ending at a sentence
/// end. Frozen: see the module doc.
pub fn templated_paragraph(rng: &mut Rng, topic: &Topic, words: usize) -> String {
    let mut out = String::new();
    let mut count = 0;
    while count < words.max(1) {
        let template = rng.pick(TEMPLATES).copied().unwrap_or(TEMPLATES[0]);
        let sentence = fill(rng, template, topic);
        count += sentence.split_whitespace().count();
        if !out.is_empty() {
            out.push(' ');
        }
        out.push_str(&sentence);
    }
    out
}

fn fill(rng: &mut Rng, template: &str, topic: &Topic) -> String {
    let mut out = String::with_capacity(template.len() * 2);
    let mut rest = template;
    while let Some(start) = rest.find('{') {
        out.push_str(&rest[..start]);
        let after = &rest[start..];
        let (value, used) = match after.get(..3) {
            Some("{t}") => (rng.pick(topic.terms).copied().unwrap_or("it").to_owned(), 3),
            Some("{c}") => (
                rng.pick(Topic::common())
                    .copied()
                    .unwrap_or("the team")
                    .to_owned(),
                3,
            ),
            Some("{l}") => (topic.label.clone(), 3),
            Some("{n}") => ((rng.below(990) + 10).to_string(), 3),
            Some("{p}") => (format!("{}%", rng.below(60) + 5), 3),
            _ => ("{".to_owned(), 1),
        };
        out.push_str(&value);
        rest = &after[used..];
    }
    out.push_str(rest);
    capitalize(&out)
}

fn capitalize(text: &str) -> String {
    let mut chars = text.chars();
    match chars.next() {
        Some(first) => first.to_uppercase().chain(chars).collect(),
        None => String::new(),
    }
}

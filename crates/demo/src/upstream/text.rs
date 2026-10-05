//! Deterministic prose: sentences from templates filled with a topic's
//! terms, generic phrases and numbers. Varied enough that two generated
//! paragraphs of useful length practically never share a long run of
//! words, so a copied paragraph is recognisable as a copy.

use crate::knobs::Rng;
use crate::protocol::Topic;

/// Sentence templates. `{t}` is a topic term, `{c}` a common phrase, `{l}`
/// the topic's label, `{n}` a number, `{p}` a percentage.
const TEMPLATES: &[&str] = &[
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

/// About `words` words of prose on `topic`, ending at a sentence end.
pub fn paragraph(rng: &mut Rng, topic: &Topic, words: usize) -> String {
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

//! Where every k-gram of a set of texts occurs: the inputs an output is
//! classified against.
//!
//! Each input message's text parts are expanded into their decode layers
//! (raw and decoded, `provenance.span.originated-absent-from-inputs`) and
//! every k-gram of every layer is recorded with the layer it sits in and its
//! position there, so the segmenter can follow a copied run k-gram by
//! k-gram through one contiguous stretch of one input.

use std::collections::HashMap;

use crosstalk_spec::derived::provenance::fingerprint::Fingerprint;
use crosstalk_spec::ids::MessageHash;
use crosstalk_spec::observed::message::Message;

use super::view::{text_parts, view};
use crate::decode::DecodePipeline;
use crate::fingerprint::Winnowing;

/// How many occurrences of one k-gram are kept. A k-gram repeated more
/// often than this is boilerplate within the inputs; following a run
/// needs only some of its occurrences.
const MAX_OCCURRENCES: usize = 8;

/// One place a k-gram occurs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Occurrence {
    /// The input message, by position in the inputs given.
    pub input: usize,
    /// The layer (one decoding of one part), numbered across all inputs.
    pub layer: usize,
    /// The k-gram's position among the layer's normalized characters.
    pub position: usize,
}

/// Every layer's k-gram fingerprints of one message, each layer in
/// position order: what the message adds to a coverage.
pub type MessageKGrams = Vec<Vec<Fingerprint>>;

/// The layers' k-grams of `message`'s text parts that `keep` admits.
pub fn message_kgrams(
    winnowing: &Winnowing,
    pipeline: &DecodePipeline,
    message: &Message,
    keep: impl Fn(u16) -> bool,
) -> MessageKGrams {
    let mut layers = Vec::new();
    for part in text_parts(message) {
        if !keep(part.index) {
            continue;
        }
        let base = view(&part.text, part.kind);
        for layer in pipeline.layers(base.text()) {
            layers.push(
                winnowing
                    .kgrams(layer.text.text())
                    .into_iter()
                    .map(|kgram| kgram.fingerprint)
                    .collect(),
            );
        }
    }
    layers
}

/// Every k-gram of a set of texts and where it occurs.
#[derive(Debug, Clone, Default)]
pub struct Coverage {
    occurrences: HashMap<Fingerprint, Vec<Occurrence>>,
    /// Each input's message; `None` for a bare text ([`Coverage::add_text`]).
    inputs: Vec<Option<MessageHash>>,
    layers: usize,
}

impl Coverage {
    /// The coverage of `inputs`: every layer of every text part.
    pub fn of_messages(
        winnowing: &Winnowing,
        pipeline: &DecodePipeline,
        inputs: &[&Message],
    ) -> Self {
        let mut coverage = Self::default();
        for message in inputs {
            coverage.add_message(winnowing, pipeline, message, |_| true);
        }
        coverage
    }

    /// Add every text part of `message` that `keep` admits (by part index).
    pub fn add_message(
        &mut self,
        winnowing: &Winnowing,
        pipeline: &DecodePipeline,
        message: &Message,
        keep: impl Fn(u16) -> bool,
    ) {
        let layers = message_kgrams(winnowing, pipeline, message, keep);
        self.add_kgrams(Some(message.hash), &layers);
    }

    /// Add one input given as its layers' k-gram fingerprints (each layer in
    /// position order); returns its input number.
    pub fn add_kgrams(&mut self, message: Option<MessageHash>, layers: &MessageKGrams) -> usize {
        let input = self.inputs.len();
        self.inputs.push(message);
        for fingerprints in layers {
            let layer = self.layers;
            self.layers += 1;
            for (position, fingerprint) in fingerprints.iter().enumerate() {
                let entry = self.occurrences.entry(*fingerprint).or_default();
                if entry.len() < MAX_OCCURRENCES {
                    entry.push(Occurrence {
                        input,
                        layer,
                        position,
                    });
                }
            }
        }
        input
    }

    /// Where `fingerprint` occurs; empty when nowhere.
    pub fn get(&self, fingerprint: Fingerprint) -> &[Occurrence] {
        self.occurrences
            .get(&fingerprint)
            .map_or(&[], |occurrences| occurrences.as_slice())
    }

    pub fn contains(&self, fingerprint: Fingerprint) -> bool {
        self.occurrences.contains_key(&fingerprint)
    }

    /// The message the `input`th input is.
    pub fn input(&self, input: usize) -> Option<MessageHash> {
        self.inputs.get(input).copied().flatten()
    }

    /// Add `text` as one more input of one layer, as it is (no decoding);
    /// returns its input number.
    pub fn add_text(&mut self, winnowing: &Winnowing, text: &str) -> usize {
        let layer = winnowing
            .kgrams(text)
            .into_iter()
            .map(|kgram| kgram.fingerprint)
            .collect();
        self.add_kgrams(None, &vec![layer])
    }

    pub fn is_empty(&self) -> bool {
        self.occurrences.is_empty()
    }
}

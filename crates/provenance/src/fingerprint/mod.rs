//! Winnowing over normalized shingles (Schleimer, Wilkerson and Aiken):
//! [`Winnowing`], the spec's `Fingerprinter`.
//!
//! 1. The text is normalized ([`crate::text::normalize`]): whitespace runs
//!    fold to one space, letters lowercase.
//! 2. Every window of `k` consecutive normalized characters (a k-gram, or
//!    shingle) is hashed ([`hash::rolling`]), so a fingerprint is a pure
//!    function of `k` normalized characters
//!    (`provenance.fingerprint.hashes-k-grams`).
//! 3. Of every `w` consecutive k-gram hashes, the minimum is selected, the
//!    rightmost one on a tie; each selected k-gram is reported once. The
//!    choice depends only on the window's contents, so a shared run of at
//!    least `k + w − 1` normalized characters (one whole window) selects the
//!    same fingerprint in both texts (`provenance.fingerprint.winnow-guarantee`).
//!    A text with fewer than `w` k-grams reports its minimum.
//!
//! Offsets are source byte offsets of the k-gram's first character; the
//! [`KGram`]s also carry the end, so callers can map a hit back to the
//! bytes it covers.

pub mod hash;

use std::collections::VecDeque;

use crosstalk_spec::derived::provenance::fingerprint::{
    Fingerprint, PositionedFingerprint, WinnowParams,
};
use crosstalk_spec::interfaces::l4_provenance::Fingerprinter;

use crate::text::{MappedText, NormChar, normalize};

/// One k-gram: its hash, the source bytes it covers and its position among
/// the text's k-grams.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KGram {
    pub fingerprint: Fingerprint,
    /// Source byte offset of the k-gram's first character.
    pub start: u32,
    /// Source byte offset just past its last character.
    pub end: u32,
    /// Index of its first character in the normalized text.
    pub position: usize,
}

/// The winnowing fingerprinter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Winnowing {
    params: WinnowParams,
}

impl Winnowing {
    pub fn new(params: WinnowParams) -> Self {
        Self { params }
    }

    pub fn k(&self) -> usize {
        usize::from(self.params.k.get())
    }

    pub fn w(&self) -> usize {
        usize::from(self.params.w.get())
    }

    /// The minimum normalized length a shared run needs to be guaranteed a
    /// shared fingerprint: `k + w − 1`.
    pub fn guarantee(&self) -> usize {
        self.k() + self.w() - 1
    }

    /// Every k-gram of `text`, in order.
    pub fn kgrams(&self, text: &str) -> Vec<KGram> {
        self.kgrams_of(&normalize(text))
    }

    /// Every k-gram of already normalized characters, in order.
    pub fn kgrams_of(&self, normalized: &[NormChar]) -> Vec<KGram> {
        let k = self.k();
        let chars: Vec<char> = normalized.iter().map(|c| c.ch).collect();
        hash::rolling(&chars, k)
            .into_iter()
            .enumerate()
            .map(|(position, value)| KGram {
                fingerprint: Fingerprint(value),
                start: normalized[position].start,
                end: normalized[position + k - 1].end,
                position,
            })
            .collect()
    }

    /// The k-grams winnowing selects from `kgrams`.
    pub fn select(&self, kgrams: &[KGram]) -> Vec<KGram> {
        let w = self.w();
        if kgrams.is_empty() {
            return Vec::new();
        }
        if kgrams.len() < w {
            return rightmost_minimum(kgrams).into_iter().collect();
        }
        let mut selected: Vec<KGram> = Vec::new();
        let mut last: Option<usize> = None;
        // Indices of a window's candidates, values strictly increasing from
        // the front: the front is the window's rightmost minimum.
        let mut window: VecDeque<usize> = VecDeque::with_capacity(w);
        for index in 0..kgrams.len() {
            while window
                .back()
                .is_some_and(|back| kgrams[*back].fingerprint >= kgrams[index].fingerprint)
            {
                window.pop_back();
            }
            window.push_back(index);
            if index + 1 < w {
                continue;
            }
            let first = index + 1 - w;
            while window.front().is_some_and(|front| *front < first) {
                window.pop_front();
            }
            if let Some(minimum) = window.front().copied()
                && last != Some(minimum)
            {
                selected.push(kgrams[minimum]);
                last = Some(minimum);
            }
        }
        selected
    }

    /// The selected k-grams of `text`.
    pub fn winnow(&self, text: &str) -> Vec<KGram> {
        self.select(&self.kgrams(text))
    }

    /// Every k-gram of a mapped text, its extent mapped into the text the
    /// map points at.
    pub fn kgrams_mapped(&self, text: &MappedText) -> Vec<KGram> {
        self.kgrams(text.text())
            .into_iter()
            .map(|kgram| map_kgram(text, kgram))
            .collect()
    }

    /// The selected k-grams of a mapped text, mapped like
    /// [`Winnowing::kgrams_mapped`].
    pub fn winnow_mapped(&self, text: &MappedText) -> Vec<KGram> {
        self.winnow(text.text())
            .into_iter()
            .map(|kgram| map_kgram(text, kgram))
            .collect()
    }
}

fn map_kgram(text: &MappedText, kgram: KGram) -> KGram {
    let start = usize::try_from(kgram.start).unwrap_or(usize::MAX);
    let end = usize::try_from(kgram.end).unwrap_or(usize::MAX);
    KGram {
        start: text.source(start),
        end: text.source(end),
        ..kgram
    }
}

/// `kgrams` as the spec's positioned fingerprints (offset = start).
pub fn positioned(kgrams: &[KGram]) -> Vec<PositionedFingerprint> {
    kgrams
        .iter()
        .map(|kgram| PositionedFingerprint {
            fingerprint: kgram.fingerprint,
            offset: kgram.start,
        })
        .collect()
}

fn rightmost_minimum(kgrams: &[KGram]) -> Option<KGram> {
    let mut best: Option<KGram> = None;
    for kgram in kgrams {
        if best.is_none_or(|b| kgram.fingerprint <= b.fingerprint) {
            best = Some(*kgram);
        }
    }
    best
}

impl Fingerprinter for Winnowing {
    fn params(&self) -> WinnowParams {
        self.params
    }

    fn fingerprints(&self, text: &str) -> Vec<PositionedFingerprint> {
        self.winnow(text)
            .into_iter()
            .map(|kgram| PositionedFingerprint {
                fingerprint: kgram.fingerprint,
                offset: kgram.start,
            })
            .collect()
    }
}

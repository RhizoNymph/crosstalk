//! Unicode normalization (`Codec::UnicodeNormalization`): NFKC, confusable
//! folding and zero-width character removal, over the whole text.
//!
//! NFKC is applied per normalization segment (a starter and the combining
//! marks after it), so every output character maps to the source segment it
//! came from; compatibility characters (fullwidth forms, ligatures,
//! mathematical alphanumerics, superscripts) fold to their plain forms.
//! Then the common Cyrillic and Greek homoglyphs of Latin letters fold to
//! the Latin letter, and zero-width characters (which split k-grams without
//! showing) are removed. The text is yielded only when it changed.

use crosstalk_spec::derived::provenance::matching::Codec;
use crosstalk_spec::support::ByteRange;
use unicode_normalization::UnicodeNormalization;
use unicode_normalization::char::canonical_combining_class;

use super::{DecodedText, Step, TextDecoder};
use crate::text::MappedBuilder;

/// The Unicode normalizer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct UnicodeNormalizer;

/// Characters that take no width: removed.
pub fn is_zero_width(ch: char) -> bool {
    matches!(
        ch,
        '\u{00AD}'
            | '\u{034F}'
            | '\u{061C}'
            | '\u{115F}'
            | '\u{1160}'
            | '\u{17B4}'
            | '\u{17B5}'
            | '\u{180E}'
            | '\u{200B}'..='\u{200F}'
            | '\u{202A}'..='\u{202E}'
            | '\u{2060}'..='\u{2064}'
            | '\u{2066}'..='\u{206F}'
            | '\u{FEFF}'
    )
}

/// The Latin letter a homoglyph stands for, or the character itself.
pub fn fold_confusable(ch: char) -> char {
    match ch {
        // Cyrillic lowercase.
        'а' => 'a',
        'е' => 'e',
        'о' => 'o',
        'р' => 'p',
        'с' => 'c',
        'у' => 'y',
        'х' => 'x',
        'ѕ' => 's',
        'і' => 'i',
        'ј' => 'j',
        'ԁ' => 'd',
        'һ' => 'h',
        'ӏ' => 'l',
        'ԛ' => 'q',
        'ԝ' => 'w',
        'ɡ' => 'g',
        // Cyrillic uppercase.
        'А' => 'A',
        'В' => 'B',
        'Е' => 'E',
        'К' => 'K',
        'М' => 'M',
        'Н' => 'H',
        'О' => 'O',
        'Р' => 'P',
        'С' => 'C',
        'Т' => 'T',
        'Х' => 'X',
        'У' => 'Y',
        'І' => 'I',
        'Ј' => 'J',
        'Ѕ' => 'S',
        // Greek.
        'Α' => 'A',
        'Β' => 'B',
        'Ε' => 'E',
        'Ζ' => 'Z',
        'Η' => 'H',
        'Ι' => 'I',
        'Κ' => 'K',
        'Μ' => 'M',
        'Ν' => 'N',
        'Ο' => 'O',
        'Ρ' => 'P',
        'Τ' => 'T',
        'Υ' => 'Y',
        'Χ' => 'X',
        'ο' => 'o',
        'ν' => 'v',
        'ρ' => 'p',
        'ι' => 'i',
        'α' => 'a',
        'κ' => 'k',
        other => other,
    }
}

impl TextDecoder for UnicodeNormalizer {
    fn step(&self) -> Step {
        Step::Codec(Codec::UnicodeNormalization)
    }

    fn decode_mapped(&self, text: &str) -> Vec<DecodedText> {
        let Ok(end) = u32::try_from(text.len()) else {
            return Vec::new();
        };
        let mut builder = MappedBuilder::new();
        let mut starts: Vec<usize> = text
            .char_indices()
            .filter(|(index, ch)| *index == 0 || canonical_combining_class(*ch) == 0)
            .map(|(index, _)| index)
            .collect();
        starts.push(text.len());
        for pair in starts.windows(2) {
            let (start, segment_end) = (pair[0], pair[1]);
            let source = u32::try_from(start).unwrap_or(end);
            for ch in text[start..segment_end].nfkc() {
                if !is_zero_width(ch) {
                    builder.push(fold_confusable(ch), source);
                }
            }
        }
        if builder.text() == text {
            return Vec::new();
        }
        let Ok(source) = ByteRange::new(0, end) else {
            return Vec::new();
        };
        vec![DecodedText {
            source,
            text: builder.finish(end),
        }]
    }
}

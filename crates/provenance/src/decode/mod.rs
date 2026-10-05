//! The decoders and the depth-bounded decode pipeline.
//!
//! A [`Step`] is one transformation a reader's text may have been through:
//! a spec [`Codec`] (base64, hex, URL encoding, Unicode normalization) or a
//! string unescape (JSON / YAML string escapes), which the spec does not
//! name yet. Each decoder ([`TextDecoder`]) finds every substring of a text
//! it can decode and yields it with a byte map back into its input
//! ([`MappedText`]); the four codec decoders also implement the spec's
//! [`Decoder`]. A decoder yields text only when the decoded bytes are valid
//! UTF-8 (no lossy conversion) and differ from the source.
//!
//! [`DecodePipeline::layers`] applies every decoder to the raw text and
//! then to its own output, breadth first, at most `max_depth` steps deep
//! (`provenance.decode.depth-bounded`) and at most `max_layers` texts in
//! all; a text already produced by a shorter chain is not kept twice. Each
//! [`Layer`] keeps its chain in the order the steps were undone
//! (`provenance.decode.codecs-in-decode-order`) and its map composed back
//! to the part text.
//!
//! The string unescapes are separate decoders, never part of
//! normalization: whitespace and case folding stay in the fingerprinter.
//! [`Step::codec`] maps them to `Codec::JsonString` and
//! `Codec::YamlString`, and [`crate::scan::kind`] reports them in
//! `MatchKind::Decoded`.

mod base64;
mod escape;
mod hex;
mod unicode;
mod url;

use std::collections::HashSet;

use crosstalk_spec::derived::provenance::matching::Codec;
use crosstalk_spec::interfaces::l4_provenance::{Decoded, Decoder};
use crosstalk_spec::support::ByteRange;

pub use self::base64::Base64Decoder;
pub use self::escape::{JsonStringDecoder, YamlStringDecoder};
pub use self::hex::HexDecoder;
pub use self::unicode::{UnicodeNormalizer, fold_confusable, is_zero_width};
pub use self::url::UrlDecoder;
use crate::config::DecodeLimits;
use crate::text::MappedText;

/// One transformation undone on a reader's text.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Step {
    /// A codec the spec names.
    Codec(Codec),
    /// JSON (and Python or JavaScript) string escapes: `\n`, `\"`, `\uXXXX`.
    JsonString,
    /// YAML string escapes: `''` in single-quoted scalars, escaped line
    /// breaks in double-quoted ones.
    YamlString,
}

impl Step {
    /// The spec codec this step is reported as in `MatchKind::Decoded`;
    /// the string unescapes are `Codec::JsonString` and
    /// `Codec::YamlString`.
    pub fn codec(self) -> Option<Codec> {
        match self {
            Self::Codec(codec) => Some(codec),
            Self::JsonString => Some(Codec::JsonString),
            Self::YamlString => Some(Codec::YamlString),
        }
    }
}

/// One decodable substring, decoded, with the map from its bytes into the
/// decoder's input text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedText {
    /// Where the encoded text sat in the input.
    pub source: ByteRange,
    /// The decoded text; its map indexes the input text.
    pub text: MappedText,
}

/// A decoder over text, with source maps.
pub trait TextDecoder {
    fn step(&self) -> Step;

    /// Every substring of `text` this decoder can decode, decoded, each
    /// differing from its source and valid UTF-8.
    fn decode_mapped(&self, text: &str) -> Vec<DecodedText>;
}

/// The spec's view of a codec decoder.
fn spec_decode<D: TextDecoder>(decoder: &D, codec: Codec, text: &str) -> Vec<Decoded> {
    decoder
        .decode_mapped(text)
        .into_iter()
        .map(|decoded| Decoded {
            codec,
            source: decoded.source,
            text: decoded.text.into_text(),
        })
        .collect()
}

macro_rules! spec_decoder {
    ($($ty:ty => $codec:expr),* $(,)?) => {$(
        impl Decoder for $ty {
            fn codec(&self) -> Codec {
                $codec
            }

            fn decode(&self, text: &str) -> Vec<Decoded> {
                spec_decode(self, $codec, text)
            }
        }
    )*};
}

spec_decoder! {
    Base64Decoder => Codec::Base64,
    HexDecoder => Codec::Hex,
    UrlDecoder => Codec::UrlEncoding,
    UnicodeNormalizer => Codec::UnicodeNormalization,
}

/// Every decoder the pipeline runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AnyDecoder {
    Base64(Base64Decoder),
    Hex(HexDecoder),
    Url(UrlDecoder),
    Unicode(UnicodeNormalizer),
    JsonString(JsonStringDecoder),
    YamlString(YamlStringDecoder),
}

impl TextDecoder for AnyDecoder {
    fn step(&self) -> Step {
        match self {
            Self::Base64(d) => d.step(),
            Self::Hex(d) => d.step(),
            Self::Url(d) => d.step(),
            Self::Unicode(d) => d.step(),
            Self::JsonString(d) => d.step(),
            Self::YamlString(d) => d.step(),
        }
    }

    fn decode_mapped(&self, text: &str) -> Vec<DecodedText> {
        match self {
            Self::Base64(d) => d.decode_mapped(text),
            Self::Hex(d) => d.decode_mapped(text),
            Self::Url(d) => d.decode_mapped(text),
            Self::Unicode(d) => d.decode_mapped(text),
            Self::JsonString(d) => d.decode_mapped(text),
            Self::YamlString(d) => d.decode_mapped(text),
        }
    }
}

/// One text an input expands to: the raw text (empty chain) or a decoding
/// of it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Layer {
    /// The steps undone, in the order they were undone.
    pub chain: Vec<Step>,
    /// The text, mapped into the part text.
    pub text: MappedText,
}

impl Layer {
    pub fn is_raw(&self) -> bool {
        self.chain.is_empty()
    }
}

/// The decoders, applied breadth first up to the configured depth.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodePipeline {
    decoders: Vec<AnyDecoder>,
    limits: DecodeLimits,
}

impl DecodePipeline {
    /// Every decoder: string unescapes first (the commonest in tool
    /// output), then the codecs.
    pub fn new(limits: DecodeLimits) -> Self {
        let run = limits.min_encoded_run();
        Self {
            decoders: vec![
                AnyDecoder::JsonString(JsonStringDecoder),
                AnyDecoder::YamlString(YamlStringDecoder),
                AnyDecoder::Base64(Base64Decoder::new(run)),
                AnyDecoder::Hex(HexDecoder::new(run)),
                AnyDecoder::Url(UrlDecoder),
                AnyDecoder::Unicode(UnicodeNormalizer),
            ],
            limits,
        }
    }

    /// A pipeline over chosen decoders.
    pub fn with_decoders(decoders: Vec<AnyDecoder>, limits: DecodeLimits) -> Self {
        Self { decoders, limits }
    }

    pub fn limits(&self) -> DecodeLimits {
        self.limits
    }

    /// `text` and every decoding of it: the raw layer first, then by depth.
    pub fn layers(&self, text: &str) -> Vec<Layer> {
        let mut layers = vec![Layer {
            chain: Vec::new(),
            text: MappedText::identity(text),
        }];
        let mut seen: HashSet<String> = HashSet::from([text.to_owned()]);
        let mut frontier: Vec<usize> = vec![0];
        let max_layers = self.limits.max_layers();
        for _ in 0..self.limits.max_depth() {
            let mut next = Vec::new();
            for parent in frontier {
                for decoder in &self.decoders {
                    if layers.len() >= max_layers {
                        return layers;
                    }
                    let Some(source) = layers.get(parent) else {
                        continue;
                    };
                    let decoded = decoder.decode_mapped(source.text.text());
                    for item in decoded {
                        if layers.len() >= max_layers {
                            return layers;
                        }
                        if !seen.insert(item.text.text().to_owned()) {
                            continue;
                        }
                        let Some(source) = layers.get(parent) else {
                            continue;
                        };
                        let mut chain = source.chain.clone();
                        chain.push(decoder.step());
                        let text = source.text.compose(item.text);
                        next.push(layers.len());
                        layers.push(Layer { chain, text });
                    }
                }
            }
            if next.is_empty() {
                break;
            }
            frontier = next;
        }
        layers
    }
}

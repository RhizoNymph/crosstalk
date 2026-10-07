//! Prediction dimensions, one to one: the bench kept the spec's names and
//! meanings, so every arm is a rename.

use a2a_bench_format as bench;
use bench::labels::{CarrierKind, Codec, DelegationDirection, MatchClass};
use crosstalk_spec::aggregates::quality::MatchClass as SpecClass;
use crosstalk_spec::derived::flow::transmission::DelegationDirection as SpecDirection;
use crosstalk_spec::derived::provenance::matching::{
    CarrierKind as SpecCarrier, Codec as SpecCodec, MatchKind as SpecKind,
};

pub fn carrier(carrier: SpecCarrier) -> CarrierKind {
    match carrier {
        SpecCarrier::ToolResult => CarrierKind::ToolResult,
        SpecCarrier::UserTurn => CarrierKind::UserTurn,
        SpecCarrier::SystemPrompt => CarrierKind::SystemPrompt,
        SpecCarrier::ReaderOutput => CarrierKind::ReaderOutput,
    }
}

pub fn codec(codec: SpecCodec) -> Codec {
    match codec {
        SpecCodec::Base64 => Codec::Base64,
        SpecCodec::Hex => Codec::Hex,
        SpecCodec::UrlEncoding => Codec::UrlEncoding,
        SpecCodec::UnicodeNormalization => Codec::UnicodeNormalization,
        SpecCodec::JsonString => Codec::JsonString,
        SpecCodec::YamlString => Codec::YamlString,
    }
}

pub fn class(class: SpecClass) -> MatchClass {
    match class {
        SpecClass::Exact => MatchClass::Exact,
        SpecClass::Normalized => MatchClass::Normalized,
        SpecClass::Decoded => MatchClass::Decoded,
        SpecClass::Semantic => MatchClass::Semantic,
    }
}

/// A content match's kind. A semantic match's similarity has no bench
/// field and is counted in `lossy`.
pub fn match_kind(kind: &SpecKind, lossy: &mut super::Lossy) -> bench::predictions::MatchKind {
    use bench::predictions::MatchKind;
    match kind {
        SpecKind::Exact => MatchKind::Exact,
        SpecKind::Normalized => MatchKind::Normalized,
        SpecKind::Decoded(codecs) => MatchKind::Decoded {
            codecs: codecs.iter().copied().map(codec).collect(),
        },
        SpecKind::Semantic(_) => {
            lossy.similarities += 1;
            MatchKind::Semantic
        }
    }
}

pub fn direction(direction: SpecDirection) -> DelegationDirection {
    match direction {
        SpecDirection::ParentToChild => DelegationDirection::ParentToChild,
        SpecDirection::ChildToParent => DelegationDirection::ChildToParent,
    }
}

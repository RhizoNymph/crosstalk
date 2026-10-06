//! Label and prediction dimensions, one to one: the bench kept ct-eval's
//! names and meanings, so every arm is a rename.

use a2a_bench_format as bench;
use bench::labels::{
    CarrierKind, Codec, DelegationDirection, ExemptionReason, MatchClass, MatchNeed,
    NegativeReason, Tier,
};
use crosstalk_spec::aggregates::quality::MatchClass as SpecClass;
use crosstalk_spec::derived::flow::transmission::DelegationDirection as SpecDirection;
use crosstalk_spec::derived::provenance::matching::{
    CarrierKind as SpecCarrier, Codec as SpecCodec, MatchKind as SpecKind,
};

use crate::corpus::Coverage as EvalCoverage;
use crate::truth::{
    ExemptionReason as EvalExemption, MatchNeed as EvalNeed, NegativeReason as EvalReason,
    Tier as EvalTier,
};

pub fn tier(tier: EvalTier) -> Tier {
    match tier {
        EvalTier::Construction => Tier::Construction,
        EvalTier::Structural => Tier::Structural,
        EvalTier::Heuristic => Tier::Heuristic,
        EvalTier::Judged => Tier::Judged,
        EvalTier::OutOfReach => Tier::OutOfReach,
        EvalTier::Forwarding => Tier::Forwarding,
    }
}

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

pub fn need(need: &EvalNeed) -> MatchNeed {
    match need {
        EvalNeed::Exact => MatchNeed::Exact,
        EvalNeed::Normalized => MatchNeed::Normalized,
        EvalNeed::Decoded { codecs } => MatchNeed::Decoded {
            codecs: codecs.iter().copied().map(codec).collect(),
        },
        EvalNeed::Semantic => MatchNeed::Semantic,
        EvalNeed::Undecodable { codec } => MatchNeed::Undecodable {
            codec: codec.clone(),
        },
        EvalNeed::Unobserved { reason, arrival } => MatchNeed::Unobserved {
            reason: reason.clone(),
            arrival: class(*arrival),
        },
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

pub fn negative(reason: EvalReason) -> NegativeReason {
    match reason {
        EvalReason::RejectedSend => NegativeReason::RejectedSend,
        EvalReason::SharedSource => NegativeReason::SharedSource,
        EvalReason::Boilerplate => NegativeReason::Boilerplate,
        EvalReason::NoSenderExchange => NegativeReason::NoSenderExchange,
        EvalReason::SelfRead => NegativeReason::SelfRead,
        EvalReason::Reread => NegativeReason::Reread,
        EvalReason::Miss => NegativeReason::Miss,
    }
}

pub fn exemption(reason: EvalExemption) -> ExemptionReason {
    match reason {
        EvalExemption::UnknownSender => ExemptionReason::UnknownSender,
    }
}

pub fn coverage(coverage: EvalCoverage) -> bench::files::Coverage {
    match coverage {
        EvalCoverage::Complete { tier: at } => bench::files::Coverage::Complete { tier: tier(at) },
        EvalCoverage::Partial => bench::files::Coverage::Partial,
    }
}

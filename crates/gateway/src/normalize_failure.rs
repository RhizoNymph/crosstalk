//! Why the capture stage refused an exchange, as fixed codes: the `reason`
//! and `protocol` labels of the `normalize_failed` outcome on `/metrics`.
//!
//! A refusal is a [`NormalizeFailure`]: its [`FailureReason`] (derived from
//! the typed [`NormalizeError`], or `unsupported_protocol` when no
//! normalizer handles the exchange's protocol) and the exchange's
//! [`WireProtocol`]. Both sets are closed and matched exhaustively, so the
//! labels never carry free text and a new spec variant does not compile
//! until it has a code. [`FailureStats`] counts every pair;
//! the health report's `normalize_failed` is their sum.

use std::sync::atomic::{AtomicU64, Ordering};

use crosstalk_spec::interfaces::l1_canonical::NormalizeError;
use crosstalk_spec::observed::exchange::WireProtocol;

/// Every wire protocol, in label order.
pub const PROTOCOLS: [WireProtocol; 5] = [
    WireProtocol::AnthropicMessages,
    WireProtocol::OpenAiChat,
    WireProtocol::OpenAiResponses,
    WireProtocol::GeminiGenerate,
    WireProtocol::GeminiCodeAssist,
];

/// The `protocol` label: the protocol's wire name.
pub fn protocol_code(protocol: WireProtocol) -> &'static str {
    match protocol {
        WireProtocol::AnthropicMessages => "anthropic_messages",
        WireProtocol::OpenAiChat => "open_ai_chat",
        WireProtocol::OpenAiResponses => "open_ai_responses",
        WireProtocol::GeminiGenerate => "gemini_generate",
        WireProtocol::GeminiCodeAssist => "gemini_code_assist",
    }
}

fn protocol_index(protocol: WireProtocol) -> usize {
    match protocol {
        WireProtocol::AnthropicMessages => 0,
        WireProtocol::OpenAiChat => 1,
        WireProtocol::OpenAiResponses => 2,
        WireProtocol::GeminiGenerate => 3,
        WireProtocol::GeminiCodeAssist => 4,
    }
}

/// Why an exchange was not normalized.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailureReason {
    /// No normalizer in this gateway handles the exchange's protocol.
    UnsupportedProtocol,
    /// The normalizer refused the request body
    /// ([`NormalizeError::RequestBody`]).
    RequestBody,
}

impl FailureReason {
    /// Every reason, in label order.
    pub const ALL: [Self; 2] = [Self::UnsupportedProtocol, Self::RequestBody];

    /// The reason a normalizer's error stands for.
    pub fn of(error: &NormalizeError) -> Self {
        match error {
            NormalizeError::RequestBody { .. } => Self::RequestBody,
        }
    }

    /// The `reason` label.
    pub fn code(self) -> &'static str {
        match self {
            Self::UnsupportedProtocol => "unsupported_protocol",
            Self::RequestBody => "request_body",
        }
    }

    fn index(self) -> usize {
        match self {
            Self::UnsupportedProtocol => 0,
            Self::RequestBody => 1,
        }
    }
}

/// One refusal: why, and the exchange's protocol.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NormalizeFailure {
    pub reason: FailureReason,
    pub protocol: WireProtocol,
}

/// Refusals counted by reason and protocol.
#[derive(Debug, Default)]
pub struct FailureStats {
    counts: [[AtomicU64; PROTOCOLS.len()]; FailureReason::ALL.len()],
}

impl FailureStats {
    pub fn bump(&self, failure: NormalizeFailure) {
        self.counts[failure.reason.index()][protocol_index(failure.protocol)]
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> FailureCounts {
        FailureCounts {
            counts: self
                .counts
                .each_ref()
                .map(|row| row.each_ref().map(|count| count.load(Ordering::Relaxed))),
        }
    }
}

/// A reading of [`FailureStats`]: a count for every reason and protocol.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct FailureCounts {
    counts: [[u64; PROTOCOLS.len()]; FailureReason::ALL.len()],
}

impl FailureCounts {
    /// These counts with `failure`'s set to `count`.
    #[must_use]
    pub fn with(mut self, failure: NormalizeFailure, count: u64) -> Self {
        self.counts[failure.reason.index()][protocol_index(failure.protocol)] = count;
        self
    }

    pub fn get(&self, failure: NormalizeFailure) -> u64 {
        self.counts[failure.reason.index()][protocol_index(failure.protocol)]
    }

    /// Every refusal counted, whatever its reason or protocol.
    pub fn total(&self) -> u64 {
        self.counts
            .iter()
            .flatten()
            .fold(0, |sum, count| sum.saturating_add(*count))
    }

    /// Every reason and protocol pair with its count, reasons in
    /// [`FailureReason::ALL`] order, then protocols in [`PROTOCOLS`] order.
    pub fn iter(&self) -> impl Iterator<Item = (NormalizeFailure, u64)> + '_ {
        FailureReason::ALL.into_iter().flat_map(move |reason| {
            PROTOCOLS.into_iter().map(move |protocol| {
                let failure = NormalizeFailure { reason, protocol };
                (failure, self.get(failure))
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    #[test]
    fn codes_are_fixed_unique_and_match_the_wire_names() {
        let reasons: BTreeSet<&str> = FailureReason::ALL
            .into_iter()
            .map(FailureReason::code)
            .collect();
        assert_eq!(
            reasons,
            BTreeSet::from(["request_body", "unsupported_protocol"])
        );
        for protocol in PROTOCOLS {
            let wire = serde_json::to_value(protocol).expect("encodes");
            assert_eq!(wire.as_str(), Some(protocol_code(protocol)));
        }
        let indices: BTreeSet<usize> = PROTOCOLS.into_iter().map(protocol_index).collect();
        assert_eq!(indices.len(), PROTOCOLS.len());
    }

    #[test]
    fn a_request_body_error_is_request_body() {
        let error = NormalizeError::RequestBody {
            reason: "message 1 has role \"developer\"".to_owned(),
        };
        assert_eq!(FailureReason::of(&error), FailureReason::RequestBody);
    }

    #[test]
    fn counts_are_kept_per_pair_and_sum_to_the_total() {
        let stats = FailureStats::default();
        let body = NormalizeFailure {
            reason: FailureReason::RequestBody,
            protocol: WireProtocol::AnthropicMessages,
        };
        let chat = NormalizeFailure {
            reason: FailureReason::UnsupportedProtocol,
            protocol: WireProtocol::OpenAiChat,
        };
        stats.bump(body);
        stats.bump(body);
        stats.bump(chat);
        let counts = stats.snapshot();
        assert_eq!(counts.get(body), 2);
        assert_eq!(counts.get(chat), 1);
        assert_eq!(counts.total(), 3);
        assert_eq!(counts, FailureCounts::default().with(body, 2).with(chat, 1));
        assert_eq!(counts.iter().count(), 10, "every pair, zeros included");
        assert_eq!(counts.iter().map(|(_, count)| count).sum::<u64>(), 3);
    }
}

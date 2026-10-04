//! The outcome message shown after an action's redirect. The query carries
//! a code, never text, so a link cannot make the page say something else.

use topcoat::context::Cx;
use topcoat::router::query_params;

/// The query key holding the flash code.
pub const KEY: &str = "flash";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Flash {
    PolicySet,
    ChannelPromoted,
    AgentRenamed,
    LabelCleared,
    Unmerged,
    Merged,
    Acknowledged,
    Resolved,
    RuleCreated,
    RuleUpdated,
    RuleEnabled,
    RuleDisabled,
    Replayed,
    VerdictRecorded,
    VerdictWithdrawn,
    ProjectionFitted,
}

impl Flash {
    pub const ALL: [Self; 16] = [
        Self::PolicySet,
        Self::ChannelPromoted,
        Self::AgentRenamed,
        Self::LabelCleared,
        Self::Unmerged,
        Self::Merged,
        Self::Acknowledged,
        Self::Resolved,
        Self::RuleCreated,
        Self::RuleUpdated,
        Self::RuleEnabled,
        Self::RuleDisabled,
        Self::Replayed,
        Self::VerdictRecorded,
        Self::VerdictWithdrawn,
        Self::ProjectionFitted,
    ];

    pub fn code(self) -> &'static str {
        match self {
            Self::PolicySet => "policy-set",
            Self::ChannelPromoted => "promoted",
            Self::AgentRenamed => "renamed",
            Self::LabelCleared => "label-cleared",
            Self::Unmerged => "unmerged",
            Self::Merged => "merged",
            Self::Acknowledged => "acknowledged",
            Self::Resolved => "resolved",
            Self::RuleCreated => "rule-created",
            Self::RuleUpdated => "rule-updated",
            Self::RuleEnabled => "rule-enabled",
            Self::RuleDisabled => "rule-disabled",
            Self::Replayed => "replayed",
            Self::VerdictRecorded => "verdict-recorded",
            Self::VerdictWithdrawn => "verdict-withdrawn",
            Self::ProjectionFitted => "projection-fitted",
        }
    }

    /// Unknown codes read as no flash: a stale link should not fail.
    pub fn parse(code: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|f| f.code() == code)
    }

    pub fn message(self) -> &'static str {
        match self {
            Self::PolicySet => "Policy updated.",
            Self::ChannelPromoted => {
                "Channel promoted. This declared channel now covers the discovered channels its pattern matches."
            }
            Self::AgentRenamed => "Agent renamed.",
            Self::LabelCleared => "Label cleared.",
            Self::Unmerged => "Merge reverted. The resolver will not re-merge this pair.",
            Self::Merged => "Agents merged.",
            Self::Acknowledged => "Alert acknowledged.",
            Self::Resolved => "Alert resolved.",
            Self::RuleCreated => "Rule created.",
            Self::RuleUpdated => "Rule updated.",
            Self::RuleEnabled => "Rule enabled.",
            Self::RuleDisabled => "Rule disabled.",
            Self::Replayed => "Dead letter replayed to its consumer group.",
            Self::VerdictRecorded => "Verdict recorded.",
            Self::VerdictWithdrawn => "Verdict withdrawn.",
            Self::ProjectionFitted => "Projection fitted.",
        }
    }
}

#[query_params]
struct FlashQuery {
    flash: Option<String>,
}

/// The flash of this request, if its query names a known one.
pub fn flash(cx: &Cx) -> Option<Flash> {
    // A query that fails to parse has no flash; the page's own query
    // parsing reports the problem.
    query_params::<FlashQuery>(cx)
        .ok()
        .and_then(|q| q.flash.as_deref())
        .and_then(Flash::parse)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codes_round_trip_and_are_unique() {
        for flash in Flash::ALL {
            assert_eq!(Flash::parse(flash.code()), Some(flash));
        }
        let mut codes: Vec<_> = Flash::ALL.iter().map(|f| f.code()).collect();
        codes.sort_unstable();
        codes.dedup();
        assert_eq!(codes.len(), Flash::ALL.len());
    }

    #[test]
    fn unknown_codes_are_ignored() {
        assert_eq!(Flash::parse("you-have-been-hacked"), None);
    }
}

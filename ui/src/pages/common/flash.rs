//! The outcome message shown after an action's redirect. The query carries
//! a code, never text, so a link cannot make the page say something else;
//! the only number a code carries is how many channels a promotion
//! superseded.

use topcoat::context::Cx;
use topcoat::router::query_params;

/// The query key holding the flash code.
pub const KEY: &str = "flash";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Flash {
    PolicySet,
    /// The promoted channel, and how many discovered channels the promotion
    /// superseded.
    ChannelPromoted {
        superseded: u16,
    },
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
    VersionPinned,
    VersionUnpinned,
    /// The action was accepted, but the state already matched it.
    Unchanged,
}

/// The code of a promotion that superseded nothing; one that superseded
/// `n` channels is `promoted-n`.
const PROMOTED: &str = "promoted";

impl Flash {
    /// Every flash with a fixed code: all but promotions that superseded
    /// channels.
    pub const FIXED: [Self; 19] = [
        Self::PolicySet,
        Self::ChannelPromoted { superseded: 0 },
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
        Self::VersionPinned,
        Self::VersionUnpinned,
        Self::Unchanged,
    ];

    /// A promotion's flash for `superseded` channels, saturating at
    /// `u16::MAX`.
    pub fn promoted(superseded: usize) -> Self {
        Self::ChannelPromoted {
            superseded: u16::try_from(superseded).unwrap_or(u16::MAX),
        }
    }

    pub fn code(self) -> String {
        match self {
            Self::ChannelPromoted { superseded: 0 } => PROMOTED.to_owned(),
            Self::ChannelPromoted { superseded } => format!("{PROMOTED}-{superseded}"),
            fixed => fixed.fixed_code().to_owned(),
        }
    }

    fn fixed_code(self) -> &'static str {
        match self {
            Self::PolicySet => "policy-set",
            Self::ChannelPromoted { .. } => PROMOTED,
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
            Self::VersionPinned => "version-pinned",
            Self::VersionUnpinned => "version-unpinned",
            Self::Unchanged => "unchanged",
        }
    }

    /// Unknown codes read as no flash: a stale link should not fail.
    pub fn parse(code: &str) -> Option<Self> {
        if let Some(count) = code
            .strip_prefix(PROMOTED)
            .and_then(|rest| rest.strip_prefix('-'))
        {
            // Digits only, no leading zero: one code per count.
            if count.starts_with('0') || !count.bytes().all(|b| b.is_ascii_digit()) {
                return None;
            }
            return count
                .parse::<u16>()
                .ok()
                .map(|superseded| Self::ChannelPromoted { superseded });
        }
        Self::FIXED.into_iter().find(|f| f.fixed_code() == code)
    }

    pub fn message(self) -> String {
        match self {
            Self::ChannelPromoted { superseded: 0 } => {
                "Channel promoted. It is now a declared channel under the same id.".to_owned()
            }
            Self::ChannelPromoted { superseded: 1 } => {
                "Channel promoted. It superseded 1 discovered channel its pattern matches, which now resolves to it."
                    .to_owned()
            }
            Self::ChannelPromoted { superseded } => format!(
                "Channel promoted. It superseded {superseded} discovered channels its pattern matches, which now resolve to it."
            ),
            Self::PolicySet => "Policy updated.".to_owned(),
            Self::AgentRenamed => "Agent renamed.".to_owned(),
            Self::LabelCleared => "Label cleared.".to_owned(),
            Self::Unmerged => "Merge reverted. The resolver will not re-merge this pair.".to_owned(),
            Self::Merged => "Agents merged.".to_owned(),
            Self::Acknowledged => "Alert acknowledged.".to_owned(),
            Self::Resolved => "Alert resolved.".to_owned(),
            Self::RuleCreated => "Rule created.".to_owned(),
            Self::RuleUpdated => "Rule updated.".to_owned(),
            Self::RuleEnabled => "Rule enabled.".to_owned(),
            Self::RuleDisabled => "Rule disabled.".to_owned(),
            Self::Replayed => "Dead letter replayed to its consumer group.".to_owned(),
            Self::VerdictRecorded => "Verdict recorded.".to_owned(),
            Self::VerdictWithdrawn => "Verdict withdrawn.".to_owned(),
            Self::ProjectionFitted => "Projection fitted.".to_owned(),
            Self::VersionPinned => {
                "Version pinned. Retention keeps its data until it is unpinned.".to_owned()
            }
            Self::VersionUnpinned => {
                "Version unpinned. Retention may now drop its data.".to_owned()
            }
            Self::Unchanged => "Nothing to change: it already was that way.".to_owned(),
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

    fn samples() -> Vec<Flash> {
        let mut all = Flash::FIXED.to_vec();
        all.extend([1, 2, 17, u16::MAX].map(|superseded| Flash::ChannelPromoted { superseded }));
        all
    }

    #[test]
    fn codes_round_trip_and_are_unique() {
        let all = samples();
        for flash in &all {
            assert_eq!(Flash::parse(&flash.code()), Some(*flash));
        }
        let mut codes: Vec<_> = all.iter().map(|f| f.code()).collect();
        codes.sort_unstable();
        codes.dedup();
        assert_eq!(codes.len(), all.len());
    }

    #[test]
    fn unknown_codes_are_ignored() {
        assert_eq!(Flash::parse("you-have-been-hacked"), None);
        for code in [
            "promoted-",
            "promoted-0",
            "promoted-01",
            "promoted-x",
            "promoted-70000",
        ] {
            assert_eq!(Flash::parse(code), None, "{code}");
        }
    }

    #[test]
    fn promotions_count_what_they_superseded() {
        assert_eq!(Flash::promoted(0).code(), "promoted");
        assert_eq!(Flash::promoted(3).code(), "promoted-3");
        assert_eq!(
            Flash::promoted(100_000),
            Flash::ChannelPromoted {
                superseded: u16::MAX
            }
        );
        assert!(
            Flash::promoted(1)
                .message()
                .contains("superseded 1 discovered channel ")
        );
        assert!(
            Flash::promoted(2)
                .message()
                .contains("superseded 2 discovered channels")
        );
        assert!(!Flash::promoted(0).message().contains("superseded"));
    }
}

//! The instants the world's scenarios happen at, each an offset back from
//! the anchor (the UI fixture's constants, made relative).

use crosstalk_spec::support::Timestamp;

use crate::clock::{Anchor, DAY, HOUR, MINUTE, plus};

/// Every named instant of the world.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Times {
    pub now: Timestamp,
    pub start: Timestamp,
    /// The first config load.
    pub config_at: Timestamp,
    /// The config load that added the design-docs channel.
    pub design_docs_at: Timestamp,
    /// The researcher promoted the team-notes channel.
    pub promote_at: Timestamp,
    pub pastebin_decided_at: Timestamp,
    pub shared_file_decided_at: Timestamp,
    /// The MCP memory server was sanctioned, and later reset.
    pub mcp_sanctioned_at: Timestamp,
    pub mcp_reset_at: Timestamp,
    /// The gist channel went quiet.
    pub gist_until: Timestamp,
    pub team_notes_from: Timestamp,
    pub hijack_from: Timestamp,
    /// `al1` and `cx1` passed notes through a local file from here until
    /// just before an operator merged them.
    pub self_notes_from: Timestamp,
    pub self_notes_until: Timestamp,
    /// v1 and v2 were activated; each fit started half an hour before and
    /// returned ten minutes before.
    pub v1_at: Timestamp,
    pub v2_at: Timestamp,
    /// The researcher pinned v1.
    pub v1_pinned_at: Timestamp,
    /// User rules were created at these times.
    pub watch_rule_at: Timestamp,
    pub stale_rule_at: Timestamp,
    pub semantic_rule_at: Timestamp,
    pub off_rule_at: Timestamp,
    pub off_rule_disabled_at: Timestamp,
    /// `cc7` uses its key-value scratch entry from here on.
    pub lone_from: Timestamp,
}

impl Times {
    pub fn of(anchor: Anchor) -> Self {
        let v1_at = anchor.ago(6 * DAY);
        Self {
            now: anchor.now(),
            start: anchor.start(),
            config_at: anchor.config_at(),
            design_docs_at: anchor.ago(20 * HOUR),
            promote_at: anchor.ago(3 * DAY),
            pastebin_decided_at: anchor.ago(5 * DAY),
            shared_file_decided_at: anchor.ago(4 * DAY),
            mcp_sanctioned_at: anchor.ago(6 * DAY),
            mcp_reset_at: anchor.ago(2 * DAY + 12 * HOUR),
            gist_until: anchor.ago(4 * DAY),
            team_notes_from: anchor.ago(5 * DAY + 6 * HOUR),
            hijack_from: anchor.ago(5 * DAY + 14 * HOUR),
            self_notes_from: anchor.ago(6 * DAY),
            self_notes_until: anchor.ago(3 * DAY + HOUR),
            v1_at,
            v2_at: anchor.ago(2 * DAY),
            v1_pinned_at: plus(v1_at, DAY),
            watch_rule_at: anchor.ago(2 * DAY - 2 * HOUR),
            stale_rule_at: anchor.ago(5 * DAY),
            semantic_rule_at: anchor.ago(4 * DAY),
            off_rule_at: anchor.ago(6 * DAY + 6 * HOUR),
            off_rule_disabled_at: anchor.ago(3 * DAY),
            lone_from: anchor.ago(36 * HOUR),
        }
    }

    /// When the fit of the version activated at `activated` started.
    pub fn fit_started(activated: Timestamp) -> Timestamp {
        crate::clock::minus(activated, 30 * MINUTE)
    }

    /// When the fit of the version activated at `activated` returned: its
    /// topics' `fitted_at`.
    pub fn fitted_at(activated: Timestamp) -> Timestamp {
        crate::clock::minus(activated, 10 * MINUTE)
    }
}

//! Who is asking, and what they may do: the [`Caller`] of one request and
//! the [`Permission`]s it holds. Every query and action names one
//! permission and checks it before reading or changing anything.

use std::fmt;

use crate::ids::OperatorId;

/// The authenticated caller of one request: an operator and the
/// permissions it holds.
///
/// Built only by [`OperatorDirectory::caller`](super::operators::OperatorDirectory::caller),
/// so its permissions are always those config gives its operator, and it
/// always holds at least one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Caller {
    pub(super) operator: OperatorId,
    pub(super) permissions: PermissionSet,
}

impl Caller {
    pub fn operator(&self) -> OperatorId {
        self.operator
    }

    pub fn permissions(&self) -> PermissionSet {
        self.permissions
    }

    pub fn has(&self, permission: Permission) -> bool {
        self.permissions.contains(permission)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum Permission {
    /// Topology (agent-centred and channel-centred, with node metadata and
    /// harness claims), series, the transmissions behind an edge (ids, times,
    /// byte counts and topic ids), channels (rows, names and promotion
    /// previews), a channel's resources and who used them, channel policy
    /// history, agents (rows, details and names), alert rules, alerts (the
    /// list and one by id) and the topic history (versions, sizes,
    /// lineage): ids, counts, times and similarities, no message content
    /// and no topic labels or terms. Also verdict logs, detection quality,
    /// transmission rows by id (state, parties, route, times, byte counts,
    /// topic ids, verdict), the overview's counts, and exports without
    /// content columns.
    View,
    /// Transmission content (the stored record and the evidence page's
    /// excerpts of message text), search, topics (their labels and terms
    /// come from message text), projections (fitting them, their jobs and
    /// their points) and exports that include content or read a
    /// projection.
    Content,
    /// Identity and policy: channel policy and promotion, agent merges,
    /// unmerges and renames, alert rules and their sinks (what the gateway
    /// alerts on, and where), and topic-version pins (what history the
    /// gateway keeps).
    Govern,
    /// Work alerts: acknowledge and resolve. Judge transmissions: set and
    /// withdraw verdicts.
    Triage,
    /// Operate the pipeline: list and replay dead-lettered deliveries. A
    /// replay re-runs a consumer on an old event, so it can reopen alerts or
    /// re-apply stale decisions.
    Operate,
    /// Read the audit log: every operator action, who asked for it and
    /// what came of it, including refused ones, and every change config
    /// made.
    Audit,
}

impl Permission {
    pub const ALL: [Self; 6] = [
        Self::View,
        Self::Content,
        Self::Govern,
        Self::Triage,
        Self::Operate,
        Self::Audit,
    ];

    const fn bit(self) -> u8 {
        1 << self as u8
    }
}

/// A set of permissions.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct PermissionSet(u8);

impl PermissionSet {
    pub const EMPTY: Self = Self(0);

    /// Every permission: what the trusted operator holds.
    pub const ALL: Self = {
        let mut bits = 0;
        let mut i = 0;
        while i < Permission::ALL.len() {
            bits |= Permission::ALL[i].bit();
            i += 1;
        }
        Self(bits)
    };

    pub fn of(permissions: impl IntoIterator<Item = Permission>) -> Self {
        Self(
            permissions
                .into_iter()
                .fold(0, |bits, permission| bits | permission.bit()),
        )
    }

    pub fn contains(self, permission: Permission) -> bool {
        self.0 & permission.bit() != 0
    }

    pub fn is_empty(self) -> bool {
        self.0 == 0
    }

    /// In `Permission::ALL` order.
    pub fn iter(self) -> impl Iterator<Item = Permission> {
        Permission::ALL
            .into_iter()
            .filter(move |permission| self.contains(*permission))
    }
}

impl fmt::Debug for PermissionSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_set().entries(self.iter()).finish()
    }
}

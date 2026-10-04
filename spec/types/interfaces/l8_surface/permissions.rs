//! Who is asking, and what they may do: the [`Caller`] of one request and
//! the [`Permission`]s it holds. Every query and action names one
//! permission and checks it before reading or changing anything.

use std::fmt;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::ids::OperatorId;
use crate::wire::Rejected;

/// The authenticated caller of one request: an operator and the
/// permissions it holds.
///
/// Built only by [`OperatorDirectory::caller`](super::operators::OperatorDirectory::caller),
/// so its permissions are always those config gives its operator, and it
/// always holds at least one.
///
/// Authority: it implements neither `Serialize` nor `Deserialize`
/// ([`crate::wire::authority`]), so no request can carry one and no
/// response leaks one; responses name its `OperatorId`. The one exception
/// is an audit record of a call (`OperatorRecord`, `ExportRecord`), a
/// response that keeps the operator and permissions of the caller as
/// authenticated: it writes them as a `RecordedCaller`, and a client that
/// decodes such a record gets a `Caller` back holding exactly what the
/// record says. The gateway never decodes one from a client.
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

/// On the wire, a string: `"content"`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
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

/// A set of permissions. On the wire, an array of [`Permission`] strings in
/// [`Permission::ALL`] order: `["view", "triage"]`.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct PermissionSet(u8);

/// The permissions in [`Permission::ALL`] order.
impl Serialize for PermissionSet {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_seq(self.iter())
    }
}

/// An array of permissions in any order; a repeat counts once, as
/// [`PermissionSet::of`] counts it.
impl<'de> Deserialize<'de> for PermissionSet {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Vec::<Permission>::deserialize(deserializer).map(Self::of)
    }
}

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

/// The caller of one recorded call, as an audit record writes it: the
/// operator and the permissions it held then. On the wire, the record's
/// `caller`: `{"operator": .., "permissions": [..]}`.
///
/// Only the audit records use it, to write the [`Caller`] they keep and to
/// read it back. Decoding refuses an empty set of permissions, which
/// `OperatorDirectory::caller` never grants.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", try_from = "RawRecordedCaller")]
pub(super) struct RecordedCaller {
    operator: OperatorId,
    permissions: PermissionSet,
}

/// A recorded caller with no permissions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct NoPermissions;

/// `RecordedCaller`'s fields, decoded without the check.
#[derive(Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
struct RawRecordedCaller {
    operator: OperatorId,
    permissions: PermissionSet,
}

impl TryFrom<RawRecordedCaller> for RecordedCaller {
    type Error = Rejected<NoPermissions>;

    fn try_from(raw: RawRecordedCaller) -> Result<Self, Self::Error> {
        if raw.permissions.is_empty() {
            return Err(Rejected::new("recorded caller", NoPermissions));
        }
        Ok(Self {
            operator: raw.operator,
            permissions: raw.permissions,
        })
    }
}

impl From<&Caller> for RecordedCaller {
    fn from(caller: &Caller) -> Self {
        Self {
            operator: caller.operator,
            permissions: caller.permissions,
        }
    }
}

impl From<RecordedCaller> for Caller {
    fn from(recorded: RecordedCaller) -> Self {
        Self {
            operator: recorded.operator,
            permissions: recorded.permissions,
        }
    }
}

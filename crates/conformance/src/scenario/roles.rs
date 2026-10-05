//! Roles: the symbolic names a scenario gives the things it is about.
//!
//! A test never names an id. It names a role (`hijacked_wiki::WIKI`), and
//! the harness binds each role to the id the implementation gave that
//! thing when it provisioned the scenario ([`super::Bindings`]). A role
//! carries its kind in its type, so an agent role can never be looked up as
//! a channel, and its scenario in its value, so two scenarios composed into
//! one world never share a name.

use std::fmt;
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;

use crosstalk_spec::ids::{AgentId, AlertRuleId, ChannelId, MergeId, ResourceId, TransmissionId};

/// What a role names, and the id it binds to.
pub trait RoleKind: 'static {
    /// The id an implementation gives a thing of this kind.
    type Id: Copy + Eq + fmt::Debug;
    /// For messages: `"agent"`, `"channel"`, ...
    const NAME: &'static str;
}

macro_rules! kinds {
    ($($(#[$doc:meta])* $kind:ident => $id:ty, $name:literal, $alias:ident;)*) => {
        $(
            $(#[$doc])*
            #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
            pub enum $kind {}

            impl RoleKind for $kind {
                type Id = $id;
                const NAME: &'static str = $name;
            }

            #[doc = concat!("A role naming one ", $name, ".")]
            pub type $alias = Role<$kind>;
        )*
    };
}

kinds! {
    /// An agent as the harness saw it: canonical, or an alias since merged.
    Agent => AgentId, "agent", AgentRole;
    /// A resource tools touched: a URL, a file, an MCP target, a key.
    Resource => ResourceId, "resource", ResourceRole;
    /// A channel: declared in config, or discovered by its first
    /// cross-agent transmission.
    Channel => ChannelId, "channel", ChannelRole;
    /// One transmission between two agents.
    Transmission => TransmissionId, "transmission", TransmissionRole;
    /// One merge record of the identity resolver.
    Merge => MergeId, "merge", MergeRole;
    /// One alert rule.
    Rule => AlertRuleId, "rule", RuleRole;
}

/// A name for one thing a scenario is about: its scenario, its name within
/// it and, in its type, its kind.
pub struct Role<K: RoleKind> {
    scenario: &'static str,
    name: &'static str,
    kind: PhantomData<K>,
}

impl<K: RoleKind> Role<K> {
    /// The role `name` of scenario `scenario`. Named scenarios declare
    /// theirs as constants.
    pub const fn new(scenario: &'static str, name: &'static str) -> Self {
        Self {
            scenario,
            name,
            kind: PhantomData,
        }
    }

    pub const fn scenario(self) -> &'static str {
        self.scenario
    }

    pub const fn name(self) -> &'static str {
        self.name
    }

    /// The kind-free key bindings are stored under.
    pub(crate) const fn key(self) -> RoleKey {
        RoleKey {
            kind: K::NAME,
            scenario: self.scenario,
            name: self.name,
        }
    }
}

// Derives would bound `K` itself; a role is plain data whatever its kind.
impl<K: RoleKind> Clone for Role<K> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<K: RoleKind> Copy for Role<K> {}

impl<K: RoleKind> PartialEq for Role<K> {
    fn eq(&self, other: &Self) -> bool {
        (self.scenario, self.name) == (other.scenario, other.name)
    }
}

impl<K: RoleKind> Eq for Role<K> {}

impl<K: RoleKind> Hash for Role<K> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        (self.scenario, self.name).hash(state);
    }
}

impl<K: RoleKind> fmt::Debug for Role<K> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}::{}", K::NAME, self.scenario, self.name)
    }
}

impl<K: RoleKind> fmt::Display for Role<K> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

/// A role with its kind as a value, for maps over every kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RoleKey {
    pub kind: &'static str,
    pub scenario: &'static str,
    pub name: &'static str,
}

impl fmt::Display for RoleKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}::{}", self.kind, self.scenario, self.name)
    }
}

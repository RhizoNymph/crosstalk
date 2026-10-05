//! What a harness hands back with a provisioned world: every role of the
//! scenario bound to the id the implementation gave it.

use std::any::Any;
use std::collections::HashMap;

use super::roles::{Role, RoleKey, RoleKind};

/// A role the harness did not bind.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("{0} is not bound")]
pub struct Unbound(pub RoleKey);

/// Each role of a provisioned scenario, bound to an id.
#[derive(Debug, Default)]
pub struct Bindings {
    ids: HashMap<RoleKey, Box<dyn Any + Send + Sync>>,
}

impl Bindings {
    pub fn new() -> Self {
        Self::default()
    }

    /// Binds `role` to `id`, replacing an earlier binding.
    pub fn bind<K: RoleKind>(&mut self, role: Role<K>, id: K::Id)
    where
        K::Id: Send + Sync,
    {
        self.ids.insert(role.key(), Box::new(id));
    }

    /// The id `role` is bound to.
    pub fn get<K: RoleKind>(&self, role: Role<K>) -> Result<K::Id, Unbound> {
        self.ids
            .get(&role.key())
            .and_then(|id| id.downcast_ref::<K::Id>())
            .copied()
            .ok_or(Unbound(role.key()))
    }

    /// Every role bound, for reports.
    pub fn roles(&self) -> impl Iterator<Item = RoleKey> + '_ {
        self.ids.keys().copied()
    }

    /// Adds every binding of `other`.
    pub fn extend(&mut self, other: Bindings) {
        self.ids.extend(other.ids);
    }
}

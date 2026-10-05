//! Who the UI acts as: the [`Access`] every request reads its caller and
//! the "signed in as" name from (`crate::app::caller`, `crate::app::access`).
//!
//! The local backends' is fixed (the trusted operator config names). The
//! http backend's follows the server: it is resolved from the server's
//! operator directory at startup and refreshed while serving
//! (`crate::backend::http::identity`), sent through a `watch` channel, so a
//! permission the server takes away stops being offered within one
//! refresh.

use tokio::sync::watch;

use crate::config::Access;

/// The current [`Access`], as an app context value.
#[derive(Debug, Clone)]
pub struct Identity(watch::Receiver<Access>);

impl Identity {
    /// An identity that never changes.
    pub fn fixed(access: Access) -> Self {
        // A receiver keeps the last value after its sender is dropped.
        let (_sender, receiver) = watch::channel(access);
        Self(receiver)
    }

    /// Whatever `receiver` holds when a request asks.
    pub fn watching(receiver: watch::Receiver<Access>) -> Self {
        Self(receiver)
    }

    /// The access in force now.
    pub fn current(&self) -> Access {
        self.0.borrow().clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::operator;
    use crate::url::ulid::UlidId;

    #[test]
    fn a_fixed_identity_outlives_its_sender() {
        let identity = Identity::fixed(operator());
        assert_eq!(identity.current(), operator());
    }

    #[test]
    fn a_watched_identity_follows_its_sender() {
        let (sender, receiver) = watch::channel(operator());
        let identity = Identity::watching(receiver);
        let other = crate::config::Access::of_operator(
            &crosstalk_spec::interfaces::l8_surface::operators::Operator {
                id: crosstalk_spec::ids::OperatorId::from_raw(2),
                name: crosstalk_spec::interfaces::l8_surface::operators::OperatorName::new("b")
                    .expect("name"),
                permissions: crosstalk_spec::interfaces::l8_surface::PermissionSet::of([
                    crosstalk_spec::interfaces::l8_surface::Permission::View,
                ]),
            },
        )
        .expect("access");
        sender.send_replace(other.clone());
        assert_eq!(identity.current(), other);
    }
}

//! Values that never serialize because they hold a secret: the
//! [`DeploymentSecret`] and the [`KeyedHasher`] that reads it
//! (`canonical.ids.secret-never-emitted`,
//! `canonical.ids.secret-never-serialized`).
//!
//! The checks are `assert_not_impl!` items below: each fails to compile if
//! its type gains a listed trait. Without `Serialize` neither can reach a
//! wire value, a stored row or a blob; without `Clone` the key exists once
//! per load; without `PartialEq` no code compares keys. Their `Debug` and
//! `Display` print the version alone. The doctests show the same from
//! outside the crate.
//!
//! ```
//! use crosstalk_spec::ids::{DeploymentSecret, SecretVersion};
//!
//! let secret = DeploymentSecret::new(SecretVersion(3), [7; 32]);
//! assert_eq!(format!("{secret}"), "deployment secret version 3");
//! assert_eq!(format!("{secret:?}"), "DeploymentSecret { version: SecretVersion(3), .. }");
//! ```
//!
//! ```compile_fail,E0277
//! use crosstalk_spec::ids::{DeploymentSecret, SecretVersion};
//!
//! // A secret has no JSON form.
//! let secret = DeploymentSecret::new(SecretVersion(3), [7; 32]);
//! let _ = serde_json::to_string(&secret);
//! ```
//!
//! ```compile_fail,E0277
//! use crosstalk_spec::ids::DeploymentSecret;
//!
//! // Nor is one read from JSON.
//! let _: DeploymentSecret = serde_json::from_str("{}").unwrap();
//! ```
//!
//! ```compile_fail,E0599
//! use crosstalk_spec::ids::{DeploymentSecret, SecretVersion};
//!
//! // Its key has no accessor.
//! let secret = DeploymentSecret::new(SecretVersion(3), [7; 32]);
//! let _ = secret.key();
//! ```
//!
//! [`DeploymentSecret`]: crate::ids::DeploymentSecret
//! [`KeyedHasher`]: crate::ids::KeyedHasher

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::ids::{DeploymentSecret, KeyedHasher};

assert_not_impl!(DeploymentSecret: Serialize, DeserializeOwned, Clone, PartialEq);
assert_not_impl!(KeyedHasher: Serialize, DeserializeOwned, Clone, PartialEq);

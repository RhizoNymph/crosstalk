//! Canonical resource identity.
//!
//! A resource's identity is its [`Locator`]: the registry refuses a second
//! resource with a stored resource's locator (`TrafficError::DuplicateLocator`),
//! so two accesses share a `ResourceId` exactly when their locators are
//! equal. Every locator the extractors build is canonical, so two agents
//! that touch the same file, page or wiki entry through different tools,
//! working directories or spellings get the same locator:
//! - [`path`]: lexical path resolution, relative paths against the stated
//!   working directory, `Opaque` when there is none;
//! - [`url`]: URL normalization;
//! - [`repo`]: files of a shared git repository, keyed by the repository
//!   whichever clone they are touched in;
//! - [`key`]: configured folding of MCP resource keys.
//!
//! Site rules ([`crate::extract::sites`]) then give the pages of known
//! wikis and forges one locator whatever URL reaches them.
//!
//! [`Locator`]: crosstalk_spec::derived::flow::resource::Locator

pub mod key;
pub mod path;
pub mod repo;
pub mod url;

pub use key::{KeyCanon, KeyError};
pub use path::{AbsolutePath, FileScope, PathError, WrittenPath, absolute_locator, file_locator};
pub use repo::{RepoBindings, RepoId};
pub use url::{UrlError, scan_urls, url_locator, url_text};

#[cfg(test)]
mod tests;

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
//! - [`url`]: URL normalization, and an `Opaque` locator for a URL whose
//!   host does not parse;
//! - [`repo`]: a shared git repository (`Locator::Repository`) and its
//!   files, keyed by the repository whichever clone or remote spelling
//!   they are touched through, and its issues and pull/merge requests;
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
pub use path::{
    AbsolutePath, FileScope, PathError, Place, WrittenPath, absolute_locator, file_locator,
};
pub use repo::{ForgeRepo, ForgeStyle, RepoBindings, RepoId, ThreadKind};
pub use url::{
    INVALID_HOST_URL_TOOL, UrlError, scan_urls, tool_url_locator, url_locator, url_text,
};

#[cfg(test)]
mod tests;

//! Spec locators as bench resources (design §6.1): `Repository` →
//! `repository`, `File` with a `<host>/<owner…>/<name>` host → `repo_file`,
//! `File` without a host → `file`, `Url` → `url` (its canonical text, which
//! the bench canonicalises again), `Opaque` → `opaque`.
//!
//! Two locators have no bench form, and are refused rather than guessed at:
//! a `File` whose host is a machine, not a repository (the bench's `file`
//! has no host), and an `Mcp` locator (the bench's `opaque` has no server
//! and needs a key where the spec's target is optional).

use a2a_bench_format::resource::{Repository, Resource};
use crosstalk_spec::derived::flow::resource::Locator;

use super::GoldenError;
use crate::truth::kinds::locator_key;

pub fn resource(locator: &Locator) -> Result<Resource, GoldenError> {
    match locator {
        Locator::Repository { host, owner, name } => Ok(Resource::Repository(Repository {
            host: host.0.clone(),
            owner: owner.clone(),
            name: name.clone(),
        })),
        Locator::File { host: None, path } => Ok(Resource::File { path: path.clone() }),
        Locator::File {
            host: Some(host),
            path,
        } => match repository(&host.0) {
            Some(repository) => Ok(Resource::RepoFile {
                repository,
                path: path.clone(),
            }),
            None => Err(GoldenError::Unexpressible(super::Gap::FileOnHost {
                locator: locator_key(locator),
            })),
        },
        Locator::Url { .. } => Ok(Resource::Url(locator_key(locator))),
        Locator::Opaque { tool, key } => Ok(Resource::Opaque {
            tool: tool.0.clone(),
            key: key.clone(),
        }),
        Locator::Mcp { .. } => Err(GoldenError::Unexpressible(super::Gap::McpLocator {
            locator: locator_key(locator),
        })),
    }
}

/// `<host>/<owner…>/<name>`: a repository file's host, as the extractors
/// write it (`RepoId::file`).
fn repository(host: &str) -> Option<Repository> {
    let (forge, rest) = host.split_once('/')?;
    let (owner, name) = rest.rsplit_once('/')?;
    if forge.is_empty() || owner.is_empty() || name.is_empty() {
        return None;
    }
    Some(Repository {
        host: forge.to_owned(),
        owner: owner.to_owned(),
        name: name.to_owned(),
    })
}

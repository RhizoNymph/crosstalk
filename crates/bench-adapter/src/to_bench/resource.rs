//! Spec locators as bench resources (design §6.1): `Repository` →
//! `repository`, `File` with a `<host>/<owner…>/<name>` host → `repo_file`,
//! any other `File` → `file` (with its host, if any), `Url` → `url` (its
//! canonical text `<scheme>://<host><path>[?<query>]`, which the bench
//! canonicalises again), `Mcp` → `mcp`,
//! `Opaque` → `opaque`. Every locator has a bench form.

use a2a_bench_format::resource::{Repository, Resource};
use crosstalk_spec::derived::flow::resource::Locator;

pub fn resource(locator: &Locator) -> Resource {
    match locator {
        Locator::Repository { host, owner, name } => Resource::Repository(Repository {
            host: host.0.clone(),
            owner: owner.clone(),
            name: name.clone(),
        }),
        Locator::File { host, path } => match host.as_ref().and_then(|host| repository(&host.0)) {
            Some(repository) => Resource::RepoFile {
                repository,
                path: path.clone(),
            },
            None => Resource::File {
                host: host.as_ref().map(|host| host.0.clone()),
                path: path.clone(),
            },
        },
        Locator::Url {
            scheme,
            host,
            path,
            query,
        } => Resource::Url(match query {
            Some(query) => format!("{scheme}://{}{path}?{query}", host.0),
            None => format!("{scheme}://{}{path}", host.0),
        }),
        Locator::Opaque { tool, key } => Resource::Opaque {
            tool: tool.0.clone(),
            key: key.clone(),
        },
        Locator::Mcp {
            server,
            tool,
            target,
        } => Resource::Mcp {
            server: server.clone(),
            tool: tool.0.clone(),
            target: target.clone(),
        },
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

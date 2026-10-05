//! Finding results files (`*.json` directly under the dataset root) and
//! picking simulations from them.
//!
//! Files are sorted by name; `include` entries are substrings that must all
//! occur in a file's name. A `limit` is a number of simulations spread over
//! the files: each file gives at most `ceil(limit / files)`, evenly spaced
//! through its simulations (so across its tasks), until the limit is met.

use std::fs;
use std::path::{Path, PathBuf};

use super::Tau2Error;

/// Which simulations to read.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Selection {
    /// At most this many simulations.
    pub limit: Option<usize>,
    /// Substrings a file name must all contain.
    pub include: Vec<String>,
}

/// The results files under `root` that `selection` picks, relative to it.
pub fn discover(root: &Path, selection: &Selection) -> Result<Vec<PathBuf>, Tau2Error> {
    let io = |source| Tau2Error::Io {
        path: root.display().to_string(),
        source,
    };
    if !root.is_dir() {
        return Err(Tau2Error::NoResults {
            root: root.display().to_string(),
        });
    }
    let mut files = Vec::new();
    for entry in fs::read_dir(root).map_err(io)? {
        let path = entry.map_err(io)?.path();
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if !path.is_file() || !name.ends_with(".json") {
            continue;
        }
        if selection
            .include
            .iter()
            .all(|needle| name.contains(needle.as_str()))
        {
            files.push(PathBuf::from(name));
        }
    }
    files.sort();
    Ok(files)
}

/// How many simulations each of `files` files may give under `limit`.
pub fn quota(limit: Option<usize>, files: usize) -> Option<usize> {
    limit.map(|limit| limit.div_ceil(files.max(1)))
}

/// The indices of at most `quota` of `count` simulations, evenly spaced.
pub fn pick(count: usize, quota: Option<usize>) -> Vec<usize> {
    match quota {
        Some(quota) if quota < count => (0..quota).map(|i| i * count / quota).collect(),
        _ => (0..count).collect(),
    }
}

/// The world key of simulation `index` of `file`: the file's stem and the
/// index.
pub fn world_name(file: &str, index: usize) -> String {
    format!("{}/{index}", file.trim_end_matches(".json"))
}

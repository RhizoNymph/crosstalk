//! Finding Open-SWE shards: `data/<harness>/<model>/<dataset>/*.parquet`
//! under the dataset root, sorted by relative path, filtered by
//! `--include` (a substring of the relative path, so `openhands` or
//! `qwen36_27b/scale-swe` pick harnesses and models) and cut to `--limit`
//! shards.

use std::fs;
use std::path::{Path, PathBuf};

use super::OpenSweError;
use crate::datasets::salt::Selection;

/// One shard and what its path says about it.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Shard {
    /// Relative to the dataset root, with `/` separators.
    pub relative: String,
    pub harness: String,
    pub model: String,
    pub dataset: String,
}

impl Shard {
    /// The shard of a relative path `data/<harness>/<model>/<dataset>/<file>`.
    pub fn parse(relative: &str) -> Option<Self> {
        let parts: Vec<&str> = relative.split('/').collect();
        match parts.as_slice() {
            ["data", harness, model, dataset, file] if file.ends_with(".parquet") => Some(Self {
                relative: relative.to_owned(),
                harness: (*harness).to_owned(),
                model: (*model).to_owned(),
                dataset: (*dataset).to_owned(),
            }),
            _ => None,
        }
    }
}

fn sorted_entries(path: &Path) -> Result<Vec<PathBuf>, OpenSweError> {
    let io = |source| OpenSweError::Io {
        path: path.display().to_string(),
        source,
    };
    let mut entries = fs::read_dir(path)
        .map_err(io)?
        .map(|entry| entry.map(|entry| entry.path()).map_err(io))
        .collect::<Result<Vec<_>, _>>()?;
    entries.sort();
    Ok(entries)
}

/// The shards `selection` picks, in path order.
pub fn discover(root: &Path, selection: &Selection) -> Result<Vec<Shard>, OpenSweError> {
    let data = root.join("data");
    if !data.is_dir() {
        return Err(OpenSweError::NoData {
            root: root.display().to_string(),
        });
    }
    let mut shards = Vec::new();
    for harness in sorted_entries(&data)?.into_iter().filter(|p| p.is_dir()) {
        for model in sorted_entries(&harness)?.into_iter().filter(|p| p.is_dir()) {
            for dataset in sorted_entries(&model)?.into_iter().filter(|p| p.is_dir()) {
                for file in sorted_entries(&dataset)? {
                    let Some(relative) = file
                        .strip_prefix(root)
                        .ok()
                        .map(|p| p.to_string_lossy().replace('\\', "/"))
                    else {
                        continue;
                    };
                    let Some(shard) = Shard::parse(&relative) else {
                        continue;
                    };
                    if selection.include.is_empty()
                        || selection
                            .include
                            .iter()
                            .any(|needle| relative.contains(needle.as_str()))
                    {
                        shards.push(shard);
                    }
                }
            }
        }
    }
    if let Some(limit) = selection.limit {
        shards.truncate(limit);
    }
    Ok(shards)
}

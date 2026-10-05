//! Where datasets live: a small TOML file naming a root directory and each
//! dataset's path under it. No dataset bytes are in the repository.
//!
//! ```toml
//! root = "~/Data/ai/agents"
//!
//! [datasets.salt]
//! path = "salt-nlp"
//! ```
//!
//! A leading `~` expands to `$HOME`. A dataset path may be absolute.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatasetConfig {
    pub path: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvalConfig {
    pub root: String,
    #[serde(default)]
    pub datasets: BTreeMap<String, DatasetConfig>,
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("reading config {path}: {source}")]
    Read {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("config {path} is not valid: {source}")]
    Parse {
        path: String,
        #[source]
        source: toml::de::Error,
    },
    #[error("dataset {0:?} is not configured")]
    UnknownDataset(String),
}

impl Default for EvalConfig {
    /// `~/Data/ai/agents`, with each dataset at the path [`DEFAULT_DATASETS`]
    /// gives it (the shipped `datasets.toml` says the same).
    fn default() -> Self {
        let datasets = DEFAULT_DATASETS
            .iter()
            .map(|(name, path)| {
                (
                    (*name).to_owned(),
                    DatasetConfig {
                        path: (*path).to_owned(),
                    },
                )
            })
            .collect();
        Self {
            root: "~/Data/ai/agents".to_owned(),
            datasets,
        }
    }
}

/// Each dataset's directory under the default root.
pub const DEFAULT_DATASETS: &[(&str, &str)] = &[
    ("salt", "salt-nlp"),
    ("agentdojo", "agentdojo"),
    ("tau2", "tau2-bench/data/tau2/results/final"),
    ("ai-village", "ai-village"),
    ("open_swe", "open-swe-traces"),
    ("lmcache", "lmcache"),
    ("swe_splice", "open-swe-traces"),
    ("cipher", "steganographic-evals/datasets/message_data"),
];

impl EvalConfig {
    pub fn parse(text: &str, path: &str) -> Result<Self, ConfigError> {
        toml::from_str(text).map_err(|source| ConfigError::Parse {
            path: path.to_owned(),
            source,
        })
    }

    pub fn load(path: &Path) -> Result<Self, ConfigError> {
        let shown = path.display().to_string();
        let text = std::fs::read_to_string(path).map_err(|source| ConfigError::Read {
            path: shown.clone(),
            source,
        })?;
        Self::parse(&text, &shown)
    }

    /// The directory of `dataset`.
    pub fn dataset_root(&self, dataset: &str, home: Option<&Path>) -> Result<PathBuf, ConfigError> {
        let entry = self
            .datasets
            .get(dataset)
            .ok_or_else(|| ConfigError::UnknownDataset(dataset.to_owned()))?;
        let path = expand(&entry.path, home);
        if path.is_absolute() {
            Ok(path)
        } else {
            Ok(expand(&self.root, home).join(path))
        }
    }
}

/// `text` with a leading `~` replaced by `home`.
pub fn expand(text: &str, home: Option<&Path>) -> PathBuf {
    match (text.strip_prefix('~'), home) {
        (Some(rest), Some(home)) => home.join(rest.trim_start_matches('/')),
        _ => PathBuf::from(text),
    }
}

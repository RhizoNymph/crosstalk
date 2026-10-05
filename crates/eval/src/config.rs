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
    /// `~/Data/ai/agents`, with SALT at `salt-nlp`, AgentDojo at `agentdojo`
    /// τ²-bench at `tau2-bench/data/tau2/results/final`, collusion-wiki at
    /// `collusion-wiki` and swarm-traces at `swarm-traces`.
    fn default() -> Self {
        let mut datasets = BTreeMap::new();
        datasets.insert(
            "salt".to_owned(),
            DatasetConfig {
                path: "salt-nlp".to_owned(),
            },
        );
        datasets.insert(
            "agentdojo".to_owned(),
            DatasetConfig {
                path: "agentdojo".to_owned(),
            },
        );
        datasets.insert(
            "tau2".to_owned(),
            DatasetConfig {
                path: "tau2-bench/data/tau2/results/final".to_owned(),
            },
        );
        datasets.insert(
            "collusion-wiki".to_owned(),
            DatasetConfig {
                path: "collusion-wiki".to_owned(),
            },
        );
        datasets.insert(
            "swarm-traces".to_owned(),
            DatasetConfig {
                path: "swarm-traces".to_owned(),
            },
        );
        Self {
            root: "~/Data/ai/agents".to_owned(),
            datasets,
        }
    }
}

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

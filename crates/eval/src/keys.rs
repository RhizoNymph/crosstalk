//! Eval-owned keys: datasets, worlds, agents and source references. Spec
//! ids (`ExchangeId`, `MessageHash`, …) are used as they are; these name
//! what the spec has no notion of.
//!
//! A **world** is a set of agents that can only communicate with each other
//! (one SALT trace file). Agent names are only unique within their world, so
//! an [`AgentKey`] always carries its world.

use std::fmt;

use serde::{Deserialize, Serialize};

/// A dataset, by its short name (`salt`).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct DatasetId(String);

impl DatasetId {
    pub fn new(name: impl Into<String>) -> Self {
        Self(name.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for DatasetId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// A world within a dataset: agents in different worlds never communicate.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct WorldKey(String);

impl WorldKey {
    pub fn new(name: impl Into<String>) -> Self {
        Self(name.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for WorldKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// One agent: its world and its name in that world.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct AgentKey {
    pub world: WorldKey,
    pub name: String,
}

impl AgentKey {
    pub fn new(world: WorldKey, name: impl Into<String>) -> Self {
        Self {
            world,
            name: name.into(),
        }
    }
}

impl fmt::Display for AgentKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}#{}", self.world, self.name)
    }
}

/// Where a record came from: a file relative to the dataset root and a
/// JSON-pointer-like path inside it. Every exchange and label carries one, so
/// a label can be traced back to raw data, and ids derive from it.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct SourceRef {
    pub file: String,
    pub path: String,
}

impl SourceRef {
    pub fn new(file: impl Into<String>, path: impl Into<String>) -> Self {
        Self {
            file: file.into(),
            path: path.into(),
        }
    }

    /// The same file, at `path`.
    pub fn at(&self, path: impl Into<String>) -> Self {
        Self {
            file: self.file.clone(),
            path: path.into(),
        }
    }
}

impl fmt::Display for SourceRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}#{}", self.file, self.path)
    }
}

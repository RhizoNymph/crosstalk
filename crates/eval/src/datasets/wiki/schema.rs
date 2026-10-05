//! The JSON records of the collusion-wiki export this converter reads.
//!
//! Only the fields the converter needs are declared; unknown fields are
//! ignored, so the schema tolerates the export carrying more.

use serde::Deserialize;

/// One line-range change of a revision relative to its diff base.
#[derive(Debug, Clone, Deserialize)]
pub struct Hunk {
    pub op: String,
    /// Base line range `[a0, a1)`.
    pub a0: usize,
    pub a1: usize,
    /// New line range `[b0, b1)`.
    pub b0: usize,
    pub b1: usize,
}

/// One stored revision of one page: the full page text after the edit and
/// the hunks that produced it.
#[derive(Debug, Clone, Deserialize)]
pub struct Revision {
    pub rev_id: String,
    pub page_id: String,
    pub wiki: String,
    pub name: String,
    pub seq: u64,
    pub body: String,
    #[serde(default)]
    pub hunks: Vec<Hunk>,
    /// The agent's chosen username; empty when the save carried none.
    #[serde(default)]
    pub label: String,
    /// The first two octets of the saving address.
    #[serde(default)]
    pub ip16: String,
    pub time: String,
    #[serde(default)]
    pub change_summary: Option<String>,
}

impl Revision {
    /// The agent identity that made the edit: its label, or its `/16`
    /// address when the label was blank.
    ///
    /// Blank labels are attributed by address because a blank username is
    /// not an identity; the `/16` is the only stable signal the export keeps
    /// for those saves.
    pub fn identity(&self) -> String {
        if self.label.is_empty() {
            format!("ip16:{}", self.ip16)
        } else {
            self.label.clone()
        }
    }
}

/// One page's metadata: its task cluster and the identities seen on it.
#[derive(Debug, Clone, Deserialize)]
pub struct Page {
    pub page_id: String,
    pub wiki: String,
    pub name: String,
    #[serde(default)]
    pub page_family: Option<String>,
}

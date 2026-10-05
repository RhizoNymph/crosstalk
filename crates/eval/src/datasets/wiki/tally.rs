//! Channel-discovery inputs per task cluster: how many pages each
//! `page_family` has, and how many of them two or more agents wrote (the
//! pages a cross-agent channel can be discovered on).

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use super::schema::Revision;

/// The family name used for a page `pages.jsonl` gives none.
pub const NO_FAMILY: &str = "(none)";

/// One task cluster's counts.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct FamilyStats {
    pub pages: u64,
    /// Pages two or more distinct identities edited.
    pub multi_author_pages: u64,
    pub revisions: u64,
}

/// Per-family counts over the selected revisions.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FamilyTally {
    pub families: BTreeMap<String, FamilyStats>,
}

impl FamilyTally {
    /// Counts `revisions`, naming each page's family through `family_of`.
    pub fn new<'a>(revisions: &'a [Revision], family_of: impl Fn(&str) -> Option<&'a str>) -> Self {
        let mut authors: BTreeMap<&str, (BTreeSet<String>, u64)> = BTreeMap::new();
        for rev in revisions {
            let entry = authors.entry(rev.page_id.as_str()).or_default();
            entry.0.insert(rev.identity());
            entry.1 += 1;
        }
        let mut families: BTreeMap<String, FamilyStats> = BTreeMap::new();
        for (page, (identities, revisions)) in authors {
            let family = family_of(page).unwrap_or(NO_FAMILY);
            let stats = families.entry(family.to_owned()).or_default();
            stats.pages += 1;
            stats.revisions += revisions;
            if identities.len() >= 2 {
                stats.multi_author_pages += 1;
            }
        }
        Self { families }
    }
}

impl fmt::Display for FamilyTally {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let pages: u64 = self.families.values().map(|s| s.pages).sum();
        let multi: u64 = self.families.values().map(|s| s.multi_author_pages).sum();
        writeln!(
            f,
            "wiki families: {pages} pages, {multi} written by two or more agents"
        )?;
        writeln!(
            f,
            "{:<36} {:>8} {:>14} {:>10}",
            "page_family", "pages", "multi_author", "revisions"
        )?;
        let mut rows: Vec<(&String, &FamilyStats)> = self.families.iter().collect();
        rows.sort_by(|a, b| b.1.pages.cmp(&a.1.pages).then_with(|| a.0.cmp(b.0)));
        for (family, stats) in rows {
            writeln!(
                f,
                "{:<36} {:>8} {:>14} {:>10}",
                family, stats.pages, stats.multi_author_pages, stats.revisions
            )?;
        }
        Ok(())
    }
}

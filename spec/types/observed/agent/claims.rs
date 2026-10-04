//! Harness claims seen on an agent's exchanges.
//!
//! A [`HarnessClaim`] (harness family, version, User-Agent) is what a request
//! says about itself; harnesses impersonate each other, so it is shown as
//! "claimed", never as identity, and never read by identity resolution.
//!
//! **Maintained by L3.** For every captured exchange that carries a claim,
//! the claim store records it, with the exchange's start time, against the
//! agent the exchange was attributed to (`ClaimStore::record`). Recording is
//! idempotent and order-independent: a claim seen again keeps the latest
//! time.
//!
//! **Merges are aliases here too.** Claims stay stored under the attributed
//! agent. A canonical agent's claims are the [`ClaimSet::union`] of its own
//! and those of every agent that resolves to it, read at query time. A merge
//! or unmerge changes no stored claim, so an unmerge splits the claims again
//! on the next read.

use crate::observed::client::{HarnessClaim, HarnessFamily};
use crate::support::Timestamp;

/// One distinct claim and the latest time an exchange carried it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SeenClaim {
    pub claim: HarnessClaim,
    pub last_seen: Timestamp,
}

/// The distinct claims seen for one agent, most recently seen first.
///
/// Built only through [`ClaimSet::default`], [`ClaimSet::from_entries`],
/// [`ClaimSet::observe`] and [`ClaimSet::union`]: no claim appears twice,
/// and entries are ordered by `last_seen` descending, ties broken by the
/// claim's family, then User-Agent, then version, so equal sets compare
/// equal whatever order they were built in.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ClaimSet {
    entries: Vec<SeenClaim>,
}

/// The same claim was listed twice.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DuplicateClaim(pub HarnessClaim);

impl ClaimSet {
    /// Rebuild a stored set, rejecting a claim listed twice.
    pub fn from_entries(entries: Vec<SeenClaim>) -> Result<Self, DuplicateClaim> {
        for (index, entry) in entries.iter().enumerate() {
            let repeated = entries
                .iter()
                .skip(index + 1)
                .any(|later| later.claim == entry.claim);
            if repeated {
                return Err(DuplicateClaim(entry.claim.clone()));
            }
        }
        let mut set = Self { entries };
        set.sort();
        Ok(set)
    }

    /// Record that an exchange at `at` carried `claim`. Keeps the latest
    /// time for a claim already present, so redelivery and out-of-order
    /// arrival give the same set.
    pub fn observe(&mut self, claim: HarnessClaim, at: Timestamp) {
        match self.entries.iter_mut().find(|entry| entry.claim == claim) {
            Some(entry) => entry.last_seen = entry.last_seen.max(at),
            None => self.entries.push(SeenClaim {
                claim,
                last_seen: at,
            }),
        }
        self.sort();
    }

    /// The claims of a canonical agent: every claim in any of `sets` (the
    /// agent's own and each alias's), once, with its latest time.
    pub fn union<'a>(sets: impl IntoIterator<Item = &'a ClaimSet>) -> Self {
        let mut union = Self::default();
        for entry in sets.into_iter().flat_map(|set| set.entries.iter()) {
            union.observe(entry.claim.clone(), entry.last_seen);
        }
        union
    }

    /// Most recently seen first.
    pub fn entries(&self) -> &[SeenClaim] {
        &self.entries
    }

    /// When `claim` was last seen, if ever.
    pub fn last_seen(&self, claim: &HarnessClaim) -> Option<Timestamp> {
        self.entries
            .iter()
            .find(|entry| entry.claim == *claim)
            .map(|entry| entry.last_seen)
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    fn sort(&mut self) {
        self.entries.sort_by(|a, b| {
            b.last_seen
                .cmp(&a.last_seen)
                .then_with(|| family_rank(&a.claim.family).cmp(&family_rank(&b.claim.family)))
                .then_with(|| a.claim.user_agent.cmp(&b.claim.user_agent))
                .then_with(|| a.claim.version.cmp(&b.claim.version))
        });
    }
}

fn family_rank(family: &HarnessFamily) -> u8 {
    match family {
        HarnessFamily::ClaudeCode => 0,
        HarnessFamily::Codex => 1,
        HarnessFamily::Pi => 2,
        HarnessFamily::OhMyPi => 3,
        HarnessFamily::Unknown => 4,
    }
}

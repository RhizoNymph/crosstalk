//! The agents list filter.
//!
//! [`AgentFilter::matches`] is the definition: L3's list query must return
//! exactly the canonical agents it admits. An empty list (or no text) does
//! not restrict, and the fields combine with AND.
//!
//! | Field | Keeps a canonical agent when |
//! | --- | --- |
//! | `states` | its state kind is listed |
//! | `claimed` | a claim in its [`ClaimSet`] (the union over its aliases, at any time) has a listed [`HarnessFamily`] |
//! | `text` | [`AgentFilter::text_matches`]: a substring of its label, or a prefix of its id or an alias's id |
//! | `parents` | its canonical parent equals the canonical form of a listed agent |
//!
//! **Claims are claims.** `claimed` selects on what the harness said, the
//! same claims a row shows as "claimed"; it never selects on identity
//! evidence. An agent with no claims matches no non-empty `claimed`.
//!
//! **Text.** Labels are words, so the text matches anywhere in one. Ids are
//! opaque and uniformly random after their time prefix, so a substring
//! match on them would match almost anything for short text; the text
//! matches an id only from its start, which is also how a pasted or typed
//! id is entered and how an index serves it. The id matched is the agent's
//! or any alias's, so an old id still finds the agent it was merged into;
//! the label matched is the canonical agent's only, since a merged agent's
//! label is shown nowhere.
//!
//! **Case.** The label and the text are compared after Unicode's default
//! lowercase mapping on both (`str::to_lowercase`, locale-independent).
//! That is not full case folding: `ß` and `ss` differ. Ids are compared
//! ignoring ASCII case; Crockford's aliases (`I` and `L` for `1`, `O` for
//! `0`) are not applied, so text with those letters matches no id.
//!
//! [`ClaimSet`]: crate::observed::agent::ClaimSet

use serde::{Deserialize, Serialize};

use crate::aggregates::node::CanonicalStateKind;
use crate::aliases::Aliases;
use crate::ids::AgentId;
use crate::observed::client::HarnessFamily;
use crate::support::DisplayText;
use crate::wire::WireRequest;

use super::AgentProfile;

/// The free text of an agents filter: trimmed, non-empty, at most 64
/// characters (a label's limit), no control characters.
pub type AgentText = DisplayText<64>;

/// Restricts `QueryApi::agents`. Only canonical agents are rows, so `states`
/// cannot name `Merged`.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct AgentFilter {
    pub states: Vec<CanonicalStateKind>,
    /// Harness families claimed on the agent's exchanges.
    pub claimed: Vec<HarnessFamily>,
    pub text: Option<AgentText>,
    /// One level of the sub-agent tree: agents whose canonical parent is
    /// one of these, each resolved through merges first.
    pub parents: Vec<AgentId>,
}

/// A client chooses every field of the agents filter.
impl WireRequest for AgentFilter {}

impl AgentFilter {
    /// Whether the canonical agent `profile` describes passes the filter,
    /// with the listed parents resolved through `aliases`.
    pub fn matches(&self, profile: &AgentProfile, aliases: impl Aliases) -> bool {
        let by_state = self.states.is_empty() || self.states.contains(&profile.state_kind());
        let by_claim = self.claimed.is_empty()
            || profile
                .claims()
                .entries()
                .iter()
                .any(|seen| self.claimed.contains(&seen.claim.family));
        let by_parent = self.parents.is_empty()
            || profile.parent().is_some_and(|parent| {
                self.parents
                    .iter()
                    .any(|&listed| aliases.agent(listed) == parent)
            });
        let by_text = self
            .text
            .as_ref()
            .is_none_or(|text| Self::text_matches(text, profile));
        by_state && by_claim && by_parent && by_text
    }

    /// Whether `text`, lowercased, is a substring of the lowercased label,
    /// or, ignoring ASCII case, a prefix of the ULID text
    /// (`AgentId::ulid_text`) of the agent or one of its aliases.
    pub fn text_matches(text: &AgentText, profile: &AgentProfile) -> bool {
        let needle = text.as_str().to_lowercase();
        let in_label = profile
            .label()
            .is_some_and(|label| label.as_str().to_lowercase().contains(&needle));
        in_label
            || std::iter::once(profile.id())
                .chain(profile.aliases().iter().copied())
                .any(|id| id_starts_with(&id.ulid_text(), text.as_str()))
    }
}

/// `prefix` begins `id`, ignoring ASCII case.
fn id_starts_with(id: &str, prefix: &str) -> bool {
    id.len() >= prefix.len()
        && id.is_char_boundary(prefix.len())
        && id[..prefix.len()].eq_ignore_ascii_case(prefix)
}

//! The channel plan: each drafted channel with its id and its resources'
//! ids. Declared channels take the ids the registry assigned when config
//! declared them; discovered channels and every resource get minted ids.

use std::collections::BTreeMap;

use crosstalk_spec::derived::flow::resource::Locator;
use crosstalk_spec::ids::{AgentId, ChannelId, ResourceId};
use crosstalk_spec::support::Timestamp;

use crate::error::WorldError;
use crate::mint::Mint;
use crate::scenario::ChannelKey;
use crate::text::Theme;

use super::agents::Cast;
use super::drafts::{Draft, DraftOrigin, Target};

#[derive(Debug, Clone, PartialEq)]
pub struct ChannelSpec {
    pub key: ChannelKey,
    pub id: ChannelId,
    pub origin: DraftOrigin,
    pub target: Target,
    /// Resources, as locators with their ids: a declared channel's, or a
    /// discovered channel's one seed.
    pub resources: Vec<(ResourceId, Locator)>,
    /// When the channel carries traffic.
    pub from: Timestamp,
    pub until: Timestamp,
    /// Relative share of channel-routed transmissions.
    pub weight: f64,
    pub writers: Vec<AgentId>,
    pub readers: Vec<AgentId>,
    pub themes: Vec<(Theme, f64)>,
    /// Whether transmissions on it can be confirmed.
    pub confirms: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ChannelPlan {
    pub specs: Vec<ChannelSpec>,
}

impl ChannelPlan {
    pub fn ids(&self) -> BTreeMap<ChannelKey, ChannelId> {
        self.specs.iter().map(|s| (s.key, s.id)).collect()
    }

    pub fn id(&self, key: ChannelKey) -> Result<ChannelId, WorldError> {
        self.spec(key).map(|s| s.id)
    }

    pub fn spec(&self, key: ChannelKey) -> Result<&ChannelSpec, WorldError> {
        self.specs
            .iter()
            .find(|s| s.key == key)
            .ok_or_else(|| WorldError::missing(format!("channel {key:?}")))
    }

    pub fn by_id(&self, id: ChannelId) -> Option<&ChannelSpec> {
        self.specs.iter().find(|s| s.id == id)
    }
}

/// The plan of `drafts`, declared channels under `declared`'s ids.
pub fn plan(
    drafts: Vec<Draft>,
    declared: &BTreeMap<ChannelKey, ChannelId>,
    mint: &mut Mint,
    cast: &Cast,
) -> Result<ChannelPlan, WorldError> {
    let mut specs = Vec::new();
    for draft in drafts {
        let id = match &draft.origin {
            DraftOrigin::Declared { .. } => *declared
                .get(&draft.key)
                .ok_or_else(|| WorldError::missing(format!("declared channel {:?}", draft.key)))?,
            DraftOrigin::Discovered { .. } => mint.at(draft.created)?,
        };
        let resources = draft
            .origin
            .locators()
            .into_iter()
            .map(|l| Ok((mint.at(draft.window.0)?, l)))
            .collect::<Result<Vec<_>, WorldError>>()?;
        specs.push(ChannelSpec {
            key: draft.key,
            id,
            origin: draft.origin,
            target: draft.target,
            resources,
            from: draft.window.0,
            until: draft.window.1,
            weight: draft.weight,
            writers: cast.ids(draft.writers)?,
            readers: cast.ids(draft.readers)?,
            themes: draft.themes.to_vec(),
            confirms: draft.confirms,
        });
    }
    Ok(ChannelPlan { specs })
}

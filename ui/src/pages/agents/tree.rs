//! The sub-agent tree below an agent, read a level at a time up to a
//! bounded depth and number of agents.

use std::collections::{HashMap, HashSet};
use std::num::NonZeroU32;

use crosstalk_spec::aggregates::agents::filter::AgentFilter;
use crosstalk_spec::aggregates::node::CanonicalStateKind;
use crosstalk_spec::ids::AgentId;
use crosstalk_spec::interfaces::l8_surface::Caller;
use topcoat::context::Cx;

use crate::app::backend;
use crate::components::{agent_name, short_id};
use crate::pages::common::links::agent_url;
use crate::url::ulid::UlidId;
use crate::url::view_state::ViewState;
use crosstalk_spec::interfaces::l8_surface::QueryApi;

/// Levels below the agent that are read.
pub const MAX_DEPTH: usize = 4;
/// Agents read for one tree.
pub const MAX_NODES: usize = 60;

/// What the tree needs to know of one agent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Node {
    pub name: String,
    pub state: CanonicalStateKind,
    pub children: Vec<AgentId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TreeRow {
    /// 1 for a direct child.
    pub depth: usize,
    pub url: String,
    pub name: String,
    /// `None` when the agent could not be read.
    pub state: Option<CanonicalStateKind>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Tree {
    pub rows: Vec<TreeRow>,
    /// More agents exist below than were read.
    pub truncated: bool,
}

/// Depth-first rows under `roots`, in the order the backend lists children.
/// Agents missing from `nodes` are shown by id without their children.
pub fn flatten(
    roots: &[AgentId],
    nodes: &HashMap<AgentId, Node>,
    state: &ViewState,
) -> Vec<TreeRow> {
    let mut rows = Vec::new();
    let mut seen = HashSet::new();
    let mut stack: Vec<(AgentId, usize)> = roots.iter().rev().map(|id| (*id, 1)).collect();
    while let Some((id, depth)) = stack.pop() {
        if !seen.insert(id) {
            continue;
        }
        let node = nodes.get(&id);
        rows.push(TreeRow {
            depth,
            url: agent_url(id, state),
            name: node.map_or_else(|| short_id(id.to_ulid()), |n| n.name.clone()),
            state: node.map(|n| n.state),
        });
        if let Some(node) = node
            && depth < MAX_DEPTH
        {
            stack.extend(node.children.iter().rev().map(|c| (*c, depth + 1)));
        }
    }
    rows
}

/// Reads the sub-agents of `agent` a level at a time (one `agents` call
/// per level, filtered by parent, over the view's window), up to the
/// bounds. `roots` are its children as its detail lists them.
pub async fn load(
    cx: &Cx,
    caller: &Caller,
    agent: AgentId,
    roots: &[AgentId],
    state: &ViewState,
) -> Tree {
    let mut nodes: HashMap<AgentId, Node> = HashMap::new();
    let mut parents = vec![agent];
    let mut truncated = false;
    for _ in 0..MAX_DEPTH {
        if parents.is_empty() {
            break;
        }
        let Some(budget) = u32::try_from(MAX_NODES.saturating_sub(nodes.len()))
            .ok()
            .and_then(NonZeroU32::new)
        else {
            truncated = true;
            break;
        };
        let filter = AgentFilter {
            parents: parents.clone(),
            ..AgentFilter::default()
        };
        let page = match backend(cx)
            .agents(
                caller,
                &filter,
                state.scope.window,
                &crate::pages::common::paging::first(budget),
            )
            .await
        {
            Ok(page) => page.value,
            Err(error) => {
                tracing::warn!(error = ?error, agent = %agent.to_ulid(), "sub-agents unavailable");
                break;
            }
        };
        truncated |= page.next().is_some();
        let mut next = Vec::new();
        for row in page.items() {
            let profile = &row.profile;
            let id = profile.id();
            if id == agent || nodes.contains_key(&id) {
                continue;
            }
            if let Some(parent) = profile.parent().and_then(|p| nodes.get_mut(&p)) {
                parent.children.push(id);
            }
            next.push(id);
            nodes.insert(
                id,
                Node {
                    name: agent_name(profile),
                    state: profile.state_kind(),
                    children: Vec::new(),
                },
            );
        }
        parents = next;
    }
    Tree {
        rows: flatten(roots, &nodes, state),
        truncated,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::components::href::tests::state;

    fn node(name: &str, children: &[u128]) -> Node {
        Node {
            name: name.to_owned(),
            state: CanonicalStateKind::Provisional,
            children: children.iter().map(|c| AgentId::from_ulid(*c)).collect(),
        }
    }

    #[test]
    fn flattens_depth_first_and_stops_at_cycles() {
        let nodes: HashMap<_, _> = [
            (AgentId::from_ulid(2), node("a", &[4, 5])),
            (AgentId::from_ulid(3), node("b", &[])),
            (AgentId::from_ulid(4), node("a1", &[2])),
        ]
        .into_iter()
        .collect();
        let rows = flatten(
            &[AgentId::from_ulid(2), AgentId::from_ulid(3)],
            &nodes,
            &state(),
        );
        let shape: Vec<_> = rows.iter().map(|r| (r.depth, r.name.as_str())).collect();
        assert_eq!(shape, [(1, "a"), (2, "a1"), (2, "…000005"), (1, "b")]);
        assert_eq!(rows[2].state, None, "an unread agent has no state");
    }

    #[test]
    fn depth_is_bounded() {
        let nodes: HashMap<_, _> = (1..=10u128)
            .map(|i| (AgentId::from_ulid(i), node(&i.to_string(), &[i + 1])))
            .collect();
        let rows = flatten(&[AgentId::from_ulid(1)], &nodes, &state());
        assert_eq!(rows.len(), MAX_DEPTH);
    }
}

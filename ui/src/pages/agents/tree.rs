//! The sub-agent tree below an agent, read a bounded number of agents deep.

use std::collections::{HashMap, HashSet};

use crosstalk_spec::ids::AgentId;
use crosstalk_spec::interfaces::l8_surface::Caller;
use topcoat::context::Cx;

use crate::app::backend;
use crate::backend::Backend;
use crate::components::{agent_name, short_id};
use crate::contract::agents::AgentStateKind;
use crate::pages::common::links::agent_url;
use crate::url::ulid::UlidId;
use crate::url::view_state::ViewState;

/// Levels below the agent that are read.
pub const MAX_DEPTH: usize = 4;
/// Agents read for one tree.
pub const MAX_NODES: usize = 60;

/// What the tree needs to know of one agent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Node {
    pub name: String,
    pub state: AgentStateKind,
    pub children: Vec<AgentId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TreeRow {
    /// 1 for a direct child.
    pub depth: usize,
    pub url: String,
    pub name: String,
    /// `None` when the agent could not be read.
    pub state: Option<AgentStateKind>,
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

/// Reads the agents below `roots`, breadth first, up to the bounds.
pub async fn load(cx: &Cx, caller: &Caller, roots: &[AgentId], state: &ViewState) -> Tree {
    let mut nodes = HashMap::new();
    let mut frontier: Vec<AgentId> = roots.to_vec();
    let mut truncated = false;
    for _ in 0..MAX_DEPTH {
        let mut next = Vec::new();
        for id in frontier {
            if nodes.contains_key(&id) {
                continue;
            }
            if nodes.len() >= MAX_NODES {
                truncated = true;
                break;
            }
            match backend(cx).agent(caller, id).await {
                Ok(Some(detail)) => {
                    next.extend(detail.children.iter().copied());
                    nodes.insert(
                        id,
                        Node {
                            name: agent_name(&detail.summary),
                            state: detail.summary.state,
                            children: detail.children,
                        },
                    );
                }
                Ok(None) => {}
                Err(error) => {
                    tracing::debug!(%error, agent = %id.to_ulid(), "sub-agent unavailable");
                }
            }
        }
        if next.is_empty() {
            break;
        }
        frontier = next;
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
            state: AgentStateKind::Provisional,
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

//! Sections of the agent page: evidence, claims, sub-agents, aliases, merge
//! history and vetoes.

use crosstalk_spec::observed::agent::{Agent, AgentState};
use topcoat::Result;
use topcoat::view::{View, component, view};

use super::evidence::EvidenceRow;
use super::tree::Tree;
use crate::components::form::{LINK, SECTION, SECTION_TITLE, SMALL_BUTTON};
use crate::components::table::{ROW, TD, TD_MUTED, TD_NUM};
use crate::components::{
    claim_badge, data_table, empty_state, error_panel, format_time, kind_badge, short_id,
};
use crate::contract::agents::{ClaimSeen, MergeRecord, MergeVeto};
use crate::contract::errors::QueryError;
use crate::pages::common::links::agent_url;
use crate::pages::common::lookup::OperatorNames;
use crate::url::ulid::UlidId;
use crate::url::view_state::ViewState;

/// An agent link: URL and short id.
pub type AgentLink = (String, String);

fn link(id: crosstalk_spec::ids::AgentId, state: &ViewState) -> AgentLink {
    (agent_url(id, state), short_id(id.to_ulid()))
}

/// The state of an agent with its time, in words.
pub fn state_text(state: &AgentState, operators: &OperatorNames) -> String {
    match state {
        AgentState::Registered { at } => format!("registered in config at {}", format_time(*at)),
        AgentState::Provisional { first_seen } => {
            format!(
                "first seen {}; not yet corroborated",
                format_time(*first_seen)
            )
        }
        AgentState::Established { since } => format!("established since {}", format_time(*since)),
        AgentState::Merged { into, at, by } => format!(
            "merged into {} at {} by {}",
            short_id(into.to_ulid()),
            format_time(*at),
            operators.merge_author(*by)
        ),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AliasRow {
    pub link: AgentLink,
    pub evidence: u32,
    pub merged: String,
}

pub fn alias_rows(
    aliases: &[Agent],
    operators: &OperatorNames,
    state: &ViewState,
) -> Vec<AliasRow> {
    aliases
        .iter()
        .map(|a| AliasRow {
            link: link(a.id, state),
            evidence: a.evidence.count().get(),
            merged: state_text(&a.state, operators),
        })
        .collect()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MergeRow {
    pub id: String,
    pub at: String,
    pub from: AgentLink,
    pub into: AgentLink,
    pub by: String,
    pub repointed: usize,
    /// Who reverted it and when.
    pub reverted: Option<String>,
}

pub fn merge_rows(
    merges: &[MergeRecord],
    operators: &OperatorNames,
    state: &ViewState,
) -> Vec<MergeRow> {
    merges
        .iter()
        .map(|m| MergeRow {
            id: m.id.to_ulid(),
            at: format_time(m.at),
            from: link(m.from, state),
            into: link(m.into, state),
            by: operators.merge_author(m.by),
            repointed: m.repointed.len(),
            reverted: m.reverted.map(|(by, at)| {
                format!("reverted by {} at {}", operators.name(by), format_time(at))
            }),
        })
        .collect()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VetoRow {
    pub a: AgentLink,
    pub b: AgentLink,
    pub by: String,
    pub at: String,
}

pub fn veto_rows(
    vetoes: &[MergeVeto],
    operators: &OperatorNames,
    state: &ViewState,
) -> Vec<VetoRow> {
    vetoes
        .iter()
        .map(|v| VetoRow {
            a: link(v.a, state),
            b: link(v.b, state),
            by: operators.name(v.by),
            at: format_time(v.at),
        })
        .collect()
}

#[component]
pub async fn evidence_section(rows: Vec<EvidenceRow>) -> Result<impl View> {
    Ok(view! {
        <section class=(SECTION)>
            <h2 class=(SECTION_TITLE)>"Identity evidence"</h2>
            data_table(
                headers: &["Kind", "Value", "Scope", "Strength"],
                for row in rows {
                    <tr class=(ROW)>
                        <td class=(TD)>(row.kind)</td>
                        <td class=(TD)><span class="font-mono text-xs" title=(row.full)>(row.value)</span></td>
                        <td class=(TD_MUTED)>(row.scope.unwrap_or_default())</td>
                        <td class=(TD)>kind_badge(value: row.strength)</td>
                    </tr>
                }
            )
            <p class="mt-1 text-xs text-zinc-500">"Most specific first. Weak evidence cannot establish an agent on its own."</p>
        </section>
    })
}

#[component]
pub async fn claims_section(claims: Vec<ClaimSeen>) -> Result<impl View> {
    let empty = claims.is_empty();
    Ok(view! {
        <section class=(SECTION)>
            <h2 class=(SECTION_TITLE)>"Harness claims"</h2>
            if empty {
                <p class="text-sm text-zinc-500">"No harness claimed anything on this agent's exchanges."</p>
            } else {
                <ul class="space-y-1 text-sm">
                    for seen in claims {
                        <li class="flex items-center gap-2">
                            claim_badge(claim: &seen.claim)
                            <span class="text-xs text-zinc-500">"last seen " (format_time(seen.last_seen))</span>
                        </li>
                    }
                </ul>
                <p class="mt-1 text-xs text-zinc-500">"Claims come from client headers; some harnesses send another's. They are not identity."</p>
            }
        </section>
    })
}

#[component]
pub async fn tree_section(tree: Tree) -> Result<impl View> {
    let empty = tree.rows.is_empty();
    Ok(view! {
        <section class=(SECTION)>
            <h2 class=(SECTION_TITLE)>"Sub-agents"</h2>
            if empty {
                <p class="text-sm text-zinc-500">"No sub-agents."</p>
            } else {
                <ul class="text-sm">
                    for row in tree.rows {
                        <li
                            class="flex items-center gap-2 border-l border-zinc-200 py-0.5 dark:border-zinc-800"
                            style=(format!("padding-left: {}rem", row.depth))
                        >
                            <a class=(LINK) href=(row.url)>(row.name)</a>
                            if let Some(state) = row.state {
                                kind_badge(value: state)
                            }
                        </li>
                    }
                </ul>
                if tree.truncated {
                    <p class="mt-1 text-xs text-zinc-500">"More sub-agents exist than are shown."</p>
                }
            }
        </section>
    })
}

#[component]
pub async fn aliases_section(rows: Vec<AliasRow>) -> Result<impl View> {
    let empty = rows.is_empty();
    Ok(view! {
        <section class=(SECTION)>
            <h2 class=(SECTION_TITLE)>"Aliases"</h2>
            if empty {
                <p class="text-sm text-zinc-500">"No agent is merged into this one."</p>
            } else {
                data_table(
                    headers: &["Agent", "Evidence", "Merged"],
                    for row in rows {
                        <tr class=(ROW)>
                            <td class=(TD)><a class=(format!("{LINK} font-mono text-xs")) href=(row.link.0)>(row.link.1)</a></td>
                            <td class=(TD_NUM)>(row.evidence)</td>
                            <td class=(TD_MUTED)>(row.merged)</td>
                        </tr>
                    }
                )
            }
        </section>
    })
}

/// The merge history. `unmerge_action` is the form target when the caller
/// may revert merges.
#[component]
pub async fn merges_section(
    rows: Vec<MergeRow>,
    unmerge_action: Option<String>,
    error: Option<QueryError>,
) -> Result<impl View> {
    let empty = rows.is_empty();
    Ok(view! {
        <section class=(SECTION)>
            <h2 class=(SECTION_TITLE)>"Merge history"</h2>
            if let Some(error) = error {
                <div class="mb-2">error_panel(error: &error)</div>
            }
            if empty {
                empty_state(message: "No merges involve this agent.")
            } else {
                data_table(
                    headers: &["When", "Merged", "Into", "By", "Repointed", "State"],
                    for row in rows {
                        <tr class=(ROW)>
                            <td class=(TD_MUTED)>(row.at)</td>
                            <td class=(TD)><a class=(format!("{LINK} font-mono text-xs")) href=(row.from.0)>(row.from.1)</a></td>
                            <td class=(TD)><a class=(format!("{LINK} font-mono text-xs")) href=(row.into.0)>(row.into.1)</a></td>
                            <td class=(TD)>(row.by)</td>
                            <td class=(TD_NUM)>(row.repointed)</td>
                            <td class=(TD)>
                                match row.reverted {
                                    Some(text) => <span class="text-xs text-zinc-500">(text)</span>,
                                    None => {
                                        <span class="text-xs">"in force"</span>
                                        if let Some(action) = unmerge_action.clone() {
                                            <form method="post" action=(action) class="ml-2 inline">
                                                <input type="hidden" name="action" value="unmerge">
                                                <input type="hidden" name="merge" value=(row.id)>
                                                <button type="submit" class=(SMALL_BUTTON)>"Unmerge"</button>
                                            </form>
                                        }
                                    },
                                }
                            </td>
                        </tr>
                    }
                )
            }
        </section>
    })
}

#[component]
pub async fn vetoes_section(rows: Vec<VetoRow>) -> Result<impl View> {
    let empty = rows.is_empty();
    Ok(view! {
        <section class=(SECTION)>
            <h2 class=(SECTION_TITLE)>"Merge vetoes"</h2>
            if empty {
                <p class="text-sm text-zinc-500">"No vetoes. The resolver may merge this agent when strong evidence matches."</p>
            } else {
                data_table(
                    headers: &["Pair", "By", "When"],
                    for row in rows {
                        <tr class=(ROW)>
                            <td class=(TD)>
                                <a class=(format!("{LINK} font-mono text-xs")) href=(row.a.0)>(row.a.1)</a>
                                " · "
                                <a class=(format!("{LINK} font-mono text-xs")) href=(row.b.0)>(row.b.1)</a>
                            </td>
                            <td class=(TD)>(row.by)</td>
                            <td class=(TD_MUTED)>(row.at)</td>
                        </tr>
                    }
                )
            }
        </section>
    })
}

#[cfg(test)]
mod tests {
    use crosstalk_spec::ids::{AgentId, OperatorId};
    use crosstalk_spec::observed::agent::MergeAuthor;
    use crosstalk_spec::support::Timestamp;

    use super::*;
    use crate::components::href::tests::state;
    use crate::contract::MergeId;

    #[test]
    fn merge_rows_show_reverts() {
        let operators = OperatorNames::new([(OperatorId::from_ulid(4), "ada".to_owned())]);
        let record = MergeRecord {
            id: MergeId::from_ulid(1),
            from: AgentId::from_ulid(2),
            into: AgentId::from_ulid(3),
            by: MergeAuthor::Resolver,
            at: Timestamp::from_micros(0),
            repointed: vec![AgentId::from_ulid(5)],
            reverted: Some((
                OperatorId::from_ulid(4),
                Timestamp::from_micros(1_790_985_600_000_000),
            )),
        };
        let rows = merge_rows(&[record], &operators, &state());
        assert_eq!(rows[0].by, "identity resolver");
        assert_eq!(rows[0].repointed, 1);
        assert_eq!(
            rows[0].reverted.as_deref(),
            Some("reverted by ada at 2026-10-03 00:00:00 UTC")
        );
        assert_eq!(rows[0].from.1, "…000002");
    }

    #[test]
    fn states_read_with_their_time() {
        let text = state_text(
            &AgentState::Established {
                since: Timestamp::from_micros(1_790_985_600_000_000),
            },
            &OperatorNames::default(),
        );
        assert_eq!(text, "established since 2026-10-03 00:00:00 UTC");
    }
}

//! Sections of the agent page: evidence, claims, sub-agents, aliases, merge
//! history and vetoes.

use topcoat::Result;
use topcoat::view::{View, component, view};

use super::evidence::EvidenceRow;
use super::tree::Tree;
use crate::components::form::{LINK, SECTION, SECTION_TITLE, SMALL_BUTTON};
use crate::components::table::{ROW, TD, TD_MUTED, TD_NUM};
use crate::components::{
    claim_badge, data_table, empty_state, error_panel, format_time, kind_badge, short_id,
};
use crate::error::UiError;
use crate::pages::common::links::agent_url;
use crate::pages::common::lookup::OperatorNames;
use crate::url::ulid::UlidId;
use crate::url::view_state::ViewState;
use crosstalk_spec::observed::agent::{
    ActiveAgentState, Agent, AgentState, MergeRecord, MergeVeto, SeenClaim,
};

/// An agent link: URL and short id.
pub type AgentLink = (String, String);

fn link(id: crosstalk_spec::ids::AgentId, state: &ViewState) -> AgentLink {
    (agent_url(id, state), short_id(id.to_ulid()))
}

/// An active state with its time, in words.
pub fn active_text(state: ActiveAgentState) -> String {
    match state {
        ActiveAgentState::Registered { at } => {
            format!("registered in config at {}", format_time(at))
        }
        ActiveAgentState::Provisional { first_seen } => {
            format!(
                "first seen {}; not yet corroborated",
                format_time(first_seen)
            )
        }
        ActiveAgentState::Established { since } => {
            format!("established since {}", format_time(since))
        }
    }
}

/// The state of an agent with its time, in words. A merged agent says
/// where it resolves, which merge put it there (from `merges`, when listed)
/// and what it was before that merge.
pub fn state_text(state: &AgentState, merges: &[MergeRecord], operators: &OperatorNames) -> String {
    match state.active() {
        Ok(active) => active_text(active),
        Err(merged) => {
            let record = merges.iter().find(|m| m.id() == merged.merge);
            let how = record.map_or_else(String::new, |m| {
                format!(
                    " at {} by {}",
                    format_time(m.at()),
                    operators.merge_author(m.by())
                )
            });
            let moved = match merged.repointed_by.len() {
                0 => String::new(),
                1 => " (repointed by a later merge)".to_owned(),
                n => format!(" (repointed by {n} later merges)"),
            };
            format!(
                "merged into {}{how}{moved}; before: {}",
                short_id(merged.into.to_ulid()),
                active_text(merged.prior)
            )
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AliasRow {
    pub link: AgentLink,
    pub evidence: u32,
    pub merged: String,
}

/// One row per alias, with the state it had before its merge.
pub fn alias_rows(
    aliases: &[Agent],
    merges: &[MergeRecord],
    operators: &OperatorNames,
    state: &ViewState,
) -> Vec<AliasRow> {
    aliases
        .iter()
        .map(|a| AliasRow {
            link: link(a.id, state),
            evidence: a.evidence.count().get(),
            merged: state_text(&a.state, merges, operators),
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
    /// Who reverted it and when, and what the revert pointed back.
    pub reverted: Option<String>,
    /// For a merge in force: what an unmerge restores, in words.
    pub restores: Option<String>,
}

fn agents_text(n: usize) -> String {
    match n {
        1 => "1 agent".to_owned(),
        n => format!("{n} agents"),
    }
}

/// What an unmerge of `merge` would restore: the prior state its source
/// keeps, and the agents it repointed that would point at the source again
/// (`Agent::restore`: those whose `repointed_by` still holds it), when
/// those agents are among `agents`.
fn restores(merge: &MergeRecord, agents: &[Agent]) -> Option<String> {
    if merge.reverted().is_some() {
        return None;
    }
    let merged = |a: &Agent| match &a.state {
        AgentState::Merged(merged) => Some((a.id, merged.clone())),
        AgentState::Registered { .. }
        | AgentState::Provisional { .. }
        | AgentState::Established { .. } => None,
    };
    let mut prior = None;
    let mut back = 0;
    for (id, state) in agents.iter().filter_map(merged) {
        if id == merge.source() && state.merge == merge.id() {
            prior = Some(state.prior);
        } else if state.repointed_by.contains(&merge.id()) {
            back += 1;
        }
    }
    let prior = active_text(prior?);
    Some(match back {
        0 => prior,
        n => format!("{prior}; {} pointed back at it", agents_text(n)),
    })
}

/// Merge history rows. `agents` are the agents whose merged states are
/// known (the page's aliases).
pub fn merge_rows(
    merges: &[MergeRecord],
    agents: &[Agent],
    operators: &OperatorNames,
    state: &ViewState,
) -> Vec<MergeRow> {
    merges
        .iter()
        .map(|m| MergeRow {
            id: m.id().to_ulid(),
            at: format_time(m.at()),
            from: link(m.source(), state),
            into: link(m.target(), state),
            by: operators.merge_author(m.by()),
            repointed: m.repointed().len(),
            reverted: m.reverted().map(|reversal| {
                let back = match reversal.restored.len() {
                    0 => String::new(),
                    n => format!("; {} pointed back", agents_text(n)),
                };
                format!(
                    "reverted by {} at {}{back}",
                    operators.name(reversal.by),
                    format_time(reversal.at)
                )
            }),
            restores: restores(m, agents),
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
            a: link(v.a(), state),
            b: link(v.b(), state),
            by: operators.name(v.by()),
            at: format_time(v.at()),
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
pub async fn claims_section(claims: Vec<SeenClaim>) -> Result<impl View> {
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
    error: Option<UiError>,
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
                                        if let Some(restores) = row.restores.clone() {
                                            <div class="text-xs text-zinc-500">"unmerging restores: " (restores)</div>
                                        }
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
    use crosstalk_spec::ids::{AgentId, MergeId, OperatorId};
    use crosstalk_spec::observed::agent::{MergeAuthor, MergeRequest, MergedInto, Reversal};
    use crosstalk_spec::support::{NonEmpty, Timestamp};

    use super::*;
    use crate::components::href::tests::state;
    use crate::pages::agents::evidence::tests::credential;

    const NOON: Timestamp = Timestamp::from_micros(1_790_985_600_000_000);

    fn record(from: u128, into: u128, by: MergeAuthor, repointed: &[u128]) -> MergeRecord {
        let request = MergeRequest::new(AgentId::from_ulid(from), AgentId::from_ulid(into), by)
            .expect("request");
        MergeRecord::new(
            MergeId::from_ulid(from * 10 + into),
            request,
            NOON,
            repointed.iter().map(|r| AgentId::from_ulid(*r)).collect(),
        )
    }

    fn alias(id: u128, merged: MergedInto) -> Agent {
        Agent {
            id: AgentId::from_ulid(id),
            evidence: NonEmpty::new(credential(1)),
            parent: None,
            state: AgentState::Merged(merged),
            label: None,
        }
    }

    #[test]
    fn merge_rows_show_reverts() {
        let operators = OperatorNames::new([(OperatorId::from_ulid(4), "ada".to_owned())]);
        let mut merge = record(2, 3, MergeAuthor::Resolver, &[5]);
        merge
            .revert(Reversal {
                by: OperatorId::from_ulid(4),
                at: NOON,
                restored: vec![AgentId::from_ulid(5)],
            })
            .expect("revert");
        let rows = merge_rows(&[merge], &[], &operators, &state());
        assert_eq!(rows[0].by, "identity resolver");
        assert_eq!(rows[0].restores, None, "a reverted merge restores nothing");
        assert_eq!(rows[0].repointed, 1);
        assert_eq!(
            rows[0].reverted.as_deref(),
            Some("reverted by ada at 2026-10-03 00:00:00 UTC; 1 agent pointed back")
        );
        assert_eq!(rows[0].from.1, "…000002");
    }

    #[test]
    fn states_read_with_their_time() {
        let text = state_text(
            &AgentState::Established { since: NOON },
            &[],
            &OperatorNames::default(),
        );
        assert_eq!(text, "established since 2026-10-03 00:00:00 UTC");
        let merge = record(2, 3, MergeAuthor::Resolver, &[]);
        let merged = AgentState::Merged(MergedInto {
            merge: merge.id(),
            into: AgentId::from_ulid(3),
            prior: ActiveAgentState::Provisional { first_seen: NOON },
            repointed_by: Vec::new(),
        });
        assert_eq!(
            state_text(&merged, &[merge], &OperatorNames::default()),
            "merged into …000003 at 2026-10-03 00:00:00 UTC by identity resolver; \
             before: first seen 2026-10-03 00:00:00 UTC; not yet corroborated"
        );
        assert_eq!(
            state_text(&merged, &[], &OperatorNames::default()),
            "merged into …000003; before: first seen 2026-10-03 00:00:00 UTC; not yet corroborated",
            "without its record the merge is still told by its target"
        );
    }

    #[test]
    fn merges_in_force_say_what_an_unmerge_restores() {
        let first = record(1, 2, MergeAuthor::Resolver, &[]);
        let second = record(2, 3, MergeAuthor::Operator(OperatorId::from_ulid(4)), &[1]);
        let aliases = [
            alias(
                1,
                MergedInto {
                    merge: first.id(),
                    into: AgentId::from_ulid(3),
                    prior: ActiveAgentState::Provisional { first_seen: NOON },
                    repointed_by: vec![second.id()],
                },
            ),
            alias(
                2,
                MergedInto {
                    merge: second.id(),
                    into: AgentId::from_ulid(3),
                    prior: ActiveAgentState::Established { since: NOON },
                    repointed_by: Vec::new(),
                },
            ),
        ];
        let rows = merge_rows(
            &[first, second],
            &aliases,
            &OperatorNames::default(),
            &state(),
        );
        assert_eq!(
            rows[0].restores.as_deref(),
            Some("first seen 2026-10-03 00:00:00 UTC; not yet corroborated")
        );
        assert_eq!(
            rows[1].restores.as_deref(),
            Some("established since 2026-10-03 00:00:00 UTC; 1 agent pointed back at it")
        );
        let alias_rows = alias_rows(&aliases, &[], &OperatorNames::default(), &state());
        assert!(
            alias_rows[0]
                .merged
                .contains("(repointed by a later merge)")
        );
        assert!(
            alias_rows[1]
                .merged
                .ends_with("before: established since 2026-10-03 00:00:00 UTC")
        );
    }

    #[tokio::test]
    async fn merges_offer_unmerge_only_when_in_force_and_allowed() {
        use topcoat::view::view;

        use crate::testing::{cx, render};

        let cx = &cx();
        let row = |reverted: Option<String>| MergeRow {
            id: "01J9ZQ3W8D0000000000000009".into(),
            at: "t".into(),
            from: ("/agents/a".into(), "…a".into()),
            into: ("/agents/b".into(), "…b".into()),
            by: "ada".into(),
            repointed: 0,
            reverted,
            restores: None,
        };
        let rows = vec![row(None), row(Some("reverted by ada at t".into()))];
        let again = rows.clone();
        let html = render(
            view! { cx => merges_section(rows: rows, unmerge_action: Some("/agents/b".to_owned()), error: None) },
            cx,
        )
        .await;
        assert_eq!(html.matches(">Unmerge</button>").count(), 1);
        assert!(html.contains("value=\"01J9ZQ3W8D0000000000000009\""));
        assert!(html.contains("reverted by ada at t"));
        let html = render(
            view! { cx => merges_section(rows: again, unmerge_action: None, error: None) },
            cx,
        )
        .await;
        assert!(!html.contains("Unmerge"), "no Govern, no button");
    }
}

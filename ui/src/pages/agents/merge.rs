//! `/agents/{id}/merge?into=<id>`: compare two agents' identity evidence
//! side by side, flag what they share and what sets them apart, then merge
//! the first into the second.

use std::num::NonZeroU32;

use crosstalk_spec::ids::AgentId;
use crosstalk_spec::interfaces::l8_surface::{Caller, Permission};
use crosstalk_spec::observed::agent::IdentityEvidence;
use topcoat::Result;
use topcoat::context::Cx;
use topcoat::router::content::Form;
use topcoat::router::{page, query_params};
use topcoat::view::{View, component, view};

use super::actions::merge_action;
use super::detail::{agent_id, agent_path};
use super::evidence::{EvidenceRow, conflicts, evidence_rows};
use crate::app::{backend, caller};
use crate::backend::Backend;
use crate::components::form::{BUTTON, BUTTON_PRIMARY, INPUT, LABEL, LINK, PANEL};
use crate::components::table::{ROW, TD, TD_MUTED};
use crate::components::{
    Tone, agent_name, data_table, error_panel, href, kind_badge, page_header, short_id,
    state_badge, state_inputs,
};
use crate::contract::agents::{AgentDetail, AgentStateKind};
use crate::contract::errors::QueryError;
use crate::contract::lists::PageRequest;
use crate::pages::common::action::{Failure, done, perform, require, status_of};
use crate::pages::common::flash::Flash;
use crate::pages::common::form::{FormFields, id, invalid};
use crate::pages::common::links::agent_url;
use crate::pages::view::view_state;
use crate::url::ulid::UlidId;
use crate::url::view_state::ViewState;

/// Agents offered as merge targets.
const CHOICES: NonZeroU32 = match NonZeroU32::new(100) {
    Some(n) => n,
    None => NonZeroU32::MIN,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MergeForm {
    Confirm,
}

#[query_params]
struct MergeQuery {
    into: Option<String>,
}

/// One side of the comparison.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Side {
    pub id: AgentId,
    pub url: String,
    pub name: String,
    pub state: AgentStateKind,
    pub evidence: Vec<EvidenceRow>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Comparison {
    pub from: Side,
    pub into: Side,
    pub shared: usize,
    pub conflicts: Vec<String>,
}

pub fn compare(from: &AgentDetail, into: &AgentDetail, state: &ViewState) -> Comparison {
    let from_evidence: Vec<&IdentityEvidence> = from.agent.evidence.iter().collect();
    let into_evidence: Vec<&IdentityEvidence> = into.agent.evidence.iter().collect();
    let side = |detail: &AgentDetail, other: &[&IdentityEvidence]| Side {
        id: detail.summary.id,
        url: agent_url(detail.summary.id, state),
        name: agent_name(&detail.summary),
        state: detail.summary.state,
        evidence: evidence_rows(detail.agent.evidence.iter(), other),
    };
    let from_side = side(from, &into_evidence);
    let shared = from_side.evidence.iter().filter(|r| r.shared).count();
    Comparison {
        from: from_side,
        into: side(into, &from_evidence),
        shared,
        conflicts: conflicts(&from_evidence, &into_evidence),
    }
}

async fn detail(
    cx: &Cx,
    caller: &Caller,
    id: AgentId,
) -> std::result::Result<AgentDetail, QueryError> {
    backend(cx)
        .agent(caller, id)
        .await?
        .ok_or(QueryError::NotFound)
}

enum Stage {
    /// No target chosen yet: the agents to choose from.
    Pick(Vec<(String, String)>),
    Compare(Comparison),
}

async fn load(
    cx: &Cx,
    caller: &Caller,
    id: AgentId,
    into: Option<&str>,
    state: &ViewState,
) -> std::result::Result<(Side, Stage), QueryError> {
    require(caller, Permission::View)?;
    require(caller, Permission::Govern)?;
    let from = detail(cx, caller, id).await?;
    let Some(into) = into else {
        let page = backend(cx)
            .agents(caller, &PageRequest::first(CHOICES))
            .await?;
        let choices = page
            .items
            .iter()
            .filter(|a| a.id != from.summary.id)
            .map(|a| (a.id.to_ulid(), agent_name(a)))
            .collect();
        let side = compare(&from, &from, state).from;
        return Ok((side, Stage::Pick(choices)));
    };
    let into_id = AgentId::parse_ulid(into).map_err(|e| invalid("into", e))?;
    let into = detail(cx, caller, into_id).await?;
    if into.summary.id == from.summary.id {
        return Err(invalid("into", "both ids name the same agent"));
    }
    let comparison = compare(&from, &into, state);
    Ok((comparison.from.clone(), Stage::Compare(comparison)))
}

#[page("/agents/{agent_ulid}/merge")]
async fn merge_get(cx: &Cx) -> Result<impl View> {
    let state = view_state(cx).await?;
    let id = agent_id(cx)?;
    let into = query_params::<MergeQuery>(cx)
        .ok()
        .and_then(|q| q.into.clone());
    Ok(view! { merge_page(id: id, state: state, into: into, failure: None) })
}

#[page(POST "/agents/{agent_ulid}/merge")]
async fn merge_post(cx: &Cx, Form(fields): Form<FormFields>) -> Result<impl View> {
    let state = view_state(cx).await?;
    let from = agent_id(cx)?;
    let caller = caller(cx);
    let result = match id::<AgentId>(&fields, "into") {
        Ok(into) => match merge_action(from, into, caller.operator) {
            Ok(action) => perform(cx, action).await.map(|_| into),
            Err(error) => Err(error),
        },
        Err(error) => Err(error),
    };
    let error = match result {
        Ok(into) => return Err(done(&agent_path(into), &state, &[], Flash::Merged)),
        Err(error) => error,
    };
    let into = fields.text("into").map(str::to_owned);
    let failure = Failure::new(Some(MergeForm::Confirm), error, fields);
    Ok(view! { merge_page(id: from, state: state, into: into, failure: Some(failure)) })
}

#[component]
async fn evidence_column(side: Side, role: &str) -> Result<impl View> {
    Ok(view! {
        <div class="min-w-0">
            <div class="mb-2 flex items-center gap-2">
                <span class="text-xs uppercase tracking-wide text-zinc-500">(role)</span>
                <a class=(format!("{LINK} font-medium")) href=(side.url)>(side.name)</a>
                kind_badge(value: side.state)
            </div>
            data_table(
                headers: &["Kind", "Value", "Scope", ""],
                for row in side.evidence {
                    <tr class=(if row.shared { "bg-emerald-50 dark:bg-emerald-950/40" } else { ROW })>
                        <td class=(TD)>(row.kind) " " kind_badge(value: row.strength)</td>
                        <td class=(TD)><span class="font-mono text-xs" title=(row.full)>(row.value)</span></td>
                        <td class=(TD_MUTED)>(row.scope.unwrap_or_default())</td>
                        <td class=(TD)>
                            if row.shared {
                                state_badge(label: "shared", tone: Tone::Good)
                            }
                        </td>
                    </tr>
                }
            )
        </div>
    })
}

#[component]
async fn merge_page(
    cx: &Cx,
    id: AgentId,
    state: ViewState,
    into: Option<String>,
    failure: Option<Failure<MergeForm>>,
) -> Result<impl View> {
    let caller = caller(cx);
    let loaded = load(cx, &caller, id, into.as_deref(), &state).await;
    let failed = failure.map(|f| (f.status(), f.error));
    let path = format!("{}/merge", agent_path(id));
    let action_url = href(&path, &state, &[]);
    let back_url = agent_url(id, &state);
    let pick_state = state.clone();
    let typed = into.clone().unwrap_or_default();

    Ok(view! {
        <div class="mb-1 text-xs text-zinc-500">
            <a class=(LINK) href=(back_url)>"Agent " <span class="font-mono">(short_id(id.to_ulid()))</span></a>
            " / merge"
        </div>
        page_header(
            title: "Merge agents",
            subtitle: "The first agent becomes an alias of the second. Its records keep their ids and count toward the second. Merges can be reverted.",
        )
        if let Some((status, error)) = failed {
            (status)
            <div class="mb-4">error_panel(error: &error)</div>
        }
        match loaded {
            Err(error) => {
                (status_of(&error))
                error_panel(error: &error)
                <form method="get" action=(path.clone()) class="mt-4 flex items-end gap-2">
                    state_inputs(state: &pick_state)
                    <label class="block">
                        <span class=(LABEL)>"Merge into agent id"</span>
                        <input type="text" name="into" value=(typed) class=(format!("{INPUT} w-80 font-mono")) required="">
                    </label>
                    <button type="submit" class=(BUTTON)>"Compare"</button>
                </form>
            },
            Ok((from, Stage::Pick(choices))) => {
                <div class=(PANEL)>
                    <p class="mb-2 text-sm">"Merge " <strong>(from.name)</strong> " into:"</p>
                    <form method="get" action=(path.clone()) class="flex flex-wrap items-end gap-2">
                        state_inputs(state: &pick_state)
                        <label class="block">
                            <span class=(LABEL)>"Agent id (pick or paste)"</span>
                            <input type="text" name="into" list="merge-targets" class=(format!("{INPUT} w-80 font-mono")) required="">
                        </label>
                        <datalist id="merge-targets">
                            for (ulid, name) in choices {
                                <option value=(ulid)>(name)</option>
                            }
                        </datalist>
                        <button type="submit" class=(BUTTON)>"Compare evidence"</button>
                    </form>
                </div>
            },
            Ok((_, Stage::Compare(comparison))) => {
                let into_ulid = comparison.into.id.to_ulid();
                let into_name = comparison.into.name.clone();
                let from_name = comparison.from.name.clone();
                <div class="mb-4 space-y-2 text-sm">
                    if comparison.shared == 0 {
                        <p class="rounded border border-amber-300 bg-amber-50 px-3 py-2 text-amber-900 dark:border-amber-800 dark:bg-amber-950 dark:text-amber-100">
                            "These agents share no identity evidence. Merge only if you know from elsewhere that they are the same agent."
                        </p>
                    } else {
                        <p class="text-emerald-800 dark:text-emerald-300">
                            "They share " (comparison.shared) " pieces of evidence (highlighted)."
                        </p>
                    }
                    for line in comparison.conflicts {
                        <p class="rounded border border-red-300 bg-red-50 px-3 py-2 text-red-900 dark:border-red-800 dark:bg-red-950 dark:text-red-100">
                            (line) " Harness ids name one agent each within a scope, so these are likely different agents."
                        </p>
                    }
                </div>
                <div class="mb-4 grid gap-4 lg:grid-cols-2">
                    evidence_column(side: comparison.from, role: "merged away")
                    evidence_column(side: comparison.into, role: "kept")
                </div>
                <form method="post" action=(action_url) class=(format!("{PANEL} flex items-center gap-3"))>
                    <input type="hidden" name="into" value=(into_ulid)>
                    <button type="submit" class=(BUTTON_PRIMARY)>"Merge " (from_name) " into " (into_name)</button>
                    <span class="text-xs text-zinc-500">"Recorded in the audit log; an operator merge clears any veto on this pair."</span>
                </form>
            },
        }
    })
}

#[cfg(test)]
mod tests {
    use topcoat::router::StatusCode;

    use super::*;
    use crate::components::href::tests::state;
    use crate::pages::agents::detail::tests::detail as agent_detail;
    use crate::pages::agents::evidence::tests::{credential, harness};
    use crate::testing::{get, post};

    #[test]
    fn comparison_marks_shared_evidence_and_conflicts() {
        let a = agent_detail(1);
        let mut b = agent_detail(2);
        let mut evidence = crosstalk_spec::support::NonEmpty::new(credential(1));
        evidence.push(harness("agent-2"));
        b.agent.evidence = evidence;
        let comparison = compare(&a, &b, &state());
        assert_eq!(comparison.shared, 1, "the stable credential");
        assert!(comparison.into.evidence.iter().any(|r| r.shared));
        assert_eq!(comparison.conflicts.len(), 1, "different harness agent ids");
    }

    const ID: &str = "01J9ZQ3W8D0000000000000001";
    const OTHER: &str = "01J9ZQ3W8D0000000000000002";

    #[tokio::test]
    async fn merge_page_reports_unknown_agents() {
        let reply = get(&format!(
            "/agents/{ID}/merge?{}&into={OTHER}",
            state().to_query()
        ))
        .await;
        assert_eq!(reply.status, StatusCode::NOT_FOUND);
        assert!(reply.body.contains("Merge into agent id"));
    }

    #[tokio::test]
    async fn self_merges_and_bad_ids_are_rejected() {
        let url = format!("/agents/{ID}/merge?{}", state().to_query());
        let reply = post(&url, &format!("into={ID}")).await;
        assert_eq!(reply.status, StatusCode::UNPROCESSABLE_ENTITY);
        assert!(reply.body.contains("cannot be merged into itself"));
        let reply = post(&url, "into=zzz").await;
        assert_eq!(reply.status, StatusCode::UNPROCESSABLE_ENTITY);
        let reply = post(&url, &format!("into={OTHER}")).await;
        assert_eq!(
            reply.status,
            StatusCode::NOT_FOUND,
            "the stub backend knows no agent"
        );
    }
    #[tokio::test]
    async fn merging_into_an_alias_of_the_source_is_a_conflict() {
        use crate::testing::agent_id;

        let (source, alias) = (agent_id("pi2").to_ulid(), agent_id("al3").to_ulid());
        let url = format!("/agents/{source}/merge?{}", state().to_query());
        let reply = post(&url, &format!("into={alias}")).await;
        assert_eq!(reply.status, StatusCode::CONFLICT, "{}", reply.body);
        assert!(
            reply
                .body
                .contains("the merge target resolves to the source agent")
        );
    }
}

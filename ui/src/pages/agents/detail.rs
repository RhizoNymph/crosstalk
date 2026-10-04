//! `/agents/{id}`: an agent's identity evidence, claims, sub-agents, aliases
//! and merge history. Posts rename it, clear its label or revert a merge.

use crosstalk_spec::ids::AgentId;
use crosstalk_spec::interfaces::l8_surface::{Caller, Permission};
use topcoat::Result;
use topcoat::context::Cx;
use topcoat::router::content::Form;
use topcoat::router::error::not_found;
use topcoat::router::{StatusCode, page, path_param};
use topcoat::view::{View, component, view};

use super::actions::{AgentForm, parse};
use super::evidence::{EvidenceRow, evidence_rows};
use super::sections::{
    AliasRow, MergeRow, VetoRow, alias_rows, aliases_section, claims_section, evidence_section,
    merge_rows, merges_section, state_text, tree_section, veto_rows, vetoes_section,
};
use super::tree::{self, Tree};
use crate::app::{backend, caller, can};
use crate::backend::Backend;
use crate::components::form::{BUTTON, BUTTON_PRIMARY, INPUT, LABEL, LINK, PANEL, SECTION};
use crate::components::{
    agent_name, empty_state, error_panel, flash_banner, format_time, href, kind_badge, page_header,
    short_id,
};
use crate::contract::agents::{AgentDetail, AgentLabel, AgentStateKind, ClaimSeen};
use crate::contract::errors::QueryError;
use crate::pages::common::action::{
    Failure, done, error_for, fields_for, general_error, perform, require, status_of,
};
use crate::pages::common::flash::{Flash, flash};
use crate::pages::common::form::FormFields;
use crate::pages::common::links::agent_url;
use crate::pages::common::lookup::{OperatorNames, operator_names};
use crate::pages::view::view_state;
use crate::url::ulid::UlidId;
use crate::url::view_state::ViewState;

path_param!(agent_ulid);

pub fn agent_id(cx: &Cx) -> Result<AgentId> {
    AgentId::parse_ulid(path_param::<AgentUlid>(cx)).map_err(|_| not_found().into())
}

pub fn agent_path(id: AgentId) -> String {
    format!("/agents/{}", id.to_ulid())
}

/// The page's data, display-ready.
pub struct Profile {
    pub id: AgentId,
    pub name: String,
    pub label: Option<String>,
    /// Set when the URL named an alias of this agent.
    pub alias_of: Option<String>,
    pub state: AgentStateKind,
    pub state_text: String,
    pub parent: Option<(String, String)>,
    pub transmissions_in: u64,
    pub transmissions_out: u64,
    pub last_seen: String,
    pub claims: Vec<ClaimSeen>,
    pub evidence: Vec<EvidenceRow>,
    pub aliases: Vec<AliasRow>,
    pub merges: Vec<MergeRow>,
    pub vetoes: Vec<VetoRow>,
}

pub fn profile(
    requested: AgentId,
    detail: &AgentDetail,
    operators: &OperatorNames,
    state: &ViewState,
) -> Profile {
    let summary = &detail.summary;
    Profile {
        id: summary.id,
        name: agent_name(summary),
        label: summary.label.as_ref().map(|l| l.as_str().to_owned()),
        alias_of: (requested != summary.id).then(|| short_id(requested.to_ulid())),
        state: summary.state,
        state_text: state_text(&detail.agent.state, operators),
        parent: summary
            .parent
            .map(|p| (agent_url(p, state), short_id(p.to_ulid()))),
        transmissions_in: summary.transmissions_in,
        transmissions_out: summary.transmissions_out,
        last_seen: format_time(summary.last_seen),
        claims: summary.claims.clone(),
        evidence: evidence_rows(detail.agent.evidence.iter(), &[]),
        aliases: alias_rows(&detail.aliases, operators, state),
        merges: merge_rows(&detail.merges, &detail.aliases, operators, state),
        vetoes: veto_rows(&detail.vetoes, operators, state),
    }
}

struct Loaded {
    profile: Profile,
    tree: Tree,
}

async fn load(
    cx: &Cx,
    caller: &Caller,
    id: AgentId,
    state: &ViewState,
) -> std::result::Result<Option<Loaded>, QueryError> {
    require(caller, Permission::View)?;
    let Some(detail) = backend(cx).agent(caller, id).await? else {
        return Ok(None);
    };
    let operators = operator_names(cx, caller).await;
    let tree = tree::load(cx, caller, detail.summary.id, &detail.children, state).await;
    Ok(Some(Loaded {
        profile: profile(id, &detail, &operators, state),
        tree,
    }))
}

#[page("/agents/{agent_ulid}")]
async fn agent_get(cx: &Cx) -> Result<impl View> {
    let state = view_state(cx).await?;
    let id = agent_id(cx)?;
    let flash = flash(cx);
    Ok(view! { agent_page(id: id, state: state, flash: flash, failure: None) })
}

#[page(POST "/agents/{agent_ulid}")]
async fn agent_post(cx: &Cx, Form(fields): Form<FormFields>) -> Result<impl View> {
    let state = view_state(cx).await?;
    let id = agent_id(cx)?;
    let failure = match parse(id, &fields) {
        Ok((form, action, flash)) => match perform(cx, action).await {
            Ok(_) => return Err(done(&agent_path(id), &state, &[], flash)),
            Err(error) => Failure::new(Some(form), error, fields),
        },
        Err((form, error)) => Failure::new(form, error, fields),
    };
    Ok(view! { agent_page(id: id, state: state, flash: None, failure: Some(failure)) })
}

#[component]
async fn agent_page(
    cx: &Cx,
    id: AgentId,
    state: ViewState,
    flash: Option<Flash>,
    failure: Option<Failure<AgentForm>>,
) -> Result<impl View> {
    let caller = caller(cx);
    let govern = can(&caller, Permission::Govern);
    let loaded = load(cx, &caller, id, &state).await;
    let failed_status = failure.as_ref().map(Failure::status);
    let any_error = failure.as_ref().map(|f| f.error.clone());
    let general = general_error(failure.as_ref());
    let rename_error = error_for(failure.as_ref(), AgentForm::Rename);
    let rename_fields = fields_for(failure.as_ref(), AgentForm::Rename);
    let unmerge_error = error_for(failure.as_ref(), AgentForm::Unmerge);
    let list_url = href("/agents", &state, &[]);

    Ok(view! {
        if let Some(status) = failed_status {
            (status)
        }
        <div class="mb-1 text-xs text-zinc-500">
            <a class=(LINK) href=(list_url)>"Agents"</a>
            " / "
            <span class="font-mono">(short_id(id.to_ulid()))</span>
        </div>
        match loaded {
            Err(error) => {
                (status_of(&error))
                page_header(title: "Agent", subtitle: "")
                if let Some(failed) = any_error {
                    <div class="mb-4">error_panel(error: &failed)</div>
                }
                error_panel(error: &error)
            },
            Ok(None) => {
                (StatusCode::NOT_FOUND)
                page_header(title: "Agent not found", subtitle: "")
                if let Some(failed) = any_error {
                    <div class="mb-4">error_panel(error: &failed)</div>
                }
                empty_state(message: "No agent has this id.")
            },
            Ok(Some(loaded)) => {
                let profile = loaded.profile;
                // Actions go to the canonical agent, never to an alias.
                let action_url = href(&agent_path(profile.id), &state, &[]);
                let merge_url = href(&format!("{}/merge", agent_path(profile.id)), &state, &[]);
                let label_value = rename_fields
                    .as_ref()
                    .and_then(|f| f.text("label").map(str::to_owned))
                    .or(profile.label.clone())
                    .unwrap_or_default();
                let has_label = profile.label.is_some();
                let unmerge_action = govern.then(|| action_url.clone());
                <header class="mb-4">
                    <h1 class="text-lg font-semibold">(profile.name)</h1>
                    <p class="font-mono text-xs text-zinc-500">(profile.id.to_ulid())</p>
                    <div class="mt-2 flex flex-wrap items-center gap-2 text-sm">
                        kind_badge(value: profile.state)
                        <span class="text-zinc-600 dark:text-zinc-400">(profile.state_text)</span>
                    </div>
                    <dl class="mt-2 flex flex-wrap gap-x-6 gap-y-1 text-xs text-zinc-500">
                        <div>
                            <dt class="inline">"parent "</dt>
                            <dd class="inline">
                                match profile.parent {
                                    Some((url, label)) => <a class=(format!("{LINK} font-mono")) href=(url)>(label)</a>,
                                    None => "none",
                                }
                            </dd>
                        </div>
                        <div><dt class="inline">"transmissions in "</dt><dd class="inline tabular-nums text-zinc-800 dark:text-zinc-200">(profile.transmissions_in)</dd></div>
                        <div><dt class="inline">"out "</dt><dd class="inline tabular-nums text-zinc-800 dark:text-zinc-200">(profile.transmissions_out)</dd></div>
                        <div><dt class="inline">"last seen "</dt><dd class="inline text-zinc-800 dark:text-zinc-200">(profile.last_seen)</dd></div>
                    </dl>
                </header>
                if let Some(alias) = profile.alias_of {
                    <div class="mb-4 rounded border border-sky-300 bg-sky-50 px-3 py-2 text-sm text-sky-900 dark:border-sky-800 dark:bg-sky-950 dark:text-sky-100">
                        "Agent " <span class="font-mono">(alias)</span> " is merged into this agent; its records count here."
                    </div>
                }
                if let Some(flash) = flash {
                    flash_banner(message: flash.message())
                }
                if let Some(error) = general {
                    <div class="mb-4">error_panel(error: &error)</div>
                }
                if govern {
                    <section class=(SECTION)>
                        <div class=(format!("{PANEL} flex flex-wrap items-end gap-3"))>
                            <form method="post" action=(action_url.clone()) class="flex flex-wrap items-end gap-2">
                                <input type="hidden" name="action" value="rename">
                                <label class="block">
                                    <span class=(LABEL)>"Label"</span>
                                    <input
                                        type="text"
                                        name="label"
                                        value=(label_value)
                                        maxlength=(AgentLabel::MAX_CHARS.to_string())
                                        class=(INPUT)
                                        placeholder="e.g. release planner"
                                    >
                                </label>
                                <button type="submit" class=(BUTTON_PRIMARY)>"Rename"</button>
                            </form>
                            if has_label {
                                <form method="post" action=(action_url.clone())>
                                    <input type="hidden" name="action" value="clear-label">
                                    <button type="submit" class=(BUTTON)>"Clear label"</button>
                                </form>
                            }
                            <a class=(format!("{BUTTON} ml-auto")) href=(merge_url)>"Merge into another agent…"</a>
                            if let Some(error) = rename_error {
                                <div class="w-full">error_panel(error: &error)</div>
                            }
                        </div>
                    </section>
                }
                evidence_section(rows: profile.evidence)
                claims_section(claims: profile.claims)
                tree_section(tree: loaded.tree)
                aliases_section(rows: profile.aliases)
                merges_section(rows: profile.merges, unmerge_action: unmerge_action, error: unmerge_error)
                vetoes_section(rows: profile.vetoes)
            },
        }
    })
}

#[cfg(test)]
pub(crate) mod tests {
    use crosstalk_spec::support::{NonEmpty, Timestamp};
    use topcoat::router::StatusCode;

    use super::*;
    use crate::components::href::tests::state;
    use crate::contract::agents::{Agent, AgentState};
    use crate::pages::agents::evidence::tests::{credential, harness};
    use crate::pages::agents::list::tests::summary;
    use crate::testing::{get, post};

    pub fn detail(id: u128) -> AgentDetail {
        let mut evidence = NonEmpty::new(credential(1));
        evidence.push(harness("agent-1"));
        AgentDetail {
            summary: summary(id),
            agent: Agent {
                id: AgentId::from_ulid(id),
                evidence,
                parent: None,
                state: AgentState::Established {
                    since: Timestamp::from_micros(0),
                },
            },
            aliases: Vec::new(),
            children: Vec::new(),
            merges: Vec::new(),
            vetoes: Vec::new(),
        }
    }

    #[test]
    fn profile_flags_alias_requests() {
        let detail = detail(1);
        let direct = profile(
            AgentId::from_ulid(1),
            &detail,
            &OperatorNames::default(),
            &state(),
        );
        assert_eq!(direct.alias_of, None);
        assert_eq!(direct.evidence[0].kind, "harness agent id");
        let via_alias = profile(
            AgentId::from_ulid(9),
            &detail,
            &OperatorNames::default(),
            &state(),
        );
        assert_eq!(via_alias.alias_of.as_deref(), Some("…000009"));
    }

    const ID: &str = "01J9ZQ3W8D0000000000000001";

    #[tokio::test]
    async fn unknown_agent_is_not_found() {
        let reply = get(&format!("/agents/{ID}?{}", state().to_query())).await;
        assert_eq!(reply.status, StatusCode::NOT_FOUND);
        assert!(reply.body.contains("Agent not found"));
    }

    #[tokio::test]
    async fn rename_errors_show_inline() {
        let url = format!("/agents/{ID}?{}", state().to_query());
        let long = "x".repeat(65);
        let reply = post(&url, &format!("action=rename&label={long}")).await;
        assert_eq!(reply.status, StatusCode::UNPROCESSABLE_ENTITY);
        assert!(
            reply
                .body
                .contains("label: label is longer than 64 characters")
        );
        let reply = post(&url, "action=unmerge&merge=nope").await;
        assert_eq!(reply.status, StatusCode::UNPROCESSABLE_ENTITY);
        assert!(reply.body.contains("merge: expected 26 characters"));
        let reply = post(&url, "action=clear-label").await;
        assert_eq!(
            reply.status,
            StatusCode::NOT_FOUND,
            "the stub knows no agent"
        );
    }

    #[tokio::test]
    async fn the_sub_agent_tree_lists_children_by_name() {
        use crate::testing::{operator, world};

        let c = operator().caller();
        let all = world()
            .agents(&c, &Default::default(), &crate::contract::lists::PageRequest::first(
                std::num::NonZeroU32::new(1000).expect("limit"),
            ))
            .await
            .expect("agents")
            .items;
        let child = all
            .iter()
            .find(|a| a.parent.is_some())
            .expect("a sub-agent");
        let parent = child.parent.expect("parent");
        let reply = get(&format!("/agents/{}?{}", parent.to_ulid(), state().to_query())).await;
        assert_eq!(reply.status, StatusCode::OK, "{}", reply.body);
        assert!(!reply.body.contains("No sub-agents."));
        assert!(reply.body.contains(&agent_name(child)), "{}", agent_name(child));
    }
}

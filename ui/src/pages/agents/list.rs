//! `/agents`: canonical agents with their harness claims and volume.

use crosstalk_spec::interfaces::l8_surface::Permission;
use topcoat::Result;
use topcoat::context::Cx;
use topcoat::router::page;
use topcoat::view::{View, view};

use crate::app::{backend, caller};
use crate::backend::Backend;
use crate::components::form::LINK;
use crate::components::table::{ROW, TD, TD_MUTED, TD_NUM};
use crate::components::{
    PageLinks, agent_name, claim_badge, data_table, empty_state, error_panel, format_time,
    kind_badge, page_header, pagination, short_id,
};
use crate::contract::agents::{AgentStateKind, AgentSummary, ClaimSeen};
use crate::contract::errors::QueryError;
use crate::contract::lists::Cursor;
use crate::pages::common::action::{require, status_of};
use crate::pages::common::links::agent_url;
use crate::pages::common::paging::page_request;
use crate::pages::view::view_state;
use crate::url::ulid::UlidId;
use crate::url::view_state::ViewState;

const PATH: &str = "/agents";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentRow {
    pub url: String,
    pub name: String,
    /// Set when the name is a label, so the id is shown beside it.
    pub id: Option<String>,
    pub state: AgentStateKind,
    pub claims: Vec<ClaimSeen>,
    pub parent: Option<(String, String)>,
    pub transmissions_in: u64,
    pub transmissions_out: u64,
    pub last_seen: String,
}

pub fn agent_row(agent: &AgentSummary, state: &ViewState) -> AgentRow {
    AgentRow {
        url: agent_url(agent.id, state),
        name: agent_name(agent),
        id: agent.label.as_ref().map(|_| short_id(agent.id.to_ulid())),
        state: agent.state,
        claims: agent.claims.clone(),
        parent: agent
            .parent
            .map(|p| (agent_url(p, state), short_id(p.to_ulid()))),
        transmissions_in: agent.transmissions_in,
        transmissions_out: agent.transmissions_out,
        last_seen: format_time(agent.last_seen),
    }
}

struct Listing {
    rows: Vec<AgentRow>,
    current: Option<Cursor>,
    next: Option<Cursor>,
}

async fn load(cx: &Cx, state: &ViewState) -> std::result::Result<Listing, QueryError> {
    let caller = caller(cx);
    require(&caller, Permission::View)?;
    let request = page_request(cx)?;
    let page = backend(cx).agents(&caller, &request).await?;
    Ok(Listing {
        rows: page.items.iter().map(|a| agent_row(a, state)).collect(),
        current: request.cursor,
        next: page.next,
    })
}

#[page("/agents")]
async fn agents_get(cx: &Cx) -> Result<impl View> {
    let state = view_state(cx).await?;
    let listing = load(cx, &state).await;
    let empty = listing.as_ref().is_ok_and(|l| l.rows.is_empty());
    Ok(view! {
        page_header(
            title: "Agents",
            subtitle: "Canonical agents. Harness claims are what clients said about themselves, not identity.",
        )
        match listing {
            Err(error) => {
                (status_of(&error))
                error_panel(error: &error)
            },
            Ok(_) if empty => empty_state(message: "No agents yet. Agents appear once the gateway sees their traffic or they are registered in config."),
            Ok(listing) => {
                let links = PageLinks::new(PATH, &state, &[], listing.current.as_ref(), listing.next.as_ref());
                data_table(
                    headers: &["Agent", "State", "Harness claims", "Parent", "In", "Out", "Last seen"],
                    for row in listing.rows {
                        <tr class=(ROW)>
                            <td class=(TD)>
                                <a class=(format!("{LINK} font-medium")) href=(row.url)>(row.name)</a>
                                if let Some(id) = row.id {
                                    <span class="ml-1.5 font-mono text-[11px] text-zinc-400">(id)</span>
                                }
                            </td>
                            <td class=(TD)>kind_badge(value: row.state)</td>
                            <td class=(TD)>
                                <div class="flex flex-wrap gap-1">
                                    for seen in row.claims {
                                        claim_badge(claim: &seen.claim)
                                    }
                                </div>
                            </td>
                            <td class=(TD)>
                                match row.parent {
                                    Some((url, label)) => <a class=(format!("{LINK} font-mono text-xs")) href=(url)>(label)</a>,
                                    None => <span class="text-xs text-zinc-400">"—"</span>,
                                }
                            </td>
                            <td class=(TD_NUM)>(row.transmissions_in)</td>
                            <td class=(TD_NUM)>(row.transmissions_out)</td>
                            <td class=(TD_MUTED)>(row.last_seen)</td>
                        </tr>
                    }
                )
                pagination(links: links)
            },
        }
    })
}

#[cfg(test)]
pub(crate) mod tests {
    use crosstalk_spec::ids::AgentId;
    use crosstalk_spec::observed::client::{HarnessClaim, HarnessFamily};
    use crosstalk_spec::support::Timestamp;
    use topcoat::router::StatusCode;

    use super::*;
    use crate::components::href::tests::state;
    use crate::contract::agents::AgentLabel;
    use crate::testing::get;

    pub fn summary(id: u128) -> AgentSummary {
        AgentSummary {
            id: AgentId::from_ulid(id),
            label: None,
            state: AgentStateKind::Established,
            parent: None,
            claims: vec![ClaimSeen {
                claim: HarnessClaim {
                    family: HarnessFamily::ClaudeCode,
                    version: Some("2.1".into()),
                    user_agent: "claude-cli/2.1".into(),
                },
                last_seen: Timestamp::from_micros(0),
            }],
            transmissions_in: 3,
            transmissions_out: 5,
            last_seen: Timestamp::from_micros(1_790_985_600_000_000),
        }
    }

    #[test]
    fn labelled_agents_show_their_id_too() {
        let mut agent = summary(1);
        let row = agent_row(&agent, &state());
        assert_eq!(row.name, "…000001");
        assert_eq!(row.id, None);
        agent.label = AgentLabel::new("planner").ok();
        agent.parent = Some(AgentId::from_ulid(2));
        let row = agent_row(&agent, &state());
        assert_eq!(row.name, "planner");
        assert_eq!(row.id.as_deref(), Some("…000001"));
        assert!(
            row.parent
                .is_some_and(|(url, _)| url.starts_with("/agents/0000"))
        );
    }

    #[tokio::test]
    async fn list_renders_agents_with_claims() {
        let reply = get(&format!("/agents?{}", state().to_query())).await;
        assert_eq!(reply.status, StatusCode::OK);
        assert!(reply.body.contains("pi-scraper"));
        assert!(reply.body.contains("claims"));
        let reply = get(&format!("/agents?{}&cursor=a%20b", state().to_query())).await;
        assert_eq!(reply.status, StatusCode::UNPROCESSABLE_ENTITY);
    }
}

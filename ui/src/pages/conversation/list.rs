//! `/agents/{id}/conversations`: the conversations of the canonical agent
//! an id resolves to, newest first, filtered by origin and traffic source
//! (`origin`, `replay`), a page at a time (`cursor`). An alias's URL shows
//! its canonical agent's conversations with a banner, as the agent page
//! does.

use crosstalk_spec::aggregates::agents::AgentLookup;
use crosstalk_spec::ids::AgentId;
use crosstalk_spec::interfaces::l8_surface::conversation::{
    ConversationFilter, ConversationRow, OriginLink, TrafficSource,
};
use crosstalk_spec::interfaces::l8_surface::{Permission, QueryApi};
use crosstalk_spec::paging::{ConversationList, Cursor};
use topcoat::Result;
use topcoat::context::Cx;
use topcoat::router::error::not_found;
use topcoat::router::{StatusCode, page, path_param, query_params};
use topcoat::view::{View, component, view};

use super::query::{ListQuery, ORIGINS, RawListQuery, ReplayChoice, origin_label};
use super::sections::replayed_badge;
use crate::app::{backend, caller};
use crate::components::form::{FACET, LINK};
use crate::components::nav::filter_chip;
use crate::components::paging::{PageLinks, pagination};
use crate::components::table::{ROW, TD, data_table};
use crate::components::{empty_state, error_panel, format_time, href, short_id};
use crate::error::UiError;
use crate::pages::common::action::{require, status_of};
use crate::pages::common::form::invalid;
use crate::pages::common::links::{agent_url, conversation_url};
use crate::pages::common::paging::page_request;
use crate::pages::view::view_state;
use crate::url::ulid::UlidId;
use crate::url::view_state::ViewState;

path_param!(agent_ulid);

fn agent_id(cx: &Cx) -> Result<AgentId> {
    AgentId::parse_ulid(path_param::<AgentUlid>(cx)).map_err(|_| not_found().into())
}

fn list_path(id: AgentId) -> String {
    format!("/agents/{}/conversations", id.to_ulid())
}

struct Row {
    url: String,
    short: String,
    origin: String,
    origin_url: Option<String>,
    started: String,
    last: String,
    turns: u32,
    replayed: Option<String>,
}

fn row(conversation: &ConversationRow, state: &ViewState) -> Row {
    let (origin, origin_url) = match &conversation.origin {
        OriginLink::Root => ("started here".to_owned(), None),
        OriginLink::Fork { parent, .. } => (
            format!("fork of {}", short_id(parent.to_ulid())),
            Some(conversation_url(*parent, None, state)),
        ),
        OriginLink::Compaction { predecessor, .. } => (
            format!("compaction of {}", short_id(predecessor.to_ulid())),
            Some(conversation_url(*predecessor, None, state)),
        ),
    };
    Row {
        url: conversation_url(conversation.id, None, state),
        short: short_id(conversation.id.to_ulid()),
        origin,
        origin_url,
        started: format_time(conversation.started_at),
        last: format_time(conversation.last_turn_at),
        turns: conversation.turns,
        replayed: match &conversation.source {
            TrafficSource::Live => None,
            TrafficSource::Replay { corpus } => Some(corpus.0.clone()),
        },
    }
}

struct Listing {
    name: String,
    canonical: AgentId,
    /// The id asked for, when it is an alias.
    alias: Option<AgentId>,
    rows: Vec<Row>,
    current: Option<Cursor<ConversationList>>,
    next: Option<Cursor<ConversationList>>,
}

async fn load(
    cx: &Cx,
    id: AgentId,
    query: &ListQuery,
    state: &ViewState,
) -> std::result::Result<Option<Listing>, UiError> {
    let caller = caller(cx);
    require(&caller, Permission::View)?;
    let backend = backend(cx);
    let Some(detail) = backend.agent(&caller, id, state.scope.window).await? else {
        return Ok(None);
    };
    let detail = detail.value;
    let canonical = detail.cluster.profile().id();
    let alias = match detail.cluster.lookup() {
        AgentLookup::Redirected { from } => Some(from),
        AgentLookup::Canonical => None,
    };
    let request = page_request(cx)?;
    let filter = ConversationFilter {
        agent: Some(canonical),
        origins: query.origins.clone(),
        replay: query.replay.filter(),
    };
    let page = backend.conversations(&caller, &filter, &request).await?;
    Ok(Some(Listing {
        name: crate::components::agent_name(detail.cluster.profile()),
        canonical,
        alias,
        rows: page.items().iter().map(|c| row(c, state)).collect(),
        current: request.after,
        next: page.next().cloned(),
    }))
}

fn list_href(id: AgentId, state: &ViewState, query: &ListQuery) -> String {
    let pairs = query.pairs();
    let borrowed: Vec<(&str, &str)> = pairs.iter().map(|(k, v)| (*k, v.as_str())).collect();
    href(&list_path(id), state, &borrowed)
}

#[page("/agents/{agent_ulid}/conversations")]
async fn conversations_get(cx: &Cx) -> Result<impl View> {
    let id = agent_id(cx)?;
    let state = view_state(cx).await?;
    let parsed = query_params::<RawListQuery>(cx)
        .map_err(|e| invalid("query", e))
        .and_then(ListQuery::parse);
    Ok(view! { conversations_page(id: id, state: state, parsed: parsed) })
}

#[component]
async fn conversations_page(
    cx: &Cx,
    id: AgentId,
    state: ViewState,
    parsed: std::result::Result<ListQuery, UiError>,
) -> Result<impl View> {
    let query = parsed.clone().unwrap_or_default();
    let listing = match parsed {
        Ok(query) => load(cx, id, &query, &state).await,
        Err(error) => Err(error),
    };
    let filtered = query != ListQuery::default();
    let origin_chips: Vec<_> = ORIGINS
        .iter()
        .map(|o| {
            (
                origin_label(*o),
                list_href(id, &state, &query.toggle_origin(*o)),
                query.origins.contains(o),
            )
        })
        .collect();
    let replay_chips: Vec<_> = [
        ("live and replayed", ReplayChoice::Include),
        ("live only", ReplayChoice::Exclude),
        ("replayed only", ReplayChoice::Only(None)),
    ]
    .into_iter()
    .map(|(label, choice)| {
        let active = query.replay == choice;
        (
            label,
            list_href(id, &state, &query.with_replay(choice)),
            active,
        )
    })
    .collect();
    let pairs = query.pairs();
    Ok(view! {
        <div class="mb-1 text-xs text-zinc-500">
            <a class=(LINK) href=(href("/agents", &state, &[]))>"Agents"</a>
            " / "
            <a class=(LINK) href=(agent_url(id, &state))>(short_id(id.to_ulid()))</a>
            " / conversations"
        </div>
        match listing {
            Err(error) => {
                (status_of(&error))
                <h1 class="mb-3 text-lg font-semibold">"Conversations"</h1>
                error_panel(error: &error)
            },
            Ok(None) => {
                (StatusCode::NOT_FOUND)
                <h1 class="mb-3 text-lg font-semibold">"Agent not found"</h1>
                empty_state(message: "No agent has this id. It may have been mistyped.")
            },
            Ok(Some(listing)) => {
                let empty = listing.rows.is_empty();
                <header class="mb-3">
                    <h1 class="text-lg font-semibold">"Conversations of "
                        <a class=(LINK) href=(agent_url(listing.canonical, &state))>(listing.name)</a>
                    </h1>
                    <p class="text-sm text-zinc-500">"Newest first. Each is one thread of exchanges the gateway reconstructed; forks and compactions link to the conversation they continue."</p>
                </header>
                if let Some(alias) = listing.alias {
                    <div class="mb-3 rounded border border-sky-300 bg-sky-50 px-3 py-2 text-sm text-sky-900 dark:border-sky-800 dark:bg-sky-950 dark:text-sky-100" data-alias="true">
                        "Agent " <span class="font-mono">(alias.to_ulid())</span> " is merged into this agent; its conversations are listed here."
                    </div>
                }
                <div class="mb-3 flex flex-wrap items-center gap-x-5 gap-y-2 text-xs">
                    <div class="flex flex-wrap items-center gap-1.5">
                        <span class=(FACET)>"Origin"</span>
                        for (label, link, active) in origin_chips {
                            filter_chip(label: label, href: link, active: active)
                        }
                    </div>
                    <div class="flex flex-wrap items-center gap-1.5">
                        <span class=(FACET)>"Traffic"</span>
                        for (label, link, active) in replay_chips {
                            filter_chip(label: label, href: link, active: active)
                        }
                    </div>
                </div>
                if empty && filtered {
                    empty_state(message: "No conversations match these filters.")
                } else if empty {
                    empty_state(message: "No conversations recorded for this agent. Conversations appear once the gateway threads its exchanges.")
                } else {
                    data_table(
                        headers: &["Conversation", "Origin", "Started", "Last turn", "Turns", "Traffic"],
                        for r in listing.rows {
                            <tr class=(ROW) data-conversation="true">
                                <td class=(TD)><a class=(format!("{LINK} font-mono")) href=(r.url)>(r.short)</a></td>
                                <td class=(TD)>
                                    match r.origin_url {
                                        Some(url) => <a class=(LINK) href=(url)>(r.origin)</a>,
                                        None => <span class="text-zinc-500">(r.origin)</span>,
                                    }
                                </td>
                                <td class=(TD)>(r.started)</td>
                                <td class=(TD)>(r.last)</td>
                                <td class=(format!("{TD} tabular-nums"))>(r.turns)</td>
                                <td class=(TD)>
                                    match r.replayed {
                                        Some(corpus) => replayed_badge(corpus: corpus),
                                        None => <span class="text-zinc-500">"live"</span>,
                                    }
                                </td>
                            </tr>
                        }
                    )
                    pagination(links: PageLinks::new(
                        &list_path(id),
                        &state,
                        &pairs.iter().map(|(k, v)| (*k, v.as_str())).collect::<Vec<_>>(),
                        listing.current.as_ref(),
                        listing.next.as_ref(),
                    ))
                }
            },
        }
    })
}

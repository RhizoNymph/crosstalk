//! The agent conversation view: one conversation turn by turn, with where
//! each turn's text came from and where it went
//! (`docs/features/conversation_view.md`), over the spec's conversation
//! reads (`docs/features/conversation_reads.md`, INV-1000..1029).
//!
//! - `/conversations/{id}` (this module): the head and one window of
//!   twenty turns ([`query::Window`]), with text when the caller has
//!   `Content`, and the readers of the highlighted span.
//! - `/agents/{id}/conversations` ([`list`]): an agent's conversations.
//! - `/exchanges/{id}`, `/spans/{id}` ([`locate`]): redirects to the turn
//!   an exchange is or a span sits in.
//!
//! Every page needs View. Without `Content` the turns render their
//! structure and marks, and the text read is never made.

pub mod list;
pub mod locate;
pub mod model;
pub mod query;
pub mod sections;
pub mod view;

#[cfg(test)]
mod links_tests;
#[cfg(test)]
mod pages_tests;
#[cfg(test)]
mod tests;

use crosstalk_spec::ids::{ConversationId, SpanId};
use crosstalk_spec::interfaces::l8_surface::conversation::text::TextLimit;
use crosstalk_spec::interfaces::l8_surface::conversation::turn::Reader;
use crosstalk_spec::interfaces::l8_surface::conversation::{TurnIndex, TurnWindow};
use crosstalk_spec::interfaces::l8_surface::{Caller, Permission, QueryApi};
use crosstalk_spec::paging::{Cursor, PageRequest, SpanReaderList};
use topcoat::Result;
use topcoat::context::Cx;
use topcoat::router::error::not_found;
use topcoat::router::{StatusCode, page, path_param, query_params};
use topcoat::view::{View, component, view};

use self::model::{HeadView, ReaderView, TurnView, WindowView};
use self::query::{ConversationQuery, RawConversationQuery, WINDOW, Window};
use self::sections::{head_section, turn_card, window_nav};
use self::view::{Labels, head_view, referenced, turn_view};
use crate::app::{backend, caller, can};
use crate::components::form::{LINK, SECTION, SECTION_TITLE};
use crate::components::{empty_state, error_panel, href, short_id};
use crate::error::UiError;
use crate::pages::common::action::{require, status_of};
use crate::pages::common::form::invalid;
use crate::pages::common::links::agent_conversations_url;
use crate::pages::common::lookup::agent_names;
use crate::pages::common::paging::{parse_cursor, size};
use crate::pages::common::transmissions::channel_names;
use crate::pages::view::view_state;
use crate::url::ulid::UlidId;
use crate::url::view_state::ViewState;

path_param!(conversation_ulid);

pub fn conversation_path(id: ConversationId) -> String {
    format!("/conversations/{}", id.to_ulid())
}

fn conversation_id(cx: &Cx) -> Result<ConversationId> {
    ConversationId::parse_ulid(path_param::<ConversationUlid>(cx)).map_err(|_| not_found().into())
}

/// Readers of one span per page of the reader list.
const READERS_PAGE: u16 = 20;

/// This page at `window`, keeping the highlighted span when it lies there.
fn window_url(
    id: ConversationId,
    window: Window,
    query: &ConversationQuery,
    highlight_turn: Option<u32>,
    state: &ViewState,
) -> String {
    let turn = window.from.to_string();
    let hl = match (query.highlight, highlight_turn) {
        (Some(span), Some(at)) if window.contains(at) => span.to_ulid(),
        _ => String::new(),
    };
    href(
        &conversation_path(id),
        state,
        &[("turn", &turn), ("hl", &hl)],
    )
}

struct Loaded {
    head: HeadView,
    agent_list: String,
    window: WindowView,
    turns: Vec<TurnView>,
    /// Whether the turns carry text.
    content: bool,
    readers: Option<ReaderList>,
}

/// The readers of the highlighted span, a page at a time.
struct ReaderList {
    span: String,
    readers: Vec<ReaderView>,
    first: Option<String>,
    next: Option<String>,
}

async fn load(
    cx: &Cx,
    caller: &Caller,
    id: ConversationId,
    query: &ConversationQuery,
    state: &ViewState,
) -> std::result::Result<Option<Loaded>, UiError> {
    require(caller, Permission::View)?;
    let backend = backend(cx);
    let Some(head) = backend.conversation(caller, id).await? else {
        return Ok(None);
    };
    let total = head.row.turns;
    let asked = query.window();
    let (window, past_end) = match query.turn {
        Some(turn) if total > 0 && !asked.reaches(total) => (Window::last(total), Some(turn)),
        _ => (asked, None),
    };
    let request = TurnWindow {
        from: TurnIndex(window.from),
        size: size(u16::try_from(WINDOW).unwrap_or(20)),
    };
    let page = backend
        .conversation_turns(caller, id, &request)
        .await?
        .ok_or_else(|| {
            UiError::from(crosstalk_spec::interfaces::l8_surface::QueryError::NotFound)
        })?;
    let content = can(caller, Permission::Content);
    let text = if content {
        backend
            .conversation_text(caller, id, &request, TextLimit::DEFAULT)
            .await?
    } else {
        None
    };
    // The highlighted span's turn, when it sits in this conversation.
    let highlight_turn = match query.highlight {
        Some(span) => {
            let batch = crosstalk_spec::batch::IdBatch::new([span])
                .map_err(|e| invalid("hl", format!("{e:?}")))?;
            backend
                .span_points(caller, &batch)
                .await?
                .get(&span)
                .and_then(|p| p.turn)
                .filter(|t| t.conversation == id)
                .map(|t| t.turn.0)
        }
        None => None,
    };
    let readers = match query.highlight {
        Some(span) => reader_page(cx, caller, span, query.readers.as_deref()).await?,
        None => None,
    };
    let (mut agents, channels) = referenced(&head, &page.turns);
    if let Some((_, readers)) = &readers {
        agents.extend(readers.iter().map(|r| r.agent));
    }
    let names = agent_names(cx, caller, agents).await;
    let channels = channel_names(cx, caller, channels).await;
    let readers_url = |span: SpanId| {
        format!(
            "{}#readers",
            href(
                &conversation_path(id),
                state,
                &[("turn", &window.from.to_string()), ("hl", &span.to_ulid())]
            )
        )
    };
    let labels = Labels {
        names: &names,
        channels: &channels,
        state,
        highlight: query.highlight,
        readers_url: &readers_url,
    };
    let turns = page
        .turns
        .iter()
        .map(|turn| {
            let text = text
                .as_ref()
                .and_then(|t| t.turns.iter().find(|tt| tt.index == turn.index));
            turn_view(turn, text, content, &head, &labels)
        })
        .collect();
    let shown_to = page
        .turns
        .last()
        .map_or(window.from, |t| t.index.0.saturating_add(1));
    let readers = match (query.highlight, readers) {
        (Some(span), Some((next, readers))) => Some(ReaderList {
            span: short_id(span.to_ulid()),
            readers: readers
                .iter()
                .map(|r| self::view::reader_view(r, &labels))
                .collect(),
            first: query.readers.as_ref().map(|_| readers_url(span)),
            next: next.map(|cursor| {
                format!(
                    "{}#readers",
                    href(
                        &conversation_path(id),
                        state,
                        &[
                            ("turn", &window.from.to_string()),
                            ("hl", &span.to_ulid()),
                            ("rcursor", cursor.token()),
                        ]
                    )
                )
            }),
        }),
        _ => None,
    };
    Ok(Some(Loaded {
        head: head_view(&head, &labels),
        agent_list: agent_conversations_url(head.row.agent, state),
        window: WindowView {
            from: window.from,
            to: shown_to,
            total: page.total,
            earlier: window
                .earlier()
                .map(|w| window_url(id, w, query, highlight_turn, state)),
            later: window
                .later(page.total)
                .map(|w| window_url(id, w, query, highlight_turn, state)),
            past_end,
        },
        turns,
        content,
        readers,
    }))
}

/// One page of the highlighted span's readers; `None` for a span the
/// gateway never recorded.
async fn reader_page(
    cx: &Cx,
    caller: &Caller,
    span: SpanId,
    cursor: Option<&str>,
) -> std::result::Result<Option<(Option<Cursor<SpanReaderList>>, Vec<Reader>)>, UiError> {
    let request = PageRequest {
        size: size(READERS_PAGE),
        after: parse_cursor::<SpanReaderList>(cursor)
            .map_err(|_| invalid("rcursor", "not a cursor"))?,
    };
    let page = backend(cx).span_readers(caller, span, &request).await?;
    Ok(page.map(|page| {
        let next = page.next().cloned();
        (next, page.into_parts().0)
    }))
}

#[page("/conversations/{conversation_ulid}")]
async fn conversation_get(cx: &Cx) -> Result<impl View> {
    let id = conversation_id(cx)?;
    let state = view_state(cx).await?;
    let parsed = query_params::<RawConversationQuery>(cx)
        .map_err(|e| invalid("query", e))
        .and_then(ConversationQuery::parse);
    Ok(view! { conversation_page(id: id, state: state, parsed: parsed) })
}

#[component]
async fn conversation_page(
    cx: &Cx,
    id: ConversationId,
    state: ViewState,
    parsed: std::result::Result<ConversationQuery, UiError>,
) -> Result<impl View> {
    let caller = caller(cx);
    let loaded = match &parsed {
        Ok(query) => load(cx, &caller, id, query, &state).await,
        Err(error) => Err(error.clone()),
    };
    let short = short_id(id.to_ulid());
    Ok(view! {
        <div class="mb-1 text-xs text-zinc-500">
            <a class=(LINK) href=(href("/agents", &state, &[]))>"Agents"</a>
            " / conversation "
            <span class="font-mono">(short)</span>
        </div>
        match loaded {
            Err(error) => {
                (status_of(&error))
                <h1 class="mb-3 text-lg font-semibold">"Conversation"</h1>
                error_panel(error: &error)
            },
            Ok(None) => {
                (StatusCode::NOT_FOUND)
                <h1 class="mb-3 text-lg font-semibold">"Conversation not found"</h1>
                empty_state(message: "No conversation has this id. The gateway may not have threaded it, or the id was mistyped.")
            },
            Ok(Some(loaded)) => {
                <div class="mb-2 text-xs">
                    <a class=(LINK) href=(loaded.agent_list)>"All conversations of " (loaded.head.agent.name.clone())</a>
                </div>
                head_section(head: loaded.head)
                if let Some(list) = loaded.readers {
                    reader_section(list: list)
                }
                if !loaded.content {
                    <p class="mb-2 text-xs text-zinc-500" data-content-hidden="true">
                        "Message text needs the Content permission: turns show their structure, sizes and marks."
                    </p>
                }
                window_nav(window: loaded.window.clone())
                if loaded.turns.is_empty() {
                    empty_state(message: "No turns in this window.")
                }
                for turn in loaded.turns {
                    turn_card(turn: turn)
                }
                window_nav(window: loaded.window)
            },
        }
    })
}

#[component]
async fn reader_section(list: ReaderList) -> Result<impl View> {
    let none = list.readers.is_empty();
    Ok(view! {
        <section id="readers" class=(format!("{SECTION} rounded border border-sky-200 p-3 dark:border-sky-900")) data-readers="true">
            <h2 class=(SECTION_TITLE)>"Read later by: span " <span class="font-mono">(list.span)</span></h2>
            if none {
                <p class="text-xs text-zinc-500">"No agent has read this span."</p>
            } else {
                <ul class="space-y-1 text-xs">
                    for reader in list.readers {
                        <li data-reader="true">
                            if let Some(delegation) = reader.delegation {
                                <span class="font-medium">(delegation) " "</span>
                            }
                            <a class=(LINK) href=(reader.agent.url)>(reader.agent.name)</a>
                            if let Some(url) = reader.turn {
                                " (" <a class=(LINK) href=(url)>"turn"</a> ")"
                            }
                            if let Some(link) = reader.transmission {
                                " " <a class=(LINK) href=(link.url)>(link.label) " ↗"</a>
                            }
                            <span class="text-zinc-500">" " (reader.carrier)</span>
                        </li>
                    }
                </ul>
            }
            <div class="mt-2 flex gap-3 text-xs">
                if let Some(url) = list.first {
                    <a class=(LINK) href=(url)>"« first readers"</a>
                }
                if let Some(url) = list.next {
                    <a class=(LINK) href=(url)>"more readers »"</a>
                }
            </div>
        </section>
    })
}

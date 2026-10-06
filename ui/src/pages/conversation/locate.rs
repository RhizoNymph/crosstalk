//! `/exchanges/{id}` and `/spans/{id}`: citeable links to the turn an
//! exchange is, or the turn holding a span, for pages that know only those
//! ids (the evidence page's matches). Each answers `303 See Other` to
//! `/conversations/{c}?turn={i}` (with `hl={span}` for a span), carrying
//! the view state. An exchange the gateway has not threaded, or a span it
//! never recorded, renders a page saying so instead.

use crosstalk_spec::batch::IdBatch;
use crosstalk_spec::ids::{ExchangeId, SpanId};
use crosstalk_spec::interfaces::l8_surface::{Permission, QueryApi};
use topcoat::Result;
use topcoat::context::Cx;
use topcoat::router::error::{not_found, see_other};
use topcoat::router::{StatusCode, page, path_param};
use topcoat::view::{View, component, view};

use super::conversation_path;
use crate::app::{backend, caller};
use crate::components::form::LINK;
use crate::components::{empty_state, error_panel, href, short_id};
use crate::error::UiError;
use crate::pages::common::action::{require, status_of};
use crate::pages::common::form::invalid;
use crate::pages::common::links::agent_url;
use crate::pages::view::view_state;
use crate::url::ulid::UlidId;
use crate::url::view_state::ViewState;

path_param!(exchange_ulid);
path_param!(span_ulid);

/// The turn's URL, scrolled to it.
fn turn_url(
    conversation: crosstalk_spec::ids::ConversationId,
    turn: u32,
    hl: Option<SpanId>,
    state: &ViewState,
) -> String {
    let at = turn.to_string();
    let hl = hl.map(|s| s.to_ulid()).unwrap_or_default();
    format!(
        "{}#turn-{turn}",
        href(
            &conversation_path(conversation),
            state,
            &[("turn", &at), ("hl", &hl)]
        )
    )
}

/// What a locate page shows when it does not redirect.
enum Shown {
    /// The span exists but its exchange is not threaded.
    Unthreaded {
        agent: String,
        agent_url: String,
        exchange: String,
    },
    Missing(&'static str),
    Failed(UiError),
}

#[component]
async fn located(what: &'static str, id: String, shown: Shown) -> Result<impl View> {
    Ok(view! {
        match shown {
            Shown::Unthreaded { agent, agent_url, exchange } => {
                <h1 class="mb-3 text-lg font-semibold">(what) " " <span class="font-mono">(id)</span></h1>
                <p class="text-sm" data-unthreaded="true">
                    "Written by "
                    <a class=(LINK) href=(agent_url)>(agent)</a>
                    " in exchange "
                    <span class="font-mono">(exchange)</span>
                    ", which the gateway has not threaded into a conversation yet."
                </p>
            },
            Shown::Missing(message) => {
                (StatusCode::NOT_FOUND)
                <h1 class="mb-3 text-lg font-semibold">(what) " " <span class="font-mono">(id)</span></h1>
                empty_state(message: message)
            },
            Shown::Failed(error) => {
                (status_of(&error))
                <h1 class="mb-3 text-lg font-semibold">(what)</h1>
                error_panel(error: &error)
            },
        }
    })
}

#[page("/exchanges/{exchange_ulid}")]
async fn exchange_get(cx: &Cx) -> Result<impl View> {
    let id = ExchangeId::parse_ulid(path_param::<ExchangeUlid>(cx)).map_err(|_| not_found())?;
    let state = view_state(cx).await?;
    let caller = caller(cx);
    let placed = async {
        require(&caller, Permission::View)?;
        let batch = IdBatch::new([id]).map_err(|e| invalid("exchange", format!("{e:?}")))?;
        Ok::<_, UiError>(
            backend(cx)
                .exchange_turns(&caller, &batch)
                .await?
                .remove(&id),
        )
    }
    .await;
    let shown = match placed {
        Ok(Some(placement)) => {
            return Err(see_other(turn_url(
                placement.conversation,
                placement.turn.0,
                None,
                &state,
            ))
            .into());
        }
        Ok(None) => Shown::Missing(
            "The gateway has not threaded this exchange into a conversation, or no exchange has this id.",
        ),
        Err(error) => Shown::Failed(error),
    };
    Ok(view! { located(what: "Exchange", id: short_id(id.to_ulid()), shown: shown) })
}

#[page("/spans/{span_ulid}")]
async fn span_get(cx: &Cx) -> Result<impl View> {
    let id = SpanId::parse_ulid(path_param::<SpanUlid>(cx)).map_err(|_| not_found())?;
    let state = view_state(cx).await?;
    let caller = caller(cx);
    let point = async {
        require(&caller, Permission::View)?;
        let batch = IdBatch::new([id]).map_err(|e| invalid("span", format!("{e:?}")))?;
        Ok::<_, UiError>(backend(cx).span_points(&caller, &batch).await?.remove(&id))
    }
    .await;
    let shown = match point {
        Ok(Some(point)) => match point.turn {
            Some(turn) => {
                return Err(
                    see_other(turn_url(turn.conversation, turn.turn.0, Some(id), &state)).into(),
                );
            }
            None => Shown::Unthreaded {
                agent: short_id(point.agent.to_ulid()),
                agent_url: agent_url(point.agent, &state),
                exchange: short_id(point.exchange.to_ulid()),
            },
        },
        Ok(None) => Shown::Missing(
            "The gateway has no record of this span: it was never indexed, or the id was mistyped.",
        ),
        Err(error) => Shown::Failed(error),
    };
    Ok(view! { located(what: "Span", id: short_id(id.to_ulid()), shown: shown) })
}

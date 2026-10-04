//! The channel page's review of its unconfirmed traffic: the cross-agent
//! transmissions through the channel that no content match confirms
//! (`channel_transmissions` with `Confirmation::Unconfirmed`: awaiting
//! content, suspected or discarded), each with who wrote and who read, its
//! state and verdict, a link to its evidence and, for `Triage`, buttons
//! recording a verdict (`SetVerdict`, posted to the channel page). Paged by
//! the page's own `tcursor` key, so it pages apart from the resources.

use crosstalk_spec::aggregates::filter::TopicVersionSelector;
use crosstalk_spec::derived::flow::channel::confirmation::Confirmation;
use crosstalk_spec::derived::flow::verdict::Verdict;
use crosstalk_spec::ids::{ChannelId, TransmissionId};
use crosstalk_spec::interfaces::l8_surface::Caller;
use crosstalk_spec::interfaces::l8_surface::channel_traffic::{
    ChannelTransmission, ChannelTransmissionFilter,
};
use crosstalk_spec::interfaces::l8_surface::summary::TransmissionStateKind;
use crosstalk_spec::paging::{ChannelTransmissionList, Cursor, PageRequest};
use topcoat::Result;
use topcoat::context::Cx;
use topcoat::router::query_params;
use topcoat::view::{View, component, view};

use crate::app::backend;
use crate::components::form::{BUTTON, LINK, SECTION, SECTION_TITLE};
use crate::components::table::{ROW, TD, TD_MUTED};
use crate::components::{data_table, empty_state, error_panel, format_time, href, kind_badge};
use crate::error::UiError;
use crate::pages::common::form::invalid;
use crate::pages::common::links::{agent_url, transmission_url};
use crate::pages::common::lookup::{AgentNames, agent_names};
use crate::pages::common::paging::{PAGE_SIZE, parse_cursor, size};
use crate::pages::transmission::verdict::Choice;
use crate::url::ulid::UlidId;
use crate::url::view_state::ViewState;
use crosstalk_spec::interfaces::l8_surface::QueryApi;

/// The query key this section pages by.
pub const CURSOR_KEY: &str = "tcursor";

#[query_params]
struct SuspectedQuery {
    tcursor: Option<String>,
}

/// An agent as a link.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentLink {
    pub url: String,
    pub name: String,
}

/// One unconfirmed transmission as the section lists it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SuspectedRow {
    pub id: TransmissionId,
    pub evidence_url: String,
    pub opened: String,
    pub state: TransmissionStateKind,
    /// The writers of its co-accesses, other than the reader.
    pub senders: Vec<AgentLink>,
    pub reader: AgentLink,
    pub verdict: Option<Verdict>,
    /// Whether its state takes a verdict (`AwaitingContent` does not yet).
    pub judgeable: bool,
}

impl SuspectedRow {
    pub fn of(row: &ChannelTransmission, names: &AgentNames, state: &ViewState) -> Self {
        let summary = row.summary();
        let link = |id| AgentLink {
            url: agent_url(id, state),
            name: names.name(id),
        };
        let kind = summary.state.kind();
        Self {
            id: summary.id,
            evidence_url: transmission_url(summary.id, state),
            opened: format_time(summary.opened_at),
            state: kind,
            senders: row.senders().iter().map(|id| link(*id)).collect(),
            reader: link(summary.to),
            verdict: summary.state.verdict(),
            judgeable: !matches!(
                kind,
                TransmissionStateKind::Detected | TransmissionStateKind::AwaitingContent
            ),
        }
    }
}

/// One page of the section, with its paging links.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Suspected {
    pub rows: Vec<SuspectedRow>,
    pub first: Option<String>,
    pub next: Option<String>,
}

/// The page of `channel`'s unconfirmed transmissions the request's
/// `tcursor` names, under the view's topic version, with every sender and
/// reader named in one lookup.
pub async fn load(
    cx: &Cx,
    caller: &Caller,
    channel: ChannelId,
    path: &str,
    state: &ViewState,
) -> std::result::Result<Suspected, UiError> {
    let raw = query_params::<SuspectedQuery>(cx).map_err(|e| invalid(CURSOR_KEY, e))?;
    let after: Option<Cursor<ChannelTransmissionList>> =
        parse_cursor(raw.tcursor.as_deref()).map_err(|_| invalid(CURSOR_KEY, "not a cursor"))?;
    let request = PageRequest {
        size: size(PAGE_SIZE),
        after,
    };
    let filter = ChannelTransmissionFilter {
        confirmation: Some(Confirmation::Unconfirmed),
    };
    let page = backend(cx)
        .channel_transmissions(
            caller,
            channel,
            &filter,
            TopicVersionSelector::Pinned(state.scope.topic_version),
            &request,
        )
        .await?;
    let items = page.page.items();
    let agents: Vec<_> = items
        .iter()
        .flat_map(|row| {
            row.senders()
                .iter()
                .copied()
                .chain([row.summary().to])
                .collect::<Vec<_>>()
        })
        .collect();
    let names = agent_names(cx, caller, agents).await;
    Ok(Suspected {
        rows: items
            .iter()
            .map(|row| SuspectedRow::of(row, &names, state))
            .collect(),
        first: request.after.as_ref().map(|_| href(path, state, &[])),
        next: page
            .page
            .next()
            .map(|cursor| href(path, state, &[(CURSOR_KEY, cursor.token())])),
    })
}

/// The section. `verdict_action` is where verdict buttons post; `None`
/// without `Triage`.
#[component]
pub async fn suspected_section(
    rows: std::result::Result<Suspected, UiError>,
    verdict_action: Option<String>,
) -> Result<impl View> {
    let empty = rows.as_ref().is_ok_and(|page| page.rows.is_empty());
    Ok(view! {
        <section class=(SECTION) id="suspected">
            <h2 class=(SECTION_TITLE)>"Suspected transmissions"</h2>
            <p class="mb-2 text-xs text-zinc-500">
                "Transmissions between agents through this channel with no content match: one agent wrote, another read. Open one for its co-access evidence; a verdict records whether it was real communication."
            </p>
            match rows {
                Err(error) => error_panel(error: &error),
                Ok(_) if empty => {
                    empty_state(message: "No suspected transmissions: every transmission through this channel is confirmed.")
                },
                Ok(page) => {
                    data_table(
                        headers: &["Opened", "Writer → reader", "State", "Verdict", ""],
                        for row in page.rows {
                            let id = row.id.to_ulid();
                            let forms = verdict_action.clone().filter(|_| row.judgeable);
                            <tr class=(ROW)>
                                <td class=(TD_MUTED)>
                                    <a class=(LINK) href=(row.evidence_url)>(row.opened)</a>
                                </td>
                                <td class=(TD)>
                                    for sender in row.senders {
                                        <a class=(LINK) href=(sender.url)>(sender.name)</a>
                                        " "
                                    }
                                    "→ "
                                    <a class=(LINK) href=(row.reader.url)>(row.reader.name)</a>
                                </td>
                                <td class=(TD)>kind_badge(value: row.state)</td>
                                <td class=(TD)>
                                    match row.verdict {
                                        Some(verdict) => kind_badge(value: verdict),
                                        None => <span class="text-xs text-zinc-500">"none"</span>,
                                    }
                                </td>
                                <td class=(TD)>
                                    if let Some(action) = forms {
                                        <div class="flex gap-1">
                                            for choice in [Choice::Genuine, Choice::FalseDetection] {
                                                <form method="post" action=(action.clone())>
                                                    <input type="hidden" name="action" value="set-verdict">
                                                    <input type="hidden" name="transmission" value=(id.clone())>
                                                    <input type="hidden" name="verdict" value=(choice.code())>
                                                    <button type="submit" class=(format!("{BUTTON} py-0.5 text-xs"))>(choice.label())</button>
                                                </form>
                                            }
                                        </div>
                                    }
                                </td>
                            </tr>
                        }
                    )
                    if page.first.is_some() || page.next.is_some() {
                        <nav class="mt-3 flex items-center gap-4 text-sm" aria-label="suspected transmissions pages">
                            if let Some(first) = page.first {
                                <a class=(LINK) href=(first)>"« First page"</a>
                            }
                            if let Some(next) = page.next {
                                <a class=(LINK) href=(next)>"Next page »"</a>
                            }
                        </nav>
                    }
                },
            }
        </section>
    })
}

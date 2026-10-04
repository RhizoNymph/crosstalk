//! Transmissions as rows, shared by the analysis pages (explore results and
//! hits, the evidence page's header): route in words, channel names, rows
//! from the spec's `TransmissionSummary` and a table.

use std::collections::HashMap;

use crosstalk_spec::aggregates::edge::RouteKind;
use crosstalk_spec::aggregates::filter::TopicVersionSelector;
use crosstalk_spec::derived::flow::transmission::{DelegationDirection, DirectCarrier, Route};
use crosstalk_spec::derived::flow::verdict::Verdict;
use crosstalk_spec::ids::{ChannelId, TransmissionId};
use crosstalk_spec::interfaces::l8_surface::summary::{
    TransmissionPage, TransmissionSelection, TransmissionStateKind, TransmissionSummary,
};
use crosstalk_spec::interfaces::l8_surface::{Caller, QueryError};
use crosstalk_spec::paging::{PageRequest, TransmissionList};
use topcoat::Result;
use topcoat::context::Cx;
use topcoat::view::{View, component, view};

use super::links::{agent_url, channel_url, transmission_url};
use super::lookup::{AgentNames, id_batches};
use crate::app::backend;
use crate::backend::Backend;
use crate::components::form::LINK;
use crate::components::table::{ROW, TD, TD_MUTED, TD_NUM};
use crate::components::{
    data_table, format_bytes, format_time_short, kind_badge, route_badge, short_id,
};
use crate::data::names::{channel_name, locator_name, pattern_name};
use crate::error::UiError;
use crate::url::ulid::UlidId;
use crate::url::view_state::ViewState;
use crosstalk_spec::interfaces::l8_surface::channels::ChannelRow;

/// A channel row's name: the declared pattern, else the seed resource,
/// else the id's tail. Names follow `data::names`, as the graph shows them.
pub fn summary_name(row: &ChannelRow) -> String {
    match (row.channel().origin.pattern(), row.seed()) {
        (Some(pattern), _) => pattern_name(pattern),
        (None, Some(seed)) => locator_name(&seed.locator),
        (None, None) => short_id(row.channel().id.to_ulid()),
    }
}

/// Channel display names (pattern or seed locator). Unknown channels show
/// as a short id.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ChannelNames(HashMap<ChannelId, String>);

impl ChannelNames {
    pub fn name(&self, id: ChannelId) -> String {
        self.0
            .get(&id)
            .cloned()
            .unwrap_or_else(|| format!("channel {}", short_id(id.to_ulid())))
    }

    #[cfg(test)]
    pub fn from_pairs(pairs: impl IntoIterator<Item = (ChannelId, String)>) -> Self {
        Self(pairs.into_iter().collect())
    }
}

/// Names for every distinct channel in `ids`: one `channel_names` call
/// per `IdBatch` of them (one call for any page of rows). A failed lookup
/// degrades to short ids.
pub async fn channel_names(
    cx: &Cx,
    caller: &Caller,
    ids: impl IntoIterator<Item = ChannelId>,
) -> ChannelNames {
    let mut names = HashMap::new();
    let looked_up = async {
        for batch in id_batches(ids)? {
            names.extend(backend(cx).channel_names(caller, &batch).await?);
        }
        Ok::<_, QueryError>(())
    };
    match looked_up.await {
        Ok(()) => ChannelNames(
            names
                .iter()
                .map(|(id, name)| (*id, channel_name(name)))
                .collect(),
        ),
        Err(error) => {
            tracing::warn!(error = ?error, "channel names unavailable");
            ChannelNames::default()
        }
    }
}

/// The channel a route goes through, if any.
pub fn route_channel(route: &Route) -> Option<ChannelId> {
    match route {
        Route::Channel(id) => Some(*id),
        _ => None,
    }
}

/// A route in words: the channel's name, the delegation's direction, what
/// carried a direct transmission.
pub fn route_text(route: &Route, channels: &ChannelNames) -> String {
    match route {
        Route::Channel(id) => channels.name(*id),
        Route::Delegation(DelegationDirection::ParentToChild) => "parent → sub-agent".to_owned(),
        Route::Delegation(DelegationDirection::ChildToParent) => "sub-agent → parent".to_owned(),
        Route::Direct(DirectCarrier::UserTurn) => "user turn".to_owned(),
        Route::Direct(DirectCarrier::SystemPrompt) => "system prompt".to_owned(),
        Route::Direct(DirectCarrier::ToolResult(name)) => format!("tool result of {}", name.0),
        Route::Unobserved => "unobserved input".to_owned(),
    }
}

/// A link with its text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Named {
    pub url: String,
    pub name: String,
}

/// The rows of `ids` (newest id first), topics under `version`, from one
/// `transmissions_by_id` call. Ids of no stored transmission are left out.
/// An empty or oversized selection is refused before the call, as the
/// surface's checked constructor refuses it.
pub async fn summaries_by_id(
    cx: &Cx,
    caller: &Caller,
    ids: Vec<TransmissionId>,
    version: TopicVersionSelector,
    page: &PageRequest<TransmissionList>,
) -> Result<TransmissionPage, UiError> {
    let selection = TransmissionSelection::new(ids).map_err(QueryError::from)?;
    Ok(backend(cx)
        .transmissions_by_id(caller, &selection, version, page)
        .await?)
}

/// A transmission list row, display-ready.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransmissionRow {
    pub id: TransmissionId,
    pub url: String,
    pub short: String,
    /// `None` until confirmed.
    pub from: Option<Named>,
    pub to: Named,
    pub route_kind: RouteKind,
    pub route: String,
    /// The channel page, for channel routes.
    pub route_url: Option<String>,
    pub state: TransmissionStateKind,
    pub opened: String,
    pub matched_bytes: String,
    pub verdict: Option<Verdict>,
}

impl TransmissionRow {
    pub fn new(
        summary: &TransmissionSummary,
        agents: &AgentNames,
        channels: &ChannelNames,
        state: &ViewState,
    ) -> Self {
        let named = |id| Named {
            url: agent_url(id, state),
            name: agents.name(id),
        };
        let delivery = summary.state.delivery();
        Self {
            id: summary.id,
            url: transmission_url(summary.id, state),
            short: short_id(summary.id.to_ulid()),
            from: delivery.map(|d| named(d.from)),
            to: named(summary.to),
            route_kind: RouteKind::from(&summary.route),
            route: route_text(&summary.route, channels),
            route_url: route_channel(&summary.route).map(|c| channel_url(c, state)),
            state: summary.state.kind(),
            opened: format_time_short(summary.opened_at),
            matched_bytes: delivery
                .map_or_else(|| "—".to_owned(), |d| format_bytes(d.matched_bytes.get())),
            verdict: summary.state.verdict(),
        }
    }
}

/// Rows for `summaries`, with the agent and channel names they need.
pub async fn rows(
    cx: &Cx,
    caller: &Caller,
    summaries: &[TransmissionSummary],
    state: &ViewState,
) -> Vec<TransmissionRow> {
    let agents = super::lookup::agent_names(
        cx,
        caller,
        summaries
            .iter()
            .flat_map(|s| {
                s.state
                    .delivery()
                    .map(|d| d.from)
                    .into_iter()
                    .chain(std::iter::once(s.to))
            })
            .collect::<Vec<_>>(),
    )
    .await;
    let channels = channel_names(
        cx,
        caller,
        summaries
            .iter()
            .filter_map(|s| route_channel(&s.route))
            .collect::<Vec<_>>(),
    )
    .await;
    summaries
        .iter()
        .map(|s| TransmissionRow::new(s, &agents, &channels, state))
        .collect()
}

/// Opened, sender → reader, route, state, bytes and verdict, each row
/// linking to its evidence page.
#[component]
pub async fn transmission_table(rows: Vec<TransmissionRow>) -> Result<impl View> {
    Ok(view! {
        data_table(
            headers: &["Opened", "From → to", "Route", "State", "Matched", "Verdict"],
            for row in rows {
                <tr class=(ROW)>
                    <td class=(TD_MUTED)>
                        <a class=(LINK) href=(row.url.clone())>(row.opened)</a>
                    </td>
                    <td class=(TD)>
                        <span class="whitespace-nowrap">
                            match row.from {
                                Some(from) => <a class=(LINK) href=(from.url)>(from.name)</a>,
                                None => <span class="italic text-zinc-400">"unknown"</span>,
                            }
                            <span class="px-1 text-zinc-400">"→"</span>
                            <a class=(LINK) href=(row.to.url)>(row.to.name)</a>
                        </span>
                    </td>
                    <td class=(TD)>
                        <div class="flex min-w-0 max-w-52 items-center gap-1.5">
                            route_badge(kind: row.route_kind)
                            match row.route_url {
                                Some(url) => <a class=(format!("{LINK} truncate font-mono text-xs")) href=(url)>(row.route)</a>,
                                None => <span class="truncate text-xs text-zinc-500">(row.route)</span>,
                            }
                        </div>
                    </td>
                    <td class=(TD)>kind_badge(value: row.state)</td>
                    <td class=(TD_NUM)>(row.matched_bytes)</td>
                    <td class=(TD)>
                        if let Some(verdict) = row.verdict {
                            kind_badge(value: verdict)
                        }
                    </td>
                </tr>
            }
        )
    })
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use crosstalk_spec::ids::AgentId;
    use crosstalk_spec::interfaces::l8_surface::summary::{Delivery, SummaryState, TopicUnder};
    use crosstalk_spec::observed::message::ToolName;
    use crosstalk_spec::support::Timestamp;

    use super::*;
    use crate::components::href::tests::state;

    fn summary(route: Route) -> TransmissionSummary {
        TransmissionSummary {
            id: TransmissionId::from_ulid(9),
            to: AgentId::from_ulid(2),
            route,
            opened_at: Timestamp::from_micros(1_790_985_600_000_000),
            state: SummaryState::Suspected { verdict: None },
        }
    }

    #[test]
    fn routes_read_as_words() {
        let channels = ChannelNames::from_pairs([(ChannelId::from_ulid(1), "wiki".to_owned())]);
        assert_eq!(
            route_text(&Route::Channel(ChannelId::from_ulid(1)), &channels),
            "wiki"
        );
        assert_eq!(
            route_text(&Route::Channel(ChannelId::from_ulid(2)), &channels),
            "channel …000002"
        );
        assert_eq!(
            route_text(
                &Route::Direct(DirectCarrier::ToolResult(ToolName("kv_get".into()))),
                &channels
            ),
            "tool result of kv_get"
        );
        assert_eq!(
            route_text(
                &Route::Delegation(DelegationDirection::ChildToParent),
                &channels
            ),
            "sub-agent → parent"
        );
    }

    #[test]
    fn suspected_rows_have_no_sender_and_no_bytes() {
        let names = AgentNames::from_pairs([(AgentId::from_ulid(2), "reader".to_owned())]);
        let row = TransmissionRow::new(
            &summary(Route::Channel(ChannelId::from_ulid(1))),
            &names,
            &ChannelNames::default(),
            &state(),
        );
        assert_eq!(row.from, None);
        assert_eq!(row.to.name, "reader");
        assert_eq!(row.matched_bytes, "—");
        assert!(
            row.url
                .starts_with("/transmissions/00000000000000000000000009?from=")
        );
        assert!(
            row.route_url
                .as_deref()
                .is_some_and(|u| u.starts_with("/channels/"))
        );
        assert_eq!(row.opened, "10-03 00:00");
        assert_eq!(row.state, TransmissionStateKind::Suspected);
    }

    #[test]
    fn confirmed_rows_name_the_sender_bytes_and_verdict() {
        let names = AgentNames::from_pairs([(AgentId::from_ulid(1), "writer".to_owned())]);
        let confirmed = TransmissionSummary {
            state: SummaryState::Classified {
                delivery: Delivery {
                    from: AgentId::from_ulid(1),
                    confirmed_at: Timestamp::from_micros(1_790_985_600_000_000),
                    matched_bytes: NonZeroU64::new(2048).expect("bytes"),
                },
                topic: TopicUnder::Outlier,
                verdict: Some(Verdict::FalseDetection),
            },
            ..summary(Route::Unobserved)
        };
        let row = TransmissionRow::new(&confirmed, &names, &ChannelNames::default(), &state());
        assert_eq!(row.from.map(|f| f.name), Some("writer".to_owned()));
        assert_eq!(row.matched_bytes, "2.0 KiB");
        assert_eq!(row.verdict, Some(Verdict::FalseDetection));
        assert_eq!(row.route_kind, RouteKind::Unobserved);
        assert_eq!(row.state, TransmissionStateKind::Classified);
    }
}

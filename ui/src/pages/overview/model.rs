//! What the landing page shows, loaded: the window's counts from one
//! `overview` call, the heaviest edges from `topology`, and the newest open
//! alerts.

use std::num::NonZeroU32;

use crosstalk_spec::aggregates::alert::Alert;
use crosstalk_spec::aggregates::watermark::Watermarked;
use crosstalk_spec::interfaces::l8_surface::overview::OverviewCounts;
use crosstalk_spec::interfaces::l8_surface::{AlertFilter, AlertStateKind, Caller, Permission};
use topcoat::context::Cx;

use crate::app::backend;
use crate::backend::Backend;
use crate::components::{format_bytes, format_time, href};
use crate::error::UiError;
use crate::pages::alerts::model::AlertRow;
use crate::pages::common::action::require;
use crate::pages::common::lookup::{agent_names, operator_names};
use crate::pages::common::rules::rule_names;
use crate::pages::common::transmissions::channel_names;
use crate::pages::topology::drawer::model::{EdgeItem, edge_items};
use crate::url::view_state::ViewState;

const HEAVIEST: usize = 5;
const NEWEST_ALERTS: NonZeroU32 = match NonZeroU32::new(5) {
    Some(n) => n,
    None => NonZeroU32::MIN,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Tile {
    pub label: &'static str,
    pub value: Result<String, UiError>,
    pub detail: String,
    pub href: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Overview {
    pub tiles: Vec<Tile>,
    pub watermark: Option<String>,
    pub edges: Result<Vec<(EdgeItem, String)>, UiError>,
    pub alerts: Result<Vec<AlertRow>, UiError>,
}

/// The four tiles: transmissions with their matched bytes and active
/// channels (the window's activity under the view's filter), and the open
/// alerts and unreviewed channels waiting now. A failed read shows each
/// tile as unavailable.
pub fn tiles(
    counts: &Result<Watermarked<OverviewCounts>, UiError>,
    state: &ViewState,
) -> Vec<Tile> {
    let value = |pick: fn(&OverviewCounts) -> u64| {
        counts
            .as_ref()
            .map(|c| pick(&c.value).to_string())
            .map_err(Clone::clone)
    };
    let matched = counts
        .as_ref()
        .map(|c| format_bytes(c.value.activity.matched_bytes))
        .unwrap_or_default();
    vec![
        Tile {
            label: "Transmissions",
            value: value(|c| c.activity.transmissions),
            detail: format!("confirmed in the window · {matched} matched"),
            href: href("/topology", state, &[]),
        },
        Tile {
            label: "Active channels",
            value: value(|c| c.activity.active_channels),
            detail: "carried transmissions in the window".to_owned(),
            href: href("/channels", state, &[]),
        },
        Tile {
            label: "Open alerts",
            value: value(|c| c.queues.open_alerts),
            detail: "waiting for triage".to_owned(),
            href: href("/alerts", state, &[]),
        },
        Tile {
            label: "Review queue",
            value: value(|c| c.queues.unreviewed_channels),
            detail: "unreviewed channels".to_owned(),
            href: href("/channels", state, &[("tab", "review")]),
        },
    ]
}

pub async fn load(cx: &Cx, caller: &Caller, state: &ViewState) -> Result<Overview, UiError> {
    require(caller, Permission::View)?;
    let backend = backend(cx);
    let filter = state.scope.topology_filter();
    let counts = backend
        .overview(caller, state.scope.window, &filter)
        .await
        .map_err(UiError::from);
    let watermark = counts.as_ref().ok().map(|c| format_time(c.watermark.at()));
    let tiles = tiles(&counts, state);
    let open_filter = AlertFilter {
        states: vec![AlertStateKind::Open],
        channel: None,
    };

    let edges = match backend
        .topology(caller, state.scope.window, state.weighting, &filter)
        .await
    {
        Ok(graph) => {
            let mut heaviest: Vec<_> = graph.value.edges.iter().collect();
            heaviest.sort_by(|a, b| b.share.get().total_cmp(&a.share.get()));
            heaviest.truncate(HEAVIEST);
            let agents = agent_names(
                cx,
                caller,
                heaviest
                    .iter()
                    .flat_map(|e| [e.from, e.to])
                    .collect::<Vec<_>>(),
            )
            .await;
            let channels = channel_names(
                cx,
                caller,
                heaviest
                    .iter()
                    .filter_map(|e| crate::pages::common::transmissions::route_channel(&e.route))
                    .collect::<Vec<_>>(),
            )
            .await;
            let owned: Vec<_> = heaviest.into_iter().cloned().collect();
            Ok(edge_items(&owned, |_| true, &agents, &channels)
                .into_iter()
                .take(HEAVIEST)
                .map(|item| {
                    let url = href("/topology", state, &[("sel", &item.code)]);
                    (item, url)
                })
                .collect())
        }
        Err(error) => Err(error.into()),
    };

    let alerts = match backend
        .alerts(
            caller,
            &open_filter,
            &crate::pages::common::paging::first(NEWEST_ALERTS),
        )
        .await
    {
        Ok(page) => {
            let rules = rule_names(cx, caller).await;
            let operators = operator_names(cx, caller).await;
            Ok(page
                .items()
                .iter()
                .map(|a: &Alert| AlertRow::new(a, &rules, &operators, state))
                .collect())
        }
        Err(error) => Err(error.into()),
    };
    Ok(Overview {
        tiles,
        watermark,
        edges,
        alerts,
    })
}

#[cfg(test)]
mod tests {
    use crosstalk_spec::aggregates::edge::EdgeTotals;
    use crosstalk_spec::aggregates::topic::TopicModelVersion;
    use crosstalk_spec::aggregates::watermark::Watermark;
    use crosstalk_spec::interfaces::l8_surface::QueryError;
    use crosstalk_spec::interfaces::l8_surface::overview::QueueCounts;
    use crosstalk_spec::support::Timestamp;

    use super::*;
    use crate::pages::topology::tests::fixture_state;

    #[test]
    fn tiles_show_activity_and_queues() {
        let counts = Ok(Watermarked {
            watermark: Watermark(Timestamp::from_micros(0)),
            value: OverviewCounts {
                activity: EdgeTotals {
                    topic_version: TopicModelVersion(2),
                    transmissions: 1234,
                    matched_bytes: 2048,
                    active_channels: 7,
                },
                queues: QueueCounts {
                    open_alerts: 12,
                    unreviewed_channels: 3,
                },
            },
        });
        let tiles = tiles(&counts, &fixture_state());
        let values: Vec<(&str, String)> = tiles
            .iter()
            .map(|t| (t.label, t.value.clone().expect("value")))
            .collect();
        assert_eq!(
            values,
            vec![
                ("Transmissions", "1234".to_owned()),
                ("Active channels", "7".to_owned()),
                ("Open alerts", "12".to_owned()),
                ("Review queue", "3".to_owned()),
            ]
        );
        assert_eq!(tiles[0].detail, "confirmed in the window · 2.0 KiB matched");
        assert!(tiles[3].href.contains("tab=review"));
    }

    #[test]
    fn a_failed_read_makes_every_tile_unavailable() {
        let counts = Err(UiError::from(QueryError::NotFound));
        assert!(
            tiles(&counts, &fixture_state())
                .iter()
                .all(|t| t.value.is_err())
        );
    }
}

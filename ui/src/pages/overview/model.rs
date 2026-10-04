//! What the landing page shows, loaded: counts for the window, the
//! heaviest edges and the newest open alerts.

use std::num::NonZeroU32;

use crosstalk_spec::aggregates::alert::Alert;
use crosstalk_spec::interfaces::l8_surface::{
    AlertFilter, AlertStateKind, Caller, Permission, PolicyKind,
};
use topcoat::context::Cx;

use crate::app::backend;
use crate::backend::Backend;
use crate::components::{format_bytes, format_time, href};
use crate::contract::channels::{ChannelListFilter, DetectionKind};
use crate::contract::errors::QueryError;
use crate::contract::lists::{Page, PageRequest};
use crate::pages::alerts::model::AlertRow;
use crate::pages::common::action::require;
use crate::pages::common::lookup::{agent_names, operator_names, rule_names};
use crate::pages::common::transmissions::channel_names;
use crate::pages::topology::drawer::model::{EdgeItem, edge_items};
use crate::url::view_state::ViewState;

const COUNT_PAGE: NonZeroU32 = match NonZeroU32::new(500) {
    Some(n) => n,
    None => NonZeroU32::MIN,
};
/// Counting stops after this many pages and shows "N+".
const COUNT_PAGES: usize = 20;
const HEAVIEST: usize = 5;
const NEWEST_ALERTS: NonZeroU32 = match NonZeroU32::new(5) {
    Some(n) => n,
    None => NonZeroU32::MIN,
};

/// A count that may have stopped early.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Count {
    pub value: usize,
    pub capped: bool,
}

impl Count {
    pub fn text(self) -> String {
        if self.capped {
            format!("{}+", self.value)
        } else {
            self.value.to_string()
        }
    }
}

/// Counts the items of a paged list as its pages arrive, up to
/// [`COUNT_PAGES`] pages.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Counter {
    value: usize,
    pages: usize,
}

impl Counter {
    pub fn new() -> Self {
        Self { value: 0, pages: 0 }
    }

    pub fn first() -> PageRequest {
        PageRequest::first(COUNT_PAGE)
    }

    /// Adds a page; the request for the next one, or the count when done.
    pub fn add<T>(&mut self, page: Page<T>) -> Result<PageRequest, Count> {
        self.value += page.items.len();
        self.pages += 1;
        match page.next {
            None => Err(Count {
                value: self.value,
                capped: false,
            }),
            Some(_) if self.pages >= COUNT_PAGES => Err(Count {
                value: self.value,
                capped: true,
            }),
            Some(cursor) => Ok(PageRequest {
                cursor: Some(cursor),
                limit: COUNT_PAGE,
            }),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Tile {
    pub label: &'static str,
    pub value: Result<String, QueryError>,
    pub detail: String,
    pub href: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Overview {
    pub tiles: Vec<Tile>,
    pub watermark: Option<String>,
    pub edges: Result<Vec<(EdgeItem, String)>, QueryError>,
    pub alerts: Result<Vec<AlertRow>, QueryError>,
}

async fn channel_count(
    cx: &Cx,
    caller: &Caller,
    filter: ChannelListFilter,
) -> Result<Count, QueryError> {
    let mut counter = Counter::new();
    let mut request = Counter::first();
    loop {
        let page = backend(cx).channels(caller, &filter, &request).await?;
        match counter.add(page) {
            Ok(next) => request = next,
            Err(count) => return Ok(count),
        }
    }
}

async fn open_alert_count(
    cx: &Cx,
    caller: &Caller,
    filter: &AlertFilter,
) -> Result<Count, QueryError> {
    let mut counter = Counter::new();
    let mut request = Counter::first();
    loop {
        let page = backend(cx).alerts(caller, filter, &request).await?;
        match counter.add(page) {
            Ok(next) => request = next,
            Err(count) => return Ok(count),
        }
    }
}

pub async fn load(cx: &Cx, caller: &Caller, state: &ViewState) -> Result<Overview, QueryError> {
    require(caller, Permission::View)?;
    let backend = backend(cx);
    let one = NonZeroU32::MIN;
    let timeline = backend.timeline(caller, &state.scope, one).await;
    let (transmissions, watermark) = match &timeline {
        Ok(t) => (
            Ok(t.buckets
                .iter()
                .map(|b| b.transmissions)
                .sum::<u64>()
                .to_string()),
            Some(format_time(t.watermark)),
        ),
        Err(error) => (Err(error.clone()), None),
    };
    let matched = timeline
        .as_ref()
        .map(|t| format_bytes(t.buckets.iter().map(|b| b.matched_bytes).sum()))
        .unwrap_or_default();
    let active = channel_count(
        cx,
        caller,
        ChannelListFilter {
            detections: vec![DetectionKind::Active],
            ..ChannelListFilter::default()
        },
    )
    .await;
    let review = channel_count(
        cx,
        caller,
        ChannelListFilter {
            policies: vec![PolicyKind::Unreviewed],
            ..ChannelListFilter::default()
        },
    )
    .await;
    let open_filter = AlertFilter {
        states: vec![AlertStateKind::Open],
        channel: None,
    };
    let open = open_alert_count(cx, caller, &open_filter).await;
    let tiles = vec![
        Tile {
            label: "Transmissions",
            value: transmissions,
            detail: format!("confirmed in the window · {matched} matched"),
            href: href("/topology", state, &[]),
        },
        Tile {
            label: "Active channels",
            value: active.map(Count::text),
            detail: "carrying traffic now".to_owned(),
            href: href("/channels", state, &[("detection", "active")]),
        },
        Tile {
            label: "Open alerts",
            value: open.map(Count::text),
            detail: "waiting for triage".to_owned(),
            href: href("/alerts", state, &[]),
        },
        Tile {
            label: "Review queue",
            value: review.map(Count::text),
            detail: "unreviewed channels".to_owned(),
            href: href("/channels", state, &[("tab", "review")]),
        },
    ];

    let edges = match backend
        .topology(caller, &state.scope, state.weighting)
        .await
    {
        Ok(view) => {
            let mut heaviest: Vec<_> = view.graph().edges.iter().collect();
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
        Err(error) => Err(error),
    };

    let alerts = match backend
        .alerts(caller, &open_filter, &PageRequest::first(NEWEST_ALERTS))
        .await
    {
        Ok(page) => {
            let rules = rule_names(cx, caller).await;
            let operators = operator_names(cx, caller).await;
            Ok(page
                .items
                .iter()
                .map(|a: &Alert| AlertRow::new(a, &rules, &operators, state))
                .collect())
        }
        Err(error) => Err(error),
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
    use super::*;
    use crate::contract::lists::Cursor;

    #[test]
    fn counters_follow_cursors_and_cap() {
        let page = |next: Option<&str>| Page {
            items: vec![(); 3],
            next: next.map(|c| Cursor(c.into())),
        };
        let mut counter = Counter::new();
        let next = counter.add(page(Some("2"))).expect("more");
        assert_eq!(next.cursor, Some(Cursor("2".into())));
        assert_eq!(
            counter.add(page(None)),
            Err(Count {
                value: 6,
                capped: false
            })
        );
        let mut endless = Counter::new();
        let mut last = Ok(Counter::first());
        for _ in 0..COUNT_PAGES {
            last = endless.add(page(Some("again")));
        }
        let count = last.expect_err("capped");
        assert_eq!(count.text(), format!("{}+", 3 * COUNT_PAGES));
    }
}

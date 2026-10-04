//! The topic sidebar: each topic of the view's version with its size and
//! trend over the window, and a link that starts a watched-topic rule.

use std::num::NonZeroU32;

use crosstalk_spec::ids::TopicId;
use crosstalk_spec::interfaces::l8_surface::Caller;
use topcoat::Result;
use topcoat::context::Cx;
use topcoat::view::{View, component, view};

use crate::app::backend;
use crate::backend::Backend;
use crate::components::form::LINK;
use crate::components::sparkline::sparkline;
use crate::components::{error_panel, href};
use crate::contract::topics::TopicStats;
use crate::error::UiError;
use crate::url::ulid::UlidId;
use crate::url::view_state::ViewState;

/// Trend buckets over the window.
pub const TREND_BUCKETS: NonZeroU32 = match NonZeroU32::new(24) {
    Some(n) => n,
    None => NonZeroU32::MIN,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicRow {
    /// `None` for the outliers row.
    pub id: Option<TopicId>,
    pub label: String,
    pub transmissions: u64,
    pub trend: Vec<u64>,
    /// The rule form with this topic picked.
    pub watch: Option<String>,
}

/// The rule form for a watched-topic rule on `topic`.
pub fn watch_url(topic: TopicId, state: &ViewState) -> String {
    href(
        "/alerts/rules/new",
        state,
        &[("kind", "watched"), ("topic", &topic.to_ulid())],
    )
}

/// Rows, largest first, outliers last.
pub fn topic_rows(
    stats: Vec<TopicStats>,
    labels: &[(TopicId, String)],
    state: &ViewState,
) -> Vec<TopicRow> {
    let mut rows: Vec<TopicRow> = stats
        .into_iter()
        .map(|s| TopicRow {
            id: s.topic,
            label: match s.topic {
                Some(id) => labels
                    .iter()
                    .find(|(t, _)| *t == id)
                    .map_or_else(|| "unlabelled topic".to_owned(), |(_, l)| l.clone()),
                None => "Outliers".to_owned(),
            },
            transmissions: s.transmissions,
            trend: s.trend,
            watch: s.topic.map(|id| watch_url(id, state)),
        })
        .collect();
    rows.sort_by(|a, b| {
        a.id.is_none()
            .cmp(&b.id.is_none())
            .then_with(|| b.transmissions.cmp(&a.transmissions))
            .then_with(|| a.label.cmp(&b.label))
    });
    rows
}

pub async fn load_topics(
    cx: &Cx,
    caller: &Caller,
    state: &ViewState,
) -> std::result::Result<Vec<TopicRow>, UiError> {
    let backend = backend(cx);
    let stats = backend
        .topic_stats(caller, &state.scope, TREND_BUCKETS)
        .await?;
    let labels: Vec<(TopicId, String)> = backend
        .topics(caller, state.scope.topic_version)
        .await?
        .into_iter()
        .map(|t| (t.id, t.label))
        .collect();
    Ok(topic_rows(stats, &labels, state))
}

#[component]
pub async fn topic_sidebar(
    rows: std::result::Result<Vec<TopicRow>, UiError>,
    version: u32,
    topics_url: String,
) -> Result<impl View> {
    Ok(view! {
        <div class="mb-2 flex items-baseline justify-between">
            <h2 class="text-[11px] font-semibold uppercase tracking-wide text-zinc-500">"Topics · v" (version)</h2>
            <a class=(format!("{LINK} text-xs")) href=(topics_url)>"all versions"</a>
        </div>
        match rows {
            Err(error) => error_panel(error: &error),
            Ok(rows) => {
                <ul class="divide-y divide-zinc-100 rounded border border-zinc-200 dark:divide-zinc-800 dark:border-zinc-800">
                    for row in rows {
                        <li class="px-2 py-1.5 text-xs">
                            <div class="flex items-baseline gap-2">
                                <span class=(if row.id.is_some() { "min-w-0 flex-1 truncate font-medium" } else { "min-w-0 flex-1 truncate italic text-zinc-500" }) title=(row.label.clone())>(row.label.clone())</span>
                                <span class="tabular-nums text-zinc-500">(row.transmissions)</span>
                            </div>
                            <div class="mt-0.5 flex items-center justify-between gap-2">
                                sparkline(values: row.trend, label: format!("{} over the window", row.label))
                                if let Some(url) = row.watch {
                                    <a class=(format!("{LINK} text-[11px]")) href=(url)>"Watch"</a>
                                }
                            </div>
                        </li>
                    }
                </ul>
            },
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::components::href::tests::state;

    #[test]
    fn rows_are_largest_first_with_outliers_last() {
        let stats = vec![
            TopicStats {
                topic: None,
                transmissions: 99,
                trend: vec![1],
            },
            TopicStats {
                topic: Some(TopicId::from_ulid(1)),
                transmissions: 5,
                trend: vec![5],
            },
            TopicStats {
                topic: Some(TopicId::from_ulid(2)),
                transmissions: 9,
                trend: vec![9],
            },
        ];
        let labels = vec![(TopicId::from_ulid(2), "Credentials".to_owned())];
        let rows = topic_rows(stats, &labels, &state());
        let labels: Vec<_> = rows.iter().map(|r| r.label.as_str()).collect();
        assert_eq!(labels, ["Credentials", "unlabelled topic", "Outliers"]);
        assert!(rows[2].watch.is_none());
        let watch = rows[0].watch.as_deref().expect("watch link");
        assert!(watch.starts_with("/alerts/rules/new?from="));
        assert!(watch.ends_with("&kind=watched&topic=00000000000000000000000002"));
    }
}

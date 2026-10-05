//! The topic sidebar: each topic of the view's version with its size over
//! the window (`topic_sizes`) and trend (`series` grouped by topic), and a
//! link that starts a watched-topic rule. Sizes count every assignment
//! under the version, whatever the view's filter.

use crosstalk_spec::aggregates::filter::TopicVersionSelector;
use crosstalk_spec::aggregates::topic_history::TopicSizes;
use crosstalk_spec::ids::TopicId;
use crosstalk_spec::interfaces::l8_surface::Caller;
use topcoat::Result;
use topcoat::context::Cx;
use topcoat::view::{View, component, view};

use crate::app::backend;
use crate::components::form::LINK;
use crate::components::sparkline::sparkline;
use crate::components::{error_panel, href};
use crate::error::UiError;
use crate::pages::common::topics::{Trends, all_topics, topic_trends};
use crate::pages::topics::model::size_of;
use crate::url::ulid::UlidId;
use crate::url::view_state::ViewState;
use crosstalk_spec::interfaces::l8_surface::QueryApi;

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

/// One row per topic of `labels` and one for the outliers, largest first,
/// outliers last.
pub fn topic_rows(
    sizes: &TopicSizes,
    trends: &Trends,
    labels: &[(TopicId, String)],
    state: &ViewState,
) -> Vec<TopicRow> {
    let topics = sizes.topics().iter().map(|size| Some(size.topic));
    let mut rows: Vec<TopicRow> = topics
        .chain(std::iter::once(None))
        .map(|topic| TopicRow {
            id: topic,
            label: match topic {
                Some(id) => labels
                    .iter()
                    .find(|(t, _)| *t == id)
                    .map_or_else(|| "unlabelled topic".to_owned(), |(_, l)| l.clone()),
                None => "Outliers".to_owned(),
            },
            transmissions: size_of(sizes, topic),
            trend: trends.of(topic),
            watch: topic.map(|id| watch_url(id, state)),
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
    let version = state.scope.topic_version;
    let sizes = backend
        .topic_sizes(caller, Some(version), Some(state.scope.window))
        .await?;
    let trends = topic_trends(backend, caller, state.scope.window, version).await?;
    let labels: Vec<(TopicId, String)> =
        all_topics(backend, caller, TopicVersionSelector::Pinned(version))
            .await?
            .1
            .into_iter()
            .map(|t| (t.id, t.label))
            .collect();
    Ok(topic_rows(&sizes.value, &trends, &labels, state))
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
    use std::collections::HashMap;
    use std::num::NonZeroU64;

    use crosstalk_spec::aggregates::edge::EdgeStats;
    use crosstalk_spec::aggregates::topic::TopicModelVersion;
    use crosstalk_spec::aggregates::topic_history::TopicSize;

    use super::*;
    use crate::components::href::tests::state;

    #[test]
    fn rows_are_largest_first_with_outliers_last() {
        let stats = |n: u64| {
            let n = NonZeroU64::new(n).expect("n");
            Some(EdgeStats {
                transmissions: n,
                matched_bytes: n,
            })
        };
        let sizes = TopicSizes::new(
            TopicModelVersion(2),
            None,
            vec![
                TopicSize {
                    topic: TopicId::from_ulid(1),
                    stats: stats(5),
                },
                TopicSize {
                    topic: TopicId::from_ulid(2),
                    stats: stats(9),
                },
            ],
            stats(99),
        )
        .expect("sizes");
        let trends = Trends::new(1, HashMap::from([(None, vec![99])]));
        let labels = vec![(TopicId::from_ulid(2), "Credentials".to_owned())];
        let rows = topic_rows(&sizes, &trends, &labels, &state());
        let labels: Vec<_> = rows.iter().map(|r| r.label.as_str()).collect();
        assert_eq!(labels, ["Credentials", "unlabelled topic", "Outliers"]);
        assert_eq!(rows[2].transmissions, 99);
        assert_eq!(rows[2].trend, vec![99]);
        assert_eq!(rows[0].trend, vec![0]);
        assert!(rows[2].watch.is_none());
        let watch = rows[0].watch.as_deref().expect("watch link");
        assert!(watch.starts_with("/alerts/rules/new?from="));
        assert!(watch.ends_with("&kind=watched&topic=00000000000000000000000002"));
    }
}

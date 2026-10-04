//! `/channels`: every channel with its origin, detection and policy, and the
//! review queue of unreviewed channels.

use crosstalk_spec::derived::flow::resource::{Locator, ResourcePattern};
use crosstalk_spec::interfaces::l8_surface::{Permission, PolicyKind};
use topcoat::Result;
use topcoat::context::Cx;
use topcoat::router::{page, query_params};
use topcoat::view::{View, view};

use super::model::{Shape, shape};
use super::query::{DETECTIONS, ListQuery, ORIGINS, RawListQuery, Tab};
use crate::app::{backend, caller};
use crate::backend::Backend;
use crate::components::badge::Badge;
use crate::components::form::{FACET, LINK};
use crate::components::table::{ROW, TD, TD_MUTED, TD_NUM};
use crate::components::{
    PageLinks, Tab as TabLink, Tone, data_table, empty_state, error_panel, filter_chip,
    format_time, href, kind_badge, locator_text, page_header, pagination, pattern_text, short_id,
    state_badge, tabs,
};
use crate::contract::channels::{ChannelSummary, DetectionKind, OriginKind};
use crate::contract::errors::QueryError;
use crate::contract::lists::{Cursor, Page};
use crate::pages::common::action::{require, status_of};
use crate::pages::common::form::POLICIES;
use crate::pages::common::links::channel_url;
use crate::pages::common::paging::page_request;
use crate::pages::view::view_state;
use crate::url::ulid::UlidId;
use crate::url::view_state::ViewState;

const PATH: &str = "/channels";

/// A list row with everything it shows, owned.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChannelRow {
    pub url: String,
    pub id: String,
    pub shape: OwnedShape,
    pub origin: OriginKind,
    pub detection: DetectionKind,
    pub policy: PolicyKind,
    pub superseded: bool,
    pub writers: u32,
    pub readers: u32,
    pub transmissions: u64,
    pub last_activity: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OwnedShape {
    Pattern(ResourcePattern),
    Seed(Locator),
    UnknownSeed,
}

pub fn row(summary: &ChannelSummary, state: &ViewState) -> ChannelRow {
    let channel = &summary.channel;
    ChannelRow {
        url: channel_url(channel.id, state),
        id: short_id(channel.id.to_ulid()),
        shape: match shape(summary) {
            Shape::Pattern(p) => OwnedShape::Pattern(p.clone()),
            Shape::Seed(l) => OwnedShape::Seed(l.clone()),
            Shape::UnknownSeed => OwnedShape::UnknownSeed,
        },
        origin: OriginKind::of(&channel.origin),
        detection: DetectionKind::of(&channel.origin),
        policy: crate::contract::channels::policy_kind(&channel.policy),
        superseded: summary.superseded.is_some(),
        writers: summary.writers,
        readers: summary.readers,
        transmissions: summary.transmissions,
        last_activity: summary
            .last_activity
            .map_or_else(|| "never".to_owned(), format_time),
    }
}

struct Listing {
    rows: Vec<ChannelRow>,
    next: Option<Cursor>,
    current: Option<Cursor>,
}

async fn load(
    cx: &Cx,
    query: &ListQuery,
    state: &ViewState,
) -> std::result::Result<Listing, QueryError> {
    let caller = caller(cx);
    require(&caller, Permission::View)?;
    let request = page_request(cx)?;
    let Page { items, next } = backend(cx)
        .channels(&caller, &query.effective_filter(), &request)
        .await?;
    Ok(Listing {
        rows: items.iter().map(|s| row(s, state)).collect(),
        next,
        current: request.cursor,
    })
}

/// A link to this list under another query.
fn list_href(state: &ViewState, query: &ListQuery) -> String {
    let pairs = query.pairs();
    let borrowed: Vec<(&str, &str)> = pairs.iter().map(|(k, v)| (*k, v.as_str())).collect();
    href(PATH, state, &borrowed)
}

#[page("/channels")]
async fn channels_get(cx: &Cx) -> Result<impl View> {
    let state = view_state(cx)?;
    let parsed = query_params::<RawListQuery>(cx)
        .map_err(|e| crate::pages::common::form::invalid("query", e))
        .and_then(ListQuery::parse);
    let query = parsed.clone().unwrap_or_default();
    let listing = match parsed {
        Ok(query) => load(cx, &query, &state).await,
        Err(error) => Err(error),
    };

    let tab_items = vec![
        TabLink {
            label: "All channels".to_owned(),
            href: list_href(&state, &query.with_tab(Tab::All)),
            active: query.tab == Tab::All,
        },
        TabLink {
            label: "Review queue".to_owned(),
            href: list_href(&state, &query.with_tab(Tab::Review)),
            active: query.tab == Tab::Review,
        },
    ];
    let origin_chips: Vec<_> = ORIGINS
        .iter()
        .map(|o| {
            (
                o.label(),
                list_href(&state, &query.toggle_origin(*o)),
                query.filter.origins.contains(o),
            )
        })
        .collect();
    let detection_chips: Vec<_> = DETECTIONS
        .iter()
        .map(|d| {
            (
                d.label(),
                list_href(&state, &query.toggle_detection(*d)),
                query.filter.detections.contains(d),
            )
        })
        .collect();
    let policy_chips: Vec<_> = POLICIES
        .iter()
        .map(|p| {
            (
                p.label(),
                list_href(&state, &query.toggle_policy(*p)),
                query.filter.policies.contains(p),
            )
        })
        .collect();
    let review = query.tab == Tab::Review;
    let superseded_href = list_href(&state, &query.toggle_superseded());
    let superseded_on = query.filter.include_superseded;
    let pairs = query.pairs();
    let empty = listing.as_ref().is_ok_and(|l| l.rows.is_empty());

    Ok(view! {
        page_header(
            title: "Channels",
            subtitle: "Resources agents write and read to reach each other, and the policy for each.",
        )
        tabs(items: tab_items)
        <div class="mb-3 flex flex-wrap items-center gap-x-5 gap-y-2 text-xs">
            <div class="flex flex-wrap items-center gap-1.5">
                <span class=(FACET)>"Origin"</span>
                for (label, link, active) in origin_chips {
                    filter_chip(label: label, href: link, active: active)
                }
            </div>
            <div class="flex flex-wrap items-center gap-1.5">
                <span class=(FACET)>"Detection"</span>
                for (label, link, active) in detection_chips {
                    filter_chip(label: label, href: link, active: active)
                }
            </div>
            if !review {
                <div class="flex flex-wrap items-center gap-1.5">
                    <span class=(FACET)>"Policy"</span>
                    for (label, link, active) in policy_chips {
                        filter_chip(label: label, href: link, active: active)
                    }
                </div>
                filter_chip(label: "include superseded", href: superseded_href, active: superseded_on)
            }
        </div>
        match listing {
            Err(error) => {
                (status_of(&error))
                error_panel(error: &error)
            },
            Ok(_) if empty => {
                if review {
                    empty_state(message: "Nothing to review: every channel in view has a policy decision.")
                } else {
                    empty_state(message: "No channels match these filters.")
                }
            },
            Ok(listing) => {
                let links = PageLinks::new(
                    PATH,
                    &state,
                    &pairs.iter().map(|(k, v)| (*k, v.as_str())).collect::<Vec<_>>(),
                    listing.current.as_ref(),
                    listing.next.as_ref(),
                );
                data_table(
                    headers: &["Channel", "Origin", "Detection", "Policy", "Writers", "Readers", "Transmissions", "Last activity"],
                    for row in listing.rows {
                        <tr class=(ROW)>
                            <td class=(TD)>
                                <a href=(row.url) class="flex min-w-0 max-w-md flex-col gap-0.5">
                                    match row.shape {
                                        OwnedShape::Pattern(pattern) => pattern_text(pattern: &pattern),
                                        OwnedShape::Seed(locator) => locator_text(locator: &locator),
                                        OwnedShape::UnknownSeed => <span class="text-xs italic text-zinc-500">"seed unknown"</span>,
                                    }
                                    <span class=(LINK)>
                                        <span class="font-mono text-[11px]">(row.id)</span>
                                    </span>
                                </a>
                                if row.superseded {
                                    state_badge(label: "superseded", tone: Tone::Muted)
                                }
                            </td>
                            <td class=(TD)>kind_badge(value: row.origin)</td>
                            <td class=(TD)>kind_badge(value: row.detection)</td>
                            <td class=(TD)>kind_badge(value: row.policy)</td>
                            <td class=(TD_NUM)>(row.writers)</td>
                            <td class=(TD_NUM)>(row.readers)</td>
                            <td class=(TD_NUM)>(row.transmissions)</td>
                            <td class=(TD_MUTED)>(row.last_activity)</td>
                        </tr>
                    }
                )
                pagination(links: links)
            },
        }
        <p class="mt-3 text-xs text-zinc-500">
            "Channels are listed by current state; the time window applies to resources and traffic on each channel's page."
        </p>
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::components::href::tests::state;
    use crate::pages::channels::model::tests::{discovered, wiki};
    use crate::testing::get;
    use topcoat::router::StatusCode;

    #[test]
    fn rows_show_seed_counts_and_link_with_state() {
        let row = row(&discovered(7), &state());
        assert_eq!(row.shape, OwnedShape::Seed(wiki()));
        assert_eq!(row.policy, PolicyKind::Unreviewed);
        assert_eq!(row.detection, DetectionKind::Active);
        assert_eq!((row.writers, row.readers, row.transmissions), (2, 3, 14));
        assert!(
            row.url
                .starts_with("/channels/00000000000000000000000007?from=")
        );
        assert!(!row.superseded);
    }

    #[tokio::test]
    async fn list_and_review_queue_render() {
        let state = state().to_query();
        let reply = get(&format!("/channels?{state}")).await;
        assert_eq!(reply.status, StatusCode::OK, "{}", reply.body);
        assert!(reply.body.contains("wiki.example.org"));
        assert!(reply.body.contains("Review queue"));
        let reply = get(&format!("/channels?{state}&tab=review&origin=discovered")).await;
        assert_eq!(reply.status, StatusCode::OK);
        assert!(reply.body.contains("wiki.example.org"), "the hijacked wiki awaits review");
        let reply = get(&format!("/channels?{state}&origin=discovered&detection=awaiting")).await;
        assert_eq!(reply.status, StatusCode::OK);
        assert!(reply.body.contains("No channels match these filters."));
    }

    #[tokio::test]
    async fn bad_filters_are_reported() {
        let state = state().to_query();
        let reply = get(&format!("/channels?{state}&detection=busy")).await;
        assert_eq!(reply.status, StatusCode::UNPROCESSABLE_ENTITY);
        assert!(reply.body.contains("detection: unknown value"));
    }

    #[tokio::test]
    async fn incomplete_urls_redirect_to_canonical() {
        let reply = get("/channels").await;
        assert_eq!(reply.status, StatusCode::TEMPORARY_REDIRECT);
    }
}

//! `/channels`: channels with cross-agent traffic (unconfirmed ones
//! marked), unconfirmed channels on their own, declarations with no traffic
//! yet, and the review queue, each row with its origin, detection,
//! confirmation and policy, and its writers, readers and transmissions in
//! the view's window. A resource that no transmission between two agents
//! has gone through is not a channel and is in none of them; a channel a
//! merge left without cross-agent traffic is hidden. "Confirmed only"
//! (`u=confirmed`) leaves unconfirmed channels out of every tab.

use crate::pending::channel_semantics::{ChannelRow, ChannelStanding};
use crate::pending::channel_semantics::{Confirmation, CrossTraffic, Listing};
use crosstalk_spec::aggregates::node::CanonicalOriginKind;
use crosstalk_spec::derived::flow::channel::detection::DetectionKind;
use crosstalk_spec::derived::flow::resource::{Locator, ResourcePattern};
use crosstalk_spec::interfaces::l8_surface::channels::{ChannelActivity, ChannelCounts};
use crosstalk_spec::interfaces::l8_surface::{Permission, PolicyKind};
use topcoat::Result;
use topcoat::context::Cx;
use topcoat::router::{page, query_params};
use topcoat::view::{View, view};

use super::model::{Shape, origin_kind, shape};
use super::query::{DETECTIONS, ListQuery, ORIGINS, RawListQuery, Tab};
use crate::app::{backend, caller};
use crate::components::badge::Badge;
use crate::components::form::{FACET, LINK};
use crate::components::live::live_watch;
use crate::components::table::{ROW, TD, TD_MUTED, TD_NUM};
use crate::components::{
    PageLinks, Tab as TabLink, Tone, data_table, empty_state, error_panel, filter_chip,
    format_time, href, kind_badge, locator_text, page_header, pagination, pattern_text, short_id,
    state_badge, tabs,
};
use crate::error::UiError;
use crate::pages::common::action::{require, status_of};
use crate::pages::common::form::POLICIES;
use crate::pages::common::links::channel_url;
use crate::pages::common::paging::page_request;
use crate::pages::view::view_state;
use crate::url::ulid::UlidId;
use crate::url::view_state::ViewState;
use crosstalk_spec::paging::{ChannelList, Cursor};

const PATH: &str = "/channels";

/// A list row with everything it shows, owned.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListRow {
    pub url: String,
    pub id: String,
    pub shape: OwnedShape,
    pub origin: CanonicalOriginKind,
    pub detection: DetectionKind,
    pub policy: PolicyKind,
    /// `None` for a superseded channel.
    pub listing: Option<Listing>,
    /// Cross-agent transmissions over all time; `None` when superseded.
    pub traffic: Option<CrossTraffic>,
    pub activity: Activity,
}

impl ListRow {
    /// What the traffic cell says beside the listing badge: how much
    /// suspected traffic an unconfirmed channel has.
    pub fn traffic_note(&self) -> Option<String> {
        match (self.listing, self.traffic) {
            (Some(Listing::Channel(Confirmation::Unconfirmed)), Some(traffic)) => {
                Some(format!("{} suspected, none confirmed", traffic.unconfirmed))
            }
            (Some(Listing::Channel(_) | Listing::Declaration | Listing::Hidden) | None, _) => None,
        }
    }
}

/// A row's activity as its count cells show it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Activity {
    /// Superseded: its activity is counted on the channel in force.
    Superseded,
    /// In force and never accessed nor carrying a transmission.
    Never,
    /// In force: counts in the window, last activity over all time.
    Seen { counts: ChannelCounts, last: String },
}

impl Activity {
    pub fn of(row: &ChannelRow) -> Self {
        match row.standing() {
            ChannelStanding::Superseded(_) => Self::Superseded,
            ChannelStanding::InForce {
                activity: ChannelActivity::Never,
                ..
            } => Self::Never,
            ChannelStanding::InForce {
                activity: ChannelActivity::Seen { last, counts },
                ..
            } => Self::Seen {
                counts,
                last: format_time(last),
            },
        }
    }

    /// Writers, readers and transmissions as cell text.
    pub fn cells(&self) -> [String; 3] {
        match self {
            Self::Superseded => ["—".to_owned(), "—".to_owned(), "—".to_owned()],
            Self::Never => ["0".to_owned(), "0".to_owned(), "0".to_owned()],
            Self::Seen { counts, .. } => [
                counts.writers.to_string(),
                counts.readers.to_string(),
                counts.transmissions.to_string(),
            ],
        }
    }

    pub fn last(&self) -> String {
        match self {
            Self::Superseded => "on its channel in force".to_owned(),
            Self::Never => "never".to_owned(),
            Self::Seen { last, .. } => last.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OwnedShape {
    Pattern(ResourcePattern),
    Seed(Locator),
    UnknownSeed,
}

pub fn row(channel_row: &ChannelRow, state: &ViewState) -> ListRow {
    let channel = channel_row.channel();
    ListRow {
        url: channel_url(channel.id, state),
        id: short_id(channel.id.to_ulid()),
        shape: match shape(channel_row) {
            Shape::Pattern(p) => OwnedShape::Pattern(p.clone()),
            Shape::Seed(l) => OwnedShape::Seed(l.clone()),
            Shape::UnknownSeed => OwnedShape::UnknownSeed,
        },
        origin: origin_kind(&channel.origin),
        detection: channel.origin.detection_kind(),
        policy: channel.policy.kind(),
        listing: channel_row.listing(),
        traffic: channel_row.traffic(),
        activity: Activity::of(channel_row),
    }
}

struct Page {
    rows: Vec<ListRow>,
    next: Option<Cursor<ChannelList>>,
    current: Option<Cursor<ChannelList>>,
}

/// One page of the tab's rows; `None` when "confirmed only" leaves the tab
/// nothing to list.
async fn load(
    cx: &Cx,
    query: &ListQuery,
    state: &ViewState,
) -> std::result::Result<Option<Page>, UiError> {
    let caller = caller(cx);
    require(&caller, Permission::View)?;
    let request = page_request(cx)?;
    let Some(filter) = query.filter(
        Some(state.scope.window),
        state.scope.filter.unconfirmed_channels,
    ) else {
        return Ok(None);
    };
    let (items, next) = backend(cx)
        .channels(&caller, &filter, &request)
        .await?
        .value
        .into_parts();
    Ok(Some(Page {
        rows: items.iter().map(|s| row(s, state)).collect(),
        next,
        current: request.after,
    }))
}

/// A link to this list under another query.
fn list_href(state: &ViewState, query: &ListQuery) -> String {
    let pairs = query.pairs();
    let borrowed: Vec<(&str, &str)> = pairs.iter().map(|(k, v)| (*k, v.as_str())).collect();
    href(PATH, state, &borrowed)
}

#[page("/channels")]
async fn channels_get(cx: &Cx) -> Result<impl View> {
    let state = view_state(cx).await?;
    let parsed = query_params::<RawListQuery>(cx)
        .map_err(|e| crate::pages::common::form::invalid("query", e))
        .and_then(ListQuery::parse);
    let query = parsed.clone().unwrap_or_default();
    let listing = match parsed {
        Ok(query) => load(cx, &query, &state).await,
        Err(error) => Err(error),
    };

    let tab_items: Vec<TabLink> = Tab::ALL
        .into_iter()
        .map(|tab| TabLink {
            label: tab.label().to_owned(),
            href: list_href(&state, &query.with_tab(tab)),
            active: query.tab == tab,
        })
        .collect();
    let confirmed_only = state.scope.filter.confirmed_only();
    let mut toggled = state.clone();
    toggled.scope.filter = state.scope.filter.toggle_confirmed_only();
    let confirmed_only_href = list_href(&toggled, &query);
    let origin_chips: Vec<_> = ORIGINS
        .iter()
        .map(|o| {
            (
                o.label(),
                list_href(&state, &query.toggle_origin(*o)),
                query.origin_kinds().contains(o),
            )
        })
        .collect();
    let detection_chips: Vec<_> = DETECTIONS
        .iter()
        .map(|d| {
            (
                d.label(),
                list_href(&state, &query.toggle_detection(*d)),
                query.detections.contains(d),
            )
        })
        .collect();
    let policy_chips: Vec<_> = POLICIES
        .iter()
        .map(|p| {
            (
                p.label(),
                list_href(&state, &query.toggle_policy(*p)),
                query.policies.contains(p),
            )
        })
        .collect();
    let review = query.tab == Tab::Review;
    let active_tab = query.tab == Tab::Active;
    let empty_message = match query.tab {
        Tab::Active => "No channels match these filters.",
        Tab::Unconfirmed => {
            "No unconfirmed channels: every channel with cross-agent traffic has a confirmed transmission."
        }
        Tab::Declared => "Every declared channel has carried cross-agent traffic.",
        Tab::Review => "Nothing to review: every channel in view has a policy decision.",
    };
    let superseded_href = list_href(&state, &query.toggle_superseded());
    let superseded_on = query.includes_superseded() && !query.superseded_only();
    let only_href = list_href(&state, &query.toggle_superseded_only());
    let only_on = query.superseded_only();
    let pairs = query.pairs();
    let empty = listing
        .as_ref()
        .is_ok_and(|l| l.as_ref().is_some_and(|page| page.rows.is_empty()));

    Ok(view! {
        // A merge or unmerge can hide a channel or list it again.
        live_watch(tokens: "channel agent".to_owned())
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
            }
            if active_tab {
                filter_chip(label: "include superseded", href: superseded_href, active: superseded_on)
                filter_chip(label: "superseded only", href: only_href, active: only_on)
            }
            filter_chip(label: "confirmed only", href: confirmed_only_href.clone(), active: confirmed_only)
        </div>
        match listing {
            Err(error) => {
                (status_of(&error))
                error_panel(error: &error)
            },
            Ok(None) => {
                <div class="rounded border border-dashed border-amber-300 p-6 text-center text-sm text-amber-900 dark:border-amber-800 dark:text-amber-100">
                    "Unconfirmed channels are left out by the confirmed-only filter. "
                    <a class=(LINK) href=(confirmed_only_href)>"Show them"</a>
                </div>
            },
            Ok(_) if empty => {
                empty_state(message: empty_message)
            },
            Ok(Some(listing)) => {
                let links = PageLinks::new(
                    PATH,
                    &state,
                    &pairs.iter().map(|(k, v)| (*k, v.as_str())).collect::<Vec<_>>(),
                    listing.current.as_ref(),
                    listing.next.as_ref(),
                );
                data_table(
                    headers: &["Channel", "Origin", "Detection", "Traffic", "Policy", "Writers", "Readers", "Transmissions", "Last activity"],
                    for row in listing.rows {
                        let [writers, readers, transmissions] = row.activity.cells();
                        let superseded = row.activity == Activity::Superseded;
                        let last = row.activity.last();
                        let note = row.traffic_note();
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
                                if superseded {
                                    state_badge(label: "superseded", tone: Tone::Muted)
                                }
                            </td>
                            <td class=(TD)>kind_badge(value: row.origin)</td>
                            <td class=(TD)>kind_badge(value: row.detection)</td>
                            <td class=(TD)>
                                match row.listing {
                                    Some(listing) => {
                                        kind_badge(value: listing)
                                        if let Some(note) = note {
                                            <div class="mt-0.5 text-[11px] text-amber-800 dark:text-amber-200">(note)</div>
                                        }
                                    },
                                    None => <span class="text-xs text-zinc-500">"—"</span>,
                                }
                            </td>
                            <td class=(TD)>kind_badge(value: row.policy)</td>
                            <td class=(TD_NUM)>(writers)</td>
                            <td class=(TD_NUM)>(readers)</td>
                            <td class=(TD_NUM)>(transmissions)</td>
                            <td class=(TD_MUTED)>(last)</td>
                        </tr>
                    }
                )
                pagination(links: links)
            },
        }
        <p class="mt-3 text-xs text-zinc-500">
            "A channel exists once a transmission between two different agents goes through it; a resource only one agent uses is not listed. Unconfirmed channels (suspected transmissions only, no content match yet) are listed and marked unless confirmed only is on. Writers, readers and transmissions are counted in the selected window; a superseded channel's are counted on the channel in force."
        </p>
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::components::href::tests::state;
    use crate::pages::channels::model::tests::{discovered, superseded, wiki};
    use crate::testing::get;
    use topcoat::router::StatusCode;

    #[test]
    fn rows_show_seed_counts_and_link_with_state() {
        let row = row(&discovered(7), &state());
        assert_eq!(row.shape, OwnedShape::Seed(wiki()));
        assert_eq!(row.policy, PolicyKind::Unreviewed);
        assert_eq!(row.detection, DetectionKind::Active);
        assert_eq!(row.activity.cells(), ["2", "3", "14"].map(str::to_owned));
        assert!(
            row.url
                .starts_with("/channels/00000000000000000000000007?from=")
        );
        assert_ne!(row.activity, Activity::Superseded);
    }

    #[test]
    fn superseded_rows_show_no_counts_of_their_own() {
        let row = row(&superseded(7, 8), &state());
        assert_eq!(row.activity, Activity::Superseded);
        assert_eq!(row.activity.cells(), ["—", "—", "—"].map(str::to_owned));
        assert_eq!(row.origin, CanonicalOriginKind::Discovered);
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
        assert!(
            reply.body.contains("wiki.example.org"),
            "the hijacked wiki awaits review"
        );
        let reply = get(&format!(
            "/channels?{state}&origin=discovered&detection=awaiting"
        ))
        .await;
        assert_eq!(reply.status, StatusCode::OK);
        assert!(reply.body.contains("No channels match these filters."));
    }

    #[tokio::test]
    async fn superseded_channels_list_on_their_own() {
        let state = state().to_query();
        let reply = get(&format!("/channels?{state}&superseded=only")).await;
        assert_eq!(reply.status, StatusCode::OK, "{}", reply.body);
        assert!(reply.body.contains("notes.corp.internal/team-a/standup"));
        assert!(!reply.body.contains("wiki.example.org"));
        let reply = get(&format!("/channels?{state}&origin=promoted")).await;
        assert_eq!(reply.status, StatusCode::OK, "{}", reply.body);
        assert!(reply.body.contains("notes.corp.internal/team-a"));
        assert!(!reply.body.contains("wiki.example.org"));
    }

    #[tokio::test]
    async fn tabs_split_channels_and_mark_unconfirmed_ones() {
        let state = state().to_query();
        let tab = |extra: &str| format!("/channels?{state}{extra}");
        let active = get(&tab("")).await;
        assert_eq!(active.status, StatusCode::OK, "{}", active.body);
        assert!(
            active.body.contains("agent-scratch"),
            "the unconfirmed S3 handoff is listed"
        );
        assert!(active.body.contains(">unconfirmed<"), "and marked");
        assert!(
            !active.body.contains("scratch/notes"),
            "a resource one agent uses is no channel"
        );
        assert!(
            !active.body.contains("handoff.md"),
            "a channel a merge left without cross-agent traffic is hidden"
        );
        assert!(
            !active.body.contains("docs.corp.internal"),
            "declarations are apart"
        );
        let confirmed = get(&tab("&u=confirmed")).await;
        assert_eq!(confirmed.status, StatusCode::OK);
        assert!(!confirmed.body.contains("agent-scratch"));
        assert!(confirmed.body.contains("wiki.example.org"));
        let unconfirmed = get(&tab("&tab=unconfirmed")).await;
        assert_eq!(unconfirmed.status, StatusCode::OK);
        assert!(unconfirmed.body.contains("agent-scratch"));
        assert!(unconfirmed.body.contains("suspected, none confirmed"));
        assert!(!unconfirmed.body.contains("wiki.example.org"));
        let left_out = get(&tab("&tab=unconfirmed&u=confirmed")).await;
        assert_eq!(left_out.status, StatusCode::OK);
        assert!(
            left_out
                .body
                .contains("left out by the confirmed-only filter")
        );
        let declared = get(&tab("&tab=declared")).await;
        assert_eq!(declared.status, StatusCode::OK);
        assert!(declared.body.contains("docs.corp.internal"));
        assert!(declared.body.contains("no traffic yet"));
        assert!(!declared.body.contains("wiki.example.org"));
    }

    #[tokio::test]
    async fn the_review_queue_marks_unconfirmed_channels_and_honours_confirmed_only() {
        let state = state().to_query();
        let review = get(&format!("/channels?{state}&tab=review")).await;
        assert_eq!(review.status, StatusCode::OK, "{}", review.body);
        assert!(review.body.contains("agent-scratch"));
        assert!(review.body.contains(">unconfirmed<"));
        let confirmed = get(&format!("/channels?{state}&tab=review&u=confirmed")).await;
        assert_eq!(confirmed.status, StatusCode::OK);
        assert!(!confirmed.body.contains("agent-scratch"));
        assert!(confirmed.body.contains("wiki.example.org"));
    }

    #[tokio::test]
    async fn the_confirmed_only_chip_toggles_the_shared_key() {
        let state = state().to_query();
        let reply = get(&format!("/channels?{state}")).await;
        assert!(reply.body.contains("u=confirmed"), "a link turns it on");
        let reply = get(&format!("/channels?{state}&u=confirmed")).await;
        assert!(reply.body.contains("aria-pressed=\"true\""));
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

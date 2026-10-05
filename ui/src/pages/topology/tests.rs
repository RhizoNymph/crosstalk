//! Router tests of the topology page and its drawer against the fixture
//! world, plus the drawer's argument validation.

use std::num::NonZeroU32;

use crosstalk_spec::aggregates::series::BucketWidth;
use crosstalk_spec::aggregates::topic::TopicModelVersion;
use crosstalk_spec::ids::OperatorId;
use crosstalk_spec::interfaces::l8_surface::{Caller, Permission};
use topcoat::router::StatusCode;

use super::drawer::model::{Drawer, load};
use super::selection::Selection;
use super::*;
use crate::backend::fixture::FixtureBackend;
use crate::testing::{cx, get};
use crate::url::ulid::UlidId;
use crosstalk_spec::interfaces::l8_surface::QueryError;
use crosstalk_spec::paging::PageRequest;

/// The default window of the fixture world (the last day) under v2.
pub fn fixture_state() -> ViewState {
    let mut state = crate::components::href::tests::state();
    state.scope.topic_version = TopicModelVersion(2);
    state
}

fn everyone() -> Caller {
    crate::testing::operator().caller()
}

fn first<L>(limit: u32) -> PageRequest<L> {
    crate::pages::common::paging::first(NonZeroU32::new(limit).expect("limit"))
}

/// The heaviest edge of the fixture's default view, as a selection.
async fn heaviest_edge() -> Selection {
    let backend = FixtureBackend::try_new(7).expect("fixture generates");
    let scope = fixture_state().scope;
    let graph = backend
        .topology(
            &everyone(),
            scope.window,
            Weighting::Transmissions,
            &scope.topology_filter(),
        )
        .await
        .expect("topology");
    let edge = graph
        .value
        .edges()
        .iter()
        .max_by(|a, b| a.share.get().total_cmp(&b.share.get()))
        .expect("an edge");
    Selection::edge(edge.from, edge.to, &edge.route)
}

pub async fn agent_labelled(label: &str) -> crosstalk_spec::ids::AgentId {
    let backend = FixtureBackend::try_new(7).expect("fixture generates");
    let page = backend
        .agents(
            &everyone(),
            &crosstalk_spec::aggregates::agents::filter::AgentFilter::default(),
            fixture_state().scope.window,
            &first(500),
        )
        .await
        .expect("agents")
        .value;
    page.items()
        .iter()
        .map(|row| &row.profile)
        .find(|a| a.label().is_some_and(|l| l.as_str() == label))
        .map(|a| a.id())
        .expect("labelled agent")
}

pub async fn wiki_channel() -> crosstalk_spec::ids::ChannelId {
    crate::testing::channel_id(crate::backend::fixture::ChannelKey::HijackedWiki)
}

/// The fixture's bucket width: five minutes.
fn bucket() -> BucketWidth {
    BucketWidth::from_micros(std::num::NonZeroU64::new(300_000_000).expect("five minutes"))
}

fn url(extra: &str) -> String {
    format!("/topology?{}{extra}", fixture_state().to_query())
}

#[test]
fn brush_shows_a_week_in_hours() {
    let state = fixture_state();
    let now = state.scope.window.end();
    let (window, buckets) = brush_window(state.scope.window, now, bucket());
    assert_eq!(buckets, 168);
    assert_eq!(window.end(), now);
    let src = timeline_src(&state, now, bucket());
    assert!(src.starts_with("/data/timeline?from=2026-09-26T00:00:00Z&to=2026-10-03T00:00:00Z"));
    assert!(src.ends_with("&buckets=168"));
}

#[test]
fn the_brush_window_is_aligned_whatever_the_present() {
    let state = fixture_state();
    // A wall clock between bucket boundaries.
    let now = Timestamp::from_micros(state.scope.window.end().as_micros() + 61_000_000);
    let (window, _) = brush_window(state.scope.window, now, bucket());
    assert!(
        crate::url::scope::is_aligned(window, bucket()),
        "{window:?}"
    );
    assert!(window.end() >= now && window.start() <= state.scope.window.start());
}

/// What the brush's `change` handler navigates to: the page URL with
/// `from` and `to` replaced by two of the payload's bucket edges.
fn brushed(from: &str, to: &str) -> String {
    let kept: Vec<String> = fixture_state()
        .to_query()
        .split('&')
        .filter(|pair| !pair.starts_with("from=") && !pair.starts_with("to="))
        .map(str::to_owned)
        .collect();
    format!("/topology?from={from}&to={to}&{}", kept.join("&"))
}

#[tokio::test]
async fn a_brushed_window_is_canonical() {
    let state = fixture_state();
    let now = state.scope.window.end();
    let reply = get(&timeline_src(&state, now, bucket())).await;
    assert_eq!(reply.status, StatusCode::OK, "{}", reply.body);
    let payload: serde_json::Value = serde_json::from_str(&reply.body).expect("json");
    let buckets = payload["buckets"].as_array().expect("buckets");
    assert_eq!(buckets.len(), 168);
    let from = buckets[100]["from"].as_str().expect("from");
    let to = buckets[130]["to"].as_str().expect("to");
    let reply = get(&brushed(from, to)).await;
    assert_eq!(
        reply.status,
        StatusCode::OK,
        "a brushed URL is served without a redirect: {:?}",
        reply.location
    );
}

#[tokio::test]
async fn incomplete_urls_redirect_to_canonical() {
    let reply = get("/topology").await;
    assert_eq!(reply.status, StatusCode::TEMPORARY_REDIRECT);
}

#[tokio::test]
async fn renders_graph_brush_filter_and_heaviest_edges() {
    let reply = get(&url("")).await;
    assert_eq!(reply.status, StatusCode::OK, "{}", reply.body);
    let body = &reply.body;
    assert!(body.contains("<ct-topology"));
    assert!(body.contains("data-src=\"/data/topology?from=2026-10-02T00:00:00Z"));
    assert!(body.contains("<ct-timebrush"));
    assert!(body.contains("buckets=168"));
    assert!(
        body.contains("final up to 2026-10-02 23:50:00 UTC"),
        "watermark"
    );
    assert!(body.contains("Heaviest edges"));
    assert!(body.contains("data-select=\"edge:"), "selectable edges");
    assert!(body.contains("name=\"fa\""), "agent choices");
    assert!(
        body.contains(">pi-scraper</span>"),
        "labelled agents are offered"
    );
    assert!(body.contains("wiki.example.org/wiki/Agent_Coordination"));
    assert!(body.contains("name=\"ft\""), "topics offered with Content");
}

#[tokio::test]
async fn follows_the_feed_in_the_elements_not_by_re_rendering() {
    let reply = get(&url("")).await;
    assert_eq!(reply.status, StatusCode::OK, "{}", reply.body);
    let body = &reply.body;
    assert!(
        !body.contains("data-live-watch"),
        "no page re-render on events"
    );
    assert_eq!(body.matches("data-live=\"/data/live\"").count(), 2);
    for stat in ["agents", "edges", "transmissions", "watermark"] {
        assert!(
            body.contains(&format!("data-topology-stat=\"{stat}\"")),
            "{stat}"
        );
    }
}

#[tokio::test]
async fn a_selected_edge_lists_its_transmissions() {
    let sel = heaviest_edge().await.encode();
    let reply = get(&url(&format!("&sel={sel}"))).await;
    assert_eq!(reply.status, StatusCode::OK, "{}", reply.body);
    assert!(reply.body.contains("Transmissions on this edge"));
    assert!(reply.body.contains("href=\"/transmissions/"));
    assert!(reply.body.contains(&format!("data-drawer=\"{sel}\"")));
    assert!(reply.body.contains(" of transmissions"));
}

#[tokio::test]
async fn agent_and_channel_selections_show_cards() {
    let agent = agent_labelled("pi-scraper").await;
    let reply = get(&url(&format!("&sel=agent:{}", agent.to_ulid()))).await;
    assert_eq!(reply.status, StatusCode::OK, "{}", reply.body);
    assert!(reply.body.contains("Open agent page"));
    assert!(
        reply.body.contains("claims"),
        "harness claims shown as claims"
    );

    let wiki = wiki_channel().await;
    let reply = get(&url(&format!("&sel=channel:{}", wiki.to_ulid()))).await;
    assert_eq!(reply.status, StatusCode::OK, "{}", reply.body);
    assert!(reply.body.contains("Open channel page"));
    assert!(reply.body.contains("Edges through it"));
    assert!(reply.body.contains("unreviewed"));
}

#[tokio::test]
async fn bad_page_keys_are_422() {
    let reply = get(&url("&sel=node:1")).await;
    assert_eq!(reply.status, StatusCode::UNPROCESSABLE_ENTITY);
    assert!(reply.body.contains("sel: unknown selection kind"));
    let reply = get(&url("&collapse=yes")).await;
    assert_eq!(reply.status, StatusCode::UNPROCESSABLE_ENTITY);
}

#[tokio::test]
async fn filter_form_redirects_to_the_canonical_filter() {
    let reply = get(&url(
        "&apply=1&fr=channel&fr=direct&fx=exclude-false&collapse=1",
    ))
    .await;
    assert_eq!(reply.status, StatusCode::SEE_OTHER, "{}", reply.body);
    let location = reply.location.expect("location");
    assert!(
        location.contains("&g=agents&r=channel,direct&x=exclude-false&collapse=1"),
        "{location}"
    );
    let reply = get(&url("&apply=1&fr=teleport")).await;
    assert_eq!(reply.status, StatusCode::UNPROCESSABLE_ENTITY);
}

#[tokio::test]
async fn active_filters_show_as_chips_and_toggles_carry_state() {
    let reply = get(&url("&r=channel&collapse=1")).await;
    assert_eq!(reply.status, StatusCode::OK, "{}", reply.body);
    assert!(reply.body.contains("Clear all"));
    assert!(reply.body.contains("data-collapse=\"true\""));
    assert!(
        reply
            .body
            .contains("g=channels&amp;r=channel&amp;collapse=1"),
        "mode toggle keeps filter and keys"
    );
}

#[tokio::test]
async fn drawer_arguments_are_validated() {
    let cx = cx();
    let caller = everyone();
    let state = fixture_state().to_query();
    let bad_state = load(&cx, &caller, "from=yesterday", "", "").await;
    assert!(matches!(
        bad_state,
        Err(UiError::Field { field: "state", .. })
    ));
    let bad_sel = load(&cx, &caller, &state, "edge:x", "").await;
    assert!(matches!(bad_sel, Err(UiError::Field { field: "sel", .. })));
    let bad_cursor = load(&cx, &caller, &state, "", "a b").await;
    assert!(bad_cursor.is_err());
    let viewer = crate::testing::caller_of(OperatorId::from_ulid(1), &[Permission::Content]);
    assert_eq!(
        load(&cx, &viewer, &state, "", "").await.map(|_| ()),
        Err(UiError::Query(QueryError::Forbidden {
            missing: Permission::View
        }))
    );
    let unknown = format!("agent:{}", "01J9ZQ3W8D0000000000000999");
    let (_, drawer) = load(&cx, &caller, &state, &unknown, "")
        .await
        .expect("load");
    assert_eq!(drawer, Drawer::Missing("No agent has this id."));
}

#[tokio::test]
async fn edge_pages_follow_the_cursor() {
    let cx = cx();
    let caller = everyone();
    let state = fixture_state().to_query();
    let sel = heaviest_edge().await.encode();
    let (_, first) = load(&cx, &caller, &state, &sel, "").await.expect("load");
    let Drawer::Edge(first) = first else {
        panic!("edge drawer")
    };
    assert!(!first.paged);
    let next = first
        .next
        .clone()
        .expect("the heaviest edge has a second page");
    let (_, second) = load(&cx, &caller, &state, &sel, &next).await.expect("load");
    let Drawer::Edge(second) = second else {
        panic!("edge drawer")
    };
    assert!(second.paged);
    assert_ne!(
        first.rows.first().map(|r| r.id),
        second.rows.first().map(|r| r.id)
    );
}

/// The `aria-pressed` the page renders on the element carrying `marker`
/// (the first `aria-pressed` after it, which is the same tag's).
fn pressed_after(body: &str, marker: &str) -> Option<bool> {
    let at = body.find(marker)?;
    let rest = &body[at..];
    let value = rest.find("aria-pressed=\"")? + "aria-pressed=\"".len();
    Some(rest[value..].starts_with("true"))
}

fn list_item(code: &str) -> String {
    format!("data-list-item=\"{code}\"")
}

fn pressed_items(body: &str) -> usize {
    body.match_indices("data-list-item=\"")
        .filter(|(at, _)| pressed_after(&body[*at..], "data-list-item") == Some(true))
        .count()
}

/// Bytes one list row may take: rows repeat once per agent and channel, so
/// they carry no script, bindings or long class lists (about 0.8 KB each
/// with the fixture's names and claims).
const ROW_BUDGET: usize = 1_100;
/// Bytes the lists panel may take besides its rows: tabs, filter boxes and
/// the lists' delegated handlers and filter bindings.
const PANEL_BUDGET: usize = 16_000;

#[tokio::test]
async fn list_rows_carry_no_script_and_stay_within_budget() {
    for path in [url(""), url("").replace("g=agents", "g=channels")] {
        let reply = get(&path).await;
        assert_eq!(reply.status, StatusCode::OK, "{}", reply.body);
        let body = &reply.body;
        let start = body.find("<section").expect("lists panel");
        let end = start + body[start..].find("</section>").expect("panel end");
        let panel = &body[start..end];
        let rows: Vec<&str> = panel
            .split("<li>")
            .skip(1)
            .map(|rest| &rest[..rest.find("</li>").expect("row end")])
            .collect();
        assert!(rows.len() > 20, "{path}: {} rows", rows.len());
        for row in &rows {
            assert!(row.contains("data-list-item="), "{row}");
            assert!(!row.contains("data-topcoat"), "a row carries script: {row}");
            assert!(row.len() <= ROW_BUDGET, "{} bytes: {row}", row.len());
        }
        let row_bytes: usize = rows.iter().map(|r| r.len() + "<li></li>".len()).sum();
        assert!(
            panel.len() - row_bytes <= PANEL_BUDGET,
            "{path}: the panel takes {} bytes besides its rows",
            panel.len() - row_bytes
        );
    }
}

#[tokio::test]
async fn lists_show_the_views_agents_and_the_channels_behind_its_edges() {
    let reply = get(&url("")).await;
    assert_eq!(reply.status, StatusCode::OK, "{}", reply.body);
    let body = &reply.body;
    let pi = format!("agent:{}", agent_labelled("pi-scraper").await.to_ulid());
    let wiki = format!("channel:{}", wiki_channel().await.to_ulid());
    assert!(body.contains(&list_item(&pi)), "pi-scraper is listed");
    assert!(
        body.contains(&list_item(&wiki)),
        "the hijacked wiki is listed"
    );
    assert!(body.contains("title=\"wiki.example.org/wiki/Agent_Coordination\""));
    assert!(body.contains("Channels behind channel-routed edges"));
    assert!(body.contains("placeholder=\"Filter agents\""));
    assert_eq!(pressed_items(body), 0, "nothing selected, nothing marked");
    assert_eq!(pressed_after(body, "data-list-tab=\"agents\""), Some(true));
    assert_eq!(
        pressed_after(body, "data-list-tab=\"channels\""),
        Some(false)
    );
    assert!(
        body.contains("data-list-clear=\"\" hidden"),
        "clear control hidden"
    );
}

#[tokio::test]
async fn the_selected_agent_is_marked_from_sel() {
    let pi = format!("agent:{}", agent_labelled("pi-scraper").await.to_ulid());
    let reply = get(&url(&format!("&sel={pi}"))).await;
    assert_eq!(reply.status, StatusCode::OK, "{}", reply.body);
    let body = &reply.body;
    assert_eq!(pressed_after(body, &list_item(&pi)), Some(true));
    assert_eq!(pressed_items(body), 1);
    assert_eq!(pressed_after(body, "data-list-tab=\"agents\""), Some(true));
    assert!(body.contains(&format!("data-drawer=\"{pi}\"")));
    assert!(!body.contains("data-list-clear=\"\" hidden"));
}

#[tokio::test]
async fn a_selected_channel_is_marked_on_the_channels_tab() {
    let wiki = format!("channel:{}", wiki_channel().await.to_ulid());
    let reply = get(&url(&format!("&sel={wiki}"))).await;
    assert_eq!(reply.status, StatusCode::OK, "{}", reply.body);
    let body = &reply.body;
    assert_eq!(pressed_after(body, &list_item(&wiki)), Some(true));
    assert_eq!(pressed_items(body), 1);
    assert_eq!(
        pressed_after(body, "data-list-tab=\"channels\""),
        Some(true)
    );
    assert_eq!(pressed_after(body, "data-list-tab=\"agents\""), Some(false));
}

#[tokio::test]
async fn channels_mode_lists_the_channel_nodes() {
    let wiki = format!("channel:{}", wiki_channel().await.to_ulid());
    let path = url("").replace("g=agents", "g=channels");
    let reply = get(&format!("{path}&sel={wiki}")).await;
    assert_eq!(reply.status, StatusCode::OK, "{}", reply.body);
    let body = &reply.body;
    assert!(body.contains("Channel nodes, by reads and writes"));
    assert_eq!(pressed_after(body, &list_item(&wiki)), Some(true));
    let pi = format!("agent:{}", agent_labelled("pi-scraper").await.to_ulid());
    assert!(body.contains(&list_item(&pi)));
    // Reads and writes, with the policy badge.
    let item = &body[body.find(&list_item(&wiki)).expect("wiki item")..];
    let end = item.find("</li>").expect("item end");
    assert!(item[..end].contains(" r</span>"), "reads and writes shown");
    assert!(item[..end].contains(">unreviewed<"), "policy badge");
}

fn s3_handoff() -> String {
    let id = crate::testing::channel_id(crate::backend::fixture::ChannelKey::S3Handoff);
    format!("channel:{}", id.to_ulid())
}

/// The row of the list item `code`, up to its `</li>`.
fn list_row<'a>(body: &'a str, code: &str) -> Option<&'a str> {
    let at = body.find(&list_item(code))?;
    let item = &body[at..];
    Some(&item[..item.find("</li>")?])
}

fn state_path(
    graph: GraphMode,
    unconfirmed: crosstalk_spec::aggregates::filter::UnconfirmedChannels,
) -> String {
    let mut state = fixture_state();
    state.graph = graph;
    state.scope.filter.unconfirmed_channels = unconfirmed;
    format!("/topology?{}", state.to_query())
}

/// Channels mode lists the S3 handoff (only suspected traffic) marked
/// unconfirmed. Agents mode cannot: its edges are confirmed transmissions,
/// so every channel behind one is confirmed, and the S3 handoff is in
/// neither its graph nor its list.
#[tokio::test]
async fn an_unconfirmed_channel_is_marked_in_the_channels_list() {
    use crosstalk_spec::aggregates::filter::UnconfirmedChannels;

    let s3 = s3_handoff();
    let wiki = format!("channel:{}", wiki_channel().await.to_ulid());
    let reply = get(&state_path(
        GraphMode::Channels,
        UnconfirmedChannels::Include,
    ))
    .await;
    assert_eq!(reply.status, StatusCode::OK, "{}", reply.body);
    let row = list_row(&reply.body, &s3).expect("the S3 handoff is listed");
    assert!(row.contains("agent-scratch/handoff"), "{row}");
    assert!(row.contains(">unconfirmed<"), "marked: {row}");
    assert!(
        row.contains(" unconfirmed\""),
        "the filter matches the marker: {row}"
    );
    let confirmed = list_row(&reply.body, &wiki).expect("the wiki is listed");
    assert!(
        !confirmed.contains("unconfirmed"),
        "a confirmed channel is not marked: {confirmed}"
    );
    let agents = get(&state_path(GraphMode::Agents, UnconfirmedChannels::Include)).await;
    assert_eq!(agents.status, StatusCode::OK, "{}", agents.body);
    assert!(!agents.body.contains(&list_item(&s3)));
    assert!(!agents.body.contains(">unconfirmed<"));
}

#[tokio::test]
async fn confirmed_only_leaves_unconfirmed_channels_out_of_list_and_graph() {
    use crosstalk_spec::aggregates::filter::UnconfirmedChannels;
    use crosstalk_spec::aggregates::node::GraphNode;

    let s3 = s3_handoff();
    let s3_id = crate::testing::channel_id(crate::backend::fixture::ChannelKey::S3Handoff);
    let backend = FixtureBackend::try_new(7).expect("fixture generates");
    for unconfirmed in [UnconfirmedChannels::Include, UnconfirmedChannels::Exclude] {
        let mut state = fixture_state();
        state.scope.filter.unconfirmed_channels = unconfirmed;
        let filter = state.scope.topology_filter();
        let agents = backend
            .topology(
                &everyone(),
                state.scope.window,
                Weighting::Transmissions,
                &filter,
            )
            .await
            .expect("topology")
            .value;
        let routed_through_s3 = agents
            .edges()
            .iter()
            .any(|e| crate::pages::common::transmissions::route_channel(&e.route) == Some(s3_id));
        let bipartite = backend
            .channel_topology(
                &everyone(),
                state.scope.window,
                Weighting::Transmissions,
                &filter,
            )
            .await
            .expect("channel topology")
            .value;
        let s3_node = bipartite
            .nodes()
            .iter()
            .any(|n| matches!(n, GraphNode::Channel(c) if c.id == s3_id));
        let kept = unconfirmed == UnconfirmedChannels::Include;
        assert_eq!(s3_node, kept, "{unconfirmed:?}: the channel graph");
        for (graph, drawn) in [
            (GraphMode::Agents, routed_through_s3),
            (GraphMode::Channels, s3_node),
        ] {
            let reply = get(&state_path(graph, unconfirmed)).await;
            assert_eq!(reply.status, StatusCode::OK, "{}", reply.body);
            let listed = reply.body.contains(&list_item(&s3));
            assert_eq!(
                listed, drawn,
                "{graph:?} {unconfirmed:?}: list and graph agree"
            );
            if !kept {
                assert!(!listed, "{graph:?}: confirmed only leaves it out");
                assert!(!reply.body.contains(">unconfirmed<"), "{graph:?}");
            }
        }
    }
}

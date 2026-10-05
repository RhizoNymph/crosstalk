//! Live updates: what a page declares it shows, for `<ct-live>`.
//!
//! The root layout renders `<ct-live>` (subscribed to `/data/live`) above
//! the page and wraps the page in `data-live-region="page"`. A page lists
//! what it shows in one hidden `data-live-watch` marker: space-separated
//! tokens, each a feed event kind (`alert`, `channel`, `agent`, `rule`,
//! `verdict`, `projection`, `topic-version`, `watermark`) alone, for any id
//! of that kind, or `kind:<id>` for one entity (`topic-version:<n>` for a
//! version). When an event matches, the element re-renders the page
//! through Topcoat's runtime and swaps the region; a page without a marker
//! never refreshes.

use topcoat::Result;
use topcoat::view::{View, component, view};

use crate::url::ulid::UlidId;

/// `kind:<ulid>`: one entity of a kind.
pub fn watch_one(kind: &str, id: impl UlidId) -> String {
    format!("{kind}:{}", id.to_ulid())
}

#[component]
pub async fn live_watch(tokens: String) -> Result<impl View> {
    Ok(view! { <span hidden=(true) data-live-watch=(tokens)></span> })
}

#[cfg(test)]
mod tests {
    use topcoat::router::StatusCode;

    use crate::pages::topology::tests::fixture_state;
    use crate::testing::get;

    #[tokio::test]
    async fn pages_declare_what_they_watch_inside_the_live_region() {
        let reply = get(&format!("/alerts?{}", fixture_state().to_query())).await;
        assert_eq!(reply.status, StatusCode::OK, "{}", reply.body);
        assert!(reply.body.contains("<ct-live data-src=\"/data/live\""));
        let region = reply
            .body
            .find("data-live-region=\"page\"")
            .expect("the page region");
        let watch = reply
            .body
            .find("data-live-watch=\"alert rule\"")
            .expect("the inbox's watch tokens");
        assert!(region < watch, "the tokens are inside the region");
    }
}

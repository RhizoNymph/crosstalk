//! The follow bar of `/` and `/topology`, and the header's finality label.
//!
//! A followed view shows "Following the last 1 d · Pin". Pin links to the
//! same page at the window this render resolved, which is the citeable URL.
//! A pinned view shows "Follow", which follows the last
//! [`FollowSpan::DEFAULT`] with the rest of the view kept.
//!
//! The bar also tells `<ct-live>` that the page follows
//! (`data-live-follow`): it then refreshes on a timer as well as on its
//! watch tokens, and the bar declares the `watermark` token, so the window
//! slides and the provisional tail settles without a reload.

use crosstalk_spec::support::{TimeWindow, Timestamp};
use topcoat::Result;
use topcoat::view::{View, component, view};

use super::form::LINK;
use super::href::href;
use super::live::live_watch;
use crate::url::follow::FollowSpan;
use crate::url::view_state::ViewState;

/// The header's finality label: "provisional after W" when the window
/// reaches past the watermark `W`, else "final up to W". `at` is the
/// watermark as the page formats times.
pub fn finality(window: TimeWindow, watermark: Timestamp, at: &str) -> String {
    if window.end() > watermark {
        format!("provisional after {at}")
    } else {
        format!("final up to {at}")
    }
}

/// "Following the last … · Pin" on a followed view, "Follow" on a pinned
/// one. `extra` are the page's own keys, kept on both links.
#[component]
pub async fn follow_bar(
    path: &'static str,
    state: &ViewState,
    extra: Vec<(&'static str, String)>,
) -> Result<impl View> {
    let extra: Vec<(&str, &str)> = extra.iter().map(|(k, v)| (*k, v.as_str())).collect();
    let pin = href(path, &state.pinned(), &extra);
    let follow = href(path, &state.following(FollowSpan::DEFAULT), &extra);
    let follow_title = format!(
        "Follow the last {}, ending now",
        FollowSpan::DEFAULT.label()
    );
    Ok(view! {
        match state.follow {
            Some(span) => {
                live_watch(tokens: "watermark".to_owned())
                <p class="mt-1 flex flex-wrap items-center gap-1.5 text-xs text-zinc-500" data-follow-bar="" data-live-follow=(span.as_str())>
                    <span class="inline-block h-1.5 w-1.5 rounded-full bg-emerald-500" aria-hidden="true"></span>
                    <span>"Following the last " (span.label())</span>
                    <span aria-hidden="true">"·"</span>
                    <a class=(LINK) href=(pin) title="This window, pinned: a link that always shows it">"Pin"</a>
                </p>
            },
            None => {
                <p class="mt-1 text-xs text-zinc-500" data-follow-bar="">
                    <a class=(LINK) href=(follow) title=(follow_title)>"Follow"</a>
                </p>
            },
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::components::href::tests::state;
    use crate::testing::{cx, render};

    const W: Timestamp = Timestamp::from_micros(1_790_985_000_000_000);

    #[test]
    fn a_window_past_the_watermark_is_provisional_after_it() {
        let window = state().scope.window;
        assert!(window.end() > W);
        assert_eq!(
            finality(window, W, "23:50"),
            "provisional after 23:50",
            "the window ends after the watermark"
        );
        assert_eq!(
            finality(window, window.end(), "00:00"),
            "final up to 00:00",
            "a window ending at the watermark is final"
        );
    }

    #[tokio::test]
    async fn a_followed_view_offers_pin_at_its_resolved_window() {
        let cx = &cx();
        let followed = state().following(FollowSpan::Day);
        let html = render(
            view! { cx => follow_bar(path: "/topology", state: &followed, extra: vec![("sel", "agent:x".to_owned())]) },
            cx,
        )
        .await;
        assert!(html.contains("Following the last 1 d"), "{html}");
        assert!(
            html.contains(
                "href=\"/topology?from=2026-10-02T00:00:00Z&amp;to=2026-10-03T00:00:00Z&amp;v=3&amp;w=tx&amp;g=agents&amp;sel=agent:x\""
            ),
            "Pin is the resolved window with the page's keys: {html}"
        );
        assert!(html.contains("data-live-follow=\"1d\""), "{html}");
        assert!(html.contains("data-live-watch=\"watermark\""), "{html}");
        assert!(!html.contains(">Follow<"), "{html}");
    }

    #[tokio::test]
    async fn a_pinned_view_offers_follow_keeping_the_rest() {
        let cx = &cx();
        let mut pinned = state();
        pinned.weighting = crosstalk_spec::aggregates::edge::Weighting::MatchedBytes;
        let html = render(
            view! { cx => follow_bar(path: "/", state: &pinned, extra: Vec::new()) },
            cx,
        )
        .await;
        assert!(
            html.contains("href=\"/?follow=1d&amp;v=3&amp;w=bytes&amp;g=agents\""),
            "{html}"
        );
        assert!(html.contains(">Follow<"), "{html}");
        assert!(!html.contains("Following"), "{html}");
        assert!(!html.contains("data-live-follow"), "{html}");
        assert!(!html.contains("data-live-watch"), "{html}");
    }
}

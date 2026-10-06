//! Rendering the conversation view's components from view models.

use crosstalk_spec::observed::client::{HarnessClaim, HarnessFamily};
use topcoat::view::view;

use super::model::*;
use super::sections::{head_section, turn_card, window_nav};
use crate::pages::common::transmissions::Named;
use crate::testing::render;

fn named(name: &str) -> Named {
    Named {
        url: format!("/agents/{name}"),
        name: name.to_owned(),
    }
}

fn link(label: &str, url: &str) -> Link {
    Link {
        label: label.to_owned(),
        url: url.to_owned(),
    }
}

fn claim() -> HarnessClaim {
    HarnessClaim {
        family: HarnessFamily::ClaudeCode,
        version: Some("2.1.4".into()),
        user_agent: "claude-cli/2.1.4".into(),
    }
}

fn head() -> HeadView {
    HeadView {
        short: "…ABCDEF".into(),
        full: "01J9ZQ3W8D0000000000ABCDEF".into(),
        agent: named("cc3"),
        claims: vec![claim()],
        started: "12:01".into(),
        last: "12:40".into(),
        turns: 84,
        received: 3,
        sent: 5,
        origin: OriginView::Fork {
            parent: link("…PARENT", "/conversations/P"),
            branch: Some(link("turn 11", "/conversations/P?turn=11#turn-11")),
            shared: 23,
        },
        spawned_by: Some(SpawnedBy {
            parent: named("cc0"),
            turn: link("turn 7 of …PARENT", "/conversations/Q?turn=7#turn-7"),
            transmission: link("transmission …T", "/transmissions/T"),
        }),
        successors: vec![SuccessorView {
            kind: "compaction".into(),
            link: link("…NEXT", "/conversations/N"),
            started: "12:41".into(),
        }],
        replayed: Some("agentdojo-workspace".into()),
    }
}

fn text_part(text: TextView, marks: Vec<MarkView>) -> PartView {
    PartView {
        kind: "text".into(),
        detail: None,
        size: Some("1.2 KB".into()),
        text,
        marks,
    }
}

fn inbound() -> MarkView {
    MarkView::Inbound(InboundView {
        from: named("al0"),
        route: Some("wiki.example.org".into()),
        route_url: Some("/channels/W".into()),
        kind: "exact".into(),
        carrier: "in a tool result (call toolu_1)".into(),
        matched: "212 B".into(),
        range: range_text(&(4..9)),
        sender_turn: Some("/spans/S".into()),
        transmission: Some(link("transmission …T", "/transmissions/T")),
        state: Some("confirmed".into()),
        delegation: None,
    })
}

fn originated(readers: usize, more: u32) -> MarkView {
    MarkView::Originated(OriginatedView {
        status: "propagated (2 hits)".into(),
        range: range_text(&(0..5)),
        readers: (0..readers)
            .map(|i| ReaderView {
                agent: named(&format!("reader{i}")),
                turn: Some(format!("/exchanges/E{i}")),
                transmission: Some(link("transmission", "/transmissions/X")),
                carrier: "tool result".into(),
                delegation: (i == 0).then(|| "delegated to sub-agent".to_owned()),
            })
            .collect(),
        more,
        more_url: (more > 0).then(|| "/conversations/C?rcursor=a#readers".to_owned()),
        expired: false,
        highlighted: true,
    })
}

fn turn(text: TextView) -> TurnView {
    TurnView {
        index: 37,
        time: "12:31:04".into(),
        model: "claude-sonnet-4-5".into(),
        transport: "sse".into(),
        connection: None,
        outcome: OutcomeView::Completed {
            stop: "tool use".into(),
            finished: "12:31:20".into(),
        },
        usage: Some("41,200 in · 812 out".into()),
        claim: Some(claim()),
        agent: None,
        replayed: None,
        boundaries: vec![
            BoundaryView::Compaction {
                predecessor: link("…OLD", "/conversations/OLD"),
                carried: 2,
            },
            BoundaryView::UnseenHistory {
                connection: Some("…CONN".into()),
            },
        ],
        pending: false,
        inputs: vec![MessageView {
            role: "tool".into(),
            system_turn: false,
            parts: vec![text_part(text.clone(), vec![inbound()])],
        }],
        carried: vec![MessageView {
            role: "assistant".into(),
            system_turn: false,
            parts: vec![text_part(TextView::Hidden, vec![])],
        }],
        output: Some(MessageView {
            role: "assistant".into(),
            system_turn: false,
            parts: vec![text_part(text, vec![originated(2, 3)])],
        }),
    }
}

#[tokio::test]
async fn the_head_shows_origin_delegation_successors_and_claims_as_claims() {
    let cx = &crate::testing::cx();
    let html = render(view! { cx => head_section(head: head()) }, cx).await;
    assert!(html.contains("forked from"), "{html}");
    assert!(html.contains("23 shared messages"));
    assert!(html.contains("/conversations/P?turn=11#turn-11"));
    assert!(html.contains("data-spawned-by"));
    assert!(html.contains("/transmissions/T"));
    assert!(html.contains("Continued in"));
    assert!(html.contains("claims"), "claims are shown as claims");
    assert!(html.contains("replayed: agentdojo-workspace"));
    assert!(html.contains("3 received · 5 sent"));
}

#[tokio::test]
async fn a_turn_with_text_marks_its_ranges() {
    let segments = segments(
        0,
        "say hello world",
        &[Marked {
            range: 4..9,
            tone: Tone::Inbound,
        }],
    );
    let text = TextView::Shown {
        segments,
        remaining: 120,
    };
    let cx = &crate::testing::cx();
    let html = render(view! { cx => turn_card(turn: turn(text)) }, cx).await;
    assert!(html.contains("id=\"turn-37\""), "{html}");
    assert!(html.contains("<mark"));
    assert!(html.contains(">hello</mark>"));
    assert!(html.contains("120 more bytes"));
    assert!(html.contains("data-mark=\"inbound\""));
    assert!(html.contains("via"));
    assert!(html.contains("data-evidence=\"true\""));
    assert!(html.contains("read later by"));
    assert!(html.contains("+3 more"));
    assert!(html.contains("delegated to sub-agent"));
    assert!(html.contains("data-boundary=\"compaction\""));
    assert!(html.contains("data-boundary=\"unseen-history\""));
    assert!(html.contains("data-carried=\"true\""));
    assert!(html.contains("carried over (1)"));
    assert!(html.contains("provenance: scanned"));
}

#[tokio::test]
async fn without_content_a_turn_shows_structure_and_no_text() {
    let cx = &crate::testing::cx();
    let html = render(view! { cx => turn_card(turn: turn(TextView::Hidden)) }, cx).await;
    assert!(html.contains("content hidden"), "{html}");
    assert!(!html.contains("<pre"), "no text is rendered");
    assert!(html.contains("bytes 4–9"), "marks give byte ranges");
}

#[tokio::test]
async fn a_dropped_body_and_a_pending_scan_say_so() {
    let mut pending = turn(TextView::Dropped);
    pending.pending = true;
    pending.outcome = OutcomeView::Failed {
        failure: "the stream ended early".into(),
        at: "12:31:09".into(),
    };
    let cx = &crate::testing::cx();
    let html = render(view! { cx => turn_card(turn: pending) }, cx).await;
    assert!(html.contains("data-body-dropped=\"true\""), "{html}");
    assert!(html.contains("scan in progress"));
    assert!(html.contains("failed: the stream ended early"));
}

#[tokio::test]
async fn an_unread_expired_span_says_no_reader_was_detected() {
    let mut card = turn(TextView::Hidden);
    card.output = Some(MessageView {
        role: "assistant".into(),
        system_turn: false,
        parts: vec![text_part(
            TextView::Hidden,
            vec![MarkView::Originated(OriginatedView {
                status: "expired".into(),
                range: range_text(&(0..5)),
                readers: vec![],
                more: 0,
                more_url: None,
                expired: true,
                highlighted: false,
            })],
        )],
    });
    let cx = &crate::testing::cx();
    let html = render(view! { cx => turn_card(turn: card) }, cx).await;
    assert!(
        html.contains("no reader detected before it expired"),
        "{html}"
    );
}

#[tokio::test]
async fn the_window_links_earlier_and_later_turns() {
    let window = WindowView {
        from: 20,
        to: 40,
        total: 84,
        earlier: Some("/conversations/C?turn=0".into()),
        later: Some("/conversations/C?turn=40".into()),
        past_end: None,
    };
    let cx = &crate::testing::cx();
    let html = render(view! { cx => window_nav(window: window) }, cx).await;
    assert!(html.contains("turns 20–39 of 84"), "{html}");
    assert!(html.contains("rel=\"prev\""));
    assert!(html.contains("rel=\"next\""));
    let past = WindowView {
        from: 80,
        to: 84,
        total: 84,
        earlier: Some("/conversations/C?turn=60".into()),
        later: None,
        past_end: Some(200),
    };
    let cx = &crate::testing::cx();
    let html = render(view! { cx => window_nav(window: past) }, cx).await;
    assert!(html.contains("Turn 200 does not exist yet"), "{html}");
}

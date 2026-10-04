//! Transmission rows on the wire: `QueryApi::transmissions_by_id` (a
//! `TransmissionSelection` in, a `TransmissionPage` of
//! `TransmissionSummary` rows out).

use super::super::harness::{assert_golden, assert_rejected, assert_request_golden};
use super::super::{ULID_A, ULID_B, ULID_C, id, ts};
use super::fixtures::{ULID_D, coder, confirmed_at, edited, field, nz, planner, topic, tx, wiki};
use crate::aggregates::topic::TopicModelVersion;
use crate::derived::flow::transmission::Route;
use crate::derived::flow::verdict::Verdict;
use crate::ids::TransmissionId;
use crate::interfaces::l8_surface::summary::{
    Delivery, SummaryState, TopicUnder, TransmissionPage, TransmissionSelection,
    TransmissionSummary,
};
use crate::paging::{Cursor, Page, PageSize, TransmissionList};
use crate::support::NonEmpty;

const AREA: &str = "surface-reads/transmissions";

fn delivery() -> Delivery {
    Delivery {
        from: planner(),
        confirmed_at: confirmed_at(),
        matched_bytes: nz(47),
    }
}

fn summary(state: SummaryState) -> TransmissionSummary {
    TransmissionSummary {
        id: tx(),
        to: coder(),
        route: Route::Channel(wiki()),
        opened_at: ts("2026-10-04T09:16:40.002513Z"),
        state,
    }
}

/// One summary in every state, named after it.
fn every_state() -> Vec<(&'static str, SummaryState)> {
    fn declared(state: SummaryState) -> SummaryState {
        match state {
            SummaryState::Detected
            | SummaryState::AwaitingContent
            | SummaryState::Suspected { .. }
            | SummaryState::Discarded { .. }
            | SummaryState::Confirmed { .. }
            | SummaryState::Classified { .. }
            | SummaryState::Aggregated { .. } => state,
        }
    }
    [
        ("transmission_summary_detected", SummaryState::Detected),
        (
            "transmission_summary_awaiting_content",
            SummaryState::AwaitingContent,
        ),
        (
            "transmission_summary_suspected",
            SummaryState::Suspected { verdict: None },
        ),
        (
            "transmission_summary_discarded",
            SummaryState::Discarded {
                verdict: Some(Verdict::FalseDetection),
            },
        ),
        (
            "transmission_summary_confirmed",
            SummaryState::Confirmed {
                delivery: delivery(),
                verdict: None,
            },
        ),
        (
            "transmission_summary_classified",
            SummaryState::Classified {
                delivery: delivery(),
                topic: TopicUnder::Topic(topic()),
                verdict: Some(Verdict::Genuine),
            },
        ),
        (
            "transmission_summary_aggregated",
            SummaryState::Aggregated {
                delivery: delivery(),
                topic: TopicUnder::Outlier,
                verdict: None,
            },
        ),
    ]
    .into_iter()
    .map(|(name, state)| (name, declared(state)))
    .collect()
}

#[test]
fn transmission_summaries_golden_in_every_state() {
    for (name, state) in every_state() {
        assert_golden(AREA, name, &summary(state));
    }
}

#[test]
fn topic_under_golden_in_every_variant() {
    fn declared(topic: TopicUnder) -> TopicUnder {
        match topic {
            TopicUnder::Topic(_) | TopicUnder::Outlier | TopicUnder::Unassigned => topic,
        }
    }
    let every = [
        TopicUnder::Topic(topic()),
        TopicUnder::Outlier,
        TopicUnder::Unassigned,
    ]
    .map(declared);
    assert_golden(AREA, "topic_under_every_variant", &every.to_vec());
}

/// `QueryApi::transmissions_by_id`'s page: with a next cursor, and the
/// last page.
#[test]
fn transmission_pages_golden() {
    let next: Cursor<TransmissionList> =
        Cursor::from_token("dHJhbnNtaXNzaW9ucy1hZnRlci0wMUo5".into()).expect("URL-safe base64");
    let rows = every_state()
        .into_iter()
        .skip(4)
        .take(2)
        .map(|(_, state)| summary(state))
        .collect();
    let page = TransmissionPage {
        topic_version: TopicModelVersion(4),
        page: Page::more(
            PageSize::new(2).expect("a valid size"),
            NonEmpty::from_vec(rows).expect("two rows"),
            next,
        )
        .expect("two rows fit a page of two"),
    };
    assert_golden(AREA, "transmission_page", &page);
    let last = TransmissionPage {
        topic_version: TopicModelVersion(4),
        page: Page::last(
            PageSize::new(50).expect("a valid size"),
            vec![summary(SummaryState::Detected)],
        )
        .expect("one row fits"),
    };
    assert_golden(AREA, "transmission_page_last", &last);
}

/// A selection is the ids, newest first, each once.
#[test]
fn transmission_selection_request_golden() {
    let ids = [ULID_A, ULID_D, ULID_B, ULID_A].map(|text| id(TransmissionId::from_ulid_text, text));
    let selection = TransmissionSelection::new(ids.to_vec()).expect("three distinct ids");
    assert_eq!(selection.ids().len(), 3);
    assert_request_golden(AREA, "transmission_selection", &selection);
}

#[test]
fn transmission_selections_are_decoded_through_their_constructor() {
    assert_rejected::<TransmissionSelection>("[]", "invalid transmission selection: Empty");
    let over: Vec<TransmissionId> = (1..=u128::try_from(TransmissionSelection::MAX + 1)
        .expect("fits"))
        .map(TransmissionId::from_ulid)
        .collect();
    let json = serde_json::to_string(&over).expect("ids encode");
    assert_rejected::<TransmissionSelection>(
        &json,
        "invalid transmission selection: TooMany { max: 100000, got: 100001 }",
    );
    assert_rejected::<TransmissionSelection>(r#"["not-an-id"]"#, "invalid ULID text");
    assert_rejected::<TransmissionSelection>(
        &format!(r#"{{"ids": ["{ULID_C}"]}}"#),
        "invalid type: map, expected a sequence",
    );
}

#[test]
fn transmission_rows_refuse_unknown_fields_and_variants() {
    let classified = summary(SummaryState::Classified {
        delivery: delivery(),
        topic: TopicUnder::Unassigned,
        verdict: None,
    });
    assert_rejected::<TransmissionSummary>(
        &edited(&classified, |json| json["from"] = field(json, "to").clone()),
        "unknown field `from`",
    );
    assert_rejected::<SummaryState>(
        r#"{"type": "settled", "data": null}"#,
        "unknown variant `settled`",
    );
    assert_rejected::<SummaryState>(
        r#"{"type": "suspected", "data": {"verdict": null, "since": "2026-10-04T09:16:40.002513Z"}}"#,
        "unknown field `since`",
    );
    assert_rejected::<TopicUnder>(r#"{"type": "noise"}"#, "unknown variant `noise`");
    assert_rejected::<Delivery>(
        &edited(&delivery(), |json| *field(json, "matched_bytes") = 0.into()),
        "nonzero",
    );
    assert_rejected::<TransmissionPage>(
        &format!(
            r#"{{"topic_version": 4, "page": {{"items": [], "next": null}}, "watermark": "{}"}}"#,
            "2026-10-04T12:00:00.000000Z"
        ),
        "unknown field `watermark`",
    );
}

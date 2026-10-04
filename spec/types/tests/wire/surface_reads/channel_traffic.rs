//! A channel's cross-agent transmissions on the wire:
//! `QueryApi::channel_transmissions` (a `ChannelTransmissionFilter` in, a
//! `ChannelTransmissionPage` of `ChannelTransmission` rows out).

use serde_json::json;

use super::super::harness::{assert_golden, assert_rejected, assert_request_golden};
use super::super::{ULID_B, ts};
use super::fixtures::{co_access, coder, confirmed, edited, field, planner, transmission, wiki};
use crate::aggregates::topic::TopicModelVersion;
use crate::aliases::NoAliases;
use crate::derived::flow::channel::confirmation::Confirmation;
use crate::derived::flow::transmission::TransmissionState;
use crate::derived::flow::verdict::Verdict;
use crate::interfaces::l8_surface::channel_traffic::{
    ChannelTransmission, ChannelTransmissionFilter, ChannelTransmissionPage,
};
use crate::interfaces::l8_surface::summary::TopicUnder;
use crate::paging::{ChannelTransmissionList, Cursor, Page, PageSize};
use crate::support::NonEmpty;

const AREA: &str = "surface_reads/channel_traffic";

/// The coder read the wiki page the planner wrote; no content match yet.
fn suspected() -> ChannelTransmission {
    let state = TransmissionState::Suspected {
        co_access: NonEmpty::new(co_access()),
        since: ts("2026-10-04T09:21:40.002513Z"),
    };
    ChannelTransmission::of(
        &transmission(state),
        NoAliases,
        |_| None,
        |_| TopicUnder::Unassigned,
    )
    .expect("the planner and the coder are two agents")
}

/// The same transmission once the planner's text was found in the read.
fn confirmed_row() -> ChannelTransmission {
    ChannelTransmission::of(
        &transmission(TransmissionState::Confirmed(confirmed())),
        NoAliases,
        |_| Some(Verdict::Genuine),
        |_| TopicUnder::Unassigned,
    )
    .expect("the planner and the coder are two agents")
}

#[test]
fn channel_transmissions_golden() {
    let row = suspected();
    assert_eq!(row.senders().first(), &planner());
    assert_eq!(row.confirmation(), Confirmation::Unconfirmed);
    assert_golden(AREA, "channel_transmission_suspected", &row);
    let row = confirmed_row();
    assert_eq!(row.confirmation(), Confirmation::Confirmed);
    assert_golden(AREA, "channel_transmission_confirmed", &row);
    let next: Cursor<ChannelTransmissionList> =
        Cursor::from_token("Y2hhbm5lbC10cmFuc21pc3Npb25z".into()).expect("URL-safe base64");
    let page = ChannelTransmissionPage {
        channel: wiki(),
        topic_version: TopicModelVersion(4),
        page: Page::more(
            PageSize::new(1).expect("a valid size"),
            NonEmpty::new(suspected()),
            next,
        )
        .expect("one row fits a page of one"),
    };
    assert_golden(AREA, "channel_transmission_page", &page);
}

/// What a client sends to list a channel's transmissions: all of them, or
/// the review list of an unconfirmed channel.
#[test]
fn channel_transmission_filters_golden() {
    assert_request_golden(
        AREA,
        "channel_transmission_filter_all",
        &ChannelTransmissionFilter::default(),
    );
    assert_request_golden(
        AREA,
        "channel_transmission_filter_unconfirmed",
        &ChannelTransmissionFilter {
            confirmation: Some(Confirmation::Unconfirmed),
        },
    );
    assert_rejected::<ChannelTransmissionFilter>(
        r#"{"confirmation": null, "verdict": null}"#,
        "unknown field `verdict`",
    );
}

#[test]
fn channel_transmission_decoding_checks_its_senders() {
    let reader = serde_json::to_value(coder()).expect("an id encodes");
    let other = json!(super::super::ULID_A);
    // Two senders out of id order.
    assert_rejected::<ChannelTransmission>(
        &edited(&suspected(), |json| {
            *field(json, "senders") = json!([ULID_B, other.clone()]);
        }),
        "invalid channel transmission: SendersUnordered",
    );
    // A repeated sender.
    assert_rejected::<ChannelTransmission>(
        &edited(&suspected(), |json| {
            *field(json, "senders") = json!([other.clone(), other.clone()]);
        }),
        "invalid channel transmission: SendersUnordered",
    );
    // The reader as a sender: a transmission within one agent.
    assert_rejected::<ChannelTransmission>(
        &edited(&suspected(), |json| {
            *field(json, "senders") = json!([reader.clone()]);
        }),
        "invalid channel transmission: SenderIsReader",
    );
    // A confirmed row whose senders are not exactly its delivery's sender.
    assert_rejected::<ChannelTransmission>(
        &edited(&confirmed_row(), |json| {
            *field(json, "senders") = json!([super::super::ULID_C]);
        }),
        "invalid channel transmission: SendersNotDelivery",
    );
    assert_rejected::<ChannelTransmission>(
        &edited(&suspected(), |json| *field(json, "senders") = json!([])),
        "invalid",
    );
    assert_rejected::<ChannelTransmission>(
        &edited(&suspected(), |json| json["reader"] = reader.clone()),
        "unknown field `reader`",
    );
}

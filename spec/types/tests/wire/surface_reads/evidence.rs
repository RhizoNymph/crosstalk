//! Evidence on the wire: `QueryApi::transmission_evidence` (an
//! `ExcerptWindow` in, an `Option<TransmissionEvidence>` out), with its
//! excerpts and access details.

use serde_json::{Value, json};

use super::super::harness::{assert_golden, assert_rejected, assert_request_golden};
use super::fixtures::{
    confirmed, edited, field, notes, page_resource, read, read_at, transmission, write,
};
use crate::aliases::NoAliases;
use crate::derived::flow::transmission::TransmissionState;
use crate::ids::AccessId;
use crate::interfaces::l8_surface::evidence::{
    AccessDetail, InvalidEvidence, MatchQuotes, TransmissionEvidence,
};
use crate::interfaces::l8_surface::excerpt::{Excerpt, ExcerptWindow, Excerpted};
use crate::support::ByteRange;

const AREA: &str = "surface-reads/evidence";

/// The planner's text as it wrote it, and the coder's tool result that
/// quotes it at bytes 12..59.
const ORIGIN: &str = "Release plan: ship v2.4 on Friday after QA signs off Thursday. Owners below.";
const READ: &str = "(page body) ship v2.4 on Friday after QA signs off Thursday. (edited 09:15)";

fn excerpt(text: &str, start: u32, end: u32, window: ExcerptWindow) -> Excerpt {
    let range = ByteRange::new(start, end).expect("not empty");
    Excerpt::cut(text, range, window).expect("the range fits the text")
}

fn window() -> ExcerptWindow {
    ExcerptWindow::new(8).expect("within the maximum")
}

fn origin_excerpt() -> Excerpt {
    excerpt(ORIGIN, 14, 61, window())
}

fn read_excerpt() -> Excerpt {
    let range = read_at().range;
    excerpt(READ, range.start(), range.end(), window())
}

fn shown() -> MatchQuotes {
    MatchQuotes {
        origin: Excerpted::Shown(origin_excerpt()),
        read: Excerpted::Shown(read_excerpt()),
    }
}

fn read_dropped() -> MatchQuotes {
    MatchQuotes {
        origin: Excerpted::Shown(origin_excerpt()),
        read: Excerpted::BodyDropped {
            message: read_at().part.message,
        },
    }
}

fn detail(id: AccessId) -> Result<AccessDetail, InvalidEvidence> {
    let access = if id == write().id { write() } else { read() };
    AccessDetail::new(access, page_resource(), NoAliases)
}

fn evidence(quotes: MatchQuotes) -> TransmissionEvidence {
    TransmissionEvidence::assemble(
        transmission(TransmissionState::Confirmed(confirmed())),
        |_| Ok::<_, InvalidEvidence>(quotes.clone()),
        detail,
    )
    .expect("the records belong together")
}

#[test]
fn excerpt_window_request_golden() {
    assert_request_golden(AREA, "excerpt_window_default", &ExcerptWindow::DEFAULT);
    assert_request_golden(
        AREA,
        "excerpt_window_match_only",
        &ExcerptWindow::MATCH_ONLY,
    );
}

#[test]
fn excerpts_golden() {
    let excerpt = origin_excerpt();
    assert_eq!(
        excerpt.matched(),
        "ship v2.4 on Friday after QA signs off Thursday"
    );
    assert_eq!(excerpt.part_len(), 76);
    assert_golden(AREA, "excerpt", &excerpt);
    fn declared(excerpted: Excerpted) -> Excerpted {
        match excerpted {
            Excerpted::Shown(_) | Excerpted::BodyDropped { .. } => excerpted,
        }
    }
    let quotes = read_dropped();
    assert_golden(
        AREA,
        "excerpted_every_variant",
        &vec![declared(quotes.origin), declared(quotes.read)],
    );
}

#[test]
fn access_detail_golden() {
    let detail = detail(read().id).expect("the read's own resource");
    assert_golden(AREA, "access_detail", &detail);
}

/// `QueryApi::transmission_evidence`: both bodies held, the reader's
/// dropped by retention, and an unknown transmission.
#[test]
fn transmission_evidence_golden() {
    let both = evidence(shown());
    assert_eq!(both.matches().len(), 1);
    assert_eq!(both.accesses().len(), 2);
    assert_golden(AREA, "transmission_evidence_shown", &Some(both));
    assert_golden(
        AREA,
        "transmission_evidence_body_dropped",
        &Some(evidence(read_dropped())),
    );
    assert_golden(
        AREA,
        "transmission_evidence_unknown",
        &None::<TransmissionEvidence>,
    );
}

#[test]
fn excerpts_are_decoded_through_their_constructor() {
    let excerpt = |text: &str, start: u32, end: u32, before: u64, after: u64| {
        json!({
            "text": text,
            "highlight": {"start": start, "end": end},
            "elided_before": before,
            "elided_after": after,
            "highlight_cut": 0,
        })
        .to_string()
    };
    assert_rejected::<Excerpt>(
        &excerpt("café au lait", 0, 4, 0, 0),
        "invalid excerpt: NotCharBoundary { at: 4 }",
    );
    assert_rejected::<Excerpt>(
        &excerpt("café au lait", 3, 3, 0, 0),
        "invalid excerpt: EmptyHighlight { start: 3, end: 3 }",
    );
    assert_rejected::<Excerpt>(
        &excerpt("ship v2.4", 0, 4, u64::from(u32::MAX), 0),
        "invalid excerpt: CountsOverflow",
    );
    assert_rejected::<Excerpt>(
        &excerpt("ship v2.4", 0, 4, 0, u64::MAX),
        "invalid excerpt: CountsOverflow",
    );
    assert_rejected::<Excerpt>(
        &excerpt("ship", 0, 9, 0, 0),
        "invalid excerpt: OutsideText { end: 9, len: 4 }",
    );
    assert_rejected::<Excerpt>(
        &edited(&origin_excerpt(), |json| {
            *field(json, "highlight_cut") = 3.into()
        }),
        "invalid excerpt: ContextAfterCut",
    );
    assert_rejected::<Excerpt>(
        &edited(&origin_excerpt(), |json| json["part_len"] = 76.into()),
        "unknown field `part_len`",
    );
}

#[test]
fn excerpt_windows_are_decoded_through_their_constructor() {
    assert_rejected::<ExcerptWindow>(
        r#"{"context": 2049}"#,
        "invalid excerpt window: InvalidWindow { max: 2048, got: 2049 }",
    );
    assert_rejected::<ExcerptWindow>(r#"{"context": 70000}"#, "invalid value");
    assert_rejected::<ExcerptWindow>(r#"{"context": 256, "lines": 3}"#, "unknown field `lines`");
}

#[test]
fn excerpted_and_quotes_refuse_unknown_variants_and_fields() {
    assert_rejected::<Excerpted>(r#"{"type": "redacted"}"#, "unknown variant `redacted`");
    assert_rejected::<MatchQuotes>(
        &edited(&read_dropped(), |json| json["both"] = Value::Bool(true)),
        "unknown field `both`",
    );
}

#[test]
fn access_details_are_decoded_through_their_constructor() {
    let detail = detail(write().id).expect("the write's own resource");
    let other = serde_json::to_value(notes()).expect("encodes");
    assert_rejected::<AccessDetail>(
        &edited(&detail, |json| *field(json, "resource") = other),
        "invalid access detail: ResourceMismatch",
    );
    assert_rejected::<AccessDetail>(
        &edited(&detail, |json| json["canonical"] = Value::Bool(true)),
        "unknown field `canonical`",
    );
}

/// Decoded evidence lists exactly what its transmission names, in order.
#[test]
fn transmission_evidence_is_decoded_through_assemble() {
    let evidence = evidence(shown());
    assert_rejected::<TransmissionEvidence>(
        &edited(&evidence, |json| *field(json, "matches") = json!([])),
        "invalid transmission evidence: MatchCount { expected: 1, got: 0 }",
    );
    assert_rejected::<TransmissionEvidence>(
        &edited(&evidence, |json| {
            let matched = &mut field(json, "matches")[0]["content_match"]["matched_bytes"];
            *matched = 45.into();
        }),
        "invalid transmission evidence: OtherMatch { index: 0 }",
    );
    assert_rejected::<TransmissionEvidence>(
        &edited(&evidence, |json| {
            let accesses = field(json, "accesses");
            let first = accesses[0].clone();
            *accesses = json!([first]);
        }),
        "invalid transmission evidence: AccessCount { got: 1 }",
    );
    assert_rejected::<TransmissionEvidence>(
        &edited(&evidence, |json| {
            let accesses = field(json, "accesses");
            let first = accesses[0].clone();
            *accesses = json!([first.clone(), accesses[1].clone(), first]);
        }),
        "invalid transmission evidence: AccessCount { got: 3 }",
    );
    assert_rejected::<TransmissionEvidence>(
        &edited(&evidence, |json| {
            let accesses = field(json, "accesses");
            *accesses = json!([accesses[1].clone(), accesses[0].clone()]);
        }),
        "invalid transmission evidence: Invalid(WrongAccess",
    );
    assert_rejected::<TransmissionEvidence>(
        &edited(&evidence, |json| json["verdicts"] = json!([])),
        "unknown field `verdicts`",
    );
}

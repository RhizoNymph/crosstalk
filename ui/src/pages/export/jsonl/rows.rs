//! One JSON object per export row, and the shared value encodings (ids as
//! ULID text, times as RFC 3339, routes as URL route text, windows as
//! `{start, end}`).
//!
//! Every row object starts with `"type": "row"` and the dataset's code;
//! content columns are `null` when the request did not include content.

use crosstalk_spec::aggregates::edge::RouteKind;
use crosstalk_spec::aggregates::quality::{MatchClass, QualityMatch};
use crosstalk_spec::derived::flow::access::AccessKind;
use crosstalk_spec::derived::flow::verdict::Verdict;
use crosstalk_spec::derived::provenance::matching::CarrierKind;
use crosstalk_spec::interfaces::l8_surface::excerpt::Excerpted;
use crosstalk_spec::interfaces::l8_surface::export::rows::{
    AccessRow, EdgeRow, LabelContent, MatchText, PointRow, TopicRow, TransmissionRow, VerdictRow,
};
use crosstalk_spec::interfaces::l8_surface::export::{ExportDatasetKind, ExportRow};
use crosstalk_spec::interfaces::l8_surface::summary::{TopicUnder, TransmissionStateKind};
use crosstalk_spec::support::{Blake3, TimeWindow, Timestamp};
use serde_json::{Value, json};

use crate::url::route::{encode as route_text, encode_kind};
use crate::url::ulid::UlidId;
use crate::url::view_state::format_time;

pub fn dataset_code(kind: ExportDatasetKind) -> &'static str {
    match kind {
        ExportDatasetKind::Transmissions => "transmissions",
        ExportDatasetKind::Edges => "edges",
        ExportDatasetKind::Accesses => "accesses",
        ExportDatasetKind::Topics => "topics",
        ExportDatasetKind::Projection => "projection",
        ExportDatasetKind::Verdicts => "verdicts",
    }
}

pub fn id(id: impl UlidId) -> Value {
    Value::String(id.to_ulid())
}

pub fn time(at: Timestamp) -> Value {
    Value::String(format_time(at))
}

pub fn window(window: TimeWindow) -> Value {
    json!({ "start": time(window.start()), "end": time(window.end()) })
}

pub fn hex(digest: &Blake3) -> String {
    digest
        .as_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

pub fn route_kind(kind: RouteKind) -> &'static str {
    encode_kind(kind)
}

fn class(class: MatchClass) -> &'static str {
    match class {
        MatchClass::Exact => "exact",
        MatchClass::Normalized => "normalized",
        MatchClass::Decoded => "decoded",
        MatchClass::Semantic => "semantic",
    }
}

fn carrier(carrier: CarrierKind) -> &'static str {
    match carrier {
        CarrierKind::ToolResult => "tool_result",
        CarrierKind::UserTurn => "user_turn",
        CarrierKind::SystemPrompt => "system_prompt",
        CarrierKind::ReaderOutput => "reader_output",
    }
}

fn verdict(verdict: Option<Verdict>) -> Value {
    match verdict {
        None => Value::Null,
        Some(Verdict::Genuine) => json!("genuine"),
        Some(Verdict::FalseDetection) => json!("false_detection"),
    }
}

fn state(kind: TransmissionStateKind) -> &'static str {
    match kind {
        TransmissionStateKind::Detected => "detected",
        TransmissionStateKind::AwaitingContent => "awaiting_content",
        TransmissionStateKind::Suspected => "suspected",
        TransmissionStateKind::Discarded => "discarded",
        TransmissionStateKind::Confirmed => "confirmed",
        TransmissionStateKind::Classified => "classified",
        TransmissionStateKind::Aggregated => "aggregated",
    }
}

fn topic_under(topic: Option<TopicUnder>) -> Value {
    match topic {
        None => Value::Null,
        Some(TopicUnder::Topic(topic)) => json!({ "topic": id(topic) }),
        Some(TopicUnder::Outlier) => json!("outlier"),
        Some(TopicUnder::Unassigned) => json!("unassigned"),
    }
}

/// One quoted side of a match: the matched range alone, or the body
/// retention dropped.
fn quote(quote: &Excerpted) -> Value {
    match quote {
        Excerpted::Shown(excerpt) => {
            let highlight = excerpt.highlight();
            json!({
                "status": "shown",
                "text": excerpt.text(),
                "highlight": [highlight.start, highlight.end],
                "elided_before": excerpt.elided_before(),
                "elided_after": excerpt.elided_after(),
                "highlight_cut": excerpt.highlight_cut(),
            })
        }
        Excerpted::BodyDropped { message } => json!({
            "status": "body dropped",
            "message": hex(message.digest()),
        }),
    }
}

fn match_text(text: &MatchText) -> Value {
    json!({
        "class": class(text.class),
        "origin": quote(&text.quotes.origin),
        "read": quote(&text.quotes.read),
    })
}

fn label(content: Option<&LabelContent>) -> Value {
    content.map_or(Value::Null, |content| json!(content.topic_label))
}

fn transmission(row: &TransmissionRow) -> Value {
    let summary = row.summary();
    let delivery = row.delivery();
    let content = row.content().map_or(Value::Null, |content| {
        json!({
            "topic_label": content.topic_label,
            "matches": content.matches.iter().map(match_text).collect::<Vec<_>>(),
        })
    });
    json!({
        "id": id(summary.id),
        "from": id(delivery.from),
        "to": id(summary.to),
        "route": route_text(&summary.route),
        "route_kind": route_kind(RouteKind::from(&summary.route)),
        "opened_at": time(summary.opened_at),
        "confirmed_at": time(delivery.confirmed_at),
        "state": state(summary.state.kind()),
        "matched_bytes": delivery.matched_bytes.get(),
        "topic": topic_under(summary.state.topic()),
        "verdict": verdict(summary.state.verdict()),
        "strongest": class(row.strongest()),
        "content": content,
    })
}

fn edge(row: &EdgeRow) -> Value {
    json!({
        "bucket": window(row.bucket),
        "from": id(row.edge.from()),
        "to": id(row.edge.to()),
        "route": route_text(row.edge.route()),
        "route_kind": route_kind(RouteKind::from(row.edge.route())),
        "topic": row.topic.map(id),
        "transmissions": row.transmissions.get(),
        "matched_bytes": row.matched_bytes.get(),
        "topic_label": label(row.content.as_ref()),
    })
}

fn access(row: &AccessRow) -> Value {
    json!({
        "bucket": window(row.bucket),
        "agent": id(row.agent),
        "channel": id(row.channel),
        "op": match row.op {
            AccessKind::Write => "write",
            AccessKind::Read => "read",
        },
        "accesses": row.accesses.get(),
    })
}

fn topic(row: &TopicRow) -> Value {
    let (label, terms) = match &row.content {
        None => (Value::Null, Value::Null),
        Some(content) => (json!(content.label), json!(content.terms)),
    };
    json!({
        "topic": id(row.topic),
        "transmissions": row.transmissions,
        "matched_bytes": row.matched_bytes,
        "label": label,
        "terms": terms,
    })
}

fn point(row: &PointRow) -> Value {
    let point = &row.point;
    json!({
        "index": row.index,
        "transmission": id(point.transmission()),
        "from": id(point.from()),
        "to": id(point.to()),
        "route_kind": route_kind(point.route().kind()),
        "topic": point.topic().map(id),
        "confirmed_at": time(point.confirmed_at()),
        "x": point.x(),
        "y": point.y(),
        "topic_label": label(row.content.as_ref()),
    })
}

fn verdict_row(row: &VerdictRow) -> Value {
    let call = match row.call {
        QualityMatch::Content {
            class: match_class,
            carrier: carrier_kind,
        } => {
            json!({ "kind": "content", "class": class(match_class), "carrier": carrier(carrier_kind) })
        }
        QualityMatch::Suspected => json!({ "kind": "suspected" }),
        QualityMatch::Discarded => json!({ "kind": "discarded" }),
    };
    json!({
        "transmission": id(row.transmission),
        "revision": row.revision.get().get(),
        "route_kind": route_kind(row.route_kind),
        "call": call,
        "verdict": verdict(row.verdict),
        "by": id(row.by),
        "at": time(row.at),
        "note": row.note,
    })
}

/// The row's line: `{"type": "row", "dataset": <code>, …its fields}`.
pub fn row(row: &ExportRow) -> Value {
    let mut fields = match row {
        ExportRow::Transmission(row) => transmission(row),
        ExportRow::Edge(row) => edge(row),
        ExportRow::Access(row) => access(row),
        ExportRow::Topic(row) => topic(row),
        ExportRow::Point(row) => point(row),
        ExportRow::Verdict(row) => verdict_row(row),
    };
    let mut line = serde_json::Map::new();
    line.insert("type".to_owned(), json!("row"));
    line.insert("dataset".to_owned(), json!(dataset_code(row.kind())));
    if let Value::Object(fields) = &mut fields {
        line.append(fields);
    }
    Value::Object(line)
}

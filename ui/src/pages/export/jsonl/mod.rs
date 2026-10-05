//! An export as JSON Lines: the header line, one line per row, the
//! trailer line (the shapes are listed in `docs/features/ui.md`, "Export
//! downloads"), served as an `application/x-ndjson` attachment.
//!
//! The body is read from the export stream to its trailer before the
//! response is sent: Topcoat 0.9 routes answer with a whole body, and a
//! streamed one would need an `http_body::Body`, which the UI crate does
//! not depend on. The fixture's exports are a few megabytes at most.

pub mod rows;

use crosstalk_spec::aggregates::edge::TopologyFilter;
use crosstalk_spec::aggregates::filter::{FalseDetections, TopicVersionSelector};
use crosstalk_spec::interfaces::l8_surface::QueryError;
use crosstalk_spec::interfaces::l8_surface::export::{
    Export, ExportBasis, ExportDataset, ExportEnd, ExportFailure, ExportFormat, ExportHeader,
    ExportStep, ExportStream, ExportTrailer,
};
use serde_json::{Value, json};
use topcoat::context::Cx;
use topcoat::router::HeaderValue;
use topcoat::router::header::{CONTENT_DISPOSITION, CONTENT_TYPE};
use topcoat::router::response::{IntoResponse, Response};

use self::rows::{dataset_code, hex, id, route_kind, time, window};
use crate::error::UiError;
use crate::url::ulid::UlidId;

pub const CONTENT_TYPE_NDJSON: &str = "application/x-ndjson";

/// A finished JSONL export, ready to send as a download.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Download {
    filename: String,
    body: Vec<u8>,
}

impl Download {
    pub fn filename(&self) -> &str {
        &self.filename
    }
}

impl IntoResponse for Download {
    fn into_response(self, cx: &Cx) -> topcoat::Result<Response> {
        let disposition =
            HeaderValue::try_from(format!("attachment; filename=\"{}\"", self.filename))?;
        (
            [
                (CONTENT_TYPE, HeaderValue::from_static(CONTENT_TYPE_NDJSON)),
                (CONTENT_DISPOSITION, disposition),
            ],
            self.body,
        )
            .into_response(cx)
    }
}

fn encode_error(error: &serde_json::Error) -> UiError {
    UiError::Query(QueryError::Store {
        reason: format!("export line not encoded: {error}"),
    })
}

fn push_line(out: &mut Vec<u8>, value: &Value) -> Result<(), UiError> {
    serde_json::to_writer(&mut *out, value).map_err(|e| encode_error(&e))?;
    out.push(b'\n');
    Ok(())
}

/// Reads `export` to its trailer and encodes every line.
pub async fn download<S: ExportStream>(export: Export<S>) -> Result<Download, UiError> {
    let Export { header, rows } = export;
    let filename = format!(
        "crosstalk-{}-{}.jsonl",
        dataset_code(header.request().dataset().kind()),
        header.id().to_ulid()
    );
    let mut body = Vec::new();
    push_line(&mut body, &header_line(&header))?;
    let mut rows = rows;
    loop {
        match rows.next().await {
            ExportStep::Row(row, rest) => {
                push_line(&mut body, &rows::row(&row))?;
                rows = rest;
            }
            ExportStep::End(trailer) => {
                push_line(&mut body, &trailer_line(&trailer))?;
                return Ok(Download { filename, body });
            }
        }
    }
}

fn filter(filter: &TopologyFilter) -> Value {
    json!({
        "agents": filter.agents.iter().copied().map(id).collect::<Vec<_>>(),
        "channels": filter.channels.iter().copied().map(id).collect::<Vec<_>>(),
        "route_kinds": filter.route_kinds.iter().copied().map(route_kind).collect::<Vec<_>>(),
        "topics": filter.topics.iter().copied().map(id).collect::<Vec<_>>(),
        "topic_version": match filter.topic_version {
            TopicVersionSelector::Current => json!("current"),
            TopicVersionSelector::Pinned(version) => json!(version.0),
        },
        "false_detections": match filter.false_detections {
            FalseDetections::Include => "include",
            FalseDetections::Exclude => "exclude",
        },
    })
}

fn selection(dataset: &ExportDataset) -> Value {
    match dataset {
        ExportDataset::Transmissions(scope) if scope.states.is_confirmed() => json!({
            "window": window(scope.window),
            "filter": filter(&scope.filter),
        }),
        ExportDataset::Transmissions(scope) => json!({
            "window": window(scope.window),
            "filter": filter(&scope.filter),
            "states": scope.states.iter().collect::<Vec<_>>(),
        }),
        ExportDataset::Edges(scope)
        | ExportDataset::Accesses(scope)
        | ExportDataset::Topics(scope) => json!({
            "window": window(scope.window),
            "filter": filter(&scope.filter),
        }),
        ExportDataset::Projection(projection) => json!({ "projection": id(*projection) }),
        ExportDataset::Verdicts(verdicts) => json!({ "window": window(*verdicts) }),
    }
}

fn basis(basis: &ExportBasis) -> Value {
    match basis {
        ExportBasis::Scoped {
            topic_version,
            filter: pinned,
            settled,
        } => json!({
            "kind": "scoped",
            "topic_version": topic_version.0,
            "filter": filter(pinned),
            "settled": settled.map(window),
        }),
        ExportBasis::Verdicts { settled } => json!({
            "kind": "verdicts",
            "settled": settled.map(window),
        }),
        ExportBasis::Projection {
            projection,
            spec,
            fitted,
        } => {
            let params = spec.params();
            json!({
                "kind": "projection",
                "projection": id(*projection),
                "window": window(spec.window()),
                "filter": filter(spec.filter()),
                "topic_version": spec.topic_version().0,
                "params": {
                    "limit": params.limit().get().get(),
                    "neighbors": params.neighbors(),
                    "min_dist": params.min_dist(),
                    "seed": params.seed(),
                },
                "embedding_model": {
                    "name": spec.embedding_model().name,
                    "dimension": spec.embedding_model().dimension.get(),
                },
                "fitted": {
                    "started_at": time(fitted.started_at),
                    "fitted_at": time(fitted.fitted_at),
                    "watermark": time(fitted.watermark.at()),
                    "matching": fitted.matching,
                    "points": fitted.points,
                },
            })
        }
    }
}

/// The header line: what the export is and how many rows follow.
pub fn header_line(header: &ExportHeader) -> Value {
    let request = header.request();
    let model = header.embedding_model();
    json!({
        "type": "header",
        "export": id(header.id()),
        "dataset": dataset_code(request.dataset().kind()),
        "selection": selection(request.dataset()),
        "format": match request.format() {
            ExportFormat::Jsonl => "jsonl",
            ExportFormat::Parquet => "parquet",
        },
        "include_content": request.include_content(),
        "by": id(header.by()),
        "started_at": time(header.started_at()),
        "watermark": time(header.watermark().at()),
        "basis": basis(header.basis()),
        "embedding_model": { "name": model.name, "dimension": model.dimension.get() },
        "gateway": header.gateway().as_str(),
        "rows": header.rows(),
    })
}

fn failure(failure: &ExportFailure) -> Value {
    match failure {
        ExportFailure::Store { reason } => json!({ "kind": "store", "reason": reason }),
        ExportFailure::VersionNotRetained { version } => {
            json!({ "kind": "version_not_retained", "version": version.0 })
        }
        ExportFailure::CountMismatch { planned, produced } => {
            json!({ "kind": "count_mismatch", "planned": planned, "produced": produced })
        }
        ExportFailure::InvalidRow { index, refused } => {
            json!({ "kind": "invalid_row", "index": index, "refused": format!("{refused:?}") })
        }
    }
}

/// The trailer line: the rows sent, their digest and how the export ended.
pub fn trailer_line(trailer: &ExportTrailer) -> Value {
    let end = match trailer.end() {
        ExportEnd::Complete => json!({ "status": "complete" }),
        ExportEnd::Failed(why) => json!({ "status": "failed", "failure": failure(why) }),
    };
    json!({
        "type": "trailer",
        "export": id(trailer.export()),
        "rows": trailer.rows(),
        "digest": hex(trailer.digest().digest()),
        "end": end,
    })
}

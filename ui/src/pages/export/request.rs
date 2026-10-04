//! The export form's fields, validated into an `ExportRequest`, and the
//! request described in words.

use crosstalk_spec::aggregates::topic::TopicModelVersion;
use crosstalk_spec::interfaces::l8_surface::{Caller, Permission};

use crate::app::can;
use crate::components::{format_time, route_kind_name, short_id};
use crate::contract::research::{ExportDataset, ExportFormat, ExportRequest};
use crate::contract::scope::VerdictFilter;
use crate::error::UiError;
use crate::pages::common::form::{FormFields, invalid};
use crate::url::ulid::UlidId;
use crate::url::view_state::ViewState;
use crosstalk_spec::ids::ProjectionId;
use crosstalk_spec::interfaces::l8_surface::QueryError;

/// A dataset without its projection id: what the form's select offers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DatasetChoice {
    Transmissions,
    Edges,
    Accesses,
    Topics,
    Projection,
    Verdicts,
}

impl DatasetChoice {
    pub const ALL: [Self; 6] = [
        Self::Transmissions,
        Self::Edges,
        Self::Accesses,
        Self::Topics,
        Self::Projection,
        Self::Verdicts,
    ];

    pub fn code(self) -> &'static str {
        match self {
            Self::Transmissions => "transmissions",
            Self::Edges => "edges",
            Self::Accesses => "accesses",
            Self::Topics => "topics",
            Self::Projection => "projection",
            Self::Verdicts => "verdicts",
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::Transmissions => "Transmissions",
            Self::Edges => "Edges (aggregated)",
            Self::Accesses => "Accesses",
            Self::Topics => "Topics",
            Self::Projection => "Projection (by id)",
            Self::Verdicts => "Verdicts",
        }
    }
}

pub const FORMATS: [ExportFormat; 2] = [ExportFormat::Jsonl, ExportFormat::Parquet];

pub fn format_code(format: ExportFormat) -> &'static str {
    match format {
        ExportFormat::Jsonl => "jsonl",
        ExportFormat::Parquet => "parquet",
    }
}

pub fn format_label(format: ExportFormat) -> &'static str {
    match format {
        ExportFormat::Jsonl => "JSON Lines",
        ExportFormat::Parquet => "Parquet",
    }
}

/// Validates a posted export form against the view state. Content needs
/// the `Content` permission; a projection needs its id.
pub fn parse(
    fields: &FormFields,
    state: &ViewState,
    caller: &Caller,
) -> Result<ExportRequest, UiError> {
    let dataset_text = fields.text("dataset").unwrap_or("transmissions");
    let choice = DatasetChoice::ALL
        .into_iter()
        .find(|d| d.code() == dataset_text)
        .ok_or_else(|| invalid("dataset", format!("unknown dataset {dataset_text:?}")))?;
    let dataset = match choice {
        DatasetChoice::Transmissions => ExportDataset::Transmissions,
        DatasetChoice::Edges => ExportDataset::Edges,
        DatasetChoice::Accesses => ExportDataset::Accesses,
        DatasetChoice::Topics => ExportDataset::Topics,
        DatasetChoice::Verdicts => ExportDataset::Verdicts,
        DatasetChoice::Projection => {
            let text = fields
                .text("projection")
                .ok_or_else(|| invalid("projection", "a projection export needs its id"))?;
            ExportDataset::Projection(
                ProjectionId::parse_ulid(text).map_err(|e| invalid("projection", e))?,
            )
        }
    };
    let version = match fields.text("version") {
        None => state.scope.topic_version,
        Some(text) => TopicModelVersion(
            text.parse()
                .map_err(|_| invalid("version", "not a topic model version"))?,
        ),
    };
    let format_text = fields.text("format").unwrap_or("jsonl");
    let format = FORMATS
        .into_iter()
        .find(|f| format_code(*f) == format_text)
        .ok_or_else(|| invalid("format", format!("unknown format {format_text:?}")))?;
    let include_content = match fields.text("content") {
        None => false,
        Some("1") => true,
        Some(_) => return Err(invalid("content", "expected 1")),
    };
    if include_content && !can(caller, Permission::Content) {
        return Err(UiError::Query(QueryError::Forbidden {
            missing: Permission::Content,
        }));
    }
    let mut scope = state.scope.clone();
    scope.topic_version = version;
    Ok(ExportRequest {
        dataset,
        scope,
        format,
        include_content,
    })
}

fn dataset_text(dataset: ExportDataset) -> String {
    match dataset {
        ExportDataset::Transmissions => "transmissions".to_owned(),
        ExportDataset::Edges => "edges".to_owned(),
        ExportDataset::Accesses => "accesses".to_owned(),
        ExportDataset::Topics => "topics".to_owned(),
        ExportDataset::Projection(id) => format!("projection {}", id.to_ulid()),
        ExportDataset::Verdicts => "verdicts".to_owned(),
    }
}

fn ids(list: impl Iterator<Item = String>) -> String {
    let all: Vec<String> = list.map(short_id).collect();
    if all.is_empty() {
        "any".to_owned()
    } else {
        all.join(", ")
    }
}

/// The request as labelled lines, in the order of its fields.
pub fn describe(request: &ExportRequest) -> Vec<(&'static str, String)> {
    let filter = &request.scope.filter;
    vec![
        ("dataset", dataset_text(request.dataset)),
        (
            "window",
            format!(
                "{} → {}",
                format_time(request.scope.window.start()),
                format_time(request.scope.window.end())
            ),
        ),
        (
            "topic version",
            format!("v{}", request.scope.topic_version.0),
        ),
        ("agents", ids(filter.agents.iter().map(|a| a.to_ulid()))),
        ("channels", ids(filter.channels.iter().map(|c| c.to_ulid()))),
        (
            "route kinds",
            if filter.route_kinds.is_empty() {
                "any".to_owned()
            } else {
                filter
                    .route_kinds
                    .iter()
                    .map(|k| route_kind_name(*k))
                    .collect::<Vec<_>>()
                    .join(", ")
            },
        ),
        ("topics", ids(filter.topics.iter().map(|t| t.to_ulid()))),
        (
            "verdicts",
            match filter.verdicts {
                VerdictFilter::IncludeAll => "all".to_owned(),
                VerdictFilter::ExcludeFalseDetections => "excluding false detections".to_owned(),
            },
        ),
        ("format", format_label(request.format).to_owned()),
        (
            "content",
            if request.include_content {
                "included".to_owned()
            } else {
                "structure only".to_owned()
            },
        ),
    ]
}

#[cfg(test)]
mod tests {
    use crosstalk_spec::aggregates::edge::RouteKind;
    use crosstalk_spec::ids::OperatorId;

    use super::*;
    use crate::components::href::tests::state;

    fn caller(permissions: Vec<Permission>) -> Caller {
        crate::testing::caller_of(OperatorId::from_ulid(1), &permissions)
    }

    #[test]
    fn a_full_form_becomes_the_request() {
        let mut state = state();
        state.scope.filter.route_kinds = vec![RouteKind::Channel];
        let fields = FormFields::from_pairs(&[
            ("dataset", "projection"),
            ("projection", "01J9ZQ3W8D0000000000000001"),
            ("version", "1"),
            ("format", "parquet"),
            ("content", "1"),
        ]);
        let request = parse(
            &fields,
            &state,
            &caller(vec![Permission::View, Permission::Content]),
        )
        .expect("request");
        assert_eq!(
            request.dataset,
            ExportDataset::Projection(
                ProjectionId::parse_ulid("01J9ZQ3W8D0000000000000001").expect("id")
            )
        );
        assert_eq!(request.scope.topic_version, TopicModelVersion(1));
        assert_eq!(request.scope.window, state.scope.window);
        assert_eq!(request.scope.filter, state.scope.filter);
        assert_eq!(request.format, ExportFormat::Parquet);
        assert!(request.include_content);
        let lines = describe(&request);
        assert_eq!(lines[0].1, "projection 01J9ZQ3W8D0000000000000001");
        assert!(lines.contains(&("route kinds", "channel".to_owned())));
        assert!(lines.contains(&("content", "included".to_owned())));
    }

    #[test]
    fn defaults_follow_the_view() {
        let request = parse(
            &FormFields::default(),
            &state(),
            &caller(vec![Permission::View]),
        )
        .expect("request");
        assert_eq!(request.dataset, ExportDataset::Transmissions);
        assert_eq!(request.scope, state().scope);
        assert_eq!(request.format, ExportFormat::Jsonl);
        assert!(!request.include_content);
    }

    #[test]
    fn invalid_fields_and_missing_permissions_are_reported() {
        let viewer = caller(vec![Permission::View]);
        let field =
            |pairs: &[(&str, &str)]| match parse(&FormFields::from_pairs(pairs), &state(), &viewer)
            {
                Err(UiError::Field { field, .. }) => Some(field),
                _ => None,
            };
        assert_eq!(field(&[("dataset", "everything")]), Some("dataset"));
        assert_eq!(field(&[("dataset", "projection")]), Some("projection"));
        assert_eq!(field(&[("version", "v2")]), Some("version"));
        assert_eq!(field(&[("format", "csv")]), Some("format"));
        assert_eq!(
            parse(
                &FormFields::from_pairs(&[("content", "1")]),
                &state(),
                &viewer
            ),
            Err(UiError::Query(QueryError::Forbidden {
                missing: Permission::Content
            }))
        );
    }
}

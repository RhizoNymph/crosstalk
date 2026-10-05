//! The export form's fields, validated into the spec's `ExportRequest`.

use crosstalk_spec::aggregates::topic::TopicModelVersion;
use crosstalk_spec::ids::ProjectionId;
use crosstalk_spec::interfaces::l8_surface::Caller;
use crosstalk_spec::interfaces::l8_surface::export::{
    ExportDataset, ExportFormat, ExportRequest, ExportScope, InvalidExportRequest,
};

use crate::error::UiError;
use crate::pages::common::action::require;
use crate::pages::common::form::{FormFields, invalid};
use crate::url::ulid::UlidId;
use crate::url::view_state::ViewState;

/// A dataset without its selection: what the form's select offers.
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
            Self::Edges => "Edges (per bucket)",
            Self::Accesses => "Accesses (per bucket)",
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

/// Validates a posted export form against the view state: the dataset over
/// the view's window and filter, the filter pinned to the chosen topic
/// version (a projection carries its own selection, verdicts the window
/// alone), a format the backend writes (`writes`), and content only for a
/// dataset with content columns. The request's one permission is checked
/// last, so a caller without Content asking for content or a projection is
/// refused before anything is sent.
pub fn parse(
    fields: &FormFields,
    state: &ViewState,
    caller: &Caller,
    writes: &[ExportFormat],
) -> Result<ExportRequest, UiError> {
    let dataset_text = fields.text("dataset").unwrap_or("transmissions");
    let choice = DatasetChoice::ALL
        .into_iter()
        .find(|d| d.code() == dataset_text)
        .ok_or_else(|| invalid("dataset", format!("unknown dataset {dataset_text:?}")))?;
    let version = match fields.text("version") {
        None => state.scope.topic_version,
        Some(text) => TopicModelVersion(
            text.parse()
                .map_err(|_| invalid("version", "not a topic model version"))?,
        ),
    };
    let scope = ExportScope {
        window: state.scope.window,
        filter: state.scope.filter.pinned(version),
    };
    let dataset = match choice {
        DatasetChoice::Transmissions => ExportDataset::Transmissions(scope.into()),
        DatasetChoice::Edges => ExportDataset::Edges(scope),
        DatasetChoice::Accesses => ExportDataset::Accesses(scope),
        DatasetChoice::Topics => ExportDataset::Topics(scope),
        DatasetChoice::Verdicts => ExportDataset::Verdicts(state.scope.window),
        DatasetChoice::Projection => {
            let text = fields
                .text("projection")
                .ok_or_else(|| invalid("projection", "a projection export needs its id"))?;
            ExportDataset::Projection(
                ProjectionId::parse_ulid(text).map_err(|e| invalid("projection", e))?,
            )
        }
    };
    let format_text = fields.text("format").unwrap_or("jsonl");
    let format = FORMATS
        .into_iter()
        .find(|f| format_code(*f) == format_text)
        .ok_or_else(|| invalid("format", format!("unknown format {format_text:?}")))?;
    if !writes.contains(&format) {
        return Err(invalid(
            "format",
            format!("this backend cannot write {}", format_label(format)),
        ));
    }
    let include_content = match fields.text("content") {
        None => false,
        Some("1") => true,
        Some(_) => return Err(invalid("content", "expected 1")),
    };
    let request =
        ExportRequest::new(dataset, format, include_content).map_err(|error| match error {
            InvalidExportRequest::NoContentColumns { .. } => invalid(
                "content",
                format!("{} exports have no content columns", choice.code()),
            ),
            // The form exports the default (confirmed) states only.
            InvalidExportRequest::ContentWithUnconfirmedStates => invalid(
                "content",
                "unconfirmed transmissions have no content columns".to_owned(),
            ),
        })?;
    require(caller, request.required_permission())?;
    Ok(request)
}

#[cfg(test)]
mod tests {
    use crosstalk_spec::aggregates::edge::RouteKind;
    use crosstalk_spec::aggregates::filter::TopicVersionSelector;
    use crosstalk_spec::ids::OperatorId;
    use crosstalk_spec::interfaces::l8_surface::{Permission, QueryError};

    use super::*;
    use crate::components::href::tests::state;

    const JSONL: &[ExportFormat] = &[ExportFormat::Jsonl];

    fn caller(permissions: Vec<Permission>) -> Caller {
        crate::testing::caller_of(OperatorId::from_ulid(1), &permissions)
    }

    #[test]
    fn a_full_form_becomes_the_request() {
        let mut state = state();
        state.scope.filter.route_kinds = vec![RouteKind::Channel];
        let fields = FormFields::from_pairs(&[
            ("dataset", "edges"),
            ("version", "1"),
            ("format", "jsonl"),
            ("content", "1"),
        ]);
        let request = parse(
            &fields,
            &state,
            &caller(vec![Permission::View, Permission::Content]),
            JSONL,
        )
        .expect("request");
        let ExportDataset::Edges(scope) = request.dataset() else {
            panic!("edges");
        };
        assert_eq!(scope.window, state.scope.window);
        assert_eq!(scope.filter.route_kinds, vec![RouteKind::Channel]);
        assert_eq!(
            scope.filter.topic_version,
            TopicVersionSelector::Pinned(TopicModelVersion(1))
        );
        assert_eq!(request.format(), ExportFormat::Jsonl);
        assert!(request.include_content());
        assert_eq!(request.required_permission(), Permission::Content);
    }

    #[test]
    fn defaults_follow_the_view() {
        let request = parse(
            &FormFields::default(),
            &state(),
            &caller(vec![Permission::View]),
            JSONL,
        )
        .expect("request");
        assert_eq!(
            request.dataset(),
            &ExportDataset::Transmissions(
                ExportScope {
                    window: state().scope.window,
                    filter: state().scope.topology_filter(),
                }
                .into()
            )
        );
        assert_eq!(request.format(), ExportFormat::Jsonl);
        assert!(!request.include_content());
    }

    #[test]
    fn projections_and_verdicts_take_their_own_selection() {
        let both = caller(vec![Permission::View, Permission::Content]);
        let request = parse(
            &FormFields::from_pairs(&[
                ("dataset", "projection"),
                ("projection", "01J9ZQ3W8D0000000000000001"),
            ]),
            &state(),
            &both,
            JSONL,
        )
        .expect("request");
        assert_eq!(
            request.dataset(),
            &ExportDataset::Projection(
                ProjectionId::parse_ulid("01J9ZQ3W8D0000000000000001").expect("id")
            )
        );
        let request = parse(
            &FormFields::from_pairs(&[("dataset", "verdicts")]),
            &state(),
            &both,
            JSONL,
        )
        .expect("request");
        assert_eq!(
            request.dataset(),
            &ExportDataset::Verdicts(state().scope.window)
        );
    }

    #[test]
    fn invalid_fields_and_missing_permissions_are_reported() {
        let viewer = caller(vec![Permission::View]);
        let field = |pairs: &[(&str, &str)], writes: &[ExportFormat]| match parse(
            &FormFields::from_pairs(pairs),
            &state(),
            &viewer,
            writes,
        ) {
            Err(UiError::Field { field, .. }) => Some(field),
            _ => None,
        };
        assert_eq!(field(&[("dataset", "everything")], JSONL), Some("dataset"));
        assert_eq!(
            field(&[("dataset", "projection")], JSONL),
            Some("projection")
        );
        assert_eq!(field(&[("version", "v2")], JSONL), Some("version"));
        assert_eq!(field(&[("format", "csv")], JSONL), Some("format"));
        assert_eq!(field(&[("format", "parquet")], JSONL), Some("format"));
        assert_eq!(field(&[("format", "parquet")], &FORMATS), None);
        assert_eq!(
            field(&[("dataset", "accesses"), ("content", "1")], JSONL),
            Some("content"),
            "accesses have no content columns"
        );
        let forbidden = Err(UiError::Query(QueryError::Forbidden {
            missing: Permission::Content,
        }));
        assert_eq!(
            parse(
                &FormFields::from_pairs(&[("content", "1")]),
                &state(),
                &viewer,
                JSONL
            ),
            forbidden
        );
        assert_eq!(
            parse(
                &FormFields::from_pairs(&[
                    ("dataset", "projection"),
                    ("projection", "01J9ZQ3W8D0000000000000001")
                ]),
                &state(),
                &viewer,
                JSONL
            ),
            forbidden,
            "a projection needs Content"
        );
    }
}

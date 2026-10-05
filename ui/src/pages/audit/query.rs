//! The audit page's own query keys: `op` (an operator id, or `config` for
//! the changes config made), `subject` (see [`super::subject`]) and `span`
//! (`all` for every time; otherwise the shared window). Together they are
//! the spec's `AuditFilter`.

use crosstalk_spec::ids::OperatorId;
use crosstalk_spec::interfaces::l8_surface::audit::{AuditAuthor, AuditFilter, AuditSubject};
use topcoat::router::query_params;

use super::subject::{parse_subject, subject_code};
use crate::error::UiError;
use crate::pages::common::form::invalid;
use crate::url::ulid::UlidId;
use crate::url::view_state::ViewState;

/// The `op` value selecting the changes config made.
pub const CONFIG: &str = "config";

#[query_params]
pub struct RawAuditQuery {
    pub op: Option<String>,
    pub subject: Option<String>,
    pub span: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct AuditQuery {
    pub author: Option<AuditAuthor>,
    pub subject: Option<AuditSubject>,
    /// Every entry, rather than those in the shared window.
    pub all_time: bool,
}

/// An `op` value: `config`, or an operator id.
pub fn parse_author(text: &str) -> Result<AuditAuthor, UiError> {
    if text == CONFIG {
        return Ok(AuditAuthor::Config);
    }
    OperatorId::parse_ulid(text)
        .map(AuditAuthor::Operator)
        .map_err(|e| invalid("op", e))
}

pub fn author_code(author: AuditAuthor) -> String {
    match author {
        AuditAuthor::Config => CONFIG.to_owned(),
        AuditAuthor::Operator(id) => id.to_ulid(),
    }
}

impl AuditQuery {
    pub fn parse(raw: &RawAuditQuery) -> Result<Self, UiError> {
        Ok(Self {
            author: raw.op.as_deref().map(parse_author).transpose()?,
            subject: raw
                .subject
                .as_deref()
                .map(parse_subject)
                .transpose()
                .map_err(|e| invalid("subject", e))?,
            all_time: match raw.span.as_deref() {
                None | Some("window") => false,
                Some("all") => true,
                Some(_) => return Err(invalid("span", "expected all or window")),
            },
        })
    }

    pub fn filter(&self, state: &ViewState) -> AuditFilter {
        AuditFilter {
            by: self.author.into_iter().collect(),
            subject: self.subject,
            window: (!self.all_time).then_some(state.scope.window),
        }
    }

    /// The canonical query pairs; empty values are dropped by the link
    /// builder.
    pub fn pairs(&self) -> Vec<(&'static str, String)> {
        vec![
            ("op", self.author.map(author_code).unwrap_or_default()),
            (
                "subject",
                self.subject.map(subject_code).unwrap_or_default(),
            ),
            (
                "span",
                if self.all_time {
                    "all".to_owned()
                } else {
                    String::new()
                },
            ),
        ]
    }

    pub fn with_subject(self, subject: Option<AuditSubject>) -> Self {
        Self { subject, ..self }
    }

    pub fn with_all_time(self, all_time: bool) -> Self {
        Self { all_time, ..self }
    }

    pub fn with_author(self, author: Option<AuditAuthor>) -> Self {
        Self { author, ..self }
    }
}

#[cfg(test)]
mod tests {
    use crosstalk_spec::ids::ChannelId;

    use super::*;
    use crate::components::href::tests::state;

    fn raw(op: Option<&str>, subject: Option<&str>, span: Option<&str>) -> RawAuditQuery {
        RawAuditQuery {
            op: op.map(str::to_owned),
            subject: subject.map(str::to_owned),
            span: span.map(str::to_owned),
        }
    }

    #[test]
    fn default_is_the_shared_window() {
        let query = AuditQuery::parse(&raw(None, None, None)).expect("parse");
        let filter = query.filter(&state());
        assert_eq!(filter.window, Some(state().scope.window));
        assert!(filter.by.is_empty());
        assert!(query.pairs().iter().all(|(_, v)| v.is_empty()));
    }

    #[test]
    fn filters_parse_and_round_trip() {
        let subject = subject_code(AuditSubject::Channel(ChannelId::from_ulid(4)));
        let op = OperatorId::from_ulid(2).to_ulid();
        let query = AuditQuery::parse(&raw(Some(&op), Some(&subject), Some("all"))).expect("parse");
        let filter = query.filter(&state());
        assert_eq!(filter.window, None);
        assert_eq!(
            filter.by,
            vec![AuditAuthor::Operator(OperatorId::from_ulid(2))]
        );
        assert_eq!(
            filter.subject,
            Some(AuditSubject::Channel(ChannelId::from_ulid(4)))
        );
        let pairs = query.pairs();
        assert!(pairs.contains(&("subject", subject)));
        assert!(pairs.contains(&("span", "all".to_owned())));
        let config = AuditQuery::parse(&raw(Some("config"), None, None)).expect("parse");
        assert_eq!(config.filter(&state()).by, vec![AuditAuthor::Config]);
        assert!(config.pairs().contains(&("op", "config".to_owned())));
    }

    #[test]
    fn bad_values_name_their_key() {
        assert!(matches!(
            AuditQuery::parse(&raw(Some("x"), None, None)),
            Err(UiError::Field { field: "op", .. })
        ));
        assert_eq!(
            AuditQuery::parse(&raw(None, None, Some("week"))),
            Err(invalid("span", "expected all or window"))
        );
        assert!(AuditQuery::parse(&raw(None, Some("zz.1"), None)).is_err());
    }
}

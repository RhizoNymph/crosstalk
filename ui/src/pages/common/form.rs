//! Form bodies as raw pairs, and the validators that turn fields into typed
//! values. Every validation failure is a `UiError::InvalidInput` naming
//! the field, so it renders next to the form like a backend error.

use crosstalk_spec::interfaces::l8_surface::PolicyKind;
use crosstalk_spec::support::Similarity;

use crate::error::UiError;
use crate::url::ulid::UlidId;

/// Notes are free text; this bounds what one form can send.
pub const NOTE_MAX_CHARS: usize = 2000;

/// A form body as its pairs, in order. Repeated keys (checkbox groups) are
/// kept, which a struct body cannot do.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Deserialize)]
#[serde(transparent)]
pub struct FormFields(Vec<(String, String)>);

impl FormFields {
    /// Fields as a form would submit them, for prefilling a form.
    pub fn from_pairs(pairs: &[(&str, &str)]) -> Self {
        Self(
            pairs
                .iter()
                .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
                .collect(),
        )
    }

    /// The first value of `key`, trimmed; `None` when absent or blank.
    pub fn text(&self, key: &str) -> Option<&str> {
        self.0
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.trim())
            .filter(|v| !v.is_empty())
    }

    /// Every non-blank value of `key`, trimmed.
    pub fn all<'a>(&'a self, key: &'a str) -> impl Iterator<Item = &'a str> + 'a {
        self.0
            .iter()
            .filter(move |(k, _)| k == key)
            .map(|(_, v)| v.trim())
            .filter(|v| !v.is_empty())
    }
}

pub fn invalid(field: &'static str, reason: impl std::fmt::Display) -> UiError {
    UiError::field(field, reason)
}

pub fn required<'a>(fields: &'a FormFields, field: &'static str) -> Result<&'a str, UiError> {
    fields.text(field).ok_or_else(|| invalid(field, "required"))
}

pub fn id<T: UlidId>(fields: &FormFields, field: &'static str) -> Result<T, UiError> {
    let text = required(fields, field)?;
    T::parse_ulid(text).map_err(|e| invalid(field, e))
}

/// An optional note: blank is `None`, longer than [`NOTE_MAX_CHARS`] fails.
pub fn note(fields: &FormFields, field: &'static str) -> Result<Option<String>, UiError> {
    match fields.text(field) {
        None => Ok(None),
        Some(text) if text.chars().count() > NOTE_MAX_CHARS => Err(invalid(
            field,
            format!("longer than {NOTE_MAX_CHARS} characters"),
        )),
        Some(text) => Ok(Some(text.to_owned())),
    }
}

pub fn policy_code(policy: PolicyKind) -> &'static str {
    match policy {
        PolicyKind::Unreviewed => "unreviewed",
        PolicyKind::Sanctioned => "sanctioned",
        PolicyKind::Unsanctioned => "unsanctioned",
    }
}

pub const POLICIES: [PolicyKind; 3] = [
    PolicyKind::Sanctioned,
    PolicyKind::Unsanctioned,
    PolicyKind::Unreviewed,
];

pub fn policy(fields: &FormFields, field: &'static str) -> Result<PolicyKind, UiError> {
    let text = required(fields, field)?;
    POLICIES
        .into_iter()
        .find(|p| policy_code(*p) == text)
        .ok_or_else(|| invalid(field, format!("unknown policy {text:?}")))
}

/// A similarity in `[0, 1]`, written as a decimal.
pub fn similarity(fields: &FormFields, field: &'static str) -> Result<Similarity, UiError> {
    let text = required(fields, field)?;
    let value: f32 = text
        .parse()
        .map_err(|_| invalid(field, format!("{text:?} is not a number")))?;
    if !value.is_finite() {
        return Err(invalid(field, "must be a finite number"));
    }
    Similarity::new(value).map_err(|_| invalid(field, "must lie between 0 and 1"))
}

#[cfg(test)]
mod tests {
    use crosstalk_spec::ids::ChannelId;
    use topcoat::router::content::Form;

    use super::*;

    #[test]
    fn parses_urlencoded_pairs_with_repeats() {
        let Form(fields) =
            Form::<FormFields>::from_bytes(b"topic=A&topic=B&note=+hi+there+&blank=")
                .expect("form");
        assert_eq!(fields.all("topic").collect::<Vec<_>>(), ["A", "B"]);
        assert_eq!(fields.text("note"), Some("hi there"));
        assert_eq!(fields.text("blank"), None);
        assert_eq!(fields.text("missing"), None);
    }

    #[test]
    fn ids_must_be_ulids() {
        let fields = FormFields::from_pairs(&[("channel", "nope")]);
        let err = id::<ChannelId>(&fields, "channel");
        assert!(matches!(
            err,
            Err(UiError::Field {
                field: "channel",
                ..
            })
        ));
        let missing = id::<ChannelId>(&FormFields::default(), "channel");
        assert_eq!(missing, Err(invalid("channel", "required")));
        let ok = FormFields::from_pairs(&[("channel", "0000000000000000000000000Z")]);
        assert_eq!(
            id::<ChannelId>(&ok, "channel"),
            Ok(ChannelId::from_ulid(31))
        );
    }

    #[test]
    fn notes_are_optional_and_bounded() {
        assert_eq!(
            note(&FormFields::from_pairs(&[("note", "  ")]), "note"),
            Ok(None)
        );
        let long = "x".repeat(NOTE_MAX_CHARS + 1);
        assert!(note(&FormFields::from_pairs(&[("note", &long)]), "note").is_err());
    }

    #[test]
    fn policies_parse_from_their_codes() {
        for p in POLICIES {
            let fields = FormFields::from_pairs(&[("policy", policy_code(p))]);
            assert_eq!(policy(&fields, "policy"), Ok(p));
        }
        assert!(policy(&FormFields::from_pairs(&[("policy", "allow")]), "policy").is_err());
    }

    #[test]
    fn similarities_are_bounded_numbers() {
        let ok = FormFields::from_pairs(&[("t", "0.75")]);
        assert_eq!(similarity(&ok, "t").map(Similarity::get), Ok(0.75));
        for bad in ["1.5", "-0.1", "NaN", "inf", "high"] {
            assert!(
                similarity(&FormFields::from_pairs(&[("t", bad)]), "t").is_err(),
                "{bad}"
            );
        }
    }
}

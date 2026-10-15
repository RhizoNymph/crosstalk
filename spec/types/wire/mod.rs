//! The JSON wire contract: how the spec's types travel between the gateway,
//! the operator UI (HTTP and SSE) and other gateway nodes (NATS).
//!
//! The spec types are the wire format. Each one derives or implements
//! serde's `Serialize` and `Deserialize` following the conventions below,
//! and a golden file under `spec/types/tests/golden/` pins its JSON, so any
//! change to the format shows up as a reviewable diff.
//!
//! # Conventions
//!
//! | Rust | JSON |
//! | --- | --- |
//! | struct | object with snake_case keys, `#[serde(rename_all = "snake_case", deny_unknown_fields)]` |
//! | enum with any data-carrying variant | adjacently tagged: `{"type": "<variant>", "data": <payload>}`, `type` in snake_case; a unit variant is `{"type": "<variant>"}` with no `data` |
//! | enum whose variants are all unit | a snake_case string, `"view"` |
//! | entity id (`AgentId`, …) | 26 upper-case characters of Crockford base32 ULID text, `"01J9Z3K8M4Q7R2T5V6W8X9Y0ZA"` ([`crate::ids`]) |
//! | content id (`MessageHash`, …), [`Blake3`](crate::support::Blake3) | 64 lower-case hex digits |
//! | [`Timestamp`](crate::support::Timestamp) | RFC 3339 UTC at fixed microsecond precision, `"2026-10-04T12:34:56.789012Z"` ([`time`]) |
//! | `std::time::Duration` | whole microseconds as a number, in a field named `<what>_micros`: `"lag_micros": 30000000` ([`duration`]) |
//! | `Option<T>` | `T` or `null`; `None` is always written as `null`, never left out |
//! | `Vec<T>`, [`NonEmpty<T>`](crate::support::NonEmpty), [`IdBatch<T>`](crate::batch::IdBatch) | array |
//! | `BTreeMap<K, V>` keyed by an id | object keyed by the id's text, in ascending id order (every id-keyed map on the wire is a `BTreeMap`, so one value has one encoding) |
//! | `NonZeroU32` and the other `NonZero*` | number; `0` is a decode error |
//! | newtype over a number (`TopicModelVersion`, `SecretVersion`) | the number, `#[serde(transparent)]` |
//! | checked type (private fields, `fn new(..) -> Result`) | the shape of its fields; decoding runs the checked constructor |
//!
//! Tagging, with [`AlertState`](crate::aggregates::alert::AlertState):
//!
//! ```json
//! {"type": "open"}
//! {"type": "acknowledged", "data": {"by": "01J9Z3K8M4Q7R2T5V6W8X9Y0ZA", "at": "2026-10-04T12:34:56.789012Z"}}
//! ```
//!
//! and a newtype variant, `AlertSubject::Channel(id)`:
//!
//! ```json
//! {"type": "channel", "data": "01J9Z3K8M4Q7R2T5V6W8X9Y0ZA"}
//! ```
//!
//! Enums whose variants are all unit are strings rather than `{"type": ..}`
//! objects because they are values: they appear in filters that travel in
//! query strings (`AlertStateKind`), as members of sets (`Permission`), and
//! as map keys, none of which can be an object.
//!
//! **Strict decoding.** Unknown fields and unknown variants are decode
//! errors (`deny_unknown_fields` on every struct and every tagged enum).
//! Nodes of different versions in one cluster therefore fail loudly on a
//! format they do not know instead of silently dropping data; a format
//! change is a coordinated upgrade, made visible by its golden diff.
//! Serde still accepts three alternate spellings, none of which adds or
//! drops data: a unit variant written with `"data": null`, an all-unit enum
//! written as `{"<variant>": null}`, and a struct written as a JSON array of
//! its fields in declaration order. The goldens pin the canonical forms,
//! nothing the gateway writes uses the others, and a checked type's
//! constructor runs whichever spelling it is decoded from.
//!
//! # Checked types: the validating-deserialization pattern
//!
//! A checked type (private fields, a constructor returning `Result`) never
//! derives `Deserialize` on itself, because a derive would build it without
//! its constructor. It deserializes through a private raw mirror of its
//! fields and `TryFrom`, so invalid JSON is a decode error and never a
//! value:
//!
//! ```ignore
//! /// The checked type: derive `Serialize` on it directly (its fields are its
//! /// wire shape), and route `Deserialize` through the raw mirror.
//! #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
//! #[serde(rename_all = "snake_case", try_from = "RawTimeWindow")]
//! pub struct TimeWindow {
//!     start: Timestamp,
//!     end: Timestamp,
//! }
//!
//! /// Private: the same fields, decoded without checks. Strictness lives here.
//! #[derive(Deserialize)]
//! #[serde(rename_all = "snake_case", deny_unknown_fields)]
//! struct RawTimeWindow {
//!     start: Timestamp,
//!     end: Timestamp,
//! }
//!
//! impl TryFrom<RawTimeWindow> for TimeWindow {
//!     type Error = Rejected<EmptyWindow>;
//!
//!     fn try_from(raw: RawTimeWindow) -> Result<Self, Self::Error> {
//!         Self::new(raw.start, raw.end).map_err(|error| Rejected::new("time window", error))
//!     }
//! }
//! ```
//!
//! A checked type with one value (`PageSize`, `Similarity`) uses
//! `#[serde(try_from = "u16", into = "u16")]` with no mirror. Decoding
//! accepts exactly what the constructor accepts, including any
//! normalization it does (`NonBlank` trims, `IdBatch` sorts and drops
//! repeats); the golden of a constructed value always round-trips.
//! [`Rejected`] carries the constructor's typed error into serde, which
//! requires `Display` of a `TryFrom` error, so the domain errors stay plain
//! enums.
//!
//! # Requests, responses and authority
//!
//! - **Requests** ([`WireRequest`]): what the gateway accepts as input from a
//!   client: filters, page requests, windows, ids, id batches, operator
//!   action requests, export requests. The HTTP layer decodes request
//!   bodies and query parameters only through [`decode_request`], which is
//!   bounded on `WireRequest`, so a type that is not a request cannot be
//!   decoded from one.
//! - **Responses and bus events**: everything the gateway sends, to the UI
//!   or to another node. They derive both traits, because the UI and the
//!   receiving node decode them, but they are not `WireRequest`.
//! - **Authority** ([`authority`]): the [`Caller`] never serializes either
//!   way; it is built per request by the operator directory. Records the
//!   server stamps with an operator and a time (a `Promotion`, a merge
//!   request's author, a verdict record, a pin) are never `WireRequest`,
//!   so a client cannot supply its own author or acceptance time. The
//!   module asserts both at compile time.
//!
//! [`Caller`]: crate::interfaces::l8_surface::Caller

use std::fmt;

use serde::de::{DeserializeOwned, Error as _};
use serde::{Deserialize, Deserializer, Serialize};

/// A type the gateway accepts as input from a client: an HTTP request body
/// or query parameter of the query surface or the operator actions.
///
/// The gateway's HTTP layer decodes client input only through
/// [`decode_request`], whose bound is this trait, so implementing it is the
/// one decision that lets a client send a type. In axum terms, the
/// surface's extractor is generic over `T: WireRequest` and calls
/// `decode_request::<T>` on the body (or on the JSON of a query parameter);
/// its rejection is `QueryError::from(DecodeError)` (or `ActionError::from`
/// on an action route), answered as `400 Bad Request` with the error's JSON.
/// The [`Caller`](crate::interfaces::l8_surface::Caller) is never part of
/// the input: the extractor builds it from the verified session through
/// `OperatorDirectory::caller`.
///
/// Implement it only for types whose every field a client may choose. A
/// type holding an author, an acceptance time or any other value the server
/// stamps is not a request; see [`authority`].
pub trait WireRequest: Serialize + DeserializeOwned {}

impl<T: WireRequest> WireRequest for Option<T> {}

/// Why client input could not be decoded into the request type: not JSON,
/// cut short, or JSON of the wrong shape (an unknown field or variant, a
/// missing field, a value a checked constructor refused).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodeError {
    pub kind: DecodeErrorKind,
    /// serde_json's description, with the line and column.
    pub reason: String,
}

/// On the wire, a string: `"data"`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DecodeErrorKind {
    /// Not valid JSON.
    Syntax,
    /// Valid JSON that is not a value of the type.
    Data,
    /// The input ended in the middle of a value.
    Eof,
}

impl From<serde_json::Error> for DecodeError {
    fn from(error: serde_json::Error) -> Self {
        let kind = match error.classify() {
            serde_json::error::Category::Data => DecodeErrorKind::Data,
            serde_json::error::Category::Eof => DecodeErrorKind::Eof,
            // Decoding from a byte slice performs no I/O; an I/O category
            // could only come from a reader, so it is reported as syntax.
            serde_json::error::Category::Syntax | serde_json::error::Category::Io => {
                DecodeErrorKind::Syntax
            }
        };
        Self {
            kind,
            reason: error.to_string(),
        }
    }
}

/// Decode client input as the request type `T`. The only way the gateway's
/// HTTP layer turns client JSON into a value.
pub fn decode_request<T: WireRequest>(json: &[u8]) -> Result<T, DecodeError> {
    serde_json::from_slice(json).map_err(DecodeError::from)
}

/// A checked constructor's refusal, as a decode error. Serde needs the
/// error of a `try_from` conversion to be `Display`; this wrapper gives any
/// typed refusal one (naming what was being decoded), so the domain error
/// enums stay plain.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Rejected<E> {
    what: &'static str,
    error: E,
}

impl<E> Rejected<E> {
    pub const fn new(what: &'static str, error: E) -> Self {
        Self { what, error }
    }

    pub fn what(&self) -> &'static str {
        self.what
    }

    pub fn error(&self) -> &E {
        &self.error
    }
}

impl<E: fmt::Debug> fmt::Display for Rejected<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid {}: {:?}", self.what, self.error)
    }
}

/// Decode a JSON string and parse it with `parse`, reporting a refusal as
/// [`Rejected`]. The deserializing half of every type with a text form
/// (ids, digests, timestamps, cursors, checked text).
pub(crate) fn decode_text<'de, D, T, E>(
    deserializer: D,
    what: &'static str,
    parse: impl FnOnce(String) -> Result<T, E>,
) -> Result<T, D::Error>
where
    D: Deserializer<'de>,
    E: fmt::Debug,
{
    let text = String::deserialize(deserializer)?;
    parse(text).map_err(|error| D::Error::custom(Rejected::new(what, error)))
}

/// Fails to compile when `$ty` implements any of the listed traits. The
/// technique `static_assertions::assert_not_impl_any!` uses, written out so
/// the spec takes no dependency for it: when `$ty` implements a listed
/// trait, `AmbiguousIfImpl<_>` has two candidate impls for it and inference
/// fails. In textual scope for the submodules declared below it; the
/// assertions live in [`authority`].
macro_rules! assert_not_impl {
    ($ty:ty: $($tr:path),+ $(,)?) => {
        const _: fn() = || {
            trait AmbiguousIfImpl<A> {
                fn some_item() {}
            }
            impl<T: ?Sized> AmbiguousIfImpl<()> for T {}
            $({
                #[allow(dead_code)]
                struct Invalid;
                impl<T: ?Sized + $tr> AmbiguousIfImpl<Invalid> for T {}
            })+
            let _ = <$ty as AmbiguousIfImpl<_>>::some_item;
        };
    };
}

pub mod authority;
pub mod duration;
pub mod time;

//! Path templates: matching a request's path to its route, and the text a
//! path parameter carries.
//!
//! A template is `/`-separated segments, each a literal of lower-case
//! letters, digits and `-`, or a `{parameter}`. A path matches a template
//! when it has the same number of segments and every literal is equal; a
//! parameter matches any non-empty segment, and decoding it as the
//! argument's type ([`PathArg::from_segment`]) decides whether it is valid,
//! so a malformed id is `MalformedRequest` (400), never "no route" (404).
//! No path matches two routes of one method (a test checks every pair), and
//! a path with an empty segment or a trailing `/` matches none.

use crate::aggregates::topic::TopicModelVersion;
use crate::ids::{
    AgentId, AlertId, AlertRuleId, ChannelId, ConversationId, ProjectionId, SpanId, TransmissionId,
};
use crate::wire::{DecodeError, DecodeErrorKind, WireRequest, decode_request};

use super::Method;
use super::routes::Route;

/// What a matched request calls: one route, or the actions endpoint,
/// whose route is the kind of the `ActionRequest` in its body
/// (`Route::Action(request.kind())`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Target {
    Route(Route),
    Actions,
}

/// The parameters a matched path carried, by template name, as raw
/// segments; [`PathParams::decode`] reads one as its argument's type.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PathParams(Vec<(&'static str, String)>);

impl PathParams {
    /// The parameter `name` as `T`. A segment that is not `T`'s text, or a
    /// name the template does not have, is a `DecodeError` naming the
    /// parameter, which the surface answers as `MalformedRequest`.
    pub fn decode<T: PathArg>(&self, name: &str) -> Result<T, DecodeError> {
        let segment = self
            .0
            .iter()
            .find(|(param, _)| *param == name)
            .map(|(_, segment)| segment.as_str())
            .ok_or_else(|| DecodeError {
                kind: DecodeErrorKind::Data,
                reason: format!("path parameter `{name}` is missing"),
            })?;
        T::from_segment(segment).map_err(|error| DecodeError {
            kind: error.kind,
            reason: format!("path parameter `{name}`: {}", error.reason),
        })
    }

    pub fn iter(&self) -> impl Iterator<Item = (&'static str, &str)> {
        self.0
            .iter()
            .map(|(name, segment)| (*name, segment.as_str()))
    }
}

/// One segment of a template.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Segment {
    Literal(&'static str),
    Param(&'static str),
}

/// The segments of a template. `None` for text that is not one: not
/// starting with `/`, an empty segment, a literal outside
/// `[a-z0-9-]`, or a parameter name outside `[a-z_]`.
pub fn segments(template: &'static str) -> Option<Vec<Segment>> {
    let rest = template.strip_prefix('/')?;
    if rest.is_empty() {
        return Some(Vec::new());
    }
    rest.split('/')
        .map(|part| {
            if let Some(name) = part.strip_prefix('{').and_then(|p| p.strip_suffix('}')) {
                let valid =
                    !name.is_empty() && name.bytes().all(|b| b.is_ascii_lowercase() || b == b'_');
                valid.then_some(Segment::Param(name))
            } else {
                let valid = !part.is_empty()
                    && part
                        .bytes()
                        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-');
                valid.then_some(Segment::Literal(part))
            }
        })
        .collect()
}

/// The parameters `path` binds against `template`, or `None` when it does
/// not match. `path` is the request's path without its query string, not
/// percent-decoded.
pub fn match_template(template: &'static str, path: &str) -> Option<PathParams> {
    let template = segments(template)?;
    let rest = path.strip_prefix('/')?;
    let parts: Vec<&str> = if rest.is_empty() {
        Vec::new()
    } else {
        rest.split('/').collect()
    };
    if parts.len() != template.len() || parts.iter().any(|part| part.is_empty()) {
        return None;
    }
    let mut params = Vec::new();
    for (segment, part) in template.iter().zip(parts) {
        match segment {
            Segment::Literal(literal) if *literal == part => {}
            Segment::Literal(_) => return None,
            Segment::Param(name) => params.push((*name, part.to_owned())),
        }
    }
    Some(PathParams(params))
}

/// The route a request's method and path select, with the parameters its
/// path carried. `None` is answered `404` with `QueryError::NotFound`.
pub fn resolve(method: Method, path: &str) -> Option<(Target, PathParams)> {
    Route::all()
        .into_iter()
        .filter(|route| route.method() == method)
        .find_map(|route| {
            match_template(route.path(), path).map(|params| {
                let target = match route {
                    Route::Action(_) => Target::Actions,
                    route => Target::Route(route),
                };
                (target, params)
            })
        })
}

/// A value that travels as one path segment: an entity id as its ULID text,
/// a topic-model version in decimal. The segment is exactly the value's
/// JSON without quotes, and decoding accepts only that text (upper-case
/// ULID, no leading zeros), so each value has one path.
pub trait PathArg: WireRequest {
    /// The segment's text.
    fn segment(&self) -> String;

    /// Reads [`PathArg::segment`]'s text back. Anything outside
    /// `[0-9A-Za-z]` is refused before the value is decoded, so no escape
    /// or quote can reach the decoder.
    fn from_segment(segment: &str) -> Result<Self, DecodeError>;
}

fn plain(segment: &str) -> Result<(), DecodeError> {
    if !segment.is_empty() && segment.bytes().all(|b| b.is_ascii_alphanumeric()) {
        Ok(())
    } else {
        Err(DecodeError {
            kind: DecodeErrorKind::Data,
            reason: format!("`{segment}` is not an id or a number"),
        })
    }
}

macro_rules! id_path_arg {
    ($($id:ty),+ $(,)?) => {
        $(
            impl PathArg for $id {
                fn segment(&self) -> String {
                    self.ulid_text()
                }

                fn from_segment(segment: &str) -> Result<Self, DecodeError> {
                    plain(segment)?;
                    decode_request(format!("\"{segment}\"").as_bytes())
                }
            }
        )+
    };
}

id_path_arg!(
    AgentId,
    AlertId,
    AlertRuleId,
    ChannelId,
    ConversationId,
    ProjectionId,
    SpanId,
    TransmissionId,
);

impl PathArg for TopicModelVersion {
    fn segment(&self) -> String {
        self.0.to_string()
    }

    fn from_segment(segment: &str) -> Result<Self, DecodeError> {
        plain(segment)?;
        decode_request(segment.as_bytes())
    }
}

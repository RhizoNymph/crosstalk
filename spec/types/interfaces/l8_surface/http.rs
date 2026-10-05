//! The HTTP binding of the L8 surface: which route serves each
//! [`QueryApi`](super::QueryApi) method, each operator action and the live
//! feed, where each argument travels, what a success looks like, which
//! status each error is answered with, and how a request's credential
//! becomes its [`Caller`](super::Caller).
//!
//! The [wire contract](crate::wire) fixes the JSON of every argument and
//! result; this module fixes the HTTP around it, as data the server
//! (`crosstalk-api`) and the client (`crosstalk-client`) are both checked
//! against: [`Route`] and its [`RouteSpec`] (the table, in [`routes`]),
//! [`RequestBuilder`] (how a client encodes a call), [`resolve`] and
//! [`QueryParams`] (how the server reads one back), and [`ErrorStatus`].
//!
//! ```text
//! request ─▶ authenticate (credential headers only) ── fails ─▶ 401 AuthError
//!         ─▶ resolve(method, path) ── no route ─▶ 404 QueryError::NotFound
//!         ─▶ decode path, query, body (decode_request) ── fails ─▶ 400 InvalidInput(MalformedRequest)
//!         ─▶ QueryApi method / ActionRequest::into_action + act / LiveFeed::subscribe
//!              ├─ Ok ─▶ the route's success status and content type
//!              └─ Err(e) ─▶ e.status(), body: e's wire JSON
//! ```
//!
//! **Arguments.** A route's arguments travel in exactly one place each
//! ([`Place`]):
//! - a path parameter is an entity id's ULID text or a topic-model version
//!   in decimal ([`PathArg`]);
//! - a query parameter's value is the argument's compact JSON
//!   (`serde_json::to_string`), form-urlencoded; an absent parameter reads
//!   as `null`, so an `Option` argument is omitted for `None` and any other
//!   argument is required. The live feed's `cursor` is the one text
//!   parameter, the SSE id's text, read as `Last-Event-ID` is;
//! - a body is JSON (`Content-Type: application/json`): either the method's
//!   one body argument itself ([`Place::Body`]: an `IdBatch`, an
//!   `ExportRequest`, an `ActionRequest`) or one object holding all of its
//!   arguments, each under its parameter name ([`Place::BodyField`]; the
//!   object types are in [`bodies`]).
//!
//! Reads are `GET` with query parameters, except where an argument holds a
//! client-chosen list of ids or free text with no bound below a URL's
//! practical size: the shared [`TopologyFilter`](crate::aggregates::edge::TopologyFilter)
//! (agents, channels, topics), an [`IdBatch`](crate::batch::IdBatch) (up to
//! 1,000 ids), a transmission selection (up to 100,000) and a search's text.
//! Those reads are `POST` to a `/query/...` path with a JSON body; they
//! change nothing. Unknown and repeated query parameters, a body on a route
//! that takes none, and a body that is not `application/json` are
//! `MalformedRequest`, like a value that does not decode.
//!
//! **Responses.** A success is the route's [`Success`]: for JSON, the
//! method's `Ok` value as JSON (an `Option` result's `None` is `200` with
//! `null`; `404` is only ever `QueryError::NotFound`). An error is
//! [`ErrorStatus::status`] with the error's wire JSON. Every response except
//! a ready projection frame carries `Cache-Control: no-store`: what a
//! caller sees depends on its permissions and on live state.

pub mod auth;
pub mod bodies;
pub mod export;
pub mod frame;
pub mod path;
pub mod request;
pub mod routes;
pub mod sse;
pub mod status;

pub use auth::{AuthError, AuthFailure, Credential, CredentialHeaders, Verification};
pub use path::{PathArg, PathParams, Target, resolve};
pub use request::{EncodeError, EncodedRequest, QueryParams, RequestBuilder};
pub use routes::Route;
pub use status::{ErrorStatus, Status};

use super::Permission;

/// The HTTP methods the surface answers. `HEAD` is answered for every
/// `GET` route as HTTP defines it; no other method is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Method {
    Get,
    Post,
}

impl Method {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Get => "GET",
            Self::Post => "POST",
        }
    }
}

/// Where one argument of a route travels.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Place {
    /// A `{name}` segment of the path template: [`PathArg`] text.
    Path,
    /// A query parameter whose value is the argument's compact JSON.
    Query,
    /// A query parameter whose value is text: the live feed's `cursor`,
    /// read as `Last-Event-ID` is.
    QueryText,
    /// A request header, read as text (the live feed's `Last-Event-ID`).
    Header,
    /// The whole JSON body is this argument.
    Body,
    /// A member of the JSON body object, under this name.
    BodyField,
}

/// One argument of a route: its name on the wire (the template's
/// parameter, the query parameter, the header or the body member) and
/// where it travels. `optional` arguments are `Option`s, left out for
/// `None`; every other one is required.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Arg {
    pub name: &'static str,
    pub place: Place,
    pub optional: bool,
}

impl Arg {
    pub const fn path(name: &'static str) -> Self {
        Self {
            name,
            place: Place::Path,
            optional: false,
        }
    }

    pub const fn query(name: &'static str) -> Self {
        Self {
            name,
            place: Place::Query,
            optional: false,
        }
    }

    pub const fn optional_query(name: &'static str) -> Self {
        Self {
            name,
            place: Place::Query,
            optional: true,
        }
    }

    pub const fn body() -> Self {
        Self {
            name: "body",
            place: Place::Body,
            optional: false,
        }
    }

    pub const fn field(name: &'static str) -> Self {
        Self {
            name,
            place: Place::BodyField,
            optional: false,
        }
    }
}

/// What a successful response's body is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ResponseBody {
    /// The method's `Ok` value as JSON, `application/json`.
    Json,
    /// The projection frame's bytes ([`frame`]), `application/octet-stream`.
    Frame,
    /// The live feed's SSE events ([`sse`]), `text/event-stream`.
    EventStream,
    /// The export's lines or file ([`export`]), its format's content type.
    Export,
}

impl ResponseBody {
    /// The `Content-Type` of the body; an export's depends on its format
    /// ([`export::content_type`]).
    pub fn content_type(self) -> &'static str {
        match self {
            Self::Json => JSON,
            Self::Frame => frame::OCTET_STREAM,
            Self::EventStream => sse::EVENT_STREAM,
            Self::Export => export::EITHER_CONTENT_TYPE,
        }
    }
}

/// `application/json`, with no parameters: JSON is UTF-8 (RFC 8259).
pub const JSON: &str = "application/json";

/// A successful response: its status and body.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Success {
    pub status: Status,
    pub body: ResponseBody,
}

/// The permission a route needs: the one its method documents.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RoutePermission {
    Fixed(Permission),
    /// `QueryApi::me`: no permission; any authenticated caller.
    AnyCaller,
    /// `QueryApi::export`: View, or Content when the request includes
    /// content or names a projection (`ExportRequest::required_permission`).
    ByExportRequest,
}

/// What a route calls.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Source {
    /// The `QueryApi` method of this name.
    Query(&'static str),
    /// `OperatorActions::act` with an action of this kind.
    Act(super::ActionKind),
    /// `LiveFeed::subscribe`.
    Subscribe,
}

/// One row of the route table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RouteSpec {
    pub method: Method,
    /// `/`-separated segments, each a literal (`[a-z0-9-]+`) or a
    /// `{parameter}`.
    pub path: &'static str,
    pub args: &'static [Arg],
    pub success: Success,
    pub permission: RoutePermission,
    pub source: Source,
}

impl RouteSpec {
    /// The arguments that travel in `place`.
    pub fn args_in(&self, place: Place) -> impl Iterator<Item = &'static Arg> {
        self.args.iter().filter(move |arg| arg.place == place)
    }

    /// Whether the route reads a JSON body.
    pub fn has_body(&self) -> bool {
        self.args
            .iter()
            .any(|arg| matches!(arg.place, Place::Body | Place::BodyField))
    }
}

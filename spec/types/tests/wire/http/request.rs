//! Reading a request back strictly: path segments, query parameters and
//! bodies that are not the route's types are decode errors, which the
//! surface answers as a 400 `MalformedRequest`; and the client's builder
//! refuses a call that does not follow the table.

use super::super::{ULID_A, id};
use super::{page, window};
use crate::aggregates::topic::TopicModelVersion;
use crate::ids::{ChannelId, ProjectionId};
use crate::interfaces::l8_surface::http::Method;
use crate::interfaces::l8_surface::http::request::{MAX_BODY_BYTES, check_body};
use crate::interfaces::l8_surface::http::{
    EncodeError, ErrorStatus, PathArg, Place, QueryParams, RequestBuilder, Route, Status, resolve,
};
use crate::interfaces::l8_surface::{InputError, QueryError};
use crate::paging::{ChannelList, PageRequest};
use crate::support::TimeWindow;
use crate::wire::{DecodeError, DecodeErrorKind};

fn pairs(list: &[(&str, &str)]) -> Vec<(String, String)> {
    list.iter()
        .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
        .collect()
}

/// The status and JSON a decode error is answered with.
fn answered(error: DecodeError) -> (Status, QueryError) {
    let error = QueryError::from(error);
    (error.status(), error)
}

fn is_malformed(error: &DecodeError, needle: &str) {
    let (status, json) = answered(error.clone());
    assert_eq!(status, Status::BadRequest, "{error:?}");
    match json {
        QueryError::InvalidInput(InputError::MalformedRequest { reason, .. }) => {
            assert!(reason.contains(needle), "`{reason}` lacks `{needle}`");
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn unknown_and_repeated_query_parameters_are_malformed() {
    let unknown = QueryParams::new(Route::Channels, pairs(&[("fitler", "{}")]));
    is_malformed(
        &unknown.err().unwrap_or_else(|| panic!("refused")),
        "unknown query parameter `fitler`",
    );
    let twice = QueryParams::new(
        Route::Channels,
        pairs(&[
            ("page", r#"{"size":5,"after":null}"#),
            ("page", r#"{"size":6,"after":null}"#),
        ]),
    );
    is_malformed(
        &twice.err().unwrap_or_else(|| panic!("refused")),
        "given twice",
    );
    // A body field is not a query parameter.
    let field = QueryParams::new(Route::Topology, pairs(&[("window", "{}")]));
    assert!(field.is_err());
}

#[test]
fn a_missing_required_parameter_is_malformed_and_an_option_is_none() {
    let params = QueryParams::new(Route::Channel, []).unwrap_or_else(|e| panic!("{e:?}"));
    assert_eq!(params.decode::<Option<TimeWindow>>("window"), Ok(None));
    let params = QueryParams::new(Route::Channels, []).unwrap_or_else(|e| panic!("{e:?}"));
    let missing = params.decode::<PageRequest<ChannelList>>("page");
    let error = missing.err().unwrap_or_else(|| panic!("refused"));
    assert_eq!(error.kind, DecodeErrorKind::Data);
    is_malformed(&error, "query parameter `page`");
    // `null` written out is the same as leaving the parameter out.
    let params = QueryParams::new(Route::Channel, pairs(&[("window", "null")]))
        .unwrap_or_else(|e| panic!("{e:?}"));
    assert_eq!(params.decode::<Option<TimeWindow>>("window"), Ok(None));
}

#[test]
fn a_query_parameter_of_the_wrong_shape_is_malformed() {
    let params = QueryParams::new(
        Route::Channels,
        pairs(&[("page", r#"{"size":0,"after":null}"#), ("filter", "{")]),
    )
    .unwrap_or_else(|e| panic!("{e:?}"));
    let zero = params.decode::<PageRequest<ChannelList>>("page");
    is_malformed(
        &zero.err().unwrap_or_else(|| panic!("refused")),
        "query parameter `page`",
    );
    let cut = params.decode::<crate::interfaces::l8_surface::lists::ChannelFilter>("filter");
    assert_eq!(cut.map_err(|e| e.kind), Err(DecodeErrorKind::Eof));
    let window_text = QueryParams::new(Route::DetectionQuality, pairs(&[("window", "yesterday")]))
        .unwrap_or_else(|e| panic!("{e:?}"));
    let error = window_text.decode::<TimeWindow>("window");
    assert_eq!(error.map_err(|e| e.kind), Err(DecodeErrorKind::Syntax));
}

#[test]
fn path_segments_are_exactly_the_value_text() {
    let channel = id(ChannelId::from_ulid_text, ULID_A);
    assert_eq!(channel.segment(), ULID_A);
    assert_eq!(ChannelId::from_segment(ULID_A), Ok(channel));
    assert_eq!(TopicModelVersion(12).segment(), "12");
    assert_eq!(
        TopicModelVersion::from_segment("12"),
        Ok(TopicModelVersion(12))
    );
    let refused = [
        "01j9z3k8m4q7r2t5v6w8x9y0za",
        "01J9Z3K8M4Q7R2T5V6W8X9Y0Z",
        "%2201J9Z3K8M4Q7R2T5V6W8X9Y0ZA%22",
        "01J9Z3K8M4Q7R2T5V6W8X9Y0ZA\"",
        "",
    ];
    for segment in refused {
        assert!(ChannelId::from_segment(segment).is_err(), "{segment}");
    }
    for segment in ["012", "-1", "1.0", "", "4294967296"] {
        assert!(
            TopicModelVersion::from_segment(segment).is_err(),
            "{segment}"
        );
    }
}

/// A malformed id in the path is a 400 naming the parameter, never a 404.
#[test]
fn a_malformed_path_id_is_a_bad_request_not_a_missing_route() {
    let (target, params) = resolve(Method::Get, "/projections/not-an-id/frame")
        .unwrap_or_else(|| panic!("the template matches any segment"));
    assert_eq!(
        target,
        crate::interfaces::l8_surface::http::Target::Route(Route::ProjectionFrame)
    );
    let error = params.decode::<ProjectionId>("id");
    is_malformed(
        &error.err().unwrap_or_else(|| panic!("refused")),
        "path parameter `id`",
    );
}

#[test]
fn bodies_must_be_json_where_the_route_takes_one() {
    let body = br#"{"window":{},"filter":{}}"#;
    assert_eq!(
        check_body(Route::Overview, Some("application/json"), body),
        Ok(Some(&body[..]))
    );
    assert_eq!(
        check_body(
            Route::Overview,
            Some("Application/JSON; charset=utf-8"),
            body
        ),
        Ok(Some(&body[..]))
    );
    for content_type in [
        None,
        Some("text/plain"),
        Some("application/x-www-form-urlencoded"),
    ] {
        let refused = check_body(Route::Overview, content_type, body);
        let error = refused
            .err()
            .unwrap_or_else(|| panic!("{content_type:?} refused"));
        is_malformed(&error, "content-type must be application/json");
    }
    let on_get = check_body(Route::Channels, None, b"{}");
    is_malformed(
        &on_get.err().unwrap_or_else(|| panic!("refused")),
        "takes no body",
    );
    assert_eq!(check_body(Route::Channels, None, b""), Ok(None));
    let huge = vec![b' '; MAX_BODY_BYTES + 1];
    let over = check_body(Route::TransmissionsById, Some("application/json"), &huge);
    is_malformed(
        &over.err().unwrap_or_else(|| panic!("refused")),
        "body over",
    );
}

#[test]
fn the_builder_refuses_calls_off_the_table() {
    let undeclared = RequestBuilder::new(Route::Channels)
        .query(
            "filter",
            &crate::interfaces::l8_surface::lists::ChannelFilter::default(),
        )
        .query("page", &page::<ChannelList>())
        .query("window", &window())
        .build();
    assert_eq!(
        undeclared,
        Err(EncodeError::Undeclared {
            name: "window".into(),
            place: Place::Query
        })
    );
    let repeated = RequestBuilder::new(Route::Channels)
        .query("page", &page::<ChannelList>())
        .query("page", &page::<ChannelList>())
        .build();
    assert_eq!(repeated, Err(EncodeError::Repeated { name: "page" }));
    let missing = RequestBuilder::new(Route::Channels)
        .query("page", &page::<ChannelList>())
        .build();
    assert_eq!(missing, Err(EncodeError::Missing { name: "filter" }));
    let wrong_place = RequestBuilder::new(Route::Topology)
        .query("window", &window())
        .build();
    assert!(
        matches!(wrong_place, Err(EncodeError::Undeclared { .. })),
        "{wrong_place:?}"
    );
    let no_body = RequestBuilder::new(Route::Watermark)
        .body(&window())
        .build();
    assert!(
        matches!(no_body, Err(EncodeError::Undeclared { .. })),
        "{no_body:?}"
    );
    let no_path = RequestBuilder::new(Route::Alert).build();
    assert_eq!(no_path, Err(EncodeError::Missing { name: "id" }));
}

/// The live feed's resume arguments travel as text, not JSON.
#[test]
fn the_live_cursor_travels_as_text() {
    let request = RequestBuilder::new(Route::Live)
        .query_text("cursor", Some("7-1042"))
        .header("last-event-id", None)
        .build()
        .unwrap_or_else(|error| panic!("{error:?}"));
    assert_eq!(request.path, "/live");
    assert_eq!(request.query, vec![("cursor", "7-1042".to_owned())]);
    assert!(request.headers.is_empty() && request.body.is_none());
    let params = QueryParams::new(Route::Live, pairs(&[("cursor", "7-1042")]))
        .unwrap_or_else(|e| panic!("{e:?}"));
    assert_eq!(params.text("cursor"), Some("7-1042"));
}

//! Page sizes, cursors, page requests, pages and id batches on the wire.

use super::harness::{assert_golden, assert_rejected, assert_request_golden};
use super::{ULID_A, ULID_B, ULID_C, id};
use crate::batch::IdBatch;
use crate::ids::AgentId;
use crate::paging::{AlertList, Cursor, Page, PageRequest, PageSize};
use crate::support::NonEmpty;

const AREA: &str = "paging";

fn size(n: u16) -> PageSize {
    PageSize::new(n).expect("a valid page size")
}

fn cursor() -> Cursor<AlertList> {
    Cursor::from_token("c2VydmVyLWlzc3VlZC10b2tlbg".into()).expect("URL-safe base64")
}

#[test]
fn paging_goldens() {
    assert_golden(AREA, "page_size", &size(50));
    assert_golden(AREA, "cursor", &cursor());
    assert_request_golden(
        AREA,
        "page_request_first",
        &PageRequest::<AlertList> {
            size: size(50),
            after: None,
        },
    );
    assert_request_golden(
        AREA,
        "page_request_after",
        &PageRequest {
            size: size(50),
            after: Some(cursor()),
        },
    );
    let batch = IdBatch::new([
        id(AgentId::from_ulid_text, ULID_C),
        id(AgentId::from_ulid_text, ULID_A),
        id(AgentId::from_ulid_text, ULID_B),
    ])
    .expect("three ids");
    assert_request_golden(AREA, "id_batch", &batch);
}

#[test]
fn pages_round_trip_through_goldens() {
    let more = Page::more(size(2), NonEmpty::new(7u32), cursor()).expect("one item fits");
    assert_golden(AREA, "page_more", &more);
    let last: Page<u32, AlertList> = Page::last(size(2), Vec::new()).expect("empty last page");
    assert_golden(AREA, "page_last_empty", &last);
}

#[test]
fn page_sizes_refuse_zero_and_above_max() {
    assert_rejected::<PageSize>("0", "invalid page size: Zero");
    assert_rejected::<PageSize>("501", "invalid page size: AboveMax { max: 500, got: 501 }");
    assert_rejected::<PageSize>("-1", "invalid value");
}

#[test]
fn cursors_refuse_tokens_the_constructor_refuses() {
    assert_rejected::<Cursor<AlertList>>(r#""""#, "invalid cursor token: Empty");
    assert_rejected::<Cursor<AlertList>>(
        r#""abc+def""#,
        "invalid cursor token: BadByte { index: 3 }",
    );
    let long = format!("\"{}\"", "a".repeat(1025));
    assert_rejected::<Cursor<AlertList>>(
        &long,
        "invalid cursor token: TooLong { max: 1024, got: 1025 }",
    );
}

#[test]
fn page_requests_refuse_unknown_fields() {
    assert_rejected::<PageRequest<AlertList>>(
        r#"{"size": 50, "after": null, "offset": 100}"#,
        "unknown field `offset`",
    );
    assert_rejected::<PageRequest<AlertList>>(r#"{"size": 0, "after": null}"#, "Zero");
}

#[test]
fn pages_refuse_what_the_surface_never_serves() {
    assert_rejected::<Page<u32, AlertList>>(
        r#"{"items": [], "next": "abc"}"#,
        "invalid page: EmptyWithNext",
    );
    let items = vec!["1"; 501].join(", ");
    assert_rejected::<Page<u32, AlertList>>(
        &format!(r#"{{"items": [{items}], "next": null}}"#),
        "invalid page: TooManyItems { max: 500, got: 501 }",
    );
    assert_rejected::<Page<u32, AlertList>>(
        r#"{"items": [], "next": null, "total": 0}"#,
        "unknown field `total`",
    );
}

#[test]
fn id_batches_normalize_and_refuse_too_many() {
    let decoded: Result<IdBatch<AgentId>, _> =
        serde_json::from_str(&format!(r#"["{ULID_B}", "{ULID_A}", "{ULID_B}"]"#));
    let expected = IdBatch::new([
        id(AgentId::from_ulid_text, ULID_A),
        id(AgentId::from_ulid_text, ULID_B),
    ]);
    assert_eq!(decoded.ok(), expected.ok());
    let ids: Vec<String> = (1..=1001u128)
        .map(|n| format!("\"{}\"", AgentId::from_ulid(n).ulid_text()))
        .collect();
    assert_rejected::<IdBatch<AgentId>>(
        &format!("[{}]", ids.join(",")),
        "invalid id batch: TooManyIds { max: 1000, got: 1001 }",
    );
}

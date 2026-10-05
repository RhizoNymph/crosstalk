//! Unit, property and golden tests. The functions named by the canonical
//! invariants' `unit` and `property` evidence live here, at
//! `crosstalk_canonical::tests::<name>`; each calls its body in `units`
//! or `props`. Generators are in `generate`, raw exchange builders in
//! `support`, the golden harness in `golden`.
//!
//! The encoding's and canonical JSON's own tests (pinned vectors, round
//! trips, RFC 8785) are the spec's, beside the code
//! (`crosstalk_spec::tests::encoding`).

mod generate;
mod golden;
mod props;
mod support;
mod units;

use crosstalk_spec::observed::exchange::{ConnectionId, Continuation, ResponseId, Transport};
use proptest::prelude::*;

use generate::anthropic::{arb_blocks, arb_request, arb_response, arb_user_block};
use generate::json::{arb_json, arb_text};
use props::{arb_delivery, arb_failure};

// --- canonical encoding and hashing -------------------------------------

/// `canonical.capture.blob-is-canonical-encoding`.
#[test]
fn capture_puts_canonical_encoding_under_message_hash() {
    units::encoding::capture_puts_canonical_encoding_under_message_hash();
}

/// `canonical.media.hash-of-decoded-bytes`.
#[test]
fn media_hash_is_of_decoded_bytes() {
    units::encoding::media_hash_is_of_decoded_bytes();
}

// --- canonical JSON through the normalizer -----------------------------

/// `canonical.json.large-integers-exact`.
#[test]
fn snowflake_id_survives_canonicalization() {
    units::json::snowflake_id_survives_canonicalization();
}

// --- requests -----------------------------------------------------------

/// `canonical.normalize.system-prompt-first`.
#[test]
fn top_level_system_prompt_becomes_first_message() {
    units::request::top_level_system_prompt_becomes_first_message();
}

/// `canonical.normalize.system-prompt-first`, for a `system` turn.
#[test]
fn system_turn_stays_in_place() {
    units::request::system_turn_stays_in_place();
}

#[test]
fn request_shape_holds_no_content() {
    units::request::request_shape_holds_no_content();
}

/// `canonical.normalize.split-mixed-roles`.
#[test]
fn anthropic_user_turn_with_tool_results_splits() {
    units::request::anthropic_user_turn_with_tool_results_splits();
}

/// `canonical.normalize.unknown-block-kept`.
#[test]
fn unknown_block_kept_in_place() {
    units::request::unknown_block_kept_in_place();
}

/// `canonical.normalize.orphan-tool-result-reported`.
#[test]
fn orphan_tool_result_warns_and_is_kept() {
    units::request::orphan_tool_result_warns_and_is_kept();
}

/// `canonical.normalize.orphan-tool-result-reported`.
#[test]
fn increment_tool_results_do_not_warn() {
    units::request::increment_tool_results_do_not_warn();
}

/// `canonical.text.verbatim`.
#[test]
fn text_with_edge_whitespace_and_nfd_kept() {
    units::request::text_with_edge_whitespace_and_nfd_kept();
}

/// `canonical.tool-call.id-verbatim`.
#[test]
fn dialect_tool_call_ids_kept_verbatim() {
    units::request::dialect_tool_call_ids_kept_verbatim();
}

#[test]
fn invalid_request_bodies_are_errors() {
    units::request::invalid_request_bodies_are_errors();
}

// --- responses ----------------------------------------------------------

/// `canonical.normalize.error-status-is-failed`.
#[test]
fn error_status_normalizes_to_upstream_failure() {
    units::response::error_status_normalizes_to_upstream_failure();
}

/// `canonical.normalize.failed-keeps-request`.
#[test]
fn failed_exchange_keeps_request_with_bad_partial_body() {
    units::response::failed_exchange_keeps_request_with_bad_partial_body();
}

/// `canonical.normalize.unparseable-response-failed`.
#[test]
fn garbled_200_body_is_unparseable_failure() {
    units::response::garbled_200_body_is_unparseable_failure();
}

/// `canonical.normalize.tool-use-stop-reason`.
#[test]
fn tool_call_responses_stop_with_tool_use() {
    units::response::tool_call_responses_stop_with_tool_use();
}

/// `canonical.reasoning.opaque-verbatim`.
#[test]
fn redacted_thinking_kept_verbatim() {
    units::response::redacted_thinking_kept_verbatim();
}

/// `canonical.tool-call.execution-classified`.
#[test]
fn server_tool_use_marked_server() {
    units::response::server_tool_use_marked_server();
}

/// `canonical.tool-call.server-result-follows-call`.
#[test]
fn anthropic_web_search_result_follows_its_call() {
    units::response::anthropic_web_search_result_follows_its_call();
}

/// `canonical.tool-call.arguments-json-or-invalid`.
#[test]
fn malformed_arguments_kept_as_invalid() {
    units::response::malformed_arguments_kept_as_invalid();
}

#[test]
fn error_event_keeps_partial_response() {
    units::response::error_event_keeps_partial_response();
}

// --- the recorded corpus ------------------------------------------------

#[test]
fn corpus_cases_match_goldens() {
    units::corpus::corpus_cases_match_goldens();
}

#[test]
fn corpus_cases_meet_their_expectations() {
    units::corpus::corpus_cases_meet_their_expectations();
}

#[test]
fn corpus_usage_maps_cache_tokens() {
    units::corpus::corpus_usage_maps_cache_tokens();
}

/// `canonical.normalize.stream-independent`.
#[test]
fn recorded_transports_normalize_equal() {
    units::corpus::recorded_transports_normalize_equal();
}

/// `canonical.normalize.echo-stable`.
#[test]
fn recorded_echoes_hash_like_their_responses() {
    units::corpus::recorded_echoes_hash_like_their_responses();
}

/// `canonical.normalize.echo-stable`, for signed thinking.
#[test]
fn thinking_signatures_echo_like_their_responses() {
    units::corpus::thinking_signatures_kept_and_echoed_alike();
}

// --- properties ---------------------------------------------------------

fn arb_continuation() -> impl Strategy<Value = Continuation> {
    prop_oneof![
        Just(Continuation::FullHistory),
        ("[a-z_0-9]{1,20}", proptest::option::of(any::<u128>())).prop_map(
            |(previous, connection)| {
                Continuation::Increment {
                    previous: ResponseId(previous),
                    connection: connection.map(ConnectionId),
                }
            }
        ),
    ]
}

fn arb_transport() -> impl Strategy<Value = Transport> {
    prop_oneof![Just(Transport::Http), Just(Transport::Sse)]
}

/// Argument text: valid JSON spelled any way, or text from JSON's
/// alphabet (no backslash, so no escapes, and short, so shallow) that
/// mostly is not JSON.
fn arb_argument_text() -> impl Strategy<Value = String> {
    prop_oneof![
        (arb_json(), any::<u64>())
            .prop_map(|(value, seed)| value.render(&mut generate::Style::new(seed))),
        "[{}\\[\\]\":,0-9a-z .eE+-]{0,40}",
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(96))]

    /// `canonical.exchange.references-resolve`.
    #[test]
    fn exchange_hashes_resolve_to_messages(
        request in arb_request(), delivery in arb_delivery(), seed in any::<u64>()
    ) {
        props::exchange_hashes_resolve(&request, &delivery, seed)?;
    }

    /// `canonical.message.hash-is-blake3-of-encoding`.
    #[test]
    fn normalized_hashes_match_encoding(
        request in arb_request(), delivery in arb_delivery(), seed in any::<u64>()
    ) {
        props::hashes_match_encoding(&request, &delivery, seed)?;
    }

    /// `canonical.normalize.content-encoding-independent`.
    #[test]
    fn normalization_ignores_request_encoding(
        request in arb_request(), delivery in arb_delivery(), seed in any::<u64>()
    ) {
        props::ignores_request_encoding(&request, &delivery, seed)?;
    }

    /// `canonical.normalize.continuation-preserved`.
    #[test]
    fn continuation_carried_through(
        request in arb_request(), continuation in arb_continuation(), seed in any::<u64>()
    ) {
        props::continuation_carried(&request, continuation, seed)?;
    }

    /// `canonical.normalize.deterministic`.
    #[test]
    fn normalize_is_deterministic(
        request in arb_request(), delivery in arb_delivery(), seed in any::<u64>()
    ) {
        props::deterministic(&request, &delivery, seed)?;
    }

    /// `canonical.normalize.echo-stable`.
    #[test]
    fn response_and_echo_hash_equal(response in arb_response(), seed in any::<u64>()) {
        props::echo_hashes_equal(&response, seed)?;
    }

    /// `canonical.normalize.failed-keeps-request`.
    #[test]
    fn failed_response_bytes_never_fail_normalization(
        request in arb_request(),
        transport in arb_transport(),
        failure in arb_failure(),
        partial in proptest::collection::vec(any::<u8>(), 0..256),
        seed in any::<u64>(),
    ) {
        props::failed_bytes_never_fail(&request, transport, failure, partial, seed)?;
    }

    /// `canonical.normalize.request-is-concatenation`.
    #[test]
    fn request_normalizes_message_by_message(request in arb_request(), seed in any::<u64>()) {
        props::request_message_by_message(&request, seed)?;
    }

    /// `canonical.normalize.response-is-assistant`.
    #[test]
    fn response_message_is_assistant(
        request in arb_request(), delivery in arb_delivery(), seed in any::<u64>()
    ) {
        props::response_is_assistant(&request, &delivery, seed)?;
    }

    /// `canonical.normalize.split-mixed-roles`.
    #[test]
    fn split_preserves_block_sequence(
        blocks in proptest::collection::vec(arb_user_block(), 0..10), seed in any::<u64>()
    ) {
        props::split_preserves_blocks(&blocks, seed)?;
    }

    /// `canonical.normalize.stream-independent`.
    #[test]
    fn response_message_independent_of_transport(response in arb_response(), seed in any::<u64>()) {
        props::transport_independent(&response, seed)?;
    }

    /// `canonical.normalize.unknown-block-kept`.
    #[test]
    fn unknown_blocks_round_trip_as_canonical_json(request in arb_request(), seed in any::<u64>()) {
        props::unknown_blocks_canonical(&request, seed)?;
    }

    /// `canonical.normalize.unknown-block-reported`.
    #[test]
    fn every_unknown_part_is_warned(
        request in arb_request(), delivery in arb_delivery(), seed in any::<u64>()
    ) {
        props::unknown_parts_warned(&request, &delivery, seed)?;
    }

    /// `canonical.normalize.unparseable-response-failed`.
    #[test]
    fn unparseable_success_bodies_keep_request(
        transport in arb_transport(), body in proptest::collection::vec(any::<u8>(), 0..256)
    ) {
        props::unparseable_keeps_request(transport, &body)?;
    }

    /// `canonical.reasoning.opaque-verbatim`.
    #[test]
    fn opaque_reasoning_bytes_preserved(data in any::<String>(), seed in any::<u64>()) {
        props::opaque_preserved(&data, seed)?;
    }

    /// `canonical.text.verbatim`.
    #[test]
    fn text_parts_preserve_strings(text in arb_text(), seed in any::<u64>()) {
        props::text_preserved(&text, seed)?;
    }

    /// `canonical.tool-call.arguments-json-or-invalid`.
    #[test]
    fn arguments_classified_by_parse(text in arb_argument_text(), seed in any::<u64>()) {
        props::arguments_by_parse(&text, seed)?;
    }

    /// `canonical.tool-call.server-result-follows-call`.
    #[test]
    fn server_results_pair_with_earlier_server_calls(blocks in arb_blocks(), seed in any::<u64>()) {
        props::server_results_paired(&blocks, seed)?;
    }

    /// `canonical.normalize.dialect-independent`, for the Anthropic
    /// Messages dialects.
    #[test]
    fn normalization_independent_of_dialect(
        request in arb_request(), delivery in arb_delivery(), seed in any::<u64>()
    ) {
        props::dialect_independent(&request, &delivery, seed)?;
    }
}

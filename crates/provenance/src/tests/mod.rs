//! Unit tests, and the invariant evidence named
//! `crosstalk_provenance::tests::<name>`: each evidence test is declared
//! here and runs a scenario from a submodule.

mod agentdojo;
mod config;
mod decode;
mod fingerprint;
pub(crate) mod fixtures;
pub(crate) mod scenarios;
mod segment;
mod store;

/// `provenance.match.carrier-from-part`: a read in a tool result.
#[tokio::test]
async fn carrier_for_tool_result() {
    scenarios::carrier_for_tool_result().await;
}

/// `provenance.match.carrier-from-part`: a read in a user turn.
#[tokio::test]
async fn carrier_for_user_turn() {
    scenarios::carrier_for_user_turn().await;
}

/// `provenance.match.carrier-from-part`: a read in a new system prompt.
#[tokio::test]
async fn carrier_for_system_prompt() {
    scenarios::carrier_for_system_prompt().await;
}

/// `provenance.match.carrier-from-part`: another agent's text in the
/// reader's own output, no input containing it.
#[tokio::test]
async fn carrier_for_reader_output() {
    scenarios::carrier_for_reader_output().await;
}

/// `provenance.match.origin-agent-is-span-agent`.
#[tokio::test]
async fn origin_agent_taken_from_span() {
    scenarios::origin_agent_taken_from_span().await;
}

/// `provenance.match.scans-delta-messages`: new inputs, the new system
/// prompt and the output are all looked up.
#[tokio::test]
async fn lookups_cover_new_inputs_system_and_output() {
    scenarios::lookups_cover_new_inputs_system_and_output().await;
}

/// `provenance.match.scans-delta-messages`: history replayed in the
/// request is not.
#[tokio::test]
async fn replayed_history_is_not_looked_up() {
    scenarios::replayed_history_is_not_looked_up().await;
}

/// `provenance.match.self-hit-skipped`.
#[tokio::test]
async fn own_span_in_input_is_skipped() {
    scenarios::own_span_in_input_is_skipped().await;
}

/// `provenance.index.originated-indexed`.
#[tokio::test]
async fn originated_span_fingerprints_are_indexed() {
    scenarios::originated_span_fingerprints_are_indexed().await;
}

/// `provenance.index.cutoff-not-inserted`: the processing path passes the
/// exchange's time, so boilerplate gets no posting.
#[tokio::test]
async fn insert_skips_fingerprints_above_cutoff() {
    scenarios::insert_skips_fingerprints_above_cutoff().await;
}

/// `provenance.index.cutoff-not-returned`.
#[tokio::test]
async fn lookup_ignores_fingerprints_that_crossed_cutoff() {
    scenarios::lookup_ignores_fingerprints_that_crossed_cutoff().await;
}

/// `provenance.span.common-above-cutoff`.
#[tokio::test]
async fn span_with_one_rare_fingerprint_is_not_common() {
    scenarios::span_with_one_rare_fingerprint_is_not_common().await;
}

/// `provenance.index.scanned-texts-observed`: every span, whatever its
/// origin.
#[tokio::test]
async fn every_span_origin_is_observed() {
    scenarios::every_span_origin_is_observed().await;
}

/// `provenance.index.scanned-texts-observed`: every scanned input part.
#[tokio::test]
async fn scanned_input_parts_are_observed() {
    scenarios::scanned_input_parts_are_observed().await;
}

/// `provenance.semantic.fallback-only`.
#[tokio::test]
async fn fingerprint_match_preempts_semantic() {
    scenarios::fingerprint_match_preempts_semantic().await;
}

/// `provenance.semantic.score-above-threshold`.
#[tokio::test]
async fn semantic_lookup_respects_threshold() {
    scenarios::semantic_lookup_respects_threshold().await;
}

/// `provenance.fingerprint.reproducible`.
#[test]
fn fingerprints_match_golden_vectors() {
    fingerprint::golden_vectors();
}

/// `provenance.index.wrong-shard-rejected`: insert.
#[tokio::test]
async fn insert_on_wrong_shard_errors() {
    store::insert_on_wrong_shard_errors().await;
}

/// `provenance.index.wrong-shard-rejected`: lookup.
#[tokio::test]
async fn lookup_on_wrong_shard_errors() {
    store::lookup_on_wrong_shard_errors().await;
}

/// `provenance.scan.status-terminal`.
#[tokio::test]
async fn delta_ends_indexed_or_failed() {
    scenarios::delta_ends_indexed_or_failed().await;
}

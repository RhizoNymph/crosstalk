//! The gateway's invariant evidence entry points
//! (`crosstalk_gateway::tests::<name>`). The end-to-end tests over real
//! sockets are the `e2e` integration test target
//! (`crosstalk_gateway::e2e::<name>`, `crates/gateway/tests/e2e/`).

mod dst;
mod ingest;
mod raw;
mod record;
mod refusals;

crosstalk_sim::sim_test! {
    /// `canonical.capture.blobs-before-event` (dst): every blob an exchange
    /// references is in the blob store before its `ExchangeCaptured` is
    /// published, and nothing is published for an exchange whose puts
    /// failed; under put latency, put failures before and after the write,
    /// and seeded feed timing.
    fn dst_blobs_written_before_capture_published(ctx) {
        dst::blobs_written_before_capture_published(ctx).await
    }
}

crosstalk_sim::sim_test! {
    /// `Pipeline::ingest` is the proxy path after L1: for every corpus
    /// exchange at the same simulated instant, the same blob puts in the
    /// same order and the same `ExchangeCaptured` envelope.
    fn pipeline_ingest_matches_the_proxy_path(ctx) {
        ingest::ingest_matches_the_proxy_path(ctx).await
    }
}

crosstalk_sim::sim_test! {
    /// Under blob store faults `ingest` retries (idempotently) and then
    /// fails with `IngestError::NotStored`, publishing nothing; transient
    /// faults are retried through.
    fn pipeline_ingest_retries_blob_faults_then_fails_typed(ctx) {
        ingest::ingest_retries_blob_faults_then_fails_typed(ctx).await
    }
}

crosstalk_sim::sim_test! {
    /// Concurrent ingests: distinct envelope ids, reaching the bus in
    /// strictly increasing order, never before their `at`.
    fn pipeline_concurrent_ingests_keep_ids_monotonic(ctx) {
        ingest::concurrent_ingests_keep_ids_monotonic(ctx).await
    }
}

/// Refusals are counted by reason and protocol (the `normalize_failed`
/// series of `/metrics`).
#[tokio::test]
async fn refusals_are_counted_by_reason_and_protocol() {
    refusals::refusals_are_counted_by_reason_and_protocol().await;
}

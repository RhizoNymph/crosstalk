//! The gateway's invariant evidence entry points
//! (`crosstalk_gateway::tests::<name>`). The end-to-end tests over real
//! sockets are the `e2e` integration test target
//! (`crosstalk_gateway::e2e::<name>`, `crates/gateway/tests/e2e/`).

mod dst;
mod raw;

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

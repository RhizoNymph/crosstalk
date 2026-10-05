//! Every harness applied to the reference itself: the reference is
//! deterministic and keeps the oracles the harnesses check.

use super::HarnessConfig;
use super::analysis::{
    ReferenceSearch, check_alert_rule_store, check_alert_triage, check_projection_store,
    check_search_index, check_topic_catalog, reference_alerts, reference_catalog,
};
use crate::analysis::projection::InMemoryProjectionStore;
use crate::support::Outbox;

fn harness() -> HarnessConfig {
    HarnessConfig::default()
}

#[test]
fn sizes_match_assignment_count() {
    // analysis.sizes.match-cross-agent-assignments, and the catalog against itself
    let result = check_topic_catalog(harness(), |config| async move {
        reference_catalog(config).expect("a reference catalog")
    });
    assert_eq!(result, Ok(()));
}

#[test]
fn search_index_agrees_with_reference() {
    let result = check_search_index(harness(), |model, world| async move {
        ReferenceSearch::new(model, world).expect("a reference search world")
    });
    assert_eq!(result, Ok(()));
}

#[test]
fn projection_store_agrees_with_reference() {
    // analysis.projection.queue-bounded, sequentially
    let result = check_projection_store(harness(), |config| async move {
        InMemoryProjectionStore::new(config, Outbox::none())
    });
    assert_eq!(result, Ok(()));
}

#[test]
fn alert_rule_store_agrees_with_reference() {
    let result = check_alert_rule_store(harness(), |world| async move { reference_alerts(world) });
    assert_eq!(result, Ok(()));
}

#[test]
fn alert_transitions_agree_with_lifecycle_model() {
    // analysis.triage.one-active-per-key, sequentially, and triage against
    // itself
    let result = check_alert_triage(harness(), |world| async move { reference_alerts(world) });
    assert_eq!(result, Ok(()));
}

#[test]
fn in_memory_graph_matches_fold() {
    // topology.graph.matches-fold-model, series.total-matches-graph, and
    // the edge store against itself
    let result = super::topology::check_edge_store(harness(), |config, world| async move {
        super::topology::ReferenceEdges::new(config, world).expect("a reference edge world")
    });
    assert_eq!(result, Ok(()));
}

#[test]
fn audit_log_agrees_with_reference() {
    // surface.audit.append-only, and the log against itself
    let result = super::surface::check_audit_log(harness(), || async {
        crate::surface::audit::InMemoryAuditLog::new()
    });
    assert_eq!(result, Ok(()));
}

#[test]
fn operator_store_agrees_with_reference() {
    // surface.audit.config-changes-recorded, sequentially, for operators
    let result = super::surface::check_operator_store(harness(), || async {
        super::surface::ReferenceOperators::new()
    });
    assert_eq!(result, Ok(()));
}

#[test]
fn every_store_is_send_and_sync() {
    use crate::analysis::aliases::StaticDirectory;
    use crate::analysis::fakes::FakeEmbedder;
    use crate::analysis::search::ManualWatermark;
    use crate::topology::env::{Env, StaticNodes};

    fn shareable<T: Send + Sync>() {}
    shareable::<crate::analysis::catalog::InMemoryTopicCatalog>();
    shareable::<crate::analysis::search::InMemorySearchIndex<StaticDirectory>>();
    shareable::<crate::analysis::search::InMemoryProjectionSource<StaticDirectory, ManualWatermark>>(
    );
    shareable::<InMemoryProjectionStore>();
    shareable::<crate::analysis::alerts::InMemoryAlertStore<FakeEmbedder, StaticDirectory>>();
    shareable::<
        crate::topology::store::InMemoryEdgeStore<
            Env<crate::analysis::catalog::InMemoryTopicCatalog, StaticDirectory, StaticNodes>,
        >,
    >();
    shareable::<crate::surface::audit::InMemoryAuditLog>();
    shareable::<crate::surface::operators::InMemoryOperatorStore>();
    shareable::<crate::surface::sinks::InMemorySinkRegistry>();
}

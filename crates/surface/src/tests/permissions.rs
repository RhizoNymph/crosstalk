//! Every query names one permission and checks it before reading anything:
//! a caller without it gets `Forbidden` naming it.

use crosstalk_spec::aggregates::edge::{EdgeSelector, TopologyFilter, Weighting};
use crosstalk_spec::aggregates::filter::TopicVersionSelector;
use crosstalk_spec::aggregates::projection::{ProjectionLimit, ProjectionParams};
use crosstalk_spec::aggregates::series::SeriesGrouping;
use crosstalk_spec::aggregates::topic::TopicModelVersion;
use crosstalk_spec::batch::IdBatch;
use crosstalk_spec::derived::flow::transmission::Route;
use crosstalk_spec::ids::{AgentId, AlertId, AlertRuleId, ChannelId, ProjectionId, TransmissionId};
use crosstalk_spec::interfaces::l8_surface::audit::AuditFilter;
use crosstalk_spec::interfaces::l8_surface::excerpt::ExcerptWindow;
use crosstalk_spec::interfaces::l8_surface::lists::{
    AgentFilter, AlertRuleFilter, ChannelFilter, SearchMode, SearchRequest,
};
use crosstalk_spec::interfaces::l8_surface::live::{LiveFeed, Resume};
use crosstalk_spec::interfaces::l8_surface::summary::TransmissionSelection;
use crosstalk_spec::interfaces::l8_surface::{
    AlertFilter, Caller, Permission, QueryApi, QueryError,
};
use crosstalk_spec::support::NonBlank;

use super::world::{Fixture, Who, grid, minutes};
use super::{page, pattern};

fn forbidden(missing: Permission) -> QueryError {
    QueryError::Forbidden { missing }
}

fn assert_forbidden<T: std::fmt::Debug>(result: Result<T, QueryError>, missing: Permission) {
    match result {
        Err(error) => assert_eq!(error, forbidden(missing)),
        Ok(value) => panic!("expected Forbidden {{ missing: {missing:?} }}, got {value:?}"),
    }
}

fn channel() -> ChannelId {
    ChannelId::from_ulid(0xC0)
}

fn agent() -> AgentId {
    AgentId::from_ulid(0xA0)
}

/// `query_api::unauthorized-forbidden` (INV-383, View endpoints).
#[tokio::test]
async fn view_endpoints_without_view_forbidden() {
    let fixture = Fixture::new().await;
    let surface = &fixture.surface;
    let caller: Caller = fixture.caller(Who::Auditor).await;
    let filter = TopologyFilter::default();
    let window = minutes(0, 10);
    let view = Permission::View;
    assert_forbidden(
        surface
            .topology(&caller, window, Weighting::Transmissions, &filter)
            .await,
        view,
    );
    assert_forbidden(
        surface
            .channel_topology(&caller, window, Weighting::Transmissions, &filter)
            .await,
        view,
    );
    assert_forbidden(surface.overview(&caller, window, &filter).await, view);
    assert_forbidden(
        surface
            .series(
                &caller,
                grid(),
                Weighting::Transmissions,
                SeriesGrouping::Total,
                &filter,
            )
            .await,
        view,
    );
    assert_forbidden(
        surface
            .alerts(&caller, &AlertFilter::default(), &page(10))
            .await,
        view,
    );
    assert_forbidden(surface.alert(&caller, AlertId::from_ulid(1)).await, view);
    assert_forbidden(
        surface
            .alert_rules(&caller, &AlertRuleFilter::default(), &page(10))
            .await,
        view,
    );
    assert_forbidden(
        surface.alert_rule(&caller, AlertRuleId::from_ulid(1)).await,
        view,
    );
    assert_forbidden(
        surface
            .channels(&caller, &ChannelFilter::default(), &page(10))
            .await,
        view,
    );
    assert_forbidden(
        surface
            .agents(&caller, &AgentFilter::default(), window, &page(10))
            .await,
        view,
    );
    assert_forbidden(surface.present(&caller).await, view);
    assert_forbidden(
        surface
            .verdicts(&caller, TransmissionId::from_ulid(1))
            .await,
        view,
    );
    assert_forbidden(surface.detection_quality(&caller, window).await, view);
}

/// INV-383, Content endpoints: a viewer gets `Forbidden { missing:
/// Content }` from every content query.
#[tokio::test]
async fn content_endpoints_without_content_forbidden() {
    let fixture = Fixture::new().await;
    let surface = &fixture.surface;
    let caller = fixture.caller(Who::Viewer).await;
    let filter = TopologyFilter::default();
    let window = minutes(0, 10);
    let content = Permission::Content;
    let Ok(text) = NonBlank::new("hello") else {
        panic!("text");
    };
    let request = SearchRequest {
        mode: SearchMode::Text,
        text,
    };
    assert_forbidden(
        surface
            .search(&caller, &request, None, &filter, &page(10))
            .await,
        content,
    );
    assert_forbidden(
        surface
            .transmission(&caller, TransmissionId::from_ulid(1))
            .await,
        content,
    );
    assert_forbidden(
        surface
            .transmission_evidence(
                &caller,
                TransmissionId::from_ulid(1),
                ExcerptWindow::DEFAULT,
            )
            .await,
        content,
    );
    assert_forbidden(
        surface
            .topics(&caller, TopicVersionSelector::Current, &page(10))
            .await,
        content,
    );
    let Ok(limit) = ProjectionLimit::new(100) else {
        panic!("limit");
    };
    let Ok(params) = ProjectionParams::new(limit, 15, 100, 7) else {
        panic!("params");
    };
    assert_forbidden(
        surface
            .fit_projection(&caller, window, &filter, params)
            .await,
        content,
    );
    assert_forbidden(
        surface
            .projection_status(&caller, ProjectionId::from_ulid(1))
            .await,
        content,
    );
    assert_forbidden(surface.projections(&caller, &page(10)).await, content);
    assert_forbidden(
        surface
            .projection(&caller, ProjectionId::from_ulid(1))
            .await,
        content,
    );
}

/// INV-407: lists without their permission are `Forbidden`.
#[tokio::test]
async fn view_lists_without_view_forbidden() {
    let fixture = Fixture::new().await;
    let surface = &fixture.surface;
    let caller = fixture.caller(Who::Operator).await;
    let window = minutes(0, 10);
    let view = Permission::View;
    assert_forbidden(
        surface
            .channels(&caller, &ChannelFilter::default(), &page(10))
            .await,
        view,
    );
    assert_forbidden(
        surface
            .agents(&caller, &AgentFilter::default(), window, &page(10))
            .await,
        view,
    );
    assert_forbidden(
        surface
            .alert_rules(&caller, &AlertRuleFilter::default(), &page(10))
            .await,
        view,
    );
    let Ok(edge) = EdgeSelector::new(agent(), AgentId::from_ulid(0xA1), Route::Unobserved) else {
        panic!("edge");
    };
    assert_forbidden(
        surface
            .edge_transmissions(
                &caller,
                &edge,
                window,
                &TopologyFilter::default(),
                &page(10),
            )
            .await,
        view,
    );
}

/// INV-407: dead letters need Operate.
#[tokio::test]
async fn dead_letters_without_operate_forbidden() {
    let fixture = Fixture::new().await;
    let caller = fixture.caller(Who::Admin).await;
    assert!(
        fixture
            .surface
            .dead_letters(&caller, None, &page(10))
            .await
            .is_ok()
    );
    for who in [
        Who::Viewer,
        Who::Reader,
        Who::Auditor,
        Who::Triager,
        Who::Governor,
    ] {
        let caller = fixture.caller(who).await;
        assert_forbidden(
            fixture.surface.dead_letters(&caller, None, &page(10)).await,
            Permission::Operate,
        );
    }
}

/// INV-432: series and the topic history need View.
#[tokio::test]
async fn history_endpoints_without_view_forbidden() {
    let fixture = Fixture::new().await;
    let surface = &fixture.surface;
    let caller = fixture.caller(Who::Auditor).await;
    let view = Permission::View;
    assert_forbidden(
        surface
            .series(
                &caller,
                grid(),
                Weighting::Transmissions,
                SeriesGrouping::Topic,
                &TopologyFilter::default(),
            )
            .await,
        view,
    );
    assert_forbidden(surface.topic_versions(&caller).await, view);
    assert_forbidden(surface.topic_sizes(&caller, None, None).await, view);
    assert_forbidden(
        surface.topic_lineage(&caller, TopicModelVersion(0)).await,
        view,
    );
}

/// INV-477: the audit log needs Audit.
#[tokio::test]
async fn audit_query_without_audit_forbidden() {
    let fixture = Fixture::new().await;
    for who in [
        Who::Viewer,
        Who::Reader,
        Who::Triager,
        Who::Governor,
        Who::Operator,
    ] {
        let caller = fixture.caller(who).await;
        assert_forbidden(
            fixture
                .surface
                .audit(&caller, &AuditFilter::default(), &page(10))
                .await,
            Permission::Audit,
        );
    }
    let auditor = fixture.caller(Who::Auditor).await;
    assert!(
        fixture
            .surface
            .audit(&auditor, &AuditFilter::default(), &page(10))
            .await
            .is_ok()
    );
}

/// INV-477: the policy history needs View.
#[tokio::test]
async fn policy_history_without_view_forbidden() {
    let fixture = Fixture::new().await;
    for who in [Who::Auditor, Who::Operator] {
        let caller = fixture.caller(who).await;
        assert_forbidden(
            fixture.surface.policy_history(&caller, channel()).await,
            Permission::View,
        );
    }
}

/// INV-551: subscribing needs View and opens no stream without it.
#[tokio::test]
async fn subscribe_without_view_forbidden() {
    let fixture = Fixture::new().await;
    let caller = fixture.caller(Who::Auditor).await;
    match fixture.surface.subscribe(&caller, Resume::Fresh).await {
        Err(error) => assert_eq!(error, forbidden(Permission::View)),
        Ok(_) => panic!("a stream opened without View"),
    }
    let viewer = fixture.caller(Who::Viewer).await;
    assert!(
        fixture
            .surface
            .subscribe(&viewer, Resume::Fresh)
            .await
            .is_ok()
    );
}

/// INV-558: the operator directory needs View.
#[tokio::test]
async fn operators_without_view_forbidden() {
    let fixture = Fixture::new().await;
    let caller = fixture.caller(Who::Auditor).await;
    assert_forbidden(fixture.surface.operators(&caller).await, Permission::View);
}

/// INV-580: the watermark needs View.
#[tokio::test]
async fn watermark_without_view_forbidden() {
    let fixture = Fixture::new().await;
    let caller = fixture.caller(Who::Operator).await;
    assert_forbidden(fixture.surface.watermark(&caller).await, Permission::View);
    let viewer = fixture.caller(Who::Viewer).await;
    assert!(fixture.surface.watermark(&viewer).await.is_ok());
}

/// INV-619: the sinks need Govern, even for a caller with every other
/// read permission.
#[tokio::test]
async fn sinks_query_needs_govern() {
    let fixture = Fixture::new().await;
    for who in [
        Who::Viewer,
        Who::Reader,
        Who::Auditor,
        Who::Triager,
        Who::Operator,
    ] {
        let caller = fixture.caller(who).await;
        assert_forbidden(fixture.surface.sinks(&caller).await, Permission::Govern);
    }
    let governor = fixture.caller(Who::Governor).await;
    match fixture.surface.sinks(&governor).await {
        Ok(sinks) => assert_eq!(sinks.len(), 2),
        Err(error) => panic!("sinks: {error:?}"),
    }
}

/// INV-669: the channel-centred graph and a channel's resources need View.
#[tokio::test]
async fn channel_views_without_view_forbidden() {
    let fixture = Fixture::new().await;
    let caller = fixture.caller(Who::Auditor).await;
    let window = minutes(0, 10);
    assert_forbidden(
        fixture
            .surface
            .channel_topology(
                &caller,
                window,
                Weighting::Transmissions,
                &TopologyFilter::default(),
            )
            .await,
        Permission::View,
    );
    assert_forbidden(
        fixture
            .surface
            .channel_resources(&caller, channel(), window, &page(10))
            .await,
        Permission::View,
    );
}

/// INV-707: the evidence needs Content, and reads nothing without it: the
/// evidence records are failing, and the answer is still `Forbidden`.
#[tokio::test]
async fn evidence_without_content_forbidden() {
    let fixture = Fixture::new().await;
    fixture.world.evidence.fail(true);
    let caller = fixture.caller(Who::Viewer).await;
    assert_forbidden(
        fixture
            .surface
            .transmission_evidence(
                &caller,
                TransmissionId::from_ulid(9),
                ExcerptWindow::DEFAULT,
            )
            .await,
        Permission::Content,
    );
}

/// INV-744: the channel read models need View.
#[tokio::test]
async fn channel_reads_without_view_forbidden() {
    let fixture = Fixture::new().await;
    let caller = fixture.caller(Who::Operator).await;
    let view = Permission::View;
    assert_forbidden(
        fixture.surface.channel(&caller, channel(), None).await,
        view,
    );
    let Ok(ids) = IdBatch::new([channel()]) else {
        panic!("batch");
    };
    assert_forbidden(fixture.surface.channel_names(&caller, &ids).await, view);
    assert_forbidden(
        fixture
            .surface
            .promotion_preview(&caller, channel(), &pattern())
            .await,
        view,
    );
}

/// INV-744: the agent read models need View.
#[tokio::test]
async fn agent_reads_without_view_forbidden() {
    let fixture = Fixture::new().await;
    let caller = fixture.caller(Who::Auditor).await;
    let view = Permission::View;
    assert_forbidden(
        fixture
            .surface
            .agent(&caller, agent(), minutes(0, 10))
            .await,
        view,
    );
    let Ok(ids) = IdBatch::new([agent()]) else {
        panic!("batch");
    };
    assert_forbidden(fixture.surface.agent_names(&caller, &ids).await, view);
}

/// INV-744: transmission rows, one alert and the overview need View.
#[tokio::test]
async fn rows_alert_and_overview_without_view_forbidden() {
    let fixture = Fixture::new().await;
    let caller = fixture.caller(Who::Operator).await;
    let view = Permission::View;
    let Ok(selection) = TransmissionSelection::new(vec![TransmissionId::from_ulid(1)]) else {
        panic!("selection");
    };
    assert_forbidden(
        fixture
            .surface
            .transmissions_by_id(
                &caller,
                &selection,
                TopicVersionSelector::Current,
                &page(10),
            )
            .await,
        view,
    );
    assert_forbidden(
        fixture.surface.alert(&caller, AlertId::from_ulid(1)).await,
        view,
    );
    assert_forbidden(
        fixture
            .surface
            .overview(&caller, minutes(0, 10), &TopologyFilter::default())
            .await,
        view,
    );
}

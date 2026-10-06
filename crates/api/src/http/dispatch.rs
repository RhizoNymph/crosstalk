//! One query route: its arguments decoded from where the table puts them,
//! under the table's names, and its method called with the caller.
//!
//! [`query`] matches every [`Route`] with no wildcard, so a route added to
//! the table does not compile until it is served here. Each arm reads the
//! route's arguments exactly as `RouteSpec::args` lists them; a JSON
//! success is the method's `Ok` value with the route's success status.
//! The frame, the live feed and the export have their own modules, and
//! actions are one endpoint ([`super::actions`]).

use axum::http::header::LOCATION;
use axum::response::Response;
use crosstalk_spec::aggregates::filter::TopicVersionSelector;
use crosstalk_spec::aggregates::topic::TopicModelVersion;
use crosstalk_spec::batch::IdBatch;
use crosstalk_spec::ids::{
    AgentId, AlertId, AlertRuleId, ChannelId, ConversationId, ExchangeId, ProjectionId, SpanId,
    TransmissionId,
};
use crosstalk_spec::interfaces::l2_transport::ConsumerGroup;
use crosstalk_spec::interfaces::l8_surface::conversation::TurnWindow;
use crosstalk_spec::interfaces::l8_surface::conversation::text::TextLimit;
use crosstalk_spec::interfaces::l8_surface::http::bodies::{
    EdgeTransmissionsBody, FitProjectionBody, GraphBody, OverviewBody, PartTextBody, SearchBody,
    SeriesBody,
};
use crosstalk_spec::interfaces::l8_surface::http::{PathArg, Route};
use crosstalk_spec::interfaces::l8_surface::{Caller, QueryError};
use crosstalk_spec::support::TimeWindow;
use serde::Serialize;

use super::input::Input;
use super::{Shared, Surface, checked, export, frame, live, respond};

/// Calls `route`'s method with `input`'s arguments and answers with its
/// result.
pub(super) async fn query<S: Surface>(
    shared: &Shared<S>,
    route: Route,
    caller: &Caller,
    input: &Input,
) -> Result<Response, QueryError> {
    let s = shared.surface.as_ref();
    let c = caller;
    let i = input;
    match route {
        Route::Channel => ok(route, &s.channel(c, path(i)?, i.query("window")?).await?),
        Route::PolicyHistory => ok(route, &s.policy_history(c, path(i)?).await?),
        Route::Channels => ok(
            route,
            &s.channels(c, &i.query("filter")?, &i.query("page")?)
                .await?,
        ),
        Route::ChannelNames => ok(route, &s.channel_names(c, &checked::id_batch(i)?).await?),
        Route::PromotionPreview => ok(
            route,
            &s.promotion_preview(c, path(i)?, &i.query("pattern")?)
                .await?,
        ),
        Route::ChannelResources => {
            let (id, window, page) = (path(i)?, i.query("window")?, i.query("page")?);
            ok(route, &s.channel_resources(c, id, window, &page).await?)
        }
        Route::ChannelTransmissions => {
            let id: ChannelId = path(i)?;
            let filter = i.query("filter")?;
            let version: TopicVersionSelector = i.query("version")?;
            let page = i.query("page")?;
            ok(
                route,
                &s.channel_transmissions(c, id, &filter, version, &page)
                    .await?,
            )
        }
        Route::Agents => {
            let (filter, window, page) = (i.query("filter")?, i.query("window")?, i.query("page")?);
            ok(route, &s.agents(c, &filter, window, &page).await?)
        }
        Route::Agent => {
            let (id, window): (AgentId, TimeWindow) = (path(i)?, i.query("window")?);
            ok(route, &s.agent(c, id, window).await?)
        }
        Route::AgentNames => ok(route, &s.agent_names(c, &checked::id_batch(i)?).await?),
        Route::Conversations => ok(
            route,
            &s.conversations(c, &i.query("filter")?, &i.query("page")?)
                .await?,
        ),
        Route::Conversation => {
            let id: ConversationId = path(i)?;
            ok(route, &s.conversation(c, id).await?)
        }
        Route::ConversationTurns => {
            let id: ConversationId = path(i)?;
            ok(
                route,
                &s.conversation_turns(c, id, &i.query("window")?).await?,
            )
        }
        Route::SpanReaders => {
            let id: SpanId = path(i)?;
            ok(route, &s.span_readers(c, id, &i.query("page")?).await?)
        }
        Route::ExchangeTurns => {
            let ids: IdBatch<ExchangeId> = checked::id_batch(i)?;
            ok(route, &s.exchange_turns(c, &ids).await?)
        }
        Route::SpanPoints => {
            let ids: IdBatch<SpanId> = checked::id_batch(i)?;
            ok(route, &s.span_points(c, &ids).await?)
        }
        Route::ConversationText => {
            let id: ConversationId = path(i)?;
            let window: TurnWindow = i.query("window")?;
            let limit: TextLimit = i.query("limit")?;
            ok(route, &s.conversation_text(c, id, &window, limit).await?)
        }
        Route::PartText => {
            let PartTextBody { part, slice } = i.body()?;
            ok(route, &s.part_text(c, part, slice).await?)
        }
        Route::AlertRules => ok(
            route,
            &s.alert_rules(c, &i.query("filter")?, &i.query("page")?)
                .await?,
        ),
        Route::AlertRule => ok(route, &s.alert_rule(c, path::<AlertRuleId>(i)?).await?),
        Route::Sinks => ok(route, &s.sinks(c).await?),
        Route::DeadLetters => {
            let group: Option<ConsumerGroup> = i.query("group")?;
            ok(
                route,
                &s.dead_letters(c, group.as_ref(), &i.query("page")?).await?,
            )
        }
        Route::Alerts => ok(
            route,
            &s.alerts(c, &i.query("filter")?, &i.query("page")?).await?,
        ),
        Route::Alert => ok(route, &s.alert(c, path::<AlertId>(i)?).await?),
        Route::Watermark => ok(route, &s.watermark(c).await?),
        Route::Topology => {
            let GraphBody {
                window,
                weighting,
                filter,
            } = i.body()?;
            ok(route, &s.topology(c, window, weighting, &filter).await?)
        }
        Route::Overview => {
            let OverviewBody { window, filter } = i.body()?;
            ok(route, &s.overview(c, window, &filter).await?)
        }
        Route::ChannelTopology => {
            let GraphBody {
                window,
                weighting,
                filter,
            } = i.body()?;
            ok(
                route,
                &s.channel_topology(c, window, weighting, &filter).await?,
            )
        }
        Route::EdgeTransmissions => {
            let EdgeTransmissionsBody {
                edge,
                window,
                filter,
                page,
            } = i.body()?;
            ok(
                route,
                &s.edge_transmissions(c, &edge, window, &filter, &page)
                    .await?,
            )
        }
        Route::TransmissionsById => {
            let body = checked::transmissions_body(i)?;
            ok(
                route,
                &s.transmissions_by_id(c, &body.selection, body.version, &body.page)
                    .await?,
            )
        }
        Route::Series => {
            let SeriesBody {
                grid,
                weighting,
                grouping,
                filter,
            } = i.body()?;
            ok(
                route,
                &s.series(c, grid, weighting, grouping, &filter).await?,
            )
        }
        Route::TopicVersions => ok(route, &s.topic_versions(c).await?),
        Route::TopicSizes => {
            let version: Option<TopicModelVersion> = i.query("version")?;
            let window: Option<TimeWindow> = i.query("window")?;
            ok(route, &s.topic_sizes(c, version, window).await?)
        }
        Route::TopicLineage => {
            let version: TopicModelVersion = i.path("version")?;
            ok(route, &s.topic_lineage(c, version).await?)
        }
        Route::Search => {
            let SearchBody {
                request,
                window,
                filter,
                page,
            } = i.body()?;
            ok(route, &s.search(c, &request, window, &filter, &page).await?)
        }
        Route::Transmission => ok(route, &s.transmission(c, path::<TransmissionId>(i)?).await?),
        Route::TransmissionEvidence => {
            let id: TransmissionId = path(i)?;
            let window = checked::excerpt_window(i, "window")?;
            ok(route, &s.transmission_evidence(c, id, window).await?)
        }
        Route::Topics => {
            let version: TopicVersionSelector = i.query("version")?;
            ok(route, &s.topics(c, version, &i.query("page")?).await?)
        }
        Route::FitProjection => {
            let FitProjectionBody {
                window,
                filter,
                params,
            } = i.body()?;
            let id = s.fit_projection(c, window, &filter, params).await?;
            let mut response = ok(route, &id)?;
            let location = respond::header_value(&format!("/projections/{}", id.segment()))?;
            response.headers_mut().insert(LOCATION, location);
            Ok(response)
        }
        Route::ProjectionStatus => ok(
            route,
            &s.projection_status(c, path::<ProjectionId>(i)?).await?,
        ),
        Route::Projections => ok(route, &s.projections(c, &i.query("page")?).await?),
        Route::ProjectionFrame => frame::serve(shared, caller, input).await,
        Route::Verdicts => ok(route, &s.verdicts(c, path::<TransmissionId>(i)?).await?),
        Route::DetectionQuality => ok(route, &s.detection_quality(c, i.query("window")?).await?),
        Route::Audit => ok(
            route,
            &s.audit(c, &i.query("filter")?, &i.query("page")?).await?,
        ),
        Route::Operators => ok(route, &s.operators(c).await?),
        Route::Me => ok(route, &s.me(c).await?),
        Route::Export => export::serve(s, caller, input).await,
        Route::Present => ok(route, &s.present(c).await?),
        Route::Live => live::serve(s, caller, input).await,
        // Every action is the one `POST /actions` endpoint; the router
        // never sends one here.
        Route::Action(_) => Err(QueryError::NotFound),
    }
}

/// The path parameter every id route names `id`.
fn path<T: PathArg>(input: &Input) -> Result<T, QueryError> {
    Ok(input.path("id")?)
}

/// `value` as JSON with `route`'s success status.
fn ok<T: Serialize>(route: Route, value: &T) -> Result<Response, QueryError> {
    respond::json(route.spec().success.status, value)
}

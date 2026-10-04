//! The `<ct-projection>` payload and its route.
//!
//! `GET /data/projection/{id}` answers with a stored projection
//! (`Backend::projection`) in the binary format of [`format`], as
//! `application/octet-stream`. Needs `Content`. The spec's frame has no
//! channel column, so the channels of the channel-routed points are read
//! from their transmissions' rows (`transmissions_by_id`, routes resolved
//! now). The header's category tables carry display names: agent names
//! (`components::agent_name_of`) from one `agent_names` call, channel
//! names ([`super::names`]) from one `channel_names` call (each chunked at
//! `IdBatch::MAX` ids) and topic labels from the projection's version's
//! `topics`.

#[cfg(test)]
pub mod decode;
pub mod format;

use std::collections::HashMap;

use crosstalk_spec::aggregates::edge::RouteKind;
use crosstalk_spec::aggregates::filter::TopicVersionSelector;
use crosstalk_spec::aggregates::projection::Projection;
use crosstalk_spec::batch::IdBatch;
use crosstalk_spec::derived::flow::transmission::Route;
use crosstalk_spec::ids::{AgentId, ChannelId, TransmissionId};
use crosstalk_spec::interfaces::l8_surface::summary::TransmissionSelection;
use crosstalk_spec::interfaces::l8_surface::{Caller, Permission};
use crosstalk_spec::paging::{PageRequest, PageSize, TransmissionList};
use topcoat::context::Cx;
use topcoat::router::error::{bad_request, internal_server_error};
use topcoat::router::{path_param, route};

use self::format::{PayloadPoints, ProjectionTables, encode};
use super::errors::query_error;
use super::names::channel_name;
use super::require;
use crate::app::{backend, caller, can};
use crate::backend::Backend;
use crate::components::{agent_name_of, short_id};
use crate::contract::agents::AgentName;
use crate::contract::channels::ChannelName;
use crate::error::UiError;
use crate::pages::common::paging::size;
use crate::pages::common::topics::all_topics;
use crate::url::ulid::UlidId;
use crosstalk_spec::ids::ProjectionId;
use crosstalk_spec::interfaces::l8_surface::QueryError;

path_param!(id);

/// Pages of transmission rows read before the channel lookup is cut short:
/// a projection holds at most `ProjectionLimit::MAX` points, which is 200
/// pages of `PageSize::MAX`.
const MAX_ROUTE_PAGES: usize = 200;

/// The channel each channel-routed point's transmission goes through, as
/// its row reports the route now (`TopicVersionSelector::Current`: the
/// rows' topics are not needed, and the active version is always
/// retained). Transmissions no longer stored are left out.
pub async fn point_channels<B: Backend>(
    backend: &B,
    caller: &Caller,
    projection: &Projection,
) -> Result<HashMap<TransmissionId, ChannelId>, UiError> {
    let routed: Vec<TransmissionId> = projection
        .frame()
        .points()
        .filter(|point| point.route == RouteKind::Channel)
        .map(|point| point.transmission)
        .collect();
    let mut channels = HashMap::with_capacity(routed.len());
    if routed.is_empty() {
        return Ok(channels);
    }
    let selection = TransmissionSelection::new(routed).map_err(QueryError::from)?;
    let mut request = PageRequest::<TransmissionList> {
        size: size(PageSize::MAX),
        after: None,
    };
    for _ in 0..MAX_ROUTE_PAGES {
        let page = backend
            .transmissions_by_id(caller, &selection, TopicVersionSelector::Current, &request)
            .await?
            .page;
        let (rows, next) = page.into_parts();
        channels.extend(rows.iter().filter_map(|row| match row.route {
            Route::Channel(channel) => Some((row.id, channel)),
            _ => None,
        }));
        match next {
            Some(cursor) => request.after = Some(cursor),
            None => return Ok(channels),
        }
    }
    Err(UiError::Query(QueryError::Store {
        reason: format!("transmission rows did not end within {MAX_ROUTE_PAGES} pages"),
    }))
}

/// Agent names for `ids`, in batches of at most `IdBatch::MAX`.
async fn agent_names<B: Backend>(
    backend: &B,
    caller: &Caller,
    ids: &[AgentId],
) -> Result<HashMap<AgentId, AgentName>, UiError> {
    let mut names = HashMap::with_capacity(ids.len());
    for chunk in ids.chunks(IdBatch::<AgentId>::MAX) {
        names.extend(backend.agent_names(caller, chunk).await?);
    }
    Ok(names)
}

/// Channel names for `ids`, in batches of at most `IdBatch::MAX`.
async fn channel_names<B: Backend>(
    backend: &B,
    caller: &Caller,
    ids: &[ChannelId],
) -> Result<HashMap<ChannelId, ChannelName>, UiError> {
    let mut names = HashMap::with_capacity(ids.len());
    for chunk in ids.chunks(IdBatch::<ChannelId>::MAX) {
        names.extend(backend.channel_names(caller, chunk).await?);
    }
    Ok(names)
}

/// Looks up the display names for a projection's category tables. Topic
/// labels are `None` without `Content` or when the projection's topic
/// version is unknown to the catalog; other backend errors fail the
/// request.
pub async fn tables<B: Backend>(
    backend: &B,
    caller: &Caller,
    projection: &Projection,
    points: &PayloadPoints,
) -> Result<ProjectionTables, UiError> {
    let agent_names = agent_names(backend, caller, points.agents()).await?;
    let agents = points
        .agents()
        .iter()
        .map(|id| {
            agent_names
                .get(id)
                .map_or_else(|| short_id(id.to_ulid()), agent_name_of)
        })
        .collect();
    let channel_names = channel_names(backend, caller, points.channels()).await?;
    let channels = points
        .channels()
        .iter()
        .map(|id| {
            channel_names
                .get(id)
                .map_or_else(|| short_id(id.to_ulid()), channel_name)
        })
        .collect();
    let hidden = vec![None; points.topics().len()];
    let topics = if can(caller, Permission::Content) {
        let version = TopicVersionSelector::Pinned(projection.topic_version());
        match all_topics(backend, caller, version).await {
            Ok((_, topics)) => points
                .topics()
                .iter()
                .map(|id| topics.iter().find(|t| t.id == *id).map(|t| t.label.clone()))
                .collect(),
            Err(QueryError::NotFound | QueryError::VersionNotRetained { .. }) => hidden,
            Err(error) => return Err(error.into()),
        }
    } else {
        hidden
    };
    ProjectionTables::new(points, agents, channels, topics).map_err(|e| {
        UiError::Query(QueryError::Store {
            reason: e.to_string(),
        })
    })
}

#[route(GET "/data/projection/{id}")]
async fn projection_data(cx: &Cx) -> topcoat::Result<Vec<u8>> {
    let caller = caller(cx);
    require(&caller, Permission::Content)?;
    let raw = path_param::<Id>(cx);
    let id = ProjectionId::parse_ulid(raw).map_err(|e| bad_request(format!("id: {e}")))?;
    let backend = backend(cx);
    let projection = backend.projection(&caller, id).await.map_err(query_error)?;
    let channels = point_channels(backend, &caller, &projection)
        .await
        .map_err(query_error)?;
    let points = PayloadPoints::new(&projection, &channels);
    let tables = tables(backend, &caller, &projection, &points)
        .await
        .map_err(query_error)?;
    let bytes = encode(&projection, &points, &tables).map_err(|e| {
        tracing::error!(error = %e, projection = %raw, "projection encoding failed");
        internal_server_error(e)
    })?;
    tracing::debug!(
        points = projection.frame().count(),
        bytes = bytes.len(),
        "projection payload"
    );
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::decode::{DecodeError, decode};
    use super::format::{NONE, ROUTE_KINDS, route_code};
    use super::*;
    use crate::data::fixtures;
    use crate::data::topology::RouteKindCode;

    #[test]
    fn round_trips_every_column() {
        let (projection, points, tables) = fixtures::payload();
        let bytes = encode(&projection, &points, &tables).expect("encode");
        let decoded = decode(&bytes).expect("decode");
        let columns = projection.frame().columns();

        assert_eq!(decoded.header.count, projection.frame().count());
        assert_eq!(decoded.header.id, projection.info().id().to_ulid());
        assert_eq!(decoded.header.topic_version, 3);
        assert_eq!(decoded.header.params.sample_limit, 5000);
        let xs: Vec<f32> = columns.xy.iter().map(|[x, _]| *x).collect();
        let ys: Vec<f32> = columns.xy.iter().map(|[_, y]| *y).collect();
        assert_eq!(decoded.xs, xs);
        assert_eq!(decoded.ys, ys);
        let agent = |i: u32| points.agents()[i as usize];
        for (i, point) in projection.frame().points().enumerate() {
            let c = points.categories()[i];
            assert_eq!(agent(decoded.senders[i]), point.from);
            assert_eq!(agent(decoded.readers[i]), point.to);
            assert_eq!(decoded.routes[i], route_code(point.route));
            assert_eq!(decoded.channels[i], c.channel);
            assert_eq!(
                decoded.topics[i].map(|t| points.topics()[t as usize]),
                point.topic
            );
        }
        let ids: Vec<u128> = columns.transmissions.iter().map(|t| t.as_ulid()).collect();
        assert_eq!(decoded.transmissions, ids);
        assert_eq!(
            decoded
                .header
                .agents
                .iter()
                .map(|a| a.id.clone())
                .collect::<Vec<_>>(),
            points
                .agents()
                .iter()
                .map(|a| a.to_ulid())
                .collect::<Vec<_>>()
        );
        assert_eq!(decoded.header.agents[0].name, "planner");
        assert!(decoded.header.topics.iter().all(|t| t.label.is_some()));
    }

    #[test]
    fn tables_are_sorted_and_shared_by_senders_and_readers() {
        let (projection, points, _) = fixtures::payload();
        assert!(points.agents().is_sorted());
        assert!(points.channels().is_sorted());
        assert!(points.topics().is_sorted());
        let frame = projection.frame().tables();
        for agent in frame.senders.iter().chain(&frame.readers) {
            assert!(points.agents().contains(agent));
        }
        assert_eq!(points.topics().len(), frame.topics.len());
    }

    #[test]
    fn header_is_aligned_and_route_table_matches_codes() {
        let (projection, points, tables) = fixtures::payload();
        let bytes = encode(&projection, &points, &tables).expect("encode");
        let header_len = u32::from_le_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]) as usize;
        assert_eq!(header_len % 4, 0);
        let n = projection.frame().count() as usize;
        assert_eq!(bytes.len(), 12 + header_len + 41 * n + (4 - n % 4) % 4);
        let decoded = decode(&bytes).expect("decode");
        let expected: Vec<RouteKindCode> = ROUTE_KINDS.iter().map(|k| (*k).into()).collect();
        assert_eq!(decoded.header.route_kinds, expected);
        for (code, kind) in ROUTE_KINDS.iter().enumerate() {
            assert_eq!(usize::from(route_code(*kind)), code);
        }
    }

    #[test]
    fn uses_none_for_absent_channel_and_topic() {
        let (projection, points, tables) = fixtures::payload();
        assert!(points.categories().iter().any(|c| c.topic.is_none()));
        assert!(points.categories().iter().any(|c| c.channel.is_none()));
        let bytes = encode(&projection, &points, &tables).expect("encode");
        let decoded = decode(&bytes).expect("decode");
        assert!(decoded.channels.iter().flatten().all(|c| *c != NONE));
        for (i, point) in projection.frame().points().enumerate() {
            assert_eq!(
                decoded.channels[i].is_some(),
                point.route == RouteKind::Channel,
                "exactly the channel-routed points name a channel"
            );
        }
    }

    #[test]
    fn a_channel_route_without_a_known_channel_is_none() {
        let (projection, _) = fixtures::projection();
        let points = PayloadPoints::new(&projection, &HashMap::new());
        assert!(points.channels().is_empty());
        assert!(points.categories().iter().all(|c| c.channel.is_none()));
    }

    #[test]
    fn empty_projection_round_trips() {
        let projection = fixtures::empty_projection();
        let points = PayloadPoints::new(&projection, &HashMap::new());
        let tables =
            ProjectionTables::new(&points, Vec::new(), Vec::new(), Vec::new()).expect("tables");
        let bytes = encode(&projection, &points, &tables).expect("encode");
        let decoded = decode(&bytes).expect("decode");
        assert_eq!(decoded.header.count, 0);
        assert!(decoded.xs.is_empty());
    }

    #[test]
    fn decoder_rejects_corruption() {
        let (projection, points, tables) = fixtures::payload();
        let bytes = encode(&projection, &points, &tables).expect("encode");
        assert!(matches!(
            decode(&bytes[..bytes.len() - 1]),
            Err(DecodeError::Truncated(_))
        ));
        let mut extra = bytes.clone();
        extra.push(0);
        assert!(matches!(decode(&extra), Err(DecodeError::Trailing(1))));
        let mut magic = bytes.clone();
        magic[0] = b'X';
        assert!(matches!(decode(&magic), Err(DecodeError::Magic)));
    }

    #[test]
    fn tables_reject_length_mismatch() {
        let (_, points, _) = fixtures::payload();
        assert!(ProjectionTables::new(&points, Vec::new(), Vec::new(), Vec::new()).is_err());
    }

    #[test]
    fn categories_must_cover_every_point() {
        let (projection, _, _) = fixtures::payload();
        let empty = PayloadPoints::new(&fixtures::empty_projection(), &HashMap::new());
        let tables =
            ProjectionTables::new(&empty, Vec::new(), Vec::new(), Vec::new()).expect("tables");
        assert!(matches!(
            encode(&projection, &empty, &tables),
            Err(format::EncodeError::Categories(0, 640))
        ));
    }
}

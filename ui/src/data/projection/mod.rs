//! The `<ct-projection>` payload and its route.
//!
//! `GET /data/projection/{id}` answers with a stored projection
//! (`Backend::projection`) in the binary format of [`format`], as
//! `application/octet-stream`. Needs `Content`. The header's category
//! tables carry display names: agent names (`components::agent_name`),
//! channel names ([`super::names`]) and topic labels.

#[cfg(test)]
mod decode;
pub mod format;

use crosstalk_spec::interfaces::l8_surface::{Caller, Permission};
use topcoat::context::Cx;
use topcoat::router::error::{bad_request, internal_server_error};
use topcoat::router::{path_param, route};

use self::format::{ProjectionTables, encode};
use super::errors::query_error;
use super::names::channel_summary_name;
use super::require;
use crate::app::{backend, caller, can};
use crate::backend::Backend;
use crate::components::{agent_name, short_id};
use crate::contract::ProjectionId;
use crate::contract::errors::QueryError;
use crate::contract::research::ProjectionPoints;
use crate::url::ulid::UlidId;

path_param!(id);

/// Looks up the display names for a projection's category tables. Topic
/// labels are `None` without `Content` or when the projection's topic
/// version is no longer retained; other backend errors fail the request.
pub async fn tables<B: Backend>(
    backend: &B,
    caller: &Caller,
    points: &ProjectionPoints,
) -> Result<ProjectionTables, QueryError> {
    let mut agents = Vec::with_capacity(points.agents().len());
    for id in points.agents() {
        agents.push(match backend.agent(caller, *id).await? {
            Some(detail) => agent_name(&detail.summary),
            None => short_id(id.to_ulid()),
        });
    }
    let mut channels = Vec::with_capacity(points.channels().len());
    for id in points.channels() {
        channels.push(match backend.channel(caller, *id).await? {
            Some(summary) => channel_summary_name(&summary),
            None => short_id(id.to_ulid()),
        });
    }
    let topics = if can(caller, Permission::Content) {
        match backend
            .topics(caller, points.meta().scope.topic_version)
            .await
        {
            Ok(topics) => points
                .topics()
                .iter()
                .map(|id| topics.iter().find(|t| t.id == *id).map(|t| t.label.clone()))
                .collect(),
            Err(QueryError::VersionNotRetained { .. }) => vec![None; points.topics().len()],
            Err(error) => return Err(error),
        }
    } else {
        vec![None; points.topics().len()]
    };
    ProjectionTables::new(points, agents, channels, topics).map_err(|e| QueryError::Store {
        reason: e.to_string(),
    })
}

#[route(GET "/data/projection/{id}")]
async fn projection_data(cx: &Cx) -> topcoat::Result<Vec<u8>> {
    let caller = caller(cx);
    require(&caller, Permission::Content)?;
    let raw = path_param::<Id>(cx);
    let id = ProjectionId::parse_ulid(raw).map_err(|e| bad_request(format!("id: {e}")))?;
    let backend = backend(cx);
    let points = backend.projection(&caller, id).await.map_err(query_error)?;
    let tables = tables(backend, &caller, &points)
        .await
        .map_err(query_error)?;
    let bytes = encode(&points, &tables).map_err(|e| {
        tracing::error!(error = %e, projection = %raw, "projection encoding failed");
        internal_server_error(e)
    })?;
    tracing::debug!(
        points = points.len(),
        bytes = bytes.len(),
        "projection payload"
    );
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::decode::{DecodeError, decode};
    use super::format::{NONE, ROUTE_KINDS, route_code};
    use super::*;
    use crate::data::fixtures;
    use crate::data::topology::RouteKindCode;

    #[test]
    fn round_trips_every_column() {
        let (points, tables) = fixtures::projection();
        let bytes = encode(&points, &tables).expect("encode");
        let decoded = decode(&bytes).expect("decode");

        assert_eq!(decoded.header.count as usize, points.len());
        assert_eq!(decoded.header.id, points.meta().id.to_ulid());
        assert_eq!(decoded.xs, points.xs());
        assert_eq!(decoded.ys, points.ys());
        for (i, c) in points.categories().iter().enumerate() {
            assert_eq!(decoded.senders[i], c.sender);
            assert_eq!(decoded.readers[i], c.reader);
            assert_eq!(decoded.routes[i], route_code(c.route));
            assert_eq!(decoded.channels[i], c.channel);
            assert_eq!(decoded.topics[i], c.topic);
        }
        let ids: Vec<u128> = points.transmissions().iter().map(|t| t.as_ulid()).collect();
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
    fn header_is_aligned_and_route_table_matches_codes() {
        let (points, tables) = fixtures::projection();
        let bytes = encode(&points, &tables).expect("encode");
        let header_len = u32::from_le_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]) as usize;
        assert_eq!(header_len % 4, 0);
        let n = points.len();
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
        let (points, tables) = fixtures::projection();
        assert!(points.categories().iter().any(|c| c.topic.is_none()));
        assert!(points.categories().iter().any(|c| c.channel.is_none()));
        let bytes = encode(&points, &tables).expect("encode");
        let decoded = decode(&bytes).expect("decode");
        assert!(decoded.channels.iter().flatten().all(|c| *c != NONE));
    }

    #[test]
    fn empty_projection_round_trips() {
        let points = fixtures::empty_projection();
        let tables =
            ProjectionTables::new(&points, Vec::new(), Vec::new(), Vec::new()).expect("tables");
        let bytes = encode(&points, &tables).expect("encode");
        let decoded = decode(&bytes).expect("decode");
        assert_eq!(decoded.header.count, 0);
        assert!(decoded.xs.is_empty());
    }

    #[test]
    fn decoder_rejects_corruption() {
        let (points, tables) = fixtures::projection();
        let bytes = encode(&points, &tables).expect("encode");
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
        let (points, _) = fixtures::projection();
        assert!(ProjectionTables::new(&points, Vec::new(), Vec::new(), Vec::new()).is_err());
    }
}

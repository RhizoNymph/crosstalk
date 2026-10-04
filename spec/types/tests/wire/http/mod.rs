//! The HTTP binding ([`crate::interfaces::l8_surface::http`]): the route
//! table and its golden, a client built over the table that implements
//! `QueryApi` (so every method must name its route), path and query
//! decoding, the status tables, authentication, SSE framing, frame caching
//! and export downloads. Goldens under `golden/http/`.

mod auth;
mod bodies;
mod client;
mod export;
mod frame;
mod request;
mod routes;
mod sse;
mod status;

use std::future::Future;
use std::num::NonZeroU64;
use std::pin::pin;
use std::task::{Context, Poll, Waker};

use super::{ULID_A, ULID_B, ULID_C, id, ts};
use crate::aggregates::edge::{EdgeSelector, RouteKind, TopologyFilter};
use crate::aggregates::filter::TopicVersionSelector;
use crate::aggregates::projection::{ProjectionLimit, ProjectionParams};
use crate::aggregates::series::{BucketWidth, SeriesGrid, SeriesStep};
use crate::aggregates::topic::TopicModelVersion;
use crate::derived::flow::transmission::Route;
use crate::ids::{AgentId, ChannelId, TopicId, TransmissionId};
use crate::paging::{PageRequest, PageSize};
use crate::support::TimeWindow;

const AREA: &str = "http";

/// Runs a future that never waits (the test client's), to completion.
fn ready<F: Future>(future: F) -> F::Output {
    let mut future = pin!(future);
    match future
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
    {
        Poll::Ready(output) => output,
        Poll::Pending => panic!("the test client never waits"),
    }
}

fn agent(text: &str) -> AgentId {
    id(AgentId::from_ulid_text, text)
}

fn channel() -> ChannelId {
    id(ChannelId::from_ulid_text, ULID_B)
}

fn transmission() -> TransmissionId {
    id(TransmissionId::from_ulid_text, ULID_C)
}

/// An hour, on bucket boundaries.
fn window() -> TimeWindow {
    TimeWindow::new(
        ts("2026-10-04T12:00:00.000000Z"),
        ts("2026-10-04T13:00:00.000000Z"),
    )
    .expect("a non-empty window")
}

fn page<L>() -> PageRequest<L> {
    PageRequest {
        size: PageSize::new(50).expect("a valid size"),
        after: None,
    }
}

/// The shared filter, with the lists that make it too long for a URL.
fn filter() -> TopologyFilter {
    TopologyFilter {
        agents: vec![agent(ULID_A), agent(ULID_C)],
        channels: vec![channel()],
        route_kinds: vec![RouteKind::Channel],
        topics: vec![id(TopicId::from_ulid_text, ULID_A)],
        topic_version: TopicVersionSelector::Pinned(TopicModelVersion(3)),
        ..TopologyFilter::default()
    }
}

fn edge() -> EdgeSelector {
    EdgeSelector::new(agent(ULID_A), agent(ULID_C), Route::Channel(channel())).expect("two agents")
}

fn grid() -> SeriesGrid {
    let minute = NonZeroU64::new(60_000_000).expect("non-zero");
    let step = SeriesStep::new(BucketWidth::from_micros(minute), minute).expect("one bucket");
    SeriesGrid::new(window(), step).expect("aligned whole steps")
}

fn params() -> ProjectionParams {
    ProjectionParams::new(
        ProjectionLimit::new(5_000).expect("in range"),
        ProjectionParams::DEFAULT_NEIGHBORS,
        ProjectionParams::DEFAULT_MIN_DIST_MILLI,
        42,
    )
    .expect("valid params")
}

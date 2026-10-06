//! The route table: one [`Route`] per `QueryApi` method, per operator
//! action kind and for the live feed, each with its [`RouteSpec`].
//!
//! [`Route::spec`] and [`Route::index`] match every variant with no
//! wildcard, so a route added to the enum does not compile until it has a
//! row; a test checks [`Route::all`] lists every route at its index. That a
//! new `QueryApi` method has a route is checked by the client the tests
//! build over this table (`tests::http::client`), which implements
//! `QueryApi` and so does not compile until every method names its route.

use super::super::{ActionKind, Permission};
use super::{Arg, Method, ResponseBody, RoutePermission, RouteSpec, Source, Status, Success};

/// Every route of the surface's HTTP API.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Route {
    // QueryApi: channels
    Channel,
    PolicyHistory,
    Channels,
    ChannelNames,
    PromotionPreview,
    ChannelResources,
    ChannelTransmissions,
    // QueryApi: agents
    Agents,
    Agent,
    AgentNames,
    // QueryApi: conversations
    Conversations,
    Conversation,
    ConversationTurns,
    SpanReaders,
    ExchangeTurns,
    SpanPoints,
    ConversationText,
    PartText,
    // QueryApi: rules, sinks, dead letters, alerts
    AlertRules,
    AlertRule,
    Sinks,
    DeadLetters,
    Alerts,
    Alert,
    // QueryApi: linked views and aggregates
    Watermark,
    Topology,
    Overview,
    ChannelTopology,
    EdgeTransmissions,
    TransmissionsById,
    Series,
    // QueryApi: topic history
    TopicVersions,
    TopicSizes,
    TopicLineage,
    // QueryApi: content
    Search,
    Transmission,
    TransmissionEvidence,
    Topics,
    // QueryApi: projections
    FitProjection,
    ProjectionStatus,
    Projections,
    /// `QueryApi::projection`: the frame bytes.
    ProjectionFrame,
    // QueryApi: verdicts, audit, operators, export
    Verdicts,
    DetectionQuality,
    Audit,
    Operators,
    Me,
    Export,
    Present,
    /// `OperatorActions::act`: every kind is `POST /actions`, told apart by
    /// the `ActionRequest`'s `type`.
    Action(ActionKind),
    /// `LiveFeed::subscribe`.
    Live,
}

/// The number of routes before the actions: every one but the actions and
/// the live feed.
const BEFORE_ACTIONS: usize = 49;

impl Route {
    /// Every route, in [`Route::index`] order: the non-action routes in
    /// declaration order, then one per action kind in `ActionKind::ALL`
    /// order, then the live feed.
    pub fn all() -> Vec<Self> {
        let mut routes = vec![
            Self::Channel,
            Self::PolicyHistory,
            Self::Channels,
            Self::ChannelNames,
            Self::PromotionPreview,
            Self::ChannelResources,
            Self::ChannelTransmissions,
            Self::Agents,
            Self::Agent,
            Self::AgentNames,
            Self::Conversations,
            Self::Conversation,
            Self::ConversationTurns,
            Self::SpanReaders,
            Self::ExchangeTurns,
            Self::SpanPoints,
            Self::ConversationText,
            Self::PartText,
            Self::AlertRules,
            Self::AlertRule,
            Self::Sinks,
            Self::DeadLetters,
            Self::Alerts,
            Self::Alert,
            Self::Watermark,
            Self::Topology,
            Self::Overview,
            Self::ChannelTopology,
            Self::EdgeTransmissions,
            Self::TransmissionsById,
            Self::Series,
            Self::TopicVersions,
            Self::TopicSizes,
            Self::TopicLineage,
            Self::Search,
            Self::Transmission,
            Self::TransmissionEvidence,
            Self::Topics,
            Self::FitProjection,
            Self::ProjectionStatus,
            Self::Projections,
            Self::ProjectionFrame,
            Self::Verdicts,
            Self::DetectionQuality,
            Self::Audit,
            Self::Operators,
            Self::Me,
            Self::Export,
            Self::Present,
        ];
        routes.extend(ActionKind::ALL.map(Self::Action));
        routes.push(Self::Live);
        routes
    }

    /// The route's position in [`Route::all`]. Exhaustive, so a new route
    /// does not compile until it has one.
    pub const fn index(self) -> usize {
        match self {
            Self::Channel => 0,
            Self::PolicyHistory => 1,
            Self::Channels => 2,
            Self::ChannelNames => 3,
            Self::PromotionPreview => 4,
            Self::ChannelResources => 5,
            Self::ChannelTransmissions => 6,
            Self::Agents => 7,
            Self::Agent => 8,
            Self::AgentNames => 9,
            Self::Conversations => 10,
            Self::Conversation => 11,
            Self::ConversationTurns => 12,
            Self::SpanReaders => 13,
            Self::ExchangeTurns => 14,
            Self::SpanPoints => 15,
            Self::ConversationText => 16,
            Self::PartText => 17,
            Self::AlertRules => 18,
            Self::AlertRule => 19,
            Self::Sinks => 20,
            Self::DeadLetters => 21,
            Self::Alerts => 22,
            Self::Alert => 23,
            Self::Watermark => 24,
            Self::Topology => 25,
            Self::Overview => 26,
            Self::ChannelTopology => 27,
            Self::EdgeTransmissions => 28,
            Self::TransmissionsById => 29,
            Self::Series => 30,
            Self::TopicVersions => 31,
            Self::TopicSizes => 32,
            Self::TopicLineage => 33,
            Self::Search => 34,
            Self::Transmission => 35,
            Self::TransmissionEvidence => 36,
            Self::Topics => 37,
            Self::FitProjection => 38,
            Self::ProjectionStatus => 39,
            Self::Projections => 40,
            Self::ProjectionFrame => 41,
            Self::Verdicts => 42,
            Self::DetectionQuality => 43,
            Self::Audit => 44,
            Self::Operators => 45,
            Self::Me => 46,
            Self::Export => 47,
            Self::Present => 48,
            Self::Action(kind) => BEFORE_ACTIONS + kind.index(),
            Self::Live => BEFORE_ACTIONS + ActionKind::ALL.len(),
        }
    }

    pub fn method(self) -> Method {
        self.spec().method
    }

    pub fn path(self) -> &'static str {
        self.spec().path
    }

    pub fn permission(self) -> RoutePermission {
        self.spec().permission
    }

    /// The route's row of the table.
    pub fn spec(self) -> RouteSpec {
        use Permission::{Audit, Content, Govern, Operate, View};
        let (method, path, args, success, permission, source): (
            Method,
            &'static str,
            &'static [Arg],
            Success,
            Permission,
            &'static str,
        ) = match self {
            Self::Channel => (
                Method::Get,
                "/channels/{id}",
                const { &[Arg::path("id"), Arg::optional_query("window")] },
                JSON_OK,
                View,
                "channel",
            ),
            Self::PolicyHistory => (
                Method::Get,
                "/channels/{id}/policy-history",
                const { &[Arg::path("id")] },
                JSON_OK,
                View,
                "policy_history",
            ),
            Self::Channels => (
                Method::Get,
                "/channels",
                const { &[Arg::query("filter"), Arg::query("page")] },
                JSON_OK,
                View,
                "channels",
            ),
            Self::ChannelNames => (
                Method::Post,
                "/query/channel-names",
                const { &[Arg::body()] },
                JSON_OK,
                View,
                "channel_names",
            ),
            Self::PromotionPreview => (
                Method::Get,
                "/channels/{id}/promotion-preview",
                const { &[Arg::path("id"), Arg::query("pattern")] },
                JSON_OK,
                View,
                "promotion_preview",
            ),
            Self::ChannelResources => (
                Method::Get,
                "/channels/{id}/resources",
                const { &[Arg::path("id"), Arg::query("window"), Arg::query("page")] },
                JSON_OK,
                View,
                "channel_resources",
            ),
            Self::ChannelTransmissions => (
                Method::Get,
                "/channels/{id}/transmissions",
                const {
                    &[
                        Arg::path("id"),
                        Arg::query("filter"),
                        Arg::query("version"),
                        Arg::query("page"),
                    ]
                },
                JSON_OK,
                View,
                "channel_transmissions",
            ),
            Self::Agents => (
                Method::Get,
                "/agents",
                const {
                    &[
                        Arg::query("filter"),
                        Arg::query("window"),
                        Arg::query("page"),
                    ]
                },
                JSON_OK,
                View,
                "agents",
            ),
            Self::Agent => (
                Method::Get,
                "/agents/{id}",
                const { &[Arg::path("id"), Arg::query("window")] },
                JSON_OK,
                View,
                "agent",
            ),
            Self::AgentNames => (
                Method::Post,
                "/query/agent-names",
                const { &[Arg::body()] },
                JSON_OK,
                View,
                "agent_names",
            ),
            Self::Conversations => (
                Method::Get,
                "/conversations",
                const { &[Arg::query("filter"), Arg::query("page")] },
                JSON_OK,
                View,
                "conversations",
            ),
            Self::Conversation => (
                Method::Get,
                "/conversations/{id}",
                const { &[Arg::path("id")] },
                JSON_OK,
                View,
                "conversation",
            ),
            Self::ConversationTurns => (
                Method::Get,
                "/conversations/{id}/turns",
                const { &[Arg::path("id"), Arg::query("window")] },
                JSON_OK,
                View,
                "conversation_turns",
            ),
            Self::SpanReaders => (
                Method::Get,
                "/spans/{id}/readers",
                const { &[Arg::path("id"), Arg::query("page")] },
                JSON_OK,
                View,
                "span_readers",
            ),
            Self::ExchangeTurns => (
                Method::Post,
                "/query/exchange-turns",
                const { &[Arg::body()] },
                JSON_OK,
                View,
                "exchange_turns",
            ),
            Self::SpanPoints => (
                Method::Post,
                "/query/span-points",
                const { &[Arg::body()] },
                JSON_OK,
                View,
                "span_points",
            ),
            Self::ConversationText => (
                Method::Get,
                "/conversations/{id}/text",
                const { &[Arg::path("id"), Arg::query("window"), Arg::query("limit")] },
                JSON_OK,
                Content,
                "conversation_text",
            ),
            Self::PartText => (
                Method::Post,
                "/query/part-text",
                const { &[Arg::field("part"), Arg::field("slice")] },
                JSON_OK,
                Content,
                "part_text",
            ),
            Self::AlertRules => (
                Method::Get,
                "/alert-rules",
                const { &[Arg::query("filter"), Arg::query("page")] },
                JSON_OK,
                View,
                "alert_rules",
            ),
            Self::AlertRule => (
                Method::Get,
                "/alert-rules/{id}",
                const { &[Arg::path("id")] },
                JSON_OK,
                View,
                "alert_rule",
            ),
            Self::Sinks => (Method::Get, "/sinks", &[], JSON_OK, Govern, "sinks"),
            Self::DeadLetters => (
                Method::Get,
                "/dead-letters",
                const { &[Arg::optional_query("group"), Arg::query("page")] },
                JSON_OK,
                Operate,
                "dead_letters",
            ),
            Self::Alerts => (
                Method::Get,
                "/alerts",
                const { &[Arg::query("filter"), Arg::query("page")] },
                JSON_OK,
                View,
                "alerts",
            ),
            Self::Alert => (
                Method::Get,
                "/alerts/{id}",
                const { &[Arg::path("id")] },
                JSON_OK,
                View,
                "alert",
            ),
            Self::Watermark => (Method::Get, "/watermark", &[], JSON_OK, View, "watermark"),
            Self::Topology => (
                Method::Post,
                "/query/topology",
                const {
                    &[
                        Arg::field("window"),
                        Arg::field("weighting"),
                        Arg::field("filter"),
                    ]
                },
                JSON_OK,
                View,
                "topology",
            ),
            Self::Overview => (
                Method::Post,
                "/query/overview",
                const { &[Arg::field("window"), Arg::field("filter")] },
                JSON_OK,
                View,
                "overview",
            ),
            Self::ChannelTopology => (
                Method::Post,
                "/query/channel-topology",
                const {
                    &[
                        Arg::field("window"),
                        Arg::field("weighting"),
                        Arg::field("filter"),
                    ]
                },
                JSON_OK,
                View,
                "channel_topology",
            ),
            Self::EdgeTransmissions => (
                Method::Post,
                "/query/edge-transmissions",
                const {
                    &[
                        Arg::field("edge"),
                        Arg::field("window"),
                        Arg::field("filter"),
                        Arg::field("page"),
                    ]
                },
                JSON_OK,
                View,
                "edge_transmissions",
            ),
            Self::TransmissionsById => (
                Method::Post,
                "/query/transmissions",
                const {
                    &[
                        Arg::field("selection"),
                        Arg::field("version"),
                        Arg::field("page"),
                    ]
                },
                JSON_OK,
                View,
                "transmissions_by_id",
            ),
            Self::Series => (
                Method::Post,
                "/query/series",
                const {
                    &[
                        Arg::field("grid"),
                        Arg::field("weighting"),
                        Arg::field("grouping"),
                        Arg::field("filter"),
                    ]
                },
                JSON_OK,
                View,
                "series",
            ),
            Self::TopicVersions => (
                Method::Get,
                "/topic-versions",
                &[],
                JSON_OK,
                View,
                "topic_versions",
            ),
            Self::TopicSizes => (
                Method::Get,
                "/topic-sizes",
                const {
                    &[
                        Arg::optional_query("version"),
                        Arg::optional_query("window"),
                    ]
                },
                JSON_OK,
                View,
                "topic_sizes",
            ),
            Self::TopicLineage => (
                Method::Get,
                "/topic-versions/{version}/lineage",
                const { &[Arg::path("version")] },
                JSON_OK,
                View,
                "topic_lineage",
            ),
            Self::Search => (
                Method::Post,
                "/query/search",
                const {
                    &[
                        Arg::field("request"),
                        Arg::field("window"),
                        Arg::field("filter"),
                        Arg::field("page"),
                    ]
                },
                JSON_OK,
                Content,
                "search",
            ),
            Self::Transmission => (
                Method::Get,
                "/transmissions/{id}",
                const { &[Arg::path("id")] },
                JSON_OK,
                Content,
                "transmission",
            ),
            Self::TransmissionEvidence => (
                Method::Get,
                "/transmissions/{id}/evidence",
                const { &[Arg::path("id"), Arg::query("window")] },
                JSON_OK,
                Content,
                "transmission_evidence",
            ),
            Self::Topics => (
                Method::Get,
                "/topics",
                const { &[Arg::query("version"), Arg::query("page")] },
                JSON_OK,
                Content,
                "topics",
            ),
            Self::FitProjection => (
                Method::Post,
                "/projections",
                const {
                    &[
                        Arg::field("window"),
                        Arg::field("filter"),
                        Arg::field("params"),
                    ]
                },
                Success {
                    status: Status::Accepted,
                    body: ResponseBody::Json,
                },
                Content,
                "fit_projection",
            ),
            Self::ProjectionStatus => (
                Method::Get,
                "/projections/{id}",
                const { &[Arg::path("id")] },
                JSON_OK,
                Content,
                "projection_status",
            ),
            Self::Projections => (
                Method::Get,
                "/projections",
                const { &[Arg::query("page")] },
                JSON_OK,
                Content,
                "projections",
            ),
            Self::ProjectionFrame => (
                Method::Get,
                "/projections/{id}/frame",
                const { &[Arg::path("id")] },
                Success {
                    status: Status::Ok,
                    body: ResponseBody::Frame,
                },
                Content,
                "projection",
            ),
            Self::Verdicts => (
                Method::Get,
                "/transmissions/{id}/verdicts",
                const { &[Arg::path("id")] },
                JSON_OK,
                View,
                "verdicts",
            ),
            Self::DetectionQuality => (
                Method::Get,
                "/detection-quality",
                const { &[Arg::query("window")] },
                JSON_OK,
                View,
                "detection_quality",
            ),
            Self::Audit => (
                Method::Get,
                "/audit",
                const { &[Arg::query("filter"), Arg::query("page")] },
                JSON_OK,
                Audit,
                "audit",
            ),
            Self::Operators => (Method::Get, "/operators", &[], JSON_OK, View, "operators"),
            Self::Present => (Method::Get, "/present", &[], JSON_OK, View, "present"),
            Self::Me => {
                return RouteSpec {
                    method: Method::Get,
                    path: "/me",
                    args: &[],
                    success: JSON_OK,
                    permission: RoutePermission::AnyCaller,
                    source: Source::Query("me"),
                };
            }
            Self::Export => {
                return RouteSpec {
                    method: Method::Post,
                    path: "/exports",
                    args: const { &[Arg::body()] },
                    success: Success {
                        status: Status::Ok,
                        body: ResponseBody::Export,
                    },
                    permission: RoutePermission::ByExportRequest,
                    source: Source::Query("export"),
                };
            }
            Self::Action(kind) => {
                return RouteSpec {
                    method: Method::Post,
                    path: "/actions",
                    args: const { &[Arg::body()] },
                    success: JSON_OK,
                    permission: RoutePermission::Fixed(kind.required_permission()),
                    source: Source::Act(kind),
                };
            }
            Self::Live => {
                return RouteSpec {
                    method: Method::Get,
                    path: "/live",
                    args: &[
                        Arg {
                            name: super::sse::LAST_EVENT_ID,
                            place: super::Place::Header,
                            optional: true,
                        },
                        Arg {
                            name: super::sse::CURSOR_PARAM,
                            place: super::Place::QueryText,
                            optional: true,
                        },
                    ],
                    success: Success {
                        status: Status::Ok,
                        body: ResponseBody::EventStream,
                    },
                    permission: RoutePermission::Fixed(View),
                    source: Source::Subscribe,
                };
            }
        };
        RouteSpec {
            method,
            path,
            args,
            success,
            permission: RoutePermission::Fixed(permission),
            source: Source::Query(source),
        }
    }
}

const JSON_OK: Success = Success {
    status: Status::Ok,
    body: ResponseBody::Json,
};

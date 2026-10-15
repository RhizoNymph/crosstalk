//! `detect::live`: the `LiveBackend` seam, end to end, over a test backend
//! whose "layers" are hand-built spec values written into crosstalk-memory's
//! stores (`MemoryFingerprintIndex`, `MemoryChannels`, `MemoryVerdicts`)
//! when the detector settles the world. The world is a bench world
//! converted as `ct-bench-detect` converts one (`convert::world`).
//! Transmissions in every state, spans, accesses (a rejected write among
//! them) and channels go in; the test checks the bench rows they become.

use std::collections::BTreeMap;
use std::num::NonZeroU32;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use a2a_bench_format as bench;
use bench::check::{WorldInputs, check_predictions};
use bench::exchange::{
    AgentDecl, Client, Driven, Exchange, Fidelity, Request, Response, WorldDecl,
};
use bench::ids::{DatasetId, SourceRef, WorldKey, exchange_id};
use bench::json::CanonicalJson;
use bench::labels::{CarrierKind, Codec as BenchCodec, MatchClass as BenchClass};
use bench::message::{
    AssistantPart, Body, ResultContent, SystemPart, ToolArguments, ToolCall as BenchCall,
    ToolExecution, ToolOutcome, ToolPart, ToolResult, UserPart,
};
use bench::predictions::{MatchKind as BenchKind, PredictedRoute, Prediction, Quality, State};
use bench::resource::Resource as BenchResource;
use crosstalk_bench_adapter::convert::{self, ConvertedWorld};
use crosstalk_bench_adapter::detect::live::{
    Attribution, BackendError, LiveBackend, LiveDetector, LiveSettings, LiveWorld, RawDetection,
    all_time,
};
use crosstalk_bench_adapter::directory::BenchDirectory;
use crosstalk_bench_adapter::location::whole_part;
use crosstalk_bench_adapter::reads::RegistryResources;
use crosstalk_bench_adapter::to_bench::predictions::{Unlocated, held, rows};
use crosstalk_bench_adapter::to_bench::{Lossy, ids};
use crosstalk_memory::analysis::aliases::StaticDirectory;
use crosstalk_memory::flow::{MemoryChannels, MemoryVerdicts};
use crosstalk_memory::provenance::{IndexConfig, MemoryFingerprintIndex};
use crosstalk_memory::support::{IdSequence, Outbox};
use crosstalk_spec::batch::IdBatch;
use crosstalk_spec::derived::flow::access::{Access, AccessOp, Extraction, WriteOutcome};
use crosstalk_spec::derived::flow::evidence::{CoAccess, InvalidCoAccess};
use crosstalk_spec::derived::flow::resource::{Locator, Resource};
use crosstalk_spec::derived::flow::transmission::{
    Confirmed, Route, Transmission, TransmissionState,
};
use crosstalk_spec::derived::provenance::matching::{Carrier, Codec, ContentMatch, MatchKind};
use crosstalk_spec::derived::provenance::span::{OriginatedSpan, Span, SpanLocation, SpanState};
use crosstalk_spec::ids::{
    AccessId, AgentId, ChannelId, ConversationId, ExchangeId, ResourceId, SpanId, TransmissionId,
};
use crosstalk_spec::interfaces::l1_canonical::NormalizedExchange;
use crosstalk_spec::interfaces::l4_provenance::SpanIndex;
use crosstalk_spec::interfaces::l5_flow::channels::ChannelTraffic;
use crosstalk_spec::interfaces::l5_flow::transmissions::TransmissionStore;
use crosstalk_spec::observed::message::{Message, PartRef, ToolCallId};
use crosstalk_spec::support::{NonEmpty, TimeWindow, Timestamp};

const NOTE: &str = "The audit owner is Omar and the review closes on Friday at noon.";
const TODO: &str = "Next: reconcile the vendor ledger before the board meeting starts.";

/// What the "layers" decide for one world: spec values the backend writes
/// into crosstalk-memory's stores at `settle`.
#[derive(Clone)]
struct Script {
    resources: Vec<Resource>,
    accesses: Vec<Access>,
    discoveries: Vec<(ChannelId, ResourceId, TransmissionId, Timestamp)>,
    spans: Vec<OriginatedSpan>,
    transmissions: Vec<Transmission>,
    attribution: BTreeMap<ExchangeId, Attribution>,
}

/// What the detector did to the backend, in order.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Call {
    Build {
        start: Timestamp,
        settle_after: Duration,
    },
    Ingest {
        exchange: ExchangeId,
        at: Timestamp,
    },
    Settle {
        until: Timestamp,
    },
    Shutdown,
}

#[derive(Clone)]
struct ScriptedBackend {
    script: Script,
    calls: Arc<Mutex<Vec<Call>>>,
}

impl ScriptedBackend {
    fn new(script: Script) -> Self {
        Self {
            script,
            calls: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn calls(&self) -> Vec<Call> {
        self.calls
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

struct ScriptedWorld {
    script: Script,
    calls: Arc<Mutex<Vec<Call>>>,
    spans: MemoryFingerprintIndex,
    channels: MemoryChannels<StaticDirectory>,
    resources: RegistryResources<MemoryChannels<StaticDirectory>>,
    transmissions: MemoryVerdicts,
    stored: Vec<TransmissionId>,
    settled: bool,
}

impl ScriptedWorld {
    fn log(&self, call: Call) {
        self.calls
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(call);
    }
}

fn store_error(error: impl std::fmt::Debug) -> BackendError {
    BackendError::Build {
        reason: format!("{error:?}"),
    }
}

impl LiveBackend for ScriptedBackend {
    type World = ScriptedWorld;

    async fn build(
        &mut self,
        settings: &LiveSettings,
        start: Timestamp,
    ) -> Result<ScriptedWorld, BackendError> {
        let channels = MemoryChannels::new(
            StaticDirectory::default(),
            IdSequence::new(0),
            Outbox::none(),
        );
        let window = all_time().map_err(store_error)?;
        let world = ScriptedWorld {
            script: self.script.clone(),
            calls: Arc::clone(&self.calls),
            spans: MemoryFingerprintIndex::new(IndexConfig::single_node(
                1000,
                Duration::from_secs(3600),
            )),
            resources: RegistryResources::new(channels.clone(), window),
            channels,
            transmissions: MemoryVerdicts::new(Outbox::none()),
            stored: Vec::new(),
            settled: false,
        };
        world.log(Call::Build {
            start,
            settle_after: settings.timing.settle_after(),
        });
        Ok(world)
    }
}

impl LiveWorld for ScriptedWorld {
    type Spans = MemoryFingerprintIndex;
    type Accesses = MemoryChannels<StaticDirectory>;
    type Channels = RegistryResources<MemoryChannels<StaticDirectory>>;

    async fn ingest(
        &mut self,
        exchange: NormalizedExchange,
        at: Timestamp,
    ) -> Result<(), BackendError> {
        self.log(Call::Ingest {
            exchange: exchange.exchange.meta.id,
            at,
        });
        Ok(())
    }

    /// The layers' decisions land in the stores here, as the real
    /// composition's would by the time it settles.
    async fn settle(&mut self, until: Timestamp) -> Result<(), BackendError> {
        self.log(Call::Settle { until });
        if self.settled {
            return Ok(());
        }
        self.settled = true;
        let script = self.script.clone();
        for resource in script.resources {
            self.channels
                .add_resource(resource)
                .await
                .map_err(store_error)?;
        }
        for access in script.accesses {
            self.channels
                .record_access(access)
                .await
                .map_err(store_error)?;
        }
        for (channel, resource, transmission, at) in script.discoveries {
            self.channels
                .discover(channel, resource, transmission, at)
                .await
                .map_err(store_error)?;
        }
        for span in &script.spans {
            self.spans.record(span).await.map_err(store_error)?;
        }
        for transmission in script.transmissions {
            if matches!(transmission.route, Route::Channel(_)) {
                self.channels
                    .record_transmission(&transmission)
                    .await
                    .map_err(store_error)?;
            }
            self.stored.push(transmission.id);
            self.transmissions
                .save(transmission)
                .await
                .map_err(store_error)?;
        }
        Ok(())
    }

    async fn transmissions(&self, window: TimeWindow) -> Result<Vec<Transmission>, BackendError> {
        let mut out = Vec::new();
        // Listed newest first, as a store's list would; the detector sorts.
        for id in self.stored.iter().rev() {
            let stored = self
                .transmissions
                .transmission(*id)
                .await
                .map_err(store_error)?;
            out.extend(stored.filter(|t| window.contains(t.opened_at)));
        }
        Ok(out)
    }

    fn spans(&self) -> &MemoryFingerprintIndex {
        &self.spans
    }

    fn accesses(&self) -> &MemoryChannels<StaticDirectory> {
        &self.channels
    }

    fn channels(&self) -> &RegistryResources<MemoryChannels<StaticDirectory>> {
        &self.resources
    }

    async fn attribution(
        &self,
        exchanges: &IdBatch<ExchangeId>,
    ) -> Result<BTreeMap<ExchangeId, Attribution>, BackendError> {
        Ok(exchanges
            .ids()
            .iter()
            .filter_map(|id| self.script.attribution.get(id).map(|a| (*id, *a)))
            .collect())
    }

    async fn shutdown(self) {
        self.log(Call::Shutdown);
    }
}

/// The world: Alice writes a note to a shared file, Bob reads it; Alice
/// writes a to-do, Bob reads it; Alice's third write is rejected.
struct Scene {
    inputs: WorldInputs,
    world: ConvertedWorld,
    /// Exchange ids in time order: A1 B2 B3 A4 B5 B6 A7.
    exchanges: Vec<ExchangeId>,
    /// Whether each exchange (by position) is Alice's.
    alice: Vec<bool>,
    /// Alice's write calls (A1, A4) and Bob's read results (B3, B6), as
    /// spec messages.
    note_call: Message,
    todo_call: Message,
    note_read: Message,
    todo_read: Message,
}

fn notes() -> Locator {
    Locator::File {
        host: None,
        path: "/shared/notes.md".into(),
    }
}

fn todo() -> Locator {
    Locator::File {
        host: None,
        path: "/shared/todo.md".into(),
    }
}

fn whole(message: &Message) -> SpanLocation {
    whole_part(message, 0).unwrap_or_else(|e| panic!("{e}"))
}

/// The world's clock: `n` seconds after a fixed start.
fn tick(n: u64) -> Timestamp {
    Timestamp::from_micros(1_767_225_600_000_000 + n * 1_000_000)
}

fn bench_message(body: Body) -> bench::message::Message {
    bench::message::Message::new(body).unwrap_or_else(|e| panic!("{e}"))
}

fn system(text: &str) -> bench::message::Message {
    bench_message(Body::System(vec![SystemPart::Text { text: text.into() }]))
}

fn user(text: &str) -> bench::message::Message {
    bench_message(Body::User(vec![UserPart::Text { text: text.into() }]))
}

fn says(text: &str) -> bench::message::Message {
    bench_message(Body::Assistant(vec![AssistantPart::Text {
        text: text.into(),
    }]))
}

/// An assistant message making one client tool call with JSON arguments.
fn calls(id: &str, name: &str, arguments: &serde_json::Value) -> bench::message::Message {
    let json =
        CanonicalJson::canonicalize(&arguments.to_string()).unwrap_or_else(|e| panic!("{e}"));
    bench_message(Body::Assistant(vec![AssistantPart::ToolCall(BenchCall {
        call_id: id.into(),
        name: name.into(),
        arguments: ToolArguments::Json(json),
        execution: ToolExecution::Client,
    })]))
}

fn result(call_id: &str, text: &str) -> bench::message::Message {
    bench_message(Body::Tool(vec![ToolPart::ToolResult(ToolResult {
        call_id: call_id.into(),
        content: vec![ResultContent::Text { text: text.into() }],
        outcome: ToolOutcome::Success,
    })]))
}

fn spec(message: &bench::message::Message) -> Message {
    convert::message(message).unwrap_or_else(|e| panic!("{e}"))
}

const KEY: &str = "k:0101010101010101010101010101010101010101010101010101010101010101";

fn scene() -> Scene {
    let dataset = DatasetId::new("synthetic").unwrap_or_else(|e| panic!("{e}"));
    let key = WorldKey::new("live").unwrap_or_else(|e| panic!("{e}"));
    let (sys_a, sys_b) = (system("You are Alice."), system("You are Bob."));
    let (start, go, next) = (user("start"), user("go"), user("next"));
    let note_call = calls(
        "call_w",
        "write_file",
        &serde_json::json!({ "path": "/shared/notes.md", "content": NOTE }),
    );
    let todo_call = calls(
        "call_w2",
        "write_file",
        &serde_json::json!({ "path": "/shared/todo.md", "content": TODO }),
    );
    let log_call = calls(
        "call_w3",
        "write_file",
        &serde_json::json!({ "path": "/shared/log.md", "content": "x".repeat(400) }),
    );
    let read_note = calls(
        "call_r",
        "read_file",
        &serde_json::json!({ "path": "/shared/notes.md" }),
    );
    let read_todo = calls(
        "call_r2",
        "read_file",
        &serde_json::json!({ "path": "/shared/todo.md" }),
    );
    let (note_read, todo_read) = (result("call_r", NOTE), result("call_r2", TODO));
    let (wrote, wrote_todo) = (result("call_w", "ok"), result("call_w2", "ok"));
    let (noted, done) = (says("noted"), says("done"));
    let steps: Vec<(
        &str,
        u64,
        Vec<&bench::message::Message>,
        &bench::message::Message,
    )> = vec![
        ("alice", 1, vec![&sys_a, &start], &note_call),
        ("bob", 2, vec![&sys_b, &go], &read_note),
        ("bob", 3, vec![&sys_b, &go, &read_note, &note_read], &noted),
        (
            "alice",
            4,
            vec![&sys_a, &start, &note_call, &wrote],
            &todo_call,
        ),
        (
            "bob",
            5,
            vec![&sys_b, &go, &read_note, &note_read, &noted, &next],
            &read_todo,
        ),
        (
            "bob",
            6,
            vec![
                &sys_b, &go, &read_note, &note_read, &noted, &next, &read_todo, &todo_read,
            ],
            &done,
        ),
        (
            "alice",
            7,
            vec![&sys_a, &start, &note_call, &wrote, &todo_call, &wrote_todo],
            &log_call,
        ),
    ];
    let mut messages: BTreeMap<bench::ids::MessageId, bench::message::Message> = BTreeMap::new();
    let mut exchanges = Vec::new();
    for (agent, at, request, response) in &steps {
        for message in request.iter().copied().chain([*response]) {
            messages.insert(message.id(), message.clone());
        }
        let source = SourceRef::new("live.json", format!("/{agent}/{at}"));
        let at_us = bench::time::Timestamp::from_micros(tick(*at).as_micros());
        exchanges.push(Exchange {
            id: exchange_id(&dataset, &source, at_us),
            at_us,
            client: Client {
                credential: KEY.to_owned(),
                session: Some(format!("session-{agent}")),
                turn: None,
                vendor: Some("openai".to_owned()),
                model: Some("m".to_owned()),
            },
            request: Request {
                messages: request.iter().map(|message| message.id()).collect(),
                tools: None,
            },
            response: Response {
                messages: vec![response.id()],
                stop: Some("end_turn".to_owned()),
                error: None,
            },
            fidelity: Fidelity::Synthetic,
            source,
        });
    }
    let agent = |name: &str| AgentDecl {
        key: bench::ids::AgentKey::new(name).unwrap_or_else(|e| panic!("{e}")),
        driven: Driven::Model,
        model: Some("m".to_owned()),
    };
    let decl = WorldDecl {
        key: key.clone(),
        agents: vec![agent("alice"), agent("bob")],
    };
    let ids: Vec<ExchangeId> = exchanges
        .iter()
        .map(|exchange| ExchangeId::from_ulid(exchange.id.raw()))
        .collect();
    let inputs = WorldInputs::new(&key, messages.into_values().collect(), decl, exchanges)
        .unwrap_or_else(|e| panic!("{e}"));
    let world = convert::world(&dataset, &inputs).unwrap_or_else(|e| panic!("{e}"));
    Scene {
        inputs,
        world,
        exchanges: ids,
        alice: steps.iter().map(|(agent, ..)| *agent == "alice").collect(),
        note_call: spec(&note_call),
        todo_call: spec(&todo_call),
        note_read: spec(&note_read),
        todo_read: spec(&todo_read),
    }
}

/// L3's own ids: the detector's agents, written as rows by their ULIDs.
const ALICE: AgentId = AgentId::from_ulid(9001);
const BOB: AgentId = AgentId::from_ulid(9002);

fn part(message: &Message) -> PartRef {
    PartRef {
        message: message.hash,
        index: 0,
    }
}

fn write(
    id: u128,
    exchange: ExchangeId,
    resource: u128,
    at: u64,
    call: &Message,
    outcome: WriteOutcome,
) -> Access {
    Access {
        id: AccessId::from_ulid(id),
        agent: ALICE,
        exchange,
        resource: ResourceId::from_ulid(resource),
        at: tick(at),
        via: Extraction::Structured,
        op: AccessOp::Write {
            call: part(call),
            spans: vec![SpanId::from_ulid(id)],
            outcome,
        },
    }
}

fn read(id: u128, exchange: ExchangeId, resource: u128, at: u64, got: &Message) -> Access {
    Access {
        id: AccessId::from_ulid(id),
        agent: BOB,
        exchange,
        resource: ResourceId::from_ulid(resource),
        at: tick(at),
        via: Extraction::Structured,
        op: AccessOp::Read { result: part(got) },
    }
}

fn resource(id: u128, locator: Locator, at: u64) -> Resource {
    Resource {
        id: ResourceId::from_ulid(id),
        locator,
        first_seen: tick(at),
    }
}

const WINDOW: Duration = Duration::from_secs(60);

/// The layers' decisions over `scene`; `attribute` names L3's agent of an
/// exchange by whether it is Alice's.
fn script(scene: &Scene, attribute: impl Fn(bool) -> AgentId) -> Script {
    let [a1, _b2, b3, a4, _b5, b6, a7] = scene.exchanges[..] else {
        panic!("seven exchanges");
    };
    let note_write = write(11, a1, 1, 1, &scene.note_call, WriteOutcome::Delivered);
    let note_read = read(12, b3, 1, 3, &scene.note_read);
    let todo_write = write(21, a4, 2, 4, &scene.todo_call, WriteOutcome::Delivered);
    let todo_read = read(22, b6, 2, 6, &scene.todo_read);
    let log_write = write(31, a7, 3, 7, &scene.todo_call, WriteOutcome::Rejected);
    let co_note =
        CoAccess::new(&note_write, &note_read, WINDOW).unwrap_or_else(|e| panic!("{e:?}"));
    let co_todo =
        CoAccess::new(&todo_write, &todo_read, WINDOW).unwrap_or_else(|e| panic!("{e:?}"));
    let note_span = OriginatedSpan::new(Span {
        id: SpanId::from_ulid(11),
        location: whole(&scene.note_call),
        agent: ALICE,
        exchange: a1,
        state: SpanState::Originated,
    })
    .unwrap_or_else(|| panic!("originated"));
    let content = |kind: MatchKind| {
        ContentMatch::new(
            SpanId::from_ulid(11),
            ALICE,
            BOB,
            b3,
            whole(&scene.note_read),
            Carrier::ToolResult(ToolCallId("call_r".into())),
            kind,
            NonZeroU32::MIN,
        )
        .unwrap_or_else(|e| panic!("{e:?}"))
    };
    let confirmed = Confirmed::new(
        NonEmpty::from_vec(vec![
            content(MatchKind::Decoded(NonEmpty::new(Codec::JsonString))),
            content(MatchKind::Exact),
        ])
        .unwrap_or_else(|| panic!("matches")),
        vec![co_note],
        tick(3),
    )
    .unwrap_or_else(|e| panic!("{e:?}"));
    let (c1, c2) = (ChannelId::from_ulid(101), ChannelId::from_ulid(102));
    let transmission =
        |id: u128, route: Route, opened: u64, state: TransmissionState| Transmission {
            id: TransmissionId::from_ulid(id),
            to: BOB,
            route,
            opened_at: tick(opened),
            state,
        };
    let transmissions = vec![
        transmission(
            1,
            Route::Channel(c1),
            3,
            TransmissionState::Confirmed(confirmed),
        ),
        transmission(
            2,
            Route::Channel(c2),
            6,
            TransmissionState::Suspected {
                co_access: NonEmpty::new(co_todo),
                since: tick(6),
            },
        ),
        transmission(
            3,
            Route::Channel(c1),
            3,
            TransmissionState::Discarded {
                at: tick(9),
                co_access: NonEmpty::new(co_note),
            },
        ),
        transmission(
            4,
            Route::Channel(c2),
            6,
            TransmissionState::AwaitingContent {
                co_access: co_todo,
                window_closes_at: tick(8),
            },
        ),
        transmission(5, Route::Unobserved, 6, TransmissionState::Detected),
    ];
    let attribution = scene
        .exchanges
        .iter()
        .zip(&scene.alice)
        .map(|(exchange, alice)| {
            let agent = attribute(*alice);
            let conversation = ConversationId::from_ulid(u128::from(agent == ALICE) + 1);
            (
                *exchange,
                Attribution {
                    agent,
                    conversation,
                },
            )
        })
        .collect();
    Script {
        resources: vec![
            resource(1, notes(), 1),
            resource(2, todo(), 4),
            resource(
                3,
                Locator::File {
                    host: None,
                    path: "/shared/log.md".into(),
                },
                7,
            ),
        ],
        accesses: vec![note_write, note_read, todo_write, todo_read, log_write],
        discoveries: vec![
            (
                c1,
                ResourceId::from_ulid(1),
                TransmissionId::from_ulid(1),
                tick(3),
            ),
            (
                c2,
                ResourceId::from_ulid(2),
                TransmissionId::from_ulid(2),
                tick(6),
            ),
        ],
        spans: vec![note_span],
        transmissions,
        attribution,
    }
}

fn faithful(alice: bool) -> AgentId {
    if alice { ALICE } else { BOB }
}

fn detector(backend: ScriptedBackend) -> LiveDetector<ScriptedBackend> {
    LiveDetector::new(
        backend,
        LiveSettings::short(7).unwrap_or_else(|e| panic!("{e}")),
    )
    .unwrap_or_else(|e| panic!("{e}"))
}

fn detect(scene: &Scene, backend: ScriptedBackend) -> RawDetection {
    detector(backend)
        .detect_exchanges(&scene.world.timed())
        .unwrap_or_else(|e| panic!("{e}"))
        .unwrap_or_else(|| panic!("a world with exchanges"))
}

/// The detection's bench rows, as `ct-bench-detect` writes a live world's.
fn bench_rows(scene: &Scene, raw: &RawDetection, lossy: &mut Lossy) -> Vec<Prediction> {
    let directory = BenchDirectory::new(&scene.world, &raw.resolved);
    let rows = rows(
        &raw.transmissions,
        &directory,
        &held(&raw.attribution),
        &BTreeMap::new(),
        Unlocated::Fail,
        &scene.world.index,
        lossy,
    )
    .unwrap_or_else(|e| panic!("{e}"));
    check_predictions(&scene.inputs, &rows).unwrap_or_else(|e| panic!("{e}"));
    rows
}

fn location(scene: &Scene, exchange: ExchangeId, message: &Message) -> bench::location::Location {
    scene
        .world
        .index
        .location(exchange, &whole(message))
        .unwrap_or_else(|e| panic!("{e}"))
}

fn agent(id: AgentId) -> bench::ids::DetectorAgent {
    ids::detector_agent(id).unwrap_or_else(|e| panic!("{e}"))
}

fn file(path: &str) -> BenchResource {
    BenchResource::File {
        host: None,
        path: path.into(),
    }
}

#[test]
fn the_detector_builds_ingests_in_order_settles_and_shuts_down() {
    let scene = scene();
    let backend = ScriptedBackend::new(script(&scene, faithful));
    let mut live = detector(backend.clone());
    let timed = scene.world.timed();
    live.detect_exchanges(&timed)
        .unwrap_or_else(|e| panic!("{e}"));
    let settle_after = Duration::from_secs(70);
    let mut expected = vec![Call::Build {
        start: tick(1),
        settle_after,
    }];
    expected.extend(timed.iter().map(|e| Call::Ingest {
        exchange: e.exchange.exchange.meta.id,
        at: e.at,
    }));
    expected.push(Call::Settle {
        until: live.settle_at(tick(7)),
    });
    expected.push(Call::Shutdown);
    assert_eq!(backend.calls(), expected);
    assert_eq!(
        live.settle_at(tick(7)).as_micros(),
        tick(7).as_micros() + 70_000_000
    );
}

#[test]
fn every_state_becomes_its_rows() {
    let scene = scene();
    let raw = detect(&scene, ScriptedBackend::new(script(&scene, faithful)));
    let ids: Vec<u128> = raw.transmissions.iter().map(|t| t.id.as_ulid()).collect();
    assert_eq!(ids, vec![1, 2, 3, 4, 5], "every state, sorted by id");
    let mut lossy = Lossy::default();
    let rows = bench_rows(&scene, &raw, &mut lossy);
    let [a1, b2, b3, a4, b5, b6, a7] = scene.exchanges[..] else {
        panic!("seven exchanges");
    };
    // Attribution first, by agent id: L3's placement of every exchange.
    let attributions: Vec<_> = rows
        .iter()
        .filter_map(|row| match row {
            Prediction::Attribution(row) => Some(row.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(attributions.len(), 2);
    assert_eq!(attributions[0].agent, agent(ALICE));
    assert_eq!(
        attributions[0].exchanges,
        [a1, a4, a7].map(ids::exchange).to_vec()
    );
    assert_eq!(attributions[1].agent, agent(BOB));
    assert_eq!(
        attributions[1].exchanges,
        [b2, b3, b5, b6].map(ids::exchange).to_vec()
    );
    assert!(
        !rows
            .iter()
            .any(|row| matches!(row, Prediction::Unattributed(_))),
        "both agents hold exchanges"
    );
    let transmissions: Vec<_> = rows
        .iter()
        .filter_map(|row| match row {
            Prediction::Transmission(row) => Some(row.fields().clone()),
            _ => None,
        })
        .collect();
    assert_eq!(transmissions.len(), 5, "one row per transmission");

    // Confirmed: one match per content match, with the strongest class as
    // its quality.
    let confirmed = &transmissions[0];
    assert_eq!(confirmed.state, State::Confirmed);
    assert_eq!(
        confirmed.quality,
        Some(Quality::Content {
            class: BenchClass::Exact,
            carrier: CarrierKind::ToolResult
        })
    );
    assert_eq!(confirmed.matches.len(), 2);
    let mut kinds: Vec<String> = confirmed
        .matches
        .iter()
        .map(|evidence| format!("{:?}", evidence.kind))
        .collect();
    kinds.sort();
    let mut expected = vec![
        format!("{:?}", BenchKind::Exact),
        format!(
            "{:?}",
            BenchKind::Decoded {
                codecs: vec![BenchCodec::JsonString]
            }
        ),
    ];
    expected.sort();
    assert_eq!(kinds, expected);
    for evidence in &confirmed.matches {
        assert_eq!((&evidence.from, &evidence.to), (&agent(ALICE), &agent(BOB)));
        assert_eq!(evidence.carrier, CarrierKind::ToolResult);
        assert_eq!(evidence.reader_exchange, ids::exchange(b3));
        assert_eq!(evidence.read_at, location(&scene, b3, &scene.note_read));
        assert_eq!(
            evidence.route,
            PredictedRoute::Channel {
                resources: vec![file("/shared/notes.md")]
            }
        );
        assert_eq!(
            evidence.origin_at,
            Some(location(&scene, a1, &scene.note_call)),
            "from L4's span record"
        );
    }
    assert!(confirmed.co_access.is_empty());
    assert_eq!(lossy.similarities, 0);

    // Suspected and discarded: one co-access per record, the write's
    // agent to the read's, at the read's whole result and the write's
    // whole call.
    let suspected = &transmissions[1];
    assert_eq!(suspected.state, State::Suspected);
    assert_eq!(suspected.quality, Some(Quality::Suspected));
    assert!(suspected.matches.is_empty());
    let [record] = &suspected.co_access[..] else {
        panic!("one suspected co-access");
    };
    assert_eq!((&record.from, &record.to), (&agent(ALICE), &agent(BOB)));
    assert_eq!(
        record.reader_exchange,
        ids::exchange(b6),
        "the read's exchange"
    );
    assert_eq!(
        record.read_at,
        location(&scene, b6, &scene.todo_read),
        "the result it read"
    );
    assert_eq!(record.write_exchange, ids::exchange(a4));
    assert_eq!(
        record.write_at,
        location(&scene, a4, &scene.todo_call),
        "the write's call"
    );
    assert_eq!(record.resource, file("/shared/todo.md"));
    let discarded = &transmissions[2];
    assert_eq!(discarded.state, State::Discarded);
    assert_eq!(discarded.quality, Some(Quality::Discarded));
    let [record] = &discarded.co_access[..] else {
        panic!("one discarded co-access");
    };
    assert_eq!(record.reader_exchange, ids::exchange(b3));

    // Awaiting content and detected: undecided, no quality, no evidence.
    for (row, state) in transmissions[3..]
        .iter()
        .zip([State::AwaitingContent, State::Detected])
    {
        assert_eq!(row.state, state);
        assert_eq!(row.quality, None);
        assert!(row.matches.is_empty() && row.co_access.is_empty());
    }
}

#[test]
fn rejected_writes_are_recorded_but_never_paired() {
    let scene = scene();
    let script = script(&scene, faithful);
    let rejected = script
        .accesses
        .iter()
        .find(|a| {
            matches!(
                a.op,
                AccessOp::Write {
                    outcome: WriteOutcome::Rejected,
                    ..
                }
            )
        })
        .unwrap_or_else(|| panic!("a rejected write"));
    let reread = read(32, scene.exchanges[5], 3, 8, &scene.todo_read);
    assert_eq!(
        CoAccess::new(rejected, &reread, WINDOW),
        Err(InvalidCoAccess::RejectedWrite)
    );
    let raw = detect(&scene, ScriptedBackend::new(script));
    assert!(
        raw.transmissions
            .iter()
            .flat_map(|t| t.state.co_accesses())
            .all(|co| co.write() != AccessId::from_ulid(31)),
        "no transmission rests on the rejected write"
    );
}

#[test]
fn an_attribution_merging_two_agents_is_written_as_the_detector_made_it() {
    // One detector agent over both true agents' exchanges is not a
    // failure here: the attribution row holds every exchange, and the
    // bench's scorer fails the world on the merge.
    let scene = scene();
    let raw = detect(&scene, ScriptedBackend::new(script(&scene, |_| ALICE)));
    let rows = bench_rows(&scene, &raw, &mut Lossy::default());
    let attributions: Vec<_> = rows
        .iter()
        .filter_map(|row| match row {
            Prediction::Attribution(row) => Some(row),
            _ => None,
        })
        .collect();
    let [only] = attributions[..] else {
        panic!("one attribution row: {attributions:?}");
    };
    assert_eq!(only.agent, agent(ALICE));
    assert_eq!(only.exchanges.len(), scene.exchanges.len());
    assert!(
        rows.iter()
            .any(|row| matches!(row, Prediction::Unattributed(row) if row.agent == agent(BOB))),
        "Bob, named by the evidence, holds no exchange"
    );
}

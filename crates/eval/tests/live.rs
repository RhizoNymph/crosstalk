//! `detect::live`: the `LiveBackend` seam, end to end, over a test backend
//! whose "layers" are hand-built spec values written into crosstalk-memory's
//! stores (`MemoryFingerprintIndex`, `MemoryChannels`, `MemoryVerdicts`)
//! when the detector settles the world. Transmissions in every state,
//! spans, accesses (a rejected write among them) and channels go in; the
//! test checks the predictions they become and how they are scored.

mod common;

use std::collections::BTreeMap;
use std::num::NonZeroU32;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use common::{calls, dataset, draft, result, says, system, tick, user};
use crosstalk_eval::corpus::{Coverage, Driven, HashedMessage, InMemory, World, WorldBuilder};
use crosstalk_eval::detect::live::{
    Attribution, BackendError, LiveBackend, LiveDetector, LiveError, LiveSettings, LiveWorld,
    all_time, gateway_backend,
};
use crosstalk_eval::keys::{AgentKey, SourceRef, WorldKey};
use crosstalk_eval::location::whole_part;
use crosstalk_eval::pipeline::{DetectError, Detector, WorldError, predictions, run};
use crosstalk_eval::predict::reads::RegistryResources;
use crosstalk_eval::predict::{EvidenceClass, PredictedRoute, WorldDirectory};
use crosstalk_eval::score::quality::detection_quality;
use crosstalk_eval::score::{Scorer, Selector};
use crosstalk_eval::truth::{
    CarrierKind, Expectation, ExpectedContent, ExpectedTransmission, MatchNeed, RouteExpectation,
    Tier, TransmissionLabel,
};
use crosstalk_memory::analysis::aliases::StaticDirectory;
use crosstalk_memory::flow::{MemoryChannels, MemoryVerdicts};
use crosstalk_memory::provenance::{IndexConfig, MemoryFingerprintIndex};
use crosstalk_memory::support::{IdSequence, Outbox};
use crosstalk_spec::aggregates::edge::RouteKind;
use crosstalk_spec::aggregates::quality::{MatchClass, QualityMatch};
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
use crosstalk_spec::observed::message::{PartRef, ToolCallId};
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
    world: World,
    alice: AgentKey,
    bob: AgentKey,
    /// Exchange ids in time order: A1 B2 B3 A4 B5 B6 A7.
    exchanges: Vec<ExchangeId>,
    /// Alice's write calls (A1, A4) and Bob's read results (B3, B6).
    note_call: HashedMessage,
    todo_call: HashedMessage,
    note_read: HashedMessage,
    todo_read: HashedMessage,
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

fn whole(message: &HashedMessage) -> SpanLocation {
    whole_part(message.message(), 0).unwrap_or_else(|e| panic!("{e}"))
}

fn label(
    scene_world: (&AgentKey, &AgentKey),
    sender: ExchangeId,
    reader: ExchangeId,
    resource: Locator,
    text: &str,
    at: SpanLocation,
) -> Expectation {
    Expectation::Transmission(
        ExpectedTransmission::new(TransmissionLabel {
            from: scene_world.0.clone(),
            to: scene_world.1.clone(),
            sender_exchange: Some(sender),
            reader_exchange: reader,
            route: RouteExpectation::Channel { resource },
            carrier: CarrierKind::ToolResult,
            content: ExpectedContent {
                text: text.into(),
                at,
            },
            needs: MatchNeed::Exact,
            tier: Tier::Construction,
            source: SourceRef::new("live.json", format!("/labels/{text}")),
        })
        .unwrap_or_else(|e| panic!("{e}")),
    )
}

fn scene() -> Scene {
    let mut builder = WorldBuilder::new(dataset(), WorldKey::new("live"));
    let alice = builder
        .agent("alice", Driven::Model, "m")
        .unwrap_or_else(|e| panic!("{e}"));
    let bob = builder
        .agent("bob", Driven::Model, "m")
        .unwrap_or_else(|e| panic!("{e}"));
    let (sys_a, sys_b) = (system("You are Alice."), system("You are Bob."));
    let (start, go) = (user("start"), user("go"));
    let note_call = calls(
        "call_w",
        "write_file",
        &serde_json::json!({ "path": "/shared/notes.md", "content": NOTE }).to_string(),
    );
    let todo_call = calls(
        "call_w2",
        "write_file",
        &serde_json::json!({ "path": "/shared/todo.md", "content": TODO }).to_string(),
    );
    let log_call = calls(
        "call_w3",
        "write_file",
        &serde_json::json!({ "path": "/shared/log.md", "content": "x".repeat(400) }).to_string(),
    );
    let read_note = calls(
        "call_r",
        "read_file",
        &serde_json::json!({ "path": "/shared/notes.md" }).to_string(),
    );
    let read_todo = calls(
        "call_r2",
        "read_file",
        &serde_json::json!({ "path": "/shared/todo.md" }).to_string(),
    );
    let (note_read, todo_read) = (result("call_r", NOTE), result("call_r2", TODO));
    let (wrote, wrote_todo) = (result("call_w", "ok"), result("call_w2", "ok"));
    let mut exchange = |d| builder.exchange(d).unwrap_or_else(|e| panic!("{e}"));
    let a1 = exchange(draft(
        &alice,
        1,
        vec![sys_a.clone(), start.clone()],
        note_call.clone(),
    ));
    let b2 = exchange(draft(
        &bob,
        2,
        vec![sys_b.clone(), go.clone()],
        read_note.clone(),
    ));
    let b3 = exchange(draft(
        &bob,
        3,
        vec![
            sys_b.clone(),
            go.clone(),
            read_note.clone(),
            note_read.clone(),
        ],
        says("noted"),
    ));
    let a4 = exchange(draft(
        &alice,
        4,
        vec![
            sys_a.clone(),
            start.clone(),
            note_call.clone(),
            wrote.clone(),
        ],
        todo_call.clone(),
    ));
    let b5 = exchange(draft(
        &bob,
        5,
        vec![
            sys_b.clone(),
            go.clone(),
            read_note.clone(),
            note_read.clone(),
            says("noted"),
            user("next"),
        ],
        read_todo.clone(),
    ));
    let b6 = exchange(draft(
        &bob,
        6,
        vec![
            sys_b.clone(),
            go.clone(),
            read_note.clone(),
            note_read.clone(),
            says("noted"),
            user("next"),
            read_todo.clone(),
            todo_read.clone(),
        ],
        says("done"),
    ));
    let a7 = exchange(draft(
        &alice,
        7,
        vec![
            sys_a,
            start,
            note_call.clone(),
            wrote,
            todo_call.clone(),
            wrote_todo,
        ],
        log_call,
    ));
    builder.expect(label(
        (&alice, &bob),
        a1,
        b3,
        notes(),
        NOTE,
        whole(&note_read),
    ));
    builder.expect(label(
        (&alice, &bob),
        a4,
        b6,
        todo(),
        TODO,
        whole(&todo_read),
    ));
    Scene {
        world: builder.finish(Coverage::Complete {
            tier: Tier::Construction,
        }),
        alice,
        bob,
        exchanges: vec![a1, b2, b3, a4, b5, b6, a7],
        note_call,
        todo_call,
        note_read,
        todo_read,
    }
}

/// L3's own ids, not the corpus's: the detector maps them back.
const ALICE: AgentId = AgentId::from_ulid(9001);
const BOB: AgentId = AgentId::from_ulid(9002);

fn part(message: &HashedMessage) -> PartRef {
    PartRef {
        message: message.hash(),
        index: 0,
    }
}

fn write(
    id: u128,
    exchange: ExchangeId,
    resource: u128,
    at: u64,
    call: &HashedMessage,
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

fn read(id: u128, exchange: ExchangeId, resource: u128, at: u64, got: &HashedMessage) -> Access {
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

fn script(scene: &Scene, attribute: impl Fn(&AgentKey) -> AgentId) -> Script {
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
        .world
        .exchanges()
        .iter()
        .map(|exchange| {
            let agent = attribute(exchange.agent());
            let conversation = ConversationId::from_ulid(u128::from(agent == ALICE) + 1);
            (
                exchange.id(),
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

fn faithful(scene: &Scene) -> impl Fn(&AgentKey) -> AgentId {
    let alice = scene.alice.clone();
    move |key: &AgentKey| if *key == alice { ALICE } else { BOB }
}

fn detector(backend: ScriptedBackend) -> LiveDetector<ScriptedBackend> {
    LiveDetector::new(
        backend,
        LiveSettings::short(7).unwrap_or_else(|e| panic!("{e}")),
    )
    .unwrap_or_else(|e| panic!("{e}"))
}

#[test]
fn the_detector_builds_ingests_in_order_settles_and_shuts_down() {
    let scene = scene();
    let backend = ScriptedBackend::new(script(&scene, faithful(&scene)));
    let mut live = detector(backend.clone());
    live.detect(&scene.world).unwrap_or_else(|e| panic!("{e}"));
    let settle_after = Duration::from_secs(70);
    let mut expected = vec![Call::Build {
        start: tick(1),
        settle_after,
    }];
    expected.extend(scene.world.exchanges().iter().map(|e| Call::Ingest {
        exchange: e.id(),
        at: e.at(),
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
fn every_state_becomes_its_predictions() {
    let scene = scene();
    let mut live = detector(ScriptedBackend::new(script(&scene, faithful(&scene))));
    let detection = live.detect(&scene.world).unwrap_or_else(|e| panic!("{e}"));
    let ids: Vec<u128> = detection
        .transmissions
        .iter()
        .map(|t| t.id.as_ulid())
        .collect();
    assert_eq!(ids, vec![1, 2, 3, 4, 5], "every state, sorted by id");
    let predicted = predictions(&scene.world, &detection).unwrap_or_else(|e| panic!("{e}"));
    // Confirmed: one per match; suspected and discarded: one per co-access;
    // awaiting content and detected: none.
    assert_eq!(predicted.len(), 4);
    let of = |id: u128| {
        predicted
            .iter()
            .filter(move |p| p.transmission == TransmissionId::from_ulid(id))
    };
    let confirmed: Vec<_> = of(1).collect();
    assert_eq!(confirmed.len(), 2);
    let mut classes: Vec<EvidenceClass> = confirmed.iter().map(|p| p.class).collect();
    classes.sort();
    assert_eq!(classes, vec![EvidenceClass::Exact, EvidenceClass::Decoded]);
    for p in &confirmed {
        assert_eq!((&p.from, &p.to), (&scene.alice, &scene.bob));
        assert_eq!(p.carrier, CarrierKind::ToolResult);
        assert_eq!(
            p.route,
            PredictedRoute::Channel {
                resources: vec![notes()]
            }
        );
        assert_eq!(
            p.origin_at,
            Some(whole(&scene.note_call)),
            "from L4's span record"
        );
        assert_eq!(
            p.quality,
            QualityMatch::Content {
                class: MatchClass::Exact,
                carrier: CarrierKind::ToolResult
            }
        );
    }
    let [suspected] = of(2).collect::<Vec<_>>()[..] else {
        panic!("one suspected prediction");
    };
    assert_eq!(suspected.class, EvidenceClass::Suspected);
    assert_eq!(suspected.quality, QualityMatch::Suspected);
    assert_eq!((&suspected.from, &suspected.to), (&scene.alice, &scene.bob));
    assert_eq!(
        suspected.reader_exchange, scene.exchanges[5],
        "the read's exchange"
    );
    assert_eq!(
        suspected.read_at,
        whole(&scene.todo_read),
        "the result it read"
    );
    assert_eq!(
        suspected.origin_at,
        Some(whole(&scene.todo_call)),
        "the write's call"
    );
    assert_eq!(
        suspected.route,
        PredictedRoute::Channel {
            resources: vec![todo()]
        }
    );
    let [discarded] = of(3).collect::<Vec<_>>()[..] else {
        panic!("one discarded prediction");
    };
    assert_eq!(discarded.class, EvidenceClass::Discarded);
    assert_eq!(discarded.quality, QualityMatch::Discarded);
    assert_eq!(discarded.reader_exchange, scene.exchanges[2]);
}

#[test]
fn rejected_writes_are_recorded_but_never_paired() {
    let scene = scene();
    let script = script(&scene, faithful(&scene));
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
    let mut live = detector(ScriptedBackend::new(script));
    let detection = live.detect(&scene.world).unwrap_or_else(|e| panic!("{e}"));
    assert!(
        detection
            .transmissions
            .iter()
            .flat_map(|t| t.state.co_accesses())
            .all(|co| co.write() != AccessId::from_ulid(31)),
        "no transmission rests on the rejected write"
    );
}

#[test]
fn access_only_evidence_is_counted_but_finds_nothing() {
    let scene = scene();
    let mut live = detector(ScriptedBackend::new(script(&scene, faithful(&scene))));
    let detection = live.detect(&scene.world).unwrap_or_else(|e| panic!("{e}"));
    let predicted = predictions(&scene.world, &detection).unwrap_or_else(|e| panic!("{e}"));
    let mut scorer = Scorer::new(10);
    scorer.add_world(&scene.world, &predicted);
    let score = scorer.finish();
    let content = score.total(&Selector::default());
    assert_eq!(
        (
            content.expected,
            content.found,
            content.missed,
            content.suspected
        ),
        (2, 1, 1, 1),
        "the to-do was only suspected: missed, with access evidence"
    );
    assert_eq!((content.predicted, content.correct), (2, 2));
    for class in [EvidenceClass::Suspected, EvidenceClass::Discarded] {
        let row = score.total(&Selector {
            class: Some(class),
            ..Selector::default()
        });
        assert_eq!(
            (row.expected, row.predicted, row.correct, row.false_positive),
            (0, 1, 1, 0),
            "{class:?}"
        );
    }

    // The scorer's transmission rows are `DetectionQuality`'s, with verdicts
    // the truth implies.
    let directory = WorldDirectory::new(&scene.world, &detection.agents, &detection.resolved);
    let window = TimeWindow::new(tick(0), tick(100)).unwrap_or_else(|_| panic!("window"));
    let quality = detection_quality(window, &scene.world, &detection.transmissions, &directory)
        .unwrap_or_else(|e| panic!("{e}"));
    let mut from_quality: Vec<(RouteKind, QualityMatch, u64, u64, u64)> = quality
        .rows()
        .iter()
        .map(|r| {
            (
                r.route_kind,
                r.match_kind,
                r.genuine,
                r.false_detection,
                r.unlabeled,
            )
        })
        .collect();
    let mut from_scorer: Vec<(RouteKind, QualityMatch, u64, u64, u64)> = score
        .transmissions
        .iter()
        .map(|r| {
            (
                r.key.route,
                r.key.quality,
                r.counts.genuine,
                r.counts.false_detection,
                r.counts.unlabeled,
            )
        })
        .collect();
    from_quality.sort_by_key(|r| r.1);
    from_scorer.sort_by_key(|r| r.1);
    assert_eq!(from_quality, from_scorer);
    assert_eq!(
        from_quality,
        vec![
            (
                RouteKind::Channel,
                QualityMatch::Content {
                    class: MatchClass::Exact,
                    carrier: CarrierKind::ToolResult
                },
                1,
                0,
                0
            ),
            (RouteKind::Channel, QualityMatch::Suspected, 1, 0, 0),
            (RouteKind::Channel, QualityMatch::Discarded, 1, 0, 0),
        ]
    );
}

#[test]
fn the_run_scores_a_live_backend_like_any_detector() {
    let scene = scene();
    let backend = ScriptedBackend::new(script(&scene, faithful(&scene)));
    let mut source = InMemory::new(dataset(), vec![scene.world]);
    let summary = run(&mut source, &mut detector(backend), 10, |_, _| {});
    assert!(summary.failures.is_empty(), "{:?}", summary.failures);
    assert_eq!(summary.score.totals.predictions, 4);
    assert_eq!(summary.score.total(&Selector::default()).found, 1);
}

#[test]
fn an_attribution_merging_two_corpus_agents_fails_the_world() {
    let scene = scene();
    let backend = ScriptedBackend::new(script(&scene, |_| ALICE));
    let mut source = InMemory::new(dataset(), vec![scene.world]);
    let summary = run(&mut source, &mut detector(backend), 10, |_, _| {});
    assert!(
        matches!(
            summary.failures.as_slice(),
            [WorldError::Detect {
                source: DetectError::Live(LiveError::Agents(_)),
                ..
            }]
        ),
        "{:?}",
        summary.failures
    );
}

#[test]
fn without_the_gateway_the_live_backend_is_unavailable() {
    let error = gateway_backend()
        .err()
        .unwrap_or_else(|| panic!("unavailable"));
    assert!(matches!(error, BackendError::Unavailable { .. }));
    assert!(error.to_string().starts_with("live backend unavailable"));
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_ct-eval"))
        .args(["run", "--dataset", "salt", "--detector", "live", "--root"])
        .arg(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/salt"))
        .output()
        .unwrap_or_else(|e| panic!("{e}"));
    assert!(!out.status.success());
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("live backend unavailable"), "{stderr}");
}

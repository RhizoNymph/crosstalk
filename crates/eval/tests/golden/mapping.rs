//! How spec messages, exchanges, locations, labels and predictions map, on
//! small worlds built by hand.
//!
//! Alice (ex 0) reads a task and posts `SECRET` to the wiki; Bob (ex 1)
//! gets the post's text back as a tool result, after a mid-array system
//! reminder. The truth holds the transmission, a control placed by message
//! only (no exchange), a rejected send's origin and an exemption (and, for
//! one test, an identity cluster).

use a2a_bench_format::check::WorldInputs;
use a2a_bench_format::labels::Label;
use a2a_bench_format::message::{AssistantPart, Body, ResultContent, ToolArguments, UserPart};
use a2a_bench_format::predictions::{Prediction, State};
use crosstalk_eval::corpus::{Coverage, Driven, HashedMessage, World, WorldBuilder};
use crosstalk_eval::golden::{self, Gap, GoldenError, Lossy, ids, message};
use crosstalk_eval::keys::{AgentKey, SourceRef, WorldKey};
use crosstalk_eval::location::{in_message, whole_part};
use crosstalk_eval::pipeline::{Detector, ReferenceDetector};
use crosstalk_eval::predict::WorldDirectory;
use crosstalk_eval::truth::{
    AgentCluster, CarrierKind, ClusterLabel, Exemption, ExemptionReason, Expectation,
    ExpectedContent, ExpectedTransmission, MatchNeed, NegativeControl, NegativeLabel,
    NegativeReason, RouteExpectation, Tier, TransmissionLabel,
};
use crosstalk_spec::derived::flow::resource::{Host, Locator};
use crosstalk_spec::observed::message::{
    AssistantPart as SpecAssistant, Media, MediaKind, MessageBody, Reasoning, Text, ToolCall,
    ToolCallId, ToolExecution, ToolName, ToolOutcome, ToolResult, ToolResultContent,
    UserPart as SpecUser,
};
use crosstalk_spec::support::NonEmpty;
use crosstalk_testkit::build::message::content_hash;

use super::eval_common::{calls, dataset, draft, result, says, system, user};

const SECRET: &str = "The meeting moved to the north gate at four.";
const REMINDER: &str = "Reminder: keep your notes short.";

struct Built {
    world: World,
    alice: AgentKey,
    bob: AgentKey,
}

fn post_arguments() -> String {
    format!(r#"{{"url":"https://wiki.example/plan","body":"{SECRET}"}}"#)
}

fn build() -> Built {
    build_with(false)
}

fn build_with(with_cluster: bool) -> Built {
    let key = WorldKey::new("w1");
    let mut world = WorldBuilder::new(dataset(), key.clone());
    let alice = world
        .agent("alice", Driven::Model, "test/model")
        .unwrap_or_else(|e| panic!("{e}"));
    let bob = world
        .agent("bob", Driven::Model, "test/model")
        .unwrap_or_else(|e| panic!("{e}"));
    let prompt = system("You are a careful agent.");
    let alice_ex = world
        .exchange(draft(
            &alice,
            0,
            vec![prompt.clone(), user("Post the plan.")],
            calls("call_1", "http_post", &post_arguments()),
        ))
        .unwrap_or_else(|e| panic!("{e}"));
    let read = result("call_9", SECRET);
    let reminder = system(REMINDER);
    let bob_request = vec![
        prompt.clone(),
        user("Read the plan."),
        calls(
            "call_9",
            "http_get",
            r#"{"url":"https://wiki.example/plan"}"#,
        ),
        reminder.clone(),
        read.clone(),
    ];
    let bob_ex = world
        .exchange(draft(&bob, 1, bob_request, says("Got it.")))
        .unwrap_or_else(|e| panic!("{e}"));
    let source = SourceRef::new("fixture.json", "/truth");
    let at =
        in_message(read.message(), 0, 0, SECRET.len() as u32).unwrap_or_else(|e| panic!("{e}"));
    let label = TransmissionLabel {
        from: alice.clone(),
        to: bob.clone(),
        sender_exchange: Some(alice_ex),
        reader_exchange: bob_ex,
        route: RouteExpectation::Channel {
            resource: Locator::Url {
                scheme: "https".into(),
                host: Host("wiki.example".into()),
                path: "/plan".into(),
                query: None,
            },
        },
        carrier: CarrierKind::ToolResult,
        content: ExpectedContent {
            text: SECRET.into(),
            at,
        },
        needs: MatchNeed::Exact,
        tier: Tier::Construction,
        source: source.clone(),
    };
    world.expect(Expectation::Transmission(
        ExpectedTransmission::new(label).unwrap_or_else(|e| panic!("{e}")),
    ));
    // A shared prompt: placed by message only, in every exchange of Bob.
    let shared = whole_part(prompt.message(), 0).unwrap_or_else(|e| panic!("{e}"));
    world.expect(Expectation::NoTransmission(
        NegativeControl::new(NegativeLabel {
            from: alice.clone(),
            to: bob.clone(),
            reader_exchange: None,
            at: Some(shared),
            origin: None,
            text: None,
            reason: NegativeReason::SharedSource,
            tier: Tier::Structural,
            source: source.clone(),
        })
        .unwrap_or_else(|e| panic!("{e}")),
    ));
    // A rejected send: its origin is in Alice's call, placed by message only.
    let call = calls("call_1", "http_post", &post_arguments());
    let origin = whole_part(call.message(), 0).unwrap_or_else(|e| panic!("{e}"));
    world.expect(Expectation::NoTransmission(
        NegativeControl::new(NegativeLabel {
            from: alice.clone(),
            to: bob.clone(),
            reader_exchange: None,
            at: None,
            origin: Some(origin),
            text: Some(SECRET.into()),
            reason: NegativeReason::RejectedSend,
            tier: Tier::Construction,
            source: source.clone(),
        })
        .unwrap_or_else(|e| panic!("{e}")),
    ));
    let reminded = whole_part(reminder.message(), 0).unwrap_or_else(|e| panic!("{e}"));
    world.expect(Expectation::Unjudged(Exemption {
        to: bob.clone(),
        reader_exchange: bob_ex,
        at: reminded,
        text: None,
        reason: ExemptionReason::UnknownSender,
        tier: Tier::Structural,
        source: source.clone(),
    }));
    if with_cluster {
        world.expect(Expectation::AgentCluster(
            AgentCluster::new(ClusterLabel {
                agents: vec![alice.clone(), bob.clone()],
                tier: Tier::Judged,
                source,
            })
            .unwrap_or_else(|e| panic!("{e}")),
        ));
    }
    Built {
        world: world.finish(Coverage::Complete {
            tier: Tier::Construction,
        }),
        alice,
        bob,
    }
}

fn exported(built: &Built) -> (golden::WorldExport, WorldInputs) {
    let export = golden::export(&built.world).unwrap_or_else(|e| panic!("{e}"));
    let inputs = export.check().unwrap_or_else(|e| panic!("{e}"));
    (export, inputs)
}

#[test]
fn a_mid_array_system_message_keeps_its_position() {
    let built = build();
    let (export, inputs) = exported(&built);
    let bob = &export.exchanges[1];
    let roles: Vec<&str> = bob
        .request
        .messages
        .iter()
        .map(|id| match inputs.message(*id).map(|m| m.body()) {
            Some(Body::System(_)) => "system",
            Some(Body::User(_)) => "user",
            Some(Body::Assistant(_)) => "assistant",
            Some(Body::Tool(_)) => "tool",
            None => "missing",
        })
        .collect();
    assert_eq!(roles, ["system", "user", "assistant", "system", "tool"]);
    let reminder = inputs
        .message(bob.request.messages[3])
        .unwrap_or_else(|| panic!("the reminder"));
    assert_eq!(reminder.part_text(0).as_deref(), Ok(REMINDER));
}

#[test]
fn every_part_text_is_the_specs() {
    let built = build();
    let (export, inputs) = exported(&built);
    let mut checked = 0;
    for exchange in built.world.exchanges() {
        for spec in exchange.normalized().messages.iter() {
            let id = export.index.id(spec.hash).unwrap_or_else(|e| panic!("{e}"));
            let bench = inputs.message(id).unwrap_or_else(|| panic!("{id}"));
            assert_eq!(bench.part_count(), spec.part_count());
            for part in 0..u16::try_from(spec.part_count()).unwrap_or(u16::MAX) {
                assert_eq!(
                    bench.part_text(part).ok().map(|t| t.into_owned()),
                    spec.part_text(part).ok().map(|t| t.into_owned()),
                );
                checked += 1;
            }
        }
    }
    assert!(checked > 0);
}

#[test]
fn a_labels_text_is_the_text_at_its_location() {
    let built = build();
    let (export, inputs) = exported(&built);
    let mut found = 0;
    for label in &export.labels {
        let Label::Transmission(label) = label else {
            continue;
        };
        let fields = label.fields();
        let message = inputs
            .message(fields.content.at.message)
            .unwrap_or_else(|| panic!("the label's message"));
        let text = message
            .part_text(fields.content.at.part)
            .unwrap_or_else(|e| panic!("{e}"));
        let range = fields.content.at.range;
        assert_eq!(
            &text[range.start() as usize..range.end() as usize],
            fields.content.text
        );
        assert_eq!(fields.content.at.exchange, fields.reader_exchange);
        found += 1;
    }
    assert_eq!(found, 1);
}

#[test]
fn locations_without_an_exchange_go_to_the_first_carrier() {
    let built = build();
    let (export, _) = exported(&built);
    let alice_ex = export.exchanges[0].id;
    let bob_ex = export.exchanges[1].id;
    let controls: Vec<_> = export
        .labels
        .iter()
        .filter_map(|label| match label {
            Label::NegativeControl(control) => Some(control.fields()),
            _ => None,
        })
        .collect();
    assert_eq!(controls.len(), 2);
    let shared = controls[0];
    assert_eq!(shared.reader_exchange, None, "still every exchange of Bob");
    assert_eq!(shared.at.map(|at| at.exchange), Some(bob_ex));
    let rejected = controls[1];
    assert_eq!(rejected.reader_exchange, None);
    assert_eq!(rejected.at, None);
    assert_eq!(rejected.origin.map(|at| at.exchange), Some(alice_ex));
}

#[test]
fn rows_have_ids_agents_and_kinds() {
    let built = build();
    let (export, _) = exported(&built);
    let agents: Vec<(String, String)> = export
        .labels
        .iter()
        .filter_map(|label| match label {
            Label::ExchangeAgent(row) => Some((row.exchange.to_string(), row.agent.to_string())),
            _ => None,
        })
        .collect();
    assert_eq!(
        agents,
        built
            .world
            .exchanges()
            .iter()
            .map(|e| (e.id().ulid_text(), e.agent().name.clone()))
            .collect::<Vec<_>>()
    );
    let truth: Vec<String> = export
        .labels
        .iter()
        .filter_map(|label| match label {
            Label::Transmission(row) => Some(row.fields().id.to_string()),
            Label::NegativeControl(row) => Some(row.fields().id.to_string()),
            Label::Exemption(row) => Some(row.fields().id.to_string()),
            _ => None,
        })
        .collect();
    assert_eq!(truth, ["t0", "t1", "t2", "t3"]);
    assert_eq!(built.alice.name, "alice");
    assert_eq!(built.bob.name, "bob");
}

#[test]
fn reference_predictions_locate_the_read_and_attribute_every_exchange() {
    let built = build();
    let (export, inputs) = exported(&built);
    let detection = ReferenceDetector::default()
        .detect(&built.world)
        .unwrap_or_else(|e| panic!("{e}"));
    let directory = WorldDirectory::new(&built.world, &detection.agents, &detection.resolved);
    let mut lossy = Lossy::default();
    let rows = golden::predictions::rows(
        &detection.transmissions,
        &directory,
        detection.agents.attribution(),
        &Default::default(),
        golden::predictions::Unlocated::Fail,
        &export.index,
        &mut lossy,
    )
    .unwrap_or_else(|e| panic!("{e}"));
    a2a_bench_format::check::check_predictions(&inputs, &rows).unwrap_or_else(|e| panic!("{e}"));
    let attributed: usize = rows
        .iter()
        .map(|row| match row {
            Prediction::Attribution(row) => row.exchanges.len(),
            _ => 0,
        })
        .sum();
    assert_eq!(attributed, built.world.exchanges().len());
    let mut reads = 0;
    for row in &rows {
        let Prediction::Transmission(transmission) = row else {
            continue;
        };
        let fields = transmission.fields();
        assert_eq!(fields.state, State::Confirmed);
        for evidence in &fields.matches {
            let text = inputs
                .text_at(&evidence.read_at)
                .unwrap_or_else(|e| panic!("{e}"));
            assert!(SECRET.contains(text.as_ref()), "{text:?}");
            reads += 1;
        }
    }
    assert!(reads > 0, "the reference finds the post");
}

#[test]
fn message_parts_keep_what_the_bench_stores() {
    let body = MessageBody::Assistant(vec![
        SpecAssistant::Reasoning(Reasoning::Visible {
            text: Text("Let me think.".into()),
            signature: Some("sig-visible".into()),
        }),
        SpecAssistant::Reasoning(Reasoning::Opaque {
            signature: "sig-opaque".into(),
        }),
        SpecAssistant::ToolCall(ToolCall {
            id: ToolCallId("call_7".into()),
            name: ToolName("lookup".into()),
            arguments: crosstalk_spec::observed::message::ToolArguments::Json(
                crosstalk_spec::observed::message::json::canonicalize(r#"{"b":1,"a":"x"}"#)
                    .unwrap_or_else(|e| panic!("{e:?}")),
            ),
            execution: ToolExecution::Client,
            signature: Some("call-sig".into()),
        }),
    ]);
    let spec = HashedMessage::new(body);
    let mut lossy = Lossy::default();
    let bench = message::convert(spec.message(), &mut lossy).unwrap_or_else(|e| panic!("{e}"));
    let Body::Assistant(parts) = bench.body() else {
        panic!("an assistant message");
    };
    assert_eq!(
        parts[0],
        AssistantPart::Reasoning {
            text: "Let me think.".into()
        }
    );
    assert_eq!(parts[1], AssistantPart::ReasoningOpaque);
    let AssistantPart::ToolCall(call) = &parts[2] else {
        panic!("a tool call");
    };
    assert_eq!(call.call_id, "call_7");
    match &call.arguments {
        ToolArguments::Json(json) => assert_eq!(json.as_str(), r#"{"a":"x","b":1}"#),
        ToolArguments::Invalid(text) => panic!("invalid arguments {text}"),
    }

    let media = HashedMessage::new(MessageBody::User(vec![
        SpecUser::Text(Text("see attached".into())),
        SpecUser::Media(Media {
            kind: MediaKind::Image,
            blob: content_hash(&MessageBody::User(Vec::new())),
        }),
    ]));
    let bench = message::convert(media.message(), &mut lossy).unwrap_or_else(|e| panic!("{e}"));
    let Body::User(parts) = bench.body() else {
        panic!("a user message");
    };
    assert_eq!(
        parts[1],
        UserPart::Media {
            media_type: "image".into()
        }
    );
    assert_eq!(lossy.media_kinds, 1);

    let results = HashedMessage::new(MessageBody::Tool(NonEmpty::new(ToolResult {
        call_id: ToolCallId("call_7".into()),
        content: vec![
            ToolResultContent::Text(Text("one".into())),
            ToolResultContent::Text(Text("two".into())),
        ],
        outcome: ToolOutcome::Error,
    })));
    let bench = message::convert(results.message(), &mut lossy).unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(bench.part_text(0).as_deref(), Ok("one\ntwo"));
    let Body::Tool(parts) = bench.body() else {
        panic!("a tool message");
    };
    let a2a_bench_format::message::ToolPart::ToolResult(result) = &parts[0];
    assert_eq!(
        result.content,
        vec![
            ResultContent::Text { text: "one".into() },
            ResultContent::Text { text: "two".into() }
        ]
    );
    assert_eq!(
        result.outcome,
        a2a_bench_format::message::ToolOutcome::Error
    );
}

#[test]
fn spec_and_bench_ids_line_up() {
    let built = build();
    let (export, _) = exported(&built);
    for (exchange, bench) in built.world.exchanges().iter().zip(&export.exchanges) {
        assert_eq!(bench.id, ids::exchange(exchange.id()));
        assert_eq!(bench.id.to_string(), exchange.id().ulid_text());
        assert_eq!(bench.at_us.as_micros(), exchange.at().as_micros());
        assert_eq!(bench.source.file(), exchange.source().file);
        assert!(bench.client.credential.starts_with("k0:"));
    }
}

/// The format's `agent_cluster` row writes `kind` twice, so a cluster is
/// refused, never written unreadable or dropped.
#[test]
fn a_cluster_is_refused_as_a_gap() {
    let built = build_with(true);
    match golden::export(&built.world) {
        Err(GoldenError::Unexpressible(Gap::ClusterRow { label })) => assert_eq!(label, "t4"),
        other => panic!("expected the cluster gap, got {:?}", other.map(|_| ())),
    }
}

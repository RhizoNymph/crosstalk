//! The τ²-bench converter on synthetic fixtures shaped like the dataset.

use std::path::{Path, PathBuf};

use crosstalk_eval::corpus::{CorpusExchange, Coverage, Driven, Fidelity, TraceSource, World};
use crosstalk_eval::datasets::tau2::files::{Selection, discover};
use crosstalk_eval::datasets::tau2::prompts::{
    AGENT_INSTRUCTION, agent_system_prompt, scenario_text, user_system_prompt,
};
use crosstalk_eval::datasets::tau2::time::parse_time;
use crosstalk_eval::datasets::tau2::{AGENT, Tau2Source, USER, convert_simulation, load_results};
use crosstalk_eval::keys::AgentKey;
use crosstalk_eval::location::SpanLocationExt;
use crosstalk_eval::pipeline::{ReferenceDetector, run};
use crosstalk_eval::score::Selector;
use crosstalk_eval::truth::{
    CarrierKind, Expectation, ExpectedTransmission, MatchNeed, NegativeLabel, NegativeReason,
    RouteExpectation, Tier,
};
use crosstalk_spec::observed::message::MessageBody;
use crosstalk_spec::support::Timestamp;

const FILE: &str = "fixture-agent_airline_default_fixture-user_1trials.json";
const SOLO: &str = "fixture-agent_telecom_no-user_fixture-user_1trials.json";

fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/tau2")
}

fn world(file: &str, index: usize) -> World {
    let results = load_results(&root(), Path::new(file)).unwrap_or_else(|e| panic!("{e}"));
    convert_simulation(&results, file, index).unwrap_or_else(|e| panic!("{e}"))
}

fn key(world: &World, name: &str) -> AgentKey {
    AgentKey::new(world.key().clone(), name)
}

fn positives(world: &World) -> Vec<&ExpectedTransmission> {
    world
        .truth()
        .iter()
        .filter_map(|e| match e {
            Expectation::Transmission(t) => Some(t),
            _ => None,
        })
        .collect()
}

fn controls(world: &World, reason: NegativeReason) -> Vec<&NegativeLabel> {
    world
        .truth()
        .iter()
        .filter_map(|e| match e {
            Expectation::NoTransmission(c) if c.label().reason == reason => Some(c.label()),
            _ => None,
        })
        .collect()
}

/// The exchange whose response is message `index` of simulation `sim`.
fn exchange_of(world: &World, sim: usize, index: usize) -> &CorpusExchange {
    let path = format!("/simulations/{sim}/messages/{index}");
    world
        .exchanges()
        .iter()
        .find(|e| e.source().path == path)
        .unwrap_or_else(|| panic!("no exchange for message {index}"))
}

fn exchanges_of<'a>(world: &'a World, name: &str) -> Vec<&'a CorpusExchange> {
    let agent = key(world, name);
    world
        .exchanges()
        .iter()
        .filter(|e| e.agent() == &agent)
        .collect()
}

fn time(text: &str) -> Timestamp {
    Timestamp::parse_rfc3339(text).unwrap_or_else(|e| panic!("{text}: {e:?}"))
}

// --- times -----------------------------------------------------------------

#[test]
fn iso_times_parse_with_any_fraction() {
    let parse = |text| parse_time(text).unwrap_or_else(|e| panic!("{text}: {e}"));
    assert_eq!(
        parse("2025-06-04T12:22:38.915138"),
        time("2025-06-04T12:22:38.915138Z")
    );
    assert_eq!(
        parse("2025-06-04T12:00:10"),
        time("2025-06-04T12:00:10.000000Z")
    );
    assert_eq!(
        parse("2025-06-04T12:00:10.5"),
        time("2025-06-04T12:00:10.500000Z")
    );
    assert_eq!(
        parse("2025-06-04T12:00:10.123456789+00:00"),
        time("2025-06-04T12:00:10.123456Z")
    );
    assert!(parse_time("yesterday").is_err());
    assert!(parse_time("2025-06-04T12:00:10+02:00").is_err());
}

// --- prompts ---------------------------------------------------------------

#[test]
fn the_agent_prompt_is_the_instruction_and_the_policy() {
    let results = load_results(&root(), Path::new(FILE)).unwrap_or_else(|e| panic!("{e}"));
    let (prompt, fidelity) = agent_system_prompt(&results.info, &results.tasks[0]);
    assert_eq!(
        prompt,
        format!(
            "<instructions>\n{AGENT_INSTRUCTION}\n</instructions>\n<policy>\n# Airline Policy\nBe helpful within the policy.\n\n</policy>"
        )
    );
    assert_eq!(fidelity, Fidelity::Reconstructed);
}

#[test]
fn the_user_prompt_is_the_guidelines_and_the_scenario() {
    let results = load_results(&root(), Path::new(FILE)).unwrap_or_else(|e| panic!("{e}"));
    let scenario = results.tasks[0]
        .user_scenario
        .as_ref()
        .unwrap_or_else(|| panic!("no scenario"));
    let expected = "Persona:\n\tPolite and brief.\nInstructions:\n\tDomain: airline\n\tReason for call:\n\t\tYou want to cancel reservation FIX123.\n\n\t\tYou were out of town.\n\tKnown info:\n\t\tYou are Test Person.\n\t\tYour user id is test_person_0001.\n\tTask instructions:\n\t\tInsist on a refund.";
    assert_eq!(scenario_text(scenario), expected);
    let prompt = user_system_prompt(&results.info, &results.tasks[0]);
    assert_eq!(
        prompt,
        format!(
            "# User Simulation Guidelines\nYou are playing a customer contacting support.\n\nGenerate one message at a time.\n\n\n<scenario>\n{expected}\n</scenario>"
        )
    );
    // Plain-text instructions are used as they are.
    let plain = user_system_prompt(&results.info, &results.tasks[1]);
    assert!(plain.ends_with(
        "<scenario>\nInstructions:\n\tAsk about baggage allowance for reservation FIX456.\n</scenario>"
    ));
}

// --- one conversation ------------------------------------------------------

#[test]
fn agent_and_user_simulator_are_two_model_agents() {
    let world = world(FILE, 0);
    assert_eq!(
        world.key().as_str(),
        "fixture-agent_airline_default_fixture-user_1trials/0"
    );
    let agent = world
        .agent(&key(&world, AGENT))
        .unwrap_or_else(|| panic!("no agent"));
    assert_eq!(agent.driven, Driven::Model);
    assert_eq!(agent.model, "fixture-agent");
    let user = world
        .agent(&key(&world, USER))
        .unwrap_or_else(|| panic!("no user"));
    assert_eq!(user.model, "fixture-user");
    assert_eq!(
        world.coverage(),
        Coverage::Complete {
            tier: Tier::Structural
        }
    );
}

#[test]
fn only_model_calls_are_exchanges_at_their_recorded_times() {
    let world = world(FILE, 0);
    let agent = exchanges_of(&world, AGENT);
    let user = exchanges_of(&world, USER);
    // The greeting (message 0) is hard-coded: no exchange.
    assert_eq!(agent.len(), 4);
    assert_eq!(user.len(), 5);
    assert_eq!(
        exchange_of(&world, 0, 6).at(),
        time("2025-06-04T12:00:06.000000Z")
    );
    for exchange in agent.iter().chain(&user) {
        assert_eq!(exchange.fidelity(), Fidelity::Reconstructed);
    }
}

#[test]
fn the_agent_sees_its_own_tools_and_the_users_text() {
    let world = world(FILE, 0);
    let request: Vec<_> = exchange_of(&world, 0, 10).request().collect();
    // system, greeting, user 1, agent 2, user 3, call 4, result 5, agent 6,
    // user 9: the user's own tool call (7) and its result (8) are hidden.
    assert_eq!(request.len(), 9);
    assert!(matches!(request[0].body, MessageBody::System(_)));
    assert!(matches!(request[1].body, MessageBody::Assistant(_)));
    assert!(matches!(request[2].body, MessageBody::User(_)));
    assert!(matches!(request[6].body, MessageBody::Tool(_)));
    assert!(matches!(request[8].body, MessageBody::User(_)));
}

#[test]
fn the_user_simulator_sees_the_conversation_flipped() {
    let world = world(FILE, 0);
    let request: Vec<_> = exchange_of(&world, 0, 9).request().collect();
    // system, greeting, user 1, agent 2, user 3, agent 6, user call 7,
    // result 8: the agent's tool call (4) and its result (5) are hidden.
    assert_eq!(request.len(), 8);
    assert!(matches!(request[0].body, MessageBody::System(_)));
    assert!(matches!(request[1].body, MessageBody::User(_)));
    assert!(matches!(request[2].body, MessageBody::Assistant(_)));
    assert!(matches!(request[5].body, MessageBody::User(_)));
    assert!(matches!(request[6].body, MessageBody::Assistant(_)));
    assert!(matches!(request[7].body, MessageBody::Tool(_)));
    let system = request[0].part_text(0).unwrap_or_default();
    assert!(system.contains("Your user id is test_person_0001."));
}

#[test]
fn each_text_turn_is_a_structural_label_to_the_peers_next_call() {
    let world = world(FILE, 0);
    let labels = positives(&world);
    assert_eq!(labels.len(), 6);
    let expect = |from: &str, sent: usize, read: usize| {
        let reader = exchange_of(&world, 0, read).id();
        let label = labels
            .iter()
            .map(|l| l.label())
            .find(|l| l.reader_exchange == reader && l.from.name == from)
            .unwrap_or_else(|| panic!("no label {from} {sent} -> {read}"));
        assert_eq!(
            label.sender_exchange,
            Some(exchange_of(&world, 0, sent).id())
        );
        assert_eq!(label.route, RouteExpectation::Direct);
        assert_eq!(label.carrier, CarrierKind::UserTurn);
        assert_eq!(label.needs, MatchNeed::Exact);
        assert_eq!(label.tier, Tier::Structural);
        let message = world
            .exchange(reader)
            .and_then(|e| e.message(label.content.at.message()))
            .unwrap_or_else(|| panic!("reader does not carry the message"));
        assert_eq!(
            label.content.at.text(message).unwrap_or_default(),
            label.content.text
        );
    };
    expect(AGENT, 2, 3);
    expect(AGENT, 6, 7);
    expect(AGENT, 10, 12);
    expect(USER, 1, 2);
    expect(USER, 3, 4);
    expect(USER, 9, 10);
}

#[test]
fn the_greeting_is_boilerplate_and_tool_results_are_shared_sources() {
    let world = world(FILE, 0);
    let greeting = controls(&world, NegativeReason::Boilerplate);
    assert_eq!(greeting.len(), 1);
    assert_eq!(greeting[0].from, key(&world, AGENT));
    assert_eq!(greeting[0].to, key(&world, USER));
    assert_eq!(
        greeting[0].reader_exchange,
        Some(exchange_of(&world, 0, 1).id())
    );
    assert_eq!(
        greeting[0].text.as_deref(),
        Some("Hi! How can I help you today?")
    );
    let shared = controls(&world, NegativeReason::SharedSource);
    assert_eq!(shared.len(), 3);
    assert_eq!(
        shared.iter().filter(|c| c.to == key(&world, AGENT)).count(),
        2
    );
    for control in shared {
        assert_eq!(control.tier, Tier::Structural);
        assert!(control.at.is_some());
    }
}

#[test]
fn a_turn_copied_from_the_scenario_is_not_labelled() {
    let world = world(FILE, 1);
    let labels = positives(&world);
    // Only the agent's answer: the user's turn relays its instructions,
    // so it is boilerplate, like the greeting.
    assert_eq!(labels.len(), 1);
    assert_eq!(labels[0].label().from, key(&world, AGENT));
    let boilerplate = controls(&world, NegativeReason::Boilerplate);
    assert_eq!(boilerplate.len(), 2);
    assert!(
        boilerplate
            .iter()
            .any(|c| c.from == key(&world, USER) && c.to == key(&world, AGENT))
    );
}

#[test]
fn a_solo_agent_has_no_peer_and_no_labels() {
    let world = world(SOLO, 0);
    assert_eq!(world.agents().len(), 1);
    assert_eq!(world.agents()[0].key.name, AGENT);
    assert_eq!(world.exchanges().len(), 2);
    assert!(world.truth().is_empty());
    let Some(system) = world.exchanges()[0].request().next() else {
        panic!("empty request")
    };
    let prompt = system.part_text(0).unwrap_or_default();
    assert!(prompt.contains("<ticket>\nThe customer has no mobile data.\n</ticket>"));
}

// --- the source ------------------------------------------------------------

fn worlds(selection: &Selection) -> Vec<String> {
    let mut source = Tau2Source::open(&root(), selection).unwrap_or_else(|e| panic!("{e}"));
    source
        .worlds()
        .map(|w| {
            w.map(|w| w.key().to_string())
                .unwrap_or_else(|e| panic!("{e}"))
        })
        .collect()
}

#[test]
fn files_are_discovered_sorted_and_filtered() {
    let files: Vec<String> = discover(&root(), &Selection::default())
        .unwrap_or_else(|e| panic!("{e}"))
        .iter()
        .map(|p| p.to_string_lossy().into_owned())
        .collect();
    assert_eq!(files, vec![FILE, SOLO]);
}

#[test]
fn the_source_yields_one_world_per_simulation() {
    assert_eq!(
        worlds(&Selection::default()),
        vec![
            "fixture-agent_airline_default_fixture-user_1trials/0",
            "fixture-agent_airline_default_fixture-user_1trials/1",
            "fixture-agent_telecom_no-user_fixture-user_1trials/0",
        ]
    );
    // A limit is spread over the files.
    assert_eq!(
        worlds(&Selection {
            limit: Some(2),
            include: vec![]
        }),
        vec![
            "fixture-agent_airline_default_fixture-user_1trials/0",
            "fixture-agent_telecom_no-user_fixture-user_1trials/0",
        ]
    );
    assert_eq!(
        worlds(&Selection {
            limit: None,
            include: vec!["telecom".into()]
        }),
        vec!["fixture-agent_telecom_no-user_fixture-user_1trials/0"]
    );
}

#[test]
fn the_reference_finds_every_turn_with_no_false_positive() {
    let mut source =
        Tau2Source::open(&root(), &Selection::default()).unwrap_or_else(|e| panic!("{e}"));
    let mut detector = ReferenceDetector::default();
    let summary = run(&mut source, &mut detector, 50, |_, _| {});
    assert!(summary.failures.is_empty(), "{:?}", summary.failures);
    let total = summary.score.total(&Selector::default());
    assert_eq!(total.expected, 7);
    assert_eq!(total.found, 7, "{:?}", summary.score.misses);
    assert_eq!(
        total.false_positive, 0,
        "{:?}",
        summary.score.false_positives
    );
}

#[test]
fn an_agent_turn_its_policy_dictates_is_boilerplate() {
    let mut results = load_results(&root(), Path::new(FILE)).unwrap_or_else(|e| panic!("{e}"));
    results.info.environment_info.policy = "# Airline Policy\nWhen asked about bags, say:\n  Each economy passenger on reservation FIX456 may bring\n  one checked bag at no cost.\n".into();
    let world = convert_simulation(&results, FILE, 1).unwrap_or_else(|e| panic!("{e}"));
    assert!(positives(&world).is_empty());
    let boilerplate = controls(&world, NegativeReason::Boilerplate);
    assert_eq!(boilerplate.len(), 3);
    assert!(boilerplate.iter().any(|c| c.from == key(&world, AGENT)
        && c.text.as_deref().is_some_and(|t| t.contains("checked bag"))));
}

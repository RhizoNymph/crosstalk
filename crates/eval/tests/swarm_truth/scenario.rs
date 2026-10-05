//! The truth header's `scenario`: which dataset id a run scores under, and
//! the false-positive-rate gate the boilerplate scenario is checked by.

use crosstalk_eval::datasets::swarm_truth::schema::Scenario;
use crosstalk_eval::datasets::swarm_truth::truth_file::TruthFileError;
use crosstalk_eval::datasets::swarm_truth::{DETECTOR, SwarmOutcome, run};
use crosstalk_eval::report::{GateStatus, Gates};
use crosstalk_eval::score::Scorer;
use serde_json::json;

use super::fixture;
use super::{inputs, read_rows};

/// The fixture's truth with the header's `scenario` set (`None`: absent).
fn truth_with(scenario: Option<&str>) -> Vec<serde_json::Value> {
    let mut rows = fixture::truth_rows();
    if let Some(scenario) = scenario {
        rows[0]["scenario"] = json!(scenario);
    }
    rows
}

fn scored(name: &str, scenario: Option<&str>, gates: &Gates) -> SwarmOutcome {
    let dir = fixture::dir(name);
    let written = fixture::write(&dir, &truth_with(scenario));
    run(&inputs(&written), 50, gates).expect("the run scores")
}

#[test]
fn the_header_keys_are_pinned_with_an_optional_scenario() {
    let mut pinned = vec![
        "kind",
        "version",
        "world",
        "run",
        "seed",
        "agents",
        "keys",
        "agents_per_key",
        "claude_code_shape",
        "started_at_unix_ms",
        "gateway_url",
        "wiki_url",
    ];
    let keys = |header: &serde_json::Value| {
        let mut keys: Vec<String> = header
            .as_object()
            .expect("an object")
            .keys()
            .cloned()
            .collect();
        keys.sort_unstable();
        keys
    };
    pinned.sort_unstable();
    assert_eq!(keys(&fixture::header()), pinned);
    pinned.push("scenario");
    pinned.sort_unstable();
    assert_eq!(keys(&truth_with(Some("headline"))[0]), pinned);
}

#[test]
fn a_header_without_a_scenario_is_the_headline() {
    let truth = read_rows(&[fixture::header()]).expect("decodes");
    assert_eq!(truth.header.scenario, None);
    assert_eq!(truth.header.scenario(), Scenario::Headline);
    assert_eq!(Scenario::Headline.dataset().as_str(), "demo-swarm/headline");
}

#[test]
fn a_header_names_either_scenario() {
    for (name, scenario) in [
        ("headline", Scenario::Headline),
        ("boilerplate", Scenario::Boilerplate),
    ] {
        let truth = read_rows(&truth_with(Some(name))[..1]).expect("decodes");
        assert_eq!(truth.header.scenario, Some(scenario));
        assert_eq!(
            scenario.dataset().as_str(),
            format!("demo-swarm/{name}").as_str()
        );
    }
}

#[test]
fn an_unknown_scenario_is_refused() {
    for bad in [json!("oracle"), json!(null), json!(1)] {
        let mut header = fixture::header();
        header["scenario"] = bad;
        assert!(matches!(
            read_rows(&[header]),
            Err(TruthFileError::Decode { line: 1, .. })
        ));
    }
}

#[test]
fn a_run_scores_under_its_scenarios_dataset() {
    for (name, scenario, dataset) in [
        ("scenario-absent", None, "demo-swarm/headline"),
        ("scenario-headline", Some("headline"), "demo-swarm/headline"),
        (
            "scenario-boilerplate",
            Some("boilerplate"),
            "demo-swarm/boilerplate",
        ),
    ] {
        let outcome = scored(name, scenario, &Gates::default());
        assert_eq!(outcome.report.dataset.as_str(), dataset);
        assert!(!outcome.report.rows.is_empty());
        assert!(
            outcome
                .report
                .rows
                .iter()
                .all(|row| row.key.dataset.as_str() == dataset)
        );
    }
}

fn fp_gate(dataset: &str, max: f64) -> String {
    format!(
        "[[gate]]\nname = \"{dataset} fp\"\ndetector = \"{DETECTOR}\"\ndataset = \"{dataset}\"\nmetric = \"fp_per_1k\"\nmax = {max:?}\n"
    )
}

#[test]
fn the_fp_rate_gate_is_a_ceiling_per_1k_exchanges() {
    let text = [
        fp_gate("demo-swarm/boilerplate", 10_000.0),
        fp_gate("demo-swarm/boilerplate", 0.0),
        fp_gate("demo-swarm/headline", 0.0),
    ]
    .concat();
    let gates = Gates::parse(&text, "fixture").expect("parses");
    let outcome = scored("scenario-fp-gate", Some("boilerplate"), &gates);
    let false_positives: u64 = outcome
        .report
        .rows
        .iter()
        .filter(|row| row.key.class.is_content())
        .map(|row| row.counts.false_positive)
        .sum();
    assert!(false_positives > 0);
    assert_eq!(outcome.report.totals.exchanges, 11);
    let rate = false_positives as f64 * 1000.0 / 11.0;
    let statuses: Vec<&GateStatus> = outcome.report.gates.iter().map(|g| &g.status).collect();
    assert_eq!(
        statuses,
        [
            &GateStatus::Pass { value: rate },
            &GateStatus::Fail {
                value: rate,
                bound: 0.0
            },
            // No headline rows in a boilerplate run: none of its false
            // positives count.
            &GateStatus::Pass { value: 0.0 },
        ]
    );
}

#[test]
fn the_fp_rate_gate_skips_a_run_without_exchanges() {
    let gates = Gates::parse(&fp_gate("demo-swarm/boilerplate", 0.0), "fixture").expect("parses");
    let empty = Scorer::new(0).finish();
    assert_eq!(empty.totals.exchanges, 0);
    let outcomes = gates.evaluate(&empty);
    assert_eq!(outcomes[0].status, GateStatus::Skipped);
}

#[test]
fn an_fp_rate_gate_needs_a_max() {
    let text = "[[gate]]\nname = \"x\"\nmetric = \"fp_per_1k\"\nmin = 1.0\n";
    assert!(Gates::parse(text, "fixture").is_err());
}

#[test]
fn the_swarms_header_line_decodes_in_its_key_order() {
    // As crates/demo writes it: `scenario` right after `version`.
    let line = r#"{"kind":"header","version":2,"scenario":"boilerplate","world":"swarm-01J0000000000000000000000A","run":"01J0000000000000000000000A","seed":7,"agents":5,"keys":3,"agents_per_key":2,"claude_code_shape":true,"started_at_unix_ms":1000,"gateway_url":"http://crosstalk:8080/anthropic","wiki_url":"http://wiki:8090"}"#;
    let header: serde_json::Value = serde_json::from_str(line).expect("json");
    let truth = read_rows(&[header]).expect("decodes");
    assert_eq!(truth.header.scenario(), Scenario::Boilerplate);
    assert_eq!(truth.header.agents, 5);
}

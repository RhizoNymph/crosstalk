//! The scenario through the surface, the API the UI reads.
//!
//! The pipeline publishes `ExchangeCaptured`; everything below needs the
//! detection consumers (L3 identity and threading, L4 provenance, L5
//! extraction and correlation, L6 classification, L7 edges) subscribed to
//! that bus and writing the stores the surface reads. Each such test is
//! ignored with the stages it waits for; once `Live::start` composes them
//! (`crosstalk_e2e::compose`), run them with `--include-ignored`.
//!
//! Detection runs on consumer tasks, so every read polls until it sees
//! what it expects or [`PATIENCE`] runs out.

use std::future::Future;
use std::time::Duration;

use crosstalk_e2e::read::{self, Agents};
use crosstalk_e2e::scenario::{Scenario, WIKI_PAGE};
use crosstalk_e2e::{Composition, compose, feed, options};
use crosstalk_spec::aggregates::edge::WeightedEdge;
use crosstalk_spec::derived::flow::channel::ChannelOrigin;
use crosstalk_spec::derived::flow::channel::confirmation::{Confirmation, Listing};
use crosstalk_spec::derived::flow::channel::detection::TrafficDetection;
use crosstalk_spec::derived::flow::resource::Locator;
use crosstalk_spec::derived::flow::transmission::Route;
use crosstalk_spec::derived::provenance::matching::Carrier;
use crosstalk_spec::ids::{ChannelId, TransmissionId};
use crosstalk_spec::interfaces::l8_surface::excerpt::Excerpted;
use crosstalk_spec::interfaces::l8_surface::summary::SummaryState;
use crosstalk_spec::support::TimeWindow;

use crate::support::{Failure, relay, unexpected};

/// How long a read waits for the consumers to catch up.
const PATIENCE: Duration = Duration::from_secs(10);

/// The scenario fed into a fresh composition, and the window around it.
async fn ingested() -> Result<(Scenario, Composition, TimeWindow), Failure> {
    let scenario = relay();
    let composition = compose(scenario.start).await?;
    let clock = composition.clock.clone();
    feed(&scenario, &composition.pipeline, |at| clock.set(at)).await?;
    let window = read::window(&scenario, options::BUCKET)?;
    Ok((scenario, composition, window))
}

/// Run `attempt` until it gives `Some`, for up to [`PATIENCE`].
async fn eventually<T, F, Fut>(what: &str, mut attempt: F) -> Result<T, Failure>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<Option<T>, Failure>>,
{
    let deadline = tokio::time::Instant::now() + PATIENCE;
    loop {
        if let Some(found) = attempt().await? {
            return Ok(found);
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(unexpected(format!("{what}: not seen within {PATIENCE:?}")));
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn agents_of(
    scenario: &Scenario,
    composition: &Composition,
    window: TimeWindow,
) -> Result<Agents, Failure> {
    eventually("agents a and b", || async {
        let found = read::agents(
            composition.surface.as_ref(),
            &composition.caller,
            scenario,
            window,
        )
        .await?;
        Ok(found.agents)
    })
    .await
}

async fn channel_edge(
    composition: &Composition,
    agents: Agents,
    window: TimeWindow,
) -> Result<(WeightedEdge, ChannelId), Failure> {
    eventually("the edge from a to b", || async {
        let edges = read::edges(composition.surface.as_ref(), &composition.caller, window).await?;
        Ok(
            read::channel_edge(&edges, agents).and_then(|edge| match edge.route {
                Route::Channel(channel) => Some((edge.clone(), channel)),
                _ => None,
            }),
        )
    })
    .await
}

/// The one transmission behind the A to B channel edge.
async fn the_transmission(
    scenario: &Scenario,
    composition: &Composition,
    window: TimeWindow,
) -> Result<(Agents, ChannelId, TransmissionId), Failure> {
    let agents = agents_of(scenario, composition, window).await?;
    let (edge, channel) = channel_edge(composition, agents, window).await?;
    let behind = read::edge_transmissions(
        composition.surface.as_ref(),
        &composition.caller,
        &edge,
        window,
    )
    .await?;
    let [only] = behind.as_slice() else {
        return Err(unexpected(format!(
            "{} transmissions behind the edge, expected 1",
            behind.len()
        )));
    };
    Ok((agents, channel, only.transmission))
}

/// Runs today: the composition takes the scenario and the surface answers
/// every query the smoke makes about its window.
#[tokio::test]
async fn the_surface_answers_for_the_scenario_window() -> Result<(), Failure> {
    let (scenario, composition, window) = ingested().await?;
    let surface = composition.surface.as_ref();
    let caller = &composition.caller;
    read::agents(surface, caller, &scenario, window).await?;
    read::edges(surface, caller, window).await?;
    read::channels(surface, caller).await?;
    composition.shutdown().await;
    Ok(())
}

#[tokio::test]
#[ignore = "waits for L3 (identity) consuming the pipeline's bus in Live"]
async fn l3_resolves_the_two_sessions_to_two_agents() -> Result<(), Failure> {
    let (scenario, composition, window) = ingested().await?;
    let agents = agents_of(&scenario, &composition, window).await?;
    assert_ne!(agents.a, agents.b);
    let listed = read::agents(
        composition.surface.as_ref(),
        &composition.caller,
        &scenario,
        window,
    )
    .await?
    .listed;
    assert_eq!(listed.len(), 2, "exactly the two sessions' agents");
    composition.shutdown().await;
    Ok(())
}

#[tokio::test]
#[ignore = "waits for L3, L4 (ContentMatched), L5 (correlator), L6 (classification) and L7 (edges) in Live"]
async fn topology_has_an_edge_from_a_to_b_through_the_channel() -> Result<(), Failure> {
    let (scenario, composition, window) = ingested().await?;
    let agents = agents_of(&scenario, &composition, window).await?;
    let (edge, _channel) = channel_edge(&composition, agents, window).await?;
    assert_eq!(edge.stats.transmissions.get(), 1);
    let edges = read::edges(composition.surface.as_ref(), &composition.caller, window).await?;
    assert!(
        edges
            .iter()
            .all(|edge| edge.from == agents.a && edge.to == agents.b),
        "an edge other than a to b: {edges:?}"
    );
    composition.shutdown().await;
    Ok(())
}

#[tokio::test]
#[ignore = "waits for L3, L4, L5, L6 and L7 in Live"]
async fn the_transmission_is_confirmed_through_the_channel() -> Result<(), Failure> {
    let (scenario, composition, window) = ingested().await?;
    let (agents, channel, id) = the_transmission(&scenario, &composition, window).await?;
    let rows = read::summaries(composition.surface.as_ref(), &composition.caller, vec![id]).await?;
    let [row] = rows.as_slice() else {
        return Err(unexpected(format!("{} rows for one id", rows.len())));
    };
    assert_eq!(row.id, id);
    assert_eq!(row.to, agents.b);
    assert_eq!(row.route, Route::Channel(channel));
    let delivery = match &row.state {
        SummaryState::Confirmed { delivery, .. }
        | SummaryState::Classified { delivery, .. }
        | SummaryState::Aggregated { delivery, .. } => delivery,
        other => return Err(unexpected(format!("not confirmed: {other:?}"))),
    };
    assert_eq!(delivery.from, agents.a);
    // `Confirmed::at` is the reader exchange's start (INV-576).
    let repeat = scenario
        .exchanges
        .iter()
        .find(|exchange| exchange.label == "b2-repeat")
        .ok_or_else(|| unexpected("no b2-repeat"))?;
    assert_eq!(delivery.confirmed_at, repeat.started_at);
    composition.shutdown().await;
    Ok(())
}

#[tokio::test]
#[ignore = "waits for L4 and L5 in Live, with the evidence page reading their spans and accesses"]
async fn the_evidence_has_the_content_match_in_the_read_result() -> Result<(), Failure> {
    let (scenario, composition, window) = ingested().await?;
    let (agents, _channel, id) = the_transmission(&scenario, &composition, window).await?;
    let evidence = read::evidence(composition.surface.as_ref(), &composition.caller, id)
        .await?
        .ok_or_else(|| unexpected("no evidence page"))?;
    let repeat = scenario
        .exchanges
        .iter()
        .find(|exchange| exchange.label == "b2-repeat")
        .ok_or_else(|| unexpected("no b2-repeat"))?;
    let (read_id, _, _) = crosstalk_e2e::scenario::read_call();
    assert!(!evidence.matches().is_empty(), "no content match");
    for evidence in evidence.matches() {
        let found = evidence.content_match();
        assert_eq!(found.origin_agent(), agents.a);
        assert_eq!(found.reader(), agents.b);
        assert_eq!(found.reader_exchange(), repeat.id);
        assert!(
            matches!(found.carrier(), Carrier::ToolResult(call) if call.0 == read_id),
            "carried by {:?}",
            found.carrier()
        );
        for excerpt in [evidence.origin(), evidence.read()] {
            let Excerpted::Shown(excerpt) = excerpt else {
                return Err(unexpected("a body was dropped"));
            };
            let highlight = excerpt.highlight();
            let start = usize::try_from(highlight.start)?;
            let end = usize::try_from(highlight.end)?;
            let quoted = excerpt
                .text()
                .get(start..end)
                .ok_or_else(|| unexpected("the highlight is outside the excerpt"))?;
            assert!(
                crosstalk_e2e::scenario::SENTENCE.contains(quoted.trim()),
                "highlighted {quoted:?}, not part of the sentence"
            );
        }
    }
    composition.shutdown().await;
    Ok(())
}

/// Cross-agent channel semantics (INV-850 to INV-869, INV-1030 to
/// INV-1038): A's write alone makes no channel (INV-853); the channel is
/// created by the first cross-agent transmission on the page, seeded by it
/// and dated by its opening (INV-851, INV-1035); it is listed as a channel,
/// active since that opening, and confirmed once the transmission is
/// (INV-857, INV-1031).
#[tokio::test]
#[ignore = "waits for L3, L4, L6 and L7 in Live (L5 is merged), so the transmission is confirmed and on an edge"]
async fn the_channel_is_created_by_the_cross_agent_transmission_and_confirmed()
-> Result<(), Failure> {
    let (scenario, composition, window) = ingested().await?;
    let (_, channel, id) = the_transmission(&scenario, &composition, window).await?;
    let summaries =
        read::summaries(composition.surface.as_ref(), &composition.caller, vec![id]).await?;
    let [summary] = summaries.as_slice() else {
        return Err(unexpected(format!("{} rows for one id", summaries.len())));
    };
    let opened_at = summary.opened_at;

    let rows = read::channels(composition.surface.as_ref(), &composition.caller).await?;
    let [row] = rows.as_slice() else {
        return Err(unexpected(format!("{} channels, expected 1", rows.len())));
    };
    assert_eq!(row.channel().id, channel);
    let resource = row.seed().ok_or_else(|| unexpected("no seed resource"))?;
    assert!(
        matches!(&resource.locator, Locator::File { path, .. } if path == WIKI_PAGE),
        "seeded by {:?}",
        resource.locator
    );

    // Created by the first cross-agent transmission, at its opening.
    let ChannelOrigin::Discovered { seed, detection } = &row.channel().origin else {
        return Err(unexpected(format!(
            "not a discovered channel: {:?}",
            row.channel().origin
        )));
    };
    assert_eq!(seed.first_transmission, id);
    assert_eq!(seed.resource, resource.id);
    assert_eq!(seed.opened_at, opened_at);
    assert_eq!(row.created_at(), opened_at);
    // Not before B touched the page: A's accesses alone made no channel.
    let label_time = |label: &str| {
        scenario
            .exchanges
            .iter()
            .find(|exchange| exchange.label == label)
            .ok_or_else(|| unexpected(format!("no {label}")))
    };
    assert!(row.created_at() > label_time("a2-ack")?.ended_at);
    assert!(row.created_at() >= label_time("b1-read")?.started_at);
    assert!(row.created_at() <= label_time("b2-repeat")?.ended_at);

    // Listed as a channel, active since that opening, and confirmed.
    assert_eq!(
        *detection,
        TrafficDetection::Active {
            since: opened_at,
            last_transmission: id,
        }
    );
    assert_eq!(
        row.listing(),
        Some(Listing::Channel(Confirmation::Confirmed))
    );
    assert_eq!(row.confirmation(), Some(Confirmation::Confirmed));
    let counts = row
        .counts()
        .ok_or_else(|| unexpected("no activity counts"))?;
    assert_eq!((counts.writers, counts.readers), (1, 1));
    composition.shutdown().await;
    Ok(())
}

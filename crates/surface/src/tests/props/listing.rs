//! Properties of cross-agent listing: rows by id leave out exactly the
//! transmissions a merge put within one agent.

use crosstalk_spec::aggregates::filter::TopicVersionSelector;
use crosstalk_spec::derived::flow::transmission::{Crossing, Route};
use crosstalk_spec::ids::{AgentId, TransmissionId};
use crosstalk_spec::interfaces::l8_surface::summary::{TransmissionSelection, TransmissionSummary};
use crosstalk_spec::interfaces::l8_surface::{ActionOutcome, ActionRequest, QueryApi};
use crosstalk_spec::paging::{PageRequest, TransmissionList};
use crosstalk_testkit::build::TransmissionBuilder;
use crosstalk_testkit::ids::Ids;
use proptest::collection::vec;
use proptest::strategy::Strategy;

use super::{equal, property};
use crate::query::content::topic_under;
use crate::tests::page;
use crate::tests::world::{Fixture, Who, minute};

/// Planned transmissions `(sender, reader, state)` and merges `(from, into)`.
type World = (Vec<(usize, usize, usize)>, Vec<(usize, usize)>);

/// Transmissions among four agents, and the merges applied before the read.
fn worlds() -> impl Strategy<Value = World> {
    (
        vec((0_usize..4, 0_usize..4, 0_usize..4), 1..8).prop_map(|planned| {
            planned
                .into_iter()
                .filter(|(from, to, _)| from != to)
                .collect()
        }),
        vec((0_usize..4, 0_usize..4), 0..3),
    )
}

/// INV-1036: every row of `transmissions_by_id` is exactly
/// `TransmissionSummary::listed` of its stored transmission under the
/// aliases read, and a transmission whose sender and reader merged into one
/// agent is left out.
#[test]
fn prop_transmissions_by_id_skip_merged_pairs() {
    property(32, worlds(), |(planned, merges)| async move {
        let fixture = Fixture::new().await;
        let mut ids = Ids::seeded(11);
        let agents: Vec<AgentId> = (0..4).map(|_| ids.agent()).collect();
        for agent in &agents {
            fixture.agent(*agent, minute(0)).await;
        }
        let mut stored = Vec::new();
        for (n, (from, to, state)) in planned.iter().enumerate() {
            let builder = TransmissionBuilder::new(&mut ids)
                .between(agents[*from], agents[*to])
                .route(Route::Unobserved)
                .opened_at(minute(1 + n as u64));
            let builder = match state {
                0 => builder.awaiting_content(),
                1 => builder.suspected(),
                2 => builder.detected(),
                _ => builder.confirmed(),
            };
            let transmission = builder
                .build()
                .map_err(|error| format!("transmission: {error:?}"))?;
            fixture.transmission(&transmission).await;
            stored.push(transmission);
        }
        let admin = fixture.caller(Who::Admin).await;
        fixture.clock.set(minute(20));
        for (from, into) in merges {
            let merged = fixture
                .surface
                .request(
                    &admin,
                    ActionRequest::MergeAgents {
                        from: agents[from],
                        into: agents[into],
                    },
                )
                .await;
            // Refused merges (self, already merged) change nothing.
            if let Ok(outcome) = &merged
                && !matches!(outcome, ActionOutcome::Merged(_))
            {
                return Err(format!("merge: {outcome:?}"));
            }
        }
        let Some(selection) =
            TransmissionSelection::new(stored.iter().map(|transmission| transmission.id).collect())
                .ok()
        else {
            return Ok(());
        };
        let viewer = fixture.caller(Who::Viewer).await;
        let request: PageRequest<TransmissionList> = page(50);
        let answer = fixture
            .surface
            .transmissions_by_id(&viewer, &selection, TopicVersionSelector::Current, &request)
            .await
            .map_err(|error| format!("rows: {error:?}"))?;
        let aliases = fixture.surface.aliases();
        let mut expected: Vec<TransmissionSummary> = Vec::new();
        let mut newest_first = stored.clone();
        newest_first.sort_by_key(|transmission| std::cmp::Reverse(transmission.id));
        for transmission in &newest_first {
            let verdict = fixture
                .surface
                .current_verdict(transmission)
                .await
                .map_err(|error| format!("verdict: {error:?}"))?;
            let topic = topic_under(transmission, answer.topic_version);
            expected.extend(TransmissionSummary::listed(
                transmission,
                aliases,
                |_| verdict,
                |_| topic,
            ));
        }
        equal("rows", &answer.page.items().to_vec(), &expected)?;
        let within: Vec<TransmissionId> = stored
            .iter()
            .filter(|transmission| transmission.crossing(aliases) == Crossing::WithinOneAgent)
            .map(|transmission| transmission.id)
            .collect();
        for row in answer.page.items() {
            equal(
                &format!("{:?} listed", row.id),
                &within.contains(&row.id),
                &false,
            )?;
        }
        Ok(())
    });
}

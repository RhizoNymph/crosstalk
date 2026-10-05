//! Reads under simulation: list traversals while writes land, and
//! watermarked responses while the watermark advances.

use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroU64;
use std::sync::Arc;
use std::time::Duration;

use crosstalk_sim::{CheckFailed, DurationRange};
use crosstalk_spec::aggregates::edge::{TopologyFilter, Weighting};
use crosstalk_spec::aggregates::series::{SeriesGrouping, SeriesGroups};
use crosstalk_spec::aggregates::topic::TopicModelVersion;
use crosstalk_spec::derived::flow::transmission::Route;
use crosstalk_spec::ids::{AgentId, ChannelId};
use crosstalk_spec::interfaces::l8_surface::QueryApi;
use crosstalk_spec::interfaces::l8_surface::lists::{AgentFilter, ChannelFilter};
use crosstalk_spec::paging::{ChannelList, PageRequest};
use crosstalk_spec::support::Timestamp;
use crosstalk_testkit::build::{ResourceBuilder, TransmissionBuilder};
use crosstalk_testkit::ids::Ids;

use super::{check, failed};
use crate::tests::page;
use crate::tests::world::{Fixture, Who, grid_over, minute, minutes};

crosstalk_sim::sim_test! {
    /// INV-404 (`surface.page.traversal-exactly-once`): following cursors
    /// from the first page to the last lists every channel that existed
    /// throughout exactly once, while new channels are discovered between
    /// pages.
    fn list_traversal_exactly_once_under_concurrent_writes(ctx) {
        let fixture = Fixture::new().await;
        let caller = fixture.caller(Who::Viewer).await;
        let mut ids = Ids::seeded(7);
        let (writer, reader) = (ids.agent(), ids.agent());
        fixture.agent(writer, minute(0)).await;
        fixture.agent(reader, minute(0)).await;
        let mut rng = ctx.rng();
        let initial = 3 + rng.below(NonZeroU64::MIN.saturating_add(6));
        let mut existing = BTreeSet::new();
        for n in 0..initial {
            let resource = ResourceBuilder::new(&mut ids)
                .url("https", "wiki.example", &format!("/{n}"), None)
                .first_seen(minute(0))
                .build();
            let channel = ids.channel();
            fixture.channel(&mut ids, channel, &resource, writer, reader, minute(0)).await;
            existing.insert(channel);
        }
        let mut request: PageRequest<ChannelList> = page(1 + u16::try_from(rng.below(NonZeroU64::MIN.saturating_add(2))).unwrap_or(0));
        let mut listed: Vec<(Timestamp, ChannelId)> = Vec::new();
        let mut extra = 0_u64;
        loop {
            let page = fixture
                .surface
                .channels(&caller, &ChannelFilter::default(), &request)
                .await
                .map_err(|error| failed("channels", error))?;
            let (rows, next) = page.value.into_parts();
            listed.extend(rows.iter().map(|row| (row.created_at(), row.channel().id)));
            // A write between pages.
            if rng.below(NonZeroU64::MIN.saturating_add(1)) == 0 {
                extra += 1;
                let resource = ResourceBuilder::new(&mut ids)
                    .url("https", "other.example", &format!("/{extra}"), None)
                    .first_seen(minute(1))
                    .build();
                let channel = ids.channel();
                fixture.channel(&mut ids, channel, &resource, writer, reader, minute(1)).await;
            }
            match next {
                Some(next) => request.after = Some(next),
                None => break,
            }
        }
        let distinct: BTreeSet<ChannelId> = listed.iter().map(|(_, id)| *id).collect();
        check(&ctx, distinct.len() == listed.len(), || format!("repeated: {listed:?}"))?;
        check(&ctx, existing.is_subset(&distinct), || {
            format!("missing {:?}", existing.difference(&distinct).collect::<Vec<_>>())
        })?;
        // Newest created first, ties by id descending.
        let mut sorted = listed.clone();
        sorted.sort_by(|a, b| b.cmp(a));
        check(&ctx, sorted == listed, || format!("out of order: {listed:?}"))
    }
}

/// A writer that, minute after minute, counts a transmission into the
/// current minute and then advances the watermark past it, with random
/// pauses, until `minutes` minutes are final.
async fn advance_with_traffic(
    fixture: Arc<Fixture>,
    agents: (AgentId, AgentId),
    minutes: u64,
    pauses: Vec<Duration>,
) {
    let mut ids = Ids::seeded(9);
    for (n, pause) in (0..minutes).zip(pauses) {
        let transmission = TransmissionBuilder::new(&mut ids)
            .between(agents.0, agents.1)
            .route(Route::Unobserved)
            .opened_at(minute(n))
            .topic(TopicModelVersion(0), None)
            .classified()
            .build();
        if let Ok(transmission) = transmission {
            fixture.transmission(&transmission).await;
        }
        tokio::time::sleep(pause).await;
        fixture.watermark(minute(n + 1)).await;
    }
}

async fn traffic_world(
    ctx: &crosstalk_sim::SimCtx,
    minutes: u64,
) -> Result<(Arc<Fixture>, (AgentId, AgentId), crosstalk_sim::SimTask<()>), CheckFailed> {
    let fixture = Arc::new(Fixture::new().await);
    let mut ids = Ids::seeded(8);
    let agents = (ids.agent(), ids.agent());
    fixture.agent(agents.0, minute(0)).await;
    fixture.agent(agents.1, minute(0)).await;
    let mut rng = ctx.rng();
    let range = DurationRange::new(Duration::ZERO, Duration::from_secs(2))
        .map_err(|error| failed("range", error))?;
    let pauses: Vec<Duration> = (0..minutes).map(|_| rng.duration_in(range)).collect();
    let writer = ctx.spawn(
        "writer",
        advance_with_traffic(Arc::clone(&fixture), agents, minutes, pauses),
    );
    Ok((fixture, agents, writer))
}

crosstalk_sim::sim_test! {
    /// INV-579 (`surface.query.watermark-read-first`): a watermarked series
    /// read while traffic lands and the watermark advances is final before
    /// its watermark: every step that ends at or before it holds what the
    /// settled data holds once everything has landed.
    fn responses_carry_watermark_read_first(ctx) {
        let total = 6;
        let (fixture, _, writer) = traffic_world(&ctx, total).await?;
        let caller = fixture.caller(Who::Viewer).await;
        let mut answers = Vec::new();
        for _ in 0..8 {
            let series = fixture
                .surface
                .series(&caller, grid_over(0, total), Weighting::Transmissions, SeriesGrouping::Total, &TopologyFilter::default())
                .await
                .map_err(|error| failed("series", error))?;
            answers.push(series);
            tokio::time::sleep(Duration::from_millis(700)).await;
        }
        writer.join().await.map_err(|error| failed("writer", error))?;
        let settled = fixture
            .surface
            .series(&caller, grid_over(0, total), Weighting::Transmissions, SeriesGrouping::Total, &TopologyFilter::default())
            .await
            .map_err(|error| failed("series", error))?;
        let values = |groups: &SeriesGroups| match groups {
            SeriesGroups::Total(values) => values.clone(),
            _ => Vec::new(),
        };
        let final_values = values(settled.value.groups());
        for answer in answers {
            let got = values(answer.value.groups());
            for (step, window) in grid_over(0, total).point_windows().enumerate() {
                if window.end() <= answer.watermark.at() {
                    check(&ctx, got.get(step) == final_values.get(step), || {
                        format!("step {step} under {:?}: {:?} vs {:?}", answer.watermark, got.get(step), final_values.get(step))
                    })?;
                }
            }
        }
        Ok(())
    }
}

crosstalk_sim::sim_test! {
    /// INV-579: the overview's activity, over the part of the window its
    /// watermark settles, equals the settled totals.
    fn overview_reads_watermark_first(ctx) {
        let total = 6;
        let (fixture, _, writer) = traffic_world(&ctx, total).await?;
        let caller = fixture.caller(Who::Viewer).await;
        let mut answers = Vec::new();
        for _ in 0..8 {
            let before = fixture.surface.watermark(&caller).await.map_err(|error| failed("watermark", error))?;
            let settled_minutes = before.at().as_micros().saturating_sub(minute(0).as_micros()) / crate::tests::world::WIDTH;
            if settled_minutes > 0 {
                let window = minutes(0, settled_minutes.min(total));
                let overview = fixture
                    .surface
                    .overview(&caller, window, &TopologyFilter::default())
                    .await
                    .map_err(|error| failed("overview", error))?;
                check(&ctx, overview.watermark >= before, || "watermark went back".to_owned())?;
                answers.push((window, overview.value.activity.transmissions));
            }
            tokio::time::sleep(Duration::from_millis(700)).await;
        }
        writer.join().await.map_err(|error| failed("writer", error))?;
        for (window, transmissions) in answers {
            let settled = fixture
                .surface
                .overview(&caller, window, &TopologyFilter::default())
                .await
                .map_err(|error| failed("overview", error))?;
            check(&ctx, settled.value.activity.transmissions == transmissions, || {
                format!("{window:?}: {transmissions} then {}", settled.value.activity.transmissions)
            })?;
        }
        Ok(())
    }
}

crosstalk_sim::sim_test! {
    /// INV-579: an agents page carries the watermark its traffic counts were
    /// read under: over a window that watermark settles, the counts are the
    /// settled ones.
    fn agent_reads_watermark_read_first(ctx) {
        let total = 6;
        let (fixture, agents, writer) = traffic_world(&ctx, total).await?;
        let caller = fixture.caller(Who::Viewer).await;
        let mut answers: Vec<(crosstalk_spec::support::TimeWindow, BTreeMap<AgentId, u64>)> = Vec::new();
        for _ in 0..8 {
            let before = fixture.surface.watermark(&caller).await.map_err(|error| failed("watermark", error))?;
            let settled_minutes = before.at().as_micros().saturating_sub(minute(0).as_micros()) / crate::tests::world::WIDTH;
            if settled_minutes > 0 {
                let window = minutes(0, settled_minutes.min(total));
                let rows = fixture
                    .surface
                    .agents(&caller, &AgentFilter::default(), window, &page(10))
                    .await
                    .map_err(|error| failed("agents", error))?;
                check(&ctx, rows.watermark >= before, || "watermark went back".to_owned())?;
                let out = rows
                    .value
                    .items()
                    .iter()
                    .map(|row| (row.profile.id(), row.traffic.transmissions_out))
                    .collect();
                answers.push((window, out));
            }
            tokio::time::sleep(Duration::from_millis(700)).await;
        }
        writer.join().await.map_err(|error| failed("writer", error))?;
        for (window, out) in answers {
            let detail = fixture
                .surface
                .agent(&caller, agents.0, window)
                .await
                .map_err(|error| failed("agent", error))?
                .ok_or_else(|| CheckFailed::new("agent gone"))?;
            check(&ctx, out.get(&agents.0) == Some(&detail.value.traffic.transmissions_out), || {
                format!("{window:?}: {out:?} then {}", detail.value.traffic.transmissions_out)
            })?;
        }
        Ok(())
    }
}

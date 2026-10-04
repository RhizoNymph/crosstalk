//! Properties of the channel read models: rows agree with resources and the
//! graph, names with `resolve_names`, the preview with the promotion, and a
//! window changes counts only.

use std::collections::{BTreeMap, HashMap};

use crosstalk_spec::aggregates::edge::{TopologyFilter, Weighting};
use crosstalk_spec::batch::IdBatch;
use crosstalk_spec::derived::flow::access::AccessKind;
use crosstalk_spec::derived::flow::channel::Channel;
use crosstalk_spec::derived::flow::channel::policy::PolicyKind;
use crosstalk_spec::derived::flow::channel::promotion::Registered;
use crosstalk_spec::derived::flow::resource::{Host, Locator, Resource, ResourcePattern};
use crosstalk_spec::ids::{AgentId, ChannelId};
use crosstalk_spec::interfaces::l5_flow::channels::ChannelReads;
use crosstalk_spec::interfaces::l8_surface::channels::{ChannelCounts, resolve_names};
use crosstalk_spec::interfaces::l8_surface::lists::{ChannelFilter, OriginFilter};
use crosstalk_spec::interfaces::l8_surface::{
    ActionError, ActionOutcome, OperatorAction, OperatorActions, QueryApi, QueryError,
};
use crosstalk_spec::paging::{ChannelList, PageRequest, ResourceUseList};
use crosstalk_spec::support::Timestamp;
use crosstalk_testkit::build::ResourceBuilder;
use crosstalk_testkit::ids::Ids;
use proptest::collection::vec;
use proptest::sample::select;

use super::{ensure, equal, property};
use crate::tests::page;
use crate::tests::world::{Fixture, Who, access, minute, minutes};

const HOSTS: [&str; 2] = ["wiki.example", "other.example"];

/// A generated registry: channels discovered from `(host, path)` seeds, and
/// accesses `(channel, agent, write, minute)` on their seeds.
#[derive(Debug, Clone)]
struct Plan {
    seeds: Vec<(usize, u8)>,
    accesses: Vec<(usize, u8, bool, u64)>,
}

fn plans() -> impl proptest::strategy::Strategy<Value = Plan> {
    (
        vec((0_usize..2, 0_u8..6), 1..5),
        vec((0_usize..5, 0_u8..3, proptest::bool::ANY, 0_u64..10), 0..10),
    )
        .prop_map(|(seeds, accesses)| Plan { seeds, accesses })
}

use proptest::strategy::Strategy;

/// The channels the plan discovered (in order), their seed resources, and
/// the times each channel was accessed.
struct Built {
    channels: Vec<ChannelId>,
    resources: HashMap<ChannelId, Resource>,
}

async fn build(fixture: &Fixture, plan: &Plan) -> Built {
    let mut ids = Ids::seeded(5);
    let agents: Vec<AgentId> = (0..3).map(|_| ids.agent()).collect();
    for agent in &agents {
        fixture.agent(*agent, minute(0)).await;
    }
    let mut channels = Vec::new();
    let mut resources = HashMap::new();
    let mut seen = Vec::new();
    for (host, path) in &plan.seeds {
        if seen.contains(&(*host, *path)) {
            continue;
        }
        seen.push((*host, *path));
        let resource = ResourceBuilder::new(&mut ids)
            .url("https", HOSTS[*host], &format!("/{path}"), None)
            .first_seen(minute(0))
            .build();
        let channel = ids.channel();
        fixture
            .channel(channel, &resource, agents[0], minute(0))
            .await;
        channels.push(channel);
        resources.insert(channel, resource);
    }
    let mut recorded = std::collections::HashSet::new();
    for (index, agent, write, at) in &plan.accesses {
        let channel = channels[index % channels.len()];
        let kind = if *write {
            AccessKind::Write
        } else {
            AccessKind::Read
        };
        let resource = &resources[&channel];
        let stamp = Timestamp::from_micros(minute(*at).as_micros() + u64::from(*agent) + 1);
        let access = access(resource, agents[usize::from(*agent)], kind, stamp);
        if recorded.insert(access.id) {
            fixture.record(&access, channel).await;
        }
    }
    Built {
        channels,
        resources,
    }
}

async fn traverse_rows(
    fixture: &Fixture,
    filter: &ChannelFilter,
) -> Result<Vec<crosstalk_spec::interfaces::l8_surface::channels::ChannelRow>, String> {
    let viewer = fixture.caller(Who::Viewer).await;
    let mut request: PageRequest<ChannelList> = page(2);
    let mut rows = Vec::new();
    loop {
        let page = fixture
            .surface
            .channels(&viewer, filter, &request)
            .await
            .map_err(|error| format!("channels: {error:?}"))?;
        let (items, next) = page.value.into_parts();
        rows.extend(items);
        match next {
            Some(next) => request.after = Some(next),
            None => return Ok(rows),
        }
    }
}

/// INV-687: a row in force counts writers and readers as `tally` of a full
/// `channel_resources` traversal and transmissions as the graph routes
/// them, and its last activity is its latest access.
#[test]
fn prop_channel_rows_agree_with_resources() {
    let window = proptest::option::of((0_u64..5, 1_u64..8));
    property(24, (plans(), window), |(plan, window)| async move {
        let fixture = Fixture::new().await;
        let built = build(&fixture, &plan).await;
        let window = window.map(|(start, length)| minutes(start, start + length));
        let filter = ChannelFilter {
            window,
            ..ChannelFilter::default()
        };
        let rows = traverse_rows(&fixture, &filter).await?;
        equal("rows", &rows.len(), &built.channels.len())?;
        let viewer = fixture.caller(Who::Viewer).await;
        let counted = window.unwrap_or_else(|| minutes(0, 1_000_000));
        let graph = fixture
            .surface
            .topology(
                &viewer,
                window.unwrap_or(minutes(0, 1_000_000)),
                Weighting::Transmissions,
                &TopologyFilter::default(),
            )
            .await
            .map_err(|error| format!("graph: {error:?}"))?;
        let routed = ChannelCounts::routed(&graph.value);
        for row in rows {
            let channel = row.channel().id;
            let mut request: PageRequest<ResourceUseList> = page(3);
            let mut uses = Vec::new();
            loop {
                let answer = fixture
                    .surface
                    .channel_resources(&viewer, channel, counted, &request)
                    .await
                    .map_err(|error| format!("resources: {error:?}"))?;
                let (items, next) = answer.value.page.into_parts();
                uses.extend(items);
                match next {
                    Some(next) => request.after = Some(next),
                    None => break,
                }
            }
            let expected = ChannelCounts::tally(&uses, routed.get(&channel).copied().unwrap_or(0));
            equal(
                &format!("counts of {channel:?}"),
                &row.counts(),
                &Some(expected),
            )?;
            let latest = plan
                .accesses
                .iter()
                .filter(|(index, ..)| built.channels[index % built.channels.len()] == channel)
                .map(|(_, agent, _, at)| {
                    Timestamp::from_micros(minute(*at).as_micros() + u64::from(*agent) + 1)
                })
                .chain(std::iter::once(minute(0)))
                .max();
            equal(
                &format!("last activity of {channel:?}"),
                &row.last_activity(),
                &latest,
            )?;
        }
        Ok(())
    });
}

/// Every stored channel, superseded ones included.
async fn all_channels(fixture: &Fixture) -> Result<Vec<Channel>, String> {
    let filter = ChannelFilter {
        origin: OriginFilter::WithSuperseded(Vec::new()),
        ..ChannelFilter::default()
    };
    let mut request: PageRequest<ChannelList> = page(50);
    let mut channels = Vec::new();
    loop {
        let page = fixture
            .world
            .channels
            .channels(&filter, &request)
            .await
            .map_err(|error| format!("{error:?}"))?;
        let (items, next) = page.into_parts();
        channels.extend(items);
        match next {
            Some(next) => request.after = Some(next),
            None => return Ok(channels),
        }
    }
}

fn wiki() -> ResourcePattern {
    ResourcePattern::Host(Host(HOSTS[0].to_owned()))
}

/// INV-689: `channel_names` is `resolve_names` over the registered
/// channels, before and after a promotion supersedes some of them.
#[test]
fn prop_channel_names_match_reference() {
    property(
        24,
        (plans(), proptest::bool::ANY, vec(0_usize..6, 1..5)),
        |(plan, promote, asked)| async move {
            let fixture = Fixture::new().await;
            let built = build(&fixture, &plan).await;
            if promote {
                let admin = fixture.caller(Who::Admin).await;
                let action = OperatorAction::PromoteChannel {
                    channel: built.channels[0],
                    pattern: ResourcePattern::Host(Host(
                        match &built.resources[&built.channels[0]].locator {
                            Locator::Url { host, .. } => host.0.clone(),
                            _ => HOSTS[0].to_owned(),
                        },
                    )),
                    policy: PolicyKind::Sanctioned,
                    note: None,
                };
                let _ = fixture.surface.act(&admin, action).await;
            }
            let ids: Vec<ChannelId> = asked
                .iter()
                .map(|index| {
                    built
                        .channels
                        .get(*index)
                        .copied()
                        .unwrap_or(ChannelId::from_ulid(0xBAD))
                })
                .collect();
            let batch = IdBatch::new(ids).map_err(|error| format!("{error:?}"))?;
            let stored = all_channels(&fixture).await?;
            let seeds: BTreeMap<ChannelId, Locator> = stored
                .iter()
                .filter_map(|channel| {
                    let seed = channel.origin.seed()?;
                    let resource = built
                        .resources
                        .values()
                        .find(|resource| resource.id == seed.resource)?;
                    Some((channel.id, resource.locator.clone()))
                })
                .collect();
            let registered: Vec<Registered<'_>> = stored
                .iter()
                .map(|channel| Registered {
                    channel,
                    seed: seeds.get(&channel.id),
                })
                .collect();
            let expected =
                resolve_names(&batch, &registered).map_err(|error| format!("{error:?}"))?;
            let viewer = fixture.caller(Who::Viewer).await;
            let got = fixture
                .surface
                .channel_names(&viewer, &batch)
                .await
                .map_err(|error| format!("names: {error:?}"))?;
            equal("names", &got, &expected)
        },
    );
}

/// INV-690: where the preview promotes, the promotion supersedes exactly
/// the preview's channels; where it has a conflict, the promotion returns
/// it; where it fails, the promotion fails the same way.
#[test]
fn prop_preview_then_promote_agree() {
    let patterns = select(vec![0_usize, 1, 2, 3]);
    property(
        32,
        (plans(), 0_usize..6, patterns, proptest::bool::ANY),
        |(plan, target, pattern, twice)| async move {
            let fixture = Fixture::new().await;
            let built = build(&fixture, &plan).await;
            let channel = built
                .channels
                .get(target)
                .copied()
                .unwrap_or(ChannelId::from_ulid(0xBAD));
            let pattern = match pattern {
                0 => wiki(),
                1 => ResourcePattern::Host(Host(HOSTS[1].to_owned())),
                2 => ResourcePattern::UrlPrefix {
                    host: Host(HOSTS[0].to_owned()),
                    path_prefix: "/1".to_owned(),
                },
                _ => ResourcePattern::Host(Host("nowhere.example".to_owned())),
            };
            let governor = fixture.caller(Who::Governor).await;
            if twice {
                // Promote something first, so later previews meet declared and
                // superseded channels.
                let first = OperatorAction::PromoteChannel {
                    channel: built.channels[0],
                    pattern: wiki(),
                    policy: PolicyKind::Sanctioned,
                    note: None,
                };
                let _ = fixture.surface.act(&governor, first).await;
            }
            let viewer = fixture.caller(Who::Viewer).await;
            let preview = fixture
                .surface
                .promotion_preview(&viewer, channel, &pattern)
                .await;
            let action = OperatorAction::PromoteChannel {
                channel,
                pattern: pattern.clone(),
                policy: PolicyKind::Sanctioned,
                note: None,
            };
            let promoted = fixture.surface.act(&governor, action).await;
            match preview {
                Ok(preview) => match preview.conflict() {
                    None => {
                        ensure(
                            preview.covered_resources().is_some()
                                && preview.uncovered_resources().is_some(),
                            || "no samples".to_owned(),
                        )?;
                        equal(
                            "promotion",
                            &promoted,
                            &Ok(ActionOutcome::ChannelPromoted {
                                channel,
                                superseded: preview.superseded_channels(),
                            }),
                        )?;
                    }
                    Some(conflict) => {
                        ensure(preview.covered_resources().is_none(), || {
                            "a refused preview has samples".to_owned()
                        })?;
                        equal(
                            "promotion",
                            &promoted,
                            &Err(ActionError::Conflict(conflict.clone())),
                        )?;
                    }
                },
                Err(error) => {
                    let mapped = promoted.map_err(QueryError::from);
                    equal("promotion", &mapped.map(|_| ()), &Err(error))?;
                }
            }
            Ok(())
        },
    );
}

/// INV-692: two traversals whose filters differ only in the window list the
/// same channels in the same order.
#[test]
fn prop_channel_window_changes_only_counts() {
    let windows = (
        proptest::option::of((0_u64..5, 1_u64..8)),
        proptest::option::of((0_u64..5, 1_u64..8)),
    );
    property(24, (plans(), windows), |(plan, (left, right))| async move {
        let fixture = Fixture::new().await;
        build(&fixture, &plan).await;
        let filter = |window: Option<(u64, u64)>| ChannelFilter {
            window: window.map(|(start, length)| minutes(start, start + length)),
            ..ChannelFilter::default()
        };
        let ids = |rows: Vec<crosstalk_spec::interfaces::l8_surface::channels::ChannelRow>| {
            rows.iter().map(|row| row.channel().id).collect::<Vec<_>>()
        };
        let left = ids(traverse_rows(&fixture, &filter(left)).await?);
        let right = ids(traverse_rows(&fixture, &filter(right)).await?);
        equal("listed", &left, &right)
    });
}

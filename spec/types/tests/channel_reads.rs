//! Channel read models: list rows and their counts, the channel list
//! filter, batch names, and the promotion preview's agreement with
//! promotion.

use std::num::NonZeroU64;

use crate::aggregates::access::{AgentAccesses, ResourceUse};
use crate::aggregates::node::CanonicalOriginKind;
use crate::batch::{IdBatch, TooManyIds};
use crate::derived::flow::channel::confirmation::CrossTraffic;
use crate::derived::flow::channel::detection::{
    DeclaredDetection, DetectionKind, TrafficDetection,
};
use crate::derived::flow::channel::policy::{Decision, Policy, PolicyAuthor, PolicyKind};
use crate::derived::flow::channel::promotion::{
    COVERAGE_CAP, PromotionRefusal, Registered, coverage, plan,
};
use crate::derived::flow::channel::{
    Channel, ChannelOrigin, Declaration, DeclaredHistory, Seed, Supersession,
};
use crate::derived::flow::resource::{Host, Locator, Resource, ResourcePattern};
use crate::ids::{ChannelId, OperatorId, ResourceId};
use crate::interfaces::l5_flow::PromoteError;
use crate::interfaces::l8_surface::actions::SupersededChannels;
use crate::interfaces::l8_surface::channels::{
    ChannelActivity, ChannelCounts, ChannelName, ChannelRow, ChannelShape, ChannelStanding,
    InvalidChannelName, InvalidChannelRow, InvalidSupersededInto, PromotionPreview, SupersededInto,
    resolve_names,
};
use crate::interfaces::l8_surface::lists::{ChannelFilter, OriginFilter};
use crate::interfaces::l8_surface::{ActionError, ConflictKind, InputError, QueryError};
use crate::support::{Capped, InvalidCapped, TimeWindow};
use crate::tests::fixtures::{agent, at, channel, resource, transmission};

fn url(path: &str) -> Locator {
    Locator::Url {
        scheme: "https".into(),
        host: Host("wiki.example".into()),
        path: path.into(),
        query: None,
    }
}

fn team_pattern() -> ResourcePattern {
    ResourcePattern::UrlPrefix {
        host: Host("wiki.example".into()),
        path_prefix: "/team".into(),
    }
}

fn operator() -> OperatorId {
    OperatorId::from_ulid(7)
}

/// The declaration a promotion by `operator()` at 100 attaches.
fn declaration(pattern: ResourcePattern) -> Declaration {
    Declaration {
        pattern,
        by: PolicyAuthor::Operator(operator()),
        at: at(100),
    }
}

fn stored(n: u128, path: &str) -> Resource {
    Resource {
        id: resource(n),
        locator: url(path),
        first_seen: at(n as u64),
    }
}

/// A discovered channel `id` seeded by resource `id`, also holding
/// `resources`.
fn discovered(id: u128, resources: &[u128]) -> Channel {
    Channel {
        id: channel(id),
        origin: ChannelOrigin::Discovered {
            seed: Seed {
                resource: resource(id),
                first_transmission: transmission(id),
            },
            detection: TrafficDetection::Active {
                since: at(id as u64),
                last_transmission: transmission(id),
            },
        },
        resources: resources.iter().map(|n| resource(*n)).collect(),
        policy: Policy::Unreviewed(None),
    }
}

fn declared_before_traffic(id: u128, pattern: ResourcePattern) -> Channel {
    Channel {
        id: channel(id),
        origin: ChannelOrigin::Declared {
            declaration: Declaration {
                pattern,
                by: PolicyAuthor::Config,
                at: at(0),
            },
            history: DeclaredHistory::BeforeTraffic(DeclaredDetection::AwaitingTraffic),
        },
        resources: Vec::new(),
        policy: Policy::Unreviewed(None),
    }
}

/// Channel `id` discovered, then promoted by `operator()` at 100.
fn promoted(id: u128) -> Channel {
    let mut channel = discovered(id, &[]);
    channel.origin = channel
        .origin
        .promoted(declaration(team_pattern()))
        .expect("discovered channels can be promoted");
    channel.policy = Policy::Sanctioned(Decision {
        by: PolicyAuthor::Operator(operator()),
        at: at(100),
        note: None,
    });
    channel
}

/// Channel `id`, discovered, superseded by channel `by` at 100.
fn superseded(id: u128, by: u128) -> Channel {
    let mut channel = discovered(id, &[]);
    channel.origin = channel
        .origin
        .superseded(Supersession {
            by: crate::tests::fixtures::channel(by),
            at: at(100),
        })
        .expect("discovered channels can be superseded");
    channel
}

/// One confirmed cross-agent transmission over all time.
const CONFIRMED: CrossTraffic = CrossTraffic {
    confirmed: 1,
    unconfirmed: 0,
};

fn seen(writers: u64, readers: u64, transmissions: u64) -> ChannelStanding {
    ChannelStanding::InForce {
        traffic: CONFIRMED,
        activity: ChannelActivity::Seen {
            last: at(500),
            counts: ChannelCounts {
                writers,
                readers,
                transmissions,
            },
        },
    }
}

/// In force, never accessed, no cross-agent traffic.
fn never() -> ChannelStanding {
    ChannelStanding::InForce {
        traffic: CrossTraffic::NONE,
        activity: ChannelActivity::Never,
    }
}

/// `channel`'s row as a store would build it: its seed resource, its own
/// supersession (by a promoted channel), or in force with one confirmed
/// cross-agent transmission when its detection has traffic and none
/// otherwise.
fn row(channel: Channel) -> ChannelRow {
    let seed = channel.origin.seed().map(|seed| Resource {
        id: seed.resource,
        locator: url("/seed"),
        first_seen: at(1),
    });
    let standing = match channel.origin.supersession() {
        Some(supersession) => ChannelStanding::Superseded(
            SupersededInto::of(supersession, &promoted_with_id(supersession.by))
                .expect("superseded by a promoted channel"),
        ),
        None if channel.origin.traffic().is_some() => seen(1, 1, 1),
        None => never(),
    };
    ChannelRow::new(channel, seed, standing).expect("a consistent fixture row")
}

/// A channel promoted by `operator()` at 100 under the id `id`.
fn promoted_with_id(id: ChannelId) -> Channel {
    let mut channel = promoted(1);
    channel.id = id;
    channel
}

fn supersession_of(channel: &Channel) -> Supersession {
    channel
        .origin
        .supersession()
        .expect("fixture channel is superseded")
}

// Rows.

#[test]
fn rows_in_force_carry_activity_and_superseded_rows_carry_their_supersession() {
    let live = ChannelRow::new(
        discovered(3, &[]),
        Some(stored(3, "/team/c")),
        seen(2, 1, 4),
    )
    .expect("a discovered channel with its seed and activity");
    assert_eq!(live.supersession(), None);
    assert_eq!(
        live.counts(),
        Some(ChannelCounts {
            writers: 2,
            readers: 1,
            transmissions: 4,
        })
    );
    assert_eq!(live.last_activity(), Some(at(500)));
    assert_eq!(live.seed().map(|seed| seed.id), Some(resource(3)));

    let absorbed = superseded(2, 1);
    let into = SupersededInto::of(supersession_of(&absorbed), &promoted(1))
        .expect("channel 1 is the promoted superseder");
    let row = ChannelRow::new(
        absorbed,
        Some(stored(2, "/team/b")),
        ChannelStanding::Superseded(into),
    )
    .expect("a superseded channel with its own supersession");
    let shown = row.supersession().expect("superseded");
    assert_eq!(
        (shown.into(), shown.by(), shown.at()),
        (channel(1), operator(), at(100))
    );
    assert_eq!(row.counts(), None);
    assert_eq!(row.last_activity(), None);

    let quiet = ChannelRow::new(declared_before_traffic(4, team_pattern()), None, never())
        .expect("a declared channel that never saw traffic");
    assert_eq!(quiet.counts(), None);
    assert_eq!(quiet.last_activity(), None);
}

#[test]
fn rows_carry_exactly_the_channels_seed_resource() {
    let missing = ChannelRow::new(discovered(3, &[]), None, seen(1, 1, 0));
    assert_eq!(missing, Err(InvalidChannelRow::SeedMismatch));
    let other = ChannelRow::new(
        discovered(3, &[]),
        Some(stored(4, "/team/d")),
        seen(1, 1, 0),
    );
    assert_eq!(other, Err(InvalidChannelRow::SeedMismatch));
    let unseeded = ChannelRow::new(
        declared_before_traffic(4, team_pattern()),
        Some(stored(4, "/team/d")),
        never(),
    );
    assert_eq!(unseeded, Err(InvalidChannelRow::SeedMismatch));
}

#[test]
fn row_standing_follows_the_channels_origin() {
    let into = SupersededInto::of(supersession_of(&superseded(2, 1)), &promoted(1))
        .expect("channel 1 superseded channel 2");
    // A channel in force shown as superseded.
    assert_eq!(
        ChannelRow::new(
            discovered(3, &[]),
            Some(stored(3, "/team/c")),
            ChannelStanding::Superseded(into),
        ),
        Err(InvalidChannelRow::StandingMismatch)
    );
    // A superseded channel shown in force, with counts of its own.
    assert_eq!(
        ChannelRow::new(superseded(2, 1), Some(stored(2, "/team/b")), seen(1, 1, 1),),
        Err(InvalidChannelRow::StandingMismatch)
    );
    // Another channel's supersession.
    let elsewhere = promoted(5);
    let wrong = SupersededInto::of(supersession_of(&superseded(6, 5)), &elsewhere)
        .expect("channel 5 superseded channel 6");
    assert_eq!(
        ChannelRow::new(
            superseded(2, 1),
            Some(stored(2, "/team/b")),
            ChannelStanding::Superseded(wrong),
        ),
        Err(InvalidChannelRow::StandingMismatch)
    );
}

#[test]
fn a_channel_with_traffic_is_never_listed_as_inactive() {
    assert_eq!(
        ChannelRow::new(discovered(3, &[]), Some(stored(3, "/team/c")), never(),),
        Err(InvalidChannelRow::TrafficWithoutActivity)
    );
    assert_eq!(
        ChannelRow::new(promoted(1), Some(stored(1, "/team/a")), never(),),
        Err(InvalidChannelRow::TrafficWithoutActivity)
    );
}

#[test]
fn superseded_into_names_the_promoting_operator() {
    let supersession = supersession_of(&superseded(2, 1));
    assert_eq!(
        SupersededInto::of(supersession, &discovered(1, &[])),
        Err(InvalidSupersededInto::NotPromoted)
    );
    assert_eq!(
        SupersededInto::of(supersession, &promoted(3)),
        Err(InvalidSupersededInto::WrongChannel {
            expected: channel(1),
            got: channel(3),
        })
    );
    let mut by_config = promoted(1);
    if let ChannelOrigin::Declared { declaration, .. } = &mut by_config.origin {
        declaration.by = PolicyAuthor::Config;
    }
    assert_eq!(
        SupersededInto::of(supersession, &by_config),
        Err(InvalidSupersededInto::NotByOperator)
    );
    let later = Supersession {
        by: channel(1),
        at: at(101),
    };
    assert_eq!(
        SupersededInto::of(later, &promoted(1)),
        Err(InvalidSupersededInto::TimeMismatch)
    );
}

fn accesses(n: u64) -> NonZeroU64 {
    NonZeroU64::new(n).expect("fixture counts are non-zero")
}

fn used_by(resource: Resource, writers: &[u128], readers: &[u128]) -> ResourceUse {
    let entries = |agents: &[u128]| {
        agents
            .iter()
            .map(|n| AgentAccesses {
                agent: agent(*n),
                accesses: accesses(1),
            })
            .collect()
    };
    ResourceUse::new(resource, entries(writers), entries(readers))
        .expect("fixture resources are used")
}

#[test]
fn counts_tally_distinct_agents_across_the_channels_resources() {
    let uses = [
        used_by(stored(1, "/team/a"), &[1, 2], &[3]),
        used_by(stored(2, "/team/b"), &[2], &[3, 4]),
        used_by(stored(3, "/team/c"), &[], &[1]),
    ];
    assert_eq!(
        ChannelCounts::tally(&uses, 9),
        ChannelCounts {
            writers: 2,
            readers: 3,
            transmissions: 9,
        }
    );
    assert_eq!(ChannelCounts::tally(&[], 0), ChannelCounts::default());
}

// The list filter.

#[test]
fn the_default_filter_lists_every_channel_in_force_and_no_superseded_one() {
    let filter = ChannelFilter::default();
    assert!(filter.matches(&row(discovered(3, &[]))));
    assert!(filter.matches(&row(promoted(1))));
    assert!(filter.matches(&row(declared_before_traffic(4, team_pattern()))));
    assert!(!filter.matches(&row(superseded(2, 1))));
}

#[test]
fn origin_filter_selects_origins_in_force_and_superseded_channels_by_variant() {
    let (live, absorbed, owner) = (discovered(3, &[]), superseded(2, 1), promoted(1));
    let promoted_only = OriginFilter::InForce(vec![CanonicalOriginKind::Promoted]);
    assert!(promoted_only.matches(&owner));
    assert!(!promoted_only.matches(&live));
    assert!(!promoted_only.matches(&absorbed));

    let with = OriginFilter::WithSuperseded(vec![CanonicalOriginKind::Promoted]);
    assert!(with.matches(&owner));
    assert!(!with.matches(&live));
    assert!(with.matches(&absorbed));
    assert!(OriginFilter::WithSuperseded(Vec::new()).matches(&live));

    assert!(OriginFilter::Superseded.matches(&absorbed));
    assert!(!OriginFilter::Superseded.matches(&owner));
    assert!(!OriginFilter::Superseded.matches(&live));
}

#[test]
fn channel_filter_combines_origin_detection_and_policy() {
    let filter = ChannelFilter {
        origin: OriginFilter::WithSuperseded(Vec::new()),
        listings: Vec::new(),
        detections: vec![DetectionKind::Active],
        policies: vec![PolicyKind::Unreviewed],
        window: None,
    };
    // A superseded channel matches on its own frozen detection and policy.
    assert!(filter.matches(&row(superseded(2, 1))));
    assert!(filter.matches(&row(discovered(3, &[]))));
    // Each case fails exactly one field.
    assert!(!filter.matches(&row(declared_before_traffic(4, team_pattern()))));
    let mut sanctioned = discovered(3, &[]);
    sanctioned.policy = Policy::Sanctioned(Decision {
        by: PolicyAuthor::Config,
        at: at(1),
        note: None,
    });
    assert!(!filter.matches(&row(sanctioned)));
    let in_force_only = ChannelFilter {
        origin: OriginFilter::InForce(Vec::new()),
        ..filter.clone()
    };
    assert!(!in_force_only.matches(&row(superseded(2, 1))));
}

#[test]
fn the_window_never_changes_which_channels_match() {
    let channels = [
        discovered(3, &[]),
        superseded(2, 1),
        promoted(1),
        declared_before_traffic(4, team_pattern()),
    ];
    let window = TimeWindow::new(at(10), at(20)).expect("non-empty");
    for origin in [
        OriginFilter::InForce(Vec::new()),
        OriginFilter::WithSuperseded(vec![CanonicalOriginKind::Discovered]),
        OriginFilter::Superseded,
    ] {
        let all_time = ChannelFilter {
            origin,
            ..ChannelFilter::default()
        };
        let windowed = ChannelFilter {
            window: Some(window),
            ..all_time.clone()
        };
        for channel in &channels {
            assert_eq!(
                all_time.matches(&row(channel.clone())),
                windowed.matches(&row(channel.clone()))
            );
        }
    }
}

// Names.

/// Channel 1 promoted over channel 2; channel 3 discovered; channel 4
/// declared before traffic. Seeds 1 to 3 are at /team/a, /team/b, /other/c.
struct World {
    channels: [Channel; 4],
    seeds: [Locator; 3],
}

impl World {
    fn new() -> Self {
        Self {
            channels: [
                promoted(1),
                superseded(2, 1),
                discovered(3, &[]),
                declared_before_traffic(4, ResourcePattern::McpServer("wiki".into())),
            ],
            seeds: [url("/team/a"), url("/team/b"), url("/other/c")],
        }
    }

    fn registry(&self) -> Vec<Registered<'_>> {
        self.channels
            .iter()
            .map(|channel| Registered {
                channel,
                seed: channel.origin.seed().map(|seed| {
                    let index = usize::try_from(seed.resource.as_ulid() - 1)
                        .expect("fixture seeds are small");
                    &self.seeds[index]
                }),
            })
            .collect()
    }
}

#[test]
fn names_resolve_each_known_id_to_the_channel_in_force() {
    let world = World::new();
    let registry = world.registry();
    let asked = [
        channel(2),
        channel(1),
        channel(3),
        channel(4),
        channel(9),
        channel(2),
    ];
    let batch = IdBatch::new(asked).expect("a small batch");
    let names = resolve_names(&batch, &registry).expect("every known id is named");
    assert_eq!(names.len(), 4, "unknown ids are left out, repeats once");
    let absorbed = &names[&channel(2)];
    assert_eq!(absorbed.id(), channel(1));
    assert_eq!(absorbed.shape(), &ChannelShape::Pattern(team_pattern()));
    assert_eq!(&names[&channel(1)], absorbed);
    assert_eq!(names[&channel(3)].id(), channel(3));
    assert_eq!(
        names[&channel(3)].shape(),
        &ChannelShape::Seed(url("/other/c"))
    );
    assert_eq!(
        names[&channel(4)].shape(),
        &ChannelShape::Pattern(ResourcePattern::McpServer("wiki".into()))
    );
    assert!(!names.contains_key(&channel(9)));
}

#[test]
fn names_refuse_a_batch_over_the_cap() {
    let world = World::new();
    let registry = world.registry();
    let max = IdBatch::<ChannelId>::MAX;
    let full = IdBatch::new((1..=max as u128).map(channel)).expect("exactly the cap");
    assert!(resolve_names(&full, &registry).is_ok());
    let over = IdBatch::new((1..=max as u128 + 1).map(channel));
    assert_eq!(over, Err(TooManyIds { max, got: max + 1 }));
    assert_eq!(
        over.map_err(QueryError::from),
        Err(QueryError::InvalidInput(InputError::TooManyIds {
            max,
            got: max + 1,
        }))
    );
}

#[test]
fn a_name_is_only_built_for_a_channel_in_force_with_a_readable_seed() {
    let absorbed = superseded(2, 1);
    assert_eq!(
        ChannelName::of(&Registered {
            channel: &absorbed,
            seed: Some(&url("/team/b")),
        }),
        Err(InvalidChannelName::Superseded {
            channel: channel(2),
            by: channel(1),
        })
    );
    let live = discovered(3, &[]);
    assert_eq!(
        ChannelName::of(&Registered {
            channel: &live,
            seed: None,
        }),
        Err(InvalidChannelName::SeedUnreadable(channel(3)))
    );
}

// The promotion preview.

/// Channel 1 (seed /team/a, also /other/x) and channel 2 (seed /team/b,
/// also /team/b/c) are discovered; channel 3 (seed /other/c) is discovered
/// elsewhere. Resources: 1 /team/a, 2 /team/b, 3 /other/c, 10 /other/x,
/// 11 /team/b/c.
struct Wiki {
    channels: [Channel; 3],
    seeds: [Locator; 3],
}

impl Wiki {
    fn new() -> Self {
        Self {
            channels: [
                discovered(1, &[10]),
                discovered(2, &[11]),
                discovered(3, &[]),
            ],
            seeds: [url("/team/a"), url("/team/b"), url("/other/c")],
        }
    }

    fn registry(&self) -> Vec<Registered<'_>> {
        self.channels
            .iter()
            .zip(&self.seeds)
            .map(|(channel, seed)| Registered {
                channel,
                seed: Some(seed),
            })
            .collect()
    }
}

fn resource_named(id: ResourceId) -> Resource {
    let path = match id.as_ulid() {
        1 => "/team/a",
        2 => "/team/b",
        3 => "/other/c",
        10 => "/other/x",
        11 => "/team/b/c",
        other => panic!("no fixture resource {other}"),
    };
    stored(id.as_ulid(), path)
}

/// What a store holds for channel `id`: its seed resource and resources.
fn held_by(channels: &[Channel]) -> impl Fn(ChannelId) -> Vec<Resource> + '_ {
    move |id| {
        channels
            .iter()
            .filter(|channel| channel.id == id)
            .flat_map(|channel| {
                channel
                    .origin
                    .seed()
                    .map(|seed| seed.resource)
                    .into_iter()
                    .chain(channel.resources.iter().copied())
            })
            .map(resource_named)
            .collect()
    }
}

fn ids(resources: &[Resource]) -> Vec<u128> {
    resources
        .iter()
        .map(|resource| resource.id.as_ulid())
        .collect()
}

#[test]
fn coverage_splits_every_held_resource_by_the_pattern() {
    let wiki = Wiki::new();
    let registry = wiki.registry();
    let covered = coverage(
        channel(1),
        &declaration(team_pattern()),
        &registry,
        held_by(&wiki.channels),
    )
    .expect("a valid promotion");
    assert_eq!(covered.superseded(), [channel(2)]);
    assert_eq!(
        ids(covered.covered().shown()),
        vec![11, 2, 1],
        "newest first"
    );
    assert_eq!(covered.covered().total(), 3);
    assert!(covered.covered().is_complete());
    assert_eq!(ids(covered.uncovered().shown()), vec![10]);
    assert_eq!(covered.uncovered().total(), 1);
}

#[test]
fn coverage_lists_a_resource_once_however_often_it_is_held() {
    let wiki = Wiki::new();
    let registry = wiki.registry();
    let twice = |id: ChannelId| {
        let once = held_by(&wiki.channels)(id);
        once.iter().cloned().chain(once.clone()).collect()
    };
    let covered = coverage(channel(1), &declaration(team_pattern()), &registry, twice)
        .expect("a valid promotion");
    assert_eq!(ids(covered.covered().shown()), vec![11, 2, 1]);
    assert_eq!(covered.covered().total(), 3);
    assert_eq!(ids(covered.uncovered().shown()), vec![10]);
    assert_eq!(covered.uncovered().total(), 1);
}

#[test]
fn coverage_shows_the_newest_of_each_side_and_counts_them_all() {
    // Channel 1 holds its seed (resource 1, /team/a), 250 more pages under
    // /team and 5 resources elsewhere.
    let target = discovered(1, &[]);
    let seed = url("/team/a");
    let registry = [Registered {
        channel: &target,
        seed: Some(&seed),
    }];
    let held = |id: ChannelId| -> Vec<Resource> {
        if id != channel(1) {
            return Vec::new();
        }
        std::iter::once(stored(1, "/team/a"))
            .chain((1000..1250).map(|n| stored(n, &format!("/team/p{n}"))))
            .chain((2000..2005).map(|n| stored(n, &format!("/other/{n}"))))
            .collect()
    };
    let covered = coverage(channel(1), &declaration(team_pattern()), &registry, held)
        .expect("a valid promotion");
    let sample = covered.covered();
    assert_eq!(sample.shown().len(), COVERAGE_CAP);
    assert_eq!(sample.total(), 251);
    assert_eq!(sample.hidden(), 51);
    assert!(!sample.is_complete());
    let shown = ids(sample.shown());
    assert_eq!(shown.first(), Some(&1249), "newest first");
    assert_eq!(shown.last(), Some(&1050));
    assert_eq!(
        ids(covered.uncovered().shown()),
        vec![2004, 2003, 2002, 2001, 2000]
    );
    assert!(covered.uncovered().is_complete());
}

#[test]
fn a_sample_never_shows_more_than_its_cap_or_its_total() {
    type Three = Capped<u8, 3>;
    let capped = Three::new(vec![1, 2, 3], 10).expect("within the cap");
    assert_eq!((capped.total(), capped.hidden()), (10, 7));
    assert!(!capped.is_complete());
    assert!(Three::new(vec![1, 2], 2).expect("complete").is_complete());
    assert_eq!(
        Three::new(vec![1, 2, 3, 4], 4),
        Err(InvalidCapped::TooMany { max: 3, got: 4 })
    );
    assert_eq!(
        Three::new(vec![1, 2], 1),
        Err(InvalidCapped::TotalBelowShown { total: 1, shown: 2 })
    );
    let first = Three::first(vec![9, 8, 7, 6, 5]);
    assert_eq!((first.shown(), first.total()), (&[9, 8, 7][..], 5));
    let short = Three::first(vec![9]);
    assert_eq!((short.shown(), short.total()), (&[9][..], 1));
}

/// Every request of the agreement tests: the wiki's promotions, plus a
/// superseded, a declared and an overlapping case.
fn agreement_cases() -> Vec<(Vec<Channel>, Vec<Locator>, ChannelId, ResourcePattern)> {
    let wiki = Wiki::new();
    let host = ResourcePattern::Host(Host("wiki.example".into()));
    let mut with_owner = wiki.channels.to_vec();
    with_owner.push(declared_before_traffic(5, host.clone()));
    let mut seeds = wiki.seeds.to_vec();
    seeds.push(url("/unused"));
    let absorbed = vec![superseded(2, 1)];
    vec![
        (
            wiki.channels.to_vec(),
            wiki.seeds.to_vec(),
            channel(1),
            team_pattern(),
        ),
        (
            wiki.channels.to_vec(),
            wiki.seeds.to_vec(),
            channel(3),
            host.clone(),
        ),
        (
            wiki.channels.to_vec(),
            wiki.seeds.to_vec(),
            channel(9),
            team_pattern(),
        ),
        (
            wiki.channels.to_vec(),
            wiki.seeds.to_vec(),
            channel(3),
            team_pattern(),
        ),
        (with_owner, seeds, channel(1), team_pattern()),
        (absorbed, vec![url("/team/b")], channel(2), team_pattern()),
        (
            vec![declared_before_traffic(4, host.clone())],
            vec![url("/unused")],
            channel(4),
            host,
        ),
    ]
}

#[test]
fn coverage_and_plan_agree_on_every_request() {
    for (channels, seeds, target, pattern) in agreement_cases() {
        let registry: Vec<Registered<'_>> = channels
            .iter()
            .zip(&seeds)
            .map(|(channel, seed)| Registered {
                channel,
                seed: channel.origin.seed().map(|_| seed),
            })
            .collect();
        let declared = declaration(pattern);
        let planned = plan(target, &declared, &registry);
        let covered = coverage(target, &declared, &registry, |_| Vec::new());
        match (planned, covered) {
            (Ok(plan), Ok(covered)) => {
                assert_eq!(
                    plan.superseded_ids().collect::<Vec<_>>(),
                    covered.superseded()
                );
            }
            (Err(planned), Err(covered)) => assert_eq!(planned, covered),
            (planned, covered) => panic!("plan {planned:?} but coverage {covered:?}"),
        }
    }
}

#[test]
fn the_preview_answers_as_promote_channel_would() {
    for (channels, seeds, target, pattern) in agreement_cases() {
        let registry: Vec<Registered<'_>> = channels
            .iter()
            .zip(&seeds)
            .map(|(channel, seed)| Registered {
                channel,
                seed: channel.origin.seed().map(|_| seed),
            })
            .collect();
        let declared = declaration(pattern);
        // What PromoteChannel returns in this state.
        let promoted = plan(target, &declared, &registry)
            .map(|plan| SupersededChannels::new(plan.superseded_ids()))
            .map_err(ActionError::from);
        let preview = PromotionPreview::from_registry(
            coverage(target, &declared, &registry, held_by(&channels))
                .map_err(PromoteError::Refused),
        );
        match (promoted, preview) {
            (Ok(superseded), Ok(preview)) => {
                assert_eq!(preview.conflict(), None);
                assert_eq!(preview.superseded_channels(), superseded);
                assert!(preview.covered_resources().is_some());
                assert!(preview.uncovered_resources().is_some());
            }
            (Err(ActionError::Conflict(kind)), Ok(preview)) => {
                assert_eq!(preview.conflict(), Some(&kind));
                assert!(preview.superseded_channels().is_empty());
                assert!(preview.covered_resources().is_none());
                assert!(preview.uncovered_resources().is_none());
            }
            (Err(refused), Err(error)) => {
                assert!(!matches!(refused, ActionError::Conflict(_)));
                assert_eq!(QueryError::from(refused), error);
            }
            (promoted, preview) => panic!("promote {promoted:?} but preview {preview:?}"),
        }
    }
}

#[test]
fn preview_maps_each_refusal_as_the_action_does() {
    let preview = |refusal| PromotionPreview::from_registry(Err(PromoteError::Refused(refusal)));
    assert_eq!(
        preview(PromotionRefusal::UnknownChannel(channel(9))),
        Err(QueryError::NotFound)
    );
    assert_eq!(
        preview(PromotionRefusal::PatternMissesSeed),
        Err(QueryError::InvalidInput(InputError::PatternMissesSeed))
    );
    let superseded = preview(PromotionRefusal::Superseded {
        channel: channel(2),
        by: channel(1),
    })
    .expect("a conflict is an answer");
    assert_eq!(
        superseded.conflict(),
        Some(&ConflictKind::ChannelSuperseded {
            channel: channel(2),
            by: channel(1),
        })
    );
    let declared = preview(PromotionRefusal::NotDiscovered(channel(4))).expect("an answer");
    assert_eq!(
        declared.conflict(),
        Some(&ConflictKind::ChannelNotDiscovered {
            channel: channel(4),
        })
    );
    let overlap = preview(PromotionRefusal::PatternOverlaps {
        existing: channel(5),
    })
    .expect("an answer");
    assert_eq!(
        overlap.conflict(),
        Some(&ConflictKind::PatternOverlaps {
            existing: channel(5),
        })
    );
    assert_eq!(
        PromotionPreview::from_registry(Err(PromoteError::Store {
            reason: "down".into(),
        })),
        Err(QueryError::Store {
            reason: "down".into(),
        })
    );
}

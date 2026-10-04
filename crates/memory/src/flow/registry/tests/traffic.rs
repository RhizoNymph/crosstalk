//! Reference tests for discovery by cross-agent transmission, resources on
//! no channel, channel traffic and its listing, and a channel's
//! transmissions.

use crosstalk_spec::derived::flow::channel::confirmation::{
    Confirmation, CrossTraffic, Listing, ListingKind,
};
use crosstalk_spec::interfaces::l8_surface::channel_traffic::ChannelTransmissionFilter;
use crosstalk_spec::paging::{PageRequest, PageSize};

use super::*;
use crate::analysis::aliases::StaticDirectory;

/// `flow.registry.at-most-one-channel-per-resource`: a second discovery
/// from the same resource finds the channel the first created, changes
/// nothing and publishes no `ChannelDiscovered`; the resource is stored
/// once.
#[tokio::test]
async fn a_second_discovery_finds_the_first_channel() {
    let (mut registry, mut events) = registry().await;
    discover(&mut registry, 0, 0).await;
    let before = model::all_channels(&registry).await;
    drain(&mut events);
    assert_eq!(
        registry
            .discover(channel(1), resource(0).id, seed_transmission(9), at(9))
            .await,
        Ok(Discovery::Existing(channel(0)))
    );
    assert!(drain(&mut events).is_empty());
    assert_eq!(model::all_channels(&registry).await, before);
    assert_eq!(registry.channel(channel(1)).await, Ok(None));
    assert_eq!(
        registry.add_resource(resource(0)).await,
        Err(TrafficError::DuplicateResource(resource(0).id))
    );
}

/// `flow.channel.discovered-by-cross-agent-transmission`, at the store: the
/// channel is seeded by the resource and the transmission, active since it
/// opened, unreviewed, holds the resource, and its creation publishes one
/// `ChannelDiscovered` with the seed.
#[tokio::test]
async fn discovered_by_first_cross_agent_transmission() {
    let (mut registry, mut events) = registry().await;
    store_resource(&mut registry, 3).await;
    drain(&mut events);
    assert_eq!(
        registry
            .discover(channel(0), resource(3).id, seed_transmission(3), at(30))
            .await,
        Ok(Discovery::Created(channel(0)))
    );
    let seed = Seed {
        resource: resource(3).id,
        first_transmission: seed_transmission(3),
        opened_at: at(30),
    };
    let stored_channel = stored(&registry, channel(0)).await;
    assert_eq!(
        stored_channel.origin,
        ChannelOrigin::Discovered {
            seed,
            detection: TrafficDetection::Active {
                since: at(30),
                last_transmission: seed_transmission(3),
            },
        }
    );
    assert_eq!(stored_channel.policy, Policy::Unreviewed(None));
    assert_eq!(
        registry.lookup(&locator(3)).await,
        Ok(ChannelLookup::Known(channel(0)))
    );
    assert_eq!(
        drain(&mut events),
        vec![
            BusEvent::Detect(DetectEvent::ChannelDiscovered {
                channel: channel(0),
                seed
            }),
            BusEvent::Changed(Changed::Channel(channel(0))),
        ]
    );
}

/// `flow.channel.resource-only-until-cross-agent`, at the store: accesses
/// to a resource on no channel, by one agent or by writers nobody else
/// reads, create no channel and publish nothing.
#[tokio::test]
async fn single_agent_resource_stays_a_resource() {
    let (mut registry, mut events) = registry().await;
    assert_eq!(registry.add_resource(resource(6)).await, Ok(None));
    for (n, write) in [(1u128, true), (2, false), (3, true)] {
        let access = crosstalk_spec::derived::flow::access::Access {
            id: AccessId::from_ulid(0x5100 | n),
            agent: access_agent(1),
            exchange: crosstalk_spec::ids::ExchangeId::from_ulid(n),
            resource: resource(6).id,
            at: at(u64::try_from(n).unwrap_or(0)),
            via: crosstalk_spec::derived::flow::access::Extraction::Structured,
            op: if write {
                crosstalk_spec::derived::flow::access::AccessOp::Write {
                    call: part(),
                    spans: Vec::new(),
                }
            } else {
                crosstalk_spec::derived::flow::access::AccessOp::Read { result: part() }
            },
        };
        assert_eq!(registry.record_access(access).await, Ok(()));
    }
    assert!(drain(&mut events).is_empty());
    let every = ChannelFilter {
        origin: OriginFilter::WithSuperseded(Vec::new()),
        ..ChannelFilter::default()
    };
    assert_eq!(
        model::channels_under(&registry, &every).await,
        Ok(Vec::new())
    );
    assert_eq!(
        registry.lookup(&locator(6)).await,
        Ok(ChannelLookup::NoChannel)
    );
}

/// A tool-call or tool-result part the tests' accesses point at.
fn part() -> crosstalk_spec::observed::message::PartRef {
    crosstalk_spec::observed::message::PartRef {
        message: crosstalk_spec::ids::MessageHash::from_digest(
            crosstalk_spec::support::Blake3::from_bytes([2; 32]),
        ),
        index: 0,
    }
}

/// A registry resolving agents through a directory the test merges and
/// unmerges.
fn registry_over(
    directory: StaticDirectory,
) -> (MemoryChannels<StaticDirectory>, UnboundedReceiver<BusEvent>) {
    let (outbox, events) = Outbox::channel();
    (
        MemoryChannels::new(directory, IdSequence::default(), outbox),
        events,
    )
}

/// Every listed channel's listing under the default filter, newest
/// created first.
async fn listings<D>(registry: &MemoryChannels<D>) -> Vec<(ChannelId, Option<Listing>)>
where
    D: crosstalk_spec::interfaces::l3_reconstruction::AgentDirectory + Send + Sync,
{
    let Ok(size) = PageSize::new(50) else {
        panic!("size");
    };
    let Ok(page) = registry
        .channels(
            &ChannelFilter::default(),
            &PageRequest { size, after: None },
        )
        .await
    else {
        panic!("channels");
    };
    page.items()
        .iter()
        .map(|read| (read.channel().id, read.listing()))
        .collect()
}

/// `flow.registry.lookup-never-creates`, at the writes: a resource joins a
/// channel only through a declared pattern, never a discovered channel; a
/// resource seen on no channel joins a declaration made since on its next
/// sighting, or when a transmission through it would discover a channel;
/// and a second resource with a known locator is refused.
#[tokio::test]
async fn resources_join_only_declared_channels() {
    let (mut registry, mut events) = registry().await;
    discover(&mut registry, 0, 0).await;
    // Locator 1 (wiki /a/b) is on no channel: a discovered channel takes
    // no resources but its seed.
    assert_eq!(registry.add_resource(resource(1)).await, Ok(None));
    assert_eq!(stored(&registry, channel(0)).await.resources, Vec::new());
    // Locator 5 (/shared/y) is stored on no channel before /shared is
    // declared.
    assert_eq!(registry.add_resource(resource(5)).await, Ok(None));
    let Ok(declared) = registry
        .declare(
            pattern(2),
            Policy::Unreviewed(None),
            PolicyAuthor::Config,
            at(100),
        )
        .await
    else {
        panic!("declare");
    };
    drain(&mut events);
    // A transmission through it now joins it to the declaration instead of
    // discovering a channel.
    assert_eq!(
        registry
            .discover(channel(1), resource(5).id, seed_transmission(5), at(110))
            .await,
        Ok(Discovery::Existing(declared))
    );
    assert_eq!(registry.channel(channel(1)).await, Ok(None));
    assert!(!drain(&mut events).iter().any(|event| matches!(
        event,
        BusEvent::Detect(DetectEvent::ChannelDiscovered { .. })
    )));
    assert_eq!(
        stored(&registry, declared).await.resources,
        vec![resource(5).id]
    );
    // A first sighting of a matching locator joins the declaration too.
    assert_eq!(registry.add_resource(resource(4)).await, Ok(Some(declared)));
    // Another resource with a stored locator is refused.
    let twin = crosstalk_spec::derived::flow::resource::Resource {
        id: crosstalk_spec::ids::ResourceId::from_ulid(0x7777),
        ..resource(1)
    };
    assert_eq!(
        registry.add_resource(twin).await,
        Err(TrafficError::DuplicateLocator {
            existing: resource(1).id,
            lookup: ChannelLookup::NoChannel,
        })
    );
    assert_eq!(
        registry
            .discover(
                channel(2),
                crosstalk_spec::ids::ResourceId::from_ulid(0x7778),
                seed_transmission(7),
                at(1)
            )
            .await,
        Err(TrafficError::UnknownResource(
            crosstalk_spec::ids::ResourceId::from_ulid(0x7778)
        ))
    );
}

/// `surface.channels.listing-from-traffic`, at L5: a discovered channel is
/// unconfirmed while its cross-agent traffic is all unconfirmed and
/// confirmed once a confirmed transmission routes through it; a declared
/// channel without traffic is a declaration; the default filter lists
/// both, and "confirmed only" leaves the unconfirmed one out.
#[tokio::test]
async fn listing_follows_cross_agent_traffic() {
    let (mut registry, _events) = registry().await;
    discover(&mut registry, 0, 0).await;
    discover(&mut registry, 1, 3).await;
    let Ok(declared) = registry
        .declare(
            pattern(2),
            Policy::Unreviewed(None),
            PolicyAuthor::Config,
            at(100),
        )
        .await
    else {
        panic!("declare");
    };
    let unconfirmed = Listing::Channel(Confirmation::Unconfirmed);
    assert_eq!(
        listings(&registry).await,
        vec![
            (declared, Some(Listing::Declaration)),
            (channel(1), Some(unconfirmed)),
            (channel(0), Some(unconfirmed)),
        ]
    );
    // Channel 0's seed transmission is confirmed.
    let confirmed = routed_transmission(seed_transmission(0), channel(0), 3, false, 0);
    assert_eq!(
        registry.record_transmission(&confirmed).await,
        Ok(Change::Applied)
    );
    let Ok(Some(read)) = registry.channel(channel(0)).await else {
        panic!("channel 0");
    };
    assert_eq!(
        read.traffic(),
        Some(CrossTraffic {
            confirmed: 1,
            unconfirmed: 0
        })
    );
    assert_eq!(
        read.listing(),
        Some(Listing::Channel(Confirmation::Confirmed))
    );
    let confirmed_only = ChannelFilter {
        listings: vec![ListingKind::Confirmed, ListingKind::Declaration],
        ..ChannelFilter::default()
    };
    let Ok(listed) = model::channels_under(&registry, &confirmed_only).await else {
        panic!("channels");
    };
    let ids: Vec<ChannelId> = listed.iter().map(|read| read.channel().id).collect();
    assert_eq!(ids, vec![channel(0), declared]);
}

/// `surface.channels.merge-hides-unmerge-restores`, at L5: once every
/// transmission through a discovered channel is between ids of one merged
/// agent the channel is hidden (in no list; `channel` still returns it,
/// with its record), and the unmerge lists it again.
#[tokio::test]
async fn a_merge_hides_a_discovered_channel_and_an_unmerge_lists_it_again() {
    let directory = StaticDirectory::new();
    let (mut registry, _events) = registry_over(directory.clone());
    assert_eq!(registry.add_resource(resource(6)).await, Ok(None));
    assert_eq!(
        registry
            .discover(channel(0), resource(6).id, seed_transmission(6), at(6))
            .await,
        Ok(Discovery::Created(channel(0)))
    );
    // Sent by agent 3 to agent 2: they cross until merged.
    let opened = routed_transmission(seed_transmission(6), channel(0), 1, true, 6);
    assert_eq!(
        registry.record_transmission(&opened).await,
        Ok(Change::Applied)
    );
    let unconfirmed = Some(Listing::Channel(Confirmation::Unconfirmed));
    assert_eq!(listings(&registry).await, vec![(channel(0), unconfirmed)]);
    let Ok(()) = directory.merge(access_agent(3), access_agent(2)) else {
        panic!("merge");
    };
    assert_eq!(listings(&registry).await, Vec::new());
    let Ok(Some(hidden)) = registry.channel(channel(0)).await else {
        panic!("a hidden channel is still returned by id");
    };
    assert_eq!(hidden.listing(), Some(Listing::Hidden));
    assert_eq!(hidden.traffic(), Some(CrossTraffic::NONE));
    directory.unmerge(access_agent(3));
    assert_eq!(listings(&registry).await, vec![(channel(0), unconfirmed)]);
}

/// The channel list is newest created first: by `created_at` (a
/// declaration's time, a discovered channel's first cross-agent
/// transmission's opening), ties by id descending, and its cursor resumes
/// after the last row.
#[tokio::test]
async fn channels_list_newest_created_first() {
    let (mut registry, _events) = registry().await;
    // Channel 3 is discovered at 7 µs, channel 0 at 2 µs and channel 2 at
    // 2 µs too (a tie, ordered by id), then a declaration at 5 µs.
    for (c, r, opened) in [(3u8, 7u8, 7u64), (0, 0, 2), (2, 6, 2)] {
        store_resource(&mut registry, r).await;
        assert_eq!(
            registry
                .discover(channel(c), resource(r).id, seed_transmission(r), at(opened))
                .await,
            Ok(Discovery::Created(channel(c)))
        );
        let opened = routed_transmission(seed_transmission(r), channel(c), 1, false, opened);
        assert_eq!(
            registry.record_transmission(&opened).await,
            Ok(Change::Applied)
        );
    }
    let Ok(declared) = registry
        .declare(
            pattern(2),
            Policy::Unreviewed(None),
            PolicyAuthor::Config,
            at(5),
        )
        .await
    else {
        panic!("declare");
    };
    let Ok(size) = PageSize::new(2) else {
        panic!("size");
    };
    let filter = ChannelFilter::default();
    let Ok(first) = registry
        .channels(&filter, &PageRequest { size, after: None })
        .await
    else {
        panic!("first page");
    };
    let (items, next) = first.into_parts();
    let ids: Vec<ChannelId> = items.iter().map(|read| read.channel().id).collect();
    assert_eq!(ids, vec![channel(3), declared]);
    let created: Vec<Timestamp> = items
        .iter()
        .map(|read| read.channel().origin.created_at())
        .collect();
    assert_eq!(created, vec![at(7), at(5)]);
    let Ok(second) = registry
        .channels(&filter, &PageRequest { size, after: next })
        .await
    else {
        panic!("second page");
    };
    let ids: Vec<ChannelId> = second
        .items()
        .iter()
        .map(|read| read.channel().id)
        .collect();
    assert_eq!(ids, vec![channel(2), channel(0)]);
    assert!(second.next().is_none());
}

/// `surface.channels.transmissions-cross-agent`, at L5: a channel's
/// transmissions are the crossing ones routed through it or a channel it
/// superseded, newest opened first, filtered by confirmation, never one
/// within one merged agent; the cursor binds the channel and filter.
#[tokio::test]
async fn a_channels_transmissions_are_crossing_and_newest_first() {
    let (mut registry, _events) = registry().await;
    discover(&mut registry, 0, 0).await;
    discover(&mut registry, 1, 1).await;
    assert!(
        registry
            .promote(channel(0), promotion(1, 200))
            .await
            .is_ok()
    );
    // Through superseded channel 1: confirmed, opened at 30 µs.
    let confirmed = routed_transmission(TransmissionId::from_ulid(0xA1), channel(1), 3, false, 30);
    // Through channel 0: suspected, opened at 40 µs.
    let suspected = routed_transmission(TransmissionId::from_ulid(0xA2), channel(0), 2, false, 40);
    // Through channel 0, but from agent 3, merged into the reader.
    let within = routed_transmission(TransmissionId::from_ulid(0xA3), channel(0), 2, true, 50);
    for transmission in [&confirmed, &suspected, &within] {
        assert_eq!(
            registry.record_transmission(transmission).await,
            Ok(Change::Applied)
        );
    }
    let every = ChannelTransmissionFilter::default();
    let Ok(pages) = model::transmissions(&registry, channel(1), &every, 2).await else {
        panic!("transmissions");
    };
    let ids: Vec<TransmissionId> = pages.iter().flatten().map(|t| t.id).collect();
    assert_eq!(
        ids,
        vec![
            suspected.id,
            confirmed.id,
            seed_transmission(1),
            seed_transmission(0)
        ]
    );
    let unconfirmed = ChannelTransmissionFilter {
        confirmation: Some(Confirmation::Unconfirmed),
    };
    let Ok(pages) = model::transmissions(&registry, channel(0), &unconfirmed, 5).await else {
        panic!("transmissions");
    };
    let ids: Vec<TransmissionId> = pages.iter().flatten().map(|t| t.id).collect();
    assert_eq!(
        ids,
        vec![suspected.id, seed_transmission(1), seed_transmission(0)]
    );
    let Ok(size) = PageSize::new(1) else {
        panic!("size");
    };
    let Ok(first) = registry
        .transmissions(channel(0), &every, &PageRequest { size, after: None })
        .await
    else {
        panic!("first page");
    };
    assert_eq!(
        registry
            .transmissions(
                channel(0),
                &unconfirmed,
                &PageRequest {
                    size,
                    after: first.next().cloned()
                }
            )
            .await
            .map(|_| ()),
        Err(RegistryError::InvalidCursor)
    );
    assert_eq!(
        registry
            .transmissions(
                model::unknown_channel(),
                &every,
                &PageRequest { size, after: None }
            )
            .await
            .map(|_| ()),
        Err(RegistryError::UnknownChannel(model::unknown_channel()))
    );
}

/// Only a channel-routed transmission is a channel's traffic; recording
/// another changes nothing.
#[tokio::test]
async fn record_transmission_refuses_routes_without_a_channel() {
    let (mut registry, mut events) = registry().await;
    discover(&mut registry, 0, 0).await;
    drain(&mut events);
    let mut direct = routed_transmission(TransmissionId::from_ulid(0xB1), channel(0), 3, false, 9);
    direct.route = Route::Unobserved;
    assert_eq!(
        registry.record_transmission(&direct).await,
        Err(TrafficError::NotChannelRouted(direct.id))
    );
    let unknown = routed_transmission(
        TransmissionId::from_ulid(0xB2),
        model::unknown_channel(),
        3,
        false,
        9,
    );
    assert_eq!(
        registry.record_transmission(&unknown).await,
        Err(TrafficError::UnknownChannel(model::unknown_channel()))
    );
    assert!(drain(&mut events).is_empty());
}

/// `flow.channel.declared-detection-on-cross-agent-transmission`: a
/// declared channel's accesses leave it awaiting traffic; its first
/// cross-agent transmission (opened by a co-access) puts it in use, active
/// since the transmission opened, from awaiting traffic or unused alike.
#[tokio::test]
async fn a_declared_channel_is_in_use_from_its_first_cross_agent_transmission() {
    for unused_first in [false, true] {
        let (mut registry, _events) = registry().await;
        let Ok(declared) = registry
            .declare(
                pattern(2),
                Policy::Unreviewed(None),
                PolicyAuthor::Config,
                at(1),
            )
            .await
        else {
            panic!("declare");
        };
        assert_eq!(registry.add_resource(resource(4)).await, Ok(Some(declared)));
        let write = crosstalk_spec::derived::flow::access::Access {
            id: AccessId::from_ulid(0x5200),
            agent: access_agent(1),
            exchange: crosstalk_spec::ids::ExchangeId::from_ulid(1),
            resource: resource(4).id,
            at: at(2),
            via: crosstalk_spec::derived::flow::access::Extraction::Structured,
            op: crosstalk_spec::derived::flow::access::AccessOp::Write {
                call: part(),
                spans: Vec::new(),
            },
        };
        assert_eq!(registry.record_access(write).await, Ok(()));
        let awaiting = DeclaredHistory::BeforeTraffic(DeclaredDetection::AwaitingTraffic);
        let history = |channel: Channel| match channel.origin {
            ChannelOrigin::Declared { history, .. } => history,
            other => panic!("not declared: {other:?}"),
        };
        assert_eq!(history(stored(&registry, declared).await), awaiting);
        if unused_first {
            assert_eq!(
                registry
                    .set_detection(declared, DetectionUpdate::Unused { since: at(3) })
                    .await,
                Ok(Change::Applied)
            );
        }
        let opened = routed_transmission(TransmissionId::from_ulid(0xC1), declared, 1, false, 9);
        assert_eq!(
            registry.record_transmission(&opened).await,
            Ok(Change::Applied)
        );
        assert_eq!(
            history(stored(&registry, declared).await),
            DeclaredHistory::BeforeTraffic(DeclaredDetection::InUse(TrafficDetection::Active {
                since: at(9),
                last_transmission: opened.id,
            }))
        );
        // A suspected or discarded state of it moves no detection.
        let suspected = routed_transmission(opened.id, declared, 2, false, 9);
        assert_eq!(
            registry.record_transmission(&suspected).await,
            Ok(Change::Applied)
        );
        assert_eq!(
            stored(&registry, declared).await.origin.traffic(),
            Some(&TrafficDetection::Active {
                since: at(9),
                last_transmission: opened.id,
            })
        );
    }
}

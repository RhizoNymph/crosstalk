//! A medium handed to another shard carries its accesses' exchange starts
//! (`flow.correlator.handoff-carries-exchange-starts`).

use crosstalk_spec::derived::flow::transmission::{DelegationDirection, NonChannelRoute};
use crosstalk_spec::interfaces::l5_flow::TransmissionUpdate;

use super::fixtures::{Scene, secs, timing};
use crate::correlate::{Kin, MediumKey, WindowedCorrelator};

/// A delegation match carried by a read the target shard has not seen
/// waits there for its reader exchange's start; once the read's medium is
/// handed over, the shard knows the start, and the match opens its
/// `Delegation` transmission when the window closes, as it would had the
/// read reached the shard directly.
#[test]
fn a_handed_read_starts_its_exchange_on_the_target_shard() {
    let mut scene = Scene::new(42);
    let (parent, child) = (scene.agent(), scene.agent());
    let resource = scene.resource();
    let channel = scene.ids.channel();
    let span = scene.span();
    let read = scene.read(parent, resource, secs(30));
    let content = scene.carried(&read, child, span);

    let mut resource_shard = WindowedCorrelator::new(timing());
    assert!(resource_shard.access(&read, None).is_empty());
    let Some(evidence) = resource_shard.take_medium(MediumKey::Resource(resource)) else {
        panic!("no evidence held for the resource");
    };

    let mut channel_shard = WindowedCorrelator::new(timing());
    for (agent, kin) in [
        (parent, Kin::root(parent)),
        (
            child,
            Kin {
                canonical: child,
                parent: Some(parent),
            },
        ),
    ] {
        channel_shard.learn_kin(agent, kin);
    }
    assert!(channel_shard.content(&content).is_empty());
    let _ = channel_shard.absorb(MediumKey::Channel(channel), evidence);
    let opened = channel_shard.tick(timing().window_closes_at(read.at));
    let routes: Vec<_> = opened
        .iter()
        .filter_map(|decided| match &decided.update {
            TransmissionUpdate::OpenConfirmed { route, .. } => Some(route.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(
        routes,
        vec![NonChannelRoute::Delegation(
            DelegationDirection::ChildToParent
        )]
    );
}

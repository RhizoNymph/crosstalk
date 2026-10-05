//! Moving a medium's evidence: a resource's to its discovered channel,
//! across shards.

use crosstalk_spec::interfaces::l5_flow::TransmissionUpdate;

use super::fixtures::{Scene, secs, timing};
use super::kinds;
use crate::correlate::lifecycle::UpdateKind;
use crate::correlate::{MediumKey, WindowedCorrelator};

/// A transmission opened on a resource, handed to another correlator as
/// its channel's, is confirmed there by a match routed to the channel, and
/// a later write on the channel pairs with the earlier read.
#[test]
fn handed_evidence_meets_later_evidence() {
    let mut scene = Scene::new(40);
    let (a, b, c) = (scene.agent(), scene.agent(), scene.agent());
    let resource = scene.resource();
    let channel = scene.ids.channel();
    let span = scene.span();
    let write = scene.write(a, resource, secs(0), vec![span]);
    let read = scene.read(b, resource, secs(30));

    let mut resource_shard = WindowedCorrelator::new(timing());
    resource_shard.access(&write, None);
    let opened = resource_shard.access(&read, None);
    assert_eq!(kinds(&opened), vec![UpdateKind::OpenChannel]);
    let id = super::subject(&opened[0].update);

    let mut channel_shard = WindowedCorrelator::new(timing());
    let Some(evidence) = resource_shard.take_medium(MediumKey::Resource(resource)) else {
        panic!("no evidence held for the resource");
    };
    assert!(!resource_shard.holds(MediumKey::Resource(resource)));
    assert!(
        channel_shard
            .absorb(MediumKey::Channel(channel), evidence)
            .is_empty()
    );

    let content = scene.carried(&read, a, span);
    assert!(channel_shard.content(&content).is_empty());
    // A later write by another agent and B's read: a second co-access, on
    // the channel, with the handed read.
    let later = scene.write(c, resource, secs(20), vec![]);
    let out = channel_shard.access(&later, Some(channel));
    assert_eq!(kinds(&out), vec![UpdateKind::OpenChannel]);
    let TransmissionUpdate::OpenChannel { on, .. } = &out[0].update else {
        panic!("{out:?}");
    };
    assert_eq!(
        *on,
        crosstalk_spec::interfaces::l5_flow::OpensOn::Channel(channel)
    );

    let out = channel_shard.tick(timing().window_closes_at(read.at));
    let confirmed: Vec<_> = out
        .iter()
        .filter(|decided| matches!(decided.update, TransmissionUpdate::Confirm { .. }))
        .map(|decided| super::subject(&decided.update))
        .collect();
    assert_eq!(confirmed, vec![id]);
}

/// Rekeying within one shard keeps every open transmission and its state.
#[test]
fn rekey_within_a_shard_keeps_state() {
    let mut scene = Scene::new(41);
    let (a, b) = (scene.agent(), scene.agent());
    let resource = scene.resource();
    let channel = scene.ids.channel();
    let span = scene.span();
    let write = scene.write(a, resource, secs(0), vec![span]);
    let read = scene.read(b, resource, secs(30));
    let mut correlator = WindowedCorrelator::new(timing());
    correlator.access(&write, None);
    correlator.access(&read, None);
    assert!(
        correlator
            .rekey(MediumKey::Resource(resource), MediumKey::Channel(channel))
            .is_empty()
    );
    assert!(correlator.holds(MediumKey::Channel(channel)));
    assert!(!correlator.holds(MediumKey::Resource(resource)));
    let out = correlator.tick(timing().window_closes_at(read.at));
    assert_eq!(kinds(&out), vec![UpdateKind::Suspect]);
}

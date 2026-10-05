//! A reader re-reading a page version it already read: the span it
//! received again refreshes the delivery and confirms no second
//! transmission (`flow.correlator.reread-refreshes-delivery`).

use crosstalk_spec::derived::flow::access::Access;
use crosstalk_spec::derived::provenance::matching::ContentMatch;
use crosstalk_spec::ids::{AgentId, TransmissionId};
use crosstalk_spec::interfaces::l5_flow::TransmissionUpdate;

use super::fixtures::{Scene, secs, timing};
use super::{fold, kinds};
use crate::correlate::lifecycle::{Stage, UpdateKind};
use crate::correlate::{Decided, WindowedCorrelator};

const DAY: u64 = 24 * 3_600;

/// A writes a page at 0 s; B reads it at `first` and again, in another
/// exchange, at `again`, A's span in both tool results.
struct Reread {
    write: Access,
    first: Access,
    again: Access,
    first_match: ContentMatch,
    again_match: ContentMatch,
    b: AgentId,
}

fn reread(seed: u32, first: u64, again: u64) -> Reread {
    let mut scene = Scene::new(seed);
    let (a, b) = (scene.agent(), scene.agent());
    let resource = scene.resource();
    let span = scene.span();
    let write = scene.write(a, resource, secs(0), vec![span]);
    let first = scene.read(b, resource, secs(first));
    let again = scene.read(b, resource, secs(again));
    let first_match = scene.carried(&first, a, span);
    let again_match = scene.carried(&again, a, span);
    Reread {
        write,
        first,
        again,
        first_match,
        again_match,
        b,
    }
}

fn confirmed(out: &[Decided]) -> Vec<TransmissionId> {
    out.iter()
        .filter_map(|decided| match &decided.update {
            TransmissionUpdate::Confirm { transmission, .. }
            | TransmissionUpdate::OpenConfirmed { transmission, .. } => Some(*transmission),
            _ => None,
        })
        .collect()
}

fn permutations(n: usize) -> Vec<Vec<usize>> {
    if n == 0 {
        return vec![Vec::new()];
    }
    let mut all = Vec::new();
    for rest in permutations(n - 1) {
        for at in 0..=rest.len() {
            let mut order = rest.clone();
            order.insert(at, n - 1);
            all.push(order);
        }
    }
    all
}

/// Two reads of the same page version by B within the window give one
/// confirmed transmission, the first read's, in every delivery order; the
/// reread's co-access is suspected and discarded, never confirmed.
#[test]
fn two_reads_of_one_page_version_give_one_transmission() {
    let drop = reread(11, 30, 90);
    let mut expected = None;
    for order in permutations(5) {
        let mut correlator = WindowedCorrelator::new(timing());
        let mut out = Vec::new();
        for index in order {
            out.extend(match index {
                0 => correlator.access(&drop.write, None),
                1 => correlator.access(&drop.first, None),
                2 => correlator.access(&drop.again, None),
                3 => correlator.content(&drop.first_match),
                _ => correlator.content(&drop.again_match),
            });
        }
        out.extend(correlator.tick(timing().expires_at(timing().window_closes_at(drop.again.at))));
        let confirmations = confirmed(&out);
        assert_eq!(confirmations.len(), 1, "{out:?}");
        let finals = fold(&out);
        let Ok(finals) = finals else {
            panic!("{finals:?}");
        };
        let confirmed_states: Vec<_> = finals
            .values()
            .filter(|state| state.stage == Stage::Confirmed)
            .collect();
        assert_eq!(confirmed_states.len(), 1, "{finals:?}");
        assert_eq!(confirmed_states[0].to, drop.b);
        assert!(
            finals
                .values()
                .all(|state| matches!(state.stage, Stage::Confirmed | Stage::Discarded)),
            "{finals:?}"
        );
        // The first read's transmission holds the first read's match only.
        let first_tx = out.iter().find_map(|decided| match &decided.update {
            TransmissionUpdate::Confirm { confirmed, .. } => Some(confirmed.clone()),
            _ => None,
        });
        assert_eq!(
            first_tx.map(|confirmed| confirmed.content().first().clone()),
            Some(drop.first_match.clone())
        );
        match &expected {
            None => expected = Some(finals),
            Some(first) => assert_eq!(first, &finals),
        }
    }
}

/// A reread a day later, past the window, opens nothing at all: the
/// first read confirmed, the reread only refreshes its delivery.
#[test]
fn a_reread_a_day_later_opens_nothing() {
    let drop = reread(12, 30, DAY);
    let mut correlator = WindowedCorrelator::new(timing());
    let mut out = correlator.access(&drop.write, None);
    out.extend(correlator.access(&drop.first, None));
    out.extend(correlator.content(&drop.first_match));
    out.extend(correlator.tick(timing().window_closes_at(drop.first.at)));
    assert_eq!(
        kinds(&out),
        vec![UpdateKind::OpenChannel, UpdateKind::Confirm]
    );
    let mut later = correlator.tick(secs(DAY - 1));
    later.extend(correlator.access(&drop.again, None));
    later.extend(correlator.content(&drop.again_match));
    later.extend(correlator.tick(timing().expires_at(timing().window_closes_at(drop.again.at))));
    assert!(later.is_empty(), "{later:?}");
}

/// A reread that carries new content as well confirms a transmission with
/// the new content only; the repeated span stays with the first.
#[test]
fn a_reread_with_new_content_confirms_only_the_new_content() {
    let mut scene = Scene::new(13);
    let (a, b) = (scene.agent(), scene.agent());
    let resource = scene.resource();
    let (old_span, new_span) = (scene.span(), scene.span());
    let first_write = scene.write(a, resource, secs(0), vec![old_span]);
    let second_write = scene.write(a, resource, secs(60), vec![old_span, new_span]);
    let first = scene.read(b, resource, secs(30));
    let again = scene.read(b, resource, secs(90));
    let first_match = scene.carried(&first, a, old_span);
    let repeated = scene.carried(&again, a, old_span);
    let fresh = scene.carried(&again, a, new_span);
    let mut correlator = WindowedCorrelator::new(timing());
    let mut out = Vec::new();
    for access in [&first_write, &second_write, &first, &again] {
        out.extend(correlator.access(access, None));
    }
    for content in [&first_match, &repeated, &fresh] {
        out.extend(correlator.content(content));
    }
    out.extend(correlator.tick(timing().window_closes_at(again.at)));
    let contents: Vec<Vec<ContentMatch>> = out
        .iter()
        .filter_map(|decided| match &decided.update {
            TransmissionUpdate::Confirm { confirmed, .. } => {
                Some(confirmed.content().iter().cloned().collect())
            }
            _ => None,
        })
        .collect();
    assert_eq!(contents, vec![vec![first_match], vec![fresh]], "{out:?}");
}

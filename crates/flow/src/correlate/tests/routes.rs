//! Route selection: delegation, direct carriers and unobserved content.

use crosstalk_spec::derived::flow::transmission::{
    DelegationDirection, DirectCarrier, NonChannelRoute,
};
use crosstalk_spec::derived::provenance::matching::{Carrier, ContentMatch};
use crosstalk_spec::ids::AgentId;
use crosstalk_spec::interfaces::l5_flow::TransmissionUpdate;
use crosstalk_spec::observed::message::{ToolCallId, ToolName};

use super::fixtures::{Scene, secs, timing, tool_result};
use crate::correlate::{Kin, UNKNOWN_TOOL, WindowedCorrelator};

/// The route `content` opens with, fed after its exchange's start and
/// decided once its window closed.
fn route_of(
    correlator: &mut WindowedCorrelator,
    content: &ContentMatch,
) -> Option<NonChannelRoute> {
    correlator.exchange(content.reader_exchange(), secs(0));
    let mut out = correlator.content(content);
    out.extend(correlator.tick(secs(1_000)));
    out.into_iter().find_map(|decided| match decided.update {
        TransmissionUpdate::OpenConfirmed { route, .. } => Some(route),
        _ => None,
    })
}

fn family(scene: &mut Scene, correlator: &mut WindowedCorrelator) -> (AgentId, AgentId) {
    let (parent, child) = (scene.agent(), scene.agent());
    correlator.learn_kin(parent, Kin::root(parent));
    correlator.learn_kin(
        child,
        Kin {
            canonical: child,
            parent: Some(parent),
        },
    );
    (parent, child)
}

/// `ParentToChild` when the sender is the reader's parent,
/// `ChildToParent` when the reader is the sender's
/// (`flow.route.delegation-direction`).
#[test]
fn delegation_direction() {
    let mut scene = Scene::new(20);
    let mut correlator = WindowedCorrelator::new(timing());
    let (parent, child) = family(&mut scene, &mut correlator);
    let (down, up) = (scene.exchange(), scene.exchange());
    let (s1, s2) = (scene.span(), scene.span());
    let task = scene.found(parent, child, down, s1, Carrier::UserTurn);
    let result = scene.found(child, parent, up, s2, tool_result(up));
    assert_eq!(
        route_of(&mut correlator, &task),
        Some(NonChannelRoute::Delegation(
            DelegationDirection::ParentToChild
        ))
    );
    assert_eq!(
        route_of(&mut correlator, &result),
        Some(NonChannelRoute::Delegation(
            DelegationDirection::ChildToParent
        ))
    );
}

/// No parent link, no delegation; a merged parent still counts, resolved
/// through the merge table (`flow.route.delegation-parent-link`).
#[test]
fn delegation_requires_parent_link() {
    let mut scene = Scene::new(21);
    let mut correlator = WindowedCorrelator::new(timing());
    let (a, b) = (scene.agent(), scene.agent());
    correlator.learn_kin(a, Kin::root(a));
    correlator.learn_kin(b, Kin::root(b));
    let exchange = scene.exchange();
    let span = scene.span();
    let unrelated = scene.found(a, b, exchange, span, Carrier::UserTurn);
    assert_eq!(
        route_of(&mut correlator, &unrelated),
        Some(NonChannelRoute::Direct(DirectCarrier::UserTurn))
    );

    // `alias` was merged into `parent`; `child`'s canonical parent is
    // `parent`. A match from the alias is from the parent.
    let (parent, alias, child) = (scene.agent(), scene.agent(), scene.agent());
    correlator.learn_kin(parent, Kin::root(parent));
    correlator.learn_kin(
        alias,
        Kin {
            canonical: parent,
            parent: None,
        },
    );
    correlator.learn_kin(
        child,
        Kin {
            canonical: child,
            parent: Some(parent),
        },
    );
    let exchange = scene.exchange();
    let span = scene.span();
    let from_alias = scene.found(alias, child, exchange, span, Carrier::UserTurn);
    assert_eq!(
        route_of(&mut correlator, &from_alias),
        Some(NonChannelRoute::Delegation(
            DelegationDirection::ParentToChild
        ))
    );

    // Unknown agents are never assumed related.
    let mut fresh = WindowedCorrelator::new(timing());
    let exchange = scene.exchange();
    let span = scene.span();
    let unknown = scene.found(parent, child, exchange, span, Carrier::UserTurn);
    assert_eq!(
        route_of(&mut fresh, &unknown),
        Some(NonChannelRoute::Direct(DirectCarrier::UserTurn))
    );
}

/// A tool-result match whose call yielded no access opens
/// `Direct(ToolResult)` named after the call; a user turn and a system
/// prompt are direct; the reader's own output is unobserved.
#[test]
fn direct_and_unobserved_carriers() {
    let mut scene = Scene::new(22);
    let mut correlator = WindowedCorrelator::new(timing());
    let (a, b) = (scene.agent(), scene.agent());
    let named = scene.exchange();
    let span = scene.span();
    let content = scene.found(a, b, named, span, tool_result(named));
    let Carrier::ToolResult(call) = content.carrier().clone() else {
        panic!("not a tool result");
    };
    correlator.tool_named(b, &call, ToolName("Bash".to_owned()), secs(0));
    assert_eq!(
        route_of(&mut correlator, &content),
        Some(NonChannelRoute::Direct(DirectCarrier::ToolResult(
            ToolName("Bash".to_owned())
        )))
    );

    let unnamed = scene.exchange();
    let span = scene.span();
    let content = scene.found(
        a,
        b,
        unnamed,
        span,
        Carrier::ToolResult(ToolCallId("toolu_unseen".to_owned())),
    );
    assert_eq!(
        route_of(&mut correlator, &content),
        Some(NonChannelRoute::Direct(DirectCarrier::ToolResult(
            ToolName(UNKNOWN_TOOL.to_owned())
        )))
    );

    for (carrier, route) in [
        (
            Carrier::UserTurn,
            NonChannelRoute::Direct(DirectCarrier::UserTurn),
        ),
        (
            Carrier::SystemPrompt,
            NonChannelRoute::Direct(DirectCarrier::SystemPrompt),
        ),
        (Carrier::ReaderOutput, NonChannelRoute::Unobserved),
    ] {
        let exchange = scene.exchange();
        let span = scene.span();
        let content = scene.found(a, b, exchange, span, carrier);
        assert_eq!(route_of(&mut correlator, &content), Some(route));
    }
}

/// A tool-result match whose read arrives before its window closes is
/// held for the channel, never opened direct.
#[test]
fn a_read_before_the_close_keeps_a_match_off_the_direct_route() {
    let mut scene = Scene::new(23);
    let mut correlator = WindowedCorrelator::new(timing());
    let (a, b) = (scene.agent(), scene.agent());
    let resource = scene.resource();
    let span = scene.span();
    let exchange = scene.exchange();
    let write = scene.write(a, resource, secs(0), vec![span]);
    let read = scene.read_in(b, resource, secs(30), exchange);
    let content = scene.carried(&read, a, span);
    assert!(correlator.content(&content).is_empty());
    correlator.access(&write, None);
    correlator.access(&read, None);
    let out = correlator.tick(timing().window_closes_at(secs(30)));
    assert!(
        out.iter()
            .all(|decided| !matches!(decided.update, TransmissionUpdate::OpenConfirmed { .. })),
        "{out:?}"
    );
    assert!(
        out.iter()
            .any(|decided| matches!(decided.update, TransmissionUpdate::Confirm { .. }))
    );
}

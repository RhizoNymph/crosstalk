//! Binding each named scenario's roles to the generated world.
//!
//! Every binder picks things the world holds that satisfy the scenario's
//! facts; the suite's scenario tests then check each fact through L8, so a
//! binder that picks wrong fails there, not silently.

use crosstalk_conformance::ProvisionError;
use crosstalk_conformance::scenario::named::{
    declared, dropped_bodies, hidden_channel, hijacked_wiki, impersonation, late_confirmation,
    lone_resource, merges, pipeline, policies, promotion, registered, routes, suspected, topics,
    verdicts,
};
use crosstalk_conformance::scenario::{AgentRole, Bindings, Scenario, TransmissionRole};
use crosstalk_spec::derived::flow::transmission::{DelegationDirection, Route, TransmissionState};
use crosstalk_spec::derived::flow::verdict::Verdict;

use super::find::{Find, confirmed_later};
use crate::store::State;
use crate::world::{BodySide, ChannelKey, TxRecord, World};

/// Binds every part of `scenario`.
pub fn scenario(
    world: &World,
    state: &State,
    scenario: &Scenario,
) -> Result<Bindings, ProvisionError> {
    let mut bindings = Bindings::new();
    for part in scenario.parts() {
        let find = Find::new(world, state, part);
        let binder: fn(&Find<'_>, &mut Bindings) -> Result<(), ProvisionError> = match *part {
            hijacked_wiki::NAME => bind_hijacked_wiki,
            late_confirmation::NAME => bind_late_confirmation,
            impersonation::NAME => bind_impersonation,
            merges::NAME => bind_merges,
            hidden_channel::NAME => bind_hidden_channel,
            suspected::NAME => bind_suspected,
            declared::NAME => bind_declared,
            lone_resource::NAME => bind_lone_resource,
            promotion::NAME => bind_promotion,
            policies::NAME => bind_policies,
            topics::NAME => bind_topics,
            verdicts::NAME => bind_verdicts,
            dropped_bodies::NAME => bind_dropped_bodies,
            pipeline::NAME => |_, _| Ok(()),
            routes::NAME => bind_routes,
            registered::NAME => bind_registered,
            other => return Err(ProvisionError::Unsupported { scenario: other }),
        };
        binder(&find, &mut bindings)?;
    }
    Ok(bindings)
}

/// Whether a transmission's state is of one kind.
type StatePredicate = fn(&TransmissionState) -> bool;

/// Binds a transmission and its writer and reader roles.
fn bind_tx(
    f: &Find<'_>,
    b: &mut Bindings,
    record: &TxRecord,
    role: TransmissionRole,
    writer: Option<AgentRole>,
    reader: AgentRole,
) -> Result<(), ProvisionError> {
    b.bind(role, record.transmission.id);
    b.bind(reader, record.transmission.to);
    if let Some(writer) = writer {
        let id = f.writer(record).ok_or_else(|| f.missing("writer"))?;
        b.bind(writer, id);
    }
    Ok(())
}

fn bind_hijacked_wiki(f: &Find<'_>, b: &mut Bindings) -> Result<(), ProvisionError> {
    use hijacked_wiki::*;
    let (wiki, talk) = (
        f.channel(ChannelKey::HijackedWiki)?,
        f.channel(ChannelKey::WikiTalk)?,
    );
    b.bind(WIKI, wiki);
    b.bind(TALK_PAGE, talk);
    b.bind(PAGE, f.seed(ChannelKey::HijackedWiki)?);
    b.bind(TALK, f.seed(ChannelKey::WikiTalk)?);
    let pi0 = f.agent("pi0")?;
    let confirmed = f.confirmed("confirmed wiki transmission from pi0", |r| {
        r.transmission.route == Route::Channel(wiki) && r.from == Some(pi0)
    })?;
    bind_tx(f, b, confirmed, CONFIRMED, Some(WRITER), READER)?;
    let on_talk = f.on_channel(ChannelKey::WikiTalk)?;
    bind_tx(f, b, on_talk, ON_TALK, Some(TALK_WRITER), TALK_READER)
}

fn bind_late_confirmation(f: &Find<'_>, b: &mut Bindings) -> Result<(), ProvisionError> {
    use late_confirmation::*;
    let late = f.confirmed("transmission confirmed a bucket later", confirmed_later)?;
    bind_tx(f, b, late, LATE, Some(SENDER), RECEIVER)
}

fn bind_impersonation(f: &Find<'_>, b: &mut Bindings) -> Result<(), ProvisionError> {
    use impersonation::*;
    let pi0 = f.agent("pi0")?;
    let sent = f.confirmed("transmission from pi0", |r| r.from == Some(pi0))?;
    bind_tx(f, b, sent, SENT, Some(IMPERSONATOR), PEER)
}

fn bind_merges(f: &Find<'_>, b: &mut Bindings) -> Result<(), ProvisionError> {
    use merges::*;
    let [cc0, al0, child, pi2, al2, al3, pi1, omp3, omp1, cc6, cc5] = [
        "cc0", "al0", "cc0.a", "pi2", "al2", "al3", "pi1", "omp3", "omp1", "cc6", "cc5",
    ]
    .map(|key| f.agent(key));
    let (cc0, al0, pi2, al2, al3, omp3, omp1) = (cc0?, al0?, pi2?, al2?, al3?, omp3?, omp1?);
    b.bind(CANONICAL, cc0);
    b.bind(ALIAS, al0);
    b.bind(CHILD, child?);
    b.bind(ALIAS_MERGE, f.merge(al0, cc0)?);
    let self_edge = f
        .world
        .transmissions
        .iter()
        .find(|r| r.from == Some(al0) && r.transmission.to == cc0)
        .ok_or_else(|| f.missing("confirmed transmission from al0 to cc0"))?;
    b.bind(SELF_EDGE, self_edge.transmission.id);
    b.bind(HOLDER, pi2);
    b.bind(CHAIN_FIRST, al2);
    b.bind(CHAIN_SECOND, al3);
    b.bind(INNER_MERGE, f.merge(al2, al3)?);
    b.bind(OUTER_MERGE, f.merge(al3, pi2)?);
    let held = f.confirmed("transmission from pi2", |r| r.from == Some(pi2))?;
    bind_tx(f, b, held, HOLDER_SENT, Some(HOLDER), HOLDER_PEER)?;
    let pi1 = pi1?;
    let target = f.confirmed("transmission from pi1", |r| r.from == Some(pi1))?;
    bind_tx(f, b, target, TARGET_SENT, Some(TARGET), TARGET_PEER)?;
    b.bind(VETOED, omp3);
    b.bind(VETOED_INTO, omp1);
    b.bind(REVERTED_MERGE, f.merge(omp3, omp1)?);
    b.bind(SPARE_A, cc6?);
    b.bind(SPARE_B, cc5?);
    Ok(())
}

fn bind_hidden_channel(f: &Find<'_>, b: &mut Bindings) -> Result<(), ProvisionError> {
    use hidden_channel::*;
    let (cx1, al1) = (f.agent("cx1")?, f.agent("al1")?);
    let notes = f.channel(ChannelKey::SelfNotes)?;
    b.bind(OWNER, cx1);
    b.bind(OTHER_ID, al1);
    b.bind(MERGE, f.merge(al1, cx1)?);
    b.bind(NOTES, f.seed(ChannelKey::SelfNotes)?);
    b.bind(SELF_NOTES, notes);
    let between = f
        .world
        .transmissions
        .iter()
        .rev()
        .find(|r| {
            r.transmission.route == Route::Channel(notes)
                && r.from == Some(al1)
                && r.transmission.to == cx1
        })
        .ok_or_else(|| f.missing("confirmed transmission from al1 to cx1 on the notes"))?;
    b.bind(BETWEEN, between.transmission.id);
    Ok(())
}

fn bind_suspected(f: &Find<'_>, b: &mut Bindings) -> Result<(), ProvisionError> {
    use suspected::*;
    let s3 = f.channel(ChannelKey::S3Handoff)?;
    b.bind(S3, s3);
    b.bind(OBJECT, f.seed(ChannelKey::S3Handoff)?);
    let tx = f.tx("suspected transmission on the S3 object", |r| {
        r.transmission.route == Route::Channel(s3)
            && matches!(r.transmission.state, TransmissionState::Suspected { .. })
            && f.crosses(r)
    })?;
    bind_tx(f, b, tx, SUSPECTED, Some(WRITER), READER)
}

fn bind_declared(f: &Find<'_>, b: &mut Bindings) -> Result<(), ProvisionError> {
    use crosstalk_spec::derived::flow::resource::Locator;
    use declared::*;
    let in_use = f.channel(ChannelKey::InternalWiki)?;
    b.bind(IN_USE, in_use);
    b.bind(UNUSED, f.channel(ChannelKey::DesignDocs)?);
    let page = f
        .world
        .resources
        .iter()
        .find(|r| {
            f.world.resource_channel.get(&r.id) == Some(&in_use)
                && matches!(&r.locator, Locator::Url { host, path, .. }
                    if host.0 == "wiki.corp.internal" && path == "/eng/runbooks/deploy")
        })
        .ok_or_else(|| f.missing("the deploy runbook page"))?
        .id;
    b.bind(PAGE, page);
    let on_page = f.confirmed("confirmed transmission on the runbook page", |r| {
        r.transmission.route == Route::Channel(in_use) && touches(f, r, page)
    })?;
    bind_tx(f, b, on_page, ON_PAGE, Some(WRITER), READER)
}

fn bind_lone_resource(f: &Find<'_>, b: &mut Bindings) -> Result<(), ProvisionError> {
    use lone_resource::*;
    b.bind(LONER, f.agent("cc7")?);
    b.bind(SCRATCH, f.world.scenario.lone_resource);
    Ok(())
}

fn bind_promotion(f: &Find<'_>, b: &mut Bindings) -> Result<(), ProvisionError> {
    use promotion::*;
    b.bind(NOTES, f.channel(ChannelKey::TeamNotes)?);
    b.bind(OLD, f.channel(ChannelKey::OldTeamNotes)?);
    b.bind(RETRO, f.seed(ChannelKey::TeamNotes)?);
    b.bind(STANDUP, f.seed(ChannelKey::OldTeamNotes)?);
    let retro = f.seed(ChannelKey::TeamNotes)?;
    let notes = f.channel(ChannelKey::TeamNotes)?;
    let on_retro = f.confirmed("confirmed transmission on the retro page", |r| {
        r.transmission.route == Route::Channel(notes) && touches(f, r, retro)
    })?;
    bind_tx(f, b, on_retro, ON_RETRO, Some(WRITER), READER)?;
    let on_standup = f.on_channel(ChannelKey::OldTeamNotes)?;
    bind_tx(f, b, on_standup, ON_STANDUP, Some(OLD_WRITER), OLD_READER)
}

/// Whether a transmission's co-access records name `resource`.
fn touches(f: &Find<'_>, record: &TxRecord, resource: crosstalk_spec::ids::ResourceId) -> bool {
    record
        .accesses
        .iter()
        .filter_map(|id| f.world.access(*id))
        .any(|access| access.resource == resource)
}

fn bind_policies(f: &Find<'_>, b: &mut Bindings) -> Result<(), ProvisionError> {
    use policies::*;
    b.bind(PASTEBIN, f.channel(ChannelKey::Pastebin)?);
    b.bind(MEMORY, f.channel(ChannelKey::McpMemory)?);
    b.bind(PASTE, f.seed(ChannelKey::Pastebin)?);
    b.bind(ENTITIES, f.seed(ChannelKey::McpMemory)?);
    let pasted = f.on_channel(ChannelKey::Pastebin)?;
    bind_tx(f, b, pasted, PASTED, Some(PASTER), PASTE_READER)?;
    let remembered = f.on_channel(ChannelKey::McpMemory)?;
    bind_tx(f, b, remembered, REMEMBERED, Some(REMEMBERER), RECALLER)
}

fn bind_topics(f: &Find<'_>, b: &mut Bindings) -> Result<(), ProvisionError> {
    b.bind(topics::STALE, f.stale_rule()?);
    Ok(())
}

fn bind_verdicts(f: &Find<'_>, b: &mut Bindings) -> Result<(), ProvisionError> {
    use verdicts::*;
    let agent = |name: &'static str| AgentRole::new(NAME, name);
    let judged_false = f.confirmed("confirmed false detection", |r| {
        f.verdicts(r.transmission.id).last() == Some(&Some(Verdict::FalseDetection))
    })?;
    bind_tx(
        f,
        b,
        judged_false,
        JUDGED_FALSE,
        Some(agent("false_writer")),
        agent("false_reader"),
    )?;
    let withdrawn = f.confirmed("confirmed withdrawn verdict", |r| {
        f.verdicts(r.transmission.id)
            .ends_with(&[Some(Verdict::Genuine), None])
    })?;
    bind_tx(
        f,
        b,
        withdrawn,
        WITHDRAWN,
        Some(agent("withdrawn_writer")),
        agent("withdrawn_reader"),
    )?;
    let unjudged = f.confirmed("confirmed unjudged transmission", |r| {
        f.verdicts(r.transmission.id).is_empty()
    })?;
    bind_tx(
        f,
        b,
        unjudged,
        UNJUDGED,
        Some(agent("unjudged_writer")),
        agent("unjudged_reader"),
    )?;
    let states: [(TransmissionRole, &str, StatePredicate); 3] = [
        (AWAITING, "awaiting", |s| {
            matches!(s, TransmissionState::AwaitingContent { .. })
        }),
        (SUSPECTED, "suspected", |s| {
            matches!(s, TransmissionState::Suspected { .. })
        }),
        (DISCARDED, "discarded", |s| {
            matches!(s, TransmissionState::Discarded { .. })
        }),
    ];
    for (role, name, kind) in states {
        let record = f.tx(name, |r| kind(&r.transmission.state) && f.crosses(r))?;
        let (writer, reader) = match name {
            "awaiting" => ("awaiting_writer", "awaiting_reader"),
            "suspected" => ("suspected_writer", "suspected_reader"),
            _ => ("discarded_writer", "discarded_reader"),
        };
        bind_tx(f, b, record, role, Some(agent(writer)), agent(reader))?;
    }
    let detected = f.tx("detected transmission", |r| {
        matches!(r.transmission.state, TransmissionState::Detected)
    })?;
    bind_tx(f, b, detected, DETECTED, None, agent("detected_reader"))
}

fn bind_dropped_bodies(f: &Find<'_>, b: &mut Bindings) -> Result<(), ProvisionError> {
    use dropped_bodies::*;
    let agent = |name: &'static str| AgentRole::new(NAME, name);
    let dropped = &f.world.scenario.dropped;
    for (role, side, writer, reader) in [
        (SENDER_GONE, BodySide::Sender, "a_writer", "a_reader"),
        (READER_GONE, BodySide::Reader, "b_writer", "b_reader"),
    ] {
        let record = f.confirmed("transmission with a dropped body", |r| {
            dropped.contains(&(r.transmission.id, side))
        })?;
        bind_tx(f, b, record, role, Some(agent(writer)), agent(reader))?;
    }
    let kept = f.confirmed("transmission with both bodies", |r| {
        dropped.iter().all(|(id, _)| *id != r.transmission.id)
    })?;
    bind_tx(f, b, kept, KEPT, Some(agent("c_writer")), agent("c_reader"))
}

fn bind_routes(f: &Find<'_>, b: &mut Bindings) -> Result<(), ProvisionError> {
    use routes::*;
    let agent = |name: &'static str| AgentRole::new(NAME, name);
    let delegated = f.confirmed("delegation from a parent to its child", |r| {
        r.transmission.route == Route::Delegation(DelegationDirection::ParentToChild)
    })?;
    bind_tx(f, b, delegated, DELEGATED, Some(PARENT), CHILD)?;
    let direct = f.confirmed("direct transmission", |r| {
        matches!(r.transmission.route, Route::Direct(_))
    })?;
    bind_tx(
        f,
        b,
        direct,
        DIRECT,
        Some(agent("direct_writer")),
        agent("direct_reader"),
    )?;
    let unobserved = f.confirmed("unobserved transmission", |r| {
        r.transmission.route == Route::Unobserved
    })?;
    bind_tx(
        f,
        b,
        unobserved,
        UNOBSERVED,
        Some(agent("unobserved_writer")),
        agent("unobserved_reader"),
    )
}

fn bind_registered(f: &Find<'_>, b: &mut Bindings) -> Result<(), ProvisionError> {
    b.bind(registered::IDLE, f.agent("reg0")?);
    Ok(())
}

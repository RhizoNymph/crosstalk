//! Binding the named scenarios to a world `crosstalk-world` seeded into any
//! stores, through the spec's store read traits.
//!
//! Every named scenario describes a case of the synthetic week the world
//! seeds (the world is the fixture's week, ported onto the write traits).
//! [`bind`] finds what satisfies each fact: agents, channels, merges and
//! rules from the world's own handles ([`crosstalk_world::Scenario`]),
//! transmissions and seeds from the stores. A harness over any store set
//! the world was seeded into (memory or Postgres, served in process or over
//! HTTP) provisions the named scenarios with it; the suite's scenario tests
//! then check every bound fact through L8.

mod find;

use crosstalk_spec::aggregates::series::BucketWidth;
use crosstalk_spec::derived::flow::channel::{ChannelOrigin, DeclaredHistory};
use crosstalk_spec::derived::flow::resource::Locator;
use crosstalk_spec::derived::flow::transmission::{
    DelegationDirection, Route, Transmission, TransmissionState,
};
use crosstalk_spec::derived::flow::verdict::Verdict;
use crosstalk_spec::ids::{ChannelId, ResourceId};
use crosstalk_spec::interfaces::l3_reconstruction::AgentDirectory;
use crosstalk_spec::interfaces::l5_flow::ChannelDirectory;
use crosstalk_spec::interfaces::l5_flow::channels::{AccessStore, ChannelReads};
use crosstalk_spec::interfaces::l5_flow::transmissions::TransmissionStore;
use crosstalk_spec::interfaces::l5_flow::verdicts::TransmissionVerdicts;
use crosstalk_spec::support::Timestamp;
use crosstalk_world::{BodySide as WorldSide, ChannelKey, MergeKey, RuleKey};

use crate::harness::{ExpectedFailure, ProvisionError};
use crate::scenario::named::{
    declared, dropped_bodies, hidden_channel, hijacked_wiki, impersonation, late_confirmation,
    lone_resource, merges, pipeline, policies, promotion, registered, routes, suspected, topics,
    verdicts,
};
use crate::scenario::{AgentRole, Bindings, Scenario, TransmissionRole};

use find::Snapshot;

/// What [`bind`] reads: the stores the world was seeded into, the world's
/// handles, and the surface's settling point and bucket width.
pub struct WorldReads<'a, A, C, T> {
    pub agents: &'a A,
    pub channels: &'a C,
    pub transmissions: &'a T,
    pub world: &'a crosstalk_world::Scenario,
    /// Confirmed transmissions bound to roles settled before it.
    pub watermark: Timestamp,
    pub bucket: BucketWidth,
}

/// Binds every part of `scenario` to the seeded world.
pub async fn bind<A, C, T>(
    reads: WorldReads<'_, A, C, T>,
    scenario: &Scenario,
) -> Result<Bindings, ProvisionError>
where
    A: AgentDirectory,
    C: ChannelDirectory + ChannelReads + AccessStore,
    T: TransmissionStore + TransmissionVerdicts,
{
    let snapshot = Snapshot::read(
        reads.agents,
        reads.channels,
        reads.transmissions,
        reads.watermark,
        reads.bucket,
        scenario.name(),
    )
    .await?;
    let f = Find {
        s: &snapshot,
        world: reads.world,
        channels: reads.channels,
    };
    let mut b = Bindings::new();
    for part in scenario.parts() {
        match *part {
            hijacked_wiki::NAME => f.hijacked_wiki(&mut b).await?,
            late_confirmation::NAME => f.late_confirmation(&mut b)?,
            impersonation::NAME => f.impersonation(&mut b)?,
            merges::NAME => f.merges(&mut b)?,
            hidden_channel::NAME => f.hidden_channel(&mut b).await?,
            suspected::NAME => f.suspected(&mut b).await?,
            declared::NAME => f.declared(&mut b)?,
            lone_resource::NAME => f.lone_resource(&mut b)?,
            promotion::NAME => f.promotion(&mut b).await?,
            policies::NAME => f.policies(&mut b).await?,
            topics::NAME => b.bind(
                topics::STALE,
                f.world
                    .rule(RuleKey::Stale)
                    .ok_or_else(|| snapshot.missing("stale rule"))?,
            ),
            verdicts::NAME => f.verdicts(&mut b)?,
            dropped_bodies::NAME => f.dropped_bodies(&mut b)?,
            pipeline::NAME => {}
            routes::NAME => f.routes(&mut b)?,
            registered::NAME => b.bind(registered::IDLE, f.agent("reg0")?),
            other => return Err(ProvisionError::Unsupported { scenario: other }),
        }
    }
    Ok(b)
}

/// The searches every part's binder shares.
struct Find<'s, 'a, A, C> {
    s: &'s Snapshot<'a, A, C>,
    world: &'s crosstalk_world::Scenario,
    channels: &'a C,
}

impl<A, C> Find<'_, '_, A, C>
where
    A: AgentDirectory,
    C: ChannelDirectory + ChannelReads + AccessStore,
{
    fn agent(&self, key: &str) -> Result<crosstalk_spec::ids::AgentId, ProvisionError> {
        self.world
            .agent(key)
            .ok_or_else(|| self.s.missing(&format!("agent {key}")))
    }

    fn channel(&self, key: ChannelKey) -> Result<ChannelId, ProvisionError> {
        self.world
            .channel(key)
            .ok_or_else(|| self.s.missing(&format!("channel {key:?}")))
    }

    fn merge(&self, key: MergeKey) -> Result<crosstalk_spec::ids::MergeId, ProvisionError> {
        self.world
            .merge(key)
            .ok_or_else(|| self.s.missing(&format!("merge {key:?}")))
    }

    /// The resource a discovered, superseded or promoted channel was
    /// seeded with, as the registry stores it.
    async fn seed(&self, key: ChannelKey) -> Result<ResourceId, ProvisionError> {
        let id = self.channel(key)?;
        let stored = self
            .channels
            .channel(id)
            .await
            .map_err(|e| self.s.missing(&format!("channel {key:?} ({e:?})")))?
            .ok_or_else(|| self.s.missing(&format!("stored channel {key:?}")))?;
        let (channel, _) = stored.into_parts();
        match channel.origin {
            ChannelOrigin::Discovered { seed, .. } | ChannelOrigin::Superseded { seed, .. } => {
                Ok(seed.resource)
            }
            ChannelOrigin::Declared {
                history: DeclaredHistory::Promoted { from, .. },
                ..
            } => Ok(from.resource),
            ChannelOrigin::Declared { .. } => Err(self.s.missing(&format!("seed of {key:?}"))),
        }
    }

    /// Binds a transmission and its writer and reader roles.
    fn bind_tx(
        &self,
        b: &mut Bindings,
        t: &Transmission,
        role: TransmissionRole,
        writer: Option<AgentRole>,
        reader: AgentRole,
    ) -> Result<(), ProvisionError> {
        b.bind(role, t.id);
        b.bind(reader, t.to);
        if let Some(writer) = writer {
            b.bind(
                writer,
                self.s.writer(t).ok_or_else(|| self.s.missing("writer"))?,
            );
        }
        Ok(())
    }

    fn on_channel(&self, id: ChannelId, what: &str) -> Result<&Transmission, ProvisionError> {
        self.s.confirmed(what, |t| t.route == Route::Channel(id))
    }

    async fn hijacked_wiki(&self, b: &mut Bindings) -> Result<(), ProvisionError> {
        use hijacked_wiki::*;
        let (wiki, talk) = (
            self.channel(ChannelKey::HijackedWiki)?,
            self.channel(ChannelKey::WikiTalk)?,
        );
        b.bind(WIKI, wiki);
        b.bind(TALK_PAGE, talk);
        b.bind(PAGE, self.seed(ChannelKey::HijackedWiki).await?);
        b.bind(TALK, self.seed(ChannelKey::WikiTalk).await?);
        let pi0 = self.agent("pi0")?;
        let confirmed = self
            .s
            .confirmed("confirmed wiki transmission from pi0", |t| {
                t.route == Route::Channel(wiki) && self.s.writer(t) == Some(pi0)
            })?;
        self.bind_tx(b, confirmed, CONFIRMED, Some(WRITER), READER)?;
        let on_talk = self.on_channel(talk, "confirmed talk-page transmission")?;
        self.bind_tx(b, on_talk, ON_TALK, Some(TALK_WRITER), TALK_READER)
    }

    fn late_confirmation(&self, b: &mut Bindings) -> Result<(), ProvisionError> {
        use late_confirmation::*;
        let late = self
            .s
            .confirmed("transmission confirmed a bucket later", |t| {
                self.s.confirmed_later(t)
            })?;
        self.bind_tx(b, late, LATE, Some(SENDER), RECEIVER)
    }

    fn impersonation(&self, b: &mut Bindings) -> Result<(), ProvisionError> {
        use impersonation::*;
        let pi0 = self.agent("pi0")?;
        let sent = self
            .s
            .confirmed("transmission from pi0", |t| self.s.writer(t) == Some(pi0))?;
        self.bind_tx(b, sent, SENT, Some(IMPERSONATOR), PEER)
    }

    fn merges(&self, b: &mut Bindings) -> Result<(), ProvisionError> {
        use merges::*;
        let (cc0, al0) = (self.agent("cc0")?, self.agent("al0")?);
        let (pi2, pi1) = (self.agent("pi2")?, self.agent("pi1")?);
        b.bind(CANONICAL, cc0);
        b.bind(ALIAS, al0);
        b.bind(CHILD, self.agent("cc0.a")?);
        b.bind(ALIAS_MERGE, self.merge(MergeKey::AtlasAlias)?);
        let self_edge = self
            .s
            .transmissions
            .iter()
            .find(|t| t.state.confirmed().is_some_and(|c| c.from() == al0) && t.to == cc0)
            .ok_or_else(|| self.s.missing("confirmed transmission from al0 to cc0"))?;
        b.bind(SELF_EDGE, self_edge.id);
        b.bind(HOLDER, pi2);
        b.bind(CHAIN_FIRST, self.agent("al2")?);
        b.bind(CHAIN_SECOND, self.agent("al3")?);
        b.bind(INNER_MERGE, self.merge(MergeKey::PiChainFirst)?);
        b.bind(OUTER_MERGE, self.merge(MergeKey::PiChainSecond)?);
        let held = self
            .s
            .confirmed("transmission from pi2", |t| self.s.writer(t) == Some(pi2))?;
        self.bind_tx(b, held, HOLDER_SENT, Some(HOLDER), HOLDER_PEER)?;
        let target = self
            .s
            .confirmed("transmission from pi1", |t| self.s.writer(t) == Some(pi1))?;
        self.bind_tx(b, target, TARGET_SENT, Some(TARGET), TARGET_PEER)?;
        b.bind(VETOED, self.agent("omp3")?);
        b.bind(VETOED_INTO, self.agent("omp1")?);
        b.bind(REVERTED_MERGE, self.merge(MergeKey::Reverted)?);
        b.bind(SPARE_A, self.agent("cc6")?);
        b.bind(SPARE_B, self.agent("cc5")?);
        Ok(())
    }

    async fn hidden_channel(&self, b: &mut Bindings) -> Result<(), ProvisionError> {
        use hidden_channel::*;
        let (cx1, al1) = (self.agent("cx1")?, self.agent("al1")?);
        let notes = self.channel(ChannelKey::SelfNotes)?;
        b.bind(OWNER, cx1);
        b.bind(OTHER_ID, al1);
        b.bind(MERGE, self.merge(MergeKey::CodexAlias)?);
        b.bind(NOTES, self.seed(ChannelKey::SelfNotes).await?);
        b.bind(SELF_NOTES, notes);
        let between = self
            .s
            .transmissions
            .iter()
            .find(|t| {
                t.route == Route::Channel(notes)
                    && t.state.confirmed().is_some_and(|c| c.from() == al1)
                    && t.to == cx1
            })
            .ok_or_else(|| {
                self.s
                    .missing("confirmed transmission from al1 to cx1 on the notes")
            })?;
        b.bind(BETWEEN, between.id);
        Ok(())
    }

    async fn suspected(&self, b: &mut Bindings) -> Result<(), ProvisionError> {
        use suspected::*;
        let s3 = self.channel(ChannelKey::S3Handoff)?;
        b.bind(S3, s3);
        b.bind(OBJECT, self.seed(ChannelKey::S3Handoff).await?);
        let tx = self.s.tx("suspected transmission on the S3 object", |t| {
            t.route == Route::Channel(s3)
                && matches!(t.state, TransmissionState::Suspected { .. })
                && self.s.crosses(t)
        })?;
        self.bind_tx(b, tx, SUSPECTED, Some(WRITER), READER)
    }

    fn declared(&self, b: &mut Bindings) -> Result<(), ProvisionError> {
        use declared::*;
        let in_use = self.channel(ChannelKey::InternalWiki)?;
        b.bind(IN_USE, in_use);
        b.bind(UNUSED, self.channel(ChannelKey::DesignDocs)?);
        let is_page = |locator: &Locator| {
            matches!(locator, Locator::Url { host, path, .. }
                if host.0 == "wiki.corp.internal" && path == "/eng/runbooks/deploy")
        };
        let on_page = self
            .s
            .confirmed("confirmed transmission on the runbook page", |t| {
                t.route == Route::Channel(in_use)
                    && self.s.resources(t).any(|r| is_page(&r.locator))
            })?;
        let page = self
            .s
            .resources(on_page)
            .find(|r| is_page(&r.locator))
            .map(|r| r.id)
            .ok_or_else(|| self.s.missing("the deploy runbook page"))?;
        b.bind(PAGE, page);
        self.bind_tx(b, on_page, ON_PAGE, Some(WRITER), READER)
    }

    fn lone_resource(&self, b: &mut Bindings) -> Result<(), ProvisionError> {
        use lone_resource::*;
        b.bind(LONER, self.agent("cc7")?);
        b.bind(
            SCRATCH,
            self.world
                .lone_resource()
                .ok_or_else(|| self.s.missing("lone resource"))?,
        );
        Ok(())
    }

    async fn promotion(&self, b: &mut Bindings) -> Result<(), ProvisionError> {
        use promotion::*;
        let notes = self.channel(ChannelKey::TeamNotes)?;
        b.bind(NOTES, notes);
        b.bind(OLD, self.channel(ChannelKey::OldTeamNotes)?);
        let retro = self.seed(ChannelKey::TeamNotes).await?;
        b.bind(RETRO, retro);
        b.bind(STANDUP, self.seed(ChannelKey::OldTeamNotes).await?);
        let on_retro = self
            .s
            .confirmed("confirmed transmission on the retro page", |t| {
                t.route == Route::Channel(notes) && self.s.touches(t, retro)
            })?;
        self.bind_tx(b, on_retro, ON_RETRO, Some(WRITER), READER)?;
        let old = self.channel(ChannelKey::OldTeamNotes)?;
        let on_standup = self.on_channel(old, "confirmed transmission on the standup page")?;
        self.bind_tx(b, on_standup, ON_STANDUP, Some(OLD_WRITER), OLD_READER)
    }

    async fn policies(&self, b: &mut Bindings) -> Result<(), ProvisionError> {
        use policies::*;
        let (pastebin, memory) = (
            self.channel(ChannelKey::Pastebin)?,
            self.channel(ChannelKey::McpMemory)?,
        );
        b.bind(PASTEBIN, pastebin);
        b.bind(MEMORY, memory);
        b.bind(PASTE, self.seed(ChannelKey::Pastebin).await?);
        b.bind(ENTITIES, self.seed(ChannelKey::McpMemory).await?);
        let pasted = self.on_channel(pastebin, "confirmed pastebin transmission")?;
        self.bind_tx(b, pasted, PASTED, Some(PASTER), PASTE_READER)?;
        let remembered = self.on_channel(memory, "confirmed memory-server transmission")?;
        self.bind_tx(b, remembered, REMEMBERED, Some(REMEMBERER), RECALLER)
    }

    fn verdicts(&self, b: &mut Bindings) -> Result<(), ProvisionError> {
        use verdicts::*;
        let agent = |name: &'static str| AgentRole::new(NAME, name);
        let judged = self.s.confirmed("confirmed false detection", |t| {
            self.s.verdicts(t.id).last() == Some(&Some(Verdict::FalseDetection))
        })?;
        self.bind_tx(
            b,
            judged,
            JUDGED_FALSE,
            Some(agent("false_writer")),
            agent("false_reader"),
        )?;
        let withdrawn = self.s.confirmed("confirmed withdrawn verdict", |t| {
            self.s
                .verdicts(t.id)
                .ends_with(&[Some(Verdict::Genuine), None])
        })?;
        self.bind_tx(
            b,
            withdrawn,
            WITHDRAWN,
            Some(agent("withdrawn_writer")),
            agent("withdrawn_reader"),
        )?;
        let unjudged = self.s.confirmed("confirmed unjudged transmission", |t| {
            self.s.verdicts(t.id).is_empty()
        })?;
        self.bind_tx(
            b,
            unjudged,
            UNJUDGED,
            Some(agent("unjudged_writer")),
            agent("unjudged_reader"),
        )?;
        type Kind = fn(&TransmissionState) -> bool;
        let states: [(TransmissionRole, &str, &'static str, &'static str, Kind); 3] = [
            (
                AWAITING,
                "awaiting",
                "awaiting_writer",
                "awaiting_reader",
                |s| matches!(s, TransmissionState::AwaitingContent { .. }),
            ),
            (
                SUSPECTED,
                "suspected",
                "suspected_writer",
                "suspected_reader",
                |s| matches!(s, TransmissionState::Suspected { .. }),
            ),
            (
                DISCARDED,
                "discarded",
                "discarded_writer",
                "discarded_reader",
                |s| matches!(s, TransmissionState::Discarded { .. }),
            ),
        ];
        for (role, what, writer, reader, kind) in states {
            let t = self.s.tx(what, |t| kind(&t.state) && self.s.crosses(t))?;
            self.bind_tx(b, t, role, Some(agent(writer)), agent(reader))?;
        }
        let detected = self.s.tx("detected transmission", |t| {
            matches!(t.state, TransmissionState::Detected)
        })?;
        self.bind_tx(b, detected, DETECTED, None, agent("detected_reader"))
    }

    fn dropped_bodies(&self, b: &mut Bindings) -> Result<(), ProvisionError> {
        use dropped_bodies::*;
        let agent = |name: &'static str| AgentRole::new(NAME, name);
        let dropped = self.world.dropped();
        for (role, side, writer, reader) in [
            (SENDER_GONE, WorldSide::Sender, "a_writer", "a_reader"),
            (READER_GONE, WorldSide::Reader, "b_writer", "b_reader"),
        ] {
            let t = self.s.confirmed("transmission with a dropped body", |t| {
                dropped.contains(&(t.id, side))
            })?;
            self.bind_tx(b, t, role, Some(agent(writer)), agent(reader))?;
        }
        let kept = self.s.confirmed("transmission with both bodies", |t| {
            dropped.iter().all(|(id, _)| *id != t.id)
        })?;
        self.bind_tx(b, kept, KEPT, Some(agent("c_writer")), agent("c_reader"))
    }

    fn routes(&self, b: &mut Bindings) -> Result<(), ProvisionError> {
        use routes::*;
        let agent = |name: &'static str| AgentRole::new(NAME, name);
        let delegated = self
            .s
            .confirmed("delegation from a parent to its child", |t| {
                t.route == Route::Delegation(DelegationDirection::ParentToChild)
            })?;
        self.bind_tx(b, delegated, DELEGATED, Some(PARENT), CHILD)?;
        let direct = self.s.confirmed("direct transmission", |t| {
            matches!(t.route, Route::Direct(_))
        })?;
        self.bind_tx(
            b,
            direct,
            DIRECT,
            Some(agent("direct_writer")),
            agent("direct_reader"),
        )?;
        let unobserved = self
            .s
            .confirmed("unobserved transmission", |t| t.route == Route::Unobserved)?;
        self.bind_tx(
            b,
            unobserved,
            UNOBSERVED,
            Some(agent("unobserved_writer")),
            agent("unobserved_reader"),
        )
    }
}

/// What `crosstalk-surface` over the memory stores seeded with the world is
/// known to get wrong or lack, in process and over HTTP alike; see
/// `docs/features/conformance.md`, "Findings". Each entry's test must fail
/// until the gap is fixed.
pub const SURFACE_FAILURES: &[ExpectedFailure] = &[];

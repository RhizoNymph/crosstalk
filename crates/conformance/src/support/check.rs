//! The scenario self-check: every fact of a provisioned scenario, observed
//! through L8 reads. Run first for each named scenario, it holds the
//! harness's bindings honest and checks what the facts imply about the
//! reads (a discovered channel's seed and listing, a merged id's redirect,
//! a confirmed transmission's canonical parties and route).
//!
//! A channel's traffic is taken to be exactly what the scenario routes
//! through it: harnesses must not route other transmissions through a
//! scenario's channels.

use std::collections::HashSet;

use crosstalk_spec::aggregates::agents::AgentLookup;
use crosstalk_spec::aggregates::node::CanonicalStateKind;
use crosstalk_spec::derived::flow::channel::confirmation::{Confirmation, Listing};
use crosstalk_spec::derived::flow::channel::{ChannelOrigin, DeclaredHistory};
use crosstalk_spec::derived::flow::transmission::Route;
use crosstalk_spec::interfaces::l8_surface::QueryApi;
use crosstalk_spec::interfaces::l8_surface::excerpt::{ExcerptWindow, Excerpted};
use crosstalk_spec::interfaces::l8_surface::summary::TransmissionStateKind;
use crosstalk_spec::observed::agent::MergeAuthor;

use super::reads::{agent_detail, channel_row, every_listed_channel, row};
use super::windows::bucket_of;
use super::{World, collect};
use crate::harness::Harness;
use crate::scenario::{
    BodySide, ChannelRole, ChannelSource, Evidence, MergeBy, Presence, Timing, Via,
};

/// Checks every fact of `w.scenario`.
pub async fn observable<H: Harness>(w: &World<'_, H>) {
    agents(w).await;
    channels(w).await;
    transmissions(w).await;
    merges(w).await;
    policies(w).await;
    verdicts(w).await;
    dropped_bodies(w).await;
    lone_resources(w).await;
    topics(w).await;
    dead_letters(w).await;
}

async fn agents<H: Harness>(w: &World<'_, H>) {
    for fact in w.scenario.agents() {
        let id = w.id(fact.role);
        let detail = agent_detail(&w.backend, &w.lead, id, w.extent).await;
        let canonical_role = w.scenario.canonical(fact.role);
        assert_eq!(
            detail.cluster.agent().id,
            w.id(canonical_role),
            "{} resolves to {canonical_role} (INV-714)",
            fact.role
        );
        let profile = detail.cluster.profile();
        let claimed: HashSet<_> = profile
            .claims()
            .entries()
            .iter()
            .map(|seen| seen.claim.family.clone())
            .collect();
        for family in fact.claims() {
            assert!(
                claimed.contains(&family),
                "{} claims {family:?}, over its cluster (INV-663): {claimed:?}",
                fact.role
            );
        }
        if canonical_role != fact.role {
            assert_eq!(
                detail.cluster.lookup(),
                AgentLookup::Redirected { from: id },
                "{} is merged",
                fact.role
            );
            continue;
        }
        assert_eq!(
            detail.cluster.lookup(),
            AgentLookup::Canonical,
            "{}",
            fact.role
        );
        if let Some(label) = fact.label {
            assert_eq!(
                profile.label().map(|l| l.as_str()),
                Some(label),
                "{}'s label",
                fact.role
            );
        }
        if let Some(parent) = fact.parent {
            assert_eq!(
                profile.parent(),
                Some(w.id(w.scenario.canonical(parent))),
                "{}'s parent",
                fact.role
            );
        }
        if fact.presence == Presence::RegisteredOnly {
            assert_eq!(profile.state_kind(), CanonicalStateKind::Registered);
            assert_eq!(profile.last_seen(), None, "{} was never seen", fact.role);
            assert!(profile.claims().is_empty(), "{} claimed nothing", fact.role);
        }
    }
}

/// What a channel's row must list as, from the scenario's traffic through
/// it (INV-857): confirmed once a confirmed cross-agent transmission went
/// through it, unconfirmed while only suspected ones did, a declaration
/// when declared (or promoted) without any, hidden when discovered and all
/// its traffic is within one agent, none when superseded.
fn expected_listing<H: Harness>(w: &World<'_, H>, role: ChannelRole) -> Option<Listing> {
    let s = &w.scenario;
    if s.superseded_by(role).is_some() {
        return None;
    }
    let crossing: Vec<_> = s
        .transmissions()
        .filter(|t| {
            t.resource()
                .and_then(|r| s.channel_at(r))
                .is_some_and(|c| s.in_force(c.role) == role)
        })
        .filter(|t| s.crosses(t))
        .collect();
    if crossing.iter().any(|t| t.state.is_confirmed()) {
        return Some(Listing::Channel(Confirmation::Confirmed));
    }
    if crossing.iter().any(|t| t.state.is_co_access()) {
        return Some(Listing::Channel(Confirmation::Unconfirmed));
    }
    let declared = matches!(
        s.channel(role).map(|c| &c.source),
        Some(ChannelSource::Declared { .. })
    ) || s.promotions().any(|p| p.channel == role);
    Some(if declared {
        Listing::Declaration
    } else {
        Listing::Hidden
    })
}

async fn channels<H: Harness>(w: &World<'_, H>) {
    for fact in w.scenario.channels() {
        let id = w.id(fact.role);
        let row = channel_row(&w.backend, &w.lead, id, None).await;
        assert_eq!(
            row.channel().id,
            id,
            "{} answers with its own record",
            fact.role
        );
        match &fact.source {
            ChannelSource::Declared { pattern } => match &row.channel().origin {
                ChannelOrigin::Declared {
                    declaration,
                    history: DeclaredHistory::BeforeTraffic(_),
                } => assert_eq!(&declaration.pattern, pattern, "{}", fact.role),
                other => panic!("{} is declared before traffic: {other:?}", fact.role),
            },
            ChannelSource::Discovered { seed } => {
                let seed_id = w.id(*seed);
                let seed_row = row
                    .seed()
                    .unwrap_or_else(|| panic!("{} has a seed", fact.role));
                assert_eq!(seed_row.id, seed_id, "{}'s seed (INV-851)", fact.role);
                if let Some(resource) = w.scenario.resource(*seed) {
                    assert_eq!(seed_row.locator, resource.locator, "{}'s seed", fact.role);
                }
                let promoted = w.scenario.promotions().find(|p| p.channel == fact.role);
                match (
                    &row.channel().origin,
                    promoted,
                    w.scenario.superseded_by(fact.role),
                ) {
                    (
                        ChannelOrigin::Declared {
                            declaration,
                            history: DeclaredHistory::Promoted { from, .. },
                        },
                        Some(promotion),
                        None,
                    ) => {
                        assert_eq!(from.resource, seed_id, "promotion keeps the seed");
                        assert_eq!(declaration.pattern, promotion.pattern);
                        assert_eq!(row.channel().policy.kind(), promotion.policy);
                    }
                    (ChannelOrigin::Superseded { seed, .. }, None, Some(by)) => {
                        assert_eq!(seed.resource, seed_id);
                        assert_eq!(
                            row.supersession().map(|s| s.into()),
                            Some(w.id(by)),
                            "{} is superseded by {by}",
                            fact.role
                        );
                    }
                    (ChannelOrigin::Discovered { seed, .. }, None, None) => {
                        assert_eq!(seed.resource, seed_id);
                    }
                    (origin, promoted, superseded) => panic!(
                        "{}: origin {origin:?}, promoted {}, superseded {superseded:?}",
                        fact.role,
                        promoted.is_some()
                    ),
                }
            }
        }
        assert_eq!(
            row.listing(),
            expected_listing(w, fact.role),
            "{}'s listing follows its traffic (INV-857)",
            fact.role
        );
    }
}

async fn transmissions<H: Harness>(w: &World<'_, H>) {
    let watermark = w
        .backend
        .watermark(&w.lead)
        .await
        .unwrap_or_else(|e| panic!("watermark: {e:?}"));
    for fact in w.scenario.transmissions() {
        let id = w.id(fact.role);
        let crosses = w.scenario.crosses(fact);
        let found = row(&w.backend, &w.lead, id).await;
        let listed = crosses || fact.state == Evidence::Detected;
        assert_eq!(
            found.is_some(),
            listed,
            "{} is listed by id exactly when it crosses or is detected (INV-1036)",
            fact.role
        );
        let Some(summary) = found else {
            continue;
        };
        assert_eq!(
            summary.to,
            w.id(w.scenario.canonical(fact.reader)),
            "{}'s reader is canonical",
            fact.role
        );
        let kind = summary.state.kind();
        match &fact.state {
            Evidence::Confirmed { writer, timing } => {
                assert!(
                    matches!(
                        kind,
                        TransmissionStateKind::Confirmed
                            | TransmissionStateKind::Classified
                            | TransmissionStateKind::Aggregated
                    ),
                    "{} is confirmed: {kind:?}",
                    fact.role
                );
                let delivery = summary
                    .state
                    .delivery()
                    .unwrap_or_else(|| panic!("{} has a delivery", fact.role));
                assert_eq!(delivery.from, w.id(w.scenario.canonical(*writer)));
                assert!(
                    delivery.confirmed_at < watermark.at(),
                    "{} settled before the watermark",
                    fact.role
                );
                if *timing == Timing::ConfirmedInLaterBucket {
                    assert!(
                        bucket_of(w.bucket, summary.opened_at).end() <= delivery.confirmed_at,
                        "{} was confirmed in a later bucket than it opened in",
                        fact.role
                    );
                }
            }
            Evidence::Suspected { .. } => assert_eq!(kind, TransmissionStateKind::Suspected),
            Evidence::AwaitingContent { .. } => {
                assert_eq!(kind, TransmissionStateKind::AwaitingContent);
            }
            Evidence::Discarded { .. } => assert_eq!(kind, TransmissionStateKind::Discarded),
            Evidence::Detected => assert_eq!(kind, TransmissionStateKind::Detected),
        }
        match &fact.route {
            Some(Via::Resource(resource)) => {
                let channel = w
                    .scenario
                    .channel_at(*resource)
                    .map(|c| w.scenario.in_force(c.role))
                    .unwrap_or_else(|| panic!("{resource} is on a channel"));
                assert_eq!(
                    summary.route,
                    Route::Channel(w.id(channel)),
                    "{}'s route resolves through supersession (INV-682)",
                    fact.role
                );
            }
            Some(Via::Delegation(direction)) => {
                assert_eq!(summary.route, Route::Delegation(*direction));
            }
            Some(Via::Direct) => assert!(matches!(summary.route, Route::Direct(_))),
            Some(Via::Unobserved) => assert_eq!(summary.route, Route::Unobserved),
            None => {}
        }
    }
}

async fn merges<H: Harness>(w: &World<'_, H>) {
    for fact in w.scenario.merges() {
        let (merge, alias, into) = (w.id(fact.role), w.id(fact.alias), w.id(fact.into));
        let detail = agent_detail(&w.backend, &w.lead, alias, w.extent).await;
        let record = detail
            .cluster
            .merges()
            .iter()
            .find(|m| m.id() == merge)
            .unwrap_or_else(|| panic!("{}'s record is in {}'s cluster", fact.role, fact.alias));
        assert_eq!(
            (record.source(), record.target()),
            (alias, into),
            "{}",
            fact.role
        );
        match (fact.by, record.by()) {
            (MergeBy::Resolver, MergeAuthor::Resolver)
            | (MergeBy::Operator, MergeAuthor::Operator(_)) => {}
            (by, author) => panic!("{} was by {by:?}, recorded {author:?}", fact.role),
        }
        assert_eq!(record.reverted().is_some(), fact.reverted, "{}", fact.role);
        if fact.reverted {
            let (a, b) = (alias.min(into), alias.max(into));
            assert!(
                detail
                    .cluster
                    .vetoes()
                    .iter()
                    .any(|v| (v.a(), v.b()) == (a, b)),
                "{}'s revert left a veto (INV-617)",
                fact.role
            );
        }
    }
}

async fn policies<H: Harness>(w: &World<'_, H>) {
    for fact in w.scenario.policies() {
        let history = w
            .backend
            .policy_history(&w.lead, w.id(fact.channel))
            .await
            .unwrap_or_else(|e| panic!("policy history: {e:?}"))
            .unwrap_or_else(|| panic!("{} has a history", fact.channel));
        let kinds: Vec<_> = history.entries().iter().map(|e| e.kind).collect();
        let decided: Vec<_> = fact.decisions.iter().copied().collect();
        assert!(
            kinds.ends_with(&decided),
            "{}'s history ends with {decided:?}: {kinds:?}",
            fact.channel
        );
    }
}

async fn verdicts<H: Harness>(w: &World<'_, H>) {
    for fact in w.scenario.verdicts() {
        let log = w
            .backend
            .verdicts(&w.lead, w.id(fact.transmission))
            .await
            .unwrap_or_else(|e| panic!("verdicts: {e:?}"))
            .unwrap_or_else(|| panic!("{} is known", fact.transmission));
        let recorded: Vec<_> = log.records().iter().map(|r| r.verdict()).collect();
        let given: Vec<_> = fact.verdicts.iter().copied().collect();
        assert!(
            recorded.ends_with(&given),
            "{}'s log ends with {given:?}: {recorded:?}",
            fact.transmission
        );
    }
}

async fn dropped_bodies<H: Harness>(w: &World<'_, H>) {
    for fact in w.scenario.dropped_bodies() {
        let evidence = w
            .backend
            .transmission_evidence(&w.lead, w.id(fact.transmission), ExcerptWindow::DEFAULT)
            .await
            .unwrap_or_else(|e| panic!("evidence: {e:?}"))
            .unwrap_or_else(|| panic!("{} is known", fact.transmission));
        assert!(!evidence.matches().is_empty(), "{}", fact.transmission);
        for m in evidence.matches() {
            let (gone, kept) = match fact.side {
                BodySide::Sender => (m.origin(), m.read()),
                BodySide::Reader => (m.read(), m.origin()),
            };
            assert!(
                matches!(gone, Excerpted::BodyDropped { .. }),
                "INV-698: {gone:?}"
            );
            assert!(matches!(kept, Excerpted::Shown(_)), "{kept:?}");
        }
    }
}

/// A resource only one agent used is on no channel (INV-853): no listed
/// or bound channel is seeded by it or lists it among its resources.
async fn lone_resources<H: Harness>(w: &World<'_, H>) {
    let lone: HashSet<_> = w
        .scenario
        .accesses()
        .map(|a| a.resource)
        .filter(|r| w.scenario.channel_at(*r).is_none())
        .map(|r| w.id(r))
        .collect();
    if lone.is_empty() {
        return;
    }
    let mut ids: Vec<_> = every_listed_channel(&w.backend, &w.lead)
        .await
        .iter()
        .map(|r| r.channel().id)
        .collect();
    ids.extend(w.scenario.channels().map(|c| w.id(c.role)));
    for id in ids {
        let row = channel_row(&w.backend, &w.lead, id, None).await;
        assert!(
            row.seed().is_none_or(|s| !lone.contains(&s.id)),
            "{id:?} is seeded by a resource one agent used"
        );
        let uses = collect(50, async |p| {
            w.backend
                .channel_resources(&w.lead, id, w.extent, &p)
                .await
                .map(|page| page.value.page)
        })
        .await;
        assert!(
            uses.iter().all(|u| !lone.contains(&u.resource().id)),
            "{id:?} holds a resource one agent used"
        );
    }
}

async fn topics<H: Harness>(w: &World<'_, H>) {
    use crosstalk_spec::aggregates::topic_history::TopicVersionStatusKind as K;
    if w.scenario.has_topic_history() {
        let history = w
            .backend
            .topic_versions(&w.lead)
            .await
            .unwrap_or_else(|e| panic!("topic versions: {e:?}"));
        let versions = history.versions();
        assert!(
            versions.iter().any(|v| !v.retention().is_retained()),
            "a version retention dropped"
        );
        let active = history.active().version();
        let older: Vec<_> = versions
            .iter()
            .filter(|v| v.retention().is_retained() && v.status().kind() == K::Superseded)
            .collect();
        assert!(!older.is_empty(), "an older retained version");
        for v in older {
            let lineage = w
                .backend
                .topic_lineage(&w.lead, v.version())
                .await
                .unwrap_or_else(|e| panic!("lineage: {e:?}"));
            assert!(lineage.is_some(), "{:?} has a successor", v.version());
        }
        assert_eq!(history.active().status().kind(), K::Active, "{active:?}");
    }
    let stale: Vec<_> = w.scenario.stale_rules().map(|r| w.id(r.rule)).collect();
    if stale.is_empty() {
        return;
    }
    let rules = collect(50, async |p| {
        w.backend
            .alert_rules(&w.lead, &Default::default(), &p)
            .await
    })
    .await;
    for id in stale {
        let rule = rules
            .iter()
            .find(|r| r.id() == id)
            .unwrap_or_else(|| panic!("rule {id:?} is listed"));
        assert!(rule.stale_reason().is_some(), "{id:?} is stale (INV-309)");
        assert_eq!(
            rule.status,
            crosstalk_spec::aggregates::alert::RuleStatus::Enabled,
            "{id:?} went stale while enabled"
        );
    }
}

async fn dead_letters<H: Harness>(w: &World<'_, H>) {
    for fact in w.scenario.dead_letters() {
        let letters = collect(50, async |p| {
            w.backend.dead_letters(&w.lead, None, &p).await
        })
        .await;
        let groups: HashSet<_> = letters.iter().map(|l| l.group.clone()).collect();
        assert!(
            groups.len() >= usize::from(fact.groups.get()),
            "dead letters in {} groups: {groups:?}",
            fact.groups
        );
    }
}

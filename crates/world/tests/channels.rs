//! The channels, read back through L5's read traits: every origin,
//! detection state and policy, the promotion and what it superseded, the
//! policy histories, and resources on one channel each.
//!
//! The channel-semantics scenarios (a channel exists only once two agents
//! communicated through it; listings computed from cross-agent traffic,
//! merges hiding a channel) are seeded as facts here; the tests that
//! assert their listing or confirmation outcomes wait for that port of the
//! spec (INV-850..869 on integration/impl).

mod support;

use crosstalk_spec::derived::flow::channel::ChannelOrigin;
use crosstalk_spec::derived::flow::channel::DeclaredHistory;
use crosstalk_spec::derived::flow::channel::detection::{
    DeclaredDetection, DetectionKind, TrafficDetection,
};
use crosstalk_spec::derived::flow::channel::policy::{Policy, PolicyAuthor, PolicyKind};
use crosstalk_spec::derived::flow::resource::ResourcePattern;
use crosstalk_spec::ids::ChannelId;
use crosstalk_spec::interfaces::l5_flow::channels::ChannelReads;
use crosstalk_spec::interfaces::l5_flow::{ChannelDirectory, ChannelLookup, ChannelRegistry};
use crosstalk_spec::interfaces::l8_surface::lists::ChannelFilter;
use crosstalk_world::ChannelKey;
use crosstalk_world::config::OPERATOR_RESEARCHER;

use support::{read, run, shared};

type Result<T = ()> = std::result::Result<T, String>;

fn id(key: ChannelKey) -> Result<ChannelId> {
    shared()
        .scenario
        .channel(key)
        .ok_or_else(|| format!("channel {key:?}"))
}

fn stored(key: ChannelKey) -> Result<crosstalk_spec::derived::flow::channel::Channel> {
    let channel = id(key)?;
    run(shared().stores.channels.channel(channel))
        .map_err(|e| format!("{e:?}"))?
        .ok_or_else(|| format!("stored channel {key:?}"))
}

fn policy_kind(policy: &Policy) -> PolicyKind {
    match policy {
        Policy::Unreviewed(_) => PolicyKind::Unreviewed,
        Policy::Sanctioned(_) => PolicyKind::Sanctioned,
        Policy::Unsanctioned(_) => PolicyKind::Unsanctioned,
    }
}

#[test]
fn every_channel_role_is_stored() -> Result {
    for key in ChannelKey::ALL {
        stored(key)?;
    }
    Ok(())
}

#[test]
fn every_channel_has_its_detection_and_policy() -> Result {
    use ChannelKey as K;
    use DetectionKind as D;
    use PolicyKind as P;
    let expected = [
        (K::InternalWiki, D::Active, P::Sanctioned),
        (K::Monorepo, D::Active, P::Sanctioned),
        (K::IssueTracker, D::Active, P::Sanctioned),
        (K::DesignDocs, D::AwaitingTraffic, P::Sanctioned),
        (K::ReleaseBucket, D::Unused, P::Sanctioned),
        (K::TeamNotes, D::Active, P::Sanctioned),
        (K::HijackedWiki, D::Active, P::Unreviewed),
        (K::WikiTalk, D::Active, P::Unreviewed),
        (K::Pastebin, D::Active, P::Unsanctioned),
        (K::McpMemory, D::Active, P::Unreviewed),
        (K::SharedFile, D::Active, P::Sanctioned),
        (K::Gist, D::Dormant, P::Unreviewed),
        // Today's spec: a co-access without content is a candidate. The
        // channel-semantics port makes it active and unconfirmed.
        (K::S3Handoff, D::Candidate, P::Unreviewed),
        (K::SelfNotes, D::Dormant, P::Unreviewed),
        // Frozen at its supersession.
        (K::OldTeamNotes, D::Active, P::Unreviewed),
        // Today's spec discovers a channel at a resource's first access.
        (K::Scratch, D::Observed, P::Unreviewed),
    ];
    for (key, detection, policy) in expected {
        let channel = stored(key)?;
        assert_eq!(channel.origin.detection_kind(), detection, "{key:?}");
        assert_eq!(policy_kind(&channel.policy), policy, "{key:?}");
    }
    Ok(())
}

#[test]
fn declared_channels_keep_their_patterns_and_config_decision() -> Result {
    for key in [
        ChannelKey::InternalWiki,
        ChannelKey::Monorepo,
        ChannelKey::IssueTracker,
        ChannelKey::DesignDocs,
        ChannelKey::ReleaseBucket,
    ] {
        let channel = stored(key)?;
        let ChannelOrigin::Declared {
            declaration,
            history,
        } = &channel.origin
        else {
            return Err(format!("{key:?} is declared"));
        };
        assert_eq!(declaration.by, PolicyAuthor::Config, "{key:?}");
        assert!(
            matches!(history, DeclaredHistory::BeforeTraffic(_)),
            "{key:?}"
        );
        let history = run(shared().stores.channels.policy_history(channel.id))
            .map_err(|e| format!("{e:?}"))?;
        assert_eq!(history.entries().len(), 1, "{key:?}");
        assert_eq!(
            history.entries().first().map(|d| d.decision.by),
            Some(PolicyAuthor::Config)
        );
    }
    let unused = stored(ChannelKey::ReleaseBucket)?;
    assert!(matches!(
        unused.origin,
        ChannelOrigin::Declared {
            history: DeclaredHistory::BeforeTraffic(DeclaredDetection::Unused { .. }),
            ..
        }
    ));
    Ok(())
}

#[test]
fn the_promotion_kept_the_id_and_superseded_the_standup_page() -> Result {
    let seeded = shared();
    let team = stored(ChannelKey::TeamNotes)?;
    let ChannelOrigin::Declared {
        declaration,
        history,
    } = &team.origin
    else {
        return Err("the promoted channel is declared".to_owned());
    };
    assert_eq!(declaration.by, PolicyAuthor::Operator(OPERATOR_RESEARCHER));
    assert!(matches!(
        &declaration.pattern,
        ResourcePattern::UrlPrefix { path_prefix, .. } if path_prefix == "/team-a"
    ));
    assert!(matches!(history, DeclaredHistory::Promoted { .. }));
    assert!(matches!(team.policy, Policy::Sanctioned(_)));

    let old = stored(ChannelKey::OldTeamNotes)?;
    let supersession = old.origin.supersession().ok_or("superseded")?;
    assert_eq!(supersession.by, team.id);
    assert_eq!(
        ChannelDirectory::canonical(&seeded.stores.channels, old.id),
        team.id
    );
    // A lookup of the standup page names the promoted channel.
    let seed = old.origin.seed().ok_or("a seed")?;
    let resource = run(async {
        let uses = seeded
            .stores
            .channels
            .resource_use(
                team.id,
                seeded
                    .world
                    .anchor()
                    .all_time()
                    .map_err(|e| format!("{e:?}"))?,
                &crosstalk_spec::paging::PageRequest {
                    size: crosstalk_spec::paging::PageSize::new(500)
                        .map_err(|e| format!("{e:?}"))?,
                    after: None,
                },
            )
            .await
            .map_err(|e| format!("{e:?}"))?;
        Ok::<_, String>(
            uses.page
                .items()
                .iter()
                .find(|u| u.resource().id == seed.resource)
                .map(|u| u.resource().locator.clone()),
        )
    })?
    .ok_or("the standup page among the promoted channel's resources")?;
    let lookup = run(seeded.stores.channels.lookup(&resource)).map_err(|e| format!("{e:?}"))?;
    assert_eq!(lookup, ChannelLookup::Known(team.id));
    Ok(())
}

#[test]
fn policy_histories_follow_the_researchers_decisions() -> Result {
    let history = |key| {
        let channel = id(key)?;
        run(shared().stores.channels.policy_history(channel))
            .map_err(|e| format!("{e:?}"))
            .map(|h| h.entries().iter().map(|d| d.kind).collect::<Vec<_>>())
    };
    assert_eq!(
        history(ChannelKey::McpMemory)?,
        [PolicyKind::Sanctioned, PolicyKind::Unreviewed],
        "sanctioned, then reset"
    );
    assert_eq!(history(ChannelKey::Pastebin)?, [PolicyKind::Unsanctioned]);
    assert_eq!(history(ChannelKey::SharedFile)?, [PolicyKind::Sanctioned]);
    assert_eq!(history(ChannelKey::TeamNotes)?, [PolicyKind::Sanctioned]);
    assert!(history(ChannelKey::HijackedWiki)?.is_empty());
    let mcp = stored(ChannelKey::McpMemory)?;
    assert!(
        matches!(mcp.policy, Policy::Unreviewed(Some(_))),
        "reset, not never reviewed"
    );
    Ok(())
}

#[test]
fn the_hijacked_wiki_is_discovered_and_holds_its_three_pages() -> Result {
    let seeded = shared();
    let wiki = stored(ChannelKey::HijackedWiki)?;
    let ChannelOrigin::Discovered { seed, detection } = &wiki.origin else {
        return Err("discovered".to_owned());
    };
    assert!(matches!(detection, TrafficDetection::Active { .. }));
    // Its seed and the two pages that joined it.
    assert_eq!(wiki.resources.len(), 2);
    assert!(!wiki.resources.contains(&seed.resource));
    let uses = run(async {
        seeded
            .stores
            .channels
            .resource_use(
                wiki.id,
                seeded
                    .world
                    .anchor()
                    .all_time()
                    .map_err(|e| format!("{e:?}"))?,
                &crosstalk_spec::paging::PageRequest {
                    size: crosstalk_spec::paging::PageSize::new(500)
                        .map_err(|e| format!("{e:?}"))?,
                    after: None,
                },
            )
            .await
            .map_err(|e| format!("{e:?}"))
    })?;
    assert_eq!(uses.page.items().len(), 3);
    for page in uses.page.items() {
        assert!(!page.writers().is_empty());
        assert!(!page.readers().is_empty());
    }
    Ok(())
}

#[test]
fn every_resource_is_on_one_channel() -> Result {
    let seeded = shared();
    let channels = run(read::channels(
        seeded,
        ChannelFilter {
            origin: crosstalk_spec::interfaces::l8_surface::lists::OriginFilter::WithSuperseded(
                Vec::new(),
            ),
            ..ChannelFilter::default()
        },
    ))?;
    let mut seen = std::collections::BTreeSet::new();
    for channel in &channels {
        let seed = channel.origin.seed().map(|s| s.resource);
        for resource in channel.resources.iter().copied().chain(seed) {
            assert!(seen.insert(resource), "{resource:?} on two channels");
        }
    }
    Ok(())
}

// --- What waits for the channel-semantics port (INV-850..869) ---

#[test]
#[ignore = "waits for the channel-semantics port"]
fn the_scratch_entry_one_agent_uses_is_no_channel() -> Result {
    let seeded = shared();
    let listed = run(read::channels(seeded, ChannelFilter::default()))?;
    let scratch = id(ChannelKey::Scratch)?;
    assert!(
        listed.iter().all(|c| c.id != scratch),
        "a resource only one agent uses is not a channel"
    );
    Ok(())
}

#[test]
#[ignore = "waits for the channel-semantics port"]
fn the_unconfirmed_s3_handoff_is_active() -> Result {
    let s3 = stored(ChannelKey::S3Handoff)?;
    assert_eq!(
        s3.origin.detection_kind(),
        DetectionKind::Active,
        "suspected cross-agent traffic makes it active and unconfirmed"
    );
    Ok(())
}

#[test]
#[ignore = "waits for the channel-semantics port"]
fn the_channel_hidden_by_a_merge_is_not_listed() -> Result {
    let seeded = shared();
    let listed = run(read::channels(seeded, ChannelFilter::default()))?;
    let hidden = id(ChannelKey::SelfNotes)?;
    assert!(
        listed.iter().all(|c| c.id != hidden),
        "every transmission through it is within one merged agent"
    );
    Ok(())
}

#[test]
#[ignore = "waits for the channel-semantics port"]
fn discovered_channels_are_seeded_by_a_cross_agent_transmission() -> Result {
    // The port replaces `Seed::first_access` with `first_transmission` and
    // creates a discovered channel at its first cross-agent transmission:
    // the scratch entry, with none, then has no channel at all.
    let scratch = stored(ChannelKey::Scratch);
    assert!(scratch.is_err(), "no channel for the scratch entry");
    Ok(())
}

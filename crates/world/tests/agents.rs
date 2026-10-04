//! The cast, read back through L3's read traits: every harness family and
//! state, impersonation, labels, sub-agents, registered agents, and the
//! merge history with its repoint, its revert and the veto.

mod support;

use std::collections::BTreeSet;

use crosstalk_spec::aggregates::agents::AgentLookup;
use crosstalk_spec::interfaces::l3_reconstruction::agents::{ActivityStore, AgentReads};
use crosstalk_spec::interfaces::l3_reconstruction::{AgentDirectory, ClaimStore};
use crosstalk_spec::observed::agent::{ActiveAgentState, MergeAuthor};
use crosstalk_spec::observed::client::HarnessFamily;
use crosstalk_world::MergeKey;

use support::{read, run, shared};

type Result<T = ()> = std::result::Result<T, String>;

fn agent(key: &str) -> Result<crosstalk_spec::ids::AgentId> {
    shared()
        .scenario
        .agent(key)
        .ok_or_else(|| format!("agent {key}"))
}

#[test]
fn forty_canonical_agents_in_every_state_with_labels_and_sub_agents() -> Result {
    let seeded = shared();
    let agents = run(read::agents(seeded))?;
    assert_eq!(agents.len(), 40, "canonical agents only");
    for kind in ["Registered", "Provisional", "Established"] {
        assert!(
            agents
                .iter()
                .any(|a| format!("{:?}", a.state()).starts_with(kind)),
            "{kind}"
        );
    }
    let labels: BTreeSet<&str> = agents
        .iter()
        .filter_map(|a| a.label().map(|l| l.as_str()))
        .collect();
    for label in [
        "atlas-lead",
        "infra-bot",
        "notes-keeper",
        "codex-reviewer",
        "pi-scraper",
        "omp-orchestrator",
        "batch-evals",
        "release-bot",
        "nightly-evals",
    ] {
        assert!(labels.contains(label), "{label}");
    }
    assert!(agents.iter().filter(|a| a.parent().is_some()).count() >= 10);
    Ok(())
}

#[test]
fn pi_and_oh_my_pi_agents_also_claim_claude_code() -> Result {
    let seeded = shared();
    run(async {
        let mut families = BTreeSet::new();
        for key in ["pi0", "pi1", "omp0", "omp2"] {
            let claims = seeded
                .stores
                .agents
                .claims(agent(key)?)
                .await
                .map_err(|e| format!("{e:?}"))?;
            let seen: Vec<HarnessFamily> = claims
                .entries()
                .iter()
                .map(|c| c.claim.family.clone())
                .collect();
            assert!(seen.contains(&HarnessFamily::ClaudeCode), "{key}: {seen:?}");
            assert!(
                seen.iter()
                    .any(|f| matches!(f, HarnessFamily::Pi | HarnessFamily::OhMyPi)),
                "{key}: {seen:?}"
            );
        }
        for (_, id) in seeded.scenario.agents() {
            let claims = seeded
                .stores
                .agents
                .claims(id)
                .await
                .map_err(|e| format!("{e:?}"))?;
            families.extend(
                claims
                    .entries()
                    .iter()
                    .map(|c| format!("{:?}", c.claim.family)),
            );
        }
        assert_eq!(families.len(), 5, "every family claimed: {families:?}");
        Ok(())
    })
}

#[test]
fn registered_agents_have_no_claims_and_were_never_seen() -> Result {
    let seeded = shared();
    run(async {
        assert_eq!(seeded.scenario.registered().len(), 3);
        for id in seeded.scenario.registered() {
            let claims = seeded
                .stores
                .agents
                .claims(*id)
                .await
                .map_err(|e| format!("{e:?}"))?;
            assert!(claims.is_empty());
            let last = seeded
                .stores
                .agents
                .last_seen(*id)
                .await
                .map_err(|e| format!("{e:?}"))?;
            assert_eq!(last, None);
            let cluster = seeded
                .stores
                .agents
                .cluster(*id)
                .await
                .map_err(|e| format!("{e:?}"))?
                .ok_or("cluster")?;
            assert!(matches!(
                cluster.profile().state(),
                ActiveAgentState::Registered { .. }
            ));
        }
        Ok(())
    })
}

#[test]
fn the_merge_history_has_resolver_and_operator_merges_a_repoint_a_revert_and_a_veto() -> Result {
    let seeded = shared();
    let directory = &seeded.stores.agents;
    // Every alias resolves to its scenario's canonical agent.
    for (alias, into) in [
        ("al0", "cc0"),
        ("al1", "cx1"),
        ("al2", "pi2"),
        ("al3", "pi2"),
    ] {
        assert_eq!(
            AgentDirectory::canonical(directory, agent(alias)?),
            agent(into)?,
            "{alias}"
        );
    }
    assert_eq!(
        AgentDirectory::canonical(directory, agent("omp3")?),
        agent("omp3")?,
        "a reverted merge's source is canonical again"
    );
    run(async {
        let pi2 = directory
            .cluster(agent("al2")?)
            .await
            .map_err(|e| format!("{e:?}"))?
            .ok_or("cluster")?;
        assert_eq!(
            pi2.lookup(),
            AgentLookup::Redirected {
                from: agent("al2")?
            }
        );
        let ids: BTreeSet<_> = pi2.alias_ids().iter().copied().collect();
        assert_eq!(ids, BTreeSet::from([agent("al2")?, agent("al3")?]));
        let repointing = pi2
            .merges()
            .iter()
            .find(|m| !m.repointed().is_empty())
            .ok_or("a repointing merge")?;
        assert_eq!(repointing.repointed(), [agent("al2")?]);
        assert!(matches!(repointing.by(), MergeAuthor::Operator(_)));

        let atlas = directory
            .cluster(agent("cc0")?)
            .await
            .map_err(|e| format!("{e:?}"))?
            .ok_or("cluster")?;
        let alias = atlas.merge_of(agent("al0")?).ok_or("al0's merge")?;
        assert_eq!(alias.by(), MergeAuthor::Resolver);
        assert_eq!(
            Some(alias.id()),
            seeded.scenario.merge(MergeKey::AtlasAlias)
        );

        let omp1 = directory
            .cluster(agent("omp1")?)
            .await
            .map_err(|e| format!("{e:?}"))?
            .ok_or("cluster")?;
        let reverted = omp1
            .merges()
            .iter()
            .find(|m| m.reverted().is_some())
            .ok_or("the reverted merge")?;
        assert_eq!(
            Some(reverted.id()),
            seeded.scenario.merge(MergeKey::Reverted)
        );
        let (a, b) = (
            agent("omp1")?.min(agent("omp3")?),
            agent("omp1")?.max(agent("omp3")?),
        );
        assert!(omp1.vetoes().iter().any(|v| (v.a(), v.b()) == (a, b)));
        Ok(())
    })
}

#[test]
fn every_merge_has_its_record() {
    let seeded = shared();
    for key in MergeKey::ALL {
        assert!(seeded.scenario.merge(key).is_some(), "{key:?}");
    }
}

#[test]
fn traffic_created_agents_were_seen_and_the_atlas_cluster_unions_its_alias() -> Result {
    let seeded = shared();
    run(async {
        let profiles = read::agents(seeded).await?;
        for profile in &profiles {
            let registered = matches!(profile.state(), ActiveAgentState::Registered { .. });
            assert_eq!(
                profile.last_seen().is_none(),
                registered,
                "{:?}",
                profile.id()
            );
        }
        let cc0 = agent("cc0")?;
        let atlas = profiles
            .iter()
            .find(|p| p.id() == cc0)
            .ok_or("atlas-lead")?;
        assert!(atlas.aliases().contains(&agent("al0")?));
        Ok(())
    })
}

#[test]
fn the_same_seed_gives_the_same_world_and_another_seed_another() -> Result {
    let seeded = shared();
    let (again, other) = run(async {
        let again = support::seed(support::SEED)
            .await
            .map_err(|e| e.to_string())?;
        let other = support::seed(support::SEED + 1)
            .await
            .map_err(|e| e.to_string())?;
        Ok::<_, String>((again, other))
    })?;
    assert_eq!(again.scenario, seeded.scenario);
    run(async {
        assert_eq!(read::agents(seeded).await?, read::agents(&again).await?);
        assert_eq!(read::alerts(seeded).await?, read::alerts(&again).await?);
        assert_eq!(read::rules(seeded).await?, read::rules(&again).await?);
        assert_eq!(read::audit(seeded).await?, read::audit(&again).await?);
        assert_eq!(read::jobs(seeded).await?, read::jobs(&again).await?);
        let every = || crosstalk_spec::interfaces::l8_surface::lists::ChannelFilter {
            origin: crosstalk_spec::interfaces::l8_surface::lists::OriginFilter::WithSuperseded(
                Vec::new(),
            ),
            ..Default::default()
        };
        assert_eq!(
            read::channels(seeded, every()).await?,
            read::channels(&again, every()).await?
        );
        Ok::<_, String>(())
    })?;
    assert_ne!(other.scenario.agent("cc0"), seeded.scenario.agent("cc0"));
    Ok(())
}

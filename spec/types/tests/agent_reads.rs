//! Agent read models: profiles, clusters, the agents filter, batch names,
//! id text, and merges of a cluster into itself.

use crate::aggregates::agents::filter::{AgentFilter, AgentText};
use crate::aggregates::agents::{
    AgentCluster, AgentClusterParts, AgentLookup, AgentName, AgentProfile, AgentProfileParts,
    InvalidCluster, InvalidProfile,
};
use crate::aggregates::node::CanonicalStateKind;
use crate::batch::{IdBatch, TooManyIds};
use crate::ids::{AgentId, MergeId, OperatorId, PromptHash};
use crate::interfaces::l3_reconstruction::ResolveError;
use crate::interfaces::l3_reconstruction::agents::AgentReadError;
use crate::interfaces::l8_surface::{
    ActionError, ConflictKind, InputError, OperatorAction, Permission, QueryError,
};
use crate::observed::agent::{
    ActiveAgentState, Agent, AgentLabel, AgentState, ClaimSet, IdentityEvidence, MergeAuthor,
    MergeConflict, MergeRecord, MergeRequest, MergeVeto, MergedInto, SelfMerge,
};
use crate::observed::client::{HarnessClaim, HarnessFamily};
use crate::support::{Blake3, NonEmpty};
use crate::tests::fixtures::{agent, at};
use crate::tests::operators::caller;

/// The ULID spec's example id, `01ARZ3NDEKTSV4RRFFQ69G5FAV`.
const EXAMPLE: u128 = 0x0156_3e3a_b5d3_d676_4c61_efb9_9302_bd5b;

fn operator() -> OperatorId {
    OperatorId::from_ulid(7)
}

fn label(text: &str) -> AgentLabel {
    AgentLabel::new(text).expect("valid label")
}

fn text(raw: &str) -> AgentText {
    AgentText::new(raw).expect("valid filter text")
}

fn provisional() -> ActiveAgentState {
    ActiveAgentState::Provisional { first_seen: at(1) }
}

fn record(id: AgentId, state: AgentState, label: Option<AgentLabel>) -> Agent {
    Agent {
        id,
        evidence: NonEmpty::new(IdentityEvidence::PromptFingerprint(
            PromptHash::from_digest(Blake3::from_bytes([3; 32])),
        )),
        parent: None,
        state,
        label,
    }
}

fn merged_into(into: AgentId, merge: u128) -> AgentState {
    AgentState::Merged(MergedInto {
        merge: MergeId::from_ulid(merge),
        into,
        prior: provisional(),
        repointed_by: Vec::new(),
    })
}

fn parts(id: AgentId) -> AgentProfileParts {
    AgentProfileParts {
        id,
        label: None,
        state: provisional(),
        parent: None,
        aliases: Vec::new(),
        claims: ClaimSet::default(),
        last_seen: Some(at(5)),
    }
}

fn profile(parts: AgentProfileParts) -> AgentProfile {
    AgentProfile::new(parts).expect("valid profile")
}

fn claims(families: &[HarnessFamily]) -> ClaimSet {
    let mut set = ClaimSet::default();
    for (n, family) in families.iter().enumerate() {
        set.observe(
            HarnessClaim {
                family: family.clone(),
                version: None,
                user_agent: format!("agent/{n}"),
            },
            at(n as u64),
        );
    }
    set
}

fn request(from: u128, into: u128) -> MergeRequest {
    MergeRequest::new(agent(from), agent(into), MergeAuthor::Operator(operator()))
        .expect("different agents")
}

fn merge_record(id: u128, from: u128, into: u128, at_micros: u64) -> MergeRecord {
    MergeRecord::new(
        MergeId::from_ulid(id),
        request(from, into),
        at(at_micros),
        Vec::new(),
    )
}

fn veto(a: u128, b: u128, at_micros: u64) -> MergeVeto {
    MergeVeto::new(agent(a), agent(b), operator(), at(at_micros)).expect("different agents")
}

// ── Id text and batches ──────────────────────────────────────────────────────

#[test]
fn ulid_text_is_crockford_base32() {
    assert_eq!(
        AgentId::from_ulid(EXAMPLE).ulid_text(),
        "01ARZ3NDEKTSV4RRFFQ69G5FAV"
    );
    assert_eq!(agent(0).ulid_text(), "0".repeat(26));
    assert_eq!(
        AgentId::from_ulid(u128::MAX).ulid_text(),
        format!("7{}", "Z".repeat(25))
    );
    assert_eq!(agent(32).ulid_text(), format!("{}10", "0".repeat(24)));
}

#[test]
fn id_batches_are_distinct_and_ascending() {
    let batch = IdBatch::new([agent(3), agent(1), agent(3), agent(2)]).expect("small batch");
    assert_eq!(batch.ids(), &[agent(1), agent(2), agent(3)]);
    assert_eq!(batch.len(), 3);
    assert!(IdBatch::<AgentId>::new([]).expect("empty batch").is_empty());
}

#[test]
fn id_batches_cap_distinct_ids() {
    let max = IdBatch::<AgentId>::MAX;
    let full = IdBatch::new((0..max as u128).map(agent)).expect("exactly the cap");
    assert_eq!(full.len(), max);
    let repeated = (0..max as u128).chain(0..max as u128).map(agent);
    assert_eq!(IdBatch::new(repeated).map(|b| b.len()), Ok(max));
    assert_eq!(
        IdBatch::new((0..=max as u128).map(agent)),
        Err(TooManyIds { max, got: max + 1 })
    );
}

#[test]
fn too_many_ids_is_invalid_input() {
    assert_eq!(
        QueryError::from(TooManyIds {
            max: 1000,
            got: 1200
        }),
        QueryError::InvalidInput(InputError::TooManyIds {
            max: 1000,
            got: 1200
        })
    );
}

// ── Profiles ─────────────────────────────────────────────────────────────────

#[test]
fn profile_sorts_aliases() {
    let built = profile(AgentProfileParts {
        aliases: vec![agent(4), agent(2), agent(3)],
        parent: Some(agent(9)),
        ..parts(agent(1))
    });
    assert_eq!(built.aliases(), &[agent(2), agent(3), agent(4)]);
    assert_eq!(built.parent(), Some(agent(9)));
    assert_eq!(built.state_kind(), CanonicalStateKind::Provisional);
}

#[test]
fn profile_rejects_a_parent_in_its_cluster() {
    assert_eq!(
        AgentProfile::new(AgentProfileParts {
            parent: Some(agent(1)),
            ..parts(agent(1))
        }),
        Err(InvalidProfile::SelfParent)
    );
    assert_eq!(
        AgentProfile::new(AgentProfileParts {
            parent: Some(agent(2)),
            aliases: vec![agent(2)],
            ..parts(agent(1))
        }),
        Err(InvalidProfile::ParentIsAlias(agent(2)))
    );
}

#[test]
fn profile_rejects_bad_aliases() {
    assert_eq!(
        AgentProfile::new(AgentProfileParts {
            aliases: vec![agent(2), agent(1)],
            ..parts(agent(1))
        }),
        Err(InvalidProfile::SelfAlias)
    );
    assert_eq!(
        AgentProfile::new(AgentProfileParts {
            aliases: vec![agent(2), agent(3), agent(2)],
            ..parts(agent(1))
        }),
        Err(InvalidProfile::DuplicateAlias(agent(2)))
    );
}

#[test]
fn only_a_registered_agent_may_be_unseen() {
    for state in [
        ActiveAgentState::Provisional { first_seen: at(1) },
        ActiveAgentState::Established { since: at(1) },
    ] {
        assert_eq!(
            AgentProfile::new(AgentProfileParts {
                state,
                last_seen: None,
                ..parts(agent(1))
            }),
            Err(InvalidProfile::NeverSeen)
        );
    }
    let registered = profile(AgentProfileParts {
        state: ActiveAgentState::Registered { at: at(1) },
        last_seen: None,
        ..parts(agent(1))
    });
    assert_eq!(registered.last_seen(), None);
    assert_eq!(registered.state_kind(), CanonicalStateKind::Registered);
}

// ── Clusters ─────────────────────────────────────────────────────────────────

/// Agent 1 with aliases 2 and 3, child 9, two merges and a veto.
fn cluster_parts() -> AgentClusterParts {
    AgentClusterParts {
        profile: profile(AgentProfileParts {
            label: Some(label("lead")),
            aliases: vec![agent(2), agent(3)],
            ..parts(agent(1))
        }),
        agent: record(
            agent(1),
            AgentState::Provisional { first_seen: at(1) },
            Some(label("lead")),
        ),
        aliases: vec![
            record(agent(3), merged_into(agent(1), 21), None),
            record(agent(2), merged_into(agent(1), 20), Some(label("old"))),
        ],
        children: vec![agent(9)],
        merges: vec![merge_record(21, 3, 1, 30), merge_record(20, 2, 1, 10)],
        vetoes: vec![veto(1, 5, 40)],
        lookup: AgentLookup::Redirected { from: agent(2) },
    }
}

#[test]
fn cluster_orders_its_lists() {
    let cluster = AgentCluster::new(cluster_parts()).expect("valid cluster");
    let alias_ids: Vec<AgentId> = cluster.aliases().iter().map(|a| a.id).collect();
    assert_eq!(alias_ids, vec![agent(2), agent(3)]);
    assert_eq!(cluster.alias_ids(), &[agent(2), agent(3)]);
    let merge_ids: Vec<MergeId> = cluster.merges().iter().map(MergeRecord::id).collect();
    assert_eq!(
        merge_ids,
        vec![MergeId::from_ulid(20), MergeId::from_ulid(21)]
    );
    assert_eq!(cluster.lookup(), AgentLookup::Redirected { from: agent(2) });
    assert!(cluster.contains(agent(3)));
    assert!(!cluster.contains(agent(9)));
}

#[test]
fn cluster_agent_agrees_with_profile() {
    let relabelled = AgentClusterParts {
        agent: record(
            agent(1),
            AgentState::Provisional { first_seen: at(1) },
            Some(label("other")),
        ),
        ..cluster_parts()
    };
    assert_eq!(
        AgentCluster::new(relabelled),
        Err(InvalidCluster::AgentMismatch)
    );
    let restated = AgentClusterParts {
        agent: record(
            agent(1),
            AgentState::Established { since: at(1) },
            Some(label("lead")),
        ),
        ..cluster_parts()
    };
    assert_eq!(
        AgentCluster::new(restated),
        Err(InvalidCluster::AgentMismatch)
    );
}

#[test]
fn cluster_aliases_are_the_profiles_and_merged_into_it() {
    let missing = AgentClusterParts {
        aliases: vec![record(agent(2), merged_into(agent(1), 20), None)],
        ..cluster_parts()
    };
    assert_eq!(
        AgentCluster::new(missing),
        Err(InvalidCluster::AliasMismatch)
    );
    let elsewhere = AgentClusterParts {
        aliases: vec![
            record(agent(2), merged_into(agent(1), 20), None),
            record(agent(3), merged_into(agent(8), 21), None),
        ],
        ..cluster_parts()
    };
    assert_eq!(
        AgentCluster::new(elsewhere),
        Err(InvalidCluster::AliasNotMerged(agent(3)))
    );
}

#[test]
fn cluster_redirects_only_from_an_alias() {
    let stranger = AgentClusterParts {
        lookup: AgentLookup::Redirected { from: agent(9) },
        ..cluster_parts()
    };
    assert_eq!(
        AgentCluster::new(stranger),
        Err(InvalidCluster::UnknownRedirect(agent(9)))
    );
    let canonical = AgentClusterParts {
        lookup: AgentLookup::Canonical,
        ..cluster_parts()
    };
    assert!(AgentCluster::new(canonical).is_ok());
}

#[test]
fn cluster_children_are_outside_it_and_distinct() {
    for inside in [agent(1), agent(3)] {
        let parts = AgentClusterParts {
            children: vec![agent(9), inside],
            ..cluster_parts()
        };
        assert_eq!(
            AgentCluster::new(parts),
            Err(InvalidCluster::ChildInCluster(inside))
        );
    }
    let repeated = AgentClusterParts {
        children: vec![agent(9), agent(8), agent(9)],
        ..cluster_parts()
    };
    assert_eq!(
        AgentCluster::new(repeated),
        Err(InvalidCluster::DuplicateChild(agent(9)))
    );
}

#[test]
fn cluster_merges_name_it_once_each() {
    let unrelated = AgentClusterParts {
        merges: vec![merge_record(20, 2, 1, 10), merge_record(22, 6, 7, 20)],
        ..cluster_parts()
    };
    assert_eq!(
        AgentCluster::new(unrelated),
        Err(InvalidCluster::UnrelatedMerge(MergeId::from_ulid(22)))
    );
    let twice = AgentClusterParts {
        merges: vec![merge_record(20, 2, 1, 10), merge_record(20, 2, 1, 10)],
        ..cluster_parts()
    };
    assert_eq!(
        AgentCluster::new(twice),
        Err(InvalidCluster::DuplicateMerge(MergeId::from_ulid(20)))
    );
    // A reverted merge of the agent into another names it as the source,
    // and a merge that repointed an alias names it as repointed.
    let mut reverted = merge_record(23, 1, 6, 5);
    reverted
        .revert(crate::observed::agent::Reversal {
            by: operator(),
            at: at(6),
            restored: Vec::new(),
        })
        .expect("first revert");
    let repointing = MergeRecord::new(MergeId::from_ulid(24), request(7, 1), at(7), vec![agent(3)]);
    let history = AgentClusterParts {
        merges: vec![reverted, repointing, merge_record(20, 2, 1, 10)],
        ..cluster_parts()
    };
    let cluster = AgentCluster::new(history).expect("every record names the cluster");
    assert!(cluster.merges()[0].reverted().is_some());
}

#[test]
fn cluster_vetoes_touch_it_once_each() {
    let unrelated = AgentClusterParts {
        vetoes: vec![veto(5, 6, 1)],
        ..cluster_parts()
    };
    assert_eq!(
        AgentCluster::new(unrelated),
        Err(InvalidCluster::UnrelatedVeto {
            a: agent(5),
            b: agent(6)
        })
    );
    let twice = AgentClusterParts {
        vetoes: vec![veto(1, 5, 1), veto(5, 1, 2)],
        ..cluster_parts()
    };
    assert_eq!(
        AgentCluster::new(twice),
        Err(InvalidCluster::DuplicateVeto {
            a: agent(1),
            b: agent(5)
        })
    );
}

#[test]
fn names_come_from_canonical_agents_only() {
    let live = record(
        agent(1),
        AgentState::Established { since: at(1) },
        Some(label("lead")),
    );
    assert_eq!(
        AgentName::of(&live),
        Some(AgentName {
            id: agent(1),
            label: Some(label("lead")),
        })
    );
    let alias = record(agent(2), merged_into(agent(1), 20), Some(label("old")));
    assert_eq!(AgentName::of(&alias), None);
}

// ── The agents filter ────────────────────────────────────────────────────────

/// No merges.
fn identity(id: AgentId) -> AgentId {
    id
}

#[test]
fn agent_filter_matches_claimed_families_over_the_union() {
    let claimed = profile(AgentProfileParts {
        claims: claims(&[HarnessFamily::Pi, HarnessFamily::ClaudeCode]),
        ..parts(agent(1))
    });
    let pi = AgentFilter {
        claimed: vec![HarnessFamily::Pi],
        ..AgentFilter::default()
    };
    let codex = AgentFilter {
        claimed: vec![HarnessFamily::Codex],
        ..AgentFilter::default()
    };
    assert!(pi.matches(&claimed, identity));
    assert!(!codex.matches(&claimed, identity));
    let unclaimed = profile(parts(agent(2)));
    assert!(!pi.matches(&unclaimed, identity));
    assert!(AgentFilter::default().matches(&unclaimed, identity));
}

#[test]
fn agent_filter_text_is_a_case_insensitive_label_substring() {
    let labelled = profile(AgentProfileParts {
        label: Some(label("Étoile Scraper")),
        ..parts(agent(1))
    });
    for hit in ["scrap", "ÉTOILE", "étoile s", "LE SC"] {
        assert!(
            AgentFilter::text_matches(&text(hit), &labelled),
            "{hit} is in the label"
        );
    }
    assert!(!AgentFilter::text_matches(&text("scrapers"), &labelled));
    let german = profile(AgentProfileParts {
        label: Some(label("Straße")),
        ..parts(agent(1))
    });
    assert!(!AgentFilter::text_matches(&text("strasse"), &german));
    assert!(AgentFilter::text_matches(&text("STRAßE"), &german));
    let unlabelled = profile(parts(agent(1)));
    assert!(!AgentFilter::text_matches(&text("scrap"), &unlabelled));
}

#[test]
fn agent_filter_text_is_an_id_prefix_of_the_agent_or_an_alias() {
    let example = AgentId::from_ulid(EXAMPLE);
    let canonical = profile(parts(example));
    for hit in ["01ARZ3", "01arz3nd", "01ARZ3NDEKTSV4RRFFQ69G5FAV"] {
        assert!(AgentFilter::text_matches(&text(hit), &canonical), "{hit}");
    }
    // Inside the id but not at its start.
    assert!(!AgentFilter::text_matches(&text("RZ3ND"), &canonical));
    // Crockford's letter aliases are not applied.
    assert!(!AgentFilter::text_matches(&text("O1ARZ3"), &canonical));
    let merged_away = profile(AgentProfileParts {
        aliases: vec![example],
        ..parts(agent(1))
    });
    assert!(AgentFilter::text_matches(&text("01arz3"), &merged_away));
}

#[test]
fn agent_filter_parents_resolve_through_merges() {
    let child = profile(AgentProfileParts {
        parent: Some(agent(5)),
        ..parts(agent(1))
    });
    let top = profile(parts(agent(2)));
    let under_alias = AgentFilter {
        parents: vec![agent(6)],
        ..AgentFilter::default()
    };
    // Agent 6 was merged into agent 5.
    let merges = |id: AgentId| if id == agent(6) { agent(5) } else { id };
    assert!(under_alias.matches(&child, merges));
    assert!(!under_alias.matches(&child, identity));
    assert!(!under_alias.matches(&top, merges));
}

#[test]
fn agent_filter_fields_combine_with_and() {
    let row = profile(AgentProfileParts {
        label: Some(label("planner")),
        parent: Some(agent(5)),
        claims: claims(&[HarnessFamily::Codex]),
        ..parts(agent(1))
    });
    let all = AgentFilter {
        states: vec![CanonicalStateKind::Provisional],
        claimed: vec![HarnessFamily::Codex],
        text: Some(text("plan")),
        parents: vec![agent(5)],
    };
    assert!(all.matches(&row, identity));
    let wrong_state = AgentFilter {
        states: vec![CanonicalStateKind::Established],
        ..all.clone()
    };
    let wrong_text = AgentFilter {
        text: Some(text("scraper")),
        ..all.clone()
    };
    let wrong_parent = AgentFilter {
        parents: vec![agent(6)],
        ..all.clone()
    };
    let wrong_claim = AgentFilter {
        claimed: vec![HarnessFamily::OhMyPi],
        ..all
    };
    for filter in [wrong_state, wrong_text, wrong_parent, wrong_claim] {
        assert!(!filter.matches(&row, identity), "{filter:?}");
    }
}

// ── Merging a cluster into itself ────────────────────────────────────────────

fn active() -> AgentState {
    AgentState::Provisional { first_seen: at(1) }
}

#[test]
fn merging_two_ids_of_one_cluster_is_into_self() {
    // 2 is merged into 1: either direction names one cluster.
    assert_eq!(
        request(2, 1).conflict(&merged_into(agent(1), 20), &active()),
        Some(MergeConflict::IntoSelf {
            canonical: agent(1)
        })
    );
    assert_eq!(
        request(1, 2).conflict(&active(), &merged_into(agent(1), 20)),
        Some(MergeConflict::IntoSelf {
            canonical: agent(1)
        })
    );
    // 2 and 3 are both merged into 1.
    assert_eq!(
        request(2, 3).conflict(&merged_into(agent(1), 20), &merged_into(agent(1), 21)),
        Some(MergeConflict::IntoSelf {
            canonical: agent(1)
        })
    );
}

#[test]
fn merging_a_merged_agent_of_another_cluster_names_it() {
    assert_eq!(
        request(2, 3).conflict(&merged_into(agent(1), 20), &active()),
        Some(MergeConflict::Merged {
            agent: agent(2),
            into: agent(1)
        })
    );
    assert_eq!(
        request(3, 2).conflict(&active(), &merged_into(agent(1), 20)),
        Some(MergeConflict::Merged {
            agent: agent(2),
            into: agent(1)
        })
    );
    // Both merged, into different agents: the source is named first.
    assert_eq!(
        request(2, 3).conflict(&merged_into(agent(1), 20), &merged_into(agent(4), 21)),
        Some(MergeConflict::Merged {
            agent: agent(2),
            into: agent(1)
        })
    );
    assert_eq!(request(2, 3).conflict(&active(), &active()), None);
}

#[test]
fn resolve_error_of_conflict_names_the_request() {
    let request = request(2, 3);
    assert_eq!(
        ResolveError::of_conflict(
            &request,
            MergeConflict::IntoSelf {
                canonical: agent(1)
            }
        ),
        ResolveError::MergeIntoSelf {
            from: agent(2),
            into: agent(3),
            canonical: agent(1),
        }
    );
    assert_eq!(
        ResolveError::of_conflict(
            &request,
            MergeConflict::Merged {
                agent: agent(2),
                into: agent(1)
            }
        ),
        ResolveError::AgentMerged {
            agent: agent(2),
            into: agent(1)
        }
    );
}

#[test]
fn resolve_errors_map_to_action_errors() {
    let merge = MergeId::from_ulid(4);
    let cases = [
        (
            ResolveError::Store {
                reason: "reset".into(),
            },
            ActionError::Store {
                reason: "reset".into(),
            },
        ),
        (ResolveError::UnknownAgent(agent(1)), ActionError::NotFound),
        (ResolveError::UnknownMerge(merge), ActionError::NotFound),
        (
            ResolveError::AgentMerged {
                agent: agent(2),
                into: agent(1),
            },
            ActionError::Conflict(ConflictKind::AgentMerged {
                agent: agent(2),
                into: agent(1),
            }),
        ),
        (
            ResolveError::MergeIntoSelf {
                from: agent(2),
                into: agent(3),
                canonical: agent(1),
            },
            ActionError::Conflict(ConflictKind::MergeIntoSelf {
                from: agent(2),
                into: agent(3),
                canonical: agent(1),
            }),
        ),
        (
            ResolveError::MergeAlreadyReverted(merge),
            ActionError::Conflict(ConflictKind::MergeAlreadyReverted { merge }),
        ),
    ];
    for (error, expected) in cases {
        assert_eq!(ActionError::from(error.clone()), expected, "{error:?}");
    }
    assert!(matches!(
        ActionError::from(ResolveError::Vetoed(veto(1, 2, 3))),
        ActionError::Store { .. }
    ));
}

#[test]
fn a_self_merge_is_invalid_input_before_act() {
    let governor = caller(1, &[Permission::Govern]);
    assert_eq!(
        OperatorAction::merge_agents(&governor, agent(2), agent(2)),
        Err(SelfMerge)
    );
    assert_eq!(
        ActionError::from(SelfMerge),
        ActionError::InvalidInput(InputError::SelfMerge)
    );
    let built = OperatorAction::merge_agents(&governor, agent(2), agent(3)).expect("two agents");
    let OperatorAction::MergeAgents(request) = built else {
        panic!("merge_agents builds MergeAgents");
    };
    assert_eq!(request.source(), agent(2));
    assert_eq!(request.target(), agent(3));
    assert_eq!(request.by(), MergeAuthor::Operator(governor.operator()));
}

#[test]
fn agent_read_errors_map_to_query_errors() {
    assert_eq!(
        QueryError::from(AgentReadError::Store {
            reason: "reset".into()
        }),
        QueryError::Store {
            reason: "reset".into()
        }
    );
    assert_eq!(
        QueryError::from(AgentReadError::InvalidCursor),
        QueryError::InvalidCursor
    );
}

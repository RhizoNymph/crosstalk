//! The merge log, exact unmerges, merge vetoes and renames.

use std::collections::BTreeMap;

use crate::ids::{AgentId, MergeId, OperatorId, PromptHash};
use crate::observed::agent::{
    ActiveAgentState, Agent, AgentLabel, AgentState, AlreadyReverted, IdentityEvidence,
    InvalidMergeTransition, InvalidReversal, MergeAuthor, MergeRecord, MergeRequest, MergeVeto,
    MergedInto, RenameMerged, Reversal, SelfMerge,
};
use crate::support::{Blake3, Change, NonEmpty};
use crate::tests::fixtures::{agent, at};

fn operator() -> OperatorId {
    OperatorId::from_ulid(7)
}

fn record_with(id: u128, from: AgentId, into: AgentId, repointed: Vec<AgentId>) -> MergeRecord {
    let request =
        MergeRequest::new(from, into, MergeAuthor::Operator(operator())).expect("different agents");
    MergeRecord::new(MergeId::from_ulid(id), request, at(10), repointed)
}

fn reversal(restored: Vec<AgentId>) -> Reversal {
    Reversal {
        by: operator(),
        at: at(20),
        restored,
    }
}

fn agent_in(id: AgentId, state: AgentState) -> Agent {
    Agent {
        id,
        evidence: NonEmpty::new(IdentityEvidence::PromptFingerprint(
            PromptHash::from_digest(Blake3::from_bytes([1; 32])),
        )),
        parent: None,
        state,
        label: None,
    }
}

fn provisional() -> AgentState {
    AgentState::Provisional { first_seen: at(1) }
}

fn merged(merge: u128, into: AgentId, repointed_by: Vec<MergeId>) -> AgentState {
    AgentState::Merged(MergedInto {
        merge: MergeId::from_ulid(merge),
        into,
        prior: ActiveAgentState::Provisional { first_seen: at(1) },
        repointed_by,
    })
}

fn label(text: &str) -> AgentLabel {
    AgentLabel::new(text).expect("valid label")
}

// ── Active states ───────────────────────────────────────────────────────────

#[test]
fn active_states_round_trip() {
    let cases = [
        ActiveAgentState::Registered { at: at(1) },
        ActiveAgentState::Provisional { first_seen: at(2) },
        ActiveAgentState::Established { since: at(3) },
    ];
    for active in cases {
        assert_eq!(AgentState::from(active).active(), Ok(active));
        assert_eq!(AgentState::from(active).merged_into(), None);
    }
    let state = merged(1, agent(2), Vec::new());
    assert!(state.active().is_err());
    assert_eq!(state.merged_into(), Some(agent(2)));
}

// ── Merge records ───────────────────────────────────────────────────────────

#[test]
fn merge_record_takes_its_agents_from_the_request() {
    let record = record_with(1, agent(1), agent(2), vec![agent(3)]);
    assert_eq!(record.id(), MergeId::from_ulid(1));
    assert_eq!(record.source(), agent(1));
    assert_eq!(record.target(), agent(2));
    assert_eq!(record.by(), MergeAuthor::Operator(operator()));
    assert_eq!(record.at(), at(10));
    assert_eq!(record.repointed(), &[agent(3)]);
    assert_eq!(record.reverted(), None);
}

#[test]
fn a_record_is_reverted_at_most_once() {
    let mut record = record_with(1, agent(1), agent(2), vec![agent(3)]);
    assert_eq!(record.revert(reversal(vec![agent(3)])), Ok(()));
    assert_eq!(
        record.revert(reversal(Vec::new())),
        Err(InvalidReversal::AlreadyReverted(AlreadyReverted {
            merge: MergeId::from_ulid(1)
        }))
    );
    assert_eq!(record.reverted(), Some(&reversal(vec![agent(3)])));
}

#[test]
fn a_reversal_restores_only_agents_the_merge_repointed_in_its_order() {
    let repointed = vec![agent(3), agent(4), agent(5)];
    for restored in [
        Vec::new(),
        vec![agent(4)],
        vec![agent(3), agent(5)],
        repointed.clone(),
    ] {
        let mut record = record_with(1, agent(1), agent(2), repointed.clone());
        assert_eq!(record.revert(reversal(restored.clone())), Ok(()));
        assert_eq!(record.reverted(), Some(&reversal(restored)));
    }
    for (restored, refused) in [
        (vec![agent(6)], agent(6)),
        (vec![agent(3), agent(3)], agent(3)),
        (vec![agent(5), agent(3)], agent(3)),
        (vec![agent(1)], agent(1)),
    ] {
        let mut record = record_with(1, agent(1), agent(2), repointed.clone());
        assert_eq!(
            record.revert(reversal(restored)),
            Err(InvalidReversal::NotRepointed { agent: refused })
        );
        assert_eq!(
            record.reverted(),
            None,
            "a refused reversal changes nothing"
        );
    }
}

#[test]
fn a_reversal_is_not_dated_before_its_merge() {
    let mut record = record_with(1, agent(1), agent(2), Vec::new());
    let early = Reversal {
        at: at(9),
        ..reversal(Vec::new())
    };
    assert_eq!(
        record.revert(early),
        Err(InvalidReversal::BeforeMerge {
            merged: at(10),
            reverted: at(9)
        })
    );
    assert_eq!(record.reverted(), None);
    let same_instant = Reversal {
        at: at(10),
        ..reversal(Vec::new())
    };
    assert_eq!(record.revert(same_instant), Ok(()));
}

// ── Agent transitions ───────────────────────────────────────────────────────

#[test]
fn merge_away_keeps_the_active_state_as_prior() {
    let record = record_with(1, agent(1), agent(2), Vec::new());
    for active in [
        ActiveAgentState::Registered { at: at(1) },
        ActiveAgentState::Provisional { first_seen: at(2) },
        ActiveAgentState::Established { since: at(3) },
    ] {
        let mut from = agent_in(agent(1), active.into());
        assert_eq!(from.merge_away(&record), Ok(()));
        assert_eq!(
            from.state,
            AgentState::Merged(MergedInto {
                merge: record.id(),
                into: agent(2),
                prior: active,
                repointed_by: Vec::new(),
            })
        );
    }
}

#[test]
fn merge_away_refuses_another_agent_and_a_merged_one() {
    let record = record_with(1, agent(1), agent(2), Vec::new());
    let mut other = agent_in(agent(9), provisional());
    assert_eq!(
        other.merge_away(&record),
        Err(InvalidMergeTransition::OtherAgent)
    );
    assert_eq!(other.state, provisional());

    let mut already = agent_in(agent(1), merged(5, agent(4), Vec::new()));
    let before = already.clone();
    assert_eq!(
        already.merge_away(&record),
        Err(InvalidMergeTransition::AlreadyMerged { into: agent(4) })
    );
    assert_eq!(already, before);
}

#[test]
fn repoint_moves_only_agents_merged_into_the_source() {
    let record = record_with(2, agent(1), agent(2), vec![agent(3)]);
    let mut follower = agent_in(agent(3), merged(1, agent(1), Vec::new()));
    assert!(follower.repoint(&record));
    assert_eq!(
        follower.state,
        merged(1, agent(2), vec![MergeId::from_ulid(2)])
    );

    for state in [merged(1, agent(5), Vec::new()), provisional()] {
        let mut unrelated = agent_in(agent(4), state.clone());
        assert!(!unrelated.repoint(&record));
        assert_eq!(unrelated.state, state);
    }
}

#[test]
fn revert_returns_the_source_to_its_prior_state() {
    let record = record_with(1, agent(1), agent(2), Vec::new());
    let mut from = agent_in(agent(1), AgentState::Established { since: at(4) });
    from.merge_away(&record).expect("active source");
    assert_eq!(from.revert(&record), Ok(()));
    assert_eq!(from.state, AgentState::Established { since: at(4) });
}

#[test]
fn revert_refuses_unless_merged_by_that_record() {
    let record = record_with(1, agent(1), agent(2), Vec::new());
    let mut other = agent_in(agent(9), merged(1, agent(2), Vec::new()));
    assert_eq!(
        other.revert(&record),
        Err(InvalidMergeTransition::OtherAgent)
    );

    for state in [provisional(), merged(5, agent(2), Vec::new())] {
        let mut from = agent_in(agent(1), state.clone());
        assert_eq!(
            from.revert(&record),
            Err(InvalidMergeTransition::NotMergedByRecord)
        );
        assert_eq!(from.state, state);
    }
}

#[test]
fn restore_returns_to_the_source_and_forgets_later_repoints() {
    let record = record_with(2, agent(1), agent(2), vec![agent(3)]);
    let mut follower = agent_in(
        agent(3),
        merged(
            1,
            agent(5),
            vec![
                MergeId::from_ulid(2),
                MergeId::from_ulid(3),
                MergeId::from_ulid(4),
            ],
        ),
    );
    assert!(follower.restore(&record));
    assert_eq!(follower.state, merged(1, agent(1), Vec::new()));
}

#[test]
fn restore_leaves_agents_the_record_did_not_repoint() {
    let record = record_with(2, agent(1), agent(2), vec![agent(3)]);
    for state in [
        merged(6, agent(2), Vec::new()),
        merged(1, agent(5), vec![MergeId::from_ulid(3)]),
        provisional(),
    ] {
        let mut agent3 = agent_in(agent(3), state.clone());
        assert!(!agent3.restore(&record));
        assert_eq!(agent3.state, state);
    }
}

// ── Vetoes ──────────────────────────────────────────────────────────────────

#[test]
fn veto_rejects_a_self_pair_and_orders_its_pair() {
    assert_eq!(
        MergeVeto::new(agent(1), agent(1), operator(), at(1)),
        Err(SelfMerge)
    );
    let forward = MergeVeto::new(agent(1), agent(2), operator(), at(1)).expect("pair");
    let backward = MergeVeto::new(agent(2), agent(1), operator(), at(1)).expect("pair");
    assert_eq!(forward, backward);
    assert_eq!((forward.a(), forward.b()), (agent(1), agent(2)));
    assert_eq!((forward.by(), forward.at()), (operator(), at(1)));
}

#[test]
fn veto_of_a_reverted_merge_separates_its_source_and_target() {
    let record = record_with(1, agent(5), agent(2), Vec::new());
    let veto = MergeVeto::of(&record, &reversal(Vec::new()));
    assert_eq!((veto.a(), veto.b()), (agent(2), agent(5)));
    assert_eq!((veto.by(), veto.at()), (operator(), at(20)));
}

#[test]
fn veto_separates_clusters_in_either_order() {
    let veto = MergeVeto::new(agent(1), agent(2), operator(), at(1)).expect("pair");
    let left = [agent(1), agent(3)];
    let right = [agent(2), agent(4)];
    assert!(veto.separates(&left, &right));
    assert!(veto.separates(&right, &left));
    assert!(!veto.separates(&left, &[agent(4)]));
    assert!(!veto.separates(&[agent(1), agent(2)], &[agent(5)]));
}

// ── Renames ─────────────────────────────────────────────────────────────────

#[test]
fn rename_sets_clears_and_reports_no_change() {
    let mut planner = agent_in(agent(1), provisional());
    assert_eq!(planner.rename(Some(label("planner"))), Ok(Change::Applied));
    assert_eq!(planner.label, Some(label("planner")));
    assert_eq!(
        planner.rename(Some(label("planner"))),
        Ok(Change::Unchanged)
    );
    assert_eq!(planner.rename(None), Ok(Change::Applied));
    assert_eq!(planner.label, None);
    assert_eq!(planner.rename(None), Ok(Change::Unchanged));
}

#[test]
fn rename_refuses_a_merged_agent_and_keeps_its_label() {
    let mut alias = agent_in(agent(1), merged(1, agent(2), Vec::new()));
    alias.label = Some(label("old"));
    assert_eq!(
        alias.rename(Some(label("new"))),
        Err(RenameMerged { into: agent(2) })
    );
    assert_eq!(alias.rename(None), Err(RenameMerged { into: agent(2) }));
    assert_eq!(alias.label, Some(label("old")));
}

#[test]
fn agent_labels_allow_64_characters() {
    assert_eq!(AgentLabel::MAX_CHARS, 64);
    assert!(AgentLabel::new(&"é".repeat(64)).is_ok());
    assert!(AgentLabel::new(&"é".repeat(65)).is_err());
}

// ── The merge table ─────────────────────────────────────────────────────────

/// The merge table as `crate::observed::agent::merge` documents it, built
/// from the agent and record transitions.
#[derive(Debug, Clone)]
struct Table {
    agents: BTreeMap<AgentId, Agent>,
    log: Vec<MergeRecord>,
    vetoes: Vec<MergeVeto>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Refused {
    AgentMerged { agent: AgentId, into: AgentId },
    Vetoed,
    AlreadyReverted,
}

impl Table {
    fn active(ids: &[u128]) -> Self {
        Self {
            agents: ids
                .iter()
                .map(|n| (agent(*n), agent_in(agent(*n), provisional())))
                .collect(),
            log: Vec::new(),
            vetoes: Vec::new(),
        }
    }

    fn states(&self) -> BTreeMap<AgentId, AgentState> {
        self.agents
            .iter()
            .map(|(id, agent)| (*id, agent.state.clone()))
            .collect()
    }

    fn canonical(&self, id: AgentId) -> AgentId {
        self.agents[&id].state.merged_into().unwrap_or(id)
    }

    fn cluster(&self, canonical: AgentId) -> Vec<AgentId> {
        self.agents
            .keys()
            .copied()
            .filter(|id| self.canonical(*id) == canonical)
            .collect()
    }

    fn merge(&mut self, from: AgentId, into: AgentId, by: MergeAuthor) -> Result<MergeId, Refused> {
        for named in [from, into] {
            if let Some(target) = self.agents[&named].state.merged_into() {
                return Err(Refused::AgentMerged {
                    agent: named,
                    into: target,
                });
            }
        }
        let (left, right) = (self.cluster(from), self.cluster(into));
        match by {
            MergeAuthor::Resolver => {
                if self.vetoes.iter().any(|v| v.separates(&left, &right)) {
                    return Err(Refused::Vetoed);
                }
            }
            MergeAuthor::Operator(_) => self.vetoes.retain(|v| !v.separates(&left, &right)),
        }
        let repointed: Vec<AgentId> = self
            .agents
            .values()
            .filter(|a| a.state.merged_into() == Some(from))
            .map(|a| a.id)
            .collect();
        let id = MergeId::from_ulid(self.log.len() as u128 + 1);
        let request = MergeRequest::new(from, into, by).expect("checked distinct");
        let record = MergeRecord::new(id, request, at(10), repointed.clone());
        self.agents
            .get_mut(&from)
            .expect("known")
            .merge_away(&record)
            .expect("active source");
        for other in &repointed {
            assert!(self.agents.get_mut(other).expect("known").repoint(&record));
        }
        self.log.push(record);
        Ok(id)
    }

    fn unmerge(&mut self, merge: MergeId) -> Result<Vec<AgentId>, Refused> {
        let index = self
            .log
            .iter()
            .position(|r| r.id() == merge)
            .expect("known record");
        let record = self.log[index].clone();
        if record.reverted().is_some() {
            return Err(Refused::AlreadyReverted);
        }
        self.agents
            .get_mut(&record.source())
            .expect("known")
            .revert(&record)
            .expect("an unreverted record's source is merged by it");
        let restored: Vec<AgentId> = record
            .repointed()
            .iter()
            .copied()
            .filter(|x| self.agents.get_mut(x).expect("known").restore(&record))
            .collect();
        let reversal = Reversal {
            by: operator(),
            at: at(20),
            restored: restored.clone(),
        };
        self.vetoes.push(MergeVeto::of(&record, &reversal));
        self.log[index]
            .revert(reversal)
            .expect("checked unreverted");
        Ok(restored)
    }

    fn target(&self, id: u128) -> Option<AgentId> {
        self.agents[&agent(id)].state.merged_into()
    }

    /// The invariants every step must keep.
    fn check(&self) {
        for agent in self.agents.values() {
            if let AgentState::Merged(merged) = &agent.state {
                assert_ne!(merged.into, agent.id, "never merged into itself");
                assert!(
                    self.agents[&merged.into].state.merged_into().is_none(),
                    "chains are flat: {agent:?}"
                );
                let record = self
                    .log
                    .iter()
                    .find(|r| r.id() == merged.merge)
                    .expect("its record exists");
                assert_eq!(record.source(), agent.id);
                assert!(
                    record.reverted().is_none(),
                    "merged by an unreverted record"
                );
            }
        }
        for record in self.log.iter().filter(|r| r.reverted().is_none()) {
            match &self.agents[&record.source()].state {
                AgentState::Merged(merged) => assert_eq!(merged.merge, record.id()),
                other => panic!("unreverted record's source is not merged: {other:?}"),
            }
        }
    }
}

fn op() -> MergeAuthor {
    MergeAuthor::Operator(operator())
}

#[test]
fn merging_a_merged_agent_is_refused() {
    let mut table = Table::active(&[1, 2, 3]);
    table.merge(agent(1), agent(2), op()).expect("active pair");
    assert_eq!(
        table.merge(agent(3), agent(1), op()),
        Err(Refused::AgentMerged {
            agent: agent(1),
            into: agent(2)
        })
    );
    assert_eq!(
        table.merge(agent(1), agent(3), op()),
        Err(Refused::AgentMerged {
            agent: agent(1),
            into: agent(2)
        })
    );
}

#[test]
fn merge_repoints_agents_already_merged_into_the_source() {
    let mut table = Table::active(&[1, 2, 3]);
    table.merge(agent(1), agent(2), op()).expect("active pair");
    let m2 = table.merge(agent(2), agent(3), op()).expect("active pair");
    assert_eq!(table.target(1), Some(agent(3)));
    assert_eq!(table.target(2), Some(agent(3)));
    assert_eq!(table.log[1].repointed(), &[agent(1)]);
    assert_eq!(table.log[1].id(), m2);
    table.check();
}

#[test]
fn reverting_the_latest_merge_restores_every_state() {
    let mut table = Table::active(&[1, 2, 3, 4]);
    table.merge(agent(1), agent(2), op()).expect("active pair");
    table.merge(agent(4), agent(2), op()).expect("active pair");
    let before = table.states();
    let m = table.merge(agent(2), agent(3), op()).expect("active pair");
    assert_eq!(table.unmerge(m), Ok(vec![agent(1), agent(4)]));
    assert_eq!(table.states(), before);
    table.check();
}

#[test]
fn reverting_out_of_order_returns_repointed_agents_to_the_source() {
    let mut table = Table::active(&[1, 2, 3, 4]);
    table.merge(agent(1), agent(2), op()).expect("active pair");
    let m2 = table.merge(agent(2), agent(3), op()).expect("active pair");
    let m3 = table.merge(agent(3), agent(4), op()).expect("active pair");

    assert_eq!(table.unmerge(m2), Ok(vec![agent(1)]));
    assert_eq!(table.target(1), Some(agent(2)));
    assert_eq!(table.target(2), None);
    assert_eq!(table.target(3), Some(agent(4)));
    table.check();

    assert_eq!(table.unmerge(m3), Ok(Vec::new()), "1 and 2 already moved");
    assert_eq!(table.target(1), Some(agent(2)));
    assert_eq!(table.target(2), None);
    assert_eq!(table.target(3), None);
    table.check();
}

#[test]
fn reverting_leaves_agents_unmerged_or_merged_afresh_since() {
    let mut table = Table::active(&[1, 2, 3, 4]);
    let m1 = table.merge(agent(1), agent(2), op()).expect("active pair");
    let m2 = table.merge(agent(2), agent(3), op()).expect("active pair");
    table.unmerge(m1).expect("unreverted");
    table.merge(agent(1), agent(4), op()).expect("active pair");

    assert_eq!(table.unmerge(m2), Ok(Vec::new()));
    assert_eq!(table.target(1), Some(agent(4)));
    table.check();
}

#[test]
fn a_record_cannot_be_reverted_twice() {
    let mut table = Table::active(&[1, 2]);
    let m = table.merge(agent(1), agent(2), op()).expect("active pair");
    table.unmerge(m).expect("unreverted");
    let states = table.states();
    assert_eq!(table.unmerge(m), Err(Refused::AlreadyReverted));
    assert_eq!(table.states(), states);
}

#[test]
fn a_veto_stops_the_resolver_until_an_operator_merges() {
    let mut table = Table::active(&[1, 2, 3]);
    table.merge(agent(3), agent(2), op()).expect("active pair");
    let m = table
        .merge(agent(1), agent(2), MergeAuthor::Resolver)
        .expect("no veto yet");
    table.unmerge(m).expect("unreverted");

    assert_eq!(
        table.merge(agent(1), agent(2), MergeAuthor::Resolver),
        Err(Refused::Vetoed)
    );
    assert_eq!(
        table.merge(agent(2), agent(1), MergeAuthor::Resolver),
        Err(Refused::Vetoed),
        "either direction"
    );
    assert_eq!(table.target(1), None);

    table
        .merge(agent(1), agent(2), op())
        .expect("operator overrides");
    assert!(
        table.vetoes.is_empty(),
        "the operator merge cleared the veto"
    );
}

#[test]
fn a_veto_keeps_whole_clusters_apart() {
    let mut table = Table::active(&[1, 2, 3, 4]);
    let m = table.merge(agent(1), agent(2), op()).expect("active pair");
    table.unmerge(m).expect("unreverted");
    // 2 joins 3's cluster; 4 joins 1's.
    table.merge(agent(2), agent(3), op()).expect("active pair");
    table.merge(agent(4), agent(1), op()).expect("active pair");
    assert_eq!(
        table.merge(agent(1), agent(3), MergeAuthor::Resolver),
        Err(Refused::Vetoed)
    );
}

/// A small linear congruential generator, so the walk is deterministic and
/// needs no dependency.
struct Lcg(u64);

impl Lcg {
    fn below(&mut self, n: usize) -> usize {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        ((self.0 >> 33) % n as u64) as usize
    }
}

#[test]
fn random_merges_and_reverts_keep_the_invariants() {
    for seed in 0..64 {
        let mut rng = Lcg(seed);
        let ids = [1, 2, 3, 4, 5, 6];
        let mut table = Table::active(&ids);
        for _ in 0..40 {
            if rng.below(3) == 0 && !table.log.is_empty() {
                let index = rng.below(table.log.len());
                let merge = table.log[index].id();
                let _ = table.unmerge(merge);
            } else {
                let from = agent(ids[rng.below(ids.len())]);
                let into = agent(ids[rng.below(ids.len())]);
                if from == into {
                    continue;
                }
                let before = table.states();
                let by = if rng.below(2) == 0 {
                    MergeAuthor::Resolver
                } else {
                    op()
                };
                if let Ok(m) = table.merge(from, into, by) {
                    table.check();
                    if rng.below(4) == 0 {
                        table.unmerge(m).expect("just made");
                        assert_eq!(table.states(), before, "seed {seed}");
                    }
                }
            }
            table.check();
        }
    }
}

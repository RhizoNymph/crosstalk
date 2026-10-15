//! Agent identity on the wire: agents and their evidence, the merge log
//! (`MergeRequest`, `MergeRecord`, `Reversal`, `MergeVeto`, `MergedInto`) and
//! harness claims (`ClaimSet`). The checked types refuse on decode exactly
//! what their constructors refuse, and normalize as they do.

use super::super::harness::{assert_golden, assert_rejected, assert_round_trips};
use super::super::{ULID_A, ULID_B, ULID_C, ts};
use super::{
    ACCOUNT_HEX, AREA, CREDENTIAL_HEX, PROMPT_HEX, ULID_E, coder, digest, merge, operator, planner,
    reviewer,
};
use crate::ids::{AccountHash, CredentialHash, PromptHash, SecretVersion};
use crate::observed::agent::{
    ActiveAgentState, Agent, AgentLabel, AgentState, ClaimSet, IdentityEvidence, IdentityScope,
    MergeAuthor, MergeRecord, MergeRequest, MergeVeto, MergedInto, Reversal, SeenClaim,
};
use crate::observed::client::{HarnessClaim, HarnessFamily, UpstreamId};
use crate::support::NonEmpty;

fn credential() -> CredentialHash {
    CredentialHash::from_keyed_digest(SecretVersion(2), digest(CREDENTIAL_HEX))
}

fn account() -> AccountHash {
    AccountHash::from_keyed_digest(SecretVersion(2), digest(ACCOUNT_HEX))
}

fn prompt() -> PromptHash {
    PromptHash::from_digest(digest(PROMPT_HEX))
}

fn every_scope() -> Vec<IdentityScope> {
    fn declared(scope: IdentityScope) -> IdentityScope {
        match scope {
            IdentityScope::Account(_)
            | IdentityScope::Credential(_)
            | IdentityScope::Upstream(_) => scope,
        }
    }
    [
        IdentityScope::Account(account()),
        IdentityScope::Credential(credential()),
        IdentityScope::Upstream(UpstreamId("vllm".into())),
    ]
    .map(declared)
    .to_vec()
}

fn every_evidence() -> Vec<IdentityEvidence> {
    fn declared(evidence: IdentityEvidence) -> IdentityEvidence {
        match evidence {
            IdentityEvidence::HarnessAgent { .. }
            | IdentityEvidence::HarnessSession { .. }
            | IdentityEvidence::Account(_)
            | IdentityEvidence::StableCredential(_)
            | IdentityEvidence::RotatingCredential(_)
            | IdentityEvidence::PromptFingerprint(_) => evidence,
        }
    }
    [
        IdentityEvidence::HarnessAgent {
            scope: IdentityScope::Credential(credential()),
            agent: "planner".into(),
        },
        IdentityEvidence::HarnessSession {
            scope: IdentityScope::Account(account()),
            session: "0199b0f2-6c1e-7d3a-8f42-5e9a1c7b3d20".into(),
        },
        IdentityEvidence::Account(account()),
        IdentityEvidence::StableCredential(credential()),
        IdentityEvidence::RotatingCredential(credential()),
        IdentityEvidence::PromptFingerprint(prompt()),
    ]
    .map(declared)
    .to_vec()
}

fn every_active_state() -> Vec<ActiveAgentState> {
    fn declared(state: ActiveAgentState) -> ActiveAgentState {
        match state {
            ActiveAgentState::Registered { .. }
            | ActiveAgentState::Provisional { .. }
            | ActiveAgentState::Established { .. } => state,
        }
    }
    [
        ActiveAgentState::Registered {
            at: ts("2026-10-01T09:00:00.000000Z"),
        },
        ActiveAgentState::Provisional {
            first_seen: ts("2026-10-04T12:34:56.789012Z"),
        },
        ActiveAgentState::Established {
            since: ts("2026-10-04T12:40:00.125000Z"),
        },
    ]
    .map(declared)
    .to_vec()
}

/// The reviewer, merged into the coder by merge E, then repointed by merge B.
fn merged_into() -> MergedInto {
    MergedInto {
        merge: merge(ULID_E),
        into: coder(),
        prior: ActiveAgentState::Provisional {
            first_seen: ts("2026-10-04T12:34:56.789012Z"),
        },
        repointed_by: vec![merge(ULID_B)],
    }
}

fn every_state() -> Vec<AgentState> {
    fn declared(state: AgentState) -> AgentState {
        match state {
            AgentState::Registered { .. }
            | AgentState::Provisional { .. }
            | AgentState::Established { .. }
            | AgentState::Merged(_) => state,
        }
    }
    every_active_state()
        .into_iter()
        .map(AgentState::from)
        .chain([AgentState::Merged(merged_into())])
        .map(declared)
        .collect()
}

fn established_agent() -> Agent {
    Agent {
        id: planner(),
        evidence: NonEmpty::from_vec(vec![
            IdentityEvidence::HarnessAgent {
                scope: IdentityScope::Credential(credential()),
                agent: "planner".into(),
            },
            IdentityEvidence::StableCredential(credential()),
        ])
        .expect("two pieces of evidence"),
        parent: None,
        state: AgentState::Established {
            since: ts("2026-10-04T12:40:00.125000Z"),
        },
        label: Some(AgentLabel::new("research lead").expect("a valid label")),
    }
}

fn merged_agent() -> Agent {
    Agent {
        id: reviewer(),
        evidence: NonEmpty::new(IdentityEvidence::PromptFingerprint(prompt())),
        parent: Some(planner()),
        state: AgentState::Merged(merged_into()),
        label: None,
    }
}

fn every_author() -> Vec<MergeAuthor> {
    fn declared(author: MergeAuthor) -> MergeAuthor {
        match author {
            MergeAuthor::Resolver | MergeAuthor::Operator(_) => author,
        }
    }
    [MergeAuthor::Resolver, MergeAuthor::Operator(operator())]
        .map(declared)
        .to_vec()
}

/// The reviewer merged into the coder by the operator; the planner, merged
/// into the reviewer earlier, was repointed.
fn request() -> MergeRequest {
    MergeRequest::new(reviewer(), coder(), MergeAuthor::Operator(operator()))
        .expect("two different agents")
}

fn record() -> MergeRecord {
    MergeRecord::new(
        merge(ULID_E),
        request(),
        ts("2026-10-04T13:00:00.000000Z"),
        vec![planner()],
    )
}

fn reversal() -> Reversal {
    Reversal {
        by: operator(),
        at: ts("2026-10-04T14:15:30.500000Z"),
        restored: vec![planner()],
    }
}

fn reverted_record() -> MergeRecord {
    let mut record = record();
    record.revert(reversal()).expect("the first reversal");
    record
}

fn veto() -> MergeVeto {
    MergeVeto::new(
        reviewer(),
        coder(),
        operator(),
        ts("2026-10-04T14:15:30.500000Z"),
    )
    .expect("two different agents")
}

fn claim(family: HarnessFamily, version: Option<&str>, user_agent: &str) -> HarnessClaim {
    HarnessClaim {
        family,
        version: version.map(Into::into),
        user_agent: user_agent.into(),
    }
}

fn claude_code() -> HarnessClaim {
    claim(
        HarnessFamily::ClaudeCode,
        Some("2.0.14"),
        "claude-cli/2.0.14 (external, cli)",
    )
}

fn pi() -> HarnessClaim {
    // pi sends Claude Code's User-Agent on Claude subscription traffic.
    claim(
        HarnessFamily::ClaudeCode,
        Some("1.0.98"),
        "claude-cli/1.0.98 (external, cli)",
    )
}

fn oh_my_pi() -> HarnessClaim {
    claim(HarnessFamily::OhMyPi, None, "oh-my-pi")
}

/// Seen in this order; the set orders them latest first.
fn seen_claims() -> Vec<SeenClaim> {
    vec![
        SeenClaim {
            claim: pi(),
            last_seen: ts("2026-10-04T12:00:00.000000Z"),
        },
        SeenClaim {
            claim: claude_code(),
            last_seen: ts("2026-10-04T12:34:56.789012Z"),
        },
        SeenClaim {
            claim: oh_my_pi(),
            last_seen: ts("2026-10-04T12:34:56.789012Z"),
        },
    ]
}

fn claims() -> ClaimSet {
    ClaimSet::from_entries(seen_claims()).expect("three distinct claims")
}

#[test]
fn identity_evidence_golden_with_every_variant() {
    assert_golden(AREA, "identity_scopes", &every_scope());
    assert_golden(AREA, "identity_evidence", &every_evidence());
}

#[test]
fn agents_golden_in_every_state() {
    assert_golden(AREA, "agent_states", &every_state());
    assert_golden(AREA, "active_agent_states", &every_active_state());
    assert_golden(AREA, "agent_established", &established_agent());
    assert_golden(AREA, "agent_merged", &merged_agent());
}

#[test]
fn merge_log_golden() {
    assert_golden(AREA, "merge_authors", &every_author());
    assert_golden(AREA, "merge_request", &request());
    assert_golden(AREA, "merge_record", &record());
    assert_golden(AREA, "merge_record_reverted", &reverted_record());
    assert_golden(AREA, "merge_veto", &veto());
}

#[test]
fn claim_sets_golden() {
    assert_golden(AREA, "claim_set", &claims());
    assert_golden(AREA, "claim_set_empty", &ClaimSet::default());
    assert_eq!(
        claims().entries().first().map(|entry| &entry.claim),
        Some(&claude_code()),
        "latest first, ties by family"
    );
}

fn record_json(from: &str, into: &str, reverted: &str) -> String {
    format!(
        r#"{{"id": "{ULID_E}", "from": "{from}", "into": "{into}",
            "by": {{"type": "operator", "data": "{ULID_C}"}},
            "at": "2026-10-04T13:00:00.000000Z", "repointed": [], "reverted": {reverted}}}"#
    )
}

const REVERSAL: &str =
    r#"{"by": "01J9Z3N4P5Q6R7S8T9V0W1X2Y3", "at": "2026-10-04T14:15:30.500000Z", "restored": []}"#;

#[test]
fn merge_requests_refuse_a_self_merge() {
    let from = reviewer().ulid_text();
    assert_rejected::<MergeRequest>(
        &format!(r#"{{"from": "{from}", "into": "{from}", "by": {{"type": "resolver"}}}}"#),
        "invalid merge request: SelfMerge",
    );
    assert_rejected::<MergeRequest>(
        &format!(
            r#"{{"from": "{from}", "into": "{ULID_B}", "by": {{"type": "resolver"}}, "reason": "same key"}}"#
        ),
        "unknown field `reason`",
    );
    assert_rejected::<MergeAuthor>(r#"{"type": "system"}"#, "unknown variant `system`");
    assert_rejected::<MergeAuthor>(
        r#"{"type": "operator", "data": "operator-7"}"#,
        "invalid ULID text",
    );
}

#[test]
fn merge_records_refuse_a_self_merge_and_a_second_reversal() {
    let agent = reviewer().ulid_text();
    assert_rejected::<MergeRecord>(
        &record_json(&agent, &agent, "null"),
        "invalid merge record: SelfMerge",
    );
    assert_rejected::<MergeRecord>(
        &record_json(&agent, &agent, REVERSAL),
        "invalid merge record: SelfMerge",
    );
    // `MergeRecord::revert` refuses a second reversal. The record has one
    // `reverted` slot, so the only way to write a second is a repeated key,
    // which serde refuses.
    let twice = record_json(&agent, ULID_B, REVERSAL).replace(
        r#""reverted": "#,
        &format!(r#""reverted": {REVERSAL}, "reverted": "#),
    );
    assert_rejected::<MergeRecord>(&twice, "duplicate field `reverted`");
    assert_rejected::<MergeRecord>(
        &record_json(&agent, ULID_B, "null")
            .replace(r#""repointed""#, r#""note": "", "repointed""#),
        "unknown field `note`",
    );
    assert_rejected::<MergeRecord>(
        &record_json(
            &agent,
            ULID_B,
            &REVERSAL.replace(
                r#""restored""#,
                r#""reason": "different hosts", "restored""#,
            ),
        ),
        "unknown field `reason`",
    );
    // A record decodes through `MergeRecord::new` and `MergeRecord::revert`:
    // both shapes come back as the constructors build them.
    assert_round_trips(&record());
    assert_round_trips(&reverted_record());
}

#[test]
fn merge_records_refuse_a_reversal_the_merge_cannot_have() {
    let agent = reviewer().ulid_text();
    // Dated before the merge it reverts (13:00).
    assert_rejected::<MergeRecord>(
        &record_json(
            &agent,
            ULID_B,
            &REVERSAL.replace("2026-10-04T14:15:30.500000Z", "2026-10-04T12:59:59.999999Z"),
        ),
        "invalid merge record: Reversal(BeforeMerge",
    );
    // Restoring an agent the merge did not repoint (its `repointed` is
    // empty).
    let planner = planner().ulid_text();
    assert_rejected::<MergeRecord>(
        &record_json(
            &agent,
            ULID_B,
            &REVERSAL.replace(
                r#""restored": []"#,
                &format!(r#""restored": ["{planner}"]"#),
            ),
        ),
        "invalid merge record: Reversal(NotRepointed",
    );
    // Restoring the repointed agents out of order, or one twice. Any two
    // ids other than the record's own agents serve as the repointed pair.
    let (first, second) = (ULID_A, ULID_C);
    let repointing = |restored: &str| {
        record_json(
            &agent,
            ULID_B,
            &REVERSAL.replace(r#""restored": []"#, &format!(r#""restored": {restored}"#)),
        )
        .replace(
            r#""repointed": []"#,
            &format!(r#""repointed": ["{first}", "{second}"]"#),
        )
    };
    assert_rejected::<MergeRecord>(
        &repointing(&format!(r#"["{second}", "{first}"]"#)),
        "invalid merge record: Reversal(NotRepointed",
    );
    assert_rejected::<MergeRecord>(
        &repointing(&format!(r#"["{first}", "{first}"]"#)),
        "invalid merge record: Reversal(NotRepointed",
    );
    // The same record restoring a subsequence in order decodes.
    let decoded: MergeRecord =
        serde_json::from_str(&repointing(&format!(r#"["{second}"]"#))).expect("a subsequence");
    assert_eq!(decoded.reverted().map(|r| r.restored.len()), Some(1));
}

#[test]
fn merge_vetoes_refuse_a_self_pair_and_order_their_pair() {
    let agent = reviewer().ulid_text();
    let at = "2026-10-04T14:15:30.500000Z";
    assert_rejected::<MergeVeto>(
        &format!(r#"{{"a": "{agent}", "b": "{agent}", "by": "{ULID_C}", "at": "{at}"}}"#),
        "invalid merge veto: SelfMerge",
    );
    assert_rejected::<MergeVeto>(
        &format!(
            r#"{{"a": "{ULID_B}", "b": "{agent}", "by": "{ULID_C}", "at": "{at}", "reason": ""}}"#
        ),
        "unknown field `reason`",
    );
    // Decoding orders the pair, as `MergeVeto::new` does.
    let reversed: MergeVeto = serde_json::from_str(&format!(
        r#"{{"a": "{agent}", "b": "{ULID_B}", "by": "{ULID_C}", "at": "{at}"}}"#
    ))
    .expect("a reversed pair is the same veto");
    assert_eq!(reversed, veto());
    assert_eq!(reversed.a(), coder());
}

#[test]
fn claim_sets_refuse_a_repeated_claim_and_order_their_entries() {
    let entry =
        |claim: &HarnessClaim, at: &str| serde_json::json!({ "claim": claim, "last_seen": at });
    let repeated = serde_json::json!({
        "entries": [
            entry(&claude_code(), "2026-10-04T12:34:56.789012Z"),
            entry(&claude_code(), "2026-10-04T12:34:56.789012Z"),
        ]
    });
    assert_rejected::<ClaimSet>(&repeated.to_string(), "invalid claim set: DuplicateClaim");
    // The same claim at two times is still one claim listed twice.
    let twice = serde_json::json!({
        "entries": [
            entry(&oh_my_pi(), "2026-10-04T12:00:00.000000Z"),
            entry(&oh_my_pi(), "2026-10-04T12:34:56.789012Z"),
        ]
    });
    assert_rejected::<ClaimSet>(&twice.to_string(), "invalid claim set: DuplicateClaim");
    // Unsorted entries decode to the ordered set, as `ClaimSet::from_entries`
    // orders them, and encode back in that order.
    let unsorted = serde_json::json!({ "entries": seen_claims() });
    let decoded: ClaimSet =
        serde_json::from_str(&unsorted.to_string()).expect("distinct claims in any order");
    assert_eq!(decoded, claims());
    assert_eq!(
        serde_json::to_value(&decoded).expect("a claim set encodes"),
        serde_json::to_value(claims()).expect("a claim set encodes"),
    );
    assert_rejected::<ClaimSet>(r#"{"entries": [], "count": 0}"#, "unknown field `count`");
    assert_rejected::<SeenClaim>(
        r#"{"claim": {"family": "pi", "version": null, "user_agent": "pi"},
            "last_seen": "2026-10-04T12:00:00.000000Z", "first_seen": null}"#,
        "unknown field `first_seen`",
    );
}

#[test]
fn agents_refuse_unknown_fields_variants_and_invalid_parts() {
    let full = serde_json::to_value(established_agent()).expect("an agent encodes");

    let mut extra = full.clone();
    extra["name"] = "planner".into();
    assert_rejected::<Agent>(&extra.to_string(), "unknown field `name`");

    let mut empty = full.clone();
    empty["evidence"] = serde_json::json!([]);
    assert_rejected::<Agent>(&empty.to_string(), "invalid non-empty list: EmptyList");

    let mut long = full;
    long["label"] = "x".repeat(65).into();
    assert_rejected::<Agent>(&long.to_string(), "invalid display text: TooLong");

    assert_rejected::<AgentState>(
        r#"{"type": "retired", "data": {"at": "2026-10-04T12:00:00.000000Z"}}"#,
        "unknown variant `retired`",
    );
    assert_rejected::<AgentState>(r#"{"type": "registered"}"#, "missing field `data`");
    // `Merged` is not an active state.
    assert_rejected::<ActiveAgentState>(
        &serde_json::to_string(&AgentState::Merged(merged_into())).expect("a state encodes"),
        "unknown variant `merged`",
    );
    assert_rejected::<MergedInto>(
        &serde_json::to_value(merged_into())
            .map(|mut value| {
                value["chain"] = serde_json::json!([]);
                value.to_string()
            })
            .expect("a merged state encodes"),
        "unknown field `chain`",
    );
    assert_rejected::<IdentityEvidence>(
        r#"{"type": "ip_address", "data": "10.0.0.7"}"#,
        "unknown variant `ip_address`",
    );
    assert_rejected::<IdentityEvidence>(
        r#"{"type": "harness_agent", "data": {"scope": {"type": "upstream", "data": "vllm"}, "agent": "a", "parent": null}}"#,
        "unknown field `parent`",
    );
    assert_rejected::<IdentityScope>(
        r#"{"type": "host", "data": "wiki.example"}"#,
        "unknown variant `host`",
    );
    assert_rejected::<Reversal>(
        &REVERSAL.replace(r#""restored": []"#, r#""restored": [], "veto": true"#),
        "unknown field `veto`",
    );
}

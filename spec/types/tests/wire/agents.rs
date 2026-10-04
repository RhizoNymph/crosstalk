//! Agent read models on the wire: `QueryApi::agents` (an `AgentFilter` in,
//! `AgentRow`s out), `QueryApi::agent` (an `AgentDetail` with its
//! `AgentCluster`), `QueryApi::agent_names`, and L7's per-agent traffic.
//!
//! Fixtures: the planner (`a`) is canonical, with `e` merged into it;
//! the coder (`c`) is its sub-agent; the orchestrator (`d`) is its parent;
//! an operator vetoed merging the planner with `b`.

use std::collections::BTreeMap;

use serde_json::{Value, json};

use super::harness::{assert_golden, assert_rejected, assert_request_golden};
use super::{ULID_A, ULID_B, ULID_C, id, ts};
use crate::aggregates::agents::filter::{AgentFilter, AgentText};
use crate::aggregates::agents::{
    AgentCluster, AgentClusterParts, AgentDetail, AgentLookup, AgentName, AgentProfile,
    AgentProfileParts, AgentRow, AgentTraffic,
};
use crate::aggregates::node::CanonicalStateKind;
use crate::aggregates::watermark::Watermarked;
use crate::ids::{AgentId, MergeId, OperatorId};
use crate::observed::agent::{
    ActiveAgentState, Agent, AgentLabel, AgentState, ClaimSet, IdentityEvidence, IdentityScope,
    MergeAuthor, MergeRecord, MergeRequest, MergeVeto, MergedInto,
};
use crate::observed::client::{HarnessClaim, HarnessFamily, UpstreamId};
use crate::paging::{AgentList, Cursor, Page, PageSize};
use crate::support::{NonEmpty, Watermark};

const AREA: &str = "agents";

/// Two more ULIDs, after `ULID_C` in order.
const ULID_D: &str = "01J9Z3P5Q6R7S8T9V0W1X2Y3Z4";
const ULID_E: &str = "01J9Z3Q6R7S8T9V0W1X2Y3Z4A5";

fn agent(text: &str) -> AgentId {
    id(AgentId::from_ulid_text, text)
}

fn a() -> AgentId {
    agent(ULID_A)
}

fn b() -> AgentId {
    agent(ULID_B)
}

fn c() -> AgentId {
    agent(ULID_C)
}

fn d() -> AgentId {
    agent(ULID_D)
}

fn e() -> AgentId {
    agent(ULID_E)
}

fn operator() -> OperatorId {
    id(OperatorId::from_ulid_text, ULID_C)
}

fn merge_id() -> MergeId {
    id(MergeId::from_ulid_text, ULID_B)
}

fn label(text: &str) -> AgentLabel {
    AgentLabel::new(text).expect("a valid label")
}

fn established() -> ActiveAgentState {
    ActiveAgentState::Established {
        since: ts("2026-10-01T08:00:00.000000Z"),
    }
}

fn claims() -> ClaimSet {
    let mut claims = ClaimSet::default();
    claims.observe(
        HarnessClaim {
            family: HarnessFamily::ClaudeCode,
            version: Some("2.1.4".into()),
            user_agent: "claude-cli/2.1.4 (external, cli)".into(),
        },
        ts("2026-10-04T12:41:07.120000Z"),
    );
    claims
}

fn profile_parts() -> AgentProfileParts {
    AgentProfileParts {
        id: a(),
        label: Some(label("planner")),
        state: established(),
        parent: Some(d()),
        aliases: vec![e()],
        claims: claims(),
        last_seen: Some(ts("2026-10-04T12:41:07.120000Z")),
    }
}

fn profile() -> AgentProfile {
    AgentProfile::new(profile_parts()).expect("a canonical agent's profile")
}

fn traffic() -> AgentTraffic {
    AgentTraffic {
        transmissions_in: 2,
        transmissions_out: 3,
    }
}

fn evidence(session: &str) -> NonEmpty<IdentityEvidence> {
    NonEmpty::new(IdentityEvidence::HarnessSession {
        scope: IdentityScope::Upstream(UpstreamId("anthropic".into())),
        session: session.into(),
    })
}

fn merge() -> MergeRecord {
    let request = MergeRequest::new(e(), a(), MergeAuthor::Operator(operator()))
        .expect("two different agents");
    MergeRecord::new(
        merge_id(),
        request,
        ts("2026-10-03T16:20:00.000000Z"),
        Vec::new(),
    )
}

fn veto() -> MergeVeto {
    MergeVeto::new(a(), b(), operator(), ts("2026-10-03T17:05:00.000000Z"))
        .expect("two different agents")
}

fn cluster_parts(lookup: AgentLookup) -> AgentClusterParts {
    AgentClusterParts {
        profile: profile(),
        agent: Agent {
            id: a(),
            evidence: evidence("3f0c9a2e-planner"),
            parent: Some(d()),
            state: established().into(),
            label: Some(label("planner")),
        },
        aliases: vec![Agent {
            id: e(),
            evidence: evidence("8b41d7c0-planner-restart"),
            parent: None,
            state: AgentState::Merged(MergedInto {
                merge: merge_id(),
                into: a(),
                prior: ActiveAgentState::Provisional {
                    first_seen: ts("2026-10-03T15:58:12.000000Z"),
                },
                repointed_by: Vec::new(),
            }),
            label: None,
        }],
        children: vec![c()],
        merges: vec![merge()],
        vetoes: vec![veto()],
        lookup,
    }
}

fn cluster(lookup: AgentLookup) -> AgentCluster {
    AgentCluster::new(cluster_parts(lookup)).expect("a consistent cluster")
}

/// `QueryApi::agents`: the filter a client sends, every field set and none.
#[test]
fn agent_filter_goldens() {
    let filter = AgentFilter {
        states: vec![
            CanonicalStateKind::Provisional,
            CanonicalStateKind::Established,
        ],
        claimed: vec![HarnessFamily::ClaudeCode, HarnessFamily::Codex],
        text: Some(AgentText::new("plan").expect("valid filter text")),
        parents: vec![d()],
    };
    assert_request_golden(AREA, "agent_filter", &filter);
    assert_request_golden(AREA, "agent_filter_everything", &AgentFilter::default());
}

/// `QueryApi::agents`'s answer: a watermarked page of rows.
#[test]
fn agent_rows_golden() {
    let registered = AgentProfile::new(AgentProfileParts {
        id: d(),
        label: Some(label("orchestrator")),
        state: ActiveAgentState::Registered {
            at: ts("2026-09-30T10:00:00.000000Z"),
        },
        parent: None,
        aliases: Vec::new(),
        claims: ClaimSet::default(),
        last_seen: None,
    })
    .expect("a registered agent never seen in traffic");
    let rows = NonEmpty::from_vec(vec![
        AgentRow {
            profile: profile(),
            traffic: traffic(),
        },
        AgentRow {
            profile: registered,
            traffic: AgentTraffic::default(),
        },
    ])
    .expect("two rows");
    let next: Cursor<AgentList> =
        Cursor::from_token("YWdlbnRzLTAxSjla".into()).expect("URL-safe base64");
    let page = Page::more(PageSize::new(2).expect("a valid size"), rows, next)
        .expect("two fit a page of two");
    assert_golden(
        AREA,
        "agent_rows_page",
        &Watermarked {
            watermark: Watermark(ts("2026-10-04T12:58:00.000000Z")),
            value: page,
        },
    );
}

/// `QueryApi::agent`: the detail of a canonical agent asked for by its own
/// id, and of one reached through its merged alias.
#[test]
fn agent_detail_goldens() {
    fn name(lookup: AgentLookup) -> &'static str {
        match lookup {
            AgentLookup::Canonical => "agent_detail",
            AgentLookup::Redirected { .. } => "agent_detail_redirected",
        }
    }
    for lookup in [
        AgentLookup::Canonical,
        AgentLookup::Redirected { from: e() },
    ] {
        let detail = AgentDetail {
            cluster: cluster(lookup),
            traffic: traffic(),
        };
        assert_golden(
            AREA,
            name(lookup),
            &Some(Watermarked {
                watermark: Watermark(ts("2026-10-04T12:58:00.000000Z")),
                value: detail,
            }),
        );
    }
    assert_golden(AREA, "agent_unknown", &None::<Watermarked<AgentDetail>>);
}

/// `QueryApi::agent_names` returns a `BTreeMap`: its keys encode in
/// ascending id order, so a map of several names has one encoding.
#[test]
fn agent_names_golden() {
    let planner = AgentName {
        id: a(),
        label: Some(label("planner")),
    };
    // The merged alias is named by the agent it resolves to.
    let names = BTreeMap::from([(e(), planner.clone())]);
    assert_golden(AREA, "agent_names", &names);
    let more = BTreeMap::from([
        (a(), planner.clone()),
        (e(), planner),
        (
            c(),
            AgentName {
                id: c(),
                label: None,
            },
        ),
    ]);
    assert_golden(AREA, "agent_names_several", &more);
}

/// `EdgeStore::agent_traffic` is a `BTreeMap`: its keys encode in ascending
/// id order, which is ascending ULID text, so a map of several agents has
/// one encoding.
#[test]
fn agent_traffic_golden() {
    let map = BTreeMap::from([
        (d(), AgentTraffic::default()),
        (a(), traffic()),
        (
            c(),
            AgentTraffic {
                transmissions_in: 1,
                transmissions_out: 0,
            },
        ),
    ]);
    let watermarked = Watermarked {
        watermark: Watermark(ts("2026-10-04T12:58:00.000000Z")),
        value: map,
    };
    assert_golden(AREA, "agent_traffic", &watermarked);
    let encoded = serde_json::to_value(&watermarked.value).unwrap_or(Value::Null);
    let keys: Vec<&String> = encoded
        .as_object()
        .map(|map| map.keys().collect())
        .unwrap_or_default();
    let mut sorted = keys.clone();
    sorted.sort();
    assert_eq!(keys, sorted);
    assert_eq!(keys.len(), 3);
}

fn edited<T: serde::Serialize>(value: &T, edit: impl FnOnce(&mut Value)) -> String {
    let mut json =
        serde_json::to_value(value).unwrap_or_else(|error| panic!("a fixture encodes: {error}"));
    edit(&mut json);
    json.to_string()
}

fn at<'v>(json: &'v mut Value, pointer: &str) -> &'v mut Value {
    json.pointer_mut(pointer)
        .unwrap_or_else(|| panic!("no {pointer} in the fixture"))
}

fn array<'v>(json: &'v mut Value, pointer: &str) -> &'v mut Vec<Value> {
    at(json, pointer)
        .as_array_mut()
        .unwrap_or_else(|| panic!("{pointer} is not an array"))
}

#[test]
fn agent_profiles_refuse_what_their_constructor_refuses() {
    let refused = |reason: &str, edit: &dyn Fn(&mut Value)| {
        assert_rejected::<AgentProfile>(&edited(&profile(), edit), reason);
    };
    refused("invalid agent profile: SelfAlias", &|json| {
        array(json, "/aliases").push(json!(ULID_A));
    });
    refused("invalid agent profile: DuplicateAlias(", &|json| {
        array(json, "/aliases").push(json!(ULID_E));
    });
    refused("invalid agent profile: SelfParent", &|json| {
        *at(json, "/parent") = json!(ULID_A);
    });
    refused("invalid agent profile: ParentIsAlias(", &|json| {
        *at(json, "/parent") = json!(ULID_E);
    });
    refused("invalid agent profile: NeverSeen", &|json| {
        *at(json, "/last_seen") = Value::Null;
    });
    refused("unknown field `merged`", &|json| {
        if let Some(profile) = json.as_object_mut() {
            profile.insert("merged".into(), json!(false));
        }
    });
    refused("unknown variant `merged`", &|json| {
        *at(json, "/state") = json!({"type": "merged", "data": {}});
    });
    // Aliases in any order decode to the constructor's ascending order.
    let unordered = edited(&profile(), |json| {
        array(json, "/aliases").insert(0, json!(ULID_C));
    });
    let decoded: AgentProfile = serde_json::from_str(&unordered)
        .unwrap_or_else(|error| panic!("unordered aliases decode: {error}"));
    assert_eq!(decoded.aliases(), &[c(), e()]);
}

#[test]
fn agent_clusters_refuse_what_their_constructor_refuses() {
    let canonical = cluster(AgentLookup::Canonical);
    let refused = |reason: &str, edit: &dyn Fn(&mut Value)| {
        assert_rejected::<AgentCluster>(&edited(&canonical, edit), reason);
    };
    refused("invalid agent cluster: AgentMismatch", &|json| {
        *at(json, "/agent/label") = json!("coder");
    });
    refused("invalid agent cluster: AliasMismatch", &|json| {
        array(json, "/aliases").clear();
    });
    refused("invalid agent cluster: AliasNotMerged(", &|json| {
        *at(json, "/aliases/0/state/data/into") = json!(ULID_B);
    });
    refused("invalid agent cluster: UnknownRedirect(", &|json| {
        *at(json, "/lookup") = json!({"type": "redirected", "data": {"from": ULID_B}});
    });
    refused("invalid agent cluster: ChildInCluster(", &|json| {
        array(json, "/children").push(json!(ULID_E));
    });
    refused("invalid agent cluster: DuplicateChild(", &|json| {
        array(json, "/children").push(json!(ULID_C));
    });
    refused("invalid agent cluster: UnrelatedMerge(", &|json| {
        let mut unrelated = at(json, "/merges/0").clone();
        if let Some(fields) = unrelated.as_object_mut() {
            fields.insert("id".into(), json!(ULID_D));
            fields.insert("from".into(), json!(ULID_B));
            fields.insert("into".into(), json!(ULID_C));
        }
        array(json, "/merges").push(unrelated);
    });
    refused("invalid agent cluster: DuplicateMerge(", &|json| {
        let again = at(json, "/merges/0").clone();
        array(json, "/merges").push(again);
    });
    refused("invalid agent cluster: UnrelatedVeto {", &|json| {
        let mut unrelated = at(json, "/vetoes/0").clone();
        if let Some(fields) = unrelated.as_object_mut() {
            fields.insert("a".into(), json!(ULID_B));
            fields.insert("b".into(), json!(ULID_C));
        }
        array(json, "/vetoes").push(unrelated);
    });
    refused("invalid agent cluster: DuplicateVeto {", &|json| {
        let again = at(json, "/vetoes/0").clone();
        array(json, "/vetoes").push(again);
    });
    refused("unknown field `traffic`", &|json| {
        if let Some(cluster) = json.as_object_mut() {
            cluster.insert("traffic".into(), json!(null));
        }
    });
    refused("unknown variant `aliased`", &|json| {
        *at(json, "/lookup") = json!({"type": "aliased"});
    });
}

#[test]
fn agent_filters_refuse_unknown_fields_and_merged_agents() {
    let filter = r#""states": [], "claimed": [], "text": null, "parents": []"#;
    let decoded: AgentFilter = serde_json::from_str(&format!("{{{filter}}}"))
        .unwrap_or_else(|error| panic!("the empty filter decodes: {error}"));
    assert_eq!(decoded, AgentFilter::default());
    assert_rejected::<AgentFilter>(
        &format!(r#"{{{filter}, "window": null}}"#),
        "unknown field `window`",
    );
    // A merged agent is never a row, so a filter cannot ask for one.
    assert_rejected::<AgentFilter>(
        &format!(
            r#"{{{}}}"#,
            filter.replace(r#""states": []"#, r#""states": ["merged"]"#)
        ),
        "unknown variant `merged`",
    );
    assert_rejected::<AgentFilter>(
        &format!(
            r#"{{{}}}"#,
            filter.replace(r#""claimed": []"#, r#""claimed": ["cursor"]"#)
        ),
        "unknown variant `cursor`",
    );
    assert_rejected::<AgentFilter>(
        &format!(
            r#"{{{}}}"#,
            filter.replace(r#""text": null"#, r#""text": "   ""#)
        ),
        "invalid display text: Blank",
    );
    assert_rejected::<AgentFilter>(
        &format!(
            r#"{{{}}}"#,
            filter.replace(
                r#""text": null"#,
                &format!(r#""text": "{}""#, "p".repeat(65))
            )
        ),
        "invalid display text: TooLong { max: 64, got: 65 }",
    );
    assert_rejected::<AgentTraffic>(
        r#"{"transmissions_in": 1, "transmissions_out": 2, "matched_bytes": 9}"#,
        "unknown field `matched_bytes`",
    );
    assert_rejected::<AgentLookup>(
        r#"{"type": "redirected", "data": {"from": "01J9Z3K8M4Q7R2T5V6W8X9Y0ZA", "via": "x"}}"#,
        "unknown field `via`",
    );
    assert_rejected::<AgentName>(
        r#"{"id": "01J9Z3K8M4Q7R2T5V6W8X9Y0ZA", "label": null, "state": "established"}"#,
        "unknown field `state`",
    );
}

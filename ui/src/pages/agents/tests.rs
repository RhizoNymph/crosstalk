//! The agent pages over the fixture: aliases, merge history, vetoes and
//! the merge, unmerge and rename refusals the spec defines.

use crosstalk_spec::ids::MergeId;
use topcoat::router::StatusCode;

use crate::components::href::tests::state;
use crate::testing::{Session, agent_id, get, operator, post, world};
use crate::url::ulid::UlidId;
use crosstalk_spec::interfaces::l8_surface::QueryApi;

fn page(key: &str) -> String {
    format!("/agents/{}?{}", agent_id(key).to_ulid(), state().to_query())
}

/// The merge in force that merged `alias` into its canonical agent.
async fn merge_of(key: &str) -> MergeId {
    let c = operator().caller();
    let detail = world()
        .agent(&c, agent_id(key), state().scope.window)
        .await
        .expect("read")
        .expect("agent")
        .value;
    detail
        .cluster
        .merges()
        .iter()
        .find(|m| m.source() == agent_id(key) && m.reverted().is_none())
        .map(|m| m.id())
        .expect("a merge in force")
}

#[tokio::test]
async fn an_alias_url_shows_its_canonical_agent_with_a_banner() {
    let reply = get(&page("al0")).await;
    assert_eq!(reply.status, StatusCode::OK, "{}", reply.body);
    assert!(reply.body.contains("atlas-lead"));
    assert!(reply.body.contains("is merged into this agent"));
    let direct = get(&page("cc0")).await;
    assert!(!direct.body.contains("is merged into this agent"));
    assert!(
        direct.body.contains("before: "),
        "aliases keep their prior state"
    );
}

#[tokio::test]
async fn the_reverted_merge_shows_its_veto_and_no_unmerge() {
    let reply = get(&page("omp3")).await;
    assert_eq!(reply.status, StatusCode::OK, "{}", reply.body);
    assert!(reply.body.contains("reverted by"));
    assert!(!reply.body.contains("No vetoes."));
    assert!(!reply.body.contains(">Unmerge</button>"));
}

#[tokio::test]
async fn an_unmerge_reverts_once() {
    let session = Session::new();
    let merge = merge_of("al3").await;
    let url = page("pi2");
    let body = format!("action=unmerge&merge={}", merge.to_ulid());
    let pi2 = session.get(&url).await;
    assert!(pi2.body.contains("unmerging restores: "), "{}", pi2.body);
    assert!(
        pi2.body.contains("pointed back at it"),
        "al2 follows al3 back"
    );
    let done = session.post(&url, &body).await;
    assert_eq!(done.status, StatusCode::SEE_OTHER, "{}", done.body);
    let again = session.post(&url, &body).await;
    assert_eq!(again.status, StatusCode::CONFLICT);
    assert!(again.body.contains("was already reverted"));
    let al3 = session.get(&page("al3")).await;
    assert!(
        !al3.body.contains("is merged into this agent"),
        "al3 is its own agent again"
    );
}

#[tokio::test]
async fn a_merged_agent_cannot_be_renamed() {
    let reply = post(&page("al0"), "action=rename&label=planner").await;
    assert_eq!(reply.status, StatusCode::CONFLICT, "{}", reply.body);
    assert!(reply.body.contains("act on that agent instead"));
}

#[tokio::test]
async fn merges_name_canonical_agents() {
    let cx3 = agent_id("cx3").to_ulid();
    let (al1, cx1) = (agent_id("al1").to_ulid(), agent_id("cx1").to_ulid());
    let url = format!("/agents/{cx3}/merge?{}", state().to_query());
    // The comparison resolves the alias, so the confirm form names cx1.
    let compare = get(&format!("{url}&into={al1}")).await;
    assert_eq!(compare.status, StatusCode::OK, "{}", compare.body);
    assert!(compare.body.contains(&format!("value=\"{cx1}\"")));
    // Posting the alias itself is the merge table's conflict.
    let reply = post(&url, &format!("into={al1}")).await;
    assert_eq!(reply.status, StatusCode::CONFLICT, "{}", reply.body);
    assert!(
        reply
            .body
            .contains(&format!("agent {al1} is merged into {cx1}"))
    );
    // Comparing with an alias of the agent itself is MergeIntoSelf.
    let pi2 = agent_id("pi2").to_ulid();
    let al3 = agent_id("al3").to_ulid();
    let reply = get(&format!(
        "/agents/{pi2}/merge?{}&into={al3}",
        state().to_query()
    ))
    .await;
    assert_eq!(reply.status, StatusCode::CONFLICT, "{}", reply.body);
    assert!(
        reply
            .body
            .contains("the merge target resolves to the source agent")
    );
}

//! The entry links other pages gain into the conversation view.

use crosstalk_spec::interfaces::l8_surface::QueryApi;
use topcoat::router::StatusCode;

use crate::components::href::tests::state;
use crate::testing::{agent_id, get, operator, world};
use crate::url::ulid::UlidId;

#[tokio::test]
async fn the_agent_page_links_to_its_conversations() {
    let id = agent_id("cc0");
    let reply = get(&format!("/agents/{}?{}", id.to_ulid(), state().to_query())).await;
    assert_eq!(reply.status, StatusCode::OK, "{}", reply.body);
    assert!(
        reply
            .body
            .contains(&format!("/agents/{}/conversations?", id.to_ulid())),
        "{}",
        reply.body
    );
    assert!(reply.body.contains("data-conversations=\"true\""));
}

#[tokio::test]
async fn each_match_on_the_evidence_page_links_to_both_turns() {
    let caller = operator().caller();
    let mut found = None;
    for id in world().transmission_ids() {
        let Some(transmission) = world().transmission(&caller, id).await.expect("read") else {
            continue;
        };
        if let Some(confirmed) = transmission.state.confirmed() {
            found = Some((id, confirmed.content().first().clone()));
            break;
        }
    }
    let (id, content) = found.expect("a confirmed transmission");
    let reply = get(&format!(
        "/transmissions/{}?{}",
        id.to_ulid(),
        crate::pages::topology::tests::fixture_state().to_query()
    ))
    .await;
    assert_eq!(reply.status, StatusCode::OK, "{}", reply.body);
    assert!(
        reply.body.contains(&format!(
            "/exchanges/{}?",
            content.reader_exchange().to_ulid()
        )),
        "{}",
        reply.body
    );
    assert!(
        reply
            .body
            .contains(&format!("/spans/{}?", content.origin().to_ulid()))
    );
    assert!(
        reply.body.contains("in sender&#x27;s conversation")
            || reply.body.contains("in sender's conversation")
    );
}

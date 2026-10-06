//! The conversation pages through the router: over the fixture (its seeded
//! conversations), over the world backend (which records none), and over
//! HTTP (the fixture behind the gateway API's binding).

use crosstalk_spec::ids::{AgentId, ConversationId};
use crosstalk_spec::interfaces::l8_surface::Permission;
use topcoat::router::StatusCode;

use super::query::ConversationQuery;
use crate::backend::AppBackend;
use crate::backend::fixture::Cases;
use crate::components::href::tests::state;
use crate::testing::{caller_with, get, get_from, router_over_app, world};
use crate::url::ulid::UlidId;

fn cases() -> Cases {
    world()
        .conversation_records()
        .cases()
        .expect("cases")
        .clone()
}

fn agent_of(id: ConversationId) -> AgentId {
    let stored = world()
        .conversation_records()
        .get(id)
        .expect("conversation")
        .agent;
    world().scenario().cast.identity.canonical(stored)
}

fn page(id: ConversationId, extra: &str) -> String {
    format!(
        "/conversations/{}?{}{extra}",
        id.to_ulid(),
        state().to_query()
    )
}

#[tokio::test]
async fn a_fork_shows_its_head_turns_and_origin() {
    let (fork, parent) = cases().fork;
    let reply = get(&page(fork, "")).await;
    assert_eq!(reply.status, StatusCode::OK, "{}", reply.body);
    let body = &reply.body;
    assert!(body.contains("forked from"), "{body}");
    assert!(body.contains(&format!("/conversations/{}", parent.to_ulid())));
    assert!(body.contains("data-turn=\"0\""));
    assert!(body.contains("data-boundary=\"fork\""));
    assert!(body.contains("claims"), "claims are shown as claims");
    assert!(body.contains("<pre"), "the trusted operator reads text");
}

#[tokio::test]
async fn the_compaction_and_its_carried_over_messages_show() {
    let (compacted, _) = cases().compaction;
    let reply = get(&page(compacted, "")).await;
    assert_eq!(reply.status, StatusCode::OK, "{}", reply.body);
    assert!(
        reply.body.contains("data-boundary=\"compaction\""),
        "{}",
        reply.body
    );
    assert!(reply.body.contains("carried over (2)"));
}

#[tokio::test]
async fn marks_link_to_evidence_and_turns() {
    // A conversation whose turns read other agents' text.
    let b = world();
    let (transmission, content) = b.confirmed_matches().into_iter().next().expect("confirmed");
    let (id, turn) = b
        .conversation_records()
        .locate(content.reader_exchange())
        .expect("threaded");
    let reply = get(&page(id, &format!("&turn={turn}"))).await;
    assert_eq!(reply.status, StatusCode::OK, "{}", reply.body);
    let body = &reply.body;
    assert!(body.contains("data-mark=\"inbound\""), "{body}");
    assert!(body.contains(&format!("/transmissions/{}", transmission.to_ulid())));
    assert!(body.contains(&format!("id=\"turn-{turn}\"")));
}

#[tokio::test]
async fn windows_page_by_twenty_and_a_turn_past_the_end_says_so() {
    let (id, _) = cases().compaction;
    let total = world()
        .conversation_records()
        .get(id)
        .expect("c")
        .turns
        .len() as u32;
    let reply = get(&page(id, "&turn=5000")).await;
    assert_eq!(reply.status, StatusCode::OK, "{}", reply.body);
    assert!(
        reply.body.contains("Turn 5000 does not exist yet"),
        "{}",
        reply.body
    );
    assert!(
        reply.body.contains(&format!("of {total}"))
            || reply.body.contains(&format!("{total} turns"))
    );
}

#[tokio::test]
async fn bad_page_keys_are_refused_by_name() {
    let (id, _) = cases().fork;
    let reply = get(&page(id, "&turn=x")).await;
    assert_eq!(
        reply.status,
        StatusCode::UNPROCESSABLE_ENTITY,
        "{}",
        reply.body
    );
    assert!(reply.body.contains("turn"));
    let reply = get(&page(id, "&hl=nope")).await;
    assert_eq!(reply.status, StatusCode::UNPROCESSABLE_ENTITY);
}

#[tokio::test]
async fn an_unknown_conversation_is_not_found() {
    let reply = get(&page(ConversationId::from_ulid(1), "")).await;
    assert_eq!(reply.status, StatusCode::NOT_FOUND, "{}", reply.body);
    assert!(reply.body.contains("Conversation not found"));
}

#[tokio::test]
async fn a_view_only_caller_sees_structure_and_no_text() {
    let (id, _) = cases().mid_system;
    let viewer = caller_with(&[Permission::View]);
    let query = ConversationQuery::default();
    let loaded = super::load(&crate::testing::cx(), &viewer, id, &query, &state())
        .await
        .expect("loads")
        .expect("found");
    assert!(!loaded.content);
    for turn in &loaded.turns {
        for message in turn.inputs.iter().chain(turn.output.iter()) {
            for part in &message.parts {
                assert!(
                    !matches!(part.text, super::model::TextView::Shown { .. }),
                    "no text without Content"
                );
            }
        }
    }
}

#[tokio::test]
async fn the_agents_list_shows_its_conversations_and_filters() {
    let (fork, _) = cases().fork;
    let agent = agent_of(fork);
    let url = format!(
        "/agents/{}/conversations?{}",
        agent.to_ulid(),
        state().to_query()
    );
    let reply = get(&url).await;
    assert_eq!(reply.status, StatusCode::OK, "{}", reply.body);
    assert!(
        reply
            .body
            .contains(&format!("/conversations/{}", fork.to_ulid())),
        "{}",
        reply.body
    );
    assert!(reply.body.contains("data-conversation=\"true\""));
    let forks = get(&format!("{url}&origin=fork")).await;
    assert_eq!(forks.status, StatusCode::OK);
    assert!(
        forks
            .body
            .contains(&format!("/conversations/{}", fork.to_ulid()))
    );
    assert!(forks.body.contains("fork of"));
    let bad = get(&format!("{url}&origin=branch")).await;
    assert_eq!(bad.status, StatusCode::UNPROCESSABLE_ENTITY);
}

#[tokio::test]
async fn the_replayed_agents_conversations_are_labelled() {
    let (corpus, one) = cases().replay;
    let agent = agent_of(one);
    let url = format!(
        "/agents/{}/conversations?{}",
        agent.to_ulid(),
        state().to_query()
    );
    let reply = get(&url).await;
    assert_eq!(reply.status, StatusCode::OK, "{}", reply.body);
    assert!(
        reply.body.contains(&format!("replayed: {}", corpus.0)),
        "{}",
        reply.body
    );
    let live = get(&format!("{url}&replay=exclude")).await;
    assert!(
        live.body.contains("No conversations match these filters"),
        "{}",
        live.body
    );
}

#[tokio::test]
async fn exchanges_and_spans_redirect_to_their_turn() {
    let b = world();
    let (_, content) = b.confirmed_matches().into_iter().next().expect("confirmed");
    let (id, turn) = b
        .conversation_records()
        .locate(content.reader_exchange())
        .expect("threaded");
    let reply = get(&format!(
        "/exchanges/{}?{}",
        content.reader_exchange().to_ulid(),
        state().to_query()
    ))
    .await;
    assert_eq!(reply.status, StatusCode::SEE_OTHER, "{}", reply.body);
    let location = reply.location.expect("location");
    assert!(
        location.starts_with(&format!("/conversations/{}?", id.to_ulid())),
        "{location}"
    );
    assert!(location.contains(&format!("turn={turn}")));
    assert!(location.ends_with(&format!("#turn-{turn}")));

    let reply = get(&format!(
        "/spans/{}?{}",
        content.origin().to_ulid(),
        state().to_query()
    ))
    .await;
    assert_eq!(reply.status, StatusCode::SEE_OTHER, "{}", reply.body);
    let location = reply.location.expect("location");
    assert!(
        location.contains(&format!("hl={}", content.origin().to_ulid())),
        "{location}"
    );
    let followed = get(&location.replace(
        &format!("#turn-{}", location.rsplit("#turn-").next().unwrap_or("")),
        "",
    ))
    .await;
    assert_eq!(followed.status, StatusCode::OK, "{}", followed.body);
    assert!(
        followed.body.contains("data-readers=\"true\""),
        "the highlighted span's readers"
    );

    let missing = get(&format!(
        "/exchanges/{}?{}",
        crosstalk_spec::ids::ExchangeId::from_ulid(1).to_ulid(),
        state().to_query()
    ))
    .await;
    assert_eq!(missing.status, StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn over_the_world_backend_the_pages_render_empty_states() {
    let (world_backend, in_process) =
        crate::backend::world::WorldBackend::start(crate::testing::SEED)
            .await
            .expect("world starts");
    let backend = AppBackend::World(world_backend);
    // One of the world's own agents.
    let agents = crosstalk_spec::interfaces::l8_surface::QueryApi::agents(
        &backend,
        &crate::testing::operator().caller(),
        &crosstalk_spec::aggregates::agents::filter::AgentFilter::default(),
        state().scope.window,
        &crate::pages::common::paging::first(1u16),
    )
    .await
    .expect("agents")
    .value;
    let agent = agents
        .items()
        .first()
        .expect("the world has agents")
        .profile
        .id();
    let router = router_over_app(backend);
    let list = get_from(
        &router,
        &format!(
            "/agents/{}/conversations?{}",
            agent.to_ulid(),
            state().to_query()
        ),
    )
    .await;
    assert_eq!(list.status, StatusCode::OK, "{}", list.body);
    assert!(
        list.body.contains("No conversations recorded"),
        "{}",
        list.body
    );
    let conversation = get_from(&router, &page(ConversationId::from_ulid(1), "")).await;
    assert_eq!(
        conversation.status,
        StatusCode::NOT_FOUND,
        "{}",
        conversation.body
    );
    let exchange = get_from(
        &router,
        &format!(
            "/exchanges/{}?{}",
            crosstalk_spec::ids::ExchangeId::from_ulid(1).to_ulid(),
            state().to_query()
        ),
    )
    .await;
    assert_eq!(exchange.status, StatusCode::NOT_FOUND, "{}", exchange.body);
    drop(router);
    in_process.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn over_http_the_pages_read_the_fixtures_conversations() {
    use crate::testing::fixture_api::FixtureApi;
    use crate::testing::http::ONCALL_TOKEN;
    let api = FixtureApi::start().await;
    let access = api.access(ONCALL_TOKEN).await.expect("access");
    let router = api.router(ONCALL_TOKEN, access);
    let (fork, parent) = cases().fork;
    let agent = agent_of(fork);
    let list = get_from(
        &router,
        &format!(
            "/agents/{}/conversations?{}",
            agent.to_ulid(),
            state().to_query()
        ),
    )
    .await;
    assert_eq!(list.status, StatusCode::OK, "{}", list.body);
    assert!(
        list.body
            .contains(&format!("/conversations/{}", fork.to_ulid())),
        "{}",
        list.body
    );
    let conversation = get_from(&router, &page(fork, "&turn=1")).await;
    assert_eq!(conversation.status, StatusCode::OK, "{}", conversation.body);
    assert!(conversation.body.contains("data-turn=\"1\""));
    assert!(
        conversation
            .body
            .contains(&format!("/conversations/{}", parent.to_ulid()))
    );
    let (_, reader) = world()
        .confirmed_matches()
        .into_iter()
        .next()
        .expect("a match");
    let placed = get_from(
        &router,
        &format!(
            "/exchanges/{}?{}",
            reader.reader_exchange().to_ulid(),
            state().to_query()
        ),
    )
    .await;
    assert_eq!(placed.status, StatusCode::SEE_OTHER, "{}", placed.body);
    let (id, turn) = world()
        .conversation_records()
        .locate(reader.reader_exchange())
        .expect("threaded");
    let location = placed.location.expect("location");
    assert!(location.contains(&format!("/conversations/{}?", id.to_ulid())));
    assert!(location.contains(&format!("turn={turn}")));
    drop(router);
    api.stop().await;
}

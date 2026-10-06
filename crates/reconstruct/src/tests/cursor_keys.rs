//! List cursor keys derived from the deployment secret
//! (`surface.cursor.survives-restart`, decision Q4 of postgres_stores.md):
//! a conversation list cursor issued by one store handle resolves on
//! another keyed from the same secret (a restarted process), and is refused
//! under another secret.

use crosstalk_spec::ids::{DeploymentSecret, KeyedHasher, SecretVersion};
use crosstalk_spec::interfaces::l3_reconstruction::conversations::{
    ConversationQuery, ConversationReadError, ConversationReads,
};
use crosstalk_spec::paging::{PageRequest, PageSize};
use crosstalk_testkit::build::ExchangeBuilder;

use super::rig::Rig;
use crate::ids::{AGENTS_CURSOR_LABEL, CONVERSATIONS_CURSOR_LABEL, cursor_key};

fn secret(byte: u8) -> KeyedHasher {
    KeyedHasher::new(DeploymentSecret::new(SecretVersion(1), [byte; 32]))
}

#[test]
fn cursor_keys_are_stable_per_secret_and_label() {
    let key = cursor_key(&secret(1), CONVERSATIONS_CURSOR_LABEL);
    assert_eq!(key, cursor_key(&secret(1), CONVERSATIONS_CURSOR_LABEL));
    assert_ne!(key, cursor_key(&secret(2), CONVERSATIONS_CURSOR_LABEL));
    assert_ne!(key, cursor_key(&secret(1), AGENTS_CURSOR_LABEL));
}

#[tokio::test]
async fn conversation_cursor_keyed_from_the_secret_resolves_after_a_restart() {
    let mut rig = Rig::new();
    for n in 0..3 {
        let client = ExchangeBuilder::new(&mut rig.scene.ids).build().meta.client;
        let user = rig.scene.user(&format!("task {n}")).await;
        let output = rig.scene.assistant(&format!("answer {n}")).await;
        let at = rig.scene.tick();
        let exchange = ExchangeBuilder::new(&mut rig.scene.ids)
            .started_at(at)
            .client(client)
            .request(vec![user])
            .response(output)
            .build();
        let handled = rig.deliver(&exchange).await;
        assert!(handled.is_ok(), "{handled:?}");
    }
    let before = rig.conversations.clone().with_cursor_secret(&secret(1));
    let every = ConversationQuery::default();
    let size = PageSize::new(1).expect("page size");
    let first = before
        .list(&every, &PageRequest { size, after: None })
        .await
        .expect("first page");
    let next = PageRequest {
        size,
        after: Some(first.next().cloned().expect("a next cursor")),
    };
    let expected = before.list(&every, &next).await.expect("second page");

    let restarted = rig.conversations.clone().with_cursor_secret(&secret(1));
    assert_eq!(restarted.list(&every, &next).await, Ok(expected));
    let rotated = rig.conversations.clone().with_cursor_secret(&secret(2));
    assert_eq!(
        rotated.list(&every, &next).await,
        Err(ConversationReadError::InvalidCursor)
    );
}

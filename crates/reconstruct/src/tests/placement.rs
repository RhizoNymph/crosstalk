//! `ExchangePlacements` on the in-memory conversation store: an exchange
//! reads back as the agent and conversation its threading recorded.

use crosstalk_spec::interfaces::l3_reconstruction::{ExchangePlacements, Placement};
use crosstalk_spec::observed::client::ClientContext;
use crosstalk_spec::observed::exchange::Exchange;
use crosstalk_testkit::build::ExchangeBuilder;

use super::rig::Rig;
use crate::consumer::Handled;

async fn exchange(rig: &mut Rig, client: &ClientContext, text: &str) -> Exchange {
    let user = rig.scene.user(text).await;
    let output = rig.scene.assistant(&format!("answer to {text}")).await;
    let at = rig.scene.tick();
    ExchangeBuilder::new(&mut rig.scene.ids)
        .started_at(at)
        .client(client.clone())
        .request(vec![user])
        .response(output)
        .build()
}

#[tokio::test]
async fn a_threaded_exchange_reads_back_as_its_delta_placed_it() {
    let mut rig = Rig::new();
    let client = ExchangeBuilder::new(&mut rig.scene.ids).build().meta.client;
    let first = exchange(&mut rig, &client, "hello").await;
    let handled = rig.deliver(&first).await;
    assert!(
        matches!(handled, Ok(Handled::Threaded { .. })),
        "{handled:?}"
    );
    let delta = rig.delta_of(first.meta.id).expect("a delta was published");
    let placed = rig.conversations.placement(first.meta.id).await;
    assert_eq!(
        placed,
        Ok(Some(Placement {
            agent: delta.agent,
            conversation: delta.conversation,
        }))
    );
    // A redelivery records nothing new and reads back the same.
    let again = rig.deliver(&first).await;
    assert!(again.is_ok(), "{again:?}");
    assert_eq!(rig.conversations.placement(first.meta.id).await, placed);
}

#[tokio::test]
async fn an_exchange_never_threaded_has_no_placement() {
    let mut rig = Rig::new();
    let client = ExchangeBuilder::new(&mut rig.scene.ids).build().meta.client;
    let unseen = exchange(&mut rig, &client, "never delivered").await;
    assert_eq!(rig.conversations.placement(unseen.meta.id).await, Ok(None));
}

//! The scenario through `Live::settle`: two runs over the same traffic end
//! with the same transmissions.

use crosstalk_e2e::{Composition, compose_with, feed};
use crosstalk_gateway::live::Ticking;
use crosstalk_gateway::pipeline::Settings;
use crosstalk_spec::derived::flow::transmission::Transmission;
use crosstalk_spec::events::{BusEvent, Subject};
use crosstalk_spec::interfaces::l2_transport::{ConsumerGroup, EventBus, Subscription};
use crosstalk_transport::MpscSubscription;

use crate::support::{Failure, relay, unexpected};

/// Every subject, for the debug observer.
const ALL: [Subject; 28] = [
    Subject::ExchangeCaptured,
    Subject::ConversationDelta,
    Subject::AgentSeen,
    Subject::AgentMerged,
    Subject::AgentUnmerged,
    Subject::AgentRenamed,
    Subject::SpanOriginated,
    Subject::SpanRelayed,
    Subject::ContentMatched,
    Subject::AccessRecorded,
    Subject::ChannelDiscovered,
    Subject::ChannelCrossAccessed,
    Subject::DeclaredChannelUnused,
    Subject::ChannelPromoted,
    Subject::TransmissionConfirmed,
    Subject::TransmissionSuspected,
    Subject::VerdictSet,
    Subject::TransmissionClassified,
    Subject::TopicVersionReady,
    Subject::TopicVersionActivated,
    Subject::TopicVersionDropped,
    Subject::WatermarkAdvanced,
    Subject::EdgeUpdated,
    Subject::AlertOpened,
    Subject::AlertChanged,
    Subject::AlertRuleChanged,
    Subject::PolicyChanged,
    Subject::Changed,
];

/// The scenario fed and settled at its end, with an observer of every
/// event subscribed before the first exchange.
async fn settled() -> Result<(Composition, MpscSubscription), Failure> {
    let scenario = relay();
    let composition = compose_with(scenario.start, Ticking::OnSettle).await?;
    let observer = composition
        .stores
        .bus
        .subscribe(
            &ALL,
            ConsumerGroup("e2e-determinism".to_owned()),
            Settings::default().consumer_retry,
        )
        .await
        .map_err(|error| unexpected(format!("subscribe: {error:?}")))?;
    let clock = composition.clock.clone();
    feed(&scenario, &composition.pipeline, |at| clock.set(at)).await?;
    composition.live().settle(scenario.ends_at()).await?;
    Ok((composition, observer))
}

/// Every event the observer has, as debug text.
async fn events(observer: &mut MpscSubscription) -> Vec<String> {
    let mut seen = Vec::new();
    while let Ok(Some(Ok(delivery))) =
        tokio::time::timeout(std::time::Duration::from_millis(50), observer.next()).await
    {
        let label = match &delivery.envelope.event {
            BusEvent::Ingest(event) => format!("{:?}", event.subject()),
            other => format!("{other:?}"),
        };
        seen.push(label);
        let _ = observer.ack(delivery.id).await;
    }
    seen
}

async fn transmissions(composition: &Composition) -> Result<Vec<Transmission>, Failure> {
    crosstalk_e2e::read::all_transmissions(&composition.stores.transmissions)
        .await
        .map_err(|error| unexpected(format!("listing transmissions: {error}")))
}

#[tokio::test]
async fn two_settled_runs_give_identical_transmissions() -> Result<(), Failure> {
    let (first, mut observer) = settled().await?;
    let seen = events(&mut observer).await;
    let one = transmissions(&first).await?;
    assert!(
        one.iter()
            .any(|transmission| transmission.state.confirmed().is_some()),
        "no confirmed transmission after settling; events: {seen:#?}"
    );
    first.shutdown().await;
    let (second, _observer) = settled().await?;
    let two = transmissions(&second).await?;
    second.shutdown().await;
    assert_eq!(one, two);
    Ok(())
}

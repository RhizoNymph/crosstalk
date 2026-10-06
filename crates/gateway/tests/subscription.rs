//! Claude Code on a Claude Pro/Max subscription through the running gateway
//! (role `all`, real sockets): an OAuth bearer token sent with the OAuth
//! capability in `anthropic-beta`, refreshed mid-session. The upstream gets
//! every request unchanged; the gateway keeps one agent for the session;
//! and no piece of either token reaches a log line, a stored blob, the
//! exchange log or any bus envelope. Its own test binary, because it
//! installs the global subscriber. Every token is fake.

#[allow(dead_code)]
#[path = "e2e/support.rs"]
mod support;

use std::io::Write;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crosstalk_gateway::logging;
use crosstalk_spec::events::ingest::IngestEvent;
use crosstalk_spec::events::{BusEvent, Envelope, Subject};
use crosstalk_spec::interfaces::l2_transport::{ConsumerGroup, EventBus, Subscription};
use crosstalk_spec::observed::client::CredentialScheme;
use crosstalk_testkit::upstream::{FakeUpstream, Reply, Script};
use crosstalk_transport::BusConfig;
use tracing_subscriber::fmt::MakeWriter;

/// The session's access token before and after the (simulated) refresh.
/// Shaped like real ones; the secret parts are distinctive so any window of
/// them is findable.
const TOKENS: [&str; 2] = [
    "sk-ant-oat01-TEST-q7Vk2mXw9RbL4tNz8HcJ5yPd3FgS6aEu",
    "sk-ant-oat01-TEST-W3nB8vKx1ZrQ6mLp4TsY9dHj2CfG7eNa",
];

/// How long a window of a token must be to count as a leak. Shorter
/// windows (`sk-ant-o`) are the public shape, not the secret.
const WINDOW: usize = 12;

const EVERY_SUBJECT: [Subject; 28] = [
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

#[derive(Debug, Clone, Default)]
struct Captured(Arc<Mutex<Vec<u8>>>);

impl Write for Captured {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0
            .lock()
            .map_err(|_| std::io::Error::other("poisoned"))?
            .extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'writer> MakeWriter<'writer> for Captured {
    type Writer = Captured;

    fn make_writer(&'writer self) -> Self::Writer {
        self.clone()
    }
}

/// The first window of a token found in `bytes`, if any.
fn leaked(bytes: &[u8]) -> Option<String> {
    TOKENS.iter().find_map(|token| {
        // Skip the public `sk-ant-oat01-` shape: windows start inside the
        // secret part.
        let secret = &token.as_bytes()["sk-ant-oat01-".len()..];
        secret
            .windows(WINDOW)
            .find(|window| bytes.windows(WINDOW).any(|candidate| candidate == *window))
            .map(|window| String::from_utf8_lossy(window).into_owned())
    })
}

/// Every file under `dir`, recursively.
fn files(dir: &Path, into: &mut Vec<std::path::PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            files(&path, into);
        } else {
            into.push(path);
        }
    }
}

/// `ingress.credential.absent-end-to-end`, with
/// `reconstruct.identity.refresh-keeps-session-agent` through the real
/// gateway.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn subscription_tokens_pass_through_and_never_persist() {
    for token in TOKENS {
        assert!(
            leaked(token.as_bytes()).is_some(),
            "the check finds a token"
        );
    }
    assert_eq!(
        leaked(b"sk-ant-oat01-TEST-"),
        None,
        "the public shape is not a leak"
    );
    let sink = Captured::default();
    logging::try_init(sink.clone(), "trace").expect("the only subscriber in this binary");

    let upstream = FakeUpstream::start(Script::new()).await.expect("upstream");
    let mut gateway = support::start(&upstream.base_url(), support::Options::default()).await;
    let mut everything = gateway
        .running
        .bus()
        .expect("role all runs a live process")
        .subscribe(
            &EVERY_SUBJECT,
            ConsumerGroup("subscription-leak-check".to_owned()),
            BusConfig::default().retry,
        )
        .await
        .expect("subscribes");

    let case = support::case("subagent_oauth_streaming");
    let mut hashes = Vec::new();
    for token in TOKENS {
        upstream
            .reply_next(Reply::from_case(&case))
            .await
            .expect("scripted");
        let request = case.request_with_credential(token).expect("a header value");
        let response = gateway.client().send(&request).await.expect("answered");
        assert_eq!(response.differences_from(&case.response), Vec::new());
        let received = upstream.received().await.expect("log");
        let last = received.last().expect("the upstream got the request");
        assert_eq!(
            last.differences_from(&request),
            Vec::new(),
            "the token and the OAuth beta reach the upstream unchanged"
        );
        let envelope = gateway
            .next_captured(Duration::from_secs(5))
            .await
            .expect("captured");
        let exchange = support::exchange(&envelope);
        let credential = exchange.meta.client.credential.expect("a credential");
        assert_eq!(credential.scheme, CredentialScheme::OauthAccessToken);
        hashes.push(credential.hash);
    }
    assert_ne!(hashes[0], hashes[1], "a refreshed token has its own digest");

    // Every envelope any layer published, until the bus goes quiet.
    let mut envelopes: Vec<Envelope> = Vec::new();
    while let Ok(Some(next)) = tokio::time::timeout(Duration::from_secs(2), everything.next()).await
    {
        let delivery = next.expect("a decodable delivery");
        everything.ack(delivery.id).await.expect("acks");
        envelopes.push(delivery.envelope);
    }
    let agents: Vec<_> = envelopes
        .iter()
        .filter_map(|envelope| match &envelope.event {
            BusEvent::Ingest(IngestEvent::ConversationDelta(delta)) => Some(delta.agent),
            _ => None,
        })
        .collect();
    assert_eq!(agents.len(), 2, "one delta per exchange: {agents:?}");
    assert_eq!(
        agents[0], agents[1],
        "the refresh keeps the session's agent"
    );

    let data_dir = gateway.data_dir.clone();
    gateway.running.shutdown().await;

    for envelope in &envelopes {
        let json = serde_json::to_vec(envelope).expect("an envelope serializes");
        assert_eq!(leaked(&json), None, "a token is in {:?}", envelope.event);
    }
    let mut stored = Vec::new();
    files(&data_dir, &mut stored);
    assert!(
        stored
            .iter()
            .any(|path| path.ends_with("exchange-log.jsonl")),
        "the exchange log was written: {stored:?}"
    );
    assert!(stored.len() > 1, "bodies were stored: {stored:?}");
    for path in &stored {
        let bytes = std::fs::read(path).expect("readable");
        assert_eq!(leaked(&bytes), None, "a token is in {}", path.display());
    }
    let logs = sink.0.lock().expect("not poisoned").clone();
    assert!(!logs.is_empty(), "the gateway logged");
    assert_eq!(leaked(&logs), None, "a token was logged");
    assert!(
        !String::from_utf8_lossy(&logs).contains(support::SECRET_HEX),
        "the deployment secret was logged"
    );
}

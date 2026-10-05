//! The gateway's logs, at debug level over a full capture and a refused
//! exchange: one JSON object per line with a top-level `level`, the refused
//! request's shape, and never the deployment secret, a credential or a
//! message body. Its own test binary, because it installs
//! the global subscriber.

#[allow(dead_code)]
#[path = "e2e/support.rs"]
mod support;

use std::io::Write;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crosstalk_gateway::logging;
use crosstalk_testkit::upstream::{FakeUpstream, Reply, Script};
use serde_json::Value;
use tracing_subscriber::fmt::MakeWriter;

/// Every log line written, in memory.
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

/// Every string at least `min` characters long inside `value`.
fn strings(value: &Value, min: usize, into: &mut Vec<String>) {
    match value {
        Value::String(text) if text.chars().count() >= min => into.push(text.clone()),
        Value::Array(items) => items.iter().for_each(|item| strings(item, min, into)),
        Value::Object(fields) => fields.values().for_each(|item| strings(item, min, into)),
        _ => {}
    }
}

/// The content of a refused request, which no log line may hold.
const REFUSED: &str = "REFUSED-BODY-MARKER-REFUSED-BODY-MARKER";

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn logs_are_json_lines_without_secrets_credentials_or_bodies() {
    let sink = Captured::default();
    logging::try_init(sink.clone(), "debug").expect("the only subscriber in this binary");

    let upstream = FakeUpstream::start(Script::new()).await.expect("upstream");
    let mut gateway = support::start(&upstream.base_url(), support::Options::default()).await;
    let credential = "sk-ant-api03-LEAKCHECKLEAKCHECKLEAKCHECK";
    let mut texts = Vec::new();
    for name in ["text_turn", "tool_use_streaming", "tool_result_followup"] {
        let case = support::case(name);
        upstream
            .reply_next(Reply::from_case(&case))
            .await
            .expect("scripted");
        let request = case
            .request_with_credential(credential)
            .expect("a valid header");
        let _ = gateway.client().send(&request).await.expect("answered");
        gateway
            .next_captured(Duration::from_secs(5))
            .await
            .expect("captured");
        let golden = support::golden(name);
        for message in golden["messages"].as_array().expect("messages") {
            strings(&message["body"]["data"], 16, &mut texts);
        }
    }
    // A refused request: its top-level shape is logged at debug level, its
    // content never.
    let refused = support::case("text_turn");
    upstream
        .reply_next(Reply::from_case(&refused))
        .await
        .expect("scripted");
    let mut request = refused
        .request_with_credential(credential)
        .expect("a valid header");
    let mut body = request.json().expect("JSON");
    body["messages"] = serde_json::json!([{"role": "developer", "content": REFUSED}]);
    request.body = serde_json::to_vec(&body).expect("encodes").into();
    let _ = gateway.client().send(&request).await.expect("answered");
    gateway.settle(4).await;
    assert_eq!(gateway.running.health().pipeline.normalize_failed, 1);
    gateway.running.shutdown().await;

    let bytes = sink.0.lock().expect("not poisoned").clone();
    let text = String::from_utf8(bytes).expect("UTF-8 logs");
    assert!(text.contains("exchange captured"), "the capture was logged");
    for line in text.lines() {
        let object: Value = serde_json::from_str(line)
            .unwrap_or_else(|error| panic!("not a JSON line ({error}): {line}"));
        assert!(object["level"].is_string(), "no top-level level: {line}");
    }
    assert!(
        !text.contains(support::SECRET_HEX),
        "the deployment secret was logged"
    );
    assert!(!text.contains("LEAKCHECK"), "a credential was logged");
    assert!(!text.contains(REFUSED), "a refused body was logged");
    let shapes: Vec<Value> = text
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .filter(|object| object["message"] == "refused request shape")
        .collect();
    assert_eq!(shapes.len(), 1, "one shape for the one refusal");
    assert_eq!(shapes[0]["level"], "DEBUG");
    let shape = shapes[0]["request_shape"].as_str().expect("a shape field");
    assert!(
        shape.contains("messages=[developer:string]"),
        "the shape names the role and content kind: {shape}"
    );
    assert!(!texts.is_empty());
    for body_text in &texts {
        assert!(
            !text.contains(body_text.as_str()),
            "a message body was logged: {body_text:?}"
        );
    }
}

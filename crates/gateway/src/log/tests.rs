//! The exchange log: append, reopen, duplicates, torn tails, corruption.

use crosstalk_spec::events::Envelope;
use crosstalk_testkit::build::event::{EnvelopeBuilder, exchange_captured};
use crosstalk_testkit::build::exchange::ExchangeBuilder;
use crosstalk_testkit::ids::Ids;

use super::{Appended, ExchangeLog, LogError, read};

fn envelopes(count: usize) -> Vec<Envelope> {
    let mut ids = Ids::seeded(11);
    (0..count)
        .map(|_| {
            let exchange = ExchangeBuilder::new(&mut ids).build();
            EnvelopeBuilder::new(&mut ids, exchange_captured(exchange)).build()
        })
        .collect()
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("a runtime")
}

#[test]
fn appended_envelopes_read_back_in_order_after_reopening() {
    runtime().block_on(async {
        let dir = tempfile::tempdir().expect("a temp dir");
        let path = dir.path().join("nested").join("exchange-log.jsonl");
        let written = envelopes(3);
        let mut log = ExchangeLog::open(&path).await.expect("opens");
        assert!(log.is_empty());
        for envelope in &written {
            assert_eq!(
                log.append(envelope).await.expect("appends"),
                Appended::Written
            );
        }
        log.close().await.expect("closes");

        let contents = read(&path).await.expect("reads");
        assert_eq!(contents.entries, written);
        assert_eq!(contents.torn_tail, 0);

        let mut reopened = ExchangeLog::open(&path).await.expect("reopens");
        assert_eq!(reopened.len(), 3);
        let more = envelopes(4);
        assert_eq!(
            reopened.append(&more[3]).await.expect("appends"),
            Appended::Written
        );
        reopened.close().await.expect("closes");
        assert_eq!(read(&path).await.expect("reads").entries.len(), 4);
    });
}

#[test]
fn a_redelivered_envelope_is_written_once_even_across_reopening() {
    runtime().block_on(async {
        let dir = tempfile::tempdir().expect("a temp dir");
        let path = dir.path().join("exchange-log.jsonl");
        let envelope = envelopes(1).remove(0);
        let mut log = ExchangeLog::open(&path).await.expect("opens");
        assert_eq!(
            log.append(&envelope).await.expect("appends"),
            Appended::Written
        );
        assert_eq!(
            log.append(&envelope).await.expect("appends"),
            Appended::Duplicate
        );
        log.close().await.expect("closes");
        let mut log = ExchangeLog::open(&path).await.expect("reopens");
        assert_eq!(
            log.append(&envelope).await.expect("appends"),
            Appended::Duplicate
        );
        log.close().await.expect("closes");
        assert_eq!(read(&path).await.expect("reads").entries, vec![envelope]);
    });
}

#[test]
fn a_torn_last_line_is_ignored_on_read_and_truncated_on_open() {
    runtime().block_on(async {
        let dir = tempfile::tempdir().expect("a temp dir");
        let path = dir.path().join("exchange-log.jsonl");
        let written = envelopes(3);
        let mut log = ExchangeLog::open(&path).await.expect("opens");
        for envelope in &written[..2] {
            log.append(envelope).await.expect("appends");
        }
        log.close().await.expect("closes");
        let mut bytes = std::fs::read(&path).expect("raw bytes");
        let whole = bytes.len();
        let torn = serde_json::to_vec(&written[2]).expect("encodes");
        bytes.extend_from_slice(&torn[..torn.len() / 2]);
        std::fs::write(&path, &bytes).expect("write a torn tail");

        let contents = read(&path).await.expect("reads");
        assert_eq!(contents.entries, written[..2].to_vec());
        assert_eq!(contents.torn_tail, torn.len() / 2);

        let mut log = ExchangeLog::open(&path).await.expect("reopens");
        assert_eq!(
            std::fs::metadata(&path).expect("metadata").len(),
            whole as u64
        );
        log.append(&written[2]).await.expect("appends");
        log.close().await.expect("closes");
        let contents = read(&path).await.expect("reads");
        assert_eq!(contents.entries, written);
        assert_eq!(contents.torn_tail, 0);
    });
}

#[test]
fn a_corrupt_complete_line_is_an_error_that_names_the_line_not_its_content() {
    runtime().block_on(async {
        let dir = tempfile::tempdir().expect("a temp dir");
        let path = dir.path().join("exchange-log.jsonl");
        let envelope = envelopes(1).remove(0);
        let mut text = serde_json::to_string(&envelope).expect("encodes");
        text.push('\n');
        text.push_str("{\"secret-looking\": \"sk-ant-api03-xyz\"}\n");
        std::fs::write(&path, text).expect("write");
        let error = read(&path).await.expect_err("line 2 is not an envelope");
        assert!(
            matches!(error, LogError::Corrupt { line: 2, .. }),
            "{error:?}"
        );
        assert!(!error.to_string().contains("sk-ant"));
        assert!(ExchangeLog::open(&path).await.is_err());
    });
}

#[test]
fn a_missing_log_reads_as_empty() {
    runtime().block_on(async {
        let dir = tempfile::tempdir().expect("a temp dir");
        let contents = read(&dir.path().join("absent.jsonl")).await.expect("reads");
        assert!(contents.entries.is_empty());
    });
}

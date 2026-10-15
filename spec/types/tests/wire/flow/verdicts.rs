//! Operator verdicts, their records and the per-transmission log.

use std::num::NonZeroU32;

use serde_json::{Value, json};

use super::super::harness::{assert_golden, assert_rejected};
use super::{AREA, ULID_A, coder, confirmed, operator, read_at, transmission_id, wiki};
use crate::derived::flow::transmission::{Route, Transmission, TransmissionState};
use crate::derived::flow::verdict::{
    InvalidVerdictLog, InvalidVerdictRecord, TransmissionVerdict, Verdict, VerdictLog,
    VerdictRevision,
};
use crate::ids::TransmissionId;
use crate::tests::wire::{id, ts};

fn judged() -> Transmission {
    Transmission {
        id: transmission_id(),
        to: coder(),
        route: Route::Channel(wiki()),
        opened_at: read_at(),
        state: TransmissionState::Confirmed(confirmed()),
    }
}

fn record(verdict: Option<Verdict>, at: &str, note: Option<&str>) -> TransmissionVerdict {
    TransmissionVerdict::new(&judged(), verdict, operator(), ts(at), note.map(Into::into))
        .expect("a confirmed transmission takes a verdict")
}

fn revision(n: u32) -> VerdictRevision {
    VerdictRevision::new(NonZeroU32::new(n).expect("revisions start at 1"))
}

/// Genuine, then withdrawn, then a false detection.
fn records() -> Vec<TransmissionVerdict> {
    vec![
        record(
            Some(Verdict::Genuine),
            "2026-10-04T14:00:00.000000Z",
            Some("the coder quoted the plan"),
        ),
        record(None, "2026-10-04T14:10:00.000000Z", None),
        record(
            Some(Verdict::FalseDetection),
            "2026-10-04T14:20:00.000000Z",
            Some("both copied the same template"),
        ),
    ]
}

fn log() -> VerdictLog {
    VerdictLog::from_records(
        transmission_id(),
        records()
            .into_iter()
            .zip(1..)
            .map(|(record, n)| (revision(n), record))
            .collect(),
    )
    .expect("consecutive revisions, each record changing the verdict")
}

#[test]
fn verdicts_golden() {
    fn verdict(verdict: Verdict) -> Verdict {
        match verdict {
            Verdict::Genuine | Verdict::FalseDetection => verdict,
        }
    }
    let verdicts = [Verdict::Genuine, Verdict::FalseDetection].map(verdict);
    assert_golden(AREA, "verdicts", &verdicts.to_vec());
    assert_golden(AREA, "verdict_revision", &revision(2));
    let [genuine, withdrawal, _] = <[TransmissionVerdict; 3]>::try_from(records())
        .unwrap_or_else(|_| unreachable!("three records"));
    assert_golden(AREA, "transmission_verdict", &genuine);
    assert_golden(AREA, "transmission_verdict_withdrawal", &withdrawal);
}

#[test]
fn verdict_log_golden_carries_each_revision() {
    let log = log();
    assert_eq!(log.revision(), Some(revision(3)));
    assert_eq!(log.current(), Some(Verdict::FalseDetection));
    assert_golden(AREA, "verdict_log", &log);
    assert_golden(
        AREA,
        "verdict_log_empty",
        &VerdictLog::new(transmission_id()),
    );
    let json = serde_json::to_value(&log).expect("a log encodes");
    let revisions: Vec<&Value> = json["records"]
        .as_array()
        .expect("an array")
        .iter()
        .map(|entry| &entry["revision"])
        .collect();
    assert_eq!(revisions, [&json!(1), &json!(2), &json!(3)]);
}

/// A repeated verdict and a gap in revisions are refused, by
/// `VerdictLog::from_records` with an `InvalidVerdictLog` and so by
/// decoding.
#[test]
fn verdict_logs_refuse_repeats_and_revision_gaps() {
    let [genuine, _, false_detection] = <[TransmissionVerdict; 3]>::try_from(records())
        .unwrap_or_else(|_| unreachable!("three records"));
    let repeated = vec![
        (revision(1), genuine.clone()),
        (revision(2), genuine.clone()),
    ];
    assert_eq!(
        VerdictLog::from_records(transmission_id(), repeated),
        Err(InvalidVerdictLog::Unchanged { index: 1 })
    );
    let gap = vec![
        (revision(1), genuine.clone()),
        (revision(3), false_detection.clone()),
    ];
    assert_eq!(
        VerdictLog::from_records(transmission_id(), gap),
        Err(InvalidVerdictLog::UnexpectedRevision {
            index: 1,
            expected: revision(2),
            found: revision(3),
        })
    );
    let other = id(TransmissionId::from_ulid_text, ULID_A);
    assert_eq!(
        VerdictLog::from_records(other, vec![(revision(1), genuine)]),
        Err(InvalidVerdictLog::Record {
            index: 0,
            error: InvalidVerdictRecord::OtherTransmission,
        })
    );

    let valid = serde_json::to_value(log()).expect("a log encodes");
    let entries = valid["records"].as_array().expect("an array").clone();
    let with_records = |records: Vec<Value>| {
        let mut value = valid.clone();
        value["records"] = Value::Array(records);
        value.to_string()
    };
    let mut repeat = entries[0].clone();
    repeat["revision"] = json!(2);
    assert_rejected::<VerdictLog>(
        &with_records(vec![entries[0].clone(), repeat]),
        "invalid verdict log: Unchanged { index: 1 }",
    );
    assert_rejected::<VerdictLog>(
        &with_records(vec![entries[0].clone(), entries[2].clone()]),
        "invalid verdict log: UnexpectedRevision { index: 1",
    );
    assert_rejected::<VerdictLog>(
        &with_records(vec![entries[1].clone()]),
        "invalid verdict log: UnexpectedRevision { index: 0",
    );
    let mut stray = entries[0].clone();
    stray["record"]["transmission"] = json!(ULID_A);
    assert_rejected::<VerdictLog>(
        &with_records(vec![stray]),
        "invalid verdict log: Record { index: 0, error: OtherTransmission }",
    );
    let mut extra = entries[0].clone();
    extra["current"] = json!(true);
    assert_rejected::<VerdictLog>(&with_records(vec![extra]), "unknown field `current`");
    let mut extra_log = valid;
    extra_log["revision"] = json!(3);
    assert_rejected::<VerdictLog>(&extra_log.to_string(), "unknown field `revision`");
}

#[test]
fn verdicts_refuse_unknown_fields_and_variants() {
    assert_rejected::<Verdict>(r#""unsure""#, "unknown variant `unsure`");
    assert_rejected::<VerdictRevision>("0", "invalid value");
    let mut record = serde_json::to_value(records().remove(0)).expect("a verdict record encodes");
    record["revision"] = json!(1);
    assert_rejected::<TransmissionVerdict>(&record.to_string(), "unknown field `revision`");
}

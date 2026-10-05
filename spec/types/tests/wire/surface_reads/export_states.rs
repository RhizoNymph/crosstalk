//! The transmissions export's `states` scope: the default (confirmed only)
//! is written byte for byte as before states existed; an explicit set adds
//! unconfirmed rows, each with a `state` column.

use serde_json::json;

use super::super::harness::{assert_golden, assert_rejected, assert_request_golden};
use super::super::ts;
use super::export::{AREA, StandInHasher, parts, scope, scoped_basis, transmission_row};
use super::fixtures::{coder, edited, field, tx, wiki};
use crate::aggregates::quality::MatchClass;
use crate::derived::flow::transmission::Route;
use crate::interfaces::l8_surface::export::rows::TransmissionRow;
use crate::interfaces::l8_surface::export::{
    ExportDataset, ExportFormat, ExportHeader, ExportRequest, ExportRow, ExportSealer,
    ExportStates, InvalidExportRequest, InvalidExportStates, RowRefused, TransmissionScope,
};
use crate::interfaces::l8_surface::summary::{
    SummaryState, TransmissionStateKind, TransmissionSummary,
};

/// What `transmission_row(true)` and `transmission_row(false)` encoded to
/// before `states` existed (computed on 0d2dd14).
const BEFORE_WITH_CONTENT: &str = "01a719561651e30b1f3ae069a33c7e9201838a19248809a2779c6985093a7e920100448d2f7ce880a78df4c906173b7e9201d1571f39005d060001ea039f3ae266172df0b984a2397e9201d0603239005d06002f000000000000000100c89d665859448d2f7ce880273d7e92010100000101100000000000000072656c6561736520706c616e6e696e67010000000000000000002f00000000000000736869702076322e34206f6e20467269646179206166746572205141207369676e73206f6666205468757273646179000000002f0000000000000000000000000000000000000000000000000000000120272e353c434a51585f666d747b828990979ea5acb3bac1c8cfd6dde4ebf2f9";
const BEFORE_WITHOUT_CONTENT: &str = "01a719561651e30b1f3ae069a33c7e9201838a19248809a2779c6985093a7e920100448d2f7ce880a78df4c906173b7e9201d1571f39005d060001ea039f3ae266172df0b984a2397e9201d0603239005d06002f000000000000000100c89d665859448d2f7ce880273d7e920101000000";

fn hex(row: &ExportRow) -> String {
    let mut out = Vec::new();
    row.encode(&mut out);
    out.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn states(kinds: &[TransmissionStateKind]) -> ExportStates {
    ExportStates::new(kinds.to_vec()).expect("a valid set")
}

fn with_suspected() -> ExportStates {
    states(&[
        TransmissionStateKind::Suspected,
        TransmissionStateKind::Confirmed,
        TransmissionStateKind::Classified,
        TransmissionStateKind::Aggregated,
    ])
}

fn transmissions(states: ExportStates) -> ExportDataset {
    let scope = scope();
    ExportDataset::Transmissions(TransmissionScope {
        window: scope.window,
        filter: scope.filter,
        states,
    })
}

fn suspected_summary() -> TransmissionSummary {
    TransmissionSummary {
        id: tx(),
        to: coder(),
        route: Route::Channel(wiki()),
        opened_at: ts("2026-10-04T09:16:40.002513Z"),
        state: SummaryState::Suspected { verdict: None },
    }
}

fn suspected_row() -> TransmissionRow {
    TransmissionRow::new_in_scope(suspected_summary(), None, None, &with_suspected())
        .expect("a suspected row of an export holding suspected ones")
}

/// INV-1068: a default transmissions export is unchanged. Its request
/// carries no `states` (the golden `export_request_transmissions` is the
/// one written before states existed), and its rows encode, for the
/// digest, to the bytes they always did.
#[test]
fn default_transmission_exports_are_byte_identical() {
    let default = ExportRow::Transmission(Box::new(transmission_row(true)));
    assert_eq!(hex(&default), BEFORE_WITH_CONTENT);
    let without = ExportRow::Transmission(Box::new(transmission_row(false)));
    assert_eq!(hex(&without), BEFORE_WITHOUT_CONTENT);
    assert_eq!(transmission_row(true).state(), None);
    let request = ExportRequest::new(
        transmissions(ExportStates::confirmed()),
        ExportFormat::Jsonl,
        false,
    )
    .expect("a default request");
    let text = serde_json::to_string(&request).expect("encodes");
    assert!(!text.contains("states"), "{text}");
    // Naming the default set explicitly is the default: it decodes equal
    // and is written without `states`.
    let named = edited(&request, |json| {
        json["dataset"]["data"]["states"] = json!(["aggregated", "confirmed", "classified"]);
    });
    let decoded: ExportRequest = serde_json::from_str(&named).expect("decodes");
    assert_eq!(decoded, request);
}

#[test]
fn explicit_states_golden() {
    let request = ExportRequest::new(
        transmissions(ExportStates::all()),
        ExportFormat::Jsonl,
        false,
    )
    .expect("all states without content");
    assert_request_golden(AREA, "export_request_transmissions_all_states", &request);
    assert_golden(
        AREA,
        "export_row_transmissions_suspected",
        &ExportRow::Transmission(Box::new(suspected_row())),
    );
    let confirmed = transmission_row(false);
    let explicit = TransmissionRow::new_in_scope(
        confirmed.summary().clone(),
        Some(MatchClass::Exact),
        None,
        &with_suspected(),
    )
    .expect("a confirmed row of an explicit export");
    assert_eq!(explicit.state(), Some(TransmissionStateKind::Classified));
    assert_golden(
        AREA,
        "export_row_transmissions_with_state",
        &ExportRow::Transmission(Box::new(explicit)),
    );
}

#[test]
fn states_and_rows_refuse_what_their_constructors_refuse() {
    use TransmissionStateKind::{Classified, Detected, Suspected};
    assert_eq!(ExportStates::new(vec![]), Err(InvalidExportStates::Empty));
    assert_eq!(
        ExportStates::new(vec![Detected]),
        Err(InvalidExportStates::Detected)
    );
    assert_eq!(
        ExportStates::new(vec![Suspected, Suspected]),
        Err(InvalidExportStates::Duplicate(Suspected))
    );
    assert_eq!(
        ExportRequest::new(transmissions(with_suspected()), ExportFormat::Jsonl, true),
        Err(InvalidExportRequest::ContentWithUnconfirmedStates)
    );
    let request = ExportRequest::new(transmissions(with_suspected()), ExportFormat::Jsonl, false)
        .expect("valid");
    assert_rejected::<ExportRequest>(
        &edited(&request, |json| {
            json["dataset"]["data"]["states"] = json!(["detected"]);
        }),
        "invalid export states: Detected",
    );
    assert_rejected::<ExportRequest>(
        &edited(&request, |json| {
            *field(json, "include_content") = true.into()
        }),
        "invalid export request: ContentWithUnconfirmedStates",
    );
    let row = suspected_row();
    assert_rejected::<TransmissionRow>(
        &edited(&row, |json| {
            json.as_object_mut().expect("an object").remove("state");
        }),
        "invalid transmission row: NotConfirmed(Suspected)",
    );
    assert_rejected::<TransmissionRow>(
        &edited(&row, |json| json["strongest"] = "exact".into()),
        "invalid transmission row: NotConfirmed(Suspected)",
    );
    assert_rejected::<TransmissionRow>(
        &edited(&row, |json| json["state"] = "discarded".into()),
        "invalid transmission row: StateMismatch { column: Discarded, summary: Suspected }",
    );
    assert_rejected::<TransmissionRow>(
        &edited(&row, |json| {
            json["summary"]["state"] = json!({"type": "detected"});
            json["state"] = "detected".into();
        }),
        "invalid transmission row: StateNotInScope(Detected)",
    );
    assert_eq!(
        TransmissionRow::new_in_scope(suspected_summary(), None, None, &ExportStates::confirmed()),
        Err(
            crate::interfaces::l8_surface::export::rows::InvalidTransmissionRow::StateNotInScope(
                Suspected
            )
        )
    );
    let _ = Classified;
}

/// INV-1069: the sealer admits a transmission row only in the header's
/// states, with its state column exactly when they are explicit.
#[test]
fn the_sealer_checks_rows_against_the_header_states() {
    let header = |states: ExportStates| {
        let request =
            ExportRequest::new(transmissions(states), ExportFormat::Jsonl, false).expect("valid");
        ExportHeader::new(parts(request, scoped_basis(), 1)).expect("the basis is the request's")
    };
    let suspected = ExportRow::Transmission(Box::new(suspected_row()));
    let default_row = ExportRow::Transmission(Box::new(transmission_row(false)));

    let mut sealer = ExportSealer::new(&header(with_suspected()), StandInHasher::default());
    assert_eq!(sealer.push(&suspected), Ok(()));

    let mut sealer = ExportSealer::new(&header(with_suspected()), StandInHasher::default());
    assert_eq!(
        sealer.push(&default_row),
        Err(RowRefused::StateNotInScope {
            state: TransmissionStateKind::Classified
        }),
        "a row of an explicit export carries its state"
    );

    let mut sealer =
        ExportSealer::new(&header(ExportStates::confirmed()), StandInHasher::default());
    assert_eq!(
        sealer.push(&suspected),
        Err(RowRefused::StateNotInScope {
            state: TransmissionStateKind::Suspected
        })
    );
    let mut sealer =
        ExportSealer::new(&header(ExportStates::confirmed()), StandInHasher::default());
    assert_eq!(sealer.push(&default_row), Ok(()));

    let discarded_only = states(&[TransmissionStateKind::Discarded]);
    let mut sealer = ExportSealer::new(&header(discarded_only), StandInHasher::default());
    assert_eq!(
        sealer.push(&suspected),
        Err(RowRefused::StateNotInScope {
            state: TransmissionStateKind::Suspected
        })
    );
}

/// An unconfirmed row is keyed and windowed by `opened_at`; a confirmed
/// one by `Confirmed::at`.
#[test]
fn rows_are_timed_by_confirmation_or_opening() {
    let row = suspected_row();
    assert_eq!(row.at(), row.summary().opened_at);
    assert_eq!(row.delivery(), None);
    assert_eq!(row.strongest(), None);
    let confirmed = transmission_row(false);
    assert_eq!(
        Some(confirmed.at()),
        confirmed.delivery().map(|delivery| delivery.confirmed_at)
    );
}

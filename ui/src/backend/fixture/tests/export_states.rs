//! `export` of transmissions in chosen states (`TransmissionScope::states`):
//! every exportable state is served, each row in the requested states, the
//! state column written exactly when the states are not the default, and
//! every export verifies against the spec's seal. The default export is
//! unchanged.

use std::collections::BTreeSet;

use crosstalk_spec::ids::TransmissionId;
use crosstalk_spec::interfaces::l8_surface::export::{
    ExportDataset, ExportFormat, ExportRequest, ExportRow, ExportStates, InvalidExportRequest,
    TransmissionScope,
};
use crosstalk_spec::interfaces::l8_surface::summary::TransmissionStateKind;

use super::export::{run, scope, verified};
use super::{fresh, researcher, week};

fn states_request(states: ExportStates) -> ExportRequest {
    let scope = scope(&week());
    ExportRequest::new(
        ExportDataset::Transmissions(TransmissionScope {
            window: scope.window,
            filter: scope.filter,
            states,
        }),
        ExportFormat::Jsonl,
        false,
    )
    .expect("request")
}

fn only(state: TransmissionStateKind) -> ExportStates {
    ExportStates::new(vec![state]).expect("states")
}

/// The ids, states and state columns of a transmissions export's rows.
fn rows_of(
    rows: &[ExportRow],
) -> Vec<(
    TransmissionId,
    TransmissionStateKind,
    Option<TransmissionStateKind>,
)> {
    rows.iter()
        .map(|row| match row {
            ExportRow::Transmission(row) => {
                (row.summary().id, row.summary().state.kind(), row.state())
            }
            other => panic!("a transmission row, not {other:?}"),
        })
        .collect()
}

#[tokio::test]
async fn every_state_exports_its_own_rows_with_the_state_column() {
    let b = &fresh();
    let c = researcher();
    for state in ExportStates::ALL {
        let (header, rows, trailer) = run(b, &c, &states_request(only(state))).await;
        verified(&header, &rows, &trailer);
        let rows = rows_of(&rows);
        assert!(
            !rows.is_empty(),
            "the fixture's week has {state:?} transmissions"
        );
        for (id, kind, column) in rows {
            assert_eq!(kind, state, "{id:?}");
            assert_eq!(column, Some(state), "{id:?}: the state column");
        }
    }
}

#[tokio::test]
async fn every_state_together_is_the_union_of_each() {
    let b = &fresh();
    let c = researcher();
    let (header, all, trailer) = run(b, &c, &states_request(ExportStates::all())).await;
    verified(&header, &all, &trailer);
    let all: BTreeSet<_> = rows_of(&all).into_iter().map(|(id, ..)| id).collect();
    let mut union = BTreeSet::new();
    for state in ExportStates::ALL {
        let (_, rows, _) = run(b, &c, &states_request(only(state))).await;
        union.extend(rows_of(&rows).into_iter().map(|(id, ..)| id));
    }
    assert_eq!(all, union);
}

#[tokio::test]
async fn the_default_export_is_unchanged_and_has_no_state_column() {
    let b = &fresh();
    let c = researcher();
    let week = scope(&week());
    let legacy = ExportRequest::new(
        ExportDataset::Transmissions(week.clone().into()),
        ExportFormat::Jsonl,
        false,
    )
    .expect("request");
    let (header, default_rows, trailer) = run(b, &c, &legacy).await;
    verified(&header, &default_rows, &trailer);
    let (_, explicit_default, _) = run(b, &c, &states_request(ExportStates::confirmed())).await;
    assert_eq!(
        default_rows, explicit_default,
        "the confirmed default is the legacy request"
    );
    for (id, kind, column) in rows_of(&default_rows) {
        assert!(ExportStates::CONFIRMED.contains(&kind), "{id:?}");
        assert_eq!(column, None, "{id:?}: no state column by default");
    }
}

#[tokio::test]
async fn an_explicit_set_keeps_the_defaults_confirmed_rows_and_adds_the_unconfirmed() {
    let b = &fresh();
    let c = researcher();
    let (_, default_rows, _) = run(b, &c, &states_request(ExportStates::confirmed())).await;
    let mut wanted = ExportStates::CONFIRMED.to_vec();
    wanted.push(TransmissionStateKind::Suspected);
    let (header, rows, trailer) = run(
        b,
        &c,
        &states_request(ExportStates::new(wanted).expect("states")),
    )
    .await;
    verified(&header, &rows, &trailer);
    let rows = rows_of(&rows);
    let confirmed: BTreeSet<_> = rows
        .iter()
        .filter(|(_, kind, _)| ExportStates::CONFIRMED.contains(kind))
        .map(|(id, ..)| *id)
        .collect();
    let default: BTreeSet<_> = rows_of(&default_rows)
        .into_iter()
        .map(|(id, ..)| id)
        .collect();
    assert_eq!(confirmed, default, "the same confirmed transmissions");
    assert!(
        rows.iter()
            .any(|(_, kind, _)| *kind == TransmissionStateKind::Suspected),
        "and the suspected ones"
    );
    assert!(rows.iter().all(|(_, kind, column)| *column == Some(*kind)));
}

#[test]
fn content_with_an_unconfirmed_state_is_refused() {
    let scope = scope(&week());
    let refused = ExportRequest::new(
        ExportDataset::Transmissions(TransmissionScope {
            window: scope.window,
            filter: scope.filter,
            states: only(TransmissionStateKind::Suspected),
        }),
        ExportFormat::Jsonl,
        true,
    );
    assert_eq!(
        refused.err(),
        Some(InvalidExportRequest::ContentWithUnconfirmedStates)
    );
}

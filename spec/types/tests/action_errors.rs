//! The mapping from the stores behind operator actions to `ActionError`:
//! the channel registry (`SetPolicy`), the verdict store (`SetVerdict`) and
//! the topic catalog and version history (pins). Each refusal keeps the
//! variant its query mapping gives, except where an action cannot meet the
//! query's case: a dropped version and a cursor.

use crate::aggregates::retention::PinError;
use crate::aggregates::topic::TopicModelVersion;
use crate::ids::TransmissionId;
use crate::interfaces::l5_flow::RegistryError;
use crate::interfaces::l5_flow::verdicts::VerdictError;
use crate::interfaces::l6_analysis::CatalogError;
use crate::interfaces::l8_surface::export::{ExportFormat, UnsupportedFormat};
use crate::interfaces::l8_surface::{ActionError, ConflictKind, InputError, QueryError};
use crate::tests::fixtures::{at, channel};

const V: TopicModelVersion = TopicModelVersion(4);

fn store() -> String {
    "connection reset".to_owned()
}

fn every_registry_error() -> Vec<RegistryError> {
    fn declared(error: RegistryError) -> RegistryError {
        match error {
            RegistryError::Store { .. }
            | RegistryError::UnknownChannel(_)
            | RegistryError::OverlappingDeclaration { .. }
            | RegistryError::Superseded { .. }
            | RegistryError::InvalidCursor => error,
        }
    }
    [
        RegistryError::Store { reason: store() },
        RegistryError::UnknownChannel(channel(1)),
        RegistryError::OverlappingDeclaration {
            existing: channel(5),
        },
        RegistryError::Superseded {
            channel: channel(2),
            by: channel(1),
        },
        RegistryError::InvalidCursor,
    ]
    .into_iter()
    .map(declared)
    .collect()
}

#[test]
fn registry_refusals_map_to_typed_action_errors() {
    let cases = [
        (
            RegistryError::Store { reason: store() },
            ActionError::Store { reason: store() },
        ),
        (
            RegistryError::UnknownChannel(channel(1)),
            ActionError::NotFound,
        ),
        (
            RegistryError::OverlappingDeclaration {
                existing: channel(5),
            },
            ActionError::Conflict(ConflictKind::PatternOverlaps {
                existing: channel(5),
            }),
        ),
        (
            RegistryError::Superseded {
                channel: channel(2),
                by: channel(1),
            },
            ActionError::Conflict(ConflictKind::ChannelSuperseded {
                channel: channel(2),
                by: channel(1),
            }),
        ),
    ];
    for (error, expected) in cases {
        assert_eq!(ActionError::from(error.clone()), expected, "{error:?}");
    }
    // An action takes no cursor: a registry reporting one is a fault.
    assert!(matches!(
        ActionError::from(RegistryError::InvalidCursor),
        ActionError::Store { .. }
    ));
}

#[test]
fn a_registry_refusal_reads_the_same_from_a_query_and_an_action() {
    for error in every_registry_error() {
        if error == RegistryError::InvalidCursor {
            continue;
        }
        assert_eq!(
            QueryError::from(ActionError::from(error.clone())),
            QueryError::from(error.clone()),
            "{error:?}"
        );
    }
}

#[test]
fn verdict_refusals_map_to_typed_action_errors() {
    let transmission = TransmissionId::from_ulid(3);
    let cases = [
        (
            VerdictError::Store { reason: store() },
            ActionError::Store { reason: store() },
        ),
        (
            VerdictError::UnknownTransmission(transmission),
            ActionError::NotFound,
        ),
        (
            VerdictError::NotJudgeable(transmission),
            ActionError::Conflict(ConflictKind::TransmissionNotJudgeable { transmission }),
        ),
    ];
    for (error, expected) in cases {
        assert_eq!(ActionError::from(error.clone()), expected, "{error:?}");
        // The query mapping gives the same variant.
        assert_eq!(QueryError::from(error), QueryError::from(expected));
    }
}

#[test]
fn catalog_refusals_of_a_pin_map_to_typed_action_errors() {
    let cases = [
        (
            CatalogError::Store { reason: store() },
            ActionError::Store { reason: store() },
        ),
        (CatalogError::UnknownVersion(V), ActionError::NotFound),
        (
            CatalogError::StillFitting(V),
            ActionError::Conflict(ConflictKind::TopicVersionFitting { version: V }),
        ),
        // Pinning reads no dropped data: a dropped version is a conflict,
        // where a query reading it is `VersionNotRetained`.
        (
            CatalogError::VersionNotRetained(V),
            ActionError::Conflict(ConflictKind::TopicVersionDropped { version: V }),
        ),
    ];
    for (error, expected) in cases {
        assert_eq!(ActionError::from(error.clone()), expected, "{error:?}");
    }
    assert_eq!(
        QueryError::from(CatalogError::VersionNotRetained(V)),
        QueryError::VersionNotRetained { version: V }
    );
    assert!(matches!(
        ActionError::from(CatalogError::InvalidCursor),
        ActionError::Store { .. }
    ));
}

#[test]
fn history_refusals_of_a_pin_map_as_the_catalog_refusals_do() {
    let cases = [
        (PinError::UnknownVersion(V), CatalogError::UnknownVersion(V)),
        (PinError::Fitting(V), CatalogError::StillFitting(V)),
        (
            PinError::Dropped {
                version: V,
                at: at(9),
            },
            CatalogError::VersionNotRetained(V),
        ),
    ];
    for (pin, catalog) in cases {
        assert_eq!(
            ActionError::from(pin),
            ActionError::from(catalog),
            "{pin:?}"
        );
    }
    assert_eq!(
        ActionError::from(PinError::Dropped {
            version: V,
            at: at(9)
        }),
        ActionError::Conflict(ConflictKind::TopicVersionDropped { version: V })
    );
}

#[test]
fn an_unwritten_export_format_is_invalid_input() {
    assert_eq!(
        QueryError::from(UnsupportedFormat {
            format: ExportFormat::Parquet
        }),
        QueryError::InvalidInput(InputError::UnsupportedFormat {
            format: ExportFormat::Parquet
        })
    );
}

//! The matrix encoding is exact and refuses everything the contract
//! refuses.

use std::num::NonZeroU16;

use proptest::prelude::*;

use crate::remote::matrix::{Matrix, MatrixError};

fn columns(n: u16) -> NonZeroU16 {
    NonZeroU16::new(n).unwrap()
}

fn matrix(json: serde_json::Value) -> Matrix {
    serde_json::from_value(json).unwrap()
}

#[test]
fn encodes_little_endian_row_major_lower_hex() {
    let rows: [&[f32]; 2] = [&[1.0, 0.0], &[2.0, -0.5]];
    let encoded = Matrix::encode(columns(2), rows).unwrap();
    assert_eq!(
        serde_json::to_string(&encoded).unwrap(),
        r#"{"rows":2,"columns":2,"data":"0000803f0000000000000040000000bf"}"#
    );
    assert_eq!(encoded.decode().unwrap(), vec![1.0, 0.0, 2.0, -0.5]);
}

#[test]
fn encode_refuses_ragged_rows_and_non_finite_values() {
    let ragged: [&[f32]; 2] = [&[1.0, 0.0], &[2.0]];
    assert_eq!(
        Matrix::encode(columns(2), ragged),
        Err(MatrixError::RowLength {
            row: 1,
            expected: 2,
            got: 1
        })
    );
    let nan: [&[f32]; 1] = [&[1.0, f32::NAN]];
    assert_eq!(
        Matrix::encode(columns(2), nan),
        Err(MatrixError::NotFinite { index: 1 })
    );
}

#[test]
fn decode_refuses_upper_case_wrong_length_and_non_finite() {
    let upper = matrix(serde_json::json!({"rows": 1, "columns": 1, "data": "0000803F"}));
    assert!(matches!(upper.decode(), Err(MatrixError::Hex(_))));
    let short = matrix(serde_json::json!({"rows": 2, "columns": 1, "data": "0000803f"}));
    assert_eq!(
        short.decode(),
        Err(MatrixError::DataLength {
            expected: 8,
            got: 4
        })
    );
    let nan = matrix(serde_json::json!({"rows": 1, "columns": 2, "data": "0000803f0000c07f"}));
    assert_eq!(nan.decode(), Err(MatrixError::NotFinite { index: 1 }));
    let infinite = matrix(serde_json::json!({"rows": 1, "columns": 1, "data": "0000807f"}));
    assert_eq!(infinite.decode(), Err(MatrixError::NotFinite { index: 0 }));
}

#[test]
fn decoding_refuses_zero_columns_and_unknown_fields() {
    assert!(
        serde_json::from_value::<Matrix>(serde_json::json!({"rows": 0, "columns": 0, "data": ""}))
            .is_err()
    );
    assert!(
        serde_json::from_value::<Matrix>(
            serde_json::json!({"rows": 0, "columns": 1, "data": "", "dtype": "f32"})
        )
        .is_err()
    );
}

proptest! {
    #[test]
    fn prop_matrix_round_trips_bits(
        width in 1u16..6,
        bits in proptest::collection::vec(any::<u32>(), 0..60),
    ) {
        let values: Vec<f32> = bits
            .into_iter()
            .map(f32::from_bits)
            .filter(|value| value.is_finite())
            .collect();
        let width_usize = usize::from(width);
        let whole = values.len() - values.len() % width_usize;
        let rows: Vec<&[f32]> = values[..whole].chunks(width_usize).collect();
        let encoded = Matrix::encode(columns(width), rows).unwrap();
        let json = serde_json::to_string(&encoded).unwrap();
        let decoded: Matrix = serde_json::from_str(&json).unwrap();
        let back = decoded.decode().unwrap();
        prop_assert_eq!(
            back.iter().map(|v| v.to_bits()).collect::<Vec<_>>(),
            values[..whole].iter().map(|v| v.to_bits()).collect::<Vec<_>>()
        );
    }
}

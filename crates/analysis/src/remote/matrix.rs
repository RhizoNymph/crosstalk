//! The sidecar's matrix encoding: `rows × columns` IEEE-754 binary32
//! values, little-endian, row-major, as lower-case hex
//! (`docs/features/topics_sidecar.md`, "Matrices"). Exact both ways: the
//! bits encoded are the bits decoded.

use std::num::NonZeroU16;

use crosstalk_spec::support::{InvalidHex, from_hex, hex};
use serde::{Deserialize, Serialize};

/// A matrix as it travels.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct Matrix {
    rows: u32,
    columns: NonZeroU16,
    data: String,
}

/// Why values cannot be encoded, or a matrix decoded.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum MatrixError {
    #[error("more than u32::MAX rows")]
    TooManyRows,
    #[error("row {row} has {got} values, not {expected}")]
    RowLength {
        row: usize,
        expected: u16,
        got: usize,
    },
    #[error("the data is not lower-case hex: {0:?}")]
    Hex(InvalidHex),
    #[error("the data holds {got} bytes, not rows × columns × 4 = {expected}")]
    DataLength { expected: u64, got: usize },
    #[error("value {index} is NaN or infinite")]
    NotFinite { index: usize },
}

impl Matrix {
    /// Encode `rows`, each exactly `columns` long and finite.
    pub fn encode<'a>(
        columns: NonZeroU16,
        rows: impl IntoIterator<Item = &'a [f32]>,
    ) -> Result<Self, MatrixError> {
        let width = usize::from(columns.get());
        let mut bytes = Vec::new();
        let mut count = 0usize;
        for (row, values) in rows.into_iter().enumerate() {
            if values.len() != width {
                return Err(MatrixError::RowLength {
                    row,
                    expected: columns.get(),
                    got: values.len(),
                });
            }
            for (offset, value) in values.iter().enumerate() {
                if !value.is_finite() {
                    return Err(MatrixError::NotFinite {
                        index: row * width + offset,
                    });
                }
                bytes.extend_from_slice(&value.to_le_bytes());
            }
            count += 1;
        }
        let rows = u32::try_from(count).map_err(|_| MatrixError::TooManyRows)?;
        Ok(Self {
            rows,
            columns,
            data: hex(&bytes),
        })
    }

    pub fn rows(&self) -> u32 {
        self.rows
    }

    pub fn columns(&self) -> NonZeroU16 {
        self.columns
    }

    /// Every value, row-major, checked: the data is lower-case hex of
    /// exactly `rows × columns` finite values.
    pub fn decode(&self) -> Result<Vec<f32>, MatrixError> {
        let bytes = from_hex(&self.data).map_err(MatrixError::Hex)?;
        let expected = u64::from(self.rows) * u64::from(self.columns.get()) * 4;
        if u64::try_from(bytes.len()).ok() != Some(expected) {
            return Err(MatrixError::DataLength {
                expected,
                got: bytes.len(),
            });
        }
        // The length check above leaves no remainder.
        bytes
            .as_chunks::<4>()
            .0
            .iter()
            .enumerate()
            .map(|(index, chunk)| {
                let value = f32::from_le_bytes(*chunk);
                if value.is_finite() {
                    Ok(value)
                } else {
                    Err(MatrixError::NotFinite { index })
                }
            })
            .collect()
    }
}

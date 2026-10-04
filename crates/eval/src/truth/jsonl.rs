//! Truth as JSONL: one [`Expectation`] per line.

use std::io::{BufRead, Write};

use super::Expectation;

#[derive(Debug, thiserror::Error)]
pub enum TruthIoError {
    #[error("writing truth: {0}")]
    Write(#[source] std::io::Error),
    #[error("reading truth line {line}: {source}")]
    Read {
        line: usize,
        #[source]
        source: std::io::Error,
    },
    #[error("encoding a label: {0}")]
    Encode(#[source] serde_json::Error),
    #[error("truth line {line} is not a label: {source}")]
    Decode {
        line: usize,
        #[source]
        source: serde_json::Error,
    },
}

/// Writes each expectation as one JSON line.
pub fn write<'a, W: Write>(
    out: &mut W,
    expectations: impl IntoIterator<Item = &'a Expectation>,
) -> Result<(), TruthIoError> {
    for expectation in expectations {
        let line = serde_json::to_string(expectation).map_err(TruthIoError::Encode)?;
        out.write_all(line.as_bytes())
            .map_err(TruthIoError::Write)?;
        out.write_all(b"\n").map_err(TruthIoError::Write)?;
    }
    Ok(())
}

/// Reads expectations back, one per non-empty line, in order.
pub fn read<R: BufRead>(input: R) -> impl Iterator<Item = Result<Expectation, TruthIoError>> {
    input.lines().enumerate().filter_map(|(at, line)| {
        let line_no = at + 1;
        match line {
            Err(source) => Some(Err(TruthIoError::Read {
                line: line_no,
                source,
            })),
            Ok(text) if text.trim().is_empty() => None,
            Ok(text) => Some(
                serde_json::from_str(&text).map_err(|source| TruthIoError::Decode {
                    line: line_no,
                    source,
                }),
            ),
        }
    })
}

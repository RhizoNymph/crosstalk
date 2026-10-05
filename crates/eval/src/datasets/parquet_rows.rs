//! Reading Parquet shards row by row into typed records.
//!
//! [`ParquetRows`] streams one file's rows through the `parquet` crate's
//! record reader, projected to the columns a converter names, and decodes
//! each row (as JSON) into the converter's serde type. Pages are read as the
//! iterator advances, so a shard whose single row group is close to a
//! gigabyte uncompressed is never held whole. A file can also be read one
//! row group at a time ([`ParquetRows::open_group`]), to start reading
//! part-way through a file sorted by something a sample should spread over.

use std::fs::File;
use std::marker::PhantomData;
use std::path::{Path, PathBuf};

use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::file::serialized_reader::ReadOptionsBuilder;
use parquet::record::reader::RowIter;
use parquet::schema::types::Type;
use serde::de::DeserializeOwned;

#[derive(Debug, thiserror::Error)]
pub enum ParquetError {
    #[error("opening {path}: {source}")]
    Open {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("reading {path}: {source}")]
    Read {
        path: String,
        #[source]
        source: parquet::errors::ParquetError,
    },
    #[error("{path} has no column {column:?}")]
    MissingColumn { path: String, column: String },
    #[error("{path} row {row} is not the expected record: {source}")]
    Decode {
        path: String,
        row: usize,
        #[source]
        source: serde_json::Error,
    },
}

/// The rows of one Parquet file, decoded as `T`, with their row numbers.
pub struct ParquetRows<T> {
    path: PathBuf,
    rows: RowIter<'static>,
    /// The file row number of the next row.
    next: usize,
    _record: PhantomData<fn() -> T>,
}

impl<T: DeserializeOwned> ParquetRows<T> {
    /// Opens `path`, reading only the top-level `columns`.
    pub fn open(path: &Path, columns: &[&str]) -> Result<Self, ParquetError> {
        let reader =
            SerializedFileReader::new(open_file(path)?).map_err(|source| ParquetError::Read {
                path: path.display().to_string(),
                source,
            })?;
        Self::over(path, reader, columns, 0)
    }

    /// Opens row group `group` of `path` only; row numbers stay the file's.
    pub fn open_group(path: &Path, columns: &[&str], group: usize) -> Result<Self, ParquetError> {
        let first_row = row_groups(path)?.into_iter().take(group).sum();
        let options = ReadOptionsBuilder::new()
            .with_predicate(Box::new(move |_, at| at == group))
            .build();
        let reader = SerializedFileReader::new_with_options(open_file(path)?, options).map_err(
            |source| ParquetError::Read {
                path: path.display().to_string(),
                source,
            },
        )?;
        Self::over(path, reader, columns, first_row)
    }

    fn over(
        path: &Path,
        reader: SerializedFileReader<File>,
        columns: &[&str],
        first_row: usize,
    ) -> Result<Self, ParquetError> {
        let shown = || path.display().to_string();
        let schema = reader.metadata().file_metadata().schema();
        let mut fields = Vec::with_capacity(columns.len());
        for column in columns {
            let field = schema
                .get_fields()
                .iter()
                .find(|field| field.name() == *column)
                .ok_or_else(|| ParquetError::MissingColumn {
                    path: shown(),
                    column: (*column).to_owned(),
                })?;
            fields.push(field.clone());
        }
        let projection = Type::group_type_builder(schema.name())
            .with_fields(fields)
            .build()
            .map_err(|source| ParquetError::Read {
                path: shown(),
                source,
            })?;
        let rows = RowIter::from_file_into(Box::new(reader))
            .project(Some(projection))
            .map_err(|source| ParquetError::Read {
                path: shown(),
                source,
            })?;
        Ok(Self {
            path: path.to_path_buf(),
            rows,
            next: first_row,
            _record: PhantomData,
        })
    }
}

fn open_file(path: &Path) -> Result<File, ParquetError> {
    File::open(path).map_err(|source| ParquetError::Open {
        path: path.display().to_string(),
        source,
    })
}

/// The number of rows in each row group of `path`.
pub fn row_groups(path: &Path) -> Result<Vec<usize>, ParquetError> {
    let reader =
        SerializedFileReader::new(open_file(path)?).map_err(|source| ParquetError::Read {
            path: path.display().to_string(),
            source,
        })?;
    Ok(reader
        .metadata()
        .row_groups()
        .iter()
        .map(|group| usize::try_from(group.num_rows()).unwrap_or(0))
        .collect())
}

impl<T: DeserializeOwned> Iterator for ParquetRows<T> {
    type Item = Result<(usize, T), ParquetError>;

    fn next(&mut self) -> Option<Self::Item> {
        let row = self.rows.next()?;
        let at = self.next;
        self.next += 1;
        let path = || self.path.display().to_string();
        Some(
            row.map_err(|source| ParquetError::Read {
                path: path(),
                source,
            })
            .and_then(|row| {
                serde_json::from_value(row.to_json_value()).map_err(|source| ParquetError::Decode {
                    path: path(),
                    row: at,
                    source,
                })
            })
            .map(|record| (at, record)),
        )
    }
}

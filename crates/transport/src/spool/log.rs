//! [`SpoolLog`]: the spool's files and its in-memory index of them. Every
//! method does blocking filesystem work; the bus runs each call on the
//! blocking pool (`spawn_blocking`), holding the log's mutex only there.
//!
//! The directory holds `LOCK`, `cursor` and `segment-<first record number,
//! 20 digits>.log` files. The fsync points:
//!
//! 1. an append writes its record and `fdatasync`s the segment before it
//!    returns, so an `Ok` publish is durable;
//! 2. a new segment's header is `fsync`ed, then the directory, before its
//!    first record is written;
//! 3. the cursor is replaced atomically (`cursor`), after the inner bus
//!    committed the batch it covers;
//! 4. a segment is removed only once the cursor is past its last record,
//!    then the directory is `fsync`ed.
//!
//! Recovery ([`SpoolLog::open`]) reads every segment from the cursor on. An
//! incomplete record, or one that fails its checksum and ends the file, at
//! the end of the last segment is a torn append: its publish never
//! returned `Ok`, so it is truncated and counted. Any other bad record is
//! a corruption barrier: draining stops before it, and the bytes stay for
//! an operator (`transport.spool.torn-tail-only`).

use std::collections::{BTreeMap, VecDeque};
use std::fs::{self, File, OpenOptions, TryLockError};
use std::io::Write;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};

use crosstalk_spec::support::Timestamp;
use serde::Deserialize;

use super::config::SpoolConfig;
use super::cursor::{self, CursorFile, sync_dir};
use super::error::{SpoolError, SpoolOp};
use super::record::{self, MAX_PAYLOAD, Parsed, RECORD_HEADER_LEN, SEGMENT_HEADER};

pub(crate) const LOCK: &str = "LOCK";
const HEADER_LEN: u64 = SEGMENT_HEADER.len() as u64;

/// A segment's file name.
pub(crate) fn segment_name(first: u64) -> String {
    format!("segment-{first:020}.log")
}

fn parse_segment_name(name: &str) -> Option<u64> {
    let digits = name.strip_prefix("segment-")?.strip_suffix(".log")?;
    if digits.len() != 20 || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    digits.parse().ok()
}

/// One record still to drain.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RecordMeta {
    pub(crate) number: u64,
    /// The first record number of its segment (the segment's name).
    pub(crate) segment: u64,
    /// Where the record starts in the segment.
    pub(crate) offset: u64,
    /// The whole record's size, header included.
    pub(crate) size: u64,
    /// The envelope's `at`, in microseconds.
    pub(crate) at: u64,
}

/// Where draining stops: a corrupt record, and how many pending records
/// come before it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Barrier {
    pub(crate) segment: u64,
    pub(crate) offset: u64,
    pub(crate) before: usize,
}

/// Counters since the spool was opened.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct Counters {
    pub(crate) appended: u64,
    pub(crate) drained: u64,
    pub(crate) rejected_full: u64,
    pub(crate) rejected_io: u64,
    pub(crate) truncated_bytes: u64,
}

/// Why an append wrote nothing.
#[derive(Debug)]
pub(crate) enum AppendError {
    /// The spool holds `bytes` and the record would take it past its bound.
    Full {
        bytes: u64,
    },
    /// The payload exceeds [`MAX_PAYLOAD`].
    TooLarge,
    Io(SpoolError),
}

/// Why a drain read failed.
#[derive(Debug)]
pub(crate) enum ReadFailure {
    /// The record at `index` in the batch no longer reads back intact.
    Corrupt {
        index: usize,
        segment: u64,
        offset: u64,
    },
    Io(SpoolError),
}

/// What a discard of a corruption removed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Discarded {
    /// The segment file the corruption was in.
    pub segment: String,
    /// Where the discarded bytes started.
    pub offset: u64,
    /// How many bytes were removed (the corrupt record and everything
    /// after it in that segment).
    pub bytes: u64,
}

#[derive(Debug)]
struct Active {
    first: u64,
    file: File,
    len: u64,
}

/// The spool's files and index. See the [module docs](self).
#[derive(Debug)]
pub(crate) struct SpoolLog {
    dir: PathBuf,
    /// Held for the log's life; closing it releases the lock.
    _lock: File,
    max_bytes: u64,
    segment_bytes: u64,
    /// Every segment file, by first record number, with its length.
    segments: BTreeMap<u64, u64>,
    active: Option<Active>,
    pending: VecDeque<RecordMeta>,
    /// Multiset of pending records' `at`, for `oldest_at`.
    ats: BTreeMap<u64, usize>,
    next_number: u64,
    cursor: CursorFile,
    bytes: u64,
    barrier: Option<Barrier>,
    counters: Counters,
}

/// The part of an envelope recovery needs: its time.
#[derive(Deserialize)]
struct At {
    at: Timestamp,
}

fn envelope_at(payload: &[u8]) -> u64 {
    match serde_json::from_slice::<At>(payload) {
        Ok(at) => at.at.as_micros(),
        Err(_) => {
            // Checksummed bytes this binary cannot read: count it as the
            // oldest possible input, which holds the watermark back.
            tracing::warn!("a spooled record's time does not decode; counting it as the epoch");
            0
        }
    }
}

impl SpoolLog {
    /// Open and recover the spool in `config.dir()`, taking its `LOCK`.
    pub(crate) fn open(config: &SpoolConfig) -> Result<Self, SpoolError> {
        let dir = config.dir().to_path_buf();
        fs::create_dir_all(&dir).map_err(SpoolError::io(SpoolOp::CreateDir, &dir))?;
        let lock_path = dir.join(LOCK);
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(&lock_path)
            .map_err(SpoolError::io(SpoolOp::Lock, &lock_path))?;
        match lock.try_lock() {
            Ok(()) => {}
            Err(TryLockError::WouldBlock) => return Err(SpoolError::Locked { dir }),
            Err(TryLockError::Error(error)) => {
                return Err(SpoolError::io(SpoolOp::Lock, lock_path)(error));
            }
        }
        cursor::remove_leftover(&dir)?;
        let cursor = cursor::read(&dir)?;
        let mut log = Self {
            dir,
            _lock: lock,
            max_bytes: config.max_bytes(),
            segment_bytes: config.segment_bytes(),
            segments: BTreeMap::new(),
            active: None,
            pending: VecDeque::new(),
            ats: BTreeMap::new(),
            next_number: 1,
            cursor,
            bytes: 0,
            barrier: None,
            counters: Counters::default(),
        };
        log.recover()?;
        Ok(log)
    }

    fn segment_path(&self, first: u64) -> PathBuf {
        self.dir.join(segment_name(first))
    }

    fn list_segments(&self) -> Result<BTreeMap<u64, PathBuf>, SpoolError> {
        let entries = fs::read_dir(&self.dir).map_err(SpoolError::io(SpoolOp::List, &self.dir))?;
        let mut segments = BTreeMap::new();
        for entry in entries {
            let entry = entry.map_err(SpoolError::io(SpoolOp::List, &self.dir))?;
            let name = entry.file_name();
            if let Some(first) = name.to_str().and_then(parse_segment_name) {
                segments.insert(first, entry.path());
            }
        }
        Ok(segments)
    }

    fn set_barrier(&mut self, segment: u64, offset: u64) {
        if self.barrier.is_none() {
            tracing::error!(segment = %segment_name(segment), offset, "spool corruption: draining stops here");
            self.barrier = Some(Barrier {
                segment,
                offset,
                before: self.pending.len(),
            });
        }
    }

    fn push(&mut self, meta: RecordMeta) {
        *self.ats.entry(meta.at).or_default() += 1;
        self.pending.push_back(meta);
    }

    fn truncate_tail(
        &mut self,
        first: u64,
        path: &Path,
        len: u64,
        offset: u64,
    ) -> Result<(), SpoolError> {
        let cut = len - offset;
        if offset < HEADER_LEN {
            fs::remove_file(path).map_err(SpoolError::io(SpoolOp::Remove, path))?;
            sync_dir(&self.dir)?;
            self.segments.remove(&first);
        } else {
            let file = OpenOptions::new()
                .write(true)
                .open(path)
                .map_err(SpoolError::io(SpoolOp::Truncate, path))?;
            file.set_len(offset)
                .and_then(|()| file.sync_all())
                .map_err(SpoolError::io(SpoolOp::Truncate, path))?;
            self.segments.insert(first, offset);
        }
        self.counters.truncated_bytes += cut;
        tracing::warn!(segment = %segment_name(first), offset, bytes = cut, "torn spool append truncated");
        Ok(())
    }

    fn recover(&mut self) -> Result<(), SpoolError> {
        let segments = self.list_segments()?;
        let last_first = segments.keys().next_back().copied();
        let mut prev = self.cursor.last;
        let mut removed = false;
        for (first, path) in segments {
            if first < self.cursor.segment {
                // Every record in it is at or before the cursor.
                fs::remove_file(&path).map_err(SpoolError::io(SpoolOp::Remove, &path))?;
                removed = true;
                continue;
            }
            let bytes = fs::read(&path).map_err(SpoolError::io(SpoolOp::Read, &path))?;
            let len = bytes.len() as u64;
            let is_last = Some(first) == last_first;
            self.segments.insert(first, len);
            let header_ok = bytes.get(..SEGMENT_HEADER.len()) == Some(SEGMENT_HEADER.as_slice());
            if !header_ok {
                if is_last && SEGMENT_HEADER.starts_with(&bytes) {
                    // Created, header not yet durable: no record was in it.
                    self.truncate_tail(first, &path, len, 0)?;
                } else {
                    self.set_barrier(first, 0);
                }
                continue;
            }
            let mut offset = if first == self.cursor.segment {
                self.cursor.offset.max(HEADER_LEN)
            } else {
                HEADER_LEN
            };
            if offset > len {
                self.set_barrier(first, len);
                continue;
            }
            while offset < len {
                // `offset < len` and `len` came from a Vec's length.
                let rest = &bytes[offset as usize..];
                match record::parse(rest) {
                    Parsed::Record {
                        number,
                        payload,
                        size,
                    } => {
                        if number <= prev {
                            self.set_barrier(first, offset);
                            break;
                        }
                        let meta = RecordMeta {
                            number,
                            segment: first,
                            offset,
                            size: size as u64,
                            at: envelope_at(payload),
                        };
                        self.push(meta);
                        prev = number;
                        offset += size as u64;
                    }
                    Parsed::Short if is_last => {
                        self.truncate_tail(first, &path, len, offset)?;
                        break;
                    }
                    Parsed::Bad { size } if is_last && offset + size as u64 == len => {
                        self.truncate_tail(first, &path, len, offset)?;
                        break;
                    }
                    Parsed::Short | Parsed::Bad { .. } => {
                        self.set_barrier(first, offset);
                        break;
                    }
                }
            }
        }
        if removed {
            sync_dir(&self.dir)?;
        }
        self.next_number = prev + 1;
        self.bytes = self.segments.values().sum();
        self.delete_drained()?;
        tracing::info!(
            dir = %self.dir.display(),
            records = self.pending.len(),
            bytes = self.bytes,
            truncated_bytes = self.counters.truncated_bytes,
            corrupt = self.barrier.is_some(),
            "spool opened"
        );
        Ok(())
    }

    /// Remove every segment no pending record is in, except the active one
    /// and the one holding a corruption barrier.
    fn delete_drained(&mut self) -> Result<(), SpoolError> {
        let keep_from = self.pending.front().map_or(u64::MAX, |meta| meta.segment);
        let active = self.active.as_ref().map(|a| a.first);
        let barrier = self.barrier.map(|b| b.segment);
        let doomed: Vec<u64> = self
            .segments
            .keys()
            .copied()
            .filter(|first| *first < keep_from && Some(*first) != active && Some(*first) != barrier)
            .collect();
        for first in &doomed {
            let path = self.segment_path(*first);
            fs::remove_file(&path).map_err(SpoolError::io(SpoolOp::Remove, &path))?;
            if let Some(len) = self.segments.remove(first) {
                self.bytes -= len;
            }
        }
        if !doomed.is_empty() {
            sync_dir(&self.dir)?;
        }
        Ok(())
    }

    /// Start a new segment for the next record: header written and synced,
    /// then the directory synced.
    fn roll(&mut self) -> Result<(), SpoolError> {
        let first = self.next_number;
        let path = self.segment_path(first);
        let mut file = OpenOptions::new()
            .create_new(true)
            .append(true)
            .open(&path)
            .map_err(SpoolError::io(SpoolOp::Create, &path))?;
        let written = file
            .write_all(SEGMENT_HEADER)
            .and_then(|()| file.sync_all())
            .map_err(SpoolError::io(SpoolOp::Write, &path))
            .and_then(|()| sync_dir(&self.dir));
        if let Err(error) = written {
            drop(file);
            // Best effort: a leftover header-only file is removed at open.
            let _ = fs::remove_file(&path);
            return Err(error);
        }
        self.segments.insert(first, HEADER_LEN);
        self.bytes += HEADER_LEN;
        self.active = Some(Active {
            first,
            file,
            len: HEADER_LEN,
        });
        tracing::debug!(segment = %segment_name(first), "spool segment started");
        Ok(())
    }

    /// Append one record durably (`fdatasync` before returning `Ok`).
    pub(crate) fn append(&mut self, payload: &[u8], at: Timestamp) -> Result<(), AppendError> {
        if payload.len() > MAX_PAYLOAD {
            return Err(AppendError::TooLarge);
        }
        let size = (RECORD_HEADER_LEN + payload.len()) as u64;
        let roll = match &self.active {
            None => true,
            Some(active) => active.len > HEADER_LEN && active.len + size > self.segment_bytes,
        };
        let extra = size + if roll { HEADER_LEN } else { 0 };
        if self.bytes + extra > self.max_bytes {
            self.counters.rejected_full += 1;
            return Err(AppendError::Full { bytes: self.bytes });
        }
        if roll && let Err(error) = self.roll() {
            self.counters.rejected_io += 1;
            return Err(AppendError::Io(error));
        }
        let number = self.next_number;
        let bytes = record::encode(number, payload);
        let path = self.segment_path(self.active.as_ref().map_or(number, |a| a.first));
        let Some(active) = self.active.as_mut() else {
            self.counters.rejected_io += 1;
            return Err(AppendError::Io(SpoolError::Interrupted));
        };
        if let Err(error) = active
            .file
            .write_all(&bytes)
            .and_then(|()| active.file.sync_data())
        {
            self.counters.rejected_io += 1;
            // Undo a partial write, so the segment never holds a bad
            // record before a good one. If that fails too, stop appending
            // to this segment; recovery then reports the damage as
            // corruption instead of skipping it.
            let undone = active
                .file
                .set_len(active.len)
                .and_then(|()| active.file.sync_data());
            if undone.is_err() {
                tracing::error!(segment = %path.display(), "could not undo a failed spool append; rolling");
                self.active = None;
            }
            return Err(AppendError::Io(SpoolError::io(SpoolOp::Write, path)(error)));
        }
        let meta = RecordMeta {
            number,
            segment: active.first,
            offset: active.len,
            size,
            at: at.as_micros(),
        };
        active.len += size;
        self.segments.insert(active.first, active.len);
        self.bytes += size;
        self.next_number += 1;
        self.counters.appended += 1;
        self.push(meta);
        Ok(())
    }

    /// The next records to drain, at most `max`, never past a barrier.
    pub(crate) fn batch(&self, max: usize) -> Vec<RecordMeta> {
        let drainable = self.barrier.map_or(self.pending.len(), |b| b.before);
        self.pending
            .iter()
            .take(drainable.min(max))
            .copied()
            .collect()
    }

    /// Read the payloads of `metas` back, checking each record again.
    pub(crate) fn read(&self, metas: &[RecordMeta]) -> Result<Vec<Vec<u8>>, ReadFailure> {
        let mut payloads = Vec::with_capacity(metas.len());
        let mut open: Option<(u64, File)> = None;
        for (index, meta) in metas.iter().enumerate() {
            let path = self.segment_path(meta.segment);
            if open
                .as_ref()
                .is_none_or(|(first, _)| *first != meta.segment)
            {
                let file = File::open(&path)
                    .map_err(|e| ReadFailure::Io(SpoolError::io(SpoolOp::Read, &path)(e)))?;
                open = Some((meta.segment, file));
            }
            let Some((_, file)) = open.as_ref() else {
                return Err(ReadFailure::Io(SpoolError::Interrupted));
            };
            let corrupt = ReadFailure::Corrupt {
                index,
                segment: meta.segment,
                offset: meta.offset,
            };
            let Ok(size) = usize::try_from(meta.size) else {
                return Err(corrupt);
            };
            let mut bytes = vec![0u8; size];
            if file.read_exact_at(&mut bytes, meta.offset).is_err() {
                return Err(corrupt);
            }
            match record::parse(&bytes) {
                Parsed::Record {
                    number, payload, ..
                } if number == meta.number => payloads.push(payload.to_vec()),
                _ => return Err(corrupt),
            }
        }
        Ok(payloads)
    }

    /// Stop draining at the `index`-th pending record, which no longer
    /// reads back intact.
    pub(crate) fn mark_corrupt(&mut self, index: usize, segment: u64, offset: u64) {
        let earlier = self.barrier.is_none_or(|b| index < b.before);
        if earlier {
            self.barrier = None;
            tracing::error!(segment = %segment_name(segment), offset, "spool record unreadable: draining stops here");
            self.barrier = Some(Barrier {
                segment,
                offset,
                before: index,
            });
        }
    }

    /// The first `n` pending records are in the inner bus: move the cursor
    /// past them (atomically), forget them, and remove segments now fully
    /// drained.
    pub(crate) fn advance(&mut self, n: usize) -> Result<(), SpoolError> {
        let Some(last) = n.checked_sub(1).and_then(|i| self.pending.get(i)).copied() else {
            return Ok(());
        };
        let next = CursorFile {
            last: last.number,
            segment: last.segment,
            offset: last.offset + last.size,
        };
        cursor::write(&self.dir, &next)?;
        self.cursor = next;
        for _ in 0..n {
            if let Some(meta) = self.pending.pop_front()
                && let Some(count) = self.ats.get_mut(&meta.at)
            {
                *count -= 1;
                if *count == 0 {
                    self.ats.remove(&meta.at);
                }
            }
        }
        if let Some(barrier) = self.barrier.as_mut() {
            barrier.before = barrier.before.saturating_sub(n);
        }
        self.counters.drained += n as u64;
        self.delete_drained()
    }

    /// The spool is empty: remove every segment, the active one included.
    pub(crate) fn clear(&mut self) -> Result<(), SpoolError> {
        if !self.pending.is_empty() {
            return Ok(());
        }
        self.active = None;
        self.delete_drained()
    }

    /// Cut the corrupt record and everything after it in its segment, so
    /// the next open drains past it. `None` when there is no corruption.
    pub(crate) fn discard_corrupt(&mut self) -> Result<Option<Discarded>, SpoolError> {
        let Some(barrier) = self.barrier else {
            return Ok(None);
        };
        let path = self.segment_path(barrier.segment);
        let len = self.segments.get(&barrier.segment).copied().unwrap_or(0);
        let bytes = len.saturating_sub(barrier.offset);
        if barrier.offset < HEADER_LEN {
            fs::remove_file(&path).map_err(SpoolError::io(SpoolOp::Remove, &path))?;
            self.segments.remove(&barrier.segment);
        } else {
            let file = OpenOptions::new()
                .write(true)
                .open(&path)
                .map_err(SpoolError::io(SpoolOp::Truncate, &path))?;
            file.set_len(barrier.offset)
                .and_then(|()| file.sync_all())
                .map_err(SpoolError::io(SpoolOp::Truncate, &path))?;
            self.segments.insert(barrier.segment, barrier.offset);
        }
        sync_dir(&self.dir)?;
        self.bytes = self.segments.values().sum();
        self.barrier = None;
        tracing::warn!(segment = %segment_name(barrier.segment), offset = barrier.offset, bytes, "spool corruption discarded");
        Ok(Some(Discarded {
            segment: segment_name(barrier.segment),
            offset: barrier.offset,
            bytes,
        }))
    }

    pub(crate) fn records(&self) -> usize {
        self.pending.len()
    }

    pub(crate) fn bytes(&self) -> u64 {
        self.bytes
    }

    pub(crate) fn oldest_at(&self) -> Option<Timestamp> {
        self.ats.keys().next().map(|at| Timestamp::from_micros(*at))
    }

    pub(crate) fn barrier(&self) -> Option<Barrier> {
        self.barrier
    }

    pub(crate) fn counters(&self) -> Counters {
        self.counters
    }
}

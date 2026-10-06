//! [`SpoolConfig`]: where the spool lives and how big it may grow.
//!
//! The gateway builds it from its `spool` section (`max_bytes`,
//! `segment_bytes`, `drain_batch`, `probe_ms`, optional `dir` under the
//! data directory) and checks the volume's free space against `max_bytes`.

use std::num::{NonZeroU64, NonZeroUsize};
use std::path::{Path, PathBuf};
use std::time::Duration;

use super::record::{RECORD_HEADER_LEN, SEGMENT_HEADER};
use crate::config::NonZeroDuration;

/// The spool's directory and bounds. Built only through
/// [`SpoolConfig::new`] and [`SpoolConfig::with_limits`]: a segment holds
/// at least one byte of payload and never exceeds `max_bytes`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpoolConfig {
    dir: PathBuf,
    max_bytes: NonZeroU64,
    segment_bytes: NonZeroU64,
    drain_batch: NonZeroUsize,
    probe: NonZeroDuration,
}

/// Why spool bounds were refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum InvalidSpoolConfig {
    #[error("segment_bytes ({segment_bytes}) exceeds max_bytes ({max_bytes})")]
    SegmentAboveMax { segment_bytes: u64, max_bytes: u64 },
    #[error("segment_bytes ({segment_bytes}) leaves no room for a record (at least {min})")]
    SegmentTooSmall { segment_bytes: u64, min: u64 },
}

// `unwrap` on literal non-zero constants, evaluated at compile time.
const DEFAULT_MAX_BYTES: NonZeroU64 = NonZeroU64::new(1 << 30).unwrap();
const DEFAULT_SEGMENT_BYTES: NonZeroU64 = NonZeroU64::new(64 << 20).unwrap();
const DEFAULT_DRAIN_BATCH: NonZeroUsize = NonZeroUsize::new(256).unwrap();
const DEFAULT_PROBE: NonZeroDuration = match NonZeroDuration::new(Duration::from_secs(1)) {
    Some(probe) => probe,
    None => panic!("zero default probe interval"),
};

/// The smallest segment: its header and one record with a one-byte payload.
const MIN_SEGMENT_BYTES: u64 = (SEGMENT_HEADER.len() + RECORD_HEADER_LEN + 1) as u64;

impl SpoolConfig {
    pub const DEFAULT_MAX_BYTES: u64 = DEFAULT_MAX_BYTES.get();
    pub const DEFAULT_SEGMENT_BYTES: u64 = DEFAULT_SEGMENT_BYTES.get();
    pub const DEFAULT_DRAIN_BATCH: usize = DEFAULT_DRAIN_BATCH.get();
    pub const DEFAULT_PROBE: Duration = DEFAULT_PROBE.get();

    /// The spool in `dir` with the default bounds: 1 GiB in 64 MiB
    /// segments, drained 256 records at a time, probing every second.
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self {
            dir: dir.into(),
            max_bytes: DEFAULT_MAX_BYTES,
            segment_bytes: DEFAULT_SEGMENT_BYTES,
            drain_batch: DEFAULT_DRAIN_BATCH,
            probe: DEFAULT_PROBE,
        }
    }

    /// The spool in `dir` with explicit bounds.
    pub fn with_limits(
        dir: impl Into<PathBuf>,
        max_bytes: NonZeroU64,
        segment_bytes: NonZeroU64,
        drain_batch: NonZeroUsize,
        probe: NonZeroDuration,
    ) -> Result<Self, InvalidSpoolConfig> {
        if segment_bytes.get() < MIN_SEGMENT_BYTES {
            return Err(InvalidSpoolConfig::SegmentTooSmall {
                segment_bytes: segment_bytes.get(),
                min: MIN_SEGMENT_BYTES,
            });
        }
        if segment_bytes > max_bytes {
            return Err(InvalidSpoolConfig::SegmentAboveMax {
                segment_bytes: segment_bytes.get(),
                max_bytes: max_bytes.get(),
            });
        }
        Ok(Self {
            dir: dir.into(),
            max_bytes,
            segment_bytes,
            drain_batch,
            probe,
        })
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// The most bytes the spool's segment files hold together
    /// (`transport.spool.bounded`).
    pub fn max_bytes(&self) -> u64 {
        self.max_bytes.get()
    }

    /// A segment rolls once the next record would take it past this.
    pub fn segment_bytes(&self) -> u64 {
        self.segment_bytes.get()
    }

    /// Records sent to the inner bus per batch.
    pub fn drain_batch(&self) -> usize {
        self.drain_batch.get()
    }

    /// How often a spooling bus asks whether the inner bus answers again.
    pub fn probe(&self) -> Duration {
        self.probe.get()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn nz(n: u64) -> NonZeroU64 {
        NonZeroU64::new(n).expect("non-zero")
    }

    #[test]
    fn defaults_and_checks() {
        let config = SpoolConfig::new("/data/spool");
        assert_eq!(config.max_bytes(), 1 << 30);
        assert_eq!(config.segment_bytes(), 64 << 20);
        assert_eq!(config.drain_batch(), 256);
        assert_eq!(config.probe(), Duration::from_secs(1));
        let batch = NonZeroUsize::MIN;
        let probe = DEFAULT_PROBE;
        assert_eq!(
            SpoolConfig::with_limits("/s", nz(100), nz(200), batch, probe),
            Err(InvalidSpoolConfig::SegmentAboveMax {
                segment_bytes: 200,
                max_bytes: 100
            })
        );
        assert!(matches!(
            SpoolConfig::with_limits("/s", nz(100), nz(10), batch, probe),
            Err(InvalidSpoolConfig::SegmentTooSmall { .. })
        ));
        assert!(SpoolConfig::with_limits("/s", nz(100), nz(100), batch, probe).is_ok());
    }
}

//! Typed configuration: shingle and winnow parameters, decode limits, the
//! index's cutoff, retention and shards, the eviction interval and the
//! semantic threshold.
//!
//! Every value exists only in a valid state: the constructors check, and
//! JSON decodes through them (unknown fields refused, every field
//! defaulted). On the wire:
//!
//! ```json
//! {"winnow": {"k": 32, "w": 16},
//!  "decode": {"max_depth": 3, "max_layers": 32, "min_encoded_run": 16},
//!  "index": {"cutoff": 50, "retention_secs": 2592000, "shards": 1, "owned": [0]},
//!  "eviction_interval_secs": 3600, "semantic_threshold": 0.85,
//!  "locator_keys": ["file_path", "path", "notebook_path", "url", "uri"]}
//! ```

use std::collections::BTreeSet;
use std::num::NonZeroU16;
use std::time::Duration;

use crosstalk_spec::derived::provenance::fingerprint::{Fingerprint, WinnowParams};
use crosstalk_spec::support::{Similarity, Timestamp};
use serde::Deserialize;

/// The smallest shingle length accepted: shorter k-grams match common
/// words between unrelated texts.
pub const MIN_K: u16 = 4;

/// The largest decode depth accepted. Each codec grows text by at most a
/// constant factor, so the depth bounds decoded bytes and lookups.
pub const MAX_DECODE_DEPTH: u8 = 8;

/// The shortest base64 or hex run a decoder considers, at the least.
pub const MIN_ENCODED_RUN: u16 = 8;

/// Why a configuration was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ConfigError {
    #[error("k must be at least {MIN_K}, got {k}")]
    ShortShingle { k: u16 },
    #[error("w must be at least 1")]
    EmptyWindow,
    #[error("max_depth must be between 1 and {MAX_DECODE_DEPTH}, got {depth}")]
    DecodeDepth { depth: u8 },
    #[error("max_layers must be at least 1")]
    NoLayers,
    #[error("min_encoded_run must be at least {MIN_ENCODED_RUN}, got {run}")]
    EncodedRun { run: u16 },
    #[error("retention must be non-zero")]
    ZeroRetention,
    #[error("the eviction interval must be non-zero")]
    ZeroEvictionInterval,
    #[error("shards must be at least 1")]
    NoShards,
    #[error("the node owns no shard")]
    NoShardOwned,
    #[error("shard {shard} does not exist among {shards}")]
    NoSuchShard { shard: u16, shards: u16 },
    #[error("the semantic threshold must be within 0..=1")]
    Threshold,
    #[error("a locator argument key must be non-empty")]
    EmptyLocatorKey,
}

/// The tool-call argument keys whose string values name the resource a
/// call acts on (a path, a URL) rather than content the tool writes: such
/// a value yields no originated span. Only a value directly under one of
/// these keys is excluded; a URL inside a content value still counts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocatorKeys(BTreeSet<String>);

/// The default locator keys.
pub const DEFAULT_LOCATOR_KEYS: [&str; 5] = ["file_path", "path", "notebook_path", "url", "uri"];

impl LocatorKeys {
    /// Every key in `keys`, each non-empty.
    pub fn new(keys: impl IntoIterator<Item = String>) -> Result<Self, ConfigError> {
        let keys: BTreeSet<String> = keys.into_iter().collect();
        if keys.iter().any(String::is_empty) {
            return Err(ConfigError::EmptyLocatorKey);
        }
        Ok(Self(keys))
    }

    /// No key is a locator: every string value can yield a span.
    pub fn none() -> Self {
        Self(BTreeSet::new())
    }

    pub fn contains(&self, key: &str) -> bool {
        self.0.contains(key)
    }

    pub fn iter(&self) -> impl Iterator<Item = &str> {
        self.0.iter().map(String::as_str)
    }
}

impl Default for LocatorKeys {
    fn default() -> Self {
        Self(
            DEFAULT_LOCATOR_KEYS
                .iter()
                .map(|key| (*key).to_owned())
                .collect(),
        )
    }
}

/// Shingle length `k` and winnow window `w`, checked.
pub fn winnow_params(k: u16, w: u16) -> Result<WinnowParams, ConfigError> {
    if k < MIN_K {
        return Err(ConfigError::ShortShingle { k });
    }
    let k = NonZeroU16::new(k).ok_or(ConfigError::ShortShingle { k })?;
    let w = NonZeroU16::new(w).ok_or(ConfigError::EmptyWindow)?;
    Ok(WinnowParams { k, w })
}

/// How far the decode pipeline goes on untrusted input.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DecodeLimits {
    max_depth: u8,
    max_layers: NonZeroU16,
    min_encoded_run: u16,
}

impl DecodeLimits {
    pub fn new(max_depth: u8, max_layers: u16, min_encoded_run: u16) -> Result<Self, ConfigError> {
        if max_depth == 0 || max_depth > MAX_DECODE_DEPTH {
            return Err(ConfigError::DecodeDepth { depth: max_depth });
        }
        let max_layers = NonZeroU16::new(max_layers).ok_or(ConfigError::NoLayers)?;
        if min_encoded_run < MIN_ENCODED_RUN {
            return Err(ConfigError::EncodedRun {
                run: min_encoded_run,
            });
        }
        Ok(Self {
            max_depth,
            max_layers,
            min_encoded_run,
        })
    }

    /// How many decoders may be chained on one text.
    pub fn max_depth(&self) -> u8 {
        self.max_depth
    }

    /// How many texts (the raw text included) one input may expand to.
    pub fn max_layers(&self) -> usize {
        usize::from(self.max_layers.get())
    }

    /// The shortest base64 or hex run decoded, in characters.
    pub fn min_encoded_run(&self) -> usize {
        usize::from(self.min_encoded_run)
    }
}

impl Default for DecodeLimits {
    /// Depth 3, 32 layers, runs of 16 characters.
    fn default() -> Self {
        Self {
            max_depth: 3,
            max_layers: NonZeroU16::new(32).unwrap_or(NonZeroU16::MIN),
            min_encoded_run: 16,
        }
    }
}

/// The fingerprint index's configuration: the boilerplate cutoff, how long
/// observations and spans count, and which shards this node owns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexSettings {
    cutoff: u64,
    retention: Duration,
    shards: NonZeroU16,
    owned: BTreeSet<u16>,
}

impl IndexSettings {
    /// One shard, owned by this node.
    pub fn single_node(cutoff: u64, retention: Duration) -> Result<Self, ConfigError> {
        Self::sharded(cutoff, retention, NonZeroU16::MIN, BTreeSet::from([0]))
    }

    /// `shards` shards, of which this node owns `owned`.
    pub fn sharded(
        cutoff: u64,
        retention: Duration,
        shards: NonZeroU16,
        owned: BTreeSet<u16>,
    ) -> Result<Self, ConfigError> {
        if retention.is_zero() {
            return Err(ConfigError::ZeroRetention);
        }
        if owned.is_empty() {
            return Err(ConfigError::NoShardOwned);
        }
        if let Some(shard) = owned.iter().copied().find(|shard| *shard >= shards.get()) {
            return Err(ConfigError::NoSuchShard {
                shard,
                shards: shards.get(),
            });
        }
        Ok(Self {
            cutoff,
            retention,
            shards,
            owned,
        })
    }

    /// A fingerprint observed in more live texts than this is boilerplate.
    pub fn cutoff(&self) -> u64 {
        self.cutoff
    }

    pub fn retention(&self) -> Duration {
        self.retention
    }

    pub fn shards(&self) -> NonZeroU16 {
        self.shards
    }

    pub fn owned(&self) -> &BTreeSet<u16> {
        &self.owned
    }

    /// Whether this node owns `fingerprint`'s shard.
    pub fn owns(&self, fingerprint: Fingerprint) -> bool {
        self.owned.contains(&fingerprint.shard(self.shards))
    }

    /// The retention period in microseconds, saturating.
    pub fn retention_micros(&self) -> u64 {
        u64::try_from(self.retention.as_micros()).unwrap_or(u64::MAX)
    }

    /// Whether something observed (or indexed) at `at` still counts at
    /// `now`: `now - retention <= at`.
    pub fn counts(&self, at: Timestamp, now: Timestamp) -> bool {
        at.as_micros().saturating_add(self.retention_micros()) >= now.as_micros()
    }
}

impl Default for IndexSettings {
    /// One shard, a cutoff of 50 texts, 30 days of retention.
    fn default() -> Self {
        Self {
            cutoff: 50,
            retention: Duration::from_secs(30 * 24 * 3600),
            shards: NonZeroU16::MIN,
            owned: BTreeSet::from([0]),
        }
    }
}

/// Everything L4 is configured with.
#[derive(Debug, Clone, PartialEq)]
pub struct ProvenanceConfig {
    winnow: WinnowParams,
    decode: DecodeLimits,
    index: IndexSettings,
    eviction_interval: Duration,
    semantic_threshold: Similarity,
    locator_keys: LocatorKeys,
}

impl ProvenanceConfig {
    pub fn new(
        winnow: WinnowParams,
        decode: DecodeLimits,
        index: IndexSettings,
        eviction_interval: Duration,
        semantic_threshold: Similarity,
    ) -> Result<Self, ConfigError> {
        if eviction_interval.is_zero() {
            return Err(ConfigError::ZeroEvictionInterval);
        }
        winnow_params(winnow.k.get(), winnow.w.get())?;
        Ok(Self {
            winnow,
            decode,
            index,
            eviction_interval,
            semantic_threshold,
            locator_keys: LocatorKeys::default(),
        })
    }

    /// The argument keys whose values yield no originated span.
    pub fn locator_keys(&self) -> &LocatorKeys {
        &self.locator_keys
    }

    /// This configuration with other locator keys.
    pub fn with_locator_keys(mut self, locator_keys: LocatorKeys) -> Self {
        self.locator_keys = locator_keys;
        self
    }

    pub fn winnow(&self) -> WinnowParams {
        self.winnow
    }

    pub fn decode(&self) -> DecodeLimits {
        self.decode
    }

    pub fn index(&self) -> &IndexSettings {
        &self.index
    }

    /// How often expired spans are evicted.
    pub fn eviction_interval(&self) -> Duration {
        self.eviction_interval
    }

    /// The lowest score a semantic hit may have.
    pub fn semantic_threshold(&self) -> Similarity {
        self.semantic_threshold
    }

    /// This configuration with other index settings.
    pub fn with_index(mut self, index: IndexSettings) -> Self {
        self.index = index;
        self
    }

    /// This configuration with other winnow parameters.
    pub fn with_winnow(mut self, winnow: WinnowParams) -> Self {
        self.winnow = winnow;
        self
    }
}

/// k = 32, w = 16: any shared run of 47 normalized characters matches.
pub const DEFAULT_K: u16 = 32;
pub const DEFAULT_W: u16 = 16;

impl Default for ProvenanceConfig {
    fn default() -> Self {
        let winnow = WinnowParams {
            k: NonZeroU16::new(DEFAULT_K).unwrap_or(NonZeroU16::MIN),
            w: NonZeroU16::new(DEFAULT_W).unwrap_or(NonZeroU16::MIN),
        };
        Self {
            winnow,
            decode: DecodeLimits::default(),
            index: IndexSettings::default(),
            eviction_interval: Duration::from_secs(3600),
            locator_keys: LocatorKeys::default(),
            // Infallible: 0.85 is within Similarity's 0..=1.
            semantic_threshold: Similarity::new(DEFAULT_THRESHOLD)
                .expect("the default threshold is a similarity"),
        }
    }
}

/// The default semantic threshold.
pub const DEFAULT_THRESHOLD: f32 = 0.85;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawWinnow {
    #[serde(default = "default_k")]
    k: u16,
    #[serde(default = "default_w")]
    w: u16,
}

fn default_k() -> u16 {
    DEFAULT_K
}

fn default_w() -> u16 {
    DEFAULT_W
}

impl Default for RawWinnow {
    fn default() -> Self {
        Self {
            k: DEFAULT_K,
            w: DEFAULT_W,
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawDecode {
    #[serde(default = "default_depth")]
    max_depth: u8,
    #[serde(default = "default_layers")]
    max_layers: u16,
    #[serde(default = "default_run")]
    min_encoded_run: u16,
}

fn default_depth() -> u8 {
    DecodeLimits::default().max_depth
}

fn default_layers() -> u16 {
    DecodeLimits::default().max_layers.get()
}

fn default_run() -> u16 {
    DecodeLimits::default().min_encoded_run
}

impl Default for RawDecode {
    fn default() -> Self {
        Self {
            max_depth: default_depth(),
            max_layers: default_layers(),
            min_encoded_run: default_run(),
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawIndex {
    #[serde(default = "default_cutoff")]
    cutoff: u64,
    #[serde(default = "default_retention")]
    retention_secs: u64,
    #[serde(default = "default_shards")]
    shards: u16,
    #[serde(default = "default_owned")]
    owned: BTreeSet<u16>,
}

fn default_cutoff() -> u64 {
    IndexSettings::default().cutoff
}

fn default_retention() -> u64 {
    IndexSettings::default().retention.as_secs()
}

fn default_shards() -> u16 {
    1
}

fn default_owned() -> BTreeSet<u16> {
    BTreeSet::from([0])
}

impl Default for RawIndex {
    fn default() -> Self {
        Self {
            cutoff: default_cutoff(),
            retention_secs: default_retention(),
            shards: default_shards(),
            owned: default_owned(),
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawConfig {
    #[serde(default)]
    winnow: RawWinnow,
    #[serde(default)]
    decode: RawDecode,
    #[serde(default)]
    index: RawIndex,
    #[serde(default = "default_eviction")]
    eviction_interval_secs: u64,
    #[serde(default = "default_threshold")]
    semantic_threshold: f32,
    #[serde(default = "default_locator_keys")]
    locator_keys: Vec<String>,
}

fn default_locator_keys() -> Vec<String> {
    DEFAULT_LOCATOR_KEYS
        .iter()
        .map(|key| (*key).to_owned())
        .collect()
}

fn default_eviction() -> u64 {
    3600
}

fn default_threshold() -> f32 {
    DEFAULT_THRESHOLD
}

impl TryFrom<RawConfig> for ProvenanceConfig {
    type Error = ConfigError;

    fn try_from(raw: RawConfig) -> Result<Self, Self::Error> {
        let winnow = winnow_params(raw.winnow.k, raw.winnow.w)?;
        let decode = DecodeLimits::new(
            raw.decode.max_depth,
            raw.decode.max_layers,
            raw.decode.min_encoded_run,
        )?;
        let shards = NonZeroU16::new(raw.index.shards).ok_or(ConfigError::NoShards)?;
        let index = IndexSettings::sharded(
            raw.index.cutoff,
            Duration::from_secs(raw.index.retention_secs),
            shards,
            raw.index.owned,
        )?;
        let threshold =
            Similarity::new(raw.semantic_threshold).map_err(|_| ConfigError::Threshold)?;
        let locator_keys = LocatorKeys::new(raw.locator_keys)?;
        Self::new(
            winnow,
            decode,
            index,
            Duration::from_secs(raw.eviction_interval_secs),
            threshold,
        )
        .map(|config| config.with_locator_keys(locator_keys))
    }
}

impl<'de> Deserialize<'de> for ProvenanceConfig {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = RawConfig::deserialize(deserializer)?;
        Self::try_from(raw).map_err(serde::de::Error::custom)
    }
}

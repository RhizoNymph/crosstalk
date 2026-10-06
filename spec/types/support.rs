//! Small building blocks shared by every type module.
//!
//! Their wire forms ([`crate::wire`]): a [`Timestamp`] is RFC 3339 text
//! (`crate::wire::time`), a [`Blake3`] lower-case hex, a [`NonEmpty`] a
//! non-empty array, checked text a string, and each checked value the shape
//! of its fields, decoded through its constructor.

use std::num::NonZeroU32;

use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::wire::{Rejected, WireRequest, decode_text};

/// A list with at least one element.
///
/// Used wherever an empty list would be a lie: the evidence behind a
/// confirmed transmission, the identity evidence behind an agent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NonEmpty<T> {
    head: T,
    tail: Vec<T>,
}

impl<T> NonEmpty<T> {
    pub fn new(head: T) -> Self {
        Self {
            head,
            tail: Vec::new(),
        }
    }

    /// `None` when `items` is empty.
    pub fn from_vec(items: Vec<T>) -> Option<Self> {
        let mut items = items.into_iter();
        let head = items.next()?;
        Some(Self {
            head,
            tail: items.collect(),
        })
    }

    pub fn push(&mut self, item: T) {
        self.tail.push(item);
    }

    pub fn first(&self) -> &T {
        &self.head
    }

    pub fn iter(&self) -> impl Iterator<Item = &T> {
        std::iter::once(&self.head).chain(self.tail.iter())
    }

    /// The elements in order; never empty.
    pub fn into_vec(self) -> Vec<T> {
        let mut items = Vec::with_capacity(self.tail.len() + 1);
        items.push(self.head);
        items.extend(self.tail);
        items
    }

    /// Always at least 1.
    pub fn count(&self) -> NonZeroU32 {
        let tail = u32::try_from(self.tail.len()).unwrap_or(u32::MAX - 1);
        NonZeroU32::MIN.saturating_add(tail)
    }
}

/// An empty array where a [`NonEmpty`] belongs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EmptyList;

/// A JSON array of the elements in order.
impl<T: Serialize> Serialize for NonEmpty<T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_seq(self.iter())
    }
}

/// A JSON array with at least one element; `[]` is a decode error.
impl<'de, T: Deserialize<'de>> Deserialize<'de> for NonEmpty<T> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let items = Vec::<T>::deserialize(deserializer)?;
        Self::from_vec(items)
            .ok_or_else(|| D::Error::custom(Rejected::new("non-empty list", EmptyList)))
    }
}

/// Microseconds since the Unix epoch, UTC. On the wire, RFC 3339 text at
/// microsecond precision (`crate::wire::time`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Timestamp(u64);

impl Timestamp {
    pub const fn from_micros(micros: u64) -> Self {
        Self(micros)
    }

    pub const fn as_micros(self) -> u64 {
        self.0
    }
}

/// The source of wall-clock time. Implementation code takes a `Clock` it
/// is handed at wiring time instead of reading the system clock, so a
/// simulation can inject time (`canonical.clock.injected`): the gateway
/// hands [`SystemClock`], and a deterministic simulation hands its virtual
/// clock.
///
/// Wall time is not monotonic. An NTP step, or a simulated one, can make a
/// later reading earlier than an earlier one, and two readings can be
/// equal. A component that needs elapsed time, a deadline or the order of
/// its own instants takes one reading and adds monotonic elapsed time from
/// the runtime (`tokio::time::Instant`, which a simulation pauses and
/// advances), never the difference of two readings
/// (`canonical.clock.elapsed-from-monotonic`).
pub trait Clock: Send + Sync {
    /// The current wall-clock time.
    fn now(&self) -> Timestamp;
}

/// The operating system's wall clock. A system time before the Unix epoch
/// reads as the epoch, and one past [`u64::MAX`] microseconds as the
/// largest [`Timestamp`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> Timestamp {
        let since_epoch = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default();
        Timestamp(u64::try_from(since_epoch.as_micros()).unwrap_or(u64::MAX))
    }
}

/// A half-open time interval `[start, end)` with `start < end`.
///
/// The reference instance of the validating-deserialization pattern
/// ([`crate::wire`]): `Serialize` is derived on the checked type, and
/// `Deserialize` goes through the private `RawTimeWindow` mirror and
/// [`TimeWindow::new`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", try_from = "RawTimeWindow")]
pub struct TimeWindow {
    start: Timestamp,
    end: Timestamp,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EmptyWindow;

/// [`TimeWindow`]'s fields, decoded without the check.
#[derive(Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
struct RawTimeWindow {
    start: Timestamp,
    end: Timestamp,
}

impl TryFrom<RawTimeWindow> for TimeWindow {
    type Error = Rejected<EmptyWindow>;

    fn try_from(raw: RawTimeWindow) -> Result<Self, Self::Error> {
        Self::new(raw.start, raw.end).map_err(|error| Rejected::new("time window", error))
    }
}

/// A client picks the window of every windowed query.
impl WireRequest for TimeWindow {}

impl TimeWindow {
    pub fn new(start: Timestamp, end: Timestamp) -> Result<Self, EmptyWindow> {
        if start < end {
            Ok(Self { start, end })
        } else {
            Err(EmptyWindow)
        }
    }

    pub fn start(self) -> Timestamp {
        self.start
    }

    pub fn end(self) -> Timestamp {
        self.end
    }

    pub fn contains(self, at: Timestamp) -> bool {
        self.start <= at && at < self.end
    }
}

/// A half-open byte range `[start, end)` into UTF-8 text, with
/// `start < end`. Both ends fall on char boundaries of the text it indexes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", try_from = "RawByteRange")]
pub struct ByteRange {
    start: u32,
    end: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EmptyRange;

#[derive(Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
struct RawByteRange {
    start: u32,
    end: u32,
}

impl TryFrom<RawByteRange> for ByteRange {
    type Error = Rejected<EmptyRange>;

    fn try_from(raw: RawByteRange) -> Result<Self, Self::Error> {
        Self::new(raw.start, raw.end).map_err(|error| Rejected::new("byte range", error))
    }
}

impl ByteRange {
    pub fn new(start: u32, end: u32) -> Result<Self, EmptyRange> {
        if start < end {
            Ok(Self { start, end })
        } else {
            Err(EmptyRange)
        }
    }

    pub fn start(self) -> u32 {
        self.start
    }

    pub fn end(self) -> u32 {
        self.end
    }

    pub fn len(self) -> NonZeroU32 {
        NonZeroU32::new(self.end - self.start).unwrap_or(NonZeroU32::MIN)
    }
}

/// A 32-byte BLAKE3 digest.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Blake3([u8; 32]);

impl Blake3 {
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// The (unkeyed) BLAKE3 digest of `bytes`: what a content id names.
    /// Secret digests are keyed instead ([`crate::ids::secret::KeyedHasher`]).
    pub fn of(bytes: &[u8]) -> Self {
        Self(*blake3::hash(bytes).as_bytes())
    }

    /// 64 lower-case hex digits, most significant byte first.
    pub fn to_hex(&self) -> String {
        hex(&self.0)
    }

    /// The digest `text` names, accepting exactly the text
    /// [`Blake3::to_hex`] writes.
    pub fn from_hex(text: &str) -> Result<Self, InvalidHex> {
        if text.len() != 64 {
            return Err(InvalidHex::Length { got: text.len() });
        }
        let mut digest = [0u8; 32];
        digest.copy_from_slice(&from_hex(text)?);
        Ok(Self(digest))
    }
}

/// `bytes` as lower-case hex, two digits per byte, in order: the wire
/// form of digests and of raw bytes ([`crate::observed::message::MediaBlob`]).
pub fn hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    bytes
        .iter()
        .flat_map(|byte| [HEX[usize::from(byte >> 4)], HEX[usize::from(byte & 0xf)]])
        .map(char::from)
        .collect()
}

/// The bytes `text` spells, accepting exactly the text [`hex`] writes: an
/// even number of lower-case hex digits.
pub fn from_hex(text: &str) -> Result<Vec<u8>, InvalidHex> {
    let digits = text.as_bytes();
    if !digits.len().is_multiple_of(2) {
        return Err(InvalidHex::Length { got: digits.len() });
    }
    let nibble = |index: usize| match digits[index] {
        digit @ b'0'..=b'9' => Ok(digit - b'0'),
        letter @ b'a'..=b'f' => Ok(letter - b'a' + 10),
        _ => Err(InvalidHex::Character { index }),
    };
    (0..digits.len() / 2)
        .map(|index| Ok((nibble(2 * index)? << 4) | nibble(2 * index + 1)?))
        .collect()
}

/// Why text is not a digest's (or raw bytes') hex.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidHex {
    /// Not 64 bytes for a digest; an odd number for raw bytes.
    Length { got: usize },
    /// The byte at `index` is not a lower-case hex digit. Upper case is
    /// refused, so every digest has one text.
    Character { index: usize },
}

impl Serialize for Blake3 {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_hex())
    }
}

impl<'de> Deserialize<'de> for Blake3 {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        decode_text(deserializer, "BLAKE3 hex", |text| Self::from_hex(&text))
    }
}

/// A similarity score in `0.0..=1.0` (cosine similarity mapped to that range,
/// or a model's confidence). Never NaN. A JSON number.
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(try_from = "f32", into = "f32")]
pub struct Similarity(f32);

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct OutOfRange(pub f32);

impl Similarity {
    pub fn new(value: f32) -> Result<Self, OutOfRange> {
        if (0.0..=1.0).contains(&value) {
            Ok(Self(value))
        } else {
            Err(OutOfRange(value))
        }
    }

    pub fn get(self) -> f32 {
        self.0
    }
}

impl TryFrom<f32> for Similarity {
    type Error = Rejected<OutOfRange>;

    fn try_from(value: f32) -> Result<Self, Self::Error> {
        Self::new(value).map_err(|error| Rejected::new("similarity", error))
    }
}

impl From<Similarity> for f32 {
    fn from(similarity: Similarity) -> Self {
        similarity.0
    }
}

/// A fraction of a total in `0.0..=1.0`. Used for edge weights. A JSON
/// number.
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(try_from = "f64", into = "f64")]
pub struct Share(f64);

/// A share outside `0.0..=1.0`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ShareOutOfRange(pub f64);

impl Share {
    pub fn new(value: f64) -> Option<Self> {
        (0.0..=1.0).contains(&value).then_some(Self(value))
    }

    pub fn get(self) -> f64 {
        self.0
    }
}

impl TryFrom<f64> for Share {
    type Error = Rejected<ShareOutOfRange>;

    fn try_from(value: f64) -> Result<Self, Self::Error> {
        Self::new(value).ok_or(Rejected::new("share", ShareOutOfRange(value)))
    }
}

impl From<Share> for f64 {
    fn from(share: Share) -> Self {
        share.0
    }
}

/// An `f32` that is neither NaN nor infinite, for a float with no narrower
/// range: a topic term's c-TF-IDF weight, a projected coordinate. A JSON
/// number. JSON has no NaN, but a number too large for an `f32` (`1e39`)
/// decodes to infinity, so decoding checks too.
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(try_from = "f32", into = "f32")]
pub struct Finite(f32);

/// NaN or an infinity.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct NotFinite(pub f32);

impl Finite {
    pub fn new(value: f32) -> Result<Self, NotFinite> {
        if value.is_finite() {
            Ok(Self(value))
        } else {
            Err(NotFinite(value))
        }
    }

    pub fn get(self) -> f32 {
        self.0
    }
}

impl TryFrom<f32> for Finite {
    type Error = Rejected<NotFinite>;

    fn try_from(value: f32) -> Result<Self, Self::Error> {
        Self::new(value).map_err(|error| Rejected::new("finite number", error))
    }
}

impl From<Finite> for f32 {
    fn from(finite: Finite) -> Self {
        finite.0
    }
}

/// Text with at least one non-whitespace character, stored trimmed.
///
/// Used for operator-written text that must say something, such as a
/// semantic alert rule's query.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct NonBlank(String);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Blank;

impl NonBlank {
    pub fn new(text: &str) -> Result<Self, Blank> {
        let trimmed = text.trim();
        if trimmed.is_empty() {
            Err(Blank)
        } else {
            Ok(Self(trimmed.to_owned()))
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A JSON string.
impl Serialize for NonBlank {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0)
    }
}

/// A JSON string with a non-whitespace character, trimmed as
/// [`NonBlank::new`] trims it.
impl<'de> Deserialize<'de> for NonBlank {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        decode_text(deserializer, "non-blank text", |text| Self::new(&text))
    }
}

/// Display text an operator writes: trimmed, non-empty, at most `MAX`
/// characters (not bytes), and free of control characters. Agent labels and
/// alert rule names are this with their own limits.
///
/// Blank text looks like no text, very long text breaks layout, and control
/// characters can spoof other text in logs and terminals.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DisplayText<const MAX: usize>(String);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidText {
    Blank,
    TooLong { max: usize, got: usize },
    ControlCharacter,
}

impl<const MAX: usize> DisplayText<MAX> {
    pub const MAX_CHARS: usize = MAX;

    pub fn new(text: &str) -> Result<Self, InvalidText> {
        let trimmed = text.trim();
        if trimmed.is_empty() {
            return Err(InvalidText::Blank);
        }
        let chars = trimmed.chars().count();
        if chars > MAX {
            return Err(InvalidText::TooLong {
                max: MAX,
                got: chars,
            });
        }
        if trimmed.chars().any(char::is_control) {
            return Err(InvalidText::ControlCharacter);
        }
        Ok(Self(trimmed.to_owned()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A JSON string.
impl<const MAX: usize> Serialize for DisplayText<MAX> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0)
    }
}

/// A JSON string that [`DisplayText::new`] accepts, trimmed as it trims.
impl<'de, const MAX: usize> Deserialize<'de> for DisplayText<MAX> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        decode_text(deserializer, "display text", |text| Self::new(&text))
    }
}

/// Text an operator writes for a model to read, such as a semantic alert
/// rule's query: trimmed, non-empty and at most `MAX` characters (not
/// bytes). Unlike [`DisplayText`] it may hold line breaks, since it is a
/// query, not a label.
///
/// The bound is a character count a client can check before sending. It is
/// not the model's context: a model may still refuse shorter text that
/// tokenizes long (`EmbedError::TooLong`, `InvalidInput(QueryTooLong)`).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct QueryText<const MAX: usize>(String);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidQueryText {
    Blank,
    TooLong { max: usize, got: usize },
}

impl<const MAX: usize> QueryText<MAX> {
    pub const MAX_CHARS: usize = MAX;

    pub fn new(text: &str) -> Result<Self, InvalidQueryText> {
        let trimmed = text.trim();
        if trimmed.is_empty() {
            return Err(InvalidQueryText::Blank);
        }
        let chars = trimmed.chars().count();
        if chars > MAX {
            return Err(InvalidQueryText::TooLong {
                max: MAX,
                got: chars,
            });
        }
        Ok(Self(trimmed.to_owned()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A JSON string.
impl<const MAX: usize> Serialize for QueryText<MAX> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0)
    }
}

/// A JSON string that [`QueryText::new`] accepts, trimmed as it trims.
impl<'de, const MAX: usize> Deserialize<'de> for QueryText<MAX> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        decode_text(deserializer, "query text", |text| Self::new(&text))
    }
}

/// At most `MAX` items of a longer list, and how long the whole list is.
///
/// A capped list that looks complete invites a wrong decision ("these are all
/// the resources it covers"), so a sampled list is never a plain `Vec`: the
/// reader always has `total` and [`Capped::hidden`] beside what is shown.
/// Built only through [`Capped::new`] (`shown.len() <= MAX`, `total >=
/// shown.len()`) and [`Capped::first`]. The order of `shown` is the
/// producer's, stated where the sample is returned.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    rename_all = "snake_case",
    try_from = "RawCapped<T>",
    bound(serialize = "T: Serialize", deserialize = "T: Deserialize<'de>")
)]
pub struct Capped<T, const MAX: usize> {
    shown: Vec<T>,
    total: u64,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
struct RawCapped<T> {
    shown: Vec<T>,
    total: u64,
}

impl<T, const MAX: usize> TryFrom<RawCapped<T>> for Capped<T, MAX> {
    type Error = Rejected<InvalidCapped>;

    fn try_from(raw: RawCapped<T>) -> Result<Self, Self::Error> {
        Self::new(raw.shown, raw.total).map_err(|error| Rejected::new("capped list", error))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidCapped {
    TooMany { max: usize, got: usize },
    TotalBelowShown { total: u64, shown: usize },
}

impl<T, const MAX: usize> Capped<T, MAX> {
    pub const MAX: usize = MAX;

    pub fn new(shown: Vec<T>, total: u64) -> Result<Self, InvalidCapped> {
        if shown.len() > MAX {
            return Err(InvalidCapped::TooMany {
                max: MAX,
                got: shown.len(),
            });
        }
        if total < len(shown.len()) {
            return Err(InvalidCapped::TotalBelowShown {
                total,
                shown: shown.len(),
            });
        }
        Ok(Self { shown, total })
    }

    /// The first `MAX` of `items`, with `total` the length of all of them.
    pub fn first(items: Vec<T>) -> Self {
        let total = len(items.len());
        let mut shown = items;
        shown.truncate(MAX);
        Self { shown, total }
    }

    /// The items shown: at most `MAX`.
    pub fn shown(&self) -> &[T] {
        &self.shown
    }

    /// How many items the whole list has.
    pub fn total(&self) -> u64 {
        self.total
    }

    /// How many items are not shown: `total - shown().len()`.
    pub fn hidden(&self) -> u64 {
        self.total - len(self.shown.len())
    }

    /// Whether every item is shown.
    pub fn is_complete(&self) -> bool {
        self.hidden() == 0
    }
}

fn len(n: usize) -> u64 {
    u64::try_from(n).unwrap_or(u64::MAX)
}

/// Whether an accepted request changed stored state. The surface reports
/// `Applied` as `ActionOutcome::Applied` and `Unchanged` as
/// `ActionOutcome::Unchanged`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Change {
    Applied,
    /// The state already matched the request.
    Unchanged,
}

/// The time before which every aggregate bucket is final. Late content
/// matches and suspected-to-confirmed upgrades can still change buckets at or
/// after it; nothing changes a bucket before it. Every aggregate response
/// reports one, so a cited view can say what was settled when it was taken.
/// On the wire, its timestamp.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Watermark(pub Timestamp);

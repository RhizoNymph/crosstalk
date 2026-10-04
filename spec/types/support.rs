//! Small building blocks shared by every type module.

use std::num::NonZeroU32;

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

/// Microseconds since the Unix epoch, UTC.
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

/// A half-open time interval `[start, end)` with `start < end`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TimeWindow {
    start: Timestamp,
    end: Timestamp,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EmptyWindow;

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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ByteRange {
    start: u32,
    end: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EmptyRange;

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
}

/// A similarity score in `0.0..=1.0` (cosine similarity mapped to that range,
/// or a model's confidence). Never NaN.
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
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

/// A fraction of a total in `0.0..=1.0`. Used for edge weights.
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
pub struct Share(f64);

impl Share {
    pub fn new(value: f64) -> Option<Self> {
        (0.0..=1.0).contains(&value).then_some(Self(value))
    }

    pub fn get(self) -> f64 {
        self.0
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

/// The time before which every aggregate bucket is final. Late content
/// matches and suspected-to-confirmed upgrades can still change buckets at or
/// after it; nothing changes a bucket before it. Every aggregate response
/// reports one, so a cited view can say what was settled when it was taken.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Watermark(pub Timestamp);

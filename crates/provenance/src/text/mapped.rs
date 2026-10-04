//! Text with a map from each of its bytes back to the part text it was
//! decoded from.
//!
//! A content match's `read_at` is a range of the part text as it arrived,
//! before decoding (`provenance.span.location-indexes-part-text`), so every
//! decoded layer keeps, for each of its bytes, the offset in the part text
//! of the source character it came from, plus the end of the source. The
//! map is non-decreasing and every entry is a character boundary of the
//! part text, so a decoded range on character boundaries maps to a part
//! range on character boundaries.

/// Where each byte of a text came from.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Origin {
    /// The text is the part text itself.
    Identity,
    /// `map[i]` is the part offset of byte `i`; `map[len]` is the source
    /// end.
    Map(Vec<u32>),
}

/// A text and where its bytes sit in the part text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MappedText {
    text: String,
    origin: Origin,
}

impl MappedText {
    /// The part text itself.
    pub fn identity(text: impl Into<String>) -> Self {
        Self {
            text: text.into(),
            origin: Origin::Identity,
        }
    }

    pub fn text(&self) -> &str {
        &self.text
    }

    pub fn into_text(self) -> String {
        self.text
    }

    /// The part offset of byte `index` of this text (`index == len` gives
    /// the source end). An index past the end gives the source end.
    pub fn source(&self, index: usize) -> u32 {
        match &self.origin {
            Origin::Identity => u32::try_from(index.min(self.text.len())).unwrap_or(u32::MAX),
            Origin::Map(map) => map.get(index).or_else(|| map.last()).copied().unwrap_or(0),
        }
    }

    /// The part range `[start, end)` of this text maps to.
    pub fn source_range(&self, start: usize, end: usize) -> (u32, u32) {
        (self.source(start), self.source(end))
    }

    /// `child`, a text decoded from this one (its map indexing this text),
    /// with its map composed through this one's into the part text.
    pub fn compose(&self, child: MappedText) -> MappedText {
        match (&self.origin, child.origin) {
            (Origin::Identity, origin) => MappedText {
                text: child.text,
                origin,
            },
            (Origin::Map(_), Origin::Identity) => {
                let map = (0..=child.text.len()).map(|i| self.source(i)).collect();
                MappedText {
                    text: child.text,
                    origin: Origin::Map(map),
                }
            }
            (Origin::Map(_), Origin::Map(inner)) => {
                let map = inner
                    .iter()
                    .map(|offset| self.source(usize::try_from(*offset).unwrap_or(usize::MAX)))
                    .collect();
                MappedText {
                    text: child.text,
                    origin: Origin::Map(map),
                }
            }
        }
    }
}

/// Builds a decoded text and its map, appending in source order.
#[derive(Debug, Default)]
pub struct MappedBuilder {
    text: String,
    map: Vec<u32>,
}

impl MappedBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Append `piece`, every byte of it from the source character at
    /// `source`.
    pub fn push_str(&mut self, piece: &str, source: u32) {
        self.text.push_str(piece);
        self.map.extend(std::iter::repeat_n(source, piece.len()));
    }

    /// Append `ch`, from the source character at `source`.
    pub fn push(&mut self, ch: char, source: u32) {
        let mut buffer = [0u8; 4];
        self.push_str(ch.encode_utf8(&mut buffer), source);
    }

    /// Bytes appended so far.
    pub fn len(&self) -> usize {
        self.text.len()
    }

    pub fn is_empty(&self) -> bool {
        self.text.is_empty()
    }

    /// The text so far.
    pub fn text(&self) -> &str {
        &self.text
    }

    /// The finished text, its source ending at `source_end`.
    pub fn finish(mut self, source_end: u32) -> MappedText {
        self.map.push(source_end);
        MappedText {
            text: self.text,
            origin: Origin::Map(self.map),
        }
    }

    /// A text from raw decoded `bytes`, byte `i` from `sources[i]`, when the
    /// bytes are valid UTF-8 (no lossy conversion); the map is kept only for
    /// character starts' sources, every continuation byte mapping to its
    /// character's start, so ranges on character boundaries stay on source
    /// character boundaries.
    pub fn from_utf8(bytes: Vec<u8>, sources: &[u32], source_end: u32) -> Option<MappedText> {
        if bytes.len() != sources.len() {
            return None;
        }
        let text = String::from_utf8(bytes).ok()?;
        let mut map = Vec::with_capacity(text.len() + 1);
        for (index, ch) in text.char_indices() {
            let source = sources.get(index).copied().unwrap_or(source_end);
            map.extend(std::iter::repeat_n(source, ch.len_utf8()));
        }
        map.push(source_end);
        Some(MappedText {
            text,
            origin: Origin::Map(map),
        })
    }
}

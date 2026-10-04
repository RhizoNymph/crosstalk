//! The columnar frame a projection is stored and served as.
//!
//! A frame holds one row per point in sample order, as columns, with the
//! values that repeat (senders, readers, route kinds, topics, channels)
//! interned into tables and referenced by `u32` index. Coordinates are
//! packed `f32` pairs, ready to upload as a WebGL vertex buffer. At 100,000
//! points it is about 5 MB, against roughly 20 MB as JSON.
//!
//! **Channels.** A channel-routed point's channel index names its canonical
//! channel when the sample was read ([`PointRoute`]); every other point's
//! is [`NO_CHANNEL`]. So a client colours or groups points by channel from
//! the frame alone, and names the channels with one `channel_names` batch
//! over the channels table.
//!
//! **Canonical form.** Each table lists its entries in order of first use:
//! reading an index column top to bottom, every index is either one already
//! seen or exactly one more than the largest seen so far, and every entry is
//! used. Entries are distinct. So the same points always give the same
//! tables and the same bytes, and a stored frame can be compared or hashed
//! as bytes. [`ProjectionFrame::from_points`] interns points into this form;
//! [`ProjectionFrame::new`] checks it.
//!
//! **Binary layout** (format 2). Little-endian throughout. Ids are their
//! ULID as a `u128` value in little-endian byte order; timestamps are
//! microseconds since the Unix epoch as `u64`. Format 1 had no channels
//! table or channel column and a 64-byte header; a decoder of format 2
//! refuses it.
//!
//! Header, 80 bytes:
//!
//! | Offset | Size | Field |
//! | --- | --- | --- |
//! | 0 | 4 | magic `b"XTPF"` |
//! | 4 | 2 | format, `u16` = 1 |
//! | 6 | 2 | route kinds table length `K`, `u16` |
//! | 8 | 16 | projection id, `u128` |
//! | 24 | 4 | topic-model version, `u32` |
//! | 28 | 4 | point count `n`, `u32` |
//! | 32 | 4 | senders table length `S`, `u32` |
//! | 36 | 4 | readers table length `R`, `u32` |
//! | 40 | 4 | topics table length `T`, `u32` |
//! | 44 | 4 | sample size (`ProjectionLimit`), `u32` |
//! | 48 | 8 | watermark, `u64` microseconds |
//! | 56 | 8 | matching (transmissions admitted before sampling), `u64` |
//! | 64 | 4 | channels table length `C`, `u32` |
//! | 68 | 12 | reserved, zero |
//!
//! Body, sections back to back in this order:
//!
//! | Section | Element | Count |
//! | --- | --- | --- |
//! | transmission ids | `u128` | `n` |
//! | senders table | `u128` agent id | `S` |
//! | readers table | `u128` agent id | `R` |
//! | topics table | `u128` topic id | `T` |
//! | channels table | `u128` channel id | `C` |
//! | confirmed at | `u64` | `n` |
//! | coordinates | `f32` x then `f32` y | `n` pairs |
//! | sender index | `u32` | `n` |
//! | reader index | `u32` | `n` |
//! | route kind index | `u32` | `n` |
//! | topic index | `u32`, [`OUTLIER`] for an outlier | `n` |
//! | channel index | `u32`, [`NO_CHANNEL`] for a route that is not a channel | `n` |
//! | route kinds table | `u8`: 0 channel, 1 delegation, 2 direct, 3 unobserved | `K` |
//! | padding | zero bytes to a multiple of 8 | 0 to 7 |
//!
//! Every `u128` section starts at a multiple of 16, the `u64` section at a
//! multiple of 16 and every `f32` and `u32` section at a multiple of 4, so a
//! client can view each section in place as a typed array. The total length
//! is `80 + 16 (S + R + T + C) + 52 n + K`, rounded up to a multiple of 8,
//! and a decoder rejects any other length, a non-zero reserved or padding
//! byte, and a channel index that disagrees with its point's route kind.

use std::collections::{HashMap, HashSet};
use std::hash::Hash;

use super::{InvalidProjectionLimit, PointParts, PointRoute, ProjectedPoint, ProjectionLimit};
use crate::aggregates::edge::RouteKind;
use crate::aggregates::topic::TopicModelVersion;
use crate::ids::{AgentId, ChannelId, ProjectionId, TopicId, TransmissionId};
use crate::support::{Finite, Timestamp, Watermark};

pub const MAGIC: [u8; 4] = *b"XTPF";
pub const FORMAT: u16 = 2;
pub const HEADER_LEN: usize = 80;
/// Where the header's reserved bytes start; they run to [`HEADER_LEN`].
pub const RESERVED_AT: usize = 68;
/// The topic index of an outlier point.
pub const OUTLIER: u32 = u32::MAX;
/// The channel index of a point whose route is not a channel.
pub const NO_CHANNEL: u32 = u32::MAX;

/// What a frame says about itself. The point count is the frame's length.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FrameHeader {
    pub projection: ProjectionId,
    pub topic_version: TopicModelVersion,
    pub watermark: Watermark,
    pub limit: ProjectionLimit,
    pub matching: u64,
}

/// The interned values, each in order of first use.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct FrameTables {
    pub senders: Vec<AgentId>,
    pub readers: Vec<AgentId>,
    pub route_kinds: Vec<RouteKind>,
    pub topics: Vec<TopicId>,
    /// The canonical channels of channel-routed points.
    pub channels: Vec<ChannelId>,
}

/// One entry per point, in sample order.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct FrameColumns {
    pub transmissions: Vec<TransmissionId>,
    pub confirmed_at: Vec<Timestamp>,
    pub xy: Vec<[f32; 2]>,
    pub sender: Vec<u32>,
    pub reader: Vec<u32>,
    pub route: Vec<u32>,
    /// [`OUTLIER`] for an outlier.
    pub topic: Vec<u32>,
    /// [`NO_CHANNEL`] exactly when the point's route kind is not
    /// `Channel`.
    pub channel: Vec<u32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Column {
    Transmissions,
    ConfirmedAt,
    Xy,
    Sender,
    Reader,
    Route,
    Topic,
    Channel,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Table {
    Senders,
    Readers,
    RouteKinds,
    Topics,
    Channels,
}

/// A frame that breaks one of [`ProjectionFrame`]'s invariants.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidFrame {
    /// The transmissions column does not hold `min(matching, limit)` rows.
    WrongCount {
        expected: u64,
        got: usize,
    },
    /// A column's length differs from the transmissions column's.
    ColumnLength {
        column: Column,
        expected: usize,
        got: usize,
    },
    IndexOutOfRange {
        column: Column,
        row: usize,
        index: u32,
    },
    /// An index skips ahead of the next unused table entry.
    TableNotCanonical {
        table: Table,
    },
    UnreferencedEntry {
        table: Table,
    },
    DuplicateEntry {
        table: Table,
    },
    /// A channel-routed point without a channel, or another point with
    /// one.
    ChannelRouteMismatch {
        row: usize,
    },
    DuplicateTransmission {
        row: usize,
    },
    NonFinite {
        row: usize,
    },
    /// A point whose sender is its reader: a transmission within one agent,
    /// which no projection holds.
    WithinOneAgent {
        row: usize,
    },
}

/// One projection's points, as columns.
///
/// Built only through [`ProjectionFrame::new`] (and [`from_points`] and
/// [`decode`], which call it): every column has one entry per point; the
/// point count is `min(matching, limit)`; every index is in range of its
/// table (the topic index may also be [`OUTLIER`], the channel index
/// [`NO_CHANNEL`]); tables are distinct and in order of first use; a point
/// has a channel exactly when its route kind is `Channel`; no transmission
/// appears twice; every coordinate is finite; no point's sender is its
/// reader.
///
/// [`from_points`]: ProjectionFrame::from_points
/// [`decode`]: ProjectionFrame::decode
#[derive(Debug, Clone, PartialEq)]
pub struct ProjectionFrame {
    header: FrameHeader,
    tables: FrameTables,
    columns: FrameColumns,
}

impl ProjectionFrame {
    pub fn new(
        header: FrameHeader,
        tables: FrameTables,
        columns: FrameColumns,
    ) -> Result<Self, InvalidFrame> {
        let count = columns.transmissions.len();
        let expected = header.matching.min(u64::from(header.limit.get().get()));
        if u64::try_from(count).ok() != Some(expected) {
            return Err(InvalidFrame::WrongCount {
                expected,
                got: count,
            });
        }
        let lengths = [
            (Column::ConfirmedAt, columns.confirmed_at.len()),
            (Column::Xy, columns.xy.len()),
            (Column::Sender, columns.sender.len()),
            (Column::Reader, columns.reader.len()),
            (Column::Route, columns.route.len()),
            (Column::Topic, columns.topic.len()),
            (Column::Channel, columns.channel.len()),
        ];
        if let Some(&(column, got)) = lengths.iter().find(|(_, len)| *len != count) {
            return Err(InvalidFrame::ColumnLength {
                column,
                expected: count,
                got,
            });
        }
        check_interned(
            Column::Sender,
            Table::Senders,
            &columns.sender,
            &tables.senders,
            None,
        )?;
        check_interned(
            Column::Reader,
            Table::Readers,
            &columns.reader,
            &tables.readers,
            None,
        )?;
        check_interned(
            Column::Route,
            Table::RouteKinds,
            &columns.route,
            &tables.route_kinds,
            None,
        )?;
        check_interned(
            Column::Topic,
            Table::Topics,
            &columns.topic,
            &tables.topics,
            Some(OUTLIER),
        )?;
        check_interned(
            Column::Channel,
            Table::Channels,
            &columns.channel,
            &tables.channels,
            Some(NO_CHANNEL),
        )?;
        for (row, (&route, &channel)) in columns.route.iter().zip(&columns.channel).enumerate() {
            // The route index is in range: checked above.
            let is_channel = usize::try_from(route)
                .ok()
                .and_then(|index| tables.route_kinds.get(index))
                == Some(&RouteKind::Channel);
            if is_channel == (channel == NO_CHANNEL) {
                return Err(InvalidFrame::ChannelRouteMismatch { row });
            }
        }
        let mut seen = HashSet::with_capacity(count);
        for (row, transmission) in columns.transmissions.iter().enumerate() {
            if !seen.insert(*transmission) {
                return Err(InvalidFrame::DuplicateTransmission { row });
            }
        }
        if let Some(row) = columns
            .xy
            .iter()
            .position(|[x, y]| !(x.is_finite() && y.is_finite()))
        {
            return Err(InvalidFrame::NonFinite { row });
        }
        // Both indexes are in range: checked above.
        let agent = |table: &[AgentId], index: u32| {
            usize::try_from(index)
                .ok()
                .and_then(|index| table.get(index))
                .copied()
        };
        if let Some(row) = columns
            .sender
            .iter()
            .zip(&columns.reader)
            .position(|(&from, &to)| agent(&tables.senders, from) == agent(&tables.readers, to))
        {
            return Err(InvalidFrame::WithinOneAgent { row });
        }
        Ok(Self {
            header,
            tables,
            columns,
        })
    }

    /// Interns `points`, kept in the given (sample) order, into canonical
    /// tables.
    pub fn from_points(
        header: FrameHeader,
        points: &[ProjectedPoint],
    ) -> Result<Self, InvalidFrame> {
        let mut tables = FrameTables::default();
        let mut columns = FrameColumns::default();
        let mut senders = Interner::default();
        let mut readers = Interner::default();
        let mut routes = Interner::default();
        let mut topics = Interner::default();
        let mut channels = Interner::default();
        for point in points {
            let point = point.parts();
            columns.transmissions.push(point.transmission);
            columns.confirmed_at.push(point.confirmed_at);
            columns.xy.push([point.x.get(), point.y.get()]);
            columns
                .sender
                .push(senders.index(point.from, &mut tables.senders));
            columns
                .reader
                .push(readers.index(point.to, &mut tables.readers));
            columns
                .route
                .push(routes.index(point.route.kind(), &mut tables.route_kinds));
            columns.topic.push(match point.topic {
                Some(topic) => topics.index(topic, &mut tables.topics),
                None => OUTLIER,
            });
            columns.channel.push(match point.route.channel() {
                Some(channel) => channels.index(channel, &mut tables.channels),
                None => NO_CHANNEL,
            });
        }
        Self::new(header, tables, columns)
    }

    pub fn header(&self) -> &FrameHeader {
        &self.header
    }

    pub fn tables(&self) -> &FrameTables {
        &self.tables
    }

    pub fn columns(&self) -> &FrameColumns {
        &self.columns
    }

    /// The number of points; at most [`ProjectionLimit::MAX`].
    pub fn count(&self) -> u32 {
        u32::try_from(self.columns.transmissions.len()).unwrap_or(u32::MAX)
    }

    /// Row `row` as a point, or `None` past the last row.
    pub fn point(&self, row: usize) -> Option<ProjectedPoint> {
        let columns = &self.columns;
        let lookup = |table: &[AgentId], index: &[u32]| {
            table.get(usize::try_from(*index.get(row)?).ok()?).copied()
        };
        let topic = match *columns.topic.get(row)? {
            OUTLIER => None,
            index => Some(*self.tables.topics.get(usize::try_from(index).ok()?)?),
        };
        let route_index = usize::try_from(*columns.route.get(row)?).ok()?;
        let channel = match *columns.channel.get(row)? {
            NO_CHANNEL => None,
            index => Some(*self.tables.channels.get(usize::try_from(index).ok()?)?),
        };
        // A frame's channels agree with its route kinds
        // (`InvalidFrame::ChannelRouteMismatch`).
        let route = PointRoute::from_parts(*self.tables.route_kinds.get(route_index)?, channel)?;
        // Every coordinate of a frame is finite (`InvalidFrame::NonFinite`).
        let [x, y] = *columns.xy.get(row)?;
        // No point's sender is its reader (`InvalidFrame::WithinOneAgent`).
        ProjectedPoint::new(PointParts {
            transmission: *columns.transmissions.get(row)?,
            from: lookup(&self.tables.senders, &columns.sender)?,
            to: lookup(&self.tables.readers, &columns.reader)?,
            route,
            topic,
            confirmed_at: *columns.confirmed_at.get(row)?,
            x: Finite::new(x).ok()?,
            y: Finite::new(y).ok()?,
        })
        .ok()
    }

    /// Every point, in sample order.
    pub fn points(&self) -> impl Iterator<Item = ProjectedPoint> + '_ {
        (0..self.columns.transmissions.len()).filter_map(|row| self.point(row))
    }

    /// The encoded size in bytes, padding included.
    pub fn encoded_len(&self) -> usize {
        let tables = &self.tables;
        encoded_len(
            self.columns.transmissions.len(),
            [
                tables.senders.len(),
                tables.readers.len(),
                tables.topics.len(),
                tables.channels.len(),
            ],
            tables.route_kinds.len(),
        )
        .unwrap_or(usize::MAX)
    }

    /// The frame in the binary layout of this module's documentation.
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.encoded_len());
        let (header, tables, columns) = (&self.header, &self.tables, &self.columns);
        out.extend_from_slice(&MAGIC);
        out.extend_from_slice(&FORMAT.to_le_bytes());
        out.extend_from_slice(&len_u16(tables.route_kinds.len()).to_le_bytes());
        out.extend_from_slice(&header.projection.as_ulid().to_le_bytes());
        out.extend_from_slice(&header.topic_version.0.to_le_bytes());
        for len in [
            columns.transmissions.len(),
            tables.senders.len(),
            tables.readers.len(),
            tables.topics.len(),
        ] {
            out.extend_from_slice(&len_u32(len).to_le_bytes());
        }
        out.extend_from_slice(&header.limit.get().get().to_le_bytes());
        out.extend_from_slice(&header.watermark.0.as_micros().to_le_bytes());
        out.extend_from_slice(&header.matching.to_le_bytes());
        out.extend_from_slice(&len_u32(tables.channels.len()).to_le_bytes());
        out.resize(HEADER_LEN, 0);
        for id in &columns.transmissions {
            out.extend_from_slice(&id.as_ulid().to_le_bytes());
        }
        for id in tables.senders.iter().chain(&tables.readers) {
            out.extend_from_slice(&id.as_ulid().to_le_bytes());
        }
        for id in &tables.topics {
            out.extend_from_slice(&id.as_ulid().to_le_bytes());
        }
        for id in &tables.channels {
            out.extend_from_slice(&id.as_ulid().to_le_bytes());
        }
        for at in &columns.confirmed_at {
            out.extend_from_slice(&at.as_micros().to_le_bytes());
        }
        for [x, y] in &columns.xy {
            out.extend_from_slice(&x.to_le_bytes());
            out.extend_from_slice(&y.to_le_bytes());
        }
        for column in [
            &columns.sender,
            &columns.reader,
            &columns.route,
            &columns.topic,
            &columns.channel,
        ] {
            for index in column {
                out.extend_from_slice(&index.to_le_bytes());
            }
        }
        out.extend(tables.route_kinds.iter().map(|kind| route_code(*kind)));
        out.resize(self.encoded_len(), 0);
        out
    }

    /// Parses and checks a frame in the binary layout. Accepts exactly the
    /// bytes [`ProjectionFrame::encode`] produces for some valid frame.
    pub fn decode(bytes: &[u8]) -> Result<Self, FrameDecodeError> {
        let mut input = Input { bytes, at: 0 };
        if bytes.len() < HEADER_LEN {
            return Err(FrameDecodeError::TooShort { got: bytes.len() });
        }
        if input.array::<4>()? != MAGIC {
            return Err(FrameDecodeError::BadMagic);
        }
        let format = u16::from_le_bytes(input.array()?);
        if format != FORMAT {
            return Err(FrameDecodeError::UnsupportedFormat(format));
        }
        let kinds = usize::from(u16::from_le_bytes(input.array()?));
        let projection = ProjectionId::from_ulid(u128::from_le_bytes(input.array()?));
        let topic_version = TopicModelVersion(u32::from_le_bytes(input.array()?));
        let count = input.len_u32()?;
        let senders = input.len_u32()?;
        let readers = input.len_u32()?;
        let topics = input.len_u32()?;
        let limit = ProjectionLimit::new(u32::from_le_bytes(input.array()?))
            .map_err(FrameDecodeError::Limit)?;
        let watermark = Watermark(Timestamp::from_micros(u64::from_le_bytes(input.array()?)));
        let matching = u64::from_le_bytes(input.array()?);
        let channels = input.len_u32()?;
        if input.array::<{ HEADER_LEN - RESERVED_AT }>()? != [0; HEADER_LEN - RESERVED_AT] {
            return Err(FrameDecodeError::NonZeroReserved);
        }
        let expected = encoded_len(count, [senders, readers, topics, channels], kinds)
            .ok_or(FrameDecodeError::TooLarge)?;
        if bytes.len() != expected {
            return Err(FrameDecodeError::WrongLength {
                expected,
                got: bytes.len(),
            });
        }
        let ids = |input: &mut Input<'_>, n: usize| -> Result<Vec<u128>, FrameDecodeError> {
            (0..n)
                .map(|_| Ok(u128::from_le_bytes(input.array()?)))
                .collect()
        };
        let indices = |input: &mut Input<'_>| -> Result<Vec<u32>, FrameDecodeError> {
            (0..count)
                .map(|_| Ok(u32::from_le_bytes(input.array()?)))
                .collect()
        };
        let transmissions = ids(&mut input, count)?
            .into_iter()
            .map(TransmissionId::from_ulid)
            .collect();
        let senders = ids(&mut input, senders)?
            .into_iter()
            .map(AgentId::from_ulid)
            .collect();
        let readers = ids(&mut input, readers)?
            .into_iter()
            .map(AgentId::from_ulid)
            .collect();
        let topics = ids(&mut input, topics)?
            .into_iter()
            .map(TopicId::from_ulid)
            .collect();
        let channels = ids(&mut input, channels)?
            .into_iter()
            .map(ChannelId::from_ulid)
            .collect();
        let confirmed_at = (0..count)
            .map(|_| Ok(Timestamp::from_micros(u64::from_le_bytes(input.array()?))))
            .collect::<Result<_, FrameDecodeError>>()?;
        let xy = (0..count)
            .map(|_| {
                let x = f32::from_le_bytes(input.array()?);
                let y = f32::from_le_bytes(input.array()?);
                Ok([x, y])
            })
            .collect::<Result<_, FrameDecodeError>>()?;
        let sender = indices(&mut input)?;
        let reader = indices(&mut input)?;
        let route = indices(&mut input)?;
        let topic = indices(&mut input)?;
        let channel = indices(&mut input)?;
        let route_kinds = (0..kinds)
            .map(|_| {
                let [byte] = input.array::<1>()?;
                kind_of_code(byte).ok_or(FrameDecodeError::UnknownRouteKind { byte })
            })
            .collect::<Result<_, FrameDecodeError>>()?;
        if bytes
            .get(input.at..)
            .is_some_and(|pad| pad.iter().any(|b| *b != 0))
        {
            return Err(FrameDecodeError::NonZeroPadding);
        }
        let header = FrameHeader {
            projection,
            topic_version,
            watermark,
            limit,
            matching,
        };
        let tables = FrameTables {
            senders,
            readers,
            route_kinds,
            topics,
            channels,
        };
        let columns = FrameColumns {
            transmissions,
            confirmed_at,
            xy,
            sender,
            reader,
            route,
            topic,
            channel,
        };
        Self::new(header, tables, columns).map_err(FrameDecodeError::Invalid)
    }
}

/// Why bytes are not a frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameDecodeError {
    /// Shorter than the header.
    TooShort {
        got: usize,
    },
    BadMagic,
    UnsupportedFormat(u16),
    Limit(InvalidProjectionLimit),
    /// Header counts whose implied length does not fit in memory.
    TooLarge,
    /// Not the length the header's counts imply.
    WrongLength {
        expected: usize,
        got: usize,
    },
    UnknownRouteKind {
        byte: u8,
    },
    /// A header byte after the channels table length is not zero.
    NonZeroReserved,
    NonZeroPadding,
    /// Well-formed bytes holding a frame that breaks an invariant.
    Invalid(InvalidFrame),
}

/// The route kind's byte in the route kinds table.
pub fn route_code(kind: RouteKind) -> u8 {
    match kind {
        RouteKind::Channel => 0,
        RouteKind::Delegation => 1,
        RouteKind::Direct => 2,
        RouteKind::Unobserved => 3,
    }
}

fn kind_of_code(byte: u8) -> Option<RouteKind> {
    match byte {
        0 => Some(RouteKind::Channel),
        1 => Some(RouteKind::Delegation),
        2 => Some(RouteKind::Direct),
        3 => Some(RouteKind::Unobserved),
        _ => None,
    }
}

/// `None` when the length overflows `usize`.
fn encoded_len(count: usize, tables: [usize; 4], kinds: usize) -> Option<usize> {
    let ids = tables
        .into_iter()
        .try_fold(0usize, |sum, len| sum.checked_add(len))?;
    HEADER_LEN
        .checked_add(ids.checked_mul(16)?)?
        .checked_add(count.checked_mul(52)?)?
        .checked_add(kinds)?
        .checked_next_multiple_of(8)
}

/// Table and column lengths are bounded by `ProjectionLimit::MAX` and the
/// four route kinds, so these never saturate for a valid frame.
fn len_u32(len: usize) -> u32 {
    u32::try_from(len).unwrap_or(u32::MAX)
}

fn len_u16(len: usize) -> u16 {
    u16::try_from(len).unwrap_or(u16::MAX)
}

fn check_interned<T: Eq + Hash>(
    column: Column,
    table: Table,
    indices: &[u32],
    entries: &[T],
    skip: Option<u32>,
) -> Result<(), InvalidFrame> {
    let mut next: usize = 0;
    for (row, &index) in indices.iter().enumerate() {
        if Some(index) == skip {
            continue;
        }
        let position = usize::try_from(index).unwrap_or(usize::MAX);
        if position >= entries.len() {
            return Err(InvalidFrame::IndexOutOfRange { column, row, index });
        }
        if position == next {
            next += 1;
        } else if position > next {
            return Err(InvalidFrame::TableNotCanonical { table });
        }
    }
    if next != entries.len() {
        return Err(InvalidFrame::UnreferencedEntry { table });
    }
    let distinct: HashSet<&T> = entries.iter().collect();
    if distinct.len() != entries.len() {
        return Err(InvalidFrame::DuplicateEntry { table });
    }
    Ok(())
}

struct Interner<T> {
    seen: HashMap<T, u32>,
}

impl<T> Default for Interner<T> {
    fn default() -> Self {
        Self {
            seen: HashMap::new(),
        }
    }
}

impl<T: Copy + Eq + Hash> Interner<T> {
    fn index(&mut self, value: T, table: &mut Vec<T>) -> u32 {
        *self.seen.entry(value).or_insert_with(|| {
            table.push(value);
            len_u32(table.len() - 1)
        })
    }
}

struct Input<'a> {
    bytes: &'a [u8],
    at: usize,
}

impl Input<'_> {
    fn array<const N: usize>(&mut self) -> Result<[u8; N], FrameDecodeError> {
        let end = self.at + N;
        let chunk = self
            .bytes
            .get(self.at..end)
            .and_then(|slice| <[u8; N]>::try_from(slice).ok())
            .ok_or(FrameDecodeError::TooShort {
                got: self.bytes.len(),
            })?;
        self.at = end;
        Ok(chunk)
    }

    fn len_u32(&mut self) -> Result<usize, FrameDecodeError> {
        let value = u32::from_le_bytes(self.array()?);
        Ok(usize::try_from(value).unwrap_or(usize::MAX))
    }
}

use crate::aggregates::edge::RouteKind;
use crate::aggregates::projection::frame::{
    Column, FORMAT, FrameColumns, FrameDecodeError, FrameHeader, FrameTables, HEADER_LEN,
    InvalidFrame, MAGIC, NO_CHANNEL, OUTLIER, ProjectionFrame, RESERVED_AT, Table,
};
use crate::aggregates::projection::{
    InvalidProjectionLimit, PointRoute, ProjectedPoint, ProjectionLimit,
};
use crate::aggregates::topic::TopicModelVersion;
use crate::ids::{ProjectionId, TopicId};
use crate::support::{Finite, Watermark};
use crate::tests::fixtures::{agent, at, channel, transmission};

fn header(limit: u32, matching: u64) -> FrameHeader {
    FrameHeader {
        projection: ProjectionId::from_ulid(0xABCD),
        topic_version: TopicModelVersion(3),
        watermark: Watermark(at(1_000)),
        limit: ProjectionLimit::new(limit).expect("fixture limit in range"),
        matching,
    }
}

fn topic(n: u128) -> TopicId {
    TopicId::from_ulid(n)
}

fn point(
    n: u128,
    from: u128,
    to: u128,
    route: PointRoute,
    topic: Option<TopicId>,
) -> ProjectedPoint {
    let coordinate = f32::from(u16::try_from(n).unwrap_or(0));
    ProjectedPoint {
        transmission: transmission(n),
        from: agent(from),
        to: agent(to),
        route,
        topic,
        confirmed_at: at(u64::try_from(n).unwrap_or(0) * 10),
        x: Finite::new(coordinate).expect("a small integer is finite"),
        y: Finite::new(-coordinate / 2.0).expect("a small integer is finite"),
    }
}

/// Three points: senders 1, 2, 1; readers 9, 9, 8; routes Direct, Channel
/// (channel 4), Direct; topics 7, outlier, 6.
fn points() -> Vec<ProjectedPoint> {
    vec![
        point(1, 1, 9, PointRoute::Direct, Some(topic(7))),
        point(2, 2, 9, PointRoute::Channel(channel(4)), None),
        point(3, 1, 8, PointRoute::Direct, Some(topic(6))),
    ]
}

fn frame() -> ProjectionFrame {
    ProjectionFrame::from_points(header(10, 3), &points()).expect("valid frame")
}

fn parts() -> (FrameHeader, FrameTables, FrameColumns) {
    let frame = frame();
    (
        *frame.header(),
        frame.tables().clone(),
        frame.columns().clone(),
    )
}

// ── Interning ──────────────────────────────────────────────────────────────

#[test]
fn from_points_interns_in_order_of_first_use() {
    let frame = frame();
    assert_eq!(frame.count(), 3);
    assert_eq!(frame.tables().senders, vec![agent(1), agent(2)]);
    assert_eq!(frame.tables().readers, vec![agent(9), agent(8)]);
    assert_eq!(
        frame.tables().route_kinds,
        vec![RouteKind::Direct, RouteKind::Channel]
    );
    assert_eq!(frame.tables().topics, vec![topic(7), topic(6)]);
    assert_eq!(frame.columns().sender, vec![0, 1, 0]);
    assert_eq!(frame.columns().reader, vec![0, 0, 1]);
    assert_eq!(frame.columns().route, vec![0, 1, 0]);
    assert_eq!(frame.columns().topic, vec![0, OUTLIER, 1]);
    assert_eq!(frame.tables().channels, vec![channel(4)]);
    assert_eq!(frame.columns().channel, vec![NO_CHANNEL, 0, NO_CHANNEL]);
}

#[test]
fn channels_intern_in_order_of_first_use() {
    let points = vec![
        point(1, 1, 9, PointRoute::Channel(channel(5)), None),
        point(2, 1, 9, PointRoute::Unobserved, None),
        point(3, 1, 9, PointRoute::Channel(channel(4)), None),
        point(4, 1, 9, PointRoute::Channel(channel(5)), None),
    ];
    let frame = ProjectionFrame::from_points(header(10, 4), &points).expect("valid frame");
    assert_eq!(frame.tables().channels, vec![channel(5), channel(4)]);
    assert_eq!(frame.columns().channel, vec![0, NO_CHANNEL, 1, 0]);
    assert_eq!(frame.points().collect::<Vec<_>>(), points);
    assert_eq!(ProjectionFrame::decode(&frame.encode()), Ok(frame));
}

#[test]
fn a_point_route_names_a_channel_exactly_when_it_is_a_channel_route() {
    let wiki = channel(4);
    assert_eq!(
        PointRoute::from_parts(RouteKind::Channel, Some(wiki)),
        Some(PointRoute::Channel(wiki))
    );
    assert_eq!(PointRoute::from_parts(RouteKind::Channel, None), None);
    for (kind, route) in [
        (RouteKind::Delegation, PointRoute::Delegation),
        (RouteKind::Direct, PointRoute::Direct),
        (RouteKind::Unobserved, PointRoute::Unobserved),
    ] {
        assert_eq!(PointRoute::from_parts(kind, None), Some(route));
        assert_eq!(PointRoute::from_parts(kind, Some(wiki)), None);
        assert_eq!((route.kind(), route.channel()), (kind, None));
    }
    let routed = PointRoute::Channel(wiki);
    assert_eq!(
        (routed.kind(), routed.channel()),
        (RouteKind::Channel, Some(wiki))
    );
}

#[test]
fn points_read_back_in_sample_order() {
    let frame = frame();
    assert_eq!(frame.points().collect::<Vec<_>>(), points());
    assert_eq!(frame.point(1), Some(points()[1]));
    assert_eq!(frame.point(3), None);
}

#[test]
fn empty_frame_when_nothing_matched() {
    let frame = ProjectionFrame::from_points(header(10, 0), &[]).expect("empty is valid");
    assert_eq!(frame.count(), 0);
    let bytes = frame.encode();
    assert_eq!(bytes.len(), HEADER_LEN);
    assert_eq!(ProjectionFrame::decode(&bytes), Ok(frame));
}

// ── Checked constructor ────────────────────────────────────────────────────

#[test]
fn new_accepts_canonical_parts() {
    let (header, tables, columns) = parts();
    assert_eq!(ProjectionFrame::new(header, tables, columns), Ok(frame()));
}

#[test]
fn new_rejects_a_count_other_than_min_of_matching_and_limit() {
    let (_, tables, columns) = parts();
    // Limit 2: three points are too many.
    assert_eq!(
        ProjectionFrame::new(header(2, 3), tables.clone(), columns.clone()),
        Err(InvalidFrame::WrongCount {
            expected: 2,
            got: 3
        })
    );
    // Limit 10 and 5 matching: three points are too few.
    assert_eq!(
        ProjectionFrame::new(header(10, 5), tables, columns),
        Err(InvalidFrame::WrongCount {
            expected: 5,
            got: 3
        })
    );
}

#[test]
fn new_rejects_columns_of_different_lengths() {
    let (header, tables, mut columns) = parts();
    columns.xy.pop();
    assert_eq!(
        ProjectionFrame::new(header, tables, columns),
        Err(InvalidFrame::ColumnLength {
            column: Column::Xy,
            expected: 3,
            got: 2
        })
    );
}

#[test]
fn new_rejects_an_index_past_its_table() {
    let (header, tables, mut columns) = parts();
    columns.reader[2] = 2;
    assert_eq!(
        ProjectionFrame::new(header, tables, columns),
        Err(InvalidFrame::IndexOutOfRange {
            column: Column::Reader,
            row: 2,
            index: 2
        })
    );
}

#[test]
fn new_accepts_outlier_only_in_the_topic_column() {
    let (header, tables, mut columns) = parts();
    columns.route[0] = OUTLIER;
    assert_eq!(
        ProjectionFrame::new(header, tables, columns),
        Err(InvalidFrame::IndexOutOfRange {
            column: Column::Route,
            row: 0,
            index: OUTLIER
        })
    );
}

#[test]
fn new_rejects_a_channel_route_without_a_channel() {
    let (header, mut tables, mut columns) = parts();
    // Point 1 is channel-routed; drop its channel.
    tables.channels.clear();
    columns.channel[1] = NO_CHANNEL;
    assert_eq!(
        ProjectionFrame::new(header, tables, columns),
        Err(InvalidFrame::ChannelRouteMismatch { row: 1 })
    );
}

#[test]
fn new_rejects_a_channel_on_another_route() {
    let (header, tables, mut columns) = parts();
    // Point 0 is a direct route; give it point 1's channel.
    columns.channel[0] = 0;
    assert_eq!(
        ProjectionFrame::new(header, tables, columns),
        Err(InvalidFrame::ChannelRouteMismatch { row: 0 })
    );
}

#[test]
fn new_checks_the_channel_column_like_any_other() {
    let (header, tables, mut columns) = parts();
    columns.channel[1] = 1;
    assert_eq!(
        ProjectionFrame::new(header, tables, columns),
        Err(InvalidFrame::IndexOutOfRange {
            column: Column::Channel,
            row: 1,
            index: 1
        })
    );
    let (header, mut tables, columns) = parts();
    tables.channels.push(channel(5));
    assert_eq!(
        ProjectionFrame::new(header, tables, columns),
        Err(InvalidFrame::UnreferencedEntry {
            table: Table::Channels
        })
    );
    let (header, tables, mut columns) = parts();
    columns.channel.pop();
    assert_eq!(
        ProjectionFrame::new(header, tables, columns),
        Err(InvalidFrame::ColumnLength {
            column: Column::Channel,
            expected: 3,
            got: 2
        })
    );
}

#[test]
fn new_rejects_tables_out_of_first_use_order() {
    let (header, mut tables, mut columns) = parts();
    tables.senders.swap(0, 1);
    columns.sender = vec![1, 0, 1];
    assert_eq!(
        ProjectionFrame::new(header, tables, columns),
        Err(InvalidFrame::TableNotCanonical {
            table: Table::Senders
        })
    );
}

#[test]
fn new_rejects_unused_table_entries() {
    let (header, mut tables, columns) = parts();
    tables.topics.push(topic(5));
    assert_eq!(
        ProjectionFrame::new(header, tables, columns),
        Err(InvalidFrame::UnreferencedEntry {
            table: Table::Topics
        })
    );
}

#[test]
fn new_rejects_duplicate_table_entries() {
    let (header, mut tables, columns) = parts();
    tables.readers[1] = agent(9);
    assert_eq!(
        ProjectionFrame::new(header, tables, columns),
        Err(InvalidFrame::DuplicateEntry {
            table: Table::Readers
        })
    );
}

#[test]
fn new_rejects_a_transmission_twice() {
    let (header, tables, mut columns) = parts();
    columns.transmissions[2] = transmission(1);
    assert_eq!(
        ProjectionFrame::new(header, tables, columns),
        Err(InvalidFrame::DuplicateTransmission { row: 2 })
    );
}

#[test]
fn new_rejects_non_finite_coordinates() {
    for bad in [f32::NAN, f32::INFINITY, f32::NEG_INFINITY] {
        let (header, tables, mut columns) = parts();
        columns.xy[1] = [0.0, bad];
        assert_eq!(
            ProjectionFrame::new(header, tables, columns),
            Err(InvalidFrame::NonFinite { row: 1 })
        );
    }
}

// ── Binary layout ──────────────────────────────────────────────────────────

/// Two senders, two readers, two topics, one channel, two route kinds,
/// three points.
const TABLE_IDS: usize = 7;
const KINDS: usize = 2;
const POINTS: usize = 3;
/// Where the route kinds table starts.
const KINDS_AT: usize = HEADER_LEN + 16 * TABLE_IDS + 52 * POINTS;

#[test]
fn encoded_length_follows_the_layout() {
    let bytes = frame().encode();
    let unpadded = KINDS_AT + KINDS;
    assert_eq!(bytes.len(), unpadded.next_multiple_of(8));
    assert_eq!(bytes.len(), frame().encoded_len());
    assert!(bytes[unpadded..].iter().all(|b| *b == 0));
}

#[test]
fn header_fields_sit_at_their_offsets() {
    let bytes = frame().encode();
    let u32_at = |at: usize| u32::from_le_bytes(bytes[at..at + 4].try_into().expect("4 bytes"));
    let u64_at = |at: usize| u64::from_le_bytes(bytes[at..at + 8].try_into().expect("8 bytes"));
    assert_eq!(bytes[0..4], MAGIC);
    assert_eq!(u16::from_le_bytes([bytes[4], bytes[5]]), FORMAT);
    assert_eq!(u16::from_le_bytes([bytes[6], bytes[7]]), 2);
    assert_eq!(
        u128::from_le_bytes(bytes[8..24].try_into().expect("16 bytes")),
        0xABCD
    );
    assert_eq!(
        [
            u32_at(24),
            u32_at(28),
            u32_at(32),
            u32_at(36),
            u32_at(40),
            u32_at(44)
        ],
        [3, 3, 2, 2, 2, 10]
    );
    assert_eq!((u64_at(48), u64_at(56)), (1_000, 3));
    assert_eq!(u32_at(64), 1, "one channel");
    assert!(bytes[RESERVED_AT..HEADER_LEN].iter().all(|b| *b == 0));
}

#[test]
fn body_sections_sit_at_their_offsets() {
    let bytes = frame().encode();
    // First transmission id right after the header.
    assert_eq!(
        u128::from_le_bytes(
            bytes[HEADER_LEN..HEADER_LEN + 16]
                .try_into()
                .expect("16 bytes")
        ),
        transmission(1).as_ulid()
    );
    // The channels table, after the topics table.
    let channels_at = HEADER_LEN + 16 * (POINTS + TABLE_IDS - 1);
    assert_eq!(
        u128::from_le_bytes(
            bytes[channels_at..channels_at + 16]
                .try_into()
                .expect("16 bytes")
        ),
        channel(4).as_ulid()
    );
    // First point's x, after the ids and the confirmation times.
    let xy_at = HEADER_LEN + 16 * (POINTS + TABLE_IDS) + 8 * POINTS;
    assert_eq!(
        f32::from_le_bytes(bytes[xy_at..xy_at + 4].try_into().expect("4 bytes")),
        1.0
    );
    // Route kinds table: Direct (2) then Channel (0).
    assert_eq!(bytes[KINDS_AT..KINDS_AT + KINDS], [2, 0]);
    // Second point's topic index is the outlier.
    let topic_at = KINDS_AT - 8 * POINTS + 4;
    assert_eq!(
        u32::from_le_bytes(bytes[topic_at..topic_at + 4].try_into().expect("4 bytes")),
        OUTLIER
    );
    // The channel column, last before the route kinds: none, 0, none.
    let channel_at = KINDS_AT - 4 * POINTS;
    let channel_column: Vec<u32> = bytes[channel_at..KINDS_AT]
        .as_chunks::<4>()
        .0
        .iter()
        .map(|chunk| u32::from_le_bytes(*chunk))
        .collect();
    assert_eq!(channel_column, vec![NO_CHANNEL, 0, NO_CHANNEL]);
}

#[test]
fn decode_inverts_encode() {
    let frame = frame();
    assert_eq!(ProjectionFrame::decode(&frame.encode()), Ok(frame));
}

#[test]
fn decode_rejects_short_input() {
    assert_eq!(
        ProjectionFrame::decode(&[0; HEADER_LEN - 1]),
        Err(FrameDecodeError::TooShort {
            got: HEADER_LEN - 1
        })
    );
}

#[test]
fn decode_rejects_bad_magic_and_format() {
    let mut bytes = frame().encode();
    bytes[0] = b'Y';
    assert_eq!(
        ProjectionFrame::decode(&bytes),
        Err(FrameDecodeError::BadMagic)
    );
    let mut bytes = frame().encode();
    bytes[4..6].copy_from_slice(&(FORMAT + 1).to_le_bytes());
    assert_eq!(
        ProjectionFrame::decode(&bytes),
        Err(FrameDecodeError::UnsupportedFormat(FORMAT + 1))
    );
}

#[test]
fn decode_rejects_format_1_frames() {
    // Format 1 had no channel column and a 64-byte header.
    assert_eq!(FORMAT, 2);
    let mut bytes = frame().encode();
    bytes[4..6].copy_from_slice(&1u16.to_le_bytes());
    assert_eq!(
        ProjectionFrame::decode(&bytes),
        Err(FrameDecodeError::UnsupportedFormat(1))
    );
}

#[test]
fn decode_rejects_non_zero_reserved_header_bytes() {
    for at in RESERVED_AT..HEADER_LEN {
        let mut bytes = frame().encode();
        bytes[at] = 1;
        assert_eq!(
            ProjectionFrame::decode(&bytes),
            Err(FrameDecodeError::NonZeroReserved),
            "byte {at}"
        );
    }
}

#[test]
fn decode_rejects_a_zero_sample_size() {
    let mut bytes = frame().encode();
    bytes[44..48].copy_from_slice(&0u32.to_le_bytes());
    assert_eq!(
        ProjectionFrame::decode(&bytes),
        Err(FrameDecodeError::Limit(InvalidProjectionLimit::Zero))
    );
}

#[test]
fn decode_rejects_any_other_length() {
    let mut longer = frame().encode();
    let expected = longer.len();
    longer.extend([0; 8]);
    assert_eq!(
        ProjectionFrame::decode(&longer),
        Err(FrameDecodeError::WrongLength {
            expected,
            got: expected + 8
        })
    );
    let shorter = &frame().encode()[..expected - 8];
    assert_eq!(
        ProjectionFrame::decode(shorter),
        Err(FrameDecodeError::WrongLength {
            expected,
            got: expected - 8
        })
    );
}

#[test]
fn decode_rejects_non_zero_padding() {
    let mut bytes = frame().encode();
    let last = bytes.len() - 1;
    bytes[last] = 1;
    assert_eq!(
        ProjectionFrame::decode(&bytes),
        Err(FrameDecodeError::NonZeroPadding)
    );
}

#[test]
fn decode_rejects_an_unknown_route_kind() {
    let mut bytes = frame().encode();
    bytes[KINDS_AT] = 4;
    assert_eq!(
        ProjectionFrame::decode(&bytes),
        Err(FrameDecodeError::UnknownRouteKind { byte: 4 })
    );
}

#[test]
fn decode_checks_channels_against_route_kinds() {
    let mut bytes = frame().encode();
    // Give the first point (a direct route) the channel at index 0.
    let channel_at = KINDS_AT - 4 * POINTS;
    bytes[channel_at..channel_at + 4].copy_from_slice(&0u32.to_le_bytes());
    assert_eq!(
        ProjectionFrame::decode(&bytes),
        Err(FrameDecodeError::Invalid(
            InvalidFrame::ChannelRouteMismatch { row: 0 }
        ))
    );
}

#[test]
fn decode_checks_the_frame_invariants() {
    let mut bytes = frame().encode();
    let sender_at = KINDS_AT - 20 * POINTS;
    bytes[sender_at..sender_at + 4].copy_from_slice(&5u32.to_le_bytes());
    assert_eq!(
        ProjectionFrame::decode(&bytes),
        Err(FrameDecodeError::Invalid(InvalidFrame::IndexOutOfRange {
            column: Column::Sender,
            row: 0,
            index: 5
        }))
    );
}

//! The Postgres index refuses a misrouted call before it reaches the
//! database: these run with a pool that never connects.

use std::collections::BTreeSet;
use std::num::NonZeroU16;
use std::time::Duration;

use crosstalk_spec::derived::provenance::fingerprint::{Fingerprint, PositionedFingerprint};
use crosstalk_spec::derived::provenance::span::{OriginatedSpan, Span, SpanLocation, SpanState};
use crosstalk_spec::interfaces::l4_provenance::{FingerprintIndex, IndexError};
use crosstalk_spec::observed::message::PartRef;
use crosstalk_spec::support::ByteRange;
use crosstalk_testkit::ids::Ids;
use crosstalk_testkit::time::T0;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};

use crate::config::IndexSettings;
use crate::index::PgFingerprintIndex;

/// An index owning shard 0 of 2, over a pool that never connects.
fn misrouted() -> PgFingerprintIndex {
    let options = PgConnectOptions::new()
        .host("127.0.0.1")
        .port(1)
        .username("nobody")
        .database("nothing");
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(Duration::from_millis(50))
        .connect_lazy_with(options);
    let shards = NonZeroU16::new(2).expect("two shards");
    let settings = IndexSettings::sharded(1, Duration::from_secs(60), shards, BTreeSet::from([0]))
        .expect("settings");
    PgFingerprintIndex::new(pool, settings)
}

fn span(ids: &mut Ids) -> OriginatedSpan {
    OriginatedSpan::new(Span {
        id: ids.span(),
        location: SpanLocation {
            part: PartRef {
                message: ids.message(),
                index: 0,
            },
            range: ByteRange::new(0, 10).expect("range"),
        },
        agent: ids.agent(),
        exchange: ids.exchange(),
        state: SpanState::Originated,
    })
    .expect("originated")
}

fn mixed() -> Vec<PositionedFingerprint> {
    [2u64, 4, 7, 9]
        .into_iter()
        .map(|value| PositionedFingerprint {
            fingerprint: Fingerprint(value),
            offset: 0,
        })
        .collect()
}

pub async fn insert_on_wrong_shard_errors() {
    let mut index = misrouted();
    let mut ids = Ids::new();
    let result = index.insert(&span(&mut ids), &mixed(), T0).await;
    assert_eq!(
        result,
        Err(IndexError::WrongShard {
            fingerprint: Fingerprint(7)
        })
    );
}

pub async fn lookup_on_wrong_shard_errors() {
    let index = misrouted();
    let result = index.lookup(&mixed(), T0).await;
    assert_eq!(
        result,
        Err(IndexError::WrongShard {
            fingerprint: Fingerprint(7)
        })
    );
}

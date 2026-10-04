//! Subscribing: the resume plan, resync first, and what follows it.

use std::time::Duration;

use crosstalk_spec::events::changed::Changed;
use crosstalk_spec::ids::AgentId;
use crosstalk_spec::interfaces::l8_surface::live::{
    FeedEpoch, LiveCursor, LiveFeed, LiveItem, LiveStream, Resume, ResyncReason,
};

use super::world::{Fixture, Who, config};

fn changed(n: u128) -> Changed {
    Changed::Agent(AgentId::from_ulid(0xA000 + n))
}

async fn first_two(fixture: &Fixture, resume: Resume) -> (LiveItem, LiveItem) {
    let caller = fixture.caller(Who::Viewer).await;
    let mut stream = match fixture.surface.subscribe(&caller, resume).await {
        Ok(stream) => stream,
        Err(error) => panic!("subscribe: {error:?}"),
    };
    let head = fixture.feed.head();
    if let Err(error) = fixture.feed.append(changed(100)).await {
        panic!("append: {error}");
    }
    let (Ok(first), Ok(second)) = (stream.next().await, stream.next().await) else {
        panic!("stream ended");
    };
    let LiveItem::Event { cursor, .. } = second else {
        panic!("second item {second:?}");
    };
    assert_eq!(
        cursor.seq,
        head.seq + 1,
        "the event after the resync was appended after its head"
    );
    (first, second)
}

/// INV-471 (`surface.live.resync-first`): a cursor that cannot be resumed
/// gets `Resync` with the plan's reason at the head first, then only events
/// appended after that head.
#[tokio::test(start_paused = true)]
async fn unresumable_cursor_gets_resync_first() {
    let fixture = Fixture::new().await;
    for n in 0..3 {
        if let Err(error) = fixture.feed.append(changed(n)).await {
            panic!("append: {error}");
        }
    }
    let head = fixture.feed.head();
    let other_epoch = LiveCursor {
        epoch: FeedEpoch(head.epoch.0 + 1),
        seq: 1,
    };
    let ahead = LiveCursor {
        seq: head.seq + 5,
        ..head
    };
    for (resume, reason) in [
        (Resume::From(other_epoch), ResyncReason::OtherEpoch),
        (Resume::From(ahead), ResyncReason::AheadOfHead),
        (Resume::Unreadable, ResyncReason::Unreadable),
    ] {
        let (first, _) = first_two(&fixture, resume).await;
        let head = LiveCursor {
            seq: fixture.feed.head().seq - 1,
            ..fixture.feed.head()
        };
        assert_eq!(
            first,
            LiveItem::Resync {
                cursor: head,
                reason
            }
        );
    }
    // Entries older than the retention are gone: a cursor before them
    // resyncs as expired.
    let old = LiveCursor { seq: 1, ..head };
    tokio::time::sleep(config().live.retention() + Duration::from_secs(1)).await;
    if let Err(error) = fixture.feed.append(changed(50)).await {
        panic!("append: {error}");
    }
    let (first, _) = first_two(&fixture, Resume::From(old)).await;
    let head = LiveCursor {
        seq: fixture.feed.head().seq - 1,
        ..fixture.feed.head()
    };
    assert_eq!(
        first,
        LiveItem::Resync {
            cursor: head,
            reason: ResyncReason::Expired
        }
    );
}

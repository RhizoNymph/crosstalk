//! The live feed over SSE: the resume point a subscription sends, items
//! and the end event as the binding frames them, and reconnects from the
//! last cursor after every kind of cut.

use std::time::Duration;

use crosstalk_spec::interfaces::l8_surface::http::sse::{
    EVENT_STREAM, end_frame, event_frame, resume as read_resume,
};
use crosstalk_spec::interfaces::l8_surface::live::{
    FeedEpoch, LiveCursor, LiveEnd, LiveFeed, LiveItem, LiveStream, Resume, ResyncReason, UiEvent,
};
use crosstalk_spec::interfaces::l8_surface::{Permission, QueryError};

use super::stub::{Recorded, Reply, Step, Stub, fast_config};
use super::{ULID_A, caller, id};
use crate::{HttpClient, HttpLiveStream};

fn cursor(seq: u64) -> LiveCursor {
    LiveCursor {
        epoch: FeedEpoch(7),
        seq,
    }
}

fn item(seq: u64) -> LiveItem {
    if seq.is_multiple_of(2) {
        LiveItem::Heartbeat {
            cursor: cursor(seq),
        }
    } else {
        LiveItem::Event {
            cursor: cursor(seq),
            event: UiEvent::AlertChanged { id: id(ULID_A) },
        }
    }
}

/// The binding's frame of each item, as the surface writes it.
fn frames(seqs: impl IntoIterator<Item = u64>) -> Vec<Step> {
    seqs.into_iter()
        .map(|seq| {
            Step::Send(
                event_frame(&item(seq))
                    .unwrap_or_else(|error| panic!("{error}"))
                    .into_bytes(),
            )
        })
        .collect()
}

fn end(reason: LiveEnd) -> Step {
    Step::Send(end_frame(reason).into_bytes())
}

fn feed(steps: Vec<Step>) -> Reply {
    Reply::stream(200, EVENT_STREAM, steps)
        .with_header("cache-control", "no-store")
        .with_header("x-accel-buffering", "no")
}

async fn subscribe(client: &HttpClient, resume: Resume) -> HttpLiveStream {
    client
        .subscribe(&caller(), resume)
        .await
        .unwrap_or_else(|error| panic!("{error:?}"))
}

async fn take(stream: &mut HttpLiveStream, n: usize) -> Vec<LiveItem> {
    let mut items = Vec::new();
    for _ in 0..n {
        match stream.next().await {
            Ok(item) => items.push(item),
            Err(end) => panic!("ended early: {end:?}"),
        }
    }
    items
}

fn resume_sent(request: &Recorded) -> Resume {
    let cursor = request
        .query
        .iter()
        .find(|(name, _)| name == "cursor")
        .map(|(_, value)| value.as_str());
    read_resume(request.header("last-event-id"), cursor)
}

/// A subscription sends its resume point as `Last-Event-ID`, which the
/// binding reads back as the same `Resume`: none for `Fresh`, the cursor's
/// text for `From`, and text that is not a cursor for `Unreadable`.
#[tokio::test]
async fn a_subscription_sends_its_resume_point() {
    for resume in [
        Resume::Fresh,
        Resume::From(cursor(1042)),
        Resume::Unreadable,
    ] {
        let mut stub = Stub::always(feed(vec![end(LiveEnd::ShuttingDown)])).await;
        let mut stream = subscribe(&stub.client(), resume).await;
        assert_eq!(stream.next().await, Err(LiveEnd::ShuttingDown));
        let request = stub.only_request();
        assert_eq!(request.method, "GET");
        assert_eq!(request.path, "/live");
        assert_eq!(request.header("accept"), Some(EVENT_STREAM));
        assert!(request.body.is_empty());
        assert_eq!(resume_sent(&request), resume, "{resume:?}");
    }
}

/// Items arrive in order as the binding frames them; the end event ends
/// the stream with its reason, for good, and moves no cursor.
#[tokio::test]
async fn items_then_the_end_event() {
    let mut steps = frames(1..=4);
    steps.push(end(LiveEnd::Lagged));
    let mut stub = Stub::always(feed(steps)).await;
    let mut stream = subscribe(&stub.client(), Resume::Fresh).await;
    assert_eq!(stream.last_cursor(), None);
    assert_eq!(
        take(&mut stream, 4).await,
        (1..=4).map(item).collect::<Vec<_>>()
    );
    assert_eq!(stream.last_cursor(), Some(cursor(4)));
    assert_eq!(stream.next().await, Err(LiveEnd::Lagged));
    assert_eq!(stream.next().await, Err(LiveEnd::Lagged));
    assert_eq!(stream.last_cursor(), Some(cursor(4)));
    assert_eq!(
        stub.requests().len(),
        1,
        "an ended stream does not reconnect"
    );
}

/// A response cut without its terminating chunk, or ended without the end
/// event, is reconnected from the last cursor received, and the items the
/// surface replays after it follow with none lost or repeated.
#[tokio::test]
async fn a_cut_reconnects_from_the_last_cursor() {
    for cut in [Step::Abort, Step::Wait(Duration::ZERO)] {
        let first_cut = cut.clone();
        let mut stub = Stub::start(move |_, n| match n {
            0 => {
                let mut steps = frames(1..=2);
                steps.push(first_cut.clone());
                feed(steps)
            }
            1 => {
                let mut steps = frames(3..=3);
                steps.push(first_cut.clone());
                feed(steps)
            }
            _ => {
                let mut steps = frames(4..=5);
                steps.push(end(LiveEnd::SessionEnded));
                feed(steps)
            }
        })
        .await;
        let mut stream = subscribe(&stub.client(), Resume::From(cursor(0))).await;
        assert_eq!(
            take(&mut stream, 5).await,
            (1..=5).map(item).collect::<Vec<_>>(),
            "{cut:?}"
        );
        assert_eq!(stream.next().await, Err(LiveEnd::SessionEnded));
        let resumes: Vec<Resume> = stub.requests().iter().map(resume_sent).collect();
        assert_eq!(
            resumes,
            vec![
                Resume::From(cursor(0)),
                Resume::From(cursor(2)),
                Resume::From(cursor(3)),
            ],
            "{cut:?}"
        );
    }
}

/// A connection that sends nothing for the idle timeout is cut and
/// reconnected from the last cursor.
#[tokio::test]
async fn a_silent_connection_is_reconnected() {
    let mut stub = Stub::start(|_, n| match n {
        0 => {
            let mut steps = frames(1..=1);
            steps.push(Step::Wait(Duration::from_secs(30)));
            feed(steps)
        }
        _ => {
            let mut steps = frames(2..=2);
            steps.push(end(LiveEnd::ShuttingDown));
            feed(steps)
        }
    })
    .await;
    let config = fast_config()
        .with_idle_timeout(Duration::from_millis(100))
        .unwrap_or_else(|error| panic!("{error}"));
    let client = HttpClient::new(stub.base(), config);
    let mut stream = subscribe(&client, Resume::Fresh).await;
    assert_eq!(take(&mut stream, 2).await, vec![item(1), item(2)]);
    assert_eq!(stream.next().await, Err(LiveEnd::ShuttingDown));
    let resumes: Vec<Resume> = stub.requests().iter().map(resume_sent).collect();
    assert_eq!(resumes, vec![Resume::Fresh, Resume::From(cursor(1))]);
}

/// An event that is not the binding's framing of its item (an id that is
/// not the cursor, a name that is not the item's, an end event with an id,
/// data that is not an item) is never delivered: the connection is cut and
/// resumed from the last cursor actually read.
#[tokio::test]
async fn a_misframed_event_is_a_cut() {
    let misframed = [
        "event: heartbeat\nid: 7-9\ndata: {\"type\":\"heartbeat\",\"data\":{\"cursor\":\"7-2\"}}\n\n",
        "event: event\nid: 7-2\ndata: {\"type\":\"heartbeat\",\"data\":{\"cursor\":\"7-2\"}}\n\n",
        "event: heartbeat\ndata: {\"type\":\"heartbeat\",\"data\":{\"cursor\":\"7-2\"}}\n\n",
        "event: end\nid: 7-2\ndata: \"lagged\"\n\n",
        "event: heartbeat\nid: 7-2\ndata: {\"type\":\"heartbeat\"}\n\n",
        "data: hello\n\n",
    ];
    for bad in misframed {
        let mut stub = Stub::start(move |_, n| match n {
            0 => {
                let mut steps = frames(1..=1);
                steps.push(Step::Send(bad.as_bytes().to_vec()));
                steps.push(Step::Wait(Duration::from_secs(30)));
                feed(steps)
            }
            _ => {
                let mut steps = frames(2..=2);
                steps.push(end(LiveEnd::Lagged));
                feed(steps)
            }
        })
        .await;
        let mut stream = subscribe(&stub.client(), Resume::Fresh).await;
        assert_eq!(take(&mut stream, 2).await, vec![item(1), item(2)], "{bad}");
        assert_eq!(stream.next().await, Err(LiveEnd::Lagged), "{bad}");
        let resumes: Vec<Resume> = stub.requests().iter().map(resume_sent).collect();
        assert_eq!(
            resumes,
            vec![Resume::Fresh, Resume::From(cursor(1))],
            "{bad}"
        );
    }
}

/// A resync item passes through like any other and moves the cursor.
#[tokio::test]
async fn a_resync_is_delivered() {
    let resync = LiveItem::Resync {
        cursor: cursor(40),
        reason: ResyncReason::Expired,
    };
    let frame = event_frame(&resync).unwrap_or_else(|error| panic!("{error}"));
    let stub = Stub::always(feed(vec![
        Step::Send(frame.into_bytes()),
        end(LiveEnd::Lagged),
    ]))
    .await;
    let mut stream = subscribe(&stub.client(), Resume::From(cursor(3))).await;
    assert_eq!(stream.next().await, Ok(resync));
    assert_eq!(stream.last_cursor(), Some(cursor(40)));
}

/// A reconnect the surface answers `401` or `403` ends the stream with
/// `SessionEnded`: the caller signs in again rather than retrying.
#[tokio::test]
async fn a_refused_reconnect_ends_the_session() {
    let refusals = [
        Reply::json(401, r#"{"reason":"invalid_credential"}"#),
        Reply::value(
            403,
            &QueryError::Forbidden {
                missing: Permission::View,
            },
        ),
    ];
    for refusal in refusals {
        let mut stub = Stub::start(move |_, n| match n {
            0 => {
                let mut steps = frames(1..=1);
                steps.push(Step::Abort);
                feed(steps)
            }
            _ => refusal.clone(),
        })
        .await;
        let mut stream = subscribe(&stub.client(), Resume::Fresh).await;
        assert_eq!(take(&mut stream, 1).await, vec![item(1)]);
        assert_eq!(stream.next().await, Err(LiveEnd::SessionEnded));
        assert_eq!(stub.requests().len(), 2);
    }
}

/// Reconnects that keep failing give up after the policy's attempts, with
/// `ShuttingDown`; the last cursor stays the resume point.
#[tokio::test]
async fn reconnects_give_up_after_the_policy() {
    let mut stub = Stub::start(|_, n| match n {
        0 => {
            let mut steps = frames(1..=1);
            steps.push(Step::Abort);
            feed(steps)
        }
        _ => Reply::json(503, r#"{"type":"store","data":{"reason":"bus down"}}"#),
    })
    .await;
    let mut stream = subscribe(&stub.client(), Resume::Fresh).await;
    assert_eq!(take(&mut stream, 1).await, vec![item(1)]);
    assert_eq!(stream.next().await, Err(LiveEnd::ShuttingDown));
    assert_eq!(stream.last_cursor(), Some(cursor(1)));
    let resumes: Vec<Resume> = stub.requests().iter().map(resume_sent).collect();
    assert_eq!(resumes.len(), 1 + 3, "the subscription and three attempts");
    assert!(
        resumes[1..]
            .iter()
            .all(|resume| *resume == Resume::From(cursor(1)))
    );
}

/// A subscription the surface refuses is the refusal.
#[tokio::test]
async fn a_refused_subscription_is_its_error() {
    let forbidden = QueryError::Forbidden {
        missing: Permission::View,
    };
    let stub = Stub::always(Reply::value(403, &forbidden)).await;
    let refused = stub.client().subscribe(&caller(), Resume::Fresh).await;
    assert_eq!(refused.err(), Some(forbidden));

    let stub = Stub::always(Reply::json(200, "{}")).await;
    let refused = stub.client().subscribe(&caller(), Resume::Fresh).await;
    assert!(
        matches!(&refused, Err(QueryError::Store { reason }) if reason.contains("text/event-stream")),
        "{refused:?}"
    );
}

//! Response framers: watch response bytes as they stream past and report
//! the exchange's progress, never holding a byte up.
//!
//! The framer is chosen from the response head alone
//! (`ingress.framer.from-response-head`), never from the request, whose
//! decode may not have finished (`ingress.framer.independent-of-request`):
//!
//! | Status | Content-Type | Framer |
//! | --- | --- | --- |
//! | 2xx | `text/event-stream` | [`FramerKind::EventStream`]: SSE events |
//! | 2xx | anything else | [`FramerKind::SingleBody`]: one JSON document |
//! | other | any | [`FramerKind::ErrorDocument`]: reports nothing |
//!
//! A non-2xx response fails its exchange at the head
//! (`ExchangeFailure::Upstream`), so its body is an error document to keep,
//! not content to frame.
//!
//! **Errors after events in one chunk.** [`ResponseFramer::push`] returns
//! either events or an error. When a chunk completes events and then hits an
//! error, `push` returns the events and holds the error back; the next push
//! (an empty one will do) returns it. The proxy pushes an empty chunk after
//! every push that returned events, so no error is lost and the events and
//! error a stream yields do not depend on how it is chunked
//! (`ingress.framer.chunking-invariant`). After an error, or after
//! `Finished`, a framer reports nothing more.

mod json;
mod sse;

use std::num::NonZeroUsize;

use crosstalk_spec::interfaces::l0_ingress::{
    FrameError, FrameEvent, ResponseFramer, ResponseFraming, ResponseHead,
};

pub use json::JsonFramer;
pub use sse::SseFramer;

pub(crate) type Emitted = Vec<FrameEvent>;

/// How far a framer has got.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum Progress {
    #[default]
    NotStarted,
    Started,
    Finished,
}

/// Which framer a response got.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FramerKind {
    EventStream,
    SingleBody,
    ErrorDocument,
}

impl FramerKind {
    /// The framer the documented table assigns to `head`.
    pub fn for_head(head: &ResponseHead) -> Self {
        if !(200..300).contains(&head.status) {
            return Self::ErrorDocument;
        }
        match head.framing() {
            ResponseFraming::EventStream => Self::EventStream,
            ResponseFraming::Whole => Self::SingleBody,
        }
    }
}

#[derive(Debug)]
enum Inner {
    EventStream(SseFramer),
    SingleBody(JsonFramer),
    ErrorDocument,
}

#[derive(Debug)]
enum State {
    Live,
    /// An error found after events in the same chunk, returned by the next
    /// push.
    Held(FrameError),
    /// An error was returned: nothing more is reported.
    Failed,
}

/// The Anthropic Messages response framer.
#[derive(Debug)]
pub struct AnthropicFramer {
    inner: Inner,
    state: State,
}

impl AnthropicFramer {
    /// The framer for a response with `head`; `max_event` bounds one
    /// server-sent event.
    pub fn for_response(head: &ResponseHead, max_event: NonZeroUsize) -> Self {
        let inner = match FramerKind::for_head(head) {
            FramerKind::EventStream => Inner::EventStream(SseFramer::new(max_event)),
            FramerKind::SingleBody => Inner::SingleBody(JsonFramer::new()),
            FramerKind::ErrorDocument => Inner::ErrorDocument,
        };
        Self {
            inner,
            state: State::Live,
        }
    }

    pub fn kind(&self) -> FramerKind {
        match self.inner {
            Inner::EventStream(_) => FramerKind::EventStream,
            Inner::SingleBody(_) => FramerKind::SingleBody,
            Inner::ErrorDocument => FramerKind::ErrorDocument,
        }
    }
}

impl ResponseFramer for AnthropicFramer {
    fn push(&mut self, chunk: &[u8]) -> Result<Vec<FrameEvent>, FrameError> {
        match std::mem::replace(&mut self.state, State::Live) {
            State::Held(error) => {
                self.state = State::Failed;
                return Err(error);
            }
            State::Failed => {
                self.state = State::Failed;
                return Ok(Vec::new());
            }
            State::Live => {}
        }
        let mut emitted = Vec::new();
        let scanned = match &mut self.inner {
            Inner::EventStream(framer) => framer.scan(chunk, &mut emitted),
            Inner::SingleBody(framer) => framer.scan(chunk, &mut emitted),
            Inner::ErrorDocument => Ok(()),
        };
        match scanned {
            Ok(()) => Ok(emitted),
            Err(error) if emitted.is_empty() => {
                self.state = State::Failed;
                Err(error)
            }
            Err(error) => {
                self.state = State::Held(error);
                Ok(emitted)
            }
        }
    }
}

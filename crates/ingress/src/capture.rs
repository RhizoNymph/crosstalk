//! The capture hand-off: finished exchanges go to the capture task over a
//! bounded channel, and what is not captured is counted by reason.
//!
//! [`CaptureSender`] wraps a `tokio::sync::mpsc::Sender`, which is always
//! bounded, and offers only [`CaptureSender::offer`], a `try_send`: there is
//! no way to wait for capacity. When the channel is full the exchange's
//! `RawExchange` is dropped and counted (`ingress.capture.channel-bounded`,
//! `ingress.capture.drop-when-full`); the client was served long before.

use std::sync::atomic::{AtomicU64, Ordering};

use crosstalk_spec::interfaces::l0_ingress::RawExchange;
use tokio::sync::mpsc;

/// The proxy's end of the capture channel. Never waits.
#[derive(Debug, Clone)]
pub struct CaptureSender {
    sender: mpsc::Sender<RawExchange>,
}

/// What became of an offered exchange.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Offer {
    Accepted,
    /// The channel was at capacity; the exchange was dropped.
    Full,
    /// The capture task is gone; the exchange was dropped.
    Closed,
}

impl CaptureSender {
    /// Wrap the sending half of a bounded channel the caller made with
    /// `tokio::sync::mpsc::channel(capacity)`.
    pub fn new(sender: mpsc::Sender<RawExchange>) -> Self {
        Self { sender }
    }

    /// The channel's configured capacity.
    pub fn capacity(&self) -> usize {
        self.sender.max_capacity()
    }

    /// Hand `exchange` to capture if there is room, without waiting.
    pub fn offer(&self, exchange: RawExchange) -> Offer {
        match self.sender.try_send(exchange) {
            Ok(()) => Offer::Accepted,
            Err(mpsc::error::TrySendError::Full(_)) => Offer::Full,
            Err(mpsc::error::TrySendError::Closed(_)) => Offer::Closed,
        }
    }
}

/// Why a request produced no `RawExchange`
/// (`ingress.capture.loss-counted`). Non-generation endpoints are uncaptured
/// by design and are not counted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum UncapturedReason {
    /// No adapter classified the request.
    Unclassified,
    /// A generation request whose body could not be decoded: not JSON, a
    /// missing field, an unsupported version or encoding, a body larger than
    /// the tee or the decoded-size bound, or a body that never arrived in
    /// full.
    DecodeError,
    /// The capture channel was full.
    ChannelFull,
    /// The capture task had stopped.
    ChannelClosed,
    /// The response was larger than the response capture bound.
    ResponseTooLarge,
}

impl UncapturedReason {
    pub const ALL: [Self; 5] = [
        Self::Unclassified,
        Self::DecodeError,
        Self::ChannelFull,
        Self::ChannelClosed,
        Self::ResponseTooLarge,
    ];

    /// The counter's label.
    pub fn label(self) -> &'static str {
        match self {
            Self::Unclassified => "unclassified",
            Self::DecodeError => "decode_error",
            Self::ChannelFull => "channel_full",
            Self::ChannelClosed => "channel_closed",
            Self::ResponseTooLarge => "response_too_large",
        }
    }

    fn index(self) -> usize {
        match self {
            Self::Unclassified => 0,
            Self::DecodeError => 1,
            Self::ChannelFull => 2,
            Self::ChannelClosed => 3,
            Self::ResponseTooLarge => 4,
        }
    }
}

/// Capture counters, shared by every connection of one proxy.
#[derive(Debug, Default)]
pub struct CaptureStats {
    captured: AtomicU64,
    uncaptured: [AtomicU64; 5],
}

/// A reading of [`CaptureStats`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct CaptureCounts {
    pub captured: u64,
    pub unclassified: u64,
    pub decode_error: u64,
    pub channel_full: u64,
    pub channel_closed: u64,
    pub response_too_large: u64,
}

impl CaptureCounts {
    pub fn uncaptured(&self, reason: UncapturedReason) -> u64 {
        match reason {
            UncapturedReason::Unclassified => self.unclassified,
            UncapturedReason::DecodeError => self.decode_error,
            UncapturedReason::ChannelFull => self.channel_full,
            UncapturedReason::ChannelClosed => self.channel_closed,
            UncapturedReason::ResponseTooLarge => self.response_too_large,
        }
    }
}

impl CaptureStats {
    pub fn new() -> Self {
        Self::default()
    }

    pub(crate) fn captured(&self) {
        self.captured.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn uncaptured(&self, reason: UncapturedReason) {
        self.uncaptured[reason.index()].fetch_add(1, Ordering::Relaxed);
        tracing::debug!(reason = reason.label(), "request not captured");
    }

    pub fn snapshot(&self) -> CaptureCounts {
        let read =
            |reason: UncapturedReason| self.uncaptured[reason.index()].load(Ordering::Relaxed);
        CaptureCounts {
            captured: self.captured.load(Ordering::Relaxed),
            unclassified: read(UncapturedReason::Unclassified),
            decode_error: read(UncapturedReason::DecodeError),
            channel_full: read(UncapturedReason::ChannelFull),
            channel_closed: read(UncapturedReason::ChannelClosed),
            response_too_large: read(UncapturedReason::ResponseTooLarge),
        }
    }
}

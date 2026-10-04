//! Provider adapters: one per wire protocol.
//!
//! Only Anthropic Messages exists (roadmap P2.4). OpenAI Chat and Responses,
//! Gemini and Code Assist adapters, and the WebSocket taps, come later (P8).

mod anthropic;

use crosstalk_spec::interfaces::l0_ingress::{FrameError, TurnEvent, WebSocketTap};
use crosstalk_spec::support::Timestamp;

pub use anthropic::{ANTHROPIC_VERSION, AnthropicAdapter};

/// The tap of a protocol with no WebSocket transport: it has no values.
#[derive(Debug)]
pub enum NoTap {}

impl WebSocketTap for NoTap {
    fn client_frame(
        &mut self,
        _frame: &[u8],
        _at: Timestamp,
    ) -> Result<Vec<TurnEvent>, FrameError> {
        match *self {}
    }

    fn server_frame(
        &mut self,
        _frame: &[u8],
        _at: Timestamp,
    ) -> Result<Vec<TurnEvent>, FrameError> {
        match *self {}
    }

    fn close(&mut self, _at: Timestamp) -> Vec<TurnEvent> {
        match *self {}
    }
}

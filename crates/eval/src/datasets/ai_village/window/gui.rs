//! Edits made through the GUI that no tool call reveals.
//!
//! Agents edit Google Docs and send Gmail by clicking and typing in a
//! browser: the turn's action is a click or keystroke, and only the
//! screenshot shows what changed. A transmission through such an edit is
//! unobservable to the gateway, so these are counted, never labelled. A GUI
//! turn counts when its action text or the model's visible text names
//! Google Docs or Gmail.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// GUI turns that touched a Google Doc or Gmail.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GuiStats {
    pub gui_turns: u64,
    pub google_docs: u64,
    pub gmail: u64,
}

const GUI_ACTIONS: &[&str] = &[
    "type",
    "key",
    "left_click",
    "double_click",
    "triple_click",
    "right_click",
    "middle_click",
    "left_click_drag",
    "scroll",
    "hold_key",
    "left_mouse_down",
    "left_mouse_up",
    "mouse_move",
];

impl GuiStats {
    /// Counts one turn, given its action and the model's visible text.
    pub fn count(&mut self, action: Option<&Value>, visible: &str) {
        let Some(kind) = action.and_then(|a| a.get("action")).and_then(Value::as_str) else {
            return;
        };
        if !GUI_ACTIONS.contains(&kind) {
            return;
        }
        self.gui_turns += 1;
        let typed = action
            .and_then(|a| a.get("text"))
            .and_then(Value::as_str)
            .unwrap_or("");
        let mentions = |needles: &[&str]| {
            needles
                .iter()
                .any(|needle| typed.contains(needle) || visible.contains(needle))
        };
        if mentions(&["docs.google.com", "Google Doc"]) {
            self.google_docs += 1;
        }
        if mentions(&["mail.google.com", "Gmail"]) {
            self.gmail += 1;
        }
    }

    pub fn add(&mut self, other: &Self) {
        self.gui_turns += other.gui_turns;
        self.google_docs += other.google_docs;
        self.gmail += other.gmail;
    }
}

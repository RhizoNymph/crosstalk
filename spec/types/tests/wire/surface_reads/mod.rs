//! The surface read models and export on the wire: channel rows, names and
//! the promotion preview (`channels`), a channel's cross-agent transmissions
//! (`channel_traffic`), transmission rows and selections
//! (`transmissions`), evidence and excerpts (`evidence`), and export
//! requests, manifests, rows and the JSONL framing (`export`), and the
//! gateway's present and config (`present`). Goldens are
//! under `golden/surface_reads/<area>/`, the one JSONL golden
//! (`export/export_complete.jsonl`) beside the JSON ones.

mod channel_traffic;
mod channels;
mod conversation;
mod evidence;
mod export;
mod export_states;
mod fixtures;
mod present;
mod transmissions;

//! The surface read models and export on the wire: channel rows, names and
//! the promotion preview (`channels`), transmission rows and selections
//! (`transmissions`), evidence and excerpts (`evidence`), and export
//! requests, manifests, rows and the JSONL framing (`export`). Goldens are
//! under `golden/surface-reads/<area>/`; the one JSONL golden is under
//! `jsonl/surface-reads/`, since `golden/` holds only `.json` files.

mod channels;
mod evidence;
mod export;
mod fixtures;
mod transmissions;

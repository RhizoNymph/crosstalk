//! Observed facts on the wire: exchanges and their client context
//! ([`exchange`]), agent identity, harness claims and the merge log
//! ([`identity`]), and the ingest bus events that carry them, each inside a
//! full `Envelope` ([`ingest`]). Goldens in `golden/observed/`.

mod exchange;
mod identity;
mod ingest;

use super::{ULID_A, ULID_B, ULID_C, id};
use crate::ids::{AgentId, MergeId, MessageHash, OperatorId};
use crate::support::Blake3;

const AREA: &str = "observed";

/// Two more realistic ULIDs for fixtures that need five distinct ids.
const ULID_D: &str = "01J9Z3P6Q7R8S9T0V1W2X3Y4Z5";
const ULID_E: &str = "01J9Z3Q8R9S0T1V2W3X4Y5Z6A7";

/// Hex digests for fixtures (SHA-256 of a word, so they look like digests).
const REQUEST_HEX: &str = "1f58b9145b24d108d7ac38887338b3ea3229833b9c1e418250343f907bfd1047";
const RESPONSE_HEX: &str = "a9f4b3d22a523fdada41c85c175425bcd15b32b4cd0f54d9433accd52d7195a1";
const SYSTEM_HEX: &str = "bbc5e661e106c6dcd8dc6dd186454c2fcba3c710fb4d8e71a60c93eaf077f073";
const PARTIAL_HEX: &str = "9834a14ab9bcaa0f6a8da71073617eac8f004e596a3fa11d807b84631b825d9d";
const TOOL_RESULT_HEX: &str = "90c6783d80fad0eefa7c2c8887502d4a9d8839e043cd5f51582aa89f71cc9fb5";
const CREDENTIAL_HEX: &str = "e265b6f564601a1fe8dc42785cd18a868bd8013eb5899560e79248767a683e6b";
const ACCOUNT_HEX: &str = "9af211329b2fc82e5efe906062c730082819b23fe8394bc435e0b1bf0458eb54";
const PREVIOUS_CREDENTIAL_HEX: &str =
    "0ec70a04edb4449fd2dd7f2119fff1fc0b9d8883c152b4d81284658b0f8b681a";
const PREVIOUS_ACCOUNT_HEX: &str =
    "856c88930ee7c7123f3b42fbd942ad983f7874c3a634b040a37f0246c589b0e1";
const PROMPT_HEX: &str = "cf07194ee232eb531e15f690000d19846dea69cf05504782658afcfacb9228a2";

fn digest(hex: &str) -> Blake3 {
    Blake3::from_hex(hex).unwrap_or_else(|error| panic!("{hex} is a digest: {error:?}"))
}

fn message(hex: &str) -> MessageHash {
    MessageHash::from_digest(digest(hex))
}

fn agent(text: &str) -> AgentId {
    id(AgentId::from_ulid_text, text)
}

fn merge(text: &str) -> MergeId {
    id(MergeId::from_ulid_text, text)
}

fn operator() -> OperatorId {
    id(OperatorId::from_ulid_text, ULID_C)
}

/// The planner, the coder and the reviewer of the fixtures.
fn planner() -> AgentId {
    agent(ULID_A)
}

fn coder() -> AgentId {
    agent(ULID_B)
}

fn reviewer() -> AgentId {
    agent(ULID_D)
}

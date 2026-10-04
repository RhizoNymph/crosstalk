//! The cursors the surface issues itself, for the one list no store pages:
//! `QueryApi::transmissions_by_id`.
//!
//! A token carries the resume point (the topic-model version the first page
//! resolved and the last id served) and a digest of the request it was
//! issued for, authenticated with a keyed BLAKE3 MAC under a key drawn when
//! the surface starts. So a token the surface did not issue, one whose
//! payload was changed, and one presented with another request all fail
//! [`CursorKey::open`] and become `InvalidCursor`; nothing is stored per
//! cursor.
//!
//! ```text
//! token = hex(version u32 BE ‖ last id u128 BE ‖ request digest 32 B) "_" hex(MAC 16 B)
//! MAC   = BLAKE3-keyed(key, payload)[..16]
//! ```

use std::collections::VecDeque;
use std::sync::{Mutex, PoisonError};

use crosstalk_spec::aggregates::topic::{EmbeddingModel, TopicModelVersion};
use crosstalk_spec::ids::RandomSource;
use crosstalk_spec::paging::Cursor;
use crosstalk_spec::support::{from_hex, hex};

const PAYLOAD_LEN: usize = 4 + 16 + 32;
const MAC_LEN: usize = 16;

/// The key the surface's cursors are authenticated with. Never serialized
/// or shown: a client that knew it could forge cursors.
#[derive(Clone)]
pub struct CursorKey([u8; 32]);

impl std::fmt::Debug for CursorKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("CursorKey(..)")
    }
}

/// Where a page of a surface-paged list resumes: the version its first page
/// resolved, and the last id it served.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Resume {
    pub version: TopicModelVersion,
    pub after: u128,
}

impl CursorKey {
    /// A key of four draws from `random`.
    pub fn draw(random: &mut impl RandomSource) -> Self {
        let mut key = [0_u8; 32];
        for chunk in key.as_chunks_mut::<8>().0 {
            *chunk = random.next_u64().to_le_bytes();
        }
        Self(key)
    }

    /// A cursor resuming at `resume` for the request whose digest is
    /// `request`.
    pub fn seal<L>(&self, resume: Resume, request: &[u8; 32]) -> Option<Cursor<L>> {
        let payload = payload(resume, request);
        let mac = self.mac(&payload);
        Cursor::from_token(format!("{}_{}", hex(&payload), hex(&mac))).ok()
    }

    /// The resume point of `cursor`, if this key sealed it for the request
    /// whose digest is `request`.
    pub fn open<L>(&self, cursor: &Cursor<L>, request: &[u8; 32]) -> Option<Resume> {
        let (payload, mac) = cursor.token().split_once('_')?;
        let payload = from_hex(payload).ok()?;
        let mac = from_hex(mac).ok()?;
        if payload.len() != PAYLOAD_LEN || mac.len() != MAC_LEN {
            return None;
        }
        // Constant time is not needed: a MAC mismatch reveals nothing a
        // client could not learn by presenting the token.
        if self.mac(&payload) != mac.as_slice() || payload[20..] != request[..] {
            return None;
        }
        let version = u32::from_be_bytes(payload[..4].try_into().ok()?);
        let after = u128::from_be_bytes(payload[4..20].try_into().ok()?);
        Some(Resume {
            version: TopicModelVersion(version),
            after,
        })
    }

    fn mac(&self, payload: &[u8]) -> [u8; MAC_LEN] {
        let full = blake3::keyed_hash(&self.0, payload);
        let mut mac = [0_u8; MAC_LEN];
        mac.copy_from_slice(&full.as_bytes()[..MAC_LEN]);
        mac
    }
}

fn payload(resume: Resume, request: &[u8; 32]) -> Vec<u8> {
    let mut payload = Vec::with_capacity(PAYLOAD_LEN);
    payload.extend_from_slice(&resume.version.0.to_be_bytes());
    payload.extend_from_slice(&resume.after.to_be_bytes());
    payload.extend_from_slice(request);
    payload
}

/// A digest of a request, built from its parts in order under a context
/// naming the list, so two lists' requests never share a digest.
#[derive(Debug)]
pub struct RequestDigest(blake3::Hasher);

impl RequestDigest {
    pub fn new(list: &str) -> Self {
        Self(blake3::Hasher::new_derive_key(&format!(
            "crosstalk surface cursor v1 {list}"
        )))
    }

    pub fn u128(mut self, value: u128) -> Self {
        self.0.update(&value.to_le_bytes());
        self
    }

    pub fn u32(mut self, value: u32) -> Self {
        self.0.update(&value.to_le_bytes());
        self
    }

    pub fn tag(mut self, tag: u8) -> Self {
        self.0.update(&[tag]);
        self
    }

    pub fn finish(self) -> [u8; 32] {
        *self.0.finalize().as_bytes()
    }
}

/// The embedding model each search cursor's traversal was embedded with,
/// for the newest [`SearchModels::CAPACITY`] cursors issued.
///
/// A search cursor is the index's, bound to the query it was issued for;
/// a later page re-embeds the text with the current model, so after a
/// model change the query no longer matches the cursor and an index would
/// report the cursor, not the change. The surface remembers the model each
/// cursor's traversal used and answers a later page embedded with another
/// as `Conflict(EmbeddingModelChanged)` before asking the index. A cursor
/// it no longer remembers (evicted, or issued by another node) is left to
/// the index.
#[derive(Debug, Default)]
pub struct SearchModels {
    issued: Mutex<VecDeque<(String, EmbeddingModel)>>,
}

impl SearchModels {
    pub const CAPACITY: usize = 4096;

    /// The model the traversal `token` continues was embedded with.
    pub fn model(&self, token: &str) -> Option<EmbeddingModel> {
        self.issued
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .rev()
            .find(|(issued, _)| issued == token)
            .map(|(_, model)| model.clone())
    }

    /// Remember that the traversal continuing at `token` was embedded with
    /// `model`.
    pub fn remember(&self, token: &str, model: EmbeddingModel) {
        let mut issued = self.issued.lock().unwrap_or_else(PoisonError::into_inner);
        if issued.len() == Self::CAPACITY {
            issued.pop_front();
        }
        issued.push_back((token.to_owned(), model));
    }
}

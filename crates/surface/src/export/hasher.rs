//! The row digest's hash: BLAKE3 in key-derivation mode under
//! [`ROW_DIGEST_CONTEXT`], as `export::digest` defines it.

use crosstalk_spec::interfaces::l8_surface::export::{ROW_DIGEST_CONTEXT, RowHasher};
use crosstalk_spec::support::Blake3;

/// `blake3::Hasher::new_derive_key(ROW_DIGEST_CONTEXT)` as a [`RowHasher`].
#[derive(Debug, Clone)]
pub struct Blake3RowHasher(blake3::Hasher);

impl Blake3RowHasher {
    pub fn new() -> Self {
        Self(blake3::Hasher::new_derive_key(ROW_DIGEST_CONTEXT))
    }
}

impl Default for Blake3RowHasher {
    fn default() -> Self {
        Self::new()
    }
}

impl RowHasher for Blake3RowHasher {
    fn update(&mut self, bytes: &[u8]) {
        self.0.update(bytes);
    }

    fn finalize(&self) -> Blake3 {
        Blake3::from_bytes(*self.0.finalize().as_bytes())
    }
}

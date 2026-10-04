//! Page cursors for L6's Postgres reads.
//!
//! A token is `<position hex>_<tag hex>`: the position is the last sort key
//! served (encoded by the read that issued it), and the tag a keyed BLAKE3
//! over the store's [`CursorKey`], the list's name, a digest of the request
//! and the position. A cursor another store issued, one issued for another
//! list or request, and an edited one all fail the tag check, which the read
//! reports as its `InvalidCursor`. The token alphabet (`0-9 a-f _`) is
//! inside the URL-safe one `Cursor::from_token` accepts.

use crosstalk_spec::paging::Cursor;

/// The secret that makes a store's cursors its own. Every node serving one
/// database should share it, so a cursor survives a restart or a hop
/// between nodes; tests use a fixed one.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct CursorKey([u8; 32]);

impl CursorKey {
    pub const fn new(key: [u8; 32]) -> Self {
        Self(key)
    }
}

impl std::fmt::Debug for CursorKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("CursorKey(..)")
    }
}

/// Bytes of the tag kept in the token.
const TAG_BYTES: usize = 16;

fn tag(key: &CursorKey, list: &str, request: &[u8], position: &[u8]) -> [u8; TAG_BYTES] {
    let mut hasher = blake3::Hasher::new_keyed(&key.0);
    for part in [list.as_bytes(), request, position] {
        // Length-prefixed, so no two splits of the same bytes collide.
        hasher.update(&(part.len() as u64).to_le_bytes());
        hasher.update(part);
    }
    let mut out = [0u8; TAG_BYTES];
    out.copy_from_slice(&hasher.finalize().as_bytes()[..TAG_BYTES]);
    out
}

/// A digest of a request, for binding cursors to it.
pub fn request_digest(parts: &[&[u8]]) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    for part in parts {
        hasher.update(&(part.len() as u64).to_le_bytes());
        hasher.update(part);
    }
    *hasher.finalize().as_bytes()
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(char::from(DIGITS[usize::from(byte >> 4)]));
        out.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
    out
}

fn unhex(text: &str) -> Option<Vec<u8>> {
    if !text.len().is_multiple_of(2) {
        return None;
    }
    let digit = |c: u8| match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        _ => None,
    };
    text.as_bytes()
        .chunks(2)
        .map(|pair| Some(digit(pair[0])? << 4 | digit(pair[1])?))
        .collect()
}

/// Why a cursor could not be issued. Only a position too long for a token,
/// which no read of this crate produces.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("a cursor position of {0} bytes does not fit a token")]
pub struct CursorTooLong(pub usize);

/// A cursor for `list` resuming after `position`, bound to `request`.
pub fn issue<L>(
    key: &CursorKey,
    list: &str,
    request: &[u8],
    position: &[u8],
) -> Result<Cursor<L>, CursorTooLong> {
    let token = format!(
        "{}_{}",
        hex(position),
        hex(&tag(key, list, request, position))
    );
    Cursor::from_token(token).map_err(|_| CursorTooLong(position.len()))
}

/// The position `cursor` resumes after, when this store issued it for
/// `list` and `request`; `None` otherwise.
pub fn resume<L>(
    key: &CursorKey,
    list: &str,
    request: &[u8],
    cursor: &Cursor<L>,
) -> Option<Vec<u8>> {
    let (position, given) = cursor.token().split_once('_')?;
    let position = unhex(position)?;
    let given = unhex(given)?;
    // Not constant time: a tag only guards against stale or foreign
    // cursors, and a forged one only reads what its request already may.
    (given == tag(key, list, request, &position)).then_some(position)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crosstalk_spec::paging::AlertList;

    const KEY: CursorKey = CursorKey::new([7; 32]);

    #[test]
    fn a_cursor_resumes_only_for_its_list_request_and_key() -> Result<(), CursorTooLong> {
        let cursor: Cursor<AlertList> = issue(&KEY, "alerts", b"filter", b"position")?;
        assert_eq!(
            resume(&KEY, "alerts", b"filter", &cursor),
            Some(b"position".to_vec())
        );
        assert_eq!(resume(&KEY, "rules", b"filter", &cursor), None);
        assert_eq!(resume(&KEY, "alerts", b"other", &cursor), None);
        assert_eq!(
            resume(&CursorKey::new([8; 32]), "alerts", b"filter", &cursor),
            None
        );
        Ok(())
    }

    #[test]
    fn an_edited_or_foreign_token_is_refused() -> Result<(), CursorTooLong> {
        let cursor: Cursor<AlertList> = issue(&KEY, "alerts", b"f", b"\x01\x02")?;
        let edited = cursor.token().replacen("0102", "0103", 1);
        let edited = Cursor::<AlertList>::from_token(edited).ok();
        assert!(edited.is_some_and(|edited| resume(&KEY, "alerts", b"f", &edited).is_none()));
        for token in ["nounderscore", "zz_00", "0_00", "-_-"] {
            let foreign = Cursor::<AlertList>::from_token(token.to_owned()).ok();
            assert!(
                foreign.is_some_and(|foreign| resume(&KEY, "alerts", b"f", &foreign).is_none())
            );
        }
        Ok(())
    }
}

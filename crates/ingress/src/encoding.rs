//! Request body encodings: reading `content-encoding` and decoding gzip and
//! zstd bodies for capture, never past a size bound.
//!
//! Decoding runs off the hot path on the tee's copy. The bound
//! (`ingress.encoding.decoded-size-bounded`) stops a small compressed body
//! from expanding without limit: past it, decoding is abandoned and only the
//! exchange's capture is lost.

use std::io::Read;

use crosstalk_spec::interfaces::l0_ingress::{ContentEncoding, RequestHead};

use crate::identify::header;

/// Why a body could not be decoded.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum EncodingError {
    /// A `content-encoding` the proxy does not decode (`br`, `deflate`, a
    /// stack of several).
    #[error("unsupported content-encoding {0:?}")]
    Unsupported(String),
    /// The compressed bytes are corrupt or cut short.
    #[error("the {encoding:?} body is corrupt: {reason}")]
    Corrupt {
        encoding: ContentEncoding,
        reason: String,
    },
    /// The body decodes to more than the configured bound.
    #[error("the {encoding:?} body decodes to more than {limit} bytes")]
    TooLarge {
        encoding: ContentEncoding,
        limit: u64,
    },
}

/// The request's `content-encoding`: absent or `identity` is
/// [`ContentEncoding::Identity`]; `gzip`, `x-gzip` and `zstd` are decoded;
/// anything else, or more than one coding, is unsupported.
pub fn content_encoding(head: &RequestHead) -> Result<ContentEncoding, EncodingError> {
    let Some(value) = header(head, "content-encoding") else {
        return Ok(ContentEncoding::Identity);
    };
    let codings: Vec<&str> = value
        .split(',')
        .map(str::trim)
        .filter(|coding| !coding.is_empty() && !coding.eq_ignore_ascii_case("identity"))
        .collect();
    match codings.as_slice() {
        [] => Ok(ContentEncoding::Identity),
        [coding]
            if coding.eq_ignore_ascii_case("gzip") || coding.eq_ignore_ascii_case("x-gzip") =>
        {
            Ok(ContentEncoding::Gzip)
        }
        [coding] if coding.eq_ignore_ascii_case("zstd") => Ok(ContentEncoding::Zstd),
        _ => Err(EncodingError::Unsupported(value.to_owned())),
    }
}

/// `body` decoded from `encoding`, at most `limit` bytes. An identity body
/// over the limit is refused too, so the bound holds whatever the encoding.
pub fn decode(
    body: &[u8],
    encoding: ContentEncoding,
    limit: u64,
) -> Result<Vec<u8>, EncodingError> {
    match encoding {
        ContentEncoding::Identity => {
            if body.len() as u64 > limit {
                return Err(EncodingError::TooLarge { encoding, limit });
            }
            Ok(body.to_vec())
        }
        ContentEncoding::Gzip => {
            read_bounded(flate2::read::MultiGzDecoder::new(body), encoding, limit)
        }
        ContentEncoding::Zstd => {
            let decoder =
                zstd::stream::read::Decoder::new(body).map_err(|error| EncodingError::Corrupt {
                    encoding,
                    reason: error.to_string(),
                })?;
            read_bounded(decoder, encoding, limit)
        }
    }
}

/// Read at most `limit + 1` bytes: one more than allowed proves the body is
/// too large without decoding the rest.
fn read_bounded(
    reader: impl Read,
    encoding: ContentEncoding,
    limit: u64,
) -> Result<Vec<u8>, EncodingError> {
    let mut decoded = Vec::new();
    reader
        .take(limit.saturating_add(1))
        .read_to_end(&mut decoded)
        .map_err(|error| EncodingError::Corrupt {
            encoding,
            reason: error.to_string(),
        })?;
    if decoded.len() as u64 > limit {
        return Err(EncodingError::TooLarge { encoding, limit });
    }
    Ok(decoded)
}

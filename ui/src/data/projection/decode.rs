//! A strict decoder for the projection format, used by the tests to check
//! the encoder round-trips. The TypeScript decoder in
//! `ui/elements/src/payloads/projection.ts` follows the same steps.

use super::format::{MAGIC, NONE, ProjectionHeader, VERSION};

#[derive(Debug, Clone, PartialEq)]
pub struct Decoded {
    pub header: ProjectionHeader,
    pub xs: Vec<f32>,
    pub ys: Vec<f32>,
    pub senders: Vec<u32>,
    pub readers: Vec<u32>,
    pub routes: Vec<u8>,
    pub channels: Vec<Option<u32>>,
    pub topics: Vec<Option<u32>>,
    pub transmissions: Vec<u128>,
}

#[derive(Debug, thiserror::Error)]
pub enum DecodeError {
    #[error("bad magic")]
    Magic,
    #[error("unsupported version {0}")]
    Version(u32),
    #[error("truncated at {0}")]
    Truncated(usize),
    #[error("header length {0} is not a multiple of 4")]
    Unaligned(usize),
    #[error("header: {0}")]
    Header(#[from] serde_json::Error),
    #[error("{0} trailing bytes")]
    Trailing(usize),
    #[error("point {0} has an index outside its table")]
    Index(usize),
}

struct Reader<'a> {
    bytes: &'a [u8],
    at: usize,
}

impl<'a> Reader<'a> {
    fn take(&mut self, len: usize) -> Result<&'a [u8], DecodeError> {
        let end = self
            .at
            .checked_add(len)
            .ok_or(DecodeError::Truncated(self.at))?;
        let slice = self
            .bytes
            .get(self.at..end)
            .ok_or(DecodeError::Truncated(self.at))?;
        self.at = end;
        Ok(slice)
    }

    fn u32(&mut self) -> Result<u32, DecodeError> {
        let mut word = [0u8; 4];
        word.copy_from_slice(self.take(4)?);
        Ok(u32::from_le_bytes(word))
    }

    fn u32s(&mut self, n: usize) -> Result<Vec<u32>, DecodeError> {
        (0..n).map(|_| self.u32()).collect()
    }

    fn f32s(&mut self, n: usize) -> Result<Vec<f32>, DecodeError> {
        (0..n).map(|_| self.u32().map(f32::from_bits)).collect()
    }
}

fn optional(values: Vec<u32>) -> Vec<Option<u32>> {
    values
        .into_iter()
        .map(|v| (v != NONE).then_some(v))
        .collect()
}

pub fn decode(bytes: &[u8]) -> Result<Decoded, DecodeError> {
    let mut r = Reader { bytes, at: 0 };
    if r.take(4)? != MAGIC {
        return Err(DecodeError::Magic);
    }
    let version = r.u32()?;
    if version != VERSION {
        return Err(DecodeError::Version(version));
    }
    let header_len = r.u32()? as usize;
    if !header_len.is_multiple_of(4) {
        return Err(DecodeError::Unaligned(header_len));
    }
    let header: ProjectionHeader = serde_json::from_slice(r.take(header_len)?)?;
    let n = header.count as usize;
    let xs = r.f32s(n)?;
    let ys = r.f32s(n)?;
    let senders = r.u32s(n)?;
    let readers = r.u32s(n)?;
    let routes = r.take(n)?.to_vec();
    r.take((4 - n % 4) % 4)?;
    let channels = optional(r.u32s(n)?);
    let topics = optional(r.u32s(n)?);
    let transmissions = (0..n)
        .map(|_| {
            let mut id = [0u8; 16];
            id.copy_from_slice(r.take(16)?);
            Ok(u128::from_be_bytes(id))
        })
        .collect::<Result<Vec<_>, DecodeError>>()?;
    if r.at != bytes.len() {
        return Err(DecodeError::Trailing(bytes.len() - r.at));
    }
    let inside = |i: Option<u32>, len: usize| i.is_none_or(|i| (i as usize) < len);
    for i in 0..n {
        if !inside(Some(senders[i]), header.agents.len())
            || !inside(Some(readers[i]), header.agents.len())
            || !inside(Some(u32::from(routes[i])), header.route_kinds.len())
            || !inside(channels[i], header.channels.len())
            || !inside(topics[i], header.topics.len())
        {
            return Err(DecodeError::Index(i));
        }
    }
    Ok(Decoded {
        header,
        xs,
        ys,
        senders,
        readers,
        routes,
        channels,
        topics,
        transmissions,
    })
}

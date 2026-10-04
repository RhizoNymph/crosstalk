//! Batches of ids for lookups that answer many ids in one call
//! (`QueryApi::agent_names` and `channel_names`), so a page of rows resolves
//! its names at once instead of one id at a time.
//!
//! Every batch lookup takes an [`IdBatch`], so there is one bound and one
//! refusal for all of them: more than [`IdBatch::MAX`] distinct ids is
//! [`TooManyIds`], which the surface returns as `InvalidInput(TooManyIds)`
//! before calling the lookup. A selection of transmissions to list
//! (`TransmissionSelection`) is not a name lookup and has its own, larger
//! bound, but reports going over it with the same `InvalidInput(TooManyIds)`.
//!
//! On the wire an [`IdBatch`] is an array of ids, and it is a
//! [`WireRequest`]. Decoding runs [`IdBatch::new`]: repeats are dropped and
//! the ids sorted, and more than [`IdBatch::MAX`] distinct ids is a decode
//! error.

use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::wire::{Rejected, WireRequest};

/// Distinct ids, ascending, at most [`IdBatch::MAX`] of them.
///
/// Built only through [`IdBatch::new`], which drops repeats before counting:
/// a lookup answers per distinct id, so asking twice costs nothing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdBatch<T> {
    ids: Vec<T>,
}

/// A batch with more distinct ids than [`IdBatch::MAX`]. The surface reports
/// it as `InvalidInput(TooManyIds)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TooManyIds {
    pub max: usize,
    pub got: usize,
}

impl<T: Ord + Copy> IdBatch<T> {
    /// One cap for every name lookup, agents and channels alike: twice the
    /// largest page (`PageSize::MAX`). A list row names at most two agents
    /// (a sender and a reader) and at most one channel (its route or its
    /// subject), so the names of any one page fit in one call of each
    /// lookup; the rare page that names more (an audit page of promotions,
    /// each naming every channel it superseded) splits its lookup. One cap
    /// rather than one per lookup, so a client sizes every batch the same
    /// way and `TooManyIds` from a name lookup always means the same bound.
    pub const MAX: usize = 1000;

    pub fn new(ids: impl IntoIterator<Item = T>) -> Result<Self, TooManyIds> {
        let mut ids: Vec<T> = ids.into_iter().collect();
        ids.sort_unstable();
        ids.dedup();
        if ids.len() > Self::MAX {
            return Err(TooManyIds {
                max: Self::MAX,
                got: ids.len(),
            });
        }
        Ok(Self { ids })
    }

    /// Ascending, each once.
    pub fn ids(&self) -> &[T] {
        &self.ids
    }

    pub fn len(&self) -> usize {
        self.ids.len()
    }

    pub fn is_empty(&self) -> bool {
        self.ids.is_empty()
    }
}

/// The ids, ascending.
impl<T: Serialize> Serialize for IdBatch<T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.ids.serialize(serializer)
    }
}

impl<'de, T: Deserialize<'de> + Ord + Copy> Deserialize<'de> for IdBatch<T> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let ids = Vec::<T>::deserialize(deserializer)?;
        Self::new(ids).map_err(|error| D::Error::custom(Rejected::new("id batch", error)))
    }
}

/// A client asks for the names of a batch of ids.
impl<T: Serialize + serde::de::DeserializeOwned + Ord + Copy> WireRequest for IdBatch<T> {}

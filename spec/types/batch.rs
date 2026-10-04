//! Batches of ids for lookups that answer many ids in one call, such as
//! `QueryApi::agent_names`, so a page of rows resolves its names at once
//! instead of one id at a time.

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
    /// Twice the largest page (`PageSize::MAX`): a page row names at most
    /// two agents (a sender and a reader), so the names of any one page fit
    /// in one batch.
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

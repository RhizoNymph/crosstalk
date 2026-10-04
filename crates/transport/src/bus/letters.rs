//! The dead-letter shelf the bus task owns: stored letters, newest envelope
//! first, and the cursors that page through them.
//!
//! Letters are keyed by (`Envelope::id`, group), the list's sort key, so a
//! second letter for the same envelope and group replaces the first (both
//! carry the envelope with that id).
//!
//! A cursor token is fixed-length text over `0-9 A-Z a-f`:
//!
//! | Part | Length | Content |
//! | --- | --- | --- |
//! | last id | 26 | the last served letter's `EventId` as ULID text |
//! | last group | 8 | hex index of the last served letter's group |
//! | filter | 9 | `A00000000` for every group, or `G` and the hex index of the group listed |
//! | check | 16 | hex of a keyed hash over the parts before it |
//!
//! Groups are numbered as cursors first name them; the numbering lives as
//! long as the bus. The check is keyed with a per-bus random key (std's
//! SipHash `RandomState`), so a token this bus did not issue, or one issued
//! for another group filter, is `InvalidCursor`. It is an integrity check
//! against misuse, not a cryptographic MAC; the surface authenticates the
//! cursors it hands to clients.

use std::collections::{BTreeMap, HashMap};
use std::hash::{BuildHasher, RandomState};
use std::ops::Bound;

use crosstalk_spec::ids::EventId;
use crosstalk_spec::interfaces::l2_transport::{BusError, ConsumerGroup, DeadLetter};
use crosstalk_spec::paging::{Cursor, DeadLetterList, Page, PageRequest};
use crosstalk_spec::support::NonEmpty;

type Key = (EventId, String);

#[derive(Debug, Default)]
pub(crate) struct Shelf {
    letters: BTreeMap<Key, DeadLetter>,
    check_key: RandomState,
    groups: Vec<String>,
    group_index: HashMap<String, u32>,
    /// Test fault: refuse this many puts before storing again.
    #[cfg(test)]
    pub(crate) failing_puts: u32,
}

const ID_LEN: usize = 26;
const INDEX_LEN: usize = 8;
const FILTER_LEN: usize = 1 + INDEX_LEN;
const CHECK_LEN: usize = 16;
const BODY_LEN: usize = ID_LEN + INDEX_LEN + FILTER_LEN;
const TOKEN_LEN: usize = BODY_LEN + CHECK_LEN;

impl Shelf {
    /// Store `letter`. Returning `Ok` means it is stored.
    pub(crate) fn put(&mut self, letter: DeadLetter) -> Result<(), BusError> {
        #[cfg(test)]
        if self.failing_puts > 0 {
            self.failing_puts -= 1;
            return Err(BusError::PublishRejected {
                reason: "injected dead-letter store failure".to_owned(),
            });
        }
        let key = (letter.envelope.id, letter.group.0.clone());
        self.letters.insert(key, letter);
        Ok(())
    }

    pub(crate) fn get(&self, group: &ConsumerGroup, id: EventId) -> Option<&DeadLetter> {
        self.letters.get(&(id, group.0.clone()))
    }

    pub(crate) fn remove(&mut self, group: &ConsumerGroup, id: EventId) -> Option<DeadLetter> {
        self.letters.remove(&(id, group.0.clone()))
    }

    pub(crate) fn len(&self) -> usize {
        self.letters.len()
    }

    /// One page of letters, descending (`Envelope::id`, group), of `group`
    /// or of every group.
    pub(crate) fn list(
        &mut self,
        group: Option<&ConsumerGroup>,
        page: &PageRequest<DeadLetterList>,
    ) -> Result<Page<DeadLetter, DeadLetterList>, BusError> {
        let upper = match &page.after {
            None => Bound::Unbounded,
            Some(cursor) => Bound::Excluded(self.position(cursor.token(), group)?),
        };
        let size = page.size;
        let wanted = usize::from(size.get().get());
        let mut items: Vec<DeadLetter> = self
            .letters
            .range((Bound::Unbounded, upper))
            .rev()
            .filter(|(_, letter)| group.is_none_or(|g| letter.group == *g))
            .take(wanted + 1)
            .map(|(_, letter)| letter.clone())
            .collect();
        let more = items.len() > wanted;
        items.truncate(wanted);
        // At most `size` items by the truncation above, so neither
        // constructor can overflow; a page with more is non-empty because
        // `wanted >= 1` items precede the extra one.
        let page = match (more, items.last()) {
            (true, Some(last)) => {
                let token = self.token(last, group);
                let cursor = Cursor::from_token(token).map_err(|_| BusError::InvalidCursor)?;
                let items = NonEmpty::from_vec(items).ok_or(BusError::InvalidCursor)?;
                Page::more(size, items, cursor)
            }
            _ => Page::last(size, items),
        };
        page.map_err(|_| BusError::InvalidCursor)
    }

    fn intern(&mut self, group: &str) -> u32 {
        if let Some(index) = self.group_index.get(group) {
            return *index;
        }
        // More than u32::MAX distinct group names cannot be held in memory.
        let index = u32::try_from(self.groups.len()).unwrap_or(u32::MAX);
        self.groups.push(group.to_owned());
        self.group_index.insert(group.to_owned(), index);
        index
    }

    fn filter_text(&mut self, group: Option<&ConsumerGroup>) -> String {
        match group {
            None => format!("A{:0INDEX_LEN$x}", 0),
            Some(group) => format!("G{:0INDEX_LEN$x}", self.intern(&group.0)),
        }
    }

    fn check(&self, body: &str) -> String {
        format!("{:0CHECK_LEN$x}", self.check_key.hash_one(body))
    }

    fn token(&mut self, last: &DeadLetter, group: Option<&ConsumerGroup>) -> String {
        let index = self.intern(&last.group.0);
        let body = format!(
            "{}{:0INDEX_LEN$x}{}",
            last.envelope.id.ulid_text(),
            index,
            self.filter_text(group)
        );
        let check = self.check(&body);
        body + &check
    }

    /// The key a token positions after, if this shelf issued it for the
    /// same group filter.
    fn position(&mut self, token: &str, group: Option<&ConsumerGroup>) -> Result<Key, BusError> {
        let invalid = || BusError::InvalidCursor;
        if token.len() != TOKEN_LEN || !token.is_ascii() {
            return Err(invalid());
        }
        let (body, check) = token.split_at(BODY_LEN);
        if self.check(body) != check {
            return Err(invalid());
        }
        let (id, rest) = body.split_at(ID_LEN);
        let (index, filter) = rest.split_at(INDEX_LEN);
        let expected_filter = match group {
            None => Some(format!("A{:0INDEX_LEN$x}", 0)),
            Some(group) => self
                .group_index
                .get(&group.0)
                .map(|index| format!("G{index:0INDEX_LEN$x}")),
        };
        if expected_filter.as_deref() != Some(filter) {
            return Err(invalid());
        }
        let id = EventId::from_ulid_text(id).map_err(|_| invalid())?;
        let index = u32::from_str_radix(index, 16).map_err(|_| invalid())?;
        let last_group = usize::try_from(index)
            .ok()
            .and_then(|index| self.groups.get(index))
            .ok_or_else(invalid)?;
        Ok((id, last_group.clone()))
    }
}

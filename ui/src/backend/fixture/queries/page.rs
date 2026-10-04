//! Keyset pagination. Every list sorts its items by a `(u64, u128)` key; a
//! cursor is the key of the last item returned, so a page boundary stays put
//! when items are added elsewhere in the list.

use crosstalk_spec::support::Timestamp;

use crate::backend::Result;
use crate::contract::lists::{Cursor, Page, PageRequest};
use crosstalk_spec::interfaces::l8_surface::QueryError;

pub type Key = (u64, u128);

/// A key that sorts newer times first.
pub fn newest_first(at: Timestamp, id: u128) -> Key {
    (u64::MAX - at.as_micros(), u128::MAX - id)
}

pub fn oldest_first(at: Timestamp, id: u128) -> Key {
    (at.as_micros(), id)
}

fn encode(list: &str, key: Key) -> Cursor {
    Cursor(format!("{list}.{:016x}{:032x}", key.0, key.1))
}

fn bad_cursor(_reason: &str) -> QueryError {
    QueryError::InvalidCursor
}

fn decode(list: &str, cursor: &Cursor) -> Result<Key> {
    let rest = cursor
        .0
        .strip_prefix(list)
        .and_then(|r| r.strip_prefix('.'))
        .ok_or_else(|| bad_cursor("issued for another list"))?;
    if rest.len() != 48 || !rest.is_ascii() {
        return Err(bad_cursor("malformed"));
    }
    let (high, low) = rest.split_at(16);
    let high = u64::from_str_radix(high, 16).map_err(|_| bad_cursor("malformed"))?;
    let low = u128::from_str_radix(low, 16).map_err(|_| bad_cursor("malformed"))?;
    Ok((high, low))
}

/// One page of `items`, sorted by key. `list` names the list so a cursor
/// from another list is rejected.
pub fn paginate<T>(list: &str, mut items: Vec<(Key, T)>, page: &PageRequest) -> Result<Page<T>> {
    items.sort_by_key(|(k, _)| *k);
    let after = page.cursor.as_ref().map(|c| decode(list, c)).transpose()?;
    let start = after.map_or(0, |key| items.partition_point(|(k, _)| *k <= key));
    let limit = usize::try_from(page.limit.get()).unwrap_or(usize::MAX);
    let end = start.saturating_add(limit).min(items.len());
    let next = (end < items.len())
        .then(|| {
            items
                .get(end.saturating_sub(1))
                .map(|(k, _)| encode(list, *k))
        })
        .flatten();
    let items = items
        .into_iter()
        .skip(start)
        .take(end - start)
        .map(|(_, t)| t)
        .collect();
    Ok(Page { items, next })
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;

    use super::*;

    fn request(cursor: Option<Cursor>, limit: u32) -> PageRequest {
        PageRequest {
            cursor,
            limit: NonZeroU32::new(limit).expect("limit"),
        }
    }

    #[test]
    fn pages_cover_every_item_once() {
        let items: Vec<(Key, u32)> = (0..10).map(|i| ((u64::from(i), 0), i)).collect();
        let mut seen = Vec::new();
        let mut cursor = None;
        loop {
            let page = paginate("t", items.clone(), &request(cursor, 3)).expect("page");
            seen.extend(page.items);
            match page.next {
                Some(next) => cursor = Some(next),
                None => break,
            }
        }
        assert_eq!(seen, (0..10).collect::<Vec<_>>());
    }

    #[test]
    fn last_page_has_no_cursor() {
        let items: Vec<(Key, u32)> = (0..3).map(|i| ((u64::from(i), 0), i)).collect();
        let page = paginate("t", items, &request(None, 3)).expect("page");
        assert_eq!(page.items.len(), 3);
        assert!(page.next.is_none());
    }

    #[test]
    fn foreign_or_malformed_cursors_are_rejected() {
        let items: Vec<(Key, u32)> = vec![((1, 1), 1)];
        let foreign = encode("other", (0, 0));
        assert!(matches!(
            paginate("t", items.clone(), &request(Some(foreign), 1)),
            Err(QueryError::InvalidCursor)
        ));
        let junk = Cursor("t.zz".to_owned());
        assert!(matches!(
            paginate("t", items, &request(Some(junk), 1)),
            Err(QueryError::InvalidCursor)
        ));
    }

    #[test]
    fn newest_first_orders_by_descending_time() {
        let a = newest_first(Timestamp::from_micros(10), 1);
        let b = newest_first(Timestamp::from_micros(20), 1);
        assert!(b < a);
    }
}

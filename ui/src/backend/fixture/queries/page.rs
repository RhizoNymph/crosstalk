//! Keyset pagination with the spec's typed cursors. Every list sorts its
//! items by a `(u64, u128)` key; a cursor holds the list's name, a digest
//! of the request it was issued for and the key of the last item served, so
//! a page boundary stays put when items are added elsewhere, and a cursor
//! presented with another request is `InvalidCursor`.

use std::fmt::Debug;
use std::hash::{DefaultHasher, Hash, Hasher};

use crosstalk_spec::interfaces::l8_surface::QueryError;
use crosstalk_spec::paging::{Cursor, Page, PageRequest};
use crosstalk_spec::support::{NonEmpty, Timestamp};

use crate::backend::Result;

pub type Key = (u64, u128);

/// A key that sorts newer times first.
pub fn newest_first(at: Timestamp, id: u128) -> Key {
    (u64::MAX - at.as_micros(), u128::MAX - id)
}

pub fn oldest_first(at: Timestamp, id: u128) -> Key {
    (at.as_micros(), id)
}

/// A digest of the request a list was asked with (its filter and anything
/// else that selects its items), for binding cursors to it.
pub fn digest(request: &impl Debug) -> u64 {
    let mut hasher = DefaultHasher::new();
    format!("{request:?}").hash(&mut hasher);
    hasher.finish()
}

fn encode<L>(list: &str, request: u64, key: Key) -> Result<Cursor<L>> {
    Cursor::from_token(format!(
        "{list}-{request:016x}-{:016x}{:032x}",
        key.0, key.1
    ))
    .map_err(|e| QueryError::Store {
        reason: format!("issued an invalid cursor: {e:?}"),
    })
}

fn decode<L>(list: &str, request: u64, cursor: &Cursor<L>) -> Result<Key> {
    let rest = cursor
        .token()
        .strip_prefix(list)
        .and_then(|r| r.strip_prefix('-'))
        .ok_or(QueryError::InvalidCursor)?;
    let (bound, key) = rest.split_once('-').ok_or(QueryError::InvalidCursor)?;
    if u64::from_str_radix(bound, 16) != Ok(request) {
        return Err(QueryError::InvalidCursor);
    }
    if key.len() != 48 || !key.is_ascii() {
        return Err(QueryError::InvalidCursor);
    }
    let (high, low) = key.split_at(16);
    let high = u64::from_str_radix(high, 16).map_err(|_| QueryError::InvalidCursor)?;
    let low = u128::from_str_radix(low, 16).map_err(|_| QueryError::InvalidCursor)?;
    Ok((high, low))
}

/// One page of `items`, sorted by key. `list` names the list and `request`
/// is the [`digest`] of what selected the items, so a cursor from another
/// list or another request is refused.
pub fn paginate<T, L>(
    list: &str,
    request: u64,
    mut items: Vec<(Key, T)>,
    page: &PageRequest<L>,
) -> Result<Page<T, L>> {
    items.sort_by_key(|(k, _)| *k);
    let after = page
        .after
        .as_ref()
        .map(|c| decode(list, request, c))
        .transpose()?;
    let start = after.map_or(0, |key| items.partition_point(|(k, _)| *k <= key));
    let limit = usize::from(page.size.get().get());
    let end = start.saturating_add(limit).min(items.len());
    let more = end < items.len();
    let last_key = items.get(end.saturating_sub(1)).map(|(k, _)| *k);
    let shown: Vec<T> = items
        .into_iter()
        .skip(start)
        .take(end - start)
        .map(|(_, t)| t)
        .collect();
    let overflow = |e| QueryError::Store {
        reason: format!("page overflow: {e:?}"),
    };
    match (more, last_key, NonEmpty::from_vec(shown)) {
        (true, Some(key), Some(shown)) => {
            Page::more(page.size, shown, encode(list, request, key)?).map_err(overflow)
        }
        (_, _, shown) => Page::last(page.size, shown.map(NonEmpty::into_vec).unwrap_or_default())
            .map_err(overflow),
    }
}

#[cfg(test)]
mod tests {
    use crosstalk_spec::paging::{AlertList, PageSize};

    use super::*;

    fn request(after: Option<Cursor<AlertList>>, size: u16) -> PageRequest<AlertList> {
        PageRequest {
            size: PageSize::new(size).expect("size"),
            after,
        }
    }

    #[test]
    fn pages_cover_every_item_once() {
        let items: Vec<(Key, u32)> = (0..10).map(|i| ((u64::from(i), 0), i)).collect();
        let mut seen = Vec::new();
        let mut cursor = None;
        loop {
            let page = paginate("t", 1, items.clone(), &request(cursor, 3)).expect("page");
            let (shown, next) = page.into_parts();
            seen.extend(shown);
            match next {
                Some(next) => cursor = Some(next),
                None => break,
            }
        }
        assert_eq!(seen, (0..10).collect::<Vec<_>>());
    }

    #[test]
    fn last_page_has_no_cursor() {
        let items: Vec<(Key, u32)> = (0..3).map(|i| ((u64::from(i), 0), i)).collect();
        let page = paginate("t", 1, items, &request(None, 3)).expect("page");
        assert_eq!(page.items().len(), 3);
        assert!(page.next().is_none());
    }

    #[test]
    fn foreign_rebound_or_malformed_cursors_are_rejected() {
        let items: Vec<(Key, u32)> = vec![((1, 1), 1), ((2, 2), 2)];
        let foreign = encode("other", 1, (0, 0)).expect("cursor");
        assert_eq!(
            paginate("t", 1, items.clone(), &request(Some(foreign), 1)).err(),
            Some(QueryError::InvalidCursor)
        );
        let issued = paginate("t", 1, items.clone(), &request(None, 1))
            .expect("page")
            .next()
            .cloned();
        assert!(issued.is_some());
        assert_eq!(
            paginate("t", 2, items.clone(), &request(issued, 1)).err(),
            Some(QueryError::InvalidCursor)
        );
        let junk = Cursor::from_token("t-zz".to_owned()).expect("token");
        assert_eq!(
            paginate("t", 1, items, &request(Some(junk), 1)).err(),
            Some(QueryError::InvalidCursor)
        );
    }

    #[test]
    fn newest_first_orders_by_descending_time() {
        let a = newest_first(Timestamp::from_micros(10), 1);
        let b = newest_first(Timestamp::from_micros(20), 1);
        assert!(b < a);
    }
}

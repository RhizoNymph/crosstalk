//! Following cursors: the one traversal every list test uses.

use crosstalk_spec::interfaces::l8_surface::QueryError;
use crosstalk_spec::paging::{Page, PageRequest, PageSize};

/// The first page of `limit` items (clamped into the spec's page sizes).
pub fn first<L>(limit: u16) -> PageRequest<L> {
    let size = PageSize::new(limit.clamp(1, PageSize::MAX))
        .unwrap_or_else(|e| panic!("page size {limit}: {e:?}"));
    PageRequest { size, after: None }
}

/// Follows a list's cursors to its end, `limit` items a page, checking
/// that every page but the last is full and none is over size
/// (INV-401), and returns every item in order.
pub async fn collect<T, L>(
    limit: u16,
    mut fetch: impl AsyncFnMut(PageRequest<L>) -> Result<Page<T, L>, QueryError>,
) -> Vec<T> {
    let mut out = Vec::new();
    let mut request = first::<L>(limit);
    let size = usize::from(request.size.get().get());
    // A list longer than this is a cursor that never ends.
    for _ in 0..100_000 {
        let page = fetch(PageRequest {
            size: request.size,
            after: request.after.clone(),
        })
        .await
        .unwrap_or_else(|e| panic!("page: {e:?}"));
        assert!(page.items().len() <= size, "a page is never over size");
        if page.next().is_some() {
            assert_eq!(page.items().len(), size, "only the last page is short");
        }
        let (items, next) = page.into_parts();
        out.extend(items);
        match next {
            Some(cursor) => request.after = Some(cursor),
            None => return out,
        }
    }
    panic!("a traversal that never ended")
}

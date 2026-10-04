//! Cursor pagination: forward with the backend's cursor, back to the start.

use topcoat::Result;
use topcoat::view::{View, component, view};

use super::form::LINK;
use super::href::href;
use crate::contract::lists::Cursor;
use crate::url::view_state::ViewState;

/// Links for a cursor-paged list.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PageLinks {
    /// To the first page; `None` when already there.
    pub first: Option<String>,
    /// To the next page; `None` on the last page.
    pub next: Option<String>,
}

impl PageLinks {
    /// `extra` are the page's own query pairs, without the cursor.
    pub fn new(
        path: &str,
        state: &ViewState,
        extra: &[(&str, &str)],
        current: Option<&Cursor>,
        next: Option<&Cursor>,
    ) -> Self {
        let with_cursor = |cursor: &Cursor| {
            let mut pairs = extra.to_vec();
            pairs.push(("cursor", cursor.0.as_str()));
            href(path, state, &pairs)
        };
        Self {
            first: current.map(|_| href(path, state, extra)),
            next: next.map(with_cursor),
        }
    }
}

#[component]
pub async fn pagination(links: PageLinks) -> Result<impl View> {
    let PageLinks { first, next } = links;
    let shown = first.is_some() || next.is_some();
    Ok(view! {
        if shown {
            <nav class="mt-3 flex items-center gap-4 text-sm" aria-label="pagination">
                if let Some(first) = first {
                    <a class=(LINK) href=(first)>"« First page"</a>
                }
                if let Some(next) = next {
                    <a class=(LINK) href=(next)>"Next page »"</a>
                }
            </nav>
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::components::href::tests::state;

    #[test]
    fn first_page_has_only_next() {
        let next = Cursor("c2".into());
        let links = PageLinks::new("/agents", &state(), &[], None, Some(&next));
        assert_eq!(links.first, None);
        assert!(
            links
                .next
                .as_deref()
                .is_some_and(|l| l.ends_with("&cursor=c2"))
        );
    }

    #[test]
    fn later_pages_link_back_without_cursor() {
        let current = Cursor("c2".into());
        let links = PageLinks::new(
            "/alerts",
            &state(),
            &[("tab", "open")],
            Some(&current),
            None,
        );
        assert!(
            links
                .first
                .as_deref()
                .is_some_and(|l| l.ends_with("&tab=open"))
        );
        assert_eq!(links.next, None);
    }
}

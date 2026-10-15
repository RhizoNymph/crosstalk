use crate::paging::{
    ChannelList, Cursor, InvalidCursorToken, InvalidPageSize, Page, PageOverflow, PageSize,
};
use crate::support::NonEmpty;

fn size(n: u16) -> PageSize {
    PageSize::new(n).expect("fixture page sizes are in range")
}

fn cursor(token: &str) -> Cursor<ChannelList> {
    Cursor::from_token(token.into()).expect("fixture tokens are valid")
}

#[test]
fn page_size_rejects_zero_and_above_max() {
    assert_eq!(PageSize::new(0), Err(InvalidPageSize::Zero));
    assert_eq!(
        PageSize::new(PageSize::MAX + 1),
        Err(InvalidPageSize::AboveMax {
            max: PageSize::MAX,
            got: PageSize::MAX + 1
        })
    );
}

#[test]
fn page_size_accepts_one_through_max() {
    assert_eq!(size(1).get().get(), 1);
    assert_eq!(size(PageSize::MAX).get().get(), PageSize::MAX);
}

#[test]
fn cursor_rejects_empty_token() {
    assert_eq!(
        Cursor::<ChannelList>::from_token(String::new()),
        Err(InvalidCursorToken::Empty)
    );
}

#[test]
fn cursor_rejects_overlong_token() {
    let max = Cursor::<ChannelList>::MAX_LEN;
    assert_eq!(
        Cursor::<ChannelList>::from_token("a".repeat(max + 1)),
        Err(InvalidCursorToken::TooLong { max, got: max + 1 })
    );
    assert!(Cursor::<ChannelList>::from_token("a".repeat(max)).is_ok());
}

#[test]
fn cursor_rejects_bytes_outside_url_safe_base64() {
    for (token, index) in [("ab+c", 2), ("abc=", 3), ("a/b", 1), ("a b", 1), ("é", 0)] {
        assert_eq!(
            Cursor::<ChannelList>::from_token(token.into()),
            Err(InvalidCursorToken::BadByte { index }),
            "{token}"
        );
    }
}

#[test]
fn cursor_keeps_its_token() {
    let token = "AZaz09-_";
    assert_eq!(cursor(token).token(), token);
}

#[test]
fn last_page_may_be_empty_and_has_no_next() {
    let page = Page::<u8, ChannelList>::last(size(2), Vec::new()).expect("empty fits");
    assert!(page.items().is_empty());
    assert_eq!(page.next(), None);
}

#[test]
fn last_page_rejects_more_items_than_its_size() {
    assert_eq!(
        Page::<u8, ChannelList>::last(size(2), vec![1, 2, 3]),
        Err(PageOverflow {
            size: size(2),
            got: 3
        })
    );
}

#[test]
fn more_page_keeps_items_in_order_and_its_cursor() {
    let items = NonEmpty::from_vec(vec![3, 2]).expect("two items");
    let page = Page::more(size(2), items, cursor("next")).expect("fits");
    assert_eq!(page.items(), &[3, 2]);
    assert_eq!(page.next(), Some(&cursor("next")));
    let (items, next) = page.into_parts();
    assert_eq!((items, next), (vec![3, 2], Some(cursor("next"))));
}

#[test]
fn more_page_rejects_more_items_than_its_size() {
    let items = NonEmpty::from_vec(vec![1, 2, 3]).expect("three items");
    assert_eq!(
        Page::more(size(2), items, cursor("next")),
        Err(PageOverflow {
            size: size(2),
            got: 3
        })
    );
}

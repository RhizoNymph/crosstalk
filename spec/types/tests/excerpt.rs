//! Excerpts: the window around a matched range, character boundaries, long
//! highlights, and bodies retention dropped.

use crate::derived::provenance::span::SpanLocation;
use crate::interfaces::l8_surface::excerpt::{
    CutError, Excerpt, ExcerptError, ExcerptWindow, Excerpted, InvalidExcerpt, InvalidWindow,
};
use crate::observed::message::text::NoPartText;
use crate::observed::message::{Message, MessageBody, PartRef, Text, UserPart};
use crate::support::ByteRange;
use crate::tests::fixtures::message;

fn range(start: u32, end: u32) -> ByteRange {
    ByteRange::new(start, end).expect("non-empty range")
}

fn window(context: u16) -> ExcerptWindow {
    ExcerptWindow::new(context).expect("within the maximum")
}

fn cut(part: &str, start: u32, end: u32, context: u16) -> Excerpt {
    Excerpt::cut(part, range(start, end), window(context)).expect("range fits the part")
}

/// Every property the module docs promise of `excerpt`, cut from `part`
/// around `start..end` with `context`.
fn assert_well_cut(part: &str, start: usize, end: usize, context: usize, excerpt: &Excerpt) {
    let label = format!("{part:?} {start}..{end} ±{context}");
    let before = usize::try_from(excerpt.elided_before()).expect("small");
    let shown = excerpt.text();
    // The excerpt is the part's text at its offset, so it is valid UTF-8
    // cut on character boundaries.
    assert_eq!(&part[before..before + shown.len()], shown, "{label}");
    // The highlight is the matched range, from its start.
    assert_eq!(before + excerpt.before().len(), start, "{label}");
    let matched = excerpt.matched();
    assert!(!matched.is_empty(), "{label}");
    assert!(part[start..end].starts_with(matched), "{label}");
    let cut = usize::try_from(excerpt.highlight_cut()).expect("small");
    assert_eq!(matched.len() + cut, end - start, "{label}");
    // Context never exceeds the window.
    assert!(excerpt.before().len() <= context, "{label}");
    assert!(excerpt.after().len() <= context, "{label}");
    // Nothing is lost: the counts add up to the part.
    assert_eq!(
        excerpt.part_len(),
        u64::try_from(part.len()).expect("small"),
        "{label}"
    );
    // The checked constructor accepts what `cut` built.
    let rebuilt = Excerpt::new(
        shown.to_owned(),
        excerpt.highlight(),
        excerpt.elided_before(),
        excerpt.elided_after(),
        excerpt.highlight_cut(),
    );
    assert_eq!(rebuilt.as_ref(), Ok(excerpt), "{label}");
}

#[test]
fn window_is_bounded() {
    assert_eq!(window(0).context(), 0);
    assert_eq!(
        window(ExcerptWindow::MAX_CONTEXT).context(),
        ExcerptWindow::MAX_CONTEXT
    );
    assert_eq!(
        ExcerptWindow::new(ExcerptWindow::MAX_CONTEXT + 1),
        Err(InvalidWindow {
            max: ExcerptWindow::MAX_CONTEXT,
            got: ExcerptWindow::MAX_CONTEXT + 1,
        })
    );
    assert_eq!(ExcerptWindow::default(), ExcerptWindow::DEFAULT);
    assert!(ExcerptWindow::DEFAULT.context() <= ExcerptWindow::MAX_CONTEXT);
}

#[test]
fn cut_shows_context_either_side_and_counts_the_rest() {
    let part = "0123456789abcdefghij";
    let excerpt = cut(part, 8, 12, 3);
    assert_eq!(excerpt.before(), "567");
    assert_eq!(excerpt.matched(), "89ab");
    assert_eq!(excerpt.after(), "cde");
    assert_eq!(excerpt.text(), "56789abcde");
    assert_eq!(excerpt.elided_before(), 5);
    assert_eq!(excerpt.elided_after(), 5);
    assert_eq!(excerpt.highlight_cut(), 0);
    assert_well_cut(part, 8, 12, 3, &excerpt);
}

#[test]
fn cut_stops_at_the_part_edges() {
    let part = "hello world";
    let excerpt = cut(part, 0, 5, 100);
    assert_eq!(excerpt.before(), "");
    assert_eq!(excerpt.matched(), "hello");
    assert_eq!(excerpt.after(), " world");
    assert_eq!((excerpt.elided_before(), excerpt.elided_after()), (0, 0));
    let whole = cut(part, 0, 11, 0);
    assert_eq!(whole.text(), part);
    assert_eq!(whole.matched(), part);
}

#[test]
fn zero_context_shows_only_the_match() {
    let part = "abcdef";
    let excerpt = cut(part, 2, 4, 0);
    assert_eq!(excerpt.text(), "cd");
    assert_eq!(excerpt.matched(), "cd");
    assert_eq!((excerpt.elided_before(), excerpt.elided_after()), (2, 2));
}

#[test]
fn context_edges_move_inward_to_character_boundaries() {
    // "é" is 2 bytes, "€" 3, "🦀" 4.
    let part = "é€🦀MATCH🦀€é";
    let start = "é€🦀".len();
    let end = start + "MATCH".len();
    // One byte of context lands inside the crab on both sides: nothing of
    // it is shown.
    let excerpt = cut(part, start as u32, end as u32, 1);
    assert_eq!(excerpt.text(), "MATCH");
    assert_eq!(excerpt.elided_before(), start as u64);
    // Four bytes take the whole crab, five do not reach the euro sign.
    let wider = cut(part, start as u32, end as u32, 5);
    assert_eq!(wider.before(), "🦀");
    assert_eq!(wider.after(), "🦀");
}

#[test]
fn every_range_and_window_over_mixed_widths_is_well_cut() {
    let part = "aé€🦀b🦀€éc";
    let boundaries: Vec<usize> = (0..=part.len())
        .filter(|&i| part.is_char_boundary(i))
        .collect();
    for (i, &start) in boundaries.iter().enumerate() {
        for &end in &boundaries[i + 1..] {
            for context in 0..=9 {
                let excerpt = cut(part, start as u32, end as u32, context);
                assert_well_cut(part, start, end, usize::from(context), &excerpt);
            }
        }
    }
}

#[test]
fn a_long_match_is_cut_on_a_boundary_with_no_context_after() {
    let max = usize::try_from(Excerpt::MAX_HIGHLIGHT).expect("small");
    // Three-byte characters so the bound lands inside one.
    let body = "€".repeat(max / 3 + 10);
    let part = format!("ab{body}yz");
    let (start, end) = (2, 2 + body.len());
    let excerpt = cut(&part, start as u32, end as u32, 2);
    assert_eq!(excerpt.before(), "ab");
    assert_eq!(excerpt.after(), "");
    assert!(excerpt.matched().len() <= max);
    assert!(excerpt.matched().len() > max - 3);
    assert!(excerpt.highlight_cut() > 0);
    assert_eq!(excerpt.elided_after(), 2);
    assert_well_cut(&part, start, end, 2, &excerpt);
}

#[test]
fn a_match_exactly_at_the_bound_is_not_cut() {
    let max = usize::try_from(Excerpt::MAX_HIGHLIGHT).expect("small");
    let part = format!("<{}>", "x".repeat(max));
    let excerpt = cut(&part, 1, 1 + max as u32, 1);
    assert_eq!(excerpt.highlight_cut(), 0);
    assert_eq!(excerpt.text(), part);
}

#[test]
fn cut_rejects_a_range_that_does_not_fit_the_part() {
    assert_eq!(
        Excerpt::cut("short", range(2, 9), window(4)),
        Err(CutError::OutsideText { end: 9, len: 5 })
    );
    assert_eq!(
        Excerpt::cut("aé", range(0, 2), window(4)),
        Err(CutError::NotCharBoundary { at: 2 })
    );
    assert_eq!(
        Excerpt::cut("éa", range(1, 3), window(4)),
        Err(CutError::NotCharBoundary { at: 1 })
    );
}

#[test]
fn new_rejects_each_malformed_excerpt() {
    let new = |text: &str, start: u32, end: u32, cut: u64| {
        Excerpt::new(text.to_owned(), start..end, 0, 0, cut)
    };
    assert_eq!(
        new("abc", 2, 2, 0),
        Err(InvalidExcerpt::EmptyHighlight { start: 2, end: 2 })
    );
    assert_eq!(
        new("abc", 2, 1, 0),
        Err(InvalidExcerpt::EmptyHighlight { start: 2, end: 1 })
    );
    assert_eq!(
        new("abc", 1, 4, 0),
        Err(InvalidExcerpt::OutsideText { end: 4, len: 3 })
    );
    assert_eq!(
        new("éa", 1, 3, 0),
        Err(InvalidExcerpt::NotCharBoundary { at: 1 })
    );
    assert_eq!(
        new("aé", 0, 2, 0),
        Err(InvalidExcerpt::NotCharBoundary { at: 2 })
    );
    assert_eq!(new("abc", 1, 2, 3), Err(InvalidExcerpt::ContextAfterCut));
    let max = Excerpt::MAX_HIGHLIGHT;
    let long = "x".repeat(usize::try_from(max).expect("small") + 1);
    assert_eq!(
        new(&long, 0, max + 1, 0),
        Err(InvalidExcerpt::HighlightTooLong { len: max + 1, max })
    );
    let context = usize::from(ExcerptWindow::MAX_CONTEXT) + 1;
    let padded = format!("{}m", "x".repeat(context));
    let at = u32::try_from(context).expect("small");
    assert_eq!(
        new(&padded, at, at + 1, 0),
        Err(InvalidExcerpt::ContextTooLong {
            len: context,
            max: ExcerptWindow::MAX_CONTEXT,
        })
    );
    let trailing = format!("m{}", "x".repeat(context));
    assert_eq!(
        new(&trailing, 0, 1, 0),
        Err(InvalidExcerpt::ContextTooLong {
            len: context,
            max: ExcerptWindow::MAX_CONTEXT,
        })
    );
    assert!(
        new("abc", 1, 3, 4).is_ok(),
        "a cut highlight may end the text"
    );
}

fn body(text: &str) -> Message {
    Message {
        hash: message(1),
        body: MessageBody::User(vec![UserPart::Text(Text(text.to_owned()))]),
    }
}

fn at_part(index: u16, start: u32, end: u32) -> SpanLocation {
    SpanLocation {
        part: PartRef {
            message: message(1),
            index,
        },
        range: range(start, end),
    }
}

#[test]
fn excerpted_cuts_the_part_the_location_names() {
    let stored = body("the secret is 42, keep it");
    let shown = Excerpted::of(at_part(0, 14, 16), Some(&stored), window(4));
    let expected = cut("the secret is 42, keep it", 14, 16, 4);
    assert_eq!(shown, Ok(Excerpted::Shown(expected)));
}

#[test]
fn a_dropped_body_is_an_outcome_naming_the_message() {
    assert_eq!(
        Excerpted::of(at_part(0, 0, 4), None, window(4)),
        Ok(Excerpted::BodyDropped {
            message: message(1)
        })
    );
}

#[test]
fn a_location_that_does_not_fit_its_body_is_an_error() {
    let stored = body("tiny");
    let mut other = body("tiny");
    other.hash = message(2);
    assert_eq!(
        Excerpted::of(at_part(0, 0, 2), Some(&other), window(1)),
        Err(ExcerptError::WrongMessage {
            expected: message(1),
            got: message(2),
        })
    );
    assert_eq!(
        Excerpted::of(at_part(1, 0, 2), Some(&stored), window(1)),
        Err(ExcerptError::Part(NoPartText::NoSuchPart {
            index: 1,
            parts: 1
        }))
    );
    assert_eq!(
        Excerpted::of(at_part(0, 2, 9), Some(&stored), window(1)),
        Err(ExcerptError::Cut(CutError::OutsideText { end: 9, len: 4 }))
    );
}

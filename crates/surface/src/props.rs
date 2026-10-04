//! Properties of the spec functions the evidence page is cut with.

use crosstalk_spec::interfaces::l8_surface::excerpt::{Excerpt, ExcerptWindow};
use crosstalk_spec::support::ByteRange;
use proptest::prelude::*;
use proptest::test_runner::{Config, TestRunner};

/// Text mixing one-, two-, three- and four-byte characters.
fn part() -> impl Strategy<Value = String> {
    proptest::collection::vec(
        prop_oneof![
            Just('a'),
            Just(' '),
            Just('é'),
            Just('漢'),
            Just('🦀'),
            Just('z')
        ],
        1..200,
    )
    .prop_map(|chars| chars.into_iter().collect())
}

/// INV-700 (`surface.excerpt.window`): `Excerpt::cut` of a part's text
/// around a range on character boundaries highlights the range (or its
/// longest prefix within `MAX_HIGHLIGHT`), shows at most the window of
/// context on each side without splitting a character, and its counts add
/// up to the part.
#[test]
fn excerpt_cut_matches_part() {
    let mut runner = TestRunner::new(Config {
        cases: 256,
        failure_persistence: None,
        ..Config::default()
    });
    let input = (
        part(),
        any::<u16>(),
        any::<u16>(),
        0_u16..=ExcerptWindow::MAX_CONTEXT,
    );
    let result = runner.run(&input, |(text, a, b, context)| {
        let boundaries: Vec<usize> = (0..=text.len())
            .filter(|i| text.is_char_boundary(*i))
            .collect();
        let pick = |n: u16| boundaries[usize::from(n) % boundaries.len()];
        let (start, end) = {
            let (x, y) = (pick(a), pick(b));
            (x.min(y), x.max(y))
        };
        if start == end {
            return Ok(());
        }
        let (Ok(start32), Ok(end32)) = (u32::try_from(start), u32::try_from(end)) else {
            return Ok(());
        };
        let range = ByteRange::new(start32, end32)
            .map_err(|error| TestCaseError::fail(format!("{error:?}")))?;
        let window = ExcerptWindow::new(context)
            .map_err(|error| TestCaseError::fail(format!("{error:?}")))?;
        let excerpt = Excerpt::cut(&text, range, window)
            .map_err(|error| TestCaseError::fail(format!("{error:?}")))?;
        let len = u64::try_from(text.len()).unwrap_or(u64::MAX);
        prop_assert_eq!(excerpt.part_len(), len);
        let from = usize::try_from(excerpt.elided_before()).unwrap_or(usize::MAX);
        prop_assert_eq!(&text[from..from + excerpt.text().len()], excerpt.text());
        let matched = &text[start..end];
        prop_assert!(matched.starts_with(excerpt.matched()));
        let cut = usize::try_from(excerpt.highlight_cut()).unwrap_or(usize::MAX);
        prop_assert_eq!(excerpt.matched().len() + cut, matched.len());
        prop_assert!(excerpt.matched().len() <= Excerpt::MAX_HIGHLIGHT as usize);
        prop_assert!(excerpt.before().len() <= usize::from(context));
        prop_assert!(excerpt.after().len() <= usize::from(context));
        if cut > 0 {
            prop_assert!(excerpt.after().is_empty());
        }
        Ok(())
    });
    if let Err(error) = result {
        panic!("{error}");
    }
}

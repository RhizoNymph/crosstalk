use crate::aggregates::alert::{RULE_QUERY_MAX_CHARS, RuleQueryText};
use crate::support::{
    Blank, ByteRange, Clock, DisplayText, EmptyRange, EmptyWindow, InvalidQueryText, InvalidText,
    NonBlank, NonEmpty, OutOfRange, QueryText, Share, Similarity, SystemClock, TimeWindow,
};
use crate::tests::fixtures::at;

#[test]
fn non_empty_rejects_empty_vec() {
    assert_eq!(NonEmpty::<u8>::from_vec(Vec::new()), None);
}

#[test]
fn non_empty_keeps_order_and_counts() {
    let mut list = NonEmpty::from_vec(vec![1, 2, 3]).expect("three items");
    list.push(4);
    assert_eq!(list.iter().copied().collect::<Vec<_>>(), vec![1, 2, 3, 4]);
    assert_eq!(*list.first(), 1);
    assert_eq!(list.count().get(), 4);
}

#[test]
fn non_empty_into_vec_keeps_order() {
    let mut list = NonEmpty::new(1);
    list.push(2);
    assert_eq!(list.into_vec(), vec![1, 2]);
}

#[test]
fn non_empty_single_counts_one() {
    assert_eq!(NonEmpty::new("only").count().get(), 1);
}

#[test]
fn time_window_rejects_empty_and_inverted() {
    assert_eq!(TimeWindow::new(at(5), at(5)), Err(EmptyWindow));
    assert_eq!(TimeWindow::new(at(6), at(5)), Err(EmptyWindow));
}

#[test]
fn time_window_is_half_open() {
    let window = TimeWindow::new(at(10), at(20)).expect("10 < 20");
    assert!(!window.contains(at(9)));
    assert!(window.contains(at(10)));
    assert!(window.contains(at(19)));
    assert!(!window.contains(at(20)));
}

#[test]
fn byte_range_rejects_empty_and_inverted() {
    assert_eq!(ByteRange::new(3, 3), Err(EmptyRange));
    assert_eq!(ByteRange::new(4, 3), Err(EmptyRange));
}

#[test]
fn byte_range_length() {
    let range = ByteRange::new(3, 10).expect("3 < 10");
    assert_eq!(range.len().get(), 7);
}

#[test]
fn similarity_accepts_closed_unit_interval() {
    assert!(Similarity::new(0.0).is_ok());
    assert!(Similarity::new(1.0).is_ok());
    assert!(Similarity::new(0.42).is_ok());
}

#[test]
fn similarity_rejects_out_of_range_and_nan() {
    assert_eq!(Similarity::new(-0.01), Err(OutOfRange(-0.01)));
    assert_eq!(Similarity::new(1.01), Err(OutOfRange(1.01)));
    assert!(Similarity::new(f32::NAN).is_err());
}

#[test]
fn share_rejects_out_of_range_and_nan() {
    assert!(Share::new(0.5).is_some());
    assert!(Share::new(1.5).is_none());
    assert!(Share::new(-0.1).is_none());
    assert!(Share::new(f64::NAN).is_none());
}

#[test]
fn non_blank_trims_and_rejects_whitespace() {
    assert_eq!(
        NonBlank::new("  deploy keys \n").map(|t| t.as_str().to_owned()),
        Ok("deploy keys".to_owned())
    );
    assert_eq!(NonBlank::new(""), Err(Blank));
    assert_eq!(NonBlank::new(" \t\n"), Err(Blank));
}

type Short = DisplayText<4>;

#[test]
fn display_text_is_trimmed() {
    assert_eq!(
        Short::new("  ab ").map(|t| t.as_str().to_owned()),
        Ok("ab".to_owned())
    );
}

#[test]
fn display_text_rejects_blank_long_and_control_text() {
    assert_eq!(Short::new(" \t "), Err(InvalidText::Blank));
    assert_eq!(
        Short::new("abcde"),
        Err(InvalidText::TooLong { max: 4, got: 5 })
    );
    assert_eq!(Short::new("a\nb"), Err(InvalidText::ControlCharacter));
}

#[test]
fn display_text_counts_characters_not_bytes() {
    assert!(Short::new("éééé").is_ok());
    assert_eq!(Short::MAX_CHARS, 4);
}

type ShortQuery = QueryText<4>;

#[test]
fn query_text_is_trimmed_and_may_span_lines() {
    assert_eq!(
        ShortQuery::new("  a\nb ").map(|t| t.as_str().to_owned()),
        Ok("a\nb".to_owned())
    );
}

#[test]
fn query_text_rejects_blank_and_long_text_by_characters() {
    assert_eq!(ShortQuery::new(" \t\n "), Err(InvalidQueryText::Blank));
    assert_eq!(
        ShortQuery::new("abcde"),
        Err(InvalidQueryText::TooLong { max: 4, got: 5 })
    );
    // Characters, not bytes, and counted after trimming.
    assert!(ShortQuery::new(" éééé ").is_ok());
    assert_eq!(ShortQuery::MAX_CHARS, 4);
}

#[test]
fn a_rule_query_is_bounded() {
    assert_eq!(RuleQueryText::MAX_CHARS, RULE_QUERY_MAX_CHARS);
    assert!(RuleQueryText::new(&"q".repeat(RULE_QUERY_MAX_CHARS)).is_ok());
    assert_eq!(
        RuleQueryText::new(&"q".repeat(RULE_QUERY_MAX_CHARS + 1)),
        Err(InvalidQueryText::TooLong {
            max: RULE_QUERY_MAX_CHARS,
            got: RULE_QUERY_MAX_CHARS + 1
        })
    );
}

#[test]
fn system_clock_reads_the_wall_clock() {
    // 2020-01-01T00:00:00Z: any machine running the tests is past it.
    let after_2020 = at(1_577_836_800_000_000);
    let clock: &dyn Clock = &SystemClock;
    assert!(clock.now() > after_2020);
}

#[test]
fn clock_is_shareable_across_tasks() {
    fn assert_send_sync<T: Send + Sync + ?Sized>() {}
    assert_send_sync::<dyn Clock>();
    assert_send_sync::<SystemClock>();
}

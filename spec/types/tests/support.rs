use crate::support::{
    Blank, ByteRange, EmptyRange, EmptyWindow, NonBlank, NonEmpty, OutOfRange, Share, Similarity,
    TimeWindow,
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

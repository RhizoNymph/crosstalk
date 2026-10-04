//! Winnowing on fixed inputs, and the golden vectors.

use crosstalk_spec::derived::provenance::fingerprint::{Fingerprint, PositionedFingerprint};
use crosstalk_spec::interfaces::l4_provenance::Fingerprinter;

use crate::config::winnow_params;
use crate::fingerprint::{Winnowing, hash};
use crate::text::normalize::normalized_string;

fn winnowing(k: u16, w: u16) -> Winnowing {
    Winnowing::new(winnow_params(k, w).expect("params"))
}

const GOLDEN_TEXTS: [&str; 3] = [
    "The quick brown fox jumps over the lazy dog.",
    "  Ünïcödé   TEXT\twith\n\nwhitespace, İstanbul and ǅungla, folded  ",
    "fn main() { println!(\"hello, world\"); }",
];

/// The fingerprints the default parameters (k = 32, w = 16) and the test
/// parameters (k = 8, w = 4) give the golden texts, pinned. A change here
/// is a migration of every stored fingerprint.
#[allow(clippy::type_complexity)]
const GOLDEN: [(u16, u16, usize, &[(u64, u32)]); 6] = [
    (32, 16, 0, &[(3407910339683269368, 8)]),
    (32, 16, 1, &[(489507141974989907, 4), (1326755550134446869, 10), (703926701440222812, 28)]),
    (32, 16, 2, &[(1095369338680206169, 6)]),
    (8, 4, 0, &[(7868777261013883018, 0), (10679323865842606171, 1), (5740521152100032399, 5), (1111046075052065146, 7), (3381293388024023201, 9), (3749167401051028233, 13), (9064837829991164729, 14), (2361015581898649359, 18), (1081548498010478613, 20), (29446986465258747, 24), (3286926515177421445, 27), (7157479432408842947, 29), (6242974372119496037, 32), (10361017175589669778, 35)]),
    (8, 4, 1, &[(2830645262180342934, 4), (1642238189369916370, 8), (2124209925889360487, 11), (2807758088977541007, 16), (15669341515015491154, 19), (1199472212975819585, 21), (9840518850317945356, 24), (3615489325024213487, 28), (1674432935874490897, 30), (2020328108735360757, 33), (8961877057594821954, 35), (290445062992463435, 39), (7480265110118067883, 41), (6081895051161918456, 45), (4074711926790541931, 46), (937464711888305606, 48), (1155812079765363890, 52), (2119637389527871350, 53), (5822063929570858833, 56), (3872485206966377401, 59), (648034781357807628, 61)]),
    (8, 4, 2, &[(870183630052704909, 0), (5656079792989654166, 4), (14439619972654872071, 7), (2603274278533176227, 9), (1875106490208901131, 13), (6688224602219073818, 16), (6898898164812057623, 19), (4516780367968227204, 21), (4228992218438672236, 25), (2001320906470340958, 29), (235527206810806141, 30)]),
];

pub fn golden_vectors() {
    let mut printed = String::new();
    for (k, w) in [(32u16, 16u16), (8, 4)] {
        for (index, text) in GOLDEN_TEXTS.iter().enumerate() {
            let got: Vec<(u64, u32)> = winnowing(k, w)
                .fingerprints(text)
                .into_iter()
                .map(|p| (p.fingerprint.0, p.offset))
                .collect();
            printed.push_str(&format!("({k}, {w}, {index}, &{got:?}),\n"));
            let expected = GOLDEN
                .iter()
                .find(|(gk, gw, gi, _)| *gk == k && *gw == w && *gi == index)
                .map(|(_, _, _, expected)| *expected);
            assert_eq!(Some(got.as_slice()), expected, "k={k} w={w} text {index}; now:\n{printed}");
        }
    }
}

#[test]
fn kgram_hash_is_the_rolling_window() {
    let chars: Vec<char> = "abcdefghij".chars().collect();
    let rolling = hash::rolling(&chars, 4);
    assert_eq!(rolling.len(), 7);
    for (index, value) in rolling.iter().enumerate() {
        assert_eq!(*value, hash::kgram(&chars[index..index + 4]));
    }
}

#[test]
fn short_text_has_no_fingerprint_and_a_small_one_its_minimum() {
    let w = winnowing(8, 4);
    assert!(w.fingerprints("short").is_empty());
    assert_eq!(w.fingerprints("exactly8").len(), 1);
    let two = w.kgrams("exactly 9");
    assert_eq!(two.len(), 2);
    let minimum = two.iter().map(|k| k.fingerprint).min();
    assert_eq!(w.fingerprints("exactly 9").first().map(|p| p.fingerprint), minimum);
}

#[test]
fn offsets_are_source_bytes() {
    let w = winnowing(4, 1);
    let text = "Ab  cd\u{e9}fg";
    let all: Vec<PositionedFingerprint> = w.fingerprints(text);
    assert_eq!(all.len(), normalized_string(text).chars().count() - 3);
    for positioned in &all {
        assert!(text.is_char_boundary(positioned.offset as usize));
    }
    // "ab c" starts at 0, " cd\u{e9}" at 2 (the folded run's start).
    assert_eq!(all[0].offset, 0);
    assert_eq!(all[2].offset, 2);
}

#[test]
fn equal_normalized_windows_hash_equal() {
    let w = winnowing(5, 1);
    let a: Vec<Fingerprint> = w.fingerprints("HELLO   World").into_iter().map(|p| p.fingerprint).collect();
    let b: Vec<Fingerprint> = w.fingerprints("hello world").into_iter().map(|p| p.fingerprint).collect();
    assert_eq!(a, b);
}

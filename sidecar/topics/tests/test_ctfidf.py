import math

import pytest

from crosstalk_topics.ctfidf import class_terms


def test_hand_computed_weights() -> None:
    # counts: apple [2, 0], banana [1, 1], cherry [0, 1]; A = 5 / 2.
    # idf: apple ln(1 + 2.5/2), banana ln(1 + 2.5/2), cherry ln(1 + 2.5/1).
    terms = class_terms(["apple apple banana", "banana cherry"], top_terms=10)
    common = math.log(1 + 2.5 / 2)
    rare = math.log(1 + 2.5 / 1)
    assert [term for term, _ in terms[0]] == ["apple", "banana"]
    assert terms[0][0][1] == pytest.approx(2 / 3 * common, rel=1e-12)
    assert terms[0][1][1] == pytest.approx(1 / 3 * common, rel=1e-12)
    assert [term for term, _ in terms[1]] == ["cherry", "banana"]
    assert terms[1][0][1] == pytest.approx(0.5 * rare, rel=1e-12)
    assert terms[1][1][1] == pytest.approx(0.5 * common, rel=1e-12)


def test_top_terms_caps_and_ties_go_to_the_smaller_term() -> None:
    terms = class_terms(["zebra yak xenon", "other"], top_terms=2)
    assert terms[0] == sorted(terms[0], key=lambda entry: (-entry[1], entry[0]))
    assert [term for term, _ in terms[0]] == ["xenon", "yak"]


def test_stop_words_and_case_are_ignored() -> None:
    terms = class_terms(["The Wiki and the WIKI", "deploy"], top_terms=5)
    assert [term for term, _ in terms[0]] == ["wiki"]


def test_no_vocabulary_gives_no_terms() -> None:
    assert class_terms(["the and of", "a an"], top_terms=5) == [[], []]


def test_a_class_of_stop_words_only_gets_no_terms() -> None:
    terms = class_terms(["wiki page", "the and"], top_terms=5)
    assert terms[1] == []
    assert len(terms[0]) == 2


def test_no_classes() -> None:
    assert class_terms([], top_terms=5) == []

from __future__ import annotations

import pytest

from citefuse.similarity import (
    author_similarity, doi_similarity, jaro, jaro_winkler, ordered_overlap,
    page_overlap, parse_page_span, token_jaccard, tokenize, year_similarity,
)


def test_jaro_known_values_and_boundaries():
    assert jaro("MARTHA", "MARHTA") == pytest.approx(0.944444, abs=1e-5)
    assert jaro("abc", "abc") == 1.0
    assert jaro("", "") == 1.0
    assert jaro("abc", "") == 0.0
    assert jaro("abc", "xyz") == 0.0


def test_jaro_winkler_prefix_boost():
    base = jaro("MARTHA", "MARHTA")
    assert jaro_winkler("MARTHA", "MARHTA") == pytest.approx(0.961111, abs=1e-5)
    assert jaro_winkler("MARTHA", "MARHTA") > base
    assert jaro_winkler("abc", "xyz") == 0.0


def test_token_jaccard_and_tokenize():
    assert tokenize("The Fictional: STUDY") == ["the", "fictional", "study"]
    assert token_jaccard("alpha beta", "beta gamma") == pytest.approx(1 / 3)
    assert token_jaccard("", "") == 1.0
    assert token_jaccard("alpha", "") == 0.0


def test_ordered_overlap_respects_order_partially():
    a = ["a", "b", "c"]
    assert ordered_overlap(a, a) == 1.0
    assert ordered_overlap(a, ["c", "a", "b"]) == pytest.approx(2 / 3)
    assert ordered_overlap(a, ["c", "b", "a"]) == pytest.approx(1 / 3)
    assert ordered_overlap([], []) == 1.0
    assert ordered_overlap(a, []) == 0.0


def test_author_similarity_uses_family_and_initial():
    left = [{"key": "zorvath|v"}, {"key": "quellmar|t"}]
    right = [{"key": "quellmar|t"}, {"key": "zorvath|v"}]
    assert author_similarity(left, right) == 0.5


def test_page_overlap():
    assert parse_page_span("129-135") == (129, 135)
    assert parse_page_span("129") == (129, 129)
    assert parse_page_span(None) is None
    assert page_overlap("10-20", "10-20") == 1.0
    assert page_overlap("10-20", "15-25") == pytest.approx(6 / 16)
    assert page_overlap("10-20", "30-40") == 0.0
    assert page_overlap("bad", "10-20") is None


def test_year_and_doi_similarity():
    bands = [1.0, 0.75, 0.4]
    assert year_similarity(2020, 2020, bands) == 1.0
    assert year_similarity(2020, 2021, bands) == 0.75
    assert year_similarity(2020, 2023, bands) == 0.0
    assert year_similarity(None, 2020, bands) is None
    assert doi_similarity("10.1/a", "10.1/a") == 1.0
    assert doi_similarity("10.1/a", "10.1/b") == 0.0
    assert doi_similarity(None, "10.1/a") is None

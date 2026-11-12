"""Unit tests for the three headline semantic drift detectors."""
from __future__ import annotations

import pytest

from citefuse.config import load_config
from citefuse.drift.context import DetectionContext
from citefuse.drift.detectors import (
    AuthorDetector, SourceDetector, YearDetector, parse_chinese_number,
)
from citefuse.drift.models import resolve_tier


@pytest.fixture
def drift_cfg():
    return load_config().drift


def _ctx(baseline=None, min_samples=20):
    return DetectionContext(baseline, min_samples)


def _tier(verdict, drift_cfg):
    """Apply the same central tier resolution the guard uses."""
    if verdict is None:
        return None
    return resolve_tier(verdict.score, drift_cfg.severity, verdict.min_tier)


def _score(verdict):
    return None if verdict is None else verdict.score


# --- Chinese numeral parsing -------------------------------------------------
@pytest.mark.parametrize(
    "text,expected",
    [("八十三", 83), ("一百零五", 105), ("三", 3), ("二十", 20), ("2024", 2024)],
)
def test_parse_chinese_number(text, expected):
    assert parse_chinese_number(text) == expected


# --- Year detector ------------------------------------------------------------
def test_modern_integer_year_is_clean(drift_cfg):
    detector = YearDetector(drift_cfg.year)
    verdict = detector.check({"year": 2001}, {}, _ctx())
    assert verdict is None


def test_circa_four_digit_year_is_minor_and_correctable(drift_cfg):
    detector = YearDetector(drift_cfg.year)
    verdict = detector.check({"year": "c. 1998"}, {}, _ctx())
    assert verdict is not None
    assert _tier(verdict, drift_cfg) == "minor"
    assert verdict.reason_code == "era_modern"
    assert verdict.suggestion == 1998


def test_two_digit_year_uses_pivot(drift_cfg):
    detector = YearDetector(drift_cfg.year)
    verdict = detector.check({"year": "98"}, {}, _ctx())
    assert _tier(verdict, drift_cfg) == "minor"
    assert verdict.suggestion == 1998
    verdict2 = detector.check({"year": "05"}, {}, _ctx())
    assert verdict2.suggestion == 2005


def test_republican_era_string_is_drift_and_converted(drift_cfg):
    detector = YearDetector(drift_cfg.year)
    verdict = detector.check({"year": "民国八十三年"}, {}, _ctx())
    assert verdict is not None
    assert verdict.reason_code == "era_imperial"
    assert verdict.normalized == 1994  # 1911 + 83
    assert _tier(verdict, drift_cfg) in ("medium", "heavy")


def test_japanese_era_string_is_drift(drift_cfg):
    detector = YearDetector(drift_cfg.year)
    verdict = detector.check({"year": "令和5年"}, {}, _ctx())
    assert verdict is not None
    assert verdict.normalized == 2023  # 2018 + 5
    assert _tier(verdict, drift_cfg) in ("medium", "heavy")


def test_unconverted_era_is_heavy(drift_cfg):
    detector = YearDetector(drift_cfg.year)
    verdict = detector.check({"year": "回历1420年"}, {}, _ctx())
    assert verdict is not None
    assert _tier(verdict, drift_cfg) == "heavy"
    assert verdict.reason_code == "era_unconverted"


def test_unrecognized_year_text_is_heavy(drift_cfg):
    detector = YearDetector(drift_cfg.year)
    verdict = detector.check({"year": "上古年间"}, {}, _ctx())
    assert verdict is not None
    assert _tier(verdict, drift_cfg) == "heavy"
    assert verdict.reason_code == "era_unrecognized"


def test_year_out_of_window_is_heavy(drift_cfg):
    detector = YearDetector(drift_cfg.year)
    verdict = detector.check({"year": 1215}, {}, _ctx())
    assert verdict is not None
    assert _tier(verdict, drift_cfg) == "heavy"
    assert verdict.reason_code == "year_out_of_window"


def test_missing_year_emits_no_verdict(drift_cfg):
    detector = YearDetector(drift_cfg.year)
    assert detector.check({}, {}, _ctx()) is None


# --- Source detector ----------------------------------------------------------
def test_known_source_code_is_clean(drift_cfg):
    detector = SourceDetector(drift_cfg.source)
    assert detector.check({"source": "ORBIT"}, {}, _ctx()) is None


def test_organization_full_name_is_minor_alias(drift_cfg):
    detector = SourceDetector(drift_cfg.source)
    verdict = detector.check({"source": "Orbital Bibliographic Exchange"}, {}, _ctx())
    assert verdict is not None
    assert _tier(verdict, drift_cfg) == "minor"
    assert verdict.reason_code == "source_alias"
    assert verdict.suggestion == "ORBIT"


def test_unknown_organization_name_is_medium(drift_cfg):
    detector = SourceDetector(drift_cfg.source)
    verdict = detector.check({"source": "Some Unknown University Library"}, {}, _ctx())
    assert verdict is not None
    assert _tier(verdict, drift_cfg) == "medium"
    assert verdict.reason_code == "source_organization_name"


def test_unknown_code_shaped_token_is_medium(drift_cfg):
    detector = SourceDetector(drift_cfg.source)
    verdict = detector.check({"source": "ZENODO"}, {}, _ctx())
    assert verdict is not None
    assert _tier(verdict, drift_cfg) == "medium"
    assert verdict.reason_code == "source_unknown_code"


def test_garbage_source_is_heavy(drift_cfg):
    detector = SourceDetector(drift_cfg.source)
    verdict = detector.check({"source": "### !!! ???"}, {}, _ctx())
    assert verdict is not None
    assert _tier(verdict, drift_cfg) == "heavy"
    assert verdict.reason_code == "source_unrecognized"


# --- Author detector ----------------------------------------------------------
def test_personal_authors_are_clean(drift_cfg):
    detector = AuthorDetector(drift_cfg.authors)
    verdict = detector.check(
        {"authors": ["Zorvath, Vaelis", "Quellmar, Tirune"]}, {}, _ctx()
    )
    assert verdict is None


def test_collective_only_is_medium(drift_cfg):
    detector = AuthorDetector(drift_cfg.authors)
    verdict = detector.check({"authors": ["Global Cardiology Consortium"]}, {}, _ctx())
    assert verdict is not None
    assert _tier(verdict, drift_cfg) == "medium"
    assert verdict.reason_code == "author_collective_only"
    assert verdict.suggestion is None


def test_collective_mixed_is_minor_and_strips_collective(drift_cfg):
    detector = AuthorDetector(drift_cfg.authors)
    verdict = detector.check(
        {"authors": ["Zorvath, Vaelis", "National Task Force on Diagnostics"]},
        {}, _ctx(),
    )
    assert verdict is not None
    assert _tier(verdict, drift_cfg) == "minor"
    assert verdict.reason_code == "author_collective_mixed"
    assert verdict.suggestion == ["Zorvath, Vaelis"]


def test_chinese_collective_marker_detected(drift_cfg):
    detector = AuthorDetector(drift_cfg.authors)
    verdict = detector.check({"authors": ["全国心血管病协作组"]}, {}, _ctx())
    assert verdict is not None
    assert _tier(verdict, drift_cfg) == "medium"
    assert verdict.reason_code == "author_collective_only"


def test_junk_authors_are_heavy(drift_cfg):
    detector = AuthorDetector(drift_cfg.authors)
    verdict = detector.check({"authors": ["@@@###", "!!!$$$"]}, {}, _ctx())
    assert verdict is not None
    assert _tier(verdict, drift_cfg) == "heavy"
    assert verdict.reason_code == "author_unrecognized"

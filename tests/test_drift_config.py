"""Tests proving every drift rule and threshold is configuration-driven."""
from __future__ import annotations

import pytest

from citefuse.config import load_config
from citefuse.drift.config import DriftConfig, SeverityConfig
from citefuse.drift.context import DetectionContext
from citefuse.drift.detectors import SourceDetector, YearDetector
from citefuse.drift.models import resolve_tier


def test_severity_thresholds_are_validated():
    with pytest.raises(Exception):
        SeverityConfig(minor=0.8, medium=0.4, heavy=0.9)  # minor > medium invalid


def test_changing_severity_threshold_reclassifies_a_score():
    # score 0.40: minor under default cut (0.25), clean under a raised cut.
    # min_tier="clean" simulates a pure score-driven verdict with no rule floor.
    default = SeverityConfig()
    assert resolve_tier(0.40, default, min_tier="clean") == "minor"
    relaxed = SeverityConfig(minor=0.5, medium=0.7, heavy=0.9)
    assert resolve_tier(0.40, relaxed, min_tier="clean") == "clean"
    # A rule-mandated floor cannot be relaxed away.
    assert resolve_tier(0.40, relaxed, min_tier="medium") == "medium"


def test_disabling_a_field_removes_its_detector():
    cfg = load_config().drift
    cfg.year.enabled = False
    from citefuse.drift.detectors import build_detectors

    names = [type(d).__name__ for d in build_detectors(cfg)]
    assert "YearDetector" not in names


def test_source_alias_table_is_configurable():
    cfg = load_config().drift
    # A custom alias only recognized after adding it to the config.
    detector = SourceDetector(cfg.source)
    verdict = detector.check(
        {"source": "Fictional University Metadata Office"}, {}, DetectionContext()
    )
    assert verdict is not None and verdict.reason_code == "source_organization_name"

    cfg.source.aliases["fictional university metadata office"] = "FUMO"
    detector2 = SourceDetector(cfg.source)
    verdict2 = detector2.check(
        {"source": "Fictional University Metadata Office"}, {}, DetectionContext()
    )
    assert verdict2.reason_code == "source_alias"
    assert verdict2.suggestion == "FUMO"


def test_year_window_is_configurable():
    cfg = load_config().drift
    detector = YearDetector(cfg.year)
    assert detector.check({"year": 1600}, {}, DetectionContext()) is None  # inside 1500-2100
    cfg.year.min_year = 1800
    detector2 = YearDetector(cfg.year)
    verdict = detector2.check({"year": 1600}, {}, DetectionContext())
    assert verdict is not None and verdict.reason_code == "year_out_of_window"


def test_collective_markers_are_configurable():
    cfg = load_config().drift
    from citefuse.drift.detectors import AuthorDetector

    detector = AuthorDetector(cfg.authors)
    # A phrase not in the marker list is treated as ordinary (personal) text.
    assert detector.check({"authors": ["The Quiet Circle of Readers"]}, {},
                          DetectionContext()) is None
    cfg.authors.collective_markers = ["circle"]
    detector2 = AuthorDetector(cfg.authors)
    verdict = detector2.check({"authors": ["The Quiet Circle of Readers"]}, {},
                              DetectionContext())
    assert verdict is not None and verdict.reason_code == "author_collective_only"


def test_generic_shape_field_ignores_baseline_distribution():
    cfg = load_config().drift
    from citefuse.drift.detectors import GenericDetector

    detector = GenericDetector("doi", cfg.generic["doi"])
    # A well-formed but never-seen DOI is clean even with a baseline present,
    # because kind=shape only enforces the pattern.
    baseline = _build_text_baseline()
    ctx = DetectionContext(baseline, min_samples=1)
    verdict = detector.check(
        {"doi": "10.9999/brand.new.identifier.12345"},
        {"doi": "10.9999/brand.new.identifier.12345"}, ctx,
    )
    assert verdict is None
    # A malformed DOI still trips the pattern rule.
    bad = detector.check({"doi": "not-a-doi"}, {"doi": "not-a-doi"}, ctx)
    assert bad is not None and bad.reason_code == "pattern_violation"


def _build_text_baseline():
    from citefuse.drift.baseline import Baseline, CountStats

    profile = CountStats(kind="text")
    for gram in ("10.", "0.5", ".55", "555", "55/"):
        profile.counts[gram] = 5
    profile.count = 25
    return Baseline(1, {"doi": profile})


def test_drift_config_block_participates_in_fingerprint():
    base = load_config()
    modified = load_config()
    modified.drift.severity.medium = 0.60
    assert base.fingerprint() != modified.fingerprint()

from __future__ import annotations

from citefuse.models import Record
from citefuse.scoring import score_pair


def _record(pk, title, **overrides):
    norm = {
        "title": title,
        "authors": [{"family": "zorvath", "given": "vaelis", "key": "zorvath|v"}],
        "venue": "journal of fictional evidence synthesis",
        "year": 2020,
        "volume": "3",
        "issue": "1",
        "pages": "10-20",
        "doi": "10.5555/a",
    }
    norm.update(overrides)
    return Record(pk, "b", "ORBIT", f"R{pk}", pk, {}, norm, None)


def test_identical_records_score_one(config):
    result = score_pair(_record(1, "alpha beta"), _record(2, "alpha beta"), config)
    assert result["score"] == 1.0
    assert result["zone"] == "match"
    assert set(result["details"]) == {"title", "authors", "venue", "year", "pages", "doi"}
    assert all(item["status"] == "scored" for item in result["details"].values())
    assert result["doi_match"] is True


def test_doi_mismatch_is_penalized(config):
    left = _record(1, "alpha beta")
    right = _record(2, "alpha beta", doi="10.5555/b")
    result = score_pair(left, right, config)
    assert result["details"]["doi"]["score"] == 0.0
    assert result["doi_match"] is False
    assert result["score"] < 1.0


def test_missing_neutral_keeps_weight(config):
    left = _record(1, "alpha beta", venue=None)
    result = score_pair(left, _record(2, "alpha beta"), config)
    venue = result["details"]["venue"]
    assert venue["status"] == "missing_neutral"
    assert venue["score"] == config.scoring.neutral_score
    assert 0.9 < result["score"] < 1.0


def test_missing_exclude_renormalizes(config):
    cfg = config.model_copy(deep=True)
    cfg.scoring.missing_policy = "exclude"
    left = _record(1, "alpha beta", venue=None)
    result = score_pair(left, _record(2, "alpha beta"), cfg)
    assert result["details"]["venue"]["status"] == "missing_excluded"
    assert result["details"]["venue"]["contribution"] == 0.0
    assert result["score"] == 1.0


def test_unrelated_pair_lands_below_review(config):
    left = _record(1, "alpha beta gamma", authors=[], venue=None, year=1990,
                   pages="1-2", doi="10.5555/a")
    right = _record(2, "omega delta sigma", authors=[], venue=None, year=2005,
                    pages="90-99", doi="10.5555/z")
    result = score_pair(left, right, config)
    assert result["zone"] == "reject"
    assert result["score"] < config.scoring.review_low

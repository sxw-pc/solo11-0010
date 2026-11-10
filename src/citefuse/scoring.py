"""Weighted field scoring and threshold classification."""
from __future__ import annotations

from typing import Any

from .config import AppConfig
from .models import Record
from .similarity import (
    author_similarity,
    doi_similarity,
    jaro_winkler,
    page_overlap,
    token_jaccard,
    year_similarity,
)


def _field_scores(left: Record, right: Record, config: AppConfig) -> dict[str, float | None]:
    authors = (
        author_similarity(left.authors, right.authors)
        if left.authors and right.authors
        else None
    )
    venue = (
        token_jaccard(left.venue, right.venue)
        if left.venue and right.venue
        else None
    )
    return {
        "title": jaro_winkler(left.title, right.title),
        "authors": authors,
        "venue": venue,
        "year": year_similarity(
            left.year, right.year, config.scoring.year_diff_scores
        ),
        "pages": page_overlap(left.pages, right.pages),
        "doi": doi_similarity(left.doi, right.doi),
    }


def score_pair(left: Record, right: Record, config: AppConfig) -> dict[str, Any]:
    """Score one candidate pair and return total + auditable per-field details.

    Complexities are dominated by title JW (O(n*m)) and author LCS
    (O(a*b)); all other field functions are linear.
    """
    scores = _field_scores(left, right, config)
    weights = config.scoring.weights
    details: dict[str, dict[str, Any]] = {}
    present_weight = 0.0
    for field, weight in weights.items():
        if scores[field] is not None:
            present_weight += weight
    denominator = (
        present_weight
        if config.scoring.missing_policy == "exclude"
        else sum(weights.values())
    )
    if denominator <= 0:
        denominator = 1.0

    total = 0.0
    for field, weight in weights.items():
        raw = scores[field]
        if raw is None and config.scoring.missing_policy == "exclude":
            details[field] = {
                "raw_score": None,
                "score": None,
                "weight": weight,
                "contribution": 0.0,
                "status": "missing_excluded",
            }
            continue
        effective = config.scoring.neutral_score if raw is None else raw
        contribution = weight * effective / denominator
        total += contribution
        details[field] = {
            "raw_score": None if raw is None else round(raw, 6),
            "score": round(effective, 6),
            "weight": weight,
            "contribution": round(contribution, 6),
            "status": "missing_neutral" if raw is None else "scored",
        }

    total = round(min(1.0, max(0.0, total)), 6)
    if total >= config.scoring.match_threshold:
        zone = "match"
    elif total >= config.scoring.review_low:
        zone = "review"
    else:
        zone = "reject"
    return {
        "score": total,
        "zone": zone,
        "details": details,
        "doi_match": bool(left.doi and right.doi and left.doi == right.doi),
        "missing_policy": config.scoring.missing_policy,
    }

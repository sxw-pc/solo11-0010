from __future__ import annotations

import random

from citefuse.blocking import generate_candidates
from citefuse.models import Record, make_pair_key


def _record(pk, source, record_id, title, year, venue, family):
    norm = {
        "title": title,
        "authors": [{"family": family, "given": "a", "key": f"{family}|a"}],
        "venue": venue,
        "year": year,
        "pages": None,
        "doi": None,
    }
    return Record(pk, "b", source, record_id, pk, {}, norm, None)


def _records():
    title = "alpha beta gamma delta epsilon"
    return [
        _record(1, "ORBIT", "A1", title, 2020, "journal of fictional evidence", "alpha"),
        _record(2, "LANTERN", "A2", title, 2020, "journal of fictional evidence", "alpha"),
        _record(3, "MEADOW", "A3", title, 2020, "journal of fictional evidence", "gamma"),
        _record(4, "COMPASS", "B1", "omega unique topic", 2021, "other venue", "omega"),
    ]


def _only(config, enabled_name):
    cfg = config.model_copy(deep=True)
    for name in ("year_venue", "title_signature", "author_year"):
        getattr(cfg.blocking, name).enabled = name == enabled_name
    return cfg


def test_each_strategy_and_union(config):
    records = _records()
    year_venue, stats = generate_candidates(records, _only(config, "year_venue"))
    assert len(year_venue) == 3
    assert stats["strategy_counts"]["year_venue"] == 3

    title, _ = generate_candidates(records, _only(config, "title_signature"))
    assert len(title) == 3

    author, _ = generate_candidates(records, _only(config, "author_year"))
    assert [pair.pair_key for pair in author] == ["ORBIT::A1||LANTERN::A2"]

    combined, combined_stats = generate_candidates(records, config)
    assert len(combined) == 3
    assert combined_stats["union_count"] == 3
    assert combined_stats["strategy_counts"]["author_year"] == 1


def test_all_strategies_can_be_disabled(config):
    cfg = _only(config, "none")
    pairs, stats = generate_candidates(_records(), cfg)
    assert pairs == []
    assert stats["union_count"] == 0


def test_pairs_are_key_ordered_and_input_independent(config):
    records = _records()
    baseline, _ = generate_candidates(records, config)
    shuffled = list(records)
    random.Random(7).shuffle(shuffled)
    replay, _ = generate_candidates(shuffled, config)
    assert [p.pair_key for p in baseline] == [p.pair_key for p in replay]
    for pair in baseline:
        first, second, expected = make_pair_key(pair.left.key, pair.right.key)
        assert pair.pair_key == expected
        assert pair.left.key == first and pair.right.key == second
        assert pair.strategies

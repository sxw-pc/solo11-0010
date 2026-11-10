from __future__ import annotations

import random

from citefuse.clustering import cluster_records, resolve_decisions
from citefuse.models import Record, make_pair_key


def _records():
    return [
        Record(1, "b", "ORBIT", "A", 1, {}, {}, None),
        Record(2, "b", "LANTERN", "B", 2, {}, {}, None),
        Record(3, "b", "MEADOW", "C", 3, {}, {}, None),
    ]


def _edge(left, right, score):
    first, second, pair_key = make_pair_key(left.key, right.key)
    return {
        "pair_key": pair_key, "rec_a_pk": left.pk, "rec_b_pk": right.pk,
        "score": score, "details": {},
    }


def _decision(left, right, decision, submitted_at, decision_id):
    return {
        "decision_id": decision_id, "pair_key": make_pair_key(left.key, right.key)[2],
        "rec_a_key": make_pair_key(left.key, right.key)[0],
        "rec_b_key": make_pair_key(left.key, right.key)[1],
        "decision": decision, "submitted_at": submitted_at,
    }


def test_union_find_groups_and_is_order_independent():
    records = _records()
    edges = [_edge(records[0], records[1], 0.9), _edge(records[1], records[2], 0.8)]
    baseline, applied, _ = cluster_records(records, edges, [])
    assert [record.key for record in baseline[0]] == [
        "LANTERN::B", "MEADOW::C", "ORBIT::A"
    ]
    assert len(applied) == 2
    shuffled = list(records)
    random.Random(11).shuffle(shuffled)
    replay, _, _ = cluster_records(shuffled, list(reversed(edges)), [])
    assert [
        [record.key for record in group] for group in replay
    ] == [[record.key for record in group] for group in baseline]


def test_human_same_and_different_override_model():
    records = _records()
    edge = _edge(records[0], records[1], 0.95)
    different = [_decision(records[0], records[1], "different", "2025-01-02", 2)]
    groups, applied, _ = cluster_records(records, [edge], different)
    assert all(len(group) == 1 for group in groups)
    assert applied == []

    same = [_decision(records[0], records[1], "same", "2025-01-02", 1)]
    groups, applied, _ = cluster_records(records, [], same)
    assert any(len(group) == 2 for group in groups)
    assert applied[0]["kind"] == "human_same"


def test_newest_decision_wins_and_conflict_is_reported():
    records = _records()
    history = [
        _decision(records[0], records[1], "same", "2025-01-01", 1),
        _decision(records[0], records[1], "different", "2025-01-02", 2),
    ]
    latest, conflicts = resolve_decisions(history)
    assert latest[make_pair_key(records[0].key, records[1].key)[2]]["decision"] == "different"
    assert conflicts and conflicts[0]["type"] == "superseded_decision"
    groups, _, _ = cluster_records(records, [], history)
    assert all(len(group) == 1 for group in groups)


def test_different_blocks_transitive_path():
    records = _records()
    a, b, c = records
    edges = [_edge(a, b, 0.99), _edge(a, c, 0.95), _edge(b, c, 0.90)]
    decisions = [_decision(a, b, "different", "2025-01-02", 1)]
    groups, _, _ = cluster_records(records, edges, decisions)
    membership = {
        record.key: index for index, group in enumerate(groups) for record in group
    }
    assert membership[a.key] != membership[b.key]
    assert len(groups) == 2


def test_human_same_newer_supersedes_older_different():
    records = _records()
    a, b = records[0], records[1]
    history = [
        _decision(a, b, "different", "2025-01-01", 1),
        _decision(a, b, "same", "2025-01-02", 2),
    ]
    groups, applied, conflicts = cluster_records(records, [], history)
    assert any(len(group) == 2 for group in groups)
    assert applied[0]["kind"] == "human_same"
    assert any(item["type"] == "superseded_decision" for item in conflicts)

"""Deterministic union-find clustering with human must-link/cannot-link rules.

Latest human decision per pair wins. A newer human 'same' decision can
supersede an older 'different' decision (and vice versa); such supersessions
are returned as audit events. Model edges can never override a human
cannot-link, even transitively.
"""
from __future__ import annotations

from typing import Any

from .models import Record


class UnionFind:
    """Union-find with path compression and union by deterministic rank."""

    def __init__(self, items: list[int]) -> None:
        self.parent = {item: item for item in items}
        self.rank = {item: 0 for item in items}

    def find(self, item: int) -> int:
        root = item
        while self.parent[root] != root:
            root = self.parent[root]
        while self.parent[item] != item:
            parent = self.parent[item]
            self.parent[item] = root
            item = parent
        return root

    def union(self, left: int, right: int) -> bool:
        root_left, root_right = self.find(left), self.find(right)
        if root_left == root_right:
            return False
        if self.rank[root_left] < self.rank[root_right]:
            root_left, root_right = root_right, root_left
        self.parent[root_right] = root_left
        if self.rank[root_left] == self.rank[root_right]:
            self.rank[root_left] += 1
        return True


def resolve_decisions(history: list[dict[str, Any]]) -> tuple[dict[str, dict], list[dict]]:
    """Return latest decision per pair plus conflict history, newest wins."""
    ordered = sorted(
        history, key=lambda item: (item["submitted_at"], item["decision_id"])
    )
    latest: dict[str, dict] = {}
    conflicts: list[dict] = []
    for decision in ordered:
        previous = latest.get(decision["pair_key"])
        if previous is not None and previous["decision"] != decision["decision"]:
            conflicts.append({
                "type": "superseded_decision",
                "pair_key": decision["pair_key"],
                "older": previous["decision"],
                "newer": decision["decision"],
                "older_at": previous["submitted_at"],
                "newer_at": decision["submitted_at"],
            })
        latest[decision["pair_key"]] = decision
    return latest, conflicts


def _decision_order(decision: dict[str, Any]) -> tuple[str, int]:
    return str(decision["submitted_at"]), int(decision["decision_id"])


def cluster_records(
    records: list[Record],
    model_edges: list[dict[str, Any]],
    decision_history: list[dict[str, Any]],
) -> tuple[list[list[Record]], list[dict[str, Any]], list[dict[str, Any]]]:
    """Cluster records and return (groups, applied_edges, conflicts).

    Model edges are processed by descending score then pair key. Human same
    edges are processed chronologically. Complexity is near-linear in edges,
    plus cannot-link validation against the (small) human review set.
    """
    by_pk = {record.pk: record for record in records}
    by_key = {record.key: record for record in records}
    uf = UnionFind(sorted(by_pk))
    latest, conflicts = resolve_decisions(decision_history)
    forbidden: dict[str, dict[str, Any]] = {
        key: value for key, value in latest.items() if value["decision"] == "different"
    }
    applied: list[dict[str, Any]] = []

    def violating(ra: int, rb: int) -> list[dict[str, Any]]:
        out = []
        for decision in forbidden.values():
            left, right = by_key.get(decision["rec_a_key"]), by_key.get(decision["rec_b_key"])
            if not left or not right:
                continue
            lr, rr = uf.find(left.pk), uf.find(right.pk)
            crosses = (lr == ra and rr == rb) or (lr == rb and rr == ra)
            if crosses:
                out.append(decision)
        return out

    same_decisions = sorted(
        (value for value in latest.values() if value["decision"] == "same"),
        key=_decision_order,
    )
    for decision in same_decisions:
        left, right = by_key.get(decision["rec_a_key"]), by_key.get(decision["rec_b_key"])
        if not left or not right or uf.find(left.pk) == uf.find(right.pk):
            continue
        blockers = violating(uf.find(left.pk), uf.find(right.pk))
        if blockers:
            newest = max(blockers, key=_decision_order)
            if _decision_order(decision) > _decision_order(newest):
                for blocker in blockers:
                    forbidden.pop(blocker["pair_key"], None)
                conflicts.append({
                    "type": "same_supersedes_different",
                    "pair_key": decision["pair_key"],
                    "superseded_pair": newest["pair_key"],
                })
            else:
                conflicts.append({
                    "type": "same_blocked_by_different",
                    "pair_key": decision["pair_key"],
                    "blocking_pair": newest["pair_key"],
                })
                continue
        uf.union(left.pk, right.pk)
        applied.append({
            "pair_key": decision["pair_key"],
            "rec_a_pk": left.pk,
            "rec_b_pk": right.pk,
            "score": 0.0,
            "kind": "human_same",
            "details": {"decision_id": decision["decision_id"]},
        })

    sorted_edges = sorted(
        model_edges, key=lambda edge: (-float(edge["score"]), edge["pair_key"])
    )
    for edge in sorted_edges:
        left, right = by_pk.get(edge["rec_a_pk"]), by_pk.get(edge["rec_b_pk"])
        if not left or not right:
            continue
        if edge["pair_key"] in latest:
            continue  # any human decision overrides the model for that pair
        if violating(uf.find(left.pk), uf.find(right.pk)):
            continue
        if uf.find(left.pk) != uf.find(right.pk):
            uf.union(left.pk, right.pk)
        applied.append({**edge, "kind": "model"})

    groups: dict[int, list[Record]] = {}
    for record in records:
        groups.setdefault(uf.find(record.pk), []).append(record)
    ordered_groups = [
        sorted(group, key=lambda record: record.key)
        for group in sorted(groups.values(), key=lambda g: min(r.key for r in g))
    ]
    return ordered_groups, applied, conflicts

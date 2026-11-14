"""Serialization helpers used by the HTTP layer (no route definitions)."""
from __future__ import annotations

import csv
import io
import json
from typing import Any

from .fingerprint import (
    fingerprint_summary, list_cross_batch_refs, list_fingerprints,
)
from .models import record_from_db
from .storage import Storage


def fingerprint_payload(storage: Storage, batch_id: str | None = None) -> dict[str, Any]:
    return {
        "batch_id": batch_id,
        "fingerprints": list_fingerprints(storage, batch_id),
        "cross_batch_refs": (
            list_cross_batch_refs(storage, batch_id) if batch_id is not None else []
        ),
    }


def batch_public(storage: Storage, row: Any, include_dead: bool = False) -> dict[str, Any]:
    """Batch fields are additive: pre-existing keys keep their shape."""
    payload = dict(row)
    payload["latest_run_id"] = storage.latest_run_id(row["batch_id"])
    payload["fingerprint_summary"] = fingerprint_summary(storage, row["batch_id"])
    payload["cross_batch_refs"] = list_cross_batch_refs(
        storage, row["batch_id"], include_records=include_dead
    )
    if include_dead:
        payload["dead_letters"] = [
            dict(dead) for dead in storage.list_dead_letters(row["batch_id"])
        ]
    return payload


def run_public(storage: Storage, row: Any) -> dict[str, Any]:
    payload = dict(row)
    payload["timings"] = json.loads(payload.pop("timings_json"))
    payload["blocking_stats"] = json.loads(payload.pop("blocking_stats_json"))
    # Cross-batch context is derived live from the index; run rows themselves
    # are never rewritten by fingerprinting or later batch ingestion.
    payload["fingerprint_summary"] = fingerprint_summary(storage, row["batch_id"])
    payload["cross_batch_refs"] = list_cross_batch_refs(storage, row["batch_id"])
    attempt = storage.latest_attempt(row["run_id"])
    count = storage.query_one(
        "SELECT COUNT(*) AS n FROM run_attempts WHERE run_id=?", (row["run_id"],)
    )
    payload["attempt_count"] = int(count["n"]) if count else 0
    if attempt is not None:
        payload["last_attempt"] = {
            "started_at": attempt["started_at"],
            "finished_at": attempt["finished_at"],
            "timings": json.loads(attempt["timings_json"]),
            "status": attempt["status"],
        }
    return payload


def _golden_row(cluster: Any) -> dict[str, Any]:
    golden = json.loads(cluster["golden_json"])
    return {
        "cluster_id": cluster["cluster_id"],
        "title": golden.get("title"),
        "authors": golden.get("authors") or [],
        "venue": golden.get("venue"),
        "year": golden.get("year"),
        "volume": golden.get("volume"),
        "issue": golden.get("issue"),
        "pages": golden.get("pages"),
        "doi": golden.get("doi"),
        "member_count": golden.get("member_count", 0),
        "member_keys": json.loads(cluster["member_keys_json"]),
        "provenance": golden.get("provenance", {}),
    }


def cluster_summaries(storage: Storage, run_id: str) -> list[dict[str, Any]]:
    output = []
    for cluster in storage.list_clusters(run_id):
        golden = json.loads(cluster["golden_json"])
        output.append({
            "run_id": run_id,
            "cluster_id": cluster["cluster_id"],
            "member_count": golden.get("member_count", 0),
            "member_keys": json.loads(cluster["member_keys_json"]),
            "golden": golden,
        })
    return output


def cluster_detail(storage: Storage, run_id: str, cluster_id: str) -> dict[str, Any]:
    cluster = storage.get_cluster(run_id, cluster_id)
    if cluster is None:
        raise LookupError(f"cluster {cluster_id!r} not found in run {run_id!r}")
    member_pks = set(json.loads(cluster["member_pks_json"]))
    member_rows = storage.query(
        f"SELECT * FROM records WHERE record_pk IN ({','.join('?' for _ in member_pks)})"
        " ORDER BY source,record_id",
        tuple(sorted(member_pks)),
    ) if member_pks else []
    records = [record_from_db(row) for row in member_rows]
    key_by_pk = {record.pk: record.key for record in records}
    edges = []
    for edge in storage.list_edges(run_id):
        if edge["rec_a_pk"] in member_pks and edge["rec_b_pk"] in member_pks:
            edges.append({
                "pair_key": edge["pair_key"],
                "left_key": key_by_pk.get(edge["rec_a_pk"]),
                "right_key": key_by_pk.get(edge["rec_b_pk"]),
                "score": edge["score"],
                "kind": edge["kind"],
                "details": json.loads(edge["details_json"]),
            })
    return {
        "run_id": run_id,
        "cluster_id": cluster_id,
        "members": [record.public() for record in records],
        "golden": json.loads(cluster["golden_json"]),
        "edges": edges,
    }


def _author_text(authors: list[dict[str, Any]]) -> str:
    return "; ".join(
        f"{author.get('family', '')}, {author.get('given', '')}".strip(", ")
        for author in authors
    )


def export_golden(storage: Storage, run_id: str, output_format: str) -> tuple[str, str, str]:
    """Return (body, media_type, filename) for deterministic golden export."""
    clusters = storage.list_clusters(run_id)
    rows = [_golden_row(cluster) for cluster in clusters]
    if output_format == "jsonl":
        body = "".join(
            json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n"
            for row in rows
        )
        return body, "application/x-ndjson", f"citefuse_golden_{run_id}.jsonl"
    if output_format != "csv":
        raise ValueError("format must be 'jsonl' or 'csv'")
    stream = io.StringIO(newline="")
    columns = [
        "cluster_id", "title", "authors", "venue", "year", "volume", "issue",
        "pages", "doi", "member_count", "member_keys",
    ]
    writer = csv.DictWriter(stream, fieldnames=columns, lineterminator="\n")
    writer.writeheader()
    for row in rows:
        writer.writerow({
            **{column: ("" if row[column] is None else row[column]) for column in columns},
            "authors": _author_text(row["authors"]),
            "member_keys": ";".join(row["member_keys"]),
        })
    return stream.getvalue(), "text/csv", f"citefuse_golden_{run_id}.csv"

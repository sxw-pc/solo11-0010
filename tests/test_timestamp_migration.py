"""Persisted-timestamp contract and the legacy-database startup migration.

The contract (see clock.py): every persisted timestamp is UTC, ISO 8601,
second precision, ``Z`` suffix — exactly ``YYYY-MM-DDTHH:MM:SSZ``. Legacy
databases written before the contract (naive ``datetime.utcnow()`` strings,
``+00:00`` offsets, fractional seconds, space separators) are normalized once
by ``Storage`` at open time, idempotently and inside a single transaction.
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest

from citefuse.api_views import export_golden
from citefuse.clock import (
    CANONICAL_TIMESTAMP_RE,
    FixedClock,
    SystemClock,
    format_timestamp,
    is_canonical_timestamp,
    normalize_timestamp,
)
from citefuse.ingest import ingest_jsonl
from citefuse.metrics import build_report
from citefuse.pipeline import run_pipeline
from citefuse.review import next_review_pairs, submit_decision
from citefuse.storage import Storage
from scripts.seed_fixtures import generate

MIGRATED_TABLES = {"batches", "runs", "run_attempts", "decisions", "audit_events"}

TIMESTAMP_COLUMNS: tuple[tuple[str, str], ...] = (
    ("batches", "created_at"),
    ("batches", "updated_at"),
    ("runs", "created_at"),
    ("run_attempts", "started_at"),
    ("run_attempts", "finished_at"),
    ("decisions", "submitted_at"),
    ("audit_events", "created_at"),
)

#: Legacy shapes produced by pre-contract code, with their canonical forms.
LEGACY_SAMPLES = [
    ("2024-05-17T10:00:00.123456", "2024-05-17T10:00:00Z"),  # utcnow().isoformat()
    ("2024-05-17T10:00:01", "2024-05-17T10:00:01Z"),  # naive seconds
    ("2024-05-17 10:00:02", "2024-05-17T10:00:02Z"),  # str(utcnow())
    ("2024-05-17T10:00:03+00:00", "2024-05-17T10:00:03Z"),  # aware, UTC offset
    ("2024-05-17T18:00:04+08:00", "2024-05-17T10:00:04Z"),  # aware, other zone
]

#: One legacy shape per timestamp column, used to regress a real database.
LEGACY_BY_COLUMN = {
    "batches.created_at": LEGACY_SAMPLES[0],
    "batches.updated_at": LEGACY_SAMPLES[1],
    "runs.created_at": LEGACY_SAMPLES[2],
    "run_attempts.started_at": LEGACY_SAMPLES[3],
    "run_attempts.finished_at": LEGACY_SAMPLES[4],
    "decisions.submitted_at": LEGACY_SAMPLES[0],
    "audit_events.created_at": LEGACY_SAMPLES[1],
}


def _seeded_payload() -> bytes:
    records, _ = generate(20240517)
    return (
        "\n".join(
            json.dumps(record, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
            for record in records
        )
        + "\n"
    ).encode("utf-8")


def _raw_timestamps(path: Path) -> dict[str, list[str]]:
    """Read every persisted timestamp straight from the file (no migration)."""
    conn = sqlite3.connect(str(path))
    try:
        return {
            f"{table}.{column}": sorted(
                row[0] for row in conn.execute(
                    f"SELECT DISTINCT {column} FROM {table}"
                ).fetchall()
            )
            for table, column in TIMESTAMP_COLUMNS
        }
    finally:
        conn.close()


def _build_legacy_db(path: Path, config, clock) -> None:
    """Create a real database, then regress its timestamps to legacy forms."""
    storage = Storage(path)
    try:
        ingest_jsonl(storage, config, clock, _seeded_payload(), batch_id="legacy")
        run_pipeline(storage, config, clock, "legacy")
        pair = next_review_pairs(storage, "legacy", 1)[0]
        submit_decision(
            storage, clock, "legacy", pair["pair_key"], "different", "reviewer-1", "n"
        )
        run_pipeline(storage, config, clock, "legacy")
    finally:
        storage.close()
    conn = sqlite3.connect(str(path))
    try:
        for key, (legacy, _) in LEGACY_BY_COLUMN.items():
            table, column = key.split(".")
            conn.execute(f"UPDATE {table} SET {column}=?", (legacy,))
        conn.commit()
    finally:
        conn.close()


# -- the contract itself -----------------------------------------------------

def test_canonical_format_is_fixed_width_zulu():
    stamp = SystemClock().now_iso()
    assert CANONICAL_TIMESTAMP_RE.fullmatch(stamp)
    assert len(stamp) == 20
    assert is_canonical_timestamp(stamp)
    naive_fixed = FixedClock(datetime(2026, 3, 4, 5, 6, 7))
    assert naive_fixed.now_iso() == "2026-03-04T05:06:07Z"
    aware = datetime(2026, 3, 4, 5, 6, 7, 123456, tzinfo=timezone.utc)
    assert format_timestamp(aware) == "2026-03-04T05:06:07Z"


@pytest.mark.parametrize("legacy,expected", LEGACY_SAMPLES)
def test_normalize_timestamp_accepts_legacy_shapes(legacy, expected):
    assert normalize_timestamp(legacy) == expected
    assert is_canonical_timestamp(expected)


def test_normalize_timestamp_roundtrips_canonical_and_rejects_garbage():
    canonical = "2024-05-17T10:00:00Z"
    assert normalize_timestamp(canonical) == canonical
    assert normalize_timestamp("not-a-timestamp") is None
    assert normalize_timestamp("") is None
    assert normalize_timestamp("2024-13-40T99:99:99") is None


# -- the startup migration ---------------------------------------------------

def test_legacy_database_is_normalized_on_open(tmp_path, config, clock):
    path = tmp_path / "legacy.sqlite3"
    _build_legacy_db(path, config, clock)
    before = _raw_timestamps(path)
    for key, (legacy, _) in LEGACY_BY_COLUMN.items():
        assert before[key] == [legacy]

    storage = Storage(path)
    try:
        assert set(storage.timestamp_migration) == MIGRATED_TABLES
        assert sum(storage.timestamp_migration.values()) > 0
        after = _raw_timestamps(path)
        for key, (_, expected) in LEGACY_BY_COLUMN.items():
            # Every legacy value landed on its expected canonical form.
            assert after[key] == [expected]
    finally:
        storage.close()


def test_migration_is_idempotent_across_restarts(tmp_path, config, clock):
    path = tmp_path / "legacy.sqlite3"
    _build_legacy_db(path, config, clock)

    first = Storage(path)
    assert sum(first.timestamp_migration.values()) > 0
    snapshot = _raw_timestamps(path)
    first.close()

    for _ in range(2):
        again = Storage(path)
        # Everything already conforms, so nothing is rewritten again.
        assert sum(again.timestamp_migration.values()) == 0
        assert _raw_timestamps(path) == snapshot
        again.close()


def test_migration_rolls_back_on_failure(tmp_path, config, clock, monkeypatch):
    path = tmp_path / "legacy.sqlite3"
    _build_legacy_db(path, config, clock)
    before = _raw_timestamps(path)

    calls = {"count": 0}

    def boom(value):
        calls["count"] += 1
        if calls["count"] >= 3:  # let at least one row get rewritten first
            raise RuntimeError("simulated crash mid-migration")
        return normalize_timestamp(value)

    monkeypatch.setattr("citefuse.storage.normalize_timestamp", boom)
    with pytest.raises(RuntimeError, match="simulated crash"):
        Storage(path)
    # The failed attempt left no half-migrated state behind.
    assert _raw_timestamps(path) == before

    monkeypatch.undo()
    storage = Storage(path)
    assert sum(storage.timestamp_migration.values()) > 0
    storage.close()
    for values in _raw_timestamps(path).values():
        for value in values:
            assert is_canonical_timestamp(value), value


def test_unparseable_timestamps_are_left_untouched(tmp_path):
    path = tmp_path / "odd.sqlite3"
    storage = Storage(path)
    storage.execute(
        "INSERT INTO batches(batch_id,name,input_hash,created_at,updated_at,"
        "total_lines,valid_lines,bad_lines,status)"
        " VALUES('odd',NULL,'h','garbage','2024-05-17T10:00:00',0,0,0,'ingested')"
    )
    storage.close()

    reopened = Storage(path)
    # The row was rewritten once (updated_at), the garbage value preserved.
    assert reopened.timestamp_migration["batches"] == 1
    row = reopened.get_batch("odd")
    assert row["created_at"] == "garbage"
    assert row["updated_at"] == "2024-05-17T10:00:00Z"
    reopened.close()

    # Garbage is never destroyed and never retried into a rewrite loop.
    third = Storage(path)
    assert third.timestamp_migration["batches"] == 0
    assert third.get_batch("odd")["created_at"] == "garbage"
    third.close()


def test_mixed_legacy_and_new_rows_sort_chronologically(tmp_path):
    path = tmp_path / "mixed.sqlite3"
    storage = Storage(path)
    storage.execute(
        "INSERT INTO batches(batch_id,name,input_hash,created_at,updated_at,"
        "total_lines,valid_lines,bad_lines,status)"
        " VALUES('m',NULL,'h','2024-05-17T10:00:00','2024-05-17T10:00:00',0,0,0,'x')"
    )
    # Legacy fractional/naive rows that pre-migration would interleave wrongly
    # with canonical ones ("." sorts before "Z" in a plain string compare).
    storage.add_decision("m", "p||1", "A::1", "B::1", "same", "r", "", "2024-05-17T10:00:00.9")
    storage.add_decision("m", "p||2", "A::2", "B::2", "same", "r", "", "2024-05-17T10:00:00")
    storage.add_decision("m", "p||3", "A::3", "B::3", "same", "r", "", "2024-05-17T10:00:01.1")
    storage.close()

    reopened = Storage(path)
    # New writes after the migration use the canonical contract.
    reopened.add_decision(
        "m", "p||4", "A::4", "B::4", "same", "r", "", "2024-05-17T10:00:01Z"
    )
    history = reopened.decision_history("m")
    stamps = [row["submitted_at"] for row in history]
    assert all(is_canonical_timestamp(stamp) for stamp in stamps)
    # Fractional legacy values were truncated; same-second rows tie and fall
    # back to decision_id order, the later second sorts last.
    assert stamps == [
        "2024-05-17T10:00:00Z",
        "2024-05-17T10:00:00Z",
        "2024-05-17T10:00:01Z",
        "2024-05-17T10:00:01Z",
    ]
    assert [row["pair_key"] for row in history] == ["p||1", "p||2", "p||3", "p||4"]
    reopened.close()


# -- the acceptance rehearsal: a pre-contract database stays fully usable ----

def test_migrated_database_remains_fully_functional(tmp_path, config, clock):
    path = tmp_path / "old.sqlite3"
    storage = Storage(path)
    ingest_jsonl(storage, config, clock, _seeded_payload(), batch_id="old")
    run_pipeline(storage, config, clock, "old")
    pair = next_review_pairs(storage, "old", 1)[0]
    submit_decision(storage, clock, "old", pair["pair_key"], "different", "qa", "note")
    second = run_pipeline(storage, config, clock, "old")
    golden_before, _, _ = export_golden(storage, second["run_id"], "jsonl")
    report_before = build_report(storage, config, tmp_path / "r1", second["run_id"])
    storage.close()

    # Regress every timestamp to the utcnow-era naive shape, then reopen.
    conn = sqlite3.connect(str(path))
    for table, column in TIMESTAMP_COLUMNS:
        conn.execute(f"UPDATE {table} SET {column}=REPLACE({column},'Z','')")
    conn.commit()
    conn.close()

    reopened = Storage(path)
    assert sum(reopened.timestamp_migration.values()) > 0
    # The naive values were canonical-minus-Z, so migration restores the exact
    # original strings and the derived run_id is stable across the migration.
    rerun = run_pipeline(reopened, config, clock, "old")
    assert rerun["run_id"] == second["run_id"]
    assert rerun["cluster_count"] == 120

    golden_after, _, _ = export_golden(reopened, second["run_id"], "jsonl")
    assert golden_after == golden_before
    report_after = build_report(reopened, config, tmp_path / "r2", second["run_id"])
    assert report_after["pairwise"]["f1"] == 1.0
    assert {
        key: value for key, value in report_after.items() if key != "report_path"
    } == {key: value for key, value in report_before.items() if key != "report_path"}

    # Review still works on the migrated database and writes canonical stamps.
    queue = next_review_pairs(reopened, "old", 1)
    assert queue
    decision = submit_decision(
        reopened, clock, "old", queue[0]["pair_key"], "same", "qa2", "post-migration"
    )
    assert is_canonical_timestamp(decision["submitted_at"])
    history = reopened.decision_history("old")
    assert all(is_canonical_timestamp(row["submitted_at"]) for row in history)
    reopened.close()

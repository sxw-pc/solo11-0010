"""Integration tests for the drift guard: tiers, correction, quarantine,
baseline learning, concurrency and fail-open degradation."""
from __future__ import annotations

import json
import threading

import pytest

from citefuse.config import load_config
from citefuse.drift.baseline import Baseline, CountStats, NumericStats
from citefuse.drift.guard import DriftGuard
from citefuse.drift.store import DriftStore
from citefuse.ingest import ingest_jsonl
from citefuse.models import record_from_db
from citefuse.storage import Storage


def _records(storage, batch_id: str):
    return [record_from_db(row) for row in storage.list_records(batch_id)]


def _guard(storage, clock, config=None):
    return DriftGuard(storage, config or load_config(), clock)


def _drift_payload() -> list[dict]:
    base = {
        "record_id": "R001", "source": "ORBIT", "title": "Stable baseline title",
        "authors": ["Zorvath, Vaelis"], "venue": "Journal of Fictional Evidence Synthesis",
        "year": 2001, "doi": "10.5555/citefuse.0001",
    }
    return [base]


def test_guard_cold_start_clean_record_passes(storage, config, clock):
    payload = json.dumps(_drift_payload()[0])
    ingest_jsonl(storage, config, clock, payload.encode(), batch_id="b1")
    guard = _guard(storage, clock, config)
    outcome = guard.guard("run-1", "b1", _records(storage, "b1"))
    assert outcome["status"] == "ok"
    assert outcome["summary"]["tier_counts"]["clean"] == 1
    assert outcome["quarantined_keys"] == []
    # A baseline version is promoted from the accepted record.
    assert outcome["new_baseline_version"] == 1


def test_heavy_drift_record_is_quarantined_and_removed_from_eligible(
    storage, config, clock
):
    rows = [
        {"record_id": "OK1", "source": "ORBIT", "title": "Normal paper",
         "authors": ["Quellmar, Tirune"], "year": 2010,
         "doi": "10.5555/citefuse.0010"},
        {"record_id": "BAD1", "source": "ORBIT", "title": "Era paper",
         "authors": ["Branteth, Soreth"], "year": "回历1420年",
         "doi": "10.5555/citefuse.0011"},
    ]
    ingest_jsonl(
        storage, config, clock,
        ("\n".join(json.dumps(r) for r in rows)).encode(), batch_id="b2",
    )
    guard = _guard(storage, clock, config)
    outcome = guard.guard("run-2", "b2", _records(storage, "b2"))
    eligible_keys = {r.key for r in outcome["eligible_records"]}
    assert "ORBIT::BAD1" not in eligible_keys
    assert "ORBIT::OK1" in eligible_keys
    assert "ORBIT::BAD1" in outcome["quarantined_keys"]
    quarantined = DriftStore(storage).list_quarantine("b2")
    assert len(quarantined) == 1
    assert quarantined[0]["record_key"] == "ORBIT::BAD1"
    assert quarantined[0]["raw"]["year"] == "回历1420年"


def test_minor_source_drift_is_auto_corrected_in_memory_only(storage, config, clock):
    row = {"record_id": "S1", "source": "Orbital Bibliographic Exchange",
           "title": "Alias source paper", "authors": ["Dunwellyn, Mavrik"],
           "year": 2012, "doi": "10.5555/citefuse.0020"}
    ingest_jsonl(storage, config, clock, json.dumps(row).encode(), batch_id="b3")
    guard = _guard(storage, clock, config)
    records = _records(storage, "b3")
    original_source = records[0].source  # source column is upper-cased at ingest
    outcome = guard.guard("run-3", "b3", records)
    corrected = outcome["eligible_records"][0]
    assert corrected.source == "ORBIT"            # in-memory canonical source
    assert corrected.norm["source_original"] == original_source
    # Database row retains the original values; nothing is rewritten on disk.
    db_row = storage.list_records("b3")[0]
    assert db_row["source"] == original_source
    assert json.loads(db_row["raw_json"])["source"] == "Orbital Bibliographic Exchange"
    disposition = DriftStore(storage).list_dispositions("b3", "minor")[0]
    assert disposition["corrected_fields"]["source"] == "ORBIT"


def test_medium_drift_record_is_tagged_low_priority(storage, config, clock):
    row = {"record_id": "M1", "source": "Some Unknown University Library",
           "title": "Unknown org paper", "authors": ["Silvaris, Liosha"],
           "year": 2014, "doi": "10.5555/citefuse.0030"}
    ingest_jsonl(storage, config, clock, json.dumps(row).encode(), batch_id="b4")
    guard = _guard(storage, clock, config)
    records = _records(storage, "b4")
    outcome = guard.guard("run-4", "b4", records)
    tagged = outcome["eligible_records"][0]
    assert tagged.norm["_drift"]["tier"] == "medium"
    assert outcome["low_priority_keys"] == [tagged.key]
    assert tagged.key not in outcome["quarantined_keys"]


def test_baseline_is_versioned_and_distribution_check_activates(
    storage, config, clock
):
    # Build a tight baseline of modern years only.
    rows = [
        {"record_id": f"H{i}", "source": "ORBIT", "title": f"History {i}",
         "authors": [f"Family{i:02d}, Given"], "year": 2000 + i % 5,
         "doi": f"10.5555/citefuse.{i:04d}"}
        for i in range(25)
    ]
    ingest_jsonl(
        storage, config, clock,
        ("\n".join(json.dumps(r) for r in rows)).encode(), batch_id="hist",
    )
    guard = _guard(storage, clock, config)
    first = guard.guard("run-h", "hist", _records(storage, "hist"))
    assert first["new_baseline_version"] == 1

    # A record with a year far outside the learned distribution.
    outlier = {"record_id": "OUT", "source": "ORBIT", "title": "Outlier",
               "authors": ["Orbain, Ondrei"], "year": 1955,
               "doi": "10.5555/citefuse.9999"}
    ingest_jsonl(storage, config, clock, json.dumps(outlier).encode(),
                 batch_id="new")
    outcome = guard.guard("run-new", "new", _records(storage, "new"))
    verdicts = {
        v["field"]: v for v in outcome["results"][0].as_dict()["verdicts"]
    }
    assert "year" in verdicts
    assert verdicts["year"]["reason_code"] == "year_distribution_outlier"


def test_concurrent_baseline_promotions_never_share_a_version(
    storage, config, clock
):
    drift_store = DriftStore(storage)
    errors: list[Exception] = []

    def promote(batch: str) -> None:
        try:
            stats = {"year": NumericStats()}
            stats["year"].observe(2000.0)
            drift_store.promote_baseline(stats, batch, 1, clock.now_iso())
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [
        threading.Thread(target=promote, args=(f"batch-{i}",)) for i in range(8)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert not errors
    versions = [row["version"] for row in drift_store.list_baseline_versions()]
    assert versions == list(range(1, 9))  # unique, gap-free, ordered


def test_cross_process_baseline_promotions_are_serialized(tmp_path):
    """Separate connections (simulating worker processes) cannot clash.

    Each promoter follows the production read-merge-promote path: load the
    latest committed snapshot, fold its own observations into a copy, then
    append a new version under an immediate write lock.
    """
    import sqlite3

    from citefuse.drift.baseline import NumericStats

    db_path = tmp_path / "cross.sqlite3"
    first = Storage(db_path)
    try:
        store_a = DriftStore(first)
        other_conn = sqlite3.connect(str(db_path))
        other_conn.row_factory = sqlite3.Row
        other_conn.execute("PRAGMA journal_mode=WAL")

        class _BareStorage:
            def __init__(self, conn):
                self.conn = conn

            class _Lock:
                def __enter__(self):
                    return None

                def __exit__(self, *args):
                    return None

            _lock = _Lock()

            def query_one(self, sql, args=()):
                return self.conn.execute(sql, args).fetchone()

        store_b = DriftStore.__new__(DriftStore)
        store_b.storage = _BareStorage(other_conn)

        def merged_promote(store, observed_year, batch, stamp):
            previous = store.load_latest_baseline()
            stats = {}
            if previous is not None:
                for name, stat in previous.stats.items():
                    if isinstance(stat, NumericStats):
                        stats[name] = NumericStats.from_dict(stat.to_dict())
                    else:
                        from citefuse.drift.baseline import CountStats

                        stats[name] = CountStats.from_dict(stat.to_dict())
            stats.setdefault("year", NumericStats()).observe(float(observed_year))
            return store.promote_baseline(stats, batch, 1, stamp)

        v1 = merged_promote(store_a, 2001, "proc-a", "2025-01-01T00:00:00Z")
        v2 = merged_promote(store_b, 2002, "proc-b", "2025-01-01T00:00:01Z")
        assert (v1, v2) == (1, 2)
        other_conn.close()

        latest = store_a.load_latest_baseline()
        assert latest is not None and latest.version == 2
        # The second promotion merged onto the first, retaining both samples.
        assert latest.numeric("year").count == 2
    finally:
        first.close()


def test_fail_open_degrades_without_blocking_pipeline(storage, config, clock):
    row = {"record_id": "F1", "source": "ORBIT", "title": "Fail open",
           "authors": ["Calyndor, Paveth"], "year": 2011,
           "doi": "10.5555/citefuse.0040"}
    ingest_jsonl(storage, config, clock, json.dumps(row).encode(), batch_id="b5")
    guard = _guard(storage, clock, config)

    def boom(*_args, **_kwargs):
        raise RuntimeError("simulated detector crash")

    guard.store.load_latest_baseline = boom  # type: ignore[method-assign]
    outcome = guard.guard("run-5", "b5", _records(storage, "b5"))
    assert outcome["status"] == "degraded"
    assert outcome["degraded"] is True
    # Every record still flows to the core pipeline.
    assert len(outcome["eligible_records"]) == 1
    assert outcome["summary"]["degradation_reason"].startswith("RuntimeError")


def test_fail_open_survives_audit_failure_too(storage, config, clock):
    row = {"record_id": "F2", "source": "ORBIT", "title": "Double fault",
           "authors": ["Vaelwyn, Wren"], "year": 2011,
           "doi": "10.5555/citefuse.0041"}
    ingest_jsonl(storage, config, clock, json.dumps(row).encode(), batch_id="b5b")
    guard = _guard(storage, clock, config)

    def boom(*_args, **_kwargs):
        raise RuntimeError("detector down")

    def audit_boom(*_args, **_kwargs):
        raise RuntimeError("audit also down")

    guard.store.load_latest_baseline = boom  # type: ignore[method-assign]
    # The audit helper is module-level in the guard namespace.
    import citefuse.drift.guard as guard_module

    guard_module.record_event = audit_boom  # type: ignore[assignment]
    try:
        outcome = guard.guard("run-5b", "b5b", _records(storage, "b5b"))
    finally:
        from citefuse.audit import record_event as real_record_event

        guard_module.record_event = real_record_event  # type: ignore[assignment]
    assert outcome["status"] == "degraded"
    assert len(outcome["eligible_records"]) == 1


def test_disabled_guard_skips_and_passes_all_records(storage, config, clock):
    row = {"record_id": "D1", "source": "ORBIT", "title": "Disabled",
           "authors": ["Tavren, Iryssa"], "year": 2011,
           "doi": "10.5555/citefuse.0050"}
    ingest_jsonl(storage, config, clock, json.dumps(row).encode(), batch_id="b6")
    cfg = load_config()
    cfg.drift.enabled = False
    guard = DriftGuard(storage, cfg, clock)
    outcome = guard.guard("run-6", "b6", _records(storage, "b6"))
    assert outcome["status"] == "skipped"
    assert len(outcome["eligible_records"]) == 1


def test_dispositions_and_summary_are_persisted(storage, config, clock):
    rows = [
        {"record_id": "C1", "source": "ORBIT", "title": "Clean",
         "authors": ["Marrowyn, Nerune"], "year": 2019,
         "doi": "10.5555/citefuse.0060"},
        {"record_id": "Q1", "source": "###", "title": "Junk source",
         "authors": ["Pellucid, Solvane"], "year": 2019,
         "doi": "10.5555/citefuse.0061"},
    ]
    ingest_jsonl(
        storage, config, clock,
        ("\n".join(json.dumps(r) for r in rows)).encode(), batch_id="b7",
    )
    guard = _guard(storage, clock, config)
    guard.guard("run-7", "b7", _records(storage, "b7"))
    store = DriftStore(storage)
    summary = store.get_drift_run("run-7")["summary"]
    assert summary["total_records"] == 2
    assert summary["tier_counts"]["heavy"] == 1
    assert "source.source_unrecognized" in summary["by_reason"]
    tiers = {d["record_key"]: d["tier"] for d in store.list_dispositions("b7")}
    assert tiers["###::Q1"] == "heavy"


def test_baseline_stats_round_trip():
    numeric = NumericStats()
    for value in (2001.0, 2002.0, 2003.0):
        numeric.observe(value)
    categorical = CountStats()
    categorical.observe("ORBIT", 100)
    baseline = Baseline(7, {"year": numeric, "source": categorical})
    restored = Baseline.from_json(7, baseline.to_json())
    assert restored.version == 7
    assert restored.numeric("year").mean == pytest.approx(2002.0)
    assert restored.categorical("source").frequency("ORBIT") == 1

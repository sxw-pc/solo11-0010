"""Pipeline-level tests proving heavy drift never reaches clusters/golden."""
from __future__ import annotations

import json

from citefuse.drift.store import DriftStore
from citefuse.ingest import ingest_jsonl
from citefuse.pipeline import run_pipeline


def _payload(rows: list[dict]) -> bytes:
    return ("\n".join(json.dumps(r) for r in rows)).encode()


def test_quarantined_record_absent_from_clusters_and_golden(storage, config, clock):
    # Two clean duplicates of the same work plus one heavy-drift third copy.
    rows = [
        {"record_id": "A1", "source": "ORBIT", "title": "Cluster target paper",
         "authors": ["Zorvath, Vaelis"], "venue": "Journal of Fictional Evidence Synthesis",
         "year": 2010, "pages": "100-110", "doi": "10.5555/citefuse.7001"},
        {"record_id": "A2", "source": "LANTERN", "title": "Cluster target paper",
         "authors": ["Zorvath, V."], "venue": "JFES",
         "year": 2010, "pages": "100-110", "doi": "10.5555/citefuse.7001"},
        {"record_id": "A3", "source": "ORBIT", "title": "Era corrupted copy",
         "authors": ["Branteth, Soreth"], "venue": "JFES",
         "year": "回历1420年", "pages": "100-110",
         "doi": "10.5555/citefuse.7001"},
    ]
    ingest_jsonl(storage, config, clock, _payload(rows), batch_id="pb1")
    result = run_pipeline(storage, config, clock, "pb1")

    # The drift summary accompanies the run result.
    summary = result["drift_summary"]
    assert summary["tier_counts"]["heavy"] == 1
    assert summary["quarantined_records"] == 1

    # Only the two clean records participate in clustering.
    all_member_keys = {
        key for cluster in result["clusters"] for key in cluster["member_keys"]
    }
    assert "ORBIT::A3" not in all_member_keys
    assert {"ORBIT::A1", "LANTERN::A2"}.issubset(all_member_keys)

    # The heavy record is in dedicated quarantine storage.
    quarantined = DriftStore(storage).list_quarantine("pb1")
    assert [item["record_key"] for item in quarantined] == ["ORBIT::A3"]

    # Golden records never embed the quarantined record as provenance.
    for cluster in result["clusters"]:
        provenance = set().union(
            *[set(v) for v in cluster["golden"].get("provenance", {}).values()]
        )
        assert "ORBIT::A3" not in provenance


def test_pipeline_runs_when_guard_degraded(storage, config, clock, monkeypatch):
    rows = [
        {"record_id": "G1", "source": "ORBIT", "title": "Degraded guard run",
         "authors": ["Quellmar, Tirune"], "year": 2015,
         "doi": "10.5555/citefuse.8001"},
    ]
    ingest_jsonl(storage, config, clock, _payload(rows), batch_id="pb2")

    def crash(*_args, **_kwargs):
        raise RuntimeError("baseline store unavailable")

    monkeypatch.setattr(DriftStore, "load_latest_baseline", crash)
    result = run_pipeline(storage, config, clock, "pb2")
    assert result["status"] == "completed"
    assert result["drift_summary"]["degraded"] is True
    # All records still clustered because detection was skipped.
    assert result["cluster_count"] == 1


def test_drift_timing_is_recorded_and_summary_persisted(storage, config, clock):
    rows = [
        {"record_id": "T1", "source": "ORBIT", "title": "Timed paper",
         "authors": ["Silvaris, Liosha"], "year": 2018,
         "doi": "10.5555/citefuse.9001"},
    ]
    ingest_jsonl(storage, config, clock, _payload(rows), batch_id="pb3")
    result = run_pipeline(storage, config, clock, "pb3")
    assert "drift_guard" in result["timings"]
    drift_run = DriftStore(storage).get_drift_run(result["run_id"])
    assert drift_run is not None
    assert drift_run["summary"]["total_records"] == 1

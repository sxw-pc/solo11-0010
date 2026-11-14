"""Cross-batch document fingerprints: refs, immutability, degradation, upgrade."""
from __future__ import annotations

import json
import sqlite3
import threading
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from citefuse.api import create_app
from citefuse.config import load_config
from citefuse.fingerprint import (
    FINGERPRINT_ALGORITHM, build_fingerprint, collision_guard,
    list_cross_batch_refs, list_fingerprints,
)
from citefuse.ingest import ingest_jsonl
from citefuse.pipeline import run_pipeline
from citefuse.storage import Storage

ENTITY_A_BASE = {
    "title": "Shared Fictional Cohort: A Synthetic Trial",
    "authors": ["Vaelis Zorvath", "Tirune Quellmar"],
    "year": 2020,
}
ENTITY_A_VARIANT = {
    "title": "SHARED FICTIONAL COHORT, A SYNTHETIC TRIAL!",
    "authors": ["Quellmar, Tirune", "ZORVATH, VAELIS"],
    "year": "c. 2020",
}
ENTITY_B = {
    "title": "Unrelated Invented Ledger: Diagnostics in Practice",
    "authors": ["Mavrik Orbain"],
    "year": 2019,
}


def _payload(records: list[dict]) -> bytes:
    return (
        "\n".join(
            json.dumps(record, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
            for record in records
        )
        + "\n"
    ).encode("utf-8")


def _record(record_id, source, base, **overrides):
    return {"record_id": record_id, "source": source, **base, **overrides}


def _run(storage, config, clock, batch_id):
    return run_pipeline(storage, config, clock, batch_id)


def _cluster_snapshot(storage, run_id):
    return [
        (row["cluster_id"], row["member_keys_json"], row["golden_json"],
         row["member_pks_json"])
        for row in storage.list_clusters(run_id)
    ]


def _entity_fingerprint(storage, batch_id, record_key):
    for row in list_fingerprints(storage, batch_id):
        if row["record_key"] == record_key and row["fingerprint"]:
            return row["fingerprint"]
    raise AssertionError(f"no fingerprint for {record_key}")


def test_cross_batch_refs_do_not_touch_history(storage, config, clock):
    batch_a = _payload([
        _record("A1", "ORBIT", ENTITY_A_BASE, venue="JFES",
                volume=12, issue=3, pages="10-20", doi="10.5555/a.1"),
        _record("A2", "LANTERN", ENTITY_B, venue="IRMQ",
                volume=4, issue=1, pages="30-40", doi="10.5555/b.1"),
    ])
    assert ingest_jsonl(storage, config, clock, batch_a, batch_id="A")["inserted"] == 2
    run_a = _run(storage, config, clock, "A")
    run_a_row_before = tuple(storage.get_run(run_a["run_id"]))
    records_before = {
        row["record_pk"]: (row["raw_json"], row["norm_json"], row["line_no"])
        for row in storage.list_records("A")
    }
    clusters_before = _cluster_snapshot(storage, run_a["run_id"])
    batch_a_row_before = tuple(storage.get_batch("A"))

    # Second batch: one convergent variant of A's first entity from another
    # source plus one genuinely new entity.
    batch_b = _payload([
        _record("B1", "MEADOW", ENTITY_A_VARIANT, venue="Journal of Fictional"
                " Evidence Synthesis", volume="12", issue="3", pages="10-20",
                doi=None),
        _record("B2", "COMPASS", {
            "title": "Fresh Imaginary Harbor: A New Study",
            "authors": ["Iryssa Pellucid"],
            "year": 2021,
        }, venue="IRMQ", volume=9, year=2021),
    ])
    result_b = ingest_jsonl(storage, config, clock, batch_b, batch_id="B")
    assert result_b["inserted"] == 2
    assert result_b["cross_batch_refs_added"] >= 1

    # Historical data is byte-identical.
    assert tuple(storage.get_batch("A")) == batch_a_row_before
    assert tuple(storage.get_run(run_a["run_id"])) == run_a_row_before
    assert {
        row["record_pk"]: (row["raw_json"], row["norm_json"], row["line_no"])
        for row in storage.list_records("A")
    } == records_before
    assert _cluster_snapshot(storage, run_a["run_id"]) == clusters_before

    # Fingerprints converge across sources without touching record identity.
    fp_a = _entity_fingerprint(storage, "A", "ORBIT::A1")
    fp_b = _entity_fingerprint(storage, "B", "MEADOW::B1")
    assert fp_a == fp_b and fp_a.startswith("fp1_")
    assert _entity_fingerprint(storage, "B", "COMPASS::B2") != fp_a

    refs_b = list_cross_batch_refs(storage, "B")
    assert len(refs_b) == 1
    ref = refs_b[0]
    assert ref["record_key"] == "MEADOW::B1"
    assert ref["matched_batch_id"] == "A"
    assert ref["matched_record_key"] == "ORBIT::A1"
    assert ref["fingerprint"] == fp_a
    assert ref["details"]["guard"]["year_equal"] is True

    # Both directions are visible on batch detail, without rewriting references.
    refs_a = list_cross_batch_refs(storage, "A")
    assert [(item["matched_batch_id"], item["record_key"]) for item in refs_a] == [
        ("B", "ORBIT::A1")
    ]

    # Re-ingesting B's exact payload is a no-op and adds no index/ref rows.
    again = ingest_jsonl(storage, config, clock, batch_b, batch_id="B")
    assert again["inserted"] == 0 and again["cross_batch_refs_added"] == 0
    assert len(list_cross_batch_refs(storage, "B")) == 1


def test_new_batch_clusters_align_through_fingerprints(storage, config, clock):
    batch_a = _payload([
        _record("A1", "ORBIT", ENTITY_A_BASE, venue="JFES", doi="10.5555/a.1"),
        _record("A2", "LANTERN", ENTITY_B, venue="IRMQ", doi="10.5555/b.1"),
    ])
    ingest_jsonl(storage, config, clock, batch_a, batch_id="A")
    run_a = _run(storage, config, clock, "A")

    batch_b = _payload([
        _record("B1", "MEADOW", ENTITY_A_VARIANT, venue="JFES", pages="10-20"),
        _record("B2", "COMPASS", ENTITY_A_BASE, venue="JFES", pages="10-20"),
        _record("B3", "ORBIT", {
            "title": "Fresh Imaginary Harbor: A New Study",
            "authors": ["Iryssa Pellucid"], "year": 2021,
        }, venue="IRMQ"),
    ])
    ingest_jsonl(storage, config, clock, batch_b, batch_id="B")
    run_b = _run(storage, config, clock, "B")

    # B clusters the two convergent records together; A has the same entity.
    cluster_members = [
        cluster["member_keys"] for cluster in run_b["clusters"]
    ]
    assert ["COMPASS::B2", "MEADOW::B1"] in cluster_members
    fp_b1 = _entity_fingerprint(storage, "B", "MEADOW::B1")
    fp_b2 = _entity_fingerprint(storage, "B", "COMPASS::B2")
    assert fp_b1 == fp_b2 == _entity_fingerprint(storage, "A", "ORBIT::A1")

    # New entity stays separate and has a distinct fingerprint.
    fp_new = _entity_fingerprint(storage, "B", "ORBIT::B3")
    assert fp_new != fp_b1
    cluster_b3 = next(
        cluster["member_keys"] for cluster in run_b["clusters"]
        if "ORBIT::B3" in cluster["member_keys"]
    )
    assert cluster_b3 == ["ORBIT::B3"]


def test_single_batch_fixture_quality_and_no_cross_refs(storage, config, clock, seeded_paths):
    payload = seeded_paths[0].read_bytes()
    result = ingest_jsonl(storage, config, clock, payload, batch_id="one")
    assert result["valid_lines"] == 200
    assert result["cross_batch_refs_added"] == 0
    run = _run(storage, config, clock, "one")
    assert run["cluster_count"] == 120
    assert list_cross_batch_refs(storage, "one") == []
    fingerprint_rows = list_fingerprints(storage, "one")
    assert len(fingerprint_rows) == 200
    assert all(row["status"] == "indexed" for row in fingerprint_rows)
    # Same-batch duplicate grouping is untouched: 120 groups, all gold-complete.
    gold_clusters = {record["gold_cluster"] for record in
                     (json.loads(line) for line in payload.decode().splitlines())}
    assert len(gold_clusters) == 120


def test_degraded_records_are_indexed_without_matching(storage, config, clock):
    now = clock.now_iso()
    batch_c = _payload([
        # Valid but not fingerprintable: single-token title.
        _record("C1", "ORBIT", {"title": "Short", "authors": ["Vaelis Zorvath"],
                                "year": 2020}),
        # No parsable year.
        _record("C2", "LANTERN", {"title": "Full Length Synthetic Title Here",
                                  "authors": ["Vaelis Zorvath"], "year": "n.d."}),
        # No authors.
        _record("C3", "MEADOW", {"title": "Another Full Length Synthetic Title",
                                 "authors": [], "year": 2020}),
        # A valid indexed record that must still be matched normally.
        _record("C4", "COMPASS", ENTITY_A_BASE),
    ])
    result = ingest_jsonl(storage, config, clock, batch_c, batch_id="C")
    assert result["inserted"] == 4
    assert result["fingerprint_degraded"] == 3
    assert result["fingerprinted"] == 1

    rows = {row["record_key"]: row for row in list_fingerprints(storage, "C")}
    for key in ("ORBIT::C1", "LANTERN::C2", "MEADOW::C3"):
        assert rows[key]["status"] == "degraded"
        assert rows[key]["fingerprint"] is None
    assert rows["COMPASS::C4"]["status"] == "indexed"

    events = storage.query(
        "SELECT event_type,detail_json FROM audit_events WHERE batch_id='C'"
        " AND event_type='fingerprint_degraded' ORDER BY event_id"
    )
    reasons = {json.loads(event["detail_json"])["record_key"]:
               json.loads(event["detail_json"])["reason"] for event in events}
    assert set(reasons) == {"ORBIT::C1", "LANTERN::C2", "MEADOW::C3"}
    assert reasons["ORBIT::C1"] == "title_too_short"
    assert reasons["LANTERN::C2"] == "missing_year"
    assert reasons["MEADOW::C3"] == "missing_authors"

    # Degraded records do not join cross-batch references even if another
    # batch carries a near-identical record.
    ingest_jsonl(storage, config, clock, _payload([
        _record("D1", "LANTERN", {"title": "Short", "authors": ["Vaelis Zorvath"],
                                  "year": 2020}),
    ]), batch_id="D")
    assert list_cross_batch_refs(storage, "D") == []


def test_same_fingerprint_different_people_is_rejected(storage, config, clock):
    cfg = load_config()
    # John Smith and Jane Smith share family + initial, so the structural
    # fingerprint collides; the given-name guard must refuse the reference.
    left = cfg.normalization
    from citefuse.normalize import Normalizer
    normalizer = Normalizer(left)
    from citefuse.models import RawRecord
    norm_john = normalizer.normalize(RawRecord.model_validate({
        "record_id": "J", "source": "ORBIT", "title": ENTITY_A_BASE["title"],
        "authors": ["John Smith"], "year": 2020,
    }))
    norm_jane = normalizer.normalize(RawRecord.model_validate({
        "record_id": "K", "source": "LANTERN", "title": ENTITY_A_BASE["title"],
        "authors": ["Jane Smith"], "year": 2020,
    }))
    fp_john, _, _ = build_fingerprint(norm_john)
    fp_jane, _, _ = build_fingerprint(norm_jane)
    assert fp_john == fp_jane  # the collision the guard exists for
    accepted, guard = collision_guard(norm_john, norm_jane)
    assert accepted is False
    assert guard["author_conflicts"][0]["author_key"] == "smith|j"

    payload = _payload([
        _record("P1", "ORBIT", {"title": ENTITY_A_BASE["title"],
                                "authors": ["John Smith"], "year": 2020}),
    ])
    ingest_jsonl(storage, config, clock, payload, batch_id="P")
    payload2 = _payload([
        _record("Q1", "LANTERN", {"title": ENTITY_A_BASE["title"],
                                  "authors": ["Jane Smith"], "year": 2020}),
    ])
    ingest_jsonl(storage, config, clock, payload2, batch_id="Q")
    assert list_cross_batch_refs(storage, "Q") == []
    collisions = storage.query(
        "SELECT * FROM fingerprint_collisions WHERE fingerprint=?", (fp_john,)
    )
    assert len(collisions) == 1
    events = storage.query(
        "SELECT * FROM audit_events WHERE event_type='fingerprint_collision'"
    )
    assert len(events) == 1
    # Re-running sync must not duplicate the collision record or audit event.
    _run(storage, config, clock, "Q")
    assert len(storage.query("SELECT * FROM fingerprint_collisions")) == 1
    assert len(storage.query(
        "SELECT * FROM audit_events WHERE event_type='fingerprint_collision'"
    )) == 1


def test_initial_forms_remain_compatible(storage, config, clock):
    cfg = load_config()
    from citefuse.models import RawRecord
    from citefuse.normalize import Normalizer
    normalizer = Normalizer(cfg.normalization)
    full = normalizer.normalize(RawRecord.model_validate({
        "record_id": "F", "source": "ORBIT", "title": ENTITY_A_BASE["title"],
        "authors": ["Vaelis Zorvath"], "year": 2020,
    }))
    initial = normalizer.normalize(RawRecord.model_validate({
        "record_id": "G", "source": "LANTERN", "title": ENTITY_A_BASE["title"],
        "authors": ["V. Zorvath"], "year": 2020,
    }))
    assert build_fingerprint(full)[0] == build_fingerprint(initial)[0]
    assert collision_guard(full, initial)[0] is True


def test_concurrent_ingest_of_identical_batch_is_idempotent(tmp_path, config, clock):
    db_path = tmp_path / "concurrent.sqlite3"
    payload = _payload([
        _record("K1", "ORBIT", ENTITY_A_BASE),
        _record("K2", "LANTERN", ENTITY_B),
    ])
    barrier = threading.Barrier(4)
    errors: list[BaseException] = []

    def worker():
        instance = Storage(db_path)
        try:
            barrier.wait()
            ingest_jsonl(instance, config, clock, payload, batch_id="RACE")
        except BaseException as exc:  # pragma: no cover - failure path
            errors.append(exc)
        finally:
            instance.close()

    threads = [threading.Thread(target=worker) for _ in range(3)]
    for thread in threads:
        thread.start()
    barrier.wait()
    for thread in threads:
        thread.join()
    assert errors == []
    storage = Storage(db_path)
    try:
        assert storage.count_records("RACE") == 2
        index_rows = storage.query("SELECT * FROM record_fingerprints")
        assert len(index_rows) == 2
        assert len({row["record_pk"] for row in index_rows}) == 2
        assert storage.query("SELECT COUNT(*) AS n FROM cross_batch_refs")[0]["n"] == 0
        assert storage.get_batch("RACE")["valid_lines"] == 2
    finally:
        storage.close()


LEGACY_SCHEMA = """
CREATE TABLE batches (
  batch_id TEXT PRIMARY KEY, name TEXT, input_hash TEXT NOT NULL,
  created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
  total_lines INTEGER NOT NULL DEFAULT 0, valid_lines INTEGER NOT NULL DEFAULT 0,
  bad_lines INTEGER NOT NULL DEFAULT 0, status TEXT NOT NULL DEFAULT 'ingested'
);
CREATE TABLE records (
  record_pk INTEGER PRIMARY KEY AUTOINCREMENT,
  batch_id TEXT NOT NULL REFERENCES batches(batch_id) ON DELETE CASCADE,
  source TEXT NOT NULL, record_id TEXT NOT NULL, line_no INTEGER NOT NULL,
  raw_json TEXT NOT NULL, norm_json TEXT NOT NULL,
  title_norm TEXT NOT NULL, authors_norm TEXT NOT NULL,
  venue_norm TEXT, year_norm INTEGER, volume_norm TEXT, issue_norm TEXT,
  pages_norm TEXT, doi_norm TEXT, gold_cluster TEXT,
  UNIQUE(batch_id, source, record_id)
);
CREATE TABLE dead_letters (
  dead_id INTEGER PRIMARY KEY AUTOINCREMENT,
  batch_id TEXT NOT NULL REFERENCES batches(batch_id) ON DELETE CASCADE,
  line_no INTEGER NOT NULL, reason TEXT NOT NULL, raw_text TEXT NOT NULL,
  UNIQUE(batch_id, line_no, reason, raw_text)
);
CREATE TABLE runs (
  run_id TEXT PRIMARY KEY,
  batch_id TEXT NOT NULL REFERENCES batches(batch_id) ON DELETE CASCADE,
  config_hash TEXT NOT NULL, input_rows INTEGER NOT NULL,
  bad_rows INTEGER NOT NULL, candidate_pairs INTEGER NOT NULL,
  matched_edges INTEGER NOT NULL, cluster_count INTEGER NOT NULL,
  golden_count INTEGER NOT NULL, timings_json TEXT NOT NULL,
  blocking_stats_json TEXT NOT NULL, status TEXT NOT NULL,
  created_at TEXT NOT NULL
);
CREATE TABLE run_attempts (
  attempt_id INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id TEXT NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
  batch_id TEXT NOT NULL, config_hash TEXT NOT NULL,
  input_rows INTEGER NOT NULL, bad_rows INTEGER NOT NULL,
  candidate_pairs INTEGER NOT NULL, matched_edges INTEGER NOT NULL,
  cluster_count INTEGER NOT NULL, golden_count INTEGER NOT NULL,
  timings_json TEXT NOT NULL, status TEXT NOT NULL,
  started_at TEXT NOT NULL, finished_at TEXT NOT NULL
);
CREATE TABLE pairs (
  run_id TEXT NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
  pair_key TEXT NOT NULL, rec_a_pk INTEGER NOT NULL, rec_b_pk INTEGER NOT NULL,
  score REAL NOT NULL, zone TEXT NOT NULL, strategies_json TEXT NOT NULL,
  details_json TEXT NOT NULL, PRIMARY KEY(run_id, pair_key)
);
CREATE TABLE edges (
  run_id TEXT NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
  pair_key TEXT NOT NULL, rec_a_pk INTEGER NOT NULL, rec_b_pk INTEGER NOT NULL,
  score REAL NOT NULL, kind TEXT NOT NULL, details_json TEXT NOT NULL,
  PRIMARY KEY(run_id, pair_key)
);
CREATE TABLE clusters (
  run_id TEXT NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
  cluster_id TEXT NOT NULL, batch_id TEXT NOT NULL,
  member_keys_json TEXT NOT NULL, member_pks_json TEXT NOT NULL,
  golden_json TEXT NOT NULL, PRIMARY KEY(run_id, cluster_id)
);
CREATE TABLE decisions (
  decision_id INTEGER PRIMARY KEY AUTOINCREMENT,
  batch_id TEXT NOT NULL REFERENCES batches(batch_id) ON DELETE CASCADE,
  pair_key TEXT NOT NULL, rec_a_key TEXT NOT NULL, rec_b_key TEXT NOT NULL,
  decision TEXT NOT NULL, reviewer TEXT NOT NULL, note TEXT NOT NULL,
  submitted_at TEXT NOT NULL
);
CREATE TABLE audit_events (
  event_id INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id TEXT, batch_id TEXT NOT NULL REFERENCES batches(batch_id) ON DELETE CASCADE,
  event_type TEXT NOT NULL, detail_json TEXT NOT NULL, created_at TEXT NOT NULL
);
"""


def _write_legacy_db(path: Path, config, clock):
    """Create a pre-fingerprint database with one normalized legacy record."""
    from citefuse.models import RawRecord
    from citefuse.normalize import Normalizer
    normalizer = Normalizer(config.normalization)
    norm = normalizer.normalize(RawRecord.model_validate({
        "record_id": "OLD1", "source": "ORBIT", "title": ENTITY_A_BASE["title"],
        "authors": ENTITY_A_BASE["authors"], "year": 2020,
    }))
    conn = sqlite3.connect(str(path))
    conn.executescript(LEGACY_SCHEMA)
    conn.execute(
        """INSERT INTO batches(batch_id,name,input_hash,created_at,updated_at,
             total_lines,valid_lines,bad_lines,status) VALUES(?,?,?,?,?,?,?,?,?)""",
        ("LEGACY", "Old", "legacy-hash", "2024-01-01T00:00:00Z",
         "2024-01-01T00:00:00Z", 1, 1, 0, "ingested"),
    )
    conn.execute(
        """INSERT INTO records(batch_id,source,record_id,line_no,raw_json,norm_json,
             title_norm,authors_norm,venue_norm,year_norm,volume_norm,issue_norm,
             pages_norm,doi_norm,gold_cluster)
           VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        ("LEGACY", "ORBIT", "OLD1", 1, json.dumps({"record_id": "OLD1"}),
         json.dumps(norm), norm["title"], json.dumps(norm["authors"]), None, 2020,
         None, None, None, None, "L1"),
    )
    conn.commit()
    conn.close()


def test_legacy_database_starts_schema_less_and_backfills(tmp_path, config, clock):
    data_dir = tmp_path / "legacy-data"
    data_dir.mkdir()
    _write_legacy_db(data_dir / "citefuse.sqlite3", config, clock)

    app = create_app(data_dir=data_dir, clock=clock)
    with TestClient(app) as client:
        # Startup and listing must not error; old records yield empty refs.
        assert client.get("/health").status_code == 200
        batches = client.get("/batches").json()
        assert [item["batch_id"] for item in batches] == ["LEGACY"]
        assert batches[0]["cross_batch_refs"] == []
        assert batches[0]["fingerprint_summary"]["pending"] == 1
        detail = client.get("/batches/LEGACY").json()
        assert detail["cross_batch_refs"] == []
        # /runs before any run exists is also safe.
        assert client.get("/runs", params={"batch_id": "LEGACY"}).json() == []

        # Backfill on demand, then the old record participates in matching.
        backfill = client.post("/fingerprints/backfill", params={
            "batch_id": "LEGACY"
        }).json()
        assert backfill["newly_indexed"] == 1
        fps = client.get("/fingerprints", params={"batch_id": "LEGACY"}).json()
        assert fps["fingerprints"][0]["status"] == "indexed"

        new_batch = _payload([
            _record("N1", "LANTERN", {
                "title": ENTITY_A_BASE["title"].upper(),
                "authors": ["Zorvath, Vaelis", "Quellmar, Tirune"],
                "year": "2020",
            }),
        ])
        created = client.post(
            "/batches?batch_id=NEW", content=new_batch,
            headers={"content-type": "application/x-ndjson"},
        )
        assert created.status_code == 201
        assert created.json()["cross_batch_refs_added"] == 1

        new_detail = client.get("/batches/NEW").json()
        refs = new_detail["cross_batch_refs"]
        assert [(ref["matched_batch_id"], ref["matched_record_key"]) for ref in refs] == [
            ("LEGACY", "ORBIT::OLD1")
        ]
        legacy_detail = client.get("/batches/LEGACY").json()
        legacy_ref = legacy_detail["cross_batch_refs"][0]
        assert legacy_ref["matched_batch_id"] == "NEW"
        assert legacy_ref["record_key"] == "ORBIT::OLD1"
        assert legacy_ref["matched_record_key"] == "LANTERN::N1"
        assert legacy_ref["direction"] == "incoming"

        # A legacy record participates in a run without altering its index.
        run = client.post("/runs", json={"batch_id": "NEW"}).json()
        assert run["cluster_count"] >= 1
        run_detail = client.get(f"/runs/{run['run_id']}").json()
        assert run_detail["fingerprint_summary"]["pending"] == 0
        assert run_detail["cross_batch_refs"]


def test_legacy_database_ingest_triggers_lazy_backfill(tmp_path, config, clock):
    data_dir = tmp_path / "legacy-lazy"
    data_dir.mkdir()
    _write_legacy_db(data_dir / "citefuse.sqlite3", config, clock)
    app = create_app(data_dir=data_dir, clock=clock)
    with TestClient(app) as client:
        new_batch = _payload([_record("N1", "COMPASS", ENTITY_A_BASE)])
        created = client.post(
            "/batches?batch_id=LAZY", content=new_batch,
            headers={"content-type": "application/x-ndjson"},
        )
        assert created.status_code == 201
        # Ingestion lazily indexed the legacy record, so the ref is created
        # without an explicit backfill call.
        assert created.json()["cross_batch_refs_added"] == 1
        refs = client.get("/cross-batch-refs", params={
            "batch_id": "LAZY"
        }).json()["cross_batch_refs"]
        assert {ref["matched_record_key"] for ref in refs} == {"ORBIT::OLD1"}


def test_malformed_legacy_row_does_not_abort_backfill(tmp_path, config, clock):
    data_dir = tmp_path / "legacy-broken"
    data_dir.mkdir()
    db_path = data_dir / "citefuse.sqlite3"
    _write_legacy_db(db_path, config, clock)
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        """INSERT INTO records(batch_id,source,record_id,line_no,raw_json,norm_json,
             title_norm,authors_norm,venue_norm,year_norm,volume_norm,issue_norm,
             pages_norm,doi_norm,gold_cluster)
           VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        ("LEGACY", "LANTERN", "BAD", 2, "{}", "not-json", "x", "[]", None, 2020,
         None, None, None, None, None),
    )
    conn.commit()
    conn.close()
    app = create_app(data_dir=data_dir, clock=clock)
    with TestClient(app) as client:
        result = client.post("/fingerprints/backfill").json()
        assert result["newly_indexed"] == 1
        assert result["errors"] == 1
        assert result["records_scanned"] == 2
        # The healthy legacy record is usable; the broken one simply stays
        # pending and is audited, and listing never errors.
        assert client.get("/batches/LEGACY").status_code == 200
        events = client.get("/runs", params={"batch_id": "LEGACY"}).json()
        assert events == []
        probe = sqlite3.connect(str(db_path))
        try:
            error_events = probe.execute(
                "SELECT COUNT(*) FROM audit_events WHERE event_type='fingerprint_error'"
            ).fetchone()[0]
        finally:
            probe.close()
        assert error_events == 1


def test_fingerprint_failure_never_blocks_ingestion(storage, config, clock, monkeypatch):
    import citefuse.ingest as ingest_module

    def boom(*_args, **_kwargs):
        raise RuntimeError("index unavailable")

    monkeypatch.setattr(ingest_module, "ensure_fingerprint", boom)
    payload = _payload([_record("Z1", "ORBIT", ENTITY_A_BASE)])
    result = ingest_jsonl(storage, config, clock, payload, batch_id="ERR")
    assert result["inserted"] == 1
    assert storage.count_records("ERR") == 1
    # The failure is audited and ingestion still succeeds. The same-call lazy
    # backfill has already re-indexed the record through the healthy path, so a
    # transient failure costs nothing beyond the audit event.
    events = storage.query(
        "SELECT * FROM audit_events WHERE batch_id='ERR'"
        " AND event_type='fingerprint_error'"
    )
    assert len(events) == 1
    assert "index unavailable" in json.loads(events[0]["detail_json"])["error"]
    assert [row["status"] for row in list_fingerprints(storage, "ERR")] == ["indexed"]
    from citefuse.fingerprint import fingerprint_summary
    assert fingerprint_summary(storage, "ERR")["pending"] == 0
    run = _run(storage, config, clock, "ERR")
    assert run["cluster_count"] == 1


def test_batches_and_runs_expose_existing_compatible_fields(client, tiny_payload):
    created = client.post(
        "/batches?batch_id=COMPAT", content=tiny_payload,
        headers={"content-type": "application/x-ndjson"},
    ).json()
    for key in ("batch_id", "name", "input_hash", "created_at", "updated_at",
                "total_lines", "valid_lines", "bad_lines", "status", "created",
                "inserted", "duplicates", "dead_added"):
        assert key in created
    batch = client.get("/batches/COMPAT").json()
    assert batch["total_lines"] == batch["valid_lines"] == 6
    assert "latest_run_id" in batch
    assert "fingerprint_summary" in batch and "cross_batch_refs" in batch
    run = client.post("/runs", json={"batch_id": "COMPAT"}).json()
    listed = client.get("/runs", params={"batch_id": "COMPAT"}).json()[0]
    assert listed["run_id"] == run["run_id"]
    assert "cross_batch_refs" in listed and "fingerprint_summary" in listed
    for key in ("fingerprint_summary", "cross_batch_refs", "timings",
                "blocking_stats", "attempt_count"):
        assert key in listed

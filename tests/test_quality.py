"""Source-level quality profile: hand-computed numbers, idempotency, cumulative."""
from __future__ import annotations

import json

import pytest

from citefuse.ingest import ingest_jsonl
from citefuse.pipeline import run_pipeline
from citefuse.quality import (
    compute_source_quality,
    cumulative_quality_report,
    run_quality_report,
)
from citefuse.review import submit_decision
from citefuse.storage import Storage


def _record(record_id, source, title, authors, venue, year, pages=None, doi=None):
    return {
        "record_id": record_id, "source": source, "title": title,
        "authors": authors, "venue": venue, "year": year, "pages": pages, "doi": doi,
    }


# Hand-computed fixture. DN/HR/ME are the three sources exercised below.
#   DN: A1 plus two malformed dead letters; joins the A1/A2 match and nothing
#       else. HR: A2 and B1 (two valid lines), joins both candidate pairs.
#       ME: B2 plus three singleton records; joins only B1/B2.
#
# Run 1 (no decisions):
#   The review pair is not merged, so B1, B2 and all three ME singletons stand
#   alone; A1/A2 are merged by the model match.
#   DN attempted 3, bad 2 -> 0.666667; records 1, singletons 0 -> 0.0;
#       pairs {A1||A2} -> avg 1.0; decided 0 -> null.
#   HR attempted 2, bad 0 -> 0.0; records 2, singletons 1 (B1) -> 0.5;
#       pairs {A1||A2=1.0, B1||B2=0.777333} -> avg .888667; decided 0 -> null.
#   ME attempted 4, bad 0 -> 0.0; records 4, singletons 4 -> 1.0;
#       pairs {B1||B2} -> avg .777333; decided 0 -> null.
# Run 2 (B1||B2 model non-match -> human same; HR and ME own an endpoint):
#   singletons: DN 0/1, HR 0/2, ME 3/4; overturn 1/1 for HR and ME, DN null.
def _seed(storage, config, clock):
    records = [
        _record("A1", "DN", "Alpha cohort ledger: a fabricated study of invented nutrition",
                ["Zorvath, Vaelis"], "Journal of Fictional Evidence Synthesis",
                2010, "100-110", "10.5555/qs.a"),
        _record("A2", "HR", "Alpha cohort ledger: a fabricated study of invented nutrition",
                ["Vaelis Zorvath"], "JFES", 2010, "100–110", "https://doi.org/10.5555/QS.A"),
        _record("B1", "HR", "Beta vane survey of simulated pediatrics",
                ["Quellmar, Tirune"], "Imaginary Research Methods Quarterly",
                2012, "200-210", "10.5555/qs.b"),
        _record("B2", "ME", "Beta vane survey of simulated pediatrics revised",
                ["Quellmar, Tirune", "Mavrik Orbain"],
                "Imaginary Research Methods Quarterly", 2013, "200-211", None),
        _record("S1", "ME", "Gamma atlas inquiry of fabricated cardiology",
                ["Dunwellyn, Soreth"], "Annals of Invented Public Health", 2001),
        _record("S2", "ME", "Delta prism notes of imaginary rehabilitation",
                ["Silvaris, Liosha"], "Fabricated Clinical Trials Review", 2003),
        _record("S3", "ME", "Epsilon harbor trial of fictional diagnostics",
                ["Orbain, Iryssa"], "Proceedings of the Synthetic Evidence Conference", 2005),
    ]
    lines = [
        json.dumps(record, sort_keys=True, separators=(",", ":")) for record in records
    ]
    # Both malformed lines name their source, so both count toward DN's bad rows.
    lines.append(json.dumps({"record_id": "BAD1", "source": "DN", "title": ""}))
    lines.append(json.dumps({"record_id": "BAD2", "source": "DN"}))
    payload = ("\n".join(lines) + "\n").encode("utf-8")
    ingest_jsonl(storage, config, clock, payload, batch_id="quality")
    return run_pipeline(storage, config, clock, "quality")


def _by_source(report):
    return {row["source"]: row for row in report["sources"]}


def _pairs_by_key(storage, run_id):
    return {row["pair_key"]: row for row in storage.list_pairs(run_id)}


def test_hand_computed_single_run_profile(storage, config, clock):
    run = _seed(storage, config, clock)
    pairs = _pairs_by_key(storage, run["run_id"])
    a_pair = pairs["DN::A1||HR::A2"]
    b_pair = pairs["HR::B1||ME::B2"]
    assert a_pair["zone"] == "match"
    assert b_pair["zone"] == "review"
    scores = {a_pair["pair_key"]: a_pair["score"], b_pair["pair_key"]: b_pair["score"]}
    hr_avg = round(scores["DN::A1||HR::A2"] + scores["HR::B1||ME::B2"], 6)

    report = run_quality_report(storage, config, clock, run["run_id"])
    assert report["scope"] == "run"
    assert [row["source"] for row in report["sources"]] == ["DN", "HR", "ME"]
    rows = _by_source(report)

    assert rows["DN"]["attempted_lines"] == 3
    assert rows["DN"]["bad_lines"] == 2
    assert rows["DN"]["bad_row_rate"] == 0.666667
    assert rows["DN"]["record_count"] == 1
    assert rows["DN"]["singleton_rate"] == 0.0
    assert rows["DN"]["candidate_pairs"] == 1
    assert rows["DN"]["avg_candidate_score"] == scores["DN::A1||HR::A2"]
    assert rows["DN"]["decided_pairs"] == 0
    assert rows["DN"]["overturn_rate"] is None  # no division by zero

    assert rows["HR"]["bad_row_rate"] == 0.0
    assert rows["HR"]["record_count"] == 2
    assert rows["HR"]["singleton_count"] == 1
    assert rows["HR"]["singleton_rate"] == 0.5
    assert rows["HR"]["candidate_pairs"] == 2
    assert rows["HR"]["avg_candidate_score"] == round(hr_avg / 2, 6)
    assert rows["HR"]["overturn_rate"] is None

    assert rows["ME"]["bad_row_rate"] == 0.0
    assert rows["ME"]["record_count"] == 4
    assert rows["ME"]["singleton_count"] == 4
    assert rows["ME"]["singleton_rate"] == 1.0
    assert rows["ME"]["candidate_pairs"] == 1
    assert rows["ME"]["avg_candidate_score"] == scores["HR::B1||ME::B2"]
    assert rows["ME"]["overturn_rate"] is None

    # Bad-row threshold is 0.10: DN at 0.666667 is flagged, HR/ME are not.
    assert rows["DN"]["anomaly_flags"] == ["high_bad_row_rate"]
    assert rows["HR"]["anomalous"] is False
    assert rows["ME"]["anomalous"] is False


def test_recompute_is_idempotent_and_overwrites(storage, config, clock):
    run = _seed(storage, config, clock)
    run_id = run["run_id"]
    first = storage.list_source_quality(run_id)
    assert len(first) == 3

    # Re-running the identical logical run produces the same run_id and must
    # overwrite, not append: still exactly one row per source.
    assert run_pipeline(storage, config, clock, "quality")["run_id"] == run_id
    assert len(storage.list_source_quality(run_id)) == 3

    # Explicit recomputation (e.g. API lazy backfill) is also a replace.
    compute_source_quality(storage, config, clock, run_id)
    again = storage.list_source_quality(run_id)
    assert len(again) == 3
    assert [row["source"] for row in again] == ["DN", "HR", "ME"]
    assert [
        {key: row[key] for key in ("source", "bad_row_rate", "singleton_rate",
                                   "avg_candidate_score", "overturn_rate")}
        for row in first
    ] == [
        {key: row[key] for key in ("source", "bad_row_rate", "singleton_rate",
                                   "avg_candidate_score", "overturn_rate")}
        for row in again
    ]
    rows = storage.query(
        "SELECT COUNT(*) AS n FROM source_quality WHERE run_id=?", (run_id,)
    )
    assert rows[0]["n"] == 3


def test_review_overturn_counts_both_sources_and_flags(storage, config, clock):
    first = _seed(storage, config, clock)
    # Both directions count: model match -> human different, model non-match ->
    # human same. A pair spanning two sources credits both sources.
    submit_decision(
        storage, clock, "quality", "DN::A1||HR::A2", "different", "reviewer", "split"
    )
    submit_decision(
        storage, clock, "quality", "HR::B1||ME::B2", "same", "reviewer", "merge"
    )
    second = run_pipeline(storage, config, clock, "quality")
    assert second["run_id"] != first["run_id"]

    report = run_quality_report(storage, config, clock, second["run_id"])
    rows = _by_source(report)
    # HR owns one endpoint of each ruled pair and was overturned twice.
    assert rows["HR"]["decided_pairs"] == 2
    assert rows["HR"]["overturned_pairs"] == 2
    assert rows["HR"]["overturn_rate"] == 1.0
    # DN owns A1 (ruled) and ME owns B2 (ruled).
    for source in ("DN", "ME"):
        assert rows[source]["decided_pairs"] == 1
        assert rows[source]["overturned_pairs"] == 1
        assert rows[source]["overturn_rate"] == 1.0
    # The merged hum/same pair collapses B1+B2, so HR keeps one singleton.
    assert rows["HR"]["singleton_count"] == 1
    assert rows["HR"]["singleton_rate"] == 0.5
    assert rows["ME"]["singleton_count"] == 3
    assert rows["ME"]["singleton_rate"] == 0.75
    # The "different" ruling split A1 back out on its own.
    assert rows["DN"]["singleton_count"] == 1
    assert rows["DN"]["singleton_rate"] == 1.0
    for source in ("DN", "HR", "ME"):
        assert "high_overturn_rate" in rows[source]["anomaly_flags"]
    assert rows["DN"]["anomaly_flags"] == ["high_bad_row_rate", "high_overturn_rate"]
    assert rows["HR"]["anomaly_flags"] == ["high_overturn_rate"]
    assert rows["ME"]["anomaly_flags"] == ["high_overturn_rate"]


def test_cumulative_excludes_superseded_and_pools_weighted(storage, config, clock):
    first = _seed(storage, config, clock)
    submit_decision(
        storage, clock, "quality", "DN::A1||HR::A2", "different", "reviewer", "split"
    )
    submit_decision(
        storage, clock, "quality", "HR::B1||ME::B2", "same", "reviewer", "merge"
    )
    second = run_pipeline(storage, config, clock, "quality")

    cumulative = cumulative_quality_report(storage, config, clock, "quality")
    assert cumulative["scope"] == "cumulative"
    assert cumulative["runs_included"] == [second["run_id"]]
    assert cumulative["superseded_runs"] == 1
    rows = _by_source(cumulative)
    for source in ("DN", "HR", "ME"):
        run_row = _by_source(
            run_quality_report(storage, config, clock, second["run_id"])
        )[source]
        # One effective run, so cumulative equals the single-run numbers.
        assert rows[source]["overturn_rate"] == run_row["overturn_rate"]
        assert rows[source]["bad_row_rate"] == run_row["bad_row_rate"]

    # A second batch is pooled by numerator/denominator, not by run averages.
    payload = (
        json.dumps(_record("C1", "DN", "Zeta motif note of synthetic epidemiology",
                           ["Calyndor, Nerune"], "Annals of Invented Public Health", 2008))
        + "\n"
        + json.dumps({"record_id": "BAD3", "source": "DN", "title": ""})
        + "\n"
    ).encode("utf-8")
    ingest_jsonl(storage, config, clock, payload, batch_id="quality2")
    run_pipeline(storage, config, clock, "quality2")
    pooled = _by_source(cumulative_quality_report(storage, config, clock))
    dn = pooled["DN"]
    included = cumulative_quality_report(storage, config, clock)
    assert len(included["runs_included"]) == 2
    assert included["superseded_runs"] == 1
    # Batch 1: DN attempted 3 / bad 2. Batch 2: DN attempted 2 / bad 1.
    # Pooled 3/5, not a simple average of 0.666667 and 0.5.
    assert dn["attempted_lines"] == 5
    assert dn["bad_lines"] == 3
    assert dn["bad_row_rate"] == 0.6
    # Batch 1's A1 was split out by the "different" ruling; batch 2's DN record
    # never had a partner. Both are singletons.
    assert dn["record_count"] == 2
    assert dn["singleton_count"] == 2
    assert dn["singleton_rate"] == 1.0
    # One candidate pair from batch 1; none from batch 2.
    assert dn["candidate_pairs"] == 1
    assert dn["avg_candidate_score"] == 1.0
    assert dn["decided_pairs"] == 1
    assert dn["overturned_pairs"] == 1
    assert dn["overturn_rate"] == 1.0
    assert dn["anomaly_flags"] == ["high_bad_row_rate", "high_overturn_rate"]


def test_empty_run_has_no_source_rows_and_no_zero_division(storage, config, clock):
    payload = b'{"record_id":"X","source":"ORBIT","title":"A lone synthetic record"}\n'
    ingest_jsonl(storage, config, clock, payload, batch_id="lonely")
    run = run_pipeline(storage, config, clock, "lonely")
    report = run_quality_report(storage, config, clock, run["run_id"])
    rows = _by_source(report)
    assert rows["ORBIT"]["candidate_pairs"] == 0
    assert rows["ORBIT"]["avg_candidate_score"] is None
    assert rows["ORBIT"]["singleton_rate"] == 1.0
    assert rows["ORBIT"]["bad_row_rate"] == 0.0
    assert rows["ORBIT"]["overturn_rate"] is None
    # No candidate pair and no threshold breach -> not flagged.
    assert rows["ORBIT"]["anomalous"] is False

    # A run with zero records and zero lines contributes no rows at all.
    ingest_jsonl(storage, config, clock, b'{"oops":1}\n', batch_id="allbad")
    empty = run_pipeline(storage, config, clock, "allbad")
    assert run_quality_report(storage, config, clock, empty["run_id"])["sources"] == []


def test_quality_rows_are_deleted_with_batch(storage, config, clock):
    run = _seed(storage, config, clock)
    assert storage.list_source_quality(run["run_id"])
    storage.delete_batch("quality")
    assert storage.list_source_quality(run["run_id"]) == []
    assert storage.get_run(run["run_id"]) is None


def test_historical_run_keeps_its_decision_snapshot(storage, config, clock):
    first = _seed(storage, config, clock)
    submit_decision(
        storage, clock, "quality", "HR::B1||ME::B2", "same", "reviewer", "merge"
    )
    run_pipeline(storage, config, clock, "quality")
    # The first run was profiled with no decisions in effect; a later rerun must
    # not rewrite its stored snapshot.
    rows = _by_source(run_quality_report(storage, config, clock, first["run_id"]))
    for source in ("DN", "HR", "ME"):
        assert rows[source]["decided_pairs"] == 0
        assert rows[source]["overturn_rate"] is None


def test_lazy_backfill_for_legacy_run(storage, config, clock):
    run = _seed(storage, config, clock)
    # Simulate a run persisted before the profile module existed.
    storage.execute("DELETE FROM source_quality WHERE run_id=?", (run["run_id"],))
    assert storage.list_source_quality(run["run_id"]) == []
    report = run_quality_report(storage, config, clock, run["run_id"])
    assert len(report["sources"]) == 3
    assert len(storage.list_source_quality(run["run_id"])) == 3


def test_failed_run_cannot_be_profiled(storage, config, clock, monkeypatch):
    from citefuse.quality import SourceQualityError

    ingest_jsonl(
        storage, config, clock,
        b'{"record_id":"X","source":"ORBIT","title":"A lone synthetic record"}\n',
        batch_id="boom",
    )
    monkeypatch.setattr(
        "citefuse.pipeline.generate_candidates",
        lambda *_: (_ for _ in ()).throw(RuntimeError("synthetic failure")),
    )
    with pytest.raises(RuntimeError):
        run_pipeline(storage, config, clock, "boom")
    failed_id = storage.list_runs("boom")[0]["run_id"]
    with pytest.raises(SourceQualityError):
        compute_source_quality(storage, config, clock, failed_id)


def test_legacy_database_gains_dead_letter_source_column(tmp_path):
    import sqlite3

    from citefuse.storage import Storage

    path = tmp_path / "legacy.sqlite3"
    connection = sqlite3.connect(path)
    connection.executescript(
        """CREATE TABLE batches (
             batch_id TEXT PRIMARY KEY, name TEXT, input_hash TEXT NOT NULL,
             created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
             total_lines INTEGER NOT NULL DEFAULT 0,
             valid_lines INTEGER NOT NULL DEFAULT 0,
             bad_lines INTEGER NOT NULL DEFAULT 0,
             status TEXT NOT NULL DEFAULT 'ingested');
           CREATE TABLE dead_letters (
             dead_id INTEGER PRIMARY KEY AUTOINCREMENT,
             batch_id TEXT NOT NULL, line_no INTEGER NOT NULL,
             reason TEXT NOT NULL, raw_text TEXT NOT NULL,
             UNIQUE(batch_id, line_no, reason, raw_text));"""
    )
    connection.commit()
    connection.close()
    migrated = Storage(path)
    columns = {row["name"] for row in migrated.query("PRAGMA table_info(dead_letters)")}
    assert "source" in columns
    migrated.execute(
        "INSERT INTO batches(batch_id,input_hash,created_at,updated_at,status)"
        " VALUES('old','h','t','t','ingested')"
    )
    migrated.insert_dead_letter("old", 1, "json", "broken", None)
    assert migrated.list_dead_letters("old")[0]["source"] is None
    migrated.close()


def test_dead_letter_source_attribution(storage, config, clock):
    payload = (
        b'{"record_id":"OK","source":"ORBIT","title":"A valid record"}\n'
        b'{"record_id":"BAD","source":"orbit","title":""}\n'
        b'not json at all\n'
    )
    ingest_jsonl(storage, config, clock, payload, batch_id="dead")
    dead = storage.list_dead_letters("dead", limit=None)
    assert [row["source"] for row in dead] == ["ORBIT", None]


def test_source_with_only_dead_letters_has_null_record_ratios(storage, config, clock):
    payload = (
        b'{"record_id":"BAD1","source":"GHOST","title":""}\n'
        b'{"record_id":"BAD2","source":"GHOST","title":""}\n'
        b'{"record_id":"OK","source":"ORBIT","title":"A valid record"}\n'
    )
    ingest_jsonl(storage, config, clock, payload, batch_id="ghost")
    run = run_pipeline(storage, config, clock, "ghost")
    rows = _by_source(run_quality_report(storage, config, clock, run["run_id"]))
    # GHOST appears only as an attributed bad-line source: it was attempted twice
    # and produced no clustered records, so record-based rates are undefined.
    assert rows["GHOST"]["attempted_lines"] == 2
    assert rows["GHOST"]["bad_lines"] == 2
    assert rows["GHOST"]["bad_row_rate"] == 1.0
    assert rows["GHOST"]["record_count"] == 0
    assert rows["GHOST"]["singleton_rate"] is None
    assert rows["GHOST"]["avg_candidate_score"] is None
    assert rows["GHOST"]["overturn_rate"] is None
    assert rows["GHOST"]["anomaly_flags"] == ["high_bad_row_rate"]

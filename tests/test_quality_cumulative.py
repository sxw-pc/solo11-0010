"""Weighting and supersession edge cases for the cumulative source profile."""
from __future__ import annotations

import json

from citefuse.ingest import ingest_jsonl
from citefuse.pipeline import run_pipeline
from citefuse.quality import cumulative_quality_report
from citefuse.review import submit_decision


def _line(payload_object):
    return json.dumps(payload_object, sort_keys=True, separators=(",", ":")) + "\n"


def _record(record_id, source, title, authors=None, venue=None, year=None):
    return {
        "record_id": record_id, "source": source, "title": title,
        "authors": authors or ["Zorvath, Vaelis"], "venue": venue,
        "year": year,
    }


def test_cumulative_pools_unequally_sized_batches(storage, config, clock):
    # Batch X: one ME record, one bad ME line -> ME rate 1/2.
    small = (
        _line(_record("X1", "ME", "Alpha cohort ledger: fabricated invented nutrition",
                      ["Zorvath, Vaelis"], "Journal of Fictional Evidence Synthesis", 2010))
        + _line({"record_id": "XBAD", "source": "ME", "title": ""})
    ).encode()
    ingest_jsonl(storage, config, clock, small, batch_id="small")
    run_pipeline(storage, config, clock, "small")

    # Batch Y: nine ME records, one bad ME line -> 1/10. A simple average of
    # the two run rates would be 0.30; pooling gives 2/12 = 0.166667.
    lines = []
    labs = ["ORBIT", "LANTERN", "MEADOW", "COMPASS"]
    for index in range(9):
        lines.append(_record(
            f"Y{index}", "ME",
            f"Topic {index} study of fabricated cardiology",
            ["Calyndor, Nerune"],
            f"Invented Venue {index % 5}",
            2000 + index,
        ))
    lines.append({"record_id": "YBAD", "source": "ME", "title": ""})
    ingest_jsonl(
        storage, config, clock, "".join(_line(item) for item in lines).encode(),
        batch_id="large",
    )
    run_pipeline(storage, config, clock, "large")

    report = cumulative_quality_report(storage, config, clock)
    assert len(report["runs_included"]) == 2
    assert report["superseded_runs"] == 0
    me = next(row for row in report["sources"] if row["source"] == "ME")
    assert me["attempted_lines"] == 12
    assert me["bad_lines"] == 2
    assert me["bad_row_rate"] == round(2 / 12, 6)
    assert me["bad_row_rate"] != round((1 / 2 + 1 / 10) / 2, 6)


def test_repeated_decision_on_same_pair_is_counted_once(storage, config, clock):
    payload = (
        _line(_record("P1", "DN", "Beta vane survey of simulated pediatrics",
                      ["Quellmar, Tirune"], "Imaginary Research Methods Quarterly", 2012))
        + _line(_record("P2", "DN", "Beta vane survey of simulated pediatrics revised",
                        ["Quellmar, Tirune"], "Imaginary Research Methods Quarterly", 2013))
    ).encode()
    ingest_jsonl(storage, config, clock, payload, batch_id="dec")
    run_pipeline(storage, config, clock, "dec")
    pair_key = "DN::P1||DN::P2"
    for decision in ("same", "different", "same"):
        submit_decision(storage, clock, "dec", pair_key, decision, "rev", "note")
    current = run_pipeline(storage, config, clock, "dec")
    latest = [
        row for row in storage.decision_history("dec", pair_key)
        if row["submitted_at"] == "2025-01-02T03:04:05Z"
    ]
    assert len(latest) == 3  # history is append-only, but the profile uses the latest
    report = cumulative_quality_report(storage, config, clock, "dec")
    dn = next(row for row in report["sources"] if row["source"] == "DN")
    assert dn["decided_pairs"] == 1
    assert dn["overturned_pairs"] == 0  # latest is "same" and the model matched
    assert dn["overturn_rate"] == 0.0

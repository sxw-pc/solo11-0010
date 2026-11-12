"""HTTP contract for the source-level quality profile endpoints."""
from __future__ import annotations

import json


def _record(record_id, source, title, authors, venue, year, pages=None, doi=None):
    return {
        "record_id": record_id, "source": source, "title": title,
        "authors": authors, "venue": venue, "year": year, "pages": pages, "doi": doi,
    }


def _post(client, batch_id):
    records = [
        _record("A1", "ORBIT", "Alpha cohort ledger: a fabricated study of invented nutrition",
                ["Zorvath, Vaelis"], "Journal of Fictional Evidence Synthesis",
                2010, "100-110", "10.5555/qs.a"),
        _record("A2", "LANTERN", "Alpha cohort ledger: a fabricated study of invented nutrition",
                ["Vaelis Zorvath"], "JFES", 2010, "100–110", "https://doi.org/10.5555/QS.A"),
        _record("B1", "LANTERN", "Beta vane survey of simulated pediatrics",
                ["Quellmar, Tirune"], "Imaginary Research Methods Quarterly",
                2012, "200-210", "10.5555/qs.b"),
        _record("B2", "MEADOW", "Beta vane survey of simulated pediatrics revised",
                ["Quellmar, Tirune", "Mavrik Orbain"],
                "Imaginary Research Methods Quarterly", 2013, "200-211", None),
        _record("S1", "MEADOW", "Gamma atlas inquiry of fabricated cardiology",
                ["Dunwellyn, Soreth"], "Annals of Invented Public Health", 2001),
    ]
    lines = [
        json.dumps(record, sort_keys=True, separators=(",", ":")) for record in records
    ]
    lines.append(json.dumps({"record_id": "BAD1", "source": "LANTERN", "title": ""}))
    response = client.post(
        f"/batches?batch_id={batch_id}", content=("\n".join(lines) + "\n").encode(),
        headers={"content-type": "application/x-ndjson"},
    )
    assert response.status_code == 201
    run = client.post("/runs", json={"batch_id": batch_id}).json()
    return run["run_id"]


def test_quality_run_and_cumulative_scopes(client):
    run_id = _post(client, "QS")

    single = client.get("/quality/sources", params={"run_id": run_id})
    assert single.status_code == 200
    payload = single.json()
    assert payload["scope"] == "run"
    assert payload["run_id"] == run_id
    assert payload["batch_id"] == "QS"
    assert [row["source"] for row in payload["sources"]] == ["LANTERN", "MEADOW", "ORBIT"]
    rows = {row["source"]: row for row in payload["sources"]}
    assert rows["LANTERN"]["bad_row_rate"] == 0.333333
    assert rows["LANTERN"]["anomaly_flags"] == ["high_bad_row_rate"]
    # The review-zone B1||B2 pair is not merged in this run, so both MEADOW
    # records are singletons.
    assert rows["MEADOW"]["singleton_rate"] == 1.0
    assert rows["ORBIT"]["anomalous"] is False

    # batch_id resolves the latest run; unknown ids are 404.
    derived = client.get("/quality/sources", params={"batch_id": "QS"}).json()
    assert derived["run_id"] == run_id
    assert client.get("/quality/sources", params={"run_id": "missing"}).status_code == 404
    assert client.get("/quality/sources", params={"batch_id": "missing"}).status_code == 404

    cumulative = client.get(
        "/quality/sources", params={"batch_id": "QS", "scope": "cumulative"}
    ).json()
    assert cumulative["scope"] == "cumulative"
    assert cumulative["runs_included"] == [run_id]
    assert cumulative["superseded_runs"] == 0
    assert [row["source"] for row in cumulative["sources"]] == ["LANTERN", "MEADOW", "ORBIT"]

    bad_scope = client.get("/quality/sources", params={"scope": "everything"})
    assert bad_scope.status_code == 400
    assert client.get(
        "/quality/sources", params={"scope": "cumulative", "run_id": run_id}
    ).status_code == 400


def test_quality_after_review_rerun_is_idempotent(client):
    run_id = _post(client, "QS")
    pair_key = "LANTERN::B1||MEADOW::B2"
    before = client.get("/quality/sources", params={"run_id": run_id}).json()
    response = client.post("/review/decisions", json={
        "batch_id": "QS", "pair_key": pair_key, "decision": "same",
        "reviewer": "qa", "note": "merge trial",
    })
    assert response.status_code == 201
    new_run_id = response.json()["run"]["run_id"]
    assert new_run_id != run_id

    report = client.get("/quality/sources", params={"run_id": new_run_id}).json()
    rows = {row["source"]: row for row in report["sources"]}
    # LANTERN and MEADOW each own one endpoint of the overturned pair.
    assert rows["LANTERN"]["overturn_rate"] == 1.0
    assert rows["MEADOW"]["overturn_rate"] == 1.0
    assert "high_overturn_rate" in rows["LANTERN"]["anomaly_flags"]
    assert "high_overturn_rate" in rows["MEADOW"]["anomaly_flags"]
    assert rows["ORBIT"]["overturn_rate"] is None

    # Re-triggering the profile (a second identical run) must not add rows.
    again = client.post("/runs", json={"batch_id": "QS"}).json()
    assert again["run_id"] == new_run_id
    second_read = client.get("/quality/sources", params={"run_id": new_run_id}).json()
    assert len(second_read["sources"]) == len(report["sources"])
    assert second_read["sources"] == report["sources"]

    cumulative = client.get(
        "/quality/sources", params={"batch_id": "QS", "scope": "cumulative"}
    ).json()
    assert cumulative["runs_included"] == [new_run_id]
    assert cumulative["superseded_runs"] == 1
    cum_rows = {row["source"]: row for row in cumulative["sources"]}
    assert cum_rows["LANTERN"]["overturn_rate"] == 1.0
    assert cum_rows["LANTERN"]["bad_row_rate"] == 0.333333

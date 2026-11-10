from __future__ import annotations

import json

from citefuse.audit import run_events
from citefuse.ingest import ingest_jsonl
from citefuse.pipeline import run_pipeline
from citefuse.review import next_review_pairs, submit_decision
from scripts.seed_fixtures import generate


def _payload():
    records, _ = generate(20240517)
    return (
        "\n".join(json.dumps(record, sort_keys=True, separators=(",", ":")) for record in records)
        + "\n"
    ).encode("utf-8")


def _membership(storage, run_id):
    result = {}
    for cluster in storage.list_clusters(run_id):
        for key in json.loads(cluster["member_keys_json"]):
            result[key] = cluster["cluster_id"]
    return result


def _gold_by_key(storage, batch_id):
    return {
        f"{row['source']}::{row['record_id']}": row["gold_cluster"]
        for row in storage.list_records(batch_id)
    }


def test_review_decision_rerun_split_merge_and_audit(storage, config, clock):
    ingest_jsonl(storage, config, clock, _payload(), batch_id="review")
    first = run_pipeline(storage, config, clock, "review")
    queue = next_review_pairs(storage, "review", 100)
    assert queue
    gold = _gold_by_key(storage, "review")
    membership = _membership(storage, first["run_id"])
    true_pair = next(
        pair for pair in queue
        if gold[pair["records"][0]["record_key"]] == gold[pair["records"][1]["record_key"]]
        and membership[pair["records"][0]["record_key"]]
        == membership[pair["records"][1]["record_key"]]
    )
    keys = [record["record_key"] for record in true_pair["records"]]

    decision = submit_decision(
        storage, clock, "review", true_pair["pair_key"], "different",
        "reviewer-a", "distinct trial",
    )
    assert decision["decision"] == "different"
    after_split = run_pipeline(storage, config, clock, "review")
    split_map = _membership(storage, after_split["run_id"])
    assert split_map[keys[0]] != split_map[keys[1]]
    assert all(
        pair["pair_key"] != true_pair["pair_key"]
        for pair in next_review_pairs(storage, "review", 100)
    )

    submit_decision(
        storage, clock, "review", true_pair["pair_key"], "same",
        "reviewer-b", "re-reviewed and merged",
    )
    after_merge = run_pipeline(storage, config, clock, "review")
    merge_map = _membership(storage, after_merge["run_id"])
    assert merge_map[keys[0]] == merge_map[keys[1]]
    events = run_events(storage, after_merge["run_id"])
    assert any(event["event_type"] == "review_conflict" for event in events)

    false_pair = next(
        pair for pair in next_review_pairs(storage, "review", 100)
        if gold[pair["records"][0]["record_key"]] != gold[pair["records"][1]["record_key"]]
        and _membership(storage, after_merge["run_id"])[pair["records"][0]["record_key"]]
        != _membership(storage, after_merge["run_id"])[pair["records"][1]["record_key"]]
    )
    before_count = len(storage.list_clusters(after_merge["run_id"]))
    submit_decision(
        storage, clock, "review", false_pair["pair_key"], "same", "reviewer-c", "merge"
    )
    after_false_merge = run_pipeline(storage, config, clock, "review")
    false_keys = [record["record_key"] for record in false_pair["records"]]
    merged_map = _membership(storage, after_false_merge["run_id"])
    assert merged_map[false_keys[0]] == merged_map[false_keys[1]]
    assert len(storage.list_clusters(after_false_merge["run_id"])) == before_count - 1

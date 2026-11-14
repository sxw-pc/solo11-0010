from __future__ import annotations

import json
import threading
from pathlib import Path

import pytest

from citefuse.ingest import IngestError, derive_batch_id, ingest_jsonl


def test_batch_validation_dead_letters_and_duplicates(storage, config, clock, tiny_payload):
    rows = [json.loads(line) for line in tiny_payload.decode("utf-8").splitlines()]
    duplicate = dict(rows[0])
    lines = [
        json.dumps(rows[0]),
        json.dumps(rows[1]),
        "{not valid json",
        json.dumps({"record_id": "X", "source": "ORBIT", "title": ""}),
        json.dumps(duplicate),
    ]
    payload = "\n".join(lines).encode("utf-8")
    result = ingest_jsonl(storage, config, clock, payload, batch_id="b-1", name="Test")
    assert result["created"] is True
    assert result["inserted"] == 2
    assert result["duplicates"] == 1
    assert result["dead_added"] == 2
    batch = storage.get_batch("b-1")
    # The duplicate line validated fine, so it counts as valid; every
    # non-blank line is either valid or bad.
    assert (batch["total_lines"], batch["valid_lines"], batch["bad_lines"]) == (5, 3, 2)
    assert batch["valid_lines"] + batch["bad_lines"] == batch["total_lines"]
    dead = storage.list_dead_letters("b-1")
    assert [row["line_no"] for row in dead] == [3, 4]
    assert "json" in dead[0]["reason"]
    assert "title" in dead[1]["reason"]

    records = [row for row in storage.list_records("b-1") if row["source"] == "ORBIT"]
    assert records[0]["record_id"] == "T001"
    assert records[0]["raw_json"]
    normalized = json.loads(records[0]["norm_json"])
    assert normalized["venue"] == "journal of fictional evidence synthesis"
    assert normalized["doi"] == "10.5555/tiny.001"


def test_reingesting_identical_payload_is_noop(storage, config, clock, tiny_payload):
    first = ingest_jsonl(storage, config, clock, tiny_payload, batch_id="same")
    second = ingest_jsonl(storage, config, clock, tiny_payload, batch_id="same")
    assert second["created"] is False
    assert second["inserted"] == 0
    assert second["valid_lines"] == first["valid_lines"]
    assert storage.count_records("same") == 6


def test_all_bad_lines_still_create_batch(storage, config, clock):
    result = ingest_jsonl(
        storage, config, clock, b'{"oops":1}\nnot-json\n', batch_id="bad-only"
    )
    assert result["valid_lines"] == 0
    assert result["bad_lines"] == 2
    assert storage.get_batch("bad-only") is not None


def test_batch_id_errors_and_empty_payload(storage, config, clock):
    with pytest.raises(IngestError):
        ingest_jsonl(storage, config, clock, b"", batch_id="x")
    with pytest.raises(IngestError):
        ingest_jsonl(storage, config, clock, b'{"source":"S"}', batch_id="bad id!")


def test_content_derived_batch_id_is_stable(storage, config, clock, tiny_payload):
    expected = derive_batch_id(tiny_payload)
    result = ingest_jsonl(storage, config, clock, tiny_payload)
    assert result["batch_id"] == expected


def test_same_content_different_batch_id_returns_existing_batch(
    storage, config, clock, tiny_payload
):
    first = ingest_jsonl(storage, config, clock, tiny_payload, batch_id="one")
    second = ingest_jsonl(storage, config, clock, tiny_payload, batch_id="two")
    assert first["created"] is True
    assert second["created"] is False
    assert second["batch_id"] == "one"
    assert second["inserted"] == 0
    assert len(storage.list_batches()) == 1
    assert storage.count_records("one") == 6


def test_different_content_same_batch_id_is_rejected(
    storage, config, clock, tiny_payload
):
    ingest_jsonl(storage, config, clock, tiny_payload, batch_id="fixed")
    other = b'{"record_id":"Z","source":"S","title":"Different"}\n'
    with pytest.raises(IngestError, match="different content"):
        ingest_jsonl(storage, config, clock, other, batch_id="fixed")
    batch = storage.get_batch("fixed")
    assert batch["valid_lines"] == 6
    assert storage.count_records("fixed") == 6


def _post_concurrently(storage, config, clock, payload, batch_ids):
    """Post the same payload from concurrent threads, one per batch_id."""
    results, errors = [], []
    barrier = threading.Barrier(len(batch_ids))

    def worker(batch_id):
        barrier.wait()
        try:
            results.append(
                ingest_jsonl(storage, config, clock, payload, batch_id=batch_id)
            )
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(bid,)) for bid in batch_ids]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert errors == []
    return results


def test_concurrent_identical_uploads_create_one_batch(
    storage, config, clock, tiny_payload
):
    results = _post_concurrently(
        storage, config, clock, tiny_payload, ["race"] * 8
    )
    assert sum(1 for r in results if r["created"]) == 1
    assert all(r["batch_id"] == "race" for r in results)
    assert len(storage.list_batches()) == 1
    assert storage.count_records("race") == 6
    batch = storage.get_batch("race")
    assert (batch["total_lines"], batch["valid_lines"], batch["bad_lines"]) == (6, 6, 0)


def test_concurrent_uploads_with_derived_ids_create_one_batch(
    storage, config, clock, tiny_payload
):
    results = _post_concurrently(
        storage, config, clock, tiny_payload, [None] * 8
    )
    assert sum(1 for r in results if r["created"]) == 1
    assert all(r["batch_id"] == derive_batch_id(tiny_payload) for r in results)
    assert len(storage.list_batches()) == 1


def test_concurrent_same_content_different_ids_still_one_batch(
    storage, config, clock, tiny_payload
):
    results = _post_concurrently(
        storage, config, clock, tiny_payload, [f"up-{i}" for i in range(8)]
    )
    batches = storage.list_batches()
    assert len(batches) == 1
    winner = batches[0]["batch_id"]
    assert all(r["batch_id"] == winner for r in results)
    assert sum(1 for r in results if r["created"]) == 1
    assert storage.count_records(winner) == 6


def test_line_counts_balance_without_trailing_newline(storage, config, clock):
    good = [
        json.dumps({"record_id": f"R{i:03d}", "source": "S", "title": f"Title {i}"})
        for i in range(100)
    ]
    lines = good + ["{bad json", "{also bad"]  # last line has no trailing newline
    payload = "\n".join(lines).encode("utf-8")
    assert not payload.endswith(b"\n")
    result = ingest_jsonl(storage, config, clock, payload, batch_id="nonl")
    assert result["inserted"] == 100
    assert result["dead_added"] == 2
    batch = storage.get_batch("nonl")
    assert (batch["total_lines"], batch["valid_lines"], batch["bad_lines"]) == (102, 100, 2)
    assert batch["valid_lines"] + batch["bad_lines"] == batch["total_lines"]
    assert [row["line_no"] for row in storage.list_dead_letters("nonl")] == [101, 102]


def test_valid_last_line_without_newline_is_counted(storage, config, clock):
    payload = b'{"record_id":"Solo","source":"S","title":"no newline at end"}'
    result = ingest_jsonl(storage, config, clock, payload, batch_id="solo")
    assert (result["total_lines"], result["valid_lines"], result["bad_lines"]) == (1, 1, 0)
    assert storage.count_records("solo") == 1


def test_line_counts_balance_with_duplicates_and_blank_lines(storage, config, clock):
    lines = [
        json.dumps({"record_id": "A", "source": "S", "title": "Alpha"}),
        "",  # blank separator lines are not data lines
        "   ",
        json.dumps({"record_id": "B", "source": "S", "title": "Bravo"}),
        json.dumps({"record_id": "A", "source": "S", "title": "Alpha"}),  # duplicate
        "{broken",
    ]
    payload = "\n".join(lines).encode("utf-8")
    result = ingest_jsonl(storage, config, clock, payload, batch_id="counts")
    assert result["inserted"] == 2
    assert result["duplicates"] == 1
    assert result["dead_added"] == 1
    batch = storage.get_batch("counts")
    assert (batch["total_lines"], batch["valid_lines"], batch["bad_lines"]) == (4, 3, 1)
    assert batch["valid_lines"] + batch["bad_lines"] == batch["total_lines"]


def test_poisonous_line_is_isolated_not_batch_aborting(storage, config, clock):
    nested = (
        '{"record_id":"N","source":"S","title":"x","extra":'
        + "[" * 5000 + "1" + "]" * 5000 + "}"
    )
    lines = [
        '{"record_id":"G1","source":"S","title":"good one"}',
        nested,
        '{"record_id":"G2","source":"S","title":"good two"}',
    ]
    payload = ("\n".join(lines) + "\n").encode("utf-8")
    result = ingest_jsonl(storage, config, clock, payload, batch_id="poison")
    assert result["inserted"] == 2
    assert result["dead_added"] == 1
    batch = storage.get_batch("poison")
    assert (batch["total_lines"], batch["valid_lines"], batch["bad_lines"]) == (3, 2, 1)
    dead = storage.list_dead_letters("poison")
    assert dead[0]["line_no"] == 2
    assert "RecursionError" in dead[0]["reason"]
    assert storage.count_records("poison") == 2


def test_missing_year_and_dirty_pages_are_valid_rows(storage, config, clock):
    lines = [
        '{"record_id":"Y1","source":"S","title":"no year at all"}',
        '{"record_id":"Y2","source":"S","title":"dirty pages","pages":"pp. 123-129"}',
        '{"record_id":"Y3","source":"S","title":"odd year","year":"n/a"}',
    ]
    payload = ("\n".join(lines) + "\n").encode("utf-8")
    result = ingest_jsonl(storage, config, clock, payload, batch_id="fields")
    assert result["inserted"] == 3
    assert result["dead_added"] == 0
    batch = storage.get_batch("fields")
    assert (batch["total_lines"], batch["valid_lines"], batch["bad_lines"]) == (3, 3, 0)
    records = {row["record_id"]: row for row in storage.list_records("fields")}
    assert json.loads(records["Y1"]["norm_json"])["year"] is None
    assert json.loads(records["Y2"]["norm_json"])["pages"] == "123-129"
    assert json.loads(records["Y3"]["norm_json"])["year"] is None

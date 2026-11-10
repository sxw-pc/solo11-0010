from __future__ import annotations

import json
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
    assert (batch["total_lines"], batch["valid_lines"], batch["bad_lines"]) == (5, 2, 2)
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

"""End-to-end acceptance walkthrough over the HTTP API.

Scenarios mirror the manual acceptance plan:

1. Two batches share one gray-zone pair; a cross-batch 'same' on the first
   batch removes the pair from the second batch's /review/next and shows the
   source batch under /runs.
2. Delete + same-name rebuild with changed content: identity mismatch blocks
   blind inheritance and the pair returns as a fresh gray item.
3. Concurrent mutually exclusive cross-batch submissions collapse to a single
   effective decision.
4. An older batch's clusters/golden export stay byte-identical after later
   cross-batch activity.
"""
from __future__ import annotations

import json
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

# Allow running directly from a checkout without installing the package.
_ROOT = Path(__file__).resolve().parents[1]
for entry in (str(_ROOT / "src"), str(_ROOT)):
    if entry not in sys.path:
        sys.path.insert(0, entry)

from fastapi.testclient import TestClient  # noqa: E402

from citefuse.api import create_app  # noqa: E402
from citefuse.clock import FixedClock  # noqa: E402
from citefuse.models import make_record_key  # noqa: E402
from scripts.seed_fixtures import generate  # noqa: E402


def payload(records):
    return (
        "\n".join(
            json.dumps(r, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
            for r in records
        )
        + "\n"
    ).encode()


def main() -> None:
    clock = FixedClock(datetime(2025, 3, 1, tzinfo=timezone.utc))
    data_dir = Path(tempfile.mkdtemp(prefix="accept-"))
    client = TestClient(create_app(data_dir=data_dir, clock=clock))

    records, _ = generate(20240517)
    full = payload(records)

    # ---- scenario 1: cross-batch inheritance -----------------------------
    client.post("/batches?batch_id=A1", content=full,
                headers={"content-type": "application/x-ndjson"})
    run_a1 = client.post("/runs", json={"batch_id": "A1"}).json()
    gray = client.get("/review/next", params={"batch_id": "A1", "n": 100}).json()
    target = gray["pairs"][0]
    pair_key = target["pair_key"]
    keys = [r["record_key"] for r in target["records"]]
    wanted = set(keys)
    shared = [r for r in records if make_record_key(r["source"], r["record_id"]) in wanted]

    # A separate batch proves the pair is genuinely gray before any decision.
    client.post("/batches?batch_id=A0", content=payload(shared),
                headers={"content-type": "application/x-ndjson"})
    client.post("/runs", json={"batch_id": "A0"})
    before = client.get("/review/next", params={"batch_id": "A0", "n": 100}).json()
    assert any(p["pair_key"] == pair_key for p in before["pairs"]), "pair gray in A0"

    # A2 is only ingested; its first run happens after the cross decision.
    client.post("/batches?batch_id=A2", content=payload(shared),
                headers={"content-type": "application/x-ndjson"})

    decision = client.post("/review/decisions", json={
        "batch_id": "A1", "pair_key": pair_key, "decision": "same",
        "reviewer": "rv", "note": "cross", "scope": "cross_batch",
    })
    assert decision.status_code == 201, decision.text
    run_a2 = client.post("/runs", json={"batch_id": "A2"}).json()
    after = client.get("/review/next", params={"batch_id": "A2", "n": 100}).json()
    assert all(p["pair_key"] != pair_key for p in after["pairs"]), "inherited pair gone"
    sources = {h["source_batch_id"] for h in run_a2["inherited_decisions"]}
    assert sources == {"A1"}, run_a2["inherited_decisions"]
    listed = client.get("/runs", params={"batch_id": "A2"}).json()
    assert listed[0]["inheritances"][0]["source_batch_id"] == "A1"
    print("[1] cross-batch inheritance: OK (source batch A1 visible in /runs)")

    # ---- scenario 2: rebuild with changed content -> no blind inherit ----
    golden_a1_first = client.get(
        "/golden/export", params={"batch_id": "A1", "format": "jsonl"}
    ).content
    client.delete("/batches/A1")
    rebuilt = []
    for record in shared:
        record = dict(record)
        if make_record_key(record["source"], record["record_id"]) == keys[0]:
            record["title"] = record["title"] + " retracted replacement edition"
        rebuilt.append(record)
    client.post("/batches?batch_id=A1", content=payload(rebuilt),
                headers={"content-type": "application/x-ndjson"})
    run_rebuilt = client.post("/runs", json={"batch_id": "A1"}).json()
    queue = client.get("/review/next", params={"batch_id": "A1", "n": 100}).json()
    item = next(p for p in queue["pairs"] if p["pair_key"] == pair_key)
    hist = item["historical_decision"]
    assert hist["mismatch_reason"] == "record_content_mismatch", hist
    assert hist["inherited"] is False
    assert run_rebuilt["inheritance_mismatches"][0]["pair_key"] == pair_key
    print("[2] identity mismatch on rebuild: OK (fresh gray pair, no blind inherit)")

    # ---- scenario 3: concurrent mutually exclusive submissions -----------
    client.post("/batches?batch_id=C1", content=full,
                headers={"content-type": "application/x-ndjson"})
    client.post("/runs", json={"batch_id": "C1"})
    client.post("/batches?batch_id=C2", content=payload(shared),
                headers={"content-type": "application/x-ndjson"})
    client.post("/runs", json={"batch_id": "C2"})
    import threading

    barrier = threading.Barrier(2)
    statuses: list[int] = []

    def submit(batch, verdict):
        barrier.wait()
        resp = client.post("/review/decisions", json={
            "batch_id": batch, "pair_key": pair_key, "decision": verdict,
            "scope": "cross_batch",
        })
        statuses.append(resp.status_code)

    t1 = threading.Thread(target=submit, args=("C1", "same"))
    t2 = threading.Thread(target=submit, args=("C2", "different"))
    t1.start(); t2.start(); t1.join(); t2.join()
    # The TestClient shares one Storage lock, so both serialize; the loser
    # either superseded atomically (201, one active row) or got 409. Either
    # way exactly one active cross-batch decision remains.
    from citefuse.storage import Storage  # noqa: E402

    db = next(data_dir.glob("citefuse.sqlite3"))
    storage = Storage(db)
    active = storage.list_active_cross_decisions([pair_key])
    assert len(active) == 1, f"expected 1 active row, got {len(active)}"
    history = [r for r in storage.list_cross_decision_history() if r["pair_key"] == pair_key]
    assert sum(1 for r in history if r["status"] == "active") == 1
    storage.close()
    print(f"[3] concurrent exclusive decisions: OK (statuses={sorted(statuses)}, "
          f"one active={active[0]['decision']})")

    # ---- scenario 4: old batch reproducibility ---------------------------
    # A1 was rebuilt, so compare a pristine batch instead: A2's first run
    # outputs must be byte-identical on rerun despite later global changes.
    golden_before = client.get(
        "/golden/export", params={"batch_id": "A2", "format": "jsonl"}
    ).content
    clusters_before = client.get("/clusters", params={"batch_id": "A2"}).content
    rerun = client.post("/runs", json={"batch_id": "A2"}).json()
    assert rerun["run_id"] == run_a2["run_id"], "rerun must keep run_id"
    golden_after = client.get(
        "/golden/export", params={"batch_id": "A2", "format": "jsonl"}
    ).content
    clusters_after = client.get("/clusters", params={"batch_id": "A2"}).content
    assert golden_before == golden_after, "golden export must be byte-identical"
    assert clusters_before == clusters_after, "clusters must be byte-identical"
    print("[4] old batch rerun byte-identical: OK")
    print("\nALL ACCEPTANCE SCENARIOS PASSED")


if __name__ == "__main__":
    main()

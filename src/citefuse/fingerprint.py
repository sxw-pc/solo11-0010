"""Cross-batch document fingerprints and advisory cross-batch references.

Design
------
A fingerprint is computed **after** normalization and covers exactly the three
core identity fields ``title + authors + year``:

* title: all normalized word tokens, sorted.  Sorting absorbs punctuation,
  full-width forms, case and word-order differences while retaining the full
  token multiset (unlike blocking's five-token signature).
* authors: the canonical ``family|given-initial`` keys, sorted and de-duplicated.
  Author order and "Family, Given" / "Given Family" formatting therefore do
  not change the fingerprint.
* year: the parsed four-digit year, exact.

The fingerprint deliberately excludes ``record_pk``, the batch id, source,
venue, DOI, raw JSON, volume, issue and pages.  The same work arriving from a
different source or in a different batch therefore converges to one value;
nothing already stored is physically merged or rewritten.

False-positive controls
-----------------------
Exact ``title+authors+year`` equality is stricter than a title-only signature,
but it is not collision-free.  The following guards are applied:

1. A record is only fingerprinted when the normalized title yields at least two
   tokens, at least one author parses, and the year is a valid integer.  Other
   records are *degraded*: they are ingested normally, get a ``degraded`` index
   row with a NULL fingerprint, never take part in cross-batch matching, and
   emit a ``fingerprint_degraded`` audit event.
2. The author component uses family + given *initial*.  Before a reference is
   written, the guard compares the stored full given names for matching author
   keys.  If two different full given names share the same family/initial
   (``John Smith`` vs ``Jane Smith``), the pair is treated as a fingerprint
   collision: no reference is written and a ``fingerprint_collision`` audit
   event records it.  Initial-vs-full forms (``J. Smith`` vs ``John Smith``)
   remain compatible.
3. References are advisory only.  They never feed blocking, scoring,
   clustering, survivorship or run ids; historical records and completed runs
   are not modified.  A false positive therefore costs one reviewable link,
   not a corrupted cluster or golden record.
4. Records missing any core component are excluded instead of falling back to
   a weaker key, so partial records cannot collide on the remaining fields.

Backfill
--------
Schema creation is part of the normal idempotent startup DDL and never scans
records.  Records from a pre-fingerprint database have no index row and simply
render an empty cross-batch list.  ``backfill_fingerprints`` (also exposed via
``POST /fingerprints/backfill``) fills them in on demand; ingestion also fills
any missing historical fingerprint before matching a new batch, so old records
participate as soon as they are needed.
"""
from __future__ import annotations

import hashlib
import json
from typing import Any, Iterable

from .audit import record_event
from .clock import Clock
from .models import make_record_key, record_from_db
from .similarity import tokenize
from .storage import Storage

FINGERPRINT_ALGORITHM = "cf-fp-v1"
FINGERPRINT_VERSION = 1
MIN_TITLE_TOKENS = 2
_FINGERPRINT_PREFIX = "fp1_"
_QUERY_CHUNK = 500


def _dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _now(clock: Clock) -> str:
    return clock.now_iso()


def build_fingerprint(norm: dict[str, Any]) -> tuple[str | None, dict[str, Any], str | None]:
    """Return ``(fingerprint, components, degradation_reason)`` for a norm dict.

    ``fingerprint`` is None exactly when ``degradation_reason`` is set.  The
    components are persisted for audit and are independent of record/batch ids.
    """
    components: dict[str, Any] = {"version": FINGERPRINT_VERSION}
    try:
        title_tokens = sorted(tokenize(norm.get("title")))
        author_keys = sorted({
            str(author.get("key") or "").strip()
            for author in (norm.get("authors") or [])
            if str(author.get("key") or "").strip()
        })
        year = norm.get("year")
        components.update({
            "title_tokens": title_tokens,
            "author_keys": author_keys,
            "year": year,
        })
        if not title_tokens:
            reason = "missing_title_tokens"
        elif len(title_tokens) < MIN_TITLE_TOKENS:
            reason = "title_too_short"
        elif not author_keys:
            reason = "missing_authors"
        elif not isinstance(year, int) or isinstance(year, bool):
            reason = "missing_year"
        else:
            reason = None
    except Exception as exc:  # defensive: malformed stored norm_json
        reason = f"extract_error:{type(exc).__name__}"
        components["reason"] = reason
        return None, components, reason

    if reason is not None:
        components["reason"] = reason
        return None, components, reason
    material = _dumps({
        "version": FINGERPRINT_VERSION,
        "title_tokens": components["title_tokens"],
        "author_keys": components["author_keys"],
        "year": components["year"],
    })
    digest = hashlib.sha256(material.encode("utf-8")).hexdigest()[:32]
    return _FINGERPRINT_PREFIX + digest, components, None


def _given_compatible(left: str, right: str) -> bool:
    """Tolerate initials/prefixes but reject genuinely different given names."""
    a, b = (left or "").strip(), (right or "").strip()
    if not a or not b:
        return True
    if a == b:
        return True
    return a.startswith(b) or b.startswith(a)


def _author_givens(norm: dict[str, Any]) -> dict[str, list[str]]:
    output: dict[str, list[str]] = {}
    for author in norm.get("authors") or []:
        key = str(author.get("key") or "")
        if key:
            output.setdefault(key, []).append(str(author.get("given") or ""))
    return output


def collision_guard(
    left_norm: dict[str, Any], right_norm: dict[str, Any]
) -> tuple[bool, dict[str, Any]]:
    """Conservative post-index check against same-initial author collisions."""
    left, right = _author_givens(left_norm), _author_givens(right_norm)
    conflicts: list[dict[str, Any]] = []
    for key in sorted(set(left) & set(right)):
        for given_left in left[key]:
            for given_right in right[key]:
                if not _given_compatible(given_left, given_right):
                    conflicts.append({
                        "author_key": key,
                        "left_given": given_left,
                        "right_given": given_right,
                    })
    return not conflicts, {
        "author_conflicts": conflicts,
        "given_name_compatible": not conflicts,
        "year_equal": left_norm.get("year") == right_norm.get("year"),
        "title_tokens_equal": sorted(tokenize(left_norm.get("title")))
        == sorted(tokenize(right_norm.get("title"))),
    }


def _record_key(row: Any) -> str:
    return make_record_key(row["source"], row["record_id"])


def _insert_fingerprint(
    storage: Storage,
    clock: Clock,
    record_pk: int,
    batch_id: str,
    fingerprint: str | None,
    status: str,
    components: dict[str, Any],
) -> bool:
    cursor = storage.execute(
        """INSERT OR IGNORE INTO record_fingerprints(
             record_pk,batch_id,fingerprint,status,algorithm,components_json,created_at)
           VALUES(?,?,?,?,?,?,?)""",
        (record_pk, batch_id, fingerprint, status, FINGERPRINT_ALGORITHM,
         _dumps(components), _now(clock)),
    )
    return cursor.rowcount == 1


def ensure_fingerprint(
    storage: Storage,
    clock: Clock,
    record_row: Any,
    norm: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Idempotently ensure one record has an index row.

    The primary key on ``record_pk`` makes concurrent ingestion safe: only one
    caller inserts, and a degraded record is audited only by that caller.
    """
    record_pk = int(record_row["record_pk"])
    existing = storage.query_one(
        "SELECT * FROM record_fingerprints WHERE record_pk=?", (record_pk,)
    )
    if existing is not None:
        return {
            "inserted": False,
            "fingerprint": existing["fingerprint"],
            "status": existing["status"],
            "reason": json.loads(existing["components_json"]).get("reason"),
        }
    if norm is None:
        norm = json.loads(record_row["norm_json"])
    fingerprint, components, reason = build_fingerprint(norm)
    status = "indexed" if fingerprint is not None else "degraded"
    inserted = _insert_fingerprint(
        storage, clock, record_pk, record_row["batch_id"], fingerprint, status, components
    )
    if inserted and reason is not None:
        record_event(
            storage, clock, record_row["batch_id"], None, "fingerprint_degraded",
            {
                "record_pk": record_pk,
                "record_key": _record_key(record_row),
                "reason": reason,
                "algorithm": FINGERPRINT_ALGORITHM,
            },
        )
    return {
        "inserted": inserted,
        "fingerprint": fingerprint,
        "status": status,
        "reason": reason,
    }


def _missing_fingerprint_rows(
    storage: Storage, batch_id: str | None = None
) -> list[Any]:
    if batch_id is None:
        return storage.query(
            """SELECT r.* FROM records r
               LEFT JOIN record_fingerprints f ON f.record_pk=r.record_pk
               WHERE f.record_pk IS NULL
               ORDER BY r.batch_id,r.source,r.record_id"""
        )
    return storage.query(
        """SELECT r.* FROM records r
           LEFT JOIN record_fingerprints f ON f.record_pk=r.record_pk
           WHERE r.batch_id=? AND f.record_pk IS NULL
           ORDER BY r.source,r.record_id""",
        (batch_id,),
    )


def has_missing_index_rows(storage: Storage) -> bool:
    """Cheap steady-state check: equal row counts means nothing to backfill."""
    record_count = storage.query_one("SELECT COUNT(*) AS n FROM records")
    index_count = storage.query_one("SELECT COUNT(*) AS n FROM record_fingerprints")
    return int(record_count["n"] or 0) > int(index_count["n"] or 0)


def _index_missing_rows(
    storage: Storage,
    clock: Clock,
    rows: list[Any],
    phase: str,
) -> dict[str, int]:
    """Index missing records, auditing per-record failures instead of aborting."""
    indexed = degraded = errors = 0
    for row in rows:
        try:
            result = ensure_fingerprint(storage, clock, row)
        except Exception as exc:
            errors += 1
            record_event(
                storage, clock, row["batch_id"], None, "fingerprint_error",
                {
                    "phase": phase,
                    "record_key": _record_key(row),
                    "error": f"{type(exc).__name__}: {exc}"[:500],
                },
            )
            continue
        if result["inserted"]:
            if result["status"] == "indexed":
                indexed += 1
            else:
                degraded += 1
    return {"indexed": indexed, "degraded": degraded, "errors": errors}


def ensure_all_fingerprints(storage: Storage, clock: Clock) -> dict[str, int]:
    """Fill every missing fingerprint row.  Called lazily from ingestion."""
    return _index_missing_rows(
        storage, clock, _missing_fingerprint_rows(storage), "ingest_backfill"
    )


def backfill_fingerprints(
    storage: Storage, clock: Clock, batch_id: str | None = None
) -> dict[str, Any]:
    """Backfill on demand, then make newly indexed records matchable.

    ``batch_id`` limits the scan; omitting it covers every batch.  A single
    malformed stored norm_json cannot abort the upgrade: it is audited and the
    scan continues, leaving that record pending.
    """
    rows = _missing_fingerprint_rows(storage, batch_id)
    counts = _index_missing_rows(storage, clock, rows, "backfill")
    if batch_id is None:
        values = storage.query(
            """SELECT DISTINCT fingerprint FROM record_fingerprints
               WHERE status='indexed' AND fingerprint IS NOT NULL"""
        )
    else:
        values = storage.query(
            """SELECT DISTINCT fingerprint FROM record_fingerprints
               WHERE batch_id=? AND status='indexed' AND fingerprint IS NOT NULL""",
            (batch_id,),
        )
    sync = sync_cross_batch_refs(
        storage, clock, [row["fingerprint"] for row in values],
        origin_batch_id=batch_id,
    )
    return {
        "batch_id": batch_id,
        "records_scanned": len(rows),
        "newly_indexed": counts["indexed"],
        "degraded": counts["degraded"],
        "errors": counts["errors"],
        "cross_batch_refs_added": sync["added"],
        "fingerprint_collisions": sync["collisions"],
    }


def _fingerprint_rows(
    storage: Storage, fingerprints: Iterable[str]
) -> list[Any]:
    values = sorted(set(fingerprints))
    rows: list[Any] = []
    for start in range(0, len(values), _QUERY_CHUNK):
        chunk = values[start:start + _QUERY_CHUNK]
        placeholders = ",".join("?" for _ in chunk)
        rows.extend(storage.query(
            f"""SELECT f.fingerprint,f.record_pk,f.batch_id,
                       r.source,r.record_id,r.norm_json
                FROM record_fingerprints f
                JOIN records r ON r.record_pk=f.record_pk
                WHERE f.fingerprint IN ({placeholders})
                ORDER BY f.fingerprint,f.batch_id,r.source,r.record_id""",
            tuple(chunk),
        ))
    return rows


def _insert_cross_ref(
    storage: Storage,
    clock: Clock,
    fingerprint: str,
    left: Any,
    right: Any,
    guard: dict[str, Any],
    origin_batch_id: str,
) -> bool:
    # Canonical orientation by insertion order keeps repeated/concurrent syncs
    # idempotent: the older record is always the stored left side.
    if int(left["record_pk"]) > int(right["record_pk"]):
        left, right = right, left
    detail = {
        "record_key": _record_key(left),
        "matched_record_key": _record_key(right),
        "algorithm": FINGERPRINT_ALGORITHM,
        "guard": guard,
    }
    cursor = storage.execute(
        """INSERT OR IGNORE INTO cross_batch_refs(
             batch_id,record_pk,fingerprint,matched_batch_id,matched_record_pk,
             origin_batch_id,details_json,created_at)
           VALUES(?,?,?,?,?,?,?,?)""",
        (left["batch_id"], left["record_pk"], fingerprint, right["batch_id"],
         right["record_pk"], origin_batch_id, _dumps(detail), _now(clock)),
    )
    return cursor.rowcount == 1


def sync_cross_batch_refs(
    storage: Storage,
    clock: Clock,
    fingerprints: Iterable[str],
    origin_batch_id: str | None = None,
) -> dict[str, int]:
    """Create missing advisory references for the given fingerprint values.

    Only rows in *different* batches are linked; same-batch duplicate handling
    remains entirely owned by the existing ingestion/clustering path.
    ``origin_batch_id`` marks which batch caused the link (used for direction
    display); when omitted, the newer record's batch is treated as the origin.
    """
    added = collisions = 0
    for fingerprint in sorted(set(fingerprints)):
        rows = _fingerprint_rows(storage, [fingerprint])
        for index, left in enumerate(rows):
            for right in rows[index + 1:]:
                if left["batch_id"] == right["batch_id"]:
                    continue
                origin = origin_batch_id
                if origin not in (left["batch_id"], right["batch_id"]):
                    # Lazy backfill can link two historical records; attribute
                    # the reference to the newer of the two instead of the
                    # batch that merely triggered the scan.
                    newer = max(
                        (left, right), key=lambda row: int(row["record_pk"])
                    )
                    origin = newer["batch_id"]
                left_norm = json.loads(left["norm_json"])
                right_norm = json.loads(right["norm_json"])
                accepted, guard = collision_guard(left_norm, right_norm)
                if not accepted:
                    collisions += 1
                    cursor = storage.execute(
                        """INSERT OR IGNORE INTO fingerprint_collisions(
                             fingerprint,record_pk,matched_batch_id,matched_record_pk,
                             details_json,created_at) VALUES(?,?,?,?,?,?)""",
                        (fingerprint, left["record_pk"], right["batch_id"],
                         right["record_pk"],
                         _dumps({
                             "record_key": _record_key(left),
                             "matched_record_key": _record_key(right),
                             "guard": guard,
                         }), _now(clock)),
                    )
                    if cursor.rowcount == 1:
                        record_event(
                            storage, clock, left["batch_id"], None,
                            "fingerprint_collision",
                            {
                                "fingerprint": fingerprint,
                                "record_key": _record_key(left),
                                "matched_batch_id": right["batch_id"],
                                "matched_record_key": _record_key(right),
                                "guard": guard,
                            },
                        )
                    continue
                if _insert_cross_ref(
                    storage, clock, fingerprint, left, right, guard, origin
                ):
                    added += 1
    return {"added": added, "collisions": collisions}


def backfill_fingerprints(
    storage: Storage, clock: Clock, batch_id: str | None = None
) -> dict[str, Any]:
    """Backfill on demand, then make newly indexed records matchable.

    ``batch_id`` limits the scan; omitting it covers every batch.  A single
    malformed stored norm_json cannot abort the upgrade: it is audited and the
    scan continues, leaving that record pending.
    """
    indexed = degraded = errors = 0
    rows = _missing_fingerprint_rows(storage, batch_id)
    for row in rows:
        try:
            result = ensure_fingerprint(storage, clock, row)
        except Exception as exc:
            errors += 1
            record_event(
                storage, clock, row["batch_id"], None, "fingerprint_error",
                {
                    "phase": "backfill",
                    "record_key": _record_key(row),
                    "error": f"{type(exc).__name__}: {exc}"[:500],
                },
            )
            continue
        if result["inserted"]:
            if result["status"] == "indexed":
                indexed += 1
            else:
                degraded += 1
    if batch_id is None:
        values = storage.query(
            """SELECT DISTINCT fingerprint FROM record_fingerprints
               WHERE status='indexed' AND fingerprint IS NOT NULL"""
        )
    else:
        values = storage.query(
            """SELECT DISTINCT fingerprint FROM record_fingerprints
               WHERE batch_id=? AND status='indexed' AND fingerprint IS NOT NULL""",
            (batch_id,),
        )
    sync = sync_cross_batch_refs(
        storage, clock, [row["fingerprint"] for row in values],
        origin_batch_id=batch_id,
    )
    return {
        "batch_id": batch_id,
        "records_scanned": len(rows),
        "newly_indexed": indexed,
        "degraded": degraded,
        "errors": errors,
        "cross_batch_refs_added": sync["added"],
        "fingerprint_collisions": sync["collisions"],
    }


def fingerprint_summary(storage: Storage, batch_id: str) -> dict[str, int]:
    """Index state for one batch.  Never triggers a backfill."""
    total = storage.query_one(
        "SELECT COUNT(*) AS n FROM records WHERE batch_id=?", (batch_id,)
    )
    counts = storage.query_one(
        """SELECT
             SUM(CASE WHEN status='indexed' THEN 1 ELSE 0 END) AS indexed,
             SUM(CASE WHEN status='degraded' THEN 1 ELSE 0 END) AS degraded
           FROM record_fingerprints WHERE batch_id=?""",
        (batch_id,),
    )
    refs = storage.query_one(
        """SELECT COUNT(*) AS n FROM cross_batch_refs
           WHERE batch_id=? OR matched_batch_id=?""",
        (batch_id, batch_id),
    )
    indexed = int(counts["indexed"] or 0)
    degraded = int(counts["degraded"] or 0)
    total_records = int(total["n"] or 0)
    return {
        "records": total_records,
        "indexed": indexed,
        "degraded": degraded,
        "pending": max(0, total_records - indexed - degraded),
        "cross_batch_refs": int(refs["n"] or 0),
    }


def list_cross_batch_refs(
    storage: Storage, batch_id: str, include_records: bool = True
) -> list[dict[str, Any]]:
    """Return references visible from ``batch_id`` in either direction."""
    rows = storage.query(
        """SELECT cr.*,
                  rb.source AS r_source, rb.record_id AS r_record_id,
                  mb.source AS m_source, mb.record_id AS m_record_id
           FROM cross_batch_refs cr
           JOIN records rb ON rb.record_pk=cr.record_pk
           JOIN records mb ON mb.record_pk=cr.matched_record_pk
           WHERE cr.batch_id=? OR cr.matched_batch_id=?
           ORDER BY cr.fingerprint,cr.record_pk,cr.matched_record_pk""",
        (batch_id, batch_id),
    )
    records_cache: dict[int, dict[str, Any]] = {}

    def public(pk: int) -> dict[str, Any] | None:
        if not include_records:
            return None
        if pk not in records_cache:
            found = storage.query_one(
                "SELECT * FROM records WHERE record_pk=?", (pk,)
            )
            records_cache[pk] = record_from_db(found).public() if found else {}
        return records_cache[pk]

    output: list[dict[str, Any]] = []
    for row in rows:
        sides = {
            row["batch_id"]: (row["record_pk"], f"{row['r_source']}::{row['r_record_id']}"),
            row["matched_batch_id"]: (
                row["matched_record_pk"], f"{row['m_source']}::{row['m_record_id']}"
            ),
        }
        # Present the queried batch's own record as the subject.
        if batch_id in sides:
            record_pk, record_key = sides[batch_id]
            other_batch = (
                row["matched_batch_id"] if batch_id == row["batch_id"]
                else row["batch_id"]
            )
            other_pk, other_key = sides[other_batch]
        else:  # pragma: no cover - query restricts to this batch
            continue
        entry = {
            "record_key": record_key,
            "record_pk": record_pk,
            "record_batch_id": batch_id,
            "fingerprint": row["fingerprint"],
            "matched_batch_id": other_batch,
            "matched_record_key": other_key,
            "matched_record_pk": other_pk,
            "direction": (
                "outgoing" if row["origin_batch_id"] == batch_id else "incoming"
            ),
            "details": json.loads(row["details_json"]),
            "created_at": row["created_at"],
        }
        matched_record = public(other_pk)
        if matched_record is not None:
            entry["matched_record"] = matched_record
        output.append(entry)
    output.sort(key=lambda item: (
        item["record_key"], item["matched_batch_id"], item["matched_record_key"]
    ))
    return output


def list_fingerprints(
    storage: Storage, batch_id: str | None = None
) -> list[dict[str, Any]]:
    """Expose index rows for audit and client-side cross-batch alignment."""
    sql = """SELECT f.*, r.source, r.record_id
             FROM record_fingerprints f
             JOIN records r ON r.record_pk=f.record_pk"""
    args: tuple[Any, ...] = ()
    if batch_id is not None:
        sql += " WHERE f.batch_id=?"
        args = (batch_id,)
    sql += " ORDER BY f.batch_id,r.source,r.record_id"
    return [
        {
            "record_pk": row["record_pk"],
            "batch_id": row["batch_id"],
            "record_key": make_record_key(row["source"], row["record_id"]),
            "fingerprint": row["fingerprint"],
            "status": row["status"],
            "algorithm": row["algorithm"],
            "components": json.loads(row["components_json"]),
            "created_at": row["created_at"],
        }
        for row in storage.query(sql, args)
    ]

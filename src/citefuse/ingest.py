"""Batch ingestion with per-line validation and a dead-letter queue.

Semantics guaranteed here:

* Idempotency — identical JSONL content is recognized by its ``input_hash``
  and mapped to the batch that already holds it, no matter which ``batch_id``
  the retry supplied. The check and the write run inside one serialized
  transaction, so concurrent duplicate uploads cannot create duplicate
  batches or rows; exactly one of them reports ``created=True``.
* Line accounting — every non-blank physical line ends up in exactly one of
  two buckets: *valid* (parsed, validated and normalized; stored or already
  present) or *bad* (dead letter). Hence ``valid_lines + bad_lines ==
  total_lines`` always. Blank separator lines are not data lines and are not
  counted. Files with or without a trailing newline are handled alike.
* Per-line isolation — any failure while handling a line (JSON syntax,
  schema, normalization, even storage of that line) becomes a dead letter
  recording the line number and reason; it never aborts the batch.
"""
from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from typing import Any

from pydantic import ValidationError

from .clock import Clock
from .config import AppConfig
from .models import BATCH_ID_RE, RawRecord
from .normalize import NormalizationError, Normalizer
from .storage import Storage

_BATCH_ID_RE = re.compile(BATCH_ID_RE)


class IngestError(ValueError):
    """Raised for request-level ingestion failures (not per-line failures)."""


def derive_batch_id(payload: bytes) -> str:
    """Content-derived default ID: re-posting identical JSONL is idempotent."""
    return "batch_" + hashlib.sha256(payload).hexdigest()[:16]


def _validation_reason(exc: Exception) -> str:
    if isinstance(exc, ValidationError):
        pieces = []
        for error in exc.errors():
            location = ".".join(str(item) for item in error.get("loc", ()))
            pieces.append(f"{location or 'record'}: {error.get('msg', 'invalid')}")
        return "schema: " + "; ".join(pieces)[:500]
    if isinstance(exc, json.JSONDecodeError):
        return f"json: line {exc.lineno} column {exc.colno}: {exc.msg}"[:500]
    if isinstance(exc, NormalizationError):
        return f"normalize: {exc}"[:500]
    return f"{type(exc).__name__}: {exc}"[:500]


def _storable(value: str) -> str:
    """Make text safe for SQLite TEXT storage (lone surrogates break UTF-8)."""
    return value.encode("utf-8", "backslashreplace").decode("utf-8")


def _existing_result(row: sqlite3.Row) -> dict[str, Any]:
    """Response for an idempotent re-post: the stored batch, no new writes."""
    result = dict(row)
    result.update({"created": False, "inserted": 0, "duplicates": 0, "dead_added": 0})
    return result


def ingest_jsonl(
    storage: Storage,
    config: AppConfig,
    clock: Clock,
    payload: str | bytes,
    batch_id: str | None = None,
    name: str | None = None,
) -> dict[str, Any]:
    """Validate, normalize and persist a JSONL batch.

    Bad lines never abort the batch; each becomes a dead letter. Re-posting an
    identical payload returns the original counts without writing new records.
    Posting different content under an existing batch_id is rejected.
    """
    if isinstance(payload, str):
        text = payload
        try:
            raw_bytes = payload.encode("utf-8")
        except UnicodeEncodeError as exc:
            raise IngestError(f"payload must be UTF-8 encodable: {exc}") from exc
    else:
        raw_bytes = bytes(payload)
        try:
            text = raw_bytes.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise IngestError(f"payload must be UTF-8: {exc}") from exc
    if not text.strip():
        raise IngestError("payload is empty")

    chosen_id = (batch_id or "").strip() or derive_batch_id(raw_bytes)
    if not _BATCH_ID_RE.fullmatch(chosen_id):
        raise IngestError("batch_id must match [A-Za-z0-9][A-Za-z0-9._-]{0,63}")

    input_hash = hashlib.sha256(raw_bytes).hexdigest()
    normalizer = Normalizer(config.normalization)
    now = clock.now_iso()

    try:
        with storage.transaction():
            # Idempotency: identical content always maps to the batch that was
            # created for it, whichever batch_id the retry supplied. Holding
            # the storage lock for the whole unit of work serializes
            # concurrent duplicate uploads, so exactly one of them creates.
            existing = storage.get_batch_by_hash(input_hash)
            if existing is not None:
                return _existing_result(existing)
            if storage.get_batch(chosen_id) is not None:
                raise IngestError(
                    f"batch_id {chosen_id!r} already exists with different content"
                )

            storage.save_batch({
                "batch_id": chosen_id,
                "name": name,
                "input_hash": input_hash,
                "created_at": now,
                "updated_at": now,
                "total_lines": 0,
                "valid_lines": 0,
                "bad_lines": 0,
                "status": "ingested",
            })
            inserted = duplicates = dead_added = 0
            for line_no, line in enumerate(text.splitlines(), start=1):
                if not line.strip():
                    continue  # blank separators are not data lines
                try:
                    parsed = json.loads(line)
                    if not isinstance(parsed, dict):
                        raise ValueError("top-level JSON value must be an object")
                    raw = RawRecord.model_validate(parsed)
                    norm = normalizer.normalize(raw)
                    row = {
                        "source": raw.source.upper(),
                        "record_id": raw.record_id,
                        "line_no": line_no,
                        "raw": raw.model_dump(mode="json"),
                        "norm": norm,
                        "title": norm["title"],
                        "authors": norm["authors"],
                        "venue": norm["venue"],
                        "year": norm["year"],
                        "volume": norm["volume"],
                        "issue": norm["issue"],
                        "pages": norm["pages"],
                        "doi": norm["doi"],
                        "gold_cluster": raw.gold_cluster,
                    }
                    is_new = storage.insert_record(chosen_id, row)
                except Exception as exc:  # noqa: BLE001 - per-line isolation
                    storage.insert_dead_letter(
                        chosen_id,
                        line_no,
                        _storable(_validation_reason(exc)),
                        _storable(line),
                    )
                    dead_added += 1
                    continue
                if is_new:
                    inserted += 1
                else:
                    duplicates += 1

            valid_lines = inserted + duplicates
            batch = {
                "batch_id": chosen_id,
                "name": name,
                "input_hash": input_hash,
                "created_at": now,
                "updated_at": now,
                "total_lines": valid_lines + dead_added,
                "valid_lines": valid_lines,
                "bad_lines": dead_added,
                "status": "ingested",
            }
            storage.save_batch(batch)
            result = dict(batch)
            result.update({
                "created": True,
                "inserted": inserted,
                "duplicates": duplicates,
                "dead_added": dead_added,
            })
            return result
    except sqlite3.IntegrityError:
        # Lost a cross-process race: another writer committed a batch with the
        # same input_hash first (input_hash is UNIQUE on fresh databases).
        existing = storage.get_batch_by_hash(input_hash)
        if existing is not None:
            return _existing_result(existing)
        raise

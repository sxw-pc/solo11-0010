"""Batch ingestion with per-line validation and a dead-letter queue."""
from __future__ import annotations

import hashlib
import json
import re
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


def _dead_letter_source(line: str, parsed: Any) -> str | None:
    """Best-effort source attribution for a dead letter.

    A line can fail before or after JSON parsing. When a parsed object carries a
    non-empty string ``source``, that value is attributed (upper-cased, matching
    how valid records normalize their source). Otherwise the line is source-
    unknown and only counts toward the run-level bad-row total, never toward a
    source's bad-row rate.
    """
    if isinstance(parsed, dict):
        value = parsed.get("source")
        if isinstance(value, str) and value.strip():
            return value.strip().upper()
    return None


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
    """
    if isinstance(payload, str):
        raw_bytes = payload.encode("utf-8")
        try:
            text = payload
        except UnicodeError as exc:  # pragma: no cover - Python str is decoded
            raise IngestError(f"invalid text payload: {exc}") from exc
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
    existing = storage.get_batch(chosen_id)
    if existing is not None and existing["input_hash"] == input_hash:
        result = dict(existing)
        result.update({"created": False, "inserted": 0, "duplicates": 0, "dead_added": 0})
        return result

    normalizer = Normalizer(config.normalization)
    now = clock.now_iso()
    inserted = duplicates = dead_added = 0
    physical_lines = 0
    bootstrap = {
        "batch_id": chosen_id,
        "name": name,
        "input_hash": input_hash,
        "created_at": existing["created_at"] if existing else now,
        "updated_at": now,
        "total_lines": int(existing["total_lines"]) if existing else 0,
        "valid_lines": int(existing["valid_lines"]) if existing else 0,
        "bad_lines": int(existing["bad_lines"]) if existing else 0,
        "status": "ingested",
    }
    storage.save_batch(bootstrap)
    for line_no, line in enumerate(text.splitlines(), start=1):
        if not line.strip():
            continue
        physical_lines += 1
        parsed: Any = None
        try:
            parsed = json.loads(line)
            if not isinstance(parsed, dict):
                raise ValueError("top-level JSON value must be an object")
            raw = RawRecord.model_validate(parsed)
            norm = normalizer.normalize(raw)
        except (json.JSONDecodeError, ValidationError, ValueError, NormalizationError) as exc:
            storage.insert_dead_letter(
                chosen_id, line_no, _validation_reason(exc), line,
                _dead_letter_source(line, parsed),
            )
            dead_added += 1
            continue

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
        if storage.insert_record(chosen_id, row):
            inserted += 1
        else:
            duplicates += 1

    valid_count = storage.count_records(chosen_id)
    bad_count = storage.count_dead_letters(chosen_id)
    total_lines = (int(existing["total_lines"]) if existing else 0) + physical_lines
    batch = {
        "batch_id": chosen_id,
        "name": name if name is not None else (existing["name"] if existing else None),
        "input_hash": input_hash,
        "created_at": existing["created_at"] if existing else now,
        "updated_at": now,
        "total_lines": total_lines,
        "valid_lines": valid_count,
        "bad_lines": bad_count,
        "status": "ingested",
    }
    storage.save_batch(batch)
    result = dict(batch)
    result.update({
        "created": existing is None,
        "inserted": inserted,
        "duplicates": duplicates,
        "dead_added": dead_added,
    })
    return result

"""Core data models shared by the ingestion, pipeline and API layers."""
from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

PAIR_SEPARATOR = "||"
KEY_SEPARATOR = "::"
BATCH_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")


class RawRecord(BaseModel):
    """A single input bibliographic record after JSON/schema validation."""

    model_config = ConfigDict(extra="ignore", str_strip_whitespace=True)

    record_id: str = Field(min_length=1, max_length=200)
    source: str = Field(min_length=1, max_length=80)
    title: str = Field(min_length=1, max_length=2000)
    authors: list[str] = Field(default_factory=list, max_length=100)
    venue: str | None = Field(default=None, max_length=500)
    year: int | str | None = None
    volume: int | str | None = None
    issue: int | str | None = None
    pages: int | str | None = None
    doi: str | None = Field(default=None, max_length=500)
    # Evaluation-only annotation emitted by scripts/seed_fixtures.py. It is
    # never used by blocking, scoring or clustering.
    gold_cluster: str | None = Field(default=None, max_length=100)

    @field_validator("record_id", "source", "title", "venue", "doi", mode="before")
    @classmethod
    def _text(cls, value: Any) -> Any:
        if isinstance(value, (int, float)):
            value = str(value)
        if isinstance(value, str):
            value = value.strip()
            return value or None
        return value

    @field_validator("authors", mode="before")
    @classmethod
    def _authors(cls, value: Any) -> list[str]:
        if value is None:
            return []
        if isinstance(value, str):
            value = [value]
        if not isinstance(value, list) or not all(isinstance(v, str) for v in value):
            raise ValueError("authors must be an array of strings")
        return [v.strip() for v in value if v.strip()]


class BatchEnvelope(BaseModel):
    """Optional JSON envelope accepted by POST /batches."""

    jsonl: str = Field(min_length=1)
    batch_id: str | None = None
    name: str | None = None


class RunRequest(BaseModel):
    batch_id: str = Field(min_length=1, max_length=64)


class DecisionRequest(BaseModel):
    batch_id: str = Field(min_length=1, max_length=64)
    pair_key: str = Field(min_length=1, max_length=500)
    decision: Literal["same", "different"]
    reviewer: str = Field(default="anonymous", min_length=1, max_length=100)
    note: str = Field(default="", max_length=2000)


@dataclass(frozen=True)
class Record:
    """A normalized record loaded from SQLite."""

    pk: int
    batch_id: str
    source: str
    record_id: str
    line_no: int
    raw: dict[str, Any]
    norm: dict[str, Any]
    gold_cluster: str | None = None

    @property
    def key(self) -> str:
        return make_record_key(self.source, self.record_id)

    @property
    def title(self) -> str:
        return self.norm.get("title") or ""

    @property
    def authors(self) -> list[dict[str, str]]:
        return list(self.norm.get("authors") or [])

    @property
    def venue(self) -> str | None:
        return self.norm.get("venue")

    @property
    def year(self) -> int | None:
        return self.norm.get("year")

    @property
    def pages(self) -> str | None:
        return self.norm.get("pages")

    @property
    def doi(self) -> str | None:
        return self.norm.get("doi")

    def public(self) -> dict[str, Any]:
        return {
            "record_key": self.key,
            "source": self.source,
            "record_id": self.record_id,
            "raw": self.raw,
            "normalized": self.norm,
            "gold_cluster": self.gold_cluster,
        }


def make_record_key(source: str, record_id: str) -> str:
    return f"{source}{KEY_SEPARATOR}{record_id}"


def pair_order(key: str) -> tuple[str, str]:
    """Order records by record_id first (the documented canonical rule)."""
    if KEY_SEPARATOR in key:
        source, record_id = key.split(KEY_SEPARATOR, 1)
        return record_id, source
    return key, ""


def make_pair_key(key_a: str, key_b: str) -> tuple[str, str, str]:
    """Return (first_key, second_key, pair_key) ordered by record_id, source."""
    first, second = sorted((key_a, key_b), key=pair_order)
    return first, second, f"{first}{PAIR_SEPARATOR}{second}"


def record_from_db(row: Any) -> Record:
    return Record(
        pk=int(row["record_pk"]),
        batch_id=row["batch_id"],
        source=row["source"],
        record_id=row["record_id"],
        line_no=int(row["line_no"]),
        raw=json.loads(row["raw_json"]),
        norm=json.loads(row["norm_json"]),
        gold_cluster=row["gold_cluster"],
    )

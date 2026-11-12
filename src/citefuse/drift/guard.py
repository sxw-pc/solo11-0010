"""Drift guard orchestration: detect, tier, dispose, audit and learn.

The guard is the single entry point the pipeline calls before blocking. It:

1. loads the latest committed baseline snapshot (or runs rule-only cold start);
2. evaluates every record with the configured detector suite;
3. applies minor auto-corrections in memory (originals stay audited);
4. routes medium records to a tagged low-priority lane;
5. moves heavy records to dedicated quarantine storage, out of core matching;
6. persists per-record dispositions and a run-level statistics summary;
7. folds accepted records into a new immutable baseline version;
8. fails open: any internal error degrades to "detection skipped" and never
   interrupts the original pipeline.
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any

from ..audit import record_event
from ..clock import Clock
from ..models import Record
from .baseline import (
    KIND_CATEGORICAL, KIND_NUMERIC, KIND_TEXT, Baseline, CountStats, FieldStat,
    NumericStats, char_ngrams,
)
from .config import TIER_RANK, DriftConfig
from .context import DetectionContext
from .detectors import build_detectors
from .models import DriftSummary, FieldVerdict, RecordDriftResult, resolve_tier
from .store import DriftStore

if TYPE_CHECKING:
    from ..config import AppConfig

NGRAM_SIZE = 3


class DriftGuard:
    def __init__(self, storage: Any, config: AppConfig, clock: Clock) -> None:
        self.storage = storage
        self.config = config
        self.drift_config: DriftConfig = config.drift
        self.clock = clock
        self.store = DriftStore(storage)

    # -- field registry for baseline stats ------------------------------------
    def _field_kinds(self) -> dict[str, str]:
        cfg = self.drift_config
        kinds: dict[str, str] = {}
        if cfg.year.enabled:
            kinds["year"] = KIND_NUMERIC
        if cfg.source.enabled:
            kinds["source"] = KIND_CATEGORICAL
        if cfg.authors.enabled:
            kinds["authors"] = KIND_TEXT
        for name, field_cfg in cfg.generic.items():
            # Shape-only identifiers are validated by regex, not distribution.
            if field_cfg.enabled and field_cfg.kind != "shape":
                kinds[name] = field_cfg.kind
        return kinds

    # -- observation extraction for baseline learning --------------------------
    def _observations(
        self, record: Record, result: RecordDriftResult | None
    ) -> dict[str, Any]:
        """Extract canonical (post-correction) observations for one record."""
        corrections = result.corrected_fields if result is not None else {}
        raw, norm = record.raw, record.norm
        observations: dict[str, Any] = {}

        if "year" in self._field_kinds():
            year = corrections.get("year", norm.get("year"))
            if year is not None:
                observations["year"] = ("numeric", float(year))
        if "source" in self._field_kinds():
            source = corrections.get("source", record.source)
            if source:
                observations["source"] = ("categorical", str(source).upper())
        if "authors" in self._field_kinds():
            authors = corrections.get("authors")
            if authors is None:
                authors = [a.get("family", "") for a in record.authors if a.get("family")]
            if authors:
                observations["authors"] = ("text", " ".join(authors))
        field_kinds = self._field_kinds()
        for name, field_cfg in self.drift_config.generic.items():
            if name not in field_kinds:
                continue  # disabled or shape-only field: no baseline statistic
            value = norm.get(name)
            if value is None:
                value = raw.get(name)
            if value is None:
                continue
            observations[name] = (field_cfg.kind, value)
        return observations

    def _apply_observation(
        self, stats: dict[str, FieldStat], field_name: str, kind: str, value: Any
    ) -> None:
        cfg = self.drift_config.baseline
        stat = stats[field_name]
        if isinstance(stat, NumericStats):
            stat.observe(float(value))
        elif isinstance(stat, CountStats) and stat.kind == KIND_TEXT:
            for gram in char_ngrams(str(value), NGRAM_SIZE):
                stat.observe(gram, cfg.max_profile_features)
        else:
            stat.observe(str(value), cfg.max_categories)

    # -- minor auto-corrections ------------------------------------------------
    @staticmethod
    def _apply_corrections(record: Record, result: RecordDriftResult) -> None:
        """Apply minor fixes in place. Original raw JSON is never mutated.

        ``Record`` is a frozen dataclass, so identity fields change via
        ``object.__setattr__``; only the in-memory pipeline copy is updated
        while the database row and audited raw JSON keep the originals.
        """
        for field_name, suggestion in result.corrected_fields.items():
            if field_name == "year":
                record.norm["year"] = suggestion
            elif field_name == "source":
                record.norm["source_original"] = record.source
                # Identity-level fix: update the in-memory record so blocking,
                # scoring, keys and survivorship all use the canonical source.
                object.__setattr__(record, "source", suggestion)
            elif field_name == "authors":
                personal = " ".join(suggestion).casefold()
                record.norm["authors"] = [
                    author for author in record.norm.get("authors", [])
                    if author.get("family", "").casefold()
                    and author["family"].casefold() in personal
                ]

    @staticmethod
    def _tag_low_priority(record: Record, result: RecordDriftResult) -> None:
        """Mark a medium-drift record for the low-priority lane (in memory)."""
        record.norm["_drift"] = {
            "tier": result.tier,
            "score": result.score,
            "reasons": [v.reason_code for v in result.drift_verdicts()],
        }

    # -- main entry point -------------------------------------------------------
    def guard(
        self, run_id: str, batch_id: str, records: list[Record]
    ) -> dict[str, Any]:
        """Run the full guard. Returns a structured outcome; never raises when
        ``fail_open`` is enabled (the default)."""
        cfg = self.drift_config
        summary = DriftSummary()
        if not cfg.enabled:
            return self._skipped_outcome(run_id, batch_id, records, "drift_guard_disabled")

        try:
            baseline = self.store.load_latest_baseline()
            baseline_version = baseline.version if baseline is not None else 0
            ctx = DetectionContext(baseline, cfg.baseline.min_samples)
            detectors = build_detectors(cfg)

            results: list[RecordDriftResult] = []
            eligible: list[Record] = []
            quarantine_items: list[dict[str, Any]] = []
            absorbed: list[tuple[Record, RecordDriftResult]] = []
            pk_index = {record.key: record.pk for record in records}
            field_weights = {
                name: self.drift_config.field_weight(name)
                for name in self.drift_config.enabled_fields()
            }

            for record in records:
                verdicts: list[FieldVerdict] = []
                for detector in detectors:
                    verdict = detector.check(record.raw, record.norm, ctx)
                    if verdict is not None:
                        # Central tier resolution: detectors only provide a
                        # score and a rule-mandated floor; configured severity
                        # cut-offs decide the final tier.
                        tier = resolve_tier(
                            verdict.score, cfg.severity, verdict.min_tier
                        )
                        object.__setattr__(verdict, "tier", tier)
                        verdicts.append(verdict)
                result = RecordDriftResult(
                    record_key=record.key, batch_id=batch_id,
                    source=record.source, record_id=record.record_id,
                    verdicts=verdicts, baseline_version=baseline_version,
                    field_weights=field_weights,
                )
                results.append(result)
                summary.add(result)

                if result.is_quarantined:
                    quarantine_items.append(self._quarantine_item(record, result))
                    continue  # heavy drift never reaches the core pipeline

                if result.tier == "minor":
                    self._apply_corrections(record, result)
                elif result.is_low_priority:
                    self._tag_low_priority(record, result)
                if result.tier in ("minor", "clean"):
                    absorbed.append((record, result))
                eligible.append(record)

            summary.baseline_version = baseline_version
            self._persist(
                run_id, batch_id, results, quarantine_items, summary,
                baseline_version, pk_index,
            )

            new_version = baseline_version
            if cfg.baseline.update_on_run and absorbed:
                # Throttle baseline rewrites: only promote a new immutable
                # version when the run contributes a meaningful number of new
                # observations; tiny/no-op runs reuse the current snapshot.
                previous_count = (
                    baseline.stats and min(
                        (s.count for s in baseline.stats.values()), default=0
                    )
                ) if baseline is not None else 0
                promote_threshold = max(
                    1, min(cfg.baseline.min_samples, max(1, previous_count // 10))
                )
                if baseline is None or len(absorbed) >= promote_threshold:
                    new_version = self._promote_baseline(
                        baseline, batch_id, absorbed
                    )

            record_event(
                self.storage, self.clock, batch_id, run_id, "drift_guard_completed",
                {"baseline_version": baseline_version,
                 "new_baseline_version": new_version,
                 "summary": summary.as_dict()},
            )
            if cfg.audit_per_record:
                for result in results:
                    record_event(
                        self.storage, self.clock, batch_id, run_id,
                        "drift_record_disposition", result.as_dict(),
                    )

            return {
                "status": "ok",
                "eligible_records": eligible,
                "quarantined_keys": [item["record_key"] for item in quarantine_items],
                "low_priority_keys": [
                    r.record_key for r in results if r.is_low_priority
                ],
                "results": results,
                "summary": summary.as_dict(),
                "baseline_version": baseline_version,
                "new_baseline_version": new_version,
                "degraded": False,
            }
        except Exception as exc:  # noqa: BLE001 - intentional fail-open boundary
            if not cfg.fail_open:
                raise
            degraded = DriftSummary(
                total_records=len(records), degraded=True,
                degradation_reason=f"{type(exc).__name__}: {exc}",
            )
            # Auditing the degradation must itself never block the pipeline.
            try:
                record_event(
                    self.storage, self.clock, batch_id, run_id,
                    "drift_guard_degraded",
                    {"error": type(exc).__name__, "detail": str(exc)},
                )
            except Exception:  # noqa: BLE001 - audit best-effort
                pass
            return {
                "status": "degraded",
                "eligible_records": list(records),
                "quarantined_keys": [],
                "low_priority_keys": [],
                "results": [],
                "summary": degraded.as_dict(),
                "baseline_version": 0,
                "new_baseline_version": 0,
                "degraded": True,
            }

    # -- helpers -----------------------------------------------------------------
    def _skipped_outcome(
        self, run_id: str, batch_id: str, records: list[Record], reason: str
    ) -> dict[str, Any]:
        summary = DriftSummary(
            total_records=len(records), clean=len(records),
            degraded=False, degradation_reason=reason,
        )
        record_event(
            self.storage, self.clock, batch_id, run_id, "drift_guard_skipped",
            {"reason": reason, "records": len(records)},
        )
        return {
            "status": "skipped",
            "eligible_records": list(records),
            "quarantined_keys": [],
            "low_priority_keys": [],
            "results": [],
            "summary": summary.as_dict(),
            "baseline_version": 0,
            "new_baseline_version": 0,
            "degraded": False,
        }

    def _quarantine_item(
        self, record: Record, result: RecordDriftResult
    ) -> dict[str, Any]:
        return {
            "record_key": record.key,
            "record_pk": record.pk,
            "tier": result.tier,
            "score": result.score,
            "reasons": [v.as_dict() for v in result.drift_verdicts()],
            "raw": record.raw,
            "norm": record.norm,
            "baseline_version": result.baseline_version,
        }

    def _persist(
        self,
        run_id: str,
        batch_id: str,
        results: list[RecordDriftResult],
        quarantine_items: list[dict[str, Any]],
        summary: DriftSummary,
        baseline_version: int,
        pk_index: dict[str, int],
    ) -> None:
        now = self.clock.now_iso()
        # Only non-clean dispositions are stored per record. A normal no-drift
        # batch therefore writes no disposition rows; its clean count still
        # appears in the run summary (and the summary + audit event are the
        # traceable evidence that every record was evaluated and passed).
        disposition_rows = [
            {
                "record_key": r.record_key,
                "record_pk": pk_index.get(r.record_key),
                "tier": r.tier,
                "score": r.score,
                "action": r.action,
                "baseline_version": r.baseline_version,
                "corrected_fields": r.corrected_fields,
                "verdicts": [v.as_dict() for v in r.verdicts],
            }
            for r in results if r.tier != "clean"
        ]
        # Single-transaction, single-commit persistence of all run artifacts.
        self.store.persist_run_artifacts(
            run_id, batch_id, disposition_rows, quarantine_items,
            summary.as_dict(), baseline_version, False, now,
        )

    def _promote_baseline(
        self,
        previous: Baseline | None,
        batch_id: str,
        absorbed: list[tuple[Record, RecordDriftResult]],
    ) -> int:
        kinds = self._field_kinds()
        stats: dict[str, FieldStat] = {}
        if previous is not None:
            # Deep-copy via (de)serialization so the old snapshot is untouched.
            for name, stat in previous.stats.items():
                if isinstance(stat, NumericStats):
                    stats[name] = NumericStats.from_dict(stat.to_dict())
                else:
                    stats[name] = CountStats.from_dict(stat.to_dict())
        for name, kind in kinds.items():
            if name not in stats:
                if kind == KIND_NUMERIC:
                    stats[name] = NumericStats()
                elif kind == KIND_TEXT:
                    stats[name] = CountStats(kind=KIND_TEXT)
                else:
                    stats[name] = CountStats(kind=KIND_CATEGORICAL)

        absorbed_count = 0
        for record, result in absorbed:
            if TIER_RANK[result.tier] > TIER_RANK[self.drift_config.baseline.absorb_max_tier]:
                continue
            for field_name, (kind, value) in self._observations(record, result).items():
                self._apply_observation(stats, field_name, kind, value)
            absorbed_count += 1

        return self.store.promote_baseline(
            stats, source_batch_id=batch_id,
            absorbed_records=absorbed_count, created_at=self.clock.now_iso(),
        )

    # -- explicit baseline bootstrap -------------------------------------------
    def bootstrap_from_records(
        self, records: list[Record], source_batch_id: str | None = None
    ) -> int:
        """Build the first baseline from historical accepted records.

        Used when an installation already holds processed batches: scans their
        records and treats them as the reference convention.
        """
        stats: dict[str, FieldStat] = {}
        kinds = self._field_kinds()
        for name, kind in kinds.items():
            if kind == KIND_NUMERIC:
                stats[name] = NumericStats()
            elif kind == KIND_TEXT:
                stats[name] = CountStats(kind=KIND_TEXT)
            else:
                stats[name] = CountStats(kind=KIND_CATEGORICAL)
        count = 0
        for record in records:
            observations = self._observations(record, None)
            if not observations:
                continue
            for field_name, (kind, value) in observations.items():
                self._apply_observation(stats, field_name, kind, value)
            count += 1
        return self.store.promote_baseline(
            stats, source_batch_id=source_batch_id,
            absorbed_records=count, created_at=self.clock.now_iso(),
        )

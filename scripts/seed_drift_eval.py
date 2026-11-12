"""Generate a fully fictional dataset mixing every drift scenario.

The dataset is split into:

* ``history`` -- clean, convention-following records used to build the
  validation baseline;
* ``eval``    -- labelled records covering clean data plus minor / medium /
  heavy semantic drift across year, source, authors and DOI fields.

Every name, organization, venue and identifier is synthetic. The generator is
deterministic for a fixed seed and writes JSONL plus a labels manifest.
"""
from __future__ import annotations

import argparse
import json
import random
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]

SOURCES = ["ORBIT", "LANTERN", "MEADOW", "COMPASS"]
SOURCE_ORG = {
    "ORBIT": "Orbital Bibliographic Exchange",
    "LANTERN": "Lantern Literature Aggregator",
    "MEADOW": "Meadow Metadata Consortium",
    "COMPASS": "Compass Citation Cooperative",
}
FAMILIES = ["Vaelwyn", "Kestrel", "Marlow", "Pemberton", "Ashford", "Bellamy"]
GIVENS = ["Rowan", "Sable", "Wren", "Caspian", "Elowen", "Thorne"]
VENUES = [
    "Journal of Fictional Evidence Synthesis",
    "Imaginary Research Methods Quarterly",
    "Proceedings of the Synthetic Evidence Conference",
]
COLLECTIVES = [
    "Fictional Cardiology Consortium",
    "Imaginary Diagnostics Task Force",
    "Synthetic Public Health Working Group",
    "虚构医学协作组",
]


def _person(rng: random.Random) -> str:
    return f"{rng.choice(FAMILIES)}, {rng.choice(GIVENS)}"


def _doi(index: int) -> str:
    return f"10.5555/driftval.{index:05d}"


def _clean(rng: random.Random, index: int) -> dict[str, Any]:
    year = rng.randint(1998, 2024)
    return {
        "record_id": f"E{index:05d}",
        "source": rng.choice(SOURCES),
        "title": f"Fabricated investigation number {index} into synthetic science",
        "authors": [_person(rng), _person(rng)],
        "venue": rng.choice(VENUES),
        "year": year,
        "volume": rng.randint(1, 60),
        "issue": rng.randint(1, 4),
        "pages": f"{100 + index % 400}-{110 + index % 400}",
        "doi": _doi(index),
    }


def _drift_year_minor(rng: random.Random, base: dict[str, Any]) -> dict[str, Any]:
    record = dict(base)
    record["year"] = f"c. {base['year']}"  # circa-prefixed, auto-correctable
    return record


def _drift_year_medium(rng: random.Random, base: dict[str, Any]) -> dict[str, Any]:
    record = dict(base)
    record["year"] = "民国八十三年"  # regnal era, convertible -> medium
    return record


def _drift_year_heavy(rng: random.Random, base: dict[str, Any]) -> dict[str, Any]:
    record = dict(base)
    record["year"] = rng.choice(["回历1420年", "上古年间", "干支丙午年"])  # unconvertible
    return record


def _drift_source_minor(rng: random.Random, base: dict[str, Any]) -> dict[str, Any]:
    record = dict(base)
    record["source"] = SOURCE_ORG[base["source"]]  # full org name -> alias fix
    return record


def _drift_source_medium(rng: random.Random, base: dict[str, Any]) -> dict[str, Any]:
    record = dict(base)
    record["source"] = rng.choice([
        "Uncatalogued Regional Medical Library",
        "Independent Fictional Studies Bureau",
        "NEWATLAS",
    ])
    return record


def _drift_source_heavy(rng: random.Random, base: dict[str, Any]) -> dict[str, Any]:
    record = dict(base)
    record["source"] = rng.choice(["### !!!", "???%%%", "@@@ ###"])
    return record


def _drift_authors_minor(rng: random.Random, base: dict[str, Any]) -> dict[str, Any]:
    record = dict(base)
    record["authors"] = [base["authors"][0], rng.choice(COLLECTIVES)]
    return record


def _drift_authors_medium(rng: random.Random, base: dict[str, Any]) -> dict[str, Any]:
    record = dict(base)
    record["authors"] = [rng.choice(COLLECTIVES)]  # collective only
    return record


def _drift_authors_heavy(rng: random.Random, base: dict[str, Any]) -> dict[str, Any]:
    record = dict(base)
    record["authors"] = ["@@@###", "!!!$$$"]
    return record


def _drift_doi_heavy(rng: random.Random, base: dict[str, Any]) -> dict[str, Any]:
    record = dict(base)
    record["doi"] = rng.choice(["not-a-doi", "DOI://missing-prefix", "###"])
    return record


# (label, tier, mutator)
SCENARIOS = [
    ("clean", "clean", None),
    ("year_era_modern", "minor", _drift_year_minor),
    ("year_era_imperial", "medium", _drift_year_medium),
    ("year_era_unconverted", "heavy", _drift_year_heavy),
    ("source_org_alias", "minor", _drift_source_minor),
    ("source_unknown_org", "medium", _drift_source_medium),
    ("source_garbage", "heavy", _drift_source_heavy),
    ("authors_collective_mixed", "minor", _drift_authors_minor),
    ("authors_collective_only", "medium", _drift_authors_medium),
    ("authors_garbage", "heavy", _drift_authors_heavy),
    ("doi_malformed", "heavy", _drift_doi_heavy),
]


def generate(seed: int = 20261112, history_size: int = 120,
             per_scenario: int = 20) -> tuple[list[dict], list[dict], dict]:
    rng = random.Random(seed)
    history = [_clean(rng, 10000 + i) for i in range(history_size)]

    eval_records: list[dict] = []
    labels: list[dict] = []
    index = 0
    for scenario, tier, mutator in SCENARIOS:
        for _ in range(per_scenario):
            base = _clean(rng, index)
            record = mutator(rng, base) if mutator else base
            eval_records.append(record)
            labels.append({
                "record_id": record["record_id"],
                "expected_tier": tier,
                "scenario": scenario,
            })
            index += 1
    rng.shuffle(eval_records)
    order = {r["record_id"]: i for i, r in enumerate(eval_records)}
    labels.sort(key=lambda item: order[item["record_id"]])
    manifest = {
        "seed": seed,
        "history_count": len(history),
        "eval_count": len(eval_records),
        "per_scenario": per_scenario,
        "scenarios": [{"label": s, "tier": t} for s, t, _ in SCENARIOS],
        "labels": labels,
    }
    return history, eval_records, manifest


def write_dataset(output_dir: Path, seed: int = 20261112) -> dict[str, Path]:
    history, eval_records, manifest = generate(seed)
    output_dir.mkdir(parents=True, exist_ok=True)
    paths = {
        "history": output_dir / "drift_history.jsonl",
        "eval": output_dir / "drift_eval.jsonl",
        "labels": output_dir / "drift_labels.json",
    }
    for key, rows in (("history", history), ("eval", eval_records)):
        lines = [json.dumps(r, ensure_ascii=False, sort_keys=True) for r in rows]
        paths[key].write_text("\n".join(lines) + "\n", encoding="utf-8")
    paths["labels"].write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return paths


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate the drift validation dataset")
    parser.add_argument("--seed", type=int, default=20261112)
    parser.add_argument("--out-dir", type=Path, default=ROOT / "data" / "drift_eval")
    args = parser.parse_args()
    paths = write_dataset(args.out_dir, args.seed)
    for key, path in paths.items():
        print(f"wrote {key}: {path}")


if __name__ == "__main__":
    main()

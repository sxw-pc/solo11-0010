"""Generate a deterministic, fully fictional bibliographic fixture.

Default output: data/fixtures/records.jsonl + gold.json.
200 records = 40 injected duplicate groups (3 records each) + 80 singletons.
All names, venues, topics and DOIs are synthetic. No network is used.
"""
from __future__ import annotations

import argparse
import json
import random
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[1]
GROUPS = 40
MEMBERS_PER_GROUP = 3
SINGLETONS = 80
SOURCES = ["ORBIT", "LANTERN", "MEADOW", "COMPASS"]
MOTIFS = ["cohort", "ledger", "atlas", "compass", "prism", "harbor"]
DOMAINS = [
    "fabricated cardiology", "invented nutrition", "synthetic epidemiology",
    "imaginary rehabilitation", "fictional diagnostics", "simulated pediatrics",
]
FAMILIES = [
    "Zorvath", "Quellmar", "Branteth", "Dunwellyn", "Silvaris",
    "Orbain", "Calyndor", "Tavren", "Marrowyn", "Pellucid",
]
GIVENS = [
    "Vaelis", "Tirune", "Soreth", "Mavrik", "Liosha",
    "Ondrei", "Paveth", "Iryssa", "Nerune", "Solvane",
]


def _topic(index: int) -> str:
    starts = ["zor", "quel", "brin", "dun", "sil", "orb", "cal", "tav", "mar", "pel"]
    ends = ["vane", "mera", "teth", "wryn", "lith", "bain", "dros", "quen", "veth", "sola"]
    return f"{starts[index % len(starts)]}{ends[(index // len(starts)) % len(ends)]}{index:03d}"


def _title(index: int) -> str:
    return (
        f"{_topic(index).title()} {MOTIFS[index % len(MOTIFS)]}: "
        f"a fabricated study of {DOMAINS[index % len(DOMAINS)]}"
    )


def _authors(group: int, quantity: int = 3) -> list[tuple[str, str]]:
    return [
        (FAMILIES[(group + offset * 3) % len(FAMILIES)],
         GIVENS[(group * 2 + offset) % len(GIVENS)])
        for offset in range(quantity)
    ]


def _format_author(family: str, given: str, style: int) -> str:
    if style == 0:
        return f"{given} {family}"
    if style == 1:
        return f"{family.upper()}, {given.upper()}"
    return f"{family}, {given}"


def _fullwidth(text: str) -> str:
    chars = []
    for char in text:
        if "!" <= char <= "~":
            chars.append(chr(ord(char) + 0xFEE0))
        elif char == " ":
            chars.append("　")
        else:
            chars.append(char)
    return "".join(chars)


def _load_venues() -> list[tuple[str, list[str]]]:
    with (ROOT / "configs" / "default.yaml").open("r", encoding="utf-8") as handle:
        config = yaml.safe_load(handle)
    aliases = config["normalization"]["venue_aliases"]
    return [(canonical, list(values)) for canonical, values in aliases.items()]


def _duplicate_group(
    group: int, rng: random.Random, venues: list[tuple[str, list[str]]]
) -> list[dict[str, Any]]:
    title = _title(200 + group)
    people = _authors(group)
    venue, aliases = venues[group % len(venues)]
    year = 1998 + (group * 7) % 27
    volume = 10 + group % 40
    issue = 1 + group % 4
    start = 100 + group * 3
    end = start + 10
    doi = f"10.5555/citefuse.{group:04d}"
    records = []
    for variant in range(MEMBERS_PER_GROUP):
        source = SOURCES[(group + variant) % len(SOURCES)]
        ordered = people if variant != 1 else [people[2], people[0], people[1]]
        author_style = variant
        formatted = [
            _format_author(family, given, author_style) for family, given in ordered
        ]
        if variant == 2:
            formatted = [_fullwidth(value) for value in formatted]
        display_title = title.upper() if variant == 1 else (
            _fullwidth(title) if variant == 2 else title
        )
        display_venue = venue if variant == 0 else aliases[variant % len(aliases)]
        pages = [f"{start}-{end}", f"{start}–{end}", f"pp. {start}-{str(end)[1:]}"][variant]
        raw_year: int | str = [year, f"c. {year}", str(year)][variant]
        records.append({
            "record_id": f"G{group:03d}{'ABC'[variant]}",
            "source": source,
            "title": display_title,
            "authors": formatted,
            "venue": display_venue,
            "year": raw_year,
            "volume": volume if variant != 1 else str(volume),
            "issue": issue,
            "pages": pages,
            "doi": None if variant == 2 else (
                doi if variant == 0 else f"https://doi.org/{doi.upper()}"
            ),
            "gold_cluster": f"G{group + 1:03d}",
        })
    rng.shuffle(records)
    return records


def _singleton(
    index: int, rng: random.Random, venues: list[tuple[str, list[str]]]
) -> dict[str, Any]:
    title = _title(index)
    people = _authors(index + 100)
    rng.shuffle(people)
    venue = venues[index % len(venues)][0]
    year = 1998 + (index * 11 + 3) % 27
    start = 300 + index * 2
    return {
        "record_id": f"U{index:03d}",
        "source": SOURCES[index % len(SOURCES)],
        "title": title.title() if index % 2 else title,
        "authors": [_format_author(family, given, index % 3) for family, given in people],
        "venue": venue,
        "year": year,
        "volume": 5 + index % 30,
        "issue": 1 + index % 3,
        "pages": f"{start}-{start + 8}",
        "doi": f"10.5555/citefuse.u{index:04d}",
        "gold_cluster": f"U{index + 1:03d}",
    }


def generate(seed: int) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    rng = random.Random(seed)
    venues = _load_venues()
    records: list[dict[str, Any]] = []
    for group in range(GROUPS):
        records.extend(_duplicate_group(group, rng, venues))
    for index in range(SINGLETONS):
        records.append(_singleton(index, rng, venues))
    rng.shuffle(records)
    labels = {
        f"{record['source']}::{record['record_id']}": record["gold_cluster"]
        for record in records
    }
    clusters: dict[str, list[str]] = {}
    for record_key, label in sorted(labels.items()):
        clusters.setdefault(label, []).append(record_key)
    gold = {
        "seed": seed,
        "record_count": len(records),
        "injected_duplicate_groups": GROUPS,
        "gold_cluster_count": len(clusters),
        "labels": labels,
        "clusters": clusters,
    }
    return records, gold


def write_fixtures(seed: int, output_dir: Path) -> tuple[Path, Path]:
    records, gold = generate(seed)
    output_dir.mkdir(parents=True, exist_ok=True)
    records_path = output_dir / "records.jsonl"
    gold_path = output_dir / "gold.json"
    lines = [
        json.dumps(record, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        for record in records
    ]
    records_path.write_bytes(("\n".join(lines) + "\n").encode("utf-8"))
    gold_path.write_bytes(
        (json.dumps(gold, ensure_ascii=False, sort_keys=True, indent=2) + "\n").encode("utf-8")
    )
    return records_path, gold_path


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate deterministic citefuse fixtures")
    parser.add_argument("--seed", type=int, default=20240517)
    parser.add_argument("--out-dir", type=Path, default=ROOT / "data" / "fixtures")
    args = parser.parse_args()
    records_path, gold_path = write_fixtures(args.seed, args.out_dir)
    print(f"wrote {records_path}")
    print(f"wrote {gold_path}")


if __name__ == "__main__":
    main()

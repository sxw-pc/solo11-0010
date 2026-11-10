from __future__ import annotations

from citefuse.models import Record
from citefuse.survivorship import build_golden


def _record(pk, source, title, venue, year, authors, volume, pages, doi):
    norm = {
        "title": title,
        "authors": authors,
        "venue": venue,
        "year": year,
        "volume": volume,
        "issue": "1",
        "pages": pages,
        "doi": doi,
    }
    return Record(pk, "b", source, f"R{pk}", pk, {}, norm, None)


def _author(key):
    family = key.split("|")[0]
    return {"family": family, "given": "", "key": key}


def test_all_survivorship_rules(config):
    records = [
        _record(1, "ORBIT", "short", "venue x", 2001, [_author("a|")],
                "3", "10-20", "10.1/orbit"),
        _record(2, "LANTERN", "a much longer title", "venue y", 1999,
                [_author("a|"), _author("b|")], "3", "10-200", "10.1/lantern"),
        _record(3, "MEADOW", "medium title", "venue x", 2002,
                [_author("a|"), _author("c|")], "4", "10-30", "10.1/meadow"),
    ]
    golden = build_golden(records, config)
    assert golden["title"] == "a much longer title"
    assert golden["venue"] == "venue x"
    assert golden["year"] == 1999
    assert golden["doi"] == "10.1/orbit"
    assert golden["volume"] == "3; 4"
    assert golden["pages"] == "10-200"
    assert [author["key"] for author in golden["authors"]] == ["a|", "b|", "c|"]
    assert golden["member_count"] == 3
    assert golden["provenance"]["doi"] == ["ORBIT::R1"]
    assert golden["provenance"]["title"] == ["LANTERN::R2"]


def test_ties_use_source_priority_then_record_key(config):
    records = [
        _record(1, "MEADOW", "same", "venue", 2000, [], None, None, None),
        _record(2, "ORBIT", "same", "venue", 2000, [], None, None, None),
    ]
    golden = build_golden(records, config)
    assert golden["title"] == "same"
    assert golden["provenance"]["title"] == ["ORBIT::R2"]


def test_golden_requires_members(config):
    import pytest

    with pytest.raises(ValueError):
        build_golden([], config)

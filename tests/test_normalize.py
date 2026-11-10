from __future__ import annotations

import pytest

from citefuse.config import DEFAULT_CONFIG_PATH, PACKAGE_CONFIG_PATH
from citefuse.models import RawRecord
from citefuse.normalize import (
    NormalizationError, Normalizer, normalize_doi, normalize_pages, normalize_year,
    parse_person,
)


def test_packaged_config_stays_in_sync():
    assert PACKAGE_CONFIG_PATH.read_bytes() == DEFAULT_CONFIG_PATH.read_bytes()


def test_title_folds_fullwidth_case_punctuation_and_whitespace(config):
    normalizer = Normalizer(config.normalization)
    assert normalizer.title("Ａ  Fictional: STUDY!!!") == "a fictional study"


def test_author_formats_are_equivalent(config):
    cfg = config.normalization
    first = parse_person("Vaelis Zorvath", cfg)
    second = parse_person("ZORVATH, Vaelis", cfg)
    assert first == second == {"family": "zorvath", "given": "vaelis", "key": "zorvath|v"}


def test_author_fullwidth_and_single_name(config):
    cfg = config.normalization
    assert parse_person("Ｚｏｒｖａｔｈ，　Ｖａｅｌｉｓ", cfg)["key"] == "zorvath|v"
    assert parse_person("Quellmar", cfg) == {
        "family": "quellmar", "given": "", "key": "quellmar|"
    }


def test_venue_aliases_and_canonicalization(config):
    normalizer = Normalizer(config.normalization)
    expected = "journal of fictional evidence synthesis"
    assert normalizer.venue("JFES") == expected
    assert normalizer.venue("J. Fict. Evid. Synth.") == expected
    assert normalizer.venue("Journal of Fictional Evidence Synthesis") == expected


def test_year_pages_doi_normalization():
    assert normalize_year("c. 2020") == 2020
    assert normalize_year(1999) == 1999
    assert normalize_year("n.d.") is None
    assert normalize_year(1400) is None
    assert normalize_pages("pp. 129–35") == "129-135"
    assert normalize_pages("123-129") == "123-129"
    assert normalize_pages("page 12") == "12"
    assert normalize_pages("n/a") is None
    assert normalize_doi("  https://doi.org/10.5555/ABC.01  ") == "10.5555/abc.01"
    assert normalize_doi("DOI: 10.5555/X") == "10.5555/x"
    assert normalize_doi(None) is None


def test_normalize_record_keeps_nullable_fields(config):
    normalizer = Normalizer(config.normalization)
    raw = RawRecord.model_validate({
        "record_id": "R1", "source": "orbit", "title": "Ｈｅｌｌｏ, World!",
        "authors": ["Vaelis Zorvath"], "venue": "JFES", "year": "2021",
        "volume": 3, "issue": "Suppl. 2", "pages": "10-12", "doi": "doi:10.1/A",
    })
    normalized = normalizer.normalize(raw)
    assert normalized["title"] == "hello world"
    assert normalized["venue"] == "journal of fictional evidence synthesis"
    assert normalized["year"] == 2021
    assert normalized["volume"] == "3"
    assert normalized["issue"] == "suppl 2"
    assert normalized["pages"] == "10-12"
    assert normalized["doi"] == "10.1/a"


def test_empty_title_after_normalization_is_an_error(config):
    normalizer = Normalizer(config.normalization)
    with pytest.raises(NormalizationError):
        normalizer.title("  !!!  ")

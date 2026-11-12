"""Field-level semantic drift detectors.

Each detector inspects one field against (a) explicit semantic rules and
(b) the historical baseline distribution. It returns either a
:class:`FieldVerdict` carrying a continuous drift score plus an optional
rule-mandated ``min_tier`` floor, or ``None`` when the field is clean. The
guard resolves the final tier centrally from the configured severity
thresholds, so all cut-offs remain YAML-tunable. Detectors are pure,
deterministic and offline; no network or ML model is used.

The three headline scenarios map onto dedicated detectors:

* :class:`YearDetector`   -- ancient/regnal era strings mixed into ``year``;
* :class:`SourceDetector` -- organization full names replacing source codes;
* :class:`AuthorDetector` -- collective/corporate bodies mixed into ``authors``.

:class:`GenericDetector` adds configurable pattern / categorical / numeric /
text-profile checks for any other field (``doi`` by default).
"""
from __future__ import annotations

import re
from typing import Any

from .baseline import char_ngrams
from .config import (
    AuthorDriftConfig, GenericDriftConfig, SourceDriftConfig, YearDriftConfig,
)
from .context import DetectionContext
from .models import FieldVerdict

# --- Chinese numeral / era handling -----------------------------------------
_CN_DIGITS = {
    "零": 0, "〇": 0, "一": 1, "二": 2, "两": 2, "三": 3, "四": 4, "五": 5,
    "六": 6, "七": 7, "八": 8, "九": 9,
}
_CN_UNITS = {"十": 10, "百": 100, "千": 1000}

# Recognized calendar/regnal era systems. The offset converts era year n into
# a Gregorian year via ``offset + n``. Systems without a fixed offset (lunar
# calendars, sexagenary cycles) are recognized as eras but not auto-converted.
ERA_SYSTEMS: dict[str, dict[str, Any]] = {
    "民国": {"offset": 1911, "convertible": True},
    "民國": {"offset": 1911, "convertible": True},
    "宣统": {"offset": 1908, "convertible": True},
    "宣統": {"offset": 1908, "convertible": True},
    "光绪": {"offset": 1874, "convertible": True},
    "光緒": {"offset": 1874, "convertible": True},
    "同治": {"offset": 1861, "convertible": True},
    "平成": {"offset": 1988, "convertible": True},
    "令和": {"offset": 2018, "convertible": True},
    "昭和": {"offset": 1925, "convertible": True},
    "皇纪": {"offset": -660, "convertible": True},
    "皇紀": {"offset": -660, "convertible": True},
    "佛历": {"offset": -543, "convertible": True},
    "佛曆": {"offset": -543, "convertible": True},
    "回历": {"offset": None, "convertible": False},
    "回歷": {"offset": None, "convertible": False},
    "伊斯兰历": {"offset": None, "convertible": False},
    "伊斯蘭歷": {"offset": None, "convertible": False},
    "干支": {"offset": None, "convertible": False},
    "公元前": {"offset": None, "convertible": False},
}
ERA_MARKERS = sorted(ERA_SYSTEMS, key=len, reverse=True)
_ERA_RE = re.compile(
    r"(" + "|".join(re.escape(m) for m in ERA_MARKERS) + r")\s*"
    r"([0-9]{1,4}|[零〇一二三四五六七八九十百千两]{1,8})\s*年?"
)
_FOUR_DIGIT_RE = re.compile(r"(?<!\d)(\d{4})(?!\d)")
_TWO_DIGIT_RE = re.compile(r"^\D*(\d{1,2})\D*$")
_CJK_RE = re.compile(r"[一-鿿]")


def parse_chinese_number(text: str) -> int | None:
    """Parse simple Chinese numerals up to 9999, e.g. 八十三 -> 83."""
    if not text:
        return None
    if text.isdigit():
        return int(text)
    section = 0
    current = 0
    saw_unit = False
    for char in text:
        if char in _CN_DIGITS:
            current = _CN_DIGITS[char]
        elif char in _CN_UNITS:
            saw_unit = True
            section += (current or 1) * _CN_UNITS[char]
            current = 0
        else:
            return None
    value = section + current
    return value if saw_unit or value > 0 else None


def _alnum_ratio(text: str) -> float:
    compact = text.replace(" ", "")
    if not compact:
        return 0.0
    return sum(1 for ch in compact if ch.isalnum()) / len(compact)


def _verdict(
    field_name: str, score: float, reason_code: str, message: str,
    observed: Any, detector: str, min_tier: str = "minor",
    normalized: Any = None, suggestion: Any = None,
) -> FieldVerdict:
    return FieldVerdict(
        field_name=field_name, score=score, reason_code=reason_code,
        message=message, observed=observed, min_tier=min_tier,
        normalized=normalized, suggestion=suggestion, detector=detector,
    )


class YearDetector:
    """Detect calendar-era and out-of-window values in the year field."""

    name = "year"

    def __init__(self, cfg: YearDriftConfig) -> None:
        self.cfg = cfg

    def check(
        self, raw: dict[str, Any], norm: dict[str, Any], ctx: DetectionContext
    ) -> FieldVerdict | None:
        value = raw.get("year")
        if value is None or isinstance(value, bool):
            return None
        cfg = self.cfg

        if isinstance(value, int):
            return self._check_numeric(value, value, ctx)

        text = str(value).strip()
        if not text:
            return None

        era_match = _ERA_RE.search(text)
        if era_match:
            return self._check_era(text, era_match, ctx)

        four = _FOUR_DIGIT_RE.search(text)
        if four and not _CJK_RE.search(text):
            year = int(four.group(1))
            if text.strip().isdigit():
                return self._check_numeric(year, value, ctx)
            # Circa / punctuation noise around an otherwise valid modern year.
            outlier = self._outlier_verdict(year, value, ctx)
            score = max(cfg.score_era_modern, outlier.score if outlier else 0.0)
            return _verdict(
                self.name, score, "era_modern",
                f"年份含非标准修饰文本，可归一为 {year}", value, self.name,
                min_tier="minor", normalized=year, suggestion=year,
            )

        two = _TWO_DIGIT_RE.fullmatch(text)
        if two and not _CJK_RE.search(text):
            yy = int(two.group(1))
            year = 1900 + yy if yy >= cfg.two_digit_pivot else 2000 + yy
            return _verdict(
                self.name, cfg.score_two_digit, "two_digit_year",
                f"两位年份简写，按枢轴 {cfg.two_digit_pivot} 归一为 {year}",
                value, self.name, min_tier="minor",
                normalized=year, suggestion=year,
            )

        # Letters/CJK with no parseable modern year.
        return _verdict(
            self.name, cfg.score_unrecognized, "era_unrecognized",
            "年份字段出现无法识别的纪年/文本表述，无法安全归一",
            value, self.name, min_tier="heavy",
        )

    def _check_era(
        self, text: str, match: re.Match[str], ctx: DetectionContext
    ) -> FieldVerdict:
        cfg = self.cfg
        marker, number_text = match.group(1), match.group(2)
        system = ERA_SYSTEMS[marker]
        era_year = parse_chinese_number(number_text)
        if era_year is None or not system["convertible"]:
            return _verdict(
                self.name, cfg.score_unrecognized, "era_unconverted",
                f"年份使用无法直接换算的纪年体系「{marker}」，需人工裁定",
                text, self.name, min_tier="heavy",
            )
        gregorian = int(system["offset"]) + era_year
        if not cfg.min_year <= gregorian <= cfg.max_year:
            return _verdict(
                self.name, cfg.score_unrecognized, "era_out_of_window",
                f"纪年「{marker}{number_text}」换算后 {gregorian} 超出可信年份窗口",
                text, self.name, min_tier="heavy", normalized=gregorian,
            )
        outlier = self._outlier_verdict(gregorian, text, ctx)
        score = max(cfg.score_era_imperial, outlier.score if outlier else 0.0)
        return _verdict(
            self.name, score, "era_imperial",
            f"年份使用纪年「{marker}{number_text}」，可换算为公历 {gregorian}，与历史批次约定不一致",
            text, self.name, min_tier="medium",
            normalized=gregorian, suggestion=gregorian,
        )

    def _check_numeric(
        self, year: int, observed: Any, ctx: DetectionContext
    ) -> FieldVerdict | None:
        cfg = self.cfg
        if not cfg.min_year <= year <= cfg.max_year:
            return _verdict(
                self.name, cfg.score_unrecognized, "year_out_of_window",
                f"年份 {year} 超出配置窗口 [{cfg.min_year}, {cfg.max_year}]",
                observed, self.name, min_tier="heavy", normalized=year,
            )
        return self._outlier_verdict(year, observed, ctx)

    def _outlier_verdict(
        self, year: int, observed: Any, ctx: DetectionContext
    ) -> FieldVerdict | None:
        cfg = self.cfg
        if not ctx.has_baseline(self.name):
            return None
        stats = ctx.baseline.numeric(self.name)  # type: ignore[union-attr]
        if stats is None:
            return None
        stddev = stats.stddev()
        if stddev <= 0 or stats.mean is None:
            return None
        if abs(year - stats.mean) > cfg.outlier_sigmas * stddev:
            return _verdict(
                self.name, cfg.score_outlier, "year_distribution_outlier",
                f"年份 {year} 偏离历史均值 {stats.mean:.1f} 超过 {cfg.outlier_sigmas} 个标准差",
                observed, self.name, min_tier="medium", normalized=year,
            )
        return None


class SourceDetector:
    """Detect source identifiers replaced by organization full names."""

    name = "source"

    def __init__(self, cfg: SourceDriftConfig) -> None:
        self.cfg = cfg

    def check(
        self, raw: dict[str, Any], norm: dict[str, Any], ctx: DetectionContext
    ) -> FieldVerdict | None:
        value = raw.get("source")
        if not value or not isinstance(value, str) or not value.strip():
            return None
        cfg = self.cfg
        text = value.strip()
        upper = text.upper()

        known = set(cfg.known_codes)
        if ctx.has_baseline(self.name):
            categories = ctx.baseline.categorical(self.name)  # type: ignore[union-attr]
            if categories is not None:
                known |= set(categories.counts)

        if upper in known:
            return None

        alias = cfg.aliases.get(text.casefold())
        if alias is not None:
            return _verdict(
                self.name, cfg.score_alias, "source_alias",
                f"来源为机构全称/别名，可归一为来源代码 {alias}",
                value, self.name, min_tier="minor",
                normalized=alias, suggestion=alias,
            )

        code_shaped = (
            " " not in text and cfg.code_min_len <= len(text) <= cfg.code_max_len
            and text.isalnum() and text.upper() == text
        )
        if _alnum_ratio(text) < 0.30:
            return _verdict(
                self.name, cfg.score_unrecognized, "source_unrecognized",
                "来源标识形态异常，无法识别为任何机构或来源代码",
                value, self.name, min_tier="heavy",
            )
        reason = "source_unknown_code" if code_shaped else "source_organization_name"
        return _verdict(
            self.name, cfg.score_unknown_category, reason,
            "来源标识与历史批次约定不一致（机构全称或未知代码），需打标人工确认",
            value, self.name, min_tier="medium",
        )


class AuthorDetector:
    """Detect collective/corporate authorship and junk author entries."""

    name = "authors"

    def __init__(self, cfg: AuthorDriftConfig) -> None:
        self.cfg = cfg
        self._markers = [m.casefold() for m in cfg.collective_markers]

    def check(
        self, raw: dict[str, Any], norm: dict[str, Any], ctx: DetectionContext
    ) -> FieldVerdict | None:
        value = raw.get("authors")
        if not value or not isinstance(value, list) or not all(
            isinstance(item, str) for item in value
        ):
            return None
        cfg = self.cfg
        entries = [item.strip() for item in value if item.strip()]
        if not entries:
            return None

        collective = [
            item for item in entries
            if any(marker in item.casefold() for marker in self._markers)
        ]
        junk = [
            item for item in entries
            if item not in collective
            and _alnum_ratio(item) < 1 - cfg.junk_char_ratio
        ]

        if junk and len(junk) * 2 >= len(entries):
            return _verdict(
                self.name, cfg.score_unrecognized, "author_unrecognized",
                "作者字段含大量无法解析为姓名的符号/数字内容",
                value, self.name, min_tier="heavy",
                suggestion=[e for e in entries if e not in junk],
            )
        if collective:
            personal = [item for item in entries if item not in collective]
            if not personal:
                return _verdict(
                    self.name, cfg.score_collective_only,
                    "author_collective_only",
                    f"作者字段仅含集体署名（{len(collective)} 项），缺少个人作者，需打标确认",
                    value, self.name, min_tier="medium", suggestion=None,
                )
            return _verdict(
                self.name, cfg.score_collective_mixed,
                "author_collective_mixed",
                f"作者字段混入 {len(collective)} 项集体署名，可剥离后保留个人作者",
                value, self.name, min_tier="minor", suggestion=personal,
            )
        if junk:
            return _verdict(
                self.name, cfg.score_collective_only, "author_junk_entry",
                f"作者字段含 {len(junk)} 项无法解析的条目，需打标确认",
                value, self.name, min_tier="medium",
                suggestion=[e for e in entries if e not in junk],
            )
        return None


class GenericDetector:
    """Pattern + baseline distribution check for arbitrary scalar fields."""

    def __init__(self, field_name: str, cfg: GenericDriftConfig, ngram_size: int = 3) -> None:
        self.name = field_name
        self.cfg = cfg
        self._pattern = re.compile(cfg.pattern) if cfg.pattern else None
        self._ngram_size = ngram_size

    def check(
        self, raw: dict[str, Any], norm: dict[str, Any], ctx: DetectionContext
    ) -> FieldVerdict | None:
        value = norm.get(self.name)
        if value is None:
            value = raw.get(self.name)
        if value is None or isinstance(value, bool):
            return None
        cfg = self.cfg
        detector = f"generic:{self.name}"

        if self._pattern is not None and not self._pattern.fullmatch(str(value)):
            return _verdict(
                self.name, cfg.score_pattern_violation, "pattern_violation",
                f"字段值不符合配置的正则约定 {cfg.pattern!r}",
                raw.get(self.name), detector,
                min_tier="heavy" if cfg.score_pattern_violation >= 0.8 else "medium",
                normalized=value,
            )

        # Shape-only fields (e.g. DOI) have no meaningful baseline distribution:
        # a conforming value is accepted regardless of whether it was seen before.
        if cfg.kind == "shape":
            return None

        if not ctx.has_baseline(self.name):
            return None
        if cfg.kind == "categorical":
            return self._check_categorical(value, raw.get(self.name), ctx, detector)
        if cfg.kind == "numeric":
            return self._check_numeric(value, raw.get(self.name), ctx, detector)
        return self._check_text(value, raw.get(self.name), ctx, detector)

    def _check_categorical(
        self, value: Any, observed: Any, ctx: DetectionContext, detector: str
    ) -> FieldVerdict | None:
        stats = ctx.baseline.categorical(self.name)  # type: ignore[union-attr]
        if stats is None or stats.frequency(str(value)) > 0:
            return None
        return _verdict(
            self.name, self.cfg.score_unknown_category, "unknown_category",
            "字段取值未在历史分类分布中出现过", observed, detector,
            min_tier="medium", normalized=value,
        )

    def _check_numeric(
        self, value: Any, observed: Any, ctx: DetectionContext, detector: str
    ) -> FieldVerdict | None:
        try:
            number = float(value)
        except (TypeError, ValueError):
            return _verdict(
                self.name, self.cfg.score_numeric_outlier, "non_numeric_value",
                "字段预期为数值但无法解析", observed, detector, min_tier="medium",
            )
        stats = ctx.baseline.numeric(self.name)  # type: ignore[union-attr]
        if stats is None or stats.stddev() <= 0 or stats.mean is None:
            return None
        if abs(number - stats.mean) > self.cfg.outlier_sigmas * stats.stddev():
            return _verdict(
                self.name, self.cfg.score_numeric_outlier, "numeric_outlier",
                f"数值 {number} 偏离历史分布超过 {self.cfg.outlier_sigmas} 个标准差",
                observed, detector, min_tier="medium", normalized=value,
            )
        return None

    def _check_text(
        self, value: Any, observed: Any, ctx: DetectionContext, detector: str
    ) -> FieldVerdict | None:
        profile = ctx.baseline.text_profile(self.name)  # type: ignore[union-attr]
        if profile is None:
            return None
        grams = char_ngrams(str(value), self._ngram_size)
        if not grams:
            return None
        similarity = len(grams & profile.features()) / len(grams | profile.features())
        if similarity >= self.cfg.min_profile_similarity:
            return None
        return _verdict(
            self.name, self.cfg.score_profile_drift, "text_profile_drift",
            f"字符轮廓与历史分布相似度 {similarity:.3f} 低于阈值 "
            f"{self.cfg.min_profile_similarity}",
            observed, detector, min_tier="medium", normalized=value,
        )


def build_detectors(drift_config: Any) -> list[Any]:
    """Instantiate the enabled detector suite from configuration."""
    detectors: list[Any] = []
    if drift_config.year.enabled:
        detectors.append(YearDetector(drift_config.year))
    if drift_config.source.enabled:
        detectors.append(SourceDetector(drift_config.source))
    if drift_config.authors.enabled:
        detectors.append(AuthorDetector(drift_config.authors))
    for field_name, field_cfg in sorted(drift_config.generic.items()):
        if field_cfg.enabled:
            detectors.append(GenericDetector(field_name, field_cfg))
    return detectors

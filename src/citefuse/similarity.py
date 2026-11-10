"""Hand-written string and field similarity functions.

No third-party fuzzy matching library is used. Functions return values in
[0, 1]; callers decide how to treat missing fields.
"""
from __future__ import annotations

import re
import unicodedata

_WORD_RE = re.compile(r"[^\W_]+", re.UNICODE)
_PAGE_NUMBER_RE = re.compile(r"\d+")


def tokenize(text: str | None) -> list[str]:
    """NFKC lowercase word tokens. O(n)."""
    if not text:
        return []
    folded = unicodedata.normalize("NFKC", text).casefold()
    return _WORD_RE.findall(folded)


def jaro(s1: str, s2: str) -> float:
    """Jaro similarity for two strings. O(len(s1) * match_window)."""
    if s1 == s2:
        return 1.0
    if not s1 or not s2:
        return 0.0
    len1, len2 = len(s1), len(s2)
    distance = max(len1, len2) // 2 - 1
    matches1 = [False] * len1
    matches2 = [False] * len2

    for i, char in enumerate(s1):
        start = max(0, i - distance)
        end = min(i + distance + 1, len2)
        for j in range(start, end):
            if not matches2[j] and s2[j] == char:
                matches1[i] = True
                matches2[j] = True
                break

    matched = sum(matches1)
    if matched == 0:
        return 0.0
    matched2 = [s2[j] for j in range(len2) if matches2[j]]
    matched1 = [s1[i] for i in range(len1) if matches1[i]]
    transpositions = sum(a != b for a, b in zip(matched1, matched2)) / 2
    return (matched / len1 + matched / len2 + (matched - transpositions) / matched) / 3


def jaro_winkler(s1: str, s2: str, prefix_weight: float = 0.1) -> float:
    """Jaro-Winkler similarity with a maximum four-character prefix boost."""
    base = jaro(s1, s2)
    if base <= 0.7:
        return base
    prefix = 0
    for a, b in zip(s1[:4], s2[:4]):
        if a != b:
            break
        prefix += 1
    return min(1.0, base + prefix * prefix_weight * (1.0 - base))


def token_jaccard(left: str | None, right: str | None) -> float:
    """Set Jaccard over NFKC-casefolded word tokens. O(n + m)."""
    a, b = set(tokenize(left)), set(tokenize(right))
    if not a and not b:
        return 1.0
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


def ordered_overlap(left: list[str], right: list[str]) -> float:
    """LCS-based ordered overlap coefficient.

    Returns LCS length / max(number of authors), so author order partially
    matters while reordered lists still retain signal. O(len(left)*len(right)).
    """
    if not left and not right:
        return 1.0
    if not left or not right:
        return 0.0
    previous = [0] * (len(right) + 1)
    for item in left:
        current = [0]
        for index, other in enumerate(right, start=1):
            if item == other:
                current.append(previous[index - 1] + 1)
            else:
                current.append(max(previous[index], current[-1]))
        previous = current
    return previous[-1] / max(len(left), len(right))


def author_similarity(
    left: list[dict[str, str]], right: list[dict[str, str]]
) -> float:
    """Ordered overlap over canonical author keys (family + given initial)."""
    return ordered_overlap([a.get("key", "") for a in left], [a.get("key", "") for a in right])


def parse_page_span(value: str | None) -> tuple[int, int] | None:
    """Parse a normalized page string such as '123-129' into inclusive ints."""
    if not value:
        return None
    numbers = [int(match) for match in _PAGE_NUMBER_RE.findall(value)]
    if not numbers:
        return None
    start = numbers[0]
    end = numbers[1] if len(numbers) > 1 else start
    if end < start:
        start, end = end, start
    return start, end


def page_overlap(left: str | None, right: str | None) -> float | None:
    """Inclusive page-range intersection over union. None if unparseable."""
    a, b = parse_page_span(left), parse_page_span(right)
    if a is None or b is None:
        return None
    intersection = max(0, min(a[1], b[1]) - max(a[0], b[0]) + 1)
    union = max(a[1], b[1]) - min(a[0], b[0]) + 1
    return intersection / union if union else 1.0


def year_similarity(left: int | None, right: int | None, scores: list[float]) -> float | None:
    """Absolute year-difference score; beyond the configured bands is zero."""
    if left is None or right is None:
        return None
    difference = abs(left - right)
    if difference >= len(scores):
        return 0.0
    return scores[difference]


def doi_similarity(left: str | None, right: str | None) -> float | None:
    """Exact normalized DOI comparison, with present-but-different scoring 0."""
    if not left or not right:
        return None
    return 1.0 if left == right else 0.0

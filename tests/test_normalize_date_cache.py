"""Tests for the normalize_date memo.

Profiling a real book put 974.7s of a 1003s run inside normalize_date, which
reached datetime.strptime 23,152,789 times — roughly 8.4 parse attempts per
call, each failure costing a regex compile and a setlocale inside _strptime.

Caching is only safe if the answer depends on nothing but the input, so these
tests pin exactly that: every input shape still returns what it did, and the
cache cannot hand one value's answer to another.
"""

import os
import sys
from datetime import date, datetime

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend import dsl_functions as D  # noqa: E402


@pytest.fixture(autouse=True)
def clear_cache():
    D._ND_CACHE.clear()
    yield
    D._ND_CACHE.clear()


# ── the memo must not change any answer ─────────────────────────────────

SHAPES = [
    "2026-03-31",
    "2026/03/31",
    "03/31/2026",
    "31/03/2026",
    "2026-03-31T00:00:00",
    "2026-03-31T00:00:00Z",
    "2026-03-31T12:34:56.789",
    "2026-03-31 12:00:00",
    "03-31-2026",
    "",
    "   ",
    "None",
    "not a date at all",
    "20260331",
]


@pytest.mark.parametrize("value", SHAPES)
def test_cached_result_matches_the_uncached_one(value):
    assert D.normalize_date(value) == D._normalize_date_uncached(value)


@pytest.mark.parametrize("value", SHAPES)
def test_repeated_calls_are_stable(value):
    first = D.normalize_date(value)
    assert [D.normalize_date(value) for _ in range(5)] == [first] * 5


def test_common_formats_still_normalise():
    assert D.normalize_date("03/31/2026") == "2026-03-31"
    assert D.normalize_date("2026-03-31T00:00:00") == "2026-03-31"
    assert D.normalize_date("2026/03/31") == "2026-03-31"


def test_a_non_date_is_handed_back_untouched():
    """lookup() normalises keys before comparing; mangling one would match the
    wrong row instead of failing loudly."""
    assert D.normalize_date("PRODUCT-A") == "PRODUCT-A"


def test_empty_and_none_are_empty():
    assert D.normalize_date("") == ""
    assert D.normalize_date("None") == ""
    assert D.normalize_date(None) == ""


# ── values must not bleed between keys ──────────────────────────────────

def test_different_strings_keep_different_answers():
    assert D.normalize_date("03/31/2026") == "2026-03-31"
    assert D.normalize_date("04/30/2026") == "2026-04-30"
    assert D.normalize_date("03/31/2026") == "2026-03-31"


def test_an_empty_result_is_cached_without_being_mistaken_for_a_miss():
    """`''` is falsy; a `.get()` truthiness check would reparse it forever."""
    assert D.normalize_date("") == ""
    assert "" in D._ND_CACHE
    assert D.normalize_date("") == ""


def test_whitespace_variants_are_separate_keys_with_equal_answers():
    assert D.normalize_date("2026-03-31") == D.normalize_date("  2026-03-31  ")


# ── only string inputs are keyed ────────────────────────────────────────

def test_datetime_and_date_objects_are_not_cached():
    """They take a cheap strftime path, and are not the hot case."""
    assert D.normalize_date(datetime(2026, 3, 31, 12, 0)) == "2026-03-31"
    assert D.normalize_date(date(2026, 3, 31)) == "2026-03-31"
    assert D._ND_CACHE == {}


def test_row_aware_arrays_are_never_keyed():
    """A context array carries per-row state; keying on it would be wrong,
    and it is unhashable anyway."""
    raa = D._RowAwareArray(["2026-01-31", "2026-02-28"], row_value="2026-02-28")
    assert D.normalize_date(raa) == "2026-02-28"
    assert D._ND_CACHE == {}


def test_a_row_aware_array_follows_its_current_row():
    first = D._RowAwareArray(["2026-01-31", "2026-02-28"], row_value="2026-01-31")
    second = D._RowAwareArray(["2026-01-31", "2026-02-28"], row_value="2026-02-28")
    assert D.normalize_date(first) == "2026-01-31"
    assert D.normalize_date(second) == "2026-02-28"


# ── bounds ──────────────────────────────────────────────────────────────

def test_the_cache_is_bounded():
    for i in range(D._ND_CACHE_MAX + 100):
        D.normalize_date(f"2026-03-{(i % 28) + 1:02d}T00:00:{i % 60:02d}")
    assert len(D._ND_CACHE) <= D._ND_CACHE_MAX


def test_the_cache_actually_holds_repeated_values():
    for _ in range(100):
        D.normalize_date("06/30/2026")
    assert D._ND_CACHE["06/30/2026"] == "2026-06-30"
    assert len(D._ND_CACHE) == 1


# ── the property the whole feature depends on ───────────────────────────

def test_transaction_dates_normalise_consistently_across_calls():
    """The regression diff keys on normalised dates; drift here would show as
    phantom missing/added transactions."""
    raw = ["2026-06-30", "06/30/2026", "2026-06-30T00:00:00"]
    assert len({D.normalize_date(r) for r in raw}) == 1

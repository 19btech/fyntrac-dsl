"""Tests for the generated-template compile cache.

A full-book run executes one template once per posting date. Validating and
compiling it every time put ~72% of a representative run into work that is a
pure function of source text that never changed between dates.

The cache is a security-relevant shortcut, so what matters most here is what
it must NOT do: never let unvalidated source through, never remember a
rejection as an approval, and never let one run's module state leak into the
next.
"""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import backend.server as S  # noqa: E402

SAFE = "x = 1\n\n\ndef process_event_data(a, b, c, d):\n    return []\n"


@pytest.fixture(autouse=True)
def clear_cache():
    S._TEMPLATE_CODE_CACHE.clear()
    yield
    S._TEMPLATE_CODE_CACHE.clear()


def test_identical_source_is_validated_once(monkeypatch):
    calls = []
    real = S._validate_template_ast
    monkeypatch.setattr(S, "_validate_template_ast",
                        lambda src, label='': (calls.append(src), real(src, label))[1])

    for _ in range(14):                       # a 14-posting-date book
        S._compile_template(SAFE)
    assert len(calls) == 1


def test_identical_source_is_compiled_once():
    first = S._compile_template(SAFE)
    second = S._compile_template(SAFE)
    assert first is second                    # the same code object, reused


def test_changed_source_is_revalidated(monkeypatch):
    calls = []
    real = S._validate_template_ast
    monkeypatch.setattr(S, "_validate_template_ast",
                        lambda src, label='': (calls.append(src), real(src, label))[1])

    S._compile_template(SAFE)
    S._compile_template(SAFE + "y = 2\n")
    assert len(calls) == 2


def test_a_single_character_change_is_a_different_entry():
    a = S._compile_template("x = 1\n")
    b = S._compile_template("x = 2\n")
    assert a is not b
    assert len(S._TEMPLATE_CODE_CACHE) == 2


# ── the cache must never weaken the sandbox ─────────────────────────────

FORBIDDEN = "__import__('os').system('echo pwned')\n"


def test_forbidden_source_is_still_rejected():
    with pytest.raises(S.DSLSecurityError):
        S._compile_template(FORBIDDEN)


def test_a_rejection_is_never_remembered_as_an_approval():
    """A failed validation must not leave anything cached, or the second
    attempt at the same payload would sail through."""
    for _ in range(3):
        with pytest.raises(S.DSLSecurityError):
            S._compile_template(FORBIDDEN)
    assert FORBIDDEN not in [k for k in S._TEMPLATE_CODE_CACHE]
    assert len(S._TEMPLATE_CODE_CACHE) == 0


def test_dunder_access_is_still_rejected():
    with pytest.raises(S.DSLSecurityError):
        S._compile_template("y = ().__class__.__bases__\n")


def test_caching_a_safe_template_does_not_admit_a_hostile_one():
    S._compile_template(SAFE)
    with pytest.raises(S.DSLSecurityError):
        S._compile_template(SAFE + FORBIDDEN)


# ── bounds ──────────────────────────────────────────────────────────────

def test_the_cache_is_bounded():
    for i in range(S._TEMPLATE_CODE_CACHE_MAX + 20):
        S._compile_template(f"x = {i}\n")
    assert len(S._TEMPLATE_CODE_CACHE) == S._TEMPLATE_CODE_CACHE_MAX


def test_eviction_drops_the_least_recently_used():
    first = "x = 0\n"
    S._compile_template(first)
    for i in range(1, S._TEMPLATE_CODE_CACHE_MAX):
        S._compile_template(f"x = {i}\n")
    S._compile_template(first)                       # refresh it
    S._compile_template("x = fresh_entry\n")         # forces one eviction

    import hashlib
    key = hashlib.sha256(first.encode()).hexdigest()
    assert key in S._TEMPLATE_CODE_CACHE             # kept: recently used
    evicted = hashlib.sha256("x = 1\n".encode()).hexdigest()
    assert evicted not in S._TEMPLATE_CODE_CACHE


# ── a reused code object must not carry state between runs ──────────────

def test_reused_code_object_executes_into_fresh_globals():
    """Templates keep module-level state; reusing the compiled object must
    not let one posting date's values reach the next."""
    source = "counter = 0\ncounter += 1\n"
    code = S._compile_template(source)

    first, second = {}, {}
    exec(code, first)
    exec(code, second)
    assert first["counter"] == 1
    assert second["counter"] == 1

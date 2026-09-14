"""Regression tests for the Anthropic provider's model capability rules.

The provider was broken for the three models people actually use. Its
fixed-temperature detector was a regex:

    r"(claude-(?:opus-4-[89]|[5-9])|claude-(?:fable|mythos))"

which cannot match `claude-opus-5` or `claude-sonnet-5` ('claude-' followed by
'[5-9]' has to match the 'o'/'s' of the tier name) and excludes `claude-opus-4-7`
('4-[89]'). Those models REMOVED `temperature` — sending it is a hard 400 — so
every request with the default model failed. The recovery path did not save it
either: the retry only fired on a narrow phrase list that omitted the current
wording, "extra inputs are not permitted".

Capability is now derived from the parsed version, so it states the API contract
rather than guessing at id spelling, and new releases are covered automatically.
"""
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.ai_providers.anthropic_provider import (  # noqa: E402
    _agent_capable,
    _claude_version,
    _fixed_temperature_model,
    _is_unsupported_temperature,
    _no_forced_tool_choice,
)

# `temperature` returns a 400 on these.
TEMPERATURE_REMOVED = [
    "claude-fable-5-1", "claude-mythos-5-1", "claude-fable-5", "claude-mythos-5",
    "claude-opus-5", "claude-opus-4-8", "claude-opus-4-7", "claude-sonnet-5",
]
# `temperature` is still accepted on these.
TEMPERATURE_OK = [
    "claude-opus-4-6", "claude-sonnet-4-6", "claude-haiku-4-5",
    "claude-sonnet-4-5", "claude-3-5-sonnet",
]


@pytest.mark.parametrize("model", TEMPERATURE_REMOVED)
def test_temperature_is_not_sent_where_it_400s(model):
    assert _fixed_temperature_model(model) is True, (
        f"{model} removed temperature; sending it is a 400")


@pytest.mark.parametrize("model", TEMPERATURE_OK)
def test_temperature_is_sent_where_it_is_supported(model):
    assert _fixed_temperature_model(model) is False


def test_unknown_model_fails_safe():
    """Omitting temperature costs nothing; sending it to a model that removed
    it is a hard failure — so an unrecognised id must omit it."""
    assert _fixed_temperature_model("claude-something-new-9") is True
    assert _fixed_temperature_model("") is True
    assert _fixed_temperature_model(None) is True


# -- the recovery path has to actually recognise the rejection -------------
@pytest.mark.parametrize("msg", [
    "temperature: Extra inputs are not permitted",
    "`temperature` is not supported on this model",
    "temperature has been removed for this model",
    "Only the default temperature is allowed",
    "temperature: unexpected keyword argument",
    "this model does not support temperature",
])
def test_temperature_rejection_is_recognised(msg):
    assert _is_unsupported_temperature(Exception(msg)) is True


@pytest.mark.parametrize("msg", [
    "rate limit exceeded", "invalid api key",
    "max_tokens: Extra inputs are not permitted",   # different field
])
def test_unrelated_errors_do_not_trigger_the_retry(msg):
    assert _is_unsupported_temperature(Exception(msg)) is False


# -- forced tool use ------------------------------------------------------
@pytest.mark.parametrize("model", ["claude-fable-5-1", "claude-mythos-5-1",
                                   "claude-fable-5"])
def test_forced_tool_choice_suppressed_where_it_400s(model):
    assert _no_forced_tool_choice(model) is True


@pytest.mark.parametrize("model", ["claude-opus-5", "claude-sonnet-5",
                                   "claude-opus-4-8", "claude-haiku-4-5"])
def test_forced_tool_choice_allowed_elsewhere(model):
    assert _no_forced_tool_choice(model) is False


# -- the picker shows current models only ---------------------------------
CURRENT = ["claude-fable-5-1", "claude-mythos-5-1", "claude-fable-5",
           "claude-opus-5", "claude-opus-4-8", "claude-opus-4-7",
           "claude-opus-4-6", "claude-sonnet-5", "claude-sonnet-4-6",
           "claude-haiku-4-5"]
RETIRED = ["claude-opus-4-5", "claude-sonnet-4-5", "claude-3-7-sonnet",
           "claude-3-5-sonnet", "claude-3-5-haiku", "claude-3-opus", "claude-2.1"]


@pytest.mark.parametrize("model", CURRENT)
def test_current_models_are_offered(model):
    assert _agent_capable(model) is True


@pytest.mark.parametrize("model", RETIRED)
def test_superseded_models_are_hidden(model):
    assert _agent_capable(model) is False


@pytest.mark.parametrize("model,expected", [
    ("claude-opus-5", ("opus", 5.0)),
    ("claude-fable-5-1", ("fable", 5.1)),
    ("claude-opus-4-8", ("opus", 4.8)),
    ("claude-haiku-4-5", ("haiku", 4.5)),
    ("claude-3-5-sonnet", ("sonnet", 3.5)),
])
def test_version_parsing(model, expected):
    assert _claude_version(model) == expected

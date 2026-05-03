"""Agent knowledge base — canonical patterns, authoring guide.

Exposed via the LLM tools `get_canonical_pattern`, `list_canonical_patterns`,
`find_similar_template`, and the textual `get_dsl_syntax_guide` extension.
"""
from .canonical_patterns import (
    CANONICAL_PATTERNS,
    get_pattern,
    list_patterns,
    match_pattern_by_intent,
)

__all__ = [
    "CANONICAL_PATTERNS",
    "get_pattern",
    "list_patterns",
    "match_pattern_by_intent",
]

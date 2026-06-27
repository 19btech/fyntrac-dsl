"""Provider factory and metadata."""

from .base import AIProvider


PROVIDER_INFO = {
    "gemini": {
        "name": "Google Gemini",
        "description": "Google's multimodal AI models",
        "key_url": "https://aistudio.google.com/apikey",
    },
    "openai": {
        "name": "OpenAI (ChatGPT)",
        "description": "GPT-4o, GPT-4, and more",
        "key_url": "https://platform.openai.com/api-keys",
    },
    "anthropic": {
        "name": "Anthropic (Claude)",
        "description": "Claude 3.5 Sonnet, Opus, and Haiku",
        "key_url": "https://console.anthropic.com/settings/keys",
    },
}


def get_provider(name: str) -> AIProvider:
    """Return an AIProvider instance by name. Raises ValueError for unknown."""
    if name == "gemini":
        from .gemini_provider import GeminiProvider
        return GeminiProvider()
    if name == "openai":
        from .openai_provider import OpenAIProvider
        return OpenAIProvider()
    if name == "anthropic":
        from .anthropic_provider import AnthropicProvider
        return AnthropicProvider()
    raise ValueError(f"Unknown provider: {name}")

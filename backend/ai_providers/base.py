"""Abstract base class for all AI providers."""

from abc import ABC, abstractmethod
from typing import Optional, AsyncIterator
from pydantic import BaseModel


class ModelInfo(BaseModel):
    id: str            # e.g. "gpt-4o"
    name: str          # e.g. "GPT-4o"


class AIResponse(BaseModel):
    text: str
    usage: Optional[dict] = None


class AIError(Exception):
    """Typed error that maps to user-facing messages in the frontend."""

    def __init__(self, error_type: str, provider: str, detail: str):
        self.error_type = error_type
        self.provider = provider
        self.detail = detail
        super().__init__(detail)


# Canonical error types (referenced by frontend error table)
ERROR_NO_PROVIDER = "no_provider"
ERROR_INVALID_KEY = "invalid_key"
ERROR_QUOTA_EXCEEDED = "quota_exceeded"
ERROR_RATE_LIMITED = "rate_limited"
ERROR_MODEL_PREMIUM = "model_premium"
ERROR_NETWORK = "network"
ERROR_MODEL_DEPRECATED = "model_deprecated"


class AIProvider(ABC):
    """Every provider implements these three methods.

    Adding a new provider = one new file that subclasses this.
    """

    @abstractmethod
    async def validate_key(self, api_key: str) -> bool:
        """Return True if the key is accepted by the provider."""
        ...

    @abstractmethod
    async def list_models(self, api_key: str) -> list[ModelInfo]:
        """Return available models for the given key."""
        ...

    @abstractmethod
    async def chat(
        self,
        api_key: str,
        model: str,
        system_prompt: str,
        user_message: str,
        history: list[dict] | None = None,
    ) -> AIResponse:
        """Send a message and return the assistant reply.

        Raises AIError with a typed error_type on failure.
        """
        ...

    async def stream_chat(
        self,
        api_key: str,
        model_id: str,
        system_prompt: str,
        user_message: str,
        history: list[dict] | None = None,
    ) -> AsyncIterator[str]:
        """Stream a response token by token. Yields text chunks.

        Default implementation falls back to non-streaming chat.
        Providers should override for real streaming.
        """
        resp = await self.chat(api_key, model_id, system_prompt, user_message, history)
        yield resp.text

    async def chat_with_tools(
        self,
        *,
        api_key: str,
        model: str,
        messages: list[dict],
        tools: list[dict],
        temperature: float = 0.1,
    ) -> dict:
        """Tool-calling chat used by the autonomous agent runtime.

        Args:
            messages: Full conversation in OpenAI-style format. Each message is
                {"role": "system"|"user"|"assistant"|"tool",
                 "content": str | None,
                 "tool_calls": [...]?,         # for assistant
                 "tool_call_id": str?,         # for tool
                 "name": str?}                 # for tool
            tools: List of OpenAI-style tool schemas
                ({"name", "description", "parameters"}).

        Returns:
            {
                "message": {"role": "assistant", "content": str|None,
                              "tool_calls": [...]},
                "tool_calls": [{"id": str, "name": str,
                                  "arguments": dict}],   # convenience
                "finish_reason": str,
                "usage": dict | None,
            }

        Providers that do not yet support tool calling should raise
        NotImplementedError.
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} does not implement chat_with_tools"
        )

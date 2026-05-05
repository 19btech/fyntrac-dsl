"""DeepSeek provider implementation (OpenAI-compatible API)."""

import asyncio
import logging
from typing import AsyncIterator
from .base import (
    AIProvider, AIResponse, ModelInfo, AIError,
    ERROR_INVALID_KEY, ERROR_QUOTA_EXCEEDED, ERROR_RATE_LIMITED,
    ERROR_NETWORK, ERROR_MODEL_PREMIUM, ERROR_MODEL_DEPRECATED,
)

logger = logging.getLogger(__name__)

DEEPSEEK_BASE_URL = "https://api.deepseek.com"


def _classify_error(exc: Exception) -> tuple[str, str]:
    msg = str(exc).lower()
    if "invalid api key" in msg or "401" in msg or "incorrect api key" in msg or "authentication" in msg:
        return ERROR_INVALID_KEY, "The API key was not accepted by DeepSeek."
    if "insufficient_quota" in msg or "billing" in msg or "balance" in msg:
        return ERROR_QUOTA_EXCEEDED, "Quota exceeded on your DeepSeek account. Please check your balance at https://platform.deepseek.com/usage"
    if "rate limit" in msg or "429" in msg:
        return ERROR_RATE_LIMITED, "Rate limit exceeded. Wait a moment."
    if "model_not_found" in msg or "does not exist" in msg:
        return ERROR_MODEL_DEPRECATED, "Model not available."
    if "permission" in msg or "403" in msg:
        return ERROR_MODEL_PREMIUM, "Your API key does not have access to this model."
    if "timeout" in msg or "connection" in msg:
        return ERROR_NETWORK, "Could not reach DeepSeek."
    return ERROR_NETWORK, str(exc)


class DeepSeekProvider(AIProvider):

    def _client(self, api_key: str):
        from openai import OpenAI
        return OpenAI(api_key=api_key, base_url=DEEPSEEK_BASE_URL)

    async def validate_key(self, api_key: str) -> bool:
        try:
            client = self._client(api_key)
            await asyncio.to_thread(lambda: list(client.models.list()))
            return True
        except Exception as exc:
            err_type, _ = _classify_error(exc)
            if err_type == ERROR_INVALID_KEY:
                return False
            raise

    async def list_models(self, api_key: str) -> list[ModelInfo]:
        try:
            client = self._client(api_key)
            raw_models = await asyncio.to_thread(lambda: list(client.models.list()))
        except Exception as exc:
            err_type, detail = _classify_error(exc)
            raise AIError(err_type, "deepseek", detail)
        if not raw_models:
            raise AIError(ERROR_INVALID_KEY, "deepseek", "API key invalid. Get yours at https://platform.deepseek.com/api_keys")

        results = []
        for m in raw_models:
            results.append(ModelInfo(id=m.id, name=m.id))
        results.sort(key=lambda x: x.id)
        return results

    async def chat(
        self,
        api_key: str,
        model_id: str,
        system_prompt: str,
        user_message: str,
        history: list[dict] | None = None,
    ) -> AIResponse:
        try:
            client = self._client(api_key)

            messages = [{"role": "system", "content": system_prompt}]
            if history:
                for msg in history:
                    role = msg.get("role", "user")
                    if role not in ("user", "assistant"):
                        role = "user"
                    messages.append({"role": role, "content": msg.get("content", "")})
            messages.append({"role": "user", "content": user_message})

            response = await asyncio.to_thread(
                client.chat.completions.create,
                model=model_id,
                messages=messages,
            )
            text = response.choices[0].message.content or ""
            usage = None
            if response.usage:
                usage = {
                    "prompt_tokens": response.usage.prompt_tokens,
                    "completion_tokens": response.usage.completion_tokens,
                }
            return AIResponse(text=text, usage=usage)
        except Exception as exc:
            error_type, detail = _classify_error(exc)
            raise AIError(error_type, "deepseek", detail) from exc

    async def stream_chat(
        self,
        api_key: str,
        model_id: str,
        system_prompt: str,
        user_message: str,
        history: list[dict] | None = None,
    ) -> AsyncIterator[str]:
        import queue, threading
        try:
            client = self._client(api_key)

            messages = [{"role": "system", "content": system_prompt}]
            if history:
                for msg in history:
                    role = msg.get("role", "user")
                    if role not in ("user", "assistant"):
                        role = "user"
                    messages.append({"role": role, "content": msg.get("content", "")})
            messages.append({"role": "user", "content": user_message})

            q = queue.Queue()
            _SENTINEL = object()

            def _stream_worker():
                try:
                    stream = client.chat.completions.create(
                        model=model_id,
                        messages=messages,
                        stream=True,
                    )
                    for chunk in stream:
                        delta = chunk.choices[0].delta if chunk.choices else None
                        if delta and delta.content:
                            q.put(delta.content)
                except Exception as e:
                    q.put(e)
                finally:
                    q.put(_SENTINEL)

            thread = threading.Thread(target=_stream_worker, daemon=True)
            thread.start()

            while True:
                item = await asyncio.to_thread(q.get)
                if item is _SENTINEL:
                    break
                if isinstance(item, Exception):
                    raise item
                yield item
        except AIError:
            raise
        except Exception as exc:
            error_type, detail = _classify_error(exc)
            raise AIError(error_type, "deepseek", detail) from exc

    async def chat_with_tools(
        self,
        *,
        api_key: str,
        model: str,
        messages: list[dict],
        tools: list[dict],
        temperature: float = 0.1,
        tool_choice: str | None = None,
    ) -> dict:
        from .openai_provider import _openai_compatible_chat_with_tools
        return await _openai_compatible_chat_with_tools(
            api_key=api_key, model=model, messages=messages,
            tools=tools, temperature=temperature,
            base_url=DEEPSEEK_BASE_URL, provider_name="deepseek",
            tool_choice=tool_choice,
        )

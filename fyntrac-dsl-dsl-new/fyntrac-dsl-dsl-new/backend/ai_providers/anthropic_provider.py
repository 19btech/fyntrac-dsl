"""Anthropic (Claude) provider implementation."""

import asyncio
import re
import logging
from typing import AsyncIterator
from .base import (
    AIProvider, AIResponse, ModelInfo, AIError,
    ERROR_INVALID_KEY, ERROR_QUOTA_EXCEEDED, ERROR_RATE_LIMITED,
    ERROR_NETWORK, ERROR_MODEL_PREMIUM, ERROR_MODEL_DEPRECATED,
)

logger = logging.getLogger(__name__)


def _classify_error(exc: Exception) -> tuple[str, str]:
    msg = str(exc).lower()
    if "invalid" in msg or "authentication" in msg or "401" in msg:
        return ERROR_INVALID_KEY, "The API key was not accepted by Anthropic."
    if "credit" in msg or "billing" in msg or "insufficient" in msg:
        return ERROR_QUOTA_EXCEEDED, "Your Anthropic account has insufficient credits. Check billing at https://console.anthropic.com/settings/billing"
    if "overloaded" in msg or "529" in msg:
        return ERROR_QUOTA_EXCEEDED, "Anthropic servers are overloaded. Try again shortly."
    if "rate" in msg or "429" in msg:
        return ERROR_RATE_LIMITED, "Rate limit exceeded. Wait a moment."
    if "not found" in msg or "404" in msg:
        return ERROR_MODEL_DEPRECATED, "Model not available."
    if "permission" in msg or "403" in msg:
        return ERROR_MODEL_PREMIUM, "Your API key does not have access to this model tier."
    if "timeout" in msg or "connection" in msg:
        return ERROR_NETWORK, "Could not reach Anthropic."
    return ERROR_NETWORK, str(exc)


class AnthropicProvider(AIProvider):

    async def validate_key(self, api_key: str) -> bool:
        try:
            import anthropic
            client = anthropic.Anthropic(api_key=api_key)
            await asyncio.to_thread(lambda: list(client.models.list()))
            return True
        except Exception as exc:
            err_type, _ = _classify_error(exc)
            if err_type == ERROR_INVALID_KEY:
                return False
            raise

    async def list_models(self, api_key: str) -> list[ModelInfo]:
        import anthropic as anth
        try:
            client = anth.Anthropic(api_key=api_key)
            raw_models = await asyncio.to_thread(lambda: list(client.models.list()))
        except Exception as exc:
            err_type, detail = _classify_error(exc)
            raise AIError(err_type, "anthropic", detail)
        if not raw_models:
            raise AIError(ERROR_INVALID_KEY, "anthropic", "API key invalid. Get yours at https://console.anthropic.com/settings/keys")

        results = []
        for m in raw_models:
            # Skip dated point-release snapshots (e.g. claude-sonnet-4-20250514)
            if re.search(r'-\d{8}$', m.id):
                continue
            results.append(ModelInfo(id=m.id, name=m.display_name))

        # Sort: stable releases first, then previews; newest version first within each group
        def _sort_key(m):
            is_preview = 1 if 'preview' in m.id else 0
            # Extract version number (e.g. claude-sonnet-4 -> 4, claude-3-5-haiku -> 3.5)
            ver_match = re.search(r'-(\d+)-(\d+)-', m.id)
            if ver_match:
                version = float(f"{ver_match.group(1)}.{ver_match.group(2)}")
            else:
                ver_match = re.search(r'-(\d+\.?\d*)', m.id)
                version = float(ver_match.group(1)) if ver_match else 0
            return (is_preview, -version, m.id)
        results.sort(key=_sort_key)
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
            import anthropic
            client = anthropic.Anthropic(api_key=api_key)

            messages = []
            if history:
                for msg in history:
                    role = msg.get("role", "user")
                    if role not in ("user", "assistant"):
                        role = "user"
                    messages.append({"role": role, "content": msg.get("content", "")})
            messages.append({"role": "user", "content": user_message})

            response = await asyncio.to_thread(
                client.messages.create,
                model=model_id,
                max_tokens=4096,
                system=system_prompt,
                messages=messages,
            )
            text = response.content[0].text if response.content else ""
            usage = None
            if response.usage:
                usage = {
                    "prompt_tokens": response.usage.input_tokens,
                    "completion_tokens": response.usage.output_tokens,
                }
            return AIResponse(text=text, usage=usage)
        except Exception as exc:
            error_type, detail = _classify_error(exc)
            raise AIError(error_type, "anthropic", detail) from exc

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
            import anthropic
            client = anthropic.Anthropic(api_key=api_key)

            messages = []
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
                    with client.messages.stream(
                        model=model_id,
                        max_tokens=4096,
                        system=system_prompt,
                        messages=messages,
                    ) as stream:
                        for text in stream.text_stream:
                            q.put(text)
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
            raise AIError(error_type, "anthropic", detail) from exc

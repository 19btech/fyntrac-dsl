"""Google Gemini provider implementation using the modern google-genai SDK."""

import asyncio
import re
import logging
from typing import AsyncIterator
from .base import (
    AIProvider, AIResponse, ModelInfo, AIError,
    ERROR_INVALID_KEY, ERROR_QUOTA_EXCEEDED, ERROR_RATE_LIMITED,
    ERROR_NETWORK, ERROR_MODEL_DEPRECATED,
)

logger = logging.getLogger(__name__)

# Patterns to exclude from the model list (non-chat models)
_EXCLUDE_PATTERNS = re.compile(
    r'(tts|image|vision|embed|aqa|retrieval|robotics|computer-use|deep-research|lyria|nano-banana|customtools)',
    re.IGNORECASE,
)
# Only show gemini-* models (not gemma, etc.)
_INCLUDE_PREFIX = 'gemini-'


def _classify_error(exc: Exception) -> tuple[str, str]:
    """Map a google-genai exception to (error_type, detail)."""
    msg = str(exc).lower()
    if "api key not valid" in msg or "api_key_invalid" in msg or "401" in msg:
        return ERROR_INVALID_KEY, "The API key was not accepted by Google."
    if "quota" in msg or "resource exhausted" in msg or "exceeded your current quota" in msg:
        return ERROR_QUOTA_EXCEEDED, "Quota exceeded on your Google account. Check your usage at https://ai.google.dev/gemini-api/docs/rate-limits"
    if "rate limit" in msg or ("429" in msg and "quota" not in msg):
        return ERROR_RATE_LIMITED, "Rate limit hit. Wait a moment and retry."
    if "not found" in msg or "deprecated" in msg or "404" in msg:
        return ERROR_MODEL_DEPRECATED, "Model not available."
    if "timeout" in msg or "connection" in msg or "network" in msg:
        return ERROR_NETWORK, "Could not reach Google AI."
    return ERROR_NETWORK, str(exc)


def _get_client(api_key: str):
    """Create a google.genai Client instance (thread-safe, no global state)."""
    from google import genai
    return genai.Client(api_key=api_key)


class GeminiProvider(AIProvider):

    async def validate_key(self, api_key: str) -> bool:
        try:
            client = _get_client(api_key)
            models = await asyncio.to_thread(lambda: list(client.models.list()))
            return len(models) > 0
        except Exception as exc:
            err_type, _ = _classify_error(exc)
            if err_type == ERROR_INVALID_KEY:
                return False
            raise

    async def list_models(self, api_key: str) -> list[ModelInfo]:
        try:
            client = _get_client(api_key)
            raw_models = await asyncio.to_thread(lambda: list(client.models.list()))
        except Exception as exc:
            err_type, detail = _classify_error(exc)
            raise AIError(err_type, "gemini", detail)
        if not raw_models:
            raise AIError(ERROR_INVALID_KEY, "gemini", "API key invalid. Get yours at https://aistudio.google.com/apikey")

        results = []
        seen = set()
        for m in raw_models:
            model_id = m.name.replace('models/', '')
            if not model_id.startswith(_INCLUDE_PREFIX):
                continue
            if _EXCLUDE_PATTERNS.search(model_id):
                continue
            # Skip -latest aliases and dated point releases (e.g. -001)
            if model_id.endswith('-latest') or re.search(r'-\d{3}$', model_id):
                continue
            if model_id in seen:
                continue
            seen.add(model_id)
            display = getattr(m, 'display_name', model_id)
            results.append(ModelInfo(id=model_id, name=display))

        # Sort: stable releases first, then previews; within each group newest version first
        def _sort_key(m):
            is_preview = 1 if 'preview' in m.id else 0
            ver_match = re.search(r'(\d+\.?\d*)', m.id)
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
        from google.genai import types
        try:
            client = _get_client(api_key)

            # Build contents list for multi-turn
            contents = []
            if history:
                for msg in history:
                    role = "user" if msg.get("role") == "user" else "model"
                    contents.append(types.Content(role=role, parts=[types.Part.from_text(text=msg.get("content", ""))]))
            contents.append(types.Content(role="user", parts=[types.Part.from_text(text=user_message)]))

            config = types.GenerateContentConfig(system_instruction=system_prompt)
            response = await asyncio.to_thread(
                client.models.generate_content,
                model=model_id,
                contents=contents,
                config=config,
            )
            return AIResponse(text=response.text)
        except Exception as exc:
            error_type, detail = _classify_error(exc)
            raise AIError(error_type, "gemini", detail) from exc

    async def stream_chat(
        self,
        api_key: str,
        model_id: str,
        system_prompt: str,
        user_message: str,
        history: list[dict] | None = None,
    ) -> AsyncIterator[str]:
        from google.genai import types
        import queue, threading
        try:
            client = _get_client(api_key)

            contents = []
            if history:
                for msg in history:
                    role = "user" if msg.get("role") == "user" else "model"
                    contents.append(types.Content(role=role, parts=[types.Part.from_text(text=msg.get("content", ""))]))
            contents.append(types.Content(role="user", parts=[types.Part.from_text(text=user_message)]))

            config = types.GenerateContentConfig(system_instruction=system_prompt)

            q = queue.Queue()
            _SENTINEL = object()

            def _stream_worker():
                try:
                    response = client.models.generate_content_stream(
                        model=model_id,
                        contents=contents,
                        config=config,
                    )
                    for chunk in response:
                        if chunk.text:
                            q.put(chunk.text)
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
            raise AIError(error_type, "gemini", detail) from exc

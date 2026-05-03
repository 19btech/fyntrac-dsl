"""OpenAI provider implementation."""

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

# Only include chat-capable model families. We allow any gpt-N* family
# (gpt-3.5, gpt-4, gpt-4o, gpt-4.1, gpt-5, future gpt-N) plus reasoning
# series (o1/o3/o4/o5...) and the chatgpt-* aliases.
_CHAT_MODEL_PATTERN = re.compile(
    r'^(gpt-\d|o\d|chatgpt)',
    re.IGNORECASE,
)
# Exclude non-chat variants
_EXCLUDE_PATTERN = re.compile(
    r'(audio|realtime|transcribe|tts|whisper|dall-e|embed|moderation|search|instruct|image|codex)',
    re.IGNORECASE,
)
_DATED_SNAPSHOT = re.compile(r'-\d{4}-\d{2}-\d{2}$|-\d{4}$|-preview-\d{4}|-\d{8}$')


def _classify_error(exc: Exception) -> tuple[str, str]:
    msg = str(exc).lower()
    if "invalid api key" in msg or "401" in msg or "incorrect api key" in msg:
        return ERROR_INVALID_KEY, "The API key was not accepted by OpenAI."
    if "insufficient_quota" in msg or "billing" in msg:
        return ERROR_QUOTA_EXCEEDED, "Quota exceeded on your OpenAI account. Please check your plan and billing at https://platform.openai.com/settings/organization/billing"
    if "rate limit" in msg or "429" in msg:
        return ERROR_RATE_LIMITED, "Rate limit exceeded. Wait a moment."
    if "model_not_found" in msg or "does not exist" in msg:
        return ERROR_MODEL_DEPRECATED, "Model not available."
    if "permission" in msg or "403" in msg:
        return ERROR_MODEL_PREMIUM, "Your API key does not have access to this model."
    if "timeout" in msg or "connection" in msg:
        return ERROR_NETWORK, "Could not reach OpenAI."
    return ERROR_NETWORK, str(exc)


class OpenAIProvider(AIProvider):

    async def validate_key(self, api_key: str) -> bool:
        try:
            from openai import OpenAI
            client = OpenAI(api_key=api_key)
            await asyncio.to_thread(lambda: list(client.models.list()))
            return True
        except Exception as exc:
            err_type, _ = _classify_error(exc)
            if err_type == ERROR_INVALID_KEY:
                return False
            raise

    async def list_models(self, api_key: str) -> list[ModelInfo]:
        from openai import OpenAI
        try:
            client = OpenAI(api_key=api_key)
            raw_models = await asyncio.to_thread(lambda: list(client.models.list()))
        except Exception as exc:
            err_type, detail = _classify_error(exc)
            raise AIError(err_type, "openai", detail)
        if not raw_models:
            raise AIError(ERROR_INVALID_KEY, "openai", "API key invalid. Get yours at https://platform.openai.com/api-keys")

        results = []
        seen_ids = set()
        # First pass: collect everything that looks like a chat model
        candidates = []
        for m in raw_models:
            if not _CHAT_MODEL_PATTERN.match(m.id):
                continue
            if _EXCLUDE_PATTERN.search(m.id):
                continue
            candidates.append(m.id)

        # Group by "base alias" (id with any trailing date snapshot stripped).
        # Prefer the base alias (e.g. "gpt-4o-mini") but if only dated
        # snapshots exist for a family, surface the most recent one so new
        # premium models like gpt-5 still appear before OpenAI publishes the
        # un-dated alias.
        from collections import defaultdict
        groups = defaultdict(list)
        for cid in candidates:
            base = _DATED_SNAPSHOT.sub("", cid)
            groups[base].append(cid)

        for base, ids in groups.items():
            if base in ids:
                pick = base
            else:
                # No bare alias — use the lexicographically largest dated id
                # (ISO-style dates sort chronologically).
                pick = sorted(ids)[-1]
            if pick in seen_ids:
                continue
            seen_ids.add(pick)
            results.append(ModelInfo(id=pick, name=pick))
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
            from openai import OpenAI
            client = OpenAI(api_key=api_key)

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
            raise AIError(error_type, "openai", detail) from exc

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
            from openai import OpenAI
            client = OpenAI(api_key=api_key)

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
            raise AIError(error_type, "openai", detail) from exc

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
        return await _openai_compatible_chat_with_tools(
            api_key=api_key, model=model, messages=messages,
            tools=tools, temperature=temperature,
            base_url=None, provider_name="openai",
            tool_choice=tool_choice,
        )


# ──────────────────────────────────────────────────────────────────────────
# Shared OpenAI-compatible tool-calling implementation.
# Used by both OpenAI and DeepSeek providers.
# ──────────────────────────────────────────────────────────────────────────

import json as _json


def _to_openai_tool_specs(tools: list[dict]) -> list[dict]:
    out = []
    for t in tools:
        out.append({
            "type": "function",
            "function": {
                "name": t["name"],
                "description": t.get("description", ""),
                "parameters": t.get("parameters", {"type": "object", "properties": {}}),
            },
        })
    return out


def _normalise_messages_for_openai(messages: list[dict]) -> list[dict]:
    """Convert our internal message format into the OpenAI SDK's expected shape.

    I18: also prunes orphan tool_calls — every assistant `tool_calls[i].id`
    must have a matching `role:'tool'` reply later in the history, otherwise
    OpenAI returns 400 "messages with role 'tool' must follow tool_call".
    Orphans happen when a previous run was cancelled mid-flight and the
    history was replayed.
    """
    # Pre-pass: collect tool_call_ids that DO have a reply.
    replied_ids: set[str] = set()
    for m in messages:
        if m.get("role") == "tool" and m.get("tool_call_id"):
            replied_ids.add(str(m["tool_call_id"]))
    out = []
    for m in messages:
        role = m.get("role")
        if role == "assistant":
            msg = {"role": "assistant", "content": m.get("content")}
            tcs = [tc for tc in (m.get("tool_calls") or [])
                   if str(tc.get("id") or "") in replied_ids]
            if tcs:
                msg["tool_calls"] = [
                    {
                        "id": tc["id"],
                        "type": "function",
                        "function": {
                            "name": tc["name"],
                            "arguments": tc["arguments"] if isinstance(tc["arguments"], str)
                                         else _json.dumps(tc.get("arguments") or {}),
                        },
                    }
                    for tc in tcs
                ]
            # Drop assistant messages that had ONLY orphan tool_calls and no
            # textual content — they would be empty and reject the request.
            if not tcs and not (msg.get("content") or "").strip():
                continue
            out.append(msg)
        elif role == "tool":
            out.append({
                "role": "tool",
                "tool_call_id": m.get("tool_call_id"),
                "content": m.get("content") or "",
            })
        else:
            out.append({"role": role or "user", "content": m.get("content") or ""})
    return out


async def _openai_compatible_chat_with_tools(
    *,
    api_key: str,
    model: str,
    messages: list[dict],
    tools: list[dict],
    temperature: float,
    base_url: str | None,
    provider_name: str,
    tool_choice: str | None = None,
) -> dict:
    try:
        from openai import OpenAI
        kwargs = {"api_key": api_key, "max_retries": 0, "timeout": 90.0}
        if base_url:
            kwargs["base_url"] = base_url
        client = OpenAI(**kwargs)
        oa_messages = _normalise_messages_for_openai(messages)
        oa_tools = _to_openai_tool_specs(tools)

        response = await asyncio.to_thread(
            client.chat.completions.create,
            model=model,
            messages=oa_messages,
            tools=oa_tools,
            tool_choice=(tool_choice or "auto"),
            temperature=temperature,
        )
        choice = response.choices[0]
        msg = choice.message
        tool_calls_out = []
        for tc in (msg.tool_calls or []):
            try:
                args = _json.loads(tc.function.arguments or "{}")
            except Exception:
                args = {}
            tool_calls_out.append({
                "id": tc.id,
                "name": tc.function.name,
                "arguments": args,
            })
        return {
            "message": {
                "role": "assistant",
                "content": msg.content,
                "tool_calls": tool_calls_out,
            },
            "tool_calls": tool_calls_out,
            "finish_reason": choice.finish_reason,
            "usage": {
                "prompt_tokens": getattr(response.usage, "prompt_tokens", None),
                "completion_tokens": getattr(response.usage, "completion_tokens", None),
            } if response.usage else None,
        }
    except Exception as exc:
        err_type, detail = _classify_error(exc)
        raise AIError(err_type, provider_name, detail) from exc

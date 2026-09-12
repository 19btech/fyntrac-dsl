"""Anthropic (Claude) provider implementation."""

import asyncio
import os
import re
import logging
from typing import AsyncIterator
from .base import (
    AIProvider, AIResponse, ModelInfo, AIError,
    ERROR_INVALID_KEY, ERROR_QUOTA_EXCEEDED, ERROR_RATE_LIMITED,
    ERROR_NETWORK, ERROR_MODEL_PREMIUM, ERROR_MODEL_DEPRECATED,
)

logger = logging.getLogger(__name__)


# Claude generations that REMOVED `temperature` — sending it returns a 400.
#
# This was a regex, r'claude-(?:opus-4-[89]|[5-9])|claude-(?:fable|mythos)',
# which silently failed to match the three ids that matter most:
#   claude-opus-5     -> 'claude-' then '[5-9]' cannot match the 'o' of opus
#   claude-sonnet-5   -> same
#   claude-opus-4-7   -> '4-[89]' excludes 4-7
# so the default model sent `temperature` on every request and every call
# 400'd. Derive it from the parsed version instead, so the rule states the
# actual API contract and new releases are covered without a code change.
#
# Removed on: Fable/Mythos (all), Opus >= 4.7, Sonnet >= 4.7 (i.e. Sonnet 5).
# Still accepted on: Opus 4.6, Sonnet 4.6, Haiku 4.5 and older.
_TEMPERATURE_REMOVED_FLOOR = 4.7
_TEMPERATURE_FREE_TIERS = ('fable', 'mythos')


def _fixed_temperature_model(model: str) -> bool:
    """
    True when `model` rejects an explicit `temperature`.

    Unrecognised ids default to True: omitting temperature costs nothing (the
    server uses its default) whereas sending it to a model that removed it is
    a hard 400, so an unknown future model should fail safe.
    """
    parsed = _claude_version(model)
    if not parsed:
        return True
    tier, ver = parsed
    if tier in _TEMPERATURE_FREE_TIERS:
        return True
    if tier == 'haiku':
        return False
    return ver >= _TEMPERATURE_REMOVED_FLOOR


# ── Capability floor ────────────────────────────────────────────────────────
# Only surface agent-capable generations in the picker so the user isn't shown
# deprecated/older models. This is deliberately VERSION-BASED (not a hardcoded
# allowlist): any new model at or above the floor passes automatically, so
# future Claude releases self-add with no code change.
#
# Keep:  Opus 4.6+, Sonnet 4.6+, Sonnet 5, Fable 5, Mythos 5, Haiku 4.5.
# Drop:  Opus/Sonnet 4.5 and older, all Claude 3.x / 2.x, Haiku 3.x.
_CLAUDE_TIERS = ("opus", "sonnet", "haiku", "fable", "mythos")
_CLAUDE_DEFAULT_FLOOR = 4.6
# Haiku's numbering trails a tier behind Opus/Sonnet; 4.5 is the current line.
_CLAUDE_TIER_FLOORS = {"haiku": 4.5}


def _claude_version(model_id: str):
    """Return (tier, version_float) for a Claude id, or None when no version can
    be parsed. Handles both the current layout (claude-opus-4-8, claude-sonnet-5)
    and the legacy one (claude-3-5-sonnet, claude-3-opus). For an unrecognised
    tier name it falls back to the version alone (tier=None) so a brand-new
    family still self-adds on version."""
    mid = (model_id or "").lower()
    tier = next((t for t in _CLAUDE_TIERS if t in mid), None)
    if tier:
        m = (re.search(rf'{tier}-(\d+)(?:[-.](\d+))?', mid)
             or re.search(rf'(\d+)(?:[-.](\d+))?-{tier}', mid))
        if m:
            return tier, float(f"{m.group(1)}.{m.group(2) or 0}")
    # Unknown/new tier name — key off the version so self-add still works.
    m = (re.search(r'claude-[a-z]+-(\d+)(?:[-.](\d+))?', mid)
         or re.search(r'claude-(\d+)(?:[-.](\d+))?', mid))
    if m:
        return None, float(f"{m.group(1)}.{m.group(2) or 0}")
    return None


def _agent_capable(model_id: str) -> bool:
    parsed = _claude_version(model_id)
    if not parsed:
        return False
    tier, ver = parsed
    return ver >= _CLAUDE_TIER_FLOORS.get(tier, _CLAUDE_DEFAULT_FLOOR)


# Phrases the API has used when rejecting `temperature`. The list used to
# omit the current wording ('extra inputs are not permitted'), so the
# fallback retry never fired and the 400 surfaced to the user instead.
_TEMPERATURE_REJECTED_PHRASES = (
    "deprecated", "not supported", "unsupported", "only the default",
    "does not support", "extra inputs are not permitted", "not permitted",
    "unexpected keyword", "removed", "cannot be specified", "not allowed",
)


def _no_forced_tool_choice(model: str) -> bool:
    """True when tool_choice 'any'/'tool' returns a 400 for this model."""
    parsed = _claude_version(model)
    return bool(parsed) and parsed[0] in _TEMPERATURE_FREE_TIERS


def _is_unsupported_temperature(exc: Exception) -> bool:
    m = str(exc).lower()
    return "temperature" in m and any(
        p in m for p in _TEMPERATURE_REJECTED_PHRASES
    )


def _anthropic_message(exc: Exception) -> str:
    """Pull the human-readable 'message' out of an Anthropic SDK error like
    "Error code: 400 - {'type':'error','error':{'type':'invalid_request_error',
    'message':'max_tokens: ...'}}" so we surface the REAL reason instead of the
    raw dict."""
    raw = str(exc)
    m = re.search(r"'message'\s*:\s*'([^']+)'", raw) or \
        re.search(r'"message"\s*:\s*"([^"]+)"', raw)
    return m.group(1) if m else raw


def _classify_error(exc: Exception) -> tuple[str, str]:
    msg = str(exc).lower()
    # WORKSPACE SCOPE — an identity-linked key is scoped to a user, not a
    # workspace, so every request must name the workspace it acts in. The key
    # itself is fine, so this must be checked BEFORE the auth branch below
    # (Anthropic may report it as an authentication_error).
    if "anthropic-workspace-id" in msg or "workspace_id_required" in msg:
        return ERROR_INVALID_KEY, (
            "This is an identity-linked API key, which isn't tied to a workspace. "
            "Either create a workspace-scoped key at "
            "https://console.anthropic.com/settings/keys, or set the "
            "ANTHROPIC_WORKSPACE_ID environment variable to your workspace id "
            "(wrkspc_...) and restart the backend."
        )
    # AUTHENTICATION — be PRECISE. Anthropic signals a bad key with
    # `authentication_error` / 401 / an x-api-key complaint. Do NOT trigger on
    # the bare word "invalid": `invalid_request_error` (a 400 about the request,
    # e.g. a bad model name or an unsupported parameter) is NOT a key problem
    # and must not be reported as one.
    if ("authentication_error" in msg or "authentication error" in msg
            or "401" in msg or "invalid x-api-key" in msg
            or "x-api-key header is invalid" in msg or "invalid api key" in msg
            or "invalid_api_key" in msg):
        return ERROR_INVALID_KEY, "The API key was not accepted by Anthropic. Check it under Settings → AI Agent Setup."
    if "permission_error" in msg or "permission" in msg or "403" in msg:
        return ERROR_MODEL_PREMIUM, ("Your Anthropic key doesn't have access to this "
                                     "model. Pick a different model, or check your Anthropic plan.")
    if "credit" in msg or "billing" in msg or "insufficient" in msg:
        return ERROR_QUOTA_EXCEEDED, "Your Anthropic account has insufficient credits. Check billing at https://console.anthropic.com/settings/billing"
    if "overloaded" in msg or "529" in msg:
        return ERROR_QUOTA_EXCEEDED, "Anthropic servers are overloaded. Try again shortly."
    if "rate_limit" in msg or "rate limit" in msg or "429" in msg:
        return ERROR_RATE_LIMITED, "Rate limit exceeded. Wait a moment and try again."
    if "not_found_error" in msg or "not found" in msg or "404" in msg:
        return ERROR_MODEL_DEPRECATED, "The selected model isn't available. Pick a different model."
    if "timeout" in msg or "connection" in msg or "network" in msg:
        return ERROR_NETWORK, "Could not reach Anthropic. Check your connection and try again."
    # invalid_request_error / 400 / anything else: surface the ACTUAL Anthropic
    # message so the real problem is visible instead of being hidden.
    return ERROR_NETWORK, _anthropic_message(exc)


def _max_output_tokens(model: str) -> int:
    """Output-token ceiling for the agent's tool-calling turns.

    The agent emits large tool-call payloads (e.g. a full create_saved_rule
    with many steps). The old flat 4096 cap truncated those into malformed
    JSON, stalling the run. Modern Claude models (3.5+, 3.7, 4.x) support at
    least 8192 output tokens with no special headers; only the legacy 3.0
    Opus/Haiku models cap at 4096, so detect those and stay safe.
    """
    m = (model or "").lower()
    if "claude-3-opus" in m or "claude-3-haiku" in m:
        return 4096
    return 8192


def _client(api_key: str):
    """Build an Anthropic client for `api_key`.

    Identity-linked API keys are scoped to a *user*, not a workspace, so the
    API can't infer which workspace a request acts in and rejects it with
    `anthropic-workspace-id is required...`. Workspace-scoped keys carry that
    context implicitly and need no header. Setting ANTHROPIC_WORKSPACE_ID makes
    both key types work; leaving it unset preserves the previous behaviour.
    """
    import anthropic
    workspace_id = (os.getenv("ANTHROPIC_WORKSPACE_ID") or "").strip()
    headers = {"anthropic-workspace-id": workspace_id} if workspace_id else None
    return anthropic.Anthropic(api_key=api_key, default_headers=headers)


class AnthropicProvider(AIProvider):

    async def validate_key(self, api_key: str) -> bool:
        try:
            client = _client(api_key)
            await asyncio.to_thread(lambda: list(client.models.list()))
            return True
        except Exception as exc:
            err_type, _ = _classify_error(exc)
            if err_type == ERROR_INVALID_KEY:
                return False
            raise

    async def list_models(self, api_key: str) -> list[ModelInfo]:
        try:
            client = _client(api_key)
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
            # Capability floor — hide deprecated/older generations (see above).
            if not _agent_capable(m.id):
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
            client = _client(api_key)

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
                max_tokens=_max_output_tokens(model_id),
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
            client = _client(api_key)

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
                        max_tokens=_max_output_tokens(model_id),
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
        import json as _json
        try:
            client = _client(api_key)

            # I18: pre-pass — drop assistant tool_use blocks whose ids never
            # got a tool_result reply (otherwise Anthropic returns 400).
            replied_ids: set[str] = set()
            for m in messages:
                if m.get("role") == "tool" and m.get("tool_call_id"):
                    replied_ids.add(str(m["tool_call_id"]))

            # Extract system prompt — Anthropic takes it as a separate param.
            system_text = ""
            anth_messages: list[dict] = []
            for m in messages:
                if m.get("role") == "system":
                    system_text = (system_text + "\n" + (m.get("content") or "")).strip()
                    continue
                if m.get("role") == "assistant":
                    parts = []
                    if m.get("content"):
                        parts.append({"type": "text", "text": m["content"]})
                    for tc in (m.get("tool_calls") or []):
                        if str(tc.get("id") or "") not in replied_ids:
                            continue   # orphan — skip
                        parts.append({
                            "type": "tool_use",
                            "id": tc["id"],
                            "name": tc["name"],
                            "input": tc.get("arguments") or {},
                        })
                    if parts:
                        anth_messages.append({"role": "assistant", "content": parts})
                elif m.get("role") == "tool":
                    anth_messages.append({
                        "role": "user",
                        "content": [{
                            "type": "tool_result",
                            "tool_use_id": m.get("tool_call_id"),
                            "content": m.get("content") or "",
                        }],
                    })
                else:  # user
                    anth_messages.append({
                        "role": "user",
                        "content": m.get("content") or "",
                    })

            # Coalesce consecutive same-role messages. Anthropic requires roles
            # to alternate; our runtime legitimately emits a tool_result (user
            # role) immediately followed by a steering/refresh user message, and
            # a user message can hold both tool_result and text blocks. Merging
            # them keeps the request valid. Also normalises string content into
            # a single text block and drops empty messages.
            coalesced: list[dict] = []
            for m in anth_messages:
                content = m.get("content")
                if isinstance(content, str):
                    content = [{"type": "text", "text": content}] if content.strip() else []
                elif not isinstance(content, list):
                    content = []
                if not content:
                    continue
                if coalesced and coalesced[-1]["role"] == m["role"]:
                    coalesced[-1]["content"].extend(content)
                else:
                    coalesced.append({"role": m["role"], "content": list(content)})
            anth_messages = coalesced

            anth_tools = [
                {
                    "name": t["name"],
                    "description": t.get("description", ""),
                    "input_schema": t.get("parameters", {"type": "object", "properties": {}}),
                }
                for t in tools
            ]

            # Prompt caching: the agent's system prompt (~850 lines) and tool
            # schemas are large and identical across every turn of a run. Mark
            # them with cache_control so Anthropic serves them from cache (5-min
            # TTL), cutting input-token cost and latency on the 2nd+ turn. Cache
            # ordering is tools → system → messages, so a breakpoint on the last
            # tool plus the system block caches the entire static prefix.
            if anth_tools:
                anth_tools[-1] = {
                    **anth_tools[-1],
                    "cache_control": {"type": "ephemeral"},
                }
            system_param = [{
                "type": "text",
                "text": system_text or "You are a helpful assistant.",
                "cache_control": {"type": "ephemeral"},
            }]

            # I19: tool_choice mapping. Internal "required" → Anthropic
            # {"type":"any"}; "none"/None → default (auto).
            #
            # Fable / Mythos removed FORCED tool use: both {'type':'any'} and
            # {'type':'tool'} return a 400 there. Fall back to auto (the
            # default) for those families rather than sending a request that
            # cannot succeed — the agent's prompt already names the tool it
            # wants, which is the documented replacement.
            anth_tool_choice = None
            if tool_choice == "required":
                anth_tool_choice = {"type": "any"}
            elif tool_choice and tool_choice not in ("auto", "none"):
                anth_tool_choice = {"type": "tool", "name": tool_choice}
            if anth_tool_choice and _no_forced_tool_choice(model):
                logger.info(
                    "Model %s does not support forced tool_choice — using auto",
                    model,
                )
                anth_tool_choice = None

            create_kwargs = dict(
                model=model,
                max_tokens=_max_output_tokens(model),
                system=system_param,
                messages=anth_messages,
                tools=anth_tools,
            )
            if anth_tool_choice:
                create_kwargs["tool_choice"] = anth_tool_choice
            # Newer Claude models deprecate/reject `temperature` (only the
            # default is allowed). Skip it up front for those families, and
            # fall back once without it if any model rejects it — so future
            # models keep working with no code change.
            if not _fixed_temperature_model(model):
                create_kwargs["temperature"] = temperature
            try:
                response = await asyncio.to_thread(
                    client.messages.create, **create_kwargs,
                )
            except Exception as exc:
                if "temperature" in create_kwargs and _is_unsupported_temperature(exc):
                    logger.info("Model %s rejected temperature — retrying without it", model)
                    create_kwargs.pop("temperature", None)
                    response = await asyncio.to_thread(
                        client.messages.create, **create_kwargs,
                    )
                else:
                    raise

            text_chunks = []
            tool_calls_out = []
            for block in (response.content or []):
                btype = getattr(block, "type", None)
                if btype == "text":
                    text_chunks.append(block.text)
                elif btype == "tool_use":
                    tool_calls_out.append({
                        "id": block.id,
                        "name": block.name,
                        "arguments": block.input or {},
                    })
            return {
                "message": {
                    "role": "assistant",
                    "content": "".join(text_chunks) or None,
                    "tool_calls": tool_calls_out,
                },
                "tool_calls": tool_calls_out,
                "finish_reason": response.stop_reason,
                "usage": {
                    "prompt_tokens": getattr(response.usage, "input_tokens", None),
                    "completion_tokens": getattr(response.usage, "output_tokens", None),
                } if response.usage else None,
            }
        except Exception as exc:
            err_type, detail = _classify_error(exc)
            raise AIError(err_type, "anthropic", detail) from exc

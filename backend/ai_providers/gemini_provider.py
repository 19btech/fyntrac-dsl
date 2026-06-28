"""Google Gemini provider implementation using the modern google-genai SDK."""

import asyncio
import json as _json
import re
import uuid
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


def _max_output_tokens(model: str) -> int:
    """Output-token ceiling for Gemini turns. All current gemini-* models
    support at least 8192 output tokens, which the agent needs for large
    tool-call payloads (a full create_saved_rule with many steps)."""
    return 8192


# JSON-schema keywords Gemini's function-calling schema validator rejects.
# `parameters_json_schema` accepts standard JSON schema but still chokes on
# these, so we strip them recursively before sending. Notably it tolerates an
# object with no `properties` (a free-form dict), which the Schema path does
# not — that's why we prefer parameters_json_schema here.
_GEMINI_SCHEMA_DROP_KEYS = frozenset({
    "additionalProperties", "$schema", "title", "examples",
    "patternProperties", "unevaluatedProperties", "$id", "$ref",
})


def _sanitize_json_schema(node):
    """Recursively strip JSON-schema keywords Gemini rejects. Returns a new
    structure; never mutates the caller's schema."""
    if isinstance(node, dict):
        return {
            k: _sanitize_json_schema(v)
            for k, v in node.items()
            if k not in _GEMINI_SCHEMA_DROP_KEYS
        }
    if isinstance(node, list):
        return [_sanitize_json_schema(x) for x in node]
    return node


def _coerce_function_response(content) -> dict:
    """Gemini's FunctionResponse.response must be a dict. Our tool observations
    are JSON strings (or dicts). Parse them, wrapping non-dict values."""
    if isinstance(content, dict):
        return content
    if isinstance(content, str):
        try:
            parsed = _json.loads(content)
        except Exception:
            return {"result": content}
        return parsed if isinstance(parsed, dict) else {"result": parsed}
    return {"result": content}


def _build_gemini_contents(messages: list[dict], types):
    """Convert our internal OpenAI-style messages into (system_text, contents)
    for Gemini. System messages are pulled out (Gemini takes them separately);
    assistant tool_calls become function_call parts (role 'model'); tool
    results become function_response parts (role 'user', the only non-model
    role Gemini supports). Consecutive same-role contents are merged."""
    # Orphan pruning: only replay an assistant function_call if its id has a
    # matching tool reply, otherwise Gemini 400s (mirrors the other providers).
    replied_ids: set[str] = set()
    for m in messages:
        if m.get("role") == "tool" and m.get("tool_call_id"):
            replied_ids.add(str(m["tool_call_id"]))

    system_chunks: list[str] = []
    contents: list = []

    def _append(role: str, parts: list):
        if not parts:
            return
        if contents and contents[-1].role == role:
            contents[-1].parts.extend(parts)
        else:
            contents.append(types.Content(role=role, parts=list(parts)))

    for m in messages:
        role = m.get("role")
        if role == "system":
            if m.get("content"):
                system_chunks.append(m["content"])
            continue
        if role == "assistant":
            parts = []
            if m.get("content"):
                parts.append(types.Part.from_text(text=m["content"]))
            for tc in (m.get("tool_calls") or []):
                if str(tc.get("id") or "") not in replied_ids:
                    continue  # orphan — skip
                args = tc.get("arguments")
                if isinstance(args, str):
                    try:
                        args = _json.loads(args)
                    except Exception:
                        args = {}
                if not isinstance(args, dict):
                    args = {}
                parts.append(types.Part(function_call=types.FunctionCall(
                    id=tc.get("id"), name=tc.get("name"), args=args)))
            _append("model", parts)
        elif role == "tool":
            part = types.Part(function_response=types.FunctionResponse(
                id=m.get("tool_call_id"),
                name=m.get("name") or "tool",
                response=_coerce_function_response(m.get("content")),
            ))
            _append("user", [part])
        else:  # user
            if m.get("content"):
                _append("user", [types.Part.from_text(text=m["content"])])

    return "\n".join(system_chunks).strip(), contents


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
        """Tool-calling chat for the autonomous agent runtime.

        Uses google-genai function calling. We drive the tool loop ourselves
        (the runtime executes tools and feeds results back), so automatic
        function calling is left off — passing FunctionDeclarations (not Python
        callables) means the SDK returns function_call parts without executing.
        """
        from google.genai import types
        try:
            client = _get_client(api_key)
            system_text, contents = _build_gemini_contents(messages, types)

            func_decls = []
            for t in tools:
                params = t.get("parameters") or {"type": "object", "properties": {}}
                func_decls.append(types.FunctionDeclaration(
                    name=t["name"],
                    description=t.get("description", ""),
                    parameters_json_schema=_sanitize_json_schema(params),
                ))
            gem_tools = [types.Tool(function_declarations=func_decls)] if func_decls else None

            # tool_choice → Gemini ToolConfig. "required" forces a call (ANY);
            # a specific tool name restricts to that function; "none" disables.
            tool_config = None
            mode_any = types.FunctionCallingConfigMode.ANY
            if tool_choice == "required":
                tool_config = types.ToolConfig(
                    function_calling_config=types.FunctionCallingConfig(mode=mode_any))
            elif tool_choice == "none":
                tool_config = types.ToolConfig(
                    function_calling_config=types.FunctionCallingConfig(
                        mode=types.FunctionCallingConfigMode.NONE))
            elif tool_choice and tool_choice != "auto":
                tool_config = types.ToolConfig(
                    function_calling_config=types.FunctionCallingConfig(
                        mode=mode_any, allowed_function_names=[tool_choice]))

            config = types.GenerateContentConfig(
                system_instruction=system_text or None,
                temperature=temperature,
                max_output_tokens=_max_output_tokens(model),
                tools=gem_tools,
                tool_config=tool_config,
            )
            response = await asyncio.to_thread(
                client.models.generate_content,
                model=model,
                contents=contents,
                config=config,
            )

            text_chunks: list[str] = []
            tool_calls_out: list[dict] = []
            cand = (response.candidates or [None])[0]
            parts = []
            if cand is not None and cand.content and cand.content.parts:
                parts = cand.content.parts
            for part in parts:
                if getattr(part, "text", None):
                    text_chunks.append(part.text)
                fc = getattr(part, "function_call", None)
                if fc:
                    tool_calls_out.append({
                        # Gemini may omit ids — synthesise one so the runtime
                        # can pair the eventual tool result back to this call.
                        "id": getattr(fc, "id", None) or uuid.uuid4().hex,
                        "name": fc.name,
                        "arguments": dict(fc.args) if fc.args else {},
                    })

            finish_reason = None
            if cand is not None:
                fr = getattr(cand, "finish_reason", None)
                finish_reason = getattr(fr, "name", None) or (str(fr) if fr else None)

            usage = None
            um = getattr(response, "usage_metadata", None)
            if um:
                usage = {
                    "prompt_tokens": getattr(um, "prompt_token_count", None),
                    "completion_tokens": getattr(um, "candidates_token_count", None),
                }

            return {
                "message": {
                    "role": "assistant",
                    "content": "".join(text_chunks) or None,
                    "tool_calls": tool_calls_out,
                },
                "tool_calls": tool_calls_out,
                "finish_reason": finish_reason,
                "usage": usage,
            }
        except Exception as exc:
            err_type, detail = _classify_error(exc)
            raise AIError(err_type, "gemini", detail) from exc

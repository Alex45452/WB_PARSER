from __future__ import annotations

import asyncio
import json
import re
from typing import Any, Optional

from openai import AsyncOpenAI

from .prompts import SYSTEM_PROMPT

RESULT_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "results": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "nm_id": {"type": "integer"},
                    "decision": {"type": "string", "enum": ["accept", "reject", "review", "accept_unknown"]},
                    "category": {"type": "string"},
                    "target": {"type": ["string", "null"]},
                    "confidence": {"type": "number"},
                    "reject_reason": {"type": ["string", "null"]},
                    "signals": {"type": "object"},
                    "explain": {"type": "object"},
                },
                "required": ["nm_id", "decision", "category", "target", "confidence", "reject_reason", "signals", "explain"],
                "additionalProperties": False,
            },
        }
    },
    "required": ["results"],
    "additionalProperties": False,
}

_JSON_FENCE_RE = re.compile(r"```(?:json)?\s*(\{.*?\})\s*```", re.DOTALL)
_FIRST_OBJ_RE = re.compile(r"(\{.*\})", re.DOTALL)


def _extract_json_text(text: str) -> str:
    """
    Tries to extract a JSON object from:
      - pure JSON
      - ```json ... ```
      - text with JSON somewhere inside
    """
    t = (text or "").strip()
    if not t:
        return t

    m = _JSON_FENCE_RE.search(t)
    if m:
        return m.group(1).strip()

    # If it already looks like JSON
    if t.startswith("{") and t.endswith("}"):
        return t

    m2 = _FIRST_OBJ_RE.search(t)
    if m2:
        return m2.group(1).strip()

    return t


def _validate_shape(obj: Any) -> bool:
    if not isinstance(obj, dict):
        return False
    results = obj.get("results")
    if not isinstance(results, list):
        return False
    # minimal per-item required keys
    req = {"nm_id", "decision", "category", "target", "confidence", "reject_reason", "signals", "explain"}
    for it in results:
        if not isinstance(it, dict):
            return False
        if not req.issubset(it.keys()):
            return False
    return True


class LLMClient:
    """
    Robust client:
      - tries Structured Outputs with response_format json_schema
      - if SDK doesn't support it, falls back to plain JSON-only prompting with 1 repair attempt
    """
    def __init__(self, api_key: str, model: str, organization: Optional[str] = None, project: Optional[str] = None):
        self.client = AsyncOpenAI(api_key=api_key, organization=organization, project=project)
        self.model = model
        self._supports_response_format: Optional[bool] = None  # autodetect

    async def _call_structured(self, user_content: str) -> dict[str, Any]:
        resp = await self.client.responses.create(
            model=self.model,
            input=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": user_content},
            ],
            response_format={
                "type": "json_schema",
                "json_schema": {
                    "name": "wb_llm_filter_results",
                    "schema": RESULT_SCHEMA,
                    "strict": True,
                },
            },
        )
        text = resp.output_text or ""
        obj = json.loads(text)
        return obj

    async def _call_plain_json(self, user_content: str) -> dict[str, Any]:
        # Ask for strict JSON (no schema enforcement)
        resp = await self.client.responses.create(
            model=self.model,
            input=[
                {"role": "system", "content": SYSTEM_PROMPT + "\n\nВЕРНИ ТОЛЬКО ВАЛИДНЫЙ JSON. НИКАКОГО ТЕКСТА."},
                {"role": "user", "content": user_content},
            ],
        )
        text = _extract_json_text(resp.output_text or "")
        obj = json.loads(text)
        return obj

    async def _repair_once(self, bad_text: str) -> dict[str, Any]:
        repair_prompt = (
            "Исправь и верни только валидный JSON-объект строго вида:\n"
            '{"results":[{"nm_id":0,"decision":"accept|reject|review|accept_unknown","category":"","target":null,'
            '"confidence":0.0,"reject_reason":null,"signals":{},"explain":{}}]}\n'
            "Никакого текста, только JSON."
        )
        resp = await self.client.responses.create(
            model=self.model,
            input=[
                {"role": "system", "content": repair_prompt},
                {"role": "user", "content": bad_text},
            ],
        )
        text = _extract_json_text(resp.output_text or "")
        obj = json.loads(text)
        return obj

    async def classify_batch(self, user_content: str, retries: int = 30) -> dict[str, Any]:
        last_err: Exception | None = None

        for attempt in range(1, retries + 1):
            try:
                # autodetect support once
                if self._supports_response_format is None or self._supports_response_format is True:
                    try:
                        obj = await self._call_structured(user_content)
                        self._supports_response_format = True
                    except TypeError:
                        # SDK method signature doesn't accept response_format
                        self._supports_response_format = False
                        obj = await self._call_plain_json(user_content)
                else:
                    obj = await self._call_plain_json(user_content)

                if _validate_shape(obj):
                    return obj

                # If shape is wrong -> try one repair
                repaired = await self._repair_once(json.dumps(obj, ensure_ascii=False))
                if _validate_shape(repaired):
                    return repaired

                # Still wrong -> raise to retry
                raise ValueError("LLM returned invalid JSON shape")

            except Exception as e:
                last_err = e
                await asyncio.sleep(min(2 ** min(attempt, 6), 120))

        raise last_err or RuntimeError("LLM classify_batch failed")
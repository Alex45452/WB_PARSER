from __future__ import annotations
import asyncio
import json
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
                    "explain": {"type": "object"}
                },
                "required": ["nm_id", "decision", "category", "target", "confidence", "reject_reason", "signals", "explain"],
                "additionalProperties": False
            }
        }
    },
    "required": ["results"],
    "additionalProperties": False
}

class LLMClient:
    def __init__(self, api_key: str, model: str):
        self.client = AsyncOpenAI(api_key=api_key)
        self.model = model

    async def classify_batch(self, user_content: str, retries: int = 30) -> dict[str, Any]:
        last_err: Exception | None = None
        for attempt in range(1, retries + 1):
            try:
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
                            "strict": True
                        }
                    }
                )
                # responses API returns structured output in output_text for json formats,
                # but safest: parse the text
                text = resp.output_text
                return json.loads(text)
            except Exception as e:
                last_err = e
                await asyncio.sleep(min(2 ** min(attempt, 6), 120))
        raise last_err or RuntimeError("LLM classify_batch failed")
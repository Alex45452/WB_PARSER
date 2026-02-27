import aiohttp
from typing import Any, Dict, Optional

async def fetch_json(session: aiohttp.ClientSession, url: str, timeout_s: float) -> Optional[Dict[str, Any]]:
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=timeout_s)) as resp:
            if resp.status != 200:
                return None
            return await resp.json(content_type=None)
    except Exception:
        return None
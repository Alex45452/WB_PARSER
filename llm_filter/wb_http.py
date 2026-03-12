import asyncio
import random
import httpx
from typing import Any, Optional


class WBHttp:
    def __init__(self, timeout_s: float, concurrency: int,
                 breaker_timeouts: int = 20, breaker_sleep_s: int = 120):
        self._timeout = httpx.Timeout(timeout_s)
        self._sem = asyncio.Semaphore(concurrency)
        self._client = httpx.AsyncClient(
            timeout=self._timeout,
            headers={
                "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                              "(KHTML, like Gecko) Chrome/122.0 Safari/537.36",
                "accept": "application/json,text/plain,*/*",
                "x-client-name":"site"
            },
        )

        self._breaker_timeouts = breaker_timeouts
        self._breaker_sleep_s = breaker_sleep_s
        self._consecutive_timeouts = 0
        self._breaker_lock = asyncio.Lock()

    async def close(self) -> None:
        await self._client.aclose()

    async def _breaker_maybe_sleep(self) -> None:
        # Called after timeout increments; if threshold reached, sleep and reset.
        async with self._breaker_lock:
            if self._consecutive_timeouts >= self._breaker_timeouts:
                await asyncio.sleep(self._breaker_sleep_s)
                self._consecutive_timeouts = 0

    async def get_json(
        self,
        url: str,
        headers: Optional[dict[str, str]] = None,
        timeout_retries: int = 10,
        other_retries: int = 30,
        backoff_base: float = 0.4,
    ) -> Any:
        last_exc: Exception | None = None

        # We handle timeouts separately with their own retry budget.
        timeout_attempts = 0
        other_attempts = 0

        while True:
            try:
                async with self._sem:
                    r = await self._client.get(url, headers=headers)

                r.raise_for_status()

                # success => reset timeout streak
                self._consecutive_timeouts = 0
                return r.json()

            except (httpx.ReadTimeout, httpx.ConnectTimeout, httpx.PoolTimeout) as e:
                last_exc = e
                timeout_attempts += 1
                self._consecutive_timeouts += 1
                await self._breaker_maybe_sleep()

                if timeout_attempts >= timeout_retries:
                    raise last_exc

                sleep_s = backoff_base * (2 ** min(timeout_attempts, 6)) + random.random() * 0.25
                await asyncio.sleep(sleep_s)

            except Exception as e:
                last_exc = e
                other_attempts += 1

                if other_attempts >= other_retries:
                    raise last_exc

                sleep_s = backoff_base * (2 ** min(other_attempts, 6)) + random.random() * 0.25
                await asyncio.sleep(sleep_s)
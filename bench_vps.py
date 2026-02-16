import asyncio
import random
import time
from dataclasses import dataclass
from typing import Dict, List, Tuple

import aiohttp
import uvloop

try:
    import orjson as jsonlib
    def loads(b: bytes): return jsonlib.loads(b)
except Exception:
    import json as jsonlib
    def loads(b: bytes): return jsonlib.loads(b)

# --- твоя карта ---
RANGES_EXACT: List[Tuple[int, int, int]] = [
    (156955254, 165599999, 10),
    (165600000, 191999999, 12),
    (192000000, 204599999, 13),
    (204600000, 218999999, 14),
    (219000000, 240599999, 15),
    (240600000, 262199999, 16),
    (262200000, 283799999, 17),
    (283800000, 305399999, 18),
    (305400000, 326999999, 19),
    (327000000, 348599999, 20),
    (348600000, 370199999, 21),
    (370200000, 391799999, 22),
    (391800000, 413399999, 23),
    (413400000, 434999999, 24),
    (435000000, 456599999, 25),
    (456600000, 487799999, 26),
    (487800000, 518999999, 27),
    (519000000, 550199999, 28),
    (550200000, 581399999, 29),
    (581400000, 612599999, 30),
    (612600000, 646659252, 31),
    (646659253, 674999999, 32),
    (675000000, 706199999, 33),
    (706200000, 730371002, 34),
]

def card_url(nm_id: int, basket: int) -> str:
    vol = nm_id // 100000
    part = nm_id // 1000
    return f"https://basket-{basket:02d}.wbbasket.ru/vol{vol}/part{part}/{nm_id}/info/ru/card.json"

def pick_random_nmids(n: int, ranges: List[Tuple[int, int, int]]) -> List[Tuple[int, int]]:
    # равномерно по ranges
    out = []
    for _ in range(n):
        start, end, basket = random.choice(ranges)
        nm_id = random.randint(start, end)
        out.append((nm_id, basket))
    return out

@dataclass
class BenchResult:
    concurrency: int
    requests: int
    seconds: float
    rps: float
    p50_ms: float
    p90_ms: float
    p99_ms: float
    status_counts: Dict[int, int]
    errors: int
    timeouts: int

def percentile(sorted_vals: List[float], p: float) -> float:
    if not sorted_vals:
        return 0.0
    k = int((len(sorted_vals) - 1) * p)
    return sorted_vals[k]

async def one_request(session: aiohttp.ClientSession, url: str, parse_json: bool, timeout_s: float):
    t0 = time.perf_counter()
    try:
        async with session.get(url, timeout=timeout_s) as resp:
            status = resp.status
            body = await resp.read()
            if parse_json and status == 200:
                # минимальный parse (имитируем “реальную” нагрузку)
                _ = loads(body)
            dt = (time.perf_counter() - t0) * 1000
            return status, dt, False, False
    except asyncio.TimeoutError:
        dt = (time.perf_counter() - t0) * 1000
        return 0, dt, False, True
    except Exception:
        dt = (time.perf_counter() - t0) * 1000
        return 0, dt, True, False

async def run_bench(concurrency: int, total_requests: int, parse_json: bool, timeout_s: float) -> BenchResult:
    connector = aiohttp.TCPConnector(limit=concurrency, ttl_dns_cache=300)
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json",
        "Connection": "keep-alive",
    }

    latencies = []
    status_counts: Dict[int, int] = {}
    errors = 0
    timeouts = 0

    sem = asyncio.Semaphore(concurrency)

    nmids = pick_random_nmids(total_requests, RANGES_EXACT)

    async with aiohttp.ClientSession(connector=connector, headers=headers) as session:
        start = time.perf_counter()

        async def worker(nm_id: int, basket: int):
            nonlocal errors, timeouts
            url = card_url(nm_id, basket)
            async with sem:
                status, dt_ms, is_error, is_timeout = await one_request(session, url, parse_json, timeout_s)
            latencies.append(dt_ms)
            status_counts[status] = status_counts.get(status, 0) + 1
            if is_error:
                errors += 1
            if is_timeout:
                timeouts += 1

        tasks = [asyncio.create_task(worker(nm, b)) for nm, b in nmids]
        await asyncio.gather(*tasks)

        seconds = time.perf_counter() - start

    latencies.sort()
    p50 = percentile(latencies, 0.50)
    p90 = percentile(latencies, 0.90)
    p99 = percentile(latencies, 0.99)

    rps = total_requests / seconds if seconds > 0 else 0.0

    return BenchResult(
        concurrency=concurrency,
        requests=total_requests,
        seconds=seconds,
        rps=rps,
        p50_ms=p50,
        p90_ms=p90,
        p99_ms=p99,
        status_counts=status_counts,
        errors=errors,
        timeouts=timeouts,
    )

async def main():
    # Набор прогонов: сеть vs CPU
    # 1) parse_json=False — меряем “чистую сеть”
    # 2) parse_json=True  — меряем “как будет в реальности”
    levels = [200, 400, 800, 1200, 1600]
    total = 50_000
    timeout_s = 6.0

    for parse_json in (False, True):
        print("\n" + "=" * 80)
        print("MODE:", "NETWORK_ONLY (no JSON parse)" if not parse_json else "REALISTIC (JSON parse)")
        print("=" * 80)

        for c in levels:
            res = await run_bench(concurrency=c, total_requests=total, parse_json=parse_json, timeout_s=timeout_s)
            print(
                f"concurrency={res.concurrency}  "
                f"rps={res.rps:.1f}  "
                f"time={res.seconds:.1f}s  "
                f"p50={res.p50_ms:.1f}ms p90={res.p90_ms:.1f}ms p99={res.p99_ms:.1f}ms  "
                f"errors={res.errors} timeouts={res.timeouts}  "
                f"statuses={dict(sorted(res.status_counts.items()))}"
            )

if __name__ == "__main__":
    uvloop.install()
    asyncio.run(main())

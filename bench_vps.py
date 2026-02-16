import asyncio
import os
import random
import time
from collections import defaultdict, deque
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

def pick_one() -> Tuple[int, int]:
    start, end, basket = random.choice(RANGES_EXACT)
    return random.randint(start, end), basket

@dataclass
class Stats:
    ok: int = 0
    not_found: int = 0
    rate_limited: int = 0
    other_status: int = 0
    timeouts: int = 0
    errors: int = 0
    bytes_rx: int = 0

class LatencyWindow:
    def __init__(self, maxlen: int = 5000):
        self.data = deque(maxlen=maxlen)

    def add_ms(self, v: float):
        self.data.append(v)

    def percentiles(self):
        if not self.data:
            return 0.0, 0.0, 0.0
        arr = sorted(self.data)
        def p(q): return arr[int((len(arr)-1)*q)]
        return p(0.50), p(0.90), p(0.99)

async def fetch_one(session, nm_id: int, basket: int, parse_json: bool, timeout_s: float):
    url = card_url(nm_id, basket)
    t0 = time.perf_counter()
    try:
        async with session.get(url, timeout=timeout_s) as resp:
            status = resp.status
            body = await resp.read()
            dt = (time.perf_counter() - t0) * 1000
            if parse_json and status == 200:
                _ = loads(body)
            return status, dt, len(body), False, False
    except asyncio.TimeoutError:
        dt = (time.perf_counter() - t0) * 1000
        return 0, dt, 0, False, True
    except Exception:
        dt = (time.perf_counter() - t0) * 1000
        return 0, dt, 0, True, False

async def worker_loop(
    session,
    sem: asyncio.Semaphore,
    stats: Stats,
    lat: LatencyWindow,
    stop_at: float,
    parse_json: bool,
    timeout_s: float,
):
    # простая retry-логика для 0/timeout/5xx/429
    while time.perf_counter() < stop_at:
        nm_id, basket = pick_one()

        async with sem:
            status, dt, nbytes, is_err, is_to = await fetch_one(session, nm_id, basket, parse_json, timeout_s)

        lat.add_ms(dt)
        stats.bytes_rx += nbytes

        if status == 200:
            stats.ok += 1
        elif status == 404:
            stats.not_found += 1
        elif status == 429:
            stats.rate_limited += 1
            # backoff
            await asyncio.sleep(0.05 + random.random() * 0.1)
        elif status == 0:
            if is_to:
                stats.timeouts += 1
                await asyncio.sleep(0.02 + random.random() * 0.05)
            elif is_err:
                stats.errors += 1
                await asyncio.sleep(0.02 + random.random() * 0.05)
            else:
                stats.errors += 1
        else:
            stats.other_status += 1

async def monitor_loop(
    stats: Stats,
    lat: LatencyWindow,
    start_t: float,
    stop_at: float,
    adjust_cb,
    interval_s: float = 5.0,
):
    prev_total = 0
    prev_t = start_t
    while True:
        now = time.perf_counter()
        if now >= stop_at:
            break

        total = stats.ok + stats.not_found + stats.rate_limited + stats.other_status + stats.timeouts + stats.errors
        dt = now - prev_t
        dreq = total - prev_total
        rps = dreq / dt if dt > 0 else 0.0

        p50, p90, p99 = lat.percentiles()

        err = stats.errors + stats.timeouts
        err_pct = (err / total * 100) if total else 0.0
        rl_pct = (stats.rate_limited / total * 100) if total else 0.0

        mbps = (stats.bytes_rx * 8) / (now - start_t) / 1e6 if (now - start_t) > 0 else 0.0

        print(
            f"[{(now-start_t):5.0f}s] rps={rps:7.1f}  p50={p50:6.1f} p90={p90:6.1f} p99={p99:6.1f}  "
            f"err%={err_pct:5.2f}  429%={rl_pct:5.2f}  mbps~={mbps:5.1f}  "
            f"ok={stats.ok} 404={stats.not_found} to={stats.timeouts} err={stats.errors}"
        )

        # авто-регулирование concurrency
        adjust_cb(p99_ms=p99, err_pct=err_pct, rl_pct=rl_pct)

        prev_total = total
        prev_t = now
        await asyncio.sleep(interval_s)

async def main():
    # Настройки (меняй через env)
    duration_s = int(os.getenv("DURATION", "180"))
    parse_json = os.getenv("PARSE_JSON", "1") == "1"
    timeout_s = float(os.getenv("TIMEOUT", "6"))
    start_concurrency = int(os.getenv("CONC", "400"))
    min_conc = int(os.getenv("MIN_CONC", "200"))
    max_conc = int(os.getenv("MAX_CONC", "900"))
    tasks = int(os.getenv("TASKS", "1200"))  # число воркеров-корутин (обычно = max_conc или чуть выше)

    # Цели авто-тюнинга
    target_p99 = float(os.getenv("TARGET_P99_MS", "1800"))
    max_err_pct = float(os.getenv("MAX_ERR_PCT", "0.2"))

    sem = asyncio.Semaphore(start_concurrency)
    current_conc = {"v": start_concurrency}

    def adjust_cb(p99_ms: float, err_pct: float, rl_pct: float):
        # если плохо — уменьшаем резко
        if err_pct > max_err_pct or p99_ms > target_p99:
            newv = max(min_conc, int(current_conc["v"] * 0.85))
        else:
            # если хорошо — растём медленно
            newv = min(max_conc, current_conc["v"] + 20)

        if newv != current_conc["v"]:
            # обновляем semaphore: делаем это аккуратно (через разницу)
            diff = newv - current_conc["v"]
            current_conc["v"] = newv
            if diff > 0:
                for _ in range(diff):
                    sem.release()
            else:
                # "забираем" пермиты постепенно
                # (просто не даём новым задачам быстро заходить)
                pass

    connector = aiohttp.TCPConnector(limit=0, ttl_dns_cache=300)  # limit=0 => без ограничения, контролируем семафором
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
    }

    stats = Stats()
    lat = LatencyWindow(maxlen=8000)

    start_t = time.perf_counter()
    stop_at = start_t + duration_s

    async with aiohttp.ClientSession(connector=connector, headers=headers) as session:
        workers = [
            asyncio.create_task(worker_loop(session, sem, stats, lat, stop_at, parse_json, timeout_s))
            for _ in range(tasks)
        ]
        mon = asyncio.create_task(monitor_loop(stats, lat, start_t, stop_at, adjust_cb))

        await asyncio.gather(*workers)
        await mon

    # финальный итог
    total = stats.ok + stats.not_found + stats.rate_limited + stats.other_status + stats.timeouts + stats.errors
    p50, p90, p99 = lat.percentiles()
    seconds = time.perf_counter() - start_t
    rps = total / seconds if seconds > 0 else 0.0
    err = stats.errors + stats.timeouts
    print("\n=== FINAL ===")
    print(f"seconds={seconds:.1f} total={total} rps={rps:.1f} p50={p50:.1f} p90={p90:.1f} p99={p99:.1f}")
    print(f"ok={stats.ok} 404={stats.not_found} 429={stats.rate_limited} other={stats.other_status} timeouts={stats.timeouts} errors={stats.errors}")
    print(f"err%={(err/total*100 if total else 0):.3f}%")

if __name__ == "__main__":
    uvloop.install()
    asyncio.run(main())

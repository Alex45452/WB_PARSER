import asyncio
import aiohttp
from typing import Optional, Tuple

TIMEOUT = aiohttp.ClientTimeout(total=6)
CONCURRENCY = 300

BASKET_HOST = "https://basket-{basket:02d}.wbbasket.ru"

def card_url(nm_id: int, basket: int) -> str:
    volume = nm_id // 100000
    part = nm_id // 1000
    return f"{BASKET_HOST.format(basket=basket)}/vol{volume}/part{part}/{nm_id}/info/ru/card.json"

async def exists(session: aiohttp.ClientSession, sem: asyncio.Semaphore, nm_id: int, basket: int) -> bool:
    url = card_url(nm_id, basket)
    async with sem:
        try:
            async with session.get(url) as resp:
                return resp.status == 200
        except Exception:
            return False

async def find_first_alive_after(
    session, sem,
    basket: int,
    start_nm: int,
    step: int = 100_000,
    max_tries: int = 2000,
) -> Optional[int]:
    nm = start_nm
    for _ in range(max_tries):
        if await exists(session, sem, nm, basket):
            return nm
        nm += step
    return None

async def find_range_end(
    session, sem,
    basket: int,
    known_alive: int,
    grow_step: int = 5_000_000,
    max_step: int = 100_000_000,
) -> int:
    lo = known_alive
    step = grow_step
    hi = lo + step

    while await exists(session, sem, hi, basket):
        lo = hi
        step = min(step * 2, max_step)
        hi = lo + step

    # binary last alive
    left, right = lo, hi
    while right - left > 1:
        mid = (left + right) // 2
        if await exists(session, sem, mid, basket):
            left = mid
        else:
            right = mid
    return left

async def binary_search_min_alive(session, sem, basket: int, low_dead: int, high_alive: int) -> int:
    lo, hi = low_dead, high_alive
    while hi - lo > 1:
        mid = (lo + hi) // 2
        if await exists(session, sem, mid, basket):
            hi = mid
        else:
            lo = mid
    return hi

async def detect_basket_range(session, sem, basket: int, start_probe: int) -> Optional[Tuple[int, int]]:
    """
    Возвращает (start, end) для basket, если он существует выше start_probe.
    """
    first = await find_first_alive_after(session, sem, basket, start_probe)
    if first is None:
        return None
    start_exact = await binary_search_min_alive(session, sem, basket, start_probe - 1, first)
    end_exact = await find_range_end(session, sem, basket, first)
    return start_exact, end_exact

async def main():
    # твой последний известный конец basket 37
    end_37 = 830_999_999

    sem = asyncio.Semaphore(CONCURRENCY)
    connector = aiohttp.TCPConnector(limit=CONCURRENCY)

    async with aiohttp.ClientSession(timeout=TIMEOUT, connector=connector) as session:

        # 1) найти диапазон 38
        r38 = await detect_basket_range(session, sem, 38, end_37 + 1)
        print("basket 38:", r38)

        if r38 is None:
            print("basket 38 не найден выше end_37 — проверь, точно ли он существует.")
            return

        start_38, end_38 = r38

        # 2) проверить basket 39
        # пробуем поиск чуть выше end_38 (если 39 есть — там он уже должен встречаться)
        r39 = await detect_basket_range(session, sem, 39, end_38 + 1)
        print("basket 39:", r39)

        # 3) на всякий случай: если 39 не найден, но ты подозреваешь, что он может начаться позже
        # можно сделать расширенный поиск дальше (например на +200M), но это уже “на будущее”.
        if r39 is None:
            print("basket 39 пока не найден (возможно, ещё не начался).")

if __name__ == "__main__":
    asyncio.run(main())
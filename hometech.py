import argparse
import asyncio
import time
from typing import Optional, Dict

import aiohttp


async def single_url_load(url: str, rps: int, concurrency: int, timeout_s: float, insecure: bool) -> None:
    timeout = aiohttp.ClientTimeout(total=timeout_s)
    connector = aiohttp.TCPConnector(limit=0 if concurrency <= 0 else max(concurrency * 2, 256), ssl=None if not insecure else False)
    sem = asyncio.Semaphore(concurrency if concurrency > 0 else 1000000)

    async def one(session: aiohttp.ClientSession) -> None:
        async with sem:
            try:
                async with session.get(url) as resp:
                    await resp.read()
            except Exception:
                pass

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        print(f"Hitting {url} at ~{rps} rps with concurrency={concurrency}")
        while True:
            base = time.perf_counter()
            spacing = 1.0 / float(rps) if rps > 0 else 0
            tasks = []
            for i in range(rps):
                target = base + i * spacing
                async def scheduled(ts: float) -> None:
                    d = ts - time.perf_counter()
                    if d > 0:
                        await asyncio.sleep(d)
                    await one(session)
                tasks.append(asyncio.create_task(scheduled(target)))
            await asyncio.gather(*tasks, return_exceptions=True)
            sleep_rem = base + 1.0 - time.perf_counter()
            if sleep_rem > 0:
                await asyncio.sleep(sleep_rem)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Single-URL load driver")
    p.add_argument("--url", required=True)
    p.add_argument("--rps", type=int, default=10000)
    p.add_argument("--concurrency", type=int, default=2500)
    p.add_argument("--timeout", type=float, default=15.0)
    p.add_argument("--insecure", action="store_true")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    asyncio.run(single_url_load(args.url, args.rps, args.concurrency, args.timeout, args.insecure))


if __name__ == "__main__":
    main()

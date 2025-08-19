import asyncio
import aiohttp
import time
from datetime import datetime

urls = [
    "https://batechdemo.in/yogathia_academy/gallery/",
    "https://batechdemo.in/hometech/",
    "https://batechdemo.in/ratchels/",
    "https://batechdemo.in/astoria/",
    "https://batechdemo.in/lkm/",
    "https://batechdemo.in/new_star_city/",
    "https://batechdemo.in/kgsv/",
    "https://batechdemo.in/lingam_oil/",
    "https://batechdemo.in/provahan/",
    "https://batechdemo.in/carportzone/",
    "https://batechdemo.in/agathiya/",
    "https://server99.batechnology.org:2087/evo/login",
    "http://mail.batechdemo.in/webmail/",
    "https://www.batechnology.org/"
]

requests_per_url = 100  # Adjust this with care
concurrency = 100        # Max concurrent requests
interval_between_rounds = 5  # seconds to wait between batches


async def fetch(url, session, sem):
    async with sem:
        try:
            async with session.get(url) as response:
                ts = datetime.utcnow().isoformat()
                print(f"[{ts}] {url} -> {response.status}")
                # read the body to complete the request
                await response.text()
        except Exception as e:
            ts = datetime.utcnow().isoformat()
            print(f"[{ts}] Error fetching {url}: {e}")


async def run_round(session, sem):
    # schedule a batch of requests (can be large); the semaphore limits concurrency
    tasks = []
    for url in urls:
        for _ in range(requests_per_url):
            tasks.append(asyncio.create_task(fetch(url, session, sem)))
    # gather but don't fail the whole batch on individual errors
    await asyncio.gather(*tasks, return_exceptions=True)


async def main():
    connector = aiohttp.TCPConnector(limit=concurrency)
    sem = asyncio.Semaphore(concurrency)
    async with aiohttp.ClientSession(connector=connector) as session:
        print("Starting continuous load test. Press Ctrl+C to stop.")
        try:
            while True:
                start = time.time()
                await run_round(session, sem)
                elapsed = time.time() - start
                print(f"Batch completed in {elapsed:.2f}s — sleeping {interval_between_rounds}s before next round")
                await asyncio.sleep(interval_between_rounds)
        except asyncio.CancelledError:
            print("Shutdown requested (cancelled).")
        except Exception as e:
            print(f"Unexpected error in main loop: {e}")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Stopped by user (KeyboardInterrupt).")

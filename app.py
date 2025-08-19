import asyncio
import argparse
import time
from typing import List, Optional, Dict

import aiohttp


class Stats:
    def __init__(self) -> None:
        self.sent_total = 0
        self.done_total = 0
        self.ok_total = 0
        self.err_total = 0
        self._window_sent = 0
        self._window_done = 0
        self._window_ok = 0
        self._window_err = 0
        self._window_latencies_ms: List[float] = []
        self._lock = asyncio.Lock()

    async def on_sent(self) -> None:
        async with self._lock:
            self.sent_total += 1
            self._window_sent += 1

    async def on_result(self, ok: bool, latency_ms: Optional[float]) -> None:
        async with self._lock:
            self.done_total += 1
            self._window_done += 1
            if ok:
                self.ok_total += 1
                self._window_ok += 1
            else:
                self.err_total += 1
                self._window_err += 1
            if latency_ms is not None:
                self._window_latencies_ms.append(latency_ms)

    async def snapshot_and_reset_window(self) -> Dict[str, float]:
        async with self._lock:
            lat = sorted(self._window_latencies_ms)
            p50 = lat[int(0.50 * len(lat))] if lat else 0.0
            p90 = lat[int(0.90 * len(lat))] if lat else 0.0
            p99 = lat[int(0.99 * len(lat))] if lat else 0.0
            snap = {
                "sent": float(self._window_sent),
                "done": float(self._window_done),
                "ok": float(self._window_ok),
                "err": float(self._window_err),
                "p50_ms": p50,
                "p90_ms": p90,
                "p99_ms": p99,
                "tot_done": float(self.done_total),
                "tot_ok": float(self.ok_total),
                "tot_err": float(self.err_total),
            }
            self._window_sent = 0
            self._window_done = 0
            self._window_ok = 0
            self._window_err = 0
            self._window_latencies_ms = []
            return snap


async def fetch(session: aiohttp.ClientSession, sem: asyncio.Semaphore, stats: Stats, method: str, url: str, headers: Optional[Dict[str, str]], data: Optional[bytes]) -> None:
    await stats.on_sent()
    start = time.perf_counter()
    async with sem:
        try:
            async with session.request(method=method, url=url, headers=headers, data=data) as resp:
                # Drain the response to completion to measure full latency
                await resp.read()
                ok = 200 <= resp.status < 400
        except Exception:
            ok = False
        finally:
            latency_ms = (time.perf_counter() - start) * 1000.0
            await stats.on_result(ok, latency_ms)


async def stats_printer(stats: Stats, stop_event: asyncio.Event) -> None:
    while not stop_event.is_set():
        await asyncio.sleep(1.0)
        snap = await stats.snapshot_and_reset_window()
        print(
            f"rps sent={int(snap['sent'])} done={int(snap['done'])} ok={int(snap['ok'])} err={int(snap['err'])} "
            f"lat(ms) p50={snap['p50_ms']:.1f} p90={snap['p90_ms']:.1f} p99={snap['p99_ms']:.1f} "
            f"totals done={int(snap['tot_done'])} ok={int(snap['tot_ok'])} err={int(snap['tot_err'])}"
        )


async def run_load(urls: List[str], rps: int, concurrency: int, method: str, timeout_s: float, headers: Optional[Dict[str, str]], data: Optional[bytes], duration_s: Optional[int], verify_tls: bool) -> None:
    timeout = aiohttp.ClientTimeout(total=timeout_s)
    connector = aiohttp.TCPConnector(limit=0 if concurrency <= 0 else max(concurrency * 2, 256), ssl=None if verify_tls else False)
    sem = asyncio.Semaphore(concurrency if concurrency > 0 else 1000000)
    stats = Stats()
    stop_event = asyncio.Event()
    all_tasks: set = set()

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        printer_task = asyncio.create_task(stats_printer(stats, stop_event))

        try:
            start_wall = time.perf_counter()
            rr = 0
            while True:
                # Stop if duration elapsed
                if duration_s is not None and (time.perf_counter() - start_wall) >= duration_s:
                    break

                second_base = time.perf_counter()
                if rps <= 0:
                    await asyncio.sleep(1.0)
                    continue

                spacing = 1.0 / float(rps)
                for i in range(rps):
                    scheduled_at = second_base + (i * spacing)
                    url = urls[rr % len(urls)]
                    rr += 1

                    async def scheduled_fetch(target_ts: float, the_url: str) -> None:
                        now = time.perf_counter()
                        delay = target_ts - now
                        if delay > 0:
                            await asyncio.sleep(delay)
                        await fetch(session, sem, stats, method, the_url, headers, data)

                    t = asyncio.create_task(scheduled_fetch(scheduled_at, url))
                    all_tasks.add(t)
                    t.add_done_callback(lambda _t: all_tasks.discard(_t))

                # Wait until the end of the current one-second window to keep pacing stable
                to_sleep = second_base + 1.0 - time.perf_counter()
                if to_sleep > 0:
                    await asyncio.sleep(to_sleep)

        finally:
            stop_event.set()
            await printer_task
            # Graceful shutdown: wait for any in-flight tasks to complete
            if all_tasks:
                await asyncio.gather(*list(all_tasks), return_exceptions=True)


def parse_headers(header_items: List[str]) -> Dict[str, str]:
    headers: Dict[str, str] = {}
    for item in header_items:
        if ":" not in item:
            continue
        k, v = item.split(":", 1)
        headers[k.strip()] = v.strip()
    return headers


def load_urls_from_file(path: str) -> List[str]:
    urls: List[str] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s or s.startswith("#"):
                continue
            urls.append(s)
    return urls


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Asynchronous rate-limited HTTP load generator")
    p.add_argument("--url", action="append", help="Target URL (repeatable). If omitted, use --urls-file.")
    p.add_argument("--urls-file", help="Path to file with newline-separated URLs.")
    p.add_argument("--rps", type=int, default=100000, help="Target requests per second (default: 100000)")
    p.add_argument("--concurrency", type=int, default=30000, help="Max concurrent in-flight requests (default: 30000)")
    p.add_argument("--method", default="GET", help="HTTP method (default: GET)")
    p.add_argument("--timeout", type=float, default=15.0, help="Total request timeout seconds (default: 15)")
    p.add_argument("--header", action="append", default=[], help="Extra header 'Name: Value' (repeatable)")
    p.add_argument("--body-file", help="Path to request body file (for POST/PUT/PATCH)")
    p.add_argument("--duration", type=int, help="Duration to run in seconds (default: infinite)")
    p.add_argument("--insecure", action="store_true", help="Disable TLS certificate verification")
    return p


def main() -> None:
    parser = build_arg_parser()
    args = parser.parse_args()

    urls: List[str] = []
    if args.url:
        urls.extend(args.url)
    if args.urls_file:
        urls.extend(load_urls_from_file(args.urls_file))

    if not urls:
        # Sensible defaults matching the user's environment if nothing provided
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
            "https://www.batechnology.org/",
            "https://batechdemo.in/posteit/"
        ]

    headers = parse_headers(args.header)
    data = None
    if args.body_file:
        with open(args.body_file, "rb") as bf:
            data = bf.read()

    try:
        asyncio.run(
            run_load(
                urls=urls,
                rps=max(0, int(args.rps)),
                concurrency=max(1, int(args.concurrency)),
                method=str(args.method).upper(),
                timeout_s=float(args.timeout),
                headers=headers if headers else None,
                data=data,
                duration_s=int(args.duration) if args.duration is not None else None,
                verify_tls=(not bool(args.insecure)),
            )
        )
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()


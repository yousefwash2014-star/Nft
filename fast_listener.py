# --- SPEED OPTIMIZATION HELPERS ---
import time as _speed_time
from collections import OrderedDict as _SpeedOrderedDict

class TTLCache:
    """Small in-process TTL cache for hot-path RPC/API data."""
    def __init__(self, ttl=1.0, maxsize=4096):
        self.ttl = ttl
        self.maxsize = maxsize
        self._data = _SpeedOrderedDict()

    def get(self, key, default=None):
        item = self._data.get(key)
        if item is None:
            return default
        value, expires = item
        if expires <= _speed_time.monotonic():
            self._data.pop(key, None)
            return default
        self._data.move_to_end(key)
        return value

    def set(self, key, value, ttl=None):
        self._data[key] = (value, _speed_time.monotonic() + (self.ttl if ttl is None else ttl))
        self._data.move_to_end(key)
        while len(self._data) > self.maxsize:
            self._data.popitem(last=False)

    def clear(self):
        self._data.clear()

# Keep hot-path caches tiny and short-lived.
RPC_RESULT_CACHE = TTLCache(ttl=0.30, maxsize=8192)
DROP_CACHE = TTLCache(ttl=2.0, maxsize=4096)
# --- END SPEED OPTIMIZATION HELPERS ---
"""Fast OpenSea Stream listener with bounded dispatch and safe shutdown."""
import asyncio
import inspect
import logging
import threading
import time
from typing import Callable, Optional, Dict

import websockets

try:
    import orjson as _orjson
    def _dumps(o) -> bytes:
        return _orjson.dumps(o)
except ImportError:
    import json as _json
    def _dumps(o) -> bytes:
        return _json.dumps(o, separators=(",", ":")).encode("utf-8")

log = logging.getLogger("fast-listener")
B_ITEM_TRANSFERRED = b"item_transferred"
B_ZERO_ADDR = b"0000000000000000000000000000000000000000"
B_SLUG = b'"slug"'
B_NFT_ID = b'"nft_id"'
CHAIN_BYTES = (
    (b'"name":"ethereum"', "ethereum"), (b'"name": "ethereum"', "ethereum"),
    (b'"name":"eth"', "ethereum"), (b'"name": "eth"', "ethereum"),
    (b'"name":"robinhood"', "robinhood"), (b'"name": "robinhood"', "robinhood"),
    (b'"name":"ink"', "ink"), (b'"name": "ink"', "ink"),
    (b'"name":"arc"', "arc"), (b'"name": "arc"', "arc"),
    (b'"name":"polygon"', "polygon"), (b'"name": "polygon"', "polygon"),
)


def _extract_quoted_after(data: bytes, key: bytes) -> Optional[bytes]:
    idx = data.find(key)
    if idx < 0:
        return None
    colon = data.find(b":", idx + len(key))
    if colon < 0:
        return None
    q1 = data.find(b'"', colon + 1)
    if q1 < 0:
        return None
    q2 = q1 + 1
    while q2 < len(data):
        q2 = data.find(b'"', q2)
        if q2 < 0:
            return None
        if data[q2 - 1:q2] != b"\\":
            return data[q1 + 1:q2]
        q2 += 1
    return None


def fast_parse_event(raw: bytes) -> Optional[dict]:
    if not isinstance(raw, (bytes, bytearray)) or B_ITEM_TRANSFERRED not in raw:
        return None
    raw = bytes(raw)
    raw_lower = raw.lower()
    if B_ZERO_ADDR not in raw_lower:
        return None
    chain_key = next((key for pattern, key in CHAIN_BYTES if pattern in raw_lower), None)
    if not chain_key:
        return None
    slug_b = _extract_quoted_after(raw, B_SLUG)
    if not slug_b:
        return None
    contract = None
    nft_b = _extract_quoted_after(raw, B_NFT_ID)
    if nft_b and b"/" in nft_b:
        parts = nft_b.split(b"/")
        if len(parts) >= 2 and parts[1].lower().startswith(b"0x"):
            candidate = parts[1].decode("ascii", errors="ignore")
            if len(candidate) == 42:
                contract = candidate
    return {"chain": chain_key, "slug": slug_b.decode("utf-8", errors="ignore"), "contract": contract}


class MultiWSListener:
    """يدير عدة اتصالات WebSocket، ويدعم عدة مفاتيح Stream (لكل مفتاح URL)."""

    def __init__(self, stream_urls=None, on_event: Callable = None, workers: int = 3,
                 max_inflight_events: int = 64, stream_url: str = None):
        # backward-compat: stream_url= مدعوم أيضاً
        if stream_urls is None and stream_url is not None:
            stream_urls = [stream_url]
        if isinstance(stream_urls, str):
            stream_urls = [stream_urls]
        self.stream_urls = [u for u in (stream_urls or []) if u] or [""]
        self.on_event = on_event
        self.workers = max(1, workers)
        self.max_inflight_events = max(1, max_inflight_events)
        self._stats = {"events": 0, "prefiltered": 0, "matched": 0, "errors": 0,
                       "reconnects": 0, "dropped": 0}
        self._stats_lock = threading.Lock()
        self._stop = False
        self._event_sem: Optional[asyncio.Semaphore] = None
        self._active_tasks = set()

    def get_stats(self) -> dict:
        with self._stats_lock:
            return dict(self._stats)

    def _bump(self, key: str, n: int = 1):
        with self._stats_lock:
            self._stats[key] = self._stats.get(key, 0) + n

    def stop(self):
        self._stop = True

    async def _dispatch(self, parsed: dict, worker_id: int):
        assert self._event_sem is not None
        try:
            try:
                result = self.on_event(parsed)
                if inspect.isawaitable(result):
                    await result
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._bump("errors")
                log.debug("[WS-%s] dispatch: %s", worker_id, exc)
        finally:
            self._event_sem.release()

    def _start_dispatch(self, parsed: dict, worker_id: int):
        task = asyncio.create_task(self._dispatch(parsed, worker_id))
        self._active_tasks.add(task)
        task.add_done_callback(self._active_tasks.discard)

    async def _worker(self, worker_id: int, stream_url: str):
        msg_ref = 0
        while not self._stop:
            try:
                async with websockets.connect(
                    stream_url, ping_interval=None, open_timeout=5,
                    max_queue=32768, close_timeout=3, compression=None, max_size=None,
                ) as ws:
                    join_ref = str(msg_ref)
                    await ws.send(_dumps([join_ref, join_ref, "collection:*", "phx_join", {}]).decode())
                    msg_ref += 1
                    log.info("[WS-%s] connected", worker_id)
                    last_hb = time.monotonic()
                    while not self._stop:
                        if time.monotonic() - last_hb > 15:
                            hb_ref = str(msg_ref)
                            await ws.send(_dumps([None, hb_ref, "phoenix", "heartbeat", {}]).decode())
                            msg_ref += 1
                            last_hb = time.monotonic()
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=3.0)
                        except asyncio.TimeoutError:
                            continue
                        if isinstance(raw, str):
                            raw = raw.encode("utf-8", errors="ignore")
                        self._bump("events")
                        parsed = fast_parse_event(raw)
                        if not parsed:
                            self._bump("prefiltered")
                            continue
                        self._bump("matched")
                        assert self._event_sem is not None
                        await self._event_sem.acquire()
                        self._start_dispatch(parsed, worker_id)
            except (websockets.ConnectionClosed, OSError, asyncio.TimeoutError) as exc:
                if self._stop:
                    break
                self._bump("reconnects")
                log.warning("[WS-%s] disconnected: %s; reconnecting", worker_id, exc)
                await asyncio.sleep(1)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                if self._stop:
                    break
                self._bump("errors")
                log.error("[WS-%s] error: %s", worker_id, exc)
                await asyncio.sleep(2)

    async def run(self):
        self._event_sem = asyncio.Semaphore(self.max_inflight_events)
        n_urls = len(self.stream_urls)
        log.info("starting %s workers across %s stream URL(s)", self.workers, n_urls)

        workers = []
        for i in range(self.workers):
            url = self.stream_urls[i % n_urls]
            workers.append(asyncio.create_task(self._worker(i, url)))

        try:
            await asyncio.gather(*workers)
        finally:
            for task in workers:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*workers, return_exceptions=True)
            if self._active_tasks:
                await asyncio.gather(*list(self._active_tasks), return_exceptions=True)


class PreSignedPool:
    def __init__(self):
        self._pool: Dict[str, Dict[str, str]] = {}
        self._lock = threading.Lock()

    def prepare(self, slug: str, wallet: str, raw_hex: str):
        with self._lock:
            self._pool.setdefault(slug, {})[wallet.lower()] = raw_hex

    def get(self, slug: str, wallet: str) -> Optional[str]:
        with self._lock:
            return self._pool.get(slug, {}).get(wallet.lower())

    def clear(self, slug: Optional[str] = None):
        with self._lock:
            if slug:
                self._pool.pop(slug, None)
            else:
                self._pool.clear()

    def size(self, slug: str) -> int:
        with self._lock:
            return len(self._pool.get(slug, {}))

    def slugs(self):
        with self._lock:
            return list(self._pool.keys())
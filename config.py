"""إعدادات مركزية — TURBO++ v4.5 (FAST محاولتان → SAFE)"""
import itertools
import os
import threading
import asyncio
import time
import logging
from typing import List
from dotenv import load_dotenv

load_dotenv()

_log = logging.getLogger("config")


def env_bool(k, default=False):
    v = os.getenv(k, "").strip().lower()
    if v in {"1", "true", "yes", "on"}:
        return True
    if v in {"0", "false", "no", "off"}:
        return False
    return default


def env_int(k, default=0):
    try:
        return int(os.getenv(k, str(default)))
    except (TypeError, ValueError):
        return default


def env_float(k, default=0.0):
    try:
        return float(os.getenv(k, str(default)))
    except (TypeError, ValueError):
        return default


def split_csv(raw):
    out = []
    seen = set()
    for x in (raw or "").split(","):
        x = x.strip().lower()
        if x and x not in seen:
            seen.add(x)
            out.append(x)
    return out


class APIKeyRotator:
    def __init__(self, keys: List[str]):
        self.keys = [x for x in keys if x]
        self.i = 0
        self.lock = threading.Lock()

    def next(self):
        if not self.keys:
            return ""
        with self.lock:
            x = self.keys[self.i % len(self.keys)]
            self.i += 1
            return x

    def all_keys(self):
        return list(self.keys)

    def count(self):
        return len(self.keys)


class RPCPool:
    def __init__(self, name, urls):
        self.name = name
        self.urls = urls
        self.cycle = itertools.cycle(urls) if urls else None
        self.lock = threading.Lock()

    def next(self):
        if not self.cycle:
            return ""
        with self.lock:
            return next(self.cycle)

    def all_urls(self):
        return list(self.urls)

    def count(self):
        return len(self.urls)


class AsyncRateLimiter:
    def __init__(self, requests_per_second: float):
        self.interval = 1.0 / max(float(requests_per_second), 0.1)
        self._last = 0.0
        self._blocked_until = 0.0
        self._lock = asyncio.Lock()

    async def wait(self):
        async with self._lock:
            now = time.monotonic()
            delay = max(self._blocked_until - now,
                        self.interval - (now - self._last), 0.0)
            if delay > 0:
                await asyncio.sleep(delay)
            self._last = time.monotonic()

    def penalize(self, retry_after: float = 5.0):
        self._blocked_until = max(
            self._blocked_until,
            time.monotonic() + max(1.0, float(retry_after))
        )


# الشبكات ثابتة داخل الإصدار؛ روابط RPC والمفاتيح الخاصة فقط من الاستضافة.
DEFAULT_CHAINS = "ethereum,robinhood,ink,arc,elysium"
ENABLED_CHAINS_LIST = split_csv(DEFAULT_CHAINS)


_ALL_CHAIN_CONFIG = {
    "ethereum":  {"chain_id": 1,     "name": "Ethereum Mainnet", "native": "ETH"},
    "robinhood": {"chain_id": 4663,  "name": "Robinhood Chain",  "native": "ETH"},
    "ink":       {"chain_id": 57073, "name": "Ink",              "native": "ETH"},
    "arc":       {"chain_id": 5042,  "name": "ARC",              "native": "USDC"},
    "elysium":   {"chain_id": 1339,  "name": "Elysium Mainnet",   "native": "ELY"},
    "polygon":   {"chain_id": 137,   "name": "Polygon Mainnet",  "native": "POL"},
}

CHAIN_CONFIG = {
    k: v for k, v in _ALL_CHAIN_CONFIG.items()
    if k in ENABLED_CHAINS_LIST
}

if not CHAIN_CONFIG:
    _log.critical("🔴 CHAIN_CONFIG فارغ — تحقق من ENABLED_CHAINS في .env")
    _log.info("   المتاح: %s", ", ".join(_ALL_CHAIN_CONFIG.keys()))


def load_api_keys():
    vals = split_csv(os.getenv("OPENSEA_API_KEYS", ""))
    legacy = os.getenv("OPENSEA_API_KEY", "").strip()
    if legacy and legacy not in vals:
        vals.append(legacy)
    return APIKeyRotator(vals)


def load_rpc_pools():
    specs = {
        "ethereum":  ("ETHEREUM_RPC_URLS",  "ETHEREUM_RPC_URL"),
        "robinhood": ("ROBINHOOD_RPC_URLS", "ROBINHOOD_RPC_URL"),
        "ink":       ("INK_RPC_URLS",       "INK_RPC_URL"),
        "arc":       ("ARC_RPC_URLS",       "ARC_RPC_URL"),
        "elysium":   ("ELYSIUM_RPC_URLS",   "ELYSIUM_RPC_URL"),
        "polygon":   ("POLYGON_RPC_URLS",   "POLYGON_RPC_URL"),
    }
    pools = {}
    for name, (multi, single) in specs.items():
        if name not in ENABLED_CHAINS_LIST:
            continue
        urls = (['https://rpc.elysiumchain.tech', 'https://rpc.elysiumchain.us']
                if name == 'elysium' else split_csv(os.getenv(multi, "")))
        for u in split_csv(os.getenv(single, "")):
            if u not in urls:
                urls.append(u)
        pools[name] = RPCPool(name, urls)
    return pools


OPENSEA_KEYS = load_api_keys()
RPC_POOLS = load_rpc_pools()
OPENSEA_RATE_LIMITER = AsyncRateLimiter(env_float("OPENSEA_RATE_PER_SECOND", 3.0))

_log.info("🌐 ENABLED_CHAINS: %s", ", ".join(ENABLED_CHAINS_LIST))
_log.info("🌐 CHAIN_CONFIG: %s", ", ".join(CHAIN_CONFIG.keys()))
_log.info("🌐 RPC_POOLS: %s", ", ".join(RPC_POOLS.keys()))

ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"

OPENSEA_DROPS_BASE = "https://api.opensea.io/api/v2/drops"
OPENSEA_ACCOUNT_BASE = "https://api.opensea.io/api/v2/account"
OPENSEA_STREAM_BASE = "wss://stream-api.opensea.io/socket/websocket?token={token}&vsn=2.0.0"
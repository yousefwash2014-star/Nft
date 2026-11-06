"""
محرك الشراء التلقائي الموحد عبر عقد SeaDrop ودعم المينت الشامل.
  - نظام RPC متعدد ومتناوب
  - دعم Ethereum + Robinhood + Arc + Ink
  - 🏁 Race Mode + 💥 Cascade Sniping
  - 💰 Gas Cap لكل سلسلة: Robinhood=$0.10, Ink=$0.10, Arc=$0.05
"""

import logging
import os
import threading
import time
import itertools
import re
import requests
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Optional

from web3 import Web3

log = logging.getLogger("buyer")

# ===========================================================================
# 🔄 RPC Pools
# ===========================================================================

FREE_RPC_POOLS = {
    "ethereum": [
        "https://eth.llamarpc.com",
        "https://ethereum.publicnode.com",
        "https://1rpc.io/eth",
    ],
    "robinhood": [
        "https://rpc.mainnet.chain.robinhood.com",
        "https://robinhood-mainnet.g.alchemy.com/v2/demo",
    ],
    "ink": [
        "https://rpc-gel.inkonchain.com",
        "https://rpc-qnd.inkonchain.com",
        "https://rpc.inkonchain.com",
        "https://ink.drpc.org",
    ],
    "arc": [
        "https://rpc.mainnet.arc.io",
        "https://rpc.drpc.mainnet.arc.io",
    ],
}

_rpc_cycles: dict[str, itertools.cycle] = {}
_rpc_cycle_locks: dict[str, threading.Lock] = {}
_rpc_health: dict[str, tuple[float, int]] = {}
_rpc_health_lock = threading.Lock()
CHAIN_NAME_BY_ID = {1: "ethereum", 4663: "robinhood", 57073: "ink", 5042: "arc"}
REQUIRE_MINT_STATS = True
REQUIRE_OWNERSHIP_CHECK = True


def _get_next_rpc(chain_name: str) -> str:
    if chain_name not in _rpc_cycles:
        pool = FREE_RPC_POOLS.get(chain_name, [])
        if not pool:
            return ""
        _rpc_cycles[chain_name] = itertools.cycle(pool)
        _rpc_cycle_locks[chain_name] = threading.Lock()
    with _rpc_cycle_locks[chain_name]:
        return next(_rpc_cycles[chain_name])


def inject_free_rpcs(chain_name: str, primary_rpc: str) -> None:
    if chain_name not in FREE_RPC_POOLS:
        FREE_RPC_POOLS[chain_name] = []
    if primary_rpc and primary_rpc not in FREE_RPC_POOLS[chain_name]:
        FREE_RPC_POOLS[chain_name].insert(0, primary_rpc)
    pool = FREE_RPC_POOLS[chain_name]
    if pool:
        _rpc_cycles[chain_name] = itertools.cycle(pool)
        _rpc_cycle_locks[chain_name] = threading.Lock()


def _rpc_candidates(chain_name: str, fallback_rpc: str = "") -> list[str]:
    pool = list(dict.fromkeys(FREE_RPC_POOLS.get(chain_name, [])))
    if fallback_rpc and fallback_rpc not in pool:
        pool.insert(0, fallback_rpc)
    if not pool:
        return []
    with _rpc_health_lock:
        start = getattr(_rpc_candidates, "_cursor", {}).get(chain_name, 0) % len(pool)
        cursor = getattr(_rpc_candidates, "_cursor", {})
        cursor[chain_name] = (start + 1) % len(pool)
        _rpc_candidates._cursor = cursor
        now = time.monotonic()
        healthy = [url for url in pool if _rpc_health.get(url, (0.0, 0))[0] <= now]
    candidates = healthy or pool
    start = start % len(candidates)
    return candidates[start:] + candidates[:start]


def _mark_rpc_failure(rpc_url: str) -> None:
    with _rpc_health_lock:
        _, failures = _rpc_health.get(rpc_url, (0.0, 0))
        failures += 1
        cooldown = min(60.0, 2.0 ** min(failures, 5))
        _rpc_health[rpc_url] = (time.monotonic() + cooldown, failures)


def _mark_rpc_success(rpc_url: str) -> None:
    with _rpc_health_lock:
        _rpc_health.pop(rpc_url, None)


def get_web3_rotating(chain_name: str, fallback_rpc: str = "") -> Optional[Web3]:
    """يتصل بأفضل RPC متاح ويضع الرابط الفاشل في تبريد مؤقت."""
    for rpc in _rpc_candidates(chain_name, fallback_rpc):
        try:
            w3 = Web3(Web3.HTTPProvider(rpc, request_kwargs={"timeout": 3}))
            if w3.is_connected():
                _mark_rpc_success(rpc)
                log.debug("RPC healthy: %s", rpc)
                return w3
        except Exception as exc:
            _mark_rpc_failure(rpc)
            log.debug("RPC failed for %s: %s", chain_name, exc)
    return None


# ===========================================================================
# 🚦 RPC Rate Limiter
# ===========================================================================

class _RpcRateLimiter:
    def __init__(self, rate: float = 18.0):
        self._rate = rate
        self._tokens = rate
        self._last = time.monotonic()
        self._lock = threading.Lock()

    def acquire(self):
        while True:
            with self._lock:
                now = time.monotonic()
                self._tokens = min(self._rate, self._tokens + (now - self._last) * self._rate)
                self._last = now
                if self._tokens >= 1:
                    self._tokens -= 1
                    return
                wait = (1 - self._tokens) / self._rate
            time.sleep(wait)


_rpc_limiter = _RpcRateLimiter(rate=18.0)
_rpc_limiter_fast = _RpcRateLimiter(rate=40.0)
_mint_api_local = threading.local()

def _mint_api_session():
    session = getattr(_mint_api_local, "session", None)
    if session is None:
        session = requests.Session()
        _mint_api_local.session = session
    return session

# ===========================================================================
# إعدادات السلاسل
# ===========================================================================

CHAINS_CONFIG = {
    "ethereum": {
        "rpc_env_var": "ETHEREUM_RPC_URL",
        "chain_id": 1,
        "seadrop_address": Web3.to_checksum_address("0x00005EA00Ac477B1030CE78506496e8C2dE24bf5"),
        "chain_name_display": "Ethereum Mainnet",
        "native_currency": "ETH",
        "explorer_url": "https://etherscan.io/tx/{tx}",
    },
    "robinhood": {
        "rpc_env_var": "ROBINHOOD_RPC_URL",
        "chain_id": 4663,
        "seadrop_address": Web3.to_checksum_address("0x00005EA00Ac477B1030CE78506496e8C2dE24bf5"),
        "chain_name_display": "Robinhood Chain",
        "native_currency": "ETH",
        "explorer_url": "https://robinhoodchain.blockscout.com/tx/{tx}",
    },
    "ink": {
        "rpc_env_var": "INK_RPC_URL",
        "chain_id": 57073,
        "seadrop_address": Web3.to_checksum_address("0x00005EA00Ac477B1030CE78506496e8C2dE24bf5"),
        "chain_name_display": "Ink Chain",
        "native_currency": "ETH",
        "explorer_url": "https://explorer.inkonchain.com/tx/{tx}",
    },
    "arc": {
        "rpc_env_var": "ARC_RPC_URL",
        "chain_id": 5042,
        "seadrop_address": Web3.to_checksum_address("0x00005EA00Ac477B1030CE78506496e8C2dE24bf5"),
        "chain_name_display": "Arc Mainnet",
        "native_currency": "USDC",
        "native_decimals": 18,
        "native_price_usd": 1.0,
        "explorer_url": "https://explorer.arc.io/tx/{tx}",
    },
}

ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"

SEADROP_ABI = [
    {
        "inputs": [
            {"name": "nftContract", "type": "address"},
            {"name": "feeRecipient", "type": "address"},
            {"name": "minterIfNotPayer", "type": "address"},
            {"name": "quantity", "type": "uint256"},
        ],
        "name": "mintPublic",
        "outputs": [],
        "stateMutability": "payable",
        "type": "function",
    },
    {
        "inputs": [{"name": "nftContract", "type": "address"}],
        "name": "getAllowedFeeRecipients",
        "outputs": [{"name": "", "type": "address[]"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [{"name": "nftContract", "type": "address"}],
        "name": "getPublicDrop",
        "outputs": [{
            "components": [
                {"name": "mintPrice", "type": "uint80"},
                {"name": "startTime", "type": "uint48"},
                {"name": "endTime", "type": "uint48"},
                {"name": "maxTotalMintableByWallet", "type": "uint16"},
                {"name": "feeBps", "type": "uint16"},
                {"name": "restrictFeeRecipients", "type": "bool"},
            ],
            "name": "",
            "type": "tuple",
        }],
        "stateMutability": "view",
        "type": "function",
    },
]

ERC721_BALANCE_ABI = [{
    "inputs": [{"name": "owner", "type": "address"}],
    "name": "balanceOf", "outputs": [{"name": "", "type": "uint256"}],
    "stateMutability": "view", "type": "function",
}]
ERC1155_BALANCE_ABI = [{
    "inputs": [{"name": "account", "type": "address"}, {"name": "id", "type": "uint256"}],
    "name": "balanceOf", "outputs": [{"name": "", "type": "uint256"}],
    "stateMutability": "view", "type": "function",
}]


NFT_MINT_STATS_ABI = [
    {
        "inputs": [{"name": "minter", "type": "address"}],
        "name": "getMintStats",
        "outputs": [
            {"name": "minterNumMinted", "type": "uint256"},
            {"name": "currentTotalSupply", "type": "uint256"},
            {"name": "maxSupply", "type": "uint256"},
        ],
        "stateMutability": "view",
        "type": "function",
    },
]

# ===========================================================================
# إعدادات الأمان
# ===========================================================================
MAX_GAS_FEE_USD = 0.5
MIN_BALANCE_RESERVE_USD = 0.10
WAIT_FOR_RECEIPT = True
RECEIPT_TIMEOUT_SECONDS = 45
FEW_THRESHOLD = 20
LIMITED_BUY_QTY = 50
GAS_LIMIT_SAFETY_MARGIN = 1.2
MAX_ETH_PER_TX = 0
SKIP_BALANCE_CHECK_RACE = True
FIXED_GAS_LIMIT = 185000
FAST_SUBMIT_MODE = SKIP_BALANCE_CHECK_RACE and FIXED_GAS_LIMIT > 0
FAST_FIRST_ENABLED = True

RACE_MODE = False
RACE_GAS_BOOST = 1.35
RACE_MAX_ATTEMPTS = 1
RACE_RETRY_DELAY_MS = 120
RACE_PRE_SUBMIT_SECONDS = 0.6

# الحماية الافتراضية: معاملة واحدة لكل محفظة/مينت.
ALLOW_MULTIPLE_TX_PER_MINT = False
RACE_CASCADE_ENABLED = False
RACE_CASCADE_SIZE = 3
RACE_CASCADE_DELAY_MS = 35
RACE_CASCADE_GAS_STEP = 1.18
RACE_MAX_WORKERS = 8

# ===========================================================================
# 💰 Gas Caps لكل سلسلة
# ===========================================================================
DEFAULT_GAS_CAP_BY_CHAIN: dict[str, float] = {
    "ethereum": 0.50,
    "robinhood": 0.10,
    "ink":       0.10,
    "arc":       0.05,
}

RACE_MAX_GAS_FEE_USD_FALLBACK = 0.20


def _resolve_gas_cap(chain_name: str) -> float:
    if chain_name in DEFAULT_GAS_CAP_BY_CHAIN:
        return DEFAULT_GAS_CAP_BY_CHAIN[chain_name]
    return RACE_MAX_GAS_FEE_USD_FALLBACK


CHAIN_GAS_CAPS: dict[str, float] = {
    chain: _resolve_gas_cap(chain)
    for chain in ("ethereum", "robinhood", "ink", "arc")
}


def get_chain_gas_cap(chain_name: str) -> float:
    return CHAIN_GAS_CAPS.get(chain_name, RACE_MAX_GAS_FEE_USD_FALLBACK)


log.info(
    "💰 Gas caps per chain (USD): %s",
    ", ".join(f"{k}=${v:.2f}" for k, v in CHAIN_GAS_CAPS.items()),
)

RETRYABLE_REASONS = {
    "gas_too_high", "gas_too_high_precise", "tx_error",
    "simulation_failed", "no_fee_recipient", "race_not_started",
}

# ===========================================================================
# Nonce Lock
# ===========================================================================
nonce_locks: dict[str, threading.Lock] = {}
nonce_locks_lock = threading.Lock()


def get_wallet_lock(address: str, chain_id: int = 0) -> threading.Lock:
    key = f"{chain_id}:{address.lower()}"
    with nonce_locks_lock:
        if key not in nonce_locks:
            nonce_locks[key] = threading.Lock()
        return nonce_locks[key]


# ===========================================================================
# كاشات
# ===========================================================================
_balance_cache: dict = {}
_balance_cache_lock = threading.Lock()
BALANCE_CACHE_TTL = 30

_fee_recipients_cache: dict = {}
_fee_recipients_lock = threading.Lock()

_gas_price_cache: dict = {}
_gas_price_cache_lock = threading.Lock()
GAS_PRICE_CACHE_TTL = 5
_nonce_cache: dict[str, tuple[float, int]] = {}
_nonce_cache_lock = threading.Lock()
NONCE_CACHE_TTL = 8.0
PREFETCH_NONCE = True

def get_cached_pending_nonce(w3: Web3, wallet_address: str, force_refresh: bool = False) -> int:
    chain_id = int(w3.eth.chain_id)
    key = f"{chain_id}:{wallet_address.lower()}"
    now = time.monotonic()
    if not force_refresh:
        with _nonce_cache_lock:
            item = _nonce_cache.get(key)
            if item and now - item[0] < NONCE_CACHE_TTL:
                return item[1]
    _rpc_limiter.acquire()
    nonce = int(w3.eth.get_transaction_count(wallet_address, "pending"))
    with _nonce_cache_lock:
        _nonce_cache[key] = (time.monotonic(), nonce)
    return nonce

def consume_cached_nonce(chain_id: int, wallet_address: str, nonce: int) -> None:
    key = f"{chain_id}:{wallet_address.lower()}"
    with _nonce_cache_lock:
        item = _nonce_cache.get(key)
        if item and item[1] == nonce:
            _nonce_cache[key] = (time.monotonic(), nonce + 1)


def get_web3_from_config(chain_config: dict) -> Web3:
    rpc_url = chain_config.get("rpc_url", "")
    if not rpc_url:
        raise ValueError(f"لا يوجد RPC URL: {chain_config.get('chain_name_display', 'unknown')}")
    return Web3(Web3.HTTPProvider(rpc_url, request_kwargs={"timeout": 3}))


def get_wallet_balance_usd(w3: Web3, wallet_address: str, eth_price_usd: float,
                            native_price_usd: Optional[float] = None) -> float:
    now = time.time()
    try:
        cache_chain_id = int(w3.eth.chain_id)
    except Exception:
        cache_chain_id = 0
    cache_key = (cache_chain_id, wallet_address.lower())
    with _balance_cache_lock:
        if cache_key in _balance_cache:
            last_check, balance = _balance_cache[cache_key]
            if now - last_check < BALANCE_CACHE_TTL:
                return balance
    try:
        _rpc_limiter.acquire()
        checksum_wallet = Web3.to_checksum_address(wallet_address)
        balance_wei = int(w3.eth.get_balance(checksum_wallet, "latest"))
        balance_native = balance_wei / 1e18
        balance_usd = balance_native * (native_price_usd if native_price_usd is not None else eth_price_usd)
        with _balance_cache_lock:
            _balance_cache[cache_key] = (now, balance_usd)
        return balance_usd
    except Exception:
        return -1.0


def get_cached_gas_price(w3: Web3) -> int:
    now = time.time()
    try:
        cid = int(w3.eth.chain_id)
    except Exception:
        cid = 0
    with _gas_price_cache_lock:
        if cid in _gas_price_cache:
            ts, gp = _gas_price_cache[cid]
            if now - ts < GAS_PRICE_CACHE_TTL:
                return gp
    try:
        _rpc_limiter.acquire()
        gp = int(w3.eth.gas_price)
        with _gas_price_cache_lock:
            _gas_price_cache[cid] = (now, gp)
        return gp
    except Exception:
        return 0


def estimate_gas_fee_usd(w3: Web3, eth_price_usd: float, gas_units: int = 150_000,
                          native_price_usd: Optional[float] = None) -> float:
    try:
        base_gas_price = get_cached_gas_price(w3) or w3.eth.gas_price
        priority_gas_price = int(base_gas_price * RACE_GAS_BOOST if RACE_MODE else base_gas_price * 1.05)
        fee_native = (priority_gas_price * gas_units) / 1e18
        fee_usd = fee_native * (native_price_usd if native_price_usd is not None else eth_price_usd)
        return fee_usd
    except Exception:
        return float("inf")


def get_fee_recipient(w3: Web3, seadrop_address: str, nft_contract: str) -> Optional[str]:
    try:
        chain_id = getattr(w3.eth, "chain_id", 0)
    except Exception:
        chain_id = 0
    contract_key = (chain_id, nft_contract.lower())
    with _fee_recipients_lock:
        if contract_key in _fee_recipients_cache:
            return _fee_recipients_cache[contract_key]
    try:
        seadrop = w3.eth.contract(address=Web3.to_checksum_address(seadrop_address), abi=SEADROP_ABI)
        _rpc_limiter.acquire()
        recipients = seadrop.functions.getAllowedFeeRecipients(
            Web3.to_checksum_address(nft_contract)
        ).call()
        recipient = recipients[0] if recipients else seadrop_address
        with _fee_recipients_lock:
            _fee_recipients_cache[contract_key] = recipient
        return recipient
    except Exception:
        return seadrop_address


def get_onchain_public_price_wei(w3: Web3, nft_contract: str, seadrop_address: str = None) -> Optional[int]:
    addr = seadrop_address or "0x00005EA00Ac477B1030CE78506496e8C2dE24bf5"
    try:
        seadrop = w3.eth.contract(address=Web3.to_checksum_address(addr), abi=SEADROP_ABI)
        _rpc_limiter.acquire()
        public_drop = seadrop.functions.getPublicDrop(Web3.to_checksum_address(nft_contract)).call()
        return int(public_drop[0])
    except Exception:
        return None


def get_drop_info(w3: Web3, nft_contract: str, seadrop_address: str = None) -> Optional[dict]:
    addr = seadrop_address or "0x00005EA00Ac477B1030CE78506496e8C2dE24bf5"
    try:
        seadrop = w3.eth.contract(address=Web3.to_checksum_address(addr), abi=SEADROP_ABI)
        _rpc_limiter.acquire()
        public_drop = seadrop.functions.getPublicDrop(Web3.to_checksum_address(nft_contract)).call()
        return {
            "price_wei": int(public_drop[0]),
            "start_time": int(public_drop[1]),
            "end_time": int(public_drop[2]),
            "max_per_wallet": int(public_drop[3]),
            "fee_bps": int(public_drop[4]),
            "restrict_fee": public_drop[5],
        }
    except Exception:
        return None


def is_mint_open(drop_info: dict, early_submit_seconds: float = 0.5) -> tuple[bool, str]:
    current_time = time.time()
    start_time = drop_info.get("start_time", 0)
    end_time = drop_info.get("end_time", 0)
    if start_time > 0 and current_time < start_time - early_submit_seconds:
        return False, f"not_started (يبدأ خلال {start_time - current_time} ثانية)"
    if end_time > 0 and current_time > end_time:
        return False, "ended"
    return True, "open"


def decide_quantity(max_per_wallet: Optional[int], remaining_supply: int) -> int:
    if max_per_wallet is None:
        qty = 5
    elif max_per_wallet <= FEW_THRESHOLD:
        qty = max_per_wallet
    else:
        qty = LIMITED_BUY_QTY
    return max(1, min(qty, remaining_supply))


def get_wallet_ownership_balance(
    w3: Web3, nft_contract: str, wallet_address: str, chain_name: str = ""
) -> Optional[int]:
    """قراءة الملكية الحالية on-chain مع تجربة RPC بديل."""
    try:
        chain_name = chain_name or CHAIN_NAME_BY_ID.get(int(w3.eth.chain_id), "")
    except Exception:
        chain_name = chain_name or ""
    # لا ننشئ اتصالًا بديلًا قبل تجربة RPC الأساسي؛ هذا يقلل زمن الفحص الطبيعي.
    candidates = [w3]
    for candidate in candidates:
        address = Web3.to_checksum_address(nft_contract)
        wallet = Web3.to_checksum_address(wallet_address)
        try:
            contract = candidate.eth.contract(address=address, abi=ERC721_BALANCE_ABI)
            _rpc_limiter.acquire()
            return int(contract.functions.balanceOf(wallet).call())
        except Exception:
            try:
                contract = candidate.eth.contract(address=address, abi=ERC1155_BALANCE_ABI)
                _rpc_limiter.acquire()
                return int(contract.functions.balanceOf(wallet, 0).call())
            except Exception:
                if len(candidates) == 1 and chain_name:
                    fallback = getattr(getattr(w3, "provider", None), "endpoint_uri", "") or ""
                    alternate = get_web3_rotating(chain_name, fallback)
                    if alternate is not None and alternate is not w3:
                        candidates.append(alternate)
                continue
    log.warning("[Ownership] فشل قراءة balanceOf للمحفظة %s عبر RPCs", wallet_address[:10])
    return None


def get_wallet_mint_stats(
    w3: Web3, nft_contract: str, wallet_address: str, chain_name: str = ""
) -> Optional[tuple[int, int, int]]:
    """قراءة إحصاءات المحفظة مع تجربة RPC بديل؛ None يعني عدم السماح بالشراء."""
    try:
        chain_name = chain_name or CHAIN_NAME_BY_ID.get(int(w3.eth.chain_id), "")
    except Exception:
        chain_name = chain_name or ""
    # لا ننشئ اتصالًا بديلًا قبل تجربة RPC الأساسي؛ هذا يقلل زمن الفحص الطبيعي.
    candidates = [w3]
    last_error = None
    for candidate in candidates:
        try:
            contract = candidate.eth.contract(
                address=Web3.to_checksum_address(nft_contract),
                abi=NFT_MINT_STATS_ABI,
            )
            _rpc_limiter.acquire()
            minted, total_supply, max_supply = contract.functions.getMintStats(
                Web3.to_checksum_address(wallet_address)
            ).call()
            return int(minted), int(total_supply), int(max_supply)
        except Exception as exc:
            last_error = exc
            if len(candidates) == 1 and chain_name:
                fallback = getattr(getattr(w3, "provider", None), "endpoint_uri", "") or ""
                alternate = get_web3_rotating(chain_name, fallback)
                if alternate is not None and alternate is not w3:
                    candidates.append(alternate)
    log.warning("[MintStats] فشل التحقق on-chain للمحفظة %s عبر RPCs: %s",
                wallet_address[:10], last_error)
    return None


def is_free_or_negligible(
    price_wei: int,
    eth_price_usd: float = 0.0,
    threshold_usd: float = 0.0,
) -> bool:
    """السماح بالمينت المجاني فقط؛ أي قيمة موجبة مرفوضة."""
    try:
        return int(price_wei or 0) == 0
    except (TypeError, ValueError):
        return False


def is_paid_mint(price_wei: int, eth_price_usd: float, threshold_usd: float = 0.01) -> tuple:
    if int(price_wei or 0) <= 0:
        return False, 0.0, "مجاني"
    price_usd = (price_wei / 1e18) * eth_price_usd
    if price_usd < threshold_usd:
        return False, price_usd, "مجاني (أقل من 1 سنت)"
    return True, price_usd, "مدفوع"


def calc_tx_gas_fee_usd(gas_price: int, gas_limit: int, native_price_usd: float) -> float:
    if native_price_usd <= 0:
        return 0.0
    return (gas_price * gas_limit / 1e18) * native_price_usd


# ===========================================================================
# 🏁 Race Plan
# ===========================================================================

@dataclass
class RacePlan:
    wallet_address: str
    private_key: str
    nft_contract: str
    seadrop_address: str
    fee_recipient: str
    price_wei: int
    quantity: int
    chain_id: int
    gas_limit: int
    base_gas_price: int
    total_value_wei: int
    native_price_usd: float = 0.0
    chain_name: str = ""
    gas_cap_usd: float = 0.20
    prepared_nonce: Optional[int] = None
    prepared_at: float = field(default_factory=time.time)
    slug: str = ""
    opensea_api_key: str = ""


def prepare_race_plan(
    w3: Web3, private_key: str, wallet_address: str,
    nft_contract: str, seadrop_address: str, price_wei: int,
    max_per_wallet: Optional[int], remaining_supply: int,
    native_price_usd: float = 0.0, chain_name: str = "",
    slug: str = "", opensea_api_key: str = "",
) -> Optional[RacePlan]:
    try:
        checksum_wallet = Web3.to_checksum_address(wallet_address)
        checksum_contract = Web3.to_checksum_address(nft_contract)
        checksum_seadrop = Web3.to_checksum_address(seadrop_address)
        chain_id = int(w3.eth.chain_id)
    except Exception as e:
        log.error("[RacePrep] فشل العناوين: %s", e)
        return None

    fee_recipient = get_fee_recipient(w3, checksum_seadrop, checksum_contract) or checksum_seadrop
    base_gas_price = get_cached_gas_price(w3) or int(w3.eth.gas_price)

    # احسب المتبقي لهذه المحفظة قبل توقيع Race؛ نفذ فحصي الملكية والإحصاءات
    # بالتوازي لتقليل زمن التجهيز، مع الإبقاء على الفحصين كاملين.
    effective_max_per_wallet = max_per_wallet
    with ThreadPoolExecutor(max_workers=2) as pool:
        ownership_future = pool.submit(
            get_wallet_ownership_balance, w3, checksum_contract, checksum_wallet, chain_name
        )
        stats_future = pool.submit(
            get_wallet_mint_stats, w3, checksum_contract, checksum_wallet, chain_name
        )
        owned_balance = ownership_future.result()
        mint_stats = stats_future.result()
    if REQUIRE_OWNERSHIP_CHECK and owned_balance is None:
        log.warning("[RacePrep] إلغاء الخطة: تعذر التحقق من balanceOf عبر RPCs")
        return None
    if owned_balance and owned_balance > 0:
        log.info("[RacePrep] تخطي المحفظة: تملك حاليًا %s NFT من المجموعة", owned_balance)
        return None
    if REQUIRE_MINT_STATS and mint_stats is None:
        log.warning("[RacePrep] إلغاء الخطة: تعذر التحقق من getMintStats عبر RPCs")
        return None
    if mint_stats is not None and max_per_wallet is not None:
        minted_before, _current_supply, _max_supply = mint_stats
        remaining_for_wallet = max(0, int(max_per_wallet) - int(minted_before))
        if remaining_for_wallet <= 0:
            log.info("[RacePrep] تخطي المحفظة؛ وصلت للحد الأقصى (%s/%s)",
                     minted_before, max_per_wallet)
            return None
        effective_max_per_wallet = min(int(max_per_wallet), remaining_for_wallet)

    quantity = decide_quantity(effective_max_per_wallet, remaining_supply)
    if quantity <= 0:
        return None
    total_value_wei = price_wei * quantity
    gas_limit = FIXED_GAS_LIMIT if FIXED_GAS_LIMIT > 0 else 200_000
    if FIXED_GAS_LIMIT <= 0:
        try:
            mint_fn = w3.eth.contract(address=checksum_seadrop, abi=SEADROP_ABI).functions.mintPublic(
                checksum_contract, Web3.to_checksum_address(fee_recipient),
                Web3.to_checksum_address(ZERO_ADDRESS), quantity,
            )
            _rpc_limiter.acquire()
            estimated = int(mint_fn.estimate_gas({
                "from": checksum_wallet, "value": total_value_wei,
            }))
            gas_limit = max(21_000, int(estimated * GAS_LIMIT_SAFETY_MARGIN))
        except Exception as exc:
            log.debug("[RacePrep] gas estimate failed; fallback=200000: %s", exc)
    gas_cap_usd = get_chain_gas_cap(chain_name) if chain_name else RACE_MAX_GAS_FEE_USD_FALLBACK
    prepared_nonce = None
    if PREFETCH_NONCE:
        try:
            prepared_nonce = get_cached_pending_nonce(w3, checksum_wallet)
        except Exception as exc:
            log.debug("[RacePrep] nonce prefetch failed for %s: %s", checksum_wallet[:10], exc)

    return RacePlan(
        wallet_address=checksum_wallet, private_key=private_key,
        nft_contract=checksum_contract, seadrop_address=checksum_seadrop,
        fee_recipient=fee_recipient, price_wei=price_wei, quantity=quantity,
        chain_id=chain_id, gas_limit=gas_limit,
        base_gas_price=base_gas_price, total_value_wei=total_value_wei,
        native_price_usd=native_price_usd, chain_name=chain_name,
        gas_cap_usd=gas_cap_usd, prepared_nonce=prepared_nonce,
        slug=slug, opensea_api_key=opensea_api_key,
    )


def _prepare_signed_cascade_tx(w3: Web3, plan: RacePlan, nonce: int, gas_price: int) -> Optional[bytes]:
    try:
        seadrop = w3.eth.contract(address=plan.seadrop_address, abi=SEADROP_ABI)
        tx = seadrop.functions.mintPublic(
            plan.nft_contract,
            Web3.to_checksum_address(plan.fee_recipient),
            Web3.to_checksum_address(ZERO_ADDRESS),
            plan.quantity,
        ).build_transaction({
            "from": plan.wallet_address, "value": plan.total_value_wei,
            "nonce": nonce, "gasPrice": gas_price,
            "gas": plan.gas_limit, "chainId": plan.chain_id,
        })
        signed = w3.eth.account.sign_transaction(tx, private_key=plan.private_key)
        return getattr(signed, "raw_transaction", None) or getattr(signed, "rawTransaction", None)
    except Exception as e:
        log.debug("[Cascade] فشل تجهيز معاملة: %s", e)
        return None


def _receipt_result(w3: Web3, tx_hash, gas_price: int = 0, quantity: int = 1,
                    native_price_usd: float = 0.0) -> dict:
    tx_hex = tx_hash.hex() if hasattr(tx_hash, "hex") else str(tx_hash)
    if not WAIT_FOR_RECEIPT:
        return {"success": True, "tx_hash": tx_hex, "quantity": quantity,
                "gas_fee_usd": 0.0}
    try:
        receipt = w3.eth.wait_for_transaction_receipt(
            tx_hash, timeout=RECEIPT_TIMEOUT_SECONDS, poll_latency=0.5
        )
        status = int(receipt.get("status", 0))
        if status != 1:
            return {"success": False, "reason": "tx_reverted",
                    "error": f"transaction reverted on-chain: {tx_hex}",
                    "tx_hash": tx_hex}
        gas_used = int(receipt.get("gasUsed", 0) or 0)
        effective = int(receipt.get("effectiveGasPrice", gas_price) or gas_price)
        return {"success": True, "tx_hash": tx_hex, "quantity": quantity,
                "gas_fee_usd": gas_used * effective / 1e18 * native_price_usd,
                "block_number": int(receipt.get("blockNumber", 0) or 0)}
    except Exception as exc:
        return {"success": False, "reason": "tx_pending",
                "error": f"receipt timeout for {tx_hex}: {exc}", "tx_hash": tx_hex,
                "quantity": quantity}


def _receipt_any_result(w3: Web3, tx_hashes: list[str], gas_price: int,
                        quantity: int, native_price_usd: float = 0.0) -> dict:
    deadline = time.monotonic() + RECEIPT_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        for tx_hash in tx_hashes:
            try:
                receipt = w3.eth.get_transaction_receipt(tx_hash)
                if int(receipt.get("status", 0)) == 1:
                    gas_used = int(receipt.get("gasUsed", 0) or 0)
                    effective = int(receipt.get("effectiveGasPrice", gas_price) or gas_price)
                    return {"success": True, "tx_hash": tx_hash, "quantity": quantity,
                            "gas_fee_usd": gas_used * effective / 1e18 * native_price_usd,
                            "block_number": int(receipt.get("blockNumber", 0) or 0)}
                return {"success": False, "reason": "tx_reverted", "tx_hash": tx_hash,
                        "error": f"transaction reverted on-chain: {tx_hash}"}
            except Exception:
                continue
        time.sleep(0.5)
    return {"success": False, "reason": "tx_pending", "tx_hash": tx_hashes[-1],
            "all_hashes": tx_hashes, "quantity": quantity}


def _decode_seadrop_quantity_error(err: str) -> dict:
    """يفك MintQuantityExceedsMaxMintedPerWallet من بيانات revert الخام."""
    text = str(err or "")
    match = re.search(r"0x(?:e12d2314|edc01273)([0-9a-fA-F]{128})", text)
    if not match:
        return {}
    raw = match.group(1)
    return {"total": int(raw[:64], 16), "allowed": int(raw[64:], 16)}


def _classify_exception(err: str) -> str:
    e = err.lower()
    if _decode_seadrop_quantity_error(e):
        return "already_minted"
    if "already minted" in e or "max mint" in e or "exceeds max" in e or "mint limit" in e:
        return "already_minted"
    if "insufficient funds" in e:
        return "insufficient_funds"
    if "nonce too low" in e or "already known" in e or "replacement" in e:
        return "nonce_conflict"
    if "reverted" in e or "execution reverted" in e:
        return "reverted"
    if "not started" in e or "not live" in e or "not open" in e:
        return "not_started"
    if "underpriced" in e:
        return "underpriced"
    return "unknown"


# ===========================================================================
# 💥 Cascade Sniping
# ===========================================================================
def _fire_cascade(w3: Web3, plan: RacePlan, base_gas_price: int) -> tuple[bool, dict]:
    # prepared_nonce is only a preparation hint; refresh under the wallet lock.
    nonce = get_cached_pending_nonce(w3, plan.wallet_address, force_refresh=True)

    gas_prices: list[int] = []
    base = max(base_gas_price, plan.base_gas_price)
    current = int(base * RACE_GAS_BOOST)
    for _ in range(RACE_CASCADE_SIZE):
        gas_prices.append(current)
        current = int(current * RACE_CASCADE_GAS_STEP)

    affordable_prices: list[int] = []
    for gp in gas_prices:
        fee_usd = calc_tx_gas_fee_usd(gp, plan.gas_limit, plan.native_price_usd)
        if plan.native_price_usd > 0 and fee_usd > plan.gas_cap_usd:
            log.warning("💰 [GasCap/%s] تجاهل gas=%.2fgwei (تكلفة $%.4f > حد $%.4f)",
                        plan.chain_name or "?", gp / 1e9, fee_usd, plan.gas_cap_usd)
            continue
        affordable_prices.append(gp)

    if not affordable_prices:
        first_fee = calc_tx_gas_fee_usd(gas_prices[0], plan.gas_limit, plan.native_price_usd)
        log.error("🛑 [GasCap/%s] %s: كل الأسعار تتجاوز حد $%.4f (أدنى $%.4f)",
                  plan.chain_name or "?", plan.wallet_address[:8], plan.gas_cap_usd, first_fee)
        return False, {"reason": "gas_exceeds_limit",
                       "error": f"chain={plan.chain_name} min_fee_usd={first_fee:.4f} > cap_usd={plan.gas_cap_usd:.4f}"}

    log.info("💥 [Cascade/%s] %s → %d/%d (nonce=%d, gas=%s gwei, cap=$%.4f)",
             plan.chain_name or "?", plan.wallet_address[:8],
             len(affordable_prices), RACE_CASCADE_SIZE, nonce,
             ", ".join(f"{gp/1e9:.1f}" for gp in affordable_prices), plan.gas_cap_usd)

    last_error = ""
    sent_hashes: list[str] = []

    for idx, gas_price in enumerate(affordable_prices):
        raw = _prepare_signed_cascade_tx(w3, plan, nonce, gas_price)
        if raw is None:
            continue
        try:
            _rpc_limiter_fast.acquire()
            tx_hash = w3.eth.send_raw_transaction(raw)
            consume_cached_nonce(plan.chain_id, plan.wallet_address, nonce)
            sent_hashes.append(tx_hash.hex())
        except Exception as e:
            err = str(e).lower()
            last_error = err
            category = _classify_exception(err)
            if category == "already_minted":
                return False, {"reason": "already_minted", "error": err}
            if category == "insufficient_funds":
                return False, {"reason": "insufficient_funds", "error": err}
            if category in ("nonce_conflict", "underpriced"):
                try:
                    get_cached_pending_nonce(w3, plan.wallet_address, force_refresh=True)
                    plan.prepared_nonce = None
                except Exception:
                    pass
                continue
        if idx < len(affordable_prices) - 1 and RACE_CASCADE_DELAY_MS > 0:
            time.sleep(RACE_CASCADE_DELAY_MS / 1000.0)

    if sent_hashes:
        if WAIT_FOR_RECEIPT:
            result = _receipt_any_result(
                w3, sent_hashes, affordable_prices[-1], plan.quantity, plan.native_price_usd
            )
            result.update({"all_hashes": sent_hashes, "gas_price": affordable_prices[-1], "nonce": nonce})
            return bool(result.get("success")), result
        return True, {
            "success": True, "tx_hash": sent_hashes[-1], "all_hashes": sent_hashes,
            "quantity": plan.quantity, "gas_price": affordable_prices[-1],
            "gas_fee_usd": calc_tx_gas_fee_usd(affordable_prices[-1], plan.gas_limit, plan.native_price_usd),
            "nonce": nonce,
        }
    return False, {"reason": _classify_exception(last_error) or "cascade_failed", "error": last_error}


def execute_race_plan(w3: Web3, plan: RacePlan) -> dict:
    wallet_lock = get_wallet_lock(plan.wallet_address, plan.chain_id)
    wallet_lock.acquire()
    race_t0 = time.perf_counter()
    try:
        if plan.opensea_api_key and plan.slug and not RACE_CASCADE_ENABLED:
            fast_result = attempt_purchase_drop_api(
                w3, plan.private_key, plan.wallet_address, plan.slug,
                plan.opensea_api_key, plan.quantity, plan.native_price_usd,
                lock_already_held=True,
            )
            if fast_result.get("success") or fast_result.get("reason") == "tx_pending":
                fast_result["latency_ms"] = round((time.perf_counter() - race_t0) * 1000, 1)
                return fast_result
        current_gas = get_cached_gas_price(w3) or plan.base_gas_price
        first_fee_usd = calc_tx_gas_fee_usd(int(current_gas * RACE_GAS_BOOST), plan.gas_limit, plan.native_price_usd)
        if plan.native_price_usd > 0 and first_fee_usd > plan.gas_cap_usd:
            log.error("🛑 [GasCap/%s] %s: $%.4f > حد $%.4f — توقف فوري",
                      plan.chain_name or "?", plan.wallet_address[:8], first_fee_usd, plan.gas_cap_usd)
            return {"success": False, "reason": "gas_exceeds_limit",
                    "error": f"chain={plan.chain_name} current_fee_usd={first_fee_usd:.4f} > cap_usd={plan.gas_cap_usd:.4f}",
                    "wallet": plan.wallet_address}

        last_error = ""
        for attempt in range(1, RACE_MAX_ATTEMPTS + 1):
            try:
                if RACE_CASCADE_ENABLED:
                    ok, result = _fire_cascade(w3, plan, plan.base_gas_price)
                else:
                    boost = min(RACE_GAS_BOOST + 0.1 * (attempt - 1), 2.5)
                    gas_price = max(int(plan.base_gas_price * boost), plan.base_gas_price + 1)
                    fee_usd = calc_tx_gas_fee_usd(gas_price, plan.gas_limit, plan.native_price_usd)
                    if plan.native_price_usd > 0 and fee_usd > plan.gas_cap_usd:
                        return {"success": False, "reason": "gas_exceeds_limit",
                                "error": f"chain={plan.chain_name} fee_usd={fee_usd:.4f} > cap_usd={plan.gas_cap_usd:.4f}",
                                "wallet": plan.wallet_address}
                    # لا نعتمد على nonce قديم تم تجهيزه قبل فتح المرحلة.
                    nonce = get_cached_pending_nonce(w3, plan.wallet_address, force_refresh=True)
                    raw = _prepare_signed_cascade_tx(w3, plan, nonce, gas_price)
                    if raw is None:
                        ok, result = False, {"reason": "sign_failed"}
                    else:
                        try:
                            _rpc_limiter_fast.acquire()
                            tx_hash = w3.eth.send_raw_transaction(raw)
                            consume_cached_nonce(plan.chain_id, plan.wallet_address, nonce)
                            result = _receipt_result(w3, tx_hash, gas_price, plan.quantity, plan.native_price_usd)
                            result.update({"gas_price": gas_price, "nonce": nonce})
                            ok = bool(result.get("success"))
                        except Exception as e:
                            ok, result = False, {"reason": _classify_exception(str(e)), "error": str(e)}

                if ok and result.get("success"):
                    log.info("🏁 [Race/%s] محاولة %d/%d نجحت — gas=%.2fgwei fee=$%.4f",
                             plan.chain_name or "?", attempt, RACE_MAX_ATTEMPTS,
                             result.get("gas_price", 0) / 1e9, result.get("gas_fee_usd", 0))
                    result["latency_ms"] = round((time.perf_counter() - race_t0) * 1000.0, 2)
                    return result

                reason = result.get("reason", "")
                last_error = result.get("error", reason)
                if reason in ("already_minted", "insufficient_funds", "gas_exceeds_limit", "tx_pending"):
                    return {"success": False, "reason": reason, "error": last_error}
                if attempt == RACE_MAX_ATTEMPTS:
                    break
                time.sleep(RACE_RETRY_DELAY_MS / 1000.0)
            except Exception as e:
                last_error = str(e)
                if attempt == RACE_MAX_ATTEMPTS:
                    break
                time.sleep(RACE_RETRY_DELAY_MS / 1000.0)
        return {"success": False, "reason": "race_exhausted", "error": last_error, "latency_ms": round((time.perf_counter() - race_t0) * 1000.0, 2)}
    finally:
        wallet_lock.release()


def race_all_wallets(w3: Web3, plans: list[RacePlan]) -> list[dict]:
    """إرسال Race بتوازٍ محدود؛ لا ننشئ خيطًا غير محدود لكل محفظة."""
    results: list[Optional[dict]] = [None] * len(plans)

    def _worker(idx: int, plan: RacePlan):
        try:
            results[idx] = execute_race_plan(w3, plan)
            results[idx]["wallet"] = plan.wallet_address
        except Exception as e:
            results[idx] = {"success": False, "reason": "race_exception",
                            "error": str(e), "wallet": plan.wallet_address}

    with ThreadPoolExecutor(max_workers=min(RACE_MAX_WORKERS, max(1, len(plans)))) as pool:
        futures = [pool.submit(_worker, i, plan) for i, plan in enumerate(plans)]
        for future in futures:
            try:
                future.result(timeout=RECEIPT_TIMEOUT_SECONDS + 10)
            except Exception:
                pass
    return [r or {"success": False, "reason": "race_timeout", "attempt_mode": "race"}
            for r in results]


# ===========================================================================
# الشراء الكلاسيكي
# ===========================================================================
def attempt_purchase_seadrop(
    w3: Web3, private_key: str, wallet_address: str,
    nft_contract: str, seadrop_address: str,
    price_wei_per_token: int, max_per_wallet: Optional[int],
    remaining_supply: int, eth_price_usd: float, chain_name: str = "",
    preflight: Optional[dict] = None,
) -> dict:
    if not is_free_or_negligible(price_wei_per_token, eth_price_usd):
        return {
            "success": False,
            "reason": "paid_mint_rejected",
            "price_wei": int(price_wei_per_token or 0),
        }

    try:
        checksum_wallet = Web3.to_checksum_address(wallet_address)
        checksum_contract = Web3.to_checksum_address(nft_contract)
        checksum_seadrop = Web3.to_checksum_address(seadrop_address)
    except Exception as e:
        return {"success": False, "reason": "invalid_address", "error": str(e)}

    preflight = preflight or {}
    balance_usd = float(preflight.get("balance_usd", -1.0))
    if not preflight and not SKIP_BALANCE_CHECK_RACE:
        balance_usd = get_wallet_balance_usd(w3, checksum_wallet, eth_price_usd)
        if balance_usd < 0:
            return {"success": False, "reason": "balance_read_failed"}
        if balance_usd < MIN_BALANCE_RESERVE_USD:
            return {"success": False, "reason": "balance_too_low", "balance_usd": balance_usd, "attempt_mode": "normal_checked"}

    gas_fee_usd = float(preflight.get("gas_fee_usd", 0.0))
    if not preflight:
        gas_fee_usd = estimate_gas_fee_usd(w3, eth_price_usd)
        if gas_fee_usd > MAX_GAS_FEE_USD:
            return {"success": False, "reason": "gas_too_high", "gas_fee_usd": gas_fee_usd}

    fee_recipient = get_fee_recipient(w3, checksum_seadrop, checksum_contract) or checksum_seadrop

    # SeaDrop يفرض الحد على الإجمالي التراكمي للمحفظة، وليس على كمية هذه المعاملة فقط.
    # بدون هذا الفحص قد يحاول البوت سك 15 إضافية لمحفظة سكّت 15 سابقًا،
    # فتعود المعاملة بخطأ MintQuantityExceedsMaxMintedPerWallet.
    effective_max_per_wallet = max_per_wallet
    chain_name = chain_name or ""
    try:
        chain_name = chain_name or CHAIN_NAME_BY_ID.get(int(w3.eth.chain_id), "")
    except Exception:
        pass
    owned_balance = preflight.get("owned_balance") if preflight else get_wallet_ownership_balance(w3, checksum_contract, checksum_wallet, chain_name)
    if REQUIRE_OWNERSHIP_CHECK and owned_balance is None:
        return {"success": False, "reason": "ownership_check_unavailable",
                "error": "تعذر التحقق من ملكية المحفظة عبر جميع RPCs"}
    if owned_balance and owned_balance > 0:
        return {"success": False, "reason": "already_bought", "owned_balance": owned_balance}
    mint_stats = preflight.get("mint_stats") if preflight else get_wallet_mint_stats(w3, checksum_contract, checksum_wallet, chain_name)
    if REQUIRE_MINT_STATS and mint_stats is None:
        return {"success": False, "reason": "mint_stats_unavailable",
                "error": "تعذر التحقق من عدد NFTs المملوكة عبر جميع RPCs"}
    if mint_stats is not None:
        minted_before, _current_supply, _max_supply = mint_stats
        if minted_before > 0:
            return {
                "success": False,
                "reason": "already_bought",
                "minted": minted_before,
            }
    if mint_stats is not None and max_per_wallet is not None:
        minted_before, _current_supply, _max_supply = mint_stats
        remaining_for_wallet = max(0, int(max_per_wallet) - minted_before)
        if remaining_for_wallet <= 0:
            return {
                "success": False,
                "reason": "already_minted",
                "minted": minted_before,
                "max_per_wallet": int(max_per_wallet),
            }
        effective_max_per_wallet = min(int(max_per_wallet), remaining_for_wallet)

    quantity = decide_quantity(effective_max_per_wallet, remaining_supply)
    total_value = price_wei_per_token * quantity

    total_eth = total_value / 1e18
    if total_eth > MAX_ETH_PER_TX:
        return {"success": False, "reason": "tx_value_too_high", "tx_value_eth": total_eth}

    try:
        current_chain_id = int(w3.eth.chain_id)
    except Exception:
        current_chain_id = 0

    wallet_lock = get_wallet_lock(wallet_address, current_chain_id)
    wallet_lock.acquire()
    raw_tx = None
    tx_send_attempted = False
    tx_hash_hex = ""
    try:
        contract = w3.eth.contract(address=checksum_seadrop, abi=SEADROP_ABI)
        _rpc_limiter.acquire()
        current_nonce = w3.eth.get_transaction_count(checksum_wallet, "pending")
        base_gas_price = get_cached_gas_price(w3) or w3.eth.gas_price
        priority_gas_price = int(base_gas_price * 1.10)

        tx = contract.functions.mintPublic(
            checksum_contract, Web3.to_checksum_address(fee_recipient),
            Web3.to_checksum_address(ZERO_ADDRESS), quantity,
        ).build_transaction({
            "from": checksum_wallet, "value": total_value,
            "nonce": current_nonce, "gasPrice": priority_gas_price,
            "chainId": current_chain_id,
        })

        # الحد الثابت سريع للكميات الصغيرة فقط؛ للكميات الكبيرة استخدم estimate_gas.
        if FIXED_GAS_LIMIT > 0 and quantity <= FEW_THRESHOLD:
            tx["gas"] = FIXED_GAS_LIMIT
        else:
            try:
                _rpc_limiter.acquire()
                estimated_gas = w3.eth.estimate_gas(tx)
                tx["gas"] = int(estimated_gas * GAS_LIMIT_SAFETY_MARGIN)
            except Exception as e:
                return {"success": False, "reason": "simulation_failed", "error": str(e)}

        gas_fee_eth = (tx["gas"] * priority_gas_price) / 1e18
        gas_fee_usd_actual = gas_fee_eth * eth_price_usd
        if gas_fee_usd_actual > MAX_GAS_FEE_USD:
            return {"success": False, "reason": "gas_too_high_precise", "gas_fee_usd": gas_fee_usd_actual}

        signed_tx = w3.eth.account.sign_transaction(tx, private_key=private_key)
        raw_tx = getattr(signed_tx, "raw_transaction", None) or getattr(signed_tx, "rawTransaction", None)
        _rpc_limiter.acquire()
        # من هذه النقطة لا نعيد استخدام nonce حتى لو أعاد RPC خطأً؛
        # قد يكون قبل المعاملة ثم قبلها فعليًا دون إعادة hash للعميل.
        tx_send_attempted = True
        tx_hash = w3.eth.send_raw_transaction(raw_tx)
        tx_hash_hex = tx_hash.hex()
        if WAIT_FOR_RECEIPT:
            try:
                receipt = w3.eth.wait_for_transaction_receipt(
                    tx_hash, timeout=RECEIPT_TIMEOUT_SECONDS, poll_latency=0.5
                )
                receipt_status = int(receipt.get("status", 0))
                if receipt_status != 1:
                    return {
                        "success": False,
                        "reason": "tx_reverted",
                        "error": f"transaction reverted on-chain: {tx_hash_hex}",
                        "tx_hash": tx_hash_hex,
                    }
                gas_used = int(receipt.get("gasUsed", tx.get("gas", 0)) or 0)
                effective_gas_price = int(
                    receipt.get("effectiveGasPrice", priority_gas_price)
                    or priority_gas_price
                )
                gas_fee_usd_actual = (
                    gas_used * effective_gas_price / 1e18 * eth_price_usd
                )
            except Exception as receipt_error:
                # المعاملة قد تكون ما زالت pending؛ لا نعلن نجاحًا ولا نعيد إرسالها.
                return {
                    "success": False,
                    "reason": "tx_pending",
                    "error": f"لم يصل تأكيد البلوك خلال المهلة: {tx_hash_hex} ({receipt_error})",
                    "tx_hash": tx_hash_hex,
                    "quantity": quantity,
                }

        return {"success": True, "tx_hash": tx_hash_hex, "quantity": quantity,
                "gas_fee_usd": gas_fee_usd_actual, "total_value_wei": total_value, "balance_usd": balance_usd}
    except Exception as e:
        error_msg = str(e).lower()
        # بعد قبول send_raw_transaction لا يجوز إنشاء nonce جديد عند أي
        # خطأ لاحق؛ المعاملة الأصلية قد تكون pending أو mined رغم فشل RPC.
        if tx_send_attempted:
            return {
                "success": False,
                "reason": "tx_pending",
                "tx_hash": tx_hash_hex or (Web3.keccak(raw_tx).hex() if raw_tx else ""),
                "quantity": quantity,
                "error": f"RPC error after broadcast; do not retry: {str(e)}",
                "balance_usd": balance_usd,
            }
        quantity_error = _decode_seadrop_quantity_error(error_msg)
        if quantity_error:
            reason = "already_minted"
            error_msg = (
                "SeaDrop MintQuantityExceedsMaxMintedPerWallet: "
                f"attempted_total={quantity_error['total']} "
                f"allowed={quantity_error['allowed']} | {str(e)}"
            )
        # selector القديم مدعوم عبر quantity_error أعلاه أيضًا؛
        # لا نتركه كـ tx_error حتى لا ينشئ Retry غير مفيد.
        elif "0xedc01273" in error_msg:
            reason = "already_minted"
            error_msg = (
                f"SeaDrop MintQuantityExceedsMaxMintedPerWallet: {str(e)}"
            )
        elif "already known" in error_msg or "known transaction" in error_msg:
            # العقدة استلمت نفس raw tx مسبقًا. لا نعيد الإرسال؛ نستخرج hash
            # من raw transaction ونسجلها Pending حتى تقوم المصالحة بمتابعتها.
            known_hash = Web3.keccak(raw_tx).hex() if raw_tx else ""
            return {
                "success": False,
                "reason": "tx_pending",
                "tx_hash": known_hash,
                "quantity": quantity,
                "error": f"RPC already knows this transaction: {known_hash or str(e)}",
                "balance_usd": balance_usd,
            }
        elif "insufficient funds" in error_msg:
            reason = "insufficient_funds_for_total_cost"
        elif "gas" in error_msg:
            reason = "gas_too_high"
        elif "reverted" in error_msg:
            reason = "reverted"
        else:
            reason = "tx_error"
        return {"success": False, "reason": reason, "error": error_msg, "balance_usd": balance_usd}
    finally:
        wallet_lock.release()


def attempt_purchase_universal(
    w3: Web3, private_key: str, wallet_address: str,
    nft_contract: str, price_wei: int, quantity: int, eth_price_usd: float,
) -> dict:
    if not is_free_or_negligible(price_wei, eth_price_usd):
        return {
            "success": False,
            "reason": "paid_mint_rejected",
            "price_wei": int(price_wei or 0),
        }
    try:
        checksum_contract = Web3.to_checksum_address(nft_contract)
        checksum_wallet = Web3.to_checksum_address(wallet_address)
    except Exception:
        return {"success": False, "reason": "invalid_address"}

    SIMPLE_MINT_ABI = [
        {"inputs": [{"name": "quantity", "type": "uint256"}], "name": "mint", "outputs": [], "stateMutability": "payable", "type": "function"},
        {"inputs": [], "name": "mint", "outputs": [], "stateMutability": "payable", "type": "function"},
    ]
    try:
        contract = w3.eth.contract(address=checksum_contract, abi=SIMPLE_MINT_ABI)
        for attempt in range(2):
            raw_tx = None
            broadcasted = False
            try:
                fn = contract.functions.mint(quantity) if attempt == 0 else contract.functions.mint()
                current_chain_id = int(w3.eth.chain_id)
                wallet_lock = get_wallet_lock(wallet_address, current_chain_id)
                wallet_lock.acquire()
                try:
                    nonce = get_cached_pending_nonce(w3, checksum_wallet, force_refresh=True)
                    base_gas_price = get_cached_gas_price(w3) or w3.eth.gas_price
                    priority_gas_price = int(base_gas_price * 1.05)
                    tx = fn.build_transaction({
                        "from": checksum_wallet, "value": 0, "nonce": nonce,
                        "gasPrice": priority_gas_price, "chainId": current_chain_id,
                    })
                    _rpc_limiter.acquire()
                    estimated_gas = w3.eth.estimate_gas(tx)
                    tx["gas"] = int(estimated_gas * GAS_LIMIT_SAFETY_MARGIN)
                    gas_fee_usd = tx["gas"] * priority_gas_price / 1e18 * eth_price_usd
                    if gas_fee_usd > MAX_GAS_FEE_USD:
                        return {"success": False, "reason": "gas_too_high_precise", "gas_fee_usd": gas_fee_usd}
                    signed_tx = w3.eth.account.sign_transaction(tx, private_key=private_key)
                    raw_tx = getattr(signed_tx, "raw_transaction", None) or getattr(signed_tx, "rawTransaction", None)
                    _rpc_limiter.acquire()
                    # بعد بدء الإرسال لا نجرّب ABI الآخر ولا nonce جديدًا.
                    broadcasted = True
                    tx_hash = w3.eth.send_raw_transaction(raw_tx)
                    consume_cached_nonce(current_chain_id, checksum_wallet, nonce)
                    result = _receipt_result(w3, tx_hash, priority_gas_price, quantity, eth_price_usd)
                    result.setdefault("gas_fee_usd", gas_fee_usd)
                    return result
                finally:
                    wallet_lock.release()
            except Exception as exc:
                msg = str(exc).lower()
                if broadcasted or "already known" in msg or "known transaction" in msg:
                    known_hash = Web3.keccak(raw_tx).hex() if raw_tx else ""
                    return {
                        "success": False, "reason": "tx_pending",
                        "tx_hash": known_hash, "quantity": quantity,
                        "error": f"المعاملة أُرسلت أو معروفة لدى RPC: {known_hash or str(exc)}",
                    }
                # لا نجرب ABI الثاني إلا إذا فشل البناء/المحاكاة قبل البث.
                continue
        return {"success": False, "reason": "universal_failed"}
    except Exception as exc:
        return {"success": False, "reason": "universal_failed", "error": str(exc)}


def build_drop_mint_transaction(opensea_api_key: str, slug: str, minter: str, quantity: int) -> Optional[dict]:
    """يطلب calldata الجاهزة من OpenSea مع احترام مرحلة الـDrop الحالية."""
    if not opensea_api_key or not slug:
        return None
    try:
        response = _mint_api_session().post(
            f"https://api.opensea.io/api/v2/drops/{slug}/mint",
            headers={"x-api-key": opensea_api_key, "content-type": "application/json"},
            json={"minter": Web3.to_checksum_address(minter), "quantity": int(quantity)},
            timeout=4,
        )
        if response.status_code != 200:
            log.debug("[OpenSea Mint API] HTTP %s لـ %s", response.status_code, slug)
            return None
        data = response.json()
        target = data.get("target") or data.get("to")
        calldata = data.get("calldata") or data.get("data")
        if not target or not calldata:
            return None
        return {"target": target, "calldata": calldata, "value": int(data.get("value", 0) or 0)}
    except Exception as exc:
        log.debug("[OpenSea Mint API] فشل %s: %s", slug, exc)
        return None


def attempt_purchase_drop_api(
    w3: Web3, private_key: str, wallet_address: str, slug: str,
    opensea_api_key: str, quantity: int, eth_price_usd: float,
    lock_already_held: bool = False, wait_for_receipt: bool = True,
) -> dict:
    """إرسال OpenSea؛ المسار الخاطف يسجل Pending فور البث، والعادي ينتظر Receipt."""
    tx_data = build_drop_mint_transaction(opensea_api_key, slug, wallet_address, quantity)
    if not tx_data:
        return {"success": False, "reason": "drop_mint_api_unavailable"}
    if int(tx_data.get("value", 0) or 0) != 0:
        return {"success": False, "reason": "paid_mint_rejected", "error": "OpenSea API returned non-zero value"}
    wallet_lock = get_wallet_lock(wallet_address, int(w3.eth.chain_id))
    acquired_here = not lock_already_held
    if acquired_here:
        wallet_lock.acquire()
    raw_tx = None
    tx_send_attempted = False
    try:
        target = Web3.to_checksum_address(tx_data["target"])
        wallet = Web3.to_checksum_address(wallet_address)
        nonce = get_cached_pending_nonce(w3, wallet, force_refresh=True)
        gas_price = int(get_cached_gas_price(w3) or w3.eth.gas_price)
        gas_price = int(gas_price * 1.10)
        tx = {"from": wallet, "to": target, "data": tx_data["calldata"],
              "value": 0, "nonce": nonce, "gasPrice": gas_price,
              "chainId": int(w3.eth.chain_id)}
        # OpenSea Mint API قد يحتاج غازًا أكبر عند طلب كمية كبيرة.
        if FIXED_GAS_LIMIT > 0 and quantity <= FEW_THRESHOLD:
            tx["gas"] = FIXED_GAS_LIMIT
        else:
            _rpc_limiter.acquire()
            tx["gas"] = int(w3.eth.estimate_gas(tx) * GAS_LIMIT_SAFETY_MARGIN)
        gas_fee_usd = tx["gas"] * gas_price / 1e18 * eth_price_usd
        if gas_fee_usd > MAX_GAS_FEE_USD:
            return {"success": False, "reason": "gas_too_high_precise", "gas_fee_usd": gas_fee_usd}
        signed = w3.eth.account.sign_transaction(tx, private_key=private_key)
        raw_tx = getattr(signed, "raw_transaction", None) or getattr(signed, "rawTransaction", None)
        _rpc_limiter.acquire()
        tx_send_attempted = True
        tx_hash = w3.eth.send_raw_transaction(raw_tx)
        consume_cached_nonce(int(w3.eth.chain_id), wallet, nonce)
        tx_hash_hex = tx_hash.hex()
        # في المسار الخاطف: البث الناجح يكفي، والمصالحة الخلفية تنتظر Receipt.
        if not wait_for_receipt:
            return {
                "success": False, "reason": "tx_pending",
                "tx_hash": tx_hash_hex, "quantity": quantity,
                "gas_fee_usd": gas_fee_usd, "gas_price": gas_price,
                "nonce": nonce, "broadcasted": True,
            }
        result = _receipt_result(w3, tx_hash, gas_price, quantity, eth_price_usd)
        result.setdefault("gas_fee_usd", gas_fee_usd)
        return result
    except Exception as exc:
        msg = str(exc).lower()
        if tx_send_attempted:
            tx_hash = Web3.keccak(raw_tx).hex() if raw_tx else ""
            return {"success": False, "reason": "tx_pending", "tx_hash": tx_hash,
                    "error": f"RPC error after fast broadcast attempt; do not fallback: {str(exc)}"}
        if "already known" in msg or "known transaction" in msg:
            tx_hash = Web3.keccak(raw_tx).hex() if raw_tx else ""
            return {"success": False, "reason": "tx_pending", "tx_hash": tx_hash,
                    "error": f"RPC already knows this transaction: {tx_hash or str(exc)}"}
        if "insufficient funds" in msg:
            return {"success": False, "reason": "insufficient_funds_for_total_cost", "error": str(exc)}
        return {"success": False, "reason": "tx_error", "error": str(exc)}
    finally:
        if acquired_here:
            wallet_lock.release()


def execute_purchase_pipeline(
    w3: Web3, private_key: str, wallet_address: str,
    nft_contract: str, seadrop_address: str, price_wei: int,
    max_per_wallet: Optional[int], remaining_supply: int,
    eth_price_usd: float, allow_universal: bool = True,
    slug: str = "", opensea_api_key: str = "",
) -> dict:
    """المسار الآمن: Fast broadcast أولاً، ثم الفحوص فقط عند فشل ما قبل البث."""
    balance_usd = -1.0
    gas_fee_usd = 0.0
    try:
        chain_name = CHAIN_NAME_BY_ID.get(int(w3.eth.chain_id), "")
    except Exception:
        chain_name = ""

    fast_attempted = False
    fast_failure_reason = ""

    # Fast-first افتراضيًا: لا get_drop_info ولا balanceOf ولا getMintStats
    # قبل محاولة OpenSea. عند فشل البث نكمل تلقائيًا بالمسار العادي.
    # OpenSea يعيد calldata/target/value للمرحلة المؤهلة؛ نتحقق من value داخل المرسل.
    if FAST_FIRST_ENABLED and slug and opensea_api_key:
        fast_attempted = True
        log.info("⚡ Fast-first: محاولة الشراء الخاطفة أولاً للمجموعة %s", slug)
        fast_result = attempt_purchase_drop_api(
            w3, private_key, wallet_address, slug, opensea_api_key,
            decide_quantity(max_per_wallet, remaining_supply), eth_price_usd,
            wait_for_receipt=False,
        )
        if fast_result.get("reason") == "tx_pending" or fast_result.get("tx_hash"):
            fast_result["balance_usd"] = balance_usd
            fast_result["attempt_mode"] = "fast_broadcast"
            return fast_result
        fast_failure_reason = fast_result.get("reason", "unknown")
        log.info(
            "↩️ Fast-first لم يبث معاملة للمجموعة %s (%s) — الانتقال للمسار العادي",
            slug, fast_failure_reason,
        )

    # لم يتم بث معاملة: الآن فقط نتحقق من المرحلة والسعر on-chain.
    drop_info = get_drop_info(w3, nft_contract, seadrop_address)
    if not drop_info:
        return {"success": False, "reason": "price_verification_failed", "eth_price_usd": eth_price_usd, "attempt_mode": "normal_checked"}
    is_open, status = is_mint_open(drop_info, early_submit_seconds=0.5)
    if not is_open:
        return {"success": False, "reason": status, "balance_usd": 0, "eth_price_usd": eth_price_usd, "attempt_mode": "normal_checked"}
    if max_per_wallet is None:
        max_per_wallet = drop_info.get("max_per_wallet")
    current_price_wei = int(drop_info.get("price_wei", 0) or 0)
    if not is_free_or_negligible(current_price_wei, eth_price_usd):
        return {
            "success": False, "reason": "paid_mint_rejected",
            "price_wei": current_price_wei, "eth_price_usd": eth_price_usd,
            "attempt_mode": "normal_checked",
        }
    price_wei = 0

    # بعد فشل Fast وقبل أي بث عادي، لا نتجاوز الفحوص حتى لو كان
    # SKIP_BALANCE_CHECK_RACE مفعلاً؛ ذلك الخيار خاص بالمسار التنافسي فقط.
    balance_usd = get_wallet_balance_usd(w3, wallet_address, eth_price_usd)
    if balance_usd < 0:
        return {"success": False, "reason": "balance_read_failed", "balance_usd": 0, "attempt_mode": "normal_checked"}
    if balance_usd < MIN_BALANCE_RESERVE_USD:
        return {"success": False, "reason": "balance_too_low", "balance_usd": balance_usd, "attempt_mode": "normal_checked"}
    gas_fee_usd = estimate_gas_fee_usd(w3, eth_price_usd)
    if gas_fee_usd > MAX_GAS_FEE_USD:
        return {"success": False, "reason": "gas_too_high", "balance_usd": balance_usd, "gas_fee_usd": gas_fee_usd, "attempt_mode": "normal_checked"}

    with ThreadPoolExecutor(max_workers=2) as pool:
        ownership_future = pool.submit(get_wallet_ownership_balance, w3, nft_contract, wallet_address, chain_name)
        stats_future = pool.submit(get_wallet_mint_stats, w3, nft_contract, wallet_address, chain_name)
        owned_balance = ownership_future.result()
        mint_stats = stats_future.result()
    if REQUIRE_OWNERSHIP_CHECK and owned_balance is None:
        return {"success": False, "reason": "ownership_check_unavailable", "balance_usd": balance_usd, "attempt_mode": "normal_checked"}
    if owned_balance and owned_balance > 0:
        return {"success": False, "reason": "already_bought", "owned_balance": owned_balance, "balance_usd": balance_usd, "attempt_mode": "normal_checked"}
    if REQUIRE_MINT_STATS and mint_stats is None:
        return {"success": False, "reason": "mint_stats_unavailable", "balance_usd": balance_usd, "attempt_mode": "normal_checked"}
    if mint_stats is not None and int(mint_stats[0]) > 0:
        return {"success": False, "reason": "already_minted", "minted": int(mint_stats[0]), "balance_usd": balance_usd, "attempt_mode": "normal_checked"}

    result = attempt_purchase_seadrop(
        w3, private_key, wallet_address, nft_contract, seadrop_address,
        price_wei, max_per_wallet, remaining_supply, eth_price_usd, chain_name,
        {"balance_usd": balance_usd, "gas_fee_usd": gas_fee_usd,
         "owned_balance": owned_balance, "mint_stats": mint_stats},
    )
    result.setdefault("attempt_mode", "normal_checked")
    if fast_attempted and result.get("success"):
        result["attempt_mode"] = "normal_fallback"
        result["fast_failure_reason"] = fast_failure_reason

    if (not result.get("success") and allow_universal and
        result.get("reason") in ("tx_error", "simulation_failed", "reverted") and
        is_free_or_negligible(price_wei, eth_price_usd)):
        qty = decide_quantity(max_per_wallet, remaining_supply)
        uni_res = attempt_purchase_universal(w3, private_key, wallet_address, nft_contract, price_wei, qty, eth_price_usd)
        if uni_res.get("success") or uni_res.get("reason") == "tx_pending" or uni_res.get("tx_hash"):
            uni_res["balance_usd"] = balance_usd
            uni_res["gas_fee_usd"] = gas_fee_usd
            uni_res.setdefault("attempt_mode", "normal_checked")
            if fast_attempted:
                uni_res["attempt_mode"] = "normal_fallback"
                uni_res["fast_failure_reason"] = fast_failure_reason
            return uni_res

    result["eth_price_usd"] = eth_price_usd
    if "gas_fee_usd" not in result or result.get("gas_fee_usd") == 0:
        result["gas_fee_usd"] = gas_fee_usd
    return result

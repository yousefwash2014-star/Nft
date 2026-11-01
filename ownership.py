"""
Ownership check — On-Chain فقط (balanceOf)
"""
import asyncio
import logging
import time
from typing import Optional

from web3 import Web3

log = logging.getLogger("ownership")

_ownership_cache: dict = {}
_ownership_cache_lock = asyncio.Lock()
OWNERSHIP_CACHE_TTL = 120

_stats = {"checks": 0, "hits": 0, "misses": 0,
          "failures": 0, "owned": 0, "not_owned": 0}
_stats_lock = asyncio.Lock()

ERC721_ABI = [
    {"inputs": [{"name": "owner", "type": "address"}], "name": "balanceOf",
     "outputs": [{"name": "", "type": "uint256"}],
     "stateMutability": "view", "type": "function"},
]

ERC1155_ABI = [
    {"inputs": [{"name": "account", "type": "address"},
                {"name": "id", "type": "uint256"}], "name": "balanceOf",
     "outputs": [{"name": "", "type": "uint256"}],
     "stateMutability": "view", "type": "function"},
]


def _check_onchain_sync(w3: Web3, contract_address: str,
                         wallet: str) -> Optional[int]:
    return _get_balance_onchain(w3, contract_address, wallet)


def _get_balance_onchain(w3: Web3, contract_address: str,
                          wallet: str) -> Optional[int]:
    try:
        checksum_contract = Web3.to_checksum_address(contract_address)
        checksum_wallet = Web3.to_checksum_address(wallet)

        try:
            nft = w3.eth.contract(address=checksum_contract, abi=ERC721_ABI)
            return int(nft.functions.balanceOf(checksum_wallet).call())
        except Exception:
            pass

        try:
            nft = w3.eth.contract(address=checksum_contract, abi=ERC1155_ABI)
            return int(nft.functions.balanceOf(checksum_wallet, 0).call())
        except Exception:
            pass

        data = "0x70a08231" + checksum_wallet[2:].rjust(64, "0")
        result = w3.eth.call({"to": checksum_contract, "data": data})
        if result and len(result) >= 32:
            return int.from_bytes(result[:32], "big")
        return 0
    except Exception as e:
        log.debug("[_get_balance_onchain] %s", e)
        return None


async def wallet_owns_collection(
    session=None, wallet: str = "", slug: str = "",
    api_key: str = "", contract_address: str = "",
    w3: Optional[Web3] = None, chain: str = "ethereum",
) -> tuple:
    if not contract_address or not w3:
        return False, False

    now = time.time()
    try:
        chain_id = int(w3.eth.chain_id)
    except Exception:
        chain_id = 0

    cache_key = (chain_id, contract_address.lower(), wallet.lower())

    async with _stats_lock:
        _stats["checks"] += 1

    async with _ownership_cache_lock:
        cached = _ownership_cache.get(cache_key)
        if cached and now - cached[0] < OWNERSHIP_CACHE_TTL:
            async with _stats_lock:
                _stats["hits"] += 1
            return cached[1] > 0, True

    async with _stats_lock:
        _stats["misses"] += 1

    try:
        balance = await asyncio.to_thread(
            _check_onchain_sync, w3, contract_address, wallet
        )
        if balance is None:
            async with _stats_lock:
                _stats["failures"] += 1
            return False, False

        async with _ownership_cache_lock:
            _ownership_cache[cache_key] = (now, balance)

        async with _stats_lock:
            if balance > 0:
                _stats["owned"] += 1
            else:
                _stats["not_owned"] += 1

        return balance > 0, True
    except Exception as e:
        log.debug("[ownership] %s", e)
        async with _stats_lock:
            _stats["failures"] += 1
        return False, False


async def get_ownership_stats() -> dict:
    async with _stats_lock:
        s = dict(_stats)
    async with _ownership_cache_lock:
        s["cache_size"] = len(_ownership_cache)
    return s


async def invalidate_ownership_cache(
    chain: str = "", contract: str = "", wallet: str = "",
):
    contract_lc = (contract or "").lower()
    wallet_lc = (wallet or "").lower()
    async with _ownership_cache_lock:
        for k in list(_ownership_cache.keys()):
            if contract_lc and k[1] != contract_lc:
                continue
            if wallet_lc and k[2] != wallet_lc:
                continue
            _ownership_cache.pop(k, None)


def invalidate_ownership_cache_sync(contract: str = "", wallet: str = ""):
    contract_lc = (contract or "").lower()
    wallet_lc = (wallet or "").lower()
    for k in list(_ownership_cache.keys()):
        if contract_lc and k[1] != contract_lc:
            continue
        if wallet_lc and k[2] != wallet_lc:
            continue
        _ownership_cache.pop(k, None)
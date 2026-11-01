"""مراقبة معاملات NFT المعلّقة عبر JSON-RPC WebSocket.

المراقب لا يخمّن العقود من تلقاء نفسه؛ يسجل العقود التي اكتشفها OpenSea/REST
ثم يراقب معاملات pending الموجهة إليها. هذا يقلل الطلبات ويحافظ على الحماية.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Awaitable, Callable, Sequence

import websockets

log = logging.getLogger("blockchain-monitor")


class PendingTxMonitor:
    def __init__(
        self,
        chain_key: str,
        ws_urls: Sequence[str],
        on_contract_tx: Callable[[str, str], Awaitable[None]],
        max_inflight: int = 64,
    ) -> None:
        self.chain_key = chain_key
        self.ws_urls = tuple(dict.fromkeys(url.strip() for url in ws_urls if url.strip()))
        self.on_contract_tx = on_contract_tx
        self.max_inflight = max(1, max_inflight)
        self.known_contracts: set[str] = set()
        self._seen_hashes: set[str] = set()
        self._request_id = 0
        self._url_index = 0
        self._semaphore = asyncio.Semaphore(self.max_inflight)

    def register_contract(self, address: str) -> None:
        if address:
            self.known_contracts.add(address.lower())

    def _next_url(self) -> str:
        url = self.ws_urls[self._url_index % len(self.ws_urls)]
        self._url_index += 1
        return url

    async def _rpc(self, ws, method: str, params: list):
        self._request_id += 1
        request_id = self._request_id
        await ws.send(json.dumps({
            "jsonrpc": "2.0", "id": request_id, "method": method, "params": params,
        }, separators=(",", ":")))
        while True:
            message = json.loads(await asyncio.wait_for(ws.recv(), timeout=10))
            if message.get("id") == request_id:
                return message.get("result")

    async def _inspect_hash(self, tx_hash: str) -> None:
        # اتصال منفصل للفحص حتى لا تتعارض recv() مع قناة الاشتراك الرئيسية.
        async with self._semaphore:
            try:
                async with websockets.connect(
                    self._next_url(), ping_interval=20, ping_timeout=20,
                    open_timeout=10, close_timeout=5, max_size=None,
                ) as probe:
                    tx = await self._rpc(probe, "eth_getTransactionByHash", [tx_hash])
                target = str((tx or {}).get("to") or "").lower()
                if target and target in self.known_contracts:
                    await self.on_contract_tx(tx_hash, target)
            except Exception as exc:
                log.debug("[%s] تعذر فحص pending tx %s: %s", self.chain_key, tx_hash, exc)

    async def run(self) -> None:
        if not self.ws_urls:
            log.info("[%s] لا توجد روابط WSS؛ مراقب pending متوقف", self.chain_key)
            return
        if not self.known_contracts:
            log.info("[%s] بانتظار عقود مكتشفة من OpenSea/REST", self.chain_key)
        while True:
            try:
                url = self._next_url()
                async with websockets.connect(
                    url,
                    ping_interval=20,
                    ping_timeout=20,
                    open_timeout=15,
                    close_timeout=10,
                    max_queue=2048,
                    max_size=None,
                ) as ws:
                    result = await self._rpc(ws, "eth_subscribe", ["newPendingTransactions"])
                    log.info("[%s] pending monitor connected: %s", self.chain_key, result)
                    async for raw in ws:
                        message = json.loads(raw)
                        tx_hash = message.get("params", {}).get("result")
                        if not isinstance(tx_hash, str) or not tx_hash.startswith("0x"):
                            continue
                        if tx_hash in self._seen_hashes:
                            continue
                        self._seen_hashes.add(tx_hash)
                        if len(self._seen_hashes) > 100_000:
                            self._seen_hashes.clear()
                        if self.known_contracts:
                            asyncio.create_task(self._inspect_hash(tx_hash))
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.warning("[%s] pending WSS انقطع: %s؛ إعادة خلال 3s", self.chain_key, exc)
                await asyncio.sleep(3)

"""Persistent confirmed-mint history."""
from __future__ import annotations
import json
import os
import tempfile
import threading
import time

DEFAULT_PATH = os.getenv("MINT_HISTORY_FILE", "mint_history.json")


class MintHistory:
    def __init__(self, path: str = DEFAULT_PATH):
        self.path = path
        self._lock = threading.RLock()
        self._data: dict = {}
        self._load()

    @staticmethod
    def key(chain: str, slug: str, wallet: str) -> str:
        return f"{chain.lower()}|{slug.lower()}|{wallet.lower()}"

    def _load(self) -> None:
        with self._lock:
            try:
                with open(self.path, "r", encoding="utf-8") as f:
                    raw = json.load(f)
                if isinstance(raw, dict):
                    self._data = raw
            except FileNotFoundError:
                self._data = {}
            except Exception:
                self._data = {}

    def has(self, chain: str, slug: str, wallet: str) -> bool:
        with self._lock:
            return self.key(chain, slug, wallet) in self._data

    def record(self, chain: str, slug: str, wallet: str, tx_hash: str,
               block_number: int = 0, quantity: int = 1) -> None:
        if not tx_hash:
            return
        with self._lock:
            self._data[self.key(chain, slug, wallet)] = {
                "chain": chain,
                "slug": slug,
                "wallet": wallet,
                "tx_hash": tx_hash,
                "block_number": int(block_number or 0),
                "quantity": int(quantity or 1),
                "confirmed_at": int(time.time()),
            }
            self._atomic_save()

    def _atomic_save(self) -> None:
        directory = os.path.dirname(os.path.abspath(self.path)) or "."
        os.makedirs(directory, exist_ok=True)
        fd, tmp = tempfile.mkstemp(
            prefix=".mint_history.", suffix=".tmp", dir=directory
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(self._data, f, ensure_ascii=False,
                          indent=2, sort_keys=True)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, self.path)
        finally:
            if os.path.exists(tmp):
                try:
                    os.unlink(tmp)
                except OSError:
                    pass

    def count(self) -> int:
        with self._lock:
            return len(self._data)
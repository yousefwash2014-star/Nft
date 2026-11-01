"""تدوير مفاتيح API مع استبعاد مؤقت للمفتاح الذي وصل إلى حد المعدل."""

from __future__ import annotations

import threading
import time
from collections.abc import Iterable


class ApiKeyPool:
    def __init__(self, keys: Iterable[str], default_cooldown: float = 30.0):
        self._keys = tuple(dict.fromkeys(key.strip() for key in keys if key and key.strip()))
        self._default_cooldown = max(1.0, float(default_cooldown))
        self._index = 0
        self._blocked_until: dict[str, float] = {}
        self._lock = threading.Lock()

    @property
    def keys(self) -> tuple[str, ...]:
        return self._keys

    def next_key(self) -> str:
        if not self._keys:
            return ""
        with self._lock:
            now = time.monotonic()
            for _ in range(len(self._keys)):
                key = self._keys[self._index % len(self._keys)]
                self._index += 1
                if self._blocked_until.get(key, 0.0) <= now:
                    return key
            # إذا كانت كلها محجوبة، استخدم الأقل انتظاراً بدلاً من التوقف.
            return min(self._keys, key=lambda item: self._blocked_until.get(item, 0.0))

    def mark_rate_limited(self, key: str, retry_after: float | None = None) -> None:
        if not key:
            return
        delay = self._default_cooldown if retry_after is None else max(1.0, float(retry_after))
        with self._lock:
            self._blocked_until[key] = time.monotonic() + delay

    def available_count(self) -> int:
        now = time.monotonic()
        with self._lock:
            return sum(1 for key in self._keys if self._blocked_until.get(key, 0.0) <= now)

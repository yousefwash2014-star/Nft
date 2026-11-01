"""OpenSea Drops mint transaction builder (احتياطي)."""
import asyncio
import logging

import aiohttp
from config import OPENSEA_RATE_LIMITER

log = logging.getLogger("opensea-mint")

BASE = "https://api.opensea.io/api/v2/drops"


class MintBuildResult:
    __slots__ = ("ok", "reason", "target", "data", "value_wei",
                 "quantity", "status", "raw", "attempts")

    def __init__(self, ok, reason="", target="", data="0x", value_wei=0,
                 quantity=1, status=0, raw=None, attempts=1):
        self.ok = ok
        self.reason = reason
        self.target = target
        self.data = data
        self.value_wei = int(value_wei or 0)
        self.quantity = quantity
        self.status = status
        self.raw = raw or {}
        self.attempts = attempts

    def as_dict(self):
        return {
            "ok": self.ok, "reason": self.reason,
            "target": self.target, "data": self.data,
            "value_wei": self.value_wei, "quantity": self.quantity,
            "status": self.status, "attempts": self.attempts,
        }


def _classify(status: int) -> str:
    if status == 422:
        return "not_eligible"
    if status == 409:
        return "stage_not_active"
    if status == 404:
        return "drop_not_found"
    if status in (401, 403):
        return "opensea_auth_failed"
    if status == 429:
        return "rate_limited"
    if status >= 500:
        return "opensea_server_error"
    return f"opensea_http_{status}"


async def build_eligible_free_mint(
    session: aiohttp.ClientSession, slug: str, wallet: str,
    api_key: str, quantity: int = 1, retries: int = 2,
) -> MintBuildResult:
    if not api_key:
        return MintBuildResult(False, "missing_opensea_api_key")
    if not slug or not wallet:
        return MintBuildResult(False, "missing_slug_or_wallet")

    headers = {"x-api-key": api_key, "content-type": "application/json"}
    url = f"{BASE}/{slug}/mint"
    payload = {"minter": wallet, "quantity": int(quantity)}

    last_error = "unknown"
    for attempt in range(retries + 1):
        try:
            await OPENSEA_RATE_LIMITER.wait()
            async with session.post(
                url, headers=headers, json=payload,
                timeout=aiohttp.ClientTimeout(total=5),
            ) as r:
                if r.status == 200:
                    data = await r.json()
                    target = data.get("target") or data.get("to")
                    calldata = data.get("calldata") or data.get("data") or "0x"
                    value = int(data.get("value") or 0)
                    if not target:
                        return MintBuildResult(False, "missing_target",
                                               status=200, raw=data,
                                               attempts=attempt + 1)
                    return MintBuildResult(
                        True, target=target, data=calldata,
                        value_wei=value, quantity=quantity,
                        status=200, raw=data, attempts=attempt + 1,
                    )

                reason = _classify(r.status)
                last_error = reason

                if r.status == 429 and attempt < retries:
                    try:
                        OPENSEA_RATE_LIMITER.penalize(
                            float(r.headers.get("Retry-After", "1"))
                        )
                    except (TypeError, ValueError):
                        OPENSEA_RATE_LIMITER.penalize()
                    try:
                        delay = float(r.headers.get("Retry-After", "1"))
                    except Exception:
                        delay = 1.0
                    await asyncio.sleep(min(max(delay, 0.2), 5.0))
                    continue

                if r.status in (422, 404, 401, 403, 409):
                    try:
                        body = await r.json(content_type=None)
                    except Exception:
                        body = {}
                    return MintBuildResult(False, reason, status=r.status,
                                           raw=body, attempts=attempt + 1)

                if r.status >= 500 and attempt < retries:
                    await asyncio.sleep(0.25 * (attempt + 1))
                    continue

                try:
                    body = await r.json(content_type=None)
                except Exception:
                    body = {}
                return MintBuildResult(False, reason, status=r.status,
                                       raw=body, attempts=attempt + 1)

        except asyncio.TimeoutError:
            last_error = "opensea_timeout"
            if attempt < retries:
                await asyncio.sleep(0.2 * (attempt + 1))
                continue
        except Exception as e:
            last_error = f"opensea_request_error: {str(e)[:80]}"
            if attempt < retries:
                await asyncio.sleep(0.25 * (attempt + 1))
                continue

    return MintBuildResult(False, last_error, attempts=retries + 1)
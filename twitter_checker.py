"""
التحقق السريع غير المتزامن من حساب X (تويتر) للمجموعات لتفادي المجموعات الاحتيالية
باستخدام aiohttp لضمان أقصى سرعة ممكنة دون حظر الـ Event Loop.
"""

import os
import logging
import aiohttp
from typing import Optional

log = logging.getLogger("twitter-verifier")


async def get_twitter_username_from_opensea_async(
    session: aiohttp.ClientSession, slug: str, opensea_api_key: str
) -> Optional[str]:
    """جلب اسم مستخدم تويتر المربوط بالمجموعة من OpenSea API بشكل غير متزامن فائق السرعة."""
    try:
        url = f"https://api.opensea.io/api/v2/collections/{slug}"
        headers = {"x-api-key": opensea_api_key}
        async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=4)) as resp:
            if resp.status == 200:
                data = await resp.json()
                return data.get("twitter_username")
            elif resp.status == 429:
                log.warning(f"[OpenSea Collections API] Rate limit (429) لـ '{slug}'")
            else:
                log.warning(f"[OpenSea Collections API] HTTP {resp.status} لـ '{slug}'")
    except Exception as e:
        log.warning(f"[Twitter Check] خطأ أثناء جلب حساب تويتر لـ '{slug}': {e}")
    return None


async def is_valid_twitter_account_async(session: aiohttp.ClientSession, username: str) -> bool:
    """التحقق من موثوقية حساب تويتر (إن وجد مفتاح Twitter Bearer Token)."""
    bearer_token = os.environ.get("TWITTER_BEARER_TOKEN", "").strip()
    if not username:
        return False
    if not bearer_token:
        return True

    try:
        url = f"https://api.x.com/2/users/by/username/{username}?user.fields=verified,public_metrics"
        headers = {"Authorization": f"Bearer {bearer_token}"}
        async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=4)) as resp:
            if resp.status == 200:
                user_data = (await resp.json()).get("data", {})
                metrics = user_data.get("public_metrics", {})
                is_verified = user_data.get("verified", False)
                followers_count = metrics.get("followers_count", 0)

                if is_verified or followers_count >= 50:
                    log.info(f"✅ حساب X موثوق: @{username} (متابعين: {followers_count})")
                    return True
                else:
                    log.info(f"⚠️ حساب X ضعيف: @{username} (متابعين: {followers_count})")
                    return False
            elif resp.status == 429:
                log.warning(f"[X API] تجاوزت حد الطلبات (429) عند فحص @{username} — سيتم تمريره للأمان.")
                return True
            else:
                log.warning(f"[X API] HTTP {resp.status} لـ @{username}")
                return True
    except Exception as e:
        log.warning(f"[X API Error] خطأ أثناء الفحص: {e}")
        return True

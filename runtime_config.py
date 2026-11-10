"""تحقق مركزي من إعدادات الشبكات قبل تشغيل البوت."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping, Sequence


@dataclass(frozen=True)
class NetworkRequirement:
    key: str
    chain_id: int
    env_prefix: str


NETWORK_REQUIREMENTS: tuple[NetworkRequirement, ...] = (
    NetworkRequirement("ethereum", 1, "ETHEREUM"),
    NetworkRequirement("robinhood", 4663, "ROBINHOOD"),
    NetworkRequirement("ink", 57073, "INK"),
    NetworkRequirement("arc", 5042, "ARC"),
    NetworkRequirement("elysium", 1339, "ELYSIUM"),
)
NETWORK_ORDER = tuple(item.key for item in NETWORK_REQUIREMENTS)
EXPECTED_CHAIN_IDS = {item.key: item.chain_id for item in NETWORK_REQUIREMENTS}


def validate_network_configs(
    chain_configs: Mapping[str, Mapping],
    rpc_urls_by_chain: Mapping[str, Sequence[str]],
) -> list[str]:
    """يعيد أخطاء إعداد واضحة دون كشف أي مفاتيح سرية."""
    issues: list[str] = []
    configured = set(chain_configs)
    expected = set(NETWORK_ORDER)

    missing = expected - configured
    extra = configured - expected
    if missing:
        issues.append(f"شبكات مطلوبة غير موجودة: {', '.join(sorted(missing))}")
    if extra:
        issues.append(f"شبكات غير معروفة في التهيئة: {', '.join(sorted(extra))}")

    for requirement in NETWORK_REQUIREMENTS:
        cfg = chain_configs.get(requirement.key)
        if cfg is None:
            continue
        actual_id = cfg.get("chain_id")
        if actual_id != requirement.chain_id:
            issues.append(
                f"{requirement.key}: Chain ID غير صحيح ({actual_id}); "
                f"المتوقع {requirement.chain_id}"
            )
        urls = [str(url).strip() for url in rpc_urls_by_chain.get(requirement.key, ()) if str(url).strip()]
        if not urls:
            issues.append(f"{requirement.key}: لا يوجد رابط RPC صالح")
        for url in urls:
            if not (url.startswith("http://") or url.startswith("https://")):
                issues.append(f"{requirement.key}: رابط RPC غير صالح")
                break
    return issues

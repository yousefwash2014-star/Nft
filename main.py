"""
=============================================================================
نظام المينت التلقائي الموحد — النسخة التنافسية (Race Mode + Cascade)
=============================================================================
  ⚡ ثلاث شبكات Mainnet: Robinhood + Arc + Ink
  👛 محافظ متعددة + إشعارات تيليجرام مخصصة
  🔄 تناوب ذكي بين RPCs
  🏁 Race Mode: تجهيز مسبق + إرسال قبل الفتح + إعادة محاولة سريعة
  💥 Cascade Sniping: إرسال متسلسل بغاز متزايد
  💰 Gas Cap لكل سلسلة: Robinhood=$0.10, Ink=$0.10, Arc=$0.05
  🛡️ Rate Limiter استباقي (Backoff أُسّي)
  📨 إشعارات Telegram موثوقة (escaping + fallback + تجاهل القيم الوهمية)
  🔕 منع تكرار الإشعارات ومحاولات الشراء
=============================================================================
"""

import asyncio
import html as _html_escape
import json
import logging
import os
import re
import time
from pathlib import Path
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass, field
from typing import Optional

import aiohttp
import websockets
from dotenv import load_dotenv

# يجب تحميل .env قبل استيراد buyer لأن محرك التنفيذ يقرأ إعداداته عند الاستيراد.
# استخدام مسار الملف يجعل التشغيل من Railway/Docker أو من أي مجلد آخر موثوقًا.
load_dotenv(Path(__file__).with_name(".env"))

from buyer import (
    get_web3_from_config,
    get_web3_rotating,
    inject_free_rpcs,
    execute_purchase_pipeline,
    CHAINS_CONFIG,
    FREE_RPC_POOLS,
    RETRYABLE_REASONS,
    MAX_GAS_FEE_USD,
    MIN_BALANCE_RESERVE_USD,
    get_onchain_public_price_wei,
    is_paid_mint,
    is_free_or_negligible,
    get_drop_info,
    is_mint_open,
    RACE_MODE,
    RACE_CASCADE_ENABLED,
    RACE_PRE_SUBMIT_SECONDS,
    RacePlan,
    prepare_race_plan,
    race_all_wallets,
)
from twitter_checker import (
    get_twitter_username_from_opensea_async,
    is_valid_twitter_account_async,
)
from runtime_config import NETWORK_ORDER, validate_network_configs
from key_pool import ApiKeyPool
from stage_policy import is_public_eligible_stage, stage_rejection_reason
from blockchain_monitor import PendingTxMonitor
from mint_history import MintHistory

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("unified-bot")


def esc(value) -> str:
    return _html_escape.escape(str(value if value is not None else ""), quote=False)


def _csv_env(*names: str) -> list[str]:
    values: list[str] = []
    for name in names:
        values.extend(v.strip() for v in os.environ.get(name, "").split(",") if v.strip())
    return list(dict.fromkeys(values))


# ===================================================================
# 🚫 اكتشاف القيم الوهمية من .env.example
# ===================================================================
_PLACEHOLDER_VALUES = {
    "chat_id_1", "chat_id_2", "chat_id_3", "chat_id_4", "chat_id_5",
    "bot_token_1", "bot_token_2", "bot_token_3", "bot_token_4", "bot_token_5",
    "chat_id", "bot_token", "your_chat_id", "your_bot_token",
    "0x_private_key_1", "0x_private_key_2", "0x_private_key_3",
    "0x_wallet_address_1", "0x_wallet_address_2", "0x_wallet_address_3",
    "key1", "key2", "key3",
}

_PLACEHOLDER_PATTERN = re.compile(
    r"^(chat_?id|bot_?token|key|token|0x_private_key|0x_wallet_address)_?\d+$"
)


def _is_placeholder(value: str) -> bool:
    """يكتشف القيم الوهمية من ملف .env.example."""
    v = (value or "").strip().lower()
    if not v:
        return True
    if v in _PLACEHOLDER_VALUES:
        return True
    if _PLACEHOLDER_PATTERN.match(v):
        return True
    return False


def _telegram_config_hint() -> str:
    """يعطي سببًا قابلًا للتنفيذ عندما لا توجد وجهة Telegram صالحة."""
    raw_tokens = _csv_env("TELEGRAM_BOT_TOKENS", "TELEGRAM_BOT_TOKEN")
    raw_chats = _csv_env("TELEGRAM_CHAT_IDS", "TELEGRAM_CHAT_ID")
    if raw_tokens and all(_is_placeholder(v) for v in raw_tokens):
        return "TELEGRAM_BOT_TOKEN(S) ما زال يحتوي قيمًا تجريبية مثل BOT_TOKEN_1"
    if raw_chats and all(_is_placeholder(v) for v in raw_chats):
        return "TELEGRAM_CHAT_ID(S) ما زال يحتوي قيمًا تجريبية مثل CHAT_ID_1"
    return "ضع TELEGRAM_BOT_TOKEN وTELEGRAM_CHAT_ID الحقيقيين في .env ثم أعد التشغيل"


# ===================================================================
# 🔑 مفاتيح OpenSea
# ===================================================================
_raw_keys = _csv_env("OPENSEA_API_KEYS", "OPENSEA_API_KEY")
OPENSEA_API_KEYS = [k for k in _raw_keys if not _is_placeholder(k)]
OPENSEA_API_KEY = OPENSEA_API_KEYS[0] if OPENSEA_API_KEYS else ""
_opensea_key_pool = ApiKeyPool(OPENSEA_API_KEYS)

log.info("🔑 OpenSea API keys loaded: %d", len(OPENSEA_API_KEYS))
if not OPENSEA_API_KEYS:
    log.warning("⚠️ لم يتم العثور على أي مفتاح OpenSea في OPENSEA_API_KEYS أو OPENSEA_API_KEY")


async def get_next_opensea_key() -> str:
    return _opensea_key_pool.next_key()


# ===================================================================
# ⚙️ متغيرات التشغيل
# ===================================================================
BOT_ENABLED = os.environ.get("BOT_ENABLED", "false").strip().lower() == "true"
CHECK_TWITTER = os.environ.get("CHECK_TWITTER", "false").strip().lower() == "true"
ENABLE_UNIVERSAL_MINTS = True
TELEGRAM_NOTIFY_FAILURES = False
VERIFY_ONCHAIN_PRICE = True
SKIP_BALANCE_CHECK_RACE = True
FIXED_GAS_LIMIT = 185000
FAST_SUBMIT_MODE = SKIP_BALANCE_CHECK_RACE and FIXED_GAS_LIMIT > 0
NOTIFY_SCHEDULED = False
# عند تعطيله، لا تُنشأ مهام للمراحل المستقبلية؛ ينتظر الماسح/الـ Stream حتى تصبح Public مفتوحة.
SCHEDULE_FUTURE_STAGES = False

STREAM_URL_BASE = "wss://stream-api.opensea.io/socket/websocket?token={token}&vsn=2.0.0"
DROPS_API_BASE = "https://api.opensea.io/api/v2/drops"
ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"
LOCAL_TZ = timezone(timedelta(hours=3))

PERFORMANCE_PROFILE = "ultra"
TURBO_MODE = PERFORMANCE_PROFILE in {"turbo", "fast", "speed", "ultra"}
ULTRA_MODE = PERFORMANCE_PROFILE == "ultra"
MAX_CONCURRENT_MINTS = 40
HEARTBEAT_INTERVAL = 30
RECV_TIMEOUT = 1.0
FREE_PRICE_THRESHOLD_USD = 0.00015
SCAN_INTERVAL = 0.25
MAX_RETRY_HOURS = 6.0
MAX_RETRY_ATTEMPTS = 2
PENDING_TX_MONITOR = False
PENDING_TX_MAX_INFLIGHT = 32


def http_session() -> aiohttp.ClientSession:
    connector = aiohttp.TCPConnector(
        limit=200 if TURBO_MODE else 100,
        limit_per_host=100 if TURBO_MODE else 50,
        ttl_dns_cache=300, keepalive_timeout=30, enable_cleanup_closed=True,
    )
    return aiohttp.ClientSession(connector=connector)


# ===================================================================
# 👛 تحميل المحافظ (مع تجاهل القيم الوهمية)
# ===================================================================
def load_wallets_config() -> list[dict]:
    wallets_data = []

    raw_pks = [k.strip() for k in os.environ.get("PRIVATE_KEYS", "").split(",") if k.strip()]
    raw_addrs = [w.strip() for w in os.environ.get("WALLETS", "").split(",") if w.strip()]
    raw_tokens = [t.strip() for t in os.environ.get("TELEGRAM_BOT_TOKENS", "").split(",") if t.strip()]
    raw_chats = [c.strip() for c in os.environ.get("TELEGRAM_CHAT_IDS", "").split(",") if c.strip()]

    default_token = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
    default_chat = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
    if _is_placeholder(default_token):
        default_token = ""
    if _is_placeholder(default_chat):
        default_chat = ""

    # النمط 1: قوائم مفصولة بفواصل
    if raw_pks and raw_addrs and len(raw_pks) == len(raw_addrs):
        for idx, (pk, addr) in enumerate(zip(raw_pks, raw_addrs)):
            if _is_placeholder(pk) or _is_placeholder(addr):
                log.warning("⚠️ تخطي محفظة %d: قيمة وهمية (placeholder)", idx + 1)
                continue
            b_tok = raw_tokens[idx] if idx < len(raw_tokens) else ""
            c_id = raw_chats[idx] if idx < len(raw_chats) else ""
            if _is_placeholder(b_tok):
                b_tok = ""
            if _is_placeholder(c_id):
                c_id = ""
            b_tok = b_tok or default_token
            c_id = c_id or default_chat
            wallets_data.append({
                "name": f"محفظة {idx + 1}",
                "private_key": pk,
                "address": addr,
                "bot_token": b_tok,
                "chat_id": c_id,
            })
        return wallets_data

    # النمط 2: محافظ فردية
    configs = [
        ("محفظة 1", "PRIVATE_KEY", "WALLET_ADDRESS"),
        ("محفظة 2", "WALLET_2_PRIVATE_KEY", "WALLET_2_ADDRESS"),
        ("محفظة 3", "WALLET_3_PRIVATE_KEY", "WALLET_3_ADDRESS"),
        ("محفظة 4", "WALLET_4_PRIVATE_KEY", "WALLET_4_ADDRESS"),
        ("محفظة 5", "WALLET_5_PRIVATE_KEY", "WALLET_5_ADDRESS"),
        ("محفظة 6", "WALLET_6_PRIVATE_KEY", "WALLET_6_ADDRESS"),
        ("محفظة 7", "WALLET_7_PRIVATE_KEY", "WALLET_7_ADDRESS"),
        ("محفظة 8", "WALLET_8_PRIVATE_KEY", "WALLET_8_ADDRESS"),
        ("محفظة 9", "WALLET_9_PRIVATE_KEY", "WALLET_9_ADDRESS"),
        ("محفظة 10", "WALLET_10_PRIVATE_KEY", "WALLET_10_ADDRESS"),
    ]
    for name, pk_var, addr_var in configs:
        pk = os.environ.get(pk_var, "").strip()
        addr = os.environ.get(addr_var, "").strip()
        if pk and addr and not _is_placeholder(pk) and not _is_placeholder(addr):
            wallets_data.append({
                "name": name, "private_key": pk, "address": addr,
                "bot_token": default_token, "chat_id": default_chat,
            })
    return wallets_data


WALLETS_DATA = load_wallets_config()


def _resolve_telegram_target(bot_token: str, chat_id: str) -> tuple[str, str]:
    tok = (bot_token or "").strip()
    cid = (chat_id or "").strip()
    if _is_placeholder(tok):
        tok = ""
    if _is_placeholder(cid):
        cid = ""
    if not tok:
        tok = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
        if _is_placeholder(tok):
            tok = ""
    if not cid:
        cid = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
        if _is_placeholder(cid):
            cid = ""
    return tok, cid


telegram_wallet_targets = sum(
    1 for w in WALLETS_DATA
    if all(_resolve_telegram_target(w.get("bot_token", ""), w.get("chat_id", "")))
)
telegram_default_configured = bool(
    os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
    and not _is_placeholder(os.environ.get("TELEGRAM_BOT_TOKEN", "").strip())
    and os.environ.get("TELEGRAM_CHAT_ID", "").strip()
    and not _is_placeholder(os.environ.get("TELEGRAM_CHAT_ID", "").strip())
)
log.info("📨 Telegram targets: %d wallet target(s), default target configured=%s",
         telegram_wallet_targets, telegram_default_configured)
if not telegram_wallet_targets and not telegram_default_configured:
    log.warning("⚠️ لا توجد وجهة Telegram صالحة — %s", _telegram_config_hint())


# ===================================================================
# 🌐 تهيئة RPC
# ===================================================================
ALCHEMY_KEY_ETHEREUM = os.environ.get("ALCHEMY_API_KEY_ETHEREUM", "").strip()
ALCHEMY_KEY_ROBINHOOD = os.environ.get("ALCHEMY_API_KEY_ROBINHOOD", os.environ.get("ALCHEMY_API_KEY", "")).strip()

ETHEREUM_RPC_URL = os.environ.get("ETHEREUM_RPC_URL", "").strip()
if not ETHEREUM_RPC_URL and ALCHEMY_KEY_ETHEREUM:
    ETHEREUM_RPC_URL = f"https://eth-mainnet.g.alchemy.com/v2/{ALCHEMY_KEY_ETHEREUM}"

ROBINHOOD_RPC_URL = os.environ.get("ROBINHOOD_RPC_URL", "").strip()
if not ROBINHOOD_RPC_URL and ALCHEMY_KEY_ROBINHOOD:
    ROBINHOOD_RPC_URL = f"https://robinhood-mainnet.g.alchemy.com/v2/{ALCHEMY_KEY_ROBINHOOD}"

INK_RPC_URL = os.environ.get("INK_RPC_URL", "https://rpc-gel.inkonchain.com").strip()
ARC_RPC_URL = os.environ.get("ARC_RPC_URL", "https://rpc.mainnet.arc.io").strip()

RPC_URLS_BY_CHAIN = {
    "ethereum": _csv_env("ETHEREUM_RPC_URLS", "ETHEREUM_RPC_URL"),
    "robinhood": _csv_env("ROBINHOOD_RPC_URLS", "ROBINHOOD_RPC_URL"),
    "arc": _csv_env("ARC_RPC_URLS", "ARC_RPC_URL"),
    "ink": _csv_env("INK_RPC_URLS", "INK_RPC_URL"),
}

def _wss_urls(chain_key: str, env_name: str, alchemy_key: str = "") -> list[str]:
    urls = _csv_env(env_name)
    if urls:
        return urls
    if alchemy_key:
        return [f"wss://{chain_key}-mainnet.g.alchemy.com/v2/{alchemy_key}"]
    return []


WSS_URLS_BY_CHAIN = {
    "ethereum": _wss_urls("eth", "ETHEREUM_WSS_URLS", ALCHEMY_KEY_ETHEREUM),
    "robinhood": _wss_urls("robinhood", "ROBINHOOD_WSS_URLS", ALCHEMY_KEY_ROBINHOOD),
    "arc": _csv_env("ARC_WSS_URLS"),
    "ink": _csv_env("INK_WSS_URLS"),
}

for chain_key, rpc_urls in RPC_URLS_BY_CHAIN.items():
    for rpc_url in rpc_urls:
        inject_free_rpcs(chain_key, rpc_url)
    if rpc_urls:
        CHAINS_CONFIG[chain_key]["rpc_url"] = rpc_urls[0]

CHAINS_CONFIG = {key: CHAINS_CONFIG[key] for key in NETWORK_ORDER}
_EFFECTIVE_RPC_URLS = {
    key: RPC_URLS_BY_CHAIN.get(key) or FREE_RPC_POOLS.get(key, [])
    for key in NETWORK_ORDER
}
_config_issues = validate_network_configs(CHAINS_CONFIG, _EFFECTIVE_RPC_URLS)
for _issue in _config_issues:
    log.error("❌ خطأ إعداد الشبكة: %s", _issue)

W3_INSTANCES = {}
ENABLED_CHAINS = []

for chain_key, cfg in CHAINS_CONFIG.items():
    rpc = cfg.get("rpc_url")
    w3_obj = None
    if rpc:
        try:
            w3_obj = get_web3_from_config(cfg)
        except Exception:
            pass
    if not w3_obj or not w3_obj.is_connected():
        w3_obj = get_web3_rotating(chain_key, rpc)

    if w3_obj and w3_obj.is_connected():
        expected_chain_id = cfg.get("chain_id")
        try:
            actual_chain_id = int(w3_obj.eth.chain_id)
        except Exception:
            actual_chain_id = None
        if expected_chain_id and actual_chain_id != expected_chain_id:
            log.error("❌ %s - Chain ID غير صحيح: المتوقع %s، الحالي %s.",
                      cfg["chain_name_display"], expected_chain_id, actual_chain_id)
            continue
        W3_INSTANCES[chain_key] = w3_obj
        ENABLED_CHAINS.append(chain_key)
        log.info(f"✅ {cfg['chain_name_display']} - متصل بنجاح")
    else:
        log.warning(f"⚠️ {cfg['chain_name_display']} - غير متصل")

STREAM_NAME_TO_CHAIN_KEY = {
    "ethereum": "ethereum", "eth": "ethereum",
    "robinhood": "robinhood",
    "ink": "ink", "arc": "arc",
}

# عقود اكتشفها OpenSea/REST؛ لا تتم مراقبة pending لعقود مجهولة عشوائياً.
PENDING_MONITORS: dict[str, PendingTxMonitor] = {}
KNOWN_MINT_CONTRACTS: dict[str, dict[str, str]] = {key: {} for key in NETWORK_ORDER}


def register_mint_contract(chain_key: str, slug: str, contract_address: str) -> None:
    address = (contract_address or "").lower()
    if not address or chain_key not in PENDING_MONITORS:
        return
    KNOWN_MINT_CONTRACTS.setdefault(chain_key, {})[address] = slug
    PENDING_MONITORS[chain_key].register_contract(address)
    log.debug("[%s] pending monitor registered contract=%s slug=%s", chain_key, address, slug)


async def on_pending_contract_tx(chain_key: str, tx_hash: str, target: str) -> None:
    slug = KNOWN_MINT_CONTRACTS.get(chain_key, {}).get(target.lower())
    if not slug:
        return
    log.info("⚡ [%s] pending mint tx detected for %s: %s", chain_key, slug, tx_hash)
    session = getattr(on_pending_contract_tx, "session", None)
    if session is not None:
        schedule_evaluation(session, slug, chain_key)


# ===================================================================
# تتبع المهام
# ===================================================================
successful_mints: dict[str, set[str]] = {}
in_flight: set[str] = set()

def drop_key(chain_key: str, slug: str) -> str:
    return f"{chain_key}:{slug}"
rejected_cooldown: dict[str, float] = {}
failed_attempts: dict[str, float] = {}
terminal_wallet_failures: set[str] = set()
pending_transactions: dict[str, dict] = {}
failure_notifications_sent: set[str] = set()
evaluation_tasks: dict[str, asyncio.Task] = {}
# حجز محفظة/Drop موحد يمنع تداخل normal وRace وretry في نفس العملية.
wallet_operation_reservations: set[str] = set()

def wallet_operation_key(chain_key: str, slug: str, wallet: str) -> str:
    return f"{chain_key}:{slug}:{wallet.lower()}"

def reserve_wallet_operation(chain_key: str, slug: str, wallet: str) -> bool:
    key = wallet_operation_key(chain_key, slug, wallet)
    if key in wallet_operation_reservations:
        return False
    wallet_operation_reservations.add(key)
    return True

def release_wallet_operation(chain_key: str, slug: str, wallet: str) -> None:
    wallet_operation_reservations.discard(wallet_operation_key(chain_key, slug, wallet))

def schedule_evaluation(session, slug: str, chain_key: str) -> None:
    """جدولة تقييم واحد فقط لكل (شبكة، مجموعة) لمنع تراكم المهام."""
    key = drop_key(chain_key, slug)
    task = evaluation_tasks.get(key)
    if task is not None and not task.done():
        return
    task = asyncio.create_task(evaluate_and_execute_mint(session, slug, chain_key))
    evaluation_tasks[key] = task
    def _cleanup(_task, k=key):
        if evaluation_tasks.get(k) is _task:
            evaluation_tasks.pop(k, None)
    task.add_done_callback(_cleanup)
mint_history = MintHistory()
REJECTION_COOLDOWN_SECONDS = 180
FAILED_COOLDOWN_SECONDS = 1.0

semaphore = asyncio.Semaphore(MAX_CONCURRENT_MINTS)


@dataclass
class RetryTracker:
    slug: str
    wallet_address: str
    chain_name: str
    detail: dict
    wallet_name: str
    wallet_private_key: str
    price_wei: int
    max_per_wallet: Optional[int]
    remaining_supply: int
    eth_price_usd: float
    bot_token: str
    chat_id: str
    start_time: float = field(default_factory=time.time)
    attempt_count: int = 1
    original_reason: str = ""

    @property
    def is_expired(self) -> bool:
        return (time.time() - self.start_time) >= (MAX_RETRY_HOURS * 3600)


retry_tasks: dict[str, RetryTracker] = {}
retry_lock = asyncio.Lock()

stage_open_tasks: dict[str, asyncio.Task] = {}
stage_open_lock = asyncio.Lock()
notified_scheduled_drops: set[str] = set()


# ===================================================================
# 🏁 ScheduledRace
# ===================================================================
@dataclass
class ScheduledRace:
    slug: str
    chain_key: str
    start_time: datetime
    contract_address: str
    seadrop_address: str
    price_wei: int
    max_per_wallet: Optional[int]
    remaining: int
    native_price_usd: float
    detail: dict


race_registry: dict[str, ScheduledRace] = {}


async def build_race_plans_for_drop(
    session: aiohttp.ClientSession,
    race: ScheduledRace,
) -> list[RacePlan]:
    w3 = W3_INSTANCES.get(race.chain_key)
    if not w3:
        return []

    async def _prepare(w):
        api_key = await get_next_opensea_key()
        return await asyncio.to_thread(
            prepare_race_plan,
            w3, w["private_key"], w["address"],
            race.contract_address, race.seadrop_address,
            race.price_wei, race.max_per_wallet, race.remaining,
            race.native_price_usd, race.chain_key, race.slug, api_key,
        )

    prepared = await asyncio.gather(*(_prepare(w) for w in WALLETS_DATA), return_exceptions=True)
    plans: list[RacePlan] = []
    for item in prepared:
        if isinstance(item, RacePlan):
            plans.append(item)
        elif isinstance(item, Exception):
            log.debug("[RacePrep] parallel wallet preparation failed: %s", item)
    return plans


async def execute_race(race: ScheduledRace, plans: list[RacePlan]):
    w3 = W3_INSTANCES.get(race.chain_key)
    if not w3 or not plans:
        return

    # إعادة التحقق on-chain مباشرة قبل Race حتى لا نرسل خطة قديمة.
    current_drop = await asyncio.to_thread(
        get_drop_info, w3, race.contract_address, race.seadrop_address
    )
    if not current_drop:
        log.warning("⏭️ [RACE] تعذر التحقق on-chain قبل الإرسال: %s", race.slug)
        return
    if int(current_drop.get("price_wei", 0) or 0) != 0:
        log.warning("⏭️ [RACE] أُلغي لأن السعر on-chain لم يعد مجانيًا: %s", race.slug)
        return
    is_open, status = is_mint_open(current_drop, early_submit_seconds=0.5)
    if not is_open:
        log.warning("⏭️ [RACE] أُلغي قبل الإرسال: %s", status)
        return

    # منع سباق مجدول من العمل بالتزامن مع حدث WebSocket أو الماسح الدوري.
    if drop_key(race.chain_key, race.slug) in in_flight:
        log.warning("⏭️ تخطي Race مكرر للمجموعة '%s'", race.slug)
        return
    if len(successful_mints.get(drop_key(race.chain_key, race.slug), set())) >= len(plans):
        return
    in_flight.add(drop_key(race.chain_key, race.slug))
    reserved_plans = []
    for plan in plans:
        pending_key = wallet_operation_key(race.chain_key, race.slug, plan.wallet_address)
        if pending_key in pending_transactions:
            continue
        if reserve_wallet_operation(race.chain_key, race.slug, plan.wallet_address):
            reserved_plans.append(plan)
    plans = reserved_plans
    if not plans:
        in_flight.discard(drop_key(race.chain_key, race.slug))
        return
    try:
        log.info(
            "🏁 [RACE] انطلاق '%s' على %s — %d محفظة بالتوازي",
            race.slug, race.chain_key, len(plans),
        )

        results = await asyncio.to_thread(race_all_wallets, w3, plans)

        for plan, result in zip(plans, results):
            wallet_data = next(
                (w for w in WALLETS_DATA if w["address"].lower() == plan.wallet_address.lower()),
                None,
            )
            wallet_name = wallet_data["name"] if wallet_data else plan.wallet_address[:8]
            bot_tok = wallet_data["bot_token"] if wallet_data else ""
            chat_id = wallet_data["chat_id"] if wallet_data else ""

            if result.get("success"):
                result.setdefault("attempt_mode", "race")
                if drop_key(race.chain_key, race.slug) not in successful_mints:
                    successful_mints[drop_key(race.chain_key, race.slug)] = set()
                successful_mints[drop_key(race.chain_key, race.slug)].add(plan.wallet_address)
                mint_history.record(
                    race.chain_key, race.slug, plan.wallet_address,
                    result.get("tx_hash", ""), result.get("block_number", 0),
                    result.get("quantity", 1),
                )
                result["wallet"] = plan.wallet_address
                enqueue_message(
                    bot_tok, chat_id,
                    build_success_msg(race.detail, result, race.chain_key, wallet_name),
                    source=f"race-success:{race.slug}",
                )
                log.info("✅ [RACE] %s نجح — %s | latency=%sms", wallet_name, result.get("tx_hash"), result.get("latency_ms", "?"))
            else:
                result.setdefault("attempt_mode", "race")
                reason = result.get("reason", "")
                if reason == "tx_pending" and result.get("tx_hash"):
                    pending_transactions[f"{race.chain_key}:{race.slug}:{plan.wallet_address.lower()}"] = {
                        "tx_hash": result["tx_hash"], "chain_key": race.chain_key,
                        "slug": race.slug, "wallet": plan.wallet_address,
                        "quantity": result.get("quantity", 1),
                        "detail": race.detail, "wallet_name": wallet_name,
                        "bot_token": bot_tok, "chat_id": chat_id,
                        "attempt_mode": result.get("attempt_mode", "race"),
                        "gas_fee_usd": result.get("gas_fee_usd", 0),
                    }
                log.warning("❌ [RACE] %s فشل — %s | %s", wallet_name, reason, result.get("error", "")[:120])
                if TELEGRAM_NOTIFY_FAILURES and reason not in ("not_free_mint", "paid_mint", "already_bought"):
                    enqueue_message(
                        bot_tok, chat_id,
                        build_fail_msg(race.detail, result, race.chain_key, wallet_name),
                        source=f"race-fail:{race.slug}:{reason}",
                    )
            if result.get("reason") != "tx_pending":
                release_wallet_operation(race.chain_key, race.slug, plan.wallet_address)
    finally:
        in_flight.discard(drop_key(race.chain_key, race.slug))


async def schedule_stage_open(
    session: aiohttp.ClientSession,
    slug: str,
    chain_key: str,
    start_time: datetime,
    race: Optional[ScheduledRace] = None,
) -> None:
    if start_time.tzinfo is None:
        start_time = start_time.replace(tzinfo=timezone.utc)
    start_time = start_time.astimezone(timezone.utc)
    task_key = f"{chain_key}:{slug}"

    async with stage_open_lock:
        existing = stage_open_tasks.get(task_key)
        if existing and not existing.done():
            return

        async def wait_for_stage_open() -> None:
            try:
                now_utc = datetime.now(timezone.utc)
                secs_to_start = (start_time - now_utc).total_seconds()

                if RACE_MODE and race is not None:
                    prep_delay = max(0.0, secs_to_start - 60.0)
                    if prep_delay > 0:
                        await asyncio.sleep(prep_delay)

                    plans = await build_race_plans_for_drop(session, race)
                    if not plans:
                        log.error("🏁 [RACE] فشل تجهيز الخطط لـ '%s' — العودة للوضع الكلاسيكي", slug)
                        await evaluate_and_execute_mint(session, slug, chain_key)
                        return

                    remaining = (start_time - datetime.now(timezone.utc)).total_seconds()
                    pre_submit_delay = max(0.0, remaining - RACE_PRE_SUBMIT_SECONDS)
                    if pre_submit_delay > 0:
                        await asyncio.sleep(pre_submit_delay)

                    await execute_race(race, plans)
                    return

                delay = max(0.0, (start_time - datetime.now(timezone.utc)).total_seconds() + 0.05)
                if delay:
                    await asyncio.sleep(delay)
                await evaluate_and_execute_mint(session, slug, chain_key)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.error("خطأ في جدولة فتح المرحلة %s/%s: %s", chain_key, slug, exc)
            finally:
                current = stage_open_tasks.get(task_key)
                if current is asyncio.current_task():
                    stage_open_tasks.pop(task_key, None)
                notify_key = f"{chain_key}:{slug}:{start_time.isoformat()}"
                notified_scheduled_drops.discard(notify_key)
                race_registry.pop(task_key, None)

        stage_open_tasks[task_key] = asyncio.create_task(wait_for_stage_open())


# ===================================================================
# 💰 سعر ETH
# ===================================================================
_eth_price_cache = {"value": 3000.0, "ts": 0}


async def eth_price_updater():
    async with http_session() as session:
        while True:
            try:
                async with session.get(
                    "https://api.coingecko.com/api/v3/simple/price?ids=ethereum&vs_currencies=usd",  # سعر ETH لـ Ink/Robinhood
                    timeout=aiohttp.ClientTimeout(total=6),
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        if "ethereum" in data and "usd" in data["ethereum"]:
                            _eth_price_cache["value"] = float(data["ethereum"]["usd"])
                            _eth_price_cache["ts"] = time.time()
            except Exception as e:
                log.debug(f"[سعر ETH] خطأ: {e}")
            await asyncio.sleep(60)


def get_eth_price_usd() -> float:
    return _eth_price_cache["value"] or 3000.0


# ===================================================================
# 🚦 OpenSea Rate Limiter
# ===================================================================
class OpenSeaRateLimiter:
    def __init__(self, rate_per_second: float = 2.0):
        base = max(0.5, rate_per_second)
        self._base_rate = base
        self._rate = base
        self._tokens = base
        self._last_refill = time.monotonic()
        self._lock = asyncio.Lock()
        self._blocked_until = 0.0
        self._consecutive_429 = 0

    async def acquire(self):
        while True:
            async with self._lock:
                now = time.monotonic()
                wait = max(0.0, self._blocked_until - now)
                if wait <= 0:
                    elapsed = now - self._last_refill
                    self._tokens = min(self._rate, self._tokens + elapsed * self._rate)
                    self._last_refill = now
                    if self._tokens >= 1:
                        self._tokens -= 1
                        return
                    wait = (1 - self._tokens) / self._rate
            await asyncio.sleep(wait)

    def on_rate_limited(self, retry_after: int = 8):
        self._consecutive_429 += 1
        backoff = max(int(retry_after), min(60, 2 ** self._consecutive_429))
        self._blocked_until = time.monotonic() + backoff
        self._rate = max(0.5, self._base_rate / (2 ** min(self._consecutive_429, 3)))
        log.warning("[Rate Limiter] 429 #%d → انتظار %ds، المعدل %.2f req/s",
                    self._consecutive_429, backoff, self._rate)

    def on_success(self):
        if self._consecutive_429 > 0:
            self._consecutive_429 -= 1
            self._rate = min(self._base_rate, self._rate * 1.5)
            log.info("[Rate Limiter] تعافٍ → %.2f req/s", self._rate)


OPENSEA_RATE_PER_KEY = float(os.environ.get("OPENSEA_RATE_PER_KEY", "1.5"))
_opensea_limiter = OpenSeaRateLimiter(
    rate_per_second=max(1.0, OPENSEA_RATE_PER_KEY * max(1, len(OPENSEA_API_KEYS)))
)
log.info("⚡ OpenSea rate: %.1f req/s across %d key(s)",
         OPENSEA_RATE_PER_KEY * max(1, len(OPENSEA_API_KEYS)), len(OPENSEA_API_KEYS))


# ===================================================================
# 🌐 OpenSea API
# ===================================================================
async def fetch_drop_detail_async(session, slug) -> tuple[Optional[bool], Optional[dict]]:
    url = f"{DROPS_API_BASE}/{slug}"
    for attempt in range(3):
        api_key = await get_next_opensea_key()
        headers = {"x-api-key": api_key}
        await _opensea_limiter.acquire()
        try:
            async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=4 if TURBO_MODE else 8)) as resp:
                if resp.status == 200:
                    _opensea_limiter.on_success()
                    return True, await resp.json()
                if resp.status == 429:
                    try:
                        retry_after = max(1, int(resp.headers.get("Retry-After", "8")))
                    except ValueError:
                        retry_after = 8
                    _opensea_limiter.on_rate_limited(retry_after)
                    _opensea_key_pool.mark_rate_limited(api_key, retry_after)
                    continue
                if resp.status == 404:
                    return False, None
                if resp.status >= 500:
                    await asyncio.sleep(min(8, 0.5 * (2 ** attempt)))
                    continue
                return None, None
        except asyncio.TimeoutError:
            log.warning("[OpenSea API] Timeout أثناء جلب %s (attempt=%d/3, key=%s)",
                        slug, attempt + 1, "loaded")
            await asyncio.sleep(min(8, 0.5 * (2 ** attempt)))
        except aiohttp.ClientError as e:
            log.warning("[OpenSea API] %s: %s (type=%s, attempt=%d/3)",
                        slug, repr(e), type(e).__name__, attempt + 1)
            await asyncio.sleep(min(8, 0.5 * (2 ** attempt)))
    return None, None


def parse_iso(ts: str) -> Optional[datetime]:
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except Exception:
        return None


def stage_is_open_now(stage: dict, now: Optional[datetime] = None) -> bool:
    now = now or datetime.now(timezone.utc)
    start = parse_iso(stage.get("start_time", ""))
    end = parse_iso(stage.get("end_time", ""))
    if start and now < start.astimezone(timezone.utc):
        return False
    if end and now >= end.astimezone(timezone.utc):
        return False
    return True


def get_free_stage_to_mint(detail: dict, eth_price_usd: float) -> tuple[Optional[dict], Optional[datetime]]:
    stages = list(detail.get("stages") or [])
    active = detail.get("active_stage")
    if active and active not in stages:
        stages.insert(0, active)

    free_stages = []
    now = datetime.now(timezone.utc)
    for stage in stages:
        try:
            price_wei = int(stage.get("price", "0") or "0")
        except (TypeError, ValueError):
            continue
        if not is_free_or_negligible(price_wei, eth_price_usd):
            continue
        if not is_public_eligible_stage(stage):
            stage_label = stage.get("name") or stage.get("stage_name") or stage.get("label") or "unknown"
            reason = stage_rejection_reason(stage) or "unknown"
            log.info("⛔ تجاهل مرحلة غير عامة/غير مؤهلة: %s | السبب=%s", stage_label, reason)
            continue
        start = parse_iso(stage.get("start_time", ""))
        if stage_is_open_now(stage, now):
            free_stages.append((start or datetime.min.replace(tzinfo=timezone.utc), stage))

    if free_stages:
        free_stages.sort(key=lambda item: item[0])
        return free_stages[0][1], None

    if stages:
        labels = []
        for stage in stages:
            label = stage.get("name") or stage.get("stage_name") or stage.get("label") or "unknown"
            reason = stage_rejection_reason(stage) or "not_open_or_not_free"
            labels.append(f"{label}:{reason}")
        log.info("⏭️ لا توجد Public مجانية مفتوحة حاليًا | %s", " ; ".join(labels[:8]))

    future = []
    for stage in stages:
        try:
            price_wei = int(stage.get("price", "0") or "0")
        except (TypeError, ValueError):
            continue
        if is_free_or_negligible(price_wei, eth_price_usd) and is_public_eligible_stage(stage):
            start = parse_iso(stage.get("start_time", ""))
            if start and start > now:
                future.append((start, stage))
    if future:
        future.sort(key=lambda item: item[0])
        return None, future[0][0]
    return None, None


# ===================================================================
# 📨 Telegram
# ===================================================================
send_queue: asyncio.Queue = asyncio.Queue()


def enqueue_message(bot_token: str, chat_id: str, text: str, source: str = ""):
    tok, cid = _resolve_telegram_target(bot_token, chat_id)
    if not tok or not cid:
        log.debug("⏭️ [TG-DROP] تجاهل إشعار (%s): لا توجد وجهة صالحة", source or "general")
        return
    log.info("📨 [TG-QUEUE] %s → chat_id=%s", source or "general", cid)
    send_queue.put_nowait({"bot_token": tok, "chat_id": cid, "text": text})


def broadcast_message(text: str, source: str = "broadcast"):
    targets: set[tuple[str, str]] = set()
    for w in WALLETS_DATA:
        tok, cid = _resolve_telegram_target(w.get("bot_token", ""), w.get("chat_id", ""))
        if tok and cid:
            targets.add((tok, cid))
    dt, dc = _resolve_telegram_target("", "")
    if dt and dc:
        targets.add((dt, dc))
    if not targets:
        log.warning("⚠️ لا توجد وجهة Telegram صالحة.")
        return
    log.info("📨 [%s] Queuing for %d target(s)", source, len(targets))
    for tok, cid in targets:
        enqueue_message(tok, cid, text, source=source)


async def telegram_sender():
    async with http_session() as session:
        while True:
            msg = await send_queue.get()
            try:
                api = f"https://api.telegram.org/bot{msg['bot_token']}/sendMessage"
                payload = {
                    "chat_id": msg["chat_id"], "text": msg["text"],
                    "parse_mode": "HTML", "disable_web_page_preview": True,
                }
                async with session.post(api, data=payload, timeout=aiohttp.ClientTimeout(total=8)) as resp:
                    body = await resp.json(content_type=None)
                    if resp.status == 200 and body.get("ok"):
                        log.info("✅ Telegram sent")
                    elif resp.status == 400:
                        log.warning("⚠️ Telegram 400 — إعادة بدون HTML")
                        fb = {"chat_id": msg["chat_id"], "text": msg["text"]}
                        async with session.post(api, data=fb, timeout=aiohttp.ClientTimeout(total=8)) as resp2:
                            b2 = await resp2.json(content_type=None)
                            if resp2.status == 200 and b2.get("ok"):
                                log.info("✅ Telegram sent (fallback)")
                            else:
                                log.error("❌ Telegram فشل: %s", b2.get("description", b2))
                    elif resp.status == 404:
                        log.warning("⚠️ Telegram 404 — chat_id غير صالح: %s", msg["chat_id"])
                    else:
                        log.error("❌ Telegram: HTTP %s | %s", resp.status, body.get("description", body))
            except Exception as e:
                log.error("خطأ Telegram: %s", e)
            finally:
                send_queue.task_done()
            await asyncio.sleep(0.08)


def attempt_mode_label(result: dict) -> str:
    mode = result.get("attempt_mode", "")
    if mode in {"fast", "fast_broadcast"}:
        return "⚡ المحاولة الخاطفة (بدون فحوص ما قبل الإرسال)"
    if mode == "normal_fallback":
        return "↩️ المسار العادي بعد فشل المسار السريع"
    if mode == "normal_checked":
        return "🛡️ المحاولة العادية (بعد فحوص الملكية والرصيد والغاز)"
    if mode == "race":
        return "🏁 Race"
    if mode == "retry_checked":
        return "🔁 إعادة محاولة عادية (بعد الفحوصات)"
    return "🛡️ المسار العادي"


def build_success_msg(detail: dict, result: dict, chain_key: str, wallet_name: str) -> str:
    name = esc(detail.get("collection_name") or detail.get("collection_slug") or "Unknown")
    url = detail.get("opensea_url", "") or ""
    chain_label = esc(CHAINS_CONFIG.get(chain_key, {}).get("chain_name_display", chain_key.upper()))
    wallet_addr = result.get("wallet", "") or ""
    w_short = esc(wallet_addr[:6] + "..." + wallet_addr[-4:]) if len(wallet_addr) >= 10 else esc(wallet_addr)
    tx_hash = result.get("tx_hash", "") or ""
    tmpl = CHAINS_CONFIG.get(chain_key, {}).get("explorer_url", "")
    tx_url = tmpl.format(tx=tx_hash) if tmpl and tx_hash else ""
    tx_link = f'<a href="{esc(tx_url)}">فتح المعاملة</a>' if tx_url else f"<code>{esc(tx_hash)}</code>"
    tx_hash_line = f"🔑 <code>{esc(tx_hash)}</code>" if tx_hash else ""
    gas_fee = float(result.get("gas_fee_usd", 0) or 0)
    quantity = int(result.get("quantity", 1) or 1)
    url_line = f'🔗 <a href="{esc(url)}">{esc(url)}</a>' if url else ""
    return (
        f"✅ <b>تم الشراء بنجاح!</b> ({chain_label})\n\n"
        f"👤 المحفظة: <b>{esc(wallet_name)}</b> (<code>{w_short}</code>)\n"
        f"🎨 المجموعة: <b>{name}</b>\n"
        f"🧭 طريقة الشراء: <b>{attempt_mode_label(result)}</b>\n"
        f"🔢 الكمية: <b>{quantity}</b>\n"
        f"⛽ رسوم الغاز: <b>${gas_fee:.4f}</b>\n"
        f"🧾 المعاملة: {tx_link}\n{tx_hash_line}\n{url_line}"
    ).strip()


def build_fail_msg(detail: dict, result: dict, chain_key: str, wallet_name: str) -> str:
    name = esc(detail.get("collection_name") or detail.get("collection_slug") or "Unknown")
    chain_label = esc(CHAINS_CONFIG.get(chain_key, {}).get("chain_name_display", chain_key.upper()))
    reason = esc(result.get("reason", "unknown"))
    err = esc((result.get("error", "") or "")[:200])
    err_line = f"\n📋 تفاصيل: <code>{err}</code>" if err else ""
    return (
        f"⏭️ <b>تعذر الشراء</b> ({chain_label})\n\n"
        f"👤 المحفظة: <b>{esc(wallet_name)}</b>\n"
        f"🎨 المجموعة: <b>{name}</b>\n"
        f"🧭 طريقة المحاولة: <b>{attempt_mode_label(result)}</b>\n"
        f"⚠️ السبب: <code>{reason}</code>{err_line}"
    )


# ===================================================================
# 💼 تنفيذ الشراء (الوضع الكلاسيكي)
# ===================================================================
async def execute_wallet_task(
    w3, item: dict, slug: str, chain_key: str, contract_address: str,
    seadrop_address: str, price_wei: int, max_per_wallet: Optional[int],
    remaining: int, eth_price_usd: float, detail: dict
) -> dict:
    wallet_addr = item["address"]
    pk = item["private_key"]
    bot_tok = item["bot_token"]
    chat_id = item["chat_id"]
    w_name = item["name"]

    pending_key = f"{chain_key}:{slug}:{wallet_addr.lower()}"
    if pending_key in pending_transactions:
        return {"success": False, "wallet": wallet_addr, "reason": "tx_pending",
                "tx_hash": pending_transactions[pending_key]["tx_hash"]}
    if (
        wallet_addr in successful_mints.get(drop_key(chain_key, slug), set())
        or mint_history.has(chain_key, slug, wallet_addr)
    ):
        successful_mints.setdefault(drop_key(chain_key, slug), set()).add(wallet_addr)
        return {"success": False, "wallet": wallet_addr, "reason": "already_bought"}

    if not reserve_wallet_operation(chain_key, slug, wallet_addr):
        return {"success": False, "wallet": wallet_addr, "reason": "operation_in_flight"}

    opensea_key = await get_next_opensea_key()
    execution_t0 = time.perf_counter()
    async with semaphore:
        result = await asyncio.to_thread(
            execute_purchase_pipeline,
            w3, pk, wallet_addr, contract_address, seadrop_address,
            price_wei, max_per_wallet, remaining, eth_price_usd,
            ENABLE_UNIVERSAL_MINTS, slug, opensea_key
        )
    result["wallet"] = wallet_addr
    result["wallet_name"] = w_name
    result["latency_ms"] = round((time.perf_counter() - execution_t0) * 1000, 1)

    if result.get("success"):
        if drop_key(chain_key, slug) not in successful_mints:
            successful_mints[drop_key(chain_key, slug)] = set()
        successful_mints[drop_key(chain_key, slug)].add(wallet_addr)
        mint_history.record(
            chain_key, slug, wallet_addr, result.get("tx_hash", ""),
            result.get("block_number", 0), result.get("quantity", 1),
        )
        enqueue_message(bot_tok, chat_id, build_success_msg(detail, result, chain_key, w_name),
                        source=f"success:{slug}")
    else:
        reason = result.get("reason", "")
        if reason == "tx_pending" and result.get("tx_hash"):
            pending_transactions[pending_key] = {
                "tx_hash": result["tx_hash"], "chain_key": chain_key,
                "slug": slug, "wallet": wallet_addr,
                "quantity": result.get("quantity", 1),
                "detail": detail, "wallet_name": w_name,
                "bot_token": bot_tok, "chat_id": chat_id,
                "attempt_mode": result.get("attempt_mode", "normal_checked"),
                "gas_fee_usd": result.get("gas_fee_usd", 0),
            }
        failure_key = f"{chain_key}:{slug}:{wallet_addr.lower()}:{reason}"
        if reason in ("already_minted", "already_bought"):
            terminal_wallet_failures.add(f"{chain_key}:{slug}:{wallet_addr.lower()}")
        if (
            TELEGRAM_NOTIFY_FAILURES
            and reason not in ("not_free_mint", "paid_mint", "already_bought")
            and failure_key not in failure_notifications_sent
        ):
            failure_notifications_sent.add(failure_key)
            enqueue_message(
                bot_tok, chat_id, build_fail_msg(detail, result, chain_key, w_name),
                source=f"fail:{slug}:{reason}",
            )
        if reason in RETRYABLE_REASONS:
            async with retry_lock:
                key = f"{slug}:{wallet_addr}:{chain_key}"
                if key not in retry_tasks:
                    tracker = RetryTracker(
                        slug=slug, wallet_address=wallet_addr, chain_name=chain_key,
                        detail=detail, wallet_name=w_name, wallet_private_key=pk,
                        price_wei=price_wei, max_per_wallet=max_per_wallet,
                        remaining_supply=remaining, eth_price_usd=eth_price_usd,
                        bot_token=bot_tok, chat_id=chat_id, original_reason=reason,
                    )
                    retry_tasks[key] = tracker
                    task = asyncio.create_task(schedule_single_retry(tracker))
                    task.add_done_callback(lambda _task, retry_key=key: retry_tasks.pop(retry_key, None))
    if result.get("reason") != "tx_pending":
        release_wallet_operation(chain_key, slug, wallet_addr)
    return result


async def schedule_single_retry(tracker: RetryTracker):
    # حماية افتراضية: لا نعيد بث mint تلقائيًا بعد فشل غير محسوم.
    # يمكن تفعيل retries صراحةً بعد التأكد من مراقبة pending_transactions.
    if MAX_RETRY_ATTEMPTS <= 0:
        log.warning("🛑 Retry disabled for '%s' (MAX_RETRY_ATTEMPTS=0)", tracker.slug)
        async with retry_lock:
            retry_tasks.pop(f"{tracker.slug}:{tracker.wallet_address}:{tracker.chain_name}", None)
        return
    wait_time = 15 if "gas" in tracker.original_reason else 60
    max_tries = MAX_RETRY_ATTEMPTS
    while not tracker.is_expired and tracker.attempt_count <= max_tries:
        await asyncio.sleep(wait_time)
        tracker.attempt_count += 1
        w3 = W3_INSTANCES.get(tracker.chain_name)
        if not w3:
            break
        seadrop_address = CHAINS_CONFIG[tracker.chain_name]["seadrop_address"]
        contract_address = (
            tracker.detail.get("contract_address")
            or (tracker.detail.get("contracts") or [{}])[0].get("address")
        )
        if not contract_address:
            break
        # إعادة التحقق أيضاً داخل retry؛ السعر المخزن في tracker قديم.
        drop_info = await asyncio.to_thread(get_drop_info, w3, contract_address, seadrop_address)
        if not drop_info:
            break
        retry_price_wei = int(drop_info.get("price_wei", 0) or 0)
        if not is_free_or_negligible(retry_price_wei, tracker.eth_price_usd):
            log.warning("🛑 إلغاء retry لـ '%s': السعر on-chain أصبح مدفوعاً", tracker.slug)
            break
        is_open, _status = is_mint_open(drop_info, early_submit_seconds=0.5)
        if not is_open:
            continue
        res = await asyncio.to_thread(
            execute_purchase_pipeline,
            w3, tracker.wallet_private_key, tracker.wallet_address,
            contract_address, seadrop_address, retry_price_wei,
            tracker.max_per_wallet, tracker.remaining_supply, tracker.eth_price_usd,
            ENABLE_UNIVERSAL_MINTS,
        )
        if res.get("success"):
            res.setdefault("attempt_mode", "retry_checked")
            successful_mints.setdefault(drop_key(tracker.chain_name, tracker.slug), set()).add(tracker.wallet_address)
            mint_history.record(
                tracker.chain_name, tracker.slug, tracker.wallet_address,
                res.get("tx_hash", ""), res.get("block_number", 0),
                res.get("quantity", 1),
            )
            enqueue_message(tracker.bot_token, tracker.chat_id,
                            build_success_msg(tracker.detail, res, tracker.chain_name, tracker.wallet_name),
                            source=f"retry-success:{tracker.slug}")
            break
        if res.get("reason", "") not in RETRYABLE_REASONS:
            break
    async with retry_lock:
        retry_tasks.pop(f"{tracker.slug}:{tracker.wallet_address}:{tracker.chain_name}", None)


# ===================================================================
# 🎯 التقييم
# ===================================================================
async def evaluate_and_execute_mint(session: aiohttp.ClientSession, slug: str, chain_key: str):
    # فحوصات ما قبل المعالجة (مع cooldown للفشل)
    if (
        len(successful_mints.get(drop_key(chain_key, slug), set())) >= len(WALLETS_DATA) > 0
        or drop_key(chain_key, slug) in in_flight
        or time.time() - rejected_cooldown.get(drop_key(chain_key, slug), 0) < REJECTION_COOLDOWN_SECONDS
        or time.time() - failed_attempts.get(drop_key(chain_key, slug), 0) < FAILED_COOLDOWN_SECONDS
    ):
        return

    in_flight.add(drop_key(chain_key, slug))
    try:
        found, detail = await fetch_drop_detail_async(session, slug)
        if not found or not detail:
            return
        if not detail.get("is_minting") and not (detail.get("stages") or detail.get("active_stage")):
            return

        native_price_usd = float(CHAINS_CONFIG.get(chain_key, {}).get("native_price_usd", get_eth_price_usd()))
        stage, next_free_start = get_free_stage_to_mint(detail, native_price_usd)
        if not stage:
            if next_free_start:
                if not SCHEDULE_FUTURE_STAGES:
                    log.debug(
                        "⏭️ '%s': Public مستقبلية، الانتظار حتى يكتشفها الماسح وهي مفتوحة",
                        slug,
                    )
                    return
                when_local = next_free_start.astimezone(LOCAL_TZ)
                notify_key = f"{chain_key}:{slug}:{next_free_start.isoformat()}"
                already_scheduled = notify_key in notified_scheduled_drops

                # لا تُسجّل ولا تُرسل إلا إذا لم يُسبق جدولته
                if not already_scheduled:
                    log.info("⏳ '%s': جدولة السحب في %s", slug, when_local.isoformat())

                if NOTIFY_SCHEDULED and not already_scheduled:
                    notified_scheduled_drops.add(notify_key)
                    chain_label = CHAINS_CONFIG.get(chain_key, {}).get("chain_name_display", chain_key)
                    collection_name = detail.get("collection_name") or slug
                    remaining_delta = next_free_start - datetime.now(timezone.utc)
                    mode_label = "🏁 Race Mode" if RACE_MODE else "🕐 عادي"
                    broadcast_message(
                        f"⏳ <b>رصد مينت مجاني مجدول</b> ({mode_label})\n\n"
                        f"🎨 المجموعة: <b>{esc(collection_name)}</b>\n"
                        f"📡 الشبكة: {esc(chain_label)}\n"
                        f"🕒 وقت الفتح: <b>{when_local.strftime('%Y-%m-%d %H:%M:%S')} (+03:00)</b>\n"
                        f"⏱️ متبقٍ: {esc(str(remaining_delta).split('.')[0])}",
                        source=f"scheduled:{slug}",
                    )

                # تجهيز بيانات السباق
                contract_address = (
                    detail.get("contract_address")
                    or (detail.get("contracts") or [{}])[0].get("address")
                )
                if contract_address:
                    register_mint_contract(chain_key, slug, contract_address)
                    max_supply = int(detail.get("max_supply") or 0)
                    total_supply = int(detail.get("total_supply") or 0)
                    remaining = 999_999 if max_supply <= 0 else max_supply - total_supply
                    max_per_wallet_raw = (
                        stage.get("max_total_mintable_by_wallet")
                        or stage.get("max_per_wallet")
                    ) if stage else None
                    max_per_wallet = int(max_per_wallet_raw) if max_per_wallet_raw is not None else None
                    price_wei_stage = int(stage.get("price", "0") or "0") if stage else 0

                    race = ScheduledRace(
                        slug=slug, chain_key=chain_key,
                        start_time=next_free_start,
                        contract_address=contract_address,
                        seadrop_address=CHAINS_CONFIG[chain_key]["seadrop_address"],
                        price_wei=price_wei_stage,
                        max_per_wallet=max_per_wallet,
                        remaining=remaining,
                        native_price_usd=native_price_usd,
                        detail=detail,
                    )
                    race_registry[f"{chain_key}:{slug}"] = race
                    await schedule_stage_open(session, slug, chain_key, next_free_start, race)
                else:
                    await schedule_stage_open(session, slug, chain_key, next_free_start)
            return

        # المرحلة مفتوحة الآن
        price_wei = int(stage.get("price", "0") or "0")
        w3 = W3_INSTANCES.get(chain_key)
        if not w3:
            return

        contract_address = (
            detail.get("contract_address")
            or (detail.get("contracts") or [{}])[0].get("address")
        )
        if not contract_address:
            return
        register_mint_contract(chain_key, slug, contract_address)

        if VERIFY_ONCHAIN_PRICE:
            onchain_price = await asyncio.to_thread(
                get_onchain_public_price_wei, w3, contract_address,
                CHAINS_CONFIG[chain_key]["seadrop_address"]
            )
            if onchain_price is not None:
                price_wei = onchain_price

        if not is_free_or_negligible(price_wei, native_price_usd):
            rejected_cooldown[drop_key(chain_key, slug)] = time.time()
            return

        if CHECK_TWITTER:
            # المرحلة الأولى: جلب الحساب المرتبط من OpenSea Collections API.
            twitter_api_key = await get_next_opensea_key()
            tw_user = await get_twitter_username_from_opensea_async(
                session, slug, twitter_api_key
            )
            # المرحلة الثانية: التحقق عبر X API إذا تم توفير Bearer Token.
            twitter_valid = bool(tw_user) and await is_valid_twitter_account_async(
                session, tw_user
            )
            if not twitter_valid:
                rejected_cooldown[drop_key(chain_key, slug)] = time.time()
                log.info("⛔ رفض '%s': لا يوجد حساب X صالح أو موثق", slug)
                return

        max_supply = int(detail.get("max_supply") or 0)
        total_supply = int(detail.get("total_supply") or 0)
        remaining = 999_999 if max_supply <= 0 else max_supply - total_supply
        if remaining <= 0:
            return

        max_per_wallet_raw = stage.get("max_total_mintable_by_wallet") or stage.get("max_per_wallet")
        max_per_wallet = int(max_per_wallet_raw) if max_per_wallet_raw is not None else None

        seadrop_address = CHAINS_CONFIG[chain_key]["seadrop_address"]
        already_bought = successful_mints.get(slug, set())
        pending_wallets = [
            w for w in WALLETS_DATA
            if w["address"] not in already_bought
            and not mint_history.has(chain_key, slug, w["address"])
            and f"{chain_key}:{slug}:{w['address'].lower()}" not in terminal_wallet_failures
            and f"{chain_key}:{slug}:{w['address'].lower()}" not in pending_transactions
        ]
        if not pending_wallets:
            return

        log.info("⚡ شراء فوري '%s' على %s — %d محفظة", slug, chain_key, len(pending_wallets))

        tasks = [
            execute_wallet_task(
                w3, w, slug, chain_key, contract_address, seadrop_address,
                price_wei, max_per_wallet, remaining, native_price_usd, detail,
            )
            for w in pending_wallets
        ]
        await asyncio.gather(*tasks, return_exceptions=True)

    except Exception as e:
        log.error(f"خطأ تقييم '{slug}': {e}")
    finally:
        in_flight.discard(drop_key(chain_key, slug))
        # تسجيل محاولة فاشلة إذا لم تنجح أي محفظة
        if not successful_mints.get(drop_key(chain_key, slug)):
            failed_attempts[drop_key(chain_key, slug)] = time.time()


# ===================================================================
# 🔌 WebSocket
# ===================================================================
WS_SEND_TIMEOUT = 10
WS_MAX_QUEUE = 2048


async def _send_ws_payload(ws, payload: list):
    """إرسال رسالة WS بمهلة حتى لا يعلق الاتصال عند ضغط OpenSea."""
    message = json.dumps(payload, separators=(",", ":"))
    await asyncio.wait_for(ws.send(message), timeout=WS_SEND_TIMEOUT)


async def listen_opensea():
    msg_ref = 0
    async with http_session() as session:
        while True:
            try:
                stream_key = await get_next_opensea_key()
                async with websockets.connect(
                    STREAM_URL_BASE.format(token=stream_key),
                    # keepalive بروتوكولي + طابور قراءة أكبر لمنع backpressure
                    ping_interval=20,
                    ping_timeout=20,
                    max_queue=WS_MAX_QUEUE,
                    max_size=None,
                    read_limit=2 ** 20,
                    write_limit=2 ** 20,
                    open_timeout=15,
                    close_timeout=10,
                ) as ws:
                    log.info(f"🚀 متصل بـ OpenSea Stream — {', '.join(ENABLED_CHAINS)}")
                    join_ref = str(msg_ref)
                    await _send_ws_payload(ws, [join_ref, join_ref, "collection:*", "phx_join", {"event_types": ["item_transferred"]}])
                    msg_ref += 1
                    last_hb = time.time()

                    while True:
                        if time.time() - last_hb > HEARTBEAT_INTERVAL:
                            hb = str(msg_ref)
                            await _send_ws_payload(ws, [None, hb, "phoenix", "heartbeat", {}])
                            msg_ref += 1
                            last_hb = time.time()
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=RECV_TIMEOUT)
                        except asyncio.TimeoutError:
                            continue
                        try:
                            parsed = json.loads(raw)
                        except json.JSONDecodeError:
                            continue
                        if not (isinstance(parsed, list) and len(parsed) == 5):
                            continue
                        _j, _r, _t, event_name, pw = parsed
                        if event_name != "item_transferred":
                            continue
                        payload = (pw or {}).get("payload") or {}
                        item = payload.get("item", {}) or {}
                        sc = (item.get("chain", {}) or {}).get("name", "").lower()
                        chain_key = STREAM_NAME_TO_CHAIN_KEY.get(sc)
                        if not chain_key or chain_key not in ENABLED_CHAINS:
                            continue
                        fa = ((payload.get("from_account") or {}).get("address", "") or "").lower()
                        if fa != ZERO_ADDRESS:
                            continue
                        slug = (payload.get("collection", {}) or {}).get("slug", "")
                        if not slug:
                            continue
                        schedule_evaluation(session, slug, chain_key)
            except (websockets.ConnectionClosed, OSError, asyncio.TimeoutError) as e:
                log.warning(f"⚠️ WS انقطع ({e}). إعادة خلال 3s...")
                await asyncio.sleep(3)
            except Exception as e:
                log.error(f"خطأ WS: {e}")
                await asyncio.sleep(5)


# ===================================================================
# 🔎 الماسح الدوري
# ===================================================================
async def fast_active_scanner():
    async with http_session() as session:
        # لا نبدّل إلى صفحات عامة أثناء البحث: الأولوية دائماً للمينتات النشطة.
        while True:
            try:
                url = f"{DROPS_API_BASE}?is_minting=true&limit=50"
                await _opensea_limiter.acquire()
                page = []
                scan_api_key = await get_next_opensea_key()
                try:
                    async with session.get(
                        url, headers={"x-api-key": scan_api_key},
                        timeout=aiohttp.ClientTimeout(total=5 if TURBO_MODE else 8),
                    ) as resp:
                        if resp.status == 200:
                            _opensea_limiter.on_success()
                            data = await resp.json()
                            page = data.get("drops") or data.get("results", [])
                        elif resp.status == 429:
                            retry_after = int(resp.headers.get("Retry-After", "8"))
                            _opensea_limiter.on_rate_limited(retry_after)
                            _opensea_key_pool.mark_rate_limited(scan_api_key, retry_after)
                except Exception as e:
                    log.warning(f"[الماسح] {e}")

                seen: set[tuple[str, str]] = set()
                for d in page:
                    slug = d.get("collection_slug") or d.get("slug", "")
                    chain_name = (d.get("chain") or "").lower()
                    chain_key = STREAM_NAME_TO_CHAIN_KEY.get(chain_name)
                    key = (slug, chain_key)
                    if slug and chain_key in ENABLED_CHAINS and key not in seen:
                        seen.add(key)
                        schedule_evaluation(session, slug, chain_key)
            except Exception as e:
                log.error(f"خطأ الماسح: {e}")
            await asyncio.sleep(SCAN_INTERVAL)


# ===================================================================
# 🧪 اختبار Telegram
# ===================================================================
async def telegram_startup_self_test():
    tt, tc = _resolve_telegram_target("", "")
    if (not tt or not tc) and WALLETS_DATA:
        tt, tc = _resolve_telegram_target(
            WALLETS_DATA[0].get("bot_token", ""),
            WALLETS_DATA[0].get("chat_id", ""),
        )
    if not tt or not tc:
        log.error("🔴 لا يمكن اختبار Telegram: بيانات مفقودة — %s", _telegram_config_hint())
        return
    try:
        async with http_session() as s:
            async with s.post(
                f"https://api.telegram.org/bot{tt}/sendMessage",
                data={"chat_id": tc, "text": "🧪 اختبار الإقلاع: البوت متصل."},
                timeout=aiohttp.ClientTimeout(total=8),
            ) as r:
                b = await r.json(content_type=None)
                if r.status == 200 and b.get("ok"):
                    log.info("✅ Telegram self-test succeeded")
                else:
                    log.error("❌ Telegram self-test: HTTP %s | %s", r.status, b.get("description", b))
    except Exception as e:
        log.error("❌ Telegram self-test exception: %s", e)


# ===================================================================
# 🔄 مصالحة المعاملات المعلّقة
# ===================================================================
async def reconcile_pending_transactions():
    while True:
        for key, item in list(pending_transactions.items()):
            w3 = W3_INSTANCES.get(item["chain_key"])
            if not w3:
                continue
            candidates = [w3]
            try:
                fallback = getattr(getattr(w3, "provider", None), "endpoint_uri", "") or ""
                alternate = await asyncio.to_thread(
                    get_web3_rotating, item["chain_key"], fallback
                )
                if alternate is not None and alternate is not w3:
                    candidates.append(alternate)
            except Exception as exc:
                log.debug("[Pending] تعذر تجهيز RPC بديل: %s", exc)
            receipt = None
            for candidate in candidates:
                try:
                    receipt = await asyncio.to_thread(
                        candidate.eth.get_transaction_receipt, item["tx_hash"]
                    )
                    W3_INSTANCES[item["chain_key"]] = candidate
                    break
                except Exception:
                    continue
            if receipt is None:
                continue
            try:
                status = int(receipt.get("status", 0))
                if status == 1:
                    drop_id = drop_key(item["chain_key"], item["slug"])
                    successful_mints.setdefault(drop_id, set()).add(item["wallet"])
                    confirmed_result = {
                        "success": True, "wallet": item["wallet"],
                        "tx_hash": item["tx_hash"],
                        "block_number": int(receipt.get("blockNumber", 0) or 0),
                        "quantity": int(item.get("quantity", 1) or 1),
                        "gas_fee_usd": float(item.get("gas_fee_usd", 0) or 0),
                        "attempt_mode": item.get("attempt_mode", "confirmed"),
                    }
                    mint_history.record(
                        item["chain_key"], item["slug"], item["wallet"],
                        item["tx_hash"], confirmed_result["block_number"],
                        confirmed_result["quantity"],
                    )
                    enqueue_message(
                        item.get("bot_token", ""), item.get("chat_id", ""),
                        build_success_msg(
                            item.get("detail", {}), confirmed_result,
                            item["chain_key"], item.get("wallet_name", "المحفظة"),
                        ),
                        source=f"confirmed-success:{item['slug']}",
                    )
                    pending_transactions.pop(key, None)
                    release_wallet_operation(item["chain_key"], item["slug"], item["wallet"])
                    log.info("✅ تمت مصالحة المعاملة المعلّقة وإرسال إشعار النجاح: %s", item["tx_hash"])
                elif status == 0:
                    pending_transactions.pop(key, None)
                    release_wallet_operation(item["chain_key"], item["slug"], item["wallet"])
                    log.warning("⚠️ فشلت المعاملة المعلّقة؛ سيسمح بإعادة المحاولة: %s", item["tx_hash"])
            except Exception:
                # لا نحذف السجل عند فشل RPC؛ الإرسال السابق قد يكون ما زال صالحًا.
                continue
        await asyncio.sleep(5)


# ===================================================================
# 🚀 التشغيل
# ===================================================================
async def run():
    default_tok = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
    default_cid = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
    if _is_placeholder(default_tok):
        default_tok = ""
    if _is_placeholder(default_cid):
        default_cid = ""
    wallet_targets = sum(
        1 for w in WALLETS_DATA
        if all(_resolve_telegram_target(w.get("bot_token", ""), w.get("chat_id", "")))
    )
    log.info("📨 Telegram config: default=%s, wallet_targets=%d",
             "OK" if (default_tok and default_cid) else "MISSING", wallet_targets)

    await telegram_startup_self_test()

    if not BOT_ENABLED:
        log.warning("🔴 BOT_ENABLED=false — وضع الأمان")
        broadcast_message("🔴 البوت بوضع الأمان (BOT_ENABLED=false).", source="startup")
        await asyncio.sleep(0.5)
        await asyncio.gather(telegram_sender())
        return

    if not WALLETS_DATA:
        log.critical("🔴 لا توجد محافظ!")
        broadcast_message("🔴 البوت لم يبدأ: لا توجد محافظ.", source="startup")
        await asyncio.sleep(0.5)
        await asyncio.gather(telegram_sender())
        return

    from eth_account import Account
    valid_wallets = []
    for wallet in WALLETS_DATA:
        try:
            derived = Account.from_key(wallet["private_key"]).address.lower()
            if derived != wallet["address"].lower():
                log.error("[محفظة] %s: العنوان لا يطابق المفتاح", wallet["name"])
                continue
            valid_wallets.append(wallet)
        except Exception as e:
            log.error("[محفظة] %s: مفتاح غير صالح: %s", wallet["name"], e)

    if not valid_wallets:
        log.critical("🔴 لا محافظ صالحة")
        broadcast_message("🔴 البوت لم يبدأ: كل المحافظ غير صالحة.", source="startup")
        await asyncio.sleep(0.5)
        await asyncio.gather(telegram_sender())
        return

    if len(valid_wallets) != len(WALLETS_DATA):
        WALLETS_DATA[:] = valid_wallets

    chains_str = ", ".join([CHAINS_CONFIG[c]["chain_name_display"] for c in ENABLED_CHAINS]) or "لا شيء"
    if RACE_MODE and RACE_CASCADE_ENABLED:
        mode_str = "🏁 تنافسي (Race + Cascade)"
    elif RACE_MODE:
        mode_str = "🏁 تنافسي (Race)"
    else:
        mode_str = "🕐 عادي"
    welcome_msg = (
        f"🚀 <b>تم تشغيل نظام المينت الموحد!</b>\n\n"
        f"⚙️ الوضع: <b>{mode_str}</b>\n"
        f"📡 الشبكات: {esc(chains_str)}\n"
        f"👛 المحافظ: {len(WALLETS_DATA)}\n"
        f"🔑 مفاتيح OpenSea: {len(OPENSEA_API_KEYS)}\n"
        f"📨 إشعارات الجدولة: {'مفعّلة' if NOTIFY_SCHEDULED else 'معطّلة'}\n"
        f"📨 إشعارات الفشل: {'مفعّلة' if TELEGRAM_NOTIFY_FAILURES else 'معطّلة'}"
    )
    broadcast_message(welcome_msg, source="startup")
    log.info("🚀 Bot started | mode=%s | profile=%s | scan=%.2fs | concurrency=%d | chains=%s | wallets=%d",
             mode_str, PERFORMANCE_PROFILE, SCAN_INTERVAL, MAX_CONCURRENT_MINTS,
             chains_str, len(WALLETS_DATA))

    await asyncio.sleep(0.5)
    monitor_tasks = []
    if PENDING_TX_MONITOR:
        for chain_key in ENABLED_CHAINS:
            monitor = PendingTxMonitor(
                chain_key,
                WSS_URLS_BY_CHAIN.get(chain_key, []),
                lambda tx_hash, target, key=chain_key: on_pending_contract_tx(key, tx_hash, target),
                max_inflight=PENDING_TX_MAX_INFLIGHT,
            )
            PENDING_MONITORS[chain_key] = monitor
            monitor_tasks.append(monitor.run())
    else:
        log.info("🔕 Pending Blockchain monitor disabled; OpenSea-only mode")
    on_pending_contract_tx.session = http_session()
    try:
        await asyncio.gather(
            listen_opensea(),
            fast_active_scanner(),
            telegram_sender(),
            eth_price_updater(),
            reconcile_pending_transactions(),
            *monitor_tasks,
        )
    finally:
        await on_pending_contract_tx.session.close()


def main():
    backoff = 2
    while True:
        try:
            asyncio.run(run())
        except KeyboardInterrupt:
            log.info("تم الإيقاف يدوياً.")
            break
        except Exception as e:
            log.critical(f"توقف غير متوقع: {e}. إعادة خلال {backoff}s...")
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)


if __name__ == "__main__":
    main()

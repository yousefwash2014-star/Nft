"""
النظام الكامل: اكتشاف مينت مجاني بدأ اليوم على Ethereum + Robinhood Chain،
التحقق من كل الضوابط عبر buyer.py، تنفيذ الشراء، وإرسال إشعار تيليجرام.

تم التطوير:
  - ⚡ معالجة متوازية (asyncio.Semaphore) للتعامل مع عدة مجموعات في نفس الوقت
  - ⛓️ دعم سلاسل متعددة (Ethereum + Robinhood) مع معالجة متوازية
  - 👛 دعم 5 محافظ مع معالجة متوازية (كل محافظة على كل سلسلة)
  - 🚀 استخدام aiohttp لطلبات API أسرع من requests التقليدية
  - 💰 تحديث سعر ETH كل 60 ثانية بدلاً من 300 (تحديث أسرع)
  - 🔄 نظام إعادة محاولة ذكي: يعيد المحاولة كل ساعة لمدة 8 ساعات عند الفشل
  - 📊 تقارير محسّنة: توضح نوع المينت (مدفوع/مجاني) وحالة الأهلية
"""

import asyncio
import json
import logging
import os
import time
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass, field

import aiohttp
import websockets
from dotenv import load_dotenv

from buyer import (
    get_web3_from_config,
    attempt_purchase,
    CHAINS_CONFIG,
    quick_checks,
    check_eligibility_reason,
    is_paid_mint,
    RETRYABLE_REASONS,
    MIN_BALANCE_RESERVE_USD,
    MAX_GAS_FEE_USD,
    get_wallet_balance_usd,
)

log = logging.getLogger(__name__)
load_dotenv()

# ===================================================================
# المتغيرات العامة
# ===================================================================
OPENSEA_API_KEY = os.environ["OPENSEA_API_KEY"].strip()
TELEGRAM_BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"].strip()
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"].strip()
BOT_ENABLED = os.environ.get("BOT_ENABLED", "false").strip().lower() == "true"

# ===================================================================
# إعداد المحافظ المتعددة
# ===================================================================
def load_wallets():
    wallets = []
    
    wallet_configs = [
        ("السمبتيك 1", "PRIVATE_KEY", "WALLET_ADDRESS"),
        ("علاء", "WALLET_2_PRIVATE_KEY", "WALLET_2_ADDRESS"),
        ("مبروك 3", "WALLET_3_PRIVATE_KEY", "WALLET_3_ADDRESS"),
        ("جواد 4", "WALLET_4_PRIVATE_KEY", "WALLET_4_ADDRESS"),
        ("ايهاب 5", "WALLET_5_PRIVATE_KEY", "WALLET_5_ADDRESS"),
    ]
    
    for name, pk_var, addr_var in wallet_configs:
        private_key = os.environ.get(pk_var)
        address = os.environ.get(addr_var)
        if private_key and address:
            wallets.append({
                "name": name,
                "private_key": private_key,
                "address": address,
            })
            log.info(f"✅ {name}: {address[:10]}... تم التحميل")
    
    return wallets

WALLETS = load_wallets()
if not WALLETS:
    logging.warning("⚠️ لم يتم العثور على أي محفظة!")

ROBINHOOD_RPC_URL = os.environ.get("ROBINHOOD_RPC_URL", "").strip()
ETHEREUM_RPC_URL = os.environ.get("ETHEREUM_RPC_URL", "").strip()

ENABLED_CHAINS = []
if ROBINHOOD_RPC_URL:
    CHAINS_CONFIG["robinhood"]["rpc_url"] = ROBINHOOD_RPC_URL
    ENABLED_CHAINS.append("robinhood")
if ETHEREUM_RPC_URL:
    CHAINS_CONFIG["ethereum"]["rpc_url"] = ETHEREUM_RPC_URL
    ENABLED_CHAINS.append("ethereum")

# ===================================================================
# ثوابت الاتصال
# ===================================================================
STREAM_URL = f"wss://stream.openseabeta.com/socket/websocket?token={OPENSEA_API_KEY}&vsn=2.0.0"
TELEGRAM_API = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"
DROPS_API_BASE = "https://api.opensea.io/api/v2/drops"
ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"

EXPLORER_URLS = {
    "ethereum": "https://etherscan.io/tx/",
    "robinhood": "https://explorer.robinhood.org/tx/",
}

LOCAL_TZ = timezone(timedelta(hours=3))

# ===================================================================
# إعدادات الأداء والأمان
# ===================================================================
MAX_CONCURRENT_MINTS = 3
HEARTBEAT_INTERVAL = 20
RECV_TIMEOUT = 5
FREE_PRICE_THRESHOLD_USD = 0.000000001
ETH_PRICE_CACHE_TTL = 60
SCAN_INTERVAL = 50

# ===================================================================
# إعدادات نظام إعادة المحاولة
# ===================================================================
MAX_RETRY_HOURS = 8
RETRY_INTERVAL = 1800
MAX_RETRIES_PER_TASK = MAX_RETRY_HOURS

GAS_RETRY_INTERVAL = 10
GAS_RETRY_MAX_ATTEMPTS = 10

PAID_MINT_RETRY_INTERVAL = 120
ELIGIBILITY_RETRY_INTERVAL = 10800

# ===================================================================
# كلاس تتبع المحاولات
# ===================================================================
@dataclass
class RetryTracker:
    slug: str
    wallet_address: str
    chain_name: str
    detail: dict
    wallet_name: str
    wallet_private_key: str
    price_wei: int
    max_per_wallet: int | None
    remaining_supply: int
    eth_price_usd: float
    start_time: float = field(default_factory=time.time)
    attempt_count: int = 1
    original_reason: str = ""
    
    @property
    def hours_passed(self) -> float:
        return (time.time() - self.start_time) / 3600
    
    @property
    def is_expired(self) -> bool:
        return self.hours_passed >= MAX_RETRY_HOURS
    
    @property
    def hours_remaining(self) -> float:
        remaining = MAX_RETRY_HOURS - self.hours_passed
        return max(0, remaining)
    
    @property
    def retry_key(self) -> str:
        return f"{self.slug}:{self.wallet_address}:{self.chain_name}"

retry_tasks: dict[str, RetryTracker] = {}
retry_lock = asyncio.Lock()

# ===================================================================
# إعدادات تسجيل الأحداث
# ===================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("auto-buyer-v2")

# ===================================================================
# إنشاء اتصالات Web3
# ===================================================================
w3_instances = {}
for chain_name in ENABLED_CHAINS:
    try:
        w3_instances[chain_name] = get_web3_from_config(CHAINS_CONFIG[chain_name])
        logging.info(f"✅ {CHAINS_CONFIG[chain_name]['chain_name_display']} - Web3 متصل")
    except Exception as e:
        logging.error(f"❌ {CHAINS_CONFIG[chain_name]['chain_name_display']} - فشل الاتصال: {e}")

semaphore = asyncio.Semaphore(MAX_CONCURRENT_MINTS)

# ===================================================================
# ذاكرة تخزين مؤقت لسعر ETH
# ===================================================================
_eth_price_cache = {"value": None, "ts": 0}

async def get_eth_price_usd(session: aiohttp.ClientSession) -> float:
    now = time.time()
    
    if _eth_price_cache["value"] and (now - _eth_price_cache["ts"] < ETH_PRICE_CACHE_TTL):
        return _eth_price_cache["value"]
    
    try:
        async with session.get(
            "https://api.coingecko.com/api/v3/simple/price?ids=ethereum&vs_currencies=usd",
            timeout=aiohttp.ClientTimeout(total=8),
        ) as resp:
            if resp.status == 200:
                data = await resp.json()
                if "ethereum" in data and "usd" in data["ethereum"]:
                    price = data["ethereum"]["usd"]
                    _eth_price_cache["value"] = price
                    _eth_price_cache["ts"] = now
                    logging.info(f"[السعر] تم تحديث سعر ETH: ${price}")
                    return price
                else:
                    logging.warning(f"[السعر] استجابة CoinGecko غير متوقعة: {data}")
            else:
                logging.warning(f"[السعر] CoinGecko رد بـ HTTP {resp.status}")
    except Exception as e:
        logging.warning(f"[السعر] تعذر جلب سعر ETH: {e}")
    
    return _eth_price_cache["value"] or 3000.0

# ===================================================================
# دوال التعامل مع OpenSea API
# ===================================================================
async def fetch_drop_detail_async(session: aiohttp.ClientSession, slug: str):
    url = f"{DROPS_API_BASE}/{slug}"
    headers = {"x-api-key": OPENSEA_API_KEY}
    
    try:
        async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status == 200:
                return True, await resp.json()
            if resp.status == 404:
                return False, None
            return None, None
    except Exception as e:
        logging.warning(f"[Drops API] خطأ: {e}")
        return None, None

def parse_iso(ts: str):
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except Exception:
        return None

def started_today_local(stage: dict) -> bool:
    start = parse_iso(stage.get("start_time", ""))
    if not start:
        return False
    return start.astimezone(LOCAL_TZ).date() == datetime.now(LOCAL_TZ).date()

def is_free_or_negligible(price_wei: int, eth_price_usd: float) -> bool:
    if price_wei == 0:
        return True
    price_usd = (price_wei / 1e18) * eth_price_usd
    return price_usd < FREE_PRICE_THRESHOLD_USD

# ===================================================================
# نظام إشعارات تيليجرام
# ===================================================================
send_queue: asyncio.Queue[str] = asyncio.Queue()

def enqueue_message(text: str):
    send_queue.put_nowait(text)

async def telegram_sender():
    async with aiohttp.ClientSession() as session:
        while True:
            text = await send_queue.get()
            
            try:
                async with session.post(
                    f"{TELEGRAM_API}/sendMessage",
                    data={
                        "chat_id": TELEGRAM_CHAT_ID,
                        "text": text,
                        "parse_mode": "HTML",
                    },
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as resp:
                    if resp.status != 200:
                        logging.warning(f"تيليجرام: استجابة غير متوقعة {resp.status}")
            except Exception as e:
                logging.error(f"خطأ إرسال تليجرام: {e}")
            
            send_queue.task_done()
            await asyncio.sleep(1.05)

# ===================================================================
# رسائل أسباب الفشل
# ===================================================================
REASON_MESSAGES = {
    "balance_too_low": "الرصيد بالمحفظة منخفض جدًا — توقف النظام عن الشراء",
    "gas_too_high": "رسوم الغاز التقديرية تجاوزت الحد المسموح",
    "gas_too_high_precise": "رسوم الغاز الفعلية (بعد التقدير الدقيق) تجاوزت الحد",
    "no_fee_recipient": "تعذر تحديد عنوان الرسوم من العقد",
    "simulation_failed": "محاكاة المعاملة فشلت — على الأغلب المينت غير متاح فعليًا",
    "insufficient_funds_for_total_cost": "الرصيد لا يكفي سعر المينت + الغاز معًا",
    "tx_value_too_high": "قيمة المعاملة تجاوزت الحد الأقصى المسموح به",
    "tx_error": "خطأ أثناء إرسال المعاملة",
}

def check_mint_type(price_wei: int, eth_price_usd: float) -> dict:
    paid, price, label = is_paid_mint(price_wei, eth_price_usd)
    return {
        "mint_type": label,
        "price_usd": price,
        "icon": "💰" if paid else "🎁",
    }

def check_user_eligibility(reason: str, balance_usd: float = 0, gas_fee_usd: float = 0) -> dict:
    analysis = check_eligibility_reason(reason)
    
    if analysis["eligible"]:
        icon = "✅"
        label = "مؤهل ✅"
    else:
        icon = "❌"
        label = "غير مؤهل ❌"
    
    extra_info = ""
    if reason == "balance_too_low":
        extra_info = f"\n📊 رصيدك الحالي: ${balance_usd:.4f} — الحد الأدنى المطلوب: ${MIN_BALANCE_RESERVE_USD}"
    elif "gas_too_high" in reason:
        extra_info = f"\n⛽ رسوم الغاز المقدرة: ${gas_fee_usd:.4f} — الحد الأقصى المسموح: ${MAX_GAS_FEE_USD}"
    
    return {
        "eligible": analysis["eligible"],
        "icon": icon,
        "label": label,
        "description": analysis["description"] + extra_info,
        "retryable": analysis["retryable"],
    }

def build_result_message(detail: dict, result: dict, chain_name: str, wallet_name: str = "المحفظة") -> str:
    name = detail.get("collection_name") or detail.get("collection_slug")
    url = detail.get("opensea_url", "")
    chain_display = CHAINS_CONFIG.get(chain_name, {}).get("chain_name_display", chain_name)
    
    stage = detail.get("active_stage") or {}
    price_wei = int(stage.get("price", "0"))
    eth_price_usd = result.get("eth_price_usd", 0)

    if result["success"]:
        return (
            f"✅ <b>تم الشراء بنجاح!</b>\n\n"
            f"المجموعة: <b>{name}</b>\n"
            f"السلسلة: {chain_display}\n"
            f"المحفظة: {wallet_name}\n"
            f"الكمية: {result['quantity']}\n"
            f"رسوم الغاز: ${result['gas_fee_usd']:.4f}\n"
            f"معاملة: {result['tx_hash']}\n"
            f"🔗 {url}"
        )

    reason = result.get("reason", "unknown")
    reason_text = REASON_MESSAGES.get(reason, reason)
    
    mint_info = check_mint_type(price_wei, eth_price_usd)
    eligibility = check_user_eligibility(
        reason, 
        balance_usd=result.get("balance_usd", 0),
        gas_fee_usd=result.get("gas_fee_usd", 0),
    )
    
    extra = ""
    if result.get("balance_usd"):
        extra += f"\nالرصيد الحالي: ${result['balance_usd']:.4f}"
    if result.get("gas_fee_usd"):
        extra += f"\nالرسوم المقدّرة: ${result['gas_fee_usd']:.4f}"
    if result.get("tx_hash"):
        extra += f"\nهاش المعاملة: {result['tx_hash']}"

    return (
        f"⏭️ <b>تم تجاهل الشراء</b>\n\n"
        f"المجموعة: <b>{name}</b>\n"
        f"السلسلة: {chain_display}\n"
        f"المحفظة: {wallet_name}\n"
        f"السبب: {reason_text}{extra}\n\n"
        f"─── ℹ️ معلومات التحليل ───\n"
        f"{mint_info['icon']} نوع المينت: {mint_info['mint_type']}\n"
        f"{eligibility['icon']} الأهلية: {eligibility['label']}\n"
        f"📝 {eligibility['description']}\n"
        f"🔗 {url}"
    )

def build_mint_info_message(detail: dict, eth_price_usd: float) -> str:
    name = detail.get("collection_name") or detail.get("collection_slug", "غير معروف")
    url = detail.get("opensea_url", "")

    stage = detail.get("active_stage") or {}
    price_wei = int(stage.get("price", "0") or "0")

    paid, price_usd, _label = is_paid_mint(price_wei, eth_price_usd)
    price_eth = price_wei / 1e18 if price_wei > 0 else 0
    if paid:
        price_str = f"💰 مدفوع: {price_eth:.4f} ETH (≈ ${price_usd:.2f})"
        header_icon = "💰"
        mint_type = "مدفوع"
        action_note = "⏭️ مينت مدفوع — سيتم التجاهل تلقائياً"
    else:
        price_str = "🎁 مجاني"
        header_icon = "🎁"
        mint_type = "مجاني"
        action_note = "⏳ جاري الشراء التلقائي..."

    def fmt_date(ts: str) -> str:
        if not ts:
            return "غير محدد"
        dt = parse_iso(ts)
        if not dt:
            return ts[:16] if len(ts) >= 16 else ts
        return dt.astimezone(LOCAL_TZ).strftime("%Y-%m-%d %H:%M")

    start_str = fmt_date(stage.get("start_time", ""))
    end_str = fmt_date(stage.get("end_time", ""))

    max_supply = int(detail.get("max_supply") or 0)
    total_supply = int(detail.get("total_supply") or 0)
    remaining = max(0, max_supply - total_supply)

    max_per_wallet_raw = stage.get("max_per_wallet")
    max_per_wallet_str = str(max_per_wallet_raw) if max_per_wallet_raw is not None else "غير محدد"

    return (
        f"{header_icon} <b>مينت {mint_type} نشط!</b>\n\n"
        f"📦 المجموعة: <b>{name}</b>\n"
        f"💲 السعر: {price_str}\n"
        f"📊 الكمية الكلية: {max_supply:,}\n"
        f"✅ المتبقي: {remaining:,} قطعة\n"
        f"👛 الحد لكل محفظة: {max_per_wallet_str}\n"
        f"🕐 بدأ: {start_str}\n"
        f"🕔 ينتهي: {end_str}\n"
        f"ℹ️ {action_note}\n"
        f"🔗 {url}"
    )

# ===================================================================
# نظام إعادة المحاولة
# ===================================================================
async def retry_purchase(session: aiohttp.ClientSession, tracker: RetryTracker):
    found, detail = await fetch_drop_detail_async(session, tracker.slug)
    if not found or not detail:
        logging.warning(f"⏭️ إعادة المحاولة: لم نجد الدروب {tracker.slug}")
        async with retry_lock:
            retry_tasks.pop(tracker.retry_key, None)
        return

    stage = detail.get("active_stage") or {}
    tracker.price_wei = int(stage.get("price", "0") or "0")
    max_supply = int(detail.get("max_supply") or 0)
    total_supply = int(detail.get("total_supply") or 0)
    tracker.remaining_supply = max(0, max_supply - total_supply)
    tracker.detail = detail

    eth_price_usd = await get_eth_price_usd(session)
    tracker.eth_price_usd = eth_price_usd

    w3 = w3_instances.get(tracker.chain_name)
    if not w3:
        logging.warning(f"⚠️ لا يوجد Web3 للسلسلة {tracker.chain_name}")
        return

    chain_config = CHAINS_CONFIG[tracker.chain_name]
    seadrop_address = chain_config["seadrop_address"]
    contract_address = detail.get("contract_address")
    if not contract_address:
        logging.warning(f"⚠️ لا يوجد contract_address في الدروب {tracker.slug}")
        return

    checks = quick_checks(
        w3, tracker.wallet_address, eth_price_usd,
        contract_address, seadrop_address,
    )
    if not checks["pass"]:
        reason = checks["reason"]
        logging.info(f"⏭️ إعادة المحاولة '{tracker.slug}' - {tracker.wallet_name}: {reason}")
        if reason not in RETRYABLE_REASONS:
            async with retry_lock:
                retry_tasks.pop(tracker.retry_key, None)
        else:
            tracker.original_reason = reason
        return

    result = await asyncio.to_thread(
        attempt_purchase,
        w3,
        tracker.wallet_private_key,
        tracker.wallet_address,
        contract_address,
        seadrop_address,
        tracker.price_wei,
        tracker.max_per_wallet,
        tracker.remaining_supply,
        eth_price_usd,
    )
    result["eth_price_usd"] = eth_price_usd
    enqueue_message(build_result_message(detail, result, tracker.chain_name, tracker.wallet_name))

    if result.get("success"):
        async with retry_lock:
            retry_tasks.pop(tracker.retry_key, None)
        logging.info(f"✅ إعادة المحاولة نجحت لـ '{tracker.slug}' - {tracker.wallet_name}")
    else:
        reason = result.get("reason", "غير معروف")
        if reason not in RETRYABLE_REASONS:
            async with retry_lock:
                retry_tasks.pop(tracker.retry_key, None)
        else:
            tracker.original_reason = reason

async def schedule_retry(session: aiohttp.ClientSession, tracker: RetryTracker):
    while not tracker.is_expired:
        reason = tracker.original_reason.lower()
        if "gas" in reason:
            interval = GAS_RETRY_INTERVAL
        elif reason in ("no_fee_recipient", "simulation_failed"):
            interval = 60
        elif reason == "balance_too_low":
            interval = ELIGIBILITY_RETRY_INTERVAL
        else:
            interval = RETRY_INTERVAL

        await asyncio.sleep(interval)
        
        found, detail = await fetch_drop_detail_async(session, tracker.slug)
        if not found or not detail or not detail.get("is_minting"):
            logging.info(f"⏹️ توقف إعادة المحاولة لـ '{tracker.slug}': المينت انتهى أو غير نشط.")
            async with retry_lock:
                retry_tasks.pop(tracker.retry_key, None)
            break

        tracker.attempt_count += 1
        if "gas" not in reason or tracker.attempt_count % 10 == 0:
            enqueue_message(
                f"🔄 <b>إعادة محاولة مستمرة</b>\n\n"
                f"المجموعة: <b>{tracker.slug}</b>\n"
                f"السبب: {tracker.original_reason}\n"
                f"المحاولة: {tracker.attempt_count}\n"
                f"الوقت المتبقي: {tracker.hours_remaining:.1f} ساعة"
            )
        
        await retry_purchase(session, tracker)
        
        async with retry_lock:
            if tracker.retry_key not in retry_tasks:
                break

    async with retry_lock:
        if tracker.retry_key in retry_tasks:
            enqueue_message(
                f"⏰ <b>انتهت محاولات إعادة الشراء</b>\n\n"
                f"المجموعة: <b>{tracker.slug}</b>\n"
                f"السلسلة: {tracker.chain_name}\n"
                f"المحفظة: {tracker.wallet_name}\n"
                f"عدد المحاولات: {tracker.attempt_count}\n"
                f"المدة: {MAX_RETRY_HOURS} ساعات"
            )
            retry_tasks.pop(tracker.retry_key, None)

# ===================================================================
# المنطق الأساسي للشراء
# ===================================================================
async def evaluate_and_buy(
    session: aiohttp.ClientSession,
    slug: str,
    notified: set,
    known_external: set,
    checking: set,
):
    try:
        found, detail = await fetch_drop_detail_async(session, slug)
        if not found or not detail:
            known_external.add(slug)
            return
        
        if not detail.get("is_minting"):
            known_external.add(slug)
            return
        
        stage = detail.get("active_stage")
        if not stage:
            known_external.add(slug)
            logging.info(f"⏭️ '{slug}': لا توجد مرحلة نشطة — تم تجاهله.")
            return

        max_supply = int(detail.get("max_supply") or 0)
        total_supply = int(detail.get("total_supply") or 0)
        remaining = max_supply - total_supply
        if remaining <= 0:
            known_external.add(slug)
            return

        price_wei = int(stage.get("price", "0") or "0")
        eth_price_usd = await get_eth_price_usd(session)

        info_msg = build_mint_info_message(detail, eth_price_usd)
        enqueue_message(info_msg)

        if not is_free_or_negligible(price_wei, eth_price_usd):
            logging.info(f"💰 '{slug}': مينت مدفوع ({price_wei} wei) — تم عرض المعلومات فقط. سيتم إعادة الفحص لاحقاً.")
            return

        contract_address = detail.get("contract_address")
        if not contract_address:
            logging.warning(f"⏭️ '{slug}': لا يوجد contract_address بالبيانات.")
            known_external.add(slug)
            return

        max_per_wallet_raw = stage.get("max_per_wallet")
        max_per_wallet = int(max_per_wallet_raw) if max_per_wallet_raw is not None else None

        notified.add(slug)

        if not w3_instances:
            logging.warning("⚠️ لا توجد سلاسل نشطة للشراء.")
            return
        if not WALLETS:
            logging.warning("⚠️ لا توجد محافظ مفعلة.")
            enqueue_message(f"⚠️ لا توجد محافظ مفعلة للمجموعة {slug}")
            return

        tasks = []
        for wallet in WALLETS:
            wallet_name = wallet["name"]
            wallet_address = wallet["address"]
            wallet_private_key = wallet["private_key"]
            
            for chain_name in ENABLED_CHAINS:
                w3 = w3_instances.get(chain_name)
                if not w3:
                    continue

                chain_config = CHAINS_CONFIG[chain_name]
                seadrop_address = chain_config["seadrop_address"]

                checks = quick_checks(
                    w3, wallet_address, eth_price_usd,
                    contract_address, seadrop_address,
                )
                if not checks["pass"]:
                    reason = checks["reason"]
                    logging.info(f"⏭️ '{slug}' على {chain_name} - {wallet_name}: {reason}")
                    
                    result = {
                        "success": False,
                        "reason": reason,
                        "balance_usd": checks.get("balance_usd", 0),
                        "gas_fee_usd": checks.get("gas_fee_usd", 0),
                        "eth_price_usd": eth_price_usd,
                    }
                    enqueue_message(build_result_message(detail, result, chain_name, wallet_name))
                    
                    if reason in RETRYABLE_REASONS:
                        async with retry_lock:
                            retry_key = f"{slug}:{wallet_address}:{chain_name}"
                            if retry_key not in retry_tasks:
                                tracker = RetryTracker(
                                    slug=slug,
                                    wallet_address=wallet_address,
                                    chain_name=chain_name,
                                    detail=detail,
                                    wallet_name=wallet_name,
                                    wallet_private_key=wallet_private_key,
                                    price_wei=price_wei,
                                    max_per_wallet=max_per_wallet,
                                    remaining_supply=remaining,
                                    eth_price_usd=eth_price_usd,
                                    original_reason=reason,
                                )
                                retry_tasks[retry_key] = tracker
                                asyncio.create_task(schedule_retry(session, tracker))
                                logging.info(f"🔄 تمت جدولة إعادة المحاولة لـ '{slug}' - {wallet_name} على {chain_name} (السبب: {reason})")
                    
                    continue

                tasks.append(
                    (wallet_name, chain_name, wallet_address, wallet_private_key, asyncio.to_thread(
                        attempt_purchase,
                        w3, wallet_private_key, wallet_address,
                        contract_address, seadrop_address,
                        price_wei, max_per_wallet, remaining, eth_price_usd,
                    ))
                )

        if not tasks:
            logging.info(f"⏭️ '{slug}': لا توجد محافظ مؤهلة للشراء.")
            return

        logging.info(f"🔄 '{slug}': بدء {len(tasks)} محاولة شراء ({len(WALLETS)} محفظة × {len(ENABLED_CHAINS)} سلسلة)")

        results = await asyncio.gather(*[t[4] for t in tasks], return_exceptions=True)

        for (wallet_name, chain_name, wallet_address, wallet_private_key, _), result in zip(tasks, results):
            if isinstance(result, Exception):
                logging.error(f"❌ '{slug}' على {chain_name} - {wallet_name}: خطأ غير متوقع: {result}")
                async with retry_lock:
                    retry_key = f"{slug}:{wallet_address}:{chain_name}"
                    if retry_key not in retry_tasks:
                        tracker = RetryTracker(
                            slug=slug,
                            wallet_address=wallet_address,
                            chain_name=chain_name,
                            detail=detail,
                            wallet_name=wallet_name,
                            wallet_private_key=wallet_private_key,
                            price_wei=price_wei,
                            max_per_wallet=max_per_wallet,
                            remaining_supply=remaining,
                            eth_price_usd=eth_price_usd,
                            original_reason="tx_error",
                        )
                        retry_tasks[retry_key] = tracker
                        asyncio.create_task(schedule_retry(session, tracker))
                continue
            
            if isinstance(result, dict):
                result["eth_price_usd"] = eth_price_usd
                enqueue_message(build_result_message(detail, result, chain_name, wallet_name))
                
                if result.get("success"):
                    logging.info(f"✅ '{slug}' على {chain_name} - {wallet_name}: شراء ناجح - {result['quantity']} قطعة")
                else:
                    reason = result.get("reason", "غير معروف")
                    logging.info(f"⏭️ '{slug}' على {chain_name} - {wallet_name}: {reason}")
                    
                    if reason in RETRYABLE_REASONS:
                        async with retry_lock:
                            retry_key = f"{slug}:{wallet_address}:{chain_name}"
                            if retry_key not in retry_tasks:
                                tracker = RetryTracker(
                                    slug=slug,
                                    wallet_address=wallet_address,
                                    chain_name=chain_name,
                                    detail=detail,
                                    wallet_name=wallet_name,
                                    wallet_private_key=wallet_private_key,
                                    price_wei=price_wei,
                                    max_per_wallet=max_per_wallet,
                                    remaining_supply=remaining,
                                    eth_price_usd=eth_price_usd,
                                    original_reason=reason,
                                )
                                retry_tasks[retry_key] = tracker
                                asyncio.create_task(schedule_retry(session, tracker))
                                logging.info(f"🔄 تمت جدولة إعادة المحاولة لـ '{slug}' - {wallet_name} على {chain_name} (السبب: {reason})")

    except Exception as e:
        logging.error(f"خطأ غير متوقع بمعالجة '{slug}': {e}")
    finally:
        checking.discard(slug)

# ===================================================================
# الاتصال بـ OpenSea Stream
# ===================================================================
async def listen_opensea(notified: set, known_external: set, checking: set):
    msg_ref = 0

    async with aiohttp.ClientSession() as session:
        while True:
            try:
                async with websockets.connect(STREAM_URL, ping_interval=None, open_timeout=15) as ws:
                    logging.info("🚀 متصل بـ OpenSea Stream — Ethereum + Robinhood")
                    
                    join_ref = str(msg_ref)
                    await ws.send(json.dumps([join_ref, join_ref, "collection:*", "phx_join", {}]))
                    msg_ref += 1
                    
                    last_heartbeat = time.time()
                    last_stats_time = time.time()

                    while True:
                        if time.time() - last_stats_time > 300:
                            actives = len(checking)
                            retry_count = len(retry_tasks)
                            logging.info(f"📊 إحصائيات: {len(notified)} معروف | {len(known_external)} خارجي | {actives} قيد الفحص | {retry_count} إعادة محاولة | {len(WALLETS)} محفظة")
                            last_stats_time = time.time()

                        if time.time() - last_heartbeat > HEARTBEAT_INTERVAL:
                            hb_ref = str(msg_ref)
                            await ws.send(json.dumps([None, hb_ref, "phoenix", "heartbeat", {}]))
                            msg_ref += 1
                            last_heartbeat = time.time()

                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=RECV_TIMEOUT)
                        except asyncio.TimeoutError:
                            continue

                        try:
                            parsed = json.loads(raw)
                        except json.JSONDecodeError:
                            continue

                        if isinstance(parsed, list) and len(parsed) == 5:
                            _jref, _ref, _topic, event_name, payload_wrapper = parsed
                        else:
                            continue

                        if event_name != "item_transferred":
                            continue

                        payload = (payload_wrapper or {}).get("payload") or {}
                        item = payload.get("item", {}) or {}
                        chain = (item.get("chain", {}) or {}).get("name", "")

                        if chain not in ("robinhood", "ethereum"):
                            continue

                        from_address = ((payload.get("from_account") or {}).get("address", "") or "").lower()
                        if from_address != ZERO_ADDRESS:
                            continue

                        slug = (payload.get("collection", {}) or {}).get("slug", "")
                        
                        if not slug or slug in notified or slug in known_external or slug in checking:
                            continue

                        checking.add(slug)
                        logging.info(f"🔍 اكتشاف جديد: '{slug}' على {chain}")
                        
                        asyncio.create_task(
                            process_with_semaphore(session, slug, notified, known_external, checking)
                        )

            except (websockets.ConnectionClosed, OSError, asyncio.TimeoutError) as e:
                logging.warning(f"⚠️ انقطع الاتصال ({e}). إعادة الاتصال خلال 3 ثوانٍ...")
                await asyncio.sleep(3)
            except Exception as e:
                logging.error(f"خطأ غير متوقع: {e}. إعادة المحاولة خلال 5 ثوانٍ...")
                await asyncio.sleep(5)

async def process_with_semaphore(
    session: aiohttp.ClientSession,
    slug: str,
    notified: set,
    known_external: set,
    checking: set,
):
    async with semaphore:
        await evaluate_and_buy(session, slug, notified, known_external, checking)

# ===================================================================
# الماسح الدوري
# ===================================================================
async def scan_active_drops(notified: set, known_external: set, checking: set):
    logging.info(f"🔍 الماسح الدوري: بدأ العمل — فحص كل {SCAN_INTERVAL // 60} دقائق")

    async with aiohttp.ClientSession() as session:
        while True:
            try:
                url = f"{DROPS_API_BASE}?is_minting=true&limit=50"
                headers = {"x-api-key": OPENSEA_API_KEY}

                async with session.get(
                    url, headers=headers, timeout=aiohttp.ClientTimeout(total=15)
                ) as resp:
                    if resp.status != 200:
                        logging.warning(f"[الماسح] OpenSea رد بـ HTTP {resp.status}")
                    else:
                        data = await resp.json()
                        drops = data.get("drops") or data.get("results", [])
                        logging.info(f"[الماسح] وجد {len(drops)} مينت نشط في السوق")

                        new_count = 0
                        for drop in drops:
                            slug = drop.get("collection_slug") or drop.get("slug", "")
                            if not slug:
                                continue
                            if slug in notified or slug in known_external or slug in checking:
                                continue
                            checking.add(slug)
                            new_count += 1
                            logging.info(f"🔍 [الماسح] اكتشاف: '{slug}'")
                            asyncio.create_task(
                                process_with_semaphore(session, slug, notified, known_external, checking)
                            )

                        if new_count > 0:
                            logging.info(f"[الماسح] أرسل {new_count} مينت جديد للمعالجة")

            except Exception as e:
                logging.error(f"[الماسح] خطأ أثناء المسح: {e}")

            await asyncio.sleep(SCAN_INTERVAL)

# ===================================================================
# التشغيل الرئيسي
# ===================================================================
async def run():
    if not BOT_ENABLED:
        logging.warning("🔴 BOT_ENABLED=false — النظام متوقف عمدًا (وضع الأمان). لن يشتري أي شي.")
        enqueue_message("🔴 البوت شغّال لكن بوضع الإيقاف (BOT_ENABLED=false) — ما رح يشتري لين تفعّله.")
        await telegram_sender()
        return

    if not WALLETS:
        logging.critical("🔴 لا توجد محافظ! تأكد من تعبئة PRIVATE_KEY و WALLET_ADDRESS في ملف .env")
        enqueue_message("🔴 لا توجد محافظ! تأكد من تعبئة PRIVATE_KEY و WALLET_ADDRESS في ملف .env")
        await telegram_sender()
        return

    chains_status = []
    for c in ENABLED_CHAINS:
        chains_status.append(CHAINS_CONFIG[c]["chain_name_display"])
    
    wallets_count = len(WALLETS)
    status_msg = "✅ نظام الشراء التلقائي v2 اشتغل الآن!\n"
    status_msg += f"📡 السلاسل النشطة: {', '.join(chains_status) if chains_status else 'لا توجد'}\n"
    status_msg += f"👛 المحافظ النشطة: {wallets_count}\n"
    status_msg += f"🔢 الحد الأقصى للمجموعات المتزامنة: {MAX_CONCURRENT_MINTS}\n"
    status_msg += f"🔄 نظام إعادة المحاولة: نشط — محاولات مستمرة وذكية لمدة {MAX_RETRY_HOURS} ساعات\n"
    status_msg += f"🔍 الماسح الدوري: نشط — يفحص المينتات كل {SCAN_INTERVAL} ثانية"

    enqueue_message(status_msg)
    logging.info(status_msg)

    notified: set[str] = set()
    known_external: set[str] = set()
    checking: set[str] = set()

    await asyncio.gather(
        listen_opensea(notified, known_external, checking),
        scan_active_drops(notified, known_external, checking),
        telegram_sender(),
    )

def main():
    backoff = 2
    
    while True:
        try:
            asyncio.run(run())
        except KeyboardInterrupt:
            logging.info("تم الإيقاف يدويًا.")
            break
        except Exception as e:
            logging.critical(f"توقف غير متوقع: {e}. إعادة التشغيل خلال {backoff} ثانية...")
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)
            continue
        else:
            break

if __name__ == "__main__":
    main()

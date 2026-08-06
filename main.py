"""
النظام الكامل: اكتشاف مينت مجاني بدأ اليوم على Ethereum + Robinhood Chain.
"""

import asyncio
import json
import logging
import os
import time
import traceback
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
    get_all_wallets_balances,
)

log = logging.getLogger(__name__)
load_dotenv()

# ===================================================================
# المتغيرات العامة
# ===================================================================
OPENSEA_API_KEY = os.environ.get("OPENSEA_API_KEY", "").strip()
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
BOT_ENABLED = os.environ.get("BOT_ENABLED", "true").strip().lower() == "true"

# ===================================================================
# التحقق من صحة متغيرات تيليجرام
# ===================================================================
if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
    logging.warning("⚠️ متغيرات تيليجرام غير مكتملة! لن يتم إرسال الإشعارات.")
    BOT_ENABLED = False

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
    CHAINS_CONFIG["robinhood"]["is_poa"] = True
    ENABLED_CHAINS.append("robinhood")
if ETHEREUM_RPC_URL:
    CHAINS_CONFIG["ethereum"]["rpc_url"] = ETHEREUM_RPC_URL
    CHAINS_CONFIG["ethereum"]["is_poa"] = False
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
# تحديد السلسلة الصحيحة للمجموعة
# ===================================================================
def determine_actual_chain(slug: str, stream_chain: str, contract_address: str) -> str:
    """تحديد السلسلة الصحيحة للمجموعة بناءً على معلومات متعددة."""
    stream_chain = stream_chain.lower() if stream_chain else ""
    slug_lower = slug.lower() if slug else ""
    contract_address = contract_address.lower() if contract_address else ""
    
    # 1. التحقق من الـ slug
    if "robinhood" in slug_lower or "rh" in slug_lower or "hood" in slug_lower:
        logging.debug(f"🔍 '{slug}' → Robinhood (من slug)")
        return "robinhood"
    
    # 2. التحقق من عنوان العقد (Robinhood Chain)
    if contract_address.startswith("0x00005ea00a"):
        logging.debug(f"🔍 '{slug}' → Robinhood (من عنوان العقد)")
        return "robinhood"
    
    # 3. إذا كانت السلسلة من Stream هي Robinhood
    if stream_chain == "robinhood":
        return "robinhood"
    
    # 4. إذا كانت السلسلة من Stream هي ethereum
    if stream_chain == "ethereum":
        return "ethereum"
    
    # 5. افتراضياً: نبحث في السلاسل المتاحة
    if "ethereum" in ENABLED_CHAINS:
        return "ethereum"
    elif "robinhood" in ENABLED_CHAINS:
        return "robinhood"
    
    return stream_chain if stream_chain else "ethereum"

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

def is_free_or_negligible(price_wei: int, eth_price_usd: float) -> bool:
    if price_wei == 0:
        return True
    price_usd = (price_wei / 1e18) * eth_price_usd
    return price_usd < FREE_PRICE_THRESHOLD_USD

# ===================================================================
# نظام إشعارات تيليجرام - مُحسّن مع إعادة محاولة قوية
# ===================================================================
send_queue: asyncio.Queue[str] = asyncio.Queue()

def enqueue_message(text: str):
    """إضافة رسالة إلى طابور الإرسال مع تسجيل للتصحيح."""
    if not BOT_ENABLED:
        logging.info(f"📝 [TELEGRAM DISABLED] {text[:200]}...")
        return
    
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logging.warning("⚠️ متغيرات تيليجرام غير مكتملة، لا يمكن إرسال الرسالة")
        return
    
    logging.info(f"📤 إضافة رسالة إلى الطابور: {text[:100]}...")
    send_queue.put_nowait(text)


async def send_telegram_message_direct(text: str) -> bool:
    """
    إرسال رسالة مباشرة إلى تيليجرام (بدون طابور) - للاختبار والرسائل الحرجة.
    """
    if not BOT_ENABLED:
        return False
    
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logging.error("❌ متغيرات تيليجرام غير مكتملة")
        return False
    
    try:
        if len(text) > 4000:
            parts = [text[i:i+4000] for i in range(0, len(text), 4000)]
            success = True
            for part in parts:
                if not await send_telegram_message_direct(part):
                    success = False
            return success
        
        url = f"{TELEGRAM_API}/sendMessage"
        data = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": text,
            "parse_mode": "HTML",
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.post(url, data=data, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                response_text = await resp.text()
                if resp.status == 200:
                    logging.info("✅ تم إرسال رسالة تيليجرام بنجاح (مباشر)")
                    return True
                else:
                    logging.error(f"❌ فشل إرسال تيليجرام: HTTP {resp.status} - {response_text[:200]}")
                    return False
    except asyncio.TimeoutError:
        logging.error("❌ تيليجرام: timeout")
        return False
    except Exception as e:
        logging.error(f"❌ تيليجرام: خطأ غير متوقع: {e}")
        return False


async def send_telegram_message(session: aiohttp.ClientSession, text: str) -> bool:
    """إرسال رسالة واحدة إلى تيليجرام."""
    if not BOT_ENABLED:
        return False
    
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logging.error("❌ متغيرات تيليجرام غير مكتملة")
        return False
    
    try:
        if len(text) > 4000:
            parts = [text[i:i+4000] for i in range(0, len(text), 4000)]
            success = True
            for part in parts:
                if not await send_telegram_message(session, part):
                    success = False
            return success
        
        url = f"{TELEGRAM_API}/sendMessage"
        data = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": text,
            "parse_mode": "HTML",
        }
        
        async with session.post(url, data=data, timeout=aiohttp.ClientTimeout(total=30)) as resp:
            response_text = await resp.text()
            if resp.status == 200:
                logging.info("✅ تم إرسال رسالة تيليجرام بنجاح")
                return True
            else:
                logging.error(f"❌ فشل إرسال تيليجرام: HTTP {resp.status} - {response_text[:200]}")
                return False
    except asyncio.TimeoutError:
        logging.error("❌ تيليجرام: timeout")
        return False
    except Exception as e:
        logging.error(f"❌ تيليجرام: خطأ غير متوقع: {e}")
        return False


async def telegram_sender():
    """إرسال الرسائل من الطابور إلى تيليجرام بشكل غير متزامن مع إعادة محاولة قوية."""
    logging.info("📡 بدء مرسل تيليجرام...")
    
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                try:
                    text = await asyncio.wait_for(send_queue.get(), timeout=60.0)
                except asyncio.TimeoutError:
                    continue
                
                logging.info(f"📨 جاري إرسال رسالة تيليجرام (طول: {len(text)} حرف)...")
                
                success = False
                for attempt in range(5):
                    logging.info(f"🔄 محاولة إرسال {attempt + 1}/5")
                    success = await send_telegram_message(session, text)
                    if success:
                        logging.info("✅ تم إرسال رسالة تيليجرام بنجاح")
                        break
                    if attempt < 4:
                        wait_time = (attempt + 1) * 3
                        logging.warning(f"⚠️ إعادة محاولة إرسال تيليجرام بعد {wait_time} ثانية")
                        await asyncio.sleep(wait_time)
                
                if not success:
                    logging.error(f"❌ فشل إرسال رسالة تيليجرام بعد 5 محاولات")
                    logging.info("🔄 محاولة الإرسال المباشر...")
                    if await send_telegram_message_direct(text):
                        logging.info("✅ تم إرسال الرسالة عبر الإرسال المباشر")
                    else:
                        logging.error("❌ فشل الإرسال المباشر أيضًا")
                    
            except asyncio.CancelledError:
                logging.info("🛑 تم إيقاف مرسل تيليجرام")
                break
            except Exception as e:
                logging.error(f"❌ خطأ في telegram_sender: {e}")
                logging.error(traceback.format_exc())
                await asyncio.sleep(5)
            finally:
                try:
                    send_queue.task_done()
                except ValueError:
                    pass
                await asyncio.sleep(0.5)

# ===================================================================
# بناء رسالة عرض الرصيد
# ===================================================================
def build_balances_message(balances: dict, eth_price_usd: float) -> str:
    """بناء رسالة تعرض رصيد كل محفظة لكل سلسلة."""
    if not balances:
        return "⚠️ لا توجد بيانات رصيد متاحة"
    
    msg_lines = []
    msg_lines.append("💰 <b>رصيد المحافظ</b>")
    msg_lines.append(f"📊 سعر ETH: ${eth_price_usd:.2f}\n")
    
    total_balance = 0.0
    for wallet_name, chain_balances in balances.items():
        msg_lines.append(f"👛 <b>{wallet_name}</b>")
        wallet_total = 0.0
        for chain_name, balance in chain_balances.items():
            chain_display = CHAINS_CONFIG.get(chain_name, {}).get("chain_name_display", chain_name)
            status = "✅" if balance >= MIN_BALANCE_RESERVE_USD else "⚠️"
            balance_str = f"{balance:.4f}"
            msg_lines.append(f"  {status} {chain_display}: ${balance_str}")
            wallet_total += balance
        msg_lines.append(f"  📊 إجمالي المحفظة: ${wallet_total:.4f}")
        total_balance += wallet_total
        msg_lines.append("")
    
    msg_lines.append(f"💵 <b>إجمالي الرصيد الكلي: ${total_balance:.4f}</b>")
    
    return "\n".join(msg_lines)


async def send_balances_report(session: aiohttp.ClientSession):
    """إرسال تقرير رصيد جميع المحافظ إلى تيليجرام."""
    if not w3_instances or not WALLETS:
        logging.warning("⚠️ لا توجد سلاسل أو محافظ لعرض الرصيد")
        return
    
    try:
        eth_price_usd = await get_eth_price_usd(session)
        balances = get_all_wallets_balances(w3_instances, WALLETS, eth_price_usd)
        msg = build_balances_message(balances, eth_price_usd)
        await send_telegram_message_direct(msg)
        logging.info("📊 تم إرسال تقرير الرصيد")
    except Exception as e:
        logging.error(f"❌ فشل إرسال تقرير الرصيد: {e}")
        logging.error(traceback.format_exc())

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
    name = detail.get("collection_name") or detail.get("collection_slug", "غير معروف")
    url = detail.get("opensea_url", "")
    chain_display = CHAINS_CONFIG.get(chain_name, {}).get("chain_name_display", chain_name)
    
    stage = detail.get("active_stage") or {}
    price_wei = int(stage.get("price", "0"))
    eth_price_usd = result.get("eth_price_usd", 0)

    if result.get("success", False):
        return (
            f"✅ <b>تم الشراء بنجاح!</b>\n\n"
            f"📦 المجموعة: <b>{name}</b>\n"
            f"⛓️ السلسلة: {chain_display}\n"
            f"👛 المحفظة: {wallet_name}\n"
            f"📊 الكمية: {result.get('quantity', 0)}\n"
            f"⛽ رسوم الغاز: ${result.get('gas_fee_usd', 0):.4f}\n"
            f"💵 رصيد المحفظة: ${result.get('balance_usd', 0):.4f}\n"
            f"🔗 معاملة: {result.get('tx_hash', '')}\n"
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
        extra += f"\n💵 الرصيد الحالي: ${result['balance_usd']:.4f}"
    if result.get("gas_fee_usd"):
        extra += f"\n⛽ الرسوم المقدّرة: ${result['gas_fee_usd']:.4f}"
    if result.get("tx_hash"):
        extra += f"\n🔗 هاش المعاملة: {result['tx_hash']}"

    return (
        f"⏭️ <b>تم تجاهل الشراء</b>\n\n"
        f"📦 المجموعة: <b>{name}</b>\n"
        f"⛓️ السلسلة: {chain_display}\n"
        f"👛 المحفظة: {wallet_name}\n"
        f"⚠️ السبب: {reason_text}{extra}\n\n"
        f"─── ℹ️ معلومات التحليل ───\n"
        f"{mint_info['icon']} نوع المينت: {mint_info['mint_type']}\n"
        f"{eligibility['icon']} الأهلية: {eligibility['label']}\n"
        f"📝 {eligibility['description']}\n"
        f"🔗 {url}"
    )

def build_mint_info_message(detail: dict, eth_price_usd: float, chain_name: str = None) -> str:
    name = detail.get("collection_name") or detail.get("collection_slug", "غير معروف")
    url = detail.get("opensea_url", "")
    
    chain_display = ""
    if chain_name:
        chain_display = f"\n⛓️ السلسلة: {CHAINS_CONFIG.get(chain_name, {}).get('chain_name_display', chain_name)}"

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
        f"📦 المجموعة: <b>{name}</b>{chain_display}\n"
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
        w3, tracker.wallet_address, tracker.chain_name, eth_price_usd,
        contract_address, seadrop_address,
    )
    if not checks["pass"]:
        reason = checks["reason"]
        logging.info(f"⏭️ إعادة المحاولة '{tracker.slug}' - {tracker.wallet_name} على {tracker.chain_name}: {reason}")
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
        tracker.chain_name,
        contract_address,
        seadrop_address,
        tracker.price_wei,
        tracker.max_per_wallet,
        tracker.remaining_supply,
        eth_price_usd,
    )
    result["eth_price_usd"] = eth_price_usd
    
    msg = build_result_message(detail, result, tracker.chain_name, tracker.wallet_name)
    enqueue_message(msg)

    if result.get("success"):
        async with retry_lock:
            retry_tasks.pop(tracker.retry_key, None)
        logging.info(f"✅ إعادة المحاولة نجحت لـ '{tracker.slug}' - {tracker.wallet_name} على {tracker.chain_name}")
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
            msg = (
                f"🔄 <b>إعادة محاولة مستمرة</b>\n\n"
                f"📦 المجموعة: <b>{tracker.slug}</b>\n"
                f"⛓️ السلسلة: {tracker.chain_name}\n"
                f"⚠️ السبب: {tracker.original_reason}\n"
                f"📊 المحاولة: {tracker.attempt_count}\n"
                f"⏳ الوقت المتبقي: {tracker.hours_remaining:.1f} ساعة"
            )
            enqueue_message(msg)
        
        await retry_purchase(session, tracker)
        
        async with retry_lock:
            if tracker.retry_key not in retry_tasks:
                break

    async with retry_lock:
        if tracker.retry_key in retry_tasks:
            msg = (
                f"⏰ <b>انتهت محاولات إعادة الشراء</b>\n\n"
                f"📦 المجموعة: <b>{tracker.slug}</b>\n"
                f"⛓️ السلسلة: {tracker.chain_name}\n"
                f"👛 المحفظة: {tracker.wallet_name}\n"
                f"📊 عدد المحاولات: {tracker.attempt_count}\n"
                f"⏳ المدة: {MAX_RETRY_HOURS} ساعات"
            )
            enqueue_message(msg)
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
    detected_chain: str = None,
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

        contract_address = detail.get("contract_address", "")
        stream_chain = detail.get("chain", "ethereum")
        actual_chain = determine_actual_chain(slug, stream_chain, contract_address)
        
        if actual_chain not in ENABLED_CHAINS:
            logging.info(f"⏭️ '{slug}': السلسلة {actual_chain} غير مدعومة.")
            known_external.add(slug)
            return

        info_msg = build_mint_info_message(detail, eth_price_usd, actual_chain)
        enqueue_message(info_msg)

        if not is_free_or_negligible(price_wei, eth_price_usd):
            logging.info(f"💰 '{slug}' على {actual_chain}: مينت مدفوع — تم عرض المعلومات فقط.")
            return

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
        w3 = w3_instances.get(actual_chain)
        if not w3:
            logging.warning(f"⚠️ لا يوجد Web3 للسلسلة {actual_chain}")
            return

        chain_config = CHAINS_CONFIG[actual_chain]
        seadrop_address = chain_config["seadrop_address"]

        for wallet in WALLETS:
            wallet_name = wallet["name"]
            wallet_address = wallet["address"]
            wallet_private_key = wallet["private_key"]

            checks = quick_checks(
                w3, wallet_address, actual_chain, eth_price_usd,
                contract_address, seadrop_address,
            )
            if not checks["pass"]:
                reason = checks["reason"]
                logging.info(f"⏭️ '{slug}' على {actual_chain} - {wallet_name}: {reason}")
                
                result = {
                    "success": False,
                    "reason": reason,
                    "balance_usd": checks.get("balance_usd", 0),
                    "gas_fee_usd": checks.get("gas_fee_usd", 0),
                    "eth_price_usd": eth_price_usd,
                    "chain": actual_chain,
                }
                
                msg = build_result_message(detail, result, actual_chain, wallet_name)
                enqueue_message(msg)
                
                if reason in RETRYABLE_REASONS:
                    async with retry_lock:
                        retry_key = f"{slug}:{wallet_address}:{actual_chain}"
                        if retry_key not in retry_tasks:
                            tracker = RetryTracker(
                                slug=slug,
                                wallet_address=wallet_address,
                                chain_name=actual_chain,
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
                            logging.info(f"🔄 تمت جدولة إعادة المحاولة لـ '{slug}' - {wallet_name} على {actual_chain} (السبب: {reason})")
                
                continue

            tasks.append(
                (wallet_name, actual_chain, wallet_address, wallet_private_key, asyncio.to_thread(
                    attempt_purchase,
                    w3, wallet_private_key, wallet_address, actual_chain,
                    contract_address, seadrop_address,
                    price_wei, max_per_wallet, remaining, eth_price_usd,
                ))
            )

        if not tasks:
            logging.info(f"⏭️ '{slug}': لا توجد محافظ مؤهلة للشراء على {actual_chain}.")
            return

        logging.info(f"🔄 '{slug}' على {actual_chain}: بدء {len(tasks)} محاولة شراء ({len(WALLETS)} محفظة)")

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
                msg = build_result_message(detail, result, chain_name, wallet_name)
                enqueue_message(msg)
                
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
        logging.error(traceback.format_exc())
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
                        
                        chain = (item.get("chain", {}) or {}).get("name", "").lower()
                        slug = (payload.get("collection", {}) or {}).get("slug", "")
                        contract_address = (item.get("contract", {}) or {}).get("address", "")
                        
                        actual_chain = determine_actual_chain(slug, chain, contract_address)
                        
                        if actual_chain not in ENABLED_CHAINS:
                            logging.debug(f"⏭️ سلسلة غير مدعومة: {actual_chain} لـ {slug}")
                            continue

                        from_address = ((payload.get("from_account") or {}).get("address", "") or "").lower()
                        if from_address != ZERO_ADDRESS:
                            continue

                        if not slug or slug in notified or slug in known_external or slug in checking:
                            continue

                        checking.add(slug)
                        logging.info(f"🔍 اكتشاف جديد: '{slug}' على {actual_chain} (من Stream: {chain})")
                        
                        asyncio.create_task(
                            process_with_semaphore(session, slug, notified, known_external, checking, actual_chain)
                        )

            except (websockets.ConnectionClosed, OSError, asyncio.TimeoutError) as e:
                logging.warning(f"⚠️ انقطع الاتصال ({e}). إعادة الاتصال خلال 3 ثوانٍ...")
                await asyncio.sleep(3)
            except Exception as e:
                logging.error(f"خطأ غير متوقع: {e}. إعادة المحاولة خلال 5 ثوانٍ...")
                logging.error(traceback.format_exc())
                await asyncio.sleep(5)

async def process_with_semaphore(
    session: aiohttp.ClientSession,
    slug: str,
    notified: set,
    known_external: set,
    checking: set,
    detected_chain: str = None,
):
    async with semaphore:
        await evaluate_and_buy(session, slug, notified, known_external, checking, detected_chain)

# ===================================================================
# الماسح الدوري
# ===================================================================
async def scan_active_drops(notified: set, known_external: set, checking: set):
    logging.info(f"🔍 الماسح الدوري: بدأ العمل — فحص كل {SCAN_INTERVAL // 60} دقائق")

    async with aiohttp.ClientSession() as session:
        while True:
            try:
                all_drops = []
                
                for chain_name in ENABLED_CHAINS:
                    url = f"{DROPS_API_BASE}?is_minting=true&limit=50"
                    headers = {"x-api-key": OPENSEA_API_KEY}
                    try:
                        async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                            if resp.status == 200:
                                data = await resp.json()
                                drops = data.get("drops") or data.get("results", [])
                                for drop in drops:
                                    slug = drop.get("collection_slug") or drop.get("slug", "")
                                    contract = drop.get("contract_address", "")
                                    chain = determine_actual_chain(slug, chain_name, contract)
                                    if chain == chain_name:
                                        drop["_chain"] = chain_name
                                        all_drops.append(drop)
                    except Exception as e:
                        logging.warning(f"[الماسح] {chain_name} API خطأ: {e}")
                
                logging.info(f"[الماسح] وجد {len(all_drops)} مينت نشط في السوق")

                new_count = 0
                for drop in all_drops:
                    slug = drop.get("collection_slug") or drop.get("slug", "")
                    if not slug:
                        continue
                    if slug in notified or slug in known_external or slug in checking:
                        continue
                    
                    chain = drop.get("_chain", "ethereum")
                    logging.info(f"🔍 [الماسح] اكتشاف: '{slug}' على {chain}")
                    
                    checking.add(slug)
                    new_count += 1
                    asyncio.create_task(
                        process_with_semaphore(session, slug, notified, known_external, checking, chain)
                    )

                if new_count > 0:
                    logging.info(f"[الماسح] أرسل {new_count} مينت جديد للمعالجة")

            except Exception as e:
                logging.error(f"[الماسح] خطأ أثناء المسح: {e}")
                logging.error(traceback.format_exc())

            await asyncio.sleep(SCAN_INTERVAL)

# ===================================================================
# التشغيل الرئيسي
# ===================================================================
async def run():
    logging.info("🚀 بدء تشغيل النظام...")
    
    # إرسال رسالة اختبار فورية
    logging.info("🧪 إرسال رسالة اختبار تيليجرام...")
    test_result = await send_telegram_message_direct("🧪 <b>رسالة اختبار من نظام الشراء التلقائي</b>\n\n✅ تم تشغيل النظام بنجاح")
    if test_result:
        logging.info("✅ رسالة الاختبار تم إرسالها بنجاح")
    else:
        logging.error("❌ فشل إرسال رسالة الاختبار - تحقق من متغيرات TELEGRAM_BOT_TOKEN و TELEGRAM_CHAT_ID")

    # التحقق من المتغيرات
    if not BOT_ENABLED:
        logging.warning("🔴 BOT_ENABLED=false — النظام متوقف عمدًا (وضع الأمان).")
        await send_telegram_message_direct("🔴 البوت شغّال لكن بوضع الإيقاف (BOT_ENABLED=false)")
        return

    if not WALLETS:
        logging.critical("🔴 لا توجد محافظ!")
        await send_telegram_message_direct("🔴 لا توجد محافظ! تأكد من تعبئة PRIVATE_KEY و WALLET_ADDRESS في ملف .env")
        return

    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logging.warning("⚠️ متغيرات تيليجرام غير مكتملة! لن يتم إرسال الإشعارات.")
        await send_telegram_message_direct("⚠️ متغيرات تيليجرام غير مكتملة! لن يتم إرسال الإشعارات.")

    # إرسال رسالة بدء التشغيل
    startup_msg = "🚀 <b>نظام الشراء التلقائي يعمل الآن!</b>\n\n"
    startup_msg += f"📡 السلاسل النشطة: {', '.join([CHAINS_CONFIG[c]['chain_name_display'] for c in ENABLED_CHAINS]) if ENABLED_CHAINS else 'لا توجد'}\n"
    startup_msg += f"👛 المحافظ النشطة: {len(WALLETS)}\n"
    startup_msg += f"🔢 الحد الأقصى للمجموعات المتزامنة: {MAX_CONCURRENT_MINTS}\n"
    startup_msg += f"🔄 نظام إعادة المحاولة: نشط لمدة {MAX_RETRY_HOURS} ساعات\n"
    startup_msg += f"💡 الرصيد يُفحص لكل سلسلة بشكل منفصل"
    
    await send_telegram_message_direct(startup_msg)
    
    # إرسال تقرير الرصيد الأولي
    async with aiohttp.ClientSession() as session:
        await send_balances_report(session)
    
    logging.info("✅ النظام جاهز للعمل")

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
            logging.error(traceback.format_exc())
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)
            continue
        else:
            break

if __name__ == "__main__":
    main()

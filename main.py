"""
النظام المتكامل المحسن: اكتشاف وشراء NFT من المراحل المجانية فقط
مع مراقبة الرصيد لكل سلسلة على حدة
وعدم إيقاف الشراء كلياً عند انخفاض الرصيد في سلسلة واحدة
على Ethereum + Robinhood Chain
🔥 الشراء من جميع المحافظ بشكل متوازي
✅ جميع الإشعارات تظهر بشكل فوري
"""

import asyncio
import json
import logging
import os
import time
import traceback
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass, field
from typing import Optional, Set, Dict, List, Tuple

import aiohttp
import websockets
from dotenv import load_dotenv

from buyer import (
    get_web3_from_config,
    attempt_purchase,
    CHAINS_CONFIG,
    quick_checks,
    is_price_free,
    find_all_free_stages,
    get_all_stages_info,
    RETRYABLE_REASONS,
    get_retry_config,
    calculate_retry_delay,
    get_reason_text,
    MintResult,
    MIN_BALANCE_RESERVE_USD,
    MAX_GAS_FEE_USD,
    FREE_PRICE_THRESHOLD_WEI,
    RetryConfig,
    get_wallet_balance,
)

load_dotenv()

# ===========================================================================
# التهيئة
# ===========================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("main")

# ===========================================================================
# متغيرات البيئة
# ===========================================================================
OPENSEA_API_KEY = os.environ.get("OPENSEA_API_KEY", "").strip()
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
BOT_ENABLED = os.environ.get("BOT_ENABLED", "false").strip().lower() == "true"

# ===========================================================================
# ✅ نظام تيليجرام المحسن - مع تأكيد الإرسال
# ===========================================================================
TELEGRAM_SENDERS = 3
send_queue: asyncio.Queue = asyncio.Queue()
telegram_semaphore = asyncio.Semaphore(TELEGRAM_SENDERS)
# تتبع الرسائل المرسلة للتأكد من عدم فقدانها
sent_count = 0
failed_count = 0

def send_telegram(text: str):
    """إرسال رسالة تيليجرام - مع تسجيل فوري"""
    if not text:
        log.warning("⚠️ محاولة إرسال رسالة فارغة")
        return
    
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log.warning(f"⚠️ تيليجرام غير مهيأ - الرسالة: {text[:100]}...")
        return
    
    try:
        send_queue.put_nowait(text)
        log.info(f"📤 تمت إضافة رسالة إلى قائمة الإرسال: {text[:80]}...")
    except asyncio.QueueFull:
        log.error("❌ قائمة إرسال تيليجرام ممتلئة!")
    except Exception as e:
        log.error(f"❌ خطأ في إضافة رسالة: {e}")

async def telegram_worker(worker_id: int):
    """عامل إرسال تيليجرام مع معالجة أفضل للأخطاء"""
    global sent_count, failed_count
    
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                text = await send_queue.get()
                
                async with telegram_semaphore:
                    try:
                        log.info(f"📤 [عامل {worker_id}] جاري إرسال: {text[:80]}...")
                        
                        async with session.post(
                            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                            data={
                                "chat_id": TELEGRAM_CHAT_ID,
                                "text": text,
                                "parse_mode": "HTML",
                                "disable_web_page_preview": False,
                            },
                            timeout=aiohttp.ClientTimeout(total=10),
                        ) as resp:
                            if resp.status == 200:
                                sent_count += 1
                                log.info(f"✅ [عامل {worker_id}] تم الإرسال بنجاح (#{sent_count})")
                            elif resp.status == 429:
                                log.warning(f"⚠️ [عامل {worker_id}] طلب كثير جداً - انتظار 3 ثواني")
                                await asyncio.sleep(3)
                                # إعادة المحاولة
                                send_queue.put_nowait(text)
                            else:
                                failed_count += 1
                                error_text = await resp.text()
                                log.error(f"❌ [عامل {worker_id}] HTTP {resp.status}: {error_text[:200]}")
                    except asyncio.TimeoutError:
                        failed_count += 1
                        log.error(f"⏰ [عامل {worker_id}] انتهت مهلة الإرسال")
                    except Exception as e:
                        failed_count += 1
                        log.error(f"❌ [عامل {worker_id}] خطأ: {e}")
                    finally:
                        send_queue.task_done()
                        
            except asyncio.CancelledError:
                log.info(f"👋 [عامل {worker_id}] تم إلغاء العامل")
                break
            except Exception as e:
                log.error(f"💥 [عامل {worker_id}] خطأ غير متوقع: {e}")
                await asyncio.sleep(1)

async def telegram_sender():
    """بدء عمال الإرسال"""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log.warning("⚠️ تيليجرام غير مهيأ - لن يتم إرسال رسائل")
        # إنشاء عامل وهمي لمنع تعليق النظام
        while True:
            try:
                text = await send_queue.get()
                log.info(f"📝 [محاكاة] رسالة: {text[:100]}...")
                send_queue.task_done()
            except:
                await asyncio.sleep(1)
        return
    
    workers = [telegram_worker(i) for i in range(TELEGRAM_SENDERS)]
    await asyncio.gather(*workers)

# اختبار تيليجرام عند البدء
def test_telegram():
    """اختبار اتصال تيليجرام"""
    if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
        log.info("📡 اختبار اتصال تيليجرام...")
        send_telegram("✅ <b>اختبار اتصال تيليجرام</b>\n\nالنظام يعمل بشكل صحيح")
    else:
        log.warning("⚠️ تيليجرام غير مهيأ - تأكد من TELEGRAM_BOT_TOKEN و TELEGRAM_CHAT_ID")

# ===========================================================================
# تحميل المحافظ
# ===========================================================================
def load_wallets():
    wallets = []
    wallet_configs = [
        ("المحفظة 1", "PRIVATE_KEY", "WALLET_ADDRESS"),
        ("المحفظة 2", "WALLET_2_PRIVATE_KEY", "WALLET_2_ADDRESS"),
        ("المحفظة 3", "WALLET_3_PRIVATE_KEY", "WALLET_3_ADDRESS"),
        ("المحفظة 4", "WALLET_4_PRIVATE_KEY", "WALLET_4_ADDRESS"),
        ("المحفظة 5", "WALLET_5_PRIVATE_KEY", "WALLET_5_ADDRESS"),
    ]
    for name, pk_var, addr_var in wallet_configs:
        private_key = os.environ.get(pk_var)
        address = os.environ.get(addr_var)
        if private_key and address:
            wallets.append({
                "name": name,
                "private_key": private_key.strip(),
                "address": address.strip(),
            })
            log.info(f"✅ {name}: {address[:10]}...")
    return wallets

WALLETS = load_wallets()

# ===========================================================================
# إعدادات السلاسل
# ===========================================================================
ROBINHOOD_RPC_URL = os.environ.get("ROBINHOOD_RPC_URL", "").strip()
ETHEREUM_RPC_URL = os.environ.get("ETHEREUM_RPC_URL", "").strip()

ENABLED_CHAINS = []
if ROBINHOOD_RPC_URL:
    CHAINS_CONFIG["robinhood"]["rpc_url"] = ROBINHOOD_RPC_URL
    ENABLED_CHAINS.append("robinhood")
if ETHEREUM_RPC_URL:
    CHAINS_CONFIG["ethereum"]["rpc_url"] = ETHEREUM_RPC_URL
    ENABLED_CHAINS.append("ethereum")

w3_instances = {}
for chain_name in ENABLED_CHAINS:
    try:
        w3_instances[chain_name] = get_web3_from_config(CHAINS_CONFIG[chain_name])
        log.info(f"✅ {CHAINS_CONFIG[chain_name]['chain_name_display']} - متصل")
    except Exception as e:
        log.error(f"❌ {CHAINS_CONFIG[chain_name]['chain_name_display']} - فشل: {e}")

# ===========================================================================
# الثوابت
# ===========================================================================
STREAM_URL = f"wss://stream.openseabeta.com/socket/websocket?token={OPENSEA_API_KEY}&vsn=2.0.0"
DROPS_API_BASE = "https://api.opensea.io/api/v2/drops"
LOCAL_TZ = timezone(timedelta(hours=3))

MAX_CONCURRENT = 3
MAX_CONCURRENT_MINTS = 10
HEARTBEAT_INTERVAL = 20

SCAN_INTERVAL = 20
BALANCE_CHECK_INTERVAL = 120
ETH_PRICE_CACHE_TTL = 60
DROPS_LIMIT = 50
MIN_SCAN_INTERVAL = 15
EVENT_CACHE_TTL = 5

# ===========================================================================
# Global State
# ===========================================================================
NOTIFIED: Set[str] = set()
CHECKING: Set[str] = set()
CHECKING_LOCK = asyncio.Lock()
LAST_SCAN: Dict[str, float] = {}

# حالة الرصيد لكل محفظة × سلسلة
WALLET_BALANCES: Dict[str, Dict[str, Dict]] = {}
BALANCE_LOCK = asyncio.Lock()

# المحافظ منخفضة الرصيد لكل سلسلة
LOW_BALANCE_BY_CHAIN: Dict[str, Set[str]] = {}
LOW_BALANCE_NOTIFIED: Set[str] = set()

# ===========================================================================
# تخزين سعر ETH
# ===========================================================================
_eth_price_cache = {"value": None, "ts": 0}

async def get_eth_price(session: aiohttp.ClientSession) -> float:
    now = time.time()
    if _eth_price_cache["value"] and (now - _eth_price_cache["ts"] < ETH_PRICE_CACHE_TTL):
        return _eth_price_cache["value"]
    try:
        async with session.get(
            "https://api.coingecko.com/api/v3/simple/price?ids=ethereum&vs_currencies=usd",
            timeout=aiohttp.ClientTimeout(total=5),
        ) as resp:
            if resp.status == 200:
                data = await resp.json()
                if "ethereum" in data and "usd" in data["ethereum"]:
                    price = data["ethereum"]["usd"]
                    _eth_price_cache["value"] = price
                    _eth_price_cache["ts"] = now
                    return price
    except Exception as e:
        log.warning(f"[السعر] خطأ: {e}")
    return _eth_price_cache["value"] or 3000.0

# ===========================================================================
# ✅ نظام فحص الرصيد لكل سلسلة
# ===========================================================================
async def update_all_balances_all_chains(session: aiohttp.ClientSession):
    """تحديث رصيد جميع المحافظ على جميع السلاسل"""
    global WALLET_BALANCES, LOW_BALANCE_BY_CHAIN, LOW_BALANCE_NOTIFIED
    
    eth_price = await get_eth_price(session)
    
    for chain_name in ENABLED_CHAINS:
        w3 = w3_instances.get(chain_name)
        if not w3:
            continue
        
        chain_display = CHAINS_CONFIG[chain_name]["chain_name_display"]
        
        if chain_name not in LOW_BALANCE_BY_CHAIN:
            LOW_BALANCE_BY_CHAIN[chain_name] = set()
        
        for wallet in WALLETS:
            wallet_address = wallet["address"]
            
            try:
                balance_eth, balance_wei = get_wallet_balance(w3, wallet_address)
                balance_usd = balance_eth * eth_price
                
                async with BALANCE_LOCK:
                    if wallet_address not in WALLET_BALANCES:
                        WALLET_BALANCES[wallet_address] = {}
                    
                    WALLET_BALANCES[wallet_address][chain_name] = {
                        "eth": balance_eth,
                        "usd": balance_usd,
                        "updated": time.time()
                    }
                
                notification_key = f"{wallet_address}:{chain_name}"
                
                if balance_usd < MIN_BALANCE_RESERVE_USD:
                    LOW_BALANCE_BY_CHAIN[chain_name].add(wallet_address)
                    
                    if notification_key not in LOW_BALANCE_NOTIFIED:
                        LOW_BALANCE_NOTIFIED.add(notification_key)
                        msg = (
                            f"⚠️ <b>رصيد منخفض</b>\n"
                            f"👛 {wallet['name']}\n"
                            f"⛓️ {chain_display}\n"
                            f"💰 الرصيد: ${balance_usd:.4f}\n"
                            f"📉 الحد الأدنى: ${MIN_BALANCE_RESERVE_USD}\n"
                            f"⏸️ تم إيقاف الشراء على {chain_display} فقط\n"
                            f"✅ السلاسل الأخرى مستمرة"
                        )
                        send_telegram(msg)
                        log.warning(f"⚠️ {wallet['name']} على {chain_display}: ${balance_usd:.4f}")
                else:
                    if wallet_address in LOW_BALANCE_BY_CHAIN.get(chain_name, set()):
                        LOW_BALANCE_BY_CHAIN[chain_name].discard(wallet_address)
                        
                        if notification_key in LOW_BALANCE_NOTIFIED:
                            LOW_BALANCE_NOTIFIED.discard(notification_key)
                            msg = (
                                f"✅ <b>عودة الرصيد</b>\n"
                                f"👛 {wallet['name']}\n"
                                f"⛓️ {chain_display}\n"
                                f"💰 الرصيد: ${balance_usd:.4f}\n"
                                f"▶️ تم استئناف الشراء على {chain_display}"
                            )
                            send_telegram(msg)
                            log.info(f"✅ {wallet['name']} على {chain_display}: ${balance_usd:.4f}")
                            
            except Exception as e:
                log.error(f"❌ خطأ في فحص رصيد {wallet['name']} على {chain_display}: {e}")

async def balance_monitor():
    """مراقبة دورية للرصيد"""
    log.info(f"💰 مراقبة الرصيد: كل {BALANCE_CHECK_INTERVAL} ثانية")
    
    async with aiohttp.ClientSession() as session:
        # تشغيل فحص أولي فوري
        log.info("🔍 فحص أولي للأرصدة...")
        await update_all_balances_all_chains(session)
        await log_balance_summary()
        
        while True:
            try:
                await asyncio.sleep(BALANCE_CHECK_INTERVAL)
                await update_all_balances_all_chains(session)
                await log_balance_summary()
            except Exception as e:
                log.error(f"[رصيد] خطأ: {e}")

async def log_balance_summary():
    """طباعة ملخص الرصيد"""
    async with BALANCE_LOCK:
        lines = ["\n📊 ملخص الرصيد:"]
        
        for wallet in WALLETS:
            addr = wallet["address"]
            wallet_balances = WALLET_BALANCES.get(addr, {})
            
            chain_summaries = []
            for chain_name in ENABLED_CHAINS:
                chain_data = wallet_balances.get(chain_name, {})
                balance_usd = chain_data.get("usd", 0)
                is_low = addr in LOW_BALANCE_BY_CHAIN.get(chain_name, set())
                
                status = "🔴" if is_low else "🟢"
                chain_display = CHAINS_CONFIG[chain_name]["chain_name_display"][:10]
                chain_summaries.append(f"{status} {chain_display}: ${balance_usd:.4f}")
            
            lines.append(f"  {wallet['name']}: {' | '.join(chain_summaries)}")
        
        log.info("\n".join(lines))

def is_wallet_balance_ok(wallet_address: str, chain_name: str) -> bool:
    """التحقق من الرصيد على سلسلة محددة"""
    return wallet_address not in LOW_BALANCE_BY_CHAIN.get(chain_name, set())

def get_wallet_balance_for_chain(wallet_address: str, chain_name: str) -> Dict:
    """الحصول على رصيد محفظة على سلسلة محددة"""
    return WALLET_BALANCES.get(wallet_address, {}).get(chain_name, {"eth": 0, "usd": 0, "updated": 0})

# ===========================================================================
# نظام تخزين مؤقت
# ===========================================================================
class DropCache:
    def __init__(self, ttl: int = 30):
        self.cache: Dict[str, tuple] = {}
        self.ttl = ttl
    
    def get(self, slug: str) -> Optional[dict]:
        if slug in self.cache:
            data, timestamp = self.cache[slug]
            if time.time() - timestamp < self.ttl:
                return data
            del self.cache[slug]
        return None
    
    def set(self, slug: str, data: dict):
        self.cache[slug] = (data, time.time())
    
    def clear(self):
        now = time.time()
        for slug in list(self.cache.keys()):
            _, timestamp = self.cache[slug]
            if now - timestamp > self.ttl:
                del self.cache[slug]

drop_cache = DropCache(ttl=30)

# ===========================================================================
# نظام ماسح ذكي
# ===========================================================================
class SmartScanner:
    def __init__(self):
        self.pending_slugs: Set[str] = set()
        self.last_scan_time: Dict[str, float] = {}
        self.scan_lock = asyncio.Lock()
        self.batch_size = 10
    
    async def add_slug(self, slug: str):
        async with self.scan_lock:
            if slug not in self.last_scan_time or \
               time.time() - self.last_scan_time[slug] > MIN_SCAN_INTERVAL:
                self.pending_slugs.add(slug)
    
    async def process_batch(self, session: aiohttp.ClientSession):
        async with self.scan_lock:
            batch = list(self.pending_slugs)[:self.batch_size]
            for slug in batch:
                self.pending_slugs.discard(slug)
                self.last_scan_time[slug] = time.time()
        
        if not batch:
            return
        
        for slug in batch:
            await handle_discovered_mint(session, slug, "ethereum")
            await asyncio.sleep(0.3)

smart_scanner = SmartScanner()

# ===========================================================================
# OpenSea API
# ===========================================================================
async def fetch_drop_detail(session: aiohttp.ClientSession, slug: str, retries: int = 2):
    cached_data = drop_cache.get(slug)
    if cached_data:
        return True, cached_data
    
    url = f"{DROPS_API_BASE}/{slug}"
    headers = {"x-api-key": OPENSEA_API_KEY}
    
    for attempt in range(retries):
        try:
            async with session.get(
                url, headers=headers, timeout=aiohttp.ClientTimeout(total=5),
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    drop_cache.set(slug, data)
                    return True, data
                elif resp.status == 429:
                    await asyncio.sleep(1)
                else:
                    return False, None
        except Exception:
            if attempt == retries - 1:
                return None, None
            await asyncio.sleep(0.5)
    
    return None, None

# ===========================================================================
# نظام إعادة المحاولة
# ===========================================================================
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
    stage_name: str = ""
    stage: dict = field(default_factory=dict)
    original_reason: str = ""
    config: RetryConfig = field(default_factory=lambda: get_retry_config("gas_too_high"))
    start_time: float = field(default_factory=time.time)
    attempt_count: int = 0
    failure_reasons: list = field(default_factory=list)
    
    @property
    def retry_key(self) -> str:
        return f"{self.slug}:{self.wallet_address}:{self.chain_name}:{self.stage_name}"
    
    @property
    def should_stop(self) -> bool:
        hours_passed = (time.time() - self.start_time) / 3600
        return hours_passed >= self.config.max_total_hours or self.attempt_count >= self.config.max_attempts

retry_tasks: Dict[str, RetryTracker] = {}
retry_lock = asyncio.Lock()
mint_semaphore = asyncio.Semaphore(MAX_CONCURRENT_MINTS)

# ===========================================================================
# ✅ بناء الرسائل - مع تحسين الشكل
# ===========================================================================
def build_free_mint_report(detail: dict, chain_name: str, eth_price: float, free_stages: dict) -> str:
    """تقرير عن المراحل المجانية"""
    name = detail.get("collection_name") or detail.get("collection_slug", "غير معروف")
    url = detail.get("opensea_url", "")
    chain_display = CHAINS_CONFIG.get(chain_name, {}).get("chain_name_display", chain_name)
    max_supply = int(detail.get("max_supply") or 0)
    total_supply = int(detail.get("total_supply") or 0)
    remaining = max(0, max_supply - total_supply)
    
    active_stages = free_stages.get("active", [])
    upcoming_stages = free_stages.get("upcoming", [])
    
    ready_wallets = sum(1 for w in WALLETS if is_wallet_balance_ok(w["address"], chain_name))
    low_wallets = len(WALLETS) - ready_wallets
    
    lines = [
        f"🎁 <b>مراحل مجانية مكتشفة!</b>",
        f"",
        f"📦 <b>{name}</b>",
        f"⛓️ {chain_display}",
        f"📊 المتبقي: {remaining:,}/{max_supply:,}",
        f"👛 المحافظ الجاهزة: {ready_wallets}/{len(WALLETS)}",
    ]
    
    if low_wallets > 0:
        lines.append(f"⚠️ محافظ منخفضة الرصيد: {low_wallets}")
    
    lines.append("")
    
    if active_stages:
        lines.append(f"✅ <b>مراحل نشطة ({len(active_stages)}):</b>")
        for stage in active_stages:
            stage_name = stage.get("stage", "مرحلة")
            max_wallet = stage.get("max_per_wallet", "غير محدد")
            lines.append(f"  • {stage_name} (الحد: {max_wallet})")
        lines.append("")
    
    if upcoming_stages:
        lines.append(f"⏳ <b>مراحل قادمة ({len(upcoming_stages)}):</b>")
        for stage in upcoming_stages:
            stage_name = stage.get("stage", "مرحلة")
            start_dt = stage.get("start_dt")
            if start_dt:
                start_local = start_dt.astimezone(LOCAL_TZ).strftime("%H:%M")
                lines.append(f"  • {stage_name} (يبدأ: {start_local})")
        lines.append("")
    
    lines.append(f"🔗 <a href='{url}'>OpenSea</a>")
    
    return "\n".join(lines)


def build_multi_wallet_success_msg(detail: dict, results: List[MintResult], chain_name: str, stage_name: str) -> str:
    """رسالة نجاح لعدة محافظ"""
    name = detail.get("collection_name") or detail.get("collection_slug", "")
    url = detail.get("opensea_url", "")
    chain_display = CHAINS_CONFIG.get(chain_name, {}).get("chain_name_display", chain_name)
    explorer_url = CHAINS_CONFIG.get(chain_name, {}).get("explorer_url", "")
    
    success_results = [r for r in results if r.success]
    fail_results = [r for r in results if not r.success]
    
    low_balance_fails = [r for r in fail_results if r.reason in ["balance_too_low", "insufficient_funds"]]
    other_fails = [r for r in fail_results if r.reason not in ["balance_too_low", "insufficient_funds"]]
    
    lines = [
        f"📊 <b>نتائج الشراء</b>",
        f"",
        f"📦 <b>{name}</b>",
        f"🎯 {stage_name}",
        f"⛓️ {chain_display}",
        f"",
    ]
    
    if success_results:
        lines.append(f"✅ <b>نجاح ({len(success_results)} محفظة):</b>")
        for r in success_results:
            gas_info = f"${r.gas_used_usd:.4f}" if r.gas_used_usd > 0 else "غير معروف"
            tx_link = f"<a href='{explorer_url}{r.tx_hash}'>TX</a>" if r.tx_hash else "لا يوجد"
            lines.append(f"  • {r.wallet_name or r.wallet[:10]}: {r.quantity} قطعة | ⛽ {gas_info} | {tx_link}")
    
    if other_fails:
        lines.append(f"")
        lines.append(f"❌ <b>فشل ({len(other_fails)} محفظة):</b>")
        for r in other_fails:
            reason = r.reason_text or get_reason_text(r.reason)
            lines.append(f"  • {r.wallet_name or r.wallet[:10]}: {reason}")
    
    if low_balance_fails:
        lines.append(f"")
        lines.append(f"💰 <b>رصيد منخفض ({len(low_balance_fails)} محفظة):</b>")
        for r in low_balance_fails:
            lines.append(f"  • {r.wallet_name or r.wallet[:10]}: ${r.balance_usd:.4f}")
    
    lines.append(f"")
    lines.append(f"🔗 <a href='{url}'>OpenSea</a>")
    
    return "\n".join(lines)

# ===========================================================================
# ✅ معالجة الشراء - مع إشعارات فورية
# ===========================================================================
async def process_stage_for_all_wallets(
    session: aiohttp.ClientSession,
    slug: str,
    detail: dict,
    stage: dict,
    chain_name: str,
) -> List[MintResult]:
    """معالجة مرحلة لجميع المحافظ"""
    
    stage_name = stage.get("stage", "عام")
    price_wei = int(stage.get("price_wei", "0") or "0")
    max_per_wallet = stage.get("max_per_wallet")
    if max_per_wallet is not None:
        max_per_wallet = int(max_per_wallet)
    
    max_supply = int(detail.get("max_supply") or 0)
    total_supply = int(detail.get("total_supply") or 0)
    remaining = max(0, max_supply - total_supply)
    
    if remaining <= 0:
        log.info(f"⏭️ '{slug}' - نفذت الكمية")
        return []
    
    contract_address = detail.get("contract_address")
    if not contract_address:
        log.warning(f"⚠️ '{slug}': لا يوجد contract_address")
        return []
    
    eth_price = await get_eth_price(session)
    
    if not is_price_free(price_wei):
        return []
    
    chain_display = CHAINS_CONFIG[chain_name]["chain_name_display"]
    
    wallet_tasks = []
    valid_wallets = []
    skipped_wallets = []
    
    for wallet in WALLETS:
        if not is_wallet_balance_ok(wallet["address"], chain_name):
            balance_data = get_wallet_balance_for_chain(wallet["address"], chain_name)
            skipped_wallets.append(f"{wallet['name']} (${balance_data.get('usd', 0):.4f})")
            continue
        valid_wallets.append(wallet)
        wallet_tasks.append(
            process_stage_for_wallet(session, slug, detail, stage, chain_name, wallet, eth_price)
        )
    
    if not wallet_tasks:
        return []
    
    log.info(f"🔥 '{slug}' - {stage_name} على {chain_display}: شراء من {len(wallet_tasks)} محفظة")
    
    # إشعار ببدء الشراء
    send_telegram(
        f"🚀 <b>بدء الشراء</b>\n"
        f"📦 {detail.get('collection_name', slug)}\n"
        f"⛓️ {chain_display}\n"
        f"👛 {len(wallet_tasks)} محفظة\n"
        f"⏭️ تم تخطي: {len(skipped_wallets)}"
    )
    
    results = await asyncio.gather(*wallet_tasks, return_exceptions=True)
    
    final_results = []
    for r in results:
        if isinstance(r, Exception):
            log.error(f"❌ خطأ: {r}")
        elif r is not None:
            r.chain_name = chain_name
            final_results.append(r)
    
    if final_results:
        success_count = sum(1 for r in final_results if r.success)
        
        # إرسال تقرير النتائج
        msg = build_multi_wallet_success_msg(detail, final_results, chain_name, stage_name)
        send_telegram(msg)
        
        # إشعار فردي لكل نجاح
        for r in final_results:
            if r.success:
                send_telegram(
                    f"✅ <b>تم الشراء!</b>\n"
                    f"👛 {r.wallet_name or r.wallet[:10]}\n"
                    f"📦 {detail.get('collection_name', slug)}\n"
                    f"📊 {r.quantity} قطعة\n"
                    f"⛽ ${r.gas_used_usd:.4f}"
                )
        
        log.info(f"📊 '{slug}': ✅{success_count} ❌{len(final_results) - success_count} ⏭️{len(skipped_wallets)}")
    
    return final_results


async def process_stage_for_wallet(
    session: aiohttp.ClientSession,
    slug: str,
    detail: dict,
    stage: dict,
    chain_name: str,
    wallet: dict,
    eth_price: float,
) -> Optional[MintResult]:
    """معالجة مرحلة لمحفظة واحدة"""
    wallet_name = wallet["name"]
    wallet_address = wallet["address"]
    wallet_private_key = wallet["private_key"]
    
    w3 = w3_instances.get(chain_name)
    if not w3:
        return None
    
    seadrop_address = CHAINS_CONFIG[chain_name]["seadrop_address"]
    stage_name = stage.get("stage", "عام")
    price_wei = int(stage.get("price_wei", "0") or "0")
    max_per_wallet = stage.get("max_per_wallet")
    if max_per_wallet is not None:
        max_per_wallet = int(max_per_wallet)
    
    max_supply = int(detail.get("max_supply") or 0)
    total_supply = int(detail.get("total_supply") or 0)
    remaining = max(0, max_supply - total_supply)
    
    contract_address = detail.get("contract_address")
    chain_display = CHAINS_CONFIG[chain_name]["chain_name_display"]
    
    checks = quick_checks(w3, wallet_address, eth_price, contract_address, seadrop_address, 1, price_wei)
    
    if not checks["pass"]:
        reason = checks["reason"]
        result = MintResult(
            success=False,
            wallet=wallet_address,
            wallet_name=wallet_name,
            reason=reason,
            reason_text=get_reason_text(reason),
            balance_usd=checks.get("balance_usd", 0),
            chain_name=chain_name,
        )
        
        # إشعار فوري بالفشل
        if reason not in ["balance_too_low", "insufficient_funds"]:
            send_telegram(
                f"❌ <b>فشل الفحص</b>\n"
                f"👛 {wallet_name}\n"
                f"📦 {detail.get('collection_name', slug)}\n"
                f"⚠️ {get_reason_text(reason)}"
            )
        
        if reason in RETRYABLE_REASONS and reason not in ["balance_too_low", "insufficient_funds"]:
            await schedule_retry(session, slug, detail, stage, chain_name, wallet, reason)
        
        return result
    
    log.info(f"🚀 '{slug}' - {wallet_name} على {chain_display}: بدء الشراء...")
    
    async with mint_semaphore:
        result = await asyncio.to_thread(
            attempt_purchase,
            w3=w3,
            private_key=wallet_private_key,
            wallet_address=wallet_address,
            nft_contract=contract_address,
            seadrop_address=seadrop_address,
            price_wei=price_wei,
            max_per_wallet=max_per_wallet,
            remaining_supply=remaining,
            eth_price_usd=eth_price,
        )
    
    if result:
        result.wallet_name = wallet_name
        result.chain_name = chain_name
    
    if result and result.success:
        log.info(f"✅ '{slug}' - {wallet_name}: نجاح!")
        send_telegram(
            f"✅ <b>تم الشراء!</b>\n"
            f"👛 {wallet_name}\n"
            f"📦 {detail.get('collection_name', slug)}\n"
            f"⛓️ {chain_display}\n"
            f"🎯 {stage_name}\n"
            f"📊 {result.quantity} قطعة\n"
            f"⛽ ${result.gas_used_usd:.4f}"
        )
        await update_single_wallet_balance(session, wallet_address, chain_name)
    elif result:
        reason_text = result.reason_text or get_reason_text(result.reason)
        log.info(f"❌ '{slug}' - {wallet_name}: {reason_text}")
        
        if result.reason not in ["balance_too_low", "insufficient_funds"]:
            send_telegram(
                f"❌ <b>فشل الشراء</b>\n"
                f"👛 {wallet_name}\n"
                f"📦 {detail.get('collection_name', slug)}\n"
                f"⚠️ {reason_text}"
            )
        
        if result.reason in RETRYABLE_REASONS and result.reason not in ["balance_too_low", "insufficient_funds"]:
            await schedule_retry(session, slug, detail, stage, chain_name, wallet, result.reason)
    
    return result


async def update_single_wallet_balance(session: aiohttp.ClientSession, wallet_address: str, chain_name: str):
    """تحديث رصيد محفظة واحدة"""
    try:
        eth_price = await get_eth_price(session)
        w3 = w3_instances.get(chain_name)
        if not w3:
            return
        
        balance_eth, _ = get_wallet_balance(w3, wallet_address)
        balance_usd = balance_eth * eth_price
        
        async with BALANCE_LOCK:
            if wallet_address not in WALLET_BALANCES:
                WALLET_BALANCES[wallet_address] = {}
            
            WALLET_BALANCES[wallet_address][chain_name] = {
                "eth": balance_eth,
                "usd": balance_usd,
                "updated": time.time()
            }
            
            if balance_usd < MIN_BALANCE_RESERVE_USD:
                if chain_name not in LOW_BALANCE_BY_CHAIN:
                    LOW_BALANCE_BY_CHAIN[chain_name] = set()
                LOW_BALANCE_BY_CHAIN[chain_name].add(wallet_address)
            else:
                if chain_name in LOW_BALANCE_BY_CHAIN:
                    LOW_BALANCE_BY_CHAIN[chain_name].discard(wallet_address)
    except Exception as e:
        log.error(f"❌ خطأ في تحديث رصيد {wallet_address[:10]}: {e}")


async def schedule_retry(session, slug, detail, stage, chain_name, wallet, reason):
    """جدولة إعادة المحاولة"""
    if reason in ["balance_too_low", "insufficient_funds"]:
        return
    
    stage_name = stage.get("stage", "عام")
    price_wei = int(stage.get("price_wei", "0") or "0")
    max_per_wallet = stage.get("max_per_wallet")
    if max_per_wallet is not None:
        max_per_wallet = int(max_per_wallet)
    
    max_supply = int(detail.get("max_supply") or 0)
    total_supply = int(detail.get("total_supply") or 0)
    remaining = max(0, max_supply - total_supply)
    
    config = get_retry_config(reason)
    
    async with retry_lock:
        key = f"{slug}:{wallet['address']}:{chain_name}:{stage_name}"
        
        if key in retry_tasks:
            existing = retry_tasks[key]
            existing.failure_reasons.append(reason)
            return
        
        tracker = RetryTracker(
            slug=slug,
            wallet_address=wallet["address"],
            chain_name=chain_name,
            detail=detail,
            wallet_name=wallet["name"],
            wallet_private_key=wallet["private_key"],
            price_wei=price_wei,
            max_per_wallet=max_per_wallet,
            remaining_supply=remaining,
            stage_name=stage_name,
            stage=stage,
            original_reason=reason,
            config=config,
        )
        retry_tasks[key] = tracker
        
        send_telegram(
            f"🔄 <b>إعادة محاولة</b>\n"
            f"👛 {wallet['name']}\n"
            f"📦 {detail.get('collection_name', slug)}\n"
            f"⚠️ {get_reason_text(reason)}\n"
            f"⏱️ كل {config.base_delay} ثانية"
        )
        
        asyncio.create_task(retry_loop(session, tracker))


async def retry_loop(session: aiohttp.ClientSession, tracker: RetryTracker):
    """حلقة إعادة المحاولة"""
    key = tracker.retry_key
    name = tracker.detail.get("collection_name", tracker.slug)
    chain_display = CHAINS_CONFIG.get(tracker.chain_name, {}).get("chain_name_display", tracker.chain_name)
    
    while True:
        tracker.attempt_count += 1
        delay = calculate_retry_delay(tracker.config, tracker.attempt_count)
        
        await asyncio.sleep(delay)
        
        async with retry_lock:
            if key not in retry_tasks or tracker.should_stop:
                retry_tasks.pop(key, None)
                return
        
        if not is_wallet_balance_ok(tracker.wallet_address, tracker.chain_name):
            continue
        
        found, updated = await fetch_drop_detail(session, tracker.slug)
        if found and updated:
            tracker.detail = updated
            ms = int(updated.get("max_supply") or 0)
            ts = int(updated.get("total_supply") or 0)
            tracker.remaining_supply = max(0, ms - ts)
            
            if tracker.remaining_supply <= 0:
                async with retry_lock:
                    retry_tasks.pop(key, None)
                return
        
        w3 = w3_instances.get(tracker.chain_name)
        if not w3:
            continue
        
        eth_price = await get_eth_price(session)
        contract = tracker.detail.get("contract_address")
        seadrop = CHAINS_CONFIG[tracker.chain_name]["seadrop_address"]
        
        async with mint_semaphore:
            result = await asyncio.to_thread(
                attempt_purchase,
                w3=w3,
                private_key=tracker.wallet_private_key,
                wallet_address=tracker.wallet_address,
                nft_contract=contract,
                seadrop_address=seadrop,
                price_wei=tracker.price_wei,
                max_per_wallet=tracker.max_per_wallet,
                remaining_supply=tracker.remaining_supply,
                eth_price_usd=eth_price,
            )
        
        if result and result.success:
            ex = CHAINS_CONFIG.get(tracker.chain_name, {}).get("explorer_url", "")
            send_telegram(
                f"✅ <b>نجحت الإعادة!</b>\n"
                f"📦 {name}\n"
                f"👛 {tracker.wallet_name}\n"
                f"⛓️ {chain_display}\n"
                f"🔄 محاولة #{tracker.attempt_count}\n"
                f"📊 {result.quantity} قطعة\n"
                f"🔗 <a href='{ex}{result.tx_hash}'>المعاملة</a>"
            )
            async with retry_lock:
                retry_tasks.pop(key, None)
            await update_single_wallet_balance(session, tracker.wallet_address, tracker.chain_name)
            return

# ===========================================================================
# ✅ معالجة المينت المكتشف
# ===========================================================================
async def handle_discovered_mint(session, slug, chain_name):
    """معالجة مينت مكتشف"""
    global NOTIFIED, CHECKING
    
    try:
        found, detail = await fetch_drop_detail(session, slug)
        
        if not found or not detail:
            async with CHECKING_LOCK:
                CHECKING.discard(slug)
            return
        
        ms = int(detail.get("max_supply") or 0)
        ts = int(detail.get("total_supply") or 0)
        if ms - ts <= 0:
            async with CHECKING_LOCK:
                CHECKING.discard(slug)
                NOTIFIED.add(slug)
            return
        
        eth_price = await get_eth_price(session)
        free_stages = find_all_free_stages(detail, WALLETS)
        
        has_active = len(free_stages["active"]) > 0
        has_upcoming = len(free_stages["upcoming"]) > 0
        
        if not has_active and not has_upcoming:
            async with CHECKING_LOCK:
                CHECKING.discard(slug)
            return
        
        chain_display = CHAINS_CONFIG[chain_name]["chain_name_display"]
        ready_wallets = sum(1 for w in WALLETS if is_wallet_balance_ok(w["address"], chain_name))
        
        log.info(f"🎁 '{slug}' على {chain_display}: {len(free_stages['active'])} نشطة | 👛 {ready_wallets}/{len(WALLETS)}")
        
        # ✅ إرسال تقرير الاكتشاف
        report = build_free_mint_report(detail, chain_name, eth_price, free_stages)
        send_telegram(report)
        
        async with CHECKING_LOCK:
            NOTIFIED.add(slug)
        
        if has_active:
            for stage in free_stages["active"]:
                await process_stage_for_all_wallets(session, slug, detail, stage, chain_name)
        
        if has_upcoming:
            for stage in free_stages["upcoming"]:
                start_dt = stage.get("start_dt")
                if start_dt:
                    wait_seconds = max(0, (start_dt - datetime.now(timezone.utc)).total_seconds())
                    if wait_seconds > 0:
                        asyncio.create_task(wait_and_mint(session, slug, detail, stage, chain_name, start_dt))
        
        async with CHECKING_LOCK:
            CHECKING.discard(slug)
    
    except Exception as e:
        log.error(f"❌ '{slug}': {e}\n{traceback.format_exc()}")
        async with CHECKING_LOCK:
            CHECKING.discard(slug)


async def wait_and_mint(session, slug, detail, stage, chain_name, start_time):
    """انتظار مرحلة قادمة"""
    wait_seconds = max(0, (start_time - datetime.now(timezone.utc)).total_seconds())
    
    if wait_seconds > 0:
        stage_name = stage.get("stage", "مرحلة")
        start_local = start_time.astimezone(LOCAL_TZ).strftime("%H:%M")
        log.info(f"⏰ '{slug}' - {stage_name}: انتظار {wait_seconds/60:.1f} دقيقة")
        
        send_telegram(
            f"⏰ <b>في الانتظار</b>\n"
            f"📦 {detail.get('collection_name', slug)}\n"
            f"🎯 {stage_name}\n"
            f"🕐 يبدأ: {start_local}\n"
            f"⏳ متبقي: {wait_seconds/60:.1f} دقيقة"
        )
        
        await asyncio.sleep(wait_seconds + 2)
    
    found, updated = await fetch_drop_detail(session, slug)
    detail_to_use = updated if found else detail
    
    log.info(f"🚀 '{slug}': بدء المرحلة القادمة!")
    await process_stage_for_all_wallets(session, slug, detail_to_use, stage, chain_name)

# ===========================================================================
# ✅ مستمع WebSocket
# ===========================================================================
async def listen_opensea():
    """الاستماع إلى OpenSea Stream"""
    sem = asyncio.Semaphore(MAX_CONCURRENT)
    event_cache: Dict[str, float] = {}
    
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                async with websockets.connect(STREAM_URL, ping_interval=None, open_timeout=15) as ws:
                    log.info("🚀 متصل بـ OpenSea Stream")
                    await ws.send(json.dumps(["1", "1", "collection:*", "phx_join", {}]))
                    
                    last_hb = time.time()
                    
                    async for raw in ws:
                        if time.time() - last_hb > HEARTBEAT_INTERVAL:
                            await ws.send(json.dumps([None, "2", "phoenix", "heartbeat", {}]))
                            last_hb = time.time()
                        
                        try:
                            parsed = json.loads(raw)
                        except:
                            continue
                        
                        if not (isinstance(parsed, list) and len(parsed) == 5):
                            continue
                        
                        _, _, _, event, pw = parsed
                        if event != "item_transferred":
                            continue
                        
                        payload = (pw or {}).get("payload", {})
                        chain = ((payload.get("item", {})).get("chain", {}) or {}).get("name", "")
                        
                        if chain not in ("robinhood", "ethereum"):
                            continue
                        
                        slug = (payload.get("collection", {}) or {}).get("slug", "")
                        if not slug:
                            continue
                        
                        now = time.time()
                        if slug in event_cache and now - event_cache[slug] < EVENT_CACHE_TTL:
                            continue
                        event_cache[slug] = now
                        
                        for s in list(event_cache.keys()):
                            if now - event_cache[s] > 60:
                                del event_cache[s]
                        
                        async with CHECKING_LOCK:
                            if slug in NOTIFIED or slug in CHECKING:
                                continue
                            CHECKING.add(slug)
                        
                        log.info(f"🔍 اكتشاف: '{slug}' على {chain}")
                        
                        async with sem:
                            await handle_discovered_mint(session, slug, chain)
            
            except (websockets.ConnectionClosed, OSError, asyncio.TimeoutError) as e:
                log.warning(f"⚠️ انقطع: {e}")
                await asyncio.sleep(2)
            except Exception as e:
                log.error(f"❌ {e}")
                await asyncio.sleep(3)

# ===========================================================================
# ✅ الماسح الدوري
# ===========================================================================
async def scan_active_drops():
    """مسح دوري"""
    log.info(f"🔍 ماسح ذكي: كل {SCAN_INTERVAL} ثانية")
    
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                url = f"{DROPS_API_BASE}?is_minting=true&limit={DROPS_LIMIT}"
                headers = {"x-api-key": OPENSEA_API_KEY}
                
                async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        drops = data.get("drops") or data.get("results", [])
                        
                        for drop in drops:
                            slug = drop.get("collection_slug") or drop.get("slug", "")
                            if not slug:
                                continue
                            
                            async with CHECKING_LOCK:
                                if slug in NOTIFIED or slug in CHECKING:
                                    continue
                            
                            chain = drop.get("chain") or "ethereum"
                            if isinstance(chain, dict):
                                chain = chain.get("name", "ethereum")
                            
                            if chain not in ENABLED_CHAINS:
                                continue
                            
                            await smart_scanner.add_slug(slug)
                        
                        await smart_scanner.process_batch(session)
                        
            except Exception as e:
                log.error(f"[ماسح] {e}")
            
            await asyncio.sleep(SCAN_INTERVAL)

# ===========================================================================
# ✅ تنظيف دوري
# ===========================================================================
async def cleanup_task():
    """تنظيف دوري"""
    log.info("🧹 بدء مهمة التنظيف")
    
    while True:
        try:
            drop_cache.clear()
            
            async with CHECKING_LOCK:
                if len(NOTIFIED) > 1000:
                    NOTIFIED.clear()
                    log.info("🧹 تم تنظيف NOTIFIED")
            
            # تقرير دوري عن حالة النظام
            log.info(f"📊 الحالة: NOTIFIED={len(NOTIFIED)}, CHECKING={len(CHECKING)}, "
                    f"إعادة المحاولة={len(retry_tasks)}, "
                    f"رسائل مرسلة={sent_count}, فشلت={failed_count}")
            
        except Exception as e:
            log.error(f"[تنظيف] {e}")
        
        await asyncio.sleep(3600)

# ===========================================================================
# ✅ التشغيل الرئيسي
# ===========================================================================
async def run():
    """تشغيل النظام"""
    if not BOT_ENABLED:
        log.warning("🔴 BOT_ENABLED=false")
        send_telegram("🔴 <b>البوت متوقف</b>")
        await telegram_sender()
        return
    
    if not WALLETS:
        log.critical("🔴 لا توجد محافظ!")
        send_telegram("🔴 <b>لا توجد محافظ!</b>")
        await telegram_sender()
        return
    
    if not ENABLED_CHAINS:
        log.critical("🔴 لا توجد سلاسل!")
        send_telegram("🔴 <b>لا توجد سلاسل!</b>")
        await telegram_sender()
        return
    
    # تهيئة هياكل البيانات
    for chain_name in ENABLED_CHAINS:
        LOW_BALANCE_BY_CHAIN[chain_name] = set()
    
    chains_list = "\n".join([f"  • {CHAINS_CONFIG[c]['chain_name_display']}" for c in ENABLED_CHAINS])
    
    # ✅ رسالة بدء التشغيل
    status_msg = (
        f"✅ <b>بدء النظام</b>\n\n"
        f"📡 <b>السلاسل:</b>\n{chains_list}\n"
        f"👛 <b>المحافظ:</b> {len(WALLETS)}\n"
        f"🔥 <b>الشراء:</b> جميع المحافظ (متوازي)\n"
        f"🎯 <b>النوع:</b> مجاني فقط\n"
        f"💰 <b>الرصيد الأدنى:</b> ${MIN_BALANCE_RESERVE_USD}\n"
        f"🔍 <b>فحص الرصيد:</b> لكل سلسلة على حدة\n"
        f"⚠️ <b>الرصيد المنخفض:</b> يوقف السلسلة المنخفضة فقط\n"
        f"⛽ <b>أقصى غاز:</b> ${MAX_GAS_FEE_USD}"
    )
    
    send_telegram(status_msg)
    log.info("🚀 بدء التشغيل...")
    
    # اختبار تيليجرام
    test_telegram()
    
    await asyncio.gather(
        listen_opensea(),
        scan_active_drops(),
        balance_monitor(),
        cleanup_task(),
        telegram_sender(),
    )


def main():
    """نقطة الدخول"""
    backoff = 2
    while True:
        try:
            asyncio.run(run())
        except KeyboardInterrupt:
            log.info("👋 تم الإيقاف يدوياً")
            break
        except Exception as e:
            log.critical(f"💥 توقف: {e}\n{traceback.format_exc()}")
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)


if __name__ == "__main__":
    main()

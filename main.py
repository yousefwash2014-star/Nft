"""
النظام المتكامل: اكتشاف وشراء NFT مع عرض تلقائي في السوق
- عرض جميع الرموز المملوكة
- إعادة المحاولة للفاشلة كل 30 دقيقة
- إعادة عرض الناجحة كل 4 ساعات
- حد أقصى 50 رمز لكل عملية
- غاز حقيقي | سرعة 10x | فحص مزدوج | إعادة كل 3ث | إشعار كل 100م
"""

import asyncio
import json
import logging
import os
import time
import traceback
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass, field
from typing import Optional, Set, Dict, List, Any

import aiohttp
import websockets
from dotenv import load_dotenv

from buyer import (
    get_web3_from_config,
    attempt_purchase,
    CHAINS_CONFIG,
    is_price_free,
    find_all_free_stages,
    RETRYABLE_REASONS,
    get_retry_config,
    calculate_retry_delay,
    get_reason_text,
    MintResult,
    MIN_BALANCE_RESERVE_USD,
    MAX_GAS_FEE_USD,
    RetryConfig,
    get_wallet_balance,
    is_reason_retryable,
    is_reason_permanent,
    parse_stage_time,
    # دوال عرض الرموز الجديدة
    ListingManager,
    list_all_owned_tokens,
    retry_failed_listings,
    relist_successful_tokens,
    LISTING_RETRY_INTERVAL,
    LISTING_RELIST_INTERVAL,
    MAX_TOKENS_PER_LISTING_BATCH,
    MAX_LISTING_PRICE_USD,
    get_listing_fees,
    calculate_listing_price,
)

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("main")

# ===================================================================
# المتغيرات العامة
# ===================================================================
OPENSEA_API_KEY = os.environ.get("OPENSEA_API_KEY", "").strip()
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
BOT_ENABLED = os.environ.get("BOT_ENABLED", "false").strip().lower() == "true"

# ===================================================================
# نظام تيليجرام
# ===================================================================
send_queue = asyncio.Queue()
telegram_sem = asyncio.Semaphore(5)
sent = 0
failed = 0

def send_telegram(text: str):
    if not text or not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        send_queue.put_nowait(text)
    except:
        pass

SENT: Dict[str, Set[str]] = {
    k: set() for k in ["discovery", "result", "status", "retry", "soldout", "permanent", "progress", "listing"]
}

def send_once(cat: str, key: str, text: str):
    if key not in SENT[cat]:
        SENT[cat].add(key)
        send_telegram(text)

async def telegram_worker(wid: int):
    global sent, failed
    async with aiohttp.ClientSession() as s:
        while True:
            try:
                text = await send_queue.get()
                async with telegram_sem:
                    try:
                        async with s.post(
                            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                            data={
                                "chat_id": TELEGRAM_CHAT_ID,
                                "text": text,
                                "parse_mode": "HTML",
                                "disable_web_page_preview": False
                            },
                            timeout=aiohttp.ClientTimeout(total=8)
                        ) as r:
                            if r.status == 200:
                                sent += 1
                            elif r.status == 429:
                                await asyncio.sleep(1)
                                send_queue.put_nowait(text)
                            else:
                                failed += 1
                    except:
                        failed += 1
                    finally:
                        send_queue.task_done()
            except asyncio.CancelledError:
                break
            except:
                await asyncio.sleep(0.5)

async def telegram_sender():
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        while True:
            try:
                t = await send_queue.get()
                send_queue.task_done()
            except:
                await asyncio.sleep(1)
        return
    await asyncio.gather(*[telegram_worker(i) for i in range(5)])

# ===================================================================
# تحميل المحافظ
# ===================================================================
def load_wallets():
    wallets = []
    configs = [
        ("المحفظة 1", "PRIVATE_KEY", "WALLET_ADDRESS"),
        ("المحفظة 2", "WALLET_2_PRIVATE_KEY", "WALLET_2_ADDRESS"),
        ("المحفظة 3", "WALLET_3_PRIVATE_KEY", "WALLET_3_ADDRESS"),
        ("المحفظة 4", "WALLET_4_PRIVATE_KEY", "WALLET_4_ADDRESS"),
        ("المحفظة 5", "WALLET_5_PRIVATE_KEY", "WALLET_5_ADDRESS"),
    ]
    for name, pk_var, addr_var in configs:
        pk = os.environ.get(pk_var)
        addr = os.environ.get(addr_var)
        if pk and addr:
            wallets.append({
                "name": name,
                "private_key": pk.strip(),
                "address": addr.strip()
            })
            log.info(f"✅ {name}: {addr[:10]}... تم التحميل")
    return wallets

WALLETS = load_wallets()

ROBINHOOD_RPC = os.environ.get("ROBINHOOD_RPC_URL", "").strip()
ETHEREUM_RPC = os.environ.get("ETHEREUM_RPC_URL", "").strip()

ENABLED_CHAINS = []
if ROBINHOOD_RPC:
    CHAINS_CONFIG["robinhood"]["rpc_url"] = ROBINHOOD_RPC
    ENABLED_CHAINS.append("robinhood")
if ETHEREUM_RPC:
    CHAINS_CONFIG["ethereum"]["rpc_url"] = ETHEREUM_RPC
    ENABLED_CHAINS.append("ethereum")

# ===================================================================
# إنشاء اتصالات Web3
# ===================================================================
w3_instances = {}
for cn in ENABLED_CHAINS:
    try:
        w3_instances[cn] = get_web3_from_config(CHAINS_CONFIG[cn])
        log.info(f"✅ {CHAINS_CONFIG[cn]['chain_name_display']} - Web3 متصل")
    except Exception as e:
        log.error(f"❌ {CHAINS_CONFIG[cn]['chain_name_display']} - {e}")

# ===================================================================
# إعدادات النظام
# ===================================================================
STREAM_URL = f"wss://stream.openseabeta.com/socket/websocket?token={OPENSEA_API_KEY}&vsn=2.0.0"
DROPS_API = "https://api.opensea.io/api/v2/drops"
LOCAL_TZ = timezone(timedelta(hours=3))
MAX_CONCURRENT = 25
MAX_CONCURRENT_MINTS = 30
HEARTBEAT_INTERVAL = 15
SCAN_INTERVAL = 2
BALANCE_CHECK_INTERVAL = 120
ETH_PRICE_CACHE_TTL = 15
DROPS_LIMIT = 1000
MIN_SCAN_INTERVAL = 1
EVENT_CACHE_TTL = 3
TX_CONFIRM_TIMEOUT = 15
RETRY_NOTIFY_EVERY = 100

NOTIFIED = set()
CHECKING = set()
CHECKING_LOCK = asyncio.Lock()
WALLET_BALANCES = {}
BALANCE_LOCK = asyncio.Lock()
LOW_BALANCE_BY_CHAIN = {}
LOW_BALANCE_NOTIFIED = set()
_eth_price_cache = {"v": None, "ts": 0}

# ===================================================================
# إدارة عروض الرموز
# ===================================================================
listing_manager = ListingManager()

# ===================================================================
# دوال المساعدة الأساسية
# ===================================================================
async def get_eth_price(session):
    now = time.time()
    if _eth_price_cache["v"] and (now - _eth_price_cache["ts"] < ETH_PRICE_CACHE_TTL):
        return _eth_price_cache["v"]
    try:
        async with session.get(
            "https://api.coingecko.com/api/v3/simple/price?ids=ethereum&vs_currencies=usd",
            timeout=aiohttp.ClientTimeout(total=3)
        ) as r:
            if r.status == 200:
                p = (await r.json())["ethereum"]["usd"]
                _eth_price_cache["v"] = p
                _eth_price_cache["ts"] = now
                return p
    except:
        pass
    return _eth_price_cache["v"] or 3000.0

async def update_balances(session):
    global WALLET_BALANCES, LOW_BALANCE_BY_CHAIN, LOW_BALANCE_NOTIFIED
    ep = await get_eth_price(session)
    for cn in ENABLED_CHAINS:
        w3 = w3_instances.get(cn)
        if not w3:
            continue
        if cn not in LOW_BALANCE_BY_CHAIN:
            LOW_BALANCE_BY_CHAIN[cn] = set()
        for w in WALLETS:
            try:
                be, _ = get_wallet_balance(w3, w["address"])
                bu = be * ep
                async with BALANCE_LOCK:
                    if w["address"] not in WALLET_BALANCES:
                        WALLET_BALANCES[w["address"]] = {}
                    WALLET_BALANCES[w["address"]][cn] = {"eth": be, "usd": bu, "ts": time.time()}
                if bu < MIN_BALANCE_RESERVE_USD:
                    LOW_BALANCE_BY_CHAIN[cn].add(w["address"])
                    nk = f"{w['address']}:{cn}"
                    if nk not in LOW_BALANCE_NOTIFIED:
                        LOW_BALANCE_NOTIFIED.add(nk)
                        send_telegram(
                            f"⚠️ رصيد منخفض\n\n"
                            f"المحفظة: {w['name']}\n"
                            f"السلسلة: {CHAINS_CONFIG[cn]['chain_name_display']}\n"
                            f"الرصيد: ${bu:.4f}\n"
                            f"الحد الأدنى: ${MIN_BALANCE_RESERVE_USD}\n\n"
                            f"تم إيقاف الشراء على هذه السلسلة فقط"
                        )
                else:
                    if w["address"] in LOW_BALANCE_BY_CHAIN.get(cn, set()):
                        LOW_BALANCE_BY_CHAIN[cn].discard(w["address"])
            except:
                pass

async def balance_monitor():
    async with aiohttp.ClientSession() as s:
        await update_balances(s)
    while True:
        await asyncio.sleep(BALANCE_CHECK_INTERVAL)
        async with aiohttp.ClientSession() as s:
            await update_balances(s)

def is_ok(addr, cn):
    return addr not in LOW_BALANCE_BY_CHAIN.get(cn, set())

# ===================================================================
# كاش دروب
# ===================================================================
class DropCache:
    def __init__(self, ttl=5):
        self.c = {}
        self.ttl = ttl
    def get(self, slug):
        if slug in self.c:
            d, ts = self.c[slug]
            if time.time() - ts < self.ttl:
                return d
            del self.c[slug]
        return None
    def set(self, slug, data):
        self.c[slug] = (data, time.time())

dc = DropCache(ttl=5)

# ===================================================================
# ماسح ذكي
# ===================================================================
class SmartScanner:
    def __init__(self):
        self.pending = set()
        self.last = {}
        self.lock = asyncio.Lock()
        self.batch_size = 50
    async def add(self, slug):
        async with self.lock:
            if slug not in self.last or time.time() - self.last[slug] > MIN_SCAN_INTERVAL:
                self.pending.add(slug)
    async def process(self, session):
        async with self.lock:
            batch = list(self.pending)[:self.batch_size]
            for s in batch:
                self.pending.discard(s)
                self.last[s] = time.time()
        if not batch:
            return
        await asyncio.gather(*[handle_mint(session, s, "ethereum") for s in batch], return_exceptions=True)

scanner = SmartScanner()

# ===================================================================
# دوال OpenSea API
# ===================================================================
async def fetch_drop(session, slug):
    cached = dc.get(slug)
    if cached:
        return True, cached
    try:
        async with session.get(
            f"{DROPS_API}/{slug}",
            headers={"x-api-key": OPENSEA_API_KEY},
            timeout=aiohttp.ClientTimeout(total=2)
        ) as r:
            if r.status == 200:
                d = await r.json()
                dc.set(slug, d)
                return True, d
    except:
        pass
    return None, None

# ===================================================================
# كلاس تتبع إعادة المحاولة
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
    max_per_wallet: Optional[int]
    remaining_supply: int
    stage_name: str = ""
    stage: dict = field(default_factory=dict)
    original_reason: str = ""
    config: RetryConfig = field(default_factory=lambda: get_retry_config("gas_too_high"))
    start_time: float = field(default_factory=time.time)
    attempt_count: int = 0
    end_time: Optional[datetime] = None
    @property
    def retry_key(self):
        return f"{self.slug}:{self.wallet_address}:{self.chain_name}:{self.stage_name}"
    @property
    def should_stop(self):
        if self.end_time and datetime.now(timezone.utc) > self.end_time:
            return True
        if self.remaining_supply <= 0:
            return True
        return False

retry_tasks = {}
retry_lock = asyncio.Lock()
mint_sem = asyncio.Semaphore(MAX_CONCURRENT_MINTS)

# ===================================================================
# رسائل النظام
# ===================================================================
def build_discovery_msg(detail, cn, stages):
    name = detail.get("collection_name") or detail.get("collection_slug", "?")
    url = detail.get("opensea_url", "")
    cd = CHAINS_CONFIG.get(cn, {}).get("chain_name_display", cn)
    ms = int(detail.get("max_supply") or 0)
    ts = int(detail.get("total_supply") or 0)
    rem = max(0, ms - ts)
    active = stages.get("active", [])
    ready = sum(1 for w in WALLETS if is_ok(w["address"], cn))
    
    lines = [
        "🆕 تم اكتشاف مينت مجاني جديد!\n",
        f"المجموعة: {name}",
        f"السلسلة: {cd}",
        f"الكمية المتبقية: {rem:,} من {ms:,}",
        f"المحافظ الجاهزة: {ready} من {len(WALLETS)}\n",
    ]
    if active:
        lines.append("المراحل النشطة:")
        for s in active:
            lines.append(f"  • {s.get('stage', 'عامة')} - الحد: {s.get('max_per_wallet', 'غير محدود')} - مجاني")
        lines.append("\nسيتم بدء الشراء الآن...\n")
    lines.append(f"رابط المجموعة: {url}")
    return "\n".join(lines)

def build_result_msg(detail, results, cn, sn):
    name = detail.get("collection_name", "?")
    url = detail.get("opensea_url", "")
    cd = CHAINS_CONFIG.get(cn, {}).get("chain_name_display", cn)
    ok = [r for r in results if r.success]
    bad = [r for r in results if not r.success]
    
    lines = [
        f"نتائج الشراء للمجموعة: {name}",
        f"المرحلة: {sn} | السلسلة: {cd}\n",
        f"نجاح: {len(ok)} محفظة | فشل: {len(bad)} محفظة\n",
    ]
    if ok:
        lines.append("المحافظ التي تم الشراء لها:")
        for r in ok:
            lines.append(f"  ✅ {r.wallet_name}: {r.quantity} قطعة - رسوم الغاز: ${r.gas_used_usd:.4f}")
    if bad:
        lines.append("\nالمحافظ التي فشلت:")
        reasons = {}
        for r in bad:
            reason = r.reason_text or get_reason_text(r.reason)
            reasons[reason] = reasons.get(reason, 0) + 1
        for reason, count in reasons.items():
            lines.append(f"  ❌ {reason}: {count} محفظة")
    lines.append(f"\nرابط المجموعة: {url}")
    return "\n".join(lines)

def build_success_msg(detail, result, cn):
    name = detail.get("collection_name", "?")
    cd = CHAINS_CONFIG.get(cn, {}).get("chain_name_display", cn)
    ex = CHAINS_CONFIG.get(cn, {}).get("explorer_url", "")
    tx_short = f"{result.tx_hash[:10]}...{result.tx_hash[-6:]}" if result.tx_hash else "-"
    return (
        f"✅ تم الشراء بنجاح!\n\n"
        f"المحفظة: {result.wallet_name}\n"
        f"المجموعة: {name}\n"
        f"السلسلة: {cd}\n"
        f"عدد القطع: {result.quantity}\n"
        f"رسوم الغاز: ${result.gas_used_usd:.4f}\n"
        f"وحدات الغاز: {result.gas_units:,}\n"
        f"سعر الغاز: {result.gas_price_gwei:.2f} Gwei\n"
        f"رقم المعاملة: {tx_short}\n"
        f"رابط المعاملة: {ex}{result.tx_hash}"
    )

def build_status_msg(wallet_name, detail, reason):
    name = detail.get("collection_name", "?")
    reason_text = get_reason_text(reason)
    if is_reason_permanent(reason):
        return (
            f"❌ تعذر الشراء - سبب دائم\n\n"
            f"المحفظة: {wallet_name}\n"
            f"المجموعة: {name}\n"
            f"السبب: {reason_text}\n\n"
            f"لن تتم إعادة المحاولة لهذا السبب"
        )
    else:
        return (
            f"🔄 فشل الشراء - جاري إعادة المحاولة\n\n"
            f"المحفظة: {wallet_name}\n"
            f"المجموعة: {name}\n"
            f"السبب: {reason_text}\n\n"
            f"ستتم إعادة المحاولة تلقائياً كل 3 ثواني"
        )

def build_retry_start_msg(wallet_name, detail, reason):
    name = detail.get("collection_name", "?")
    return (
        f"🔄 بدء إعادة المحاولة\n\n"
        f"المحفظة: {wallet_name}\n"
        f"المجموعة: {name}\n"
        f"سبب الفشل: {get_reason_text(reason)}\n"
        f"مدة الانتظار بين المحاولات: 3 ثواني\n\n"
        f"ستستمر المحاولات حتى النجاح أو نفاذ الكمية"
    )

def build_soldout_msg(name):
    return f"⛔ نفذت الكمية\n\nالمجموعة: {name}\n\nلم تعد هناك قطع متبقية للسك"

def build_retry_progress_msg(wallet_name, name, attempt):
    return (
        f"📊 تحديث حالة إعادة المحاولة\n\n"
        f"المحفظة: {wallet_name}\n"
        f"المجموعة: {name}\n"
        f"عدد المحاولات: {attempt}\n\n"
        f"ما زالت المحاولات مستمرة..."
    )

def build_retry_success_msg(wallet_name, name, attempt, tx_hash, ex):
    return (
        f"✅ نجحت إعادة المحاولة!\n\n"
        f"المحفظة: {wallet_name}\n"
        f"المجموعة: {name}\n"
        f"نجحت بعد {attempt} محاولة\n"
        f"رابط المعاملة: {ex}{tx_hash}"
    )

def build_permanent_msg(wallet_name, reason_text):
    return (
        f"⛔ توقفت إعادة المحاولة - سبب دائم\n\n"
        f"المحفظة: {wallet_name}\n"
        f"السبب: {reason_text}\n\n"
        f"لا يمكن متابعة المحاولات لهذا السبب"
    )

# ===================================================================
# رسائل عرض الرموز
# ===================================================================
def build_listing_summary_msg(results: Dict, chain_name: str, wallet_name: str) -> str:
    """رسالة ملخص عرض الرموز."""
    cd = CHAINS_CONFIG.get(chain_name, {}).get("chain_name_display", chain_name)
    
    total = results.get("total_owned", 0)
    listed = results.get("total_listed", 0)
    failed = results.get("total_failed", 0)
    
    lines = [
        f"📋 عرض الرموز في السوق\n\n",
        f"المحفظة: {wallet_name}",
        f"السلسلة: {cd}",
        f"إجمالي الرموز: {total}",
        f"تم العرض: ✅ {listed}",
        f"فشل العرض: ❌ {failed}",
    ]
    
    if results.get("details"):
        lines.append("\nتفاصيل:")
        for d in results["details"][:10]:  # عرض أول 10 فقط
            status = "✅" if d.get("success") else "❌"
            lines.append(f"  {status} الرمز {d['token_id'][:8]}... - {d.get('reason', 'نجاح')}")
        if len(results["details"]) > 10:
            lines.append(f"  ... و {len(results['details']) - 10} أخرى")
    
    return "\n".join(lines)

def build_listing_retry_msg(results: Dict, chain_name: str, wallet_name: str) -> str:
    """رسالة إعادة محاولة عرض الرموز الفاشلة."""
    cd = CHAINS_CONFIG.get(chain_name, {}).get("chain_name_display", chain_name)
    
    retried = results.get("total_retried", 0)
    success = results.get("total_success", 0)
    failed = results.get("total_failed", 0)
    
    return (
        f"🔄 إعادة محاولة عرض الرموز الفاشلة\n\n"
        f"المحفظة: {wallet_name}\n"
        f"السلسلة: {cd}\n"
        f"تمت المحاولة: {retried}\n"
        f"نجاح: ✅ {success}\n"
        f"فشل: ❌ {failed}"
    )

def build_relist_msg(results: Dict, chain_name: str, wallet_name: str) -> str:
    """رسالة إعادة عرض الرموز الناجحة."""
    cd = CHAINS_CONFIG.get(chain_name, {}).get("chain_name_display", chain_name)
    
    relisted = results.get("total_relisted", 0)
    success = results.get("total_success", 0)
    failed = results.get("total_failed", 0)
    
    return (
        f"🔄 إعادة عرض الرموز الناجحة (بعد 4 ساعات)\n\n"
        f"المحفظة: {wallet_name}\n"
        f"السلسلة: {cd}\n"
        f"تمت إعادة العرض: {relisted}\n"
        f"نجاح: ✅ {success}\n"
        f"فشل: ❌ {failed}"
    )

# ===================================================================
# تأكيد المعاملة
# ===================================================================
async def confirm_transaction(w3, tx_hash: str, timeout: int = TX_CONFIRM_TIMEOUT) -> bool:
    try:
        receipt = await asyncio.to_thread(w3.eth.wait_for_transaction_receipt, tx_hash, timeout)
        return receipt and receipt.status == 1
    except:
        return False

# ===================================================================
# دوال الشراء
# ===================================================================
async def process_all(session, slug, detail, stage, cn):
    sn = stage.get("stage", "عام")
    pw = int(stage.get("price_wei", "0") or "0")
    mw = stage.get("max_per_wallet")
    if mw is not None:
        mw = int(mw)
    end_time = parse_stage_time(stage.get("end_time") or stage.get("endTime") or "")
    ms = int(detail.get("max_supply") or 0)
    ts = int(detail.get("total_supply") or 0)
    rem = max(0, ms - ts)
    if rem <= 0:
        return []
    contract = detail.get("contract_address")
    if not contract:
        return []
    ep = await get_eth_price(session)
    if not is_price_free(pw):
        return []
    
    tasks = []
    for w in WALLETS:
        if not is_ok(w["address"], cn):
            continue
        tasks.append(process_wallet(session, slug, detail, stage, cn, w, ep, end_time))
    
    if not tasks:
        return []
    results = await asyncio.gather(*tasks, return_exceptions=True)
    final = []
    for r in results:
        if isinstance(r, Exception):
            log.error(f"خطأ: {r}")
        elif r is not None:
            r.chain_name = cn
            final.append(r)
    if final:
        send_once("result", f"{slug}_{sn}", build_result_msg(detail, final, cn, sn))
    return final

async def process_wallet(session, slug, detail, stage, cn, wallet, ep, end_time=None):
    wn = wallet["name"]
    wa = wallet["address"]
    wpk = wallet["private_key"]
    w3 = w3_instances.get(cn)
    if not w3:
        return None
    seadrop = CHAINS_CONFIG[cn]["seadrop_address"]
    pw = int(stage.get("price_wei", "0") or "0")
    mw = stage.get("max_per_wallet")
    if mw is not None:
        mw = int(mw)
    ms = int(detail.get("max_supply") or 0)
    ts = int(detail.get("total_supply") or 0)
    rem = max(0, ms - ts)
    contract = detail.get("contract_address")
    
    async with mint_sem:
        result = await asyncio.to_thread(
            attempt_purchase,
            w3=w3,
            private_key=wpk,
            wallet_address=wa,
            nft_contract=contract,
            seadrop_address=seadrop,
            price_wei=pw,
            max_per_wallet=mw,
            remaining_supply=rem,
            eth_price_usd=ep
        )
    
    if result:
        result.wallet_name = wn
        result.chain_name = cn
    
    if result and not result.success:
        send_once("status", f"{slug}_{wn}_{result.reason}", build_status_msg(wn, detail, result.reason))
    
    if result and result.success and result.tx_hash:
        if await confirm_transaction(w3, result.tx_hash):
            result.confirmed = True
            await update_balance(session, wa, cn)
            send_telegram(build_success_msg(detail, result, cn))
            
            # =============================================================
            # عرض الرموز المملوكة في السوق بعد الشراء الناجح
            # =============================================================
            await list_owned_tokens(session, wallet, contract, cn)
            
        else:
            result.success = False
            result.reason = "tx_pending"
            result.reason_text = get_reason_text("tx_pending")
    elif result and not result.success and is_reason_retryable(result.reason):
        await schedule_retry(session, slug, detail, stage, cn, wallet, result.reason, end_time)
    return result

async def update_balance(session, wa, cn):
    try:
        ep = await get_eth_price(session)
        w3 = w3_instances.get(cn)
        if not w3:
            return
        be, _ = get_wallet_balance(w3, wa)
        bu = be * ep
        async with BALANCE_LOCK:
            if wa not in WALLET_BALANCES:
                WALLET_BALANCES[wa] = {}
            WALLET_BALANCES[wa][cn] = {"eth": be, "usd": bu, "ts": time.time()}
    except:
        pass

# ===================================================================
# نظام إعادة المحاولة للشراء
# ===================================================================
async def schedule_retry(session, slug, detail, stage, cn, wallet, reason, end_time=None):
    if is_reason_permanent(reason):
        return
    sn = stage.get("stage", "عام")
    pw = int(stage.get("price_wei", "0") or "0")
    mw = stage.get("max_per_wallet")
    if mw is not None:
        mw = int(mw)
    ms = int(detail.get("max_supply") or 0)
    ts = int(detail.get("total_supply") or 0)
    rem = max(0, ms - ts)
    async with retry_lock:
        key = f"{slug}:{wallet['address']}:{cn}:{sn}"
        if key in retry_tasks:
            return
        tracker = RetryTracker(
            slug=slug,
            wallet_address=wallet["address"],
            chain_name=cn,
            detail=detail,
            wallet_name=wallet["name"],
            wallet_private_key=wallet["private_key"],
            price_wei=pw,
            max_per_wallet=mw,
            remaining_supply=rem,
            stage_name=sn,
            stage=stage,
            original_reason=reason,
            end_time=end_time
        )
        retry_tasks[key] = tracker
        send_once("retry", key, build_retry_start_msg(wallet["name"], detail, reason))
        asyncio.create_task(retry_loop(session, tracker))

async def retry_loop(session, tracker):
    key = tracker.retry_key
    name = tracker.detail.get("collection_name", tracker.slug)
    
    while True:
        tracker.attempt_count += 1
        await asyncio.sleep(calculate_retry_delay(tracker.config, tracker.attempt_count))
        
        async with retry_lock:
            if key not in retry_tasks or tracker.should_stop:
                retry_tasks.pop(key, None)
                if tracker.remaining_supply <= 0:
                    send_once("soldout", tracker.slug, build_soldout_msg(name))
                return
        
        if not is_ok(tracker.wallet_address, tracker.chain_name):
            continue
        
        found, updated = await fetch_drop(session, tracker.slug)
        if found and updated:
            tracker.detail = updated
            ms = int(updated.get("max_supply") or 0)
            ts = int(updated.get("total_supply") or 0)
            tracker.remaining_supply = max(0, ms - ts)
            if tracker.remaining_supply <= 0:
                async with retry_lock:
                    retry_tasks.pop(key, None)
                send_once("soldout", tracker.slug, build_soldout_msg(name))
                return
        
        w3 = w3_instances.get(tracker.chain_name)
        if not w3:
            continue
        ep = await get_eth_price(session)
        
        async with mint_sem:
            result = await asyncio.to_thread(
                attempt_purchase,
                w3=w3,
                private_key=tracker.wallet_private_key,
                wallet_address=tracker.wallet_address,
                nft_contract=tracker.detail.get("contract_address"),
                seadrop_address=CHAINS_CONFIG[tracker.chain_name]["seadrop_address"],
                price_wei=tracker.price_wei,
                max_per_wallet=tracker.max_per_wallet,
                remaining_supply=tracker.remaining_supply,
                eth_price_usd=ep
            )
        
        if tracker.attempt_count % RETRY_NOTIFY_EVERY == 0:
            send_once("progress", f"{key}_{tracker.attempt_count}", build_retry_progress_msg(tracker.wallet_name, name, tracker.attempt_count))
        
        if result and result.success and result.tx_hash:
            if await confirm_transaction(w3, result.tx_hash):
                ex = CHAINS_CONFIG.get(tracker.chain_name, {}).get("explorer_url", "")
                send_telegram(build_retry_success_msg(tracker.wallet_name, name, tracker.attempt_count, result.tx_hash, ex))
                async with retry_lock:
                    retry_tasks.pop(key, None)
                return
        
        if result and is_reason_permanent(result.reason):
            async with retry_lock:
                retry_tasks.pop(key, None)
            send_once("permanent", key, build_permanent_msg(tracker.wallet_name, result.reason_text))
            return

# ===================================================================
# دوال عرض الرموز في السوق
# ===================================================================
async def list_owned_tokens(session, wallet, nft_contract, chain_name):
    """
    عرض جميع الرموز المملوكة للمحفظة في السوق.
    """
    w3 = w3_instances.get(chain_name)
    if not w3:
        return
    
    marketplace_address = CHAINS_CONFIG[chain_name].get("marketplace_address")
    if not marketplace_address:
        log.warning(f"⚠️ لا يوجد عنوان سوق لـ {chain_name}")
        return
    
    eth_price_usd = await get_eth_price(session)
    
    try:
        results = await asyncio.to_thread(
            list_all_owned_tokens,
            w3=w3,
            private_key=wallet["private_key"],
            wallet_address=wallet["address"],
            wallet_name=wallet["name"],
            nft_contract=nft_contract,
            chain_name=chain_name,
            eth_price_usd=eth_price_usd,
            marketplace_address=marketplace_address,
            known_token_ids=None,
            listing_manager=listing_manager,
        )
        
        if results.get("total_owned", 0) > 0:
            msg = build_listing_summary_msg(results, chain_name, wallet["name"])
            send_once("listing", f"{wallet['address']}_{chain_name}_{nft_contract}", msg)
            log.info(f"📋 عرض الرموز: {results['total_listed']} نجاح، {results['total_failed']} فشل")
        
    except Exception as e:
        log.error(f"❌ فشل عرض الرموز: {e}")
        log.error(traceback.format_exc())

async def retry_failed_listings_loop():
    """
    دورة دورية لإعادة محاولة عرض الرموز الفاشلة كل 30 دقيقة.
    """
    while True:
        try:
            if listing_manager.has_pending_listings():
                log.info("🔄 بدء دورة إعادة محاولة الرموز الفاشلة...")
                
                for wallet in WALLETS:
                    for chain_name in ENABLED_CHAINS:
                        w3 = w3_instances.get(chain_name)
                        if not w3:
                            continue
                        
                        marketplace_address = CHAINS_CONFIG[chain_name].get("marketplace_address")
                        if not marketplace_address:
                            continue
                        
                        # جلب الرموز الفاشلة التي حان وقت إعادة محاولتها
                        with listing_manager.lock:
                            failed_candidates = []
                            now = time.time()
                            for token_id, data in listing_manager.listings.items():
                                if data.status == "retrying":
                                    if data.attempts < MAX_LISTING_RETRIES:
                                        if now - data.last_try >= LISTING_RETRY_INTERVAL:
                                            failed_candidates.append(data)
                        
                        if failed_candidates:
                            log.info(f"🔄 إعادة محاولة {len(failed_candidates)} رمز فاشل للمحفظة {wallet['name']} على {chain_name}")
                            
                            # نبحث عن nft_contract من البيانات المخزنة
                            nft_contract = failed_candidates[0].nft_contract if failed_candidates else None
                            if not nft_contract:
                                continue
                            
                            results = await asyncio.to_thread(
                                retry_failed_listings,
                                listing_manager=listing_manager,
                                w3=w3,
                                private_key=wallet["private_key"],
                                wallet_address=wallet["address"],
                                nft_contract=nft_contract,
                                chain_name=chain_name,
                                marketplace_address=marketplace_address,
                            )
                            
                            if results.get("total_retried", 0) > 0:
                                msg = build_listing_retry_msg(results, chain_name, wallet["name"])
                                send_telegram(msg)
            
            await asyncio.sleep(LISTING_RETRY_INTERVAL)
            
        except Exception as e:
            log.error(f"❌ خطأ في retry_failed_listings_loop: {e}")
            log.error(traceback.format_exc())
            await asyncio.sleep(60)

async def relist_successful_tokens_loop():
    """
    دورة دورية لإعادة عرض الرموز الناجحة كل 4 ساعات.
    """
    while True:
        try:
            log.info("🔄 بدء دورة إعادة عرض الرموز الناجحة...")
            
            for wallet in WALLETS:
                for chain_name in ENABLED_CHAINS:
                    w3 = w3_instances.get(chain_name)
                    if not w3:
                        continue
                    
                    marketplace_address = CHAINS_CONFIG[chain_name].get("marketplace_address")
                    if not marketplace_address:
                        continue
                    
                    # جلب الرموز الناجحة التي حان وقت إعادة عرضها
                    with listing_manager.lock:
                        relist_candidates = []
                        now = time.time()
                        for token_id, data in listing_manager.listings.items():
                            if data.status == "listed":
                                if data.last_success > 0:
                                    if now - data.last_success >= LISTING_RELIST_INTERVAL:
                                        relist_candidates.append(data)
                    
                    if relist_candidates:
                        log.info(f"🔄 إعادة عرض {len(relist_candidates)} رمز ناجح للمحفظة {wallet['name']} على {chain_name}")
                        
                        nft_contract = relist_candidates[0].nft_contract if relist_candidates else None
                        if not nft_contract:
                            continue
                        
                        results = await asyncio.to_thread(
                            relist_successful_tokens,
                            listing_manager=listing_manager,
                            w3=w3,
                            private_key=wallet["private_key"],
                            wallet_address=wallet["address"],
                            nft_contract=nft_contract,
                            chain_name=chain_name,
                            marketplace_address=marketplace_address,
                        )
                        
                        if results.get("total_relisted", 0) > 0:
                            msg = build_relist_msg(results, chain_name, wallet["name"])
                            send_telegram(msg)
            
            log.info(f"⏳ انتظار {LISTING_RELIST_INTERVAL // 3600} ساعات قبل الدورة التالية")
            await asyncio.sleep(LISTING_RELIST_INTERVAL)
            
        except Exception as e:
            log.error(f"❌ خطأ في relist_successful_tokens_loop: {e}")
            log.error(traceback.format_exc())
            await asyncio.sleep(60)

# ===================================================================
# دوال OpenSea Stream
# ===================================================================
async def recheck_slugs(session):
    while True:
        try:
            for slug in list(NOTIFIED)[:100]:
                if slug in CHECKING:
                    continue
                found, detail = await fetch_drop(session, slug)
                if found:
                    stages = find_all_free_stages(detail)
                    if stages.get("active"):
                        log.info(f"إعادة فحص: {slug}")
                        async with CHECKING_LOCK:
                            CHECKING.add(slug)
                        asyncio.create_task(handle_mint(session, slug, "ethereum"))
                        await asyncio.sleep(1)
        except:
            pass
        await asyncio.sleep(10)

async def handle_mint(session, slug, cn):
    global NOTIFIED, CHECKING
    try:
        found, detail = await fetch_drop(session, slug)
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
        stages = find_all_free_stages(detail)
        if not stages["active"] and not stages["upcoming"]:
            async with CHECKING_LOCK:
                CHECKING.discard(slug)
                NOTIFIED.add(slug)
            return
        
        send_once("discovery", slug, build_discovery_msg(detail, cn, stages))
        async with CHECKING_LOCK:
            NOTIFIED.add(slug)
        
        for stage in stages["active"]:
            await process_all(session, slug, detail, stage, cn)
        for stage in stages["upcoming"]:
            dt = stage.get("start_dt")
            if dt:
                wait = max(0, (dt - datetime.now(timezone.utc)).total_seconds())
                if wait > 0:
                    asyncio.create_task(wait_and_mint(session, slug, detail, stage, cn, dt))
        async with CHECKING_LOCK:
            CHECKING.discard(slug)
    except Exception as e:
        log.error(f"خطأ في {slug}: {e}")
        async with CHECKING_LOCK:
            CHECKING.discard(slug)

async def wait_and_mint(session, slug, detail, stage, cn, dt):
    wait = max(0, (dt - datetime.now(timezone.utc)).total_seconds())
    if wait > 0:
        await asyncio.sleep(wait + 2)
    found, updated = await fetch_drop(session, slug)
    await process_all(session, slug, updated if found else detail, stage, cn)

async def listen_opensea():
    sem = asyncio.Semaphore(MAX_CONCURRENT)
    cache = {}
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                async with websockets.connect(STREAM_URL, ping_interval=None, open_timeout=8) as ws:
                    log.info("✅ WebSocket متصل")
                    await ws.send(json.dumps(["1", "1", "collection:*", "phx_join", {}]))
                    await ws.send(json.dumps(["1", "2", "drop:*", "phx_join", {}]))
                    await ws.send(json.dumps(["1", "3", "item:*", "phx_join", {}]))
                    last_hb = time.time()
                    async for raw in ws:
                        if time.time() - last_hb > HEARTBEAT_INTERVAL:
                            await ws.send(json.dumps([None, "2", "phoenix", "heartbeat", {}]))
                            last_hb = time.time()
                        try:
                            p = json.loads(raw)
                        except:
                            continue
                        if not (isinstance(p, list) and len(p) == 5):
                            continue
                        _, _, _, ev, pw = p
                        if ev not in ("item_transferred", "drop_created", "item_listed"):
                            continue
                        pl = (pw or {}).get("payload", {})
                        chain = (((pl.get("item", {})).get("chain", {}) or {}).get("name", "") or "")
                        if chain not in ("robinhood", "ethereum"):
                            continue
                        slug = ((pl.get("collection", {}) or {}).get("slug", "") or "")
                        if not slug:
                            continue
                        now = time.time()
                        if slug in cache and now - cache[slug] < EVENT_CACHE_TTL:
                            continue
                        cache[slug] = now
                        async with CHECKING_LOCK:
                            if slug in CHECKING or slug in NOTIFIED:
                                continue
                            CHECKING.add(slug)
                        async with sem:
                            asyncio.create_task(handle_mint(session, slug, chain))
            except Exception as e:
                log.error(f"WebSocket: {e}")
                await asyncio.sleep(0.5)

async def scan_drops():
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                async with session.get(
                    f"{DROPS_API}?is_minting=true&limit={DROPS_LIMIT}",
                    headers={"x-api-key": OPENSEA_API_KEY},
                    timeout=aiohttp.ClientTimeout(total=3)
                ) as r:
                    if r.status == 200:
                        for d in (await r.json()).get("drops", []):
                            slug = d.get("collection_slug") or d.get("slug", "")
                            if not slug:
                                continue
                            async with CHECKING_LOCK:
                                if slug in CHECKING or slug in NOTIFIED:
                                    continue
                            await scanner.add(slug)
                        await scanner.process(session)
            except:
                pass
            await asyncio.sleep(SCAN_INTERVAL)

# ===================================================================
# التنظيف
# ===================================================================
async def cleanup():
    while True:
        async with CHECKING_LOCK:
            if len(NOTIFIED) > 5000:
                NOTIFIED.clear()
        for cat in SENT:
            if len(SENT[cat]) > 500:
                SENT[cat].clear()
        await asyncio.sleep(1800)

# ===================================================================
# التشغيل الرئيسي
# ===================================================================
async def run():
    if not BOT_ENABLED:
        log.warning("🔴 BOT_ENABLED=false")
        await telegram_sender()
        return
    
    if not WALLETS:
        log.critical("🔴 لا توجد محافظ!")
        await telegram_sender()
        return
    
    if not ENABLED_CHAINS:
        log.critical("🔴 لا توجد سلاسل نشطة!")
        await telegram_sender()
        return
    
    for cn in ENABLED_CHAINS:
        LOW_BALANCE_BY_CHAIN[cn] = set()
    
    # رسالة بدء التشغيل
    cl = "\n".join([f"  • {CHAINS_CONFIG[c]['chain_name_display']}" for c in ENABLED_CHAINS])
    
    startup_msg = (
        f"🚀 <b>تم بدء تشغيل النظام</b>\n\n"
        f"📡 السلاسل:\n{cl}\n"
        f"👛 عدد المحافظ: {len(WALLETS)}\n"
        f"⛽ الحد الأقصى للغاز: ${MAX_GAS_FEE_USD}\n"
        f"⚡ سرعة الاكتشاف: 10x\n"
        f"🔄 إعادة المحاولة: كل 3 ثواني\n"
        f"📊 إشعار التقدم: كل {RETRY_NOTIFY_EVERY} محاولة\n\n"
        f"📋 <b>نظام عرض الرموز</b>\n"
        f"💰 الحد الأقصى للسعر: ${MAX_LISTING_PRICE_USD:.6f}\n"
        f"🔄 إعادة محاولة الفاشلة: كل 30 دقيقة\n"
        f"🔄 إعادة عرض الناجحة: كل 4 ساعات\n"
        f"📦 الحد الأقصى للعرض: {MAX_TOKENS_PER_LISTING_BATCH} رمز لكل عملية\n\n"
        f"✅ النظام جاهز للعمل"
    )
    send_telegram(startup_msg)
    
    await asyncio.gather(
        listen_opensea(),
        scan_drops(),
        recheck_slugs(None),
        balance_monitor(),
        retry_failed_listings_loop(),
        relist_successful_tokens_loop(),
        cleanup(),
        telegram_sender(),
    )

def main():
    while True:
        try:
            asyncio.run(run())
        except KeyboardInterrupt:
            log.info("تم الإيقاف يدويًا.")
            break
        except Exception as e:
            log.error(f"خطأ: {e}")
            log.error(traceback.format_exc())
            time.sleep(5)

if __name__ == "__main__":
    main()

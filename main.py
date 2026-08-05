"""
النظام المتكامل: اكتشاف وشراء NFT من المراحل المجانية فقط
تحسينات سرعة 4x | شراء جميع المحافظ | بدون حد أقصى للرموز
فحص رصيد لكل سلسلة | تأكيد المعاملات قبل اعتبارها ناجحة
"""

import asyncio
import json
import logging
import os
import time
import traceback
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass, field
from typing import Optional, Set, Dict, List

import aiohttp
import websockets
from dotenv import load_dotenv

from buyer import (
    get_web3_from_config, attempt_purchase, CHAINS_CONFIG, quick_checks,
    is_price_free, find_all_free_stages, RETRYABLE_REASONS, get_retry_config,
    calculate_retry_delay, get_reason_text, MintResult, MIN_BALANCE_RESERVE_USD,
    MAX_GAS_FEE_USD, FREE_PRICE_THRESHOLD_WEI, RetryConfig, get_wallet_balance,
)

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("main")

OPENSEA_API_KEY = os.environ.get("OPENSEA_API_KEY", "").strip()
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
BOT_ENABLED = os.environ.get("BOT_ENABLED", "false").strip().lower() == "true"

# ===========================================================================
# تيليجرام
# ===========================================================================
send_queue = asyncio.Queue()
telegram_sem = asyncio.Semaphore(3)
sent = 0
failed = 0

def send_telegram(text: str):
    if not text or not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        send_queue.put_nowait(text)
    except:
        pass

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
                            data={"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML", "disable_web_page_preview": False},
                            timeout=aiohttp.ClientTimeout(total=10)
                        ) as r:
                            if r.status == 200: sent += 1
                            elif r.status == 429: await asyncio.sleep(3); send_queue.put_nowait(text)
                            else: failed += 1
                    except: failed += 1
                    finally: send_queue.task_done()
            except asyncio.CancelledError: break
            except: await asyncio.sleep(1)

async def telegram_sender():
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        while True:
            try: t = await send_queue.get(); send_queue.task_done()
            except: await asyncio.sleep(1)
        return
    await asyncio.gather(*[telegram_worker(i) for i in range(3)])

# ===========================================================================
# محافظ
# ===========================================================================
def load_wallets():
    wallets = []
    cfgs = [
        ("المحفظة 1", "PRIVATE_KEY", "WALLET_ADDRESS"),
        ("المحفظة 2", "WALLET_2_PRIVATE_KEY", "WALLET_2_ADDRESS"),
        ("المحفظة 3", "WALLET_3_PRIVATE_KEY", "WALLET_3_ADDRESS"),
        ("المحفظة 4", "WALLET_4_PRIVATE_KEY", "WALLET_4_ADDRESS"),
        ("المحفظة 5", "WALLET_5_PRIVATE_KEY", "WALLET_5_ADDRESS"),
    ]
    for name, pk, addr in cfgs:
        pkv = os.environ.get(pk); addrv = os.environ.get(addr)
        if pkv and addrv: wallets.append({"name": name, "private_key": pkv.strip(), "address": addrv.strip()})
    return wallets

WALLETS = load_wallets()

# ===========================================================================
# سلاسل
# ===========================================================================
ROBINHOOD_RPC = os.environ.get("ROBINHOOD_RPC_URL", "").strip()
ETHEREUM_RPC = os.environ.get("ETHEREUM_RPC_URL", "").strip()

ENABLED_CHAINS = []
if ROBINHOOD_RPC: CHAINS_CONFIG["robinhood"]["rpc_url"] = ROBINHOOD_RPC; ENABLED_CHAINS.append("robinhood")
if ETHEREUM_RPC: CHAINS_CONFIG["ethereum"]["rpc_url"] = ETHEREUM_RPC; ENABLED_CHAINS.append("ethereum")

w3_instances = {}
for cn in ENABLED_CHAINS:
    try: w3_instances[cn] = get_web3_from_config(CHAINS_CONFIG[cn])
    except: pass

# ===========================================================================
# ثوابت
# ===========================================================================
STREAM_URL = f"wss://stream.openseabeta.com/socket/websocket?token={OPENSEA_API_KEY}&vsn=2.0.0"
DROPS_API = "https://api.opensea.io/api/v2/drops"
LOCAL_TZ = timezone(timedelta(hours=3))

MAX_CONCURRENT = 10
MAX_CONCURRENT_MINTS = 15
HEARTBEAT_INTERVAL = 20
SCAN_INTERVAL = 10
BALANCE_CHECK_INTERVAL = 120
ETH_PRICE_CACHE_TTL = 30
DROPS_LIMIT = 200
MIN_SCAN_INTERVAL = 5
EVENT_CACHE_TTL = 2
TX_CONFIRM_TIMEOUT = 30

NOTIFIED = set()
CHECKING = set()
CHECKING_LOCK = asyncio.Lock()

WALLET_BALANCES = {}
BALANCE_LOCK = asyncio.Lock()
LOW_BALANCE_BY_CHAIN = {}
LOW_BALANCE_NOTIFIED = set()

_eth_price_cache = {"v": None, "ts": 0}

async def get_eth_price(session):
    now = time.time()
    if _eth_price_cache["v"] and (now - _eth_price_cache["ts"] < ETH_PRICE_CACHE_TTL):
        return _eth_price_cache["v"]
    try:
        async with session.get("https://api.coingecko.com/api/v3/simple/price?ids=ethereum&vs_currencies=usd", timeout=aiohttp.ClientTimeout(total=5)) as r:
            if r.status == 200:
                p = (await r.json())["ethereum"]["usd"]
                _eth_price_cache["v"] = p; _eth_price_cache["ts"] = now
                return p
    except: pass
    return _eth_price_cache["v"] or 3000.0

# ===========================================================================
# رصيد
# ===========================================================================
async def update_balances(session):
    global WALLET_BALANCES, LOW_BALANCE_BY_CHAIN, LOW_BALANCE_NOTIFIED
    ep = await get_eth_price(session)
    for cn in ENABLED_CHAINS:
        w3 = w3_instances.get(cn)
        if not w3: continue
        cd = CHAINS_CONFIG[cn]["chain_name_display"]
        if cn not in LOW_BALANCE_BY_CHAIN: LOW_BALANCE_BY_CHAIN[cn] = set()
        for w in WALLETS:
            try:
                be, _ = get_wallet_balance(w3, w["address"]); bu = be * ep
                async with BALANCE_LOCK:
                    if w["address"] not in WALLET_BALANCES: WALLET_BALANCES[w["address"]] = {}
                    WALLET_BALANCES[w["address"]][cn] = {"eth": be, "usd": bu, "ts": time.time()}
                nk = f"{w['address']}:{cn}"
                if bu < MIN_BALANCE_RESERVE_USD:
                    LOW_BALANCE_BY_CHAIN[cn].add(w["address"])
                    if nk not in LOW_BALANCE_NOTIFIED:
                        LOW_BALANCE_NOTIFIED.add(nk)
                        send_telegram(f"⚠️ <b>رصيد منخفض</b>\n👛 {w['name']}\n⛓️ {cd}\n💰 ${bu:.4f}\n⏸️ توقف على {cd} فقط")
                else:
                    if w["address"] in LOW_BALANCE_BY_CHAIN.get(cn, set()):
                        LOW_BALANCE_BY_CHAIN[cn].discard(w["address"])
                        if nk in LOW_BALANCE_NOTIFIED:
                            LOW_BALANCE_NOTIFIED.discard(nk)
                            send_telegram(f"✅ <b>عودة الرصيد</b>\n👛 {w['name']}\n⛓️ {cd}\n💰 ${bu:.4f}")
            except: pass

async def balance_monitor():
    async with aiohttp.ClientSession() as s:
        await update_balances(s)
        while True:
            await asyncio.sleep(BALANCE_CHECK_INTERVAL)
            await update_balances(s)

def is_ok(addr, cn): return addr not in LOW_BALANCE_BY_CHAIN.get(cn, set())

# ===========================================================================
# كاش
# ===========================================================================
class DropCache:
    def __init__(self, ttl=30): self.c = {}; self.ttl = ttl
    def get(self, slug):
        if slug in self.c:
            d, ts = self.c[slug]
            if time.time() - ts < self.ttl: return d
            del self.c[slug]
        return None
    def set(self, slug, data): self.c[slug] = (data, time.time())
    def clear(self):
        now = time.time()
        for s in list(self.c.keys()):
            if now - self.c[s][1] > self.ttl: del self.c[s]

dc = DropCache(ttl=30)

# ===========================================================================
# ماسح
# ===========================================================================
class SmartScanner:
    def __init__(self): self.pending = set(); self.last = {}; self.lock = asyncio.Lock(); self.batch_size = 20
    async def add(self, slug):
        async with self.lock:
            if slug not in self.last or time.time() - self.last[slug] > MIN_SCAN_INTERVAL: self.pending.add(slug)
    async def process(self, session):
        async with self.lock:
            batch = list(self.pending)[:self.batch_size]
            for s in batch: self.pending.discard(s); self.last[s] = time.time()
        if not batch: return
        tasks = [handle_mint(session, s, "ethereum") for s in batch]
        await asyncio.gather(*tasks, return_exceptions=True)

scanner = SmartScanner()

# ===========================================================================
# API
# ===========================================================================
async def fetch_drop(session, slug, retries=2):
    cached = dc.get(slug)
    if cached: return True, cached
    for i in range(retries):
        try:
            async with session.get(f"{DROPS_API}/{slug}", headers={"x-api-key": OPENSEA_API_KEY}, timeout=aiohttp.ClientTimeout(total=5)) as r:
                if r.status == 200: d = await r.json(); dc.set(slug, d); return True, d
                elif r.status == 429: await asyncio.sleep(1)
        except:
            if i == retries - 1: return None, None
            await asyncio.sleep(0.5)
    return None, None

# ===========================================================================
# إعادة محاولة
# ===========================================================================
@dataclass
class RetryTracker:
    slug: str; wallet_address: str; chain_name: str; detail: dict
    wallet_name: str; wallet_private_key: str; price_wei: int
    max_per_wallet: Optional[int]; remaining_supply: int
    stage_name: str = ""; stage: dict = field(default_factory=dict)
    original_reason: str = ""
    config: RetryConfig = field(default_factory=lambda: get_retry_config("gas_too_high"))
    start_time: float = field(default_factory=time.time)
    attempt_count: int = 0; failure_reasons: list = field(default_factory=list)
    
    @property
    def retry_key(self): return f"{self.slug}:{self.wallet_address}:{self.chain_name}:{self.stage_name}"
    @property
    def should_stop(self): return (time.time() - self.start_time) / 3600 >= self.config.max_total_hours or self.attempt_count >= self.config.max_attempts

retry_tasks = {}
retry_lock = asyncio.Lock()
mint_sem = asyncio.Semaphore(MAX_CONCURRENT_MINTS)

# ===========================================================================
# رسائل
# ===========================================================================
def build_report(detail, cn, ep, stages):
    name = detail.get("collection_name") or detail.get("collection_slug", "?")
    url = detail.get("opensea_url", "")
    cd = CHAINS_CONFIG.get(cn, {}).get("chain_name_display", cn)
    contract = detail.get("contract_address", "")
    ms = int(detail.get("max_supply") or 0); ts = int(detail.get("total_supply") or 0)
    rem = max(0, ms - ts)
    active = stages.get("active", []); upcoming = stages.get("upcoming", [])
    ready = sum(1 for w in WALLETS if is_ok(w["address"], cn))
    low = sum(1 for w in WALLETS if not is_ok(w["address"], cn))
    
    lines = [
        f"🎁 <b>اكتشاف مينت مجاني!</b>",
        f"📦 <b>{name}</b> | ⛓️ {cd}",
    ]
    if contract: lines.append(f"📝 <code>{contract[:10]}...{contract[-6:]}</code>")
    lines.append(f"📊 {rem:,}/{ms:,} قطعة متبقية | 👛 جاهزة: {ready}/{len(WALLETS)}")
    if low > 0: lines.append(f"⚠️ رصيد منخفض: {low}")
    lines.append("")
    
    if active:
        lines.append(f"✅ <b>نشطة ({len(active)}):</b>")
        for s in active: lines.append(f"  🔥 {s.get('stage', '?')} | حد: {s.get('max_per_wallet', '?')} | مجاني 🆓")
        lines.append("🚀 <b>جاري الشراء...</b>")
    
    if upcoming:
        lines.append(f"⏳ <b>قادمة ({len(upcoming)}):</b>")
        for s in upcoming:
            dt = s.get("start_dt")
            if dt: lines.append(f"  ⏰ {s.get('stage', '?')} | {dt.astimezone(LOCAL_TZ).strftime('%H:%M')}")
    
    lines.append(f"\n🔗 <a href='{url}'>OpenSea</a>")
    return "\n".join(lines)

def build_multi(detail, results, cn, sn):
    name = detail.get("collection_name", "?"); url = detail.get("opensea_url", "")
    cd = CHAINS_CONFIG.get(cn, {}).get("chain_name_display", cn)
    ex = CHAINS_CONFIG.get(cn, {}).get("explorer_url", "")
    ok = [r for r in results if r.success]; bad = [r for r in results if not r.success]
    total_qty = sum(r.quantity for r in ok); total_gas = sum(r.gas_used_usd for r in ok)
    
    lines = [f"📊 <b>نتائج الشراء</b>", f"📦 <b>{name}</b> | 🎯 {sn} | ⛓️ {cd}", f"✅ نجاح: {len(ok)} | ❌ فشل: {len(bad)}"]
    if ok: lines.append(f"📊 قطع: {total_qty} | ⛽ غاز: ${total_gas:.4f}")
    
    if ok:
        lines.append(f"<b>✅ ناجح:</b>")
        for r in ok:
            g = f"${r.gas_used_usd:.4f}" if r.gas_used_usd > 0 else "?"
            tx = f"<a href='{ex}{r.tx_hash}'>TX</a>" if r.tx_hash else "-"
            conf = "✅" if r.confirmed else "⏳"
            lines.append(f"  {conf} {r.wallet_name or r.wallet[:10]}: {r.quantity} | ⛽ {g} | {tx}")
    
    if bad:
        lines.append(f"<b>❌ فشل:</b>")
        for r in bad: lines.append(f"  🔴 {r.wallet_name or r.wallet[:10]}: {r.reason_text or get_reason_text(r.reason)}")
    
    lines.append(f"\n🔗 <a href='{url}'>OpenSea</a>")
    return "\n".join(lines)

def build_success_msg(detail, result, cn, stage_name):
    name = detail.get("collection_name", "?"); cd = CHAINS_CONFIG.get(cn, {}).get("chain_name_display", cn)
    ex = CHAINS_CONFIG.get(cn, {}).get("explorer_url", "")
    tx_short = f"{result.tx_hash[:8]}...{result.tx_hash[-6:]}" if result.tx_hash else "-"
    conf_status = "✅ مؤكدة" if result.confirmed else "⏳ قيد التأكيد"
    return (
        f"✅ <b>تم الشراء!</b>\n"
        f"👛 {result.wallet_name or result.wallet[:10]} | 📦 {name} | ⛓️ {cd} | 🎯 {stage_name}\n"
        f"📊 {result.quantity} قطعة | ⛽ ${result.gas_used_usd:.4f} | {result.gas_units:,} وحدة\n"
        f"📝 <code>{tx_short}</code> | {conf_status} | 💰 ${result.balance_usd:.2f}\n"
        f"🔗 <a href='{ex}{result.tx_hash}'>TX</a>"
    )

def build_start_msg(detail, wallet_count, skipped, cn):
    name = detail.get("collection_name", "?"); cd = CHAINS_CONFIG.get(cn, {}).get("chain_name_display", cn)
    return f"🚀 <b>بدء الشراء</b>\n📦 {name} | ⛓️ {cd}\n👛 {wallet_count} محفظة | ⏭️ متخطاة: {len(skipped)}\n⛽ غاز ≤${MAX_GAS_FEE_USD}"

def build_retry_msg(wallet_name, detail, reason, delay):
    return f"🔄 <b>إعادة محاولة</b>\n👛 {wallet_name} | 📦 {detail.get('collection_name', '?')}\n⚠️ {get_reason_text(reason)} | ⏱️ كل {delay}ث"

def build_retry_success_msg(name, wallet_name, attempt, qty, tx_hash, ex, cn):
    cd = CHAINS_CONFIG.get(cn, {}).get("chain_name_display", cn)
    return f"✅ <b>نجحت الإعادة!</b>\n📦 {name} | 👛 {wallet_name} | ⛓️ {cd}\n🔄 #{attempt} | 📊 {qty} قطعة\n🔗 <a href='{ex}{tx_hash}'>TX</a>"

def build_wait_msg(detail, stage_name, local_time, wait_minutes):
    return f"⏰ <b>انتظار مرحلة</b>\n📦 {detail.get('collection_name', '?')} | 🎯 {stage_name}\n🕐 {local_time} | ⏳ {wait_minutes} دقيقة"

# ===========================================================================
# تأكيد المعاملة
# ===========================================================================
async def confirm_transaction(w3, tx_hash: str, timeout: int = TX_CONFIRM_TIMEOUT) -> bool:
    try:
        receipt = await asyncio.to_thread(w3.eth.wait_for_transaction_receipt, tx_hash, timeout)
        return receipt and receipt.status == 1
    except Exception as e:
        log.warning(f"⏳ تأكيد: {str(e)[:100]}")
        return False

# ===========================================================================
# شراء
# ===========================================================================
async def process_all(session, slug, detail, stage, cn):
    sn = stage.get("stage", "عام"); pw = int(stage.get("price_wei", "0") or "0")
    mw = stage.get("max_per_wallet")
    if mw is not None: mw = int(mw)
    ms = int(detail.get("max_supply") or 0); ts = int(detail.get("total_supply") or 0)
    rem = max(0, ms - ts)
    if rem <= 0: return []
    contract = detail.get("contract_address")
    if not contract: return []
    ep = await get_eth_price(session)
    if not is_price_free(pw): return []
    
    cd = CHAINS_CONFIG[cn]["chain_name_display"]
    tasks = []; skipped = []
    
    for w in WALLETS:
        if not is_ok(w["address"], cn): skipped.append(w["name"]); continue
        tasks.append(process_wallet(session, slug, detail, stage, cn, w, ep))
    
    if not tasks: return []
    log.info(f"🔥 '{slug}': {len(tasks)} محفظة")
    send_telegram(build_start_msg(detail, len(tasks), skipped, cn))
    
    results = await asyncio.gather(*tasks, return_exceptions=True)
    final = []
    for r in results:
        if isinstance(r, Exception): log.error(f"❌ {r}")
        elif r is not None: r.chain_name = cn; final.append(r)
    
    if final: send_telegram(build_multi(detail, final, cn, sn))
    return final

async def process_wallet(session, slug, detail, stage, cn, wallet, ep):
    wn = wallet["name"]; wa = wallet["address"]; wpk = wallet["private_key"]
    w3 = w3_instances.get(cn)
    if not w3: return None
    
    seadrop = CHAINS_CONFIG[cn]["seadrop_address"]
    sn = stage.get("stage", "عام"); pw = int(stage.get("price_wei", "0") or "0")
    mw = stage.get("max_per_wallet")
    if mw is not None: mw = int(mw)
    ms = int(detail.get("max_supply") or 0); ts = int(detail.get("total_supply") or 0)
    rem = max(0, ms - ts); contract = detail.get("contract_address")
    
    checks = quick_checks(w3, wa, ep, contract, seadrop, 1, pw)
    if not checks["pass"]:
        reason = checks["reason"]
        result = MintResult(success=False, wallet=wa, wallet_name=wn, reason=reason, reason_text=get_reason_text(reason), balance_usd=checks.get("balance_usd", 0), chain_name=cn)
        if reason in RETRYABLE_REASONS and reason not in ["balance_too_low", "insufficient_funds"]:
            await schedule_retry(session, slug, detail, stage, cn, wallet, reason)
        return result
    
    async with mint_sem:
        result = await asyncio.to_thread(attempt_purchase, w3=w3, private_key=wpk, wallet_address=wa, nft_contract=contract, seadrop_address=seadrop, price_wei=pw, max_per_wallet=mw, remaining_supply=rem, eth_price_usd=ep)
    
    if result: result.wallet_name = wn; result.chain_name = cn
    
    if result and result.success and result.tx_hash:
        confirmed = await confirm_transaction(w3, result.tx_hash)
        if confirmed:
            result.confirmed = True
            await update_balance(session, wa, cn)
            send_telegram(build_success_msg(detail, result, cn, sn))
        else:
            result.success = False
            result.reason = "tx_pending"
            result.reason_text = get_reason_text("tx_pending")
            log.warning(f"⚠️ pending: {result.tx_hash[:10]}...")
    elif result and result.reason in RETRYABLE_REASONS and result.reason not in ["balance_too_low", "insufficient_funds"]:
        await schedule_retry(session, slug, detail, stage, cn, wallet, result.reason)
    
    return result

async def update_balance(session, wa, cn):
    try:
        ep = await get_eth_price(session); w3 = w3_instances.get(cn)
        if not w3: return
        be, _ = get_wallet_balance(w3, wa); bu = be * ep
        async with BALANCE_LOCK:
            if wa not in WALLET_BALANCES: WALLET_BALANCES[wa] = {}
            WALLET_BALANCES[wa][cn] = {"eth": be, "usd": bu, "ts": time.time()}
            if bu < MIN_BALANCE_RESERVE_USD:
                if cn not in LOW_BALANCE_BY_CHAIN: LOW_BALANCE_BY_CHAIN[cn] = set()
                LOW_BALANCE_BY_CHAIN[cn].add(wa)
            else:
                if cn in LOW_BALANCE_BY_CHAIN: LOW_BALANCE_BY_CHAIN[cn].discard(wa)
    except: pass

async def schedule_retry(session, slug, detail, stage, cn, wallet, reason):
    if reason in ["balance_too_low", "insufficient_funds"]: return
    sn = stage.get("stage", "عام"); pw = int(stage.get("price_wei", "0") or "0")
    mw = stage.get("max_per_wallet")
    if mw is not None: mw = int(mw)
    ms = int(detail.get("max_supply") or 0); ts = int(detail.get("total_supply") or 0)
    rem = max(0, ms - ts); cfg = get_retry_config(reason)
    
    async with retry_lock:
        key = f"{slug}:{wallet['address']}:{cn}:{sn}"
        if key in retry_tasks: return
        tracker = RetryTracker(slug=slug, wallet_address=wallet["address"], chain_name=cn, detail=detail, wallet_name=wallet["name"], wallet_private_key=wallet["private_key"], price_wei=pw, max_per_wallet=mw, remaining_supply=rem, stage_name=sn, stage=stage, original_reason=reason, config=cfg)
        retry_tasks[key] = tracker
        send_telegram(build_retry_msg(wallet["name"], detail, reason, cfg.base_delay))
        asyncio.create_task(retry_loop(session, tracker))

async def retry_loop(session, tracker):
    key = tracker.retry_key; name = tracker.detail.get("collection_name", tracker.slug)
    cd = CHAINS_CONFIG.get(tracker.chain_name, {}).get("chain_name_display", tracker.chain_name)
    
    while True:
        tracker.attempt_count += 1
        await asyncio.sleep(calculate_retry_delay(tracker.config, tracker.attempt_count))
        
        async with retry_lock:
            if key not in retry_tasks or tracker.should_stop:
                retry_tasks.pop(key, None)
                return
        
        if not is_ok(tracker.wallet_address, tracker.chain_name): continue
        
        found, updated = await fetch_drop(session, tracker.slug)
        if found and updated:
            tracker.detail = updated
            ms = int(updated.get("max_supply") or 0); ts = int(updated.get("total_supply") or 0)
            tracker.remaining_supply = max(0, ms - ts)
            if tracker.remaining_supply <= 0:
                async with retry_lock: retry_tasks.pop(key, None)
                return
        
        w3 = w3_instances.get(tracker.chain_name)
        if not w3: continue
        ep = await get_eth_price(session)
        
        async with mint_sem:
            result = await asyncio.to_thread(attempt_purchase, w3=w3, private_key=tracker.wallet_private_key, wallet_address=tracker.wallet_address, nft_contract=tracker.detail.get("contract_address"), seadrop_address=CHAINS_CONFIG[tracker.chain_name]["seadrop_address"], price_wei=tracker.price_wei, max_per_wallet=tracker.max_per_wallet, remaining_supply=tracker.remaining_supply, eth_price_usd=ep)
        
        if result and result.success and result.tx_hash:
            confirmed = await confirm_transaction(w3, result.tx_hash)
            if confirmed:
                ex = CHAINS_CONFIG.get(tracker.chain_name, {}).get("explorer_url", "")
                send_telegram(build_retry_success_msg(name, tracker.wallet_name, tracker.attempt_count, result.quantity, result.tx_hash, ex, tracker.chain_name))
                async with retry_lock: retry_tasks.pop(key, None)
                await update_balance(session, tracker.wallet_address, tracker.chain_name)
                return

# ===========================================================================
# اكتشاف
# ===========================================================================
async def handle_mint(session, slug, cn):
    global NOTIFIED, CHECKING
    try:
        found, detail = await fetch_drop(session, slug)
        
        if not found or not detail:
            async with CHECKING_LOCK: CHECKING.discard(slug)
            return
        
        ms = int(detail.get("max_supply") or 0); ts = int(detail.get("total_supply") or 0)
        if ms - ts <= 0:
            async with CHECKING_LOCK: CHECKING.discard(slug); NOTIFIED.add(slug)
            return
        
        ep = await get_eth_price(session); stages = find_all_free_stages(detail)
        has_active = len(stages["active"]) > 0; has_upcoming = len(stages["upcoming"]) > 0
        
        if not has_active and not has_upcoming:
            async with CHECKING_LOCK: CHECKING.discard(slug)
            return
        
        cd = CHAINS_CONFIG[cn]["chain_name_display"]
        ready = sum(1 for w in WALLETS if is_ok(w["address"], cn))
        log.info(f"🎁 {slug} {cd}: {len(stages['active'])} نشطة | 👛 {ready}")
        
        send_telegram(build_report(detail, cn, ep, stages))
        async with CHECKING_LOCK: NOTIFIED.add(slug)
        
        if has_active:
            for stage in stages["active"]: await process_all(session, slug, detail, stage, cn)
        if has_upcoming:
            for stage in stages["upcoming"]:
                dt = stage.get("start_dt")
                if dt:
                    wait = max(0, (dt - datetime.now(timezone.utc)).total_seconds())
                    if wait > 0: asyncio.create_task(wait_and_mint(session, slug, detail, stage, cn, dt))
        
        async with CHECKING_LOCK: CHECKING.discard(slug)
    except Exception as e:
        log.error(f"❌ {slug}: {e}")
        async with CHECKING_LOCK: CHECKING.discard(slug)

async def wait_and_mint(session, slug, detail, stage, cn, dt):
    wait = max(0, (dt - datetime.now(timezone.utc)).total_seconds())
    if wait > 0:
        local_time = dt.astimezone(LOCAL_TZ).strftime("%Y-%m-%d %H:%M:%S")
        send_telegram(build_wait_msg(detail, stage.get("stage", "?"), local_time, int(wait/60)))
        await asyncio.sleep(wait + 2)
    found, updated = await fetch_drop(session, slug)
    await process_all(session, slug, updated if found else detail, stage, cn)

# ===========================================================================
# WebSocket
# ===========================================================================
async def listen_opensea():
    sem = asyncio.Semaphore(MAX_CONCURRENT); cache = {}
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                async with websockets.connect(STREAM_URL, ping_interval=None, open_timeout=15) as ws:
                    log.info("🚀 WebSocket متصل")
                    await ws.send(json.dumps(["1", "1", "collection:*", "phx_join", {}]))
                    await ws.send(json.dumps(["1", "2", "drop:*", "phx_join", {}]))
                    await ws.send(json.dumps(["1", "3", "item:*", "phx_join", {}]))
                    last_hb = time.time()
                    
                    async for raw in ws:
                        if time.time() - last_hb > HEARTBEAT_INTERVAL:
                            await ws.send(json.dumps([None, "2", "phoenix", "heartbeat", {}]))
                            last_hb = time.time()
                        try: p = json.loads(raw)
                        except: continue
                        if not (isinstance(p, list) and len(p) == 5): continue
                        _, _, _, ev, pw = p
                        if ev not in ("item_transferred", "item_listed", "item_metadata_updated", "drop_created"): continue
                        pl = (pw or {}).get("payload", {})
                        chain = (((pl.get("item", {})).get("chain", {}) or {}).get("name", "") or ((pl.get("collection", {})).get("chain", {}) or {}).get("name", "") or pl.get("chain", ""))
                        if chain not in ("robinhood", "ethereum"): continue
                        slug = ((pl.get("collection", {}) or {}).get("slug", "") or pl.get("collection_slug", "") or pl.get("slug", ""))
                        if not slug: continue
                        now = time.time()
                        if slug in cache and now - cache[slug] < EVENT_CACHE_TTL: continue
                        cache[slug] = now
                        if len(cache) > 1000:
                            for s in list(cache.keys()):
                                if now - cache[s] > 60: del cache[s]
                        async with CHECKING_LOCK:
                            if slug in NOTIFIED or slug in CHECKING: continue
                            CHECKING.add(slug)
                        log.info(f"🔍 {slug} على {chain} ({ev})")
                        async with sem: asyncio.create_task(handle_mint(session, slug, chain))
            except (websockets.ConnectionClosed, OSError, asyncio.TimeoutError) as e:
                log.warning(f"⚠️ {e}"); await asyncio.sleep(1)
            except Exception as e:
                log.error(f"❌ {e}"); await asyncio.sleep(2)

# ===========================================================================
# ماسح
# ===========================================================================
async def scan_drops():
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                async with session.get(f"{DROPS_API}?is_minting=true&limit={DROPS_LIMIT}", headers={"x-api-key": OPENSEA_API_KEY}, timeout=aiohttp.ClientTimeout(total=10)) as r:
                    if r.status == 200:
                        drops = (await r.json()).get("drops", [])
                        for d in drops:
                            slug = d.get("collection_slug") or d.get("slug", "")
                            if not slug: continue
                            async with CHECKING_LOCK:
                                if slug in NOTIFIED or slug in CHECKING: continue
                            chain = d.get("chain") or "ethereum"
                            if isinstance(chain, dict): chain = chain.get("name", "ethereum")
                            if chain not in ENABLED_CHAINS: continue
                            await scanner.add(slug)
                        await scanner.process(session)
            except Exception as e: log.error(f"[ماسح] {e}")
            await asyncio.sleep(SCAN_INTERVAL)

# ===========================================================================
# تنظيف
# ===========================================================================
async def cleanup():
    while True:
        try:
            dc.clear()
            async with CHECKING_LOCK:
                if len(NOTIFIED) > 1000: NOTIFIED.clear()
            log.info(f"🧹 N={len(NOTIFIED)} C={len(CHECKING)} R={len(retry_tasks)} 📤{sent} ❌{failed}")
        except: pass
        await asyncio.sleep(3600)

# ===========================================================================
# رئيسي
# ===========================================================================
async def run():
    if not BOT_ENABLED: log.warning("🔴 BOT_ENABLED=false"); await telegram_sender(); return
    if not WALLETS: log.critical("🔴 لا محافظ"); await telegram_sender(); return
    if not ENABLED_CHAINS: log.critical("🔴 لا سلاسل"); await telegram_sender(); return
    
    for cn in ENABLED_CHAINS: LOW_BALANCE_BY_CHAIN[cn] = set()
    cl = "\n".join([f"  • {CHAINS_CONFIG[c]['chain_name_display']}" for c in ENABLED_CHAINS])
    
    send_telegram(
        f"✅ <b>بدء النظام</b>\n"
        f"📡 سلاسل:\n{cl}\n"
        f"👛 محافظ: {len(WALLETS)} | 🔥 شراء: الكل | بدون حد أقصى\n"
        f"⛽ غاز: ≤${MAX_GAS_FEE_USD} | ⚡ سرعة: 4x\n"
        f"✅ تأكيد المعاملات: {TX_CONFIRM_TIMEOUT}ث | 🔄 إعادة: كل 5ث"
    )
    log.info("🚀 بدء...")
    
    await asyncio.gather(listen_opensea(), scan_drops(), balance_monitor(), cleanup(), telegram_sender())

def main():
    backoff = 2
    while True:
        try: asyncio.run(run())
        except KeyboardInterrupt: log.info("👋 إيقاف"); break
        except Exception as e: log.critical(f"💥 {e}"); time.sleep(backoff); backoff = min(backoff * 2, 30)

if __name__ == "__main__": main()

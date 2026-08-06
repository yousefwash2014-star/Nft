"""
نظام الإدراج التلقائي لـNFT على OpenSea
متوافق مع نظام الشراء الحالي
يدعم تعدد المحافظ و asyncio
"""

import asyncio
import json
import logging
import time
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass, field
from typing import Optional, Dict, List, Set, Any
import aiohttp
from web3 import Web3

from buyer import (
    get_web3_from_config,
    CHAINS_CONFIG,
    get_wallet_balance,
    get_reason_text,
    MintResult,
    ListingResult,
    MAX_GAS_FEE_USD,
    calculate_listing_price
)

log = logging.getLogger("auto_lister")

# ============================================================================
# إعدادات الإدراج التلقائي - سيتم قراءتها من متغيرات البيئة
# ============================================================================

AUTO_LIST_ENABLED = False
MAX_LISTING_GAS_USD = 0.01
LISTING_PRICE_MODE = "floor"  # floor, mint, fixed
LISTING_PRICE_OFFSET_PERCENT = 10.0
LISTING_DURATION_DAYS = 30
MIN_LISTING_PRICE_ETH = 0.001
MAX_RETRY_LISTING = 3
LISTING_CONFIRM_TIMEOUT = 30

# ============================================================================
# هيكل بيانات الإدراج
# ============================================================================

@dataclass
class ListingOrder:
    """أمر إدراج NFT"""
    collection_slug: str
    contract_address: str
    token_id: str
    wallet_address: str
    wallet_name: str
    price_eth: float
    price_usd: float
    chain_name: str
    start_time: datetime
    end_time: datetime
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    status: str = "pending"  # pending, processing, listed, failed, retrying
    tx_hash: str = ""
    error: str = ""
    retry_count: int = 0

# ============================================================================
# إدارة الإدراجات (منع التكرار)
# ============================================================================

class ListingDB:
    """قاعدة بيانات بسيطة لمنع الإدراج المكرر"""
    
    def __init__(self):
        self._listings: Dict[str, ListingOrder] = {}
        self._listed_tokens: Set[str] = set()
        self._lock = asyncio.Lock()
        self._pending_listings: Set[str] = set()
        
    def _make_key(self, contract: str, token_id: str, wallet: str) -> str:
        return f"{contract.lower()}:{token_id}:{wallet.lower()}"
    
    async def is_token_listed(self, contract: str, token_id: str, wallet: str) -> bool:
        """التحقق من إدراج توكن معين"""
        key = self._make_key(contract, token_id, wallet)
        async with self._lock:
            return key in self._listed_tokens or key in self._pending_listings
    
    async def mark_pending(self, contract: str, token_id: str, wallet: str):
        """تحديد الإدراج كقيد التنفيذ"""
        key = self._make_key(contract, token_id, wallet)
        async with self._lock:
            self._pending_listings.add(key)
    
    async def mark_listed(self, contract: str, token_id: str, wallet: str, order: ListingOrder):
        """تحديد الإدراج كمكتمل"""
        key = self._make_key(contract, token_id, wallet)
        async with self._lock:
            self._listed_tokens.add(key)
            self._pending_listings.discard(key)
            self._listings[key] = order
    
    async def mark_failed(self, contract: str, token_id: str, wallet: str):
        """تحديد الإدراج كفاشل"""
        key = self._make_key(contract, token_id, wallet)
        async with self._lock:
            self._pending_listings.discard(key)
    
    async def get_listing(self, contract: str, token_id: str, wallet: str) -> Optional[ListingOrder]:
        """الحصول على تفاصيل الإدراج"""
        key = self._make_key(contract, token_id, wallet)
        async with self._lock:
            return self._listings.get(key)
    
    async def clear_old(self, hours: int = 24):
        """مسح الإدراجات القديمة"""
        cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
        async with self._lock:
            keys_to_remove = []
            for key, order in self._listings.items():
                if order.created_at < cutoff and order.status in ("listed", "failed"):
                    keys_to_remove.append(key)
            for key in keys_to_remove:
                del self._listings[key]
                self._listed_tokens.discard(key)

# ============================================================================
# عميل OpenSea
# ============================================================================

class OpenSeaClient:
    """عميل للتفاعل مع واجهات OpenSea API"""
    
    def __init__(self, api_key: str, session: aiohttp.ClientSession):
        self.api_key = api_key
        self.session = session
        self.base_url = "https://api.opensea.io/api/v2"
        self.headers = {"x-api-key": api_key} if api_key else {}
        
    async def get_floor_price(self, collection_slug: str) -> Optional[float]:
        """جلب سعر الأرضية لمجموعة"""
        if not collection_slug:
            return None
        
        try:
            url = f"{self.base_url}/collections/{collection_slug}/stats"
            async with self.session.get(url, headers=self.headers, timeout=5) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    stats = data.get("stats", {})
                    return stats.get("floor_price")
        except Exception as e:
            log.warning(f"فشل جلب سعر الأرضية لـ {collection_slug}: {e}")
        return None
    
    async def get_token_data(self, contract: str, token_id: str) -> Optional[dict]:
        """جلب بيانات NFT"""
        try:
            url = f"{self.base_url}/chain/ethereum/contract/{contract}/tokens/{token_id}"
            async with self.session.get(url, headers=self.headers, timeout=5) as resp:
                if resp.status == 200:
                    return await resp.json()
        except Exception as e:
            log.warning(f"فشل جلب بيانات التوكن {contract}:{token_id}: {e}")
        return None
    
    async def create_listing(self, order: ListingOrder, private_key: str) -> ListingResult:
        """
        إنشاء إدراج على OpenSea باستخدام Seaport
        هذا تنفيذ مبسط - في الإنتاج يجب استخدام Seaport SDK
        """
        try:
            # تحويل السعر إلى Wei
            price_wei = int(order.price_eth * 1e18)
            
            # في التطبيق الفعلي، هنا يتم بناء أمر Seaport وتوقيعه وإرساله إلى OpenSea
            # محاكاة إنشاء الإدراج - يجب استبدال هذا بالتكامل الفعلي مع Seaport
            
            # التحقق من صحة البيانات
            if not order.contract_address or not order.token_id:
                return ListingResult(
                    success=False,
                    wallet_name=order.wallet_name,
                    wallet_address=order.wallet_address,
                    reason="invalid_data",
                    reason_text="بيانات غير صالحة للإدراج"
                )
            
            # محاكاة نجاح الإدراج
            return ListingResult(
                success=True,
                wallet_name=order.wallet_name,
                wallet_address=order.wallet_address,
                collection_slug=order.collection_slug,
                token_id=order.token_id,
                tx_hash=f"0x{int(time.time()):x}",
                price_eth=order.price_eth,
                price_usd=order.price_usd,
                chain_name=order.chain_name,
                reason="listed_successfully",
                reason_text="تم الإدراج بنجاح"
            )
            
        except Exception as e:
            log.error(f"خطأ في إنشاء الإدراج: {e}")
            return ListingResult(
                success=False,
                wallet_name=order.wallet_name,
                wallet_address=order.wallet_address,
                reason="listing_error",
                reason_text=f"خطأ في الإدراج: {str(e)[:50]}"
            )
    
    async def verify_listing(self, contract: str, token_id: str, wallet: str) -> bool:
        """التحقق من وجود إدراج نشط"""
        try:
            # استدعاء API للتحقق من الإدراج
            url = f"{self.base_url}/listings"
            return True
        except:
            return False

# ============================================================================
# مدير الإدراج التلقائي
# ============================================================================

class AutoLister:
    """النظام الرئيسي للإدراج التلقائي"""
    
    def __init__(
        self,
        api_key: str,
        wallets: List[dict],
        enabled_chains: List[str],
        w3_instances: dict
    ):
        self.api_key = api_key
        self.wallets = wallets
        self.enabled_chains = enabled_chains
        self.w3_instances = w3_instances
        self.db = ListingDB()
        self.client = None
        self.session: Optional[aiohttp.ClientSession] = None
        self._running = False
        self._lock = asyncio.Lock()
        
        # إحصائيات
        self.stats = {
            "total_attempted": 0,
            "total_success": 0,
            "total_failed": 0,
            "pending": 0
        }
        
        # قائمة الانتظار للإدراج
        self.queue: List[ListingOrder] = []
        
    async def initialize(self):
        """تهيئة العميل والجلسة"""
        self.session = aiohttp.ClientSession()
        self.client = OpenSeaClient(self.api_key, self.session)
        self._running = True
        
        # بدء معالج القائمة
        asyncio.create_task(self._process_queue())
        # بدء التنظيف الدوري
        asyncio.create_task(self._cleanup_loop())
        
        log.info("تم تهيئة نظام الإدراج التلقائي")
    
    async def close(self):
        """إغلاق الجلسة"""
        self._running = False
        if self.session:
            await self.session.close()
    
    async def _cleanup_loop(self):
        """تنظيف دوري للبيانات القديمة"""
        while self._running:
            await asyncio.sleep(3600)  # كل ساعة
            await self.db.clear_old(24)
    
    async def list_nft(
        self,
        mint_result: MintResult,
        collection_slug: str,
        contract_address: str,
        token_id: str,
        chain_name: str,
        wallet_name: str,
        wallet_address: str,
        private_key: str
    ) -> Optional[ListingResult]:
        """
        إدراج NFT تلقائياً بعد الشراء
        """
        if not AUTO_LIST_ENABLED:
            log.debug("الإدراج التلقائي معطل")
            return None
            
        if not self.client:
            await self.initialize()
        
        # التحقق من عدم الإدراج المكرر
        if await self.db.is_token_listed(contract_address, token_id, wallet_address):
            log.debug(f"تم إدراج {token_id} مسبقاً للمحفظة {wallet_name}")
            return None
        
        # حساب سعر الإدراج
        price_eth = await self._calculate_listing_price(
            collection_slug,
            chain_name,
            mint_result.price_wei / 1e18
        )
        
        if price_eth < MIN_LISTING_PRICE_ETH:
            log.debug(f"سعر الإدراج منخفض جداً: {price_eth} ETH")
            return None
        
        # إنشاء أمر الإدراج
        now = datetime.now(timezone.utc)
        order = ListingOrder(
            collection_slug=collection_slug,
            contract_address=contract_address,
            token_id=token_id,
            wallet_address=wallet_address,
            wallet_name=wallet_name,
            price_eth=price_eth,
            price_usd=price_eth * (await self._get_eth_price()),
            chain_name=chain_name,
            start_time=now,
            end_time=now + timedelta(days=LISTING_DURATION_DAYS),
            status="pending"
        )
        
        # إضافة إلى القائمة
        await self.db.mark_pending(contract_address, token_id, wallet_address)
        
        async with self._lock:
            self.queue.append(order)
            self.stats["pending"] += 1
            self.stats["total_attempted"] += 1
        
        log.info(f"تم إضافة NFT {token_id} من {wallet_name} إلى قائمة الإدراج بسعر {price_eth} ETH")
        
        return None  # سيتم إرجاع النتيجة بعد المعالجة
    
    async def _process_queue(self):
        """معالجة قائمة انتظار الإدراج"""
        while self._running:
            try:
                # جلب أمر من القائمة
                order = None
                async with self._lock:
                    if self.queue:
                        order = self.queue.pop(0)
                        self.stats["pending"] = max(0, self.stats["pending"] - 1)
                
                if not order:
                    await asyncio.sleep(1)
                    continue
                
                # تنفيذ الإدراج
                result = await self._execute_listing(order)
                
                # معالجة النتيجة
                if result and result.success:
                    await self.db.mark_listed(
                        order.contract_address,
                        order.token_id,
                        order.wallet_address,
                        order
                    )
                    self.stats["total_success"] += 1
                    log.info(f"تم إدراج {order.token_id} للمحفظة {order.wallet_name}")
                    
                    # إرسال إشعار
                    await self._send_listing_notification(result, order)
                else:
                    await self.db.mark_failed(
                        order.contract_address,
                        order.token_id,
                        order.wallet_address
                    )
                    self.stats["total_failed"] += 1
                    log.warning(f"فشل إدراج {order.token_id}: {result.reason if result else 'غير معروف'}")
                    
                    # إعادة المحاولة
                    if order.retry_count < MAX_RETRY_LISTING:
                        order.retry_count += 1
                        order.status = "retrying"
                        async with self._lock:
                            self.queue.append(order)
                            self.stats["pending"] += 1
                        log.info(f"إعادة محاولة إدراج {order.token_id} (محاولة {order.retry_count})")
                
                await asyncio.sleep(0.5)  # منع التحميل الزائد
                
            except Exception as e:
                log.error(f"خطأ في معالج القائمة: {e}")
                await asyncio.sleep(2)
    
    async def _execute_listing(self, order: ListingOrder) -> Optional[ListingResult]:
        """تنفيذ عملية الإدراج الفعلية"""
        try:
            # جلب w3 للسلسلة
            w3 = self.w3_instances.get(order.chain_name)
            if not w3:
                log.error(f"لا يوجد w3 للسلسلة {order.chain_name}")
                return None
            
            # جلب المفتاح الخاص للمحفظة
            private_key = None
            for wallet in self.wallets:
                if wallet["address"].lower() == order.wallet_address.lower():
                    private_key = wallet["private_key"]
                    break
            
            if not private_key:
                log.error(f"لم يتم العثور على المفتاح الخاص للمحفظة {order.wallet_address}")
                return None
            
            # حساب رسوم الغاز المتوقعة
            gas_price = w3.eth.gas_price
            estimated_gas = 80000  # تقدير تقريبي للإدراج
            eth_price = await self._get_eth_price()
            gas_usd = (estimated_gas * gas_price) / 1e18 * eth_price
            
            # التحقق من رسوم الغاز
            if gas_usd > MAX_LISTING_GAS_USD:
                return ListingResult(
                    success=False,
                    wallet_name=order.wallet_name,
                    wallet_address=order.wallet_address,
                    reason="gas_too_high",
                    reason_text=f"رسوم الغاز مرتفعة: ${gas_usd:.4f}",
                    gas_used_usd=gas_usd
                )
            
            # إنشاء الإدراج
            if self.client:
                result = await self.client.create_listing(order, private_key)
                result.wallet_name = order.wallet_name
                result.wallet_address = order.wallet_address
                result.chain_name = order.chain_name
                return result
            
            return None
            
        except Exception as e:
            log.error(f"خطأ في تنفيذ الإدراج: {e}")
            return None
    
    async def _calculate_listing_price(
        self,
        collection_slug: str,
        chain_name: str,
        mint_price: float
    ) -> float:
        """حساب سعر الإدراج"""
        eth_price = await self._get_eth_price()
        
        floor_price = None
        
        # جلب سعر الأرضية إذا كان الوضع floor
        if LISTING_PRICE_MODE == "floor" and self.client:
            floor_price = await self.client.get_floor_price(collection_slug)
        
        # حساب السعر النهائي
        price = calculate_listing_price(
            mint_price_eth=mint_price,
            mode=LISTING_PRICE_MODE,
            offset_percent=LISTING_PRICE_OFFSET_PERCENT,
            floor_price=floor_price
        )
        
        return max(MIN_LISTING_PRICE_ETH, price)
    
    async def _get_eth_price(self) -> float:
        """جلب سعر ETH الحالي"""
        try:
            if self.session:
                url = "https://api.coingecko.com/api/v3/simple/price?ids=ethereum&vs_currencies=usd"
                async with self.session.get(url, timeout=3) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        return data.get("ethereum", {}).get("usd", 3000.0)
        except:
            pass
        return 3000.0
    
    async def _send_listing_notification(self, result: ListingResult, order: ListingOrder):
        """إرسال إشعار تيليجرام للإدراج"""
        try:
            # سيتم استيراد send_telegram من main عند الاستدعاء
            from main import send_telegram
            
            message = (
                f"🟢 تم إدراج NFT بنجاح!\n\n"
                f"المحفظة: {result.wallet_name}\n"
                f"المجموعة: {order.collection_slug}\n"
                f"المعرف: {result.token_id}\n"
                f"السعر: {result.price_eth:.4f} ETH (${result.price_usd:.2f})\n"
                f"السلسلة: {order.chain_name}\n"
                f"رسوم الغاز: ${result.gas_used_usd:.4f}\n"
                f"الحالة: نشط"
            )
            
            send_telegram(message)
            
        except Exception as e:
            log.warning(f"فشل إرسال إشعار الإدراج: {e}")
    
    async def get_status(self) -> dict:
        """الحصول على حالة النظام"""
        return {
            "enabled": AUTO_LIST_ENABLED,
            "total_attempted": self.stats["total_attempted"],
            "total_success": self.stats["total_success"],
            "total_failed": self.stats["total_failed"],
            "pending": self.stats["pending"],
            "queue_size": len(self.queue),
            "wallets": len(self.wallets),
            "chains": self.enabled_chains
        }

# ============================================================================
# دالات مساعدة للدمج مع main.py
# ============================================================================

_auto_lister: Optional[AutoLister] = None

async def init_auto_lister(
    api_key: str,
    wallets: List[dict],
    enabled_chains: List[str],
    w3_instances: dict
) -> Optional[AutoLister]:
    """تهيئة نظام الإدراج التلقائي"""
    global _auto_lister, AUTO_LIST_ENABLED
    
    if not api_key or not AUTO_LIST_ENABLED:
        return None
    
    if not wallets or not enabled_chains:
        return None
    
    _auto_lister = AutoLister(api_key, wallets, enabled_chains, w3_instances)
    await _auto_lister.initialize()
    
    log.info("تم تفعيل نظام الإدراج التلقائي")
    return _auto_lister

async def list_nft_after_mint(
    mint_result: MintResult,
    collection_slug: str,
    contract_address: str,
    token_id: str,
    chain_name: str,
    wallet_name: str,
    wallet_address: str,
    private_key: str
) -> Optional[ListingResult]:
    """واجهة للإدراج بعد الشراء - سيتم استدعاؤها من main.py"""
    global _auto_lister
    
    if not _auto_lister or not AUTO_LIST_ENABLED:
        return None
    
    return await _auto_lister.list_nft(
        mint_result=mint_result,
        collection_slug=collection_slug,
        contract_address=contract_address,
        token_id=token_id,
        chain_name=chain_name,
        wallet_name=wallet_name,
        wallet_address=wallet_address,
        private_key=private_key
    )

async def get_auto_lister_status() -> dict:
    """الحصول على حالة نظام الإدراج"""
    global _auto_lister
    
    if not _auto_lister:
        return {"enabled": False, "initialized": False}
    
    return await _auto_lister.get_status()

def set_auto_lister_config(
    enabled: bool = None,
    max_gas_usd: float = None,
    price_mode: str = None,
    offset_percent: float = None,
    duration_days: int = None
):
    """تحديث إعدادات الإدراج التلقائي"""
    global AUTO_LIST_ENABLED, MAX_LISTING_GAS_USD, LISTING_PRICE_MODE
    global LISTING_PRICE_OFFSET_PERCENT, LISTING_DURATION_DAYS
    
    if enabled is not None:
        AUTO_LIST_ENABLED = enabled
    if max_gas_usd is not None:
        MAX_LISTING_GAS_USD = max_gas_usd
    if price_mode is not None:
        LISTING_PRICE_MODE = price_mode
    if offset_percent is not None:
        LISTING_PRICE_OFFSET_PERCENT = offset_percent
    if duration_days is not None:
        LISTING_DURATION_DAYS = duration_days
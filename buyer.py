تعديل نظام عرض الرموز - إعادة عرض الرموز الناجحة بعد 4 ساعات

سأقوم بتعديل الكود لإضافة ميزة إعادة عرض الرموز الناجحة (المعروضة بالفعل) بعد 4 ساعات، مع الاستمرار في إعادة محاولة الرموز الفاشلة كل 30 دقيقة.

---

buyer.py - الكود المطور بالكامل

```python
"""
محرك الشراء التلقائي مع نظام عرض الرموز في السوق
عرض جميع الرموز المملوكة - إعادة المحاولة للفاشلة كل 30 دقيقة
إعادة عرض الناجحة كل 4 ساعات - حد أقصى 50 رمز لكل عملية
"""

import logging
import threading
import random
import time
from datetime import datetime, timezone
from enum import Enum
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Tuple, Any, Set
from web3 import Web3

log = logging.getLogger("buyer")

# ===========================================================================
# إعدادات نظام عرض الرموز
# ===========================================================================
MAX_LISTING_PRICE_USD = 0.0001  # 0.01 سنت كحد أقصى
MIN_LISTING_PRICE_USD = 0.00001  # 0.001 سنت كحد أدنى
LISTING_RETRY_INTERVAL = 1800  # 30 دقيقة للرموز الفاشلة
LISTING_RELIST_INTERVAL = 14400  # 4 ساعات لإعادة عرض الرموز الناجحة
MAX_LISTING_RETRIES = 5  # عدد محاولات إعادة العرض للفاشلة
LISTING_GAS_LIMIT = 300000
MAX_TOKENS_PER_LISTING_BATCH = 50  # حد أقصى 50 رمز لكل عملية عرض
BATCH_DELAY_BETWEEN_TOKENS = 1  # تأخير ثانية بين كل رمز
BATCH_DELAY_BETWEEN_BATCHES = 60  # تأخير دقيقة بين كل دفعة

# ===========================================================================
# إعدادات الشراء
# ===========================================================================
nonce_locks = {}
nonce_locks_lock = threading.Lock()

def get_wallet_lock(address: str) -> threading.Lock:
    with nonce_locks_lock:
        if address not in nonce_locks:
            nonce_locks[address] = threading.Lock()
        return nonce_locks[address]

MAX_ETH_PER_TX = 0.02
MAX_GAS_FEE_USD = 0.15
MIN_BALANCE_RESERVE_USD = 0.1
GAS_LIMIT_SAFETY_MARGIN = 1.05
FREE_PRICE_THRESHOLD_WEI = 1000

# ===========================================================================
# إعدادات السلاسل
# ===========================================================================
CHAINS_CONFIG = {
    "robinhood": {
        "seadrop_address": Web3.to_checksum_address("0x00005EA00Ac477B1030CE78506496e8C2dE24bf5"),
        "chain_name_display": "Robinhood Chain",
        "explorer_url": "https://explorer.robinhood.org/tx/",
        "marketplace_address": Web3.to_checksum_address("0x00005EA00Ac477B1030CE78506496e8C2dE24bf5"),
        "marketplace_name": "Robinhood NFT Marketplace",
        "is_poa": True,
        "rpc_url": "",
    },
    "ethereum": {
        "seadrop_address": Web3.to_checksum_address("0x00005EA00Ac477B1030CE78506496e8C2dE24bf5"),
        "chain_name_display": "Ethereum Mainnet",
        "explorer_url": "https://etherscan.io/tx/",
        "marketplace_address": Web3.to_checksum_address("0x00000000006c3852cbEf3e08E8dF289169EdE581"),
        "marketplace_name": "OpenSea",
        "is_poa": False,
        "rpc_url": "",
    },
}

ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"

# ===========================================================================
# ABI للعقود
# ===========================================================================
SEADROP_ABI = [
    {
        "inputs": [
            {"name": "nftContract", "type": "address"},
            {"name": "feeRecipient", "type": "address"},
            {"name": "minterIfNotPayer", "type": "address"},
            {"name": "quantity", "type": "uint256"},
        ],
        "name": "mintPublic",
        "outputs": [],
        "stateMutability": "payable",
        "type": "function",
    },
    {
        "inputs": [{"name": "nftContract", "type": "address"}],
        "name": "getAllowedFeeRecipients",
        "outputs": [{"name": "", "type": "address[]"}],
        "stateMutability": "view",
        "type": "function",
    },
]

# ABI للسوق (Marketplace)
MARKETPLACE_ABI = [
    {
        "inputs": [
            {"name": "nftContract", "type": "address"},
            {"name": "tokenId", "type": "uint256"},
            {"name": "price", "type": "uint256"},
        ],
        "name": "createListing",
        "outputs": [],
        "stateMutability": "nonpayable",
        "type": "function",
    },
    {
        "inputs": [
            {"name": "nftContract", "type": "address"},
            {"name": "tokenId", "type": "uint256"},
        ],
        "name": "getListing",
        "outputs": [
            {"name": "seller", "type": "address"},
            {"name": "price", "type": "uint256"},
            {"name": "isActive", "type": "bool"},
        ],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [
            {"name": "nftContract", "type": "address"},
            {"name": "tokenId", "type": "uint256"},
            {"name": "newPrice", "type": "uint256"},
        ],
        "name": "updateListing",
        "outputs": [],
        "stateMutability": "nonpayable",
        "type": "function",
    },
    {
        "inputs": [
            {"name": "nftContract", "type": "address"},
            {"name": "tokenId", "type": "uint256"},
        ],
        "name": "cancelListing",
        "outputs": [],
        "stateMutability": "nonpayable",
        "type": "function",
    },
    {
        "inputs": [],
        "name": "getListingFee",
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [],
        "name": "getMarketplaceFee",
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [{"name": "nftContract", "type": "address"}],
        "name": "getCollectionFee",
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [
            {"name": "nftContract", "type": "address"},
            {"name": "tokenId", "type": "uint256"},
        ],
        "name": "getListingFeeForToken",
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [{"name": "nftContract", "type": "address"}],
        "name": "getAllListings",
        "outputs": [
            {"name": "tokenIds", "type": "uint256[]"},
            {"name": "prices", "type": "uint256[]"},
            {"name": "sellers", "type": "address[]"},
        ],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [{"name": "nftContract", "type": "address"}],
        "name": "getActiveListings",
        "outputs": [
            {"name": "tokenIds", "type": "uint256[]"},
            {"name": "prices", "type": "uint256[]"},
            {"name": "sellers", "type": "address[]"},
        ],
        "stateMutability": "view",
        "type": "function",
    },
]

# ABI للـ NFT (ERC721)
ERC721_ABI = [
    {
        "inputs": [{"name": "owner", "type": "address"}],
        "name": "balanceOf",
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [{"name": "tokenId", "type": "uint256"}],
        "name": "ownerOf",
        "outputs": [{"name": "", "type": "address"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [
            {"name": "from", "type": "address"},
            {"name": "to", "type": "address"},
            {"name": "tokenId", "type": "uint256"},
        ],
        "name": "safeTransferFrom",
        "outputs": [],
        "stateMutability": "nonpayable",
        "type": "function",
    },
    {
        "inputs": [{"name": "owner", "type": "address"}],
        "name": "tokensOfOwner",
        "outputs": [{"name": "", "type": "uint256[]"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [{"name": "owner", "type": "address"}],
        "name": "getTokensOfOwner",
        "outputs": [{"name": "", "type": "uint256[]"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [{"name": "owner", "type": "address"}],
        "name": "walletOfOwner",
        "outputs": [{"name": "", "type": "uint256[]"}],
        "stateMutability": "view",
        "type": "function",
    },
]

# ===========================================================================
# كلاس البيانات الأساسية
# ===========================================================================
class RetryStrategy(Enum):
    FIXED = "fixed"
    EXPONENTIAL = "exponential"
    LINEAR = "linear"

@dataclass
class RetryConfig:
    max_attempts: Optional[int] = None
    base_delay: float = 3
    max_delay: float = 60
    strategy: RetryStrategy = RetryStrategy.FIXED
    backoff_multiplier: float = 1.5
    jitter: bool = True
    max_total_hours: Optional[float] = None

@dataclass
class MintResult:
    success: bool
    wallet: str = ""
    wallet_name: str = ""
    tx_hash: str = ""
    reason: str = ""
    reason_text: str = ""
    quantity: int = 0
    gas_used_eth: float = 0.0
    gas_used_usd: float = 0.0
    gas_units: int = 0
    gas_price_gwei: float = 0.0
    gas_estimated: int = 0
    balance_eth: float = 0.0
    balance_usd: float = 0.0
    total_value_eth: float = 0.0
    price_wei: int = 0
    chain_name: str = ""
    confirmed: bool = False
    is_free: bool = True
    is_eligible: bool = True
    token_ids: List[str] = field(default_factory=list)

@dataclass
class NFTListing:
    """بيانات عرض رمز في السوق."""
    token_id: str
    nft_contract: str
    wallet_address: str
    wallet_name: str
    price_wei: int
    price_usd: float
    chain_name: str
    listing_fee_wei: int = 0
    marketplace_fee_wei: int = 0
    attempts: int = 0  # عدد محاولات الفشل
    relist_attempts: int = 0  # عدد مرات إعادة العرض الناجحة
    last_try: float = field(default_factory=time.time)
    last_success: float = 0  # وقت آخر عرض ناجح
    status: str = "pending"  # pending, listed, failed, retrying, relisting
    error: str = ""
    tx_hash: str = ""
    created_at: float = field(default_factory=time.time)

# ===========================================================================
# إدارة عروض الرموز - مع إعادة عرض الناجحة بعد 4 ساعات
# ===========================================================================
class ListingManager:
    """
    إدارة عرض الرموز في السوق.
    - يعرض جميع الرموز المملوكة
    - يعيد المحاولة للرموز الفاشلة كل 30 دقيقة
    - يعيد عرض الرموز الناجحة كل 4 ساعات
    - حد أقصى 50 رمز لكل عملية
    """
    
    def __init__(self):
        self.listings: Dict[str, NFTListing] = {}
        self.lock = threading.Lock()
        self._listed_tokens: Set[str] = set()  # الرموز التي تم عرضها بنجاح
        self._failed_tokens: Set[str] = set()  # الرموز التي فشل عرضها
        self._pending_tokens: Set[str] = set()  # الرموز في انتظار العرض
        self._relist_tokens: Set[str] = set()  # الرموز التي تحتاج إعادة عرض
        self._active_batch: List[str] = []  # الرموز الجاري عرضها حالياً
        self._total_tokens_processed = 0
        self._last_full_scan = 0  # وقت آخر فحص كامل
    
    def add_listing(self, token_id: str, nft_contract: str, wallet_address: str,
                   wallet_name: str, price_wei: int, price_usd: float,
                   chain_name: str) -> bool:
        """
        إضافة رمز جديد لقائمة العرض.
        يعيد True إذا تمت الإضافة، False إذا كان مكرراً أو تم عرضه سابقاً.
        """
        listing_key = f"{token_id}:{nft_contract}:{wallet_address}"
        
        with self.lock:
            # التحقق من عدم التكرار
            if listing_key in self._listed_tokens:
                return False
            
            # التحقق من أنه ليس فاشلاً سابقاً (سيتم إعادة المحاولة)
            if listing_key in self._failed_tokens:
                if token_id in self.listings:
                    self.listings[token_id].status = "retrying"
                    self.listings[token_id].last_try = time.time()
                    self._failed_tokens.remove(listing_key)
                    self._pending_tokens.add(listing_key)
                    log.info(f"🔄 إعادة محاولة عرض الرمز {token_id[:8]}... (فاشل سابقاً)")
                    return True
            
            if token_id not in self.listings:
                self.listings[token_id] = NFTListing(
                    token_id=token_id,
                    nft_contract=nft_contract,
                    wallet_address=wallet_address,
                    wallet_name=wallet_name,
                    price_wei=price_wei,
                    price_usd=price_usd,
                    chain_name=chain_name,
                )
                self._pending_tokens.add(listing_key)
                self._total_tokens_processed += 1
                log.info(f"📋 تم إضافة الرمز {token_id[:8]}... (المجموع: {self._total_tokens_processed})")
                return True
            
            return False
    
    def add_listings_batch(self, listings: List[Dict]) -> int:
        """إضافة مجموعة من الرموز دفعة واحدة."""
        added = 0
        for listing in listings:
            if self.add_listing(
                token_id=listing["token_id"],
                nft_contract=listing["nft_contract"],
                wallet_address=listing["wallet_address"],
                wallet_name=listing["wallet_name"],
                price_wei=listing["price_wei"],
                price_usd=listing["price_usd"],
                chain_name=listing["chain_name"],
            ):
                added += 1
        return added
    
    def get_all_tokens_for_listing(self) -> List[NFTListing]:
        """
        الحصول على جميع الرموز التي تحتاج للعرض:
        1. الرموز الجديدة (pending)
        2. الرموز الفاشلة التي حان وقت إعادة محاولتها (بعد 30 دقيقة)
        3. الرموز الناجحة التي حان وقت إعادة عرضها (بعد 4 ساعات)
        """
        with self.lock:
            now = time.time()
            result = []
            
            # 1. الرموز الجديدة (pending)
            for token_id, data in self.listings.items():
                if data.status == "pending":
                    result.append(data)
            
            # 2. الرموز الفاشلة التي حان وقت إعادة محاولتها (بعد 30 دقيقة)
            for token_id, data in self.listings.items():
                if data.status == "retrying":
                    if data.attempts < MAX_LISTING_RETRIES:
                        if now - data.last_try >= LISTING_RETRY_INTERVAL:
                            result.append(data)
            
            # 3. الرموز الناجحة التي حان وقت إعادة عرضها (بعد 4 ساعات)
            for token_id, data in self.listings.items():
                if data.status == "listed":
                    if data.last_success > 0:
                        if now - data.last_success >= LISTING_RELIST_INTERVAL:
                            data.status = "relisting"
                            result.append(data)
                            log.info(f"🔄 إعادة عرض الرمز {token_id[:8]}... (آخر عرض منذ { (now - data.last_success) / 3600:.1f} ساعة)")
            
            return result
    
    def get_next_batch(self, limit: int = MAX_TOKENS_PER_LISTING_BATCH) -> List[NFTListing]:
        """
        الحصول على الدفعة التالية من الرموز (حتى 50).
        الأولوية: جدد > فاشلة > ناجحة تحتاج إعادة عرض
        """
        all_tokens = self.get_all_tokens_for_listing()
        
        # ترتيب حسب الأولوية
        priority_order = {"pending": 0, "retrying": 1, "relisting": 2, "listed": 3}
        sorted_tokens = sorted(all_tokens, key=lambda x: priority_order.get(x.status, 4))
        
        return sorted_tokens[:limit]
    
    def get_failed_tokens(self) -> List[NFTListing]:
        """الحصول على جميع الرموز الفاشلة."""
        with self.lock:
            return [data for data in self.listings.values() if data.status == "failed"]
    
    def get_listed_tokens(self) -> List[NFTListing]:
        """الحصول على جميع الرموز الناجحة."""
        with self.lock:
            return [data for data in self.listings.values() if data.status == "listed"]
    
    def get_pending_count(self) -> int:
        """عدد الرموز في انتظار العرض."""
        with self.lock:
            return sum(1 for d in self.listings.values() if d.status == "pending")
    
    def get_failed_count(self) -> int:
        """عدد الرموز الفاشلة."""
        with self.lock:
            return sum(1 for d in self.listings.values() if d.status == "failed")
    
    def get_retrying_count(self) -> int:
        """عدد الرموز في انتظار إعادة المحاولة."""
        with self.lock:
            return sum(1 for d in self.listings.values() if d.status == "retrying")
    
    def get_listed_count(self) -> int:
        """عدد الرموز المعروضة بنجاح."""
        with self.lock:
            return sum(1 for d in self.listings.values() if d.status == "listed")
    
    def get_relisting_count(self) -> int:
        """عدد الرموز في انتظار إعادة العرض."""
        with self.lock:
            return sum(1 for d in self.listings.values() if d.status == "relisting")
    
    def mark_success(self, token_id: str, tx_hash: str) -> None:
        """تحديث حالة الرمز إلى نجاح."""
        with self.lock:
            if token_id in self.listings:
                data = self.listings[token_id]
                data.status = "listed"
                data.tx_hash = tx_hash
                data.last_success = time.time()
                data.relist_attempts += 1
                
                listing_key = f"{token_id}:{data.nft_contract}:{data.wallet_address}"
                self._listed_tokens.add(listing_key)
                self._pending_tokens.discard(listing_key)
                self._failed_tokens.discard(listing_key)
                self._relist_tokens.discard(listing_key)
                
                if token_id in self._active_batch:
                    self._active_batch.remove(token_id)
                
                log.info(f"✅ تم عرض الرمز {token_id[:8]}... بنجاح (إعادة عرض #{data.relist_attempts})")
    
    def mark_failed(self, token_id: str, error: str) -> None:
        """
        تحديث حالة الرمز إلى فشل.
        سيتم إعادة المحاولة بعد 30 دقيقة.
        """
        with self.lock:
            if token_id in self.listings:
                data = self.listings[token_id]
                data.attempts += 1
                data.last_try = time.time()
                data.error = error
                listing_key = f"{token_id}:{data.nft_contract}:{data.wallet_address}"
                
                if token_id in self._active_batch:
                    self._active_batch.remove(token_id)
                
                if data.attempts >= MAX_LISTING_RETRIES:
                    data.status = "failed"
                    self._failed_tokens.add(listing_key)
                    self._pending_tokens.discard(listing_key)
                    log.warning(f"❌ فشل عرض الرمز {token_id[:8]}... بعد {MAX_LISTING_RETRIES} محاولات")
                else:
                    data.status = "retrying"
                    self._pending_tokens.discard(listing_key)
                    log.info(f"🔄 سيتم إعادة محاولة عرض الرمز {token_id[:8]}... بعد 30 دقيقة")
    
    def has_pending_listings(self) -> bool:
        """التحقق من وجود رموز معلقة للعرض."""
        with self.lock:
            # رموز جديدة في انتظار العرض
            for data in self.listings.values():
                if data.status == "pending":
                    return True
            # رموز فاشلة في انتظار إعادة المحاولة
            for data in self.listings.values():
                if data.status == "retrying":
                    if data.attempts < MAX_LISTING_RETRIES:
                        if time.time() - data.last_try >= LISTING_RETRY_INTERVAL:
                            return True
            # رموز ناجحة تحتاج إعادة عرض
            for data in self.listings.values():
                if data.status == "listed":
                    if data.last_success > 0:
                        if time.time() - data.last_success >= LISTING_RELIST_INTERVAL:
                            return True
            return False
    
    def get_stats(self) -> Dict:
        """الحصول على إحصائيات العروض."""
        with self.lock:
            total = len(self.listings)
            success = sum(1 for d in self.listings.values() if d.status == "listed")
            failed = sum(1 for d in self.listings.values() if d.status == "failed")
            pending = sum(1 for d in self.listings.values() if d.status == "pending")
            retrying = sum(1 for d in self.listings.values() if d.status == "retrying")
            relisting = sum(1 for d in self.listings.values() if d.status == "relisting")
            
            return {
                "total": total,
                "success": success,
                "failed": failed,
                "pending": pending,
                "retrying": retrying,
                "relisting": relisting,
                "batch_size": len(self._active_batch),
                "total_processed": self._total_tokens_processed,
                "max_per_batch": MAX_TOKENS_PER_LISTING_BATCH,
                "retry_interval_minutes": LISTING_RETRY_INTERVAL // 60,
                "relist_interval_hours": LISTING_RELIST_INTERVAL // 3600,
            }

# ===========================================================================
# دوال استخراج رسوم العرض من العقد
# ===========================================================================
def get_listing_fees(
    w3: Web3,
    marketplace_address: str,
    nft_contract: Optional[str] = None,
    token_id: Optional[int] = None,
) -> Dict[str, int]:
    """استخراج رسوم العرض من عقد السوق."""
    result = {
        "listing_fee": 0,
        "marketplace_fee": 0,
        "collection_fee": 0,
        "token_fee": 0,
        "total_fee": 0,
    }
    
    try:
        marketplace = w3.eth.contract(
            address=Web3.to_checksum_address(marketplace_address),
            abi=MARKETPLACE_ABI
        )
        
        try:
            result["listing_fee"] = marketplace.functions.getListingFee().call()
        except:
            pass
        
        try:
            result["marketplace_fee"] = marketplace.functions.getMarketplaceFee().call()
        except:
            pass
        
        if nft_contract:
            try:
                result["collection_fee"] = marketplace.functions.getCollectionFee(
                    Web3.to_checksum_address(nft_contract)
                ).call()
            except:
                pass
        
        if nft_contract and token_id is not None:
            try:
                result["token_fee"] = marketplace.functions.getListingFeeForToken(
                    Web3.to_checksum_address(nft_contract),
                    token_id
                ).call()
            except:
                pass
        
        result["total_fee"] = (
            result["listing_fee"] +
            result["marketplace_fee"] +
            result["collection_fee"] +
            result["token_fee"]
        )
        
        return result
        
    except Exception as e:
        log.warning(f"⚠️ فشل استخراج رسوم العرض: {e}")
        return result


def get_listing_fee_eth(fees: Dict[str, int]) -> float:
    return fees.get("total_fee", 0) / 1e18


def get_listing_fee_usd(fees: Dict[str, int], eth_price_usd: float) -> float:
    return (fees.get("total_fee", 0) / 1e18) * eth_price_usd


def calculate_listing_price(
    eth_price_usd: float,
    stage_price_wei: int = 0,
    fees: Optional[Dict[str, int]] = None,
) -> Tuple[int, float, Dict]:
    """
    حساب سعر العرض المناسب مع مراعاة رسوم السوق.
    """
    if fees is None:
        fees = {"total_fee": 0}
    
    base_wei = stage_price_wei
    profit_wei = int((0.00005 / eth_price_usd) * 1e18)
    fee_wei = fees.get("total_fee", 0)
    
    final_wei = base_wei + profit_wei + fee_wei
    final_usd = (final_wei / 1e18) * eth_price_usd
    
    if final_usd > MAX_LISTING_PRICE_USD:
        final_wei = int((MAX_LISTING_PRICE_USD / eth_price_usd) * 1e18)
        final_usd = (final_wei / 1e18) * eth_price_usd
    
    if final_wei <= 0:
        final_wei = int((MIN_LISTING_PRICE_USD / eth_price_usd) * 1e18)
        final_usd = (final_wei / 1e18) * eth_price_usd
    
    return final_wei, final_usd, fees

# ===========================================================================
# دوال الحصول على الرموز المملوكة
# ===========================================================================
def get_nft_balance(
    w3: Web3,
    nft_contract: str,
    wallet_address: str,
) -> int:
    """جلب عدد الرموز المملوكة للمحفظة."""
    try:
        contract = w3.eth.contract(
            address=Web3.to_checksum_address(nft_contract),
            abi=ERC721_ABI
        )
        return contract.functions.balanceOf(
            Web3.to_checksum_address(wallet_address)
        ).call()
    except:
        return 0


def get_tokens_of_owner(
    w3: Web3,
    nft_contract: str,
    wallet_address: str,
) -> List[int]:
    """جلب جميع الرموز المملوكة للمحفظة."""
    contract = w3.eth.contract(
        address=Web3.to_checksum_address(nft_contract),
        abi=ERC721_ABI
    )
    
    wallet = Web3.to_checksum_address(wallet_address)
    
    try:
        return contract.functions.tokensOfOwner(wallet).call()
    except:
        pass
    
    try:
        return contract.functions.getTokensOfOwner(wallet).call()
    except:
        pass
    
    try:
        return contract.functions.walletOfOwner(wallet).call()
    except:
        pass
    
    return []


def get_all_owned_tokens(
    w3: Web3,
    nft_contract: str,
    wallet_address: str,
    known_token_ids: Optional[List[str]] = None,
) -> List[int]:
    """الحصول على جميع الرموز المملوكة للمحفظة."""
    tokens = []
    
    try:
        tokens = get_tokens_of_owner(w3, nft_contract, wallet_address)
    except Exception as e:
        log.warning(f"⚠️ فشل الحصول على الرموز المملوكة: {e}")
    
    if known_token_ids:
        for tid in known_token_ids:
            try:
                token_id = int(tid, 16) if tid.startswith("0x") else int(tid)
                if token_id not in tokens:
                    tokens.append(token_id)
            except:
                pass
    
    return tokens

# ===========================================================================
# دوال عرض الرموز في السوق
# ===========================================================================
def create_listing_tx(
    w3: Web3,
    nft_contract: str,
    token_id: int,
    price_wei: int,
    wallet_address: str,
    marketplace_address: str,
) -> Dict:
    """بناء معاملة عرض الرمز في السوق."""
    marketplace = w3.eth.contract(
        address=Web3.to_checksum_address(marketplace_address),
        abi=MARKETPLACE_ABI
    )
    
    tx = marketplace.functions.createListing(
        Web3.to_checksum_address(nft_contract),
        token_id,
        price_wei,
    ).build_transaction({
        "from": Web3.to_checksum_address(wallet_address),
        "nonce": w3.eth.get_transaction_count(
            Web3.to_checksum_address(wallet_address),
            "pending"
        ),
        "gasPrice": w3.eth.gas_price,
        "chainId": w3.eth.chain_id,
    })
    
    return tx


def attempt_listing(
    w3: Web3,
    private_key: str,
    wallet_address: str,
    nft_contract: str,
    token_id: int,
    price_wei: int,
    marketplace_address: str,
    chain_name: str,
) -> Dict:
    """محاولة عرض رمز في السوق."""
    wallet_lock = get_wallet_lock(wallet_address)
    wallet_lock.acquire()
    
    try:
        tx = create_listing_tx(
            w3, nft_contract, token_id, price_wei,
            wallet_address, marketplace_address
        )
        
        try:
            estimated_gas = w3.eth.estimate_gas(tx)
            tx["gas"] = int(estimated_gas * GAS_LIMIT_SAFETY_MARGIN)
            log.info(f"⛽ الغاز المقدر للعرض: {estimated_gas}")
        except Exception as e:
            log.warning(f"⚠️ فشل estimate_gas للعرض: {e}")
            tx["gas"] = LISTING_GAS_LIMIT
        
        signed_tx = w3.eth.account.sign_transaction(tx, private_key=private_key)
        raw_tx = getattr(signed_tx, 'raw_transaction', None)
        if raw_tx is None:
            raw_tx = getattr(signed_tx, 'rawTransaction', None)
        
        if raw_tx is None:
            raise ValueError("raw_transaction غير موجود")
        
        tx_hash = w3.eth.send_raw_transaction(raw_tx)
        tx_hash_str = tx_hash.hex() if hasattr(tx_hash, 'hex') else str(tx_hash)
        
        try:
            receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=30)
            if receipt and receipt.status == 1:
                log.info(f"✅ تم عرض الرمز {token_id} في السوق")
                return {
                    "success": True,
                    "tx_hash": tx_hash_str,
                    "gas_used": receipt.gasUsed,
                }
            else:
                return {"success": False, "reason": "listing_reverted"}
        except:
            log.info(f"✅ تم إرسال معاملة عرض الرمز {token_id}")
            return {
                "success": True,
                "tx_hash": tx_hash_str,
                "gas_used": tx.get("gas", 0),
                "pending": True,
            }
            
    except Exception as e:
        error_msg = str(e).lower()
        log.error(f"❌ فشل عرض الرمز: {e}")
        
        if "insufficient funds" in error_msg:
            return {"success": False, "reason": "insufficient_funds"}
        elif "gas" in error_msg:
            return {"success": False, "reason": "gas_error"}
        elif "nonce" in error_msg:
            return {"success": False, "reason": "nonce_error"}
        else:
            return {"success": False, "reason": "listing_error", "error": str(e)}
    finally:
        wallet_lock.release()


def list_all_owned_tokens(
    w3: Web3,
    private_key: str,
    wallet_address: str,
    wallet_name: str,
    nft_contract: str,
    chain_name: str,
    eth_price_usd: float,
    marketplace_address: str,
    known_token_ids: Optional[List[str]] = None,
    listing_manager: Optional[ListingManager] = None,
) -> Dict:
    """
    عرض جميع الرموز المملوكة في السوق.
    """
    results = {
        "total_owned": 0,
        "total_listed": 0,
        "total_failed": 0,
        "total_retrying": 0,
        "details": [],
    }
    
    token_ids = get_all_owned_tokens(
        w3, nft_contract, wallet_address, known_token_ids
    )
    
    if not token_ids:
        log.info(f"ℹ️ لا توجد رموز مملوكة للمحفظة {wallet_name}")
        return results
    
    results["total_owned"] = len(token_ids)
    
    fees = get_listing_fees(w3, marketplace_address, nft_contract)
    price_wei, price_usd, fees = calculate_listing_price(
        eth_price_usd, 0, fees
    )
    
    log.info(f"📋 سيتم عرض {len(token_ids)} رمز بسعر ${price_usd:.6f}")
    log.info(f"💰 رسوم العرض: {get_listing_fee_eth(fees):.6f} ETH")
    
    for token_id in token_ids:
        try:
            if listing_manager:
                added = listing_manager.add_listing(
                    token_id=str(token_id),
                    nft_contract=nft_contract,
                    wallet_address=wallet_address,
                    wallet_name=wallet_name,
                    price_wei=price_wei,
                    price_usd=price_usd,
                    chain_name=chain_name,
                )
                
                if added:
                    result = attempt_listing(
                        w3=w3,
                        private_key=private_key,
                        wallet_address=wallet_address,
                        nft_contract=nft_contract,
                        token_id=token_id,
                        price_wei=price_wei,
                        marketplace_address=marketplace_address,
                        chain_name=chain_name,
                    )
                    
                    if result.get("success"):
                        listing_manager.mark_success(str(token_id), result.get("tx_hash", ""))
                        results["total_listed"] += 1
                    else:
                        listing_manager.mark_failed(str(token_id), result.get("reason", "unknown"))
                        results["total_failed"] += 1
                    
                    results["details"].append({
                        "token_id": str(token_id),
                        "success": result.get("success", False),
                        "tx_hash": result.get("tx_hash", ""),
                        "reason": result.get("reason", ""),
                    })
                    
                    time.sleep(BATCH_DELAY_BETWEEN_TOKENS)
            else:
                result = attempt_listing(
                    w3=w3,
                    private_key=private_key,
                    wallet_address=wallet_address,
                    nft_contract=nft_contract,
                    token_id=token_id,
                    price_wei=price_wei,
                    marketplace_address=marketplace_address,
                    chain_name=chain_name,
                )
                
                if result.get("success"):
                    results["total_listed"] += 1
                else:
                    results["total_failed"] += 1
                
                results["details"].append({
                    "token_id": str(token_id),
                    "success": result.get("success", False),
                    "tx_hash": result.get("tx_hash", ""),
                    "reason": result.get("reason", ""),
                })
                
                time.sleep(BATCH_DELAY_BETWEEN_TOKENS)
            
        except Exception as e:
            log.error(f"❌ فشل عرض الرمز {token_id}: {e}")
            results["total_failed"] += 1
            results["details"].append({
                "token_id": str(token_id),
                "success": False,
                "reason": str(e),
            })
    
    return results


def retry_failed_listings(
    listing_manager: ListingManager,
    w3: Web3,
    private_key: str,
    wallet_address: str,
    nft_contract: str,
    chain_name: str,
    marketplace_address: str,
) -> Dict:
    """
    إعادة محاولة عرض الرموز الفاشلة فقط (كل 30 دقيقة).
    """
    results = {
        "total_retried": 0,
        "total_success": 0,
        "total_failed": 0,
        "details": [],
    }
    
    failed_tokens = []
    with listing_manager.lock:
        now = time.time()
        for token_id, data in listing_manager.listings.items():
            if data.status == "retrying":
                if data.attempts < MAX_LISTING_RETRIES:
                    if now - data.last_try >= LISTING_RETRY_INTERVAL:
                        failed_tokens.append(data)
    
    if not failed_tokens:
        log.info("ℹ️ لا توجد رموز فاشلة لإعادة المحاولة")
        return results
    
    log.info(f"🔄 إعادة محاولة عرض {len(failed_tokens)} رمز فاشل")
    results["total_retried"] = len(failed_tokens)
    
    for data in failed_tokens:
        try:
            token_id = int(data.token_id)
            
            result = attempt_listing(
                w3=w3,
                private_key=private_key,
                wallet_address=wallet_address,
                nft_contract=nft_contract,
                token_id=token_id,
                price_wei=data.price_wei,
                marketplace_address=marketplace_address,
                chain_name=chain_name,
            )
            
            if result.get("success"):
                listing_manager.mark_success(data.token_id, result.get("tx_hash", ""))
                results["total_success"] += 1
            else:
                listing_manager.mark_failed(data.token_id, result.get("reason", "unknown"))
                results["total_failed"] += 1
            
            results["details"].append({
                "token_id": data.token_id,
                "success": result.get("success", False),
                "tx_hash": result.get("tx_hash", ""),
                "reason": result.get("reason", ""),
            })
            
            time.sleep(BATCH_DELAY_BETWEEN_TOKENS)
            
        except Exception as e:
            log.error(f"❌ فشل إعادة محاولة عرض الرمز {data.token_id}: {e}")
            listing_manager.mark_failed(data.token_id, str(e))
            results["total_failed"] += 1
            results["details"].append({
                "token_id": data.token_id,
                "success": False,
                "reason": str(e),
            })
    
    return results


def relist_successful_tokens(
    listing_manager: ListingManager,
    w3: Web3,
    private_key: str,
    wallet_address: str,
    nft_contract: str,
    chain_name: str,
    marketplace_address: str,
) -> Dict:
    """
    إعادة عرض الرموز الناجحة بعد 4 ساعات.
    """
    results = {
        "total_relisted": 0,
        "total_success": 0,
        "total_failed": 0,
        "details": [],
    }
    
    relist_tokens = []
    with listing_manager.lock:
        now = time.time()
        for token_id, data in listing_manager.listings.items():
            if data.status == "listed":
                if data.last_success > 0:
                    if now - data.last_success >= LISTING_RELIST_INTERVAL:
                        relist_tokens.append(data)
    
    if not relist_tokens:
        log.info("ℹ️ لا توجد رموز ناجحة تحتاج إعادة عرض")
        return results
    
    log.info(f"🔄 إعادة عرض {len(relist_tokens)} رمز ناجح (بعد 4 ساعات)")
    results["total_relisted"] = len(relist_tokens)
    
    for data in relist_tokens:
        try:
            token_id = int(data.token_id)
            
            result = attempt_listing(
                w3=w3,
                private_key=private_key,
                wallet_address=wallet_address,
                nft_contract=nft_contract,
                token_id=token_id,
                price_wei=data.price_wei,
                marketplace_address=marketplace_address,
                chain_name=chain_name,
            )
            
            if result.get("success"):
                listing_manager.mark_success(data.token_id, result.get("tx_hash", ""))
                results["total_success"] += 1
            else:
                listing_manager.mark_failed(data.token_id, result.get("reason", "unknown"))
                results["total_failed"] += 1
            
            results["details"].append({
                "token_id": data.token_id,
                "success": result.get("success", False),
                "tx_hash": result.get("tx_hash", ""),
                "reason": result.get("reason", ""),
            })
            
            time.sleep(BATCH_DELAY_BETWEEN_TOKENS)
            
        except Exception as e:
            log.error(f"❌ فشل إعادة عرض الرمز {data.token_id}: {e}")
            results["total_failed"] += 1
            results["details"].append({
                "token_id": data.token_id,
                "success": False,
                "reason": str(e),
            })
    
    return results

# ===========================================================================
# دوال مساعدة أخرى
# ===========================================================================
def get_reason_text(reason: str) -> str:
    if not reason:
        return "غير محدد"
    
    reasons = {
        "balance_too_low": "الرصيد منخفض جداً",
        "gas_too_high": "رسوم الغاز مرتفعة (أكثر من 15 سنت)",
        "tx_value_too_high": "قيمة المعاملة عالية",
        "no_fee_recipient": "لا يوجد عنوان رسوم متاح",
        "simulation_failed": "فشلت محاكاة المعاملة",
        "not_free_mint": "المينت مدفوع وليس مجاني",
        "tx_error": "خطأ في الشبكة",
        "nonce_error": "خطأ في رقم المعاملة (nonce)",
        "insufficient_funds": "الرصيد غير كاف للشراء",
        "sold_out": "نفذت الكمية",
        "not_eligible": "المحفظة غير مؤهلة للشراء",
        "tx_reverted": "المعاملة فشلت على البلوكشين",
        "tx_pending": "المعاملة قيد التأكيد",
        "wallet_limit_reached": "وصلت للحد الأقصى",
        "listing_error": "فشل عرض الرمز في السوق",
        "listing_reverted": "فشلت معاملة العرض",
        "gas_error": "خطأ في الغاز",
        "unknown": "خطأ غير معروف",
    }
    return reasons.get(reason, f"خطأ: {reason}")

def is_reason_retryable(reason: str) -> bool:
    retryable = {
        "gas_too_high", "simulation_failed", "tx_error", "no_fee_recipient",
        "nonce_error", "insufficient_funds", "tx_pending", "listing_error",
        "gas_error", "listing_reverted"
    }
    return reason in retryable

def is_reason_permanent(reason: str) -> bool:
    permanent = {
        "not_free_mint", "not_eligible", "wallet_limit_reached",
        "balance_too_low", "tx_value_too_high", "sold_out"
    }
    return reason in permanent

def is_price_free(price_wei: int) -> bool:
    return price_wei <= FREE_PRICE_THRESHOLD_WEI

def parse_stage_time(time_str: str) -> Optional[datetime]:
    if not time_str:
        return None
    try:
        return datetime.fromisoformat(time_str.replace("Z", "+00:00"))
    except:
        return None

def get_stage_status(stage: dict) -> str:
    now = datetime.now(timezone.utc)
    start_dt = parse_stage_time(stage.get("start_time", ""))
    end_dt = parse_stage_time(stage.get("end_time", ""))
    if start_dt and now < start_dt:
        return "upcoming"
    if end_dt and now > end_dt:
        return "ended"
    if start_dt and now >= start_dt:
        return "active"
    return "unknown"

def safe_parse_price(price_value) -> int:
    if price_value is None:
        return 0
    try:
        if isinstance(price_value, str):
            price_value = price_value.strip()
            if not price_value:
                return 0
            price_float = float(price_value)
            if 0 < price_float < 1:
                return int(price_float * 1e18)
            return int(price_float)
        if isinstance(price_value, (int, float)):
            return int(price_value)
        return 0
    except:
        return 0

def extract_price_from_stage(stage: dict) -> int:
    for field in ["price", "price_wei", "mint_price", "mintPrice", "cost", "value", "price_per_token", "pricePerToken"]:
        value = stage.get(field)
        if value is not None and value != "":
            return safe_parse_price(value)
    return 0

def extract_stage_name(stage: dict) -> str:
    for field in ["stage", "name", "phase", "title", "stage_name"]:
        value = stage.get(field)
        if value and isinstance(value, str) and value.strip():
            return value.strip()
    return "عام"

def decide_quantity(max_per_wallet, remaining_supply):
    if max_per_wallet is None:
        return min(20, remaining_supply)
    return max(1, min(max_per_wallet, remaining_supply, 20))

def get_web3_from_config(chain_config: dict) -> Web3:
    rpc_url = chain_config.get("rpc_url", "")
    if not rpc_url:
        raise ValueError("لا يوجد RPC URL")
    
    import requests
    session = requests.Session()
    session.headers.update({
        "Accept": "application/json",
        "Content-Type": "application/json",
    })
    
    w3 = Web3(Web3.HTTPProvider(rpc_url, session=session, request_kwargs={"timeout": 30}))
    
    if chain_config.get("is_poa", False):
        try:
            from web3.middleware import ExtraDataToPOAMiddleware
            w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
        except:
            pass
    
    return w3

def get_wallet_balance(w3: Web3, wallet_address: str) -> tuple:
    try:
        balance_wei = w3.eth.get_balance(Web3.to_checksum_address(wallet_address))
        return balance_wei / 1e18, balance_wei
    except:
        return 0.0, 0

def get_fee_recipient(w3: Web3, seadrop_address: str, nft_contract: str) -> Optional[str]:
    try:
        contract = w3.eth.contract(address=Web3.to_checksum_address(seadrop_address), abi=SEADROP_ABI)
        recipients = contract.functions.getAllowedFeeRecipients(Web3.to_checksum_address(nft_contract)).call()
        return recipients[0] if recipients else None
    except:
        return None

# ===========================================================================
# دوال الشراء (معدلة)
# ===========================================================================
def attempt_purchase(
    w3,
    private_key,
    wallet_address,
    nft_contract,
    seadrop_address,
    price_wei,
    max_per_wallet,
    remaining_supply,
    eth_price_usd
):
    is_free = is_price_free(price_wei)
    if not is_free:
        return MintResult(
            success=False,
            wallet=wallet_address,
            reason="not_free_mint",
            reason_text=get_reason_text("not_free_mint"),
            price_wei=price_wei,
            is_free=False,
            is_eligible=True
        )
    
    balance_eth, balance_wei = get_wallet_balance(w3, wallet_address)
    balance_usd = balance_eth * eth_price_usd
    
    if balance_usd < MIN_BALANCE_RESERVE_USD:
        return MintResult(
            success=False,
            wallet=wallet_address,
            reason="balance_too_low",
            reason_text=get_reason_text("balance_too_low"),
            balance_eth=balance_eth,
            balance_usd=balance_usd,
            is_free=True,
            is_eligible=True
        )
    
    quantity = decide_quantity(max_per_wallet, remaining_supply)
    total_value_wei = price_wei * quantity
    
    if total_value_wei / 1e18 > MAX_ETH_PER_TX:
        return MintResult(
            success=False,
            wallet=wallet_address,
            reason="tx_value_too_high",
            reason_text=get_reason_text("tx_value_too_high"),
            balance_eth=balance_eth,
            balance_usd=balance_usd,
            is_free=True,
            is_eligible=True
        )
    
    wallet_lock = get_wallet_lock(wallet_address)
    wallet_lock.acquire()
    
    try:
        contract = w3.eth.contract(address=Web3.to_checksum_address(seadrop_address), abi=SEADROP_ABI)
        nonce = w3.eth.get_transaction_count(Web3.to_checksum_address(wallet_address), "pending")
        gas_price = w3.eth.gas_price
        
        fee_recipient = get_fee_recipient(w3, seadrop_address, nft_contract)
        if not fee_recipient:
            return MintResult(
                success=False,
                wallet=wallet_address,
                reason="no_fee_recipient",
                reason_text=get_reason_text("no_fee_recipient"),
                balance_eth=balance_eth,
                balance_usd=balance_usd,
                is_free=True,
                is_eligible=True
            )
        
        tx = contract.functions.mintPublic(
            Web3.to_checksum_address(nft_contract),
            Web3.to_checksum_address(fee_recipient),
            Web3.to_checksum_address(ZERO_ADDRESS),
            quantity
        ).build_transaction({
            "from": Web3.to_checksum_address(wallet_address),
            "value": total_value_wei,
            "nonce": nonce,
            "gasPrice": gas_price,
            "chainId": w3.eth.chain_id,
        })
        
        try:
            estimated_gas = w3.eth.estimate_gas(tx)
            tx["gas"] = int(estimated_gas * GAS_LIMIT_SAFETY_MARGIN)
            gas_price_gwei = gas_price / 1e9
            gas_usd = (tx["gas"] * gas_price) / 1e18 * eth_price_usd
            
            if gas_usd > MAX_GAS_FEE_USD:
                return MintResult(
                    success=False,
                    wallet=wallet_address,
                    reason="gas_too_high",
                    reason_text=get_reason_text("gas_too_high"),
                    gas_units=tx["gas"],
                    gas_estimated=estimated_gas,
                    gas_price_gwei=gas_price_gwei,
                    gas_used_usd=gas_usd,
                    balance_eth=balance_eth,
                    balance_usd=balance_usd,
                    is_free=True,
                    is_eligible=True
                )
        except Exception as e:
            return MintResult(
                success=False,
                wallet=wallet_address,
                reason="simulation_failed",
                reason_text=get_reason_text("simulation_failed"),
                balance_eth=balance_eth,
                balance_usd=balance_usd,
                is_free=True,
                is_eligible=True
            )
        
        signed_tx = w3.eth.account.sign_transaction(tx, private_key)
        tx_hash = w3.eth.send_raw_transaction(
            signed_tx.rawTransaction if hasattr(signed_tx, 'rawTransaction') else signed_tx.raw_transaction
        )
        tx_hash_str = tx_hash.hex() if hasattr(tx_hash, 'hex') else str(tx_hash)
        gas_used_usd = (tx["gas"] * gas_price) / 1e18 * eth_price_usd
        
        return MintResult(
            success=True,
            wallet=wallet_address,
            tx_hash=tx_hash_str,
            quantity=quantity,
            gas_used_eth=(tx["gas"] * gas_price) / 1e18,
            gas_used_usd=gas_used_usd,
            gas_units=tx["gas"],
            gas_price_gwei=gas_price / 1e9,
            gas_estimated=estimated_gas,
            total_value_eth=total_value_wei / 1e18,
            balance_eth=balance_eth,
            balance_usd=balance_usd,
            confirmed=False,
            is_free=True,
            is_eligible=True
        )
        
    except Exception as e:
        error_msg = str(e).lower()
        
        if "not eligible" in error_msg or "not in allowlist" in error_msg:
            reason = "not_eligible"
        elif "insufficient funds" in error_msg:
            reason = "insufficient_funds"
        elif "gas" in error_msg:
            reason = "gas_too_high"
        elif "nonce" in error_msg:
            reason = "nonce_error"
        elif "revert" in error_msg or "execution reverted" in error_msg:
            reason = "simulation_failed"
        else:
            reason = "tx_error"
        
        return MintResult(
            success=False,
            wallet=wallet_address,
            reason=reason,
            reason_text=get_reason_text(reason),
            balance_eth=balance_eth,
            balance_usd=balance_usd,
            price_wei=price_wei,
            is_free=is_free,
            is_eligible=(reason != "not_eligible")
        )
    finally:
        wallet_lock.release()

# ===========================================================================
# دوال استخراج المراحل
# ===========================================================================
def extract_all_stages_raw(detail: dict) -> list:
    all_stages = []
    if detail.get("active_stage"):
        all_stages.append(detail["active_stage"])
    for s in detail.get("stages", []):
        all_stages.append(s)
    for p in detail.get("phases", []):
        all_stages.append(p)
    for m in detail.get("mint_stages", []):
        all_stages.append(m)
    if detail.get("public_mint"):
        all_stages.append(detail["public_mint"])
    if detail.get("allow_list"):
        all_stages.append(detail["allow_list"])
    return all_stages

def find_all_free_stages(detail: dict, wallets=None) -> dict:
    result = {"active": [], "upcoming": [], "ended": [], "paid": []}
    all_stages = extract_all_stages_raw(detail)
    seen = set()
    
    for stage in all_stages:
        price_wei = extract_price_from_stage(stage)
        stage_name = extract_stage_name(stage)
        start_time = stage.get("start_time") or stage.get("startTime") or ""
        stage_id = f"{stage_name}_{start_time}"
        
        if stage_id in seen:
            continue
        seen.add(stage_id)
        
        status = get_stage_status(stage)
        start_dt = parse_stage_time(start_time)
        end_dt = parse_stage_time(stage.get("end_time") or stage.get("endTime") or "")
        is_free = is_price_free(price_wei)
        
        stage_data = {
            **stage,
            "stage": stage_name,
            "price_wei": price_wei,
            "price_eth": price_wei / 1e18,
            "status": status,
            "start_dt": start_dt,
            "end_dt": end_dt,
            "is_free": is_free,
        }
        
        if is_free:
            result[status].append(stage_data)
        else:
            result["paid"].append(stage_data)
    
    return result

def get_retry_config(reason: str) -> RetryConfig:
    configs = {
        "gas_too_high": RetryConfig(base_delay=3),
        "simulation_failed": RetryConfig(base_delay=3),
        "tx_error": RetryConfig(base_delay=3),
        "no_fee_recipient": RetryConfig(base_delay=3),
        "nonce_error": RetryConfig(base_delay=3),
        "insufficient_funds": RetryConfig(base_delay=3),
        "tx_pending": RetryConfig(base_delay=3),
    }
    return configs.get(reason, configs["gas_too_high"])

def calculate_retry_delay(config: RetryConfig, attempt_count: int) -> float:
    delay = config.base_delay
    if config.strategy == RetryStrategy.EXPONENTIAL:
        delay *= (config.backoff_multiplier ** (attempt_count - 1))
    elif config.strategy == RetryStrategy.LINEAR:
        delay *= attempt_count
    delay = min(delay, config.max_delay)
    if config.jitter:
        delay *= random.uniform(0.75, 1.25)
    return delay
```

---

main.py - الجزء المعدل لتشغيل دورات إعادة العرض

```python
# ===================================================================
# إدارة عروض الرموز - إضافة الدوال لدورة إعادة العرض
# ===================================================================

from buyer import (
    ListingManager, 
    list_all_owned_tokens, 
    retry_failed_listings,
    relist_successful_tokens,  # دالة جديدة
    LISTING_RELIST_INTERVAL,
)

# إنشاء مدير العروض
listing_manager = ListingManager()

# ===================================================================
# دورة إعادة عرض الرموز الناجحة (كل 4 ساعات)
# ===================================================================
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
                    
                    # جلب الرموز التي تحتاج إعادة عرض من المدير
                    with listing_manager.lock:
                        relist_candidates = []
                        now = time.time()
                        for token_id, data in listing_manager.listings.items():
                            if data.status == "listed":
                                if data.last_success > 0:
                                    if now - data.last_success >= LISTING_RELIST_INTERVAL:
                                        relist_candidates.append(data)
                    
                    if relist_candidates:
                        log.info(f"🔄 إعادة عرض {len(relist_candidates)} رمز للمحفظة {wallet['name']} على {chain_name}")
                        
                        # إعادة عرض الرموز
                        results = await asyncio.to_thread(
                            relist_successful_tokens,
                            listing_manager=listing_manager,
                            w3=w3,
                            private_key=wallet["private_key"],
                            wallet_address=wallet["address"],
                            nft_contract=nft_contract,  # يجب تحديد العقد
                            chain_name=chain_name,
                            marketplace_address=marketplace_address,
                        )
                        
                        if results.get("total_success", 0) > 0:
                            log.info(f"✅ تم إعادة عرض {results['total_success']} رمز بنجاح")
                        if results.get("total_failed", 0) > 0:
                            log.warning(f"⚠️ فشل إعادة عرض {results['total_failed']} رمز")
            
            # انتظار 4 ساعات قبل الدورة التالية
            log.info(f"⏳ انتظار {LISTING_RELIST_INTERVAL // 3600} ساعات قبل الدورة التالية")
            await asyncio.sleep(LISTING_RELIST_INTERVAL)
            
        except Exception as e:
            log.error(f"❌ خطأ في relist_successful_tokens_loop: {e}")
            await asyncio.sleep(60)

# ===================================================================
# تشغيل جميع الدورات
# ===================================================================
async def run():
    if not BOT_ENABLED:
        return
    
    if not WALLETS or not ENABLED_CHAINS:
        return
    
    # إرسال رسالة بدء التشغيل
    stats = listing_manager.get_stats()
    startup_msg = (
        f"🚀 نظام العرض التلقائي يعمل الآن!\n\n"
        f"📋 إحصائيات العروض:\n"
        f"  • الرموز الكلية: {stats['total']}\n"
        f"  • المعروضة: {stats['success']}\n"
        f"  • الفاشلة: {stats['failed']}\n"
        f"  • قيد المحاولة: {stats['pending']}\n"
        f"  • قيد إعادة المحاولة: {stats['retrying']}\n"
        f"  • تحتاج إعادة عرض: {stats['relisting']}\n\n"
        f"⏱️ إعادة محاولة الفاشلة: كل 30 دقيقة\n"
        f"⏱️ إعادة عرض الناجحة: كل 4 ساعات\n"
        f"📦 حد العرض: {stats['max_per_batch']} رمز لكل عملية"
    )
    send_telegram(startup_msg)
    
    # تشغيل الدورات
    await asyncio.gather(
        # ... المهام الأخرى ...
        relist_successful_tokens_loop(),
        # ... المهام الأخرى ...
    )
```

---

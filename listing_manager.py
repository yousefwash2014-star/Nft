"""
مدير الإدراجات - إدارة الطلبات وحالة الإدراجات
"""

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Optional, Dict, List, Set
from dataclasses import dataclass, field

log = logging.getLogger("listing_manager")

@dataclass
class ListingRequest:
    """طلب إدراج NFT"""
    collection_slug: str
    contract_address: str
    token_id: str
    wallet_address: str
    wallet_name: str
    price_eth: float
    chain_name: str
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    status: str = "pending"  # pending, processing, listed, failed
    tx_hash: str = ""
    error_message: str = ""
    retry_count: int = 0
    max_retries: int = 3
    
class ListingManager:
    """مدير الإدراجات"""
    
    def __init__(self):
        self._requests: Dict[str, ListingRequest] = {}
        self._lock = asyncio.Lock()
        self._processed: Set[str] = set()
        self._pending_queue: List[str] = []
        
    def _make_key(self, contract: str, token_id: str, wallet: str) -> str:
        return f"{contract.lower()}:{token_id}:{wallet.lower()}"
    
    async def add_request(self, request: ListingRequest) -> str:
        """إضافة طلب إدراج"""
        key = self._make_key(
            request.contract_address,
            request.token_id,
            request.wallet_address
        )
        
        async with self._lock:
            if key in self._requests:
                log.debug(f"طلب الإدراج موجود بالفعل: {key}")
                return key
            
            self._requests[key] = request
            self._pending_queue.append(key)
            
        log.info(f"تم إضافة طلب إدراج {request.token_id} للمحفظة {request.wallet_name}")
        return key
    
    async def get_request(
        self,
        contract: str,
        token_id: str,
        wallet: str
    ) -> Optional[ListingRequest]:
        """الحصول على طلب الإدراج"""
        key = self._make_key(contract, token_id, wallet)
        async with self._lock:
            return self._requests.get(key)
    
    async def update_status(
        self,
        contract: str,
        token_id: str,
        wallet: str,
        status: str,
        tx_hash: str = "",
        error: str = ""
    ):
        """تحديث حالة طلب الإدراج"""
        key = self._make_key(contract, token_id, wallet)
        async with self._lock:
            if key in self._requests:
                request = self._requests[key]
                request.status = status
                if tx_hash:
                    request.tx_hash = tx_hash
                if error:
                    request.error_message = error
                
                if status in ("listed", "failed"):
                    self._processed.add(key)
    
    async def get_next_pending(self) -> Optional[ListingRequest]:
        """الحصول على الطلب التالي في قائمة الانتظار"""
        async with self._lock:
            while self._pending_queue:
                key = self._pending_queue.pop(0)
                if key in self._requests:
                    request = self._requests[key]
                    if request.status == "pending":
                        request.status = "processing"
                        return request
            return None
    
    async def get_stats(self) -> dict:
        """الحصول على إحصائيات الإدراجات"""
        async with self._lock:
            total = len(self._requests)
            pending = sum(1 for r in self._requests.values() if r.status == "pending")
            processing = sum(1 for r in self._requests.values() if r.status == "processing")
            listed = sum(1 for r in self._requests.values() if r.status == "listed")
            failed = sum(1 for r in self._requests.values() if r.status == "failed")
            
            return {
                "total": total,
                "pending": pending,
                "processing": processing,
                "listed": listed,
                "failed": failed,
                "queue_size": len(self._pending_queue)
            }
    
    async def clear_old(self, hours: int = 24):
        """مسح الطلبات القديمة"""
        cutoff = datetime.now(timezone.utc).timestamp() - (hours * 3600)
        async with self._lock:
            to_remove = []
            for key, request in self._requests.items():
                if request.created_at.timestamp() < cutoff and request.status in ("listed", "failed"):
                    to_remove.append(key)
            
            for key in to_remove:
                del self._requests[key]
                self._processed.discard(key)
            
            # تنظيف قائمة الانتظار
            self._pending_queue = [k for k in self._pending_queue if k in self._requests]
            
            log.info(f"تم تنظيف {len(to_remove)} طلب إدراج قديم")

# Singleton
_listing_manager = None

def get_listing_manager() -> ListingManager:
    """الحصول على مدير الإدراجات (Singleton)"""
    global _listing_manager
    if _listing_manager is None:
        _listing_manager = ListingManager()
    return _listing_manager
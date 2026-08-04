"""
محرك الشراء التلقائي عبر عقد SeaDrop على سلاسل متعددة (Ethereum + Robinhood).
نسخة محسنة - رسوم الغاز محددة بـ 15 سنت مع إعادة محاولة سريعة كل 2 ثانية
الشراء من جميع المحافظ بشكل متوازي
"""

import logging
import threading
import random
from datetime import datetime, timezone
from enum import Enum
from dataclasses import dataclass, field
from typing import Optional, List
from web3 import Web3

log = logging.getLogger("buyer")

# ===========================================================================
# Nonce Management
# ===========================================================================
nonce_locks = {}
nonce_locks_lock = threading.Lock()

def get_wallet_lock(address: str) -> threading.Lock:
    with nonce_locks_lock:
        if address not in nonce_locks:
            nonce_locks[address] = threading.Lock()
        return nonce_locks[address]

# ===========================================================================
# Constants
# ===========================================================================
MAX_ETH_PER_TX = 0.02
MAX_GAS_FEE_USD = 0.15  # 🔥 15 سنت
MIN_BALANCE_RESERVE_USD = 0.001
GAS_LIMIT_SAFETY_MARGIN = 1.05  # 5% هامش أمان
FREE_PRICE_THRESHOLD_WEI = 1000

# ===========================================================================
# Chain Configurations
# ===========================================================================
CHAINS_CONFIG = {
    "robinhood": {
        "rpc_env_var": "ROBINHOOD_RPC_URL",
        "seadrop_address": Web3.to_checksum_address("0x00005EA00Ac477B1030CE78506496e8C2dE24bf5"),
        "chain_name_display": "Robinhood Chain",
        "explorer_url": "https://explorer.robinhood.org/tx/",
    },
    "ethereum": {
        "rpc_env_var": "ETHEREUM_RPC_URL",
        "seadrop_address": Web3.to_checksum_address("0x00005EA00Ac477B1030CE78506496e8C2dE24bf5"),
        "chain_name_display": "Ethereum Mainnet",
        "explorer_url": "https://etherscan.io/tx/",
    },
}

ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"

# ===========================================================================
# ABI Definitions
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

# ===========================================================================
# Enums & Data Classes
# ===========================================================================
class RetryStrategy(Enum):
    FIXED = "fixed"
    EXPONENTIAL = "exponential"
    LINEAR = "linear"

@dataclass
class RetryConfig:
    max_attempts: int = 1000      # 🔥 زيادة إلى 1000 محاولة
    base_delay: float = 2         # 🔥 تغيير إلى 2 ثانية
    max_delay: float = 30         # 🔥 حد أقصى 30 ثانية
    strategy: RetryStrategy = RetryStrategy.FIXED
    backoff_multiplier: float = 1.5
    jitter: bool = True
    max_total_hours: float = 6    # 🔥 زيادة إلى 6 ساعات

@dataclass
class MintResult:
    """نتيجة عملية الشراء"""
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

# ===========================================================================
# ✅ Retry Configurations - معدل: 2 ثانية وعدد محاولات أكبر
# ===========================================================================
REASON_RETRY_CONFIGS = {
    # 🔥 غاز مرتفع: 2 ثانية, 1000 محاولة, 6 ساعات
    "gas_too_high": RetryConfig(
        max_attempts=1000,
        base_delay=2,
        max_delay=30,
        strategy=RetryStrategy.FIXED,
        max_total_hours=6
    ),
    
    # 🔥 فشل المحاكاة: 2 ثانية, 500 محاولة, 12 ساعة
    "simulation_failed": RetryConfig(
        max_attempts=500,
        base_delay=2,
        max_delay=30,
        strategy=RetryStrategy.EXPONENTIAL,
        max_total_hours=12
    ),
    
    # 🔥 خطأ الشبكة: 2 ثانية, 1500 محاولة, 5 ساعات
    "tx_error": RetryConfig(
        max_attempts=1500,
        base_delay=2,
        max_delay=30,
        strategy=RetryStrategy.EXPONENTIAL,
        max_total_hours=5
    ),
    
    # 🔥 لا يوجد عنوان رسوم: 2 ثانية, 300 محاولة, 10 ساعات
    "no_fee_recipient": RetryConfig(
        max_attempts=300,
        base_delay=2,
        max_delay=30,
        strategy=RetryStrategy.FIXED,
        max_total_hours=10
    ),
    
    # 🔥 خطأ Nonce: 2 ثانية, 300 محاولة, 1 ساعة
    "nonce_error": RetryConfig(
        max_attempts=300,
        base_delay=2,
        max_delay=30,
        strategy=RetryStrategy.LINEAR,
        max_total_hours=1
    ),
    
    # 🔥 رصيد غير كاف: 2 ثانية (لكن لن يُستخدم لأننا لا نعيد المحاولة لنقص الرصيد)
    "insufficient_funds": RetryConfig(
        max_attempts=100,
        base_delay=2,
        max_delay=30,
        strategy=RetryStrategy.FIXED,
        max_total_hours=12
    ),
}

RETRYABLE_REASONS = set(REASON_RETRY_CONFIGS.keys())

# ===========================================================================
# Arabic Messages
# ===========================================================================
REASON_MESSAGES_AR = {
    "balance_too_low": "💰 الرصيد منخفض",
    "gas_too_high": "⛽ رسوم الغاز مرتفعة",
    "tx_value_too_high": "📊 قيمة المعاملة عالية",
    "no_fee_recipient": "📝 تعذر عنوان الرسوم",
    "simulation_failed": "🔍 فشلت المحاكاة",
    "not_free_mint": "💲 ليس مجانياً",
    "tx_error": "🌐 خطأ شبكة",
    "nonce_error": "🔢 خطأ nonce",
    "insufficient_funds": "💰 رصيد غير كاف",
    "sold_out": "🏁 نفذت الكمية",
    "not_eligible": "🚫 المحفظة غير مؤهلة",
    "unknown": "❓ غير معروف",
}

def get_reason_text(reason: str) -> str:
    if not reason:
        return "❓ غير محدد"
    return REASON_MESSAGES_AR.get(reason, f"⚠️ {reason}")

# ===========================================================================
# Utility Functions
# ===========================================================================
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
        return 1
    qty = max_per_wallet if max_per_wallet <= 20 else 5
    return max(1, min(qty, remaining_supply))

# ===========================================================================
# Core Functions
# ===========================================================================
def get_web3_from_config(chain_config: dict) -> Web3:
    return Web3(Web3.HTTPProvider(chain_config.get("rpc_url", "")))

def get_wallet_balance(w3: Web3, wallet_address: str) -> tuple:
    try:
        balance_wei = w3.eth.get_balance(Web3.to_checksum_address(wallet_address))
        return balance_wei / 1e18, balance_wei
    except:
        return 0.0, 0

def get_fee_recipient(w3: Web3, seadrop_address: str, nft_contract: str) -> Optional[str]:
    try:
        contract = w3.eth.contract(
            address=Web3.to_checksum_address(seadrop_address),
            abi=SEADROP_ABI
        )
        recipients = contract.functions.getAllowedFeeRecipients(
            Web3.to_checksum_address(nft_contract)
        ).call()
        return recipients[0] if recipients else None
    except:
        return None

# ===========================================================================
# quick_checks - محسنة مع رسائل واضحة
# ===========================================================================
def quick_checks(w3, wallet_address, eth_price_usd, nft_contract, seadrop_address, quantity=1, price_wei=0):
    """
    فحص الرصيد وعنوان الرسوم مع تقدير الغاز الفعلي
    الأولوية: الرصيد أولاً ثم الغاز ثم العنوان
    """
    
    # 1️⃣ تحقق من الرصيد أولاً
    balance_eth, balance_wei = get_wallet_balance(w3, wallet_address)
    balance_usd = balance_eth * eth_price_usd
    
    if balance_usd < MIN_BALANCE_RESERVE_USD:
        return {
            "pass": False,
            "reason": "insufficient_funds",
            "reason_text": f"💰 الرصيد منخفض (${balance_usd:.4f} < ${MIN_BALANCE_RESERVE_USD})",
            "balance_eth": balance_eth,
            "balance_usd": balance_usd,
            "gas_eth": 0,
            "gas_usd": 0,
            "fee_recipient": None,
        }
    
    # 2️⃣ ثم تحقق من الغاز
    gas_eth = 0
    gas_usd = 0
    gas_price = 0
    gas_units = 150000
    
    try:
        gas_price = w3.eth.gas_price
        gas_eth = (gas_price * gas_units) / 1e18
        gas_usd = gas_eth * eth_price_usd
    except:
        pass
    
    if gas_usd > MAX_GAS_FEE_USD:
        return {
            "pass": False,
            "reason": "gas_too_high",
            "reason_text": f"⛽ رسوم الغاز مرتفعة (${gas_usd:.4f} > ${MAX_GAS_FEE_USD})",
            "balance_eth": balance_eth,
            "balance_usd": balance_usd,
            "gas_eth": gas_eth,
            "gas_usd": gas_usd,
            "gas_price": gas_price,
            "gas_units": gas_units,
            "fee_recipient": None,
        }
    
    # 3️⃣ ثم تحقق من عنوان الرسوم
    fee_recipient = get_fee_recipient(w3, seadrop_address, nft_contract)
    if not fee_recipient:
        return {
            "pass": False,
            "reason": "no_fee_recipient",
            "reason_text": "📝 تعذر عنوان الرسوم",
            "balance_eth": balance_eth,
            "balance_usd": balance_usd,
            "gas_eth": gas_eth,
            "gas_usd": gas_usd,
            "fee_recipient": None,
        }
    
    return {
        "pass": True,
        "reason": "ok",
        "reason_text": "✅ جميع الفحوصات ناجحة",
        "balance_eth": balance_eth,
        "balance_usd": balance_usd,
        "gas_eth": gas_eth,
        "gas_usd": gas_usd,
        "gas_price": gas_price,
        "gas_units": gas_units,
        "fee_recipient": fee_recipient,
    }

# ===========================================================================
# attempt_purchase - محاولة الشراء
# ===========================================================================
def attempt_purchase(w3, private_key, wallet_address, nft_contract, seadrop_address, price_wei, max_per_wallet, remaining_supply, eth_price_usd):
    """محاولة شراء NFT - مع حد غاز 15 سنت"""
    
    # الحصول على الرصيد أولاً
    balance_eth, balance_wei = get_wallet_balance(w3, wallet_address)
    balance_usd = balance_eth * eth_price_usd
    
    # التحقق من الرصيد
    if balance_usd < MIN_BALANCE_RESERVE_USD:
        return MintResult(
            success=False,
            wallet=wallet_address,
            reason="insufficient_funds",
            reason_text=f"💰 الرصيد منخفض (${balance_usd:.4f})",
            balance_eth=balance_eth,
            balance_usd=balance_usd,
            price_wei=price_wei,
        )
    
    # التحقق من أن السعر مجاني
    if not is_price_free(price_wei):
        return MintResult(
            success=False,
            wallet=wallet_address,
            reason="not_free_mint",
            reason_text="💲 السعر ليس مجانياً",
            price_wei=price_wei,
            balance_eth=balance_eth,
            balance_usd=balance_usd,
        )
    
    # تحديد الكمية
    quantity = decide_quantity(max_per_wallet, remaining_supply)
    total_value_wei = price_wei * quantity
    total_value_eth = total_value_wei / 1e18
    
    # التحقق من قيمة المعاملة
    if total_value_eth > MAX_ETH_PER_TX:
        return MintResult(
            success=False,
            wallet=wallet_address,
            reason="tx_value_too_high",
            reason_text=f"📊 قيمة المعاملة عالية (${total_value_eth:.4f} ETH)",
            balance_eth=balance_eth,
            balance_usd=balance_usd,
        )
    
    # الحصول على قفل المحفظة
    wallet_lock = get_wallet_lock(wallet_address)
    
    with wallet_lock:
        try:
            # إنشاء عقد SeaDrop
            contract = w3.eth.contract(
                address=Web3.to_checksum_address(seadrop_address),
                abi=SEADROP_ABI
            )
            
            # الحصول على nonce
            nonce = w3.eth.get_transaction_count(
                Web3.to_checksum_address(wallet_address),
                "pending"
            )
            
            # الحصول على سعر الغاز
            gas_price = w3.eth.gas_price
            
            # الحصول على عنوان الرسوم
            fee_recipient = get_fee_recipient(w3, seadrop_address, nft_contract)
            if not fee_recipient:
                return MintResult(
                    success=False,
                    wallet=wallet_address,
                    reason="no_fee_recipient",
                    reason_text="📝 تعذر عنوان الرسوم",
                    balance_eth=balance_eth,
                    balance_usd=balance_usd,
                )
            
            # بناء المعاملة
            tx = contract.functions.mintPublic(
                Web3.to_checksum_address(nft_contract),
                Web3.to_checksum_address(fee_recipient),
                Web3.to_checksum_address(ZERO_ADDRESS),
                quantity,
            ).build_transaction({
                "from": Web3.to_checksum_address(wallet_address),
                "value": total_value_wei,
                "nonce": nonce,
                "gasPrice": gas_price,
                "chainId": w3.eth.chain_id,
            })
            
            # تقدير الغاز
            try:
                estimated_gas = w3.eth.estimate_gas(tx)
                tx["gas"] = int(estimated_gas * GAS_LIMIT_SAFETY_MARGIN)
                
                gas_price_gwei = gas_price / 1e9
                gas_eth = (tx["gas"] * gas_price) / 1e18
                gas_usd = gas_eth * eth_price_usd
                
                # التحقق النهائي من الغاز
                if gas_usd > MAX_GAS_FEE_USD:
                    return MintResult(
                        success=False,
                        wallet=wallet_address,
                        reason="gas_too_high",
                        reason_text=f"⛽ رسوم الغاز مرتفعة (${gas_usd:.4f} > ${MAX_GAS_FEE_USD})",
                        gas_units=tx["gas"],
                        gas_estimated=estimated_gas,
                        gas_price_gwei=gas_price_gwei,
                        gas_used_usd=gas_usd,
                        balance_eth=balance_eth,
                        balance_usd=balance_usd,
                    )
                
            except Exception as e:
                return MintResult(
                    success=False,
                    wallet=wallet_address,
                    reason="simulation_failed",
                    reason_text=f"🔍 فشلت المحاكاة: {str(e)[:50]}",
                    balance_eth=balance_eth,
                    balance_usd=balance_usd,
                )
            
            # توقيع المعاملة
            signed_tx = w3.eth.account.sign_transaction(tx, private_key)
            
            # إرسال المعاملة
            tx_hash = w3.eth.send_raw_transaction(signed_tx.rawTransaction if hasattr(signed_tx, 'rawTransaction') else signed_tx.raw_transaction)
            tx_hash_str = tx_hash.hex() if hasattr(tx_hash, 'hex') else str(tx_hash)
            
            # حساب تكلفة الغاز
            gas_used_eth = (tx["gas"] * gas_price) / 1e18
            gas_used_usd = gas_used_eth * eth_price_usd
            
            # نجاح
            return MintResult(
                success=True,
                wallet=wallet_address,
                tx_hash=tx_hash_str,
                quantity=quantity,
                gas_used_eth=gas_used_eth,
                gas_used_usd=gas_used_usd,
                gas_units=tx["gas"],
                gas_price_gwei=gas_price_gwei,
                gas_estimated=estimated_gas,
                total_value_eth=total_value_eth,
                balance_eth=balance_eth,
                balance_usd=balance_usd,
                reason_text="✅ تم الشراء بنجاح",
            )
            
        except Exception as e:
            error_msg = str(e).lower()
            
            # تحديد سبب الفشل
            if "insufficient funds" in error_msg:
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
            )

# ===========================================================================
# Stage Discovery Functions
# ===========================================================================
def extract_all_stages_raw(detail: dict) -> list:
    """استخراج جميع المراحل من البيانات"""
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
    """اكتشاف جميع المراحل المجانية"""
    result = {"active": [], "upcoming": [], "ended": []}
    all_stages = extract_all_stages_raw(detail)
    seen = set()
    
    for stage in all_stages:
        price_wei = extract_price_from_stage(stage)
        if not is_price_free(price_wei):
            continue
        
        stage_name = extract_stage_name(stage)
        start_time = stage.get("start_time") or stage.get("startTime") or ""
        stage_id = f"{stage_name}_{start_time}"
        
        if stage_id in seen:
            continue
        seen.add(stage_id)
        
        status = get_stage_status(stage)
        start_dt = parse_stage_time(start_time)
        end_dt = parse_stage_time(stage.get("end_time") or stage.get("endTime") or "")
        
        result[status].append({
            **stage,
            "stage": stage_name,
            "price_wei": price_wei,
            "price_eth": price_wei / 1e18,
            "status": status,
            "start_dt": start_dt,
            "end_dt": end_dt,
        })
    
    return result

def get_all_stages_info(detail: dict, wallets=None) -> dict:
    """معلومات جميع المراحل"""
    result = {
        "all_stages": [],
        "free_active": [],
        "free_upcoming": [],
        "paid_active": [],
        "paid_upcoming": [],
        "total": 0,
    }
    
    all_stages = extract_all_stages_raw(detail)
    result["total"] = len(all_stages)
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
        is_free = is_price_free(price_wei)
        
        info = {
            "name": stage_name,
            "type": "public",
            "price_wei": price_wei,
            "price_eth": price_wei / 1e18,
            "price_usd": (price_wei / 1e18) * 3000,
            "max_per_wallet": stage.get("max_per_wallet") or stage.get("maxPerWallet"),
            "start_time": start_time,
            "end_time": stage.get("end_time") or stage.get("endTime") or "",
            "status": status,
            "is_free": is_free,
        }
        
        result["all_stages"].append(info)
        
        if is_free and status == "active":
            result["free_active"].append(info)
        elif is_free and status == "upcoming":
            result["free_upcoming"].append(info)
        elif not is_free and status == "active":
            result["paid_active"].append(info)
        elif not is_free and status == "upcoming":
            result["paid_upcoming"].append(info)
    
    return result

# ===========================================================================
# Retry Logic
# ===========================================================================
def get_retry_config(reason: str) -> RetryConfig:
    return REASON_RETRY_CONFIGS.get(reason, REASON_RETRY_CONFIGS["gas_too_high"])

def calculate_retry_delay(config: RetryConfig, attempt_count: int) -> float:
    if config.strategy == RetryStrategy.FIXED:
        delay = config.base_delay
    elif config.strategy == RetryStrategy.EXPONENTIAL:
        delay = config.base_delay * (config.backoff_multiplier ** (attempt_count - 1))
    elif config.strategy == RetryStrategy.LINEAR:
        delay = config.base_delay * attempt_count
    else:
        delay = config.base_delay
    delay = min(delay, config.max_delay)
    if config.jitter:
        delay *= random.uniform(0.75, 1.25)
    return delay

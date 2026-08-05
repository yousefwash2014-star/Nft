"""
محرك الشراء التلقائي عبر عقد SeaDrop على سلاسل متعددة (Ethereum + Robinhood).
نسخة محسنة - رسوم الغاز محددة بـ 15 سنت مع إعادة محاولة سريعة كل 5 ثواني
الشراء من جميع المحافظ بشكل متوازي - بدون حد أقصى للرموز
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
MAX_GAS_FEE_USD = 0.15
MIN_BALANCE_RESERVE_USD = 0.1
GAS_LIMIT_SAFETY_MARGIN = 1.05
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
    max_attempts: int = 500
    base_delay: float = 5
    max_delay: float = 60
    strategy: RetryStrategy = RetryStrategy.FIXED
    backoff_multiplier: float = 1.5
    jitter: bool = True
    max_total_hours: float = 4

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

REASON_RETRY_CONFIGS = {
    "gas_too_high": RetryConfig(max_attempts=500, base_delay=5, max_delay=60, strategy=RetryStrategy.FIXED, max_total_hours=4),
    "simulation_failed": RetryConfig(max_attempts=240, base_delay=5, max_delay=60, strategy=RetryStrategy.EXPONENTIAL, max_total_hours=8),
    "tx_error": RetryConfig(max_attempts=720, base_delay=5, max_delay=60, strategy=RetryStrategy.EXPONENTIAL, max_total_hours=3),
    "no_fee_recipient": RetryConfig(max_attempts=160, base_delay=5, max_delay=60, strategy=RetryStrategy.FIXED, max_total_hours=8),
    "nonce_error": RetryConfig(max_attempts=180, base_delay=5, max_delay=60, strategy=RetryStrategy.LINEAR, max_total_hours=0.5),
    "insufficient_funds": RetryConfig(max_attempts=72, base_delay=5, max_delay=60, strategy=RetryStrategy.FIXED, max_total_hours=12),
}

RETRYABLE_REASONS = set(REASON_RETRY_CONFIGS.keys())

REASON_MESSAGES_AR = {
    "balance_too_low": "💰 الرصيد منخفض",
    "gas_too_high": "⛽ رسوم الغاز مرتفعة (الحد 15 سنت)",
    "tx_value_too_high": "📊 قيمة المعاملة عالية",
    "no_fee_recipient": "📝 تعذر عنوان الرسوم",
    "simulation_failed": "🔍 فشلت المحاكاة",
    "not_free_mint": "💲 ليس مجانياً",
    "tx_error": "🌐 خطأ شبكة",
    "nonce_error": "🔢 خطأ nonce",
    "insufficient_funds": "💸 رصيد غير كاف",
    "sold_out": "🏁 نفذت الكمية",
    "not_eligible": "🚫 المحفظة غير مؤهلة",
    "tx_reverted": "❌ المعاملة فشلت على البلوكشين",
    "tx_pending": "⏳ المعاملة قيد التأكيد",
    "unknown": "❓ غير معروف",
}

def get_reason_text(reason: str) -> str:
    if not reason:
        return "❓ غير محدد"
    return REASON_MESSAGES_AR.get(reason, f"⚠️ {reason}")

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
    qty = min(max_per_wallet, remaining_supply, 20)
    return max(1, qty)

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
        contract = w3.eth.contract(address=Web3.to_checksum_address(seadrop_address), abi=SEADROP_ABI)
        recipients = contract.functions.getAllowedFeeRecipients(Web3.to_checksum_address(nft_contract)).call()
        return recipients[0] if recipients else None
    except:
        return None

def quick_checks(w3, wallet_address, eth_price_usd, nft_contract, seadrop_address, quantity=1, price_wei=0):
    balance_eth, balance_wei = get_wallet_balance(w3, wallet_address)
    balance_usd = balance_eth * eth_price_usd
    
    gas_eth = 0
    gas_usd = 0
    try:
        gas_price = w3.eth.gas_price
        gas_units = 150000
        gas_eth = (gas_price * gas_units) / 1e18
        gas_usd = gas_eth * eth_price_usd
    except:
        pass
    
    if gas_usd > MAX_GAS_FEE_USD:
        return {"pass": False, "reason": "gas_too_high", "balance_eth": balance_eth, "balance_usd": balance_usd, "gas_eth": gas_eth, "gas_usd": gas_usd, "fee_recipient": None}
    
    fee_recipient = get_fee_recipient(w3, seadrop_address, nft_contract)
    if not fee_recipient:
        return {"pass": False, "reason": "no_fee_recipient", "balance_eth": balance_eth, "balance_usd": balance_usd, "gas_eth": gas_eth, "gas_usd": gas_usd, "fee_recipient": None}
    
    return {"pass": True, "reason": "ok", "balance_eth": balance_eth, "balance_usd": balance_usd, "gas_eth": gas_eth, "gas_usd": gas_usd, "fee_recipient": fee_recipient}

def attempt_purchase(w3, private_key, wallet_address, nft_contract, seadrop_address, price_wei, max_per_wallet, remaining_supply, eth_price_usd):
    if not is_price_free(price_wei):
        return MintResult(success=False, wallet=wallet_address, reason="not_free_mint", reason_text=get_reason_text("not_free_mint"), price_wei=price_wei)
    
    balance_eth, balance_wei = get_wallet_balance(w3, wallet_address)
    balance_usd = balance_eth * eth_price_usd
    
    quantity = decide_quantity(max_per_wallet, remaining_supply)
    total_value_wei = price_wei * quantity
    total_value_eth = total_value_wei / 1e18
    
    if total_value_eth > MAX_ETH_PER_TX:
        return MintResult(success=False, wallet=wallet_address, reason="tx_value_too_high", reason_text=get_reason_text("tx_value_too_high"), balance_eth=balance_eth, balance_usd=balance_usd)
    
    wallet_lock = get_wallet_lock(wallet_address)
    
    with wallet_lock:
        try:
            contract = w3.eth.contract(address=Web3.to_checksum_address(seadrop_address), abi=SEADROP_ABI)
            nonce = w3.eth.get_transaction_count(Web3.to_checksum_address(wallet_address), "pending")
            gas_price = w3.eth.gas_price
            
            fee_recipient = get_fee_recipient(w3, seadrop_address, nft_contract)
            if not fee_recipient:
                return MintResult(success=False, wallet=wallet_address, reason="no_fee_recipient", reason_text=get_reason_text("no_fee_recipient"), balance_eth=balance_eth, balance_usd=balance_usd)
            
            tx = contract.functions.mintPublic(
                Web3.to_checksum_address(nft_contract), Web3.to_checksum_address(fee_recipient),
                Web3.to_checksum_address(ZERO_ADDRESS), quantity
            ).build_transaction({
                "from": Web3.to_checksum_address(wallet_address), "value": total_value_wei,
                "nonce": nonce, "gasPrice": gas_price, "chainId": w3.eth.chain_id,
            })
            
            try:
                estimated_gas = w3.eth.estimate_gas(tx)
                tx["gas"] = int(estimated_gas * GAS_LIMIT_SAFETY_MARGIN)
                gas_price_gwei = gas_price / 1e9
                gas_eth = (tx["gas"] * gas_price) / 1e18
                gas_usd = gas_eth * eth_price_usd
                
                if gas_usd > MAX_GAS_FEE_USD:
                    return MintResult(success=False, wallet=wallet_address, reason="gas_too_high", reason_text=get_reason_text("gas_too_high"), gas_units=tx["gas"], gas_estimated=estimated_gas, gas_price_gwei=gas_price_gwei, gas_used_usd=gas_usd, balance_eth=balance_eth, balance_usd=balance_usd)
            except Exception:
                return MintResult(success=False, wallet=wallet_address, reason="simulation_failed", reason_text=get_reason_text("simulation_failed"), balance_eth=balance_eth, balance_usd=balance_usd)
            
            signed_tx = w3.eth.account.sign_transaction(tx, private_key)
            tx_hash = w3.eth.send_raw_transaction(signed_tx.rawTransaction if hasattr(signed_tx, 'rawTransaction') else signed_tx.raw_transaction)
            tx_hash_str = tx_hash.hex() if hasattr(tx_hash, 'hex') else str(tx_hash)
            
            gas_used_eth = (tx["gas"] * gas_price) / 1e18
            gas_used_usd = gas_used_eth * eth_price_usd
            
            return MintResult(
                success=True, wallet=wallet_address, tx_hash=tx_hash_str,
                quantity=quantity, gas_used_eth=gas_used_eth, gas_used_usd=gas_used_usd,
                gas_units=tx["gas"], gas_price_gwei=gas_price / 1e9, gas_estimated=estimated_gas,
                total_value_eth=total_value_eth, balance_eth=balance_eth, balance_usd=balance_usd,
                confirmed=False
            )
            
        except Exception as e:
            error_msg = str(e).lower()
            if "insufficient funds" in error_msg: reason = "insufficient_funds"
            elif "gas" in error_msg: reason = "gas_too_high"
            elif "nonce" in error_msg: reason = "nonce_error"
            elif "revert" in error_msg or "execution reverted" in error_msg: reason = "simulation_failed"
            else: reason = "tx_error"
            return MintResult(success=False, wallet=wallet_address, reason=reason, reason_text=get_reason_text(reason), balance_eth=balance_eth, balance_usd=balance_usd, price_wei=price_wei)

def extract_all_stages_raw(detail: dict) -> list:
    all_stages = []
    if detail.get("active_stage"): all_stages.append(detail["active_stage"])
    for s in detail.get("stages", []): all_stages.append(s)
    for p in detail.get("phases", []): all_stages.append(p)
    for m in detail.get("mint_stages", []): all_stages.append(m)
    if detail.get("public_mint"): all_stages.append(detail["public_mint"])
    if detail.get("allow_list"): all_stages.append(detail["allow_list"])
    return all_stages

def find_all_free_stages(detail: dict, wallets=None) -> dict:
    result = {"active": [], "upcoming": [], "ended": []}
    all_stages = extract_all_stages_raw(detail)
    seen = set()
    for stage in all_stages:
        price_wei = extract_price_from_stage(stage)
        if not is_price_free(price_wei): continue
        stage_name = extract_stage_name(stage)
        start_time = stage.get("start_time") or stage.get("startTime") or ""
        stage_id = f"{stage_name}_{start_time}"
        if stage_id in seen: continue
        seen.add(stage_id)
        status = get_stage_status(stage)
        start_dt = parse_stage_time(start_time)
        end_dt = parse_stage_time(stage.get("end_time") or stage.get("endTime") or "")
        result[status].append({**stage, "stage": stage_name, "price_wei": price_wei, "price_eth": price_wei / 1e18, "status": status, "start_dt": start_dt, "end_dt": end_dt})
    return result

def get_retry_config(reason: str) -> RetryConfig:
    return REASON_RETRY_CONFIGS.get(reason, REASON_RETRY_CONFIGS["gas_too_high"])

def calculate_retry_delay(config: RetryConfig, attempt_count: int) -> float:
    if config.strategy == RetryStrategy.FIXED: delay = config.base_delay
    elif config.strategy == RetryStrategy.EXPONENTIAL: delay = config.base_delay * (config.backoff_multiplier ** (attempt_count - 1))
    elif config.strategy == RetryStrategy.LINEAR: delay = config.base_delay * attempt_count
    else: delay = config.base_delay
    delay = min(delay, config.max_delay)
    if config.jitter: delay *= random.uniform(0.75, 1.25)
    return delay

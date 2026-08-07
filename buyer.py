"""
محرك الشراء التلقائي عبر عقد SeaDrop على سلاسل متعددة (Ethereum + Robinhood).
غاز حقيقي من العقد - حد 15 سنت - إعادة محاولة كل 3 ثواني
بدون استسلام إلا عند النفاذ - فحص أهلية ومجانية
"""

import logging, threading, random
from datetime import datetime, timezone
from enum import Enum
from dataclasses import dataclass, field
from typing import Optional, List
from web3 import Web3

log = logging.getLogger("buyer")

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

CHAINS_CONFIG = {
    "robinhood": {
        "seadrop_address": Web3.to_checksum_address("0x00005EA00Ac477B1030CE78506496e8C2dE24bf5"),
        "chain_name_display": "Robinhood Chain",
        "explorer_url": "https://explorer.robinhood.org/tx/",
    },
    "ethereum": {
        "seadrop_address": Web3.to_checksum_address("0x00005EA00Ac477B1030CE78506496e8C2dE24bf5"),
        "chain_name_display": "Ethereum Mainnet",
        "explorer_url": "https://etherscan.io/tx/",
    },
}

ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"

SEADROP_ABI = [
    {
        "inputs": [
            {"name": "nftContract", "type": "address"}, {"name": "feeRecipient", "type": "address"},
            {"name": "minterIfNotPayer", "type": "address"}, {"name": "quantity", "type": "uint256"},
        ],
        "name": "mintPublic", "outputs": [], "stateMutability": "payable", "type": "function",
    },
    {
        "inputs": [{"name": "nftContract", "type": "address"}],
        "name": "getAllowedFeeRecipients", "outputs": [{"name": "", "type": "address[]"}],
        "stateMutability": "view", "type": "function",
    },
]

class RetryStrategy(Enum):
    FIXED = "fixed"; EXPONENTIAL = "exponential"; LINEAR = "linear"

@dataclass
class RetryConfig:
    max_attempts: Optional[int] = None; base_delay: float = 3; max_delay: float = 60
    strategy: RetryStrategy = RetryStrategy.FIXED; backoff_multiplier: float = 1.5
    jitter: bool = True; max_total_hours: Optional[float] = None

@dataclass
class MintResult:
    success: bool; wallet: str = ""; wallet_name: str = ""; tx_hash: str = ""
    reason: str = ""; reason_text: str = ""; quantity: int = 0
    gas_used_eth: float = 0.0; gas_used_usd: float = 0.0; gas_units: int = 0
    gas_price_gwei: float = 0.0; gas_estimated: int = 0
    balance_eth: float = 0.0; balance_usd: float = 0.0; total_value_eth: float = 0.0
    price_wei: int = 0; chain_name: str = ""; confirmed: bool = False
    is_free: bool = True; is_eligible: bool = True

REASON_RETRY_CONFIGS = {
    "gas_too_high": RetryConfig(base_delay=3), "simulation_failed": RetryConfig(base_delay=3),
    "tx_error": RetryConfig(base_delay=3), "no_fee_recipient": RetryConfig(base_delay=3),
    "nonce_error": RetryConfig(base_delay=3), "insufficient_funds": RetryConfig(base_delay=3),
    "tx_pending": RetryConfig(base_delay=3),
}

RETRYABLE_REASONS = {"gas_too_high", "simulation_failed", "tx_error", "no_fee_recipient", "nonce_error", "insufficient_funds", "tx_pending"}
PERMANENT_REASONS = {"not_free_mint", "not_eligible", "wallet_limit_reached", "balance_too_low", "tx_value_too_high", "sold_out"}

REASON_MESSAGES_AR = {
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
    "unknown": "خطأ غير معروف",
}

def get_reason_text(reason: str) -> str:
    if not reason: return "غير محدد"
    return REASON_MESSAGES_AR.get(reason, f"خطأ: {reason}")

def is_reason_retryable(reason: str) -> bool: return reason in RETRYABLE_REASONS
def is_reason_permanent(reason: str) -> bool: return reason in PERMANENT_REASONS
def is_price_free(price_wei: int) -> bool: return price_wei <= FREE_PRICE_THRESHOLD_WEI

def parse_stage_time(time_str: str) -> Optional[datetime]:
    if not time_str: return None
    try: return datetime.fromisoformat(time_str.replace("Z", "+00:00"))
    except: return None

def get_stage_status(stage: dict) -> str:
    now = datetime.now(timezone.utc)
    start_dt = parse_stage_time(stage.get("start_time", ""))
    end_dt = parse_stage_time(stage.get("end_time", ""))
    if start_dt and now < start_dt: return "upcoming"
    if end_dt and now > end_dt: return "ended"
    if start_dt and now >= start_dt: return "active"
    return "unknown"

def safe_parse_price(price_value) -> int:
    if price_value is None: return 0
    try:
        if isinstance(price_value, str):
            price_value = price_value.strip()
            if not price_value: return 0
            price_float = float(price_value)
            if 0 < price_float < 1: return int(price_float * 1e18)
            return int(price_float)
        if isinstance(price_value, (int, float)): return int(price_value)
        return 0
    except: return 0

def extract_price_from_stage(stage: dict) -> int:
    for field in ["price", "price_wei", "mint_price", "mintPrice", "cost", "value", "price_per_token", "pricePerToken"]:
        value = stage.get(field)
        if value is not None and value != "": return safe_parse_price(value)
    return 0

def extract_stage_name(stage: dict) -> str:
    for field in ["stage", "name", "phase", "title", "stage_name"]:
        value = stage.get(field)
        if value and isinstance(value, str) and value.strip(): return value.strip()
    return "عام"

def decide_quantity(max_per_wallet, remaining_supply):
    """
    تحديد كمية الرموز المطلوب شراؤها
    - الحد الأقصى 5 رموز فقط لكل معاملة
    """
    if max_per_wallet is None: 
        return min(5, remaining_supply)
    return max(1, min(max_per_wallet, remaining_supply, 5))

def get_web3_from_config(chain_config: dict) -> Web3:
    return Web3(Web3.HTTPProvider(chain_config.get("rpc_url", "")))

def get_wallet_balance(w3: Web3, wallet_address: str) -> tuple:
    try:
        balance_wei = w3.eth.get_balance(Web3.to_checksum_address(wallet_address))
        return balance_wei / 1e18, balance_wei
    except: return 0.0, 0

def get_fee_recipient(w3: Web3, seadrop_address: str, nft_contract: str) -> Optional[str]:
    try:
        contract = w3.eth.contract(address=Web3.to_checksum_address(seadrop_address), abi=SEADROP_ABI)
        recipients = contract.functions.getAllowedFeeRecipients(Web3.to_checksum_address(nft_contract)).call()
        return recipients[0] if recipients else None
    except: return None

def attempt_purchase(w3, private_key, wallet_address, nft_contract, seadrop_address, price_wei, max_per_wallet, remaining_supply, eth_price_usd):
    is_free = is_price_free(price_wei)
    if not is_free:
        return MintResult(success=False, wallet=wallet_address, reason="not_free_mint", reason_text=get_reason_text("not_free_mint"), price_wei=price_wei, is_free=False)
    
    balance_eth, balance_wei = get_wallet_balance(w3, wallet_address)
    balance_usd = balance_eth * eth_price_usd
    if balance_usd < MIN_BALANCE_RESERVE_USD:
        return MintResult(success=False, wallet=wallet_address, reason="balance_too_low", reason_text=get_reason_text("balance_too_low"), balance_eth=balance_eth, balance_usd=balance_usd, is_free=True)
    
    quantity = decide_quantity(max_per_wallet, remaining_supply)
    total_value_wei = price_wei * quantity
    if total_value_wei / 1e18 > MAX_ETH_PER_TX:
        return MintResult(success=False, wallet=wallet_address, reason="tx_value_too_high", reason_text=get_reason_text("tx_value_too_high"), balance_eth=balance_eth, balance_usd=balance_usd, is_free=True)
    
    wallet_lock = get_wallet_lock(wallet_address)
    with wallet_lock:
        try:
            contract = w3.eth.contract(address=Web3.to_checksum_address(seadrop_address), abi=SEADROP_ABI)
            nonce = w3.eth.get_transaction_count(Web3.to_checksum_address(wallet_address), "pending")
            gas_price = w3.eth.gas_price
            fee_recipient = get_fee_recipient(w3, seadrop_address, nft_contract)
            if not fee_recipient:
                return MintResult(success=False, wallet=wallet_address, reason="no_fee_recipient", reason_text=get_reason_text("no_fee_recipient"), balance_eth=balance_eth, balance_usd=balance_usd, is_free=True)
            
            tx = contract.functions.mintPublic(
                Web3.to_checksum_address(nft_contract), Web3.to_checksum_address(fee_recipient),
                Web3.to_checksum_address(ZERO_ADDRESS), quantity
            ).build_transaction({"from": Web3.to_checksum_address(wallet_address), "value": total_value_wei, "nonce": nonce, "gasPrice": gas_price, "chainId": w3.eth.chain_id})
            
            try:
                estimated_gas = w3.eth.estimate_gas(tx)
                tx["gas"] = int(estimated_gas * GAS_LIMIT_SAFETY_MARGIN)
                gas_price_gwei = gas_price / 1e9
                gas_usd = (tx["gas"] * gas_price) / 1e18 * eth_price_usd
                if gas_usd > MAX_GAS_FEE_USD:
                    return MintResult(success=False, wallet=wallet_address, reason="gas_too_high", reason_text=get_reason_text("gas_too_high"), gas_units=tx["gas"], gas_estimated=estimated_gas, gas_price_gwei=gas_price_gwei, gas_used_usd=gas_usd, balance_eth=balance_eth, balance_usd=balance_usd, is_free=True)
            except Exception:
                return MintResult(success=False, wallet=wallet_address, reason="simulation_failed", reason_text=get_reason_text("simulation_failed"), balance_eth=balance_eth, balance_usd=balance_usd, is_free=True)
            
            signed_tx = w3.eth.account.sign_transaction(tx, private_key)
            tx_hash = w3.eth.send_raw_transaction(signed_tx.rawTransaction if hasattr(signed_tx, 'rawTransaction') else signed_tx.raw_transaction)
            tx_hash_str = tx_hash.hex() if hasattr(tx_hash, 'hex') else str(tx_hash)
            gas_used_usd = (tx["gas"] * gas_price) / 1e18 * eth_price_usd
            
            return MintResult(success=True, wallet=wallet_address, tx_hash=tx_hash_str, quantity=quantity, gas_used_eth=(tx["gas"]*gas_price)/1e18, gas_used_usd=gas_used_usd, gas_units=tx["gas"], gas_price_gwei=gas_price/1e9, gas_estimated=estimated_gas, total_value_eth=total_value_wei/1e18, balance_eth=balance_eth, balance_usd=balance_usd, confirmed=False, is_free=True, is_eligible=True)
        except Exception as e:
            error_msg = str(e).lower()
            if "not eligible" in error_msg or "not in allowlist" in error_msg: reason = "not_eligible"
            elif "insufficient funds" in error_msg: reason = "insufficient_funds"
            elif "gas" in error_msg: reason = "gas_too_high"
            elif "nonce" in error_msg: reason = "nonce_error"
            elif "revert" in error_msg or "execution reverted" in error_msg: reason = "simulation_failed"
            else: reason = "tx_error"
            return MintResult(success=False, wallet=wallet_address, reason=reason, reason_text=get_reason_text(reason), balance_eth=balance_eth, balance_usd=balance_usd, price_wei=price_wei, is_free=is_free, is_eligible=(reason!="not_eligible"))

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
    result = {"active": [], "upcoming": [], "ended": [], "paid": []}
    all_stages = extract_all_stages_raw(detail)
    seen = set()
    for stage in all_stages:
        price_wei = extract_price_from_stage(stage)
        stage_name = extract_stage_name(stage)
        start_time = stage.get("start_time") or stage.get("startTime") or ""
        stage_id = f"{stage_name}_{start_time}"
        if stage_id in seen: continue
        seen.add(stage_id)
        status = get_stage_status(stage)
        start_dt = parse_stage_time(start_time)
        end_dt = parse_stage_time(stage.get("end_time") or stage.get("endTime") or "")
        is_free = is_price_free(price_wei)
        stage_data = {**stage, "stage": stage_name, "price_wei": price_wei, "price_eth": price_wei/1e18, "status": status, "start_dt": start_dt, "end_dt": end_dt, "is_free": is_free}
        if is_free: result[status].append(stage_data)
        else: result["paid"].append(stage_data)
    return result

def get_retry_config(reason: str) -> RetryConfig:
    return REASON_RETRY_CONFIGS.get(reason, REASON_RETRY_CONFIGS["gas_too_high"])

def calculate_retry_delay(config: RetryConfig, attempt_count: int) -> float:
    delay = config.base_delay
    if config.strategy == RetryStrategy.EXPONENTIAL: delay *= (config.backoff_multiplier**(attempt_count-1))
    elif config.strategy == RetryStrategy.LINEAR: delay *= attempt_count
    delay = min(delay, config.max_delay)
    if config.jitter: delay *= random.uniform(0.75, 1.25)
    return delay

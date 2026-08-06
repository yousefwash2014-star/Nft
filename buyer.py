"""
محرك الشراء التلقائي عبر عقد SeaDrop على سلاسل متعددة (Ethereum + Robinhood).
"""

import logging
import threading
import time
import random
from web3 import Web3

# تهيئة مسجل الأحداث الخاص بالمشتري
log = logging.getLogger("buyer")

# ===========================================================================
# استيراد POA middleware مع توافق جميع إصدارات web3.py
# ===========================================================================
geth_poa_middleware = None

try:
    from web3.middleware import geth_poa_middleware
except ImportError:
    try:
        from web3.middleware.poa import geth_poa_middleware
    except ImportError:
        try:
            from web3.middleware import poa_middleware as geth_poa_middleware
        except ImportError:
            log.warning("⚠️ POA middleware غير متوفرة. قد تواجه مشاكل مع Robinhood Chain.")
            geth_poa_middleware = None

# ===========================================================================
# إعدادات فحص الرصيد - لكل سلسلة على حدة
# ===========================================================================
_balance_cache = {}  # المفتاح: f"{wallet_address}:{chain_name}"
_balance_cache_lock = threading.Lock()

MAX_RETRIES_BALANCE = 4
BASE_RETRY_DELAY = 1.5
MAX_RETRY_DELAY = 30
BALANCE_CHECK_INTERVAL = 10800  # 3 ساعات

# ===========================================================================
# نظام قفل Nonce
# ===========================================================================
nonce_locks = {}
nonce_locks_lock = threading.Lock()

def get_wallet_lock(address: str) -> threading.Lock:
    with nonce_locks_lock:
        if address not in nonce_locks:
            nonce_locks[address] = threading.Lock()
        return nonce_locks[address]

# ===========================================================================
# الحد الأقصى المسموح به لقيمة المعاملة
# ===========================================================================
MAX_ETH_PER_TX = 0.02

# ===========================================================================
# إعدادات السلاسل المدعومة
# ===========================================================================
CHAINS_CONFIG = {
    "robinhood": {
        "rpc_env_var": "ROBINHOOD_RPC_URL",
        "seadrop_address": Web3.to_checksum_address("0x00005EA00Ac477B1030CE78506496e8C2dE24bf5"),
        "chain_name_display": "Robinhood Chain",
        "native_currency": "ETH",
    },
    "ethereum": {
        "rpc_env_var": "ETHEREUM_RPC_URL",
        "seadrop_address": Web3.to_checksum_address("0x00005EA00Ac477B1030CE78506496e8C2dE24bf5"),
        "chain_name_display": "Ethereum Mainnet",
        "native_currency": "ETH",
    },
}

ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"

# ===========================================================================
# ABI عقد SeaDrop
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
# إعدادات الأمان
# ===========================================================================
MAX_GAS_FEE_USD = 0.05
MIN_BALANCE_RESERVE_USD = 0.05
FEW_THRESHOLD = 10
LIMITED_BUY_QTY = 10
GAS_LIMIT_SAFETY_MARGIN = 1.2


def get_web3_from_config(chain_config: dict) -> Web3:
    """إنشاء كائن Web3 من إعدادات سلسلة محددة."""
    rpc_url = chain_config.get("rpc_url")
    if not rpc_url:
        raise ValueError(f"لا يوجد RPC URL للسلسلة: {chain_config.get('chain_name_display', 'unknown')}")
    
    w3 = Web3(Web3.HTTPProvider(rpc_url))
    
    if geth_poa_middleware is not None:
        try:
            w3.middleware_onion.inject(geth_poa_middleware, layer=0)
            log.info(f"✅ تم تطبيق POA middleware على {chain_config.get('chain_name_display', 'unknown')}")
        except Exception as e:
            log.warning(f"⚠️ فشل تطبيق POA middleware: {e}")
    
    return w3


def get_balance_cache_key(wallet_address: str, chain_name: str) -> str:
    """إنشاء مفتاح فريد للتخزين المؤقت لكل محفظة وسلسلة."""
    return f"{wallet_address.lower()}:{chain_name.lower()}"


def get_wallet_balance_usd(w3: Web3, wallet_address: str, chain_name: str, eth_price_usd: float) -> float:
    """
    يرجع رصيد المحفظة بالدولار - لكل سلسلة بشكل منفصل.
    """
    now = time.time()
    checksum_address = Web3.to_checksum_address(wallet_address)
    cache_key = get_balance_cache_key(wallet_address, chain_name)
    
    # =============================================================
    # 1. فحص التخزين المؤقت أولاً
    # =============================================================
    with _balance_cache_lock:
        if cache_key in _balance_cache:
            last_check, balance = _balance_cache[cache_key]
            if now - last_check < BALANCE_CHECK_INTERVAL:
                log.debug(f"💰 [CACHE] {chain_name} - رصيد {wallet_address[:8]}...: ${balance:.4f}")
                return balance
    
    # =============================================================
    # 2. محاولة جلب الرصيد مع إعادة محاولة ذكية
    # =============================================================
    for attempt in range(MAX_RETRIES_BALANCE):
        try:
            balance_wei = w3.eth.get_balance(checksum_address)
            balance_usd = (balance_wei / 1e18) * eth_price_usd
            
            with _balance_cache_lock:
                _balance_cache[cache_key] = (now, balance_usd)
            
            log.info(f"💰 [UPDATED] {chain_name} - رصيد {wallet_address[:8]}...: ${balance_usd:.4f}")
            return balance_usd
            
        except Exception as e:
            error_msg = str(e).lower()
            
            is_rate_limit = "429" in error_msg or "rate limit" in error_msg or "too many requests" in error_msg
            
            if is_rate_limit:
                delay = min(BASE_RETRY_DELAY * (2 ** attempt) + random.uniform(0, 0.5), MAX_RETRY_DELAY)
                log.warning(f"⚠️ [RATE LIMIT] {chain_name} - محاولة {attempt + 1}/{MAX_RETRIES_BALANCE} - انتظار {delay:.1f} ثانية")
                time.sleep(delay)
                continue
            else:
                log.error(f"[الرصيد] {chain_name} - تعذر القراءة: {e}")
                break
    
    # =============================================================
    # 3. إذا فشلت كل المحاولات، استخدم آخر قيمة معروفة
    # =============================================================
    with _balance_cache_lock:
        if cache_key in _balance_cache:
            log.warning(f"⚠️ [FALLBACK] {chain_name} - استخدام رصيد مخزن قديم لـ {wallet_address[:8]}...")
            return _balance_cache[cache_key][1]
    
    log.error(f"❌ [FALLBACK] {chain_name} - تعذر الحصول على رصيد {wallet_address[:8]}...، استخدم 0.0")
    return 0.0


def get_all_wallets_balances(w3_instances: dict, wallets: list, eth_price_usd: float) -> dict:
    """
    جلب رصيد جميع المحافظ على جميع السلاسل.
    
    المعاملات:
        w3_instances: dict - قاموس كائنات Web3 لكل سلسلة
        wallets: list - قائمة المحافظ
        eth_price_usd: float - سعر ETH الحالي
    
    المخرجات:
        dict: {
            "wallet_name": {
                "ethereum": 0.123,
                "robinhood": 0.456
            }
        }
    """
    balances = {}
    
    for wallet in wallets:
        wallet_name = wallet["name"]
        wallet_address = wallet["address"]
        balances[wallet_name] = {}
        
        for chain_name, w3 in w3_instances.items():
            balance = get_wallet_balance_usd(w3, wallet_address, chain_name, eth_price_usd)
            balances[wallet_name][chain_name] = balance
    
    return balances


def estimate_gas_fee_usd(w3: Web3, eth_price_usd: float, gas_units: int = 150_000) -> float:
    """تقدير أولي سريع لرسوم المعاملة."""
    try:
        gas_price_wei = w3.eth.gas_price
        fee_eth = (gas_price_wei * gas_units) / 1e18
        return fee_eth * eth_price_usd
    except Exception as e:
        log.warning(f"[الغاز] تعذر التقدير: {e}")
        return float("inf")


def get_fee_recipient(w3: Web3, seadrop_address: str, nft_contract: str) -> str | None:
    """استعلام عنوان الرسوم المسموح من عقد SeaDrop."""
    try:
        seadrop = w3.eth.contract(address=Web3.to_checksum_address(seadrop_address), abi=SEADROP_ABI)
        recipients = seadrop.functions.getAllowedFeeRecipients(
            Web3.to_checksum_address(nft_contract)
        ).call()
        if not recipients:
            log.warning(f"[عنوان الرسوم] لا يوجد عنوان مسموح لـ {nft_contract}")
            return None
        return recipients[0]
    except Exception as e:
        log.error(f"[عنوان الرسوم] خطأ استعلام: {e}")
        return None


def decide_quantity(max_per_wallet: int | None, remaining_supply: int) -> int:
    """تحديد الكمية المناسبة للشراء."""
    if max_per_wallet is None:
        qty = 1
    elif max_per_wallet <= FEW_THRESHOLD:
        qty = max_per_wallet
    else:
        qty = LIMITED_BUY_QTY
    
    return max(1, min(qty, remaining_supply))


def quick_checks(
    w3: Web3,
    wallet_address: str,
    chain_name: str,
    eth_price_usd: float,
    nft_contract: str,
    seadrop_address: str,
) -> dict:
    """
    فحوصات أولية سريعة - مع تخزين الرصيد لكل سلسلة بشكل منفصل.
    """
    result = {
        "pass": True,
        "reason": "",
        "balance_usd": 0,
        "gas_fee_usd": 0,
        "fee_recipient": None,
        "chain": chain_name,
    }
    
    try:
        now = time.time()
        cache_key = get_balance_cache_key(wallet_address, chain_name)
        
        # =============================================================
        # فحص الرصيد المخزن لهذه السلسلة
        # =============================================================
        with _balance_cache_lock:
            if cache_key in _balance_cache:
                last_check, balance = _balance_cache[cache_key]
                time_since = now - last_check
                
                if time_since < BALANCE_CHECK_INTERVAL:
                    result["balance_usd"] = balance
                    log.debug(f"💰 {chain_name} - رصيد مخزن لـ {wallet_address[:10]}: ${balance:.4f} (منذ {time_since/3600:.1f} ساعة)")
                    
                    if balance < MIN_BALANCE_RESERVE_USD:
                        result["pass"] = False
                        result["reason"] = "balance_too_low"
                        log.warning(f"⚠️ {chain_name} - رصيد منخفض: ${balance:.4f} < ${MIN_BALANCE_RESERVE_USD}")
                        return result
                    
                    try:
                        gas_price_wei = w3.eth.gas_price
                        result["gas_fee_usd"] = (gas_price_wei * 100000 / 1e18) * eth_price_usd
                    except:
                        result["gas_fee_usd"] = 0.01
                    
                    if result["gas_fee_usd"] > MAX_GAS_FEE_USD:
                        result["pass"] = False
                        result["reason"] = "gas_too_high"
                        log.warning(f"⚠️ {chain_name} - رسوم غاز مرتفعة: ${result['gas_fee_usd']:.4f} > ${MAX_GAS_FEE_USD}")
                        return result
                    
                    result["fee_recipient"] = seadrop_address
                    result["pass"] = True
                    return result
        
        # =============================================================
        # جلب الرصيد من السلسلة
        # =============================================================
        balance_usd = get_wallet_balance_usd(w3, wallet_address, chain_name, eth_price_usd)
        result["balance_usd"] = balance_usd
        
        if balance_usd < MIN_BALANCE_RESERVE_USD:
            result["pass"] = False
            result["reason"] = "balance_too_low"
            log.warning(f"⚠️ {chain_name} - رصيد منخفض: ${balance_usd:.4f} < ${MIN_BALANCE_RESERVE_USD}")
            return result
        
        try:
            gas_price_wei = w3.eth.gas_price
            result["gas_fee_usd"] = (gas_price_wei * 100000 / 1e18) * eth_price_usd
        except:
            result["gas_fee_usd"] = 0.01
        
        if result["gas_fee_usd"] > MAX_GAS_FEE_USD:
            result["pass"] = False
            result["reason"] = "gas_too_high"
            log.warning(f"⚠️ {chain_name} - رسوم غاز مرتفعة: ${result['gas_fee_usd']:.4f} > ${MAX_GAS_FEE_USD}")
            return result
        
        fee_recipient = get_fee_recipient(w3, seadrop_address, nft_contract)
        result["fee_recipient"] = fee_recipient if fee_recipient else seadrop_address
        
        if not fee_recipient:
            result["pass"] = False
            result["reason"] = "no_fee_recipient"
            log.warning(f"⚠️ {chain_name} - لا يوجد عنوان رسوم لـ {nft_contract}")
            return result
        
        result["pass"] = True
        return result
        
    except Exception as e:
        log.error(f"⚠️ {chain_name} - خطأ في quick_checks: {e}")
        result["pass"] = True
        return result


def is_paid_mint(price_wei: int, eth_price_usd: float, threshold_usd: float = 0.01) -> tuple:
    """التحقق مما إذا كان المينت مدفوعًا أم مجانيًا."""
    if price_wei <= 0:
        return False, 0.0, "مجاني"
    
    price_usd = (price_wei / 1e18) * eth_price_usd
    if price_usd < threshold_usd:
        return False, price_usd, "مجاني (أقل من 1 سنت)"
    
    return True, price_usd, "مدفوع"


def check_eligibility_reason(reason: str) -> dict:
    """تحليل سبب الفشل وتحديد الأهلية وإمكانية إعادة المحاولة."""
    if reason in ("gas_too_high", "gas_too_high_precise"):
        return {
            "eligible": True,
            "issue_type": "network",
            "description": "رسوم الغاز مرتفعة حاليًا، أنت مؤهل لكن الشبكة مزدحمة",
            "retryable": True,
        }
    
    if reason == "balance_too_low":
        return {
            "eligible": False,
            "issue_type": "wallet",
            "description": "الرصيد في المحفظة غير كافٍ، تحتاج تمويل المحفظة",
            "retryable": False,
        }
    
    if reason == "insufficient_funds_for_total_cost":
        return {
            "eligible": False,
            "issue_type": "wallet",
            "description": "الرصيد لا يكفي لتغطية سعر المينت + رسوم الغاز معًا",
            "retryable": False,
        }
    
    if reason == "tx_value_too_high":
        return {
            "eligible": True,
            "issue_type": "safe",
            "description": "قيمة المعاملة تجاوزت الحد الأقصى المسموح به في الإعدادات",
            "retryable": False,
        }
    
    if reason == "no_fee_recipient":
        return {
            "eligible": True,
            "issue_type": "contract",
            "description": "تعذر الحصول على عنوان الرسوم من العقد، قد يكون المينت غير نشط بعد",
            "retryable": True,
        }
    
    if reason == "simulation_failed":
        return {
            "eligible": True,
            "issue_type": "contract",
            "description": "فشلت محاكاة المعاملة — قد لا يكون المينت متاحًا حاليًا",
            "retryable": True,
        }
    
    if reason == "tx_error":
        return {
            "eligible": True,
            "issue_type": "network",
            "description": "حدث خطأ أثناء إرسال المعاملة — مشكلة شبكة مؤقتة",
            "retryable": True,
        }
    
    return {
        "eligible": False,
        "issue_type": "unknown",
        "description": f"سبب غير معروف: {reason}",
        "retryable": False,
    }


RETRYABLE_REASONS = {
    "gas_too_high",
    "gas_too_high_precise",
    "tx_error",
    "simulation_failed",
    "no_fee_recipient",
}


def attempt_purchase(
    w3: Web3,
    private_key: str,
    wallet_address: str,
    chain_name: str,
    nft_contract: str,
    seadrop_address: str,
    price_wei_per_token: int,
    max_per_wallet: int | None,
    remaining_supply: int,
    eth_price_usd: float,
) -> dict:
    """
    محاولة شراء واحدة - مع تحقق صارم من السعر.
    """
    if price_wei_per_token != 0:
        log.warning(f"⛔ {chain_name} - السعر ليس مجانياً: {price_wei_per_token} wei")
        return {
            "success": False, 
            "reason": "not_free_mint",
            "price_wei": price_wei_per_token,
            "balance_usd": 0,
            "chain": chain_name,
        }
    
    price_usd = (price_wei_per_token / 1e18) * eth_price_usd
    if price_usd > 0.0000000001:
        log.warning(f"⛔ {chain_name} - السعر ليس مجانياً: ${price_usd:.10f}")
        return {
            "success": False, 
            "reason": "not_free_mint",
            "price_usd": price_usd,
            "balance_usd": 0,
            "chain": chain_name,
        }
    
    balance_usd = get_wallet_balance_usd(w3, wallet_address, chain_name, eth_price_usd)
    log.info(f"💰 {chain_name} - رصيد المحفظة قبل الشراء: ${balance_usd:.4f}")
    
    quantity = decide_quantity(max_per_wallet, remaining_supply)
    total_value = price_wei_per_token * quantity
    
    total_eth = total_value / 1e18
    if total_eth > MAX_ETH_PER_TX:
        log.warning(f"[أمان] {chain_name} - ⛔ قيمة المعاملة {total_eth:.6f} ETH > الحد الأقصى {MAX_ETH_PER_TX} ETH")
        return {"success": False, "reason": "tx_value_too_high", "tx_value_eth": total_eth, "chain": chain_name}

    wallet_lock = get_wallet_lock(wallet_address)
    wallet_lock.acquire()
    signed_tx = None
    try:
        contract = w3.eth.contract(address=Web3.to_checksum_address(seadrop_address), abi=SEADROP_ABI)
        current_nonce = w3.eth.get_transaction_count(Web3.to_checksum_address(wallet_address), "pending")
        current_gas_price = w3.eth.gas_price
        
        fee_recipient = get_fee_recipient(w3, seadrop_address, nft_contract)
        if not fee_recipient:
            log.warning(f"⚠️ {chain_name} - لا يوجد عنوان رسوم، نحاول باستخدام عنوان افتراضي")
            fee_recipient = seadrop_address
        
        tx = contract.functions.mintPublic(
            Web3.to_checksum_address(nft_contract),
            Web3.to_checksum_address(fee_recipient),
            Web3.to_checksum_address(ZERO_ADDRESS),
            quantity,
        ).build_transaction({
            "from": Web3.to_checksum_address(wallet_address),
            "value": total_value,
            "nonce": current_nonce,
            "gasPrice": current_gas_price,
            "chainId": w3.eth.chain_id,
        })

        try:
            estimated_gas = w3.eth.estimate_gas(tx)
            tx["gas"] = int(estimated_gas * GAS_LIMIT_SAFETY_MARGIN)
            log.info(f"⛽ {chain_name} - الغاز المقدر: {estimated_gas}, مع الهامش: {tx['gas']}")
        except Exception as e:
            log.error(f"⚠️ {chain_name} - فشل estimate_gas: {e}")
            return {"success": False, "reason": "simulation_failed", "error": str(e), "chain": chain_name}

        signed_tx = w3.eth.account.sign_transaction(tx, private_key=private_key)
        raw_tx = getattr(signed_tx, 'raw_transaction', None)
        if raw_tx is None:
            raw_tx = getattr(signed_tx, 'rawTransaction', None)
        if raw_tx is None:
            raise ValueError("raw_transaction غير موجود")
            
        tx_hash = w3.eth.send_raw_transaction(raw_tx)
        
        gas_used = tx.get("gas", 0)
        gas_fee_eth = (gas_used * current_gas_price) / 1e18
        gas_fee_usd = gas_fee_eth * eth_price_usd

        log.info(f"✅ {chain_name} - [شراء ناجح] {tx_hash.hex()} — كمية: {quantity}")
        return {
            "success": True,
            "tx_hash": tx_hash.hex(),
            "quantity": quantity,
            "gas_fee_usd": gas_fee_usd,
            "total_value_wei": total_value,
            "balance_usd": balance_usd,
            "chain": chain_name,
        }

    except Exception as e:
        error_msg = str(e).lower()
        log.error(f"❌ {chain_name} - [خطأ إرسال] {e}")
        
        if "insufficient funds" in error_msg:
            reason = "insufficient_funds_for_total_cost"
            log.warning(f"⚠️ {chain_name} - رصيد غير كافٍ: ${balance_usd:.4f}")
        elif "gas" in error_msg:
            reason = "gas_too_high"
        else:
            reason = "tx_error"
        
        result = {"success": False, "reason": reason, "error": str(e), "balance_usd": balance_usd, "chain": chain_name}
        if signed_tx is not None:
            try:
                result["tx_hash"] = signed_tx.hash.hex()
            except Exception:
                pass
        return result
    finally:
        wallet_lock.release()

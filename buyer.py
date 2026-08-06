"""
محرك الشراء التلقائي عبر عقد SeaDrop على سلاسل متعددة (Ethereum + Robinhood).

يحتوي كل ضوابط الأمان بمكان واحد، بالترتيب الصحيح للتحقق:
  1. الرصيد الحالي بالمحفظة (توقف لو منخفض جدًا)
  2. رسوم الغاز الحالية (إلغاء لو مرتفعة)
  3. عنوان الرسوم المسموح (يُقرأ من العقد نفسه، بدون تخمين)
  4. تنفيذ المعاملة

هذا الملف هو المسؤول عن:
- الاتصال بشبكات الإيثيريوم والـ Robinhood Chain
- التحقق من الرصيد ورسوم الغاز
- استعلام عنوان الرسوم المسموح من العقد
- بناء وتوقيع وإرسال معاملات المينت
"""

import logging
import threading
from web3 import Web3

# تهيئة مسجل الأحداث الخاص بالمشتري
log = logging.getLogger("buyer")

# ===========================================================================
# نظام قفل Nonce - يمنع تضارب المعاملات المتزامنة
# ===========================================================================
# هذا القفل يضمن أن كل محفظة ترسل معاملة واحدة فقط في نفس الوقت
# وبالتالي nonce صحيح لكل معاملة
nonce_locks = {}          # قاموس الأقفال لكل محفظة
nonce_locks_lock = threading.Lock()  # قفل لحماية قاموس الأقفال نفسه

def get_wallet_lock(address: str) -> threading.Lock:
    """
    يرجع قفل خاص بالمحفظة.
    كل محفظة لها قفل منفصل.
    """
    with nonce_locks_lock:
        if address not in nonce_locks:
            nonce_locks[address] = threading.Lock()
        return nonce_locks[address]

# ===========================================================================
# الحد الأقصى المسموح به لقيمة المعاملة (ETH)
# ===========================================================================
MAX_ETH_PER_TX = 0.02  # لا ترسل أكثر من 0.01 ETH في معاملة واحدة (حماية)

# ===========================================================================
# إعدادات السلاسل المدعومة
# ===========================================================================
# نحدد هنا كل سلسلة نريد دعمها، مع:
# - متغير البيئة الخاص بـ RPC URL
# - عنوان عقد SeaDrop (ثابت لكل سلسلة)
# - اسم السلسلة للعرض
# - العملة الأصلية (ETH في الحالتين)

CHAINS_CONFIG = {
    "robinhood": {
        "rpc_env_var": "ROBINHOOD_RPC_URL",                              # متغير البيئة لـ RPC
        "seadrop_address": Web3.to_checksum_address("0x00005EA00Ac477B1030CE78506496e8C2dE24bf5"),  # عنوان عقد SeaDrop
        "chain_name_display": "Robinhood Chain",                         # الاسم المعروض
        "native_currency": "ETH",                                         # العملة الأصلية
    },
    "ethereum": {
        "rpc_env_var": "ETHEREUM_RPC_URL",                               # متغير البيئة لـ RPC
        "seadrop_address": Web3.to_checksum_address("0x00005EA00Ac477B1030CE78506496e8C2dE24bf5"),  # عنوان عقد SeaDrop
        "chain_name_display": "Ethereum Mainnet",                        # الاسم المعروض
        "native_currency": "ETH",                                         # العملة الأصلية
    },
}

# عنوان الصفر (0x0...) المستخدم في معاملات المينت
ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"

# ===========================================================================
# ABI (واجهة) عقد SeaDrop
# ===========================================================================
# هذه هي التواقيع التي نحتاجها من العقد:
# - mintPublic: دالة المينت العامة
# - getAllowedFeeRecipients: لاستعلام عنوان الرسوم المسموح

SEADROP_ABI = [
    {
        "inputs": [
            {"name": "nftContract", "type": "address"},     # عنوان عقد الـ NFT
            {"name": "feeRecipient", "type": "address"},     # عنوان مستلم الرسوم
            {"name": "minterIfNotPayer", "type": "address"}, # العنوان الذي سيقوم بالمينت (0x00 = الدافع نفسه)
            {"name": "quantity", "type": "uint256"},         # الكمية المراد شراؤها
        ],
        "name": "mintPublic",                                # اسم الدالة
        "outputs": [],                                       # لا مخرجات
        "stateMutability": "payable",                        # تقبل دفع ETH
        "type": "function",
    },
    {
        "inputs": [{"name": "nftContract", "type": "address"}],  # عنوان عقد الـ NFT
        "name": "getAllowedFeeRecipients",                        # اسم الدالة
        "outputs": [{"name": "", "type": "address[]"}],           # ترجع مصفوفة من العناوين
        "stateMutability": "view",                                # دالة قراءة فقط (مجانية)
        "type": "function",
    },
]

# ===========================================================================
# ضوابط قابلة للتعديل (إعدادات الأمان)
# ===========================================================================
# هذه القيم تتحكم بسلوك الأمان للنظام:
MAX_GAS_FEE_USD = 0.06         # ألغِ الشراء لو رسوم الغاز أعلى من هذا المبلغ
MIN_BALANCE_RESERVE_USD = 0.10  # توقف عن الشراء لو الرصيد أقل من هذا المبلغ
FEW_THRESHOLD = 20              # إذا كان الحد الأقصى 20 قطعة أو أقل، اشترِ الكل
LIMITED_BUY_QTY = 5             # إذا كان الحد الأقصى أكثر من 20، اشترِ هذا العدد فقط
GAS_LIMIT_SAFETY_MARGIN = 1.2   # هامش أمان 20% إضافي فوق تقدير الغاز


def get_web3_from_config(chain_config: dict) -> Web3:
    """
    إنشاء كائن Web3 من إعدادات سلسلة محددة.
    تُستدعى هذه الدالة بعد تحميل متغيرات البيئة من ملف .env.
    
    المعاملات:
        chain_config: dict - إعدادات السلسلة (تحتوي على rpc_url)
    
    المخرجات:
        Web3 - كائن Web3 جاهز للاستخدام
    
    الأخطاء:
        ValueError - إذا لم يتم توفير RPC URL
    """
    # استخراج رابط RPC من الإعدادات
    rpc_url = chain_config.get("rpc_url")
    if not rpc_url:
        raise ValueError(f"لا يوجد RPC URL للسلسلة: {chain_config.get('chain_name_display', 'unknown')}")
    return Web3(Web3.HTTPProvider(rpc_url))


def get_wallet_balance_usd(w3: Web3, wallet_address: str, eth_price_usd: float) -> float:
    """
    يرجع رصيد المحفظة بالدولار الأمريكي.
    عند أي خطأ، يرجع 0.0 (إجراء أمان: يمنع الشراء).
    
    المعاملات:
        w3: Web3 - كائن Web3 متصل
        wallet_address: str - عنوان المحفظة
        eth_price_usd: float - سعر ETH الحالي بالدولار
    
    المخرجات:
        float - رصيد المحفظة بالدولار
    """
    try:
        # جلب الرصيد بالـ Wei وتحويله إلى ETH ثم ضربه بسعر الصرف
        balance_wei = w3.eth.get_balance(Web3.to_checksum_address(wallet_address))
        return (balance_wei / 1e18) * eth_price_usd
    except Exception as e:
        # في حالة الخطأ، نسجل الحدث ونرجع 0.0 (يمنع الشراء)
        log.error(f"[الرصيد] تعذر القراءة: {e}")
        return 0.0


def estimate_gas_fee_usd(w3: Web3, eth_price_usd: float, gas_units: int = 150_000) -> float:
    """
    تقدير أولي سريع لرسوم معاملة عادية، قبل بناء المعاملة الفعلية.
    يُستخدم هذا الفحص السريع لتصفية الرسوم المرتفعة دون إضاعة وقت.
    
    المعاملات:
        w3: Web3 - كائن Web3 متصل
        eth_price_usd: float - سعر ETH الحالي بالدولار
        gas_units: int - عدد وحدات الغاز المتوقعة (افتراضي 150,000)
    
    المخرجات:
        float - رسوم الغاز المقدرة بالدولار (أو ما لا نهاية عند الخطأ)
    """
    try:
        # جلب سعر الغاز الحالي وحساب التكلفة
        gas_price_wei = w3.eth.gas_price
        fee_eth = (gas_price_wei * gas_units) / 1e18
        return fee_eth * eth_price_usd
    except Exception as e:
        # في حالة الخطأ، نسجل تحذيرًا ونرجع ما لا نهاية (يمنع الشراء)
        log.warning(f"[الغاز] تعذر التقدير: {e}")
        return float("inf")  # عند الشك، اعتبرها عالية جدًا ولا تشترِ


def get_fee_recipient(w3: Web3, seadrop_address: str, nft_contract: str) -> str | None:
    """
    يسأل عقد SeaDrop مباشرة عن عنوان الرسوم المسموح لهذا العقد تحديدًا.
    هذا أكثر أمانًا من تخمين العنوان أو استخدام عنوان ثابت.
    
    المعاملات:
        w3: Web3 - كائن Web3 متصل
        seadrop_address: str - عنوان عقد SeaDrop
        nft_contract: str - عنوان عقد الـ NFT
    
    المخرجات:
        str | None - عنوان مستلم الرسوم الأول، أو None إذا لم يوجد
    """
    try:
        # إنشاء كائن العقد واستدعاء الدالة
        seadrop = w3.eth.contract(address=Web3.to_checksum_address(seadrop_address), abi=SEADROP_ABI)
        recipients = seadrop.functions.getAllowedFeeRecipients(
            Web3.to_checksum_address(nft_contract)
        ).call()
        # التحقق من وجود عنوان مسموح
        if not recipients:
            log.warning(f"[عنوان الرسوم] لا يوجد عنوان مسموح لـ {nft_contract}")
            return None
        return recipients[0]  # نأخذ أول عنوان مسموح
    except Exception as e:
        log.error(f"[عنوان الرسوم] خطأ استعلام: {e}")
        return None


def decide_quantity(max_per_wallet: int | None, remaining_supply: int) -> int:
    """
    تحدد الكمية المناسبة للشراء بناءً على:
    - max_per_wallet <= 5  => اشترِ الحد الأقصى المسموح
    - max_per_wallet > 5  => اشترِ 5 فقط (تفادي مينتات ذات كمية ضخمة)
    - max_per_wallet مجهول  => اشترِ قطعة واحدة فقط (أمان)
    
    المعاملات:
        max_per_wallet: int | None - الحد الأقصى لكل محفظة
        remaining_supply: int - الكمية المتبقية من المينت
    
    المخرجات:
        int - الكمية التي سيتم شراؤها (على الأقل 1، وعلى الأكثر الكمية المتبقية)
    """
    # تحديد الكمية بناءً على القواعد
    if max_per_wallet is None:
        qty = 1                     # أمان: إذا لا يوجد حد، اشترِ قطعة واحدة
    elif max_per_wallet <= FEW_THRESHOLD:
        qty = max_per_wallet        # كمية قليلة: اشترِ الكل
    else:
        qty = LIMITED_BUY_QTY       # كمية كبيرة: حددها بـ 5
    
    # التأكد من أن الكمية بين 1 والكمية المتبقية
    return max(1, min(qty, remaining_supply))


# ===========================================================================
# فحوصات سريعة (تستخدم قبل بناء المعاملة)
# ===========================================================================
# هذه الدالة تُجري الفحوصات الأولية الثلاثة (الرصيد، الغاز، عنوان الرسوم)
# في خطوة واحدة سريعة لتجنب إضاعة الوقت في المعاملات غير المجدية

def quick_checks(
    w3: Web3,
    wallet_address: str,
    eth_price_usd: float,
    nft_contract: str,
    seadrop_address: str,
) -> dict:
    """
    فحوصات أولية سريعة (الرصيد + الغاز + عنوان الرسوم).
    
    المعاملات:
        w3: Web3 - كائن Web3 متصل
        wallet_address: str - عنوان المحفظة
        eth_price_usd: float - سعر ETH
        nft_contract: str - عنوان عقد الـ NFT
        seadrop_address: str - عنوان عقد SeaDrop
    
    المخرجات:
        dict: {
            pass: bool | None,      # هل نجحت كل الفحوصات؟
            reason: str,             # سبب الفشل (إن وجد)
            balance_usd: float,      # رصيد المحفظة
            gas_fee_usd: float,      # رسوم الغاز المقدرة
            fee_recipient: str | None  # عنوان الرسوم
        }
    """
    # الفحص 1: التحقق من الرصيد
    balance_usd = get_wallet_balance_usd(w3, wallet_address, eth_price_usd)
    if balance_usd < MIN_BALANCE_RESERVE_USD:
        return {
            "pass": False,
            "reason": "balance_too_low",          # الرصيد منخفض جدًا
            "balance_usd": balance_usd,
            "gas_fee_usd": 0,
            "fee_recipient": None,
        }

    # الفحص 2: التحقق من رسوم الغاز
    gas_fee_usd = estimate_gas_fee_usd(w3, eth_price_usd)
    if gas_fee_usd > MAX_GAS_FEE_USD:
        return {
            "pass": False,
            "reason": "gas_too_high",             # رسوم الغاز مرتفعة جدًا
            "balance_usd": balance_usd,
            "gas_fee_usd": gas_fee_usd,
            "fee_recipient": None,
        }

    # الفحص 3: التحقق من عنوان الرسوم
    fee_recipient = get_fee_recipient(w3, seadrop_address, nft_contract)
    if not fee_recipient:
        return {
            "pass": False,
            "reason": "no_fee_recipient",         # لا يوجد عنوان رسوم
            "balance_usd": balance_usd,
            "gas_fee_usd": gas_fee_usd,
            "fee_recipient": None,
        }

    # كل الفحوصات نجحت
    return {
        "pass": True,
        "reason": "",
        "balance_usd": balance_usd,
        "gas_fee_usd": gas_fee_usd,
        "fee_recipient": fee_recipient,
    }


# ===========================================================================
# دوال مساعدة لتحليل أسباب الفشل وإعادة المحاولة
# ===========================================================================

# أسباب الفشل التي تستحق إعادة المحاولة (قابلة للتغير مع الوقت)
RETRYABLE_REASONS = {
    "gas_too_high",           # رسوم الغاز قد تنخفض لاحقًا
    "gas_too_high_precise",   # نفس الشيء بعد التقدير الدقيق
    "tx_error",               # خطأ في الشبكة قد يختفي
    "simulation_failed",      # المحاكاة قد تنجح لاحقًا (لو المينت مازال مغلق)
}


def is_paid_mint(price_wei: int, eth_price_usd: float, threshold_usd: float = 0.01) -> tuple:
    """
    التحقق مما إذا كان المينت مدفوعًا أم مجانيًا.
    
    المعاملات:
        price_wei: int - السعر بـ Wei
        eth_price_usd: float - سعر ETH الحالي بالدولار
        threshold_usd: float - الحد الفاصل بين المجاني والمدفوع (افتراضي 0.01 دولار)
    
    المخرجات:
        tuple: (is_paid: bool, price_usd: float, label: str)
        - is_paid: True إذا كان مدفوعًا، False إذا كان مجانيًا
        - price_usd: السعر بالدولار
        - label: نص وصفي (مدفوع / مجاني)
    """
    if price_wei <= 0:
        return False, 0.0, "مجاني"
    
    price_usd = (price_wei / 1e18) * eth_price_usd
    if price_usd < threshold_usd:
        return False, price_usd, "مجاني (أقل من 1 سنت)"
    
    return True, price_usd, "مدفوع"


def check_eligibility_reason(reason: str) -> dict:
    """
    تحليل سبب الفشل وتحديد:
    - هل المشكلة من المستخدم (غير مؤهل) أم من الشبكة/الغاز (مؤهل لكن الظروف غير مناسبة)
    
    المعاملات:
        reason: str - سبب الفشل من attempt_purchase أو quick_checks
    
    المخرجات:
        dict: {
            eligible: bool,          # هل المستخدم مؤهل للمشاركة؟
            issue_type: str,         # نوع المشكلة (network / wallet / contract / unknown)
            description: str,         # وصف المشكلة للمستخدم
            retryable: bool,         # هل تستحق إعادة المحاولة؟
        }
    """
    
    # مشاكل الشبكة والغاز - المستخدم مؤهل لكن الظروف غير مناسبة
    if reason in ("gas_too_high", "gas_too_high_precise"):
        return {
            "eligible": True,
            "issue_type": "network",
            "description": "رسوم الغاز مرتفعة حاليًا، أنت مؤهل لكن الشبكة مزدحمة",
            "retryable": True,
        }
    
    # مشاكل المحفظة - المستخدم غير مؤهل بسبب نقص الرصيد
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
    
    # مشاكل العقد - المستخدم مؤهل لكن العقد لا يستجيب أو المينت غير متاح
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
    
    # خطأ عام في إرسال المعاملة
    if reason == "tx_error":
        return {
            "eligible": True,
            "issue_type": "network",
            "description": "حدث خطأ أثناء إرسال المعاملة — مشكلة شبكة مؤقتة",
            "retryable": True,
        }
    
    # أسباب غير معروفة
    return {
        "eligible": False,
        "issue_type": "unknown",
        "description": f"سبب غير معروف: {reason}",
        "retryable": False,
    }


# ===========================================================================
# الدالة الرئيسية: تنفذ كل الفحوصات بالترتيب، ثم الشراء إن نجحت كلها
# ===========================================================================
# هذه هي الدالة النهائية التي تقوم بكل شيء:
# 1. تتحقق من الرصيد
# 2. تتحقق من الغاز
# 3. تستعلم عن عنوان الرسوم
# 4. تحدد الكمية
# 5. تبني المعاملة
# 6. تقدر الغاز الفعلي
# 7. تتحقق من التكلفة النهائية
# 8. ترسل المعاملة

def attempt_purchase(
    w3: Web3,
    private_key: str,
    wallet_address: str,
    nft_contract: str,
    seadrop_address: str,
    price_wei_per_token: int,
    max_per_wallet: int | None,
    remaining_supply: int,
    eth_price_usd: float,
) -> dict:
    """
    محاولة شراء واحدة - مع تحقق صارم من السعر
    """
    
    # =============================================================
    # 🔴 التحقق النهائي: السعر يجب أن يكون 0 فقط
    # =============================================================
    if price_wei_per_token != 0:
        log.warning(f"⛔ السعر ليس مجانياً: {price_wei_per_token} wei")
        return {
            "success": False, 
            "reason": "not_free_mint",
            "price_wei": price_wei_per_token,
            "balance_usd": 0,
        }
    
    # تحقق إضافي: السعر بالدولار يجب أن يكون صفراً
    price_usd = (price_wei_per_token / 1e18) * eth_price_usd
    if price_usd > 0.0000000001:  # عملياً صفر
        log.warning(f"⛔ السعر ليس مجانياً: ${price_usd:.10f}")
        return {
            "success": False, 
            "reason": "not_free_mint",
            "price_usd": price_usd,
            "balance_usd": 0,
        }
    
    # =============================================================
    # جلب الرصيد للتسجيل فقط
    # =============================================================
    balance_usd = get_wallet_balance_usd(w3, wallet_address, eth_price_usd)
    log.info(f"💰 رصيد المحفظة قبل الشراء: ${balance_usd:.4f}")
    
    # =============================================================
    # تحديد الكمية
    # =============================================================
    quantity = decide_quantity(max_per_wallet, remaining_supply)
    total_value = price_wei_per_token * quantity
    
    # =============================================================
    # فحص قيمة المعاملة (للأمان)
    # =============================================================
    total_eth = total_value / 1e18
    if total_eth > MAX_ETH_PER_TX:
        log.warning(f"[أمان] ⛔ قيمة المعاملة {total_eth:.6f} ETH > الحد الأقصى {MAX_ETH_PER_TX} ETH")
        return {"success": False, "reason": "tx_value_too_high", "tx_value_eth": total_eth}

    # =============================================================
    # بناء وإرسال المعاملة
    # =============================================================
    wallet_lock = get_wallet_lock(wallet_address)
    wallet_lock.acquire()
    signed_tx = None
    try:
        contract = w3.eth.contract(address=Web3.to_checksum_address(seadrop_address), abi=SEADROP_ABI)
        current_nonce = w3.eth.get_transaction_count(Web3.to_checksum_address(wallet_address), "pending")
        current_gas_price = w3.eth.gas_price
        
        # جلب عنوان الرسوم
        fee_recipient = get_fee_recipient(w3, seadrop_address, nft_contract)
        if not fee_recipient:
            log.warning(f"⚠️ لا يوجد عنوان رسوم، نحاول باستخدام عنوان افتراضي")
            fee_recipient = seadrop_address
        
        # بناء المعاملة
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

        # تقدير الغاز
        try:
            estimated_gas = w3.eth.estimate_gas(tx)
            tx["gas"] = int(estimated_gas * GAS_LIMIT_SAFETY_MARGIN)
            log.info(f"⛽ الغاز المقدر: {estimated_gas}, مع الهامش: {tx['gas']}")
        except Exception as e:
            log.error(f"⚠️ فشل estimate_gas: {e}")
            return {"success": False, "reason": "simulation_failed", "error": str(e)}

        # التوقيع والإرسال
        signed_tx = w3.eth.account.sign_transaction(tx, private_key=private_key)
        raw_tx = getattr(signed_tx, 'raw_transaction', None)
        if raw_tx is None:
            raw_tx = getattr(signed_tx, 'rawTransaction', None)
        if raw_tx is None:
            raise ValueError("raw_transaction غير موجود")
            
        tx_hash = w3.eth.send_raw_transaction(raw_tx)
        
        # حساب رسوم الغاز الفعلية
        gas_used = tx.get("gas", 0)
        gas_fee_eth = (gas_used * current_gas_price) / 1e18
        gas_fee_usd = gas_fee_eth * eth_price_usd

        log.info(f"✅ [شراء ناجح] {tx_hash.hex()} — كمية: {quantity}")
        return {
            "success": True,
            "tx_hash": tx_hash.hex(),
            "quantity": quantity,
            "gas_fee_usd": gas_fee_usd,
            "total_value_wei": total_value,
            "balance_usd": balance_usd,
        }

    except Exception as e:
        error_msg = str(e).lower()
        log.error(f"❌ [خطأ إرسال] {e}")
        
        # تحليل سبب الخطأ
        if "insufficient funds" in error_msg:
            reason = "insufficient_funds_for_total_cost"
            log.warning(f"⚠️ رصيد غير كافٍ: ${balance_usd:.4f}")
        elif "gas" in error_msg:
            reason = "gas_too_high"
        else:
            reason = "tx_error"
        
        result = {"success": False, "reason": reason, "error": str(e), "balance_usd": balance_usd}
        if signed_tx is not None:
            try:
                result["tx_hash"] = signed_tx.hash.hex()
            except Exception:
                pass
        return result
    finally:
        wallet_lock.release()

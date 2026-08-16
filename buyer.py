"""
محرك الشراء التلقائي عبر عقد SeaDrop — يدعم أكتر من شبكة (Robinhood + Ethereum).
جميع الضوابط الأمنية مركزة هنا بدالة واحدة، مع استخراج رسوم الغاز الفعلية من الشبكة.
"""

import logging
from web3 import Web3

log = logging.getLogger("buyer")

SEADROP_ADDRESS = Web3.to_checksum_address("0x00005EA00Ac477B1030CE78506496e8C2dE24bf5")
ZERO_ADDRESS = Web3.to_checksum_address("0x0000000000000000000000000000000000000000")

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
    {
        "inputs": [{"name": "nftContract", "type": "address"}],
        "name": "getPublicDrop",
        "outputs": [{
            "components": [
                {"name": "mintPrice", "type": "uint80"},
                {"name": "startTime", "type": "uint48"},
                {"name": "endTime", "type": "uint48"},
                {"name": "maxTotalMintableByWallet", "type": "uint16"},
                {"name": "feeBps", "type": "uint16"},
                {"name": "restrictFeeRecipients", "type": "bool"},
            ],
            "name": "",
            "type": "tuple",
        }],
        "stateMutability": "view",
        "type": "function",
    },
]

MIN_BALANCE_RESERVE_USD = 0.10
FEW_THRESHOLD = 20
LIMITED_BUY_QTY = 5
GAS_LIMIT_SAFETY_MARGIN = 1.2   # هامش أمان 20% زيادة عن الحد المقدّر


def get_web3(rpc_url: str) -> Web3:
    """إنشاء كائن Web3 من رابط RPC."""
    return Web3(Web3.HTTPProvider(rpc_url))


def get_wallet_balance_usd(w3: Web3, wallet_address: str, eth_price_usd: float) -> float:
    """الحصول على رصيد المحفظة بالدولار."""
    try:
        checksum_wallet = Web3.to_checksum_address(wallet_address)
        balance_wei = w3.eth.get_balance(checksum_wallet)
        return (balance_wei / 1e18) * eth_price_usd
    except Exception as e:
        log.error(f"[الرصيد] تعذر القراءة: {e}")
        return 0.0


def get_fee_recipient(w3: Web3, nft_contract: str) -> str | None:
    """استرجاع أول عنوان مسموح لاستلام رسوم المينت."""
    try:
        seadrop = w3.eth.contract(address=SEADROP_ADDRESS, abi=SEADROP_ABI)
        recipients = seadrop.functions.getAllowedFeeRecipients(
            Web3.to_checksum_address(nft_contract)
        ).call()
        if not recipients:
            log.warning(f"[عنوان الرسوم] لا يوجد عنوان مسموح لـ {nft_contract}")
            return None
        return Web3.to_checksum_address(recipients[0])
    except Exception as e:
        log.error(f"[عنوان الرسوم] خطأ استعلام: {e}")
        return None


def decide_quantity(max_per_wallet: int | None, remaining_supply: int) -> int:
    """تحديد الكمية المناسبة للشراء بناءً على الحد الأقصى لكل محفظة والكمية المتبقية."""
    if max_per_wallet is None:
        qty = 1
    elif max_per_wallet <= FEW_THRESHOLD:
        qty = max_per_wallet
    else:
        qty = LIMITED_BUY_QTY
    return max(1, min(qty, remaining_supply))


def get_onchain_public_price_wei(w3: Web3, nft_contract: str) -> int | None:
    """استخراج سعر المينت من العقد مباشرة (بـ Wei)."""
    try:
        seadrop = w3.eth.contract(address=SEADROP_ADDRESS, abi=SEADROP_ABI)
        public_drop = seadrop.functions.getPublicDrop(
            Web3.to_checksum_address(nft_contract)
        ).call()
        return int(public_drop[0])
    except Exception as e:
        log.warning(f"[سعر on-chain] تعذر القراءة، سنعتمد بيانات OpenSea: {e}")
        return None


def attempt_purchase(
    w3: Web3,
    private_key: str,
    wallet_address: str,
    nft_contract: str,
    price_wei_per_token: int,
    max_per_wallet: int | None,
    remaining_supply: int,
    eth_price_usd: float,
    max_gas_fee_usd: float,   # سيتم استخدامه للمقارنة بالحد الأقصى (0.05$)
) -> dict:
    """
    محاولة شراء كمية من المينت.
    - تستخرج رسوم الغاز الفعلية من الشبكة عبر estimate_gas.
    - ترفض الشراء إذا تجاوزت رسوم الغاز الحد الأقصى المحدد.
    - تتحقق من كفاية الرصيد لتغطية السعر + الغاز.
    """
    try:
        checksum_wallet = Web3.to_checksum_address(wallet_address)
        checksum_contract = Web3.to_checksum_address(nft_contract)
    except Exception as e:
        log.error(f"[العنوان] تنسيق غير صالح: {e}")
        return {"success": False, "reason": "invalid_address", "error": str(e)}

    # التحقق من الرصيد الأدنى
    balance_usd = get_wallet_balance_usd(w3, checksum_wallet, eth_price_usd)
    if balance_usd < MIN_BALANCE_RESERVE_USD:
        log.warning(f"[توقف] الرصيد ${balance_usd:.4f} أقل من الحد ${MIN_BALANCE_RESERVE_USD}.")
        return {"success": False, "reason": "balance_too_low", "balance_usd": balance_usd}

    # الحصول على عنوان الرسوم
    fee_recipient = get_fee_recipient(w3, checksum_contract)
    if not fee_recipient:
        return {"success": False, "reason": "no_fee_recipient"}

    # تحديد الكمية
    quantity = decide_quantity(max_per_wallet, remaining_supply)
    total_value = price_wei_per_token * quantity

    try:
        contract = w3.eth.contract(address=SEADROP_ADDRESS, abi=SEADROP_ABI)
        nonce = w3.eth.get_transaction_count(checksum_wallet, "pending")

        # بناء المعاملة
        tx = contract.functions.mintPublic(
            checksum_contract,
            Web3.to_checksum_address(fee_recipient),
            ZERO_ADDRESS,
            quantity,
        ).build_transaction({
            "from": checksum_wallet,
            "value": total_value,
            "nonce": nonce,
            "chainId": w3.eth.chain_id,
        })

        # ---- تقدير الغاز الفعلي من الشبكة ----
        try:
            estimated_gas = w3.eth.estimate_gas(tx)
            tx["gas"] = int(estimated_gas * GAS_LIMIT_SAFETY_MARGIN)
        except Exception as e:
            log.error(f"[إلغاء] فشل estimate_gas — المعاملة على الأغلب رح ترفض: {e}")
            return {"success": False, "reason": "simulation_failed", "error": str(e)}

        # حساب رسوم الغاز الفعلية (باستخدام القيمة المقدّرة مع هامش الأمان)
        gas_price_wei = w3.eth.gas_price
        actual_gas_fee_wei = tx["gas"] * gas_price_wei
        actual_gas_fee_usd = (actual_gas_fee_wei / 1e18) * eth_price_usd

        # التحقق من أن رسوم الغاز لا تتجاوز الحد الأقصى (مثلاً 0.05$)
        if actual_gas_fee_usd > max_gas_fee_usd:
            log.info(f"[تأجيل] رسوم الغاز الفعلية ${actual_gas_fee_usd:.4f} > الحد ${max_gas_fee_usd:.2f}.")
            return {"success": False, "reason": "gas_too_high", "gas_fee_usd": actual_gas_fee_usd}

        # التحقق من كفاية الرصيد لتغطية السعر + الغاز
        total_cost_wei = total_value + actual_gas_fee_wei
        wallet_balance_wei = w3.eth.get_balance(checksum_wallet)
        if wallet_balance_wei < total_cost_wei:
            log.warning("[إلغاء] الرصيد لا يكفي لتغطية سعر المينت + الغاز معًا.")
            return {"success": False, "reason": "insufficient_funds_for_total_cost"}

        # توقيع وإرسال المعاملة
        signed = w3.eth.account.sign_transaction(tx, private_key=private_key)
        tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)

        log.info(f"[شراء ناجح] {tx_hash.hex()} — كمية: {quantity}")
        return {
            "success": True,
            "tx_hash": tx_hash.hex(),
            "quantity": quantity,
            "gas_fee_usd": actual_gas_fee_usd,
            "total_value_wei": total_value,
        }

    except Exception as e:
        log.error(f"[خطأ إرسال] {e}")
        return {"success": False, "reason": "tx_error", "error": str(e)}

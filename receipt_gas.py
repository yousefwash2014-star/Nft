"""Receipt-based actual gas accounting.

Call after a transaction hash is broadcast and mined.
Do not report an actual fee before a receipt exists.
"""

async def wait_and_calculate_actual_gas(w3, tx_hash, network, native_price_usd=None, timeout=180):
    receipt = await w3.eth.wait_for_transaction_receipt(tx_hash, timeout=timeout)
    if not receipt:
        return None

    def get(obj, key, default=None):
        if isinstance(obj, dict):
            return obj.get(key, default)
        return getattr(obj, key, default)

    gas_used = int(get(receipt, "gasUsed", 0) or 0)
    effective = get(receipt, "effectiveGasPrice", None)
    if effective is None:
        effective = get(receipt, "gasPrice", 0)
    effective = int(effective or 0)

    cost_wei = gas_used * effective
    cost_native = cost_wei / 10**18

    n = str(network or "").lower()
    symbol = "USDC" if "arc" in n else "ETH"

    usd = None
    if native_price_usd is not None:
        try:
            usd = cost_native * float(native_price_usd)
        except Exception:
            pass

    return {
        "receipt": receipt,
        "gas_used": gas_used,
        "effective_gas_price": effective,
        "gas_cost_wei": cost_wei,
        "gas_cost_native": cost_native,
        "gas_symbol": symbol,
        "gas_fee_usd": usd,
        "gas_fee_actual": True,
    }

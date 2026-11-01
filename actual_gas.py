"""Actual transaction gas calculator.

Use this only after the transaction receipt is available.
Supports both the current call style and the historical compatibility style:

    calculate_actual_gas(receipt)
    calculate_actual_gas(receipt, native_decimals=18)
    calculate_actual_gas(receipt, chain_name, native_price_usd)
"""


def _get(obj, key, default=None):
    if isinstance(obj, dict):
        return obj.get(key, default)
    return getattr(obj, key, default)


def calculate_actual_gas(receipt, native_decimals_or_chain=18, native_price_usd=None):
    """Return receipt gas cost with backward-compatible USD calculation.

    The second positional argument historically contained a chain name while
    the newer API uses it for native token decimals.  A string therefore keeps
    the historical meaning and defaults decimals to 18.
    """
    if isinstance(native_decimals_or_chain, str):
        native_decimals = 18
    else:
        native_decimals = int(native_decimals_or_chain or 18)

    gas_used = int(_get(receipt, "gasUsed", 0) or 0)
    effective_price = _get(receipt, "effectiveGasPrice", None)
    if effective_price is None:
        effective_price = _get(receipt, "gasPrice", 0)
    effective_price = int(effective_price or 0)
    cost_wei = gas_used * effective_price
    cost_native = cost_wei / (10 ** native_decimals)
    result = {
        "gas_used": gas_used,
        "effective_gas_price": effective_price,
        "gas_cost_wei": cost_wei,
        "gas_cost_native": cost_native,
    }
    if native_price_usd is not None:
        result["gas_fee_actual"] = cost_native * float(native_price_usd)
    else:
        result["gas_fee_actual"] = cost_native
    return result

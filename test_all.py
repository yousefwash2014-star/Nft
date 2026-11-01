"""اختبار شامل — FAST×2 → SAFE + On-Chain Ownership"""
import ast
import sys
from pathlib import Path

print("=" * 70)
print("🧪 اختبار النظام — FAST×2 → SAFE")
print("=" * 70)

root = Path(__file__).parent
errors = []
warnings = []


# ─── 1. Syntax ───
print("\n[1/8] 🔍 فحص صياغة Python...")
FILES = [
    "main.py", "buyer.py", "config.py", "opensea_mint.py",
    "ownership.py", "mint_history.py", "actual_gas.py", "twitter_checker.py",
]
for fname in FILES:
    fpath = root / fname
    if not fpath.exists():
        errors.append(f"❌ {fname} غير موجود")
        continue
    try:
        ast.parse(fpath.read_text(encoding="utf-8"), filename=fname)
        print(f"   ✅ {fname}")
    except SyntaxError as e:
        errors.append(f"❌ {fname}: {e}")

if errors:
    for e in errors:
        print(f"   {e}")
    sys.exit(1)


# ─── 2. Imports ───
print("\n[2/8] 🔍 فحص الاستيرادات...")
sys.path.insert(0, str(root))
for mod in ["config", "actual_gas", "opensea_mint", "ownership",
            "mint_history", "buyer", "twitter_checker"]:
    try:
        __import__(mod)
        print(f"   ✅ {mod}.py")
    except Exception as e:
        errors.append(f"❌ {mod}: {e}")


# ─── 3. STRICT_FREE_ONLY ───
print("\n[3/8] 🔒 فحص صرامة المجانية...")
try:
    import buyer
    if getattr(buyer, "STRICT_FREE_ONLY", False) is True:
        print(f"   ✅ STRICT_FREE_ONLY = True")
    else:
        errors.append("❌ STRICT_FREE_ONLY ليس True")

    if buyer.is_free_or_negligible(0, 3000.0) is True:
        print(f"   ✅ value=0 → مجاني")
    else:
        errors.append("❌ value=0 يجب أن يكون مجاني")

    if buyer.is_free_or_negligible(1, 3000.0) is False:
        print(f"   ✅ value>0 → مرفوض")
    else:
        errors.append("❌ value>0 يجب أن يُرفض")
except Exception as e:
    errors.append(f"❌ فحص STRICT: {e}")


# ─── 4. FAST Pipeline ───
print("\n[4/8] ⚡ فحص FAST Pipeline...")
try:
    import buyer
    fast_funcs = [
        "fast_pipeline",
        "_get_wallet_balance_usd_fast",
        "_estimate_gas_fee_usd_fast",
        "_get_fee_recipient_fast",
        "_decide_quantity_fast",
        "_build_drop_mint_transaction",
        "_attempt_purchase_drop_api",
        "_attempt_purchase_seadrop_fast",
        "_attempt_purchase_universal_fast",
    ]
    for fn in fast_funcs:
        if hasattr(buyer, fn):
            print(f"   ✅ buyer.{fn}")
        else:
            errors.append(f"❌ buyer.{fn} مفقود")

    # ثوابت
    if buyer.FEW_THRESHOLD == 20:
        print(f"   ✅ FEW_THRESHOLD = 20")
    else:
        errors.append(f"❌ FEW_THRESHOLD != 20")

    if buyer.LIMITED_BUY_QTY == 59:
        print(f"   ✅ LIMITED_BUY_QTY = 59")
    else:
        errors.append(f"❌ LIMITED_BUY_QTY != 59")

    if buyer.MAX_ETH_PER_TX == 0.02:
        print(f"   ✅ MAX_ETH_PER_TX = 0.02")
    else:
        errors.append(f"❌ MAX_ETH_PER_TX != 0.02")

except Exception as e:
    errors.append(f"❌ فحص FAST: {e}")


# ─── 5. On-Chain Ownership ───
print("\n[5/8] 🔗 فحص On-Chain Ownership...")
try:
    import ownership
    required = [
        "wallet_owns_collection",
        "invalidate_ownership_cache",
        "get_ownership_stats",
        "_check_onchain_sync",
        "_get_balance_onchain",
    ]
    for fn in required:
        if hasattr(ownership, fn):
            print(f"   ✅ ownership.{fn}")
        else:
            errors.append(f"❌ ownership.{fn} مفقود")

    src = (root / "ownership.py").read_text(encoding="utf-8")
    if "api.opensea.io" in src:
        warnings.append("⚠️ ownership.py يحتوي على OpenSea API")
    else:
        print(f"   ✅ لا يعتمد على OpenSea API")
except Exception as e:
    errors.append(f"❌ فحص Ownership: {e}")


# ─── 6. Chains ───
print("\n[6/8] 🌐 فحص السلاسل...")
try:
    import config
    expected = {
        "robinhood": 4663, "ethereum": 1, "ink": 57073,
        "arc": 5042, "polygon": 137,
    }
    for chain, cid in expected.items():
        actual = config._ALL_CHAIN_CONFIG.get(chain, {}).get("chain_id")
        if actual != cid:
            errors.append(f"❌ {chain}: chain_id={actual} != {cid}")
        else:
            print(f"   ✅ {chain}: {cid}")
except Exception as e:
    errors.append(f"❌ فحص السلاسل: {e}")


# ─── 7. main.py ───
print("\n[7/8] 🔧 فحص main.py...")
try:
    import main
    checks = [
        ("phase1_fast", "Phase 1 FAST"),
        ("phase2_safe", "Phase 2 SAFE"),
        ("process_wallet", "Process wallet"),
        ("_record_success", "Record success"),
        ("drop_has_free_stage", "Free stage check"),
        ("FINAL_REASONS", "Final reasons set"),
        ("wallet_owns_collection", "Ownership import"),
        ("get_twitter_username_from_opensea_async", "Twitter check"),
        ("is_free", "is_free helper"),
        ("_phase_emoji", "Phase emoji"),
        ("_phase_label", "Phase label"),
    ]
    for attr, desc in checks:
        if hasattr(main, attr):
            print(f"   ✅ {desc}")
        else:
            warnings.append(f"⚠️ {desc} مفقود ({attr})")

    if hasattr(main, "FINAL_REASONS") and "already_minted" in main.FINAL_REASONS:
        print(f"   ✅ FINAL_REASONS يحتوي already_minted")
    else:
        errors.append("❌ FINAL_REASONS ناقص")
except Exception as e:
    errors.append(f"❌ فحص main: {e}")


# ─── 8. Runtime ───
print("\n[8/8] 🧪 اختبارات runtime...")

try:
    from actual_gas import calculate_actual_gas
    g = calculate_actual_gas(
        {"gasUsed": 21000, "effectiveGasPrice": 20_000_000_000},
        "ethereum", 3000,
    )
    assert g and g["gas_fee_actual"] and g["gas_used"] == 21000
    print(f"   ✅ actual_gas")
except Exception as e:
    errors.append(f"❌ actual_gas: {e}")

try:
    import tempfile, os
    from mint_history import MintHistory
    with tempfile.TemporaryDirectory() as d:
        h = MintHistory(os.path.join(d, "h.json"))
        assert not h.has("ethereum", "test", "0xABC")
        h.record("ethereum", "test", "0xABC", "0xTX", 100, 1)
        h2 = MintHistory(os.path.join(d, "h.json"))
        assert h2.count() == 1
    print(f"   ✅ mint_history")
except Exception as e:
    errors.append(f"❌ mint_history: {e}")

try:
    from opensea_mint import MintBuildResult, _classify
    r = MintBuildResult(False, "not_eligible", status=422)
    assert not r.ok
    assert _classify(422) == "not_eligible"
    print(f"   ✅ opensea_mint")
except Exception as e:
    errors.append(f"❌ opensea_mint: {e}")

try:
    import ownership, asyncio
    from unittest.mock import MagicMock, patch

    with patch("ownership._check_onchain_sync", return_value=3):
        asyncio.run(ownership.invalidate_ownership_cache())
        owns, checked = asyncio.run(ownership.wallet_owns_collection(
            session=None, wallet="0xTEST1", slug="test",
            api_key="key", contract_address="0xCONTRACT",
            w3=MagicMock(), chain="ethereum",
        ))
        assert owns is True and checked is True
        print(f"   ✅ on-chain owns=True")

    with patch("ownership._check_onchain_sync", return_value=None):
        asyncio.run(ownership.invalidate_ownership_cache())
        owns, checked = asyncio.run(ownership.wallet_owns_collection(
            session=None, wallet="0xTEST3", slug="test",
            api_key="key", contract_address="0xCONTRACT",
            w3=MagicMock(), chain="ethereum",
        ))
        assert owns is False and checked is False
        print(f"   ✅ on-chain fail-closed")
except Exception as e:
    errors.append(f"❌ on-chain runtime: {e}")

try:
    import buyer
    # fast_pipeline بدون drop_info → لا يرفع استثناء
    with patch.object(buyer, "get_drop_info", return_value=None), \
         patch.object(buyer, "_get_wallet_balance_usd_fast", return_value=10.0), \
         patch.object(buyer, "_estimate_gas_fee_usd_fast", return_value=0.01):
        res = buyer.fast_pipeline(
            w3=MagicMock(),
            private_key="0x" + "a" * 64,
            wallet_address="0x" + "b" * 40,
            nft_contract="0x" + "c" * 40,
            seadrop_address="0x" + "d" * 40,
            price_wei=0, max_per_wallet=None,
            remaining_supply=100, eth_price_usd=3000.0,
            allow_universal=False, slug="", opensea_api_key="",
        )
        assert isinstance(res, dict)
    print(f"   ✅ fast_pipeline (mocked)")
except Exception as e:
    errors.append(f"❌ fast_pipeline: {e}")


# ─── Results ───
print("\n" + "=" * 70)
if errors:
    print(f"❌ فشل — {len(errors)} خطأ:")
    for e in errors:
        print(f"   {e}")
    sys.exit(1)

if warnings:
    print(f"⚠️ نجح مع {len(warnings)} تحذير:")
    for w in warnings:
        print(f"   {w}")

print("=" * 70)
print("✅ جميع الاختبارات نجحت — FAST×2 → SAFE جاهز!")
print("=" * 70)
sys.exit(0)
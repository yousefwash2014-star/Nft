"""اختبارات تحقق من إعدادات السرعة والـ FAST/SAFE."""
from pathlib import Path
import ast

ROOT = Path(__file__).parent
MAIN = (ROOT / "main.py").read_text(encoding="utf-8")
BUYER = (ROOT / "buyer.py").read_text(encoding="utf-8")
ENV = (ROOT / ".env.example").read_text(encoding="utf-8")


def test_syntax():
    for name in ("main.py", "config.py", "opensea_mint.py", "buyer.py"):
        ast.parse((ROOT / name).read_text(encoding="utf-8"))


def test_fast_pipeline_in_buyer():
    assert "def execute_purchase_pipeline" in BUYER
    assert "attempt_purchase_seadrop" in BUYER
    assert "attempt_purchase_universal" in BUYER


def test_fast_constants_in_buyer():
    assert "FEW_THRESHOLD = 20" in BUYER
    assert "LIMITED_BUY_QTY = 50" in BUYER
    assert "MAX_ETH_PER_TX = 0" in BUYER
    assert "BALANCE_CACHE_TTL = 30" in BUYER


def test_phase1_fast_in_main():
    assert "async def evaluate_and_execute_mint" in MAIN
    assert "execute_purchase_pipeline" in MAIN


def test_phase2_safe_in_main():
    assert "execute_wallet_task" in MAIN
    assert "mint_history.has" in MAIN


def test_final_reasons_in_main():
    assert "RETRYABLE_REASONS" in BUYER
    assert "already_minted" in BUYER
    assert "not_started" in BUYER


def test_embedded_safe_defaults():
    assert "ENABLE_UNIVERSAL_MINTS = False" in MAIN
    assert "CHECK_TWITTER = True" in MAIN
    assert "MAX_RETRY_ATTEMPTS = 0" in MAIN
    assert "PENDING_TX_MONITOR = True" in MAIN
    assert "RETRYABLE_REASONS = set()" in BUYER
    assert "_simulate_transaction_before_broadcast" in BUYER


def test_rpc_rotation_in_buyer():
    assert "_rpc_limiter_fast" in BUYER
    assert "_rpc_limiter_fast" in BUYER


def test_opensea_limiter_used():
    assert "OpenSeaRateLimiter" in MAIN


if __name__ == "__main__":
    failed = 0
    passed = 0
    for name, fn in sorted(globals().items()):
        if not name.startswith("test_"):
            continue
        try:
            fn()
            passed += 1
            print(f"✅ {name}")
        except Exception as e:
            failed += 1
            print(f"❌ {name}: {e}")
    print()
    print(f"✅ نجح: {passed} | ❌ فشل: {failed}")
    import sys
    sys.exit(1 if failed else 0)
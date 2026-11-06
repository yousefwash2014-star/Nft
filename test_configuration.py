from pathlib import Path
import ast

root = Path(__file__).parent
main = (root / "main.py").read_text()
buyer = (root / "buyer.py").read_text()
env = (root / ".env.example").read_text()

for source_name, source in (("main.py", main), ("buyer.py", buyer)):
    ast.parse(source, filename=source_name)

assert '"ethereum"' in buyer and '"robinhood"' in buyer and '"base"' not in buyer and '"arbitrum"' not in buyer
assert '"arc"' in buyer and '"ink"' in buyer
assert '"link"' not in buyer.split("CHAINS_CONFIG =", 1)[1].split("}", 1)[0]
assert '"chain_id": 1' in buyer
assert '"chain_id": 4663' in buyer
assert '"chain_id": 5042' in buyer
assert '"chain_id": 57073' in buyer
assert "ETHEREUM_RPC_URLS" in env and "BASE_RPC_URLS" not in env and "ARBITRUM_RPC_URLS" not in env and "ARC_RPC_URLS" in env and "OPENSEA_API_KEYS" in env
assert "STREAM_URL_BASE" in main and "get_next_opensea_key" in main
assert "chainlink" not in main.lower()
assert "e12d2314|edc01273" in buyer
assert "get_wallet_mint_stats" in buyer
assert "REQUIRE_OWNERSHIP_CHECK" in buyer
assert "get_wallet_ownership_balance" in buyer
print("configuration checks passed")
assert "schedule_evaluation" in main
# التشغيل والأداء ثوابت داخل الكود؛ ملف البيئة يحتوي الأسرار والروابط فقط.
assert "PERFORMANCE_PROFILE=turbo" not in env
assert "OPENSEA_API_KEYS" in env and "PRIVATE_KEYS" in env and "ETHEREUM_RPC_URLS" in env

from stage_policy import stage_rejection_reason
assert stage_rejection_reason({"name": "Public Stage", "type": "public", "allowlist": {"id": "old"}}) is None
assert stage_rejection_reason({"name": "Whitelist", "type": "allowlist"}) is not None
assert stage_rejection_reason({"name": "Public Stage", "eligibility": {"status": "eligible"}, "allowlist": {"id": "old"}}) is None

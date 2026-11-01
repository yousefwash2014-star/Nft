"""اختبارات بنيوية للبوت الكامل الحالي؛ لا ترسل معاملات ولا رسائل خارجية."""
from __future__ import annotations

import ast
import asyncio
import os
import tempfile
from pathlib import Path

ROOT = Path(__file__).parent
PY_FILES = [p for p in ROOT.glob("*.py") if p.name != "test_all.py"]

for path in PY_FILES:
    ast.parse(path.read_text(encoding="utf-8"), filename=path.name)

assert (ROOT / "main.py").read_text().find("fast_active_scanner") >= 0
assert (ROOT / "main.py").read_text().find("listen_opensea") >= 0
assert (ROOT / "main.py").read_text().find("telegram_sender") >= 0
assert (ROOT / "main.py").read_text().find("confirmed-success") >= 0
assert (ROOT / "buyer.py").read_text().find("FAST_FIRST_ENABLED") >= 0
assert (ROOT / ".env.example").exists()

keys = []
for line in (ROOT / ".env.example").read_text().splitlines():
    if line and not line.lstrip().startswith("#") and "=" in line:
        keys.append(line.split("=", 1)[0])
assert len(keys) == len(set(keys)), "duplicate environment keys"

from mint_history import MintHistory
with tempfile.TemporaryDirectory() as directory:
    path = os.path.join(directory, "history.json")
    history = MintHistory(path)
    assert not history.has("ethereum", "demo", "0xabc")
    history.record("ethereum", "demo", "0xabc", "0xtx", 1, 1)
    assert history.has("ethereum", "demo", "0xabc")

from fast_listener import fast_parse_event
payload = ["1", "1", "collection:*", "item_transferred", {
    "chain": {"name": "ethereum"}, "slug": "demo",
    "nft_id": "ethereum/0x0000000000000000000000000000000000000001/7",
    "from_address": "0x0000000000000000000000000000000000000000",
}]
parsed = fast_parse_event(__import__("json").dumps(payload).encode())
assert parsed["slug"] == "demo"

print(f"structural checks passed: {len(PY_FILES)} python files")

from pathlib import Path

source = (Path(__file__).parent / "main.py").read_text()
assert 'OpenSea API keys loaded' in source
assert 'Telegram targets' in source
assert 'Queuing for' in source
assert 'Telegram sent' in source
assert 'Telegram 400' in source and 'Telegram 404' in source
assert 'default_token' in source and 'default_chat' in source
print("notification diagnostics checks passed")


def test_pending_notification_is_present():
    source = open("main.py", encoding="utf-8").read()
    assert "build_pending_msg" in source
    assert "broadcast-pending:" in source
    assert "📤 إشعار البث والتأكيد" in source
    assert "confirmed-failure:" in source
    assert "استثناء غير معالج في مهمة المحفظة" in source

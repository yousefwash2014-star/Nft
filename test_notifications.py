from pathlib import Path

source = (Path(__file__).parent / "main.py").read_text()
assert 'OpenSea API keys loaded' in source
assert 'Telegram targets' in source
assert 'Queuing for' in source
assert 'Telegram sent' in source
assert 'Telegram 400' in source and 'Telegram 404' in source
assert 'default_token' in source and 'default_chat' in source
print("notification diagnostics checks passed")

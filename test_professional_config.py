from key_pool import ApiKeyPool
from runtime_config import NETWORK_ORDER, validate_network_configs


configs = {key: {"chain_id": cid} for key, cid in {
    "ethereum": 1,
    "robinhood": 4663,
    "ink": 57073,
    "arc": 5042,
    "elysium": 1339,
}.items()}
rpcs = {key: [f"https://{key}.example/rpc"] for key in NETWORK_ORDER}
assert validate_network_configs(configs, rpcs) == []
assert validate_network_configs({"robinhood": {"chain_id": 1}}, {"robinhood": ["bad-url"]})

pool = ApiKeyPool(["key-a", "key-b", "key-a"])
assert pool.keys == ("key-a", "key-b")
first = pool.next_key()
pool.mark_rate_limited(first, 60)
assert pool.available_count() == 1
assert pool.next_key() != first
print("professional configuration checks passed")


def test_public_stage_filter():
    from stage_policy import is_public_eligible_stage, stage_rejection_reason
    assert is_public_eligible_stage({"name": "Public stage", "price": "0"})
    assert not is_public_eligible_stage({"name": "Treasure Allowlist", "price": "0"})
    assert not is_public_eligible_stage({"name": "Private", "eligible": False, "price": "0"})
    assert stage_rejection_reason({"name": "Treasure Allowlist", "price": "0"}) == "restricted=allowlist"


test_public_stage_filter()

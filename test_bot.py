import asyncio
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from fast_listener import fast_parse_event, PreSignedPool, MultiWSListener


def test_parser():
    payload = ["1", "1", "collection:*", "item_transferred", {
        "chain": {"name": "ethereum"},
        "slug": "demo-collection",
        "nft_id": "ethereum/0x0000000000000000000000000000000000000001/7",
        "from_address": "0x0000000000000000000000000000000000000000",
    }]
    parsed = fast_parse_event(json.dumps(payload).encode())
    assert parsed == {"chain": "ethereum", "slug": "demo-collection",
                      "contract": "0x0000000000000000000000000000000000000001"}
    assert fast_parse_event(b'{"name":"ethereum","slug":"x"}') is None


def test_pool():
    pool = PreSignedPool()
    pool.prepare("slug", "0xAbC", "0xdead")
    assert pool.get("slug", "0xabc") == "0xdead"
    assert pool.size("slug") == 1
    pool.clear("slug")
    assert pool.size("slug") == 0


def test_bounded_dispatch():
    async def run():
        active = 0
        peak = 0
        seen = 0
        async def callback(_event):
            nonlocal active, peak, seen
            active += 1
            peak = max(peak, active)
            seen += 1
            await asyncio.sleep(0.01)
            active -= 1
        listener = MultiWSListener("ws://invalid", callback, workers=1, max_inflight_events=2)
        listener._event_sem = asyncio.Semaphore(2)
        for i in range(5):
            await listener._event_sem.acquire()
            listener._start_dispatch({"i": i}, 0)
        await asyncio.gather(*list(listener._active_tasks))
        assert seen == 5
        assert peak <= 2
    asyncio.run(run())


if __name__ == "__main__":
    test_parser()
    test_pool()
    test_bounded_dispatch()
    print("ok")

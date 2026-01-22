import json

import pytest

from order_service.app.services import idempotency as module


class DummyRedis:
    def __init__(self):
        self.store = {}

    async def get(self, key):
        return self.store.get(key)

    async def setex(self, key, ttl, value):
        self.store[key] = value

    async def delete(self, *keys):
        deleted = 0
        for k in keys:
            if k in self.store:
                deleted += 1
                self.store.pop(k)
        return deleted

    async def keys(self, pattern):
        # simplistic pattern support
        return list(self.store.keys())

    async def close(self):
        pass


@pytest.fixture()
def service(monkeypatch):
    svc = module.IdempotencyService(redis_url="redis://localhost", ttl_hours=1)
    dummy = DummyRedis()
    monkeypatch.setattr(module.aioredis, "from_url", lambda *a, **k: dummy)
    # Force connect to set _redis_client
    return svc


@pytest.mark.asyncio
async def test_fingerprint_stable(service):
    fp1 = service._fingerprint({"b": 2, "a": 1})
    fp2 = service._fingerprint({"a": 1, "b": 2})
    assert fp1 == fp2


@pytest.mark.asyncio
async def test_check_and_store_new_and_duplicate(monkeypatch, service):
    await service.connect()
    payload = {"order": "buy"}

    # First request should return None (not cached)
    cached = await service.check_and_store("key1", user_id=1, request_data=payload)
    assert cached is None

    # Store response
    response = {"status": "ok"}
    await service.store_response("key1", user_id=1, request_data=payload, response_data=response)

    cached_again = await service.check_and_store("key1", user_id=1, request_data=payload)
    assert cached_again == response


@pytest.mark.asyncio
async def test_idempotency_key_conflict_raises(monkeypatch, service):
    await service.connect()
    await service._redis_client.setex(
        service._get_key("key2", 1),
        service.ttl_seconds,
        json.dumps({"request_fingerprint": service._fingerprint({"a": 1}), "response": {"ok": True}})
    )

    with pytest.raises(module.HTTPException):
        await service.check_and_store("key2", user_id=1, request_data={"a": 2})


@pytest.mark.asyncio
async def test_get_stats_reports_counts(monkeypatch, service):
    await service.connect()
    # Set some keys
    await service._redis_client.setex("idempotency:user:1:key:abc", 10, "{}")
    await service._redis_client.setex("idempotency:user:1:key:abc:pending", 10, "{}")

    stats = await service.get_stats()
    assert stats["total_keys"] == 2
    assert stats["pending_requests"] == 1

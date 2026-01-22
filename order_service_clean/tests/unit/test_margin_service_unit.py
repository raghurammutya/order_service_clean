"""
Comprehensive unit tests for Margin Service.

Tests cover:
1. Polling interval logic (weekend, active orders, positions, market status)
2. Cache operations (hit, miss, expiry, corruption)
3. Redis failures (connection, timeout, client lifecycle)
4. Broker API failures (timeout, connection, errors)
5. Error handling and masking issues
"""
import json
import pytest
from fastapi import HTTPException

from order_service.app.services.margin_service import MarginService


class DummyRedis:
    """Mock Redis client for testing cache operations."""

    def __init__(self, should_fail=False, failure_type="connection", corrupt_data=False):
        self.store = {}
        self.should_fail = should_fail
        self.failure_type = failure_type
        self.corrupt_data = corrupt_data
        self.closed = False
        self.setex_calls = []
        self.get_calls = []

    async def setex(self, key, ttl, value):
        self.setex_calls.append({"key": key, "ttl": ttl, "value": value})
        if self.should_fail:
            if self.failure_type == "timeout":
                raise TimeoutError("Redis timeout")
            elif self.failure_type == "connection":
                raise ConnectionError("Redis connection failed")
            else:
                raise RuntimeError("Redis error")
        self.store[key] = value
        return True

    async def get(self, key):
        self.get_calls.append(key)
        if self.should_fail:
            if self.failure_type == "timeout":
                raise TimeoutError("Redis timeout")
            elif self.failure_type == "connection":
                raise ConnectionError("Redis connection failed")

        if self.corrupt_data and key in self.store:
            return "invalid json {"  # Corrupted JSON

        return self.store.get(key)

    async def close(self):
        self.closed = True


class DummyKite:
    """Mock Kite client for testing broker interactions."""

    def __init__(self, margins=None, should_fail=False, failure_type="runtime"):
        self.margins = margins or {"equity": {"net": 1000, "enabled": True}}
        self.should_fail = should_fail
        self.failure_type = failure_type
        self.get_margins_calls = []

    async def get_margins(self, segment=None):
        self.get_margins_calls.append({"segment": segment})
        if self.should_fail:
            if self.failure_type == "timeout":
                raise TimeoutError("Broker API timeout")
            elif self.failure_type == "connection":
                raise ConnectionError("Broker connection failed")
            else:
                raise RuntimeError("Broker API error")
        return self.margins


class DummyDB:
    """Mock database session for testing."""

    def __init__(self, has_active_orders=False, has_open_positions=False):
        self.has_active_orders = has_active_orders
        self.has_open_positions = has_open_positions

    async def execute(self, *args, **kwargs):
        # Return mock query result based on configuration
        if self.has_active_orders:
            return type("Result", (), {
                "scalars": lambda self: type("S", (), {
                    "first": lambda self: type("Order", (), {})()
                })()
            })()
        elif self.has_open_positions:
            return type("Result", (), {
                "scalars": lambda self: type("S", (), {
                    "first": lambda self: type("Position", (), {})()
                })()
            })()
        else:
            # No active orders/positions
            return type("Result", (), {
                "scalars": lambda self: type("S", (), {
                    "first": lambda self: None
                })()
            })()


# ==========================================
# POLLING INTERVAL TESTS
# ==========================================

@pytest.mark.asyncio
async def test_get_polling_interval_weekend(monkeypatch):
    """Test polling interval returns 0 on weekends."""
    svc = MarginService(db=DummyDB(), user_id=1, trading_account_id=1)
    # Mock datetime to return Saturday (weekday=5)
    mock_datetime = type("DateTime", (), {
        "now": staticmethod(lambda: type("D", (), {"weekday": lambda self: 5})())
    })
    monkeypatch.setattr("order_service.app.services.margin_service.datetime", mock_datetime)

    interval = await svc.get_polling_interval()
    assert interval == svc.INTERVAL_WEEKEND  # 0 seconds


@pytest.mark.asyncio
async def test_get_polling_interval_active_orders(monkeypatch):
    """Test polling interval when active orders exist."""
    db = DummyDB(has_active_orders=True)
    svc = MarginService(db=db, user_id=1, trading_account_id=1)

    # Mock datetime to return weekday
    mock_datetime = type("DateTime", (), {
        "now": staticmethod(lambda: type("D", (), {"weekday": lambda self: 2})())  # Wednesday
    })
    monkeypatch.setattr("order_service.app.services.margin_service.datetime", mock_datetime)

    interval = await svc.get_polling_interval()
    assert interval == svc.INTERVAL_ACTIVE_ORDERS  # 30 seconds


@pytest.mark.asyncio
async def test_get_polling_interval_open_positions_market_hours(monkeypatch):
    """Test polling interval when positions are open during market hours."""
    db = DummyDB(has_open_positions=True)
    svc = MarginService(db=db, user_id=1, trading_account_id=1)

    # Mock market hours to return True
    monkeypatch.setattr("order_service.app.services.margin_service.MarketHoursService.is_market_open", lambda segment: True)

    # Mock datetime to return weekday
    mock_datetime = type("DateTime", (), {
        "now": staticmethod(lambda: type("D", (), {"weekday": lambda self: 2})())
    })
    monkeypatch.setattr("order_service.app.services.margin_service.datetime", mock_datetime)

    interval = await svc.get_polling_interval()
    assert interval == svc.INTERVAL_OPEN_POSITIONS  # 60 seconds


@pytest.mark.asyncio
async def test_get_polling_interval_market_idle(monkeypatch):
    """Test polling interval when market is open but no activity."""
    db = DummyDB()  # No active orders/positions
    svc = MarginService(db=db, user_id=1, trading_account_id=1)

    # Mock market hours to return True
    monkeypatch.setattr("order_service.app.services.margin_service.MarketHoursService.is_market_open", lambda segment: True)

    # Mock datetime to return weekday
    mock_datetime = type("DateTime", (), {
        "now": staticmethod(lambda: type("D", (), {"weekday": lambda self: 2})())
    })
    monkeypatch.setattr("order_service.app.services.margin_service.datetime", mock_datetime)

    interval = await svc.get_polling_interval()
    assert interval == svc.INTERVAL_MARKET_IDLE  # 300 seconds


@pytest.mark.asyncio
async def test_get_polling_interval_after_hours(monkeypatch):
    """Test polling interval when market is closed."""
    db = DummyDB()
    svc = MarginService(db=db, user_id=1, trading_account_id=1)

    # Mock market hours to return False
    monkeypatch.setattr("order_service.app.services.margin_service.MarketHoursService.is_market_open", lambda segment: False)

    # Mock datetime to return weekday
    mock_datetime = type("DateTime", (), {
        "now": staticmethod(lambda: type("D", (), {"weekday": lambda self: 2})())
    })
    monkeypatch.setattr("order_service.app.services.margin_service.datetime", mock_datetime)

    interval = await svc.get_polling_interval()
    assert interval == svc.INTERVAL_AFTER_HOURS  # 1800 seconds


# ==========================================
# CACHE OPERATIONS TESTS
# ==========================================

@pytest.mark.asyncio
async def test_fetch_and_cache_margins_success(monkeypatch):
    """Test successful margin fetch and caching."""
    svc = MarginService(db=DummyDB(), user_id=1, trading_account_id=1)
    redis = DummyRedis()
    kite = DummyKite({"equity": {"net": 5000, "enabled": True}})

    monkeypatch.setattr("order_service.app.services.margin_service.aioredis",
                       type("A", (), {"from_url": lambda *a, **k: redis})())
    monkeypatch.setattr("order_service.app.services.margin_service.get_kite_client_for_account", lambda acc: kite)

    margins = await svc.fetch_and_cache_margins(segment=None)

    assert margins["equity"]["net"] == 5000
    assert len(redis.setex_calls) == 1
    assert redis.setex_calls[0]["ttl"] == svc.CACHE_TTL
    assert len(kite.get_margins_calls) == 1


@pytest.mark.asyncio
async def test_get_cached_margins_hit(monkeypatch):
    """Test cache hit scenario."""
    svc = MarginService(db=DummyDB(), user_id=1, trading_account_id=1)
    redis = DummyRedis()

    # Pre-populate cache
    cache_data = {"equity": {"net": 3000}}
    redis.store["margins:user:1:segment:all"] = json.dumps(cache_data)

    monkeypatch.setattr("order_service.app.services.margin_service.aioredis",
                       type("A", (), {"from_url": lambda *a, **k: redis})())

    cached = await svc.get_cached_margins(segment=None)

    assert cached is not None
    assert cached["equity"]["net"] == 3000
    assert len(redis.get_calls) == 1


@pytest.mark.asyncio
async def test_get_cached_margins_miss(monkeypatch):
    """Test cache miss scenario."""
    svc = MarginService(db=DummyDB(), user_id=1, trading_account_id=1)
    redis = DummyRedis()

    monkeypatch.setattr("order_service.app.services.margin_service.aioredis",
                       type("A", (), {"from_url": lambda *a, **k: redis})())

    cached = await svc.get_cached_margins(segment=None)

    assert cached is None
    assert len(redis.get_calls) == 1


@pytest.mark.asyncio
async def test_get_margins_uses_cache_when_available(monkeypatch):
    """Test get_margins returns cached data without broker call."""
    svc = MarginService(db=DummyDB(), user_id=1, trading_account_id=1)
    redis = DummyRedis()
    kite = DummyKite()

    # Pre-populate cache
    cache_data = {"equity": {"net": 7000}}
    redis.store["margins:user:1:segment:all"] = json.dumps(cache_data)

    monkeypatch.setattr("order_service.app.services.margin_service.aioredis",
                       type("A", (), {"from_url": lambda *a, **k: redis})())
    monkeypatch.setattr("order_service.app.services.margin_service.get_kite_client_for_account", lambda acc: kite)

    margins = await svc.get_margins()

    assert margins["equity"]["net"] == 7000
    assert len(kite.get_margins_calls) == 0  # Should NOT call broker


@pytest.mark.asyncio
async def test_get_margins_force_refresh_bypasses_cache(monkeypatch):
    """Test force_refresh bypasses cache and calls broker."""
    svc = MarginService(db=DummyDB(), user_id=1, trading_account_id=1)
    redis = DummyRedis()
    kite = DummyKite({"equity": {"net": 9000}})

    # Pre-populate cache with different value
    cache_data = {"equity": {"net": 1000}}
    redis.store["margins:user:1:segment:all"] = json.dumps(cache_data)

    monkeypatch.setattr("order_service.app.services.margin_service.aioredis",
                       type("A", (), {"from_url": lambda *a, **k: redis})())
    monkeypatch.setattr("order_service.app.services.margin_service.get_kite_client_for_account", lambda acc: kite)

    margins = await svc.get_margins(force_refresh=True)

    assert margins["equity"]["net"] == 9000  # Broker value, not cache
    assert len(kite.get_margins_calls) == 1  # SHOULD call broker


# ==========================================
# REDIS FAILURE TESTS
# ==========================================

@pytest.mark.asyncio
async def test_fetch_margins_redis_connection_failure(monkeypatch):
    """Test margin fetch handles Redis connection failures."""
    svc = MarginService(db=DummyDB(), user_id=1, trading_account_id=1)
    redis = DummyRedis(should_fail=True, failure_type="connection")
    kite = DummyKite({"equity": {"net": 5000}})

    monkeypatch.setattr("order_service.app.services.margin_service.aioredis",
                       type("A", (), {"from_url": lambda *a, **k: redis})())
    monkeypatch.setattr("order_service.app.services.margin_service.get_kite_client_for_account", lambda acc: kite)

    with pytest.raises(HTTPException) as exc_info:
        await svc.fetch_and_cache_margins()

    assert exc_info.value.status_code == 500
    # BUG: Generic error message masks Redis-specific failure
    assert "Failed to fetch margins" in str(exc_info.value.detail)


@pytest.mark.asyncio
async def test_fetch_margins_redis_timeout(monkeypatch):
    """Test margin fetch handles Redis timeout errors."""
    svc = MarginService(db=DummyDB(), user_id=1, trading_account_id=1)
    redis = DummyRedis(should_fail=True, failure_type="timeout")
    kite = DummyKite({"equity": {"net": 5000}})

    monkeypatch.setattr("order_service.app.services.margin_service.aioredis",
                       type("A", (), {"from_url": lambda *a, **k: redis})())
    monkeypatch.setattr("order_service.app.services.margin_service.get_kite_client_for_account", lambda acc: kite)

    with pytest.raises(HTTPException) as exc_info:
        await svc.fetch_and_cache_margins()

    assert exc_info.value.status_code == 500


@pytest.mark.asyncio
async def test_get_cached_margins_redis_failure_returns_none(monkeypatch):
    """Test get_cached_margins returns None on Redis failure (silent fail-open)."""
    svc = MarginService(db=DummyDB(), user_id=1, trading_account_id=1)
    redis = DummyRedis(should_fail=True, failure_type="connection")

    monkeypatch.setattr("order_service.app.services.margin_service.aioredis",
                       type("A", (), {"from_url": lambda *a, **k: redis})())

    # BUG: Returns None instead of raising exception
    cached = await svc.get_cached_margins()

    assert cached is None  # Silent failure


@pytest.mark.asyncio
async def test_get_cached_margins_corrupted_json_returns_none(monkeypatch):
    """Test get_cached_margins handles corrupted cache data."""
    svc = MarginService(db=DummyDB(), user_id=1, trading_account_id=1)
    redis = DummyRedis(corrupt_data=True)

    # Add corrupted data to cache
    redis.store["margins:user:1:segment:all"] = "invalid json {"

    monkeypatch.setattr("order_service.app.services.margin_service.aioredis",
                       type("A", (), {"from_url": lambda *a, **k: redis})())

    # BUG: Returns None on JSON decode error without indication
    cached = await svc.get_cached_margins()

    assert cached is None


# ==========================================
# BROKER FAILURE TESTS
# ==========================================

@pytest.mark.asyncio
async def test_fetch_margins_broker_timeout(monkeypatch):
    """Test margin fetch handles broker API timeout."""
    svc = MarginService(db=DummyDB(), user_id=1, trading_account_id=1)
    redis = DummyRedis()
    kite = DummyKite(should_fail=True, failure_type="timeout")

    monkeypatch.setattr("order_service.app.services.margin_service.aioredis",
                       type("A", (), {"from_url": lambda *a, **k: redis})())
    monkeypatch.setattr("order_service.app.services.margin_service.get_kite_client_for_account", lambda acc: kite)

    with pytest.raises(HTTPException) as exc_info:
        await svc.fetch_and_cache_margins()

    assert exc_info.value.status_code == 500
    assert "Failed to fetch margins" in str(exc_info.value.detail)


@pytest.mark.asyncio
async def test_fetch_margins_broker_connection_error(monkeypatch):
    """Test margin fetch handles broker connection failures."""
    svc = MarginService(db=DummyDB(), user_id=1, trading_account_id=1)
    redis = DummyRedis()
    kite = DummyKite(should_fail=True, failure_type="connection")

    monkeypatch.setattr("order_service.app.services.margin_service.aioredis",
                       type("A", (), {"from_url": lambda *a, **k: redis})())
    monkeypatch.setattr("order_service.app.services.margin_service.get_kite_client_for_account", lambda acc: kite)

    with pytest.raises(HTTPException) as exc_info:
        await svc.fetch_and_cache_margins()

    assert exc_info.value.status_code == 500


# ==========================================
# REDIS CLIENT LIFECYCLE TESTS
# ==========================================

@pytest.mark.asyncio
async def test_redis_client_reused_across_calls(monkeypatch):
    """Test Redis client is reused, not recreated for each call."""
    svc = MarginService(db=DummyDB(), user_id=1, trading_account_id=1)

    redis_instances = []
    original_from_url = None

    def mock_from_url(*args, **kwargs):
        redis = DummyRedis()
        redis_instances.append(redis)
        return redis

    monkeypatch.setattr("order_service.app.services.margin_service.aioredis",
                       type("A", (), {"from_url": mock_from_url})())

    # Make multiple calls
    await svc._get_redis_client()
    await svc._get_redis_client()
    await svc._get_redis_client()

    # Should only create ONE Redis client
    assert len(redis_instances) == 1


@pytest.mark.asyncio
async def test_redis_client_close(monkeypatch):
    """Test Redis client is properly closed."""
    svc = MarginService(db=DummyDB(), user_id=1, trading_account_id=1)
    redis = DummyRedis()

    monkeypatch.setattr("order_service.app.services.margin_service.aioredis",
                       type("A", (), {"from_url": lambda *a, **k: redis})())

    await svc._get_redis_client()
    await svc.close()

    assert redis.closed is True

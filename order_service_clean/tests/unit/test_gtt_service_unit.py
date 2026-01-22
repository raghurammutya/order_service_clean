"""
Comprehensive unit tests for GTT Service.

Tests cover:
1. GTT creation validation (type, trigger values, orders)
2. Broker failure handling (API errors, timeouts, connection failures)
3. Database transaction handling (commit/rollback)
4. Edge cases and error scenarios
"""
import pytest
from datetime import datetime
from fastapi import HTTPException

from order_service.app.services.gtt_service import GttService
from order_service.app.models.gtt_order import GttOrder


class DummyDB:
    """Mock database session for testing."""

    def __init__(self, should_fail_commit=False):
        self.added = []
        self.commits = 0
        self.rollbacks = 0
        self.refreshed = []
        self.should_fail_commit = should_fail_commit

    async def execute(self, stmt):
        return type("R", (), {"scalar_one_or_none": lambda self: None})()

    def add(self, obj):
        self.added.append(obj)

    async def commit(self):
        if self.should_fail_commit:
            raise RuntimeError("Database commit failed")
        self.commits += 1

    async def rollback(self):
        self.rollbacks += 1

    async def refresh(self, obj):
        self.refreshed.append(obj)


class DummyKite:
    """Mock Kite client for testing broker interactions."""

    def __init__(self, should_fail=False, failure_type="runtime", broker_gtt_id="broker_gtt_1"):
        self.should_fail = should_fail
        self.failure_type = failure_type
        self.broker_gtt_id = broker_gtt_id
        self.place_gtt_calls = []

    async def place_gtt(self, **kwargs):
        self.place_gtt_calls.append(kwargs)
        if self.should_fail:
            if self.failure_type == "timeout":
                raise TimeoutError("Broker API timeout")
            elif self.failure_type == "connection":
                raise ConnectionError("Failed to connect to broker")
            elif self.failure_type == "runtime":
                raise RuntimeError("Broker API error")
            elif self.failure_type == "http":
                from kiteconnect.exceptions import NetworkException
                raise NetworkException("Network error", code=503)
        return self.broker_gtt_id


# ==========================================
# GTT CREATION SUCCESS TESTS
# ==========================================

@pytest.mark.asyncio
async def test_place_gtt_order_success_single_leg(monkeypatch):
    """Test successful single-leg GTT creation."""
    db = DummyDB()
    monkeypatch.setattr("order_service.app.services.gtt_service.get_kite_client_for_account", lambda acc: DummyKite())
    svc = GttService(db=db, user_id=1, trading_account_id=1)

    result = await svc.place_gtt_order(
        gtt_type="single",
        symbol="RELIANCE",
        exchange="NSE",
        tradingsymbol="RELIANCE",
        trigger_values=[2500.0],
        last_price=2450.0,
        orders=[{"transaction_type": "BUY", "quantity": 10, "price": 2500.0}],
    )

    assert result["broker_gtt_id"] == "broker_gtt_1"
    assert result["gtt_type"] == "single"
    assert result["symbol"] == "RELIANCE"
    assert db.commits == 1
    assert db.rollbacks == 0
    assert len(db.added) == 1
    assert isinstance(db.added[0], GttOrder)


@pytest.mark.asyncio
async def test_place_gtt_order_success_two_leg(monkeypatch):
    """Test successful two-leg (OCO) GTT creation."""
    db = DummyDB()
    monkeypatch.setattr("order_service.app.services.gtt_service.get_kite_client_for_account", lambda acc: DummyKite())
    svc = GttService(db=db, user_id=1, trading_account_id=1)

    result = await svc.place_gtt_order(
        gtt_type="two-leg",
        symbol="NIFTY",
        exchange="NSE",
        tradingsymbol="NIFTY",
        trigger_values=[18000.0, 17500.0],  # Target and stop-loss
        last_price=17800.0,
        orders=[
            {"transaction_type": "SELL", "quantity": 50, "price": 18000.0},
            {"transaction_type": "SELL", "quantity": 50, "price": 17500.0}
        ],
    )

    assert result["broker_gtt_id"] == "broker_gtt_1"
    assert result["gtt_type"] == "two-leg"
    assert len(result["condition"]["trigger_values"]) == 2
    assert db.commits == 1


@pytest.mark.asyncio
async def test_place_gtt_order_with_optional_fields(monkeypatch):
    """Test GTT creation with optional fields (expires_at, user_tag, user_notes)."""
    db = DummyDB()
    monkeypatch.setattr("order_service.app.services.gtt_service.get_kite_client_for_account", lambda acc: DummyKite())
    svc = GttService(db=db, user_id=1, trading_account_id=1)

    expires_at = datetime(2025, 12, 31, 23, 59, 59)
    result = await svc.place_gtt_order(
        gtt_type="single",
        symbol="TCS",
        exchange="NSE",
        tradingsymbol="TCS",
        trigger_values=[3500.0],
        last_price=3450.0,
        orders=[{"transaction_type": "BUY", "quantity": 5}],
        expires_at=expires_at,
        user_tag="swing_trade",
        user_notes="Buy TCS on breakout"
    )

    assert result["user_tag"] == "swing_trade"
    assert result["user_notes"] == "Buy TCS on breakout"
    # Note: expires_at comparison would need proper datetime handling
    assert db.commits == 1


# ==========================================
# GTT VALIDATION TESTS
# ==========================================

@pytest.mark.asyncio
async def test_place_gtt_order_invalid_gtt_type(monkeypatch):
    """Test GTT creation fails with invalid GTT type."""
    db = DummyDB()
    monkeypatch.setattr("order_service.app.services.gtt_service.get_kite_client_for_account", lambda acc: DummyKite())
    svc = GttService(db=db, user_id=1, trading_account_id=1)

    with pytest.raises(HTTPException) as exc_info:
        await svc.place_gtt_order(
            gtt_type="invalid",
            symbol="SYM",
            exchange="NSE",
            tradingsymbol="SYM",
            trigger_values=[100.0],
            last_price=99.0,
            orders=[{"price": 100, "quantity": 1}],
        )

    assert exc_info.value.status_code == 400
    assert "Invalid GTT type" in str(exc_info.value.detail)
    assert db.rollbacks == 1


@pytest.mark.asyncio
async def test_place_gtt_order_single_wrong_trigger_count(monkeypatch):
    """Test single-leg GTT validation fails with wrong number of trigger values."""
    db = DummyDB()
    monkeypatch.setattr("order_service.app.services.gtt_service.get_kite_client_for_account", lambda acc: DummyKite())
    svc = GttService(db=db, user_id=1, trading_account_id=1)

    with pytest.raises(HTTPException) as exc_info:
        await svc.place_gtt_order(
            gtt_type="single",
            symbol="SYM",
            exchange="NSE",
            tradingsymbol="SYM",
            trigger_values=[100.0, 101.0],  # Should be 1, not 2
            last_price=99.0,
            orders=[{"price": 100, "quantity": 1}],
        )

    assert exc_info.value.status_code == 400
    assert "exactly 1 trigger value" in str(exc_info.value.detail)
    assert db.rollbacks == 1


@pytest.mark.asyncio
async def test_place_gtt_order_two_leg_wrong_trigger_count(monkeypatch):
    """Test two-leg GTT validation fails with wrong number of trigger values."""
    db = DummyDB()
    monkeypatch.setattr("order_service.app.services.gtt_service.get_kite_client_for_account", lambda acc: DummyKite())
    svc = GttService(db=db, user_id=1, trading_account_id=1)

    with pytest.raises(HTTPException) as exc_info:
        await svc.place_gtt_order(
            gtt_type="two-leg",
            symbol="SYM",
            exchange="NSE",
            tradingsymbol="SYM",
            trigger_values=[100.0],  # Should be 2, not 1
            last_price=99.0,
            orders=[{"price": 100, "quantity": 1}],
        )

    assert exc_info.value.status_code == 400
    assert "exactly 2 trigger values" in str(exc_info.value.detail)
    assert db.rollbacks == 1


@pytest.mark.asyncio
async def test_place_gtt_order_empty_orders(monkeypatch):
    """Test GTT creation fails when orders list is empty."""
    db = DummyDB()
    monkeypatch.setattr("order_service.app.services.gtt_service.get_kite_client_for_account", lambda acc: DummyKite())
    svc = GttService(db=db, user_id=1, trading_account_id=1)

    with pytest.raises(HTTPException) as exc_info:
        await svc.place_gtt_order(
            gtt_type="single",
            symbol="SYM",
            exchange="NSE",
            tradingsymbol="SYM",
            trigger_values=[100.0],
            last_price=99.0,
            orders=[],  # Empty orders list
        )

    assert exc_info.value.status_code == 400
    assert "At least one order is required" in str(exc_info.value.detail)
    assert db.rollbacks == 1


# ==========================================
# BROKER FAILURE TESTS
# ==========================================

@pytest.mark.asyncio
async def test_place_gtt_order_broker_runtime_error(monkeypatch):
    """Test GTT creation handles broker API runtime errors."""
    db = DummyDB()
    monkeypatch.setattr("order_service.app.services.gtt_service.get_kite_client_for_account",
                       lambda acc: DummyKite(should_fail=True, failure_type="runtime"))
    svc = GttService(db=db, user_id=1, trading_account_id=1)

    with pytest.raises(HTTPException) as exc_info:
        await svc.place_gtt_order(
            gtt_type="single",
            symbol="SYM",
            exchange="NSE",
            tradingsymbol="SYM",
            trigger_values=[100.0],
            last_price=99.0,
            orders=[{"price": 100, "quantity": 1}],
        )

    assert exc_info.value.status_code == 500
    assert "Failed to place GTT order" in str(exc_info.value.detail)
    assert db.rollbacks == 1
    assert db.commits == 0


@pytest.mark.asyncio
async def test_place_gtt_order_broker_timeout(monkeypatch):
    """Test GTT creation handles broker API timeout errors."""
    db = DummyDB()
    monkeypatch.setattr("order_service.app.services.gtt_service.get_kite_client_for_account",
                       lambda acc: DummyKite(should_fail=True, failure_type="timeout"))
    svc = GttService(db=db, user_id=1, trading_account_id=1)

    with pytest.raises(HTTPException) as exc_info:
        await svc.place_gtt_order(
            gtt_type="single",
            symbol="SYM",
            exchange="NSE",
            tradingsymbol="SYM",
            trigger_values=[100.0],
            last_price=99.0,
            orders=[{"price": 100, "quantity": 1}],
        )

    assert exc_info.value.status_code == 500
    assert db.rollbacks == 1
    assert db.commits == 0


@pytest.mark.asyncio
async def test_place_gtt_order_broker_connection_error(monkeypatch):
    """Test GTT creation handles broker connection failures."""
    db = DummyDB()
    monkeypatch.setattr("order_service.app.services.gtt_service.get_kite_client_for_account",
                       lambda acc: DummyKite(should_fail=True, failure_type="connection"))
    svc = GttService(db=db, user_id=1, trading_account_id=1)

    with pytest.raises(HTTPException) as exc_info:
        await svc.place_gtt_order(
            gtt_type="single",
            symbol="SYM",
            exchange="NSE",
            tradingsymbol="SYM",
            trigger_values=[100.0],
            last_price=99.0,
            orders=[{"price": 100, "quantity": 1}],
        )

    assert exc_info.value.status_code == 500
    assert db.rollbacks == 1


# ==========================================
# DATABASE FAILURE TESTS
# ==========================================

@pytest.mark.asyncio
async def test_place_gtt_order_database_commit_failure(monkeypatch):
    """Test GTT creation handles database commit failures."""
    db = DummyDB(should_fail_commit=True)
    monkeypatch.setattr("order_service.app.services.gtt_service.get_kite_client_for_account", lambda acc: DummyKite())
    svc = GttService(db=db, user_id=1, trading_account_id=1)

    with pytest.raises(HTTPException) as exc_info:
        await svc.place_gtt_order(
            gtt_type="single",
            symbol="SYM",
            exchange="NSE",
            tradingsymbol="SYM",
            trigger_values=[100.0],
            last_price=99.0,
            orders=[{"price": 100, "quantity": 1}],
        )

    assert exc_info.value.status_code == 500
    assert db.rollbacks == 1


@pytest.mark.asyncio
async def test_place_gtt_order_ensures_rollback_on_any_exception(monkeypatch):
    """Test GTT creation always rolls back database on any exception."""
    db = DummyDB()
    kite = DummyKite(should_fail=True, failure_type="runtime")
    monkeypatch.setattr("order_service.app.services.gtt_service.get_kite_client_for_account", lambda acc: kite)
    svc = GttService(db=db, user_id=1, trading_account_id=1)

    # Initial state
    assert db.rollbacks == 0
    assert db.commits == 0

    with pytest.raises(HTTPException):
        await svc.place_gtt_order(
            gtt_type="single",
            symbol="SYM",
            exchange="NSE",
            tradingsymbol="SYM",
            trigger_values=[100.0],
            last_price=99.0,
            orders=[{"price": 100, "quantity": 1}],
        )

    # Verify rollback was called
    assert db.rollbacks == 1
    assert db.commits == 0
    # Verify no database record was persisted
    assert len(db.added) == 1  # Object was added but not committed


# ==========================================
# BROKER PARAMETERS VALIDATION TESTS
# ==========================================

@pytest.mark.asyncio
async def test_place_gtt_order_passes_correct_params_to_broker(monkeypatch):
    """Test GTT creation passes correct parameters to broker API."""
    db = DummyDB()
    kite = DummyKite()
    monkeypatch.setattr("order_service.app.services.gtt_service.get_kite_client_for_account", lambda acc: kite)
    svc = GttService(db=db, user_id=1, trading_account_id=1)

    await svc.place_gtt_order(
        gtt_type="single",
        symbol="INFY",
        exchange="NSE",
        tradingsymbol="INFY",
        trigger_values=[1500.0],
        last_price=1450.0,
        orders=[{"transaction_type": "BUY", "quantity": 20, "price": 1500.0}],
    )

    # Verify broker was called with correct parameters
    assert len(kite.place_gtt_calls) == 1
    call_args = kite.place_gtt_calls[0]
    assert call_args["gtt_type"] == "single"
    assert call_args["symbol"] == "INFY"
    assert call_args["exchange"] == "NSE"
    assert call_args["trigger_values"] == [1500.0]
    assert call_args["last_price"] == 1450.0
    assert len(call_args["orders"]) == 1

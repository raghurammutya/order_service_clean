import asyncio
from datetime import date
import pytest

from order_service.app.services.trade_service import TradeService
from order_service.app.models.trade import Trade


class DummyResult:
    def __init__(self, obj=None):
        self._obj = obj

    def scalar_one_or_none(self):
        return self._obj


class DummyDB:
    def __init__(self):
        self.added = []
        self.commits = 0
        self.executed = []

    async def execute(self, stmt):
        self.executed.append(stmt)
        # Return empty result to simulate no existing trade
        return DummyResult(None)

    def add(self, obj):
        self.added.append(obj)

    async def commit(self):
        self.commits += 1


class DummyKite:
    def __init__(self, trades):
        self.trades = trades

    async def get_trades(self):
        return self.trades


@pytest.mark.asyncio
async def test_sync_trades_creates_and_commits(monkeypatch):
    db = DummyDB()
    broker_trades = [
        {
            "trade_id": "t1",
            "order_id": "o1",
            "symbol": "ABC",
            "exchange": "NSE",
            "transaction_type": "BUY",
            "product": "CNC",
            "quantity": 10,
            "average_price": 100,
            "fill_timestamp": None,
            "exchange_timestamp": "2024-01-01T00:00:00Z",
        }
    ]
    kite = DummyKite(broker_trades)
    monkeypatch.setattr("order_service.app.services.trade_service.get_kite_client_for_account", lambda acc: kite)
    monkeypatch.setattr("order_service.app.services.trade_service.invalidate_trade_cache", lambda *a, **k: asyncio.sleep(0))

    svc = TradeService(db=db, user_id=1, trading_account_id=1)
    stats = await svc.sync_trades_from_broker()

    assert stats["trades_synced"] == 1
    assert db.added and isinstance(db.added[0], Trade)
    assert db.commits == 1


@pytest.mark.asyncio
async def test_list_trades_filters_and_limits(monkeypatch):
    db = DummyDB()
    svc = TradeService(db=db, user_id=1, trading_account_id=1)

    # Stub execute to return empty list for list_trades
    async def fake_execute(stmt):
        return DummyResult([])

    monkeypatch.setattr(db, "execute", fake_execute)
    trades = await svc.list_trades(symbol="ABC", start_date=date(2024, 1, 1), end_date=date(2024, 1, 2), limit=10, offset=0)
    assert trades == []

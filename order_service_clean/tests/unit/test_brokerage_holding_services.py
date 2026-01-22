import pytest

from order_service.app.services.brokerage_service import BrokerageService
from order_service.app.services.holding_service import HoldingService


class DummyBrokerClient:
    def __init__(self, positions=None, holdings=None):
        self.positions = positions or []
        self.holdings = holdings or []

    async def get_positions(self):
        return self.positions

    async def get_holdings(self):
        return self.holdings


class DummyDB:
    def __init__(self):
        self.added = []
        self.commits = 0

    async def execute(self, *args, **kwargs):
        return type("Result", (), {"fetchall": lambda self: [], "fetchone": lambda self: None})()

    def add(self, obj):
        self.added.append(obj)

    async def commit(self):
        self.commits += 1


@pytest.mark.asyncio
async def test_brokerage_service_sync_positions(monkeypatch):
    db = DummyDB()
    broker = DummyBrokerClient(positions=[{"net": 1, "product": "MIS", "symbol": "ABC"}])

    monkeypatch.setattr("order_service.app.services.brokerage_service.get_kite_client_for_account", lambda acc: broker)

    svc = BrokerageService(db=db, user_id=1, trading_account_id=1)
    stats = await svc.sync_positions()

    assert "positions_synced" in stats
    assert db.commits == 1


@pytest.mark.asyncio
async def test_holding_service_sync(monkeypatch):
    db = DummyDB()
    broker = DummyBrokerClient(holdings=[{"symbol": "ABC", "quantity": 10, "average_price": 100}])
    monkeypatch.setattr("order_service.app.services.holding_service.get_kite_client_for_account", lambda acc: broker)

    svc = HoldingService(db=db, user_id=1, trading_account_id=1)
    stats = await svc.sync_holdings()

    assert "holdings_synced" in stats
    assert db.commits == 1

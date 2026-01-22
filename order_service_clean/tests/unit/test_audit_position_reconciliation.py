import pytest

from order_service.app.services.audit_service import OrderAuditService
from order_service.app.services.position_service import PositionService
from order_service.app.services.reconciliation_service import ReconciliationService
from order_service.app.models.order_state_history import OrderStateHistory


class DummyDB:
    def __init__(self):
        self.added = []
        self.flushes = 0
        self.commits = 0

    async def flush(self):
        self.flushes += 1

    def add(self, obj):
        self.added.append(obj)

    async def execute(self, *args, **kwargs):
        return type("R", (), {"fetchone": lambda self: None, "fetchall": lambda self: []})()

    async def commit(self):
        self.commits += 1


@pytest.mark.asyncio
async def test_order_audit_log_creation():
    db = DummyDB()
    svc = OrderAuditService(db=db, user_id=1)
    record = await svc.log_state_change(order_id=1, old_status="PENDING", new_status="SUBMITTED", reason="test")

    assert isinstance(record, OrderStateHistory)
    assert db.flushes == 1
    assert db.added


@pytest.mark.asyncio
async def test_position_service_sync_positions(monkeypatch):
    db = DummyDB()

    class DummyKite:
        async def get_positions(self):
            return {"net": [{"symbol": "ABC", "exchange": "NSE", "quantity": 1, "average_price": 100}], "day": []}

    monkeypatch.setattr("order_service.app.services.position_service.get_kite_client_for_account", lambda acc: DummyKite())
    monkeypatch.setattr("order_service.app.services.position_service.invalidate_position_cache", lambda *a, **k: None)
    svc = PositionService(db=db, user_id=1, trading_account_id="acc1")
    stats = await svc.sync_positions_from_broker()

    assert "net_positions_synced" in stats
    assert db.commits == 1


@pytest.mark.asyncio
async def test_reconciliation_service_skips_on_missing_orders(monkeypatch):
    db = DummyDB()
    svc = ReconciliationService(db=db, user_id=1, trading_account_id=1)

    # Stub methods used inside reconcile
    async def fake_get_positions(*args, **kwargs):
        return []

    monkeypatch.setattr(svc, "_fetch_latest_orders", lambda: [])
    monkeypatch.setattr(svc, "_fetch_positions", fake_get_positions)
    result = await svc.reconcile_positions()

    assert "reconciled" in result

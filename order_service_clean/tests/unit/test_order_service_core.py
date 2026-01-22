import pytest
from fastapi import HTTPException

from order_service.app.services.order_service import OrderService


class DummyDB:
    async def execute(self, *args, **kwargs):
        return type("R", (), {"scalar_one_or_none": lambda self: None})()

    def add(self, obj):
        pass

    async def commit(self):
        pass

    async def rollback(self):
        pass


class DummyIdempotency:
    async def check_and_store(self, *a, **k):
        return None

    async def store_response(self, *a, **k):
        return None


@pytest.mark.asyncio
async def test_validate_order_payload_rejects_invalid(monkeypatch):
    svc = OrderService(db=DummyDB(), user_id=1, trading_account_id=1)
    svc.idempotency = DummyIdempotency()
    with pytest.raises(HTTPException):
        await svc.validate_order_payload({"quantity": -1, "price": -1})

from datetime import date
from decimal import Decimal

import pytest

from order_service.app.services.pnl_calculator import PnLCalculator


class DummyResult:
    def __init__(self, row):
        self._row = row

    def fetchone(self):
        return self._row


class DummyDB:
    def __init__(self):
        self.queries = []

    async def execute(self, statement, params=None):
        self.queries.append((statement, params))
        # Emulate different result shapes based on query text
        sql = str(statement)
        if "total_realized_pnl" in sql:
            return DummyResult(type("Row", (), {"total_realized_pnl": Decimal("10")})())
        if "total_unrealized_pnl" in sql:
            return DummyResult(type("Row", (), {"total_unrealized_pnl": Decimal("5")})())
        if "total_trades" in sql:
            return DummyResult(type("Row", (), {"total_trades": 3})())
        if "winning_trades" in sql:
            return DummyResult(type("Row", (), {"winning_trades": 2, "losing_trades": 1})())
        if "open_positions" in sql:
            return DummyResult(type("Row", (), {"open_positions": 1, "closed_positions": 2})())
        return DummyResult(None)


@pytest.mark.asyncio
async def test_calculate_realized_and_unrealized_pnl():
    db = DummyDB()
    calc = PnLCalculator(db)

    realized = await calc.calculate_realized_pnl(strategy_id=1, trading_day=date(2024, 1, 1))
    unrealized = await calc.calculate_unrealized_pnl(strategy_id=1, trading_day=date(2024, 1, 1))

    assert realized == Decimal("10")
    assert unrealized == Decimal("5")
    assert len(db.queries) == 2


@pytest.mark.asyncio
async def test_calculate_trade_metrics_and_position_counts():
    db = DummyDB()
    calc = PnLCalculator(db)
    metrics = await calc.calculate_trade_metrics(strategy_id=1, trading_day=date(2024, 1, 1))
    counts = await calc.calculate_position_counts(strategy_id=1, trading_day=date(2024, 1, 1))

    assert metrics == {"total_trades": 3, "winning_trades": 2, "losing_trades": 1}
    assert counts == {"open_positions": 1, "closed_positions": 2}


@pytest.mark.asyncio
async def test_calculate_handles_exceptions(monkeypatch):
    async def failing_execute(*args, **kwargs):
        raise RuntimeError("db down")

    db = DummyDB()
    monkeypatch.setattr(db, "execute", failing_execute)
    calc = PnLCalculator(db)

    assert await calc.calculate_realized_pnl(1) == Decimal("0")
    assert await calc.calculate_unrealized_pnl(1) == Decimal("0")
    assert await calc.calculate_trade_metrics(1) == {"total_trades": 0, "winning_trades": 0, "losing_trades": 0}
    assert await calc.calculate_position_counts(1) == {"open_positions": 0, "closed_positions": 0}

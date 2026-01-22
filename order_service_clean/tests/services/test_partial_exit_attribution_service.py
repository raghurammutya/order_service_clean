"""
Tests for Partial Exit Attribution Service

Tests the FIFO allocation algorithm and partial exit attribution logic.
"""

import pytest
from decimal import Decimal
from datetime import datetime, timezone, timedelta
from unittest.mock import AsyncMock, MagicMock
from sqlalchemy.ext.asyncio import AsyncSession

from order_service.app.services.partial_exit_attribution_service import (
    PartialExitAttributionService,
    AllocationMethod
)


class TestPartialExitAttributionService:
    """Test cases for PartialExitAttributionService."""

    @pytest.fixture
    async def mock_db(self):
        """Mock database session."""
        db = AsyncMock(spec=AsyncSession)
        db.execute = AsyncMock()
        db.commit = AsyncMock()
        db.rollback = AsyncMock()
        return db

    @pytest.fixture
    def service(self, mock_db):
        """Partial exit attribution service instance."""
        return PartialExitAttributionService(mock_db)

    @pytest.fixture
    def sample_positions(self):
        """Sample position data for testing."""
        base_time = datetime.now(timezone.utc) - timedelta(days=10)
        
        return [
            {
                "position_id": "pos_1",
                "symbol": "AAPL",
                "quantity": 100,
                "strategy_id": 1,
                "execution_id": "exec_1",
                "portfolio_id": "port_1",
                "buy_price": Decimal("150.00"),
                "created_at": base_time,
                "entry_trades": [
                    {
                        "trade_id": "trade_1",
                        "quantity": 100,
                        "price": Decimal("150.00"),
                        "timestamp": base_time
                    }
                ]
            },
            {
                "position_id": "pos_2", 
                "symbol": "AAPL",
                "quantity": 50,
                "strategy_id": 2,
                "execution_id": "exec_2",
                "portfolio_id": "port_2",
                "buy_price": Decimal("160.00"),
                "created_at": base_time + timedelta(hours=1),
                "entry_trades": [
                    {
                        "trade_id": "trade_2",
                        "quantity": 50,
                        "price": Decimal("160.00"),
                        "timestamp": base_time + timedelta(hours=1)
                    }
                ]
            },
            {
                "position_id": "pos_3",
                "symbol": "AAPL", 
                "quantity": 75,
                "strategy_id": 1,
                "execution_id": "exec_3",
                "portfolio_id": "port_1",
                "buy_price": Decimal("155.00"),
                "created_at": base_time + timedelta(hours=2),
                "entry_trades": [
                    {
                        "trade_id": "trade_3",
                        "quantity": 75,
                        "price": Decimal("155.00"),
                        "timestamp": base_time + timedelta(hours=2)
                    }
                ]
            }
        ]

    async def test_fifo_allocation_simple(self, service, mock_db, sample_positions):
        """Test simple FIFO allocation with full position allocation."""
        # Mock database calls
        mock_result = MagicMock()
        mock_result.fetchall.return_value = [
            (p["position_id"], p["symbol"], p["quantity"], p["strategy_id"], 
             p["execution_id"], p["portfolio_id"], str(p["buy_price"]), 
             p["created_at"], []) for p in sample_positions
        ]
        mock_db.execute.return_value = mock_result

        # Test allocating 100 shares (should fully allocate first position)
        result = await service.attribute_partial_exit(
            trading_account_id="acc_001",
            symbol="AAPL",
            exit_quantity=Decimal("100"),
            exit_price=Decimal("170.00"),
            exit_timestamp=datetime.now(timezone.utc),
            allocation_method=AllocationMethod.FIFO
        )

        assert len(result.allocations) == 1
        assert result.allocations[0].position_id == "pos_1"
        assert result.allocations[0].allocated_quantity == Decimal("100")
        assert result.total_allocated_quantity == Decimal("100")
        assert result.unallocated_quantity == Decimal("0")
        assert not result.requires_manual_intervention

    async def test_fifo_allocation_partial(self, service, mock_db, sample_positions):
        """Test FIFO allocation requiring multiple positions."""
        # Mock database response
        mock_result = MagicMock()
        mock_result.fetchall.return_value = [
            (p["position_id"], p["symbol"], p["quantity"], p["strategy_id"],
             p["execution_id"], p["portfolio_id"], str(p["buy_price"]),
             p["created_at"], []) for p in sample_positions
        ]
        mock_db.execute.return_value = mock_result

        # Test allocating 175 shares (pos_1: 100 + pos_2: 50 + pos_3: 25)
        result = await service.attribute_partial_exit(
            trading_account_id="acc_001",
            symbol="AAPL", 
            exit_quantity=Decimal("175"),
            exit_price=Decimal("170.00"),
            exit_timestamp=datetime.now(timezone.utc),
            allocation_method=AllocationMethod.FIFO
        )

        assert len(result.allocations) == 3
        
        # Check allocation order and quantities
        assert result.allocations[0].position_id == "pos_1"
        assert result.allocations[0].allocated_quantity == Decimal("100")
        
        assert result.allocations[1].position_id == "pos_2"
        assert result.allocations[1].allocated_quantity == Decimal("50")
        
        assert result.allocations[2].position_id == "pos_3"
        assert result.allocations[2].allocated_quantity == Decimal("25")
        assert result.allocations[2].remaining_quantity == Decimal("50")
        
        assert result.total_allocated_quantity == Decimal("175")
        assert result.unallocated_quantity == Decimal("0")

    async def test_fifo_allocation_insufficient_positions(self, service, mock_db, sample_positions):
        """Test FIFO allocation when exit quantity exceeds available positions."""
        mock_result = MagicMock()
        mock_result.fetchall.return_value = [
            (p["position_id"], p["symbol"], p["quantity"], p["strategy_id"],
             p["execution_id"], p["portfolio_id"], str(p["buy_price"]),
             p["created_at"], []) for p in sample_positions
        ]
        mock_db.execute.return_value = mock_result

        # Test allocating 300 shares (more than total 225)
        result = await service.attribute_partial_exit(
            trading_account_id="acc_001",
            symbol="AAPL",
            exit_quantity=Decimal("300"),
            exit_price=Decimal("170.00"),
            exit_timestamp=datetime.now(timezone.utc),
            allocation_method=AllocationMethod.FIFO
        )

        assert len(result.allocations) == 3  # All positions allocated
        assert result.total_allocated_quantity == Decimal("225")  # Total available
        assert result.unallocated_quantity == Decimal("75")  # Unallocated remainder
        assert result.requires_manual_intervention  # Manual review needed

    async def test_manual_allocation_specific_trades(self, service, mock_db, sample_positions):
        """Test manual allocation to specific trades."""
        mock_result = MagicMock()
        mock_result.fetchall.return_value = [
            (p["position_id"], p["symbol"], p["quantity"], p["strategy_id"],
             p["execution_id"], p["portfolio_id"], str(p["buy_price"]),
             p["created_at"], p["entry_trades"]) for p in sample_positions
        ]
        mock_db.execute.return_value = mock_result

        # Test manual allocation to specific trades
        result = await service.attribute_partial_exit(
            trading_account_id="acc_001",
            symbol="AAPL",
            exit_quantity=Decimal("50"),
            exit_price=Decimal("170.00"),
            exit_timestamp=datetime.now(timezone.utc),
            allocation_method=AllocationMethod.MANUAL,
            specific_trade_ids=["trade_2"]  # Only allocate to trade_2
        )

        assert len(result.allocations) == 1
        assert result.allocations[0].position_id == "pos_2"
        assert result.allocations[0].allocated_quantity == Decimal("50")
        assert result.total_allocated_quantity == Decimal("50")

    async def test_lifo_allocation(self, service, mock_db, sample_positions):
        """Test LIFO (Last-In-First-Out) allocation method."""
        mock_result = MagicMock()
        mock_result.fetchall.return_value = [
            (p["position_id"], p["symbol"], p["quantity"], p["strategy_id"],
             p["execution_id"], p["portfolio_id"], str(p["buy_price"]),
             p["created_at"], []) for p in sample_positions
        ]
        mock_db.execute.return_value = mock_result

        # Test LIFO allocation (should start with newest position)
        result = await service.attribute_partial_exit(
            trading_account_id="acc_001",
            symbol="AAPL",
            exit_quantity=Decimal("100"),
            exit_price=Decimal("170.00"),
            exit_timestamp=datetime.now(timezone.utc),
            allocation_method=AllocationMethod.LIFO
        )

        # Should allocate from pos_3 first (newest), then pos_2
        assert len(result.allocations) == 2
        assert result.allocations[0].position_id == "pos_3"
        assert result.allocations[0].allocated_quantity == Decimal("75")
        assert result.allocations[1].position_id == "pos_2"
        assert result.allocations[1].allocated_quantity == Decimal("25")

    async def test_proportional_allocation(self, service, mock_db, sample_positions):
        """Test proportional allocation method."""
        mock_result = MagicMock()
        mock_result.fetchall.return_value = [
            (p["position_id"], p["symbol"], p["quantity"], p["strategy_id"],
             p["execution_id"], p["portfolio_id"], str(p["buy_price"]),
             p["created_at"], []) for p in sample_positions
        ]
        mock_db.execute.return_value = mock_result

        # Test proportional allocation
        result = await service.attribute_partial_exit(
            trading_account_id="acc_001",
            symbol="AAPL",
            exit_quantity=Decimal("90"),  # 40% of total 225
            exit_price=Decimal("170.00"),
            exit_timestamp=datetime.now(timezone.utc),
            allocation_method=AllocationMethod.PROPORTIONAL
        )

        # Each position should be allocated proportionally
        total_positions = sum(p["quantity"] for p in sample_positions)  # 225
        exit_ratio = Decimal("90") / Decimal("225")  # 0.4
        
        assert len(result.allocations) == 3
        
        # Check proportional allocation (40% of each position)
        expected_allocations = [40, 20, 30]  # 40% of [100, 50, 75]
        for i, expected in enumerate(expected_allocations):
            assert abs(result.allocations[i].allocated_quantity - Decimal(str(expected))) < Decimal("0.01")

    async def test_allocation_audit_trail(self, service, mock_db, sample_positions):
        """Test that allocation creates proper audit trail."""
        mock_result = MagicMock()
        mock_result.fetchall.return_value = [
            (p["position_id"], p["symbol"], p["quantity"], p["strategy_id"],
             p["execution_id"], p["portfolio_id"], str(p["buy_price"]),
             p["created_at"], []) for p in sample_positions
        ]
        mock_db.execute.return_value = mock_result

        result = await service.attribute_partial_exit(
            trading_account_id="acc_001",
            symbol="AAPL",
            exit_quantity=Decimal("100"),
            exit_price=Decimal("170.00"),
            exit_timestamp=datetime.now(timezone.utc),
            allocation_method=AllocationMethod.FIFO
        )

        # Verify database operations were called
        assert mock_db.execute.call_count >= 2  # At least query + insert audit
        mock_db.commit.assert_called()

        # Verify allocation ID is generated
        assert result.allocation_id is not None
        assert len(result.allocation_id) > 0

    async def test_error_handling_no_positions(self, service, mock_db):
        """Test handling when no positions are found."""
        # Mock empty result
        mock_result = MagicMock()
        mock_result.fetchall.return_value = []
        mock_db.execute.return_value = mock_result

        result = await service.attribute_partial_exit(
            trading_account_id="acc_001",
            symbol="NONEXISTENT",
            exit_quantity=Decimal("100"),
            exit_price=Decimal("170.00"),
            exit_timestamp=datetime.now(timezone.utc),
            allocation_method=AllocationMethod.FIFO
        )

        assert len(result.allocations) == 0
        assert result.total_allocated_quantity == Decimal("0")
        assert result.unallocated_quantity == Decimal("100")
        assert result.requires_manual_intervention

    async def test_complex_fifo_scenario(self, service, mock_db):
        """Test complex FIFO scenario with multiple strategies and executions."""
        complex_positions = [
            {
                "position_id": "pos_A1",
                "symbol": "TSLA",
                "quantity": 30,
                "strategy_id": 1,
                "execution_id": "exec_A", 
                "portfolio_id": "port_A",
                "buy_price": Decimal("200.00"),
                "created_at": datetime(2024, 1, 1, 9, 0, tzinfo=timezone.utc),
                "entry_trades": []
            },
            {
                "position_id": "pos_B1",
                "symbol": "TSLA",
                "quantity": 20,
                "strategy_id": 2,
                "execution_id": "exec_B",
                "portfolio_id": "port_B", 
                "buy_price": Decimal("210.00"),
                "created_at": datetime(2024, 1, 1, 10, 0, tzinfo=timezone.utc),
                "entry_trades": []
            },
            {
                "position_id": "pos_A2",
                "symbol": "TSLA",
                "quantity": 40,
                "strategy_id": 1,
                "execution_id": "exec_A",
                "portfolio_id": "port_A",
                "buy_price": Decimal("220.00"),
                "created_at": datetime(2024, 1, 1, 11, 0, tzinfo=timezone.utc),
                "entry_trades": []
            }
        ]

        mock_result = MagicMock()
        mock_result.fetchall.return_value = [
            (p["position_id"], p["symbol"], p["quantity"], p["strategy_id"],
             p["execution_id"], p["portfolio_id"], str(p["buy_price"]),
             p["created_at"], []) for p in complex_positions
        ]
        mock_db.execute.return_value = mock_result

        # Allocate 65 shares (should take pos_A1: 30 + pos_B1: 20 + pos_A2: 15)
        result = await service.attribute_partial_exit(
            trading_account_id="acc_001",
            symbol="TSLA",
            exit_quantity=Decimal("65"),
            exit_price=Decimal("250.00"),
            exit_timestamp=datetime.now(timezone.utc),
            allocation_method=AllocationMethod.FIFO
        )

        assert len(result.allocations) == 3
        
        # Verify FIFO order based on created_at timestamps
        assert result.allocations[0].position_id == "pos_A1"
        assert result.allocations[0].allocated_quantity == Decimal("30")
        
        assert result.allocations[1].position_id == "pos_B1"
        assert result.allocations[1].allocated_quantity == Decimal("20")
        
        assert result.allocations[2].position_id == "pos_A2"
        assert result.allocations[2].allocated_quantity == Decimal("15")
        assert result.allocations[2].remaining_quantity == Decimal("25")

        assert result.total_allocated_quantity == Decimal("65")
        assert result.unallocated_quantity == Decimal("0")

    async def test_database_transaction_rollback(self, service, mock_db, sample_positions):
        """Test that database transactions are properly rolled back on error."""
        mock_result = MagicMock()
        mock_result.fetchall.return_value = [
            (p["position_id"], p["symbol"], p["quantity"], p["strategy_id"],
             p["execution_id"], p["portfolio_id"], str(p["buy_price"]),
             p["created_at"], []) for p in sample_positions
        ]
        
        # Make the second execute call fail (audit insert)
        mock_db.execute.side_effect = [mock_result, Exception("Database error")]

        with pytest.raises(Exception):
            await service.attribute_partial_exit(
                trading_account_id="acc_001",
                symbol="AAPL",
                exit_quantity=Decimal("100"),
                exit_price=Decimal("170.00"),
                exit_timestamp=datetime.now(timezone.utc),
                allocation_method=AllocationMethod.FIFO
            )

        mock_db.rollback.assert_called_once()

    @pytest.mark.parametrize("method", [
        AllocationMethod.FIFO,
        AllocationMethod.LIFO,
        AllocationMethod.PROPORTIONAL,
        AllocationMethod.MANUAL
    ])
    async def test_all_allocation_methods(self, service, mock_db, sample_positions, method):
        """Test all allocation methods produce valid results."""
        mock_result = MagicMock()
        mock_result.fetchall.return_value = [
            (p["position_id"], p["symbol"], p["quantity"], p["strategy_id"],
             p["execution_id"], p["portfolio_id"], str(p["buy_price"]),
             p["created_at"], p.get("entry_trades", [])) for p in sample_positions
        ]
        mock_db.execute.return_value = mock_result

        kwargs = {
            "trading_account_id": "acc_001",
            "symbol": "AAPL",
            "exit_quantity": Decimal("100"),
            "exit_price": Decimal("170.00"),
            "exit_timestamp": datetime.now(timezone.utc),
            "allocation_method": method
        }
        
        if method == AllocationMethod.MANUAL:
            kwargs["specific_trade_ids"] = ["trade_1"]

        result = await service.attribute_partial_exit(**kwargs)

        # All methods should produce valid allocation results
        assert result.allocation_id is not None
        assert isinstance(result.allocations, list)
        assert result.total_allocated_quantity >= Decimal("0")
        assert result.unallocated_quantity >= Decimal("0")
        assert isinstance(result.requires_manual_intervention, bool)
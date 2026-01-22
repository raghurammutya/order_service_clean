"""
Tests for Holdings Reconciliation Integration Service

Tests the integration between holdings reconciliation and partial exit attribution,
including variance detection, resolution, and manual case creation.
"""

import pytest
from decimal import Decimal
from datetime import datetime, timezone, timedelta
from unittest.mock import AsyncMock, MagicMock, patch
from sqlalchemy.ext.asyncio import AsyncSession

from order_service.app.services.holdings_reconciliation_integration import (
    HoldingsReconciliationIntegration,
    VarianceType,
    VarianceResolution,
    HoldingsVariance,
    VarianceResolutionResult,
    reconcile_holdings_variance
)
from order_service.app.services.partial_exit_attribution_service import (
    AllocationResult,
    PositionAllocation,
    AllocationMethod
)


class TestHoldingsReconciliationIntegration:
    """Test cases for HoldingsReconciliationIntegration."""

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
        """Holdings reconciliation integration service instance."""
        return HoldingsReconciliationIntegration(mock_db)

    @pytest.fixture
    def sample_internal_positions(self):
        """Sample internal positions for testing."""
        return [
            {
                "position_id": "pos_1",
                "symbol": "AAPL",
                "quantity": 100,
                "strategy_id": 1,
                "execution_id": "exec_1",
                "portfolio_id": "port_1",
                "buy_price": "150.00"
            },
            {
                "position_id": "pos_2",
                "symbol": "AAPL",
                "quantity": 50,
                "strategy_id": 2,
                "execution_id": "exec_2",
                "portfolio_id": "port_2", 
                "buy_price": "160.00"
            }
        ]

    @pytest.fixture
    def sample_allocation_result(self):
        """Sample allocation result for testing."""
        allocations = [
            PositionAllocation(
                position_id="pos_1",
                symbol="AAPL",
                strategy_id=1,
                execution_id="exec_1",
                portfolio_id="port_1",
                allocated_quantity=Decimal("25"),
                remaining_quantity=Decimal("75"),
                allocation_reason="FIFO allocation"
            )
        ]
        
        return AllocationResult(
            allocation_id="alloc_variance_123",
            trading_account_id="acc_001",
            symbol="AAPL",
            exit_quantity=Decimal("25"),
            allocations=allocations,
            total_allocated_quantity=Decimal("25"),
            unallocated_quantity=Decimal("0"),
            requires_manual_intervention=False,
            audit_trail=[]
        )

    async def test_reconcile_holdings_variance_below_threshold(self, service, mock_db, sample_internal_positions):
        """Test variance below threshold is ignored."""
        trading_account_id = "acc_001"
        symbol = "AAPL"
        broker_quantity = Decimal("150.005")  # Tiny difference
        threshold = Decimal("0.01")

        result = await service.reconcile_holdings_variance(
            trading_account_id, symbol, broker_quantity, 
            sample_internal_positions, threshold
        )

        assert result.resolution_type == VarianceResolution.IGNORED
        assert result.variance_resolved == Decimal("0")
        assert not result.requires_manual_review
        assert "below threshold" in result.warnings[0]

    async def test_reconcile_unknown_exit_auto_resolved(self, service, mock_db, sample_internal_positions, sample_allocation_result):
        """Test unknown exit variance that is automatically resolved."""
        trading_account_id = "acc_001"
        symbol = "AAPL"
        broker_quantity = Decimal("125")  # Internal: 150, Broker: 125 (25 missing - unknown exit)
        
        # Mock no recent external orders
        with patch.object(service, '_has_recent_external_orders', return_value=False):
            # Mock successful attribution
            with patch.object(service.attribution_service, 'attribute_partial_exit', return_value=sample_allocation_result):
                result = await service.reconcile_holdings_variance(
                    trading_account_id, symbol, broker_quantity, sample_internal_positions
                )

                assert result.resolution_type == VarianceResolution.AUTO_RESOLVED
                assert result.allocation_id == "alloc_variance_123"
                assert result.variance_resolved == Decimal("25")
                assert not result.requires_manual_review

    async def test_reconcile_unknown_exit_manual_required(self, service, mock_db, sample_internal_positions):
        """Test unknown exit variance requiring manual intervention."""
        trading_account_id = "acc_001"
        symbol = "AAPL"
        broker_quantity = Decimal("125")

        # Mock allocation that requires manual intervention
        manual_allocation_result = AllocationResult(
            allocation_id="alloc_manual_123",
            trading_account_id=trading_account_id,
            symbol=symbol,
            exit_quantity=Decimal("25"),
            allocations=[],
            total_allocated_quantity=Decimal("10"),
            unallocated_quantity=Decimal("15"),
            requires_manual_intervention=True,
            audit_trail=[]
        )

        with patch.object(service, '_has_recent_external_orders', return_value=False):
            with patch.object(service.attribution_service, 'attribute_partial_exit', return_value=manual_allocation_result):
                with patch.object(service, '_create_manual_case_from_allocation', return_value="case_123"):
                    result = await service.reconcile_holdings_variance(
                        trading_account_id, symbol, broker_quantity, sample_internal_positions
                    )

                    assert result.resolution_type == VarianceResolution.MANUAL_REQUIRED
                    assert result.attribution_case_id == "case_123"
                    assert result.variance_resolved == Decimal("10")
                    assert result.variance_remaining == Decimal("15")
                    assert result.requires_manual_review

    async def test_reconcile_known_exit_with_external_order(self, service, mock_db, sample_internal_positions, sample_allocation_result):
        """Test known exit variance with matching external order."""
        trading_account_id = "acc_001"
        symbol = "AAPL"
        broker_quantity = Decimal("125")

        # Mock external order found
        external_order = {
            "id": "ext_ord_123",
            "symbol": "AAPL",
            "quantity": -25,  # Sell order
            "price": Decimal("170.00"),
            "timestamp": datetime.now(timezone.utc) - timedelta(hours=1)
        }

        with patch.object(service, '_has_recent_external_orders', return_value=True):
            with patch.object(service, '_find_matching_external_order', return_value=external_order):
                with patch.object(service.attribution_service, 'attribute_partial_exit', return_value=sample_allocation_result):
                    result = await service.reconcile_holdings_variance(
                        trading_account_id, symbol, broker_quantity, sample_internal_positions
                    )

                    assert result.resolution_type == VarianceResolution.AUTO_RESOLVED
                    assert result.variance_resolved == Decimal("25")

    async def test_reconcile_unknown_entry_variance(self, service, mock_db, sample_internal_positions):
        """Test unknown entry variance (broker has more than internal)."""
        trading_account_id = "acc_001"
        symbol = "AAPL"
        broker_quantity = Decimal("175")  # Internal: 150, Broker: 175 (25 extra - unknown entry)

        # Mock manual case creation
        with patch.object(service.manual_attribution_service, 'create_attribution_case', return_value="case_entry_123"):
            result = await service.reconcile_holdings_variance(
                trading_account_id, symbol, broker_quantity, sample_internal_positions
            )

            assert result.resolution_type == VarianceResolution.MANUAL_REQUIRED
            assert result.attribution_case_id == "case_entry_123"
            assert result.variance_remaining == Decimal("25")
            assert result.requires_manual_review
            assert "Unknown entry" in result.warnings[0]

    async def test_reconcile_rounding_difference(self, service, mock_db, sample_internal_positions):
        """Test small rounding difference resolution."""
        trading_account_id = "acc_001"
        symbol = "AAPL"
        broker_quantity = Decimal("150.05")  # Small rounding difference

        result = await service.reconcile_holdings_variance(
            trading_account_id, symbol, broker_quantity, sample_internal_positions
        )

        assert result.resolution_type == VarianceResolution.IGNORED
        assert result.variance_remaining == Decimal("0.05")
        assert not result.requires_manual_review
        assert "rounding difference ignored" in result.warnings[0]

    async def test_classify_variance_unknown_exit(self, service, mock_db, sample_internal_positions):
        """Test variance classification for unknown exit."""
        variance_quantity = Decimal("-25")  # Negative = broker has less

        # Mock no recent external orders
        with patch.object(service, '_has_recent_external_orders', return_value=False):
            variance_type = await service._classify_variance(
                variance_quantity, sample_internal_positions, "acc_001", "AAPL"
            )

            assert variance_type == VarianceType.UNKNOWN_EXIT

    async def test_classify_variance_known_exit(self, service, mock_db, sample_internal_positions):
        """Test variance classification for known exit."""
        variance_quantity = Decimal("-25")

        # Mock recent external orders found
        with patch.object(service, '_has_recent_external_orders', return_value=True):
            variance_type = await service._classify_variance(
                variance_quantity, sample_internal_positions, "acc_001", "AAPL"
            )

            assert variance_type == VarianceType.KNOWN_EXIT

    async def test_classify_variance_unknown_entry(self, service, mock_db, sample_internal_positions):
        """Test variance classification for unknown entry."""
        variance_quantity = Decimal("50")  # Positive = broker has more

        variance_type = await service._classify_variance(
            variance_quantity, sample_internal_positions, "acc_001", "AAPL"
        )

        assert variance_type == VarianceType.UNKNOWN_ENTRY

    async def test_classify_variance_rounding_difference(self, service, mock_db, sample_internal_positions):
        """Test variance classification for rounding difference."""
        variance_quantity = Decimal("0.05")  # Very small

        variance_type = await service._classify_variance(
            variance_quantity, sample_internal_positions, "acc_001", "AAPL"
        )

        assert variance_type == VarianceType.ROUNDING_DIFFERENCE

    async def test_has_recent_external_orders_found(self, service, mock_db):
        """Test detection of recent external orders."""
        trading_account_id = "acc_001"
        symbol = "AAPL"
        quantity = Decimal("25")

        # Mock database response with matching orders
        mock_result = MagicMock()
        mock_result.fetchone.return_value = (2,)  # 2 matching orders found
        mock_db.execute.return_value = mock_result

        has_orders = await service._has_recent_external_orders(trading_account_id, symbol, quantity)

        assert has_orders is True

        # Verify query parameters
        call_args = mock_db.execute.call_args[1]
        assert call_args["trading_account_id"] == trading_account_id
        assert call_args["symbol"] == symbol
        assert call_args["quantity"] == "25"

    async def test_has_recent_external_orders_not_found(self, service, mock_db):
        """Test when no recent external orders are found."""
        mock_result = MagicMock()
        mock_result.fetchone.return_value = (0,)  # No matching orders
        mock_db.execute.return_value = mock_result

        has_orders = await service._has_recent_external_orders("acc_001", "AAPL", Decimal("25"))

        assert has_orders is False

    async def test_find_matching_external_order_found(self, service, mock_db):
        """Test finding matching external order."""
        trading_account_id = "acc_001"
        symbol = "AAPL"
        quantity = Decimal("25")
        detected_at = datetime.now(timezone.utc)

        # Mock database response with matching order
        order_time = detected_at - timedelta(hours=2)
        mock_result = MagicMock()
        mock_result.fetchone.return_value = (
            "ext_ord_123",
            "broker_ord_456",
            "AAPL",
            "SELL",
            "-25",
            "170.00",
            "169.50",
            "COMPLETE",
            order_time,
            order_time
        )
        mock_db.execute.return_value = mock_result

        order = await service._find_matching_external_order(
            trading_account_id, symbol, quantity, detected_at
        )

        assert order is not None
        assert order["id"] == "ext_ord_123"
        assert order["symbol"] == "AAPL"
        assert order["quantity"] == "-25"
        assert order["price"] == "170.00"
        assert order["timestamp"] == order_time

    async def test_find_matching_external_order_not_found(self, service, mock_db):
        """Test when no matching external order is found."""
        mock_result = MagicMock()
        mock_result.fetchone.return_value = None
        mock_db.execute.return_value = mock_result

        order = await service._find_matching_external_order(
            "acc_001", "AAPL", Decimal("25"), datetime.now(timezone.utc)
        )

        assert order is None

    async def test_store_holdings_variance(self, service, mock_db, sample_internal_positions):
        """Test storing holdings variance for audit."""
        variance = HoldingsVariance(
            variance_id="var_123",
            trading_account_id="acc_001",
            symbol="AAPL",
            broker_quantity=Decimal("125"),
            internal_quantity=Decimal("150"),
            variance_quantity=Decimal("-25"),
            variance_type=VarianceType.UNKNOWN_EXIT,
            detected_at=datetime.now(timezone.utc),
            positions_involved=sample_internal_positions,
            metadata={"source": "holdings_sync"}
        )

        await service._store_holdings_variance(variance)

        # Verify database insert
        insert_calls = [call for call in mock_db.execute.call_args_list 
                       if "holdings_reconciliation_variances" in str(call)]
        assert len(insert_calls) == 1

        call_args = insert_calls[0][1]
        assert call_args["variance_id"] == "var_123"
        assert call_args["trading_account_id"] == "acc_001"
        assert call_args["symbol"] == "AAPL"
        assert call_args["variance_type"] == VarianceType.UNKNOWN_EXIT.value

        mock_db.commit.assert_called()

    async def test_record_variance_audit(self, service, mock_db):
        """Test recording variance audit event."""
        variance_id = "var_123"
        event_type = "variance_resolved"
        user_id = "system"
        event_data = {"allocation_id": "alloc_123"}

        await service._record_variance_audit(variance_id, event_type, user_id, event_data)

        # Verify audit insert
        insert_calls = [call for call in mock_db.execute.call_args_list 
                       if "holdings_variance_audit" in str(call)]
        assert len(insert_calls) == 1

        call_args = insert_calls[0][1]
        assert call_args["variance_id"] == variance_id
        assert call_args["event_type"] == event_type
        assert call_args["user_id"] == user_id
        assert call_args["event_data"] == event_data

    async def test_create_manual_case_from_allocation(self, service, mock_db, sample_internal_positions, sample_allocation_result):
        """Test creating manual case from partial allocation result."""
        variance = HoldingsVariance(
            variance_id="var_123",
            trading_account_id="acc_001",
            symbol="AAPL",
            broker_quantity=Decimal("125"),
            internal_quantity=Decimal("150"),
            variance_quantity=Decimal("-25"),
            variance_type=VarianceType.UNKNOWN_EXIT,
            detected_at=datetime.now(timezone.utc),
            positions_involved=sample_internal_positions,
            metadata={}
        )

        # Mock manual case creation
        with patch.object(service.manual_attribution_service, 'create_attribution_case', return_value="case_from_alloc_123"):
            case_id = await service._create_manual_case_from_allocation(variance, sample_allocation_result)

            assert case_id == "case_from_alloc_123"

            # Verify case creation was called with correct parameters
            service.manual_attribution_service.create_attribution_case.assert_called_once()
            call_args = service.manual_attribution_service.create_attribution_case.call_args
            assert call_args[1]["trading_account_id"] == "acc_001"
            assert call_args[1]["symbol"] == "AAPL"
            assert "suggested_allocation" in call_args[1]

    async def test_reconcile_variance_error_handling(self, service, mock_db, sample_internal_positions):
        """Test error handling during variance reconciliation."""
        trading_account_id = "acc_001"
        symbol = "AAPL"
        broker_quantity = Decimal("125")

        # Mock database error
        mock_db.execute.side_effect = Exception("Database connection failed")

        result = await service.reconcile_holdings_variance(
            trading_account_id, symbol, broker_quantity, sample_internal_positions
        )

        assert result.resolution_type == VarianceResolution.FAILED
        assert result.variance_resolved == Decimal("0")
        assert result.requires_manual_review is True
        assert len(result.errors) > 0
        assert "Database connection failed" in result.errors[0]

    async def test_resolve_known_exit_fallback_to_unknown(self, service, mock_db, sample_internal_positions, sample_allocation_result):
        """Test known exit resolution falling back to unknown exit when no order found."""
        variance = HoldingsVariance(
            variance_id="var_123",
            trading_account_id="acc_001",
            symbol="AAPL",
            broker_quantity=Decimal("125"),
            internal_quantity=Decimal("150"),
            variance_quantity=Decimal("-25"),
            variance_type=VarianceType.KNOWN_EXIT,
            detected_at=datetime.now(timezone.utc),
            positions_involved=sample_internal_positions,
            metadata={}
        )

        # Mock no matching external order found
        with patch.object(service, '_find_matching_external_order', return_value=None):
            # Mock successful unknown exit resolution
            with patch.object(service, '_resolve_unknown_exit', return_value=VarianceResolutionResult(
                variance_id="var_123",
                resolution_type=VarianceResolution.AUTO_RESOLVED,
                allocation_id="fallback_alloc",
                attribution_case_id=None,
                variance_resolved=Decimal("25"),
                variance_remaining=Decimal("0"),
                requires_manual_review=False,
                audit_trail=[],
                errors=[],
                warnings=[]
            )):
                result = await service._resolve_known_exit(variance)

                assert result.resolution_type == VarianceResolution.AUTO_RESOLVED
                assert result.allocation_id == "fallback_alloc"

    async def test_helper_function_reconcile_holdings_variance(self, mock_db, sample_internal_positions):
        """Test the helper function for reconciling holdings variance."""
        trading_account_id = "acc_001"
        symbol = "AAPL"
        broker_quantity = Decimal("125")

        with patch('order_service.app.services.holdings_reconciliation_integration.HoldingsReconciliationIntegration') as mock_service_class:
            mock_service_instance = mock_service_class.return_value
            mock_result = VarianceResolutionResult(
                variance_id="helper_var_123",
                resolution_type=VarianceResolution.AUTO_RESOLVED,
                allocation_id="helper_alloc_123",
                attribution_case_id=None,
                variance_resolved=Decimal("25"),
                variance_remaining=Decimal("0"),
                requires_manual_review=False,
                audit_trail=[],
                errors=[],
                warnings=[]
            )
            mock_service_instance.reconcile_holdings_variance.return_value = mock_result

            result = await reconcile_holdings_variance(
                mock_db, trading_account_id, symbol, broker_quantity, sample_internal_positions
            )

            assert result.variance_id == "helper_var_123"
            assert result.resolution_type == VarianceResolution.AUTO_RESOLVED
            mock_service_class.assert_called_once_with(mock_db)

    @pytest.mark.parametrize("broker_qty,internal_qty,expected_type", [
        (100, 125, VarianceType.UNKNOWN_EXIT),      # Broker less = exit
        (150, 125, VarianceType.UNKNOWN_ENTRY),     # Broker more = entry  
        (125.05, 125, VarianceType.ROUNDING_DIFFERENCE),  # Small diff = rounding
        (125, 125, VarianceType.POSITION_MISMATCH)  # No difference = mismatch
    ])
    async def test_variance_classification_scenarios(self, service, mock_db, sample_internal_positions, broker_qty, internal_qty, expected_type):
        """Test variance classification for different scenarios."""
        # Adjust internal positions to match test quantity
        test_positions = sample_internal_positions.copy()
        test_positions[0]["quantity"] = internal_qty
        test_positions = [test_positions[0]]  # Use only first position

        variance_quantity = Decimal(str(broker_qty)) - Decimal(str(internal_qty))
        
        # Mock external orders check for exit scenarios
        has_external = expected_type == VarianceType.KNOWN_EXIT
        with patch.object(service, '_has_recent_external_orders', return_value=has_external):
            result_type = await service._classify_variance(
                variance_quantity, test_positions, "acc_001", "AAPL"
            )

            if variance_quantity == 0:
                # Special case: no variance should be position mismatch in this context
                assert result_type == VarianceType.POSITION_MISMATCH
            else:
                assert result_type == expected_type

    async def test_concurrent_variance_processing(self, service, mock_db, sample_internal_positions):
        """Test concurrent processing of multiple variances."""
        # This would test race conditions and database locking in real implementation
        # For now, just verify basic sequential processing works
        
        variances = [
            (Decimal("125"), "AAPL"),
            (Decimal("75"), "MSFT"),
            (Decimal("200"), "GOOGL")
        ]
        
        results = []
        for broker_qty, symbol in variances:
            with patch.object(service, '_has_recent_external_orders', return_value=False):
                with patch.object(service.attribution_service, 'attribute_partial_exit') as mock_attr:
                    mock_attr.return_value = AllocationResult(
                        allocation_id=f"alloc_{symbol}",
                        trading_account_id="acc_001",
                        symbol=symbol,
                        exit_quantity=Decimal("25"),
                        allocations=[],
                        total_allocated_quantity=Decimal("25"),
                        unallocated_quantity=Decimal("0"),
                        requires_manual_intervention=False,
                        audit_trail=[]
                    )
                    
                    result = await service.reconcile_holdings_variance(
                        "acc_001", symbol, broker_qty, sample_internal_positions
                    )
                    results.append(result)

        assert len(results) == 3
        assert all(r.resolution_type == VarianceResolution.AUTO_RESOLVED for r in results)
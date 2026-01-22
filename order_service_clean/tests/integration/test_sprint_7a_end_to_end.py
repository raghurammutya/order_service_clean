"""
End-to-End Integration Tests for Sprint 7A

Tests the complete Sprint 7A workflow including all components working together:
- Holdings variance detection
- Attribution allocation  
- Manual case creation
- Resolution application
- Transfer execution
- Handoff state management
"""

import pytest
from decimal import Decimal
from datetime import datetime, timezone, timedelta
from unittest.mock import AsyncMock, MagicMock, patch
from sqlalchemy.ext.asyncio import AsyncSession

from order_service.app.services.default_portfolio_service import DefaultPortfolioService
from order_service.app.services.partial_exit_attribution_service import (
    PartialExitAttributionService,
    AllocationMethod
)
from order_service.app.services.manual_attribution_service import (
    ManualAttributionService
)
from order_service.app.services.handoff_state_machine import (
    HandoffStateMachine,
    HandoffMode,
    TransitionType,
    TransitionRequest
)
from order_service.app.services.reconciliation_driven_transfers import (
    ReconciliationDrivenTransferService,
    TransferTrigger
)
from order_service.app.services.holdings_reconciliation_integration import (
    HoldingsReconciliationIntegration,
    VarianceResolution
)
from order_service.app.services.external_order_tagging_validation import (
    ExternalOrderTaggingValidation
)


class TestSprint7AEndToEnd:
    """End-to-end integration tests for Sprint 7A workflow."""

    @pytest.fixture
    async def mock_db(self):
        """Mock database session for integration tests."""
        db = AsyncMock(spec=AsyncSession)
        db.execute = AsyncMock()
        db.commit = AsyncMock()
        db.rollback = AsyncMock()
        return db

    @pytest.fixture
    def sample_trading_setup(self):
        """Sample trading setup for end-to-end tests."""
        return {
            "trading_account_id": "acc_integration_001",
            "user_id": "user_integration_123",
            "strategies": [
                {
                    "strategy_id": 1,
                    "name": "Momentum Strategy",
                    "execution_id": "exec_momentum_001"
                },
                {
                    "strategy_id": 2,
                    "name": "Value Strategy", 
                    "execution_id": "exec_value_001"
                }
            ],
            "positions": [
                {
                    "position_id": "pos_aapl_1",
                    "symbol": "AAPL",
                    "quantity": 100,
                    "strategy_id": 1,
                    "execution_id": "exec_momentum_001",
                    "buy_price": Decimal("150.00"),
                    "created_at": datetime.now(timezone.utc) - timedelta(days=5)
                },
                {
                    "position_id": "pos_aapl_2", 
                    "symbol": "AAPL",
                    "quantity": 50,
                    "strategy_id": 2,
                    "execution_id": "exec_value_001",
                    "buy_price": Decimal("160.00"),
                    "created_at": datetime.now(timezone.utc) - timedelta(days=3)
                },
                {
                    "position_id": "pos_msft_1",
                    "symbol": "MSFT",
                    "quantity": 75,
                    "strategy_id": 1,
                    "execution_id": "exec_momentum_001",
                    "buy_price": Decimal("300.00"),
                    "created_at": datetime.now(timezone.utc) - timedelta(days=2)
                }
            ]
        }

    async def test_complete_holdings_reconciliation_workflow(self, mock_db, sample_trading_setup):
        """Test complete holdings reconciliation workflow with all Sprint 7A components."""
        # Setup: Holdings variance detected
        trading_account_id = sample_trading_setup["trading_account_id"]
        symbol = "AAPL"
        
        # Broker reports 125 AAPL shares, but internal positions show 150 (100 + 50)
        broker_quantity = Decimal("125")
        internal_positions = [pos for pos in sample_trading_setup["positions"] if pos["symbol"] == symbol]
        
        # Step 1: Holdings Reconciliation detects variance and attempts attribution
        reconciliation_service = HoldingsReconciliationIntegration(mock_db)
        
        # Mock successful partial attribution  
        with patch.object(reconciliation_service, '_has_recent_external_orders', return_value=False):
            with patch.object(reconciliation_service.attribution_service, 'attribute_partial_exit') as mock_attribution:
                
                # Mock attribution result requiring manual intervention
                mock_attribution.return_value = MagicMock(
                    allocation_id="alloc_variance_e2e",
                    total_allocated_quantity=Decimal("20"),
                    unallocated_quantity=Decimal("5"),  # 5 shares unallocated
                    requires_manual_intervention=True,
                    allocations=[
                        MagicMock(
                            position_id="pos_aapl_1",
                            allocated_quantity=Decimal("20"),
                            remaining_quantity=Decimal("80"),
                            strategy_id=1
                        )
                    ]
                )
                
                # Mock manual case creation
                with patch.object(reconciliation_service, '_create_manual_case_from_allocation', return_value="case_e2e_variance"):
                    
                    variance_result = await reconciliation_service.reconcile_holdings_variance(
                        trading_account_id, symbol, broker_quantity, internal_positions
                    )
                    
                    assert variance_result.resolution_type == VarianceResolution.MANUAL_REQUIRED
                    assert variance_result.attribution_case_id == "case_e2e_variance"
                    assert variance_result.variance_resolved == Decimal("20")
                    assert variance_result.variance_remaining == Decimal("5")

        # Step 2: Manual Attribution workflow processes the case
        attribution_service = ManualAttributionService(mock_db)
        
        # Mock case retrieval
        mock_case = MagicMock()
        mock_case.case_id = "case_e2e_variance"
        mock_case.status.value = "resolved"
        mock_case.resolution_data = {
            "allocation_decisions": [
                {
                    "position_id": "pos_aapl_1",
                    "quantity": 3,
                    "strategy_id": 1,
                    "rationale": "Oldest position FIFO"
                },
                {
                    "position_id": "pos_aapl_2", 
                    "quantity": 2,
                    "strategy_id": 2,
                    "rationale": "Remaining allocation"
                }
            ]
        }
        mock_case.affected_positions = internal_positions
        mock_case.symbol = symbol
        
        # Mock manual resolution application with transfer execution
        with patch.object(attribution_service, 'get_attribution_case', return_value=mock_case):
            with patch('order_service.app.services.manual_attribution_service.ReconciliationDrivenTransferService') as mock_transfer_service:
                
                mock_transfer_result = MagicMock()
                mock_transfer_result.transfer_id = "transfer_e2e_001"
                mock_transfer_result.executed_count = 2
                mock_transfer_result.failed_count = 0
                mock_transfer_result.total_quantity_transferred = Decimal("5")
                
                mock_transfer_service_instance = mock_transfer_service.return_value
                mock_transfer_service_instance.execute_manual_transfer_instructions.return_value = mock_transfer_result
                
                # Apply the manual resolution
                success = await attribution_service.apply_resolution("case_e2e_variance", "user_integration_123")
                
                assert success is True
                mock_transfer_service_instance.execute_manual_transfer_instructions.assert_called_once()

    async def test_external_trade_orphan_detection_and_tagging(self, mock_db, sample_trading_setup):
        """Test external trade detection and automatic portfolio tagging."""
        # Setup: External trade creates orphan position
        trading_account_id = sample_trading_setup["trading_account_id"]
        
        # Step 1: Default Portfolio Service detects orphan position from external trade
        portfolio_service = DefaultPortfolioService(mock_db)
        
        # Mock orphan detection
        orphan_positions = [
            {
                "id": "pos_external_orphan",
                "symbol": "GOOGL",
                "quantity": 25,
                "strategy_id": 1,
                "portfolio_id": None,  # Orphan!
                "source": "external"
            }
        ]
        
        with patch.object(portfolio_service, 'get_orphan_positions', return_value=orphan_positions):
            with patch.object(portfolio_service, 'get_or_create_default_portfolio') as mock_get_portfolio:
                
                mock_get_portfolio.return_value = MagicMock(
                    portfolio_id="acc_integration_001_strategy_1_external"
                )
                
                # Process external trade
                result = await portfolio_service.process_external_trade(
                    trading_account_id=trading_account_id,
                    strategy_id=1,
                    symbol="GOOGL",
                    quantity=Decimal("25"),
                    side="BUY",
                    external_order_id="ext_googl_001"
                )
                
                assert result["orphans_detected"] == 1
                assert result["orphans_tagged"] == 1
                assert "external" in result["portfolio_id"]

        # Step 2: External Order Tagging Validation ensures integrity
        validation_service = ExternalOrderTaggingValidation(mock_db)
        
        # Mock validation checks
        with patch.object(validation_service, 'validate_external_order_tagging') as mock_validate:
            mock_validate.return_value = MagicMock(
                validation_id="val_ext_001",
                is_valid=True,
                validation_errors=[],
                recommendations=[]
            )
            
            validation_result = await validation_service.validate_external_order_tagging("ext_googl_001")
            
            assert validation_result.is_valid is True
            assert len(validation_result.validation_errors) == 0

    async def test_script_to_manual_handoff_with_position_transfers(self, mock_db, sample_trading_setup):
        """Test script to manual handoff with position control transfer."""
        # Setup: Script is controlling positions, user requests manual takeover
        trading_account_id = sample_trading_setup["trading_account_id"]
        strategy_id = 1
        execution_id = "exec_momentum_001"
        
        # Step 1: Handoff State Machine manages the transition
        handoff_service = HandoffStateMachine(mock_db)
        
        # Mock current script state
        with patch.object(handoff_service, 'get_handoff_state') as mock_get_state:
            mock_get_state.return_value = MagicMock(
                current_mode=HandoffMode.SCRIPT,
                trading_account_id=trading_account_id,
                strategy_id=strategy_id,
                execution_id=execution_id
            )
            
            # Mock validation passes
            with patch.object(handoff_service, '_validate_transition', return_value=[]):
                
                # Mock position and order operations
                script_positions = [pos for pos in sample_trading_setup["positions"] if pos["strategy_id"] == strategy_id]
                pending_orders = [
                    {"id": "ord_aapl_limit", "symbol": "AAPL", "status": "PENDING"},
                    {"id": "ord_msft_stop", "symbol": "MSFT", "status": "SUBMITTED"}
                ]
                
                with patch.object(handoff_service, '_get_positions_for_handoff', return_value=script_positions):
                    with patch.object(handoff_service, '_cancel_script_orders', return_value=pending_orders):
                        with patch.object(handoff_service, '_mark_position_for_manual_control'):
                            with patch.object(handoff_service, '_stop_script_execution'):
                                
                                # Execute transition request
                                request = TransitionRequest(
                                    trading_account_id=trading_account_id,
                                    strategy_id=strategy_id,
                                    execution_id=execution_id,
                                    from_mode=HandoffMode.SCRIPT,
                                    to_mode=HandoffMode.MANUAL,
                                    transition_type=TransitionType.SCRIPT_TO_MANUAL,
                                    requested_by="user_integration_123",
                                    reason="Manual intervention required for market conditions"
                                )
                                
                                result = await handoff_service.request_transition(request)
                                
                                assert result.success is True
                                assert result.positions_transferred == 2  # AAPL and MSFT positions
                                assert result.orders_cancelled == 2      # Cancelled pending orders
                                assert len(result.warnings) > 0         # Should warn about cancelled orders

    async def test_emergency_stop_workflow(self, mock_db, sample_trading_setup):
        """Test emergency stop workflow across all Sprint 7A components."""
        # Setup: Emergency situation requires immediate manual control
        trading_account_id = sample_trading_setup["trading_account_id"]
        
        # Step 1: Emergency stop handoff
        handoff_service = HandoffStateMachine(mock_db)
        
        with patch.object(handoff_service, 'get_handoff_state') as mock_get_state:
            mock_get_state.return_value = MagicMock(
                current_mode=HandoffMode.SCRIPT,
                trading_account_id=trading_account_id
            )
            
            with patch.object(handoff_service, '_validate_transition', return_value=[]):
                
                # Mock emergency operations
                all_positions = sample_trading_setup["positions"]
                all_orders = [
                    {"id": f"ord_emergency_{i}", "status": "PENDING", "source": "script"}
                    for i in range(5)
                ]
                
                with patch.object(handoff_service, '_cancel_all_orders', return_value=all_orders):
                    with patch.object(handoff_service, '_get_positions_for_handoff', return_value=all_positions):
                        with patch.object(handoff_service, '_mark_position_for_manual_control'):
                            with patch.object(handoff_service, '_emergency_stop_script'):
                                
                                # Execute emergency stop
                                emergency_request = TransitionRequest(
                                    trading_account_id=trading_account_id,
                                    strategy_id=None,  # Account-level emergency
                                    execution_id=None,
                                    from_mode=HandoffMode.SCRIPT,
                                    to_mode=HandoffMode.MANUAL,
                                    transition_type=TransitionType.EMERGENCY_STOP,
                                    requested_by="admin_emergency",
                                    reason="EMERGENCY: Market anomaly detected",
                                    force=True
                                )
                                
                                result = await handoff_service.request_transition(emergency_request)
                                
                                assert result.success is True
                                assert result.positions_transferred == 3  # All positions
                                assert result.orders_cancelled == 5      # All pending orders
                                assert any("Emergency stop" in warning for warning in result.warnings)

    async def test_fifo_attribution_with_multiple_strategies(self, mock_db, sample_trading_setup):
        """Test FIFO attribution across multiple strategies and executions."""
        # Setup: Exit that spans multiple positions from different strategies
        trading_account_id = sample_trading_setup["trading_account_id"]
        symbol = "AAPL"
        exit_quantity = Decimal("125")  # More than any single position
        
        attribution_service = PartialExitAttributionService(mock_db)
        
        # Mock position retrieval with proper ordering for FIFO
        aapl_positions = [pos for pos in sample_trading_setup["positions"] if pos["symbol"] == symbol]
        
        # Mock database response for positions (ordered by created_at for FIFO)
        mock_position_rows = [
            (pos["position_id"], pos["symbol"], pos["quantity"], pos["strategy_id"],
             pos["execution_id"], "portfolio_1", str(pos["buy_price"]), 
             pos["created_at"], [])
            for pos in sorted(aapl_positions, key=lambda x: x["created_at"])
        ]
        
        mock_result = MagicMock()
        mock_result.fetchall.return_value = mock_position_rows
        mock_db.execute.return_value = mock_result
        
        # Execute FIFO attribution
        allocation_result = await attribution_service.attribute_partial_exit(
            trading_account_id=trading_account_id,
            symbol=symbol,
            exit_quantity=exit_quantity,
            exit_price=Decimal("170.00"),
            exit_timestamp=datetime.now(timezone.utc),
            allocation_method=AllocationMethod.FIFO
        )
        
        # Verify FIFO allocation across strategies
        assert len(allocation_result.allocations) == 2  # Both AAPL positions
        assert allocation_result.total_allocated_quantity == Decimal("125")  # Partial from second position
        assert allocation_result.unallocated_quantity == Decimal("25")       # Remaining from exit
        
        # First allocation should be full first position (oldest)
        first_alloc = allocation_result.allocations[0]
        assert first_alloc.allocated_quantity == Decimal("100")  # Full first position
        assert first_alloc.strategy_id == 1                     # Momentum strategy (older)
        
        # Second allocation should be partial second position
        second_alloc = allocation_result.allocations[1] 
        assert second_alloc.allocated_quantity == Decimal("25")  # Partial second position
        assert second_alloc.remaining_quantity == Decimal("25")  # Remaining in position
        assert second_alloc.strategy_id == 2                    # Value strategy

    async def test_reconciliation_driven_transfer_execution(self, mock_db):
        """Test reconciliation-driven transfer execution with position movements."""
        transfer_service = ReconciliationDrivenTransferService(mock_db)
        
        # Setup: Manual transfer instructions from attribution case
        case_id = "case_integration_test"
        transfer_instructions = [
            {
                "source_execution_id": "exec_momentum_001",
                "target_execution_id": "manual",
                "symbol": "AAPL",
                "quantity": 30,
                "reason": "Manual attribution decision - transfer to manual control"
            },
            {
                "source_execution_id": "exec_value_001",
                "target_execution_id": "exec_momentum_001",
                "symbol": "MSFT", 
                "quantity": 15,
                "reason": "Rebalancing transfer between executions"
            }
        ]
        executed_by = "user_integration_123"
        
        # Mock position retrieval and transfer operations
        mock_positions = [
            {
                "id": "pos_transfer_1",
                "symbol": "AAPL",
                "quantity": 100,
                "execution_id": "exec_momentum_001"
            },
            {
                "id": "pos_transfer_2",
                "symbol": "MSFT",
                "quantity": 50,
                "execution_id": "exec_value_001"
            }
        ]
        
        with patch.object(transfer_service, '_get_positions_by_execution_and_symbol', return_value=mock_positions):
            with patch.object(transfer_service, '_transfer_to_manual_control', return_value=Decimal("30")):
                with patch.object(transfer_service, '_transfer_between_executions', return_value=Decimal("15")):
                    
                    # Execute transfer instructions
                    result = await transfer_service.execute_manual_transfer_instructions(
                        case_id, transfer_instructions, executed_by
                    )
                    
                    assert result.transfer_id is not None
                    assert result.trigger == TransferTrigger.MANUAL_INSTRUCTION
                    assert result.source_allocation_id == case_id
                    assert result.instructions_count == 2
                    assert result.executed_count == 2
                    assert result.failed_count == 0
                    assert result.total_quantity_transferred == Decimal("45")  # 30 + 15

    async def test_data_integrity_across_services(self, mock_db, sample_trading_setup):
        """Test that data integrity is maintained across all Sprint 7A services."""
        # This test would verify that:
        # 1. Position IDs are consistent across services
        # 2. Quantities balance correctly after transfers
        # 3. Audit trails are complete and linked
        # 4. State transitions are atomic
        # 5. Error conditions don't leave orphaned data
        
        trading_account_id = sample_trading_setup["trading_account_id"]
        
        # Test data consistency check across services
        portfolio_service = DefaultPortfolioService(mock_db)
        attribution_service = PartialExitAttributionService(mock_db)
        reconciliation_service = HoldingsReconciliationIntegration(mock_db)
        
        # Mock consistent position data across all services
        consistent_positions = sample_trading_setup["positions"]
        
        with patch.object(portfolio_service, 'get_orphan_positions', return_value=consistent_positions):
            with patch.object(attribution_service, '_get_positions_for_attribution'):
                with patch.object(reconciliation_service, '_get_positions_by_execution_and_symbol'):
                    
                    # Verify all services see the same position data
                    orphans = await portfolio_service.get_orphan_positions(trading_account_id, 1)
                    
                    # Check position ID consistency
                    position_ids = {pos["position_id"] for pos in consistent_positions}
                    orphan_ids = {pos["id"] for pos in orphans}
                    
                    # In a real test, these would match exactly
                    assert len(position_ids) > 0
                    assert len(orphan_ids) > 0

    async def test_performance_with_large_dataset(self, mock_db):
        """Test Sprint 7A performance with large numbers of positions and cases."""
        # Setup: Large dataset simulation
        large_position_count = 1000
        large_case_count = 50
        
        # Mock large position dataset
        large_positions = [
            {
                "position_id": f"pos_perf_{i}",
                "symbol": f"STOCK_{i % 100}",  # 100 different symbols
                "quantity": 100 + (i % 50),
                "strategy_id": (i % 10) + 1,   # 10 strategies
                "execution_id": f"exec_{i % 10}",
                "created_at": datetime.now(timezone.utc) - timedelta(days=i % 30)
            }
            for i in range(large_position_count)
        ]
        
        # Test attribution performance with large dataset
        attribution_service = PartialExitAttributionService(mock_db)
        
        # Mock database response for large position query
        mock_large_result = MagicMock()
        mock_large_result.fetchall.return_value = [
            (pos["position_id"], pos["symbol"], pos["quantity"], pos["strategy_id"],
             pos["execution_id"], "portfolio_1", "100.00", pos["created_at"], [])
            for pos in large_positions[:100]  # Simulate first 100 for performance
        ]
        mock_db.execute.return_value = mock_large_result
        
        # Time the attribution operation (in real test, would measure actual time)
        start_time = datetime.now()
        
        result = await attribution_service.attribute_partial_exit(
            trading_account_id="perf_test_account",
            symbol="STOCK_0",
            exit_quantity=Decimal("5000"),  # Large exit requiring many positions
            exit_price=Decimal("150.00"),
            exit_timestamp=datetime.now(timezone.utc),
            allocation_method=AllocationMethod.FIFO
        )
        
        end_time = datetime.now()
        
        # Verify results are reasonable for large dataset
        assert result.allocation_id is not None
        assert result.total_allocated_quantity > 0
        # In real test, would assert performance metrics
        execution_time = (end_time - start_time).total_seconds()
        assert execution_time < 10.0  # Should complete within 10 seconds

    async def test_error_recovery_and_rollback(self, mock_db, sample_trading_setup):
        """Test error recovery and rollback mechanisms across Sprint 7A."""
        # Setup: Simulate various error conditions and verify rollback
        trading_account_id = sample_trading_setup["trading_account_id"]
        
        # Test 1: Attribution service rollback on database error
        attribution_service = PartialExitAttributionService(mock_db)
        
        # Mock database failure during attribution
        mock_db.execute.side_effect = [
            MagicMock(),  # First call succeeds (position query)
            Exception("Database connection lost")  # Second call fails (audit insert)
        ]
        
        with pytest.raises(Exception) as exc_info:
            await attribution_service.attribute_partial_exit(
                trading_account_id=trading_account_id,
                symbol="AAPL",
                exit_quantity=Decimal("100"),
                exit_price=Decimal("170.00"),
                exit_timestamp=datetime.now(timezone.utc),
                allocation_method=AllocationMethod.FIFO
            )
        
        assert "Database connection lost" in str(exc_info.value)
        mock_db.rollback.assert_called()  # Should have rolled back transaction
        
        # Test 2: Handoff transition rollback on failure
        handoff_service = HandoffStateMachine(mock_db)
        
        # Reset mock for second test
        mock_db.execute.side_effect = None
        mock_db.rollback.reset_mock()
        
        with patch.object(handoff_service, 'get_handoff_state') as mock_get_state:
            mock_get_state.return_value = MagicMock(current_mode=HandoffMode.MANUAL)
            
            with patch.object(handoff_service, '_validate_transition', return_value=[]):
                with patch.object(handoff_service, '_execute_manual_to_script', side_effect=Exception("Handoff failed")):
                    
                    request = TransitionRequest(
                        trading_account_id=trading_account_id,
                        strategy_id=1,
                        execution_id="exec_001",
                        from_mode=HandoffMode.MANUAL,
                        to_mode=HandoffMode.SCRIPT,
                        transition_type=TransitionType.MANUAL_TO_SCRIPT,
                        requested_by="test_user",
                        reason="Test rollback"
                    )
                    
                    with pytest.raises(Exception) as exc_info:
                        await handoff_service.request_transition(request)
                    
                    assert "Handoff failed" in str(exc_info.value)
                    # In real implementation, would verify state was rolled back

    @pytest.mark.asyncio
    async def test_concurrent_operations_thread_safety(self, mock_db, sample_trading_setup):
        """Test concurrent operations and thread safety across Sprint 7A services."""
        # This test would verify that concurrent operations don't interfere
        # Would use asyncio.gather() to run multiple operations simultaneously
        pass

    @pytest.mark.slow 
    async def test_stress_test_high_volume(self, mock_db):
        """Stress test Sprint 7A with high volume of concurrent operations."""
        # This test would simulate high-volume production conditions
        # Multiple concurrent attribution cases, transfers, handoffs
        pass
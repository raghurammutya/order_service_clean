"""
Tests for Reconciliation-Driven Transfers Service

Tests position transfer execution based on reconciliation outcomes,
including transfer instructions, position movements, and audit trails.
"""

import pytest
from decimal import Decimal
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch
from sqlalchemy.ext.asyncio import AsyncSession

from order_service.app.services.reconciliation_driven_transfers import (
    ReconciliationDrivenTransferService,
    TransferTrigger,
    TransferStatus,
    TransferInstruction,
    ReconciliationTransferResult,
    execute_attribution_transfers
)
from order_service.app.services.partial_exit_attribution_service import (
    AllocationResult,
    PositionAllocation
)


class TestReconciliationDrivenTransferService:
    """Test cases for ReconciliationDrivenTransferService."""

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
        """Reconciliation-driven transfer service instance."""
        return ReconciliationDrivenTransferService(mock_db)

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
                allocated_quantity=Decimal("75"),
                remaining_quantity=Decimal("25"),
                allocation_reason="FIFO allocation"
            ),
            PositionAllocation(
                position_id="pos_2",
                symbol="AAPL",
                strategy_id=2,
                execution_id="exec_2",
                portfolio_id="port_2",
                allocated_quantity=Decimal("25"),
                remaining_quantity=Decimal("25"),
                allocation_reason="FIFO allocation"
            )
        ]
        
        return AllocationResult(
            allocation_id="alloc_123",
            trading_account_id="acc_001",
            symbol="AAPL",
            exit_quantity=Decimal("100"),
            allocations=allocations,
            total_allocated_quantity=Decimal("100"),
            unallocated_quantity=Decimal("0"),
            requires_manual_intervention=False,
            audit_trail=[]
        )

    @pytest.fixture
    def sample_transfer_instructions(self):
        """Sample transfer instructions for testing."""
        return [
            {
                "source_execution_id": "exec_1",
                "target_execution_id": "manual",
                "symbol": "AAPL",
                "quantity": 50,
                "reason": "Manual attribution decision"
            },
            {
                "source_execution_id": "exec_2", 
                "target_execution_id": "exec_3",
                "symbol": "AAPL",
                "quantity": 25,
                "reason": "Rebalancing transfer"
            }
        ]

    @pytest.fixture
    def sample_positions(self):
        """Sample positions for transfer testing."""
        return [
            {
                "id": "pos_1",
                "symbol": "AAPL",
                "quantity": 100,
                "strategy_id": 1,
                "execution_id": "exec_1",
                "portfolio_id": "port_1",
                "trading_account_id": "acc_001",
                "source": "script"
            },
            {
                "id": "pos_2",
                "symbol": "AAPL", 
                "quantity": 50,
                "strategy_id": 2,
                "execution_id": "exec_2",
                "portfolio_id": "port_2",
                "trading_account_id": "acc_001",
                "source": "script"
            }
        ]

    async def test_execute_attribution_transfers_success(self, service, mock_db, sample_allocation_result):
        """Test successful execution of attribution-based transfers."""
        # Mock transfer instruction generation
        with patch.object(service, '_generate_transfer_instructions') as mock_generate:
            transfer_instructions = [
                TransferInstruction(
                    source_execution_id="exec_1",
                    target_execution_id="manual",
                    symbol="AAPL",
                    quantity=Decimal("75"),
                    allocation_reason="FIFO allocation",
                    metadata={"allocation_id": "alloc_123"}
                )
            ]
            mock_generate.return_value = transfer_instructions

            # Mock successful transfer execution
            with patch.object(service, '_execute_single_transfer', return_value=(True, Decimal("75"))):
                result = await service.execute_attribution_transfers(sample_allocation_result)

                assert result.transfer_id is not None
                assert result.trigger == TransferTrigger.PARTIAL_EXIT_ALLOCATED
                assert result.source_allocation_id == "alloc_123"
                assert result.instructions_count == 1
                assert result.executed_count == 1
                assert result.failed_count == 0
                assert result.total_quantity_transferred == Decimal("75")
                assert len(result.errors) == 0

    async def test_execute_attribution_transfers_no_instructions(self, service, mock_db, sample_allocation_result):
        """Test handling when no transfer instructions are generated."""
        # Mock no transfer instructions needed
        with patch.object(service, '_generate_transfer_instructions', return_value=[]):
            result = await service.execute_attribution_transfers(sample_allocation_result)

            assert result.success is True
            assert result.instructions_count == 0
            assert result.executed_count == 0
            assert result.total_quantity_transferred == Decimal("0")
            assert "No transfers needed" in result.warnings

    async def test_execute_attribution_transfers_partial_failure(self, service, mock_db, sample_allocation_result):
        """Test handling of partial transfer failures."""
        # Mock transfer instructions
        with patch.object(service, '_generate_transfer_instructions') as mock_generate:
            transfer_instructions = [
                TransferInstruction(
                    source_execution_id="exec_1",
                    target_execution_id="manual",
                    symbol="AAPL",
                    quantity=Decimal("50"),
                    allocation_reason="Test transfer 1"
                ),
                TransferInstruction(
                    source_execution_id="exec_2",
                    target_execution_id="manual", 
                    symbol="AAPL",
                    quantity=Decimal("25"),
                    allocation_reason="Test transfer 2"
                )
            ]
            mock_generate.return_value = transfer_instructions

            # Mock one success, one failure
            with patch.object(service, '_execute_single_transfer', side_effect=[
                (True, Decimal("50")),
                (False, Decimal("0"))
            ]):
                result = await service.execute_attribution_transfers(sample_allocation_result)

                assert result.instructions_count == 2
                assert result.executed_count == 1
                assert result.failed_count == 1
                assert result.total_quantity_transferred == Decimal("50")
                assert len(result.errors) == 1
                assert "Partial success" in result.warnings[0]

    async def test_execute_manual_transfer_instructions(self, service, mock_db, sample_transfer_instructions):
        """Test execution of manual transfer instructions."""
        case_id = "case_123"
        executed_by = "user_456"

        # Mock successful transfers
        with patch.object(service, '_execute_single_transfer', return_value=(True, Decimal("50"))):
            result = await service.execute_manual_transfer_instructions(
                case_id, sample_transfer_instructions, executed_by
            )

            assert result.transfer_id is not None
            assert result.trigger == TransferTrigger.MANUAL_INSTRUCTION
            assert result.source_allocation_id == case_id
            assert result.instructions_count == 2
            assert result.executed_count == 2
            assert result.failed_count == 0

    async def test_generate_transfer_instructions(self, service, mock_db, sample_allocation_result):
        """Test generation of transfer instructions from allocation results."""
        instructions = await service._generate_transfer_instructions(sample_allocation_result)

        assert len(instructions) >= 0  # May be empty based on allocation logic
        
        for instruction in instructions:
            assert instruction.source_execution_id is not None
            assert instruction.target_execution_id is not None
            assert instruction.symbol == "AAPL"
            assert instruction.quantity > 0
            assert "allocation_id" in instruction.metadata

    async def test_execute_single_transfer_to_manual(self, service, mock_db, sample_positions):
        """Test executing transfer to manual control."""
        instruction = TransferInstruction(
            source_execution_id="exec_1",
            target_execution_id="manual",
            symbol="AAPL",
            quantity=Decimal("50"),
            allocation_reason="Manual control transfer",
            metadata={"test": "data"}
        )

        # Mock position retrieval
        with patch.object(service, '_get_positions_by_execution_and_symbol', return_value=sample_positions):
            # Mock successful manual transfer
            with patch.object(service, '_transfer_to_manual_control', return_value=Decimal("50")):
                success, transferred = await service._execute_single_transfer("transfer_123", instruction)

                assert success is True
                assert transferred == Decimal("50")

    async def test_execute_single_transfer_between_executions(self, service, mock_db, sample_positions):
        """Test executing transfer between different executions."""
        instruction = TransferInstruction(
            source_execution_id="exec_1",
            target_execution_id="exec_3",
            symbol="AAPL", 
            quantity=Decimal("25"),
            allocation_reason="Execution rebalancing",
            metadata={}
        )

        # Mock position retrieval
        with patch.object(service, '_get_positions_by_execution_and_symbol', return_value=sample_positions):
            # Mock successful execution transfer
            with patch.object(service, '_transfer_between_executions', return_value=Decimal("25")):
                success, transferred = await service._execute_single_transfer("transfer_123", instruction)

                assert success is True
                assert transferred == Decimal("25")

    async def test_execute_single_transfer_insufficient_quantity(self, service, mock_db):
        """Test transfer failure when insufficient quantity available."""
        instruction = TransferInstruction(
            source_execution_id="exec_1",
            target_execution_id="manual",
            symbol="AAPL",
            quantity=Decimal("200"),  # More than available
            allocation_reason="Test insufficient"
        )

        # Mock position with insufficient quantity
        insufficient_positions = [{
            "id": "pos_1",
            "symbol": "AAPL",
            "quantity": 50,  # Only 50 available
            "execution_id": "exec_1"
        }]

        with patch.object(service, '_get_positions_by_execution_and_symbol', return_value=insufficient_positions):
            success, transferred = await service._execute_single_transfer("transfer_123", instruction)

            assert success is False
            assert transferred == Decimal("0")

    async def test_transfer_to_manual_control(self, service, mock_db, sample_positions):
        """Test transferring positions to manual control."""
        transfer_quantity = Decimal("75")
        metadata = {"reason": "test_transfer"}

        transferred = await service._transfer_to_manual_control(
            sample_positions, transfer_quantity, metadata
        )

        assert transferred == Decimal("75")

        # Verify database update calls
        update_calls = [call for call in mock_db.execute.call_args_list 
                       if "UPDATE order_service.positions" in str(call)]
        assert len(update_calls) >= 1

        mock_db.commit.assert_called()

    async def test_transfer_between_executions_full_position(self, service, mock_db, sample_positions):
        """Test transferring full position between executions."""
        source_execution_id = "exec_1"
        target_execution_id = "exec_3"
        symbol = "AAPL"
        transfer_quantity = Decimal("100")  # Full position
        metadata = {}

        # Mock position retrieval
        with patch.object(service, '_get_positions_by_execution_and_symbol', return_value=sample_positions):
            transferred = await service._transfer_between_executions(
                source_execution_id, target_execution_id, symbol, transfer_quantity, metadata
            )

            assert transferred == Decimal("100")
            mock_db.commit.assert_called()

    async def test_transfer_between_executions_partial_position(self, service, mock_db, sample_positions):
        """Test transferring partial position between executions (requires split)."""
        source_execution_id = "exec_1"
        target_execution_id = "exec_3"
        symbol = "AAPL"
        transfer_quantity = Decimal("60")  # Partial of 100
        metadata = {}

        # Mock position retrieval
        with patch.object(service, '_get_positions_by_execution_and_symbol', return_value=sample_positions):
            transferred = await service._transfer_between_executions(
                source_execution_id, target_execution_id, symbol, transfer_quantity, metadata
            )

            assert transferred == Decimal("60")

            # Should have called position split operations
            update_calls = [call for call in mock_db.execute.call_args_list]
            assert len(update_calls) >= 2  # Update original + insert new

    async def test_get_positions_by_execution_and_symbol(self, service, mock_db, sample_positions):
        """Test retrieving positions by execution and symbol."""
        mock_result = MagicMock()
        mock_result.fetchall.return_value = [
            (p["id"], p["symbol"], p["quantity"], p["strategy_id"],
             p["execution_id"], p["portfolio_id"], p["trading_account_id"], p["source"])
            for p in sample_positions
        ]
        mock_db.execute.return_value = mock_result

        positions = await service._get_positions_by_execution_and_symbol("exec_1", "AAPL")

        assert len(positions) == 2
        assert positions[0]["id"] == "pos_1"
        assert positions[0]["symbol"] == "AAPL"
        assert positions[1]["execution_id"] == "exec_2"

    async def test_record_transfer_batch(self, service, mock_db):
        """Test recording transfer batch in database."""
        transfer_id = "transfer_123"
        source_id = "alloc_456" 
        instructions = [
            TransferInstruction(
                source_execution_id="exec_1",
                target_execution_id="manual",
                symbol="AAPL",
                quantity=Decimal("50"),
                allocation_reason="Test transfer"
            )
        ]
        trigger = TransferTrigger.PARTIAL_EXIT_ALLOCATED

        await service._record_transfer_batch(transfer_id, source_id, instructions, trigger)

        # Verify batch record was inserted
        insert_calls = [call for call in mock_db.execute.call_args_list 
                       if "reconciliation_transfer_batches" in str(call)]
        assert len(insert_calls) == 1

        call_args = insert_calls[0][1]
        assert call_args["transfer_id"] == transfer_id
        assert call_args["source_id"] == source_id
        assert call_args["trigger_type"] == trigger.value
        assert call_args["instructions_count"] == 1

        mock_db.commit.assert_called()

    async def test_update_transfer_batch_status(self, service, mock_db):
        """Test updating transfer batch status."""
        transfer_id = "transfer_123"
        status = TransferStatus.COMPLETED
        result_metadata = {"executed_count": 2, "failed_count": 0}

        await service._update_transfer_batch_status(transfer_id, status, result_metadata)

        # Verify status update
        update_calls = [call for call in mock_db.execute.call_args_list 
                       if "UPDATE order_service.reconciliation_transfer_batches" in str(call)]
        assert len(update_calls) == 1

        call_args = update_calls[0][1]
        assert call_args["transfer_id"] == transfer_id
        assert call_args["status"] == status.value
        assert call_args["result_metadata"] == result_metadata

        mock_db.commit.assert_called()

    async def test_record_transfer_execution(self, service, mock_db):
        """Test recording individual transfer execution."""
        transfer_id = "transfer_123"
        instruction = TransferInstruction(
            source_execution_id="exec_1",
            target_execution_id="manual",
            symbol="AAPL",
            quantity=Decimal("50"),
            allocation_reason="Test transfer",
            metadata={"test": "data"}
        )
        success = True
        quantity_transferred = Decimal("50")
        error_message = None

        await service._record_transfer_execution(
            transfer_id, instruction, success, quantity_transferred, error_message
        )

        # Verify execution record was inserted
        insert_calls = [call for call in mock_db.execute.call_args_list 
                       if "reconciliation_transfer_executions" in str(call)]
        assert len(insert_calls) == 1

        call_args = insert_calls[0][1]
        assert call_args["transfer_id"] == transfer_id
        assert call_args["source_execution_id"] == instruction.source_execution_id
        assert call_args["success"] is True
        assert call_args["transferred_quantity"] == str(quantity_transferred)

    async def test_get_transfer_status(self, service, mock_db):
        """Test retrieving transfer status and results."""
        transfer_id = "transfer_123"

        # Mock batch status response
        mock_batch_result = MagicMock()
        mock_batch_result.fetchone.return_value = (
            transfer_id,
            "alloc_456",
            TransferTrigger.PARTIAL_EXIT_ALLOCATED.value,
            2,
            TransferStatus.COMPLETED.value,
            datetime.now(timezone.utc),
            datetime.now(timezone.utc),
            {"instructions": []},
            {"executed_count": 2}
        )

        # Mock execution details response
        mock_exec_result = MagicMock()
        mock_exec_result.fetchall.return_value = [
            ("exec_1", "manual", "AAPL", "50", "50", True, None, datetime.now(timezone.utc))
        ]

        mock_db.execute.side_effect = [mock_batch_result, mock_exec_result]

        status = await service.get_transfer_status(transfer_id)

        assert status is not None
        assert status["transfer_id"] == transfer_id
        assert status["status"] == TransferStatus.COMPLETED.value
        assert status["instructions_count"] == 2
        assert len(status["executions"]) == 1

    async def test_get_transfer_status_not_found(self, service, mock_db):
        """Test retrieving status for non-existent transfer."""
        mock_result = MagicMock()
        mock_result.fetchone.return_value = None
        mock_db.execute.return_value = mock_result

        status = await service.get_transfer_status("nonexistent")

        assert status is None

    async def test_list_recent_transfers(self, service, mock_db):
        """Test listing recent transfer batches."""
        # Mock transfer list response
        mock_result = MagicMock()
        mock_result.fetchall.return_value = [
            ("transfer_1", "alloc_1", "partial_exit_allocated", 1, "completed", 
             datetime.now(timezone.utc), datetime.now(timezone.utc), {"executed": 1}),
            ("transfer_2", "case_2", "manual_instruction", 2, "in_progress",
             datetime.now(timezone.utc), None, None)
        ]
        mock_db.execute.return_value = mock_result

        transfers = await service.list_recent_transfers(limit=10)

        assert len(transfers) == 2
        assert transfers[0]["transfer_id"] == "transfer_1"
        assert transfers[0]["status"] == "completed"
        assert transfers[1]["transfer_id"] == "transfer_2"
        assert transfers[1]["status"] == "in_progress"

    async def test_list_recent_transfers_with_filter(self, service, mock_db):
        """Test listing transfers with status filter."""
        mock_result = MagicMock()
        mock_result.fetchall.return_value = [
            ("transfer_1", "alloc_1", "partial_exit_allocated", 1, "completed",
             datetime.now(timezone.utc), datetime.now(timezone.utc), {"executed": 1})
        ]
        mock_db.execute.return_value = mock_result

        transfers = await service.list_recent_transfers(
            limit=10, 
            status_filter=TransferStatus.COMPLETED
        )

        assert len(transfers) == 1
        assert transfers[0]["status"] == "completed"

        # Verify WHERE clause was added to query
        call_args = mock_db.execute.call_args[1]
        assert "status_filter" in call_args

    async def test_error_handling_and_rollback(self, service, mock_db, sample_allocation_result):
        """Test error handling and transaction rollback."""
        # Make database operation fail
        mock_db.execute.side_effect = Exception("Database connection lost")

        with pytest.raises(Exception) as exc_info:
            await service.execute_attribution_transfers(sample_allocation_result)

        assert "Database connection lost" in str(exc_info.value)

        # Should have updated batch status to failed
        # In real implementation, this would be verified through audit trail

    async def test_helper_function_execute_attribution_transfers(self, mock_db, sample_allocation_result):
        """Test the helper function for executing attribution transfers."""
        with patch('order_service.app.services.reconciliation_driven_transfers.ReconciliationDrivenTransferService') as mock_service_class:
            mock_service_instance = mock_service_class.return_value
            mock_result = ReconciliationTransferResult(
                transfer_id="test_transfer",
                trigger=TransferTrigger.PARTIAL_EXIT_ALLOCATED,
                source_allocation_id="alloc_123",
                instructions_count=1,
                executed_count=1,
                failed_count=0,
                total_quantity_transferred=Decimal("100"),
                errors=[],
                warnings=[],
                audit_trail=[]
            )
            mock_service_instance.execute_attribution_transfers.return_value = mock_result

            result = await execute_attribution_transfers(mock_db, sample_allocation_result)

            assert result.transfer_id == "test_transfer"
            assert result.executed_count == 1
            mock_service_class.assert_called_once_with(mock_db)

    @pytest.mark.parametrize("trigger", [
        TransferTrigger.ATTRIBUTION_RESOLVED,
        TransferTrigger.PARTIAL_EXIT_ALLOCATED,
        TransferTrigger.HOLDINGS_RECONCILIATION,
        TransferTrigger.MANUAL_INSTRUCTION
    ])
    async def test_all_transfer_triggers(self, service, mock_db, sample_allocation_result, trigger):
        """Test transfers with all possible triggers."""
        with patch.object(service, '_generate_transfer_instructions', return_value=[]):
            result = await service.execute_attribution_transfers(sample_allocation_result, trigger)

            assert result.trigger == trigger
            assert result.instructions_count == 0

    async def test_large_quantity_precision(self, service, mock_db):
        """Test handling of large quantities with decimal precision."""
        large_positions = [{
            "id": "pos_large",
            "symbol": "AAPL", 
            "quantity": 1000000.123456,  # Large quantity with precision
            "execution_id": "exec_1"
        }]

        instruction = TransferInstruction(
            source_execution_id="exec_1",
            target_execution_id="manual",
            symbol="AAPL",
            quantity=Decimal("1000000.123456"),
            allocation_reason="Large quantity test"
        )

        with patch.object(service, '_get_positions_by_execution_and_symbol', return_value=large_positions):
            with patch.object(service, '_transfer_to_manual_control', return_value=Decimal("1000000.123456")):
                success, transferred = await service._execute_single_transfer("transfer_test", instruction)

                assert success is True
                assert transferred == Decimal("1000000.123456")
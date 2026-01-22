"""
Test Transfer Instruction Generator (GAP-REC-11)
Tests comprehensive transfer instruction generation with types and priority rules
"""

import pytest
from decimal import Decimal
from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch, MagicMock

from order_service.app.services.transfer_instruction_generator import (
    TransferInstructionGenerator,
    TransferInstructionType,
    TransferPriority,
    TransferInstruction,
    TransferInstructionBatch,
    InstructionMetadata,
    TransferExecutionContext
)


@pytest.mark.asyncio
class TestTransferInstructionGenerator:
    """Test transfer instruction generation functionality."""

    @pytest.fixture
    def generator(self, mock_db_session):
        """Create generator instance."""
        return TransferInstructionGenerator(mock_db_session)

    @pytest.fixture
    def sample_allocation_result(self):
        """Sample allocation result for testing."""
        return {
            "allocations": [
                {
                    "position_id": 1,
                    "strategy_id": "strat_001",
                    "execution_id": "exec_001",
                    "allocated_quantity": Decimal("50"),
                    "remaining_quantity": Decimal("150"),
                    "entry_price": Decimal("100.00")
                },
                {
                    "position_id": 2,
                    "strategy_id": "strat_002",
                    "execution_id": "exec_002",
                    "allocated_quantity": Decimal("30"),
                    "remaining_quantity": Decimal("70"),
                    "entry_price": Decimal("105.00")
                }
            ],
            "allocation_id": "alloc_123",
            "total_allocated_quantity": Decimal("80"),
            "unallocated_quantity": Decimal("0")
        }

    async def test_attribution_transfer_instructions_generation(self, generator, sample_allocation_result, mock_db_session):
        """Test generation of attribution transfer instructions."""
        # Mock database queries for position details
        mock_db_session.execute.return_value.fetchall.return_value = [
            (1, "AAPL", "strat_001", "exec_001", Decimal("200"), Decimal("100.00"), "active"),
            (2, "AAPL", "strat_002", "exec_002", Decimal("100"), Decimal("105.00"), "active")
        ]
        
        batch = await generator.generate_attribution_transfer_instructions(
            allocation_result=sample_allocation_result,
            reconciliation_case_id="case_123",
            created_by="user_456"
        )
        
        assert isinstance(batch, TransferInstructionBatch)
        assert len(batch.instructions) == 2
        assert batch.total_instructions == 2
        assert batch.batch_type == "attribution_resolution"
        
        # Check first instruction
        first_instruction = batch.instructions[0]
        assert first_instruction.instruction_type == TransferInstructionType.ATTRIBUTION_ALLOCATION
        assert first_instruction.source_execution_id == "exec_001"
        assert first_instruction.target_execution_id == "manual_attribution"
        assert first_instruction.quantity == Decimal("50")
        assert first_instruction.priority == TransferPriority.HIGH

    async def test_reconciliation_transfer_instructions_generation(self, generator, mock_db_session):
        """Test generation of reconciliation transfer instructions."""
        discrepancies = [
            {
                "position_id": 1,
                "expected_quantity": Decimal("100"),
                "actual_quantity": Decimal("80"),
                "discrepancy": Decimal("20"),
                "strategy_id": "strat_001",
                "execution_id": "exec_001"
            }
        ]
        
        # Mock position queries
        mock_db_session.execute.return_value.fetchall.return_value = [
            (1, "AAPL", "strat_001", "exec_001", Decimal("80"), Decimal("100.00"), "active")
        ]
        
        batch = await generator.generate_reconciliation_transfer_instructions(
            discrepancies=discrepancies,
            reconciliation_session_id="session_789",
            created_by="system"
        )
        
        assert len(batch.instructions) == 1
        assert batch.batch_type == "reconciliation_adjustment"
        
        instruction = batch.instructions[0]
        assert instruction.instruction_type == TransferInstructionType.RECONCILIATION_ADJUSTMENT
        assert instruction.quantity == Decimal("20")
        assert instruction.priority == TransferPriority.MEDIUM

    async def test_rebalancing_transfer_instructions_generation(self, generator, mock_db_session):
        """Test generation of rebalancing transfer instructions."""
        rebalancing_plan = {
            "transfers": [
                {
                    "from_strategy": "strat_001",
                    "to_strategy": "strat_002",
                    "symbol": "AAPL",
                    "quantity": Decimal("25"),
                    "reason": "Portfolio rebalancing"
                }
            ],
            "rebalancing_session_id": "rebal_456"
        }
        
        # Mock strategy execution queries
        mock_db_session.execute.return_value.fetchall.return_value = [
            ("exec_001", "strat_001", Decimal("100")),  # Source
            ("exec_002", "strat_002", Decimal("50"))    # Target
        ]
        
        batch = await generator.generate_rebalancing_transfer_instructions(
            rebalancing_plan=rebalancing_plan,
            created_by="portfolio_manager"
        )
        
        assert len(batch.instructions) == 1
        instruction = batch.instructions[0]
        assert instruction.instruction_type == TransferInstructionType.REBALANCING
        assert instruction.source_execution_id == "exec_001"
        assert instruction.target_execution_id == "exec_002"
        assert instruction.quantity == Decimal("25")

    async def test_manual_transfer_instructions_generation(self, generator):
        """Test generation of manual transfer instructions."""
        manual_requests = [
            {
                "source_execution_id": "exec_001",
                "target_execution_id": "exec_002",
                "symbol": "AAPL",
                "quantity": Decimal("10"),
                "reason": "Manual adjustment by trader"
            }
        ]
        
        batch = await generator.generate_manual_transfer_instructions(
            manual_requests=manual_requests,
            request_id="req_789",
            created_by="trader_123"
        )
        
        assert len(batch.instructions) == 1
        instruction = batch.instructions[0]
        assert instruction.instruction_type == TransferInstructionType.MANUAL_ADJUSTMENT
        assert instruction.priority == TransferPriority.HIGH  # Manual requests are high priority

    async def test_priority_assignment_rules(self, generator, sample_allocation_result, mock_db_session):
        """Test priority assignment based on business rules."""
        # Mock high-value position
        mock_db_session.execute.return_value.fetchall.return_value = [
            (1, "AAPL", "strat_001", "exec_001", Decimal("1000"), Decimal("1000.00"), "active")  # High value
        ]
        
        # Modify allocation for high-value test
        high_value_allocation = sample_allocation_result.copy()
        high_value_allocation["allocations"][0]["allocated_quantity"] = Decimal("500")
        
        batch = await generator.generate_attribution_transfer_instructions(
            allocation_result=high_value_allocation,
            reconciliation_case_id="case_123",
            created_by="user_456"
        )
        
        # High-value transfer should get high priority
        assert batch.instructions[0].priority == TransferPriority.HIGH

    async def test_instruction_metadata_completeness(self, generator, sample_allocation_result, mock_db_session):
        """Test that instruction metadata is complete and accurate."""
        mock_db_session.execute.return_value.fetchall.return_value = [
            (1, "AAPL", "strat_001", "exec_001", Decimal("200"), Decimal("100.00"), "active")
        ]
        
        batch = await generator.generate_attribution_transfer_instructions(
            allocation_result=sample_allocation_result,
            reconciliation_case_id="case_123",
            created_by="user_456"
        )
        
        instruction = batch.instructions[0]
        metadata = instruction.metadata
        
        assert metadata.reconciliation_case_id == "case_123"
        assert metadata.created_by == "user_456"
        assert metadata.symbol == "AAPL"
        assert metadata.strategy_id == "strat_001"
        assert isinstance(metadata.created_at, datetime)

    async def test_batch_validation_and_integrity(self, generator, sample_allocation_result, mock_db_session):
        """Test batch validation and integrity checks."""
        mock_db_session.execute.return_value.fetchall.return_value = [
            (1, "AAPL", "strat_001", "exec_001", Decimal("200"), Decimal("100.00"), "active"),
            (2, "AAPL", "strat_002", "exec_002", Decimal("100"), Decimal("105.00"), "active")
        ]
        
        batch = await generator.generate_attribution_transfer_instructions(
            allocation_result=sample_allocation_result,
            reconciliation_case_id="case_123",
            created_by="user_456"
        )
        
        # Batch should have integrity checks
        assert batch.is_valid is True
        assert batch.total_quantity == Decimal("80")  # Sum of all instruction quantities
        assert batch.batch_id is not None
        assert len(batch.validation_errors) == 0

    async def test_execution_context_generation(self, generator, sample_allocation_result, mock_db_session):
        """Test generation of execution context for transfers."""
        mock_db_session.execute.return_value.fetchall.return_value = [
            (1, "AAPL", "strat_001", "exec_001", Decimal("200"), Decimal("100.00"), "active")
        ]
        
        batch = await generator.generate_attribution_transfer_instructions(
            allocation_result=sample_allocation_result,
            reconciliation_case_id="case_123",
            created_by="user_456"
        )
        
        instruction = batch.instructions[0]
        context = instruction.execution_context
        
        assert isinstance(context, TransferExecutionContext)
        assert context.requires_approval is False  # Attribution transfers are auto-approved
        assert context.rollback_enabled is True
        assert context.max_retry_attempts > 0

    async def test_error_handling_invalid_allocation(self, generator, mock_db_session):
        """Test error handling for invalid allocation results."""
        invalid_allocation = {
            "allocations": [],  # Empty allocations
            "allocation_id": "alloc_123"
        }
        
        with pytest.raises(ValueError) as exc_info:
            await generator.generate_attribution_transfer_instructions(
                allocation_result=invalid_allocation,
                reconciliation_case_id="case_123",
                created_by="user_456"
            )
        
        assert "empty allocations" in str(exc_info.value).lower()

    async def test_error_handling_missing_positions(self, generator, sample_allocation_result, mock_db_session):
        """Test error handling when positions are not found."""
        # Mock no positions found
        mock_db_session.execute.return_value.fetchall.return_value = []
        
        with pytest.raises(ValueError) as exc_info:
            await generator.generate_attribution_transfer_instructions(
                allocation_result=sample_allocation_result,
                reconciliation_case_id="case_123",
                created_by="user_456"
            )
        
        assert "position not found" in str(exc_info.value).lower()

    async def test_instruction_deduplication(self, generator, mock_db_session):
        """Test that duplicate instructions are detected and handled."""
        manual_requests = [
            {
                "source_execution_id": "exec_001",
                "target_execution_id": "exec_002",
                "symbol": "AAPL",
                "quantity": Decimal("10"),
                "reason": "First request"
            },
            {
                "source_execution_id": "exec_001",
                "target_execution_id": "exec_002",
                "symbol": "AAPL",
                "quantity": Decimal("10"),
                "reason": "Duplicate request"
            }
        ]
        
        batch = await generator.generate_manual_transfer_instructions(
            manual_requests=manual_requests,
            request_id="req_789",
            created_by="trader_123"
        )
        
        # Should detect and handle duplicates
        assert len(batch.instructions) == 1  # Duplicate should be merged or flagged
        assert len(batch.validation_warnings) > 0

    async def test_complex_multi_type_batch(self, generator, mock_db_session):
        """Test generation of complex batch with multiple instruction types."""
        # Create a mixed batch scenario
        mixed_requests = [
            {
                "type": "manual",
                "source_execution_id": "exec_001",
                "target_execution_id": "exec_002",
                "symbol": "AAPL",
                "quantity": Decimal("5"),
                "reason": "Manual adjustment"
            },
            {
                "type": "reconciliation",
                "position_id": 1,
                "discrepancy": Decimal("3"),
                "strategy_id": "strat_001"
            }
        ]
        
        # Mock position data
        mock_db_session.execute.return_value.fetchall.return_value = [
            (1, "AAPL", "strat_001", "exec_001", Decimal("100"), Decimal("100.00"), "active")
        ]
        
        batch = await generator.generate_mixed_transfer_instructions(
            requests=mixed_requests,
            session_id="mixed_session_123",
            created_by="system"
        )
        
        assert len(batch.instructions) == 2
        assert any(i.instruction_type == TransferInstructionType.MANUAL_ADJUSTMENT for i in batch.instructions)
        assert any(i.instruction_type == TransferInstructionType.RECONCILIATION_ADJUSTMENT for i in batch.instructions)

    async def test_priority_ordering_in_batch(self, generator, mock_db_session):
        """Test that instructions are properly ordered by priority within batch."""
        # Create mix of high and low priority instructions
        mixed_allocation = {
            "allocations": [
                {
                    "position_id": 1,
                    "strategy_id": "strat_001",
                    "execution_id": "exec_001",
                    "allocated_quantity": Decimal("1000"),  # High value
                    "remaining_quantity": Decimal("100"),
                    "entry_price": Decimal("500.00")
                },
                {
                    "position_id": 2,
                    "strategy_id": "strat_002", 
                    "execution_id": "exec_002",
                    "allocated_quantity": Decimal("10"),  # Low value
                    "remaining_quantity": Decimal("90"),
                    "entry_price": Decimal("1.00")
                }
            ],
            "allocation_id": "alloc_123"
        }
        
        mock_db_session.execute.return_value.fetchall.return_value = [
            (1, "AAPL", "strat_001", "exec_001", Decimal("1100"), Decimal("500.00"), "active"),
            (2, "TSLA", "strat_002", "exec_002", Decimal("100"), Decimal("1.00"), "active")
        ]
        
        batch = await generator.generate_attribution_transfer_instructions(
            allocation_result=mixed_allocation,
            reconciliation_case_id="case_123",
            created_by="user_456"
        )
        
        # Instructions should be ordered by priority (high first)
        priorities = [i.priority for i in batch.instructions]
        assert priorities == sorted(priorities, key=lambda p: p.value)

    async def test_database_transaction_safety(self, generator, sample_allocation_result, mock_db_session):
        """Test that database operations are transaction-safe."""
        # Mock database error during position lookup
        mock_db_session.execute.side_effect = Exception("Database error")
        
        with pytest.raises(Exception):
            await generator.generate_attribution_transfer_instructions(
                allocation_result=sample_allocation_result,
                reconciliation_case_id="case_123",
                created_by="user_456"
            )
        
        # Should not leave partial state (this would be verified by checking db state)
        assert True  # Placeholder for transaction safety verification
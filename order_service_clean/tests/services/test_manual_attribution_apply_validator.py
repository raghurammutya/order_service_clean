"""
Test Manual Attribution Apply Validator (GAP-REC-9)
Tests pre-apply validation for manual attribution decisions with transfer safety
"""

import pytest
from decimal import Decimal
from datetime import datetime
from unittest.mock import patch

from order_service.app.services.manual_attribution_apply_validator import (
    ManualAttributionApplyValidator,
    ManualAttributionDecision,
    ValidationFailure,
    FailureType
)


@pytest.mark.asyncio
class TestManualAttributionApplyValidator:
    """Test manual attribution apply validation."""

    @pytest.fixture
    def validator(self, mock_db_session):
        """Create validator instance."""
        return ManualAttributionApplyValidator(mock_db_session)

    @pytest.fixture
    def valid_decision(self):
        """Valid attribution decision."""
        return ManualAttributionDecision(
            case_id="case_123",
            decision_maker="user_456",
            allocation_decisions=[
                {"position_id": 1, "quantity": 50, "strategy_id": "strat_1"}
            ],
            decision_rationale="Test allocation",
            exit_quantity=Decimal("50"),
            symbol="AAPL",
            trading_account_id="acc_001"
        )

    async def test_valid_decision_passes_validation(self, validator, valid_decision, mock_db_session):
        """Test that valid decision passes all validation."""
        # Mock position exists with sufficient quantity
        mock_db_session.execute.return_value.fetchone.return_value = (
            1, "acc_001", "AAPL", 100, Decimal("150"), "strat_1", datetime.now(), "active"
        )
        
        result = await validator.validate_manual_attribution_decision(valid_decision)
        
        assert result.can_proceed is True
        assert len(result.failures) == 0
        assert len(result.warnings) == 0

    async def test_position_not_found_fails_validation(self, validator, valid_decision, mock_db_session):
        """Test validation fails when position doesn't exist."""
        # Mock position not found
        mock_db_session.execute.return_value.fetchone.return_value = None
        
        result = await validator.validate_manual_attribution_decision(valid_decision)
        
        assert result.can_proceed is False
        assert len(result.failures) == 1
        assert result.failures[0].failure_type == FailureType.POSITION_NOT_FOUND
        assert "Position 1 not found" in result.failures[0].description

    async def test_insufficient_quantity_fails_validation(self, validator, valid_decision, mock_db_session):
        """Test validation fails when position has insufficient quantity."""
        # Mock position with insufficient quantity
        mock_db_session.execute.return_value.fetchone.return_value = (
            1, "acc_001", "AAPL", 30, Decimal("150"), "strat_1", datetime.now(), "active"  # Only 30, need 50
        )
        
        result = await validator.validate_manual_attribution_decision(valid_decision)
        
        assert result.can_proceed is False
        assert len(result.failures) == 1
        assert result.failures[0].failure_type == FailureType.INSUFFICIENT_QUANTITY
        assert "insufficient quantity" in result.failures[0].description.lower()

    async def test_wrong_account_fails_validation(self, validator, valid_decision, mock_db_session):
        """Test validation fails when position belongs to different account."""
        # Mock position from different account
        mock_db_session.execute.return_value.fetchone.return_value = (
            1, "different_acc", "AAPL", 100, Decimal("150"), "strat_1", datetime.now(), "active"
        )
        
        result = await validator.validate_manual_attribution_decision(valid_decision)
        
        assert result.can_proceed is False
        assert len(result.failures) == 1
        assert result.failures[0].failure_type == FailureType.ACCOUNT_MISMATCH

    async def test_wrong_symbol_fails_validation(self, validator, valid_decision, mock_db_session):
        """Test validation fails when position is for different symbol."""
        # Mock position for different symbol
        mock_db_session.execute.return_value.fetchone.return_value = (
            1, "acc_001", "TSLA", 100, Decimal("150"), "strat_1", datetime.now(), "active"  # Different symbol
        )
        
        result = await validator.validate_manual_attribution_decision(valid_decision)
        
        assert result.can_proceed is False
        assert len(result.failures) == 1
        assert result.failures[0].failure_type == FailureType.SYMBOL_MISMATCH

    async def test_quantity_mismatch_fails_validation(self, validator, mock_db_session):
        """Test validation fails when allocation quantities don't sum to exit quantity."""
        decision = ManualAttributionDecision(
            case_id="case_123",
            decision_maker="user_456",
            allocation_decisions=[
                {"position_id": 1, "quantity": 30, "strategy_id": "strat_1"},  # Total 40, need 50
                {"position_id": 2, "quantity": 10, "strategy_id": "strat_2"}
            ],
            decision_rationale="Test allocation",
            exit_quantity=Decimal("50"),
            symbol="AAPL",
            trading_account_id="acc_001"
        )
        
        # Mock both positions exist
        mock_db_session.execute.return_value.fetchone.side_effect = [
            (1, "acc_001", "AAPL", 100, Decimal("150"), "strat_1", datetime.now(), "active"),
            (2, "acc_001", "AAPL", 100, Decimal("150"), "strat_2", datetime.now(), "active")
        ]
        
        result = await validator.validate_manual_attribution_decision(decision)
        
        assert result.can_proceed is False
        assert len(result.failures) == 1
        assert result.failures[0].failure_type == FailureType.QUANTITY_MISMATCH

    async def test_duplicate_position_fails_validation(self, validator, mock_db_session):
        """Test validation fails when same position is allocated multiple times."""
        decision = ManualAttributionDecision(
            case_id="case_123",
            decision_maker="user_456",
            allocation_decisions=[
                {"position_id": 1, "quantity": 25, "strategy_id": "strat_1"},
                {"position_id": 1, "quantity": 25, "strategy_id": "strat_1"}  # Duplicate
            ],
            decision_rationale="Test allocation",
            exit_quantity=Decimal("50"),
            symbol="AAPL",
            trading_account_id="acc_001"
        )
        
        result = await validator.validate_manual_attribution_decision(decision)
        
        assert result.can_proceed is False
        assert len(result.failures) == 1
        assert result.failures[0].failure_type == FailureType.DUPLICATE_POSITION

    async def test_pending_transfers_warning(self, validator, valid_decision, mock_db_session):
        """Test warning when position has pending transfers."""
        # Mock position exists
        mock_db_session.execute.return_value.fetchone.return_value = (
            1, "acc_001", "AAPL", 100, Decimal("150"), "strat_1", datetime.now(), "active"
        )
        
        # Mock pending transfers check
        with patch.object(validator, '_check_pending_transfers', return_value=[
            {"transfer_id": "tf_123", "quantity": 20, "status": "pending"}
        ]):
            result = await validator.validate_manual_attribution_decision(valid_decision)
        
        assert result.can_proceed is True  # Warnings don't block
        assert len(result.warnings) == 1
        assert "pending transfer" in result.warnings[0].lower()

    async def test_concurrent_modification_check(self, validator, valid_decision, mock_db_session):
        """Test concurrent modification detection."""
        # Mock position exists
        mock_db_session.execute.return_value.fetchone.return_value = (
            1, "acc_001", "AAPL", 100, Decimal("150"), "strat_1", datetime.now(), "active"
        )
        
        # Mock recent modification
        with patch.object(validator, '_check_recent_modifications', return_value=[
            {"modification_time": datetime.now(), "modified_by": "other_user", "change_type": "quantity_update"}
        ]):
            result = await validator.validate_manual_attribution_decision(valid_decision)
        
        assert result.can_proceed is True  # Warning, not failure
        assert len(result.warnings) == 1
        assert "concurrent modification" in result.warnings[0].lower()

    async def test_transfer_safety_validation(self, validator, valid_decision, mock_db_session):
        """Test transfer safety validation checks."""
        # Mock position exists
        mock_db_session.execute.return_value.fetchone.return_value = (
            1, "acc_001", "AAPL", 100, Decimal("150"), "strat_1", datetime.now(), "active"
        )
        
        # Mock transfer safety check finds issues
        with patch.object(validator, '_validate_transfer_safety', return_value=[
            ValidationFailure(
                failure_type=FailureType.TRANSFER_CONFLICT,
                description="Transfer would conflict with existing order",
                position_id=1,
                details={"conflicting_order_id": "ord_123"}
            )
        ]):
            result = await validator.validate_manual_attribution_decision(valid_decision)
        
        assert result.can_proceed is False
        assert len(result.failures) == 1
        assert result.failures[0].failure_type == FailureType.TRANSFER_CONFLICT

    async def test_empty_allocation_decisions_fails(self, validator, mock_db_session):
        """Test validation fails with no allocation decisions."""
        decision = ManualAttributionDecision(
            case_id="case_123",
            decision_maker="user_456",
            allocation_decisions=[],  # Empty
            decision_rationale="Test allocation",
            exit_quantity=Decimal("50"),
            symbol="AAPL",
            trading_account_id="acc_001"
        )
        
        result = await validator.validate_manual_attribution_decision(decision)
        
        assert result.can_proceed is False
        assert len(result.failures) == 1
        assert result.failures[0].failure_type == FailureType.NO_ALLOCATIONS

    async def test_validation_with_database_error(self, validator, valid_decision, mock_db_session):
        """Test validation handles database errors gracefully."""
        # Mock database error
        mock_db_session.execute.side_effect = Exception("Database connection failed")
        
        with pytest.raises(Exception):
            await validator.validate_manual_attribution_decision(valid_decision)

    async def test_position_validation_query_structure(self, validator, valid_decision, mock_db_session):
        """Test that position validation uses correct SQL query."""
        # Mock position exists
        mock_db_session.execute.return_value.fetchone.return_value = (
            1, "acc_001", "AAPL", 100, Decimal("150"), "strat_1", datetime.now(), "active"
        )
        
        await validator.validate_manual_attribution_decision(valid_decision)
        
        # Verify query was called
        mock_db_session.execute.assert_called()
        call_args = mock_db_session.execute.call_args[0]
        query_text = str(call_args[0])
        
        # Should query positions table with proper filters
        assert "order_service.positions" in query_text
        assert "position_id" in query_text
        assert "trading_account_id" in query_text
        assert "symbol" in query_text

    async def test_multiple_positions_validation(self, validator, mock_db_session):
        """Test validation with multiple position allocations."""
        decision = ManualAttributionDecision(
            case_id="case_123",
            decision_maker="user_456",
            allocation_decisions=[
                {"position_id": 1, "quantity": 30, "strategy_id": "strat_1"},
                {"position_id": 2, "quantity": 20, "strategy_id": "strat_2"}
            ],
            decision_rationale="Multi-position allocation",
            exit_quantity=Decimal("50"),
            symbol="AAPL",
            trading_account_id="acc_001"
        )
        
        # Mock both positions exist with sufficient quantities
        mock_db_session.execute.return_value.fetchone.side_effect = [
            (1, "acc_001", "AAPL", 50, Decimal("150"), "strat_1", datetime.now(), "active"),
            (2, "acc_001", "AAPL", 30, Decimal("155"), "strat_2", datetime.now(), "active")
        ]
        
        result = await validator.validate_manual_attribution_decision(decision)
        
        assert result.can_proceed is True
        assert len(result.failures) == 0
        # Should have called position validation for each position
        assert mock_db_session.execute.call_count >= 2

    async def test_negative_quantity_fails_validation(self, validator, mock_db_session):
        """Test validation fails with negative allocation quantities."""
        decision = ManualAttributionDecision(
            case_id="case_123",
            decision_maker="user_456",
            allocation_decisions=[
                {"position_id": 1, "quantity": -10, "strategy_id": "strat_1"}  # Negative
            ],
            decision_rationale="Test allocation",
            exit_quantity=Decimal("50"),
            symbol="AAPL",
            trading_account_id="acc_001"
        )
        
        result = await validator.validate_manual_attribution_decision(decision)
        
        assert result.can_proceed is False
        assert len(result.failures) == 1
        assert result.failures[0].failure_type == FailureType.INVALID_QUANTITY
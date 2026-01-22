"""
Tests for Manual Attribution Service

Tests the manual attribution workflow including case management, 
decision capture, and resolution application.
"""

import pytest
from decimal import Decimal
from datetime import datetime, timezone, timedelta
from unittest.mock import AsyncMock, MagicMock, patch
from sqlalchemy.ext.asyncio import AsyncSession

from order_service.app.services.manual_attribution_service import (
    ManualAttributionService,
    AttributionCase,
    AttributionDecision,
    AttributionStatus,
    AttributionPriority,
    create_attribution_case
)


class TestManualAttributionService:
    """Test cases for ManualAttributionService."""

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
        """Manual attribution service instance."""
        return ManualAttributionService(mock_db)

    @pytest.fixture
    def sample_positions(self):
        """Sample affected positions for testing."""
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
    def sample_case_data(self, sample_positions):
        """Sample case data for testing."""
        return {
            "trading_account_id": "acc_001",
            "symbol": "AAPL",
            "exit_quantity": Decimal("75"),
            "exit_price": Decimal("170.00"),
            "exit_timestamp": datetime.now(timezone.utc),
            "affected_positions": sample_positions,
            "suggested_allocation": None,
            "priority": AttributionPriority.NORMAL,
            "context": {"reason": "Automatic attribution failed"}
        }

    async def test_create_attribution_case_success(self, service, mock_db, sample_case_data):
        """Test successful creation of attribution case."""
        case_id = await service.create_attribution_case(**sample_case_data)

        assert case_id is not None
        assert len(case_id) > 0

        # Verify database calls
        assert mock_db.execute.call_count >= 2  # Insert case + audit event
        mock_db.commit.assert_called()

        # Verify case data structure in database call
        call_args = mock_db.execute.call_args_list[0][1]
        assert call_args["trading_account_id"] == "acc_001"
        assert call_args["symbol"] == "AAPL"
        assert call_args["status"] == AttributionStatus.PENDING.value

    async def test_priority_auto_determination(self, service, mock_db, sample_case_data):
        """Test automatic priority determination based on trade characteristics."""
        # High value trade should get HIGH priority
        high_value_case = sample_case_data.copy()
        high_value_case["exit_quantity"] = Decimal("10000")  # Large quantity
        high_value_case["exit_price"] = Decimal("200.00")   # High price

        case_id = await service.create_attribution_case(**high_value_case)

        # Check that priority was elevated in the database call
        call_args = mock_db.execute.call_args_list[0][1]
        case_data = call_args["case_data"]
        assert case_data["priority"] == AttributionPriority.HIGH.value

    async def test_get_attribution_case_found(self, service, mock_db, sample_positions):
        """Test retrieving existing attribution case."""
        case_id = "test_case_123"
        
        # Mock database response for case
        mock_case_result = MagicMock()
        mock_case_result.fetchone.return_value = (
            case_id,
            "acc_001",
            "AAPL",
            {
                "exit_quantity": "75",
                "exit_price": "170.00", 
                "exit_timestamp": datetime.now(timezone.utc).isoformat(),
                "affected_positions": sample_positions,
                "suggested_allocation": None
            },
            AttributionStatus.PENDING.value,
            AttributionPriority.NORMAL.value,
            datetime.now(timezone.utc),
            datetime.now(timezone.utc),
            None,  # assigned_to
            None   # resolution_data
        )
        
        # Mock audit trail response
        mock_audit_result = MagicMock()
        mock_audit_result.fetchall.return_value = [
            ("case_created", "system", {"priority": "normal"}, datetime.now(timezone.utc))
        ]
        
        mock_db.execute.side_effect = [mock_case_result, mock_audit_result]

        case = await service.get_attribution_case(case_id)

        assert case is not None
        assert case.case_id == case_id
        assert case.trading_account_id == "acc_001"
        assert case.symbol == "AAPL"
        assert case.exit_quantity == Decimal("75")
        assert case.status == AttributionStatus.PENDING
        assert len(case.audit_trail) == 1

    async def test_get_attribution_case_not_found(self, service, mock_db):
        """Test retrieving non-existent attribution case."""
        mock_result = MagicMock()
        mock_result.fetchone.return_value = None
        mock_db.execute.return_value = mock_result

        case = await service.get_attribution_case("nonexistent_case")

        assert case is None

    async def test_list_pending_cases_with_filters(self, service, mock_db, sample_positions):
        """Test listing pending cases with various filters."""
        mock_result = MagicMock()
        mock_result.fetchall.return_value = [
            (
                "case_1", "acc_001", "AAPL",
                {
                    "exit_quantity": "100",
                    "exit_price": "170.00",
                    "exit_timestamp": datetime.now(timezone.utc).isoformat(),
                    "affected_positions": sample_positions
                },
                AttributionStatus.PENDING.value,
                AttributionPriority.HIGH.value,
                datetime.now(timezone.utc),
                datetime.now(timezone.utc),
                None, None
            ),
            (
                "case_2", "acc_001", "MSFT", 
                {
                    "exit_quantity": "50",
                    "exit_price": "300.00",
                    "exit_timestamp": datetime.now(timezone.utc).isoformat(),
                    "affected_positions": sample_positions
                },
                AttributionStatus.IN_PROGRESS.value,
                AttributionPriority.NORMAL.value,
                datetime.now(timezone.utc),
                datetime.now(timezone.utc),
                "user_123", None
            )
        ]
        mock_db.execute.return_value = mock_result

        cases = await service.list_pending_cases(
            trading_account_id="acc_001",
            priority=AttributionPriority.HIGH,
            limit=10
        )

        assert len(cases) == 2
        assert cases[0].case_id == "case_1" 
        assert cases[0].priority == AttributionPriority.HIGH
        assert cases[1].case_id == "case_2"
        assert cases[1].assigned_to == "user_123"

    async def test_assign_case_success(self, service, mock_db):
        """Test successful case assignment."""
        case_id = "test_case"
        assigned_to = "user_456"
        assigned_by = "manager_123"

        # Mock case exists and is assignable
        mock_result = MagicMock()
        mock_result.fetchone.return_value = (AttributionStatus.PENDING.value,)
        mock_db.execute.return_value = mock_result

        success = await service.assign_case(case_id, assigned_to, assigned_by)

        assert success is True
        
        # Verify database operations
        assert mock_db.execute.call_count >= 2  # Check + update
        mock_db.commit.assert_called()

    async def test_assign_case_invalid_status(self, service, mock_db):
        """Test assignment failure when case has invalid status."""
        case_id = "test_case"
        
        # Mock case with non-assignable status
        mock_result = MagicMock()
        mock_result.fetchone.return_value = (AttributionStatus.APPLIED.value,)
        mock_db.execute.return_value = mock_result

        with pytest.raises(ValueError) as exc_info:
            await service.assign_case(case_id, "user_456", "manager_123")

        assert "cannot be assigned" in str(exc_info.value)

    async def test_resolve_case_success(self, service, mock_db, sample_positions):
        """Test successful case resolution with decision."""
        case_id = "test_case"
        
        # Mock case retrieval
        case = AttributionCase(
            case_id=case_id,
            trading_account_id="acc_001",
            symbol="AAPL",
            exit_quantity=Decimal("75"),
            exit_price=Decimal("170.00"),
            exit_timestamp=datetime.now(timezone.utc),
            affected_positions=sample_positions,
            suggested_allocation=None,
            status=AttributionStatus.IN_PROGRESS,
            priority=AttributionPriority.NORMAL,
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
            assigned_to="user_456",
            resolution_data=None,
            audit_trail=[]
        )

        # Mock get_attribution_case
        with patch.object(service, 'get_attribution_case', return_value=case):
            decision = AttributionDecision(
                case_id=case_id,
                decision_maker="user_456",
                allocation_decisions=[
                    {"position_id": "pos_1", "quantity": 50, "strategy_id": 1},
                    {"position_id": "pos_2", "quantity": 25, "strategy_id": 2}
                ],
                decision_rationale="Manual review determined best allocation",
                decision_timestamp=datetime.now(timezone.utc)
            )

            success = await service.resolve_case(case_id, decision)

            assert success is True
            mock_db.commit.assert_called()

    async def test_resolve_case_invalid_allocation(self, service, mock_db, sample_positions):
        """Test resolution failure with invalid allocation decisions."""
        case_id = "test_case"
        
        case = AttributionCase(
            case_id=case_id,
            trading_account_id="acc_001", 
            symbol="AAPL",
            exit_quantity=Decimal("75"),
            exit_price=Decimal("170.00"),
            exit_timestamp=datetime.now(timezone.utc),
            affected_positions=sample_positions,
            suggested_allocation=None,
            status=AttributionStatus.IN_PROGRESS,
            priority=AttributionPriority.NORMAL,
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
            assigned_to="user_456",
            resolution_data=None,
            audit_trail=[]
        )

        with patch.object(service, 'get_attribution_case', return_value=case):
            # Decision with wrong total quantity
            decision = AttributionDecision(
                case_id=case_id,
                decision_maker="user_456",
                allocation_decisions=[
                    {"position_id": "pos_1", "quantity": 100}  # Wrong total (should be 75)
                ],
                decision_rationale="Test decision",
                decision_timestamp=datetime.now(timezone.utc)
            )

            with pytest.raises(ValueError) as exc_info:
                await service.resolve_case(case_id, decision)

            assert "does not match exit quantity" in str(exc_info.value)

    async def test_apply_resolution_success(self, service, mock_db, sample_positions):
        """Test successful application of resolution to positions."""
        case_id = "test_case"
        
        # Mock resolved case with resolution data
        case = AttributionCase(
            case_id=case_id,
            trading_account_id="acc_001",
            symbol="AAPL",
            exit_quantity=Decimal("75"),
            exit_price=Decimal("170.00"),
            exit_timestamp=datetime.now(timezone.utc),
            affected_positions=sample_positions,
            suggested_allocation=None,
            status=AttributionStatus.RESOLVED,
            priority=AttributionPriority.NORMAL,
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
            assigned_to="user_456",
            resolution_data={
                "decision_maker": "user_456",
                "allocation_decisions": [
                    {"position_id": "pos_1", "quantity": 50, "strategy_id": 1},
                    {"position_id": "pos_2", "quantity": 25, "strategy_id": 2}
                ],
                "decision_rationale": "Test resolution"
            },
            audit_trail=[]
        )

        # Mock successful transfer execution
        mock_transfer_result = MagicMock()
        mock_transfer_result.transfer_id = "transfer_123"
        mock_transfer_result.instructions_count = 2
        mock_transfer_result.executed_count = 2
        mock_transfer_result.failed_count = 0
        mock_transfer_result.total_quantity_transferred = Decimal("75")

        with patch.object(service, 'get_attribution_case', return_value=case):
            with patch('order_service.app.services.manual_attribution_service.ReconciliationDrivenTransferService') as mock_transfer_service:
                mock_transfer_service_instance = mock_transfer_service.return_value
                mock_transfer_service_instance.execute_manual_transfer_instructions.return_value = mock_transfer_result

                success = await service.apply_resolution(case_id, "user_456")

                assert success is True
                mock_db.commit.assert_called()
                mock_transfer_service_instance.execute_manual_transfer_instructions.assert_called_once()

    async def test_apply_resolution_not_resolved(self, service, mock_db, sample_positions):
        """Test application failure when case is not resolved."""
        case_id = "test_case"
        
        case = AttributionCase(
            case_id=case_id,
            trading_account_id="acc_001",
            symbol="AAPL", 
            exit_quantity=Decimal("75"),
            exit_price=Decimal("170.00"),
            exit_timestamp=datetime.now(timezone.utc),
            affected_positions=sample_positions,
            suggested_allocation=None,
            status=AttributionStatus.PENDING,  # Not resolved yet
            priority=AttributionPriority.NORMAL,
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
            assigned_to="user_456",
            resolution_data=None,
            audit_trail=[]
        )

        with patch.object(service, 'get_attribution_case', return_value=case):
            with pytest.raises(ValueError) as exc_info:
                await service.apply_resolution(case_id, "user_456")

            assert "is not resolved" in str(exc_info.value)

    async def test_audit_trail_creation(self, service, mock_db, sample_case_data):
        """Test that proper audit trail is created for all operations."""
        # Create case
        case_id = await service.create_attribution_case(**sample_case_data)

        # Verify audit event was recorded
        audit_calls = [call for call in mock_db.execute.call_args_list 
                      if "manual_attribution_audit" in str(call)]
        assert len(audit_calls) >= 1

    async def test_error_handling_and_rollback(self, service, mock_db, sample_case_data):
        """Test error handling and transaction rollback."""
        # Make database operation fail
        mock_db.execute.side_effect = [MagicMock(), Exception("Database error")]

        with pytest.raises(Exception) as exc_info:
            await service.create_attribution_case(**sample_case_data)

        assert "Database error" in str(exc_info.value)
        mock_db.rollback.assert_called_once()

    async def test_priority_determination_edge_cases(self, service, mock_db):
        """Test priority determination for various edge cases."""
        # Very high value trade
        high_value_positions = [{"position_id": "pos_1", "quantity": 1000}]
        priority = service._determine_priority(
            Decimal("10000"), Decimal("200"), high_value_positions
        )
        assert priority == AttributionPriority.HIGH

        # Many affected positions
        many_positions = [{"position_id": f"pos_{i}"} for i in range(5)]
        priority = service._determine_priority(
            Decimal("100"), Decimal("50"), many_positions
        )
        assert priority == AttributionPriority.HIGH

        # Small trade, few positions
        priority = service._determine_priority(
            Decimal("10"), Decimal("50"), [{"position_id": "pos_1"}]
        )
        assert priority == AttributionPriority.LOW

    async def test_concurrent_case_assignment(self, service, mock_db):
        """Test handling of concurrent case assignments."""
        # This would test race conditions in a real implementation
        # For now, just verify basic assignment logic
        case_id = "concurrent_case"
        
        mock_result = MagicMock()
        mock_result.fetchone.return_value = (AttributionStatus.PENDING.value,)
        mock_db.execute.return_value = mock_result

        success = await service.assign_case(case_id, "user_1", "manager_1")
        assert success is True

    async def test_case_expiry_handling(self, service, mock_db):
        """Test handling of expired cases (if implemented)."""
        # This would test automatic expiry of old unresolved cases
        # Implementation would depend on business requirements
        pass

    @pytest.mark.parametrize("status,expected_assignable", [
        (AttributionStatus.PENDING, True),
        (AttributionStatus.IN_PROGRESS, True), 
        (AttributionStatus.RESOLVED, False),
        (AttributionStatus.APPLIED, False),
        (AttributionStatus.FAILED, False),
        (AttributionStatus.EXPIRED, False)
    ])
    async def test_case_assignability(self, service, mock_db, status, expected_assignable):
        """Test case assignability for different statuses."""
        case_id = "test_case"
        
        mock_result = MagicMock()
        mock_result.fetchone.return_value = (status.value,)
        mock_db.execute.return_value = mock_result

        if expected_assignable:
            success = await service.assign_case(case_id, "user_1", "manager_1")
            assert success is True
        else:
            with pytest.raises(ValueError):
                await service.assign_case(case_id, "user_1", "manager_1")


class TestManualAttributionIntegration:
    """Integration tests for manual attribution workflow."""

    async def test_complete_attribution_workflow(self, real_db_session):
        """Test complete workflow from case creation to resolution application."""
        # This would test the complete workflow with a real database
        # Requires proper test database setup
        pass

    async def test_stress_test_many_cases(self, real_db_session):
        """Test performance with many concurrent attribution cases."""
        pass
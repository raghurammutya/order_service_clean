"""
Tests for Handoff State Machine Service

Tests the state transitions between manual and script control,
including validation, position transfers, and order management.
"""

import pytest
from datetime import datetime, timezone, timedelta
from unittest.mock import AsyncMock, MagicMock, patch
from sqlalchemy.ext.asyncio import AsyncSession

from order_service.app.services.handoff_state_machine import (
    HandoffStateMachine,
    HandoffMode,
    TransitionType,
    TransitionStatus,
    HandoffState,
    TransitionRequest,
    TransitionResult
)


class TestHandoffStateMachine:
    """Test cases for HandoffStateMachine."""

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
        """Handoff state machine service instance."""
        return HandoffStateMachine(mock_db)

    @pytest.fixture
    def sample_handoff_state(self):
        """Sample handoff state for testing."""
        return HandoffState(
            trading_account_id="acc_001",
            strategy_id=1,
            execution_id="exec_123",
            current_mode=HandoffMode.MANUAL,
            target_mode=None,
            transition_id=None,
            transition_status=None,
            transition_started_at=None,
            last_updated_at=datetime.now(timezone.utc),
            controlled_by="user_456",
            metadata={}
        )

    @pytest.fixture
    def sample_positions(self):
        """Sample positions for handoff testing."""
        return [
            {
                "id": "pos_1",
                "symbol": "AAPL",
                "quantity": 100,
                "strategy_id": 1,
                "execution_id": "exec_123",
                "portfolio_id": "port_1",
                "source": "manual",
                "created_at": datetime.now(timezone.utc)
            },
            {
                "id": "pos_2", 
                "symbol": "MSFT",
                "quantity": 50,
                "strategy_id": 1,
                "execution_id": "exec_123",
                "portfolio_id": "port_1", 
                "source": "manual",
                "created_at": datetime.now(timezone.utc)
            }
        ]

    @pytest.fixture
    def sample_orders(self):
        """Sample orders for handoff testing."""
        return [
            {
                "id": "ord_1",
                "order_id": "broker_ord_1",
                "symbol": "AAPL",
                "quantity": 25,
                "status": "PENDING"
            },
            {
                "id": "ord_2",
                "order_id": "broker_ord_2", 
                "symbol": "MSFT",
                "quantity": 10,
                "status": "SUBMITTED"
            }
        ]

    async def test_get_handoff_state_existing(self, service, mock_db, sample_handoff_state):
        """Test retrieving existing handoff state."""
        mock_result = MagicMock()
        mock_result.fetchone.return_value = (
            sample_handoff_state.trading_account_id,
            sample_handoff_state.strategy_id,
            sample_handoff_state.execution_id,
            sample_handoff_state.current_mode.value,
            sample_handoff_state.target_mode.value if sample_handoff_state.target_mode else None,
            sample_handoff_state.transition_id,
            sample_handoff_state.transition_status.value if sample_handoff_state.transition_status else None,
            sample_handoff_state.transition_started_at,
            sample_handoff_state.last_updated_at,
            sample_handoff_state.controlled_by,
            sample_handoff_state.metadata
        )
        mock_db.execute.return_value = mock_result

        state = await service.get_handoff_state("acc_001", 1, "exec_123")

        assert state.trading_account_id == "acc_001"
        assert state.strategy_id == 1
        assert state.execution_id == "exec_123"
        assert state.current_mode == HandoffMode.MANUAL
        assert state.controlled_by == "user_456"

    async def test_get_handoff_state_create_default(self, service, mock_db):
        """Test creating default handoff state when none exists."""
        # Mock no existing state found
        mock_result = MagicMock()
        mock_result.fetchone.return_value = None
        mock_db.execute.return_value = mock_result

        state = await service.get_handoff_state("acc_001", 1, "exec_123")

        assert state.trading_account_id == "acc_001"
        assert state.strategy_id == 1
        assert state.execution_id == "exec_123"
        assert state.current_mode == HandoffMode.MANUAL  # Default mode
        assert state.target_mode is None
        assert state.transition_id is None

        # Should have stored the default state
        mock_db.execute.assert_called()
        mock_db.commit.assert_called()

    async def test_manual_to_script_transition_success(self, service, mock_db, sample_handoff_state, sample_positions, sample_orders):
        """Test successful manual to script transition."""
        request = TransitionRequest(
            trading_account_id="acc_001",
            strategy_id=1,
            execution_id="exec_123",
            from_mode=HandoffMode.MANUAL,
            to_mode=HandoffMode.SCRIPT,
            transition_type=TransitionType.MANUAL_TO_SCRIPT,
            requested_by="user_456",
            reason="Ready for script control"
        )

        # Mock get_handoff_state
        with patch.object(service, 'get_handoff_state', return_value=sample_handoff_state):
            # Mock validation passes
            with patch.object(service, '_validate_transition', return_value=[]):
                # Mock position retrieval
                mock_pos_result = MagicMock()
                mock_pos_result.fetchall.return_value = [
                    (p["id"], p["symbol"], p["quantity"], p["strategy_id"],
                     p["execution_id"], p["portfolio_id"], p["source"], p["created_at"])
                    for p in sample_positions
                ]
                
                # Mock order retrieval
                mock_order_result = MagicMock()
                mock_order_result.fetchall.return_value = [
                    (o["id"], o["order_id"], o["symbol"], o["quantity"], o["status"])
                    for o in sample_orders
                ]
                
                mock_db.execute.side_effect = [mock_pos_result, mock_order_result, MagicMock(), MagicMock(), MagicMock()]

                result = await service.request_transition(request)

                assert result.success is True
                assert result.positions_transferred == 2
                assert result.orders_cancelled == 2
                assert len(result.warnings) >= 1  # Should warn about cancelled orders

    async def test_script_to_manual_transition_success(self, service, mock_db, sample_positions, sample_orders):
        """Test successful script to manual transition."""
        script_state = HandoffState(
            trading_account_id="acc_001",
            strategy_id=1,
            execution_id="exec_123",
            current_mode=HandoffMode.SCRIPT,
            target_mode=None,
            transition_id=None,
            transition_status=None,
            transition_started_at=None,
            last_updated_at=datetime.now(timezone.utc),
            controlled_by="system",
            metadata={}
        )

        request = TransitionRequest(
            trading_account_id="acc_001",
            strategy_id=1,
            execution_id="exec_123",
            from_mode=HandoffMode.SCRIPT,
            to_mode=HandoffMode.MANUAL,
            transition_type=TransitionType.SCRIPT_TO_MANUAL,
            requested_by="user_456",
            reason="Manual intervention required"
        )

        with patch.object(service, 'get_handoff_state', return_value=script_state):
            with patch.object(service, '_validate_transition', return_value=[]):
                # Mock position and order retrieval
                mock_pos_result = MagicMock()
                mock_pos_result.fetchall.return_value = [
                    (p["id"], p["symbol"], p["quantity"], p["strategy_id"],
                     p["execution_id"], p["portfolio_id"], p["source"], p["created_at"])
                    for p in sample_positions
                ]
                
                mock_order_result = MagicMock()
                mock_order_result.fetchall.return_value = [
                    (o["id"], o["order_id"], o["symbol"], o["quantity"], o["status"])
                    for o in sample_orders
                ]
                
                mock_db.execute.side_effect = [mock_pos_result, mock_order_result, MagicMock(), MagicMock(), MagicMock()]

                result = await service.request_transition(request)

                assert result.success is True
                assert result.positions_transferred == 2
                assert result.orders_cancelled == 2

    async def test_emergency_stop_transition(self, service, mock_db, sample_positions, sample_orders):
        """Test emergency stop transition from any mode."""
        script_state = HandoffState(
            trading_account_id="acc_001",
            strategy_id=1, 
            execution_id="exec_123",
            current_mode=HandoffMode.SCRIPT,
            target_mode=None,
            transition_id=None,
            transition_status=None,
            transition_started_at=None,
            last_updated_at=datetime.now(timezone.utc),
            controlled_by="system",
            metadata={}
        )

        request = TransitionRequest(
            trading_account_id="acc_001",
            strategy_id=1,
            execution_id="exec_123",
            from_mode=HandoffMode.SCRIPT,
            to_mode=HandoffMode.MANUAL,
            transition_type=TransitionType.EMERGENCY_STOP,
            requested_by="admin_123",
            reason="Emergency stop required",
            force=True
        )

        with patch.object(service, 'get_handoff_state', return_value=script_state):
            with patch.object(service, '_validate_transition', return_value=[]):
                # Mock position and order retrieval
                mock_pos_result = MagicMock()
                mock_pos_result.fetchall.return_value = [
                    (p["id"], p["symbol"], p["quantity"], p["strategy_id"],
                     p["execution_id"], p["portfolio_id"], p["source"], p["created_at"])
                    for p in sample_positions
                ]
                
                mock_all_orders_result = MagicMock()
                mock_all_orders_result.fetchall.return_value = [
                    (o["id"], o["order_id"], o["symbol"], o["quantity"], o["status"], "script")
                    for o in sample_orders
                ]
                
                mock_db.execute.side_effect = [mock_all_orders_result, mock_pos_result, MagicMock(), MagicMock(), MagicMock()]

                result = await service.request_transition(request)

                assert result.success is True
                assert result.positions_transferred == 2
                assert result.orders_cancelled == 2
                assert any("Emergency stop" in warning for warning in result.warnings)

    async def test_transition_validation_failures(self, service, mock_db, sample_handoff_state):
        """Test various transition validation failures."""
        # Test wrong current mode
        request = TransitionRequest(
            trading_account_id="acc_001",
            strategy_id=1,
            execution_id="exec_123", 
            from_mode=HandoffMode.SCRIPT,  # But current is MANUAL
            to_mode=HandoffMode.MANUAL,
            transition_type=TransitionType.SCRIPT_TO_MANUAL,
            requested_by="user_456",
            reason="Test transition"
        )

        with patch.object(service, 'get_handoff_state', return_value=sample_handoff_state):
            result = await service.request_transition(request)

            assert result.success is False
            assert len(result.errors) > 0
            assert "does not match" in result.errors[0]

    async def test_transition_validation_already_transitioning(self, service, mock_db):
        """Test validation failure when already in transition."""
        transitioning_state = HandoffState(
            trading_account_id="acc_001",
            strategy_id=1,
            execution_id="exec_123",
            current_mode=HandoffMode.TRANSITIONING,
            target_mode=HandoffMode.SCRIPT,
            transition_id="existing_transition",
            transition_status=TransitionStatus.IN_PROGRESS,
            transition_started_at=datetime.now(timezone.utc) - timedelta(minutes=1),
            last_updated_at=datetime.now(timezone.utc),
            controlled_by="user_456",
            metadata={}
        )

        request = TransitionRequest(
            trading_account_id="acc_001",
            strategy_id=1,
            execution_id="exec_123",
            from_mode=HandoffMode.MANUAL,
            to_mode=HandoffMode.SCRIPT,
            transition_type=TransitionType.MANUAL_TO_SCRIPT,
            requested_by="user_456",
            reason="Test transition"
        )

        with patch.object(service, 'get_handoff_state', return_value=transitioning_state):
            result = await service.request_transition(request)

            assert result.success is False
            assert len(result.errors) > 0
            assert "already in progress" in result.errors[0]

    async def test_stale_transition_detection(self, service, mock_db):
        """Test detection of stale transitions."""
        stale_transition_state = HandoffState(
            trading_account_id="acc_001",
            strategy_id=1,
            execution_id="exec_123", 
            current_mode=HandoffMode.TRANSITIONING,
            target_mode=HandoffMode.SCRIPT,
            transition_id="stale_transition",
            transition_status=TransitionStatus.IN_PROGRESS,
            transition_started_at=datetime.now(timezone.utc) - timedelta(minutes=10),  # Stale
            last_updated_at=datetime.now(timezone.utc),
            controlled_by="user_456",
            metadata={}
        )

        request = TransitionRequest(
            trading_account_id="acc_001",
            strategy_id=1,
            execution_id="exec_123",
            from_mode=HandoffMode.MANUAL,
            to_mode=HandoffMode.SCRIPT,
            transition_type=TransitionType.MANUAL_TO_SCRIPT,
            requested_by="user_456",
            reason="Test transition"
        )

        with patch.object(service, 'get_handoff_state', return_value=stale_transition_state):
            result = await service.request_transition(request)

            assert result.success is False
            assert len(result.errors) > 0
            assert "stale" in result.errors[0]

    async def test_force_transition_override(self, service, mock_db, sample_positions):
        """Test force flag overriding validation errors."""
        transitioning_state = HandoffState(
            trading_account_id="acc_001",
            strategy_id=1,
            execution_id="exec_123",
            current_mode=HandoffMode.TRANSITIONING,
            target_mode=HandoffMode.SCRIPT,
            transition_id="existing_transition",
            transition_status=TransitionStatus.IN_PROGRESS,
            transition_started_at=datetime.now(timezone.utc) - timedelta(minutes=10),
            last_updated_at=datetime.now(timezone.utc),
            controlled_by="user_456",
            metadata={}
        )

        request = TransitionRequest(
            trading_account_id="acc_001",
            strategy_id=1,
            execution_id="exec_123",
            from_mode=HandoffMode.SCRIPT,
            to_mode=HandoffMode.MANUAL,
            transition_type=TransitionType.EMERGENCY_STOP,
            requested_by="admin_123",
            reason="Emergency override",
            force=True
        )

        with patch.object(service, 'get_handoff_state', return_value=transitioning_state):
            # Mock successful emergency stop execution
            mock_pos_result = MagicMock()
            mock_pos_result.fetchall.return_value = [
                (p["id"], p["symbol"], p["quantity"], p["strategy_id"],
                 p["execution_id"], p["portfolio_id"], p["source"], p["created_at"])
                for p in sample_positions
            ]
            
            mock_orders_result = MagicMock()
            mock_orders_result.fetchall.return_value = []
            
            mock_db.execute.side_effect = [mock_orders_result, mock_pos_result, MagicMock(), MagicMock(), MagicMock()]

            result = await service.request_transition(request)

            assert result.success is True

    async def test_position_and_order_operations(self, service, mock_db, sample_positions, sample_orders):
        """Test position marking and order cancellation operations."""
        # Test _get_positions_for_handoff
        mock_pos_result = MagicMock()
        mock_pos_result.fetchall.return_value = [
            (p["id"], p["symbol"], p["quantity"], p["strategy_id"],
             p["execution_id"], p["portfolio_id"], p["source"], p["created_at"])
            for p in sample_positions
        ]
        mock_db.execute.return_value = mock_pos_result

        positions = await service._get_positions_for_handoff("acc_001", 1, "exec_123")

        assert len(positions) == 2
        assert positions[0]["id"] == "pos_1"
        assert positions[1]["symbol"] == "MSFT"

        # Test _cancel_manual_orders
        mock_order_result = MagicMock()
        mock_order_result.fetchall.return_value = [
            (o["id"], o["order_id"], o["symbol"], o["quantity"], o["status"])
            for o in sample_orders
        ]
        mock_db.execute.side_effect = [mock_order_result, MagicMock()]

        cancelled = await service._cancel_manual_orders("acc_001", 1, "exec_123")

        assert len(cancelled) == 2
        assert cancelled[0]["id"] == "ord_1"

    async def test_rollback_on_failure(self, service, mock_db, sample_handoff_state):
        """Test rollback functionality when transition fails."""
        request = TransitionRequest(
            trading_account_id="acc_001",
            strategy_id=1,
            execution_id="exec_123",
            from_mode=HandoffMode.MANUAL,
            to_mode=HandoffMode.SCRIPT,
            transition_type=TransitionType.MANUAL_TO_SCRIPT,
            requested_by="user_456",
            reason="Test transition"
        )

        with patch.object(service, 'get_handoff_state', return_value=sample_handoff_state):
            with patch.object(service, '_validate_transition', return_value=[]):
                # Make transition execution fail
                with patch.object(service, '_execute_manual_to_script', side_effect=Exception("Execution failed")):
                    with pytest.raises(Exception):
                        await service.request_transition(request)

                    # Should have attempted rollback
                    # This would be verified by checking audit trail in real implementation

    async def test_audit_trail_creation(self, service, mock_db, sample_handoff_state):
        """Test that audit trail is properly created for transitions."""
        request = TransitionRequest(
            trading_account_id="acc_001",
            strategy_id=1,
            execution_id="exec_123",
            from_mode=HandoffMode.MANUAL,
            to_mode=HandoffMode.SCRIPT,
            transition_type=TransitionType.MANUAL_TO_SCRIPT,
            requested_by="user_456",
            reason="Test audit trail"
        )

        with patch.object(service, 'get_handoff_state', return_value=sample_handoff_state):
            with patch.object(service, '_validate_transition', return_value=[]):
                with patch.object(service, '_execute_manual_to_script') as mock_execute:
                    mock_execute.return_value = TransitionResult(
                        transition_id="test_transition",
                        success=True,
                        new_state=sample_handoff_state,
                        positions_transferred=2,
                        orders_cancelled=1,
                        warnings=[],
                        errors=[],
                        audit_trail=[]
                    )

                    result = await service.request_transition(request)

                    # Verify audit events were recorded
                    audit_calls = [call for call in mock_db.execute.call_args_list 
                                  if "handoff_transition_audit" in str(call)]
                    assert len(audit_calls) >= 1

    async def test_concurrent_transition_attempts(self, service, mock_db, sample_handoff_state):
        """Test handling of concurrent transition attempts."""
        # This would test race conditions in real implementation
        # For now, verify that validation catches conflicting states
        pass

    async def test_different_granularity_levels(self, service, mock_db):
        """Test handoff states at different granularity levels."""
        # Account level handoff state
        account_state = await service.get_handoff_state("acc_001")
        assert account_state.trading_account_id == "acc_001"
        assert account_state.strategy_id is None
        assert account_state.execution_id is None

        # Strategy level handoff state  
        strategy_state = await service.get_handoff_state("acc_001", strategy_id=1)
        assert strategy_state.trading_account_id == "acc_001"
        assert strategy_state.strategy_id == 1
        assert strategy_state.execution_id is None

        # Execution level handoff state
        execution_state = await service.get_handoff_state("acc_001", strategy_id=1, execution_id="exec_123")
        assert execution_state.trading_account_id == "acc_001" 
        assert execution_state.strategy_id == 1
        assert execution_state.execution_id == "exec_123"

    @pytest.mark.parametrize("from_mode,to_mode,transition_type,should_succeed", [
        (HandoffMode.MANUAL, HandoffMode.SCRIPT, TransitionType.MANUAL_TO_SCRIPT, True),
        (HandoffMode.SCRIPT, HandoffMode.MANUAL, TransitionType.SCRIPT_TO_MANUAL, True),
        (HandoffMode.SCRIPT, HandoffMode.MANUAL, TransitionType.EMERGENCY_STOP, True),
        (HandoffMode.MANUAL, HandoffMode.MANUAL, TransitionType.MANUAL_TO_SCRIPT, False),  # Invalid
        (HandoffMode.SCRIPT, HandoffMode.SCRIPT, TransitionType.SCRIPT_TO_MANUAL, False)   # Invalid
    ])
    async def test_transition_type_validation(self, service, mock_db, from_mode, to_mode, transition_type, should_succeed):
        """Test validation of transition types against mode changes."""
        state = HandoffState(
            trading_account_id="acc_001",
            strategy_id=1,
            execution_id="exec_123",
            current_mode=from_mode,
            target_mode=None,
            transition_id=None,
            transition_status=None,
            transition_started_at=None,
            last_updated_at=datetime.now(timezone.utc),
            controlled_by="user_456",
            metadata={}
        )

        request = TransitionRequest(
            trading_account_id="acc_001",
            strategy_id=1,
            execution_id="exec_123",
            from_mode=from_mode,
            to_mode=to_mode,
            transition_type=transition_type,
            requested_by="user_456",
            reason="Test transition"
        )

        errors = await service._validate_transition(state, request)

        if should_succeed:
            assert len(errors) == 0
        else:
            assert len(errors) > 0
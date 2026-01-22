"""
Tests for Exit Attribution Policy Service (GAP-REC-8)
Sprint 7B requirement: Explicit exit attribution policy enforcement
"""

import pytest
from decimal import Decimal
from datetime import datetime, timezone

from order_service.app.services.exit_attribution_policy import (
    ExitAttributionPolicyService,
    ExitAttributionPolicy,
    AttributionDecision,
    ExitContext
)


@pytest.mark.asyncio
class TestExitAttributionPolicyService:
    """Test exit attribution policy enforcement."""

    async def test_single_strategy_auto_approval(self, mock_db_session):
        """Single strategy position → auto-approve"""
        service = ExitAttributionPolicyService(mock_db_session)
        
        # Mock single strategy position
        mock_db_session.execute.return_value.fetchall.return_value = [
            (1, 100, None, None, Decimal('100'), Decimal('150'), datetime.now(), 'manual')
        ]
        
        exit_context = ExitContext(
            exit_id="test_exit",
            trading_account_id="acc_001",
            symbol="AAPL",
            exit_quantity=Decimal('50'),
            exit_price=Decimal('155'),
            exit_timestamp=datetime.now(timezone.utc),
            broker_trade_id=None,
            order_id=None
        )
        
        result = await service.evaluate_exit_attribution_policy(exit_context)
        
        assert result.policy_applied == ExitAttributionPolicy.AUTO_SINGLE_STRATEGY
        assert result.decision == AttributionDecision.AUTO_APPROVED
        assert result.recommended_allocation is not None
        assert result.recommended_allocation["method"] == "FIFO"

    async def test_multi_strategy_full_exit_auto(self, mock_db_session):
        """Multi-strategy full exit → auto-approve FIFO"""
        service = ExitAttributionPolicyService(mock_db_session)
        
        # Mock multi-strategy positions
        mock_db_session.execute.return_value.fetchall.return_value = [
            (1, 100, None, None, Decimal('50'), Decimal('150'), datetime.now(), 'manual'),
            (2, 101, None, None, Decimal('50'), Decimal('150'), datetime.now(), 'manual')
        ]
        
        exit_context = ExitContext(
            exit_id="test_exit",
            trading_account_id="acc_001",
            symbol="AAPL",
            exit_quantity=Decimal('100'),  # Full exit
            exit_price=Decimal('155'),
            exit_timestamp=datetime.now(timezone.utc),
            broker_trade_id=None,
            order_id=None
        )
        
        result = await service.evaluate_exit_attribution_policy(exit_context)
        
        assert result.policy_applied == ExitAttributionPolicy.AUTO_MULTI_FULL
        assert result.decision == AttributionDecision.AUTO_APPROVED
        assert result.recommended_allocation["method"] == "FIFO_CROSS_STRATEGY"

    async def test_multi_strategy_partial_manual(self, mock_db_session):
        """Multi-strategy partial exit → manual required"""
        service = ExitAttributionPolicyService(mock_db_session)
        
        # Mock multi-strategy positions
        mock_db_session.execute.return_value.fetchall.return_value = [
            (1, 100, None, None, Decimal('50'), Decimal('150'), datetime.now(), 'manual'),
            (2, 101, None, None, Decimal('50'), Decimal('150'), datetime.now(), 'manual')
        ]
        
        exit_context = ExitContext(
            exit_id="test_exit",
            trading_account_id="acc_001",
            symbol="AAPL",
            exit_quantity=Decimal('75'),  # Partial exit
            exit_price=Decimal('155'),
            exit_timestamp=datetime.now(timezone.utc),
            broker_trade_id=None,
            order_id=None
        )
        
        result = await service.evaluate_exit_attribution_policy(exit_context)
        
        assert result.policy_applied == ExitAttributionPolicy.MANUAL_MULTI_PARTIAL
        assert result.decision == AttributionDecision.MANUAL_REQUIRED
        assert result.recommended_allocation is None
        assert "manual attribution decision" in result.manual_intervention_reason

    async def test_insufficient_data_blocked(self, mock_db_session):
        """Missing position data → blocked"""
        service = ExitAttributionPolicyService(mock_db_session)
        
        # Mock no positions found
        mock_db_session.execute.return_value.fetchall.return_value = []
        
        exit_context = ExitContext(
            exit_id="test_exit",
            trading_account_id="acc_001",
            symbol="AAPL",
            exit_quantity=Decimal('100'),
            exit_price=Decimal('155'),
            exit_timestamp=datetime.now(timezone.utc),
            broker_trade_id=None,
            order_id=None
        )
        
        result = await service.evaluate_exit_attribution_policy(exit_context)
        
        assert result.policy_applied == ExitAttributionPolicy.BLOCKED_INSUFFICIENT_DATA
        assert result.decision == AttributionDecision.BLOCKED
        assert "No eligible positions found" in result.reason

    async def test_policy_override_enforcement(self, mock_db_session):
        """Override policy applied correctly"""
        service = ExitAttributionPolicyService(mock_db_session)
        
        # Mock positions
        mock_db_session.execute.return_value.fetchall.return_value = [
            (1, 100, None, None, Decimal('100'), Decimal('150'), datetime.now(), 'manual')
        ]
        
        exit_context = ExitContext(
            exit_id="test_exit",
            trading_account_id="acc_001",
            symbol="AAPL",
            exit_quantity=Decimal('50'),
            exit_price=Decimal('155'),
            exit_timestamp=datetime.now(timezone.utc),
            broker_trade_id=None,
            order_id=None
        )
        
        # Test override policy
        result = await service.evaluate_exit_attribution_policy(
            exit_context, 
            override_policy=ExitAttributionPolicy.MANUAL_AMBIGUOUS
        )
        
        assert result.policy_applied == ExitAttributionPolicy.MANUAL_AMBIGUOUS
        assert result.decision == AttributionDecision.MANUAL_REQUIRED

    async def test_audit_trail_generation(self, mock_db_session):
        """Policy decisions create audit trails"""
        service = ExitAttributionPolicyService(mock_db_session)
        
        mock_db_session.execute.return_value.fetchall.return_value = [
            (1, 100, None, None, Decimal('100'), Decimal('150'), datetime.now(), 'manual')
        ]
        
        exit_context = ExitContext(
            exit_id="test_exit",
            trading_account_id="acc_001",
            symbol="AAPL",
            exit_quantity=Decimal('50'),
            exit_price=Decimal('155'),
            exit_timestamp=datetime.now(timezone.utc),
            broker_trade_id=None,
            order_id=None
        )
        
        result = await service.evaluate_exit_attribution_policy(exit_context)
        
        assert result.audit_data is not None
        assert "policy_eval_id" in result.audit_data
        assert "exit_context" in result.audit_data
        assert "position_analysis" in result.audit_data

    async def test_quantity_mismatch_handling(self, mock_db_session):
        """Exit > available quantity handling"""
        service = ExitAttributionPolicyService(mock_db_session)
        
        # Mock insufficient quantity
        mock_db_session.execute.return_value.fetchall.return_value = [
            (1, 100, None, None, Decimal('50'), Decimal('150'), datetime.now(), 'manual')
        ]
        
        exit_context = ExitContext(
            exit_id="test_exit",
            trading_account_id="acc_001",
            symbol="AAPL",
            exit_quantity=Decimal('100'),  # More than available (50)
            exit_price=Decimal('155'),
            exit_timestamp=datetime.now(timezone.utc),
            broker_trade_id=None,
            order_id=None
        )
        
        result = await service.evaluate_exit_attribution_policy(exit_context)
        
        assert result.policy_applied == ExitAttributionPolicy.MANUAL_AMBIGUOUS
        assert result.decision == AttributionDecision.MANUAL_REQUIRED
        assert "exceeds available quantity" in result.reason

    @pytest.mark.performance
    async def test_policy_performance_metrics(self, mock_db_session):
        """Policy evaluation performance < 100ms"""
        import time
        
        service = ExitAttributionPolicyService(mock_db_session)
        
        mock_db_session.execute.return_value.fetchall.return_value = [
            (1, 100, None, None, Decimal('100'), Decimal('150'), datetime.now(), 'manual')
        ]
        
        exit_context = ExitContext(
            exit_id="test_exit",
            trading_account_id="acc_001",
            symbol="AAPL",
            exit_quantity=Decimal('50'),
            exit_price=Decimal('155'),
            exit_timestamp=datetime.now(timezone.utc),
            broker_trade_id=None,
            order_id=None
        )
        
        start_time = time.time()
        result = await service.evaluate_exit_attribution_policy(exit_context)
        end_time = time.time()
        
        execution_time = (end_time - start_time) * 1000  # Convert to ms
        assert execution_time < 100, f"Policy evaluation took {execution_time}ms, should be < 100ms"
        assert result.policy_applied == ExitAttributionPolicy.AUTO_SINGLE_STRATEGY
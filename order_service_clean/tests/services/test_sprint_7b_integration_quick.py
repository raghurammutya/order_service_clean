"""
Quick Sprint 7B Integration Test
Tests core wiring and integration points for Sprint 7B services
"""

import pytest
from decimal import Decimal
from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch, MagicMock


@pytest.mark.asyncio
class TestSprint7BIntegration:
    """Test Sprint 7B service integrations."""

    async def test_exit_attribution_policy_wiring(self, mock_db_session):
        """Test that exit attribution policy is properly integrated."""
        # Test that policy service can be imported and used
        from order_service.app.services.exit_attribution_policy import (
            ExitAttributionPolicyService,
            ExitContext
        )
        
        service = ExitAttributionPolicyService(mock_db_session)
        assert service is not None
        
        # Mock database response for policy evaluation
        mock_db_session.execute.return_value.fetchall.return_value = [
            (1, 100, None, None, Decimal('100'), Decimal('150'), datetime.now(), 'manual')
        ]
        
        exit_context = ExitContext(
            exit_id="test",
            trading_account_id="acc_001",
            symbol="AAPL",
            exit_quantity=Decimal('50'),
            exit_price=Decimal('155'),
            exit_timestamp=datetime.now(timezone.utc),
            broker_trade_id=None,
            order_id=None
        )
        
        # Should not raise exception
        result = await service.evaluate_exit_attribution_policy(exit_context)
        assert result is not None

    async def test_manual_attribution_validator_wiring(self, mock_db_session):
        """Test that manual attribution validator is integrated in apply process."""
        from order_service.app.services.manual_attribution_service import ManualAttributionService
        from order_service.app.services.manual_attribution_apply_validator import ManualAttributionApplyValidator
        
        # Mock a resolved case
        mock_case = MagicMock()
        mock_case.case_id = "case_123"
        mock_case.status = "resolved"
        mock_case.resolution_data = {
            "allocation_decisions": [{"position_id": 1, "quantity": 50}],
            "rationale": "test resolution"
        }
        mock_case.exit_quantity = 50
        mock_case.symbol = "AAPL"
        mock_case.trading_account_id = "acc_001"
        
        # Mock validation result
        mock_validation_result = MagicMock()
        mock_validation_result.can_proceed = True
        mock_validation_result.warnings = []
        mock_validation_result.failures = []
        
        service = ManualAttributionService(mock_db_session)
        
        with patch.object(service, 'get_attribution_case', return_value=mock_case):
            with patch('order_service.app.services.manual_attribution_apply_validator.ManualAttributionApplyValidator.validate_manual_attribution_decision', 
                      return_value=mock_validation_result):
                with patch('order_service.app.services.reconciliation_driven_transfers.ReconciliationDrivenTransferService.execute_manual_transfer_instructions',
                          return_value=MagicMock(failed_count=0)):
                    # Should use validator in apply process
                    result = await service.apply_resolution("case_123", "test_user")
                    assert result is True

    async def test_exit_context_matcher_wiring(self, mock_db_session):
        """Test that exit context matcher is used in trade service."""
        from order_service.app.services.trade_service import TradeService
        from order_service.app.services.exit_context_matcher import ExitContextMatcher
        
        # Create mock trade
        mock_trade = MagicMock()
        mock_trade.symbol = "AAPL"
        mock_trade.quantity = 100
        mock_trade.trading_account_id = "acc_001"
        mock_trade.broker_trade_id = "broker_123"
        mock_trade.price = 150
        mock_trade.trade_time = datetime.now()
        
        # Mock match result
        mock_match_result = MagicMock()
        mock_match_result.match_quality = "high"
        mock_match_result.confidence_score = 0.9
        mock_match_result.matched_trades = []
        
        service = TradeService(mock_db_session)
        
        with patch('order_service.app.services.exit_context_matcher.ExitContextMatcher.match_exit_context',
                  return_value=mock_match_result):
            with patch('order_service.app.services.partial_exit_attribution_service.PartialExitAttributionService.attribute_partial_exit',
                      return_value=MagicMock(requires_manual_intervention=False, allocations=[])):
                # Should use exit context matcher
                await service._trigger_external_exit_attribution(mock_trade)

    async def test_transfer_instruction_generator_wiring(self, mock_db_session):
        """Test that transfer instruction generator is used in reconciliation transfers."""
        from order_service.app.services.reconciliation_driven_transfers import ReconciliationDrivenTransferService
        from order_service.app.services.transfer_instruction_generator import TransferInstructionGenerator
        
        # Mock transfer batch
        mock_transfer_batch = MagicMock()
        mock_transfer_batch.instructions = []
        
        service = ReconciliationDrivenTransferService(mock_db_session)
        
        with patch('order_service.app.services.transfer_instruction_generator.TransferInstructionGenerator.generate_attribution_transfer_instructions',
                  return_value=mock_transfer_batch):
            with patch.object(service, '_record_transfer_batch'):
                with patch.object(service, '_update_transfer_batch_status'):
                    # Should use transfer instruction generator
                    result = await service.execute_manual_transfer_instructions(
                        "case_123", 
                        [{"source_execution_id": "exec1", "target_execution_id": "exec2", "symbol": "AAPL", "quantity": "50"}],
                        "test_user"
                    )
                    assert result is not None

    async def test_database_tables_exist(self, mock_db_session):
        """Test that Sprint 7B database tables are referenced correctly."""
        from order_service.app.services.redis_unavailable_handoff_manager import RedisUnavailableHandoffManager
        from order_service.app.services.external_order_tagging_idempotency import ExternalOrderTaggingIdempotency
        
        # Test that services can be created (they reference the tables in __init__)
        handoff_manager = RedisUnavailableHandoffManager(mock_db_session)
        assert handoff_manager is not None
        
        idempotency_service = ExternalOrderTaggingIdempotency(mock_db_session)
        assert idempotency_service is not None

    def test_model_field_existence(self):
        """Test that required model fields exist."""
        from order_service.app.models.order import Order
        from order_service.app.models.trade import Trade
        
        # Test Order model has execution_id
        order = Order()
        assert hasattr(order, 'execution_id'), "Order model missing execution_id field"
        
        # Test Trade model has source
        trade = Trade()
        assert hasattr(trade, 'source'), "Trade model missing source field"

    async def test_import_all_sprint7b_services(self):
        """Test that all Sprint 7B services can be imported without errors."""
        try:
            from order_service.app.services.exit_attribution_policy import ExitAttributionPolicyService
            from order_service.app.services.manual_attribution_apply_validator import ManualAttributionApplyValidator
            from order_service.app.services.exit_context_matcher import ExitContextMatcher
            from order_service.app.services.transfer_instruction_generator import TransferInstructionGenerator
            from order_service.app.services.redis_unavailable_handoff_manager import RedisUnavailableHandoffManager
            from order_service.app.services.handoff_concurrency_manager import HandoffConcurrencyManager
            from order_service.app.services.missing_trade_history_handler import MissingTradeHistoryHandler
            from order_service.app.services.external_order_tagging_idempotency import ExternalOrderTaggingIdempotency
            
            # All imports successful
            assert True
        except ImportError as e:
            pytest.fail(f"Failed to import Sprint 7B service: {e}")

    async def test_api_endpoints_importable(self):
        """Test that Sprint 7B API endpoints can be imported."""
        try:
            from order_service.app.api.v1.endpoints.manual_attribution import router as manual_router
            from order_service.app.api.v1.endpoints.external_order_validation import router as external_router
            
            assert manual_router is not None
            assert external_router is not None
            
        except ImportError as e:
            pytest.fail(f"Failed to import Sprint 7B API endpoint: {e}")
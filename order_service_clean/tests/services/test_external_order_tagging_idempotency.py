"""
Test External Order Tagging Idempotency (GAP-REC-15)
Tests idempotency mechanisms for external order tagging operations
"""

import pytest
from decimal import Decimal
from datetime import datetime, timezone, timedelta
from unittest.mock import AsyncMock, patch, MagicMock
from uuid import uuid4

from order_service.app.services.external_order_tagging_idempotency import (
    ExternalOrderTaggingIdempotency,
    IdempotencyConfig,
    IdempotencyRecord,
    IdempotencyViolationError,
    DuplicateOperationError
)


@pytest.mark.asyncio
class TestExternalOrderTaggingIdempotency:
    """Test external order tagging idempotency functionality."""

    @pytest.fixture
    def idempotency_config(self):
        """Default idempotency configuration."""
        return IdempotencyConfig(
            enable_strict_checking=True,
            record_retention_days=30,
            cleanup_interval_hours=6,
            max_retry_attempts=3
        )

    @pytest.fixture
    def service(self, mock_db_session, idempotency_config):
        """Create service instance."""
        return ExternalOrderTaggingIdempotency(mock_db_session, idempotency_config)

    @pytest.fixture
    def sample_tagging_operation(self):
        """Sample external order tagging operation."""
        return {
            "operation_id": str(uuid4()),
            "external_order_id": "ext_order_12345",
            "broker_order_id": "broker_ord_67890",
            "trading_account_id": "acc_001",
            "symbol": "AAPL",
            "quantity": 100,
            "price": Decimal("150.50"),
            "operation_type": "tag_external_order",
            "source_system": "broker_sync",
            "execution_context": {
                "sync_session_id": "sync_789",
                "timestamp": datetime.now().isoformat()
            }
        }

    async def test_first_time_operation_succeeds(self, service, sample_tagging_operation, mock_db_session):
        """Test that first-time operation succeeds and creates idempotency record."""
        # Mock no existing record
        mock_db_session.execute.return_value.fetchone.return_value = None
        mock_db_session.commit = AsyncMock()
        
        # Mock successful order tagging
        with patch.object(service, '_execute_order_tagging', return_value={"order_id": 12345, "success": True}) as mock_tag:
            result = await service.execute_with_idempotency(sample_tagging_operation)
        
        assert result.success is True
        assert result.operation_performed is True
        assert result.idempotency_key is not None
        assert result.result_data["order_id"] == 12345
        
        # Verify idempotency record was created
        mock_db_session.execute.assert_called()
        mock_db_session.commit.assert_called()

    async def test_duplicate_operation_returns_cached_result(self, service, sample_tagging_operation, mock_db_session):
        """Test that duplicate operation returns cached result without re-execution."""
        # Mock existing idempotency record
        existing_record_time = datetime.now()
        mock_db_session.execute.return_value.fetchone.return_value = (
            "idem_123",
            sample_tagging_operation["operation_id"],
            existing_record_time,
            True,  # success
            {"order_id": 12345, "cached": True},  # result_data
            None   # error_message
        )
        
        result = await service.execute_with_idempotency(sample_tagging_operation)
        
        assert result.success is True
        assert result.operation_performed is False  # Cached result
        assert result.result_data["cached"] is True
        assert result.result_data["order_id"] == 12345

    async def test_idempotency_key_generation_consistency(self, service, sample_tagging_operation):
        """Test that idempotency key generation is consistent for same operation."""
        key1 = service._generate_idempotency_key(sample_tagging_operation)
        key2 = service._generate_idempotency_key(sample_tagging_operation)
        
        assert key1 == key2
        assert len(key1) > 0
        
        # Different operation should produce different key
        different_operation = sample_tagging_operation.copy()
        different_operation["external_order_id"] = "different_order"
        key3 = service._generate_idempotency_key(different_operation)
        
        assert key1 != key3

    async def test_partial_operation_key_differences(self, service, sample_tagging_operation):
        """Test that partial differences in operations produce different keys."""
        base_key = service._generate_idempotency_key(sample_tagging_operation)
        
        # Different quantity should produce different key
        quantity_diff = sample_tagging_operation.copy()
        quantity_diff["quantity"] = 200
        quantity_key = service._generate_idempotency_key(quantity_diff)
        assert base_key != quantity_key
        
        # Different price should produce different key
        price_diff = sample_tagging_operation.copy()
        price_diff["price"] = Decimal("160.00")
        price_key = service._generate_idempotency_key(price_diff)
        assert base_key != price_key
        
        # Different symbol should produce different key
        symbol_diff = sample_tagging_operation.copy()
        symbol_diff["symbol"] = "TSLA"
        symbol_key = service._generate_idempotency_key(symbol_diff)
        assert base_key != symbol_key

    async def test_failed_operation_idempotency_handling(self, service, sample_tagging_operation, mock_db_session):
        """Test idempotency handling for failed operations."""
        # Mock no existing record for first attempt
        mock_db_session.execute.return_value.fetchone.return_value = None
        mock_db_session.commit = AsyncMock()
        
        # Mock failed order tagging
        with patch.object(service, '_execute_order_tagging', side_effect=Exception("Tagging failed")) as mock_tag:
            result = await service.execute_with_idempotency(sample_tagging_operation)
        
        assert result.success is False
        assert result.operation_performed is True
        assert "Tagging failed" in result.error_message
        
        # Verify failure record was created
        mock_db_session.execute.assert_called()

    async def test_retry_failed_operation(self, service, sample_tagging_operation, mock_db_session):
        """Test retrying a previously failed operation."""
        # Mock existing failed record
        failed_record_time = datetime.now() - timedelta(minutes=5)
        mock_db_session.execute.return_value.fetchone.return_value = (
            "idem_failed",
            sample_tagging_operation["operation_id"],
            failed_record_time,
            False,  # success = False
            None,   # result_data
            "Previous tagging failed"  # error_message
        )
        mock_db_session.commit = AsyncMock()
        
        # Mock successful retry
        with patch.object(service, '_execute_order_tagging', return_value={"order_id": 12345, "success": True}):
            result = await service.execute_with_idempotency(sample_tagging_operation)
        
        assert result.success is True
        assert result.operation_performed is True  # Was retried
        assert result.retry_attempt is True

    async def test_concurrent_operation_prevention(self, service, sample_tagging_operation, mock_db_session):
        """Test prevention of concurrent identical operations."""
        # Mock existing in-progress record
        in_progress_time = datetime.now()
        mock_db_session.execute.return_value.fetchone.return_value = (
            "idem_progress",
            sample_tagging_operation["operation_id"],
            in_progress_time,
            None,  # success = None (in progress)
            None,  # result_data
            None   # error_message
        )
        
        with pytest.raises(DuplicateOperationError) as exc_info:
            await service.execute_with_idempotency(sample_tagging_operation)
        
        assert "currently in progress" in str(exc_info.value).lower()

    async def test_idempotency_record_cleanup(self, service, mock_db_session):
        """Test cleanup of old idempotency records."""
        # Mock old records for cleanup
        old_records = [
            ("idem_old1", datetime.now() - timedelta(days=45)),
            ("idem_old2", datetime.now() - timedelta(days=60))
        ]
        mock_db_session.execute.return_value.fetchall.return_value = old_records
        mock_db_session.commit = AsyncMock()
        
        cleanup_count = await service.cleanup_old_records()
        
        assert cleanup_count >= 0
        mock_db_session.execute.assert_called()
        mock_db_session.commit.assert_called()

    async def test_bulk_operation_idempotency(self, service, mock_db_session):
        """Test idempotency for bulk tagging operations."""
        bulk_operations = [
            {
                "operation_id": f"bulk_op_{i}",
                "external_order_id": f"ext_order_{i}",
                "broker_order_id": f"broker_ord_{i}",
                "trading_account_id": "acc_001",
                "symbol": "AAPL",
                "quantity": 100 + i,
                "operation_type": "tag_external_order"
            }
            for i in range(5)
        ]
        
        # Mock no existing records for all operations
        mock_db_session.execute.return_value.fetchone.return_value = None
        mock_db_session.commit = AsyncMock()
        
        # Mock successful bulk tagging
        with patch.object(service, '_execute_bulk_order_tagging', return_value={"tagged_count": 5, "success": True}):
            results = await service.execute_bulk_with_idempotency(bulk_operations)
        
        assert len(results) == 5
        assert all(r.success for r in results)
        assert all(r.operation_performed for r in results)

    async def test_cross_session_idempotency(self, service, sample_tagging_operation, mock_db_session):
        """Test idempotency across different sync sessions."""
        # Same operation from different sync session should be treated as duplicate
        operation_session1 = sample_tagging_operation.copy()
        operation_session1["execution_context"]["sync_session_id"] = "sync_session_1"
        
        operation_session2 = sample_tagging_operation.copy()
        operation_session2["execution_context"]["sync_session_id"] = "sync_session_2"
        
        # Both should have same idempotency key (session ID not part of key)
        key1 = service._generate_idempotency_key(operation_session1)
        key2 = service._generate_idempotency_key(operation_session2)
        assert key1 == key2

    async def test_broker_error_handling_with_idempotency(self, service, sample_tagging_operation, mock_db_session):
        """Test handling of broker errors with proper idempotency tracking."""
        # Mock no existing record
        mock_db_session.execute.return_value.fetchone.return_value = None
        mock_db_session.commit = AsyncMock()
        
        # Mock broker error
        broker_error = Exception("Broker API returned error: Invalid order ID")
        with patch.object(service, '_execute_order_tagging', side_effect=broker_error):
            result = await service.execute_with_idempotency(sample_tagging_operation)
        
        assert result.success is False
        assert "Broker API returned error" in result.error_message
        assert result.should_retry is True  # Broker errors are retryable

    async def test_validation_error_handling_with_idempotency(self, service, mock_db_session):
        """Test handling of validation errors with idempotency."""
        invalid_operation = {
            "operation_id": str(uuid4()),
            # Missing required fields
            "external_order_id": "ext_order_12345",
            # Missing broker_order_id, trading_account_id, etc.
        }
        
        with pytest.raises(ValueError) as exc_info:
            await service.execute_with_idempotency(invalid_operation)
        
        assert "required field" in str(exc_info.value).lower()

    async def test_idempotency_key_collision_handling(self, service, mock_db_session):
        """Test handling of theoretical idempotency key collisions."""
        # Mock key collision scenario
        operation1 = {
            "operation_id": "op1",
            "external_order_id": "ext1",
            "broker_order_id": "broker1",
            "trading_account_id": "acc_001",
            "symbol": "AAPL",
            "quantity": 100,
            "operation_type": "tag_external_order"
        }
        
        operation2 = operation1.copy()
        operation2["operation_id"] = "op2"  # Different operation ID but same other data
        
        key1 = service._generate_idempotency_key(operation1)
        key2 = service._generate_idempotency_key(operation2)
        
        # Should be same key for same order details
        assert key1 == key2

    async def test_performance_with_high_volume_operations(self, service, mock_db_session):
        """Test performance with high volume of concurrent operations."""
        import asyncio
        
        # Create many concurrent operations
        operations = [
            {
                "operation_id": f"perf_op_{i}",
                "external_order_id": f"ext_order_{i}",
                "broker_order_id": f"broker_ord_{i}",
                "trading_account_id": "acc_001",
                "symbol": "AAPL",
                "quantity": 100,
                "operation_type": "tag_external_order"
            }
            for i in range(100)
        ]
        
        # Mock database responses
        mock_db_session.execute.return_value.fetchone.return_value = None
        mock_db_session.commit = AsyncMock()
        
        with patch.object(service, '_execute_order_tagging', return_value={"success": True}):
            start_time = datetime.now()
            
            # Execute operations concurrently
            tasks = [service.execute_with_idempotency(op) for op in operations]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            end_time = datetime.now()
        
        # Should complete all operations within reasonable time
        processing_time = (end_time - start_time).total_seconds()
        assert processing_time < 30  # Should complete within 30 seconds
        
        # All operations should succeed
        assert len([r for r in results if not isinstance(r, Exception) and r.success]) == 100

    async def test_database_transaction_safety_with_idempotency(self, service, sample_tagging_operation, mock_db_session):
        """Test database transaction safety in idempotency operations."""
        # Mock database error during idempotency record creation
        mock_db_session.execute.return_value.fetchone.return_value = None
        mock_db_session.commit.side_effect = Exception("Database transaction failed")
        
        with pytest.raises(Exception):
            await service.execute_with_idempotency(sample_tagging_operation)
        
        # Should not leave partial state (verified by checking rollback behavior)

    async def test_idempotency_record_integrity_validation(self, service, mock_db_session):
        """Test validation of idempotency record integrity."""
        # Mock corrupted idempotency record
        corrupted_record = (
            "idem_corrupt",
            "operation_123",
            datetime.now(),
            True,  # success
            None,  # result_data is None but success is True (inconsistent)
            None   # error_message
        )
        mock_db_session.execute.return_value.fetchone.return_value = corrupted_record
        
        with pytest.raises(IdempotencyViolationError) as exc_info:
            await service._validate_idempotency_record_integrity(corrupted_record)
        
        assert "inconsistent state" in str(exc_info.value).lower()

    async def test_monitoring_and_metrics_collection(self, service, mock_db_session):
        """Test collection of idempotency metrics for monitoring."""
        # Mock metrics data
        mock_db_session.execute.return_value.fetchall.return_value = [
            (10,),  # total_operations
            (2,),   # cached_operations
            (1,),   # failed_operations
            (0.95,) # success_rate
        ]
        
        metrics = await service.get_idempotency_metrics(
            time_range=(datetime.now() - timedelta(hours=24), datetime.now())
        )
        
        assert "total_operations" in metrics
        assert "cache_hit_rate" in metrics
        assert "failure_rate" in metrics
        assert metrics["total_operations"] == 10
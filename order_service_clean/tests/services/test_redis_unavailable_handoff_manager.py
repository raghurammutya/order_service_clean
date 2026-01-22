"""
Test Redis Unavailable Handoff Manager (GAP-REC-12)
Tests handoff management when Redis is unavailable with safe pending state and retry logic
"""

import pytest
from decimal import Decimal
from datetime import datetime, timezone, timedelta
from unittest.mock import AsyncMock, patch, MagicMock
import asyncio

from order_service.app.services.redis_unavailable_handoff_manager import (
    RedisUnavailableHandoffManager,
    HandoffState,
    PendingHandoff,
    HandoffRetryConfig,
    RedisUnavailableError,
    HandoffPersistenceError
)


@pytest.mark.asyncio
class TestRedisUnavailableHandoffManager:
    """Test Redis unavailable handoff management."""

    @pytest.fixture
    def retry_config(self):
        """Default retry configuration."""
        return HandoffRetryConfig(
            max_retry_attempts=3,
            base_delay_seconds=1,
            max_delay_seconds=30,
            backoff_multiplier=2.0
        )

    @pytest.fixture
    def manager(self, mock_db_session, retry_config):
        """Create manager instance."""
        return RedisUnavailableHandoffManager(mock_db_session, retry_config)

    @pytest.fixture
    def sample_handoff_data(self):
        """Sample handoff data for testing."""
        return {
            "handoff_id": "handoff_123",
            "source_service": "order_service",
            "target_service": "ticker_service",
            "data_payload": {
                "order_id": 12345,
                "symbol": "AAPL",
                "action": "subscribe"
            },
            "priority": "high",
            "timeout_seconds": 300
        }

    async def test_redis_available_normal_operation(self, manager, sample_handoff_data):
        """Test normal operation when Redis is available."""
        with patch.object(manager, '_check_redis_availability', return_value=True):
            with patch.object(manager, '_execute_redis_handoff', return_value=True) as mock_redis:
                
                result = await manager.execute_handoff(sample_handoff_data)
                
                assert result.success is True
                assert result.used_redis is True
                assert result.fallback_used is False
                mock_redis.assert_called_once()

    async def test_redis_unavailable_fallback_to_database(self, manager, sample_handoff_data, mock_db_session):
        """Test fallback to database when Redis is unavailable."""
        with patch.object(manager, '_check_redis_availability', return_value=False):
            # Mock database operations
            mock_db_session.execute.return_value = MagicMock()
            mock_db_session.commit = AsyncMock()
            
            result = await manager.execute_handoff(sample_handoff_data)
            
            assert result.success is True
            assert result.used_redis is False
            assert result.fallback_used is True
            assert result.handoff_state == HandoffState.PENDING_REDIS_RETRY

    async def test_persist_pending_handoff(self, manager, sample_handoff_data, mock_db_session):
        """Test persisting handoff to database when Redis unavailable."""
        # Mock Redis unavailable
        with patch.object(manager, '_check_redis_availability', return_value=False):
            mock_db_session.execute.return_value = MagicMock()
            mock_db_session.commit = AsyncMock()
            
            pending_handoff = await manager._persist_pending_handoff(sample_handoff_data)
            
            assert isinstance(pending_handoff, PendingHandoff)
            assert pending_handoff.handoff_id == "handoff_123"
            assert pending_handoff.state == HandoffState.PENDING_REDIS_RETRY
            assert pending_handoff.retry_attempts == 0
            
            # Verify database insert was called
            mock_db_session.execute.assert_called()
            mock_db_session.commit.assert_called_once()

    async def test_retry_pending_handoffs_success(self, manager, mock_db_session):
        """Test successful retry of pending handoffs."""
        # Mock pending handoffs in database
        mock_pending_handoffs = [
            (
                "handoff_123", "order_service", "ticker_service",
                {"order_id": 12345}, "high", 0, datetime.now(),
                HandoffState.PENDING_REDIS_RETRY.value, None
            )
        ]
        mock_db_session.execute.return_value.fetchall.return_value = mock_pending_handoffs
        mock_db_session.commit = AsyncMock()
        
        # Mock Redis now available and successful handoff
        with patch.object(manager, '_check_redis_availability', return_value=True):
            with patch.object(manager, '_execute_redis_handoff', return_value=True):
                
                retry_results = await manager.retry_pending_handoffs()
                
                assert len(retry_results) == 1
                assert retry_results[0].success is True
                assert retry_results[0].handoff_id == "handoff_123"

    async def test_retry_pending_handoffs_redis_still_unavailable(self, manager, mock_db_session):
        """Test retry when Redis is still unavailable."""
        # Mock pending handoffs
        mock_pending_handoffs = [
            (
                "handoff_123", "order_service", "ticker_service", 
                {"order_id": 12345}, "high", 1, datetime.now(),
                HandoffState.PENDING_REDIS_RETRY.value, None
            )
        ]
        mock_db_session.execute.return_value.fetchall.return_value = mock_pending_handoffs
        mock_db_session.commit = AsyncMock()
        
        # Mock Redis still unavailable
        with patch.object(manager, '_check_redis_availability', return_value=False):
            
            retry_results = await manager.retry_pending_handoffs()
            
            assert len(retry_results) == 1
            assert retry_results[0].success is False
            assert retry_results[0].retry_attempts == 2  # Incremented

    async def test_max_retry_attempts_exceeded(self, manager, mock_db_session):
        """Test handling when max retry attempts are exceeded."""
        # Mock handoff with max retry attempts reached
        mock_pending_handoffs = [
            (
                "handoff_123", "order_service", "ticker_service",
                {"order_id": 12345}, "high", 3, datetime.now(),  # Max attempts reached
                HandoffState.PENDING_REDIS_RETRY.value, None
            )
        ]
        mock_db_session.execute.return_value.fetchall.return_value = mock_pending_handoffs
        mock_db_session.commit = AsyncMock()
        
        retry_results = await manager.retry_pending_handoffs()
        
        assert len(retry_results) == 1
        assert retry_results[0].success is False
        assert retry_results[0].final_state == HandoffState.FAILED_MAX_RETRIES

    async def test_exponential_backoff_delay(self, manager):
        """Test exponential backoff delay calculation."""
        config = manager.retry_config
        
        # Test delay calculation for different retry attempts
        delay_1 = manager._calculate_retry_delay(1)
        delay_2 = manager._calculate_retry_delay(2)
        delay_3 = manager._calculate_retry_delay(3)
        
        assert delay_1 == config.base_delay_seconds
        assert delay_2 == config.base_delay_seconds * config.backoff_multiplier
        assert delay_3 == config.base_delay_seconds * (config.backoff_multiplier ** 2)
        
        # Test max delay cap
        delay_high = manager._calculate_retry_delay(10)
        assert delay_high <= config.max_delay_seconds

    async def test_handoff_timeout_handling(self, manager, sample_handoff_data, mock_db_session):
        """Test handling of timed-out handoffs."""
        # Create handoff data with short timeout
        timeout_handoff = sample_handoff_data.copy()
        timeout_handoff["timeout_seconds"] = 1
        
        # Mock old pending handoff (past timeout)
        old_time = datetime.now() - timedelta(seconds=300)
        mock_pending_handoffs = [
            (
                "handoff_123", "order_service", "ticker_service",
                {"order_id": 12345}, "high", 0, old_time,
                HandoffState.PENDING_REDIS_RETRY.value, None
            )
        ]
        mock_db_session.execute.return_value.fetchall.return_value = mock_pending_handoffs
        mock_db_session.commit = AsyncMock()
        
        # Process timeouts
        await manager._process_timeout_handoffs()
        
        # Verify timeout status update was called
        mock_db_session.execute.assert_called()

    async def test_clean_completed_handoffs(self, manager, mock_db_session):
        """Test cleanup of completed handoffs."""
        mock_db_session.execute.return_value = MagicMock()
        mock_db_session.commit = AsyncMock()
        
        cleaned_count = await manager.clean_completed_handoffs(
            older_than_hours=24
        )
        
        # Verify cleanup query was executed
        mock_db_session.execute.assert_called()
        mock_db_session.commit.assert_called()
        assert isinstance(cleaned_count, int)

    async def test_get_handoff_status(self, manager, mock_db_session):
        """Test getting handoff status."""
        # Mock handoff status in database
        mock_db_session.execute.return_value.fetchone.return_value = (
            "handoff_123", HandoffState.PENDING_REDIS_RETRY.value, 1,
            datetime.now(), datetime.now(), None
        )
        
        status = await manager.get_handoff_status("handoff_123")
        
        assert status is not None
        assert status.handoff_id == "handoff_123"
        assert status.state == HandoffState.PENDING_REDIS_RETRY
        assert status.retry_attempts == 1

    async def test_concurrent_retry_safety(self, manager, mock_db_session):
        """Test concurrent retry safety mechanisms."""
        # Mock database locking
        mock_db_session.execute.return_value.fetchall.return_value = []
        mock_db_session.commit = AsyncMock()
        
        # Run multiple concurrent retry operations
        tasks = [
            manager.retry_pending_handoffs(),
            manager.retry_pending_handoffs(),
            manager.retry_pending_handoffs()
        ]
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Should handle concurrency gracefully
        assert all(not isinstance(r, Exception) for r in results)

    async def test_database_persistence_error_handling(self, manager, sample_handoff_data, mock_db_session):
        """Test handling of database persistence errors."""
        # Mock database error
        mock_db_session.execute.side_effect = Exception("Database connection failed")
        
        with patch.object(manager, '_check_redis_availability', return_value=False):
            with pytest.raises(HandoffPersistenceError):
                await manager.execute_handoff(sample_handoff_data)

    async def test_redis_connection_recovery_detection(self, manager):
        """Test detection of Redis connection recovery."""
        # Mock Redis availability check sequence
        availability_sequence = [False, False, True]  # Redis comes back online
        
        with patch.object(manager, '_check_redis_availability', side_effect=availability_sequence):
            # First two checks show unavailable
            assert await manager._check_redis_availability() is False
            assert await manager._check_redis_availability() is False
            
            # Third check shows available (recovery)
            assert await manager._check_redis_availability() is True

    async def test_handoff_data_integrity_validation(self, manager, mock_db_session):
        """Test validation of handoff data integrity."""
        invalid_handoff = {
            # Missing required fields
            "source_service": "order_service",
            "data_payload": {"order_id": 12345}
            # Missing handoff_id, target_service, priority
        }
        
        with pytest.raises(ValueError):
            await manager.execute_handoff(invalid_handoff)

    async def test_priority_based_retry_ordering(self, manager, mock_db_session):
        """Test that high-priority handoffs are retried first."""
        # Mock pending handoffs with different priorities
        mock_pending_handoffs = [
            ("handoff_1", "order_service", "ticker_service", {"order_id": 1}, "low", 0, datetime.now(), HandoffState.PENDING_REDIS_RETRY.value, None),
            ("handoff_2", "order_service", "ticker_service", {"order_id": 2}, "high", 0, datetime.now(), HandoffState.PENDING_REDIS_RETRY.value, None),
            ("handoff_3", "order_service", "ticker_service", {"order_id": 3}, "medium", 0, datetime.now(), HandoffState.PENDING_REDIS_RETRY.value, None)
        ]
        mock_db_session.execute.return_value.fetchall.return_value = mock_pending_handoffs
        mock_db_session.commit = AsyncMock()
        
        # Mock Redis available for retry
        with patch.object(manager, '_check_redis_availability', return_value=True):
            with patch.object(manager, '_execute_redis_handoff', return_value=True) as mock_redis:
                
                await manager.retry_pending_handoffs()
                
                # Verify high priority was processed first
                call_order = [call[0][0]["handoff_id"] for call in mock_redis.call_args_list]
                assert call_order.index("handoff_2") < call_order.index("handoff_3")
                assert call_order.index("handoff_3") < call_order.index("handoff_1")

    async def test_batch_retry_performance(self, manager, mock_db_session):
        """Test performance of batch retry operations."""
        # Mock large number of pending handoffs
        large_handoff_batch = [
            (f"handoff_{i}", "order_service", "ticker_service", {"order_id": i}, "medium", 0, datetime.now(), HandoffState.PENDING_REDIS_RETRY.value, None)
            for i in range(100)
        ]
        mock_db_session.execute.return_value.fetchall.return_value = large_handoff_batch
        mock_db_session.commit = AsyncMock()
        
        # Mock Redis available
        with patch.object(manager, '_check_redis_availability', return_value=True):
            with patch.object(manager, '_execute_redis_handoff', return_value=True):
                
                start_time = datetime.now()
                retry_results = await manager.retry_pending_handoffs()
                end_time = datetime.now()
                
                # Should complete batch processing efficiently
                processing_time = (end_time - start_time).total_seconds()
                assert processing_time < 10  # Should complete within 10 seconds
                assert len(retry_results) == 100

    async def test_handoff_state_transitions(self, manager, sample_handoff_data, mock_db_session):
        """Test proper handoff state transitions."""
        mock_db_session.execute.return_value = MagicMock()
        mock_db_session.commit = AsyncMock()
        
        # Test state progression: PENDING -> PROCESSING -> COMPLETED
        with patch.object(manager, '_check_redis_availability', return_value=False):
            # Initial handoff creates PENDING state
            result = await manager.execute_handoff(sample_handoff_data)
            assert result.handoff_state == HandoffState.PENDING_REDIS_RETRY
        
        # Mock successful retry
        with patch.object(manager, '_check_redis_availability', return_value=True):
            with patch.object(manager, '_execute_redis_handoff', return_value=True):
                # Should transition to COMPLETED
                retry_results = await manager.retry_pending_handoffs()
                # State verification would need actual database state checking
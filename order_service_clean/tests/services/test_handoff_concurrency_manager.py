"""
Test Handoff Concurrency Manager (GAP-REC-13)
Tests concurrency safety for handoff state transitions with locking and coordination
"""

import pytest
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, patch, MagicMock

from order_service.app.services.handoff_concurrency_manager import (
    HandoffConcurrencyManager,
    ConcurrencyConfig,
    HandoffLock,
    LockConflictError,
    DeadlockDetectionError,
    ConcurrentModificationError
)


@pytest.mark.asyncio
class TestHandoffConcurrencyManager:
    """Test handoff concurrency management."""

    @pytest.fixture
    def concurrency_config(self):
        """Default concurrency configuration."""
        return ConcurrencyConfig(
            lock_timeout_seconds=30,
            deadlock_detection_enabled=True,
            max_concurrent_handoffs_per_service=5,
            lock_cleanup_interval_seconds=60
        )

    @pytest.fixture
    def manager(self, mock_db_session, concurrency_config):
        """Create manager instance."""
        return HandoffConcurrencyManager(mock_db_session, concurrency_config)

    @pytest.fixture
    def sample_handoff_context(self):
        """Sample handoff context for testing."""
        return {
            "handoff_id": "handoff_123",
            "source_service": "order_service",
            "target_service": "ticker_service",
            "resource_ids": ["order_12345", "position_67890"],
            "operation_type": "transfer",
            "priority": "high"
        }

    async def test_acquire_handoff_lock_success(self, manager, sample_handoff_context, mock_db_session):
        """Test successful acquisition of handoff lock."""
        # Mock no existing locks
        mock_db_session.execute.return_value.fetchone.return_value = None
        mock_db_session.commit = AsyncMock()
        
        lock = await manager.acquire_handoff_lock(
            handoff_id="handoff_123",
            resource_ids=["order_12345"],
            held_by="worker_001"
        )
        
        assert isinstance(lock, HandoffLock)
        assert lock.handoff_id == "handoff_123"
        assert lock.held_by == "worker_001"
        assert lock.is_active is True
        
        # Verify lock was inserted into database
        mock_db_session.execute.assert_called()
        mock_db_session.commit.assert_called_once()

    async def test_acquire_lock_conflict_detection(self, manager, mock_db_session):
        """Test detection of lock conflicts."""
        # Mock existing conflicting lock
        existing_lock_time = datetime.now()
        mock_db_session.execute.return_value.fetchone.return_value = (
            "lock_456", "handoff_456", "order_12345", "worker_002", existing_lock_time, True
        )
        
        with pytest.raises(LockConflictError) as exc_info:
            await manager.acquire_handoff_lock(
                handoff_id="handoff_123",
                resource_ids=["order_12345"],  # Same resource as existing lock
                held_by="worker_001"
            )
        
        assert "order_12345" in str(exc_info.value)
        assert "worker_002" in str(exc_info.value)

    async def test_release_handoff_lock_success(self, manager, mock_db_session):
        """Test successful release of handoff lock."""
        mock_db_session.execute.return_value = MagicMock()
        mock_db_session.commit = AsyncMock()
        
        success = await manager.release_handoff_lock(
            handoff_id="handoff_123",
            held_by="worker_001"
        )
        
        assert success is True
        mock_db_session.execute.assert_called()
        mock_db_session.commit.assert_called_once()

    async def test_release_lock_not_held(self, manager, mock_db_session):
        """Test release of lock not held by requester."""
        # Mock no matching lock found
        mock_db_session.execute.return_value.rowcount = 0
        
        success = await manager.release_handoff_lock(
            handoff_id="handoff_123",
            held_by="worker_wrong"
        )
        
        assert success is False

    async def test_concurrent_lock_acquisition_prevention(self, manager, mock_db_session):
        """Test prevention of concurrent lock acquisition on same resource."""
        # Simulate two workers trying to acquire lock on same resource
        mock_db_session.execute.return_value.fetchone.side_effect = [
            None,  # First worker sees no lock
            ("lock_456", "handoff_456", "order_12345", "worker_001", datetime.now(), True)  # Second worker sees first lock
        ]
        mock_db_session.commit = AsyncMock()
        
        # First acquisition should succeed
        lock1 = await manager.acquire_handoff_lock(
            handoff_id="handoff_123",
            resource_ids=["order_12345"],
            held_by="worker_001"
        )
        assert lock1 is not None
        
        # Second acquisition should fail with conflict
        with pytest.raises(LockConflictError):
            await manager.acquire_handoff_lock(
                handoff_id="handoff_456",
                resource_ids=["order_12345"],
                held_by="worker_002"
            )

    async def test_deadlock_detection(self, manager, mock_db_session):
        """Test deadlock detection between handoffs."""
        # Mock scenario where handoffs would create circular dependency
        # Handoff A locks resource 1, wants resource 2
        # Handoff B locks resource 2, wants resource 1
        
        with patch.object(manager, '_detect_deadlock', return_value=True):
            with pytest.raises(DeadlockDetectionError):
                await manager.acquire_handoff_lock(
                    handoff_id="handoff_deadlock",
                    resource_ids=["resource_1", "resource_2"],
                    held_by="worker_001"
                )

    async def test_lock_timeout_handling(self, manager, mock_db_session):
        """Test handling of expired locks."""
        # Mock expired lock
        expired_time = datetime.now() - timedelta(seconds=60)
        mock_db_session.execute.return_value.fetchall.return_value = [
            ("lock_expired", "handoff_expired", "order_12345", "worker_dead", expired_time, True)
        ]
        mock_db_session.commit = AsyncMock()
        
        cleaned_count = await manager.cleanup_expired_locks()
        
        assert cleaned_count >= 0
        mock_db_session.execute.assert_called()
        mock_db_session.commit.assert_called()

    async def test_lock_renewal(self, manager, mock_db_session):
        """Test renewal of existing locks."""
        mock_db_session.execute.return_value.rowcount = 1
        mock_db_session.commit = AsyncMock()
        
        success = await manager.renew_handoff_lock(
            handoff_id="handoff_123",
            held_by="worker_001"
        )
        
        assert success is True
        mock_db_session.execute.assert_called()
        mock_db_session.commit.assert_called_once()

    async def test_handoff_state_transition_coordination(self, manager, sample_handoff_context, mock_db_session):
        """Test coordination of handoff state transitions."""
        # Mock successful lock acquisition
        mock_db_session.execute.return_value.fetchone.return_value = None
        mock_db_session.commit = AsyncMock()
        
        async with manager.coordinate_handoff_transition(
            handoff_id="handoff_123",
            from_state="pending",
            to_state="processing",
            worker_id="worker_001"
        ) as coordinator:
            # Within coordination context, state changes should be safe
            assert coordinator.is_locked is True
            assert coordinator.handoff_id == "handoff_123"
            
            # Mock state transition
            await coordinator.update_handoff_state("processing")
        
        # Lock should be released after context
        # This would be verified by checking database state

    async def test_concurrent_state_modification_prevention(self, manager, mock_db_session):
        """Test prevention of concurrent state modifications."""
        # Mock existing state modification in progress
        mock_db_session.execute.return_value.fetchone.return_value = (
            "lock_123", "handoff_123", "state_transition", "worker_other", datetime.now(), True
        )
        
        with pytest.raises(ConcurrentModificationError):
            async with manager.coordinate_handoff_transition(
                handoff_id="handoff_123",
                from_state="pending",
                to_state="processing",
                worker_id="worker_001"
            ):
                pass  # Should not reach here due to lock conflict

    async def test_worker_failure_recovery(self, manager, mock_db_session):
        """Test recovery from worker failures that leave stale locks."""
        # Mock stale locks from failed workers
        stale_time = datetime.now() - timedelta(minutes=10)
        mock_db_session.execute.return_value.fetchall.return_value = [
            ("lock_stale", "handoff_stale", "order_12345", "worker_failed", stale_time, True)
        ]
        mock_db_session.commit = AsyncMock()
        
        recovery_count = await manager.recover_from_worker_failures(
            max_lock_age_minutes=5
        )
        
        assert recovery_count >= 0
        mock_db_session.execute.assert_called()

    async def test_priority_based_lock_queue(self, manager, mock_db_session):
        """Test priority-based handling of lock requests."""
        # Mock high-priority handoff waiting for lock
        mock_db_session.execute.return_value.fetchall.return_value = [
            ("handoff_high", "high", datetime.now() - timedelta(seconds=10)),
            ("handoff_low", "low", datetime.now() - timedelta(seconds=20))
        ]
        
        next_handoff = await manager.get_next_handoff_for_processing()
        
        # Should return high-priority handoff first, despite being newer
        assert next_handoff.handoff_id == "handoff_high"

    async def test_resource_dependency_tracking(self, manager, mock_db_session):
        """Test tracking of resource dependencies between handoffs."""
        dependencies = {
            "handoff_123": ["order_1", "position_1"],
            "handoff_456": ["order_2", "position_1"]  # Shared position_1
        }
        
        conflicts = await manager.detect_resource_conflicts(dependencies)
        
        assert len(conflicts) > 0
        assert any("position_1" in conflict.resources for conflict in conflicts)

    async def test_bulk_lock_operations(self, manager, mock_db_session):
        """Test bulk acquisition and release of locks."""
        handoff_batch = [
            {"handoff_id": f"handoff_{i}", "resource_ids": [f"resource_{i}"], "held_by": "worker_batch"}
            for i in range(5)
        ]
        
        # Mock successful bulk operations
        mock_db_session.execute.return_value = MagicMock()
        mock_db_session.commit = AsyncMock()
        
        results = await manager.acquire_bulk_handoff_locks(handoff_batch)
        
        assert len(results) == 5
        assert all(r.success for r in results)

    async def test_lock_escalation_handling(self, manager, mock_db_session):
        """Test handling of lock escalation scenarios."""
        # Mock scenario where read lock needs to be escalated to write lock
        existing_read_lock = (
            "lock_read", "handoff_123", "order_12345", "worker_001", datetime.now(), True
        )
        mock_db_session.execute.return_value.fetchone.return_value = existing_read_lock
        mock_db_session.commit = AsyncMock()
        
        success = await manager.escalate_lock(
            handoff_id="handoff_123",
            resource_id="order_12345",
            from_mode="read",
            to_mode="write",
            held_by="worker_001"
        )
        
        assert success is True
        mock_db_session.execute.assert_called()

    async def test_distributed_lock_coordination(self, manager, mock_db_session):
        """Test coordination of locks across distributed workers."""
        # Mock multiple workers attempting coordination
        worker_ids = ["worker_001", "worker_002", "worker_003"]
        
        coordination_results = []
        for worker_id in worker_ids:
            try:
                # Mock different lock states for each worker
                if worker_id == "worker_001":
                    mock_db_session.execute.return_value.fetchone.return_value = None
                else:
                    mock_db_session.execute.return_value.fetchone.return_value = (
                        "lock_held", "handoff_123", "order_12345", "worker_001", datetime.now(), True
                    )
                
                lock = await manager.acquire_handoff_lock(
                    handoff_id=f"handoff_{worker_id}",
                    resource_ids=["order_12345"],
                    held_by=worker_id
                )
                coordination_results.append({"worker": worker_id, "success": True, "lock": lock})
            except LockConflictError:
                coordination_results.append({"worker": worker_id, "success": False, "lock": None})
        
        # Only one worker should succeed
        successful_workers = [r for r in coordination_results if r["success"]]
        assert len(successful_workers) == 1
        assert successful_workers[0]["worker"] == "worker_001"

    async def test_lock_metrics_and_monitoring(self, manager, mock_db_session):
        """Test collection of lock metrics for monitoring."""
        # Mock lock statistics
        mock_db_session.execute.return_value.fetchall.return_value = [
            ("handoff_123", 5, 1.5),  # handoff_id, lock_count, avg_hold_time
            ("handoff_456", 3, 2.1)
        ]
        
        metrics = await manager.get_lock_metrics()
        
        assert "total_active_locks" in metrics
        assert "average_hold_time" in metrics
        assert "lock_contention_rate" in metrics

    async def test_graceful_shutdown_lock_cleanup(self, manager, mock_db_session):
        """Test graceful cleanup of locks during shutdown."""
        # Mock locks held by shutting down worker
        mock_db_session.execute.return_value.fetchall.return_value = [
            ("lock_1", "handoff_1", "resource_1", "worker_shutdown", datetime.now(), True),
            ("lock_2", "handoff_2", "resource_2", "worker_shutdown", datetime.now(), True)
        ]
        mock_db_session.commit = AsyncMock()
        
        cleanup_count = await manager.cleanup_worker_locks(
            worker_id="worker_shutdown",
            reason="graceful_shutdown"
        )
        
        assert cleanup_count >= 0
        mock_db_session.execute.assert_called()
        mock_db_session.commit.assert_called()
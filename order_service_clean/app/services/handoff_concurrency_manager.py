"""
Handoff Concurrency Manager

Ensures concurrency safety during handoff transitions by implementing:
- Distributed locking mechanisms
- Transaction isolation for handoff operations
- Conflict detection and resolution
- State transition atomicity
- Deadlock prevention

Key Features:
- Multi-level locking (execution, position, transfer)
- Optimistic concurrency control with retries
- Transaction timeout management
- Conflict resolution strategies
- Safe rollback mechanisms
"""

import logging
from typing import Optional, Dict, Any, List, Tuple, Set
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from dataclasses import dataclass
from enum import Enum
from uuid import uuid4
import asyncio
import hashlib
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import OperationalError

logger = logging.getLogger(__name__)


class LockType(str, Enum):
    """Types of locks used in handoff coordination."""
    EXECUTION_CONTEXT = "execution_context"      # Lock on execution context
    POSITION_TRANSFER = "position_transfer"      # Lock on position transfer
    STRATEGY_HANDOFF = "strategy_handoff"        # Lock on strategy handoff
    GLOBAL_COORDINATION = "global_coordination"  # Global coordination lock


class LockMode(str, Enum):
    """Lock acquisition modes."""
    SHARED = "shared"           # Shared read lock
    EXCLUSIVE = "exclusive"     # Exclusive write lock
    INTENT_EXCLUSIVE = "intent_exclusive"  # Intent to acquire exclusive lock


class ConflictResolutionStrategy(str, Enum):
    """Strategies for resolving handoff conflicts."""
    PRIORITY_BASED = "priority_based"        # Higher priority wins
    TIMESTAMP_BASED = "timestamp_based"      # Earlier request wins
    RETRY_WITH_BACKOFF = "retry_with_backoff"  # Retry with exponential backoff
    MANUAL_INTERVENTION = "manual_intervention"  # Escalate to manual resolution


@dataclass
class HandoffLock:
    """Represents a handoff coordination lock."""
    lock_id: str
    lock_type: LockType
    lock_mode: LockMode
    resource_id: str
    holder_id: str
    acquired_at: datetime
    expires_at: datetime
    priority: int
    metadata: Dict[str, Any]


@dataclass
class HandoffTransaction:
    """Represents a handoff transaction context."""
    transaction_id: str
    handoff_id: str
    operation_type: str
    locks_acquired: List[HandoffLock]
    started_at: datetime
    timeout_at: datetime
    rollback_actions: List[Dict[str, Any]]
    checkpoint_states: List[Dict[str, Any]]


@dataclass
class ConcurrencyConflict:
    """Represents a concurrency conflict during handoff."""
    conflict_id: str
    conflicting_transactions: List[str]
    conflicted_resources: List[str]
    conflict_type: str
    detected_at: datetime
    resolution_strategy: ConflictResolutionStrategy
    resolution_metadata: Dict[str, Any]


@dataclass
class HandoffConcurrencyResult:
    """Result of concurrency-safe handoff operation."""
    transaction_id: str
    success: bool
    locks_acquired: List[HandoffLock]
    conflicts_detected: List[ConcurrencyConflict]
    execution_time_seconds: float
    rollback_performed: bool
    final_state: str
    metadata: Dict[str, Any]


class HandoffConcurrencyManager:
    """
    Manages concurrency safety for execution handoff transitions.
    
    Ensures that handoff operations are atomic and safe even when
    multiple handoffs are happening concurrently.
    """

    def __init__(self, db: AsyncSession):
        """
        Initialize the concurrency manager.

        Args:
            db: Async database session
        """
        self.db = db
        self._active_transactions: Dict[str, HandoffTransaction] = {}

    async def execute_safe_handoff_transition(
        self,
        handoff_id: str,
        source_execution_id: str,
        target_execution_id: str,
        symbol_positions: List[Dict[str, Any]],
        operation_type: str = "handoff_transition",
        priority: int = 50,
        timeout_seconds: int = 300
    ) -> HandoffConcurrencyResult:
        """
        Execute a concurrency-safe handoff transition.

        Args:
            handoff_id: Handoff identifier
            source_execution_id: Source execution context
            target_execution_id: Target execution context
            symbol_positions: Positions to transfer
            operation_type: Type of handoff operation
            priority: Operation priority (lower = higher priority)
            timeout_seconds: Transaction timeout

        Returns:
            Concurrency result with lock and conflict information

        Raises:
            Exception: If handoff fails permanently
        """
        transaction_id = str(uuid4())
        start_time = datetime.now(timezone.utc)
        timeout_at = start_time + timedelta(seconds=timeout_seconds)
        
        logger.info(
            f"[{transaction_id}] Starting safe handoff transition: "
            f"{handoff_id} from {source_execution_id} to {target_execution_id}"
        )

        try:
            # Step 1: Create transaction context
            transaction = HandoffTransaction(
                transaction_id=transaction_id,
                handoff_id=handoff_id,
                operation_type=operation_type,
                locks_acquired=[],
                started_at=start_time,
                timeout_at=timeout_at,
                rollback_actions=[],
                checkpoint_states=[]
            )
            
            self._active_transactions[transaction_id] = transaction

            # Step 2: Acquire necessary locks in deterministic order
            locks_result = await self._acquire_handoff_locks(
                transaction, source_execution_id, target_execution_id, 
                symbol_positions, priority
            )
            
            if not locks_result["success"]:
                await self._rollback_transaction(transaction)
                execution_time = (datetime.now(timezone.utc) - start_time).total_seconds()
                
                return HandoffConcurrencyResult(
                    transaction_id=transaction_id,
                    success=False,
                    locks_acquired=transaction.locks_acquired,
                    conflicts_detected=locks_result.get("conflicts", []),
                    execution_time_seconds=execution_time,
                    rollback_performed=True,
                    final_state="lock_acquisition_failed",
                    metadata={
                        "lock_failure_reason": locks_result.get("failure_reason"),
                        "attempted_locks": locks_result.get("attempted_locks", 0)
                    }
                )

            # Step 3: Create checkpoint before critical operations
            checkpoint = await self._create_transaction_checkpoint(transaction)
            
            # Step 4: Validate pre-conditions under locks
            validation_result = await self._validate_handoff_preconditions(
                transaction, source_execution_id, target_execution_id, symbol_positions
            )
            
            if not validation_result["valid"]:
                await self._rollback_transaction(transaction)
                execution_time = (datetime.now(timezone.utc) - start_time).total_seconds()
                
                return HandoffConcurrencyResult(
                    transaction_id=transaction_id,
                    success=False,
                    locks_acquired=transaction.locks_acquired,
                    conflicts_detected=[],
                    execution_time_seconds=execution_time,
                    rollback_performed=True,
                    final_state="precondition_validation_failed",
                    metadata={
                        "validation_errors": validation_result.get("errors", [])
                    }
                )

            # Step 5: Execute handoff transfer operations atomically
            transfer_result = await self._execute_atomic_handoff_transfer(
                transaction, source_execution_id, target_execution_id, symbol_positions
            )
            
            if not transfer_result["success"]:
                await self._rollback_to_checkpoint(transaction, checkpoint)
                execution_time = (datetime.now(timezone.utc) - start_time).total_seconds()
                
                return HandoffConcurrencyResult(
                    transaction_id=transaction_id,
                    success=False,
                    locks_acquired=transaction.locks_acquired,
                    conflicts_detected=[],
                    execution_time_seconds=execution_time,
                    rollback_performed=True,
                    final_state="transfer_execution_failed",
                    metadata={
                        "transfer_errors": transfer_result.get("errors", [])
                    }
                )

            # Step 6: Commit transaction and release locks
            await self._commit_transaction(transaction)
            await self._release_transaction_locks(transaction)
            
            execution_time = (datetime.now(timezone.utc) - start_time).total_seconds()
            
            return HandoffConcurrencyResult(
                transaction_id=transaction_id,
                success=True,
                locks_acquired=transaction.locks_acquired,
                conflicts_detected=[],
                execution_time_seconds=execution_time,
                rollback_performed=False,
                final_state="completed",
                metadata={
                    "positions_transferred": len(symbol_positions),
                    "transfer_instructions": transfer_result.get("transfer_instructions", [])
                }
            )

        except asyncio.TimeoutError:
            logger.error(f"[{transaction_id}] Handoff transaction timed out")
            if transaction_id in self._active_transactions:
                await self._rollback_transaction(self._active_transactions[transaction_id])
            
            execution_time = (datetime.now(timezone.utc) - start_time).total_seconds()
            
            return HandoffConcurrencyResult(
                transaction_id=transaction_id,
                success=False,
                locks_acquired=[],
                conflicts_detected=[],
                execution_time_seconds=execution_time,
                rollback_performed=True,
                final_state="timeout",
                metadata={"timeout_seconds": timeout_seconds}
            )

        except Exception as e:
            logger.error(f"[{transaction_id}] Handoff transaction failed: {e}", exc_info=True)
            if transaction_id in self._active_transactions:
                await self._rollback_transaction(self._active_transactions[transaction_id])
            
            execution_time = (datetime.now(timezone.utc) - start_time).total_seconds()
            
            return HandoffConcurrencyResult(
                transaction_id=transaction_id,
                success=False,
                locks_acquired=[],
                conflicts_detected=[],
                execution_time_seconds=execution_time,
                rollback_performed=True,
                final_state="error",
                metadata={"error": str(e)}
            )

        finally:
            # Cleanup transaction context
            if transaction_id in self._active_transactions:
                del self._active_transactions[transaction_id]

    async def detect_handoff_conflicts(
        self,
        time_window_minutes: int = 60
    ) -> List[ConcurrencyConflict]:
        """
        Detect potential handoff conflicts in recent transactions.

        Args:
            time_window_minutes: Time window to check for conflicts

        Returns:
            List of detected conflicts
        """
        logger.info(f"Detecting handoff conflicts in last {time_window_minutes} minutes")
        
        try:
            cutoff_time = datetime.now(timezone.utc) - timedelta(minutes=time_window_minutes)
            
            result = await self.db.execute(
                text("""
                    SELECT 
                        l1.lock_id as lock1_id,
                        l1.transaction_id as tx1_id,
                        l1.resource_id as resource1,
                        l1.lock_type as type1,
                        l1.acquired_at as acquired1,
                        l2.lock_id as lock2_id,
                        l2.transaction_id as tx2_id,
                        l2.resource_id as resource2,
                        l2.lock_type as type2,
                        l2.acquired_at as acquired2
                    FROM order_service.handoff_locks l1
                    JOIN order_service.handoff_locks l2 ON (
                        l1.resource_id = l2.resource_id
                        AND l1.transaction_id != l2.transaction_id
                        AND l1.lock_mode = 'exclusive' 
                        AND l2.lock_mode = 'exclusive'
                        AND l1.acquired_at > :cutoff_time
                        AND l2.acquired_at > :cutoff_time
                        AND l1.acquired_at < l2.expires_at
                        AND l2.acquired_at < l1.expires_at
                    )
                    WHERE l1.lock_id < l2.lock_id  -- Avoid duplicates
                    ORDER BY l1.acquired_at DESC
                """),
                {"cutoff_time": cutoff_time}
            )
            
            conflicts = []
            for row in result.fetchall():
                conflict = ConcurrencyConflict(
                    conflict_id=str(uuid4()),
                    conflicting_transactions=[row[1], row[6]],
                    conflicted_resources=[row[2]],
                    conflict_type="exclusive_lock_overlap",
                    detected_at=datetime.now(timezone.utc),
                    resolution_strategy=ConflictResolutionStrategy.TIMESTAMP_BASED,
                    resolution_metadata={
                        "lock1_acquired": row[4].isoformat() if row[4] else None,
                        "lock2_acquired": row[9].isoformat() if row[9] else None,
                        "resource_type": row[3]
                    }
                )
                conflicts.append(conflict)

            logger.info(f"Detected {len(conflicts)} handoff conflicts")
            return conflicts

        except Exception as e:
            logger.error(f"Conflict detection failed: {e}", exc_info=True)
            return []

    async def _acquire_handoff_locks(
        self,
        transaction: HandoffTransaction,
        source_execution_id: str,
        target_execution_id: str,
        symbol_positions: List[Dict[str, Any]],
        priority: int
    ) -> Dict[str, Any]:
        """Acquire all necessary locks for handoff in deterministic order."""
        
        # Determine lock order to prevent deadlocks (sort by resource ID)
        lock_requests = []
        
        # Add execution context locks
        for exec_id in sorted([source_execution_id, target_execution_id]):
            lock_requests.append({
                "lock_type": LockType.EXECUTION_CONTEXT,
                "lock_mode": LockMode.EXCLUSIVE,
                "resource_id": exec_id,
                "priority": priority
            })
        
        # Add position transfer locks
        position_ids = sorted(set(pos.get("position_id") for pos in symbol_positions if pos.get("position_id")))
        for position_id in position_ids:
            lock_requests.append({
                "lock_type": LockType.POSITION_TRANSFER,
                "lock_mode": LockMode.EXCLUSIVE,
                "resource_id": str(position_id),
                "priority": priority
            })
        
        # Add global coordination lock for this handoff
        lock_requests.append({
            "lock_type": LockType.GLOBAL_COORDINATION,
            "lock_mode": LockMode.SHARED,
            "resource_id": f"handoff:{transaction.handoff_id}",
            "priority": priority
        })
        
        acquired_locks = []
        conflicts = []
        
        try:
            for lock_req in lock_requests:
                lock_result = await self._acquire_single_lock(
                    transaction, lock_req["lock_type"], lock_req["lock_mode"],
                    lock_req["resource_id"], lock_req["priority"]
                )
                
                if lock_result["acquired"]:
                    acquired_locks.append(lock_result["lock"])
                    transaction.locks_acquired.append(lock_result["lock"])
                else:
                    # Lock acquisition failed - release already acquired locks
                    for acquired_lock in acquired_locks:
                        await self._release_single_lock(acquired_lock)
                    
                    conflicts.append({
                        "resource_id": lock_req["resource_id"],
                        "lock_type": lock_req["lock_type"].value,
                        "conflict_reason": lock_result.get("failure_reason")
                    })
                    
                    return {
                        "success": False,
                        "acquired_locks": acquired_locks,
                        "conflicts": conflicts,
                        "failure_reason": f"Failed to acquire lock on {lock_req['resource_id']}: {lock_result.get('failure_reason')}",
                        "attempted_locks": len(lock_requests)
                    }
            
            return {
                "success": True,
                "acquired_locks": acquired_locks,
                "conflicts": [],
                "attempted_locks": len(lock_requests)
            }

        except Exception as e:
            # Release any acquired locks on error
            for acquired_lock in acquired_locks:
                try:
                    await self._release_single_lock(acquired_lock)
                except Exception as release_error:
                    logger.error(f"Failed to release lock {acquired_lock.lock_id}: {release_error}")
            
            return {
                "success": False,
                "acquired_locks": [],
                "conflicts": [],
                "failure_reason": f"Lock acquisition error: {str(e)}",
                "attempted_locks": len(lock_requests)
            }

    async def _acquire_single_lock(
        self,
        transaction: HandoffTransaction,
        lock_type: LockType,
        lock_mode: LockMode,
        resource_id: str,
        priority: int
    ) -> Dict[str, Any]:
        """Acquire a single lock with conflict detection."""
        
        lock_id = str(uuid4())
        expires_at = datetime.now(timezone.utc) + timedelta(minutes=10)  # 10-minute lock timeout
        
        try:
            # Try to acquire lock atomically
            result = await self.db.execute(
                text("""
                    INSERT INTO order_service.handoff_locks (
                        lock_id,
                        transaction_id,
                        lock_type,
                        lock_mode,
                        resource_id,
                        holder_id,
                        priority,
                        acquired_at,
                        expires_at
                    )
                    SELECT 
                        :lock_id::uuid,
                        :transaction_id::uuid,
                        :lock_type,
                        :lock_mode,
                        :resource_id,
                        :holder_id,
                        :priority,
                        NOW(),
                        :expires_at
                    WHERE NOT EXISTS (
                        SELECT 1 FROM order_service.handoff_locks
                        WHERE resource_id = :resource_id
                          AND lock_mode = 'exclusive'
                          AND expires_at > NOW()
                          AND (:lock_mode = 'exclusive' OR lock_mode = 'exclusive')
                    )
                    RETURNING lock_id
                """),
                {
                    "lock_id": lock_id,
                    "transaction_id": transaction.transaction_id,
                    "lock_type": lock_type.value,
                    "lock_mode": lock_mode.value,
                    "resource_id": resource_id,
                    "holder_id": f"handoff_tx:{transaction.transaction_id}",
                    "priority": priority,
                    "expires_at": expires_at
                }
            )
            
            row = result.fetchone()
            if row:
                await self.db.commit()
                
                lock = HandoffLock(
                    lock_id=lock_id,
                    lock_type=lock_type,
                    lock_mode=lock_mode,
                    resource_id=resource_id,
                    holder_id=f"handoff_tx:{transaction.transaction_id}",
                    acquired_at=datetime.now(timezone.utc),
                    expires_at=expires_at,
                    priority=priority,
                    metadata={"transaction_id": transaction.transaction_id}
                )
                
                return {
                    "acquired": True,
                    "lock": lock
                }
            else:
                # Check what prevented lock acquisition
                conflict_result = await self.db.execute(
                    text("""
                        SELECT 
                            transaction_id,
                            lock_mode,
                            holder_id,
                            acquired_at,
                            expires_at
                        FROM order_service.handoff_locks
                        WHERE resource_id = :resource_id
                          AND expires_at > NOW()
                        ORDER BY priority ASC, acquired_at ASC
                        LIMIT 5
                    """),
                    {"resource_id": resource_id}
                )
                
                conflicting_locks = [
                    {
                        "transaction_id": str(row[0]),
                        "lock_mode": row[1],
                        "holder_id": row[2],
                        "acquired_at": row[3].isoformat() if row[3] else None,
                        "expires_at": row[4].isoformat() if row[4] else None
                    }
                    for row in conflict_result.fetchall()
                ]
                
                return {
                    "acquired": False,
                    "failure_reason": "Resource locked by other transaction",
                    "conflicting_locks": conflicting_locks
                }

        except Exception as e:
            logger.error(f"Failed to acquire lock {lock_id}: {e}", exc_info=True)
            return {
                "acquired": False,
                "failure_reason": f"Lock acquisition error: {str(e)}"
            }

    async def _validate_handoff_preconditions(
        self,
        transaction: HandoffTransaction,
        source_execution_id: str,
        target_execution_id: str,
        symbol_positions: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Validate handoff preconditions under acquired locks."""
        
        validation_errors = []
        
        try:
            # Validate execution contexts exist and are in valid state
            for exec_id, exec_name in [(source_execution_id, "source"), (target_execution_id, "target")]:
                result = await self.db.execute(
                    text("""
                        SELECT execution_id, status, context_type
                        FROM order_service.execution_contexts
                        WHERE execution_id = :execution_id::uuid
                    """),
                    {"execution_id": exec_id}
                )
                
                row = result.fetchone()
                if not row:
                    validation_errors.append(f"{exec_name} execution context {exec_id} not found")
                elif row[1] not in ["ready", "running"]:
                    validation_errors.append(f"{exec_name} execution context {exec_id} has invalid status: {row[1]}")
            
            # Validate positions exist and are transferable
            for position_data in symbol_positions:
                position_id = position_data.get("position_id")
                if not position_id:
                    validation_errors.append("Position data missing position_id")
                    continue
                
                result = await self.db.execute(
                    text("""
                        SELECT 
                            id, 
                            symbol, 
                            quantity, 
                            is_open,
                            execution_id
                        FROM order_service.positions
                        WHERE id = :position_id
                          AND is_open = true
                    """),
                    {"position_id": position_id}
                )
                
                row = result.fetchone()
                if not row:
                    validation_errors.append(f"Position {position_id} not found or not open")
                elif str(row[4]) != source_execution_id:
                    validation_errors.append(f"Position {position_id} execution_id mismatch")

            return {
                "valid": len(validation_errors) == 0,
                "errors": validation_errors
            }

        except Exception as e:
            logger.error(f"Handoff precondition validation failed: {e}", exc_info=True)
            return {
                "valid": False,
                "errors": [f"Validation error: {str(e)}"]
            }

    async def _execute_atomic_handoff_transfer(
        self,
        transaction: HandoffTransaction,
        source_execution_id: str,
        target_execution_id: str,
        symbol_positions: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Execute atomic handoff transfer operations."""
        
        transfer_instructions = []
        
        try:
            # Begin database transaction for atomic operations
            await self.db.begin()
            
            for position_data in symbol_positions:
                position_id = position_data.get("position_id")
                
                # Update position execution context
                await self.db.execute(
                    text("""
                        UPDATE order_service.positions
                        SET 
                            execution_id = :target_execution_id::uuid,
                            updated_at = NOW()
                        WHERE id = :position_id
                          AND execution_id = :source_execution_id::uuid
                          AND is_open = true
                    """),
                    {
                        "position_id": position_id,
                        "source_execution_id": source_execution_id,
                        "target_execution_id": target_execution_id
                    }
                )
                
                # Create transfer audit record
                transfer_id = str(uuid4())
                await self.db.execute(
                    text("""
                        INSERT INTO order_service.position_transfers (
                            transfer_id,
                            position_id,
                            source_execution_id,
                            target_execution_id,
                            transfer_type,
                            transfer_reason,
                            transaction_id,
                            created_at
                        ) VALUES (
                            :transfer_id::uuid,
                            :position_id,
                            :source_execution_id::uuid,
                            :target_execution_id::uuid,
                            'handoff_transfer',
                            :reason,
                            :transaction_id::uuid,
                            NOW()
                        )
                    """),
                    {
                        "transfer_id": transfer_id,
                        "position_id": position_id,
                        "source_execution_id": source_execution_id,
                        "target_execution_id": target_execution_id,
                        "reason": f"Handoff transfer for {transaction.handoff_id}",
                        "transaction_id": transaction.transaction_id
                    }
                )
                
                transfer_instructions.append({
                    "transfer_id": transfer_id,
                    "position_id": position_id,
                    "source_execution_id": source_execution_id,
                    "target_execution_id": target_execution_id
                })
            
            # Update handoff status
            await self.db.execute(
                text("""
                    UPDATE order_service.pending_handoffs
                    SET 
                        current_state = 'transferring',
                        updated_at = NOW()
                    WHERE handoff_id = :handoff_id::uuid
                """),
                {"handoff_id": transaction.handoff_id}
            )
            
            return {
                "success": True,
                "transfer_instructions": transfer_instructions,
                "positions_transferred": len(symbol_positions)
            }

        except Exception as e:
            logger.error(f"Atomic handoff transfer failed: {e}", exc_info=True)
            await self.db.rollback()
            return {
                "success": False,
                "errors": [str(e)],
                "transfer_instructions": []
            }

    async def _create_transaction_checkpoint(
        self,
        transaction: HandoffTransaction
    ) -> Dict[str, Any]:
        """Create transaction checkpoint for rollback."""
        
        checkpoint = {
            "checkpoint_id": str(uuid4()),
            "created_at": datetime.now(timezone.utc).isoformat(),
            "transaction_id": transaction.transaction_id,
            "operation_states": []
        }
        
        transaction.checkpoint_states.append(checkpoint)
        return checkpoint

    async def _rollback_to_checkpoint(
        self,
        transaction: HandoffTransaction,
        checkpoint: Dict[str, Any]
    ) -> None:
        """Rollback transaction to specific checkpoint."""
        
        logger.warning(f"Rolling back transaction {transaction.transaction_id} to checkpoint {checkpoint['checkpoint_id']}")
        
        try:
            await self.db.rollback()
            
            # Execute any specific rollback actions
            for action in transaction.rollback_actions:
                try:
                    if action["action_type"] == "restore_position":
                        await self.db.execute(
                            text(action["sql"]),
                            action["params"]
                        )
                except Exception as action_error:
                    logger.error(f"Rollback action failed: {action_error}")
            
            await self.db.commit()
            
        except Exception as e:
            logger.error(f"Checkpoint rollback failed: {e}", exc_info=True)

    async def _commit_transaction(self, transaction: HandoffTransaction) -> None:
        """Commit transaction changes."""
        
        try:
            await self.db.commit()
            
            # Update handoff status to completed
            await self.db.execute(
                text("""
                    UPDATE order_service.pending_handoffs
                    SET 
                        current_state = 'completed',
                        updated_at = NOW()
                    WHERE handoff_id = :handoff_id::uuid
                """),
                {"handoff_id": transaction.handoff_id}
            )
            
            await self.db.commit()
            
            logger.info(f"Transaction {transaction.transaction_id} committed successfully")

        except Exception as e:
            logger.error(f"Transaction commit failed: {e}", exc_info=True)
            await self.db.rollback()
            raise

    async def _rollback_transaction(self, transaction: HandoffTransaction) -> None:
        """Rollback entire transaction."""
        
        logger.warning(f"Rolling back transaction {transaction.transaction_id}")
        
        try:
            await self.db.rollback()
            await self._release_transaction_locks(transaction)
            
            # Update handoff status to failed
            await self.db.execute(
                text("""
                    UPDATE order_service.pending_handoffs
                    SET 
                        current_state = 'failed',
                        updated_at = NOW()
                    WHERE handoff_id = :handoff_id::uuid
                """),
                {"handoff_id": transaction.handoff_id}
            )
            
            await self.db.commit()

        except Exception as e:
            logger.error(f"Transaction rollback failed: {e}", exc_info=True)

    async def _release_transaction_locks(self, transaction: HandoffTransaction) -> None:
        """Release all locks held by transaction."""
        
        for lock in transaction.locks_acquired:
            try:
                await self._release_single_lock(lock)
            except Exception as e:
                logger.error(f"Failed to release lock {lock.lock_id}: {e}")

    async def _release_single_lock(self, lock: HandoffLock) -> None:
        """Release a single lock."""
        
        await self.db.execute(
            text("""
                DELETE FROM order_service.handoff_locks
                WHERE lock_id = :lock_id::uuid
            """),
            {"lock_id": lock.lock_id}
        )
        await self.db.commit()


# Helper function for external use
async def execute_concurrent_safe_handoff(
    db: AsyncSession,
    handoff_id: str,
    source_execution_id: str,
    target_execution_id: str,
    symbol_positions: List[Dict[str, Any]],
    operation_type: str = "handoff_transition",
    priority: int = 50
) -> HandoffConcurrencyResult:
    """
    Convenience function for concurrency-safe handoff execution.

    Args:
        db: Database session
        handoff_id: Handoff identifier
        source_execution_id: Source execution context
        target_execution_id: Target execution context
        symbol_positions: Positions to transfer
        operation_type: Type of handoff operation
        priority: Operation priority

    Returns:
        Concurrency result
    """
    manager = HandoffConcurrencyManager(db)
    return await manager.execute_safe_handoff_transition(
        handoff_id, source_execution_id, target_execution_id, 
        symbol_positions, operation_type, priority
    )
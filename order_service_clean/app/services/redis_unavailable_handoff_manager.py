"""
Redis Unavailable Handoff Manager

Handles execution handoff coordination when Redis is unavailable by implementing:
- Safe pending state management
- Retry logic with exponential backoff
- Fallback coordination mechanisms
- State persistence for recovery

Key Features:
- Persistent pending handoff state
- Exponential backoff retry strategy
- Database-backed coordination fallback
- Safe recovery mechanisms
- Handoff status monitoring
"""

import logging
from typing import Optional, Dict, Any, List
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass
from enum import Enum
import asyncio
import json
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)


class HandoffState(str, Enum):
    """Handoff coordination states."""
    PENDING = "pending"                    # Handoff requested, waiting for coordination
    COORDINATING = "coordinating"          # Actively coordinating handoff
    COORDINATED = "coordinated"            # Coordination complete, ready for execution
    TRANSFERRING = "transferring"          # Handoff in progress
    COMPLETED = "completed"                # Handoff completed successfully
    FAILED = "failed"                      # Handoff failed, needs manual intervention
    RETRYING = "retrying"                  # In retry cycle due to Redis unavailability


class HandoffRetryStrategy(str, Enum):
    """Retry strategies for handoff coordination."""
    EXPONENTIAL_BACKOFF = "exponential_backoff"     # Exponential backoff with jitter
    FIXED_INTERVAL = "fixed_interval"               # Fixed interval retry
    IMMEDIATE_FALLBACK = "immediate_fallback"       # Immediate fallback to DB coordination


@dataclass
class HandoffCoordinationRequest:
    """Request for handoff coordination."""
    handoff_id: str
    source_execution_id: str
    target_execution_id: str
    symbol_positions: List[Dict[str, Any]]
    handoff_reason: str
    requested_by: str
    priority: int  # 1=highest, 10=lowest
    timeout_seconds: int
    retry_strategy: HandoffRetryStrategy


@dataclass
class HandoffRetryState:
    """State for handoff retry management."""
    handoff_id: str
    attempt_count: int
    last_attempt_at: datetime
    next_retry_at: datetime
    max_attempts: int
    backoff_multiplier: float
    base_delay_seconds: int
    jitter_enabled: bool
    failure_reasons: List[str]


@dataclass
class HandoffCoordinationResult:
    """Result of handoff coordination attempt."""
    handoff_id: str
    success: bool
    final_state: HandoffState
    coordination_method: str  # 'redis', 'database', 'manual'
    execution_time_seconds: float
    transfer_instructions: List[Dict[str, Any]]
    failure_reason: Optional[str]
    retry_state: Optional[HandoffRetryState]
    metadata: Dict[str, Any]


class RedisUnavailableHandoffManager:
    """
    Manages execution handoff coordination when Redis is unavailable.
    
    Provides safe pending state management and retry logic to ensure
    handoff coordination continues even when Redis is temporarily unavailable.
    """

    def __init__(self, db: AsyncSession, redis_client=None):
        """
        Initialize the handoff manager.

        Args:
            db: Async database session
            redis_client: Optional Redis client (for availability checking)
        """
        self.db = db
        self.redis_client = redis_client

    async def coordinate_handoff_with_fallback(
        self,
        coordination_request: HandoffCoordinationRequest
    ) -> HandoffCoordinationResult:
        """
        Coordinate execution handoff with Redis unavailability fallback.

        Args:
            coordination_request: Handoff coordination request

        Returns:
            Coordination result with transfer instructions or retry state

        Raises:
            Exception: If coordination fails permanently
        """
        handoff_id = coordination_request.handoff_id
        start_time = datetime.now(timezone.utc)
        
        logger.info(
            f"[{handoff_id}] Starting handoff coordination with fallback: "
            f"{coordination_request.source_execution_id} -> {coordination_request.target_execution_id}"
        )

        try:
            # Step 1: Create or update pending handoff state
            await self._create_pending_handoff_state(coordination_request)

            # Step 2: Try Redis coordination first
            redis_result = await self._try_redis_coordination(coordination_request)
            if redis_result.success:
                await self._update_handoff_state(handoff_id, HandoffState.COORDINATED)
                execution_time = (datetime.now(timezone.utc) - start_time).total_seconds()
                
                return HandoffCoordinationResult(
                    handoff_id=handoff_id,
                    success=True,
                    final_state=HandoffState.COORDINATED,
                    coordination_method="redis",
                    execution_time_seconds=execution_time,
                    transfer_instructions=redis_result.transfer_instructions,
                    failure_reason=None,
                    retry_state=None,
                    metadata={
                        "coordination_method": "redis",
                        "attempts": 1,
                        "fallback_used": False
                    }
                )

            # Step 3: Redis unavailable - try database coordination fallback
            logger.warning(f"[{handoff_id}] Redis unavailable, attempting database coordination fallback")
            await self._update_handoff_state(handoff_id, HandoffState.RETRYING)
            
            db_result = await self._try_database_coordination(coordination_request)
            if db_result.success:
                await self._update_handoff_state(handoff_id, HandoffState.COORDINATED)
                execution_time = (datetime.now(timezone.utc) - start_time).total_seconds()
                
                return HandoffCoordinationResult(
                    handoff_id=handoff_id,
                    success=True,
                    final_state=HandoffState.COORDINATED,
                    coordination_method="database",
                    execution_time_seconds=execution_time,
                    transfer_instructions=db_result.transfer_instructions,
                    failure_reason=None,
                    retry_state=None,
                    metadata={
                        "coordination_method": "database",
                        "attempts": 2,
                        "fallback_used": True
                    }
                )

            # Step 4: Both methods failed - set up retry mechanism
            logger.error(f"[{handoff_id}] Both Redis and database coordination failed")
            retry_state = await self._setup_retry_mechanism(coordination_request)
            await self._update_handoff_state(handoff_id, HandoffState.RETRYING)
            
            execution_time = (datetime.now(timezone.utc) - start_time).total_seconds()
            
            return HandoffCoordinationResult(
                handoff_id=handoff_id,
                success=False,
                final_state=HandoffState.RETRYING,
                coordination_method="retry_scheduled",
                execution_time_seconds=execution_time,
                transfer_instructions=[],
                failure_reason="Both Redis and database coordination failed",
                retry_state=retry_state,
                metadata={
                    "coordination_method": "retry_scheduled",
                    "attempts": 2,
                    "fallback_used": True,
                    "next_retry": retry_state.next_retry_at.isoformat()
                }
            )

        except Exception as e:
            logger.error(f"[{handoff_id}] Handoff coordination failed: {e}", exc_info=True)
            await self._update_handoff_state(handoff_id, HandoffState.FAILED)
            execution_time = (datetime.now(timezone.utc) - start_time).total_seconds()
            
            return HandoffCoordinationResult(
                handoff_id=handoff_id,
                success=False,
                final_state=HandoffState.FAILED,
                coordination_method="error",
                execution_time_seconds=execution_time,
                transfer_instructions=[],
                failure_reason=str(e),
                retry_state=None,
                metadata={"error": str(e), "coordination_failed": True}
            )

    async def process_retry_queue(self, max_retries: int = 10) -> Dict[str, Any]:
        """
        Process pending handoff retries.

        Args:
            max_retries: Maximum number of retries to process in one batch

        Returns:
            Processing summary with success/failure counts
        """
        logger.info(f"Processing handoff retry queue (max {max_retries} retries)")
        
        try:
            # Get pending retries
            pending_retries = await self._get_pending_retries(max_retries)
            
            results = {
                "processed": 0,
                "succeeded": 0,
                "failed": 0,
                "still_retrying": 0,
                "details": []
            }

            for retry_info in pending_retries:
                handoff_id = retry_info['handoff_id']
                
                try:
                    # Reconstruct coordination request
                    request = await self._reconstruct_coordination_request(retry_info)
                    
                    # Attempt coordination again
                    result = await self._retry_coordination(request, retry_info)
                    
                    results["processed"] += 1
                    
                    if result.success:
                        results["succeeded"] += 1
                        logger.info(f"[{handoff_id}] Retry coordination succeeded")
                    elif result.final_state == HandoffState.RETRYING:
                        results["still_retrying"] += 1
                        logger.info(f"[{handoff_id}] Still retrying - next attempt at {result.retry_state.next_retry_at}")
                    else:
                        results["failed"] += 1
                        logger.error(f"[{handoff_id}] Retry coordination failed permanently")
                    
                    results["details"].append({
                        "handoff_id": handoff_id,
                        "success": result.success,
                        "final_state": result.final_state.value,
                        "coordination_method": result.coordination_method
                    })

                except Exception as e:
                    logger.error(f"[{handoff_id}] Retry processing failed: {e}", exc_info=True)
                    results["processed"] += 1
                    results["failed"] += 1
                    results["details"].append({
                        "handoff_id": handoff_id,
                        "success": False,
                        "error": str(e)
                    })

            logger.info(
                f"Retry queue processing complete: {results['processed']} processed, "
                f"{results['succeeded']} succeeded, {results['failed']} failed, "
                f"{results['still_retrying']} still retrying"
            )
            
            return results

        except Exception as e:
            logger.error(f"Retry queue processing failed: {e}", exc_info=True)
            raise

    async def get_handoff_status(self, handoff_id: str) -> Optional[Dict[str, Any]]:
        """
        Get current status of a handoff coordination.

        Args:
            handoff_id: Handoff identifier

        Returns:
            Handoff status information or None if not found
        """
        result = await self.db.execute(
            text("""
                SELECT 
                    handoff_id,
                    current_state,
                    source_execution_id,
                    target_execution_id,
                    handoff_reason,
                    requested_by,
                    created_at,
                    updated_at,
                    coordination_data,
                    retry_state
                FROM order_service.pending_handoffs
                WHERE handoff_id = :handoff_id::uuid
            """),
            {"handoff_id": handoff_id}
        )
        
        row = result.fetchone()
        if not row:
            return None
        
        return {
            "handoff_id": str(row[0]),
            "current_state": row[1],
            "source_execution_id": str(row[2]) if row[2] else None,
            "target_execution_id": str(row[3]) if row[3] else None,
            "handoff_reason": row[4],
            "requested_by": row[5],
            "created_at": row[6].isoformat() if row[6] else None,
            "updated_at": row[7].isoformat() if row[7] else None,
            "coordination_data": row[8],
            "retry_state": row[9]
        }

    async def _create_pending_handoff_state(
        self,
        request: HandoffCoordinationRequest
    ) -> None:
        """Create persistent pending handoff state."""
        await self.db.execute(
            text("""
                INSERT INTO order_service.pending_handoffs (
                    handoff_id,
                    current_state,
                    source_execution_id,
                    target_execution_id,
                    handoff_reason,
                    requested_by,
                    priority,
                    timeout_seconds,
                    retry_strategy,
                    coordination_data,
                    created_at
                ) VALUES (
                    :handoff_id::uuid,
                    :current_state,
                    :source_execution_id::uuid,
                    :target_execution_id::uuid,
                    :handoff_reason,
                    :requested_by,
                    :priority,
                    :timeout_seconds,
                    :retry_strategy,
                    :coordination_data::jsonb,
                    NOW()
                ) ON CONFLICT (handoff_id) DO UPDATE SET
                    current_state = :current_state,
                    updated_at = NOW()
            """),
            {
                "handoff_id": request.handoff_id,
                "current_state": HandoffState.PENDING.value,
                "source_execution_id": request.source_execution_id,
                "target_execution_id": request.target_execution_id,
                "handoff_reason": request.handoff_reason,
                "requested_by": request.requested_by,
                "priority": request.priority,
                "timeout_seconds": request.timeout_seconds,
                "retry_strategy": request.retry_strategy.value,
                "coordination_data": {
                    "symbol_positions": request.symbol_positions,
                    "original_request_time": datetime.now(timezone.utc).isoformat()
                }
            }
        )
        await self.db.commit()

    async def _try_redis_coordination(
        self,
        request: HandoffCoordinationRequest
    ) -> HandoffCoordinationResult:
        """Attempt Redis-based coordination."""
        handoff_id = request.handoff_id
        
        try:
            # Check Redis availability
            if not self.redis_client:
                return HandoffCoordinationResult(
                    handoff_id=handoff_id,
                    success=False,
                    final_state=HandoffState.PENDING,
                    coordination_method="redis_unavailable",
                    execution_time_seconds=0.0,
                    transfer_instructions=[],
                    failure_reason="Redis client not available",
                    retry_state=None,
                    metadata={"redis_client": "not_configured"}
                )

            # Try Redis ping with timeout
            try:
                await asyncio.wait_for(self.redis_client.ping(), timeout=5.0)
            except (asyncio.TimeoutError, Exception) as e:
                return HandoffCoordinationResult(
                    handoff_id=handoff_id,
                    success=False,
                    final_state=HandoffState.PENDING,
                    coordination_method="redis_unavailable",
                    execution_time_seconds=0.0,
                    transfer_instructions=[],
                    failure_reason=f"Redis ping failed: {str(e)}",
                    retry_state=None,
                    metadata={"redis_ping_error": str(e)}
                )

            # Perform Redis-based coordination (simplified for now)
            coordination_key = f"handoff:coordination:{handoff_id}"
            
            # Set coordination lock with timeout
            lock_acquired = await self.redis_client.set(
                coordination_key, 
                json.dumps({
                    "source": request.source_execution_id,
                    "target": request.target_execution_id,
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }),
                ex=request.timeout_seconds,
                nx=True
            )
            
            if not lock_acquired:
                return HandoffCoordinationResult(
                    handoff_id=handoff_id,
                    success=False,
                    final_state=HandoffState.PENDING,
                    coordination_method="redis_conflict",
                    execution_time_seconds=0.1,
                    transfer_instructions=[],
                    failure_reason="Redis coordination lock could not be acquired",
                    retry_state=None,
                    metadata={"coordination_key": coordination_key}
                )

            # Generate transfer instructions using existing service
            from .transfer_instruction_generator import generate_handoff_transfer_instructions
            
            transfer_batch = await generate_handoff_transfer_instructions(
                self.db,
                request.source_execution_id,
                request.target_execution_id,
                request.symbol_positions,
                request.handoff_reason,
                request.requested_by
            )
            
            # Store coordination result in Redis
            result_key = f"handoff:result:{handoff_id}"
            await self.redis_client.set(
                result_key,
                json.dumps({
                    "success": True,
                    "transfer_batch_id": transfer_batch.batch_id,
                    "instructions_count": len(transfer_batch.instructions),
                    "completed_at": datetime.now(timezone.utc).isoformat()
                }),
                ex=3600  # Keep result for 1 hour
            )
            
            # Convert transfer batch to instructions
            transfer_instructions = [
                {
                    "instruction_id": instr.instruction_id,
                    "instruction_type": instr.instruction_type.value,
                    "symbol": instr.symbol,
                    "quantity": str(instr.quantity),
                    "source_execution_id": instr.source_execution_id,
                    "target_execution_id": instr.target_execution_id
                }
                for instr in transfer_batch.instructions
            ]
            
            return HandoffCoordinationResult(
                handoff_id=handoff_id,
                success=True,
                final_state=HandoffState.COORDINATED,
                coordination_method="redis",
                execution_time_seconds=0.5,
                transfer_instructions=transfer_instructions,
                failure_reason=None,
                retry_state=None,
                metadata={
                    "transfer_batch_id": transfer_batch.batch_id,
                    "coordination_key": coordination_key,
                    "result_key": result_key
                }
            )

        except Exception as e:
            logger.error(f"[{handoff_id}] Redis coordination failed: {e}", exc_info=True)
            return HandoffCoordinationResult(
                handoff_id=handoff_id,
                success=False,
                final_state=HandoffState.PENDING,
                coordination_method="redis_error",
                execution_time_seconds=0.1,
                transfer_instructions=[],
                failure_reason=f"Redis coordination error: {str(e)}",
                retry_state=None,
                metadata={"error": str(e)}
            )

    async def _try_database_coordination(
        self,
        request: HandoffCoordinationRequest
    ) -> HandoffCoordinationResult:
        """Attempt database-based coordination fallback."""
        handoff_id = request.handoff_id
        
        try:
            # Create coordination lock in database
            lock_result = await self.db.execute(
                text("""
                    INSERT INTO order_service.handoff_coordination_locks (
                        handoff_id,
                        source_execution_id,
                        target_execution_id,
                        lock_holder,
                        locked_at,
                        expires_at
                    ) VALUES (
                        :handoff_id::uuid,
                        :source_execution_id::uuid,
                        :target_execution_id::uuid,
                        :lock_holder,
                        NOW(),
                        NOW() + INTERVAL ':timeout_seconds seconds'
                    )
                    ON CONFLICT (handoff_id) DO NOTHING
                    RETURNING handoff_id
                """),
                {
                    "handoff_id": handoff_id,
                    "source_execution_id": request.source_execution_id,
                    "target_execution_id": request.target_execution_id,
                    "lock_holder": f"handoff_manager:{request.requested_by}",
                    "timeout_seconds": request.timeout_seconds
                }
            )
            
            if not lock_result.fetchone():
                return HandoffCoordinationResult(
                    handoff_id=handoff_id,
                    success=False,
                    final_state=HandoffState.PENDING,
                    coordination_method="database_conflict",
                    execution_time_seconds=0.1,
                    transfer_instructions=[],
                    failure_reason="Database coordination lock could not be acquired",
                    retry_state=None,
                    metadata={"lock_conflict": True}
                )

            await self.db.commit()

            # Generate transfer instructions
            from .transfer_instruction_generator import generate_handoff_transfer_instructions
            
            transfer_batch = await generate_handoff_transfer_instructions(
                self.db,
                request.source_execution_id,
                request.target_execution_id,
                request.symbol_positions,
                request.handoff_reason,
                request.requested_by
            )
            
            # Store coordination result in database
            await self.db.execute(
                text("""
                    UPDATE order_service.handoff_coordination_locks
                    SET 
                        coordination_result = :result::jsonb,
                        completed_at = NOW()
                    WHERE handoff_id = :handoff_id::uuid
                """),
                {
                    "handoff_id": handoff_id,
                    "result": {
                        "success": True,
                        "transfer_batch_id": transfer_batch.batch_id,
                        "instructions_count": len(transfer_batch.instructions),
                        "coordination_method": "database"
                    }
                }
            )
            await self.db.commit()
            
            # Convert transfer batch to instructions
            transfer_instructions = [
                {
                    "instruction_id": instr.instruction_id,
                    "instruction_type": instr.instruction_type.value,
                    "symbol": instr.symbol,
                    "quantity": str(instr.quantity),
                    "source_execution_id": instr.source_execution_id,
                    "target_execution_id": instr.target_execution_id
                }
                for instr in transfer_batch.instructions
            ]
            
            return HandoffCoordinationResult(
                handoff_id=handoff_id,
                success=True,
                final_state=HandoffState.COORDINATED,
                coordination_method="database",
                execution_time_seconds=1.0,
                transfer_instructions=transfer_instructions,
                failure_reason=None,
                retry_state=None,
                metadata={
                    "transfer_batch_id": transfer_batch.batch_id,
                    "coordination_method": "database_fallback"
                }
            )

        except Exception as e:
            logger.error(f"[{handoff_id}] Database coordination failed: {e}", exc_info=True)
            return HandoffCoordinationResult(
                handoff_id=handoff_id,
                success=False,
                final_state=HandoffState.PENDING,
                coordination_method="database_error",
                execution_time_seconds=0.5,
                transfer_instructions=[],
                failure_reason=f"Database coordination error: {str(e)}",
                retry_state=None,
                metadata={"error": str(e)}
            )

    async def _setup_retry_mechanism(
        self,
        request: HandoffCoordinationRequest
    ) -> HandoffRetryState:
        """Set up retry mechanism for failed coordination."""
        base_delay = 30  # 30 seconds base delay
        max_attempts = 10
        
        if request.retry_strategy == HandoffRetryStrategy.EXPONENTIAL_BACKOFF:
            next_retry = datetime.now(timezone.utc) + timedelta(seconds=base_delay)
            backoff_multiplier = 2.0
        else:  # FIXED_INTERVAL
            next_retry = datetime.now(timezone.utc) + timedelta(seconds=base_delay)
            backoff_multiplier = 1.0
        
        retry_state = HandoffRetryState(
            handoff_id=request.handoff_id,
            attempt_count=2,  # Already tried Redis and DB
            last_attempt_at=datetime.now(timezone.utc),
            next_retry_at=next_retry,
            max_attempts=max_attempts,
            backoff_multiplier=backoff_multiplier,
            base_delay_seconds=base_delay,
            jitter_enabled=True,
            failure_reasons=["redis_unavailable", "database_coordination_failed"]
        )
        
        # Store retry state
        await self.db.execute(
            text("""
                UPDATE order_service.pending_handoffs
                SET 
                    retry_state = :retry_state::jsonb,
                    updated_at = NOW()
                WHERE handoff_id = :handoff_id::uuid
            """),
            {
                "handoff_id": request.handoff_id,
                "retry_state": {
                    "attempt_count": retry_state.attempt_count,
                    "last_attempt_at": retry_state.last_attempt_at.isoformat(),
                    "next_retry_at": retry_state.next_retry_at.isoformat(),
                    "max_attempts": retry_state.max_attempts,
                    "backoff_multiplier": retry_state.backoff_multiplier,
                    "base_delay_seconds": retry_state.base_delay_seconds,
                    "jitter_enabled": retry_state.jitter_enabled,
                    "failure_reasons": retry_state.failure_reasons
                }
            }
        )
        await self.db.commit()
        
        return retry_state

    async def _update_handoff_state(self, handoff_id: str, new_state: HandoffState) -> None:
        """Update handoff state in database."""
        await self.db.execute(
            text("""
                UPDATE order_service.pending_handoffs
                SET 
                    current_state = :new_state,
                    updated_at = NOW()
                WHERE handoff_id = :handoff_id::uuid
            """),
            {
                "handoff_id": handoff_id,
                "new_state": new_state.value
            }
        )
        await self.db.commit()

    async def _get_pending_retries(self, limit: int) -> List[Dict[str, Any]]:
        """Get pending handoff retries ready for processing."""
        result = await self.db.execute(
            text("""
                SELECT 
                    handoff_id,
                    source_execution_id,
                    target_execution_id,
                    handoff_reason,
                    requested_by,
                    coordination_data,
                    retry_state
                FROM order_service.pending_handoffs
                WHERE current_state = 'retrying'
                  AND (retry_state ->> 'next_retry_at')::timestamp <= NOW()
                  AND (retry_state ->> 'attempt_count')::int < (retry_state ->> 'max_attempts')::int
                ORDER BY priority ASC, created_at ASC
                LIMIT :limit
            """),
            {"limit": limit}
        )
        
        return [
            {
                "handoff_id": str(row[0]),
                "source_execution_id": str(row[1]),
                "target_execution_id": str(row[2]),
                "handoff_reason": row[3],
                "requested_by": row[4],
                "coordination_data": row[5],
                "retry_state": row[6]
            }
            for row in result.fetchall()
        ]

    async def _reconstruct_coordination_request(
        self,
        retry_info: Dict[str, Any]
    ) -> HandoffCoordinationRequest:
        """Reconstruct coordination request from retry info."""
        retry_state = retry_info["retry_state"]
        coordination_data = retry_info["coordination_data"]
        
        return HandoffCoordinationRequest(
            handoff_id=retry_info["handoff_id"],
            source_execution_id=retry_info["source_execution_id"],
            target_execution_id=retry_info["target_execution_id"],
            symbol_positions=coordination_data.get("symbol_positions", []),
            handoff_reason=retry_info["handoff_reason"],
            requested_by=retry_info["requested_by"],
            priority=5,  # Default priority for retries
            timeout_seconds=300,  # 5 minute timeout
            retry_strategy=HandoffRetryStrategy.EXPONENTIAL_BACKOFF
        )

    async def _retry_coordination(
        self,
        request: HandoffCoordinationRequest,
        retry_info: Dict[str, Any]
    ) -> HandoffCoordinationResult:
        """Retry coordination with updated retry state."""
        retry_state = retry_info["retry_state"]
        attempt_count = retry_state["attempt_count"] + 1
        
        # Try coordination again
        result = await self.coordinate_handoff_with_fallback(request)
        
        if not result.success and attempt_count < retry_state["max_attempts"]:
            # Update retry state for next attempt
            base_delay = retry_state["base_delay_seconds"]
            backoff_multiplier = retry_state["backoff_multiplier"]
            delay_seconds = base_delay * (backoff_multiplier ** (attempt_count - 2))
            
            # Add jitter if enabled
            if retry_state.get("jitter_enabled", True):
                import random
                jitter = random.uniform(0.5, 1.5)
                delay_seconds *= jitter
            
            next_retry = datetime.now(timezone.utc) + timedelta(seconds=delay_seconds)
            
            await self.db.execute(
                text("""
                    UPDATE order_service.pending_handoffs
                    SET 
                        retry_state = jsonb_set(
                            jsonb_set(
                                retry_state,
                                '{attempt_count}',
                                :attempt_count::text::jsonb
                            ),
                            '{next_retry_at}',
                            :next_retry_at::text::jsonb
                        ),
                        updated_at = NOW()
                    WHERE handoff_id = :handoff_id::uuid
                """),
                {
                    "handoff_id": request.handoff_id,
                    "attempt_count": attempt_count,
                    "next_retry_at": next_retry.isoformat()
                }
            )
            await self.db.commit()
        
        return result


# Helper function for external use
async def coordinate_handoff_with_redis_fallback(
    db: AsyncSession,
    handoff_request: HandoffCoordinationRequest,
    redis_client=None
) -> HandoffCoordinationResult:
    """
    Convenience function for handoff coordination with Redis fallback.

    Args:
        db: Database session
        handoff_request: Handoff coordination request
        redis_client: Optional Redis client

    Returns:
        Coordination result
    """
    manager = RedisUnavailableHandoffManager(db, redis_client)
    return await manager.coordinate_handoff_with_fallback(handoff_request)
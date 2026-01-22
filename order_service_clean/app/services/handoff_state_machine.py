"""
Handoff State Machine Service

Manages transitions between manual trading and script control with proper safeguards.
Ensures clean handoffs without position duplication or loss of control.

Key Features:
- Explicit handoff modes: MANUAL, SCRIPT, TRANSITIONING
- Safe transition logic with validation
- Position ownership tracking
- Rollback capabilities for failed transitions
- Comprehensive audit trail
"""

import logging
from typing import Optional, Dict, Any, List
from datetime import datetime, timezone, timedelta
from enum import Enum
from dataclasses import dataclass
from uuid import uuid4
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)


class HandoffMode(str, Enum):
    """Handoff modes for position control."""
    MANUAL = "manual"              # Manual trading - user has control
    SCRIPT = "script"              # Script trading - algorithm has control  
    TRANSITIONING = "transitioning" # Transition in progress - exclusive lock


class TransitionType(str, Enum):
    """Types of handoff transitions."""
    MANUAL_TO_SCRIPT = "manual_to_script"  # User hands off to script
    SCRIPT_TO_MANUAL = "script_to_manual"  # Script hands off to user
    EMERGENCY_STOP = "emergency_stop"       # Emergency takeover


class TransitionStatus(str, Enum):
    """Status of handoff transitions."""
    PENDING = "pending"           # Transition requested but not started
    IN_PROGRESS = "in_progress"   # Transition actively happening
    COMPLETED = "completed"       # Transition completed successfully
    FAILED = "failed"             # Transition failed
    ROLLED_BACK = "rolled_back"   # Failed transition was rolled back


@dataclass
class HandoffState:
    """Current handoff state for a trading account or strategy."""
    trading_account_id: str
    strategy_id: Optional[int]
    execution_id: Optional[str]
    current_mode: HandoffMode
    target_mode: Optional[HandoffMode]
    transition_id: Optional[str]
    transition_status: Optional[TransitionStatus]
    transition_started_at: Optional[datetime]
    last_updated_at: datetime
    controlled_by: Optional[str]  # User ID or "system"
    metadata: Dict[str, Any]


@dataclass
class TransitionRequest:
    """Request for handoff transition."""
    trading_account_id: str
    strategy_id: Optional[int]
    execution_id: Optional[str]
    from_mode: HandoffMode
    to_mode: HandoffMode
    transition_type: TransitionType
    requested_by: str
    reason: str
    force: bool = False  # Force transition even if risky


@dataclass
class TransitionResult:
    """Result of handoff transition."""
    transition_id: str
    success: bool
    new_state: HandoffState
    positions_transferred: int
    orders_cancelled: int
    warnings: List[str]
    errors: List[str]
    audit_trail: List[Dict[str, Any]]


class HandoffStateMachine:
    """
    Service for managing handoff state machine between manual and script control.

    Ensures safe transitions without position duplication or control conflicts.
    """

    def __init__(self, db: AsyncSession):
        """
        Initialize the handoff state machine.

        Args:
            db: Async database session
        """
        self.db = db
        self._redis_config = self._get_redis_config()
        
        # GAP-REC-12: Initialize Redis fallback manager for unavailability scenarios
        from .redis_unavailable_handoff_manager import RedisUnavailableHandoffManager
        self._fallback_manager = RedisUnavailableHandoffManager(db)
        
        # GAP-REC-13: Initialize concurrency manager for safe handoff transitions
        from .handoff_concurrency_manager import HandoffConcurrencyManager
        self._concurrency_manager = HandoffConcurrencyManager(db)

    def _get_redis_config(self) -> Dict[str, Any]:
        """Get Redis configuration from order service config settings."""
        try:
            # Use order service settings (already config-service compliant)
            from ..config.settings import settings
            redis_url = settings.redis_url
            
            # Parse Redis URL (format: redis://host:port/db)
            import re
            match = re.match(r'redis://([^:]+):(\d+)(?:/(\d+))?', redis_url)
            if match:
                host, port, db_num = match.groups()
                return {
                    "host": host,
                    "port": int(port),
                    "db": int(db_num) if db_num else 0,
                    "decode_responses": True
                }
            else:
                logger.error(f"Invalid Redis URL format: {redis_url}")
                
        except ImportError as e:
            logger.error(f"Could not import Redis config from order service settings: {e}")
        except (ConnectionError, TimeoutError, OSError) as e:
            logger.error(f"Could not get Redis config due to infrastructure error: {e}")
        except Exception as e:
            logger.error(f"Could not get Redis config due to unexpected error: {e}")

        # Fail gracefully if critical config is missing
        logger.warning(
            "Redis configuration missing: Could not parse REDIS_URL from config service. "
            "Algo engine coordination will be disabled, using database fallback."
        )
        return None

    async def get_handoff_state(
        self,
        trading_account_id: str,
        strategy_id: Optional[int] = None,
        execution_id: Optional[str] = None
    ) -> HandoffState:
        """
        Get current handoff state for a trading account/strategy/execution.

        Args:
            trading_account_id: Trading account ID
            strategy_id: Optional strategy ID for strategy-level control
            execution_id: Optional execution ID for execution-level control

        Returns:
            Current handoff state
        """
        # Build query based on granularity level
        where_clause = "trading_account_id = :trading_account_id"
        params = {"trading_account_id": trading_account_id}

        if execution_id:
            where_clause += " AND execution_id = :execution_id"
            params["execution_id"] = execution_id
        elif strategy_id:
            where_clause += " AND strategy_id = :strategy_id AND execution_id IS NULL"
            params["strategy_id"] = strategy_id
        else:
            where_clause += " AND strategy_id IS NULL AND execution_id IS NULL"

        result = await self.db.execute(
            text(f"""
                SELECT 
                    trading_account_id,
                    strategy_id,
                    execution_id,
                    current_mode,
                    target_mode,
                    transition_id,
                    transition_status,
                    transition_started_at,
                    last_updated_at,
                    controlled_by,
                    metadata
                FROM order_service.handoff_states
                WHERE {where_clause}
                LIMIT 1
            """),
            params
        )

        row = result.fetchone()
        
        if row:
            return HandoffState(
                trading_account_id=row[0],
                strategy_id=row[1],
                execution_id=row[2],
                current_mode=HandoffMode(row[3]),
                target_mode=HandoffMode(row[4]) if row[4] else None,
                transition_id=row[5],
                transition_status=TransitionStatus(row[6]) if row[6] else None,
                transition_started_at=row[7],
                last_updated_at=row[8],
                controlled_by=row[9],
                metadata=row[10] or {}
            )
        else:
            # Create default state (manual mode)
            default_state = HandoffState(
                trading_account_id=trading_account_id,
                strategy_id=strategy_id,
                execution_id=execution_id,
                current_mode=HandoffMode.MANUAL,
                target_mode=None,
                transition_id=None,
                transition_status=None,
                transition_started_at=None,
                last_updated_at=datetime.now(timezone.utc),
                controlled_by=None,
                metadata={}
            )

            await self._store_handoff_state(default_state)
            return default_state

    async def request_transition(
        self,
        request: TransitionRequest
    ) -> TransitionResult:
        """
        Request a handoff transition between manual and script control.

        Args:
            request: Transition request details

        Returns:
            Transition result with status and audit trail

        Raises:
            ValueError: If transition is invalid
            Exception: If transition fails
        """
        transition_id = str(uuid4())
        start_time = datetime.now(timezone.utc)
        
        logger.info(
            f"[{transition_id}] Requesting handoff transition: "
            f"{request.from_mode} -> {request.to_mode} for account {request.trading_account_id}"
        )

        try:
            # Step 1: Get current state and validate transition
            current_state = await self.get_handoff_state(
                request.trading_account_id,
                request.strategy_id,
                request.execution_id
            )

            # Validate transition is allowed
            validation_errors = await self._validate_transition(current_state, request)
            if validation_errors and not request.force:
                return TransitionResult(
                    transition_id=transition_id,
                    success=False,
                    new_state=current_state,
                    positions_transferred=0,
                    orders_cancelled=0,
                    warnings=[],
                    errors=validation_errors,
                    audit_trail=[]
                )

            # GAP-REC-13: Acquire concurrency locks before critical transition section
            from .handoff_concurrency_manager import execute_concurrent_safe_handoff
            
            symbol_positions = []
            if hasattr(current_state, 'positions') and current_state.positions:
                for pos in current_state.positions:
                    symbol_positions.append({
                        "symbol": pos.get("symbol", "UNKNOWN"),
                        "position_id": pos.get("position_id"),
                        "quantity": pos.get("quantity", 0),
                        "execution_id": current_state.execution_id
                    })
            
            # Execute concurrency-safe handoff
            concurrency_result = await execute_concurrent_safe_handoff(
                db=self.db,
                handoff_id=transition_id,
                source_execution_id=current_state.execution_id if request.transition_type == TransitionType.SCRIPT_TO_MANUAL else None,
                target_execution_id=current_state.execution_id if request.transition_type == TransitionType.MANUAL_TO_SCRIPT else request.execution_id,
                symbol_positions=symbol_positions,
                operation_type=f"handoff_{request.transition_type.value}",
                priority=getattr(request, 'priority', 50)
            )
            
            if not concurrency_result.success:
                conflicts = "; ".join([c.conflict_type for c in concurrency_result.conflicts_detected])
                raise Exception(f"Handoff concurrency conflict: {conflicts}")
            
            logger.info(f"[{transition_id}] Concurrency locks acquired, transaction: {concurrency_result.transaction_id}")

            # Step 2: Lock state for transition
            await self._lock_for_transition(current_state, transition_id, request)

            # Step 3: Perform transition steps
            if request.transition_type == TransitionType.MANUAL_TO_SCRIPT:
                result = await self._execute_manual_to_script(transition_id, current_state, request)
            elif request.transition_type == TransitionType.SCRIPT_TO_MANUAL:
                result = await self._execute_script_to_manual(transition_id, current_state, request)
            elif request.transition_type == TransitionType.EMERGENCY_STOP:
                result = await self._execute_emergency_stop(transition_id, current_state, request)
            else:
                raise ValueError(f"Unknown transition type: {request.transition_type}")

            # Step 4: Update final state
            if result.success:
                final_state = HandoffState(
                    trading_account_id=current_state.trading_account_id,
                    strategy_id=current_state.strategy_id,
                    execution_id=current_state.execution_id,
                    current_mode=request.to_mode,
                    target_mode=None,
                    transition_id=None,
                    transition_status=None,
                    transition_started_at=None,
                    last_updated_at=datetime.now(timezone.utc),
                    controlled_by=request.requested_by,
                    metadata={
                        **current_state.metadata,
                        "last_transition_id": transition_id,
                        "last_transition_at": start_time.isoformat()
                    }
                )
                await self._store_handoff_state(final_state)
                result.new_state = final_state
                
                await self._record_transition_audit(
                    transition_id, "transition_completed", request.requested_by,
                    {"success": True, "positions_transferred": result.positions_transferred}
                )
            else:
                # Rollback on failure
                await self._rollback_transition(transition_id, current_state, request)

            return result

        except (ConnectionError, TimeoutError, OSError) as e:
            logger.error(f"[{transition_id}] Transition failed due to network/database error: {e}", exc_info=True)
            
            # Attempt rollback for infrastructure failures
            try:
                await self._rollback_transition(transition_id, current_state, request)
            except Exception as rollback_error:
                logger.error(f"[{transition_id}] Rollback also failed: {rollback_error}")

            await self._record_transition_audit(
                transition_id, "transition_failed", request.requested_by,
                {"error": f"Infrastructure error: {e}"}
            )
            
            from ..exceptions import DatabaseError
            raise DatabaseError(f"Handoff transition failed due to infrastructure error: {e}")
            
        except ValueError as e:
            logger.error(f"[{transition_id}] Transition failed due to invalid parameters: {e}")
            
            await self._record_transition_audit(
                transition_id, "transition_failed", request.requested_by,
                {"error": f"Validation error: {e}"}
            )
            
            from ..exceptions import ValidationError
            raise ValidationError(f"Invalid handoff transition parameters: {e}")
            
        except Exception as e:
            logger.critical(f"[{transition_id}] CRITICAL: Unexpected handoff transition failure: {e}", exc_info=True)
            
            # Attempt rollback for unexpected errors
            try:
                await self._rollback_transition(transition_id, current_state, request)
            except Exception as rollback_error:
                logger.critical(f"[{transition_id}] CRITICAL: Rollback also failed during unexpected error: {rollback_error}")

            await self._record_transition_audit(
                transition_id, "transition_failed", request.requested_by,
                {"error": f"Unexpected error: {e}"}
            )
            
            from ..exceptions import OrderServiceError
            raise OrderServiceError(f"Critical handoff transition failure: {e}")

    async def _validate_transition(
        self,
        current_state: HandoffState,
        request: TransitionRequest
    ) -> List[str]:
        """
        Validate that a transition is allowed.

        Args:
            current_state: Current handoff state
            request: Transition request

        Returns:
            List of validation error messages (empty if valid)
        """
        errors = []

        # Check current mode matches request
        if current_state.current_mode != request.from_mode:
            errors.append(
                f"Current mode {current_state.current_mode} does not match "
                f"requested from_mode {request.from_mode}"
            )

        # Check not already in transition
        if current_state.current_mode == HandoffMode.TRANSITIONING:
            if current_state.transition_started_at:
                # Check if transition is stale (older than 5 minutes)
                stale_threshold = datetime.now(timezone.utc) - timedelta(minutes=5)
                if current_state.transition_started_at < stale_threshold:
                    errors.append("Previous transition appears stale - consider force option")
                else:
                    errors.append("Another transition is already in progress")

        # Validate transition type matches modes
        valid_transitions = {
            TransitionType.MANUAL_TO_SCRIPT: (HandoffMode.MANUAL, HandoffMode.SCRIPT),
            TransitionType.SCRIPT_TO_MANUAL: (HandoffMode.SCRIPT, HandoffMode.MANUAL),
            TransitionType.EMERGENCY_STOP: (None, HandoffMode.MANUAL)  # From any mode
        }

        if request.transition_type != TransitionType.EMERGENCY_STOP:
            expected_from, expected_to = valid_transitions[request.transition_type]
            if request.from_mode != expected_from or request.to_mode != expected_to:
                errors.append(
                    f"Transition type {request.transition_type} expects "
                    f"{expected_from} -> {expected_to}, got {request.from_mode} -> {request.to_mode}"
                )

        return errors

    async def _lock_for_transition(
        self,
        current_state: HandoffState,
        transition_id: str,
        request: TransitionRequest
    ) -> None:
        """
        Lock handoff state for transition.

        Args:
            current_state: Current state
            transition_id: Transition identifier
            request: Transition request
        """
        transition_state = HandoffState(
            trading_account_id=current_state.trading_account_id,
            strategy_id=current_state.strategy_id,
            execution_id=current_state.execution_id,
            current_mode=HandoffMode.TRANSITIONING,
            target_mode=request.to_mode,
            transition_id=transition_id,
            transition_status=TransitionStatus.IN_PROGRESS,
            transition_started_at=datetime.now(timezone.utc),
            last_updated_at=datetime.now(timezone.utc),
            controlled_by=current_state.controlled_by,
            metadata={
                **current_state.metadata,
                "previous_mode": current_state.current_mode.value,
                "transition_type": request.transition_type.value,
                "requested_by": request.requested_by,
                "reason": request.reason
            }
        )

        await self._store_handoff_state(transition_state)
        await self._record_transition_audit(
            transition_id, "transition_started", request.requested_by,
            {
                "from_mode": request.from_mode.value,
                "to_mode": request.to_mode.value,
                "reason": request.reason
            }
        )

    async def _execute_manual_to_script(
        self,
        transition_id: str,
        current_state: HandoffState,
        request: TransitionRequest
    ) -> TransitionResult:
        """
        Execute manual to script transition.

        Args:
            transition_id: Transition identifier
            current_state: Current state
            request: Transition request

        Returns:
            Transition result
        """
        warnings = []
        errors = []
        positions_transferred = 0
        orders_cancelled = 0

        try:
            # Step 1: Get all open positions that need script control
            positions = await self._get_positions_for_handoff(
                request.trading_account_id,
                request.strategy_id,
                request.execution_id
            )

            # Step 2: Cancel any pending manual orders
            cancelled_orders = await self._cancel_manual_orders(
                request.trading_account_id,
                request.strategy_id,
                request.execution_id
            )
            orders_cancelled = len(cancelled_orders)

            if cancelled_orders:
                warnings.append(f"Cancelled {orders_cancelled} pending manual orders")

            # Step 3: Mark positions for script control
            for position in positions:
                await self._mark_position_for_script_control(position, request.execution_id)
                positions_transferred += 1

            # Step 4: Initialize script state
            await self._initialize_script_state(request.execution_id, positions)

            return TransitionResult(
                transition_id=transition_id,
                success=True,
                new_state=current_state,  # Will be updated by caller
                positions_transferred=positions_transferred,
                orders_cancelled=orders_cancelled,
                warnings=warnings,
                errors=errors,
                audit_trail=[]
            )

        except (ConnectionError, TimeoutError, OSError) as e:
            logger.error(f"Manual to script transition failed due to database/network error: {e}")
            errors.append(f"Manual to script transition failed due to infrastructure error: {str(e)}")
            return TransitionResult(
                transition_id=transition_id,
                success=False,
                new_state=current_state,
                positions_transferred=positions_transferred,
                orders_cancelled=orders_cancelled,
                warnings=warnings,
                errors=errors,
                audit_trail=[]
            )
            
        except ValueError as e:
            logger.error(f"Manual to script transition failed due to validation error: {e}")
            errors.append(f"Manual to script transition failed due to invalid parameters: {str(e)}")
            return TransitionResult(
                transition_id=transition_id,
                success=False,
                new_state=current_state,
                positions_transferred=positions_transferred,
                orders_cancelled=orders_cancelled,
                warnings=warnings,
                errors=errors,
                audit_trail=[]
            )
            
        except Exception as e:
            logger.critical(f"CRITICAL: Unexpected manual to script transition failure: {e}", exc_info=True)
            errors.append(f"Manual to script transition failed due to unexpected error: {str(e)}")
            return TransitionResult(
                transition_id=transition_id,
                success=False,
                new_state=current_state,
                positions_transferred=positions_transferred,
                orders_cancelled=orders_cancelled,
                warnings=warnings,
                errors=errors,
                audit_trail=[]
            )

    async def _execute_script_to_manual(
        self,
        transition_id: str,
        current_state: HandoffState,
        request: TransitionRequest
    ) -> TransitionResult:
        """
        Execute script to manual transition.

        Args:
            transition_id: Transition identifier
            current_state: Current state
            request: Transition request

        Returns:
            Transition result
        """
        warnings = []
        errors = []
        positions_transferred = 0
        orders_cancelled = 0

        try:
            # Step 1: Stop script execution
            await self._stop_script_execution(request.execution_id)

            # Step 2: Cancel any pending script orders
            cancelled_orders = await self._cancel_script_orders(
                request.trading_account_id,
                request.strategy_id,
                request.execution_id
            )
            orders_cancelled = len(cancelled_orders)

            if cancelled_orders:
                warnings.append(f"Cancelled {orders_cancelled} pending script orders")

            # Step 3: Get positions to transfer to manual control
            positions = await self._get_positions_for_handoff(
                request.trading_account_id,
                request.strategy_id,
                request.execution_id
            )

            # Step 4: Mark positions for manual control
            for position in positions:
                await self._mark_position_for_manual_control(position)
                positions_transferred += 1

            return TransitionResult(
                transition_id=transition_id,
                success=True,
                new_state=current_state,  # Will be updated by caller
                positions_transferred=positions_transferred,
                orders_cancelled=orders_cancelled,
                warnings=warnings,
                errors=errors,
                audit_trail=[]
            )

        except (ConnectionError, TimeoutError, OSError) as e:
            logger.error(f"Script to manual transition failed due to database/network error: {e}")
            errors.append(f"Script to manual transition failed due to infrastructure error: {str(e)}")
            return TransitionResult(
                transition_id=transition_id,
                success=False,
                new_state=current_state,
                positions_transferred=positions_transferred,
                orders_cancelled=orders_cancelled,
                warnings=warnings,
                errors=errors,
                audit_trail=[]
            )
            
        except Exception as e:
            logger.critical(f"CRITICAL: Unexpected script to manual transition failure: {e}", exc_info=True)
            errors.append(f"Script to manual transition failed due to unexpected error: {str(e)}")
            return TransitionResult(
                transition_id=transition_id,
                success=False,
                new_state=current_state,
                positions_transferred=positions_transferred,
                orders_cancelled=orders_cancelled,
                warnings=warnings,
                errors=errors,
                audit_trail=[]
            )

    async def _execute_emergency_stop(
        self,
        transition_id: str,
        current_state: HandoffState,
        request: TransitionRequest
    ) -> TransitionResult:
        """
        Execute emergency stop transition.

        Args:
            transition_id: Transition identifier
            current_state: Current state
            request: Transition request

        Returns:
            Transition result
        """
        warnings = []
        errors = []
        positions_transferred = 0
        orders_cancelled = 0

        try:
            # Emergency stop: aggressively take control
            
            # Step 1: Stop all script execution immediately
            if request.execution_id:
                await self._emergency_stop_script(request.execution_id)

            # Step 2: Cancel ALL orders (script and manual)
            all_cancelled = await self._cancel_all_orders(
                request.trading_account_id,
                request.strategy_id
            )
            orders_cancelled = len(all_cancelled)

            # Step 3: Take manual control of all positions
            positions = await self._get_positions_for_handoff(
                request.trading_account_id,
                request.strategy_id,
                None  # All positions regardless of execution
            )

            for position in positions:
                await self._mark_position_for_manual_control(position)
                positions_transferred += 1

            warnings.append(f"Emergency stop: Took control of {positions_transferred} positions")
            if orders_cancelled > 0:
                warnings.append(f"Emergency stop: Cancelled {orders_cancelled} orders")

            return TransitionResult(
                transition_id=transition_id,
                success=True,
                new_state=current_state,  # Will be updated by caller
                positions_transferred=positions_transferred,
                orders_cancelled=orders_cancelled,
                warnings=warnings,
                errors=errors,
                audit_trail=[]
            )

        except (ConnectionError, TimeoutError, OSError) as e:
            logger.error(f"Emergency stop failed due to database/network error: {e}")
            errors.append(f"Emergency stop failed due to infrastructure error: {str(e)}")
            return TransitionResult(
                transition_id=transition_id,
                success=False,
                new_state=current_state,
                positions_transferred=0,
                orders_cancelled=0,
                warnings=warnings,
                errors=errors,
                audit_trail=[]
            )
            
        except Exception as e:
            logger.critical(f"CRITICAL: Emergency stop failed with unexpected error: {e}", exc_info=True)
            errors.append(f"Emergency stop failed due to unexpected error: {str(e)}")
            return TransitionResult(
                transition_id=transition_id,
                success=False,
                new_state=current_state,
                positions_transferred=0,
                orders_cancelled=0,
                warnings=warnings,
                errors=errors,
                audit_trail=[]
            )

    async def _store_handoff_state(self, state: HandoffState) -> None:
        """Store handoff state in database."""
        await self.db.execute(
            text("""
                INSERT INTO order_service.handoff_states (
                    trading_account_id,
                    strategy_id,
                    execution_id,
                    current_mode,
                    target_mode,
                    transition_id,
                    transition_status,
                    transition_started_at,
                    last_updated_at,
                    controlled_by,
                    metadata
                ) VALUES (
                    :trading_account_id,
                    :strategy_id,
                    :execution_id,
                    :current_mode,
                    :target_mode,
                    :transition_id,
                    :transition_status,
                    :transition_started_at,
                    :last_updated_at,
                    :controlled_by,
                    :metadata::jsonb
                )
                ON CONFLICT (trading_account_id, COALESCE(strategy_id, 0), COALESCE(execution_id, ''))
                DO UPDATE SET
                    current_mode = :current_mode,
                    target_mode = :target_mode,
                    transition_id = :transition_id,
                    transition_status = :transition_status,
                    transition_started_at = :transition_started_at,
                    last_updated_at = :last_updated_at,
                    controlled_by = :controlled_by,
                    metadata = :metadata::jsonb
            """),
            {
                "trading_account_id": state.trading_account_id,
                "strategy_id": state.strategy_id,
                "execution_id": state.execution_id,
                "current_mode": state.current_mode.value,
                "target_mode": state.target_mode.value if state.target_mode else None,
                "transition_id": state.transition_id,
                "transition_status": state.transition_status.value if state.transition_status else None,
                "transition_started_at": state.transition_started_at,
                "last_updated_at": state.last_updated_at,
                "controlled_by": state.controlled_by,
                "metadata": state.metadata
            }
        )
        await self.db.commit()

    async def _record_transition_audit(
        self,
        transition_id: str,
        event_type: str,
        user_id: str,
        event_data: Dict[str, Any]
    ) -> None:
        """Record transition audit event."""
        await self.db.execute(
            text("""
                INSERT INTO order_service.handoff_transition_audit (
                    transition_id,
                    event_type,
                    user_id,
                    event_data,
                    created_at
                ) VALUES (
                    :transition_id,
                    :event_type,
                    :user_id,
                    :event_data::jsonb,
                    :created_at
                )
            """),
            {
                "transition_id": transition_id,
                "event_type": event_type,
                "user_id": user_id,
                "event_data": event_data,
                "created_at": datetime.now(timezone.utc)
            }
        )

    # Position and order operations for handoff transitions

    async def _get_positions_for_handoff(
        self,
        trading_account_id: str,
        strategy_id: Optional[int],
        execution_id: Optional[str]
    ) -> List[Dict[str, Any]]:
        """Get positions for handoff transition."""
        where_clauses = ["trading_account_id = :trading_account_id", "is_open = true"]
        params = {"trading_account_id": trading_account_id}

        if execution_id:
            where_clauses.append("execution_id = :execution_id")
            params["execution_id"] = execution_id
        elif strategy_id:
            where_clauses.append("strategy_id = :strategy_id")
            params["strategy_id"] = strategy_id

        where_clause = " AND ".join(where_clauses)

        result = await self.db.execute(
            text(f"""
                SELECT 
                    id,
                    symbol,
                    quantity,
                    strategy_id,
                    execution_id,
                    portfolio_id,
                    source,
                    created_at
                FROM order_service.positions
                WHERE {where_clause}
                ORDER BY created_at
            """),
            params
        )

        positions = []
        for row in result.fetchall():
            positions.append({
                "id": row[0],
                "symbol": row[1],
                "quantity": row[2],
                "strategy_id": row[3],
                "execution_id": row[4],
                "portfolio_id": row[5],
                "source": row[6],
                "created_at": row[7]
            })

        logger.debug(f"Retrieved {len(positions)} positions for handoff transition")
        return positions

    async def _cancel_manual_orders(
        self,
        trading_account_id: str,
        strategy_id: Optional[int],
        execution_id: Optional[str]
    ) -> List[Dict[str, Any]]:
        """Cancel pending manual orders."""
        where_clauses = [
            "trading_account_id = :trading_account_id",
            "source IN ('manual', 'external')",
            "status IN ('PENDING', 'SUBMITTED', 'OPEN', 'TRIGGER_PENDING')"
        ]
        params = {"trading_account_id": trading_account_id}

        if execution_id:
            where_clauses.append("execution_id = :execution_id")
            params["execution_id"] = execution_id
        elif strategy_id:
            where_clauses.append("strategy_id = :strategy_id")
            params["strategy_id"] = strategy_id

        where_clause = " AND ".join(where_clauses)

        # First, get the orders to cancel
        result = await self.db.execute(
            text(f"""
                SELECT id, order_id, symbol, quantity, status
                FROM order_service.orders
                WHERE {where_clause}
            """),
            params
        )

        orders_to_cancel = []
        for row in result.fetchall():
            orders_to_cancel.append({
                "id": row[0],
                "order_id": row[1],
                "symbol": row[2],
                "quantity": row[3],
                "status": row[4]
            })

        # Cancel the orders by updating their status
        if orders_to_cancel:
            order_ids = [order["id"] for order in orders_to_cancel]
            await self.db.execute(
                text("""
                    UPDATE order_service.orders
                    SET status = 'CANCELLED',
                        updated_at = NOW(),
                        cancelled_reason = 'Handoff transition - manual orders cancelled'
                    WHERE id = ANY(:order_ids)
                """),
                {"order_ids": order_ids}
            )

        logger.info(f"Cancelled {len(orders_to_cancel)} manual orders for handoff")
        return orders_to_cancel

    async def _cancel_script_orders(
        self,
        trading_account_id: str,
        strategy_id: Optional[int],
        execution_id: Optional[str]
    ) -> List[Dict[str, Any]]:
        """Cancel pending script orders."""
        where_clauses = [
            "trading_account_id = :trading_account_id",
            "source = 'script'",
            "status IN ('PENDING', 'SUBMITTED', 'OPEN', 'TRIGGER_PENDING')"
        ]
        params = {"trading_account_id": trading_account_id}

        if execution_id:
            where_clauses.append("execution_id = :execution_id")
            params["execution_id"] = execution_id
        elif strategy_id:
            where_clauses.append("strategy_id = :strategy_id")
            params["strategy_id"] = strategy_id

        where_clause = " AND ".join(where_clauses)

        # Get orders to cancel
        result = await self.db.execute(
            text(f"""
                SELECT id, order_id, symbol, quantity, status
                FROM order_service.orders
                WHERE {where_clause}
            """),
            params
        )

        orders_to_cancel = []
        for row in result.fetchall():
            orders_to_cancel.append({
                "id": row[0],
                "order_id": row[1],
                "symbol": row[2],
                "quantity": row[3],
                "status": row[4]
            })

        # Cancel the orders
        if orders_to_cancel:
            order_ids = [order["id"] for order in orders_to_cancel]
            await self.db.execute(
                text("""
                    UPDATE order_service.orders
                    SET status = 'CANCELLED',
                        updated_at = NOW(),
                        cancelled_reason = 'Handoff transition - script orders cancelled'
                    WHERE id = ANY(:order_ids)
                """),
                {"order_ids": order_ids}
            )

        logger.info(f"Cancelled {len(orders_to_cancel)} script orders for handoff")
        return orders_to_cancel

    async def _cancel_all_orders(
        self,
        trading_account_id: str,
        strategy_id: Optional[int]
    ) -> List[Dict[str, Any]]:
        """Cancel all orders for emergency stop."""
        where_clauses = [
            "trading_account_id = :trading_account_id",
            "status IN ('PENDING', 'SUBMITTED', 'OPEN', 'TRIGGER_PENDING')"
        ]
        params = {"trading_account_id": trading_account_id}

        if strategy_id:
            where_clauses.append("strategy_id = :strategy_id")
            params["strategy_id"] = strategy_id

        where_clause = " AND ".join(where_clauses)

        # Get all orders to cancel
        result = await self.db.execute(
            text(f"""
                SELECT id, order_id, symbol, quantity, status, source
                FROM order_service.orders
                WHERE {where_clause}
            """),
            params
        )

        orders_to_cancel = []
        for row in result.fetchall():
            orders_to_cancel.append({
                "id": row[0],
                "order_id": row[1],
                "symbol": row[2],
                "quantity": row[3],
                "status": row[4],
                "source": row[5]
            })

        # Emergency cancel all orders
        if orders_to_cancel:
            order_ids = [order["id"] for order in orders_to_cancel]
            await self.db.execute(
                text("""
                    UPDATE order_service.orders
                    SET status = 'CANCELLED',
                        updated_at = NOW(),
                        cancelled_reason = 'Emergency stop - all orders cancelled'
                    WHERE id = ANY(:order_ids)
                """),
                {"order_ids": order_ids}
            )

        logger.warning(f"Emergency cancelled {len(orders_to_cancel)} orders for handoff")
        return orders_to_cancel

    async def _mark_position_for_script_control(
        self,
        position: Dict[str, Any],
        execution_id: Optional[str]
    ) -> None:
        """Mark position for script control."""
        await self.db.execute(
            text("""
                UPDATE order_service.positions
                SET execution_id = :execution_id::uuid,
                    source = 'script',
                    updated_at = NOW(),
                    metadata = COALESCE(metadata, '{}'::jsonb) || '{"handoff_source": "manual_to_script"}'::jsonb
                WHERE id = :position_id
            """),
            {
                "execution_id": execution_id,
                "position_id": position["id"]
            }
        )
        logger.debug(f"Marked position {position['id']} for script control (execution {execution_id})")

    async def _mark_position_for_manual_control(
        self,
        position: Dict[str, Any]
    ) -> None:
        """Mark position for manual control."""
        await self.db.execute(
            text("""
                UPDATE order_service.positions
                SET source = 'manual',
                    updated_at = NOW(),
                    metadata = COALESCE(metadata, '{}'::jsonb) || '{"handoff_source": "script_to_manual"}'::jsonb
                WHERE id = :position_id
            """),
            {"position_id": position["id"]}
        )
        logger.debug(f"Marked position {position['id']} for manual control")

    async def _initialize_script_state(
        self,
        execution_id: Optional[str],
        positions: List[Dict[str, Any]]
    ) -> None:
        """Initialize script state after handoff."""
        if not execution_id:
            logger.warning("Cannot initialize script state without execution_id")
            return

        logger.info(f"Initializing script state for execution {execution_id} with {len(positions)} positions")
        
        # Prepare handoff context data
        handoff_data = {
            "execution_id": execution_id,
            "handoff_timestamp": datetime.now(timezone.utc).isoformat(),
            "positions_count": len(positions),
            "position_ids": [pos["id"] for pos in positions],
            "symbols": list(set(pos["symbol"] for pos in positions)),
            "total_value": sum(float(pos.get("quantity", 0)) * 100 for pos in positions),  # Approximate value
            "handoff_type": "manual_to_script"
        }
        
        try:
            # Step 1: Store execution context in database for algo engine pickup
            await self._store_execution_context(execution_id, handoff_data, positions)
            
            # Step 2: Signal algo engine via Redis queue (real implementation)
            await self._signal_algo_engine_start(execution_id, handoff_data)
            
            # Step 3: Wait for algo engine acknowledgment with timeout
            acknowledgment = await self._wait_for_algo_engine_ack(execution_id, timeout_seconds=30)
            
            if acknowledgment["status"] == "ready":
                logger.info(f"Algo engine successfully initialized for execution {execution_id}")
                
                # Step 4: Set up position monitoring
                await self._setup_position_monitoring(execution_id, positions)
                
            else:
                logger.error(f"Algo engine failed to initialize for execution {execution_id}: {acknowledgment}")
                raise Exception(f"Algo engine initialization failed: {acknowledgment.get('error', 'Unknown error')}")
                
        except (ConnectionError, TimeoutError, OSError) as e:
            logger.error(f"Failed to initialize script state due to infrastructure error for execution {execution_id}: {e}")
            # Rollback positions to manual control on failure
            await self._rollback_positions_to_manual(positions)
            from ..exceptions import ServiceUnavailableError
            raise ServiceUnavailableError(f"Script initialization failed due to infrastructure error: {e}")
            
        except ValueError as e:
            logger.error(f"Failed to initialize script state due to invalid parameters for execution {execution_id}: {e}")
            await self._rollback_positions_to_manual(positions)
            from ..exceptions import ValidationError
            raise ValidationError(f"Script initialization failed due to invalid parameters: {e}")
            
        except Exception as e:
            logger.critical(f"CRITICAL: Script state initialization failed unexpectedly for execution {execution_id}: {e}", exc_info=True)
            # Rollback positions to manual control on failure
            await self._rollback_positions_to_manual(positions)
            from ..exceptions import OrderServiceError
            raise OrderServiceError(f"Critical script initialization failure: {e}")

    async def _stop_script_execution(self, execution_id: Optional[str]) -> None:
        """Stop script execution gracefully."""
        if not execution_id:
            logger.warning("Cannot stop script without execution_id")
            return

        logger.info(f"Requesting graceful stop for script execution {execution_id}")
        
        try:
            # Step 1: Signal algo engine to stop execution
            stop_signal = {
                "execution_id": execution_id,
                "action": "graceful_stop",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "reason": "handoff_transition",
                "timeout_seconds": 60
            }
            
            await self._signal_algo_engine_stop(execution_id, stop_signal)
            
            # Step 2: Wait for graceful shutdown confirmation
            shutdown_ack = await self._wait_for_shutdown_confirmation(execution_id, timeout_seconds=90)
            
            if shutdown_ack["status"] == "stopped":
                logger.info(f"Algo engine gracefully stopped execution {execution_id}")
                
                # Step 3: Cancel any remaining pending orders from this execution
                cancelled_orders = await self._cancel_execution_orders(execution_id, "graceful_handoff")
                logger.info(f"Cancelled {len(cancelled_orders)} remaining orders for execution {execution_id}")
                
                # Step 4: Mark execution as stopped in database
                await self._mark_execution_stopped(execution_id, "graceful_handoff")
                
            else:
                logger.warning(f"Graceful stop failed for execution {execution_id}: {shutdown_ack}")
                # Fallback to force stop if graceful fails
                await self._emergency_stop_script(execution_id)
                
        except (ConnectionError, TimeoutError, OSError) as e:
            logger.error(f"Failed to gracefully stop execution {execution_id} due to infrastructure error: {e}")
            # Attempt emergency stop as fallback
            await self._emergency_stop_script(execution_id)
            
        except Exception as e:
            logger.critical(f"CRITICAL: Graceful stop failed unexpectedly for execution {execution_id}: {e}", exc_info=True)
            # Attempt emergency stop as fallback
            await self._emergency_stop_script(execution_id)

    async def _emergency_stop_script(self, execution_id: Optional[str]) -> None:
        """Emergency stop script execution."""
        if not execution_id:
            logger.warning("Cannot emergency stop script without execution_id")
            return

        logger.warning(f"Emergency stop requested for script execution {execution_id}")

        try:
            # Step 1: Force stop algo engine execution immediately (no wait)
            emergency_signal = {
                "execution_id": execution_id,
                "action": "emergency_stop", 
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "reason": "emergency_handoff",
                "force": True
            }
            
            # Fire-and-forget emergency signal (don't wait for response)
            await self._signal_algo_engine_emergency_stop(execution_id, emergency_signal)
            
            # Step 2: Cancel ALL orders from this execution immediately
            cancelled_orders = await self._cancel_execution_orders(execution_id, "emergency_stop")
            logger.warning(f"Emergency cancelled {len(cancelled_orders)} orders for execution {execution_id}")
            
            # Step 3: Mark execution as emergency stopped
            await self._mark_execution_stopped(execution_id, "emergency_stop")
            
            # Step 4: Alert administrators via notification system
            await self._send_emergency_alert(execution_id, {
                "alert_type": "emergency_stop",
                "execution_id": execution_id,
                "orders_cancelled": len(cancelled_orders),
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "severity": "critical"
            })
            
            logger.warning(f"Emergency stop completed for execution {execution_id}")
            
        except (ConnectionError, TimeoutError, OSError) as e:
            logger.critical(f"Emergency stop failed for execution {execution_id} due to infrastructure error: {e}")
            # Even if emergency stop fails, try to cancel orders at database level
            try:
                await self.db.execute(
                    text("""
                        UPDATE order_service.orders
                        SET status = 'CANCELLED',
                            updated_at = NOW(),
                            cancelled_reason = 'Emergency stop - script execution halted (infrastructure failure)'
                        WHERE execution_id = :execution_id::uuid
                          AND status IN ('PENDING', 'SUBMITTED', 'OPEN', 'TRIGGER_PENDING')
                    """),
                    {"execution_id": execution_id}
                )
                await self.db.commit()
            except Exception as db_error:
                logger.critical(f"CRITICAL: Failed to cancel orders at database level for execution {execution_id}: {db_error}")
            
            from ..exceptions import ServiceUnavailableError
            raise ServiceUnavailableError(f"Emergency stop failed due to infrastructure error: {e}")
            
        except Exception as e:
            logger.critical(f"CRITICAL: Emergency stop failed unexpectedly for execution {execution_id}: {e}", exc_info=True)
            # Even if emergency stop fails, cancel orders at database level
            try:
                await self.db.execute(
                    text("""
                        UPDATE order_service.orders
                        SET status = 'CANCELLED',
                            updated_at = NOW(),
                            cancelled_reason = 'Emergency stop - script execution halted (unexpected error)'
                        WHERE execution_id = :execution_id::uuid
                          AND status IN ('PENDING', 'SUBMITTED', 'OPEN', 'TRIGGER_PENDING')
                    """),
                    {"execution_id": execution_id}
                )
                await self.db.commit()
            except Exception as db_error:
                logger.critical(f"CRITICAL: Failed to cancel orders at database level for execution {execution_id}: {db_error}")
            
            from ..exceptions import OrderServiceError
            raise OrderServiceError(f"Critical emergency stop failure: {e}")

    # Algo Engine Integration Methods

    async def _store_execution_context(
        self,
        execution_id: str,
        handoff_data: Dict[str, Any],
        positions: List[Dict[str, Any]]
    ) -> None:
        """Store execution context in database for algo engine pickup."""
        await self.db.execute(
            text("""
                INSERT INTO order_service.execution_contexts (
                    execution_id,
                    context_type,
                    handoff_data,
                    position_data,
                    status,
                    created_at,
                    updated_at
                ) VALUES (
                    :execution_id::uuid,
                    :context_type,
                    :handoff_data::jsonb,
                    :position_data::jsonb,
                    :status,
                    :created_at,
                    :updated_at
                )
                ON CONFLICT (execution_id) DO UPDATE SET
                    handoff_data = :handoff_data::jsonb,
                    position_data = :position_data::jsonb,
                    status = :status,
                    updated_at = :updated_at
            """),
            {
                "execution_id": execution_id,
                "context_type": "handoff_initialization",
                "handoff_data": handoff_data,
                "position_data": positions,
                "status": "initializing",
                "created_at": datetime.now(timezone.utc),
                "updated_at": datetime.now(timezone.utc)
            }
        )
        await self.db.commit()

    async def _signal_algo_engine_start(
        self,
        execution_id: str,
        handoff_data: Dict[str, Any]
    ) -> None:
        """Signal algo engine to start execution via Redis queue."""
        if not self._redis_config:
            logger.warning(f"Redis not configured - cannot signal algo engine start for execution {execution_id}")
            return
            
        try:
            import redis
            import json
            
            # Try Redis first
            try:
                # Connect to Redis using configuration
                redis_client = redis.Redis(**self._redis_config)
                
                # Publish to algo engine control queue
                message = {
                    "action": "start_execution",
                    "execution_id": execution_id,
                    "handoff_data": handoff_data,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "source": "handoff_state_machine"
                }
                
                # Test Redis connectivity
                redis_client.ping()
                
                # Publish to algo engine queue
                redis_client.lpush(f"algo_engine:control:{execution_id}", json.dumps(message))
                
                # Also publish to general algo engine queue for pickup
                redis_client.publish("algo_engine:handoff", json.dumps(message))
                
                logger.info(f"Signaled algo engine start for execution {execution_id}")
                
            except (ConnectionError, TimeoutError, OSError) as redis_error:
                # GAP-REC-12: Fallback to database-coordinated handoff when Redis is unavailable
                logger.warning(f"Redis connection failed ({redis_error}), using fallback coordination for execution {execution_id}")
                
            except Exception as redis_error:
                # GAP-REC-12: Fallback to database-coordinated handoff when Redis is unavailable  
                logger.warning(f"Redis unavailable due to unexpected error ({redis_error}), using fallback coordination for execution {execution_id}")
                
                from .redis_unavailable_handoff_manager import HandoffCoordinationRequest
                coordination_request = HandoffCoordinationRequest(
                    handoff_id=str(uuid4()),
                    source_execution_id=None,  # Manual to script transition
                    target_execution_id=execution_id,
                    handoff_reason=f"Manual to script handoff for execution {execution_id}",
                    requested_by="handoff_state_machine",
                    priority=50,
                    timeout_seconds=300,
                    retry_strategy="exponential_backoff",
                    coordination_data=handoff_data,
                    retry_state={}
                )
                
                result = await self._fallback_manager.coordinate_handoff_with_fallback(coordination_request)
                
                if result.success:
                    logger.info(f"Successfully coordinated handoff for execution {execution_id} via database fallback")
                else:
                    raise Exception(f"Handoff coordination failed: {result.error_message}")
                
        except (ConnectionError, TimeoutError, OSError) as e:
            logger.error(f"Failed to signal algo engine start due to infrastructure error: {e}")
            from ..exceptions import ServiceUnavailableError
            raise ServiceUnavailableError(f"Unable to signal algo engine start: {e}")
            
        except Exception as e:
            logger.critical(f"CRITICAL: Failed to signal algo engine start due to unexpected error: {e}", exc_info=True)
            from ..exceptions import OrderServiceError
            raise OrderServiceError(f"Critical algo engine signaling failure: {e}")

    async def _wait_for_algo_engine_ack(
        self,
        execution_id: str,
        timeout_seconds: int = 30
    ) -> Dict[str, Any]:
        """Wait for algo engine acknowledgment with timeout."""
        import asyncio
        import time
        
        start_time = time.time()
        while time.time() - start_time < timeout_seconds:
            # Check database for acknowledgment
            result = await self.db.execute(
                text("""
                    SELECT status, ack_data
                    FROM order_service.execution_contexts
                    WHERE execution_id = :execution_id::uuid
                      AND status IN ('ready', 'failed', 'error')
                """),
                {"execution_id": execution_id}
            )
            
            row = result.fetchone()
            if row:
                status, ack_data = row
                return {
                    "status": status,
                    "data": ack_data or {},
                    "execution_id": execution_id
                }
            
            # Wait before next check
            await asyncio.sleep(1)
        
        # Timeout - return failure
        return {
            "status": "timeout",
            "error": f"Algo engine did not acknowledge within {timeout_seconds} seconds",
            "execution_id": execution_id
        }

    async def _setup_position_monitoring(
        self,
        execution_id: str,
        positions: List[Dict[str, Any]]
    ) -> None:
        """Set up position monitoring for the execution."""
        if not self._redis_config:
            logger.warning(f"Redis not configured - cannot setup position monitoring for execution {execution_id}")
            return
            
        try:
            import redis
            import json
            
            redis_client = redis.Redis(**self._redis_config)
            
            # Set up monitoring configuration
            monitoring_config = {
                "execution_id": execution_id,
                "position_ids": [pos["id"] for pos in positions],
                "symbols": list(set(pos["symbol"] for pos in positions)),
                "monitoring_enabled": True,
                "alerts_enabled": True,
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
            
            # Store monitoring config in Redis
            redis_client.hset(
                f"monitoring:execution:{execution_id}",
                mapping={
                    "config": json.dumps(monitoring_config),
                    "status": "active",
                    "last_updated": datetime.now(timezone.utc).isoformat()
                }
            )
            
            # Set expiration for monitoring config (24 hours)
            redis_client.expire(f"monitoring:execution:{execution_id}", 86400)
            
            logger.info(f"Set up position monitoring for execution {execution_id}")
            
        except (ConnectionError, TimeoutError, OSError) as e:
            logger.error(f"Failed to setup position monitoring due to infrastructure error: {e}")
            # Don't raise - monitoring setup failure shouldn't stop handoff
            
        except Exception as e:
            logger.error(f"Failed to setup position monitoring due to unexpected error: {e}")
            # Don't raise - monitoring setup failure shouldn't stop handoff

    async def _signal_algo_engine_stop(
        self,
        execution_id: str,
        stop_signal: Dict[str, Any]
    ) -> None:
        """Signal algo engine to stop execution gracefully."""
        if not self._redis_config:
            logger.warning(f"Redis not configured - cannot signal graceful stop for execution {execution_id}")
            return
            
        try:
            import redis
            import json
            
            redis_client = redis.Redis(**self._redis_config)
            
            # Send stop signal
            message = {
                **stop_signal,
                "source": "handoff_state_machine",
                "priority": "high"
            }
            
            # Send to execution-specific queue (highest priority)
            redis_client.lpush(f"algo_engine:control:{execution_id}", json.dumps(message))
            
            # Also broadcast to general queue
            redis_client.publish("algo_engine:stop", json.dumps(message))
            
            logger.info(f"Sent graceful stop signal for execution {execution_id}")
            
        except (ConnectionError, TimeoutError, OSError) as e:
            logger.error(f"Failed to signal algo engine stop due to infrastructure error: {e}")
            from ..exceptions import ServiceUnavailableError
            raise ServiceUnavailableError(f"Unable to signal algo engine stop: {e}")
            
        except Exception as e:
            logger.critical(f"CRITICAL: Failed to signal algo engine stop due to unexpected error: {e}", exc_info=True)
            from ..exceptions import OrderServiceError
            raise OrderServiceError(f"Critical algo engine stop signaling failure: {e}")

    async def _wait_for_shutdown_confirmation(
        self,
        execution_id: str,
        timeout_seconds: int = 90
    ) -> Dict[str, Any]:
        """Wait for algo engine shutdown confirmation."""
        import asyncio
        import time
        
        start_time = time.time()
        while time.time() - start_time < timeout_seconds:
            # Check execution context status
            result = await self.db.execute(
                text("""
                    SELECT status, ack_data
                    FROM order_service.execution_contexts
                    WHERE execution_id = :execution_id::uuid
                """),
                {"execution_id": execution_id}
            )
            
            row = result.fetchone()
            if row:
                status, ack_data = row
                if status in ["stopped", "failed", "error"]:
                    return {
                        "status": status,
                        "data": ack_data or {},
                        "execution_id": execution_id
                    }
            
            await asyncio.sleep(2)  # Check every 2 seconds for shutdown
        
        return {
            "status": "timeout",
            "error": f"Algo engine did not confirm shutdown within {timeout_seconds} seconds",
            "execution_id": execution_id
        }

    async def _signal_algo_engine_emergency_stop(
        self,
        execution_id: str,
        emergency_signal: Dict[str, Any]
    ) -> None:
        """Signal algo engine emergency stop (fire-and-forget)."""
        if not self._redis_config:
            logger.warning(f"Redis not configured - cannot signal emergency stop for execution {execution_id}")
            return
            
        try:
            import redis
            import json
            
            redis_client = redis.Redis(**self._redis_config)
            
            message = {
                **emergency_signal,
                "source": "handoff_state_machine",
                "priority": "emergency"
            }
            
            # Send to multiple channels for maximum reliability
            redis_client.lpush(f"algo_engine:emergency:{execution_id}", json.dumps(message))
            redis_client.lpush(f"algo_engine:control:{execution_id}", json.dumps(message))
            redis_client.publish("algo_engine:emergency", json.dumps(message))
            
            logger.warning(f"Sent emergency stop signal for execution {execution_id}")
            
        except (ConnectionError, TimeoutError, OSError) as e:
            logger.critical(f"Failed to signal emergency stop due to infrastructure error: {e}")
            # Don't raise - emergency stop must continue even if signaling fails
            
        except Exception as e:
            logger.critical(f"CRITICAL: Failed to signal emergency stop due to unexpected error: {e}", exc_info=True)
            # Don't raise - emergency stop must continue even if signaling fails

    async def _cancel_execution_orders(
        self,
        execution_id: str,
        reason: str
    ) -> List[Dict[str, Any]]:
        """Cancel all orders for an execution."""
        # Get orders to cancel
        result = await self.db.execute(
            text("""
                SELECT id, order_id, symbol, quantity, status
                FROM order_service.orders
                WHERE execution_id = :execution_id::uuid
                  AND status IN ('PENDING', 'SUBMITTED', 'OPEN', 'TRIGGER_PENDING')
            """),
            {"execution_id": execution_id}
        )
        
        orders_to_cancel = []
        for row in result.fetchall():
            orders_to_cancel.append({
                "id": row[0],
                "order_id": row[1],
                "symbol": row[2],
                "quantity": row[3],
                "status": row[4]
            })
        
        # Cancel the orders
        if orders_to_cancel:
            order_ids = [order["id"] for order in orders_to_cancel]
            await self.db.execute(
                text("""
                    UPDATE order_service.orders
                    SET status = 'CANCELLED',
                        updated_at = NOW(),
                        cancelled_reason = :reason
                    WHERE id = ANY(:order_ids)
                """),
                {"order_ids": order_ids, "reason": f"Execution stop: {reason}"}
            )
            await self.db.commit()
        
        return orders_to_cancel

    async def _mark_execution_stopped(
        self,
        execution_id: str,
        reason: str
    ) -> None:
        """Mark execution as stopped in database."""
        await self.db.execute(
            text("""
                UPDATE order_service.execution_contexts
                SET status = 'stopped',
                    stop_reason = :reason,
                    stopped_at = :stopped_at,
                    updated_at = :updated_at
                WHERE execution_id = :execution_id::uuid
            """),
            {
                "execution_id": execution_id,
                "reason": reason,
                "stopped_at": datetime.now(timezone.utc),
                "updated_at": datetime.now(timezone.utc)
            }
        )
        await self.db.commit()

    async def _send_emergency_alert(
        self,
        execution_id: str,
        alert_data: Dict[str, Any]
    ) -> None:
        """Send emergency alert to administrators."""
        if not self._redis_config:
            logger.warning(f"Redis not configured - cannot send emergency alert for execution {execution_id}")
            return
            
        try:
            import redis
            import json
            
            redis_client = redis.Redis(**self._redis_config)
            
            # Send to alert service
            alert_message = {
                **alert_data,
                "alert_id": str(uuid4()),
                "source": "handoff_state_machine"
            }
            
            redis_client.lpush("alerts:emergency", json.dumps(alert_message))
            redis_client.publish("alerts:broadcast", json.dumps(alert_message))
            
            logger.warning(f"Sent emergency alert for execution {execution_id}")
            
        except (ConnectionError, TimeoutError, OSError) as e:
            logger.error(f"Failed to send emergency alert due to infrastructure error: {e}")
            # Don't raise - alert failure shouldn't stop emergency procedures
            
        except Exception as e:
            logger.error(f"Failed to send emergency alert due to unexpected error: {e}")
            # Don't raise - alert failure shouldn't stop emergency procedures

    async def _rollback_positions_to_manual(
        self,
        positions: List[Dict[str, Any]]
    ) -> None:
        """Rollback positions to manual control on initialization failure."""
        for position in positions:
            await self.db.execute(
                text("""
                    UPDATE order_service.positions
                    SET source = 'manual',
                        execution_id = NULL,
                        updated_at = NOW(),
                        metadata = COALESCE(metadata, '{}'::jsonb) || '{\"rollback_reason\": "script_init_failed"}'::jsonb
                    WHERE id = :position_id
                """),
                {"position_id": position["id"]}
            )
        
        await self.db.commit()
        logger.warning(f"Rolled back {len(positions)} positions to manual control")

    async def _rollback_transition(
        self,
        transition_id: str,
        original_state: HandoffState,
        request: TransitionRequest
    ) -> None:
        """Rollback failed transition."""
        # Restore original state
        await self._store_handoff_state(original_state)
        await self._record_transition_audit(
            transition_id, "transition_rolled_back", request.requested_by, {}
        )


# Helper functions for use outside of class context
async def get_handoff_state(
    db: AsyncSession,
    trading_account_id: str,
    strategy_id: Optional[int] = None,
    execution_id: Optional[str] = None
) -> HandoffState:
    """Get current handoff state."""
    service = HandoffStateMachine(db)
    return await service.get_handoff_state(trading_account_id, strategy_id, execution_id)


async def request_handoff_transition(
    db: AsyncSession,
    request: TransitionRequest
) -> TransitionResult:
    """Request a handoff transition."""
    service = HandoffStateMachine(db)
    return await service.request_transition(request)
"""
Manual Attribution Workflow Service

Handles cases where automatic attribution is ambiguous and requires human intervention.
Provides APIs for manual decision-making with comprehensive audit trails.

Key Features:
- Queue management for unresolved attribution cases
- Manual decision capture with audit trails
- Attribution resolution workflow
- Integration with partial exit attribution service
- Compliance and audit support
"""

import logging
from typing import Optional, Dict, Any, List, Tuple
from datetime import datetime, timezone
from decimal import Decimal
from dataclasses import dataclass
from enum import Enum
from uuid import uuid4
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)


class AttributionStatus(str, Enum):
    """Status of manual attribution cases."""
    PENDING = "pending"              # Waiting for manual review
    IN_PROGRESS = "in_progress"      # Being reviewed by user
    RESOLVED = "resolved"            # Manual decision made
    APPLIED = "applied"              # Attribution has been applied to positions
    FAILED = "failed"                # Failed to apply attribution
    EXPIRED = "expired"              # Case expired without resolution


class AttributionPriority(str, Enum):
    """Priority levels for manual attribution."""
    LOW = "low"                      # Minor discrepancy, can wait
    NORMAL = "normal"                # Standard case
    HIGH = "high"                    # Large amount or time-sensitive
    URGENT = "urgent"                # Critical case requiring immediate attention


@dataclass
class AttributionCase:
    """Represents a manual attribution case."""
    case_id: str
    trading_account_id: str
    symbol: str
    exit_quantity: Decimal
    exit_price: Optional[Decimal]
    exit_timestamp: datetime
    affected_positions: List[Dict[str, Any]]
    suggested_allocation: Optional[Dict[str, Any]]
    status: AttributionStatus
    priority: AttributionPriority
    created_at: datetime
    updated_at: datetime
    assigned_to: Optional[str]
    resolution_data: Optional[Dict[str, Any]]
    audit_trail: List[Dict[str, Any]]


@dataclass
class AttributionDecision:
    """Represents a manual attribution decision."""
    case_id: str
    decision_maker: str
    allocation_decisions: List[Dict[str, Any]]  # position_id -> quantity allocations
    decision_rationale: str
    decision_timestamp: datetime


class ManualAttributionService:
    """
    Service for managing manual attribution workflow.

    Handles cases where automatic attribution fails or is ambiguous,
    providing a structured workflow for human decision-making.
    """

    def __init__(self, db: AsyncSession):
        """
        Initialize the manual attribution service.

        Args:
            db: Async database session
        """
        self.db = db

    async def create_attribution_case(
        self,
        trading_account_id: str,
        symbol: str,
        exit_quantity: Decimal,
        exit_price: Optional[Decimal],
        exit_timestamp: datetime,
        affected_positions: List[Dict[str, Any]],
        suggested_allocation: Optional[Dict[str, Any]] = None,
        priority: AttributionPriority = AttributionPriority.NORMAL,
        context: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Create a new manual attribution case.

        Args:
            trading_account_id: Trading account where exit occurred
            symbol: Symbol that was exited
            exit_quantity: Quantity that was exited
            exit_price: Price at which exit occurred
            exit_timestamp: When the exit occurred
            affected_positions: List of positions that could be attributed
            suggested_allocation: Optional suggested allocation from auto-attribution
            priority: Priority level for this case
            context: Additional context for the case

        Returns:
            Case ID for the newly created case

        Raises:
            Exception: If case creation fails
        """
        case_id = str(uuid4())
        created_at = datetime.now(timezone.utc)
        
        logger.info(
            f"Creating manual attribution case {case_id} for {symbol} "
            f"qty={exit_quantity} account={trading_account_id}"
        )

        try:
            # GAP-REC-10: Use ExitContextMatcher to find robust matches before manual attribution
            from .exit_context_matcher import ExitContextMatcher, ExitMatchResult
            
            exit_matcher = ExitContextMatcher(self.db)
            
            # Prepare external exit data for context matching
            external_exit = {
                "symbol": symbol,
                "quantity": float(exit_quantity),
                "price": float(exit_price) if exit_price else None,
                "timestamp": exit_timestamp.isoformat(),
                "trading_account_id": trading_account_id,
                "source": "external_broker_sync"
            }
            
            # Attempt to find robust matches using advanced matching logic
            match_result = await exit_matcher.match_exit_to_internal_trades(
                trading_account_id=trading_account_id,
                symbol=symbol,
                external_exit=external_exit
            )
            
            if match_result.match_quality.value >= ExitMatchResult.MatchQuality.GOOD.value:
                logger.info(
                    f"[{case_id}] Found {match_result.match_quality.value} quality match "
                    f"for external exit: {match_result.matched_trade_ids}"
                )
                
                # If we found a good match, we may not need manual attribution
                if context is None:
                    context = {}
                context.update({
                    "exit_context_match": {
                        "match_quality": match_result.match_quality.value,
                        "matched_trades": match_result.matched_trade_ids,
                        "confidence_score": match_result.confidence_score,
                        "auto_attribution_candidate": match_result.match_quality.value >= ExitMatchResult.MatchQuality.EXCELLENT.value
                    }
                })
                
                if match_result.match_quality == ExitMatchResult.MatchQuality.EXCELLENT:
                    logger.info(f"[{case_id}] Excellent match found - suggesting auto-attribution")
                    if suggested_allocation is None:
                        suggested_allocation = {
                            "allocation_type": "auto_from_context_match",
                            "matched_trade_ids": match_result.matched_trade_ids,
                            "confidence": match_result.confidence_score
                        }
                    priority = AttributionPriority.LOW  # Lower priority for high-confidence matches
                
            else:
                logger.warning(
                    f"[{case_id}] Poor exit context match quality: {match_result.match_quality.value} "
                    f"- proceeding with manual attribution"
                )
                if context is None:
                    context = {}
                context.update({
                    "exit_context_match": {
                        "match_quality": match_result.match_quality.value,
                        "no_good_match_found": True,
                        "match_failure_reason": getattr(match_result, 'failure_reason', 'unknown')
                    }
                })
                
        except Exception as context_match_error:
            logger.error(f"[{case_id}] Exit context matching failed: {context_match_error}")
            # Continue with manual attribution even if context matching fails
            if context is None:
                context = {}
            context.update({
                "exit_context_match_error": str(context_match_error)
            })

        try:
            # Determine priority automatically if not specified
            if priority == AttributionPriority.NORMAL:
                priority = self._determine_priority(exit_quantity, exit_price, affected_positions)

            # Create case data
            case_data = {
                "case_id": case_id,
                "trading_account_id": trading_account_id,
                "symbol": symbol,
                "exit_quantity": str(exit_quantity),
                "exit_price": str(exit_price) if exit_price else None,
                "exit_timestamp": exit_timestamp.isoformat(),
                "affected_positions": affected_positions,
                "suggested_allocation": suggested_allocation,
                "priority": priority.value,
                "context": context or {},
                "created_at": created_at.isoformat(),
                "created_by": "system",
                "reason": "automatic_attribution_failed"
            }

            # Store in database
            await self.db.execute(
                text("""
                    INSERT INTO order_service.manual_attribution_cases (
                        case_id,
                        trading_account_id,
                        symbol,
                        case_data,
                        status,
                        priority,
                        created_at,
                        updated_at
                    ) VALUES (
                        :case_id,
                        :trading_account_id,
                        :symbol,
                        :case_data::jsonb,
                        :status,
                        :priority,
                        :created_at,
                        :updated_at
                    )
                """),
                {
                    "case_id": case_id,
                    "trading_account_id": trading_account_id,
                    "symbol": symbol,
                    "case_data": case_data,
                    "status": AttributionStatus.PENDING.value,
                    "priority": priority.value,
                    "created_at": created_at,
                    "updated_at": created_at
                }
            )

            await self.db.commit()

            # Log case creation in audit trail
            await self._add_audit_event(
                case_id, "case_created", "system", 
                {"priority": priority.value, "positions_count": len(affected_positions)}
            )

            logger.info(f"Created manual attribution case {case_id} with priority {priority}")
            return case_id

        except Exception as e:
            logger.error(f"Failed to create attribution case: {e}", exc_info=True)
            await self.db.rollback()
            raise

    async def get_attribution_case(self, case_id: str) -> Optional[AttributionCase]:
        """
        Retrieve an attribution case by ID.

        Args:
            case_id: Case ID to retrieve

        Returns:
            AttributionCase object or None if not found
        """
        result = await self.db.execute(
            text("""
                SELECT 
                    case_id,
                    trading_account_id,
                    symbol,
                    case_data,
                    status,
                    priority,
                    created_at,
                    updated_at,
                    assigned_to,
                    resolution_data
                FROM order_service.manual_attribution_cases
                WHERE case_id = :case_id
            """),
            {"case_id": case_id}
        )
        
        row = result.fetchone()
        if not row:
            return None

        # Get audit trail
        audit_trail = await self._get_audit_trail(case_id)
        
        case_data = row[3]
        return AttributionCase(
            case_id=row[0],
            trading_account_id=row[1],
            symbol=row[2],
            exit_quantity=Decimal(case_data.get("exit_quantity", "0")),
            exit_price=Decimal(case_data.get("exit_price", "0")) if case_data.get("exit_price") else None,
            exit_timestamp=datetime.fromisoformat(case_data.get("exit_timestamp")),
            affected_positions=case_data.get("affected_positions", []),
            suggested_allocation=case_data.get("suggested_allocation"),
            status=AttributionStatus(row[4]),
            priority=AttributionPriority(row[5]),
            created_at=row[6],
            updated_at=row[7],
            assigned_to=row[8],
            resolution_data=row[9],
            audit_trail=audit_trail
        )

    async def list_pending_cases(
        self,
        trading_account_id: Optional[str] = None,
        symbol: Optional[str] = None,
        priority: Optional[AttributionPriority] = None,
        assigned_to: Optional[str] = None,
        limit: int = 50,
        offset: int = 0
    ) -> List[AttributionCase]:
        """
        List pending attribution cases with filtering.

        Args:
            trading_account_id: Filter by trading account
            symbol: Filter by symbol
            priority: Filter by priority level
            assigned_to: Filter by assigned user
            limit: Maximum cases to return
            offset: Number of cases to skip

        Returns:
            List of AttributionCase objects
        """
        where_clauses = ["status IN ('pending', 'in_progress')"]
        params = {"limit": limit, "offset": offset}

        if trading_account_id:
            where_clauses.append("trading_account_id = :trading_account_id")
            params["trading_account_id"] = trading_account_id

        if symbol:
            where_clauses.append("symbol = :symbol") 
            params["symbol"] = symbol

        if priority:
            where_clauses.append("priority = :priority")
            params["priority"] = priority.value

        if assigned_to:
            where_clauses.append("assigned_to = :assigned_to")
            params["assigned_to"] = assigned_to

        where_clause = " AND ".join(where_clauses)

        result = await self.db.execute(
            text(f"""
                SELECT 
                    case_id,
                    trading_account_id,
                    symbol,
                    case_data,
                    status,
                    priority,
                    created_at,
                    updated_at,
                    assigned_to,
                    resolution_data
                FROM order_service.manual_attribution_cases
                WHERE {where_clause}
                ORDER BY 
                    CASE priority 
                        WHEN 'urgent' THEN 1 
                        WHEN 'high' THEN 2
                        WHEN 'normal' THEN 3
                        WHEN 'low' THEN 4
                    END,
                    created_at ASC
                LIMIT :limit OFFSET :offset
            """),
            params
        )

        cases = []
        for row in result.fetchall():
            case_data = row[3]
            cases.append(AttributionCase(
                case_id=row[0],
                trading_account_id=row[1],
                symbol=row[2],
                exit_quantity=Decimal(case_data.get("exit_quantity", "0")),
                exit_price=Decimal(case_data.get("exit_price", "0")) if case_data.get("exit_price") else None,
                exit_timestamp=datetime.fromisoformat(case_data.get("exit_timestamp")),
                affected_positions=case_data.get("affected_positions", []),
                suggested_allocation=case_data.get("suggested_allocation"),
                status=AttributionStatus(row[4]),
                priority=AttributionPriority(row[5]),
                created_at=row[6],
                updated_at=row[7],
                assigned_to=row[8],
                resolution_data=row[9],
                audit_trail=[]  # Don't load full audit trail for list view
            ))

        return cases

    async def assign_case(
        self,
        case_id: str,
        assigned_to: str,
        assigned_by: str
    ) -> bool:
        """
        Assign a case to a user for resolution.

        Args:
            case_id: Case ID to assign
            assigned_to: User ID to assign to
            assigned_by: User ID who is making the assignment

        Returns:
            True if assignment was successful

        Raises:
            ValueError: If case cannot be assigned
        """
        # Check if case exists and is assignable
        result = await self.db.execute(
            text("""
                SELECT status FROM order_service.manual_attribution_cases
                WHERE case_id = :case_id
            """),
            {"case_id": case_id}
        )
        
        row = result.fetchone()
        if not row:
            raise ValueError(f"Case {case_id} not found")

        if row[0] not in [AttributionStatus.PENDING.value, AttributionStatus.IN_PROGRESS.value]:
            raise ValueError(f"Case {case_id} cannot be assigned (status: {row[0]})")

        # Update assignment
        await self.db.execute(
            text("""
                UPDATE order_service.manual_attribution_cases
                SET assigned_to = :assigned_to,
                    status = :status,
                    updated_at = :updated_at
                WHERE case_id = :case_id
            """),
            {
                "case_id": case_id,
                "assigned_to": assigned_to,
                "status": AttributionStatus.IN_PROGRESS.value,
                "updated_at": datetime.now(timezone.utc)
            }
        )

        await self.db.commit()

        # Add audit event
        await self._add_audit_event(
            case_id, "case_assigned", assigned_by,
            {"assigned_to": assigned_to}
        )

        logger.info(f"Case {case_id} assigned to {assigned_to} by {assigned_by}")
        return True

    async def resolve_case(
        self,
        case_id: str,
        decision: AttributionDecision
    ) -> bool:
        """
        Resolve a case with manual attribution decision.

        Args:
            case_id: Case ID to resolve
            decision: Attribution decision details

        Returns:
            True if resolution was successful

        Raises:
            ValueError: If resolution is invalid
            Exception: If resolution fails
        """
        logger.info(f"Resolving attribution case {case_id}")

        try:
            # Validate case exists and is resolvable
            case = await self.get_attribution_case(case_id)
            if not case:
                raise ValueError(f"Case {case_id} not found")

            if case.status not in [AttributionStatus.PENDING, AttributionStatus.IN_PROGRESS]:
                raise ValueError(f"Case {case_id} cannot be resolved (status: {case.status})")

            # Validate allocation decisions
            await self._validate_attribution_decision(case, decision)

            # Store resolution
            resolution_data = {
                "decision_maker": decision.decision_maker,
                "decision_timestamp": decision.decision_timestamp.isoformat(),
                "allocation_decisions": decision.allocation_decisions,
                "decision_rationale": decision.decision_rationale,
                "total_allocated": str(sum(
                    Decimal(str(alloc.get("quantity", 0))) 
                    for alloc in decision.allocation_decisions
                ))
            }

            await self.db.execute(
                text("""
                    UPDATE order_service.manual_attribution_cases
                    SET status = :status,
                        resolution_data = :resolution_data::jsonb,
                        updated_at = :updated_at
                    WHERE case_id = :case_id
                """),
                {
                    "case_id": case_id,
                    "status": AttributionStatus.RESOLVED.value,
                    "resolution_data": resolution_data,
                    "updated_at": datetime.now(timezone.utc)
                }
            )

            await self.db.commit()

            # Add audit event
            await self._add_audit_event(
                case_id, "case_resolved", decision.decision_maker,
                {
                    "allocations_count": len(decision.allocation_decisions),
                    "rationale": decision.decision_rationale[:100] + "..." if len(decision.decision_rationale) > 100 else decision.decision_rationale
                }
            )

            logger.info(f"Case {case_id} resolved by {decision.decision_maker}")
            return True

        except Exception as e:
            logger.error(f"Failed to resolve case {case_id}: {e}", exc_info=True)
            await self.db.rollback()
            raise

    async def apply_resolution(
        self,
        case_id: str,
        applied_by: str
    ) -> bool:
        """
        Apply the resolution to actual positions/trades.

        Args:
            case_id: Case ID to apply
            applied_by: User applying the resolution

        Returns:
            True if application was successful

        Raises:
            Exception: If application fails
        """
        logger.info(f"Applying resolution for case {case_id}")

        try:
            case = await self.get_attribution_case(case_id)
            if not case:
                raise ValueError(f"Case {case_id} not found")

            if case.status != AttributionStatus.RESOLVED:
                raise ValueError(f"Case {case_id} is not resolved (status: {case.status})")

            if not case.resolution_data:
                raise ValueError(f"Case {case_id} has no resolution data")

            # GAP-REC-9: Pre-apply validation with transfer safety checks
            from .manual_attribution_apply_validator import (
                ManualAttributionApplyValidator, 
                ManualAttributionDecision
            )
            
            validator = ManualAttributionApplyValidator(self.db)
            attribution_decision = ManualAttributionDecision(
                case_id=case_id,
                decision_maker=applied_by,
                allocation_decisions=case.resolution_data.get("allocation_decisions", []),
                decision_rationale=case.resolution_data.get("rationale", "Manual resolution"),
                exit_quantity=Decimal(str(case.exit_quantity)),
                symbol=case.symbol,
                trading_account_id=case.trading_account_id
            )
            
            validation_result = await validator.validate_manual_attribution_decision(attribution_decision)
            
            if not validation_result.can_proceed:
                error_details = "; ".join([f.description for f in validation_result.failures])
                raise ValueError(f"Attribution validation failed: {error_details}")
            
            if validation_result.warnings:
                logger.warning(f"Attribution validation warnings for case {case_id}: {len(validation_result.warnings)} warnings")

            # Apply the allocation decisions by creating transfers
            from .reconciliation_driven_transfers import ReconciliationDrivenTransferService
            transfer_service = ReconciliationDrivenTransferService(self.db)

            # Convert resolution decisions to transfer instructions
            transfer_instructions = []
            allocation_decisions = case.resolution_data.get("allocation_decisions", [])
            
            for decision in allocation_decisions:
                # Find the affected position to determine source execution
                source_execution_id = None
                for affected_pos in case.affected_positions:
                    if affected_pos.get("position_id") == decision.get("position_id"):
                        source_execution_id = affected_pos.get("execution_id")
                        break
                
                if not source_execution_id:
                    logger.warning(f"Could not find source execution for position {decision.get('position_id')}")
                    continue

                # Create transfer instruction to move partial position
                transfer_instructions.append({
                    "source_execution_id": source_execution_id,
                    "target_execution_id": "manual",  # Manual control after attribution
                    "symbol": case.symbol,
                    "quantity": decision.get("quantity", 0),
                    "reason": f"Manual attribution case {case_id}: {decision.get('rationale', 'Manual decision')}",
                    "metadata": {
                        "case_id": case_id,
                        "position_id": decision.get("position_id"),
                        "strategy_id": decision.get("strategy_id"),
                        "decision_maker": case.resolution_data.get("decision_maker")
                    }
                })

            # Execute the transfers if we have any
            if transfer_instructions:
                try:
                    transfer_result = await transfer_service.execute_manual_transfer_instructions(
                        case_id, transfer_instructions, applied_by
                    )
                    
                    if transfer_result.failed_count > 0:
                        logger.warning(
                            f"Some transfers failed for case {case_id}: "
                            f"{transfer_result.executed_count}/{transfer_result.instructions_count} succeeded"
                        )
                    
                    # Record transfer results in case audit
                    await self._add_audit_event(
                        case_id, "transfers_executed", applied_by, {
                            "transfer_id": transfer_result.transfer_id,
                            "instructions_count": transfer_result.instructions_count,
                            "executed_count": transfer_result.executed_count,
                            "failed_count": transfer_result.failed_count,
                            "total_quantity_transferred": str(transfer_result.total_quantity_transferred)
                        }
                    )
                    
                except Exception as transfer_error:
                    logger.error(f"Transfer execution failed for case {case_id}: {transfer_error}")
                    # Don't fail the entire application, but log the error
                    await self._add_audit_event(
                        case_id, "transfer_execution_failed", applied_by, {
                            "error": str(transfer_error)
                        }
                    )

            # Update case status to applied
            await self.db.execute(
                text("""
                    UPDATE order_service.manual_attribution_cases
                    SET status = :status,
                        updated_at = :updated_at
                    WHERE case_id = :case_id
                """),
                {
                    "case_id": case_id,
                    "status": AttributionStatus.APPLIED.value,
                    "updated_at": datetime.now(timezone.utc)
                }
            )

            await self.db.commit()

            # Add final audit event
            await self._add_audit_event(
                case_id, "resolution_applied", applied_by, {
                    "transfer_instructions_count": len(transfer_instructions)
                }
            )

            logger.info(f"Resolution applied for case {case_id} by {applied_by}")
            return True

        except Exception as e:
            logger.error(f"Failed to apply resolution for case {case_id}: {e}", exc_info=True)
            await self.db.rollback()
            raise

    def _determine_priority(
        self,
        exit_quantity: Decimal,
        exit_price: Optional[Decimal],
        affected_positions: List[Dict[str, Any]]
    ) -> AttributionPriority:
        """
        Automatically determine priority based on case characteristics.

        Args:
            exit_quantity: Quantity being attributed
            exit_price: Price of the exit
            affected_positions: Positions affected

        Returns:
            Determined priority level
        """
        # Calculate approximate value
        value = abs(exit_quantity) * (exit_price or Decimal('100'))  # Assume ₹100 if no price
        
        # High value trades get higher priority
        if value > 1000000:  # > ₹10 lakh
            return AttributionPriority.HIGH
        elif value > 100000:  # > ₹1 lakh
            return AttributionPriority.NORMAL
        
        # Multiple affected strategies increase priority
        if len(affected_positions) > 3:
            return AttributionPriority.HIGH
        elif len(affected_positions) > 1:
            return AttributionPriority.NORMAL
            
        return AttributionPriority.LOW

    async def _validate_attribution_decision(
        self,
        case: AttributionCase,
        decision: AttributionDecision
    ) -> None:
        """
        Validate that attribution decision is valid.

        Args:
            case: Attribution case being resolved
            decision: Decision to validate

        Raises:
            ValueError: If decision is invalid
        """
        # Check that all allocated quantities sum to exit quantity
        total_allocated = sum(
            Decimal(str(alloc.get("quantity", 0)))
            for alloc in decision.allocation_decisions
        )
        
        if abs(total_allocated - abs(case.exit_quantity)) > Decimal('0.001'):
            raise ValueError(
                f"Allocation total {total_allocated} does not match exit quantity {case.exit_quantity}"
            )

        # Check that all position IDs are valid
        position_ids = {pos["position_id"] for pos in case.affected_positions}
        for allocation in decision.allocation_decisions:
            if allocation.get("position_id") not in position_ids:
                raise ValueError(f"Invalid position ID: {allocation.get('position_id')}")

    async def _add_audit_event(
        self,
        case_id: str,
        event_type: str,
        user_id: str,
        event_data: Dict[str, Any]
    ) -> None:
        """
        Add an audit event for a case.

        Args:
            case_id: Case ID
            event_type: Type of event
            user_id: User who performed the action
            event_data: Additional event data
        """
        await self.db.execute(
            text("""
                INSERT INTO order_service.manual_attribution_audit (
                    case_id,
                    event_type,
                    user_id,
                    event_data,
                    created_at
                ) VALUES (
                    :case_id,
                    :event_type,
                    :user_id,
                    :event_data::jsonb,
                    :created_at
                )
            """),
            {
                "case_id": case_id,
                "event_type": event_type,
                "user_id": user_id,
                "event_data": event_data,
                "created_at": datetime.now(timezone.utc)
            }
        )

    async def _get_audit_trail(self, case_id: str) -> List[Dict[str, Any]]:
        """
        Get audit trail for a case.

        Args:
            case_id: Case ID

        Returns:
            List of audit events
        """
        result = await self.db.execute(
            text("""
                SELECT event_type, user_id, event_data, created_at
                FROM order_service.manual_attribution_audit
                WHERE case_id = :case_id
                ORDER BY created_at ASC
            """),
            {"case_id": case_id}
        )

        return [
            {
                "event_type": row[0],
                "user_id": row[1], 
                "event_data": row[2],
                "created_at": row[3].isoformat() if row[3] else None
            }
            for row in result.fetchall()
        ]


# Helper function for use outside of class context
async def create_attribution_case(
    db: AsyncSession,
    trading_account_id: str,
    symbol: str,
    exit_quantity: Decimal,
    exit_price: Optional[Decimal],
    exit_timestamp: datetime,
    affected_positions: List[Dict[str, Any]],
    suggested_allocation: Optional[Dict[str, Any]] = None
) -> str:
    """
    Convenience function to create an attribution case.

    Args:
        db: Database session
        trading_account_id: Trading account where exit occurred
        symbol: Symbol that was exited
        exit_quantity: Quantity that was exited
        exit_price: Price at which exit occurred
        exit_timestamp: When the exit occurred
        affected_positions: List of positions that could be attributed
        suggested_allocation: Optional suggested allocation

    Returns:
        Case ID for the newly created case
    """
    service = ManualAttributionService(db)
    return await service.create_attribution_case(
        trading_account_id=trading_account_id,
        symbol=symbol,
        exit_quantity=exit_quantity,
        exit_price=exit_price,
        exit_timestamp=exit_timestamp,
        affected_positions=affected_positions,
        suggested_allocation=suggested_allocation
    )
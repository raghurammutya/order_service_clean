"""
Exit Attribution Policy Service

Implements explicit policies for attributing external exits across strategies.
Provides deterministic rules for single vs multi-strategy exits and full vs partial attribution.

Key Features:
- Single-strategy exit auto-attribution
- Multi-strategy full exit auto-attribution  
- Multi-strategy partial exit policy enforcement
- Policy violation detection and manual escalation
- Audit trail for all attribution decisions
"""

import logging
from typing import Optional, Dict, Any, List
from datetime import datetime, timezone
from decimal import Decimal
from dataclasses import dataclass
from enum import Enum
from uuid import uuid4
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)


class ExitAttributionPolicy(str, Enum):
    """Attribution policies for external exits."""
    AUTO_SINGLE_STRATEGY = "auto_single_strategy"        # Single strategy = auto-attribute
    AUTO_MULTI_FULL = "auto_multi_full"                  # Multi-strategy full exit = auto-attribute
    MANUAL_MULTI_PARTIAL = "manual_multi_partial"       # Multi-strategy partial = manual required
    MANUAL_AMBIGUOUS = "manual_ambiguous"               # Ambiguous cases = manual required
    BLOCKED_INSUFFICIENT_DATA = "blocked_insufficient_data"  # Insufficient data = blocked


class AttributionDecision(str, Enum):
    """Attribution decision outcomes."""
    AUTO_APPROVED = "auto_approved"                      # Auto-attributed per policy
    MANUAL_REQUIRED = "manual_required"                  # Escalated to manual attribution
    BLOCKED = "blocked"                                  # Attribution blocked (insufficient data)
    POLICY_VIOLATION = "policy_violation"                # Policy violated, needs review


@dataclass
class ExitContext:
    """Context for an external exit requiring attribution."""
    exit_id: str
    trading_account_id: str
    symbol: str
    exit_quantity: Decimal
    exit_price: Optional[Decimal]
    exit_timestamp: datetime
    broker_trade_id: Optional[str]
    order_id: Optional[str]


@dataclass
class PositionCandidate:
    """Candidate position for exit attribution."""
    position_id: int
    strategy_id: int
    execution_id: Optional[str]
    portfolio_id: Optional[str]
    available_quantity: Decimal
    entry_price: Decimal
    entry_timestamp: datetime
    position_source: str


@dataclass
class AttributionPolicyResult:
    """Result of policy-based attribution decision."""
    policy_applied: ExitAttributionPolicy
    decision: AttributionDecision
    reason: str
    eligible_positions: List[PositionCandidate]
    recommended_allocation: Optional[Dict[str, Any]]
    manual_intervention_reason: Optional[str]
    audit_data: Dict[str, Any]


class ExitAttributionPolicyService:
    """
    Service for applying explicit attribution policies to external exits.
    
    Enforces business rules for how external exits should be attributed
    across single vs multi-strategy scenarios.
    """

    def __init__(self, db: AsyncSession):
        """
        Initialize the attribution policy service.

        Args:
            db: Async database session
        """
        self.db = db

    async def evaluate_exit_attribution_policy(
        self,
        exit_context: ExitContext,
        override_policy: Optional[ExitAttributionPolicy] = None
    ) -> AttributionPolicyResult:
        """
        Evaluate which attribution policy applies to an external exit.

        Args:
            exit_context: Context of the external exit
            override_policy: Optional policy override for testing/manual control

        Returns:
            Attribution policy result with decision and recommendations

        Raises:
            Exception: If policy evaluation fails
        """
        policy_eval_id = str(uuid4())
        logger.info(
            f"[{policy_eval_id}] Evaluating attribution policy for {exit_context.symbol} "
            f"qty={exit_context.exit_quantity} account={exit_context.trading_account_id}"
        )

        try:
            # Step 1: Find eligible positions for attribution
            eligible_positions = await self._find_eligible_positions(exit_context)
            
            # Step 2: Analyze position distribution
            strategies_involved = set(pos.strategy_id for pos in eligible_positions)
            total_available = sum(pos.available_quantity for pos in eligible_positions)
            
            # Step 3: Apply policy logic
            policy_result = await self._apply_attribution_policy(
                exit_context, eligible_positions, strategies_involved, 
                total_available, override_policy
            )
            
            # Step 4: Generate audit data
            policy_result.audit_data.update({
                "policy_eval_id": policy_eval_id,
                "exit_context": {
                    "exit_id": exit_context.exit_id,
                    "symbol": exit_context.symbol,
                    "exit_quantity": str(exit_context.exit_quantity),
                    "trading_account_id": exit_context.trading_account_id
                },
                "position_analysis": {
                    "total_positions": len(eligible_positions),
                    "strategies_involved": len(strategies_involved),
                    "total_available_quantity": str(total_available),
                    "position_breakdown": [
                        {
                            "strategy_id": pos.strategy_id,
                            "available_quantity": str(pos.available_quantity)
                        }
                        for pos in eligible_positions
                    ]
                },
                "evaluation_timestamp": datetime.now(timezone.utc).isoformat()
            })
            
            logger.info(
                f"[{policy_eval_id}] Policy evaluation complete: "
                f"policy={policy_result.policy_applied}, decision={policy_result.decision}"
            )
            
            return policy_result

        except Exception as e:
            logger.error(f"[{policy_eval_id}] Policy evaluation failed: {e}", exc_info=True)
            # Return blocked result as fallback
            return AttributionPolicyResult(
                policy_applied=ExitAttributionPolicy.BLOCKED_INSUFFICIENT_DATA,
                decision=AttributionDecision.BLOCKED,
                reason=f"Policy evaluation failed: {str(e)}",
                eligible_positions=[],
                recommended_allocation=None,
                manual_intervention_reason=f"System error during policy evaluation: {str(e)}",
                audit_data={"error": str(e), "policy_eval_id": policy_eval_id}
            )

    async def _find_eligible_positions(self, exit_context: ExitContext) -> List[PositionCandidate]:
        """Find positions eligible for exit attribution."""
        result = await self.db.execute(
            text("""
                SELECT 
                    p.id as position_id,
                    p.strategy_id,
                    p.execution_id,
                    p.portfolio_id,
                    p.quantity as available_quantity,
                    COALESCE(p.buy_price, 0) as entry_price,
                    p.created_at as entry_timestamp,
                    p.source as position_source
                FROM order_service.positions p
                WHERE p.trading_account_id = :trading_account_id
                  AND p.symbol = :symbol
                  AND p.is_open = true
                  AND p.quantity > 0  -- Only long positions for sell exits
                  AND p.strategy_id IS NOT NULL  -- Must have strategy assignment
                ORDER BY p.created_at ASC  -- FIFO ordering
            """),
            {
                "trading_account_id": exit_context.trading_account_id,
                "symbol": exit_context.symbol
            }
        )

        positions = []
        for row in result.fetchall():
            positions.append(PositionCandidate(
                position_id=row[0],
                strategy_id=row[1],
                execution_id=str(row[2]) if row[2] else None,
                portfolio_id=row[3],
                available_quantity=Decimal(str(row[4])),
                entry_price=Decimal(str(row[5])),
                entry_timestamp=row[6],
                position_source=row[7]
            ))

        logger.debug(
            f"Found {len(positions)} eligible positions for {exit_context.symbol} "
            f"in account {exit_context.trading_account_id}"
        )
        return positions

    async def _apply_attribution_policy(
        self,
        exit_context: ExitContext,
        eligible_positions: List[PositionCandidate],
        strategies_involved: set,
        total_available: Decimal,
        override_policy: Optional[ExitAttributionPolicy]
    ) -> AttributionPolicyResult:
        """Apply attribution policy logic."""
        
        exit_quantity = abs(exit_context.exit_quantity)
        
        # Policy 1: No eligible positions - block
        if not eligible_positions:
            return AttributionPolicyResult(
                policy_applied=ExitAttributionPolicy.BLOCKED_INSUFFICIENT_DATA,
                decision=AttributionDecision.BLOCKED,
                reason="No eligible positions found for attribution",
                eligible_positions=[],
                recommended_allocation=None,
                manual_intervention_reason="No open positions exist for this symbol",
                audit_data={"block_reason": "no_eligible_positions"}
            )

        # Policy 2: Insufficient quantity - manual required
        if exit_quantity > total_available:
            return AttributionPolicyResult(
                policy_applied=ExitAttributionPolicy.MANUAL_AMBIGUOUS,
                decision=AttributionDecision.MANUAL_REQUIRED,
                reason=f"Exit quantity ({exit_quantity}) exceeds available quantity ({total_available})",
                eligible_positions=eligible_positions,
                recommended_allocation=None,
                manual_intervention_reason=f"Exit quantity exceeds position quantities by {exit_quantity - total_available}",
                audit_data={
                    "exit_quantity": str(exit_quantity),
                    "total_available": str(total_available),
                    "excess_quantity": str(exit_quantity - total_available)
                }
            )

        # Apply override policy if provided
        if override_policy:
            return self._apply_specific_policy(
                override_policy, exit_context, eligible_positions, 
                strategies_involved, total_available
            )

        # Policy 3: Single strategy - auto-attribute
        if len(strategies_involved) == 1:
            return self._apply_single_strategy_policy(
                exit_context, eligible_positions, total_available
            )

        # Policy 4: Multi-strategy full exit - auto-attribute FIFO
        if exit_quantity == total_available:
            return self._apply_multi_strategy_full_policy(
                exit_context, eligible_positions
            )

        # Policy 5: Multi-strategy partial exit - manual required
        return self._apply_multi_strategy_partial_policy(
            exit_context, eligible_positions, strategies_involved, total_available
        )

    def _apply_single_strategy_policy(
        self,
        exit_context: ExitContext,
        eligible_positions: List[PositionCandidate],
        total_available: Decimal
    ) -> AttributionPolicyResult:
        """Apply single strategy auto-attribution policy."""
        strategy_id = eligible_positions[0].strategy_id
        exit_quantity = abs(exit_context.exit_quantity)
        
        # Generate FIFO allocation for single strategy
        allocation = []
        remaining_to_allocate = exit_quantity
        
        for position in eligible_positions:
            if remaining_to_allocate <= 0:
                break
            
            allocated_quantity = min(remaining_to_allocate, position.available_quantity)
            allocation.append({
                "position_id": position.position_id,
                "allocated_quantity": str(allocated_quantity),
                "strategy_id": position.strategy_id,
                "allocation_method": "FIFO"
            })
            remaining_to_allocate -= allocated_quantity

        return AttributionPolicyResult(
            policy_applied=ExitAttributionPolicy.AUTO_SINGLE_STRATEGY,
            decision=AttributionDecision.AUTO_APPROVED,
            reason=f"Single strategy ({strategy_id}) - auto-attribution approved",
            eligible_positions=eligible_positions,
            recommended_allocation={
                "method": "FIFO",
                "allocations": allocation
            },
            manual_intervention_reason=None,
            audit_data={
                "strategy_id": strategy_id,
                "allocation_method": "FIFO",
                "fully_allocated": remaining_to_allocate == 0
            }
        )

    def _apply_multi_strategy_full_policy(
        self,
        exit_context: ExitContext,
        eligible_positions: List[PositionCandidate]
    ) -> AttributionPolicyResult:
        """Apply multi-strategy full exit auto-attribution policy."""
        exit_quantity = abs(exit_context.exit_quantity)
        
        # Generate FIFO allocation across all strategies
        allocation = []
        remaining_to_allocate = exit_quantity
        
        for position in eligible_positions:
            if remaining_to_allocate <= 0:
                break
            
            allocated_quantity = min(remaining_to_allocate, position.available_quantity)
            allocation.append({
                "position_id": position.position_id,
                "allocated_quantity": str(allocated_quantity),
                "strategy_id": position.strategy_id,
                "allocation_method": "FIFO_CROSS_STRATEGY"
            })
            remaining_to_allocate -= allocated_quantity

        return AttributionPolicyResult(
            policy_applied=ExitAttributionPolicy.AUTO_MULTI_FULL,
            decision=AttributionDecision.AUTO_APPROVED,
            reason="Multi-strategy full exit - auto-attribution approved with FIFO",
            eligible_positions=eligible_positions,
            recommended_allocation={
                "method": "FIFO_CROSS_STRATEGY",
                "allocations": allocation
            },
            manual_intervention_reason=None,
            audit_data={
                "strategies_count": len(set(pos.strategy_id for pos in eligible_positions)),
                "allocation_method": "FIFO_CROSS_STRATEGY",
                "is_full_exit": True
            }
        )

    def _apply_multi_strategy_partial_policy(
        self,
        exit_context: ExitContext,
        eligible_positions: List[PositionCandidate],
        strategies_involved: set,
        total_available: Decimal
    ) -> AttributionPolicyResult:
        """Apply multi-strategy partial exit manual intervention policy."""
        exit_quantity = abs(exit_context.exit_quantity)
        
        return AttributionPolicyResult(
            policy_applied=ExitAttributionPolicy.MANUAL_MULTI_PARTIAL,
            decision=AttributionDecision.MANUAL_REQUIRED,
            reason=f"Multi-strategy partial exit requires manual attribution decision",
            eligible_positions=eligible_positions,
            recommended_allocation=None,  # No recommendation - manual decision required
            manual_intervention_reason=(
                f"Partial exit ({exit_quantity}) across {len(strategies_involved)} strategies "
                f"requires explicit allocation decision. Total available: {total_available}"
            ),
            audit_data={
                "strategies_count": len(strategies_involved),
                "exit_quantity": str(exit_quantity),
                "total_available": str(total_available),
                "partial_percentage": float((exit_quantity / total_available) * 100),
                "requires_manual_allocation": True
            }
        )

    def _apply_specific_policy(
        self,
        policy: ExitAttributionPolicy,
        exit_context: ExitContext,
        eligible_positions: List[PositionCandidate],
        strategies_involved: set,
        total_available: Decimal
    ) -> AttributionPolicyResult:
        """Apply a specific override policy."""
        if policy == ExitAttributionPolicy.AUTO_SINGLE_STRATEGY:
            return self._apply_single_strategy_policy(exit_context, eligible_positions, total_available)
        elif policy == ExitAttributionPolicy.AUTO_MULTI_FULL:
            return self._apply_multi_strategy_full_policy(exit_context, eligible_positions)
        elif policy == ExitAttributionPolicy.MANUAL_MULTI_PARTIAL:
            return self._apply_multi_strategy_partial_policy(
                exit_context, eligible_positions, strategies_involved, total_available
            )
        else:
            return AttributionPolicyResult(
                policy_applied=policy,
                decision=AttributionDecision.MANUAL_REQUIRED,
                reason=f"Override policy {policy} applied - manual review required",
                eligible_positions=eligible_positions,
                recommended_allocation=None,
                manual_intervention_reason=f"Policy override: {policy}",
                audit_data={"override_policy": policy.value}
            )


# Helper functions for external use
async def evaluate_exit_attribution_policy(
    db: AsyncSession,
    exit_context: ExitContext,
    override_policy: Optional[ExitAttributionPolicy] = None
) -> AttributionPolicyResult:
    """
    Convenience function for evaluating exit attribution policy.

    Args:
        db: Database session
        exit_context: External exit context
        override_policy: Optional policy override

    Returns:
        Attribution policy result
    """
    service = ExitAttributionPolicyService(db)
    return await service.evaluate_exit_attribution_policy(exit_context, override_policy)
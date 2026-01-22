"""
Holdings Reconciliation Integration Service

Integrates holdings reconciliation with the partial exit attribution allocator
to resolve variances when broker holdings don't match internal position sums.

Key Features:
- Detection of holdings variances during reconciliation
- Integration with PartialExitAttributionService for variance resolution
- Automatic variance classification (known vs unknown exits)
- Manual case creation for unresolved variances
- Comprehensive audit trail linking holdings to attribution
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

from .partial_exit_attribution_service import (
    PartialExitAttributionService,
    AllocationResult,
    AllocationMethod,
    attribute_partial_exit
)
from .manual_attribution_service import (
    ManualAttributionService,
    AttributionPriority,
    create_attribution_case
)

logger = logging.getLogger(__name__)


class VarianceType(str, Enum):
    """Types of holdings variances."""
    UNKNOWN_EXIT = "unknown_exit"           # Broker shows less, unknown exit
    UNKNOWN_ENTRY = "unknown_entry"        # Broker shows more, unknown entry
    KNOWN_EXIT = "known_exit"               # Broker shows less, known external exit
    POSITION_MISMATCH = "position_mismatch" # Complex variance requiring analysis
    ROUNDING_DIFFERENCE = "rounding_difference" # Small rounding/fractional differences


class VarianceResolution(str, Enum):
    """Variance resolution status."""
    AUTO_RESOLVED = "auto_resolved"         # Automatically resolved via attribution
    MANUAL_REQUIRED = "manual_required"     # Requires manual intervention
    IGNORED = "ignored"                     # Below threshold, ignored
    FAILED = "failed"                       # Resolution attempt failed


@dataclass
class HoldingsVariance:
    """Represents a holdings variance requiring resolution."""
    variance_id: str
    trading_account_id: str
    symbol: str
    broker_quantity: Decimal
    internal_quantity: Decimal
    variance_quantity: Decimal
    variance_type: VarianceType
    detected_at: datetime
    positions_involved: List[Dict[str, Any]]
    metadata: Dict[str, Any]


@dataclass
class VarianceResolutionResult:
    """Result of holdings variance resolution."""
    variance_id: str
    resolution_type: VarianceResolution
    allocation_id: Optional[str]
    attribution_case_id: Optional[str]
    variance_resolved: Decimal
    variance_remaining: Decimal
    requires_manual_review: bool
    audit_trail: List[Dict[str, Any]]
    errors: List[str]
    warnings: List[str]


class HoldingsReconciliationIntegration:
    """
    Service for integrating holdings reconciliation with attribution allocator.

    Resolves holdings variances by attempting automatic attribution and
    creating manual cases when automatic resolution isn't possible.
    """

    def __init__(self, db: AsyncSession):
        """
        Initialize the holdings reconciliation integration.

        Args:
            db: Async database session
        """
        self.db = db
        self.attribution_service = PartialExitAttributionService(db)
        self.manual_attribution_service = ManualAttributionService(db)

    async def reconcile_holdings_variance(
        self,
        trading_account_id: str,
        symbol: str,
        broker_quantity: Decimal,
        internal_positions: List[Dict[str, Any]],
        variance_threshold: Decimal = Decimal('0.01')
    ) -> VarianceResolutionResult:
        """
        Reconcile a holdings variance between broker and internal positions.

        Args:
            trading_account_id: Trading account ID
            symbol: Symbol with variance
            broker_quantity: Actual quantity at broker
            internal_positions: List of internal positions for this symbol
            variance_threshold: Minimum variance to process

        Returns:
            Variance resolution result

        Raises:
            Exception: If reconciliation fails
        """
        variance_id = str(uuid4())
        detected_at = datetime.now(timezone.utc)
        
        # Calculate internal quantity and variance
        internal_quantity = sum(Decimal(str(pos.get('quantity', 0))) for pos in internal_positions)
        variance_quantity = broker_quantity - internal_quantity

        logger.info(
            f"[{variance_id}] Reconciling holdings variance for {symbol}: "
            f"broker={broker_quantity}, internal={internal_quantity}, variance={variance_quantity}"
        )

        # Check if variance is below threshold
        if abs(variance_quantity) <= variance_threshold:
            return VarianceResolutionResult(
                variance_id=variance_id,
                resolution_type=VarianceResolution.IGNORED,
                allocation_id=None,
                attribution_case_id=None,
                variance_resolved=Decimal('0'),
                variance_remaining=variance_quantity,
                requires_manual_review=False,
                audit_trail=[],
                errors=[],
                warnings=[f"Variance {variance_quantity} below threshold {variance_threshold}"]
            )

        try:
            # Step 1: Classify variance type
            variance = HoldingsVariance(
                variance_id=variance_id,
                trading_account_id=trading_account_id,
                symbol=symbol,
                broker_quantity=broker_quantity,
                internal_quantity=internal_quantity,
                variance_quantity=variance_quantity,
                variance_type=await self._classify_variance(
                    variance_quantity, internal_positions, trading_account_id, symbol
                ),
                detected_at=detected_at,
                positions_involved=internal_positions,
                metadata={}
            )

            # Step 2: Store variance for audit
            await self._store_holdings_variance(variance)

            # Step 3: Attempt resolution based on variance type
            if variance.variance_type == VarianceType.UNKNOWN_EXIT:
                return await self._resolve_unknown_exit(variance)
            elif variance.variance_type == VarianceType.KNOWN_EXIT:
                return await self._resolve_known_exit(variance)
            elif variance.variance_type == VarianceType.UNKNOWN_ENTRY:
                return await self._resolve_unknown_entry(variance)
            elif variance.variance_type == VarianceType.ROUNDING_DIFFERENCE:
                return await self._resolve_rounding_difference(variance)
            else:
                return await self._create_manual_resolution_case(variance)

        except Exception as e:
            logger.error(f"[{variance_id}] Holdings reconciliation failed: {e}", exc_info=True)
            
            await self._record_variance_audit(
                variance_id, "reconciliation_failed", "system",
                {"error": str(e)}
            )

            return VarianceResolutionResult(
                variance_id=variance_id,
                resolution_type=VarianceResolution.FAILED,
                allocation_id=None,
                attribution_case_id=None,
                variance_resolved=Decimal('0'),
                variance_remaining=variance_quantity,
                requires_manual_review=True,
                audit_trail=[],
                errors=[str(e)],
                warnings=[]
            )

    async def _classify_variance(
        self,
        variance_quantity: Decimal,
        internal_positions: List[Dict[str, Any]],
        trading_account_id: str,
        symbol: str
    ) -> VarianceType:
        """
        Classify the type of variance.

        Args:
            variance_quantity: Variance amount
            internal_positions: Internal positions
            trading_account_id: Trading account ID
            symbol: Symbol

        Returns:
            Variance type classification
        """
        # Negative variance = broker has less (unknown exit)
        if variance_quantity < 0:
            # Check if this looks like a known external exit
            if await self._has_recent_external_orders(trading_account_id, symbol, abs(variance_quantity)):
                return VarianceType.KNOWN_EXIT
            else:
                return VarianceType.UNKNOWN_EXIT
        
        # Positive variance = broker has more (unknown entry)
        elif variance_quantity > 0:
            return VarianceType.UNKNOWN_ENTRY
        
        # Check for rounding differences
        elif abs(variance_quantity) <= Decimal('0.1'):
            return VarianceType.ROUNDING_DIFFERENCE
        
        else:
            return VarianceType.POSITION_MISMATCH

    async def _resolve_unknown_exit(
        self,
        variance: HoldingsVariance
    ) -> VarianceResolutionResult:
        """
        Resolve an unknown exit variance using attribution allocator.

        Args:
            variance: Holdings variance to resolve

        Returns:
            Variance resolution result
        """
        try:
            # Use attribution service to allocate the unknown exit
            allocation_result = await self.attribution_service.attribute_partial_exit(
                trading_account_id=variance.trading_account_id,
                symbol=variance.symbol,
                exit_quantity=abs(variance.variance_quantity),  # Exit is positive quantity
                exit_price=None,  # No price information from holdings
                exit_timestamp=variance.detected_at,
                allocation_method=AllocationMethod.FIFO  # Default to FIFO for unknown exits
            )

            await self._record_variance_audit(
                variance.variance_id, "unknown_exit_allocated", "system",
                {
                    "allocation_id": allocation_result.allocation_id,
                    "allocations_count": len(allocation_result.allocations),
                    "requires_manual": allocation_result.requires_manual_intervention
                }
            )

            if allocation_result.requires_manual_intervention:
                # Create manual case for unresolved portions
                case_id = await self._create_manual_case_from_allocation(
                    variance, allocation_result
                )
                
                return VarianceResolutionResult(
                    variance_id=variance.variance_id,
                    resolution_type=VarianceResolution.MANUAL_REQUIRED,
                    allocation_id=allocation_result.allocation_id,
                    attribution_case_id=case_id,
                    variance_resolved=allocation_result.total_allocated_quantity,
                    variance_remaining=allocation_result.unallocated_quantity,
                    requires_manual_review=True,
                    audit_trail=[],
                    errors=[],
                    warnings=["Manual review required for unallocated portion"]
                )
            else:
                return VarianceResolutionResult(
                    variance_id=variance.variance_id,
                    resolution_type=VarianceResolution.AUTO_RESOLVED,
                    allocation_id=allocation_result.allocation_id,
                    attribution_case_id=None,
                    variance_resolved=allocation_result.total_allocated_quantity,
                    variance_remaining=allocation_result.unallocated_quantity,
                    requires_manual_review=False,
                    audit_trail=[],
                    errors=[],
                    warnings=[]
                )

        except Exception as e:
            logger.error(f"Unknown exit resolution failed: {e}", exc_info=True)
            return await self._create_manual_resolution_case(variance)

    async def _resolve_known_exit(
        self,
        variance: HoldingsVariance
    ) -> VarianceResolutionResult:
        """
        Resolve a known exit variance (where we have external order info).

        Args:
            variance: Holdings variance to resolve

        Returns:
            Variance resolution result
        """
        # For known exits, we can be more precise with attribution
        # by using the actual exit order details
        
        # Get the external order that likely caused this variance
        external_order = await self._find_matching_external_order(
            variance.trading_account_id,
            variance.symbol,
            abs(variance.variance_quantity),
            variance.detected_at
        )

        if external_order:
            # Use the external order details for more accurate attribution
            allocation_result = await self.attribution_service.attribute_partial_exit(
                trading_account_id=variance.trading_account_id,
                symbol=variance.symbol,
                exit_quantity=abs(variance.variance_quantity),
                exit_price=external_order.get('price'),
                exit_timestamp=external_order.get('timestamp', variance.detected_at),
                allocation_method=AllocationMethod.FIFO
            )

            await self._record_variance_audit(
                variance.variance_id, "known_exit_allocated", "system",
                {
                    "external_order_id": external_order.get('id'),
                    "allocation_id": allocation_result.allocation_id
                }
            )

            return VarianceResolutionResult(
                variance_id=variance.variance_id,
                resolution_type=VarianceResolution.AUTO_RESOLVED if not allocation_result.requires_manual_intervention else VarianceResolution.MANUAL_REQUIRED,
                allocation_id=allocation_result.allocation_id,
                attribution_case_id=None,
                variance_resolved=allocation_result.total_allocated_quantity,
                variance_remaining=allocation_result.unallocated_quantity,
                requires_manual_review=allocation_result.requires_manual_intervention,
                audit_trail=[],
                errors=[],
                warnings=[]
            )
        else:
            # Treat as unknown exit if we can't find matching order
            return await self._resolve_unknown_exit(variance)

    async def _resolve_unknown_entry(
        self,
        variance: HoldingsVariance
    ) -> VarianceResolutionResult:
        """
        Resolve an unknown entry variance (broker has more than internal).

        Args:
            variance: Holdings variance to resolve

        Returns:
            Variance resolution result
        """
        # Unknown entries typically require manual review
        # as they may represent missing buy orders or corporate actions
        
        case_id = await self.manual_attribution_service.create_attribution_case(
            trading_account_id=variance.trading_account_id,
            symbol=variance.symbol,
            exit_quantity=-variance.variance_quantity,  # Negative for entry
            exit_price=None,
            exit_timestamp=variance.detected_at,
            affected_positions=variance.positions_involved,
            priority=AttributionPriority.HIGH,  # Unknown entries are high priority
            context={
                "variance_id": variance.variance_id,
                "variance_type": variance.variance_type.value,
                "reason": "Unknown entry detected - broker has more holdings than internal positions"
            }
        )

        await self._record_variance_audit(
            variance.variance_id, "unknown_entry_manual_case", "system",
            {"case_id": case_id}
        )

        return VarianceResolutionResult(
            variance_id=variance.variance_id,
            resolution_type=VarianceResolution.MANUAL_REQUIRED,
            allocation_id=None,
            attribution_case_id=case_id,
            variance_resolved=Decimal('0'),
            variance_remaining=variance.variance_quantity,
            requires_manual_review=True,
            audit_trail=[],
            errors=[],
            warnings=["Unknown entry requires manual investigation"]
        )

    async def _resolve_rounding_difference(
        self,
        variance: HoldingsVariance
    ) -> VarianceResolutionResult:
        """
        Resolve a small rounding difference.

        Args:
            variance: Holdings variance to resolve

        Returns:
            Variance resolution result
        """
        # For small rounding differences, we can auto-adjust
        await self._record_variance_audit(
            variance.variance_id, "rounding_difference_ignored", "system",
            {"variance_amount": str(variance.variance_quantity)}
        )

        return VarianceResolutionResult(
            variance_id=variance.variance_id,
            resolution_type=VarianceResolution.IGNORED,
            allocation_id=None,
            attribution_case_id=None,
            variance_resolved=Decimal('0'),
            variance_remaining=variance.variance_quantity,
            requires_manual_review=False,
            audit_trail=[],
            errors=[],
            warnings=[f"Small rounding difference ignored: {variance.variance_quantity}"]
        )

    async def _create_manual_resolution_case(
        self,
        variance: HoldingsVariance
    ) -> VarianceResolutionResult:
        """
        Create a manual attribution case for complex variances.

        Args:
            variance: Holdings variance requiring manual review

        Returns:
            Variance resolution result with manual case
        """
        case_id = await self.manual_attribution_service.create_attribution_case(
            trading_account_id=variance.trading_account_id,
            symbol=variance.symbol,
            exit_quantity=variance.variance_quantity,
            exit_price=None,
            exit_timestamp=variance.detected_at,
            affected_positions=variance.positions_involved,
            priority=AttributionPriority.HIGH,
            context={
                "variance_id": variance.variance_id,
                "variance_type": variance.variance_type.value,
                "broker_quantity": str(variance.broker_quantity),
                "internal_quantity": str(variance.internal_quantity),
                "reason": "Complex holdings variance requiring manual resolution"
            }
        )

        await self._record_variance_audit(
            variance.variance_id, "manual_resolution_case_created", "system",
            {"case_id": case_id, "variance_type": variance.variance_type.value}
        )

        return VarianceResolutionResult(
            variance_id=variance.variance_id,
            resolution_type=VarianceResolution.MANUAL_REQUIRED,
            allocation_id=None,
            attribution_case_id=case_id,
            variance_resolved=Decimal('0'),
            variance_remaining=variance.variance_quantity,
            requires_manual_review=True,
            audit_trail=[],
            errors=[],
            warnings=["Complex variance requires manual attribution"]
        )

    async def _create_manual_case_from_allocation(
        self,
        variance: HoldingsVariance,
        allocation_result: AllocationResult
    ) -> str:
        """
        Create manual case for unresolved allocation portions.

        Args:
            variance: Original holdings variance
            allocation_result: Partial allocation result

        Returns:
            Manual case ID
        """
        case_id = await self.manual_attribution_service.create_attribution_case(
            trading_account_id=variance.trading_account_id,
            symbol=variance.symbol,
            exit_quantity=allocation_result.unallocated_quantity,
            exit_price=None,
            exit_timestamp=variance.detected_at,
            affected_positions=variance.positions_involved,
            suggested_allocation={
                "allocation_id": allocation_result.allocation_id,
                "allocated_quantity": str(allocation_result.total_allocated_quantity),
                "allocations": [
                    {
                        "position_id": alloc.position_id,
                        "strategy_id": alloc.strategy_id,
                        "allocated_quantity": str(alloc.allocated_quantity)
                    }
                    for alloc in allocation_result.allocations
                ]
            },
            priority=AttributionPriority.NORMAL,
            context={
                "variance_id": variance.variance_id,
                "partial_allocation": True,
                "reason": "Partial allocation from holdings reconciliation"
            }
        )

        return case_id

    async def _store_holdings_variance(self, variance: HoldingsVariance) -> None:
        """Store holdings variance for audit."""
        await self.db.execute(
            text("""
                INSERT INTO order_service.holdings_reconciliation_variances (
                    variance_id,
                    trading_account_id,
                    symbol,
                    broker_quantity,
                    internal_quantity,
                    variance_quantity,
                    variance_type,
                    detected_at,
                    positions_data,
                    metadata
                ) VALUES (
                    :variance_id,
                    :trading_account_id,
                    :symbol,
                    :broker_quantity,
                    :internal_quantity,
                    :variance_quantity,
                    :variance_type,
                    :detected_at,
                    :positions_data::jsonb,
                    :metadata::jsonb
                )
            """),
            {
                "variance_id": variance.variance_id,
                "trading_account_id": variance.trading_account_id,
                "symbol": variance.symbol,
                "broker_quantity": str(variance.broker_quantity),
                "internal_quantity": str(variance.internal_quantity),
                "variance_quantity": str(variance.variance_quantity),
                "variance_type": variance.variance_type.value,
                "detected_at": variance.detected_at,
                "positions_data": variance.positions_involved,
                "metadata": variance.metadata
            }
        )
        await self.db.commit()

    async def _record_variance_audit(
        self,
        variance_id: str,
        event_type: str,
        user_id: str,
        event_data: Dict[str, Any]
    ) -> None:
        """Record variance resolution audit event."""
        await self.db.execute(
            text("""
                INSERT INTO order_service.holdings_variance_audit (
                    variance_id,
                    event_type,
                    user_id,
                    event_data,
                    created_at
                ) VALUES (
                    :variance_id,
                    :event_type,
                    :user_id,
                    :event_data::jsonb,
                    :created_at
                )
            """),
            {
                "variance_id": variance_id,
                "event_type": event_type,
                "user_id": user_id,
                "event_data": event_data,
                "created_at": datetime.now(timezone.utc)
            }
        )

    # Helper methods for external order lookup
    async def _has_recent_external_orders(
        self,
        trading_account_id: str,
        symbol: str,
        quantity: Decimal
    ) -> bool:
        """Check if there are recent external orders that could explain variance."""
        # Look for external orders in the last 24 hours that match the variance
        cutoff_time = datetime.now(timezone.utc) - timedelta(hours=24)
        
        result = await self.db.execute(
            text("""
                SELECT COUNT(*)
                FROM order_service.orders
                WHERE trading_account_id = :trading_account_id
                  AND symbol = :symbol
                  AND source = 'external'
                  AND ABS(ABS(quantity) - :quantity) <= 0.01
                  AND created_at >= :cutoff_time
                  AND status IN ('COMPLETE', 'FILLED')
            """),
            {
                "trading_account_id": trading_account_id,
                "symbol": symbol,
                "quantity": str(abs(quantity)),
                "cutoff_time": cutoff_time
            }
        )
        
        row = result.fetchone()
        external_order_count = row[0] if row else 0
        
        logger.debug(
            f"Found {external_order_count} recent external orders for {symbol} "
            f"qty~{quantity} in account {trading_account_id}"
        )
        
        return external_order_count > 0

    async def _find_matching_external_order(
        self,
        trading_account_id: str,
        symbol: str,
        quantity: Decimal,
        detected_at: datetime
    ) -> Optional[Dict[str, Any]]:
        """Find external order that matches the variance."""
        # Look for external orders within the last 24 hours before variance detection
        cutoff_time = detected_at - timedelta(hours=24)
        
        result = await self.db.execute(
            text("""
                SELECT 
                    id,
                    order_id,
                    symbol,
                    side,
                    quantity,
                    price,
                    average_price,
                    status,
                    created_at,
                    updated_at
                FROM order_service.orders
                WHERE trading_account_id = :trading_account_id
                  AND symbol = :symbol
                  AND source = 'external'
                  AND ABS(ABS(quantity) - :quantity) <= 0.01
                  AND created_at >= :cutoff_time
                  AND created_at <= :detected_at
                  AND status IN ('COMPLETE', 'FILLED')
                ORDER BY ABS(ABS(quantity) - :quantity) ASC, created_at DESC
                LIMIT 1
            """),
            {
                "trading_account_id": trading_account_id,
                "symbol": symbol,
                "quantity": str(abs(quantity)),
                "cutoff_time": cutoff_time,
                "detected_at": detected_at
            }
        )
        
        row = result.fetchone()
        if not row:
            logger.debug(
                f"No matching external order found for {symbol} qty={quantity} "
                f"in account {trading_account_id}"
            )
            return None
        
        external_order = {
            "id": row[0],
            "order_id": row[1], 
            "symbol": row[2],
            "side": row[3],
            "quantity": row[4],
            "price": row[5],
            "average_price": row[6],
            "status": row[7],
            "timestamp": row[8],
            "updated_at": row[9]
        }
        
        logger.debug(
            f"Found matching external order {external_order['id']} for variance: "
            f"{symbol} qty={quantity}"
        )
        
        return external_order


# Helper function for use outside of class context
async def reconcile_holdings_variance(
    db: AsyncSession,
    trading_account_id: str,
    symbol: str,
    broker_quantity: Decimal,
    internal_positions: List[Dict[str, Any]]
) -> VarianceResolutionResult:
    """
    Reconcile a holdings variance.

    Args:
        db: Database session
        trading_account_id: Trading account ID
        symbol: Symbol with variance
        broker_quantity: Actual quantity at broker
        internal_positions: List of internal positions

    Returns:
        Variance resolution result
    """
    service = HoldingsReconciliationIntegration(db)
    return await service.reconcile_holdings_variance(
        trading_account_id, symbol, broker_quantity, internal_positions
    )
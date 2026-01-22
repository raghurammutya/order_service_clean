"""
Partial Exit Attribution Service

Handles allocation of partial external exits when the same position exists
across multiple strategies with deterministic and auditable allocation.

Key Features:
- FIFO (First-In-First-Out) allocation method for tax compliance and audit transparency
- Handles partial exits from broker that need to be attributed across multiple strategies
- Deterministic allocation algorithm ensuring consistent results
- Comprehensive audit trail for compliance and debugging
- Integration with execution transfer service for position movement
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


class AllocationMethod(str, Enum):
    """Allocation methods for partial exit attribution."""
    FIFO = "fifo"  # First-In-First-Out (oldest positions first)
    LIFO = "lifo"  # Last-In-First-Out (newest positions first)
    MANUAL = "manual"  # Manual allocation with specific trade IDs


@dataclass
class PositionAllocation:
    """Represents how much of a position should be allocated to an exit."""
    position_id: int
    strategy_id: int
    execution_id: str
    symbol: str
    allocated_quantity: Decimal
    remaining_quantity: Decimal
    entry_price: Decimal
    entry_timestamp: datetime
    allocation_reason: str


@dataclass
class AllocationResult:
    """Result of partial exit attribution."""
    allocation_id: str
    total_exit_quantity: Decimal
    total_allocated_quantity: Decimal
    unallocated_quantity: Decimal
    allocations: List[PositionAllocation]
    allocation_method: AllocationMethod
    requires_manual_intervention: bool
    audit_trail: Dict[str, Any]


class PartialExitAttributionService:
    """
    Service for attributing partial exits across multiple strategies.

    When an external exit occurs (e.g., user sells 100 shares via broker terminal)
    and the same symbol has positions in multiple strategies, this service determines
    how to allocate the exit across those strategies using deterministic algorithms.
    """

    def __init__(self, db: AsyncSession):
        """
        Initialize the attribution service.

        Args:
            db: Async database session
        """
        self.db = db

    async def attribute_partial_exit(
        self,
        trading_account_id: str,
        symbol: str,
        exit_quantity: Decimal,
        exit_price: Optional[Decimal] = None,
        exit_timestamp: Optional[datetime] = None,
        allocation_method: AllocationMethod = AllocationMethod.FIFO,
        specific_trade_ids: Optional[List[int]] = None,
        enforce_policy: bool = True
    ) -> AllocationResult:
        """
        Attribute a partial exit across multiple strategies.

        Args:
            trading_account_id: Trading account ID where exit occurred
            symbol: Symbol that was exited
            exit_quantity: Quantity that was exited (positive for sells, negative for buys)
            exit_price: Price at which exit occurred (optional)
            exit_timestamp: When the exit occurred (defaults to now)
            allocation_method: How to allocate across strategies (FIFO, LIFO, MANUAL)
            specific_trade_ids: For MANUAL method, specific trade IDs to allocate
            enforce_policy: Whether to enforce attribution policies (Sprint 7B)

        Returns:
            AllocationResult with allocation details and audit trail

        Raises:
            ValueError: If allocation parameters are invalid
            Exception: If allocation fails
        """
        allocation_id = str(uuid4())
        exit_timestamp = exit_timestamp or datetime.now(timezone.utc)
        
        logger.info(
            f"[{allocation_id}] Starting partial exit attribution: "
            f"{symbol} qty={exit_quantity} account={trading_account_id} method={allocation_method} "
            f"enforce_policy={enforce_policy}"
        )

        try:
            # Sprint 7B: Enforce attribution policies if enabled
            if enforce_policy:
                from .exit_attribution_policy import (
                    ExitAttributionPolicyService, ExitContext, AttributionDecision
                )
                
                exit_context = ExitContext(
                    exit_id=allocation_id,
                    trading_account_id=trading_account_id,
                    symbol=symbol,
                    exit_quantity=exit_quantity,
                    exit_price=exit_price,
                    exit_timestamp=exit_timestamp,
                    broker_trade_id=None,
                    order_id=None
                )
                
                policy_service = ExitAttributionPolicyService(self.db)
                policy_result = await policy_service.evaluate_exit_attribution_policy(exit_context)
                
                # Handle policy decisions
                if policy_result.decision == AttributionDecision.BLOCKED:
                    return AllocationResult(
                        allocation_id=allocation_id,
                        total_exit_quantity=exit_quantity,
                        total_allocated_quantity=Decimal('0'),
                        unallocated_quantity=exit_quantity,
                        allocations=[],
                        allocation_method=allocation_method,
                        requires_manual_intervention=True,
                        audit_trail={
                            "policy_blocked": True,
                            "policy_applied": policy_result.policy_applied.value,
                            "block_reason": policy_result.reason,
                            "policy_audit": policy_result.audit_data
                        }
                    )
                
                elif policy_result.decision == AttributionDecision.MANUAL_REQUIRED:
                    return AllocationResult(
                        allocation_id=allocation_id,
                        total_exit_quantity=exit_quantity,
                        total_allocated_quantity=Decimal('0'),
                        unallocated_quantity=exit_quantity,
                        allocations=[],
                        allocation_method=allocation_method,
                        requires_manual_intervention=True,
                        audit_trail={
                            "policy_requires_manual": True,
                            "policy_applied": policy_result.policy_applied.value,
                            "manual_reason": policy_result.manual_intervention_reason,
                            "policy_audit": policy_result.audit_data
                        }
                    )
                
                # For AUTO_APPROVED, use recommended allocation if available
                elif policy_result.decision == AttributionDecision.AUTO_APPROVED and policy_result.recommended_allocation:
                    logger.info(f"[{allocation_id}] Using policy-recommended allocation")
                    # Convert policy recommendation to allocation method
                    if policy_result.recommended_allocation.get("method") == "FIFO_CROSS_STRATEGY":
                        allocation_method = AllocationMethod.FIFO
                
                logger.info(f"[{allocation_id}] Policy check passed: {policy_result.policy_applied}")
            
            # Continue with existing attribution logic
            # Step 1: Get all open positions for this symbol across strategies
            positions = await self._get_open_positions_for_symbol(
                trading_account_id, symbol
            )

            if not positions:
                # No positions found - this might be a new position or already closed
                logger.warning(f"No open positions found for {symbol} in account {trading_account_id}")
                return AllocationResult(
                    allocation_id=allocation_id,
                    total_exit_quantity=exit_quantity,
                    total_allocated_quantity=Decimal('0'),
                    unallocated_quantity=exit_quantity,
                    allocations=[],
                    allocation_method=allocation_method,
                    requires_manual_intervention=True,
                    audit_trail={
                        "error": "no_open_positions",
                        "symbol": symbol,
                        "trading_account_id": trading_account_id,
                        "exit_quantity": str(exit_quantity)
                    }
                )

            # Step 2: Calculate total available quantity
            total_available = sum(Decimal(str(pos['quantity'])) for pos in positions)
            
            # Step 3: Validate allocation is possible
            if abs(exit_quantity) > abs(total_available):
                logger.error(
                    f"Exit quantity {exit_quantity} exceeds available quantity {total_available} "
                    f"for {symbol}"
                )
                return AllocationResult(
                    allocation_id=allocation_id,
                    total_exit_quantity=exit_quantity,
                    total_allocated_quantity=Decimal('0'),
                    unallocated_quantity=exit_quantity,
                    allocations=[],
                    allocation_method=allocation_method,
                    requires_manual_intervention=True,
                    audit_trail={
                        "error": "insufficient_quantity",
                        "requested": str(exit_quantity),
                        "available": str(total_available),
                        "symbol": symbol
                    }
                )

            # Step 4: Perform allocation based on method
            if allocation_method == AllocationMethod.MANUAL:
                allocations = await self._allocate_manual(
                    positions, exit_quantity, specific_trade_ids or []
                )
            elif allocation_method == AllocationMethod.LIFO:
                allocations = await self._allocate_lifo(positions, exit_quantity)
            else:  # FIFO (default)
                allocations = await self._allocate_fifo(positions, exit_quantity)

            # Step 5: Calculate results
            total_allocated = sum(alloc.allocated_quantity for alloc in allocations)
            unallocated = exit_quantity - total_allocated
            requires_manual = abs(unallocated) > Decimal('0.001')  # Allow for rounding

            # Step 6: Create audit trail
            audit_trail = {
                "allocation_id": allocation_id,
                "timestamp": exit_timestamp.isoformat(),
                "trading_account_id": trading_account_id,
                "symbol": symbol,
                "exit_quantity": str(exit_quantity),
                "exit_price": str(exit_price) if exit_price else None,
                "allocation_method": allocation_method.value,
                "total_allocated": str(total_allocated),
                "unallocated": str(unallocated),
                "positions_found": len(positions),
                "allocations_created": len(allocations),
                "requires_manual_intervention": requires_manual,
                "allocations_detail": [
                    {
                        "position_id": alloc.position_id,
                        "strategy_id": alloc.strategy_id,
                        "allocated_quantity": str(alloc.allocated_quantity),
                        "allocation_reason": alloc.allocation_reason
                    }
                    for alloc in allocations
                ]
            }

            # Step 7: Store allocation in database for audit
            await self._store_allocation_audit(allocation_id, audit_trail)

            result = AllocationResult(
                allocation_id=allocation_id,
                total_exit_quantity=exit_quantity,
                total_allocated_quantity=total_allocated,
                unallocated_quantity=unallocated,
                allocations=allocations,
                allocation_method=allocation_method,
                requires_manual_intervention=requires_manual,
                audit_trail=audit_trail
            )

            logger.info(
                f"[{allocation_id}] Attribution complete: "
                f"allocated={total_allocated}, unallocated={unallocated}, "
                f"manual_required={requires_manual}"
            )

            return result

        except Exception as e:
            logger.error(f"[{allocation_id}] Attribution failed: {e}", exc_info=True)
            await self._store_allocation_audit(allocation_id, {
                "error": str(e),
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "trading_account_id": trading_account_id,
                "symbol": symbol,
                "exit_quantity": str(exit_quantity)
            })
            raise

    async def _get_open_positions_for_symbol(
        self,
        trading_account_id: str,
        symbol: str
    ) -> List[Dict[str, Any]]:
        """
        Get all open positions for a symbol across all strategies.

        Args:
            trading_account_id: Trading account ID
            symbol: Symbol to find positions for

        Returns:
            List of position dictionaries with strategy and execution info
        """
        # Get positions without strategy JOIN (since public.strategy doesn't exist)
        # CRITICAL: public.strategy table doesn't exist in order_service database
        result = await self.db.execute(
            text("""
                SELECT 
                    p.id as position_id,
                    p.strategy_id,
                    p.execution_id,
                    p.symbol,
                    p.quantity,
                    p.buy_price,
                    p.sell_price,
                    p.last_price,
                    p.created_at,
                    p.updated_at,
                    -- Get entry trades for FIFO/LIFO ordering
                    (
                        SELECT json_agg(
                            json_build_object(
                                'trade_id', t.id,
                                'quantity', t.quantity,
                                'price', t.price,
                                'timestamp', t.timestamp,
                                'created_at', t.created_at
                            ) ORDER BY t.timestamp, t.created_at
                        )
                        FROM order_service.trades t
                        WHERE t.position_id = p.id
                          AND t.side = CASE 
                                WHEN p.quantity > 0 THEN 'buy'
                                WHEN p.quantity < 0 THEN 'sell'
                                ELSE 'buy'
                              END
                    ) as entry_trades
                FROM order_service.positions p
                WHERE p.trading_account_id = :trading_account_id
                  AND p.symbol = :symbol
                  AND p.is_open = true
                  AND p.quantity != 0
                  AND p.strategy_id IS NOT NULL
                ORDER BY p.created_at ASC  -- Oldest positions first for FIFO
            """),
            {
                "trading_account_id": trading_account_id,
                "symbol": symbol
            }
        )

        positions = []
        for row in result.fetchall():
            position_data = {
                "position_id": row[0],
                "strategy_id": row[1],
                "execution_id": str(row[2]) if row[2] else None,
                "symbol": row[3],
                "quantity": row[4],
                "buy_price": row[5],
                "sell_price": row[6],
                "last_price": row[7],
                "created_at": row[8],
                "updated_at": row[9],
                "entry_trades": row[10] or []
                # strategy_name and is_default removed since we no longer JOIN strategy
            }
            positions.append(position_data)

        logger.debug(
            f"Found {len(positions)} open positions for {symbol} "
            f"in account {trading_account_id}"
        )
        
        return positions

    async def _allocate_fifo(
        self,
        positions: List[Dict[str, Any]],
        exit_quantity: Decimal
    ) -> List[PositionAllocation]:
        """
        Allocate exit using FIFO (First-In-First-Out) method.

        Args:
            positions: List of position dictionaries
            exit_quantity: Quantity to allocate

        Returns:
            List of PositionAllocation objects
        """
        allocations = []
        remaining_to_allocate = abs(exit_quantity)
        
        # Sort positions by creation time (oldest first for FIFO)
        sorted_positions = sorted(positions, key=lambda p: p['created_at'])
        
        for position in sorted_positions:
            if remaining_to_allocate <= Decimal('0.001'):  # Allow for rounding
                break
                
            available_quantity = abs(Decimal(str(position['quantity'])))
            
            # Determine how much to allocate from this position
            allocated_from_position = min(remaining_to_allocate, available_quantity)
            
            # Determine entry price (use weighted average of entry trades if available)
            entry_price = self._calculate_entry_price(position)
            
            allocation = PositionAllocation(
                position_id=position['position_id'],
                strategy_id=position['strategy_id'],
                execution_id=position['execution_id'],
                symbol=position['symbol'],
                allocated_quantity=allocated_from_position if exit_quantity > 0 else -allocated_from_position,
                remaining_quantity=available_quantity - allocated_from_position,
                entry_price=entry_price,
                entry_timestamp=position['created_at'],
                allocation_reason=f"FIFO allocation - oldest position first (created {position['created_at']})"
            )
            
            allocations.append(allocation)
            remaining_to_allocate -= allocated_from_position
            
            logger.debug(
                f"FIFO allocated {allocated_from_position} from position {position['position_id']} "
                f"(strategy {position['strategy_id']}, remaining to allocate: {remaining_to_allocate})"
            )

        return allocations

    async def _allocate_lifo(
        self,
        positions: List[Dict[str, Any]],
        exit_quantity: Decimal
    ) -> List[PositionAllocation]:
        """
        Allocate exit using LIFO (Last-In-First-Out) method.

        Args:
            positions: List of position dictionaries
            exit_quantity: Quantity to allocate

        Returns:
            List of PositionAllocation objects
        """
        allocations = []
        remaining_to_allocate = abs(exit_quantity)
        
        # Sort positions by creation time (newest first for LIFO)
        sorted_positions = sorted(positions, key=lambda p: p['created_at'], reverse=True)
        
        for position in sorted_positions:
            if remaining_to_allocate <= Decimal('0.001'):  # Allow for rounding
                break
                
            available_quantity = abs(Decimal(str(position['quantity'])))
            
            # Determine how much to allocate from this position
            allocated_from_position = min(remaining_to_allocate, available_quantity)
            
            # Determine entry price
            entry_price = self._calculate_entry_price(position)
            
            allocation = PositionAllocation(
                position_id=position['position_id'],
                strategy_id=position['strategy_id'],
                execution_id=position['execution_id'],
                symbol=position['symbol'],
                allocated_quantity=allocated_from_position if exit_quantity > 0 else -allocated_from_position,
                remaining_quantity=available_quantity - allocated_from_position,
                entry_price=entry_price,
                entry_timestamp=position['created_at'],
                allocation_reason=f"LIFO allocation - newest position first (created {position['created_at']})"
            )
            
            allocations.append(allocation)
            remaining_to_allocate -= allocated_from_position
            
            logger.debug(
                f"LIFO allocated {allocated_from_position} from position {position['position_id']} "
                f"(strategy {position['strategy_id']}, remaining to allocate: {remaining_to_allocate})"
            )

        return allocations

    async def _allocate_manual(
        self,
        positions: List[Dict[str, Any]],
        exit_quantity: Decimal,
        specific_trade_ids: List[int]
    ) -> List[PositionAllocation]:
        """
        Allocate exit using manual method with specific trade IDs.

        Args:
            positions: List of position dictionaries
            exit_quantity: Quantity to allocate
            specific_trade_ids: Specific trade IDs to allocate from

        Returns:
            List of PositionAllocation objects

        Raises:
            ValueError: If manual allocation parameters are invalid
        """
        if not specific_trade_ids:
            raise ValueError("Manual allocation requires specific_trade_ids")

        allocations = []
        remaining_to_allocate = abs(exit_quantity)

        # Get trades matching the specific trade IDs
        for position in positions:
            entry_trades = position.get('entry_trades', [])
            
            for trade in entry_trades:
                trade_id = trade.get('trade_id')
                
                # Skip if this trade ID not in the manual selection
                if trade_id not in specific_trade_ids:
                    continue
                
                if remaining_to_allocate <= Decimal('0.001'):
                    break
                
                trade_quantity = abs(Decimal(str(trade.get('quantity', 0))))
                allocated_from_trade = min(remaining_to_allocate, trade_quantity)
                
                # Create allocation for this position/trade
                allocation = PositionAllocation(
                    position_id=position['position_id'],
                    strategy_id=position['strategy_id'],
                    execution_id=position['execution_id'],
                    symbol=position['symbol'],
                    allocated_quantity=allocated_from_trade if exit_quantity > 0 else -allocated_from_trade,
                    remaining_quantity=trade_quantity - allocated_from_trade,
                    entry_price=Decimal(str(trade.get('price', 0))),
                    entry_timestamp=datetime.fromisoformat(trade.get('timestamp')) if trade.get('timestamp') else position['created_at'],
                    allocation_reason=f"Manual allocation - specific trade {trade_id} selected"
                )
                
                allocations.append(allocation)
                remaining_to_allocate -= allocated_from_trade
                
                logger.debug(
                    f"Manual allocated {allocated_from_trade} from trade {trade_id} "
                    f"(position {position['position_id']}, remaining: {remaining_to_allocate})"
                )

            if remaining_to_allocate <= Decimal('0.001'):
                break

        # Validate that we could allocate from the specified trades
        if remaining_to_allocate > Decimal('0.001'):
            allocated_quantity = abs(exit_quantity) - remaining_to_allocate
            logger.warning(
                f"Manual allocation incomplete: allocated {allocated_quantity} of {abs(exit_quantity)} "
                f"from specified trades {specific_trade_ids}"
            )

        return allocations

    def _calculate_entry_price(self, position: Dict[str, Any]) -> Decimal:
        """
        Calculate weighted average entry price for a position.

        Args:
            position: Position dictionary with entry_trades

        Returns:
            Weighted average entry price
        """
        entry_trades = position.get('entry_trades', [])
        
        if not entry_trades:
            # Fallback to position's buy/sell price
            if position['quantity'] > 0:
                return Decimal(str(position['buy_price'] or 0))
            else:
                return Decimal(str(position['sell_price'] or 0))
        
        # Calculate weighted average
        total_value = Decimal('0')
        total_quantity = Decimal('0')
        
        for trade in entry_trades:
            trade_qty = Decimal(str(trade['quantity']))
            trade_price = Decimal(str(trade['price']))
            
            total_value += trade_qty * trade_price
            total_quantity += trade_qty
        
        if total_quantity == 0:
            return Decimal('0')
        
        return total_value / total_quantity

    async def _store_allocation_audit(
        self,
        allocation_id: str,
        audit_data: Dict[str, Any]
    ) -> None:
        """
        Store allocation audit trail in database.

        Args:
            allocation_id: Unique allocation identifier
            audit_data: Audit trail data
        """
        await self.db.execute(
            text("""
                INSERT INTO order_service.partial_exit_allocation_audit (
                    allocation_id,
                    audit_data,
                    created_at
                ) VALUES (
                    :allocation_id,
                    :audit_data::jsonb,
                    NOW()
                )
                ON CONFLICT (allocation_id) DO UPDATE SET
                    audit_data = :audit_data::jsonb,
                    updated_at = NOW()
            """),
            {
                "allocation_id": allocation_id,
                "audit_data": audit_data
            }
        )
        
        await self.db.commit()
        logger.debug(f"Stored allocation audit for {allocation_id}")

    async def get_allocation_audit(
        self,
        allocation_id: str
    ) -> Optional[Dict[str, Any]]:
        """
        Retrieve allocation audit trail.

        Args:
            allocation_id: Allocation ID to retrieve

        Returns:
            Audit data dictionary or None if not found
        """
        result = await self.db.execute(
            text("""
                SELECT audit_data, created_at, updated_at
                FROM order_service.partial_exit_allocation_audit
                WHERE allocation_id = :allocation_id
            """),
            {"allocation_id": allocation_id}
        )
        
        row = result.fetchone()
        if not row:
            return None
            
        return {
            "audit_data": row[0],
            "created_at": row[1].isoformat() if row[1] else None,
            "updated_at": row[2].isoformat() if row[2] else None
        }

    async def list_allocations_for_symbol(
        self,
        trading_account_id: str,
        symbol: str,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        List recent allocation audit records for a symbol.

        Args:
            trading_account_id: Trading account ID
            symbol: Symbol to filter by
            limit: Maximum number of records to return

        Returns:
            List of allocation audit records
        """
        result = await self.db.execute(
            text("""
                SELECT 
                    allocation_id,
                    audit_data,
                    created_at,
                    updated_at
                FROM order_service.partial_exit_allocation_audit
                WHERE (audit_data ->> 'trading_account_id') = :trading_account_id
                  AND (audit_data ->> 'symbol') = :symbol
                ORDER BY created_at DESC
                LIMIT :limit
            """),
            {
                "trading_account_id": trading_account_id,
                "symbol": symbol,
                "limit": limit
            }
        )
        
        return [
            {
                "allocation_id": row[0],
                "audit_data": row[1],
                "created_at": row[2].isoformat() if row[2] else None,
                "updated_at": row[3].isoformat() if row[3] else None
            }
            for row in result.fetchall()
        ]


# Helper function for use outside of class context
async def attribute_partial_exit(
    db: AsyncSession,
    trading_account_id: str,
    symbol: str,
    exit_quantity: Decimal,
    exit_price: Optional[Decimal] = None,
    exit_timestamp: Optional[datetime] = None,
    allocation_method: AllocationMethod = AllocationMethod.FIFO
) -> AllocationResult:
    """
    Convenience function to attribute a partial exit.

    Args:
        db: Database session
        trading_account_id: Trading account ID
        symbol: Symbol that was exited
        exit_quantity: Quantity that was exited
        exit_price: Price at which exit occurred (optional)
        exit_timestamp: When the exit occurred (defaults to now)
        allocation_method: How to allocate across strategies

    Returns:
        AllocationResult with allocation details
    """
    service = PartialExitAttributionService(db)
    return await service.attribute_partial_exit(
        trading_account_id=trading_account_id,
        symbol=symbol,
        exit_quantity=exit_quantity,
        exit_price=exit_price,
        exit_timestamp=exit_timestamp,
        allocation_method=allocation_method
    )
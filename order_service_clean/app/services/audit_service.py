"""
Order Audit Service

Provides audit trail logging for order state transitions.
Ensures SEBI compliance and debugging capability.
"""
import logging
from typing import Optional, Dict, Any, List
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func

from ..models.order_state_history import OrderStateHistory

logger = logging.getLogger(__name__)


class OrderAuditService:
    """
    Service for logging order state changes to audit trail.

    Usage:
        audit_service = OrderAuditService(db, user_id=123)
        await audit_service.log_state_change(
            order_id=456,
            old_status="PENDING",
            new_status="SUBMITTED",
            reason="Order submitted to broker"
        )
    """

    def __init__(self, db: AsyncSession, user_id: Optional[int] = None):
        """
        Initialize audit service.

        Args:
            db: Database session
            user_id: User ID (if this is a user action), None for system actions
        """
        self.db = db
        self.user_id = user_id

    async def log_state_change(
        self,
        order_id: int,
        old_status: Optional[str],
        new_status: str,
        reason: Optional[str] = None,
        changed_by_system: Optional[str] = None,
        broker_response: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> OrderStateHistory:
        """
        Log order state transition to audit trail.

        This method should be called every time an order status changes,
        whether by user action, system action, or broker update.

        Args:
            order_id: Order ID
            old_status: Previous status (None for order creation)
            new_status: New status after transition
            reason: Human-readable reason for change
            changed_by_system: System component that made change (default: "order_service")
            broker_response: Broker error/response message
            metadata: Additional context (JSONB)

        Returns:
            Created OrderStateHistory record

        Example:
            # User cancels order
            await audit_service.log_state_change(
                order_id=123,
                old_status="OPEN",
                new_status="CANCELLED",
                reason="User cancelled order",
                metadata={"ip_address": "192.168.1.1"}
            )

            # Broker rejects order
            await audit_service.log_state_change(
                order_id=123,
                old_status="PENDING",
                new_status="REJECTED",
                reason="Broker rejected order",
                changed_by_system="broker_api",
                broker_response="Insufficient funds",
                metadata={"error_code": "RMS:Margin Exceeded"}
            )
        """
        history = OrderStateHistory(
            order_id=order_id,
            old_status=old_status,
            new_status=new_status,
            changed_by_user_id=self.user_id,
            changed_by_system=changed_by_system or "order_service",
            reason=reason,
            broker_response=broker_response,
            event_metadata=metadata
        )

        self.db.add(history)
        await self.db.flush()  # Get ID but don't commit (let caller manage transaction)

        # Log to application logs as well
        actor = f"user={self.user_id}" if self.user_id else f"system={changed_by_system or 'order_service'}"
        logger.info(
            f"AUDIT: Order {order_id} state change: {old_status} → {new_status} "
            f"({actor}, reason={reason})"
        )

        return history

    async def log_order_creation(
        self,
        order_id: int,
        initial_status: str = "PENDING",
        metadata: Optional[Dict[str, Any]] = None
    ) -> OrderStateHistory:
        """
        Log order creation (convenience method).

        Args:
            order_id: Order ID
            initial_status: Initial status (default PENDING)
            metadata: Additional context

        Returns:
            Created OrderStateHistory record
        """
        return await self.log_state_change(
            order_id=order_id,
            old_status=None,  # No previous state
            new_status=initial_status,
            reason="Order created by user",
            metadata=metadata
        )

    async def log_broker_submission(
        self,
        order_id: int,
        broker_order_id: str,
        metadata: Optional[Dict[str, Any]] = None
    ) -> OrderStateHistory:
        """
        Log successful broker submission (convenience method).

        Args:
            order_id: Order ID
            broker_order_id: Broker's order ID
            metadata: Additional context

        Returns:
            Created OrderStateHistory record
        """
        return await self.log_state_change(
            order_id=order_id,
            old_status="PENDING",
            new_status="SUBMITTED",
            reason="Order submitted to broker",
            metadata={
                **(metadata or {}),
                "broker_order_id": broker_order_id
            }
        )

    async def log_broker_rejection(
        self,
        order_id: int,
        broker_response: str,
        metadata: Optional[Dict[str, Any]] = None
    ) -> OrderStateHistory:
        """
        Log broker rejection (convenience method).

        Args:
            order_id: Order ID
            broker_response: Broker error message
            metadata: Additional context

        Returns:
            Created OrderStateHistory record
        """
        return await self.log_state_change(
            order_id=order_id,
            old_status="PENDING",
            new_status="REJECTED",
            reason="Order rejected by broker",
            changed_by_system="broker_api",
            broker_response=broker_response,
            metadata=metadata
        )

    async def get_order_history(
        self,
        order_id: int,
        limit: int = 100
    ) -> List[OrderStateHistory]:
        """
        Get complete history for an order.

        Args:
            order_id: Order ID
            limit: Maximum records to return (default 100)

        Returns:
            List of state transitions (newest first)

        Example:
            history = await audit_service.get_order_history(123)
            for event in history:
                print(f"{event.changed_at}: {event.old_status} → {event.new_status}")
        """
        result = await self.db.execute(
            select(OrderStateHistory)
            .where(OrderStateHistory.order_id == order_id)
            .order_by(OrderStateHistory.changed_at.desc())
            .limit(limit)
        )
        return list(result.scalars().all())

    async def get_user_actions(
        self,
        user_id: int,
        limit: int = 100
    ) -> List[OrderStateHistory]:
        """
        Get all order actions by a user.

        Useful for compliance audits - "show me all orders cancelled by user X".

        Args:
            user_id: User ID
            limit: Maximum records to return

        Returns:
            List of user actions (newest first)
        """
        result = await self.db.execute(
            select(OrderStateHistory)
            .where(OrderStateHistory.changed_by_user_id == user_id)
            .order_by(OrderStateHistory.changed_at.desc())
            .limit(limit)
        )
        return list(result.scalars().all())

    async def get_system_actions(
        self,
        system_name: str,
        limit: int = 100
    ) -> List[OrderStateHistory]:
        """
        Get all actions by a system component.

        Useful for debugging - "show me all reconciliation corrections".

        Args:
            system_name: System component name (e.g., "reconciliation_worker")
            limit: Maximum records to return

        Returns:
            List of system actions (newest first)
        """
        result = await self.db.execute(
            select(OrderStateHistory)
            .where(OrderStateHistory.changed_by_system == system_name)
            .order_by(OrderStateHistory.changed_at.desc())
            .limit(limit)
        )
        return list(result.scalars().all())

    async def get_transition_count(
        self,
        old_status: Optional[str] = None,
        new_status: Optional[str] = None
    ) -> int:
        """
        Get count of specific state transitions.

        Useful for analytics - "how many orders were cancelled today?".

        Args:
            old_status: Filter by old status (optional)
            new_status: Filter by new status (optional)

        Returns:
            Count of matching transitions

        Example:
            # Count all cancellations
            count = await audit_service.get_transition_count(new_status="CANCELLED")

            # Count all successful submissions
            count = await audit_service.get_transition_count(
                old_status="PENDING",
                new_status="SUBMITTED"
            )
        """
        query = select(func.count(OrderStateHistory.id))

        if old_status:
            query = query.where(OrderStateHistory.old_status == old_status)

        if new_status:
            query = query.where(OrderStateHistory.new_status == new_status)

        result = await self.db.execute(query)
        return result.scalar_one()

    async def get_recent_failures(
        self,
        hours: int = 24,
        limit: int = 100
    ) -> List[OrderStateHistory]:
        """
        Get recent failed order attempts.

        Useful for monitoring - "show me orders that failed in last 24 hours".

        Args:
            hours: Number of hours to look back (default 24)
            limit: Maximum records to return

        Returns:
            List of failed order attempts (newest first)
        """
        from datetime import datetime, timedelta

        cutoff = datetime.utcnow() - timedelta(hours=hours)

        result = await self.db.execute(
            select(OrderStateHistory)
            .where(
                OrderStateHistory.new_status.in_(['REJECTED', 'FAILED']),
                OrderStateHistory.changed_at >= cutoff
            )
            .order_by(OrderStateHistory.changed_at.desc())
            .limit(limit)
        )
        return list(result.scalars().all())

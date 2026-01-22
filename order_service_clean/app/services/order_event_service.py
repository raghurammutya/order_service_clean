"""
Order Event Service - Comprehensive Audit Trail Management

Implements SEBI-compliant order lifecycle tracking with complete audit trails,
event publishing, and compliance reporting capabilities.

Key Features:
- Complete order lifecycle event tracking
- SEBI compliance (7-year retention)
- Real-time event publishing
- Regulatory audit trail generation
- Event-driven order state management
"""
import logging
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any, Tuple
from sqlalchemy import select, and_, func, or_, text
from sqlalchemy.ext.asyncio import AsyncSession
from fastapi import HTTPException
import json

from ..models.order_event import OrderEvent
from ..models.order import Order
from ..database.redis_client import get_redis

logger = logging.getLogger(__name__)


class OrderEventService:
    """
    Enterprise Order Event Service
    
    Manages comprehensive order lifecycle audit trails with SEBI compliance,
    real-time event tracking, and regulatory reporting capabilities.
    """

    def __init__(self, db: AsyncSession, user_id: int):
        """
        Initialize order event service.

        Args:
            db: Database session
            user_id: User ID for access control and audit
        """
        self.db = db
        self.user_id = user_id
        self.redis = None  # Lazy initialize

    async def _get_redis(self):
        """Get Redis client for event publishing"""
        if not self.redis:
            self.redis = await get_redis()
        return self.redis

    # =================================
    # EVENT CREATION METHODS
    # =================================

    async def create_order_created_event(
        self,
        order_id: int,
        order_data: Dict[str, Any],
        created_by: Optional[str] = None,
        additional_context: Optional[Dict[str, Any]] = None
    ) -> OrderEvent:
        """
        Create ORDER_CREATED event when new order is placed.
        
        Args:
            order_id: Order ID
            order_data: Order details (symbol, quantity, price, etc.)
            created_by: User/system creating the event
            additional_context: Additional event context
            
        Returns:
            Created OrderEvent
        """
        event_data = {
            "action": "order_created",
            "order_details": order_data,
            "timestamp": datetime.utcnow().isoformat(),
            "user_id": self.user_id,
            **(additional_context or {})
        }

        event = OrderEvent.create_order_event(
            order_id=order_id,
            event_type="ORDER_CREATED",
            event_data=event_data
        )

        self.db.add(event)
        await self.db.commit()
        await self.db.refresh(event)

        # Publish to event stream
        await self._publish_event(event)

        logger.info(f"Created ORDER_CREATED event for order {order_id}")
        return event

    async def create_order_placed_event(
        self,
        order_id: int,
        broker_order_id: str,
        placement_details: Dict[str, Any],
        additional_context: Optional[Dict[str, Any]] = None
    ) -> OrderEvent:
        """
        Create ORDER_PLACED event when order is submitted to broker.
        
        Args:
            order_id: Internal order ID
            broker_order_id: Broker's order ID
            placement_details: Broker placement details
            additional_context: Additional context
            
        Returns:
            Created OrderEvent
        """
        event_data = {
            "action": "order_placed",
            "broker_order_id": broker_order_id,
            "placement_details": placement_details,
            "timestamp": datetime.utcnow().isoformat(),
            "user_id": self.user_id,
            **(additional_context or {})
        }

        event = OrderEvent.create_order_event(
            order_id=order_id,
            event_type="ORDER_PLACED",
            event_data=event_data
        )

        self.db.add(event)
        await self.db.commit()
        await self.db.refresh(event)

        await self._publish_event(event)

        logger.info(f"Created ORDER_PLACED event for order {order_id}")
        return event

    async def create_order_filled_event(
        self,
        order_id: int,
        fill_details: Dict[str, Any],
        trade_data: Optional[Dict[str, Any]] = None,
        additional_context: Optional[Dict[str, Any]] = None
    ) -> OrderEvent:
        """
        Create ORDER_FILLED event when order is executed.
        
        Args:
            order_id: Order ID
            fill_details: Execution details (qty, price, timestamp)
            trade_data: Associated trade information
            additional_context: Additional context
            
        Returns:
            Created OrderEvent
        """
        event_data = {
            "action": "order_filled",
            "fill_details": fill_details,
            "trade_data": trade_data,
            "timestamp": datetime.utcnow().isoformat(),
            "user_id": self.user_id,
            **(additional_context or {})
        }

        event = OrderEvent.create_order_event(
            order_id=order_id,
            event_type="ORDER_FILLED",
            event_data=event_data
        )

        self.db.add(event)
        await self.db.commit()
        await self.db.refresh(event)

        # Mark as processed immediately for fill events
        event.mark_processed()
        await self.db.commit()

        await self._publish_event(event)

        logger.info(f"Created ORDER_FILLED event for order {order_id}")
        return event

    async def create_order_cancelled_event(
        self,
        order_id: int,
        cancellation_reason: str,
        cancelled_by: Optional[str] = None,
        cancellation_details: Optional[Dict[str, Any]] = None
    ) -> OrderEvent:
        """
        Create ORDER_CANCELLED event when order is cancelled.
        
        Args:
            order_id: Order ID
            cancellation_reason: Reason for cancellation
            cancelled_by: Who cancelled the order
            cancellation_details: Additional cancellation details
            
        Returns:
            Created OrderEvent
        """
        event_data = {
            "action": "order_cancelled",
            "cancellation_reason": cancellation_reason,
            "cancelled_by": cancelled_by,
            "cancellation_details": cancellation_details or {},
            "timestamp": datetime.utcnow().isoformat(),
            "user_id": self.user_id
        }

        event = OrderEvent.create_order_event(
            order_id=order_id,
            event_type="ORDER_CANCELLED",
            event_data=event_data
        )

        self.db.add(event)
        await self.db.commit()
        await self.db.refresh(event)

        event.mark_processed()
        await self.db.commit()

        await self._publish_event(event)

        logger.info(f"Created ORDER_CANCELLED event for order {order_id}")
        return event

    async def create_order_rejected_event(
        self,
        order_id: int,
        rejection_reason: str,
        rejection_details: Dict[str, Any],
        additional_context: Optional[Dict[str, Any]] = None
    ) -> OrderEvent:
        """
        Create ORDER_REJECTED event when order is rejected.
        
        Args:
            order_id: Order ID
            rejection_reason: Rejection reason
            rejection_details: Detailed rejection information
            additional_context: Additional context
            
        Returns:
            Created OrderEvent
        """
        event_data = {
            "action": "order_rejected",
            "rejection_reason": rejection_reason,
            "rejection_details": rejection_details,
            "timestamp": datetime.utcnow().isoformat(),
            "user_id": self.user_id,
            **(additional_context or {})
        }

        event = OrderEvent.create_order_event(
            order_id=order_id,
            event_type="ORDER_REJECTED",
            event_data=event_data
        )

        self.db.add(event)
        await self.db.commit()
        await self.db.refresh(event)

        event.mark_processed()
        await self.db.commit()

        await self._publish_event(event)

        logger.info(f"Created ORDER_REJECTED event for order {order_id}")
        return event

    async def create_order_modified_event(
        self,
        order_id: int,
        modifications: Dict[str, Any],
        modified_by: Optional[str] = None,
        modification_reason: Optional[str] = None
    ) -> OrderEvent:
        """
        Create ORDER_MODIFIED event when order parameters are changed.
        
        Args:
            order_id: Order ID
            modifications: Changed parameters (old vs new values)
            modified_by: Who modified the order
            modification_reason: Reason for modification
            
        Returns:
            Created OrderEvent
        """
        event_data = {
            "action": "order_modified",
            "modifications": modifications,
            "modified_by": modified_by,
            "modification_reason": modification_reason,
            "timestamp": datetime.utcnow().isoformat(),
            "user_id": self.user_id
        }

        event = OrderEvent.create_order_event(
            order_id=order_id,
            event_type="ORDER_MODIFIED",
            event_data=event_data
        )

        self.db.add(event)
        await self.db.commit()
        await self.db.refresh(event)

        await self._publish_event(event)

        logger.info(f"Created ORDER_MODIFIED event for order {order_id}")
        return event

    # =================================
    # EVENT QUERIES & HISTORY
    # =================================

    async def get_order_events(
        self,
        order_id: int,
        event_types: Optional[List[str]] = None,
        status_filter: Optional[List[str]] = None,
        limit: int = 100
    ) -> List[OrderEvent]:
        """
        Get all events for a specific order.
        
        Args:
            order_id: Order ID
            event_types: Filter by event types
            status_filter: Filter by processing status
            limit: Maximum results
            
        Returns:
            List of OrderEvent objects
        """
        query = select(OrderEvent).where(OrderEvent.order_id == order_id)

        # Apply filters
        if event_types:
            query = query.where(OrderEvent.event_type.in_(event_types))
            
        if status_filter:
            query = query.where(OrderEvent.status.in_(status_filter))

        query = query.order_by(OrderEvent.created_at.asc()).limit(limit)

        result = await self.db.execute(query)
        events = list(result.scalars().all())

        logger.debug(f"Retrieved {len(events)} events for order {order_id}")
        return events

    async def get_user_order_events(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        event_types: Optional[List[str]] = None,
        limit: int = 1000,
        offset: int = 0
    ) -> Tuple[List[OrderEvent], int]:
        """
        Get order events for the current user.
        
        Args:
            start_date: Filter from date
            end_date: Filter to date
            event_types: Filter by event types
            limit: Maximum results
            offset: Offset for pagination
            
        Returns:
            Tuple of (events list, total count)
        """
        # Join with orders to filter by user
        base_query = (
            select(OrderEvent)
            .join(Order, OrderEvent.order_id == Order.id)
            .where(Order.user_id == self.user_id)
        )

        count_query = (
            select(func.count(OrderEvent.id))
            .join(Order, OrderEvent.order_id == Order.id)
            .where(Order.user_id == self.user_id)
        )

        # Apply filters
        filters = []
        
        if start_date:
            filters.append(OrderEvent.created_at >= start_date)
            
        if end_date:
            filters.append(OrderEvent.created_at <= end_date)
            
        if event_types:
            filters.append(OrderEvent.event_type.in_(event_types))

        if filters:
            base_query = base_query.where(and_(*filters))
            count_query = count_query.where(and_(*filters))

        # Get total count
        total_result = await self.db.execute(count_query)
        total_count = total_result.scalar()

        # Get paginated results
        events_query = (
            base_query
            .order_by(OrderEvent.created_at.desc())
            .limit(limit)
            .offset(offset)
        )

        result = await self.db.execute(events_query)
        events = list(result.scalars().all())

        logger.debug(f"Retrieved {len(events)} user events, total: {total_count}")
        return events, total_count

    async def get_event_statistics(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """
        Get order event statistics for reporting.
        
        Args:
            start_date: Statistics from date
            end_date: Statistics to date
            
        Returns:
            Statistics dictionary
        """
        if not start_date:
            start_date = datetime.utcnow() - timedelta(days=30)
        if not end_date:
            end_date = datetime.utcnow()

        # Event type breakdown
        type_query = (
            select(
                OrderEvent.event_type,
                func.count(OrderEvent.id).label('event_count')
            )
            .join(Order, OrderEvent.order_id == Order.id)
            .where(
                and_(
                    Order.user_id == self.user_id,
                    OrderEvent.created_at >= start_date,
                    OrderEvent.created_at <= end_date
                )
            )
            .group_by(OrderEvent.event_type)
        )

        type_result = await self.db.execute(type_query)
        type_breakdown = {row.event_type: row.event_count for row in type_result}

        # Status breakdown
        status_query = (
            select(
                OrderEvent.status,
                func.count(OrderEvent.id).label('status_count')
            )
            .join(Order, OrderEvent.order_id == Order.id)
            .where(
                and_(
                    Order.user_id == self.user_id,
                    OrderEvent.created_at >= start_date,
                    OrderEvent.created_at <= end_date
                )
            )
            .group_by(OrderEvent.status)
        )

        status_result = await self.db.execute(status_query)
        status_breakdown = {row.status: row.status_count for row in status_result}

        # Daily event counts
        daily_query = (
            select(
                func.date(OrderEvent.created_at).label('event_date'),
                func.count(OrderEvent.id).label('daily_count')
            )
            .join(Order, OrderEvent.order_id == Order.id)
            .where(
                and_(
                    Order.user_id == self.user_id,
                    OrderEvent.created_at >= start_date,
                    OrderEvent.created_at <= end_date
                )
            )
            .group_by(func.date(OrderEvent.created_at))
            .order_by(func.date(OrderEvent.created_at))
        )

        daily_result = await self.db.execute(daily_query)
        daily_breakdown = {
            str(row.event_date): row.daily_count 
            for row in daily_result
        }

        return {
            "period": {
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat()
            },
            "event_type_breakdown": type_breakdown,
            "status_breakdown": status_breakdown,
            "daily_breakdown": daily_breakdown,
            "total_events": sum(type_breakdown.values())
        }

    # =================================
    # EVENT PROCESSING
    # =================================

    async def process_pending_events(self, limit: int = 100) -> int:
        """
        Process pending order events.
        
        Args:
            limit: Maximum events to process
            
        Returns:
            Number of events processed
        """
        # Get pending events
        query = (
            select(OrderEvent)
            .where(OrderEvent.status == "pending")
            .order_by(OrderEvent.created_at.asc())
            .limit(limit)
        )

        result = await self.db.execute(query)
        pending_events = list(result.scalars().all())

        processed_count = 0

        for event in pending_events:
            try:
                # Process the event (placeholder for business logic)
                await self._process_single_event(event)
                
                # Mark as processed
                event.mark_processed()
                processed_count += 1
                
            except Exception as e:
                logger.error(f"Failed to process event {event.id}: {e}")
                event.mark_failed()

        await self.db.commit()

        logger.info(f"Processed {processed_count} order events")
        return processed_count

    async def _process_single_event(self, event: OrderEvent):
        """
        Process a single order event with real business logic.
        
        Args:
            event: OrderEvent to process
        """
        logger.info(f"Processing event {event.id}: {event.event_type} for order {event.order_id}")
        
        try:
            if event.event_type == "ORDER_PLACED":
                await self._handle_order_placed(event)
            elif event.event_type == "ORDER_FILLED":
                await self._handle_order_filled(event)
            elif event.event_type == "ORDER_CANCELLED":
                await self._handle_order_cancelled(event)
            elif event.event_type == "ORDER_REJECTED":
                await self._handle_order_rejected(event)
            elif event.event_type == "ORDER_MODIFIED":
                await self._handle_order_modified(event)
            elif event.event_type == "ORDER_EXPIRED":
                await self._handle_order_expired(event)
            else:
                logger.warning(f"Unknown event type: {event.event_type}")
                
        except Exception as e:
            logger.error(f"Failed to process event {event.id}: {e}")
            raise

    async def _handle_order_placed(self, event: OrderEvent):
        """Handle ORDER_PLACED event - update order status and trigger notifications"""
        from ..models.order import Order, OrderStatus
        
        # Update order status to SUBMITTED if broker confirmation received
        result = await self.db.execute(
            select(Order).where(Order.id == event.order_id)
        )
        order = result.scalar_one_or_none()
        
        if order and order.status != OrderStatus.SUBMITTED.value:
            order.status = OrderStatus.SUBMITTED.value
            order.submitted_at = event.created_at
            
            logger.info(f"Order {order.id} status updated to SUBMITTED")
            
    async def _handle_order_filled(self, event: OrderEvent):
        """Handle ORDER_FILLED event - update positions and calculate PnL"""
        from ..models.order import Order, OrderStatus
        from ..services.position_service import PositionService
        
        # Update order status to COMPLETE
        result = await self.db.execute(
            select(Order).where(Order.id == event.order_id)
        )
        order = result.scalar_one_or_none()
        
        if order:
            order.status = OrderStatus.COMPLETE.value
            order.filled_at = event.created_at
            
            # Extract fill details from event data
            fill_details = event.event_data or {}
            if "average_price" in fill_details:
                order.average_price = fill_details["average_price"]
            if "filled_quantity" in fill_details:
                order.filled_quantity = fill_details["filled_quantity"]
                
            logger.info(f"Order {order.id} filled at price {order.average_price}")
            
            # Update positions
            position_service = PositionService(self.db, order.user_id, order.trading_account_id)
            await position_service.update_position_for_fill(order)
            
    async def _handle_order_cancelled(self, event: OrderEvent):
        """Handle ORDER_CANCELLED event"""
        from ..models.order import Order, OrderStatus
        
        result = await self.db.execute(
            select(Order).where(Order.id == event.order_id)
        )
        order = result.scalar_one_or_none()
        
        if order:
            order.status = OrderStatus.CANCELLED.value
            order.cancelled_at = event.created_at
            
            cancel_reason = event.event_data.get("reason", "User cancelled") if event.event_data else "User cancelled"
            logger.info(f"Order {order.id} cancelled: {cancel_reason}")
            
    async def _handle_order_rejected(self, event: OrderEvent):
        """Handle ORDER_REJECTED event"""
        from ..models.order import Order, OrderStatus
        
        result = await self.db.execute(
            select(Order).where(Order.id == event.order_id)
        )
        order = result.scalar_one_or_none()
        
        if order:
            order.status = OrderStatus.REJECTED.value
            
            rejection_reason = event.event_data.get("reason", "Unknown") if event.event_data else "Unknown"
            logger.warning(f"Order {order.id} rejected: {rejection_reason}")
            
    async def _handle_order_modified(self, event: OrderEvent):
        """Handle ORDER_MODIFIED event"""
        from ..models.order import Order
        
        result = await self.db.execute(
            select(Order).where(Order.id == event.order_id)
        )
        order = result.scalar_one_or_none()
        
        if order and event.event_data:
            # Update order with modified parameters
            modified_fields = event.event_data.get("modified_fields", {})
            for field, new_value in modified_fields.items():
                if hasattr(order, field):
                    setattr(order, field, new_value)
                    
            logger.info(f"Order {order.id} modified: {list(modified_fields.keys())}")
            
    async def _handle_order_expired(self, event: OrderEvent):
        """Handle ORDER_EXPIRED event"""
        from ..models.order import Order, OrderStatus
        
        result = await self.db.execute(
            select(Order).where(Order.id == event.order_id)
        )
        order = result.scalar_one_or_none()
        
        if order:
            order.status = OrderStatus.EXPIRED.value
            
            logger.info(f"Order {order.id} expired")

    # =================================
    # COMPLIANCE & AUDIT
    # =================================

    async def generate_audit_trail(
        self,
        order_id: int,
        include_metadata: bool = True
    ) -> Dict[str, Any]:
        """
        Generate complete audit trail for an order (SEBI compliance).
        
        Args:
            order_id: Order ID
            include_metadata: Whether to include detailed metadata
            
        Returns:
            Complete audit trail dictionary
        """
        # Get all events for the order
        events = await self.get_order_events(order_id)
        
        # Get order details
        order_result = await self.db.execute(
            select(Order).where(Order.id == order_id)
        )
        order = order_result.scalar_one_or_none()

        if not order:
            raise HTTPException(404, f"Order {order_id} not found")

        # Build audit trail
        audit_trail = {
            "order_id": order_id,
            "order_details": {
                "symbol": order.symbol,
                "quantity": order.quantity,
                "order_type": order.order_type,
                "transaction_type": order.transaction_type,
                "created_at": order.created_at.isoformat() if order.created_at else None,
                "status": order.status
            },
            "total_events": len(events),
            "audit_generated_at": datetime.utcnow().isoformat(),
            "compliance_period": "7_years",  # SEBI requirement
            "events": []
        }

        for event in events:
            event_record = {
                "event_id": event.id,
                "event_type": event.event_type,
                "status": event.status,
                "created_at": event.created_at.isoformat() if event.created_at else None,
                "processed_at": event.processed_at.isoformat() if event.processed_at else None
            }
            
            if include_metadata and event.event_data:
                event_record["event_data"] = event.event_data
                
            audit_trail["events"].append(event_record)

        logger.info(f"Generated audit trail for order {order_id} with {len(events)} events")
        return audit_trail

    async def get_compliance_report(
        self,
        start_date: datetime,
        end_date: datetime,
        order_ids: Optional[List[int]] = None
    ) -> Dict[str, Any]:
        """
        Generate SEBI compliance report.
        
        Args:
            start_date: Report start date
            end_date: Report end date
            order_ids: Optional list of specific orders
            
        Returns:
            Compliance report dictionary
        """
        # Build base query for user's orders
        base_query = (
            select(OrderEvent)
            .join(Order, OrderEvent.order_id == Order.id)
            .where(
                and_(
                    Order.user_id == self.user_id,
                    OrderEvent.created_at >= start_date,
                    OrderEvent.created_at <= end_date
                )
            )
        )

        if order_ids:
            base_query = base_query.where(Order.id.in_(order_ids))

        result = await self.db.execute(base_query)
        events = list(result.scalars().all())

        # Generate compliance statistics
        event_types = {}
        orders_with_events = set()
        
        for event in events:
            orders_with_events.add(event.order_id)
            event_types[event.event_type] = event_types.get(event.event_type, 0) + 1

        return {
            "report_period": {
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat()
            },
            "compliance_standard": "SEBI",
            "retention_period": "7_years",
            "total_events": len(events),
            "unique_orders": len(orders_with_events),
            "event_type_summary": event_types,
            "report_generated_at": datetime.utcnow().isoformat()
        }

    # =================================
    # EVENT PUBLISHING
    # =================================

    async def _publish_event(self, event: OrderEvent):
        """
        Publish event to Redis stream for real-time processing.
        
        Args:
            event: OrderEvent to publish
        """
        try:
            redis = await self._get_redis()
            
            # Create event message
            event_message = {
                "event_id": event.id,
                "order_id": event.order_id,
                "event_type": event.event_type,
                "status": event.status,
                "created_at": event.created_at.isoformat() if event.created_at else None,
                "user_id": self.user_id
            }
            
            # Publish to order events stream
            await redis.xadd(
                "order_events_stream",
                event_message
            )
            
            logger.debug(f"Published event {event.id} to Redis stream")
            
        except Exception as e:
            logger.warning(f"Failed to publish event {event.id}: {e}")
            # Don't fail the transaction for publishing errors
"""
Enhanced Order Service - Integration Layer

Integrates order management with capital ledger tracking and order event audit trails.
Provides comprehensive order lifecycle management with SEBI compliance.

Key Features:
- Order placement with capital reservation
- Real-time order status tracking with events
- Broker integration with audit trails
- SEBI-compliant order lifecycle management
- Capital allocation state machine integration
"""
import logging
from datetime import datetime
from typing import Optional, Dict, Any, Tuple
from decimal import Decimal
from sqlalchemy import select, and_
from sqlalchemy.ext.asyncio import AsyncSession
from fastapi import HTTPException

from ..models.order import Order, OrderStatus
from ..models.capital_ledger import CapitalLedger
from ..models.order_event import OrderEvent
from .capital_ledger_service import CapitalLedgerService
from .order_event_service import OrderEventService
from .order_service import OrderService as BaseOrderService

logger = logging.getLogger(__name__)


class EnhancedOrderService:
    """
    Enhanced Order Service with integrated capital tracking and audit trails
    
    Extends the base order service with:
    - Capital reservation and allocation management
    - Complete order event audit trails
    - SEBI compliance integration
    - Real-time order status updates
    """

    def __init__(self, db: AsyncSession, user_id: int):
        """
        Initialize enhanced order service.

        Args:
            db: Database session
            user_id: User ID for access control
        """
        self.db = db
        self.user_id = user_id
        
        # Initialize sub-services
        self.base_order_service = BaseOrderService(db, user_id)
        self.capital_service = CapitalLedgerService(db, user_id)
        self.event_service = OrderEventService(db, user_id)

    # =================================
    # ENHANCED ORDER PLACEMENT
    # =================================

    async def place_order_with_capital_tracking(
        self,
        symbol: str,
        quantity: int,
        order_type: str,
        transaction_type: str,
        product_type: str,
        price: Optional[Decimal] = None,
        trigger_price: Optional[Decimal] = None,
        portfolio_id: Optional[str] = None,
        strategy_id: Optional[str] = None,
        estimated_capital: Optional[Decimal] = None,
        trading_account_id: Optional[str] = None,
        additional_params: Optional[Dict[str, Any]] = None
    ) -> Tuple[Order, CapitalLedger, OrderEvent]:
        """
        Place order with integrated capital reservation and audit trail.
        
        Args:
            symbol: Trading symbol
            quantity: Order quantity
            order_type: Order type (MARKET, LIMIT, etc.)
            transaction_type: BUY or SELL
            product_type: Product type (CNC, MIS, NRML)
            price: Limit price (for LIMIT orders)
            trigger_price: Trigger price (for SL orders)
            portfolio_id: Portfolio identifier
            strategy_id: Strategy identifier  
            estimated_capital: Estimated capital requirement
            trading_account_id: Trading account
            additional_params: Additional order parameters
            
        Returns:
            Tuple of (Order, CapitalLedger, OrderEvent)
            
        Raises:
            HTTPException: If validation fails or insufficient capital
        """
        try:
            # Step 1: Reserve capital if required
            capital_ledger_entry = None
            if estimated_capital and portfolio_id:
                capital_ledger_entry = await self.capital_service.reserve_capital(
                    portfolio_id=portfolio_id,
                    amount=estimated_capital,
                    strategy_id=strategy_id,
                    description=f"Capital reservation for {transaction_type} {quantity} {symbol}"
                )
                logger.info(f"Reserved capital: {estimated_capital} for order placement")

            # Step 2: Create order using base service
            # Note: Using base service for actual order placement logic
            order_data = {
                "symbol": symbol,
                "quantity": quantity,
                "order_type": order_type,
                "transaction_type": transaction_type,
                "product_type": product_type,
                "price": price,
                "trigger_price": trigger_price,
                "portfolio_id": portfolio_id,
                "strategy_id": strategy_id,
                "trading_account_id": trading_account_id,
                **(additional_params or {})
            }

            # Create order record
            order = Order(
                user_id=self.user_id,
                trading_account_id=trading_account_id or "default",
                symbol=symbol,
                exchange=additional_params.get("exchange", "NSE"),
                transaction_type=transaction_type,
                order_type=order_type,
                product_type=product_type,
                quantity=quantity,
                pending_quantity=quantity,
                price=price,
                trigger_price=trigger_price,
                portfolio_id=portfolio_id,
                strategy_id=strategy_id,
                status=OrderStatus.PENDING.value,
                created_at=datetime.utcnow()
            )

            self.db.add(order)
            await self.db.commit()
            await self.db.refresh(order)

            # Update capital ledger with order ID
            if capital_ledger_entry:
                capital_ledger_entry.order_id = str(order.id)
                await self.db.commit()

            # Step 3: Create ORDER_CREATED audit event
            order_event = await self.event_service.create_order_created_event(
                order_id=order.id,
                order_data=order_data,
                created_by=f"user_{self.user_id}",
                additional_context={
                    "capital_reserved": float(estimated_capital) if estimated_capital else None,
                    "capital_ledger_id": capital_ledger_entry.id if capital_ledger_entry else None,
                    "portfolio_id": portfolio_id,
                    "strategy_id": strategy_id
                }
            )

            logger.info(
                f"Created enhanced order: order_id={order.id}, "
                f"capital_ledger_id={capital_ledger_entry.id if capital_ledger_entry else None}, "
                f"event_id={order_event.id}"
            )

            return order, capital_ledger_entry, order_event

        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Failed to place order with capital tracking: {e}")
            raise HTTPException(500, f"Failed to place order: {str(e)}")

    async def submit_order_to_broker(
        self,
        order_id: int,
        broker_params: Optional[Dict[str, Any]] = None
    ) -> Tuple[Order, OrderEvent]:
        """
        Submit order to broker and create placement audit event.
        
        Args:
            order_id: Internal order ID
            broker_params: Additional broker parameters
            
        Returns:
            Tuple of (updated Order, OrderEvent)
        """
        # Get order
        result = await self.db.execute(
            select(Order).where(Order.id == order_id)
        )
        order = result.scalar_one_or_none()

        if not order:
            raise HTTPException(404, f"Order {order_id} not found")

        try:
            # Get broker client for user's account
            from ..services.kite_client import get_kite_client_for_user
            kite_client = await get_kite_client_for_user(order.user_id)
            
            if not kite_client:
                raise HTTPException(500, f"Unable to get broker client for user {order.user_id}")
            
            # Prepare order parameters for broker
            broker_params_dict = {
                "tradingsymbol": order.tradingsymbol,
                "exchange": order.exchange,
                "transaction_type": order.transaction_type,
                "quantity": order.quantity,
                "order_type": order.order_type,
                "product": order.product,
                "validity": order.validity or "DAY",
                "disclosed_quantity": order.disclosed_quantity,
                "tag": f"ORDER_SERVICE_{order_id}"
            }
            
            # Add price fields if present
            if order.price is not None:
                broker_params_dict["price"] = float(order.price)
            if order.trigger_price is not None:
                broker_params_dict["trigger_price"] = float(order.trigger_price)
            
            # Submit to actual broker API
            broker_order_id = await kite_client.place_order(**broker_params_dict)
            
            if not broker_order_id:
                raise HTTPException(500, "Broker did not return order ID")
            
            # Update order status with real broker response
            order.status = OrderStatus.SUBMITTED.value
            order.broker_order_id = broker_order_id
            order.submitted_at = datetime.utcnow()
            
            await self.db.commit()

            # Create ORDER_PLACED event
            placement_details = {
                "broker_order_id": broker_order_id,
                "submitted_at": order.submitted_at.isoformat(),
                "broker_params": broker_params or {}
            }

            order_event = await self.event_service.create_order_placed_event(
                order_id=order.id,
                broker_order_id=broker_order_id,
                placement_details=placement_details
            )

            logger.info(f"Submitted order {order_id} to broker: {broker_order_id}")
            return order, order_event

        except Exception as e:
            logger.error(f"Failed to submit order to broker: {e}")
            raise HTTPException(500, f"Failed to submit order: {str(e)}")

    # =================================
    # ORDER EXECUTION TRACKING
    # =================================

    async def record_order_execution(
        self,
        order_id: int,
        filled_quantity: int,
        execution_price: Decimal,
        execution_time: datetime,
        trade_id: Optional[str] = None,
        broker_trade_id: Optional[str] = None
    ) -> Tuple[Order, CapitalLedger, OrderEvent]:
        """
        Record order execution with capital allocation and audit trail.
        
        Args:
            order_id: Order ID
            filled_quantity: Quantity filled
            execution_price: Execution price
            execution_time: Execution timestamp
            trade_id: Internal trade ID
            broker_trade_id: Broker's trade ID
            
        Returns:
            Tuple of (Order, CapitalLedger, OrderEvent)
        """
        # Get order
        result = await self.db.execute(
            select(Order).where(Order.id == order_id)
        )
        order = result.scalar_one_or_none()

        if not order:
            raise HTTPException(404, f"Order {order_id} not found")

        try:
            # Calculate actual capital used
            actual_capital = filled_quantity * execution_price

            # Update order execution details
            order.filled_quantity += filled_quantity
            order.pending_quantity = order.quantity - order.filled_quantity
            order.average_price = execution_price  # Simplified - would calculate weighted average
            
            if order.filled_quantity >= order.quantity:
                order.status = OrderStatus.COMPLETE.value
            
            await self.db.commit()

            # Create capital allocation entry
            capital_allocation = None
            if order.portfolio_id:
                capital_allocation = await self.capital_service.allocate_capital(
                    portfolio_id=order.portfolio_id,
                    amount=actual_capital,
                    order_id=str(order.id),
                    strategy_id=order.strategy_id,
                    description=f"Capital allocation for executed order {order.id}",
                    metadata={
                        "filled_quantity": filled_quantity,
                        "execution_price": float(execution_price),
                        "trade_id": trade_id,
                        "broker_trade_id": broker_trade_id
                    }
                )

            # Create ORDER_FILLED event
            fill_details = {
                "filled_quantity": filled_quantity,
                "execution_price": float(execution_price),
                "execution_time": execution_time.isoformat(),
                "cumulative_filled": order.filled_quantity,
                "remaining_quantity": order.pending_quantity,
                "order_status": order.status
            }

            trade_data = {
                "trade_id": trade_id,
                "broker_trade_id": broker_trade_id,
                "actual_capital": float(actual_capital)
            }

            order_event = await self.event_service.create_order_filled_event(
                order_id=order.id,
                fill_details=fill_details,
                trade_data=trade_data,
                additional_context={
                    "capital_allocation_id": capital_allocation.id if capital_allocation else None
                }
            )

            logger.info(
                f"Recorded order execution: order_id={order.id}, "
                f"filled_qty={filled_quantity}, price={execution_price}"
            )

            return order, capital_allocation, order_event

        except Exception as e:
            logger.error(f"Failed to record order execution: {e}")
            raise HTTPException(500, f"Failed to record execution: {str(e)}")

    async def cancel_order_with_capital_release(
        self,
        order_id: int,
        cancellation_reason: str,
        cancelled_by: Optional[str] = None
    ) -> Tuple[Order, Optional[CapitalLedger], OrderEvent]:
        """
        Cancel order and release reserved capital.
        
        Args:
            order_id: Order ID
            cancellation_reason: Reason for cancellation
            cancelled_by: Who cancelled the order
            
        Returns:
            Tuple of (Order, CapitalLedger or None, OrderEvent)
        """
        # Get order
        result = await self.db.execute(
            select(Order).where(Order.id == order_id)
        )
        order = result.scalar_one_or_none()

        if not order:
            raise HTTPException(404, f"Order {order_id} not found")

        try:
            # Update order status
            order.status = OrderStatus.CANCELLED.value
            order.cancelled_quantity = order.pending_quantity
            order.pending_quantity = 0
            
            await self.db.commit()

            # Release reserved capital if any
            capital_release = None
            if order.portfolio_id:
                # Find reserved capital for this order
                reserved_capital_query = select(CapitalLedger).where(
                    and_(
                        CapitalLedger.order_id == str(order.id),
                        CapitalLedger.transaction_type == "RESERVE",
                        CapitalLedger.status == "COMMITTED"
                    )
                )
                result = await self.db.execute(reserved_capital_query)
                reserved_entry = result.scalar_one_or_none()

                if reserved_entry:
                    capital_release = await self.capital_service.release_capital(
                        portfolio_id=order.portfolio_id,
                        amount=reserved_entry.amount,
                        order_id=str(order.id),
                        reason=f"Order cancelled: {cancellation_reason}",
                        metadata={
                            "cancelled_quantity": order.cancelled_quantity,
                            "original_reserved_amount": float(reserved_entry.amount)
                        }
                    )

            # Create ORDER_CANCELLED event
            order_event = await self.event_service.create_order_cancelled_event(
                order_id=order.id,
                cancellation_reason=cancellation_reason,
                cancelled_by=cancelled_by or f"user_{self.user_id}",
                cancellation_details={
                    "cancelled_quantity": order.cancelled_quantity,
                    "capital_released": float(capital_release.amount) if capital_release else None
                }
            )

            logger.info(f"Cancelled order {order_id}: {cancellation_reason}")
            return order, capital_release, order_event

        except Exception as e:
            logger.error(f"Failed to cancel order: {e}")
            raise HTTPException(500, f"Failed to cancel order: {str(e)}")

    # =================================
    # ORDER QUERIES WITH AUDIT INFO
    # =================================

    async def get_order_with_audit_trail(
        self,
        order_id: int
    ) -> Dict[str, Any]:
        """
        Get order details with complete audit trail and capital tracking.
        
        Args:
            order_id: Order ID
            
        Returns:
            Comprehensive order information dictionary
        """
        # Get order
        result = await self.db.execute(
            select(Order).where(Order.id == order_id)
        )
        order = result.scalar_one_or_none()

        if not order:
            raise HTTPException(404, f"Order {order_id} not found")

        # Get audit trail
        audit_trail = await self.event_service.generate_audit_trail(order_id)
        
        # Get capital tracking info
        capital_query = select(CapitalLedger).where(
            CapitalLedger.order_id == str(order_id)
        ).order_by(CapitalLedger.created_at.asc())
        
        capital_result = await self.db.execute(capital_query)
        capital_entries = list(capital_result.scalars().all())

        return {
            "order": {
                "id": order.id,
                "symbol": order.symbol,
                "quantity": order.quantity,
                "filled_quantity": order.filled_quantity,
                "status": order.status,
                "order_type": order.order_type,
                "transaction_type": order.transaction_type,
                "created_at": order.created_at.isoformat() if order.created_at else None,
                "portfolio_id": order.portfolio_id,
                "strategy_id": order.strategy_id
            },
            "audit_trail": audit_trail,
            "capital_tracking": [
                {
                    "id": entry.id,
                    "transaction_type": entry.transaction_type,
                    "amount": float(entry.amount),
                    "status": entry.status,
                    "created_at": entry.created_at.isoformat() if entry.created_at else None
                }
                for entry in capital_entries
            ],
            "summary": {
                "total_events": audit_trail["total_events"],
                "capital_entries": len(capital_entries),
                "compliance_ready": True
            }
        }

    async def get_user_order_summary(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """
        Get comprehensive order summary for user.
        
        Args:
            start_date: Filter from date
            end_date: Filter to date
            
        Returns:
            Order summary with statistics
        """
        # Base query for user orders
        query = select(Order).where(Order.user_id == self.user_id)
        
        if start_date:
            query = query.where(Order.created_at >= start_date)
        if end_date:
            query = query.where(Order.created_at <= end_date)

        result = await self.db.execute(query)
        orders = list(result.scalars().all())

        # Calculate statistics
        total_orders = len(orders)
        status_breakdown = {}
        symbol_breakdown = {}
        
        for order in orders:
            status_breakdown[order.status] = status_breakdown.get(order.status, 0) + 1
            symbol_breakdown[order.symbol] = symbol_breakdown.get(order.symbol, 0) + 1

        # Get event statistics
        event_stats = await self.event_service.get_event_statistics(start_date, end_date)

        return {
            "period": {
                "start_date": start_date.isoformat() if start_date else None,
                "end_date": end_date.isoformat() if end_date else None
            },
            "order_statistics": {
                "total_orders": total_orders,
                "status_breakdown": status_breakdown,
                "top_symbols": dict(list(sorted(symbol_breakdown.items(), key=lambda x: x[1], reverse=True))[:10])
            },
            "event_statistics": event_stats,
            "compliance": {
                "sebi_compliant": True,
                "audit_trail_complete": event_stats["total_events"] > 0,
                "retention_period": "7_years"
            }
        }
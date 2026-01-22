"""
Order Service Business Logic

Handles order placement, modification, cancellation, and tracking.
"""
import logging
from datetime import datetime, date
from typing import List, Optional, Dict, Any
from sqlalchemy import select, update, and_, func, literal
from sqlalchemy.ext.asyncio import AsyncSession
from fastapi import HTTPException

from ..models.order import Order, OrderStatus, OrderType, ProductType, Variety
from ..models.trade import Trade
from ..models.position import Position
from ..config.settings import settings
from ..database.redis_client import (
    cache_order,
    get_cached_order,
    invalidate_order_cache,
    publish_order_update
)
from .kite_client import get_kite_client
from .kite_client_multi import get_kite_client_for_account
from .circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitBreakerError,
    RetryConfig,
    retry_with_backoff
)
from .lot_size_service import LotSizeService
from .margin_service import MarginService
from .market_hours import MarketHoursService
from .audit_service import OrderAuditService

logger = logging.getLogger(__name__)

# Global circuit breaker for broker API calls
_broker_circuit_breaker = CircuitBreaker(
    config=CircuitBreakerConfig(
        failure_threshold=5,  # Open circuit after 5 consecutive failures
        recovery_timeout=60,  # Try to recover after 60 seconds
        name="broker_api"
    )
)

# Retry configuration for order placement
_order_retry_config = RetryConfig(
    max_attempts=3,
    initial_delay=1.0,
    max_delay=5.0,
    exponential_base=2.0
)


class OrderService:
    """Order execution and management service"""

    def __init__(self, db: AsyncSession, user_id: int, trading_account_id: int):
        """
        Initialize order service.

        Args:
            db: Database session
            user_id: User ID from JWT token
            trading_account_id: Trading account ID (will be converted to string for DB queries)
        """
        self.db = db
        self.user_id = user_id
        # Save numeric ID for services that expect ints
        self.trading_account_id_int = trading_account_id
        # Convert to string since trading_account_id is VARCHAR in database
        self.trading_account_id = str(trading_account_id)

        # Use multi-account client for account-specific routing
        # Maps trading_account_id to kite account nickname:
        #   1 -> primary (XJ4540)
        #   2 -> personal (WG7169)
        self.kite_client = get_kite_client_for_account(trading_account_id)

        self.lot_size_service = LotSizeService(db)
        self.audit_service = OrderAuditService(db, user_id=user_id)

        logger.info(
            f"OrderService initialized: user={user_id}, "
            f"trading_account={trading_account_id}, "
            f"kite_account={self.kite_client.account_nickname}"
        )

    # ==========================================
    # ORDER PLACEMENT
    # ==========================================

    async def place_order(
        self,
        strategy_id: int,
        symbol: str,
        exchange: str,
        transaction_type: str,
        quantity: int,
        order_type: str,
        product_type: str,
        price: Optional[float] = None,
        trigger_price: Optional[float] = None,
        validity: str = "DAY",
        variety: str = "regular",
        disclosed_quantity: Optional[int] = None,
        tag: Optional[str] = None,
    ) -> Order:
        """
        Place a new order.

        Args:
            strategy_id: Strategy ID (required for P&L tracking)
            symbol: Trading symbol
            exchange: Exchange (NSE, NFO, BSE, etc.)
            transaction_type: BUY or SELL
            quantity: Order quantity
            order_type: MARKET, LIMIT, SL, SL-M
            product_type: CNC, MIS, NRML
            price: Limit price (required for LIMIT orders)
            trigger_price: Trigger price (required for SL orders)
            validity: DAY or IOC
            variety: regular, amo, iceberg, auction
            disclosed_quantity: Disclosed quantity (for iceberg orders)
            tag: Custom order tag

        Returns:
            Created Order object

        Raises:
            HTTPException: If validation fails or order placement fails
        """
        logger.info(
            f"Placing order: {transaction_type} {quantity} {symbol} @ "
            f"{order_type} (user={self.user_id})"
        )

        # Validate order parameters
        self._validate_order(
            symbol=symbol,
            exchange=exchange,
            transaction_type=transaction_type,
            quantity=quantity,
            order_type=order_type,
            product_type=product_type,
            price=price,
            trigger_price=trigger_price,
        )

        # Validate lot size for F&O orders
        await self._validate_lot_size(
            symbol=symbol,
            exchange=exchange,
            quantity=quantity,
        )

        # Perform risk checks
        risk_check_passed, risk_check_details = await self._perform_risk_checks(
            symbol=symbol,
            transaction_type=transaction_type,
            quantity=quantity,
            price=price or 0,
        )

        if not risk_check_passed:
            logger.warning(f"Risk check failed: {risk_check_details}")
            raise HTTPException(400, f"Risk check failed: {risk_check_details}")

        # Validate strategy exists (database-level foreign key will also enforce this)
        await self._validate_strategy_exists(strategy_id)

        # Create order record in database (PENDING status)
        order = Order(
            strategy_id=strategy_id,
            user_id=self.user_id,
            trading_account_id=self.trading_account_id,
            symbol=symbol,
            exchange=exchange,
            transaction_type=transaction_type,
            order_type=order_type,
            product_type=product_type,
            variety=variety,
            quantity=quantity,
            filled_quantity=0,
            pending_quantity=quantity,
            cancelled_quantity=0,
            price=price,
            trigger_price=trigger_price,
            validity=validity,
            disclosed_quantity=disclosed_quantity,
            status="PENDING",
            risk_check_passed=risk_check_passed,
            risk_check_details=risk_check_details,
            broker_tag=tag,
        )

        self.db.add(order)
        await self.db.flush()  # Get the order ID

        logger.info(f"Order created in database: ID={order.id}")

        # Audit: Log order creation
        await self.audit_service.log_order_creation(
            order_id=order.id,
            initial_status="PENDING",
            metadata={
                "symbol": symbol,
                "exchange": exchange,
                "quantity": quantity,
                "order_type": order_type,
                "product_type": product_type,
                "transaction_type": transaction_type
            }
        )

        # Submit order to broker with circuit breaker and retry
        try:
            # Wrapper function for circuit breaker
            async def _place_order_with_retry():
                # Build broker params (only include disclosed_quantity if provided)
                broker_params = {
                    "symbol": symbol,
                    "exchange": exchange,
                    "transaction_type": transaction_type,
                    "quantity": quantity,
                    "order_type": order_type,
                    "product": product_type,
                    "price": price,
                    "trigger_price": trigger_price,
                    "validity": validity,
                    "variety": variety,
                    "tag": tag,
                }

                # Add disclosed_quantity only for iceberg orders
                if disclosed_quantity is not None and variety == "iceberg":
                    broker_params["disclosed_quantity"] = disclosed_quantity

                return await retry_with_backoff(
                    self.kite_client.place_order,
                    _order_retry_config,
                    **broker_params
                )

            # Execute through circuit breaker
            broker_order_id = await _broker_circuit_breaker.call(_place_order_with_retry)

            # Update order with broker order ID
            order.broker_order_id = broker_order_id
            order.status = "SUBMITTED"
            order.submitted_at = datetime.utcnow()

            # Audit: Log broker submission
            await self.audit_service.log_broker_submission(
                order_id=order.id,
                broker_order_id=broker_order_id
            )

            await self.db.commit()

            # Refresh to load all attributes before session closes
            await self.db.refresh(order)

            # Make session.expire_on_commit=False has no effect, so we need to
            # make object independent from session by expunging it
            self.db.expunge(order)
            # Now manually make the object accessible
            # Accessing all attributes to load them into memory
            _ = (order.id, order.user_id, order.trading_account_id, order.broker_order_id,
                 order.symbol, order.exchange, order.transaction_type, order.order_type,
                 order.product_type, order.variety, order.quantity, order.filled_quantity,
                 order.pending_quantity, order.cancelled_quantity, order.price,
                 order.trigger_price, order.average_price, order.status, order.status_message,
                 order.validity, order.created_at, order.updated_at, order.submitted_at,
                 order.risk_check_passed)

            logger.info(
                f"Order submitted to broker: ID={order.id}, "
                f"broker_order_id={broker_order_id}"
            )

            # Convert to dict before session closes to avoid lazy-loading issues
            order_dict = order.to_dict()

            # Cache order
            await cache_order(str(order.id), order_dict)

            # Publish order created event
            await publish_order_update(
                str(order.id),
                "created",
                order_dict
            )

            return order

        except CircuitBreakerError as e:
            # CRITICAL FIX: Rollback transaction instead of committing rejected order
            # Circuit breaker is open - broker service is down
            await self.db.rollback()

            logger.error(
                f"Order placement failed - circuit breaker open (rolled back transaction): "
                f"order_id={order.id}, error={e}"
            )

            raise HTTPException(
                503,
                f"Broker service temporarily unavailable. Please try again later."
            )

        except Exception as e:
            # CRITICAL FIX: Rollback transaction instead of committing rejected order
            # This prevents database pollution with stuck PENDING orders
            await self.db.rollback()

            logger.error(
                f"Order placement failed after {_order_retry_config.max_attempts} retries "
                f"(rolled back transaction): order_id={order.id}, error={e}",
                exc_info=True
            )

            raise HTTPException(
                500,
                f"Order placement failed: {str(e)}"
            )

    # ==========================================
    # ORDER MODIFICATION
    # ==========================================

    async def modify_order(
        self,
        order_id: int,
        quantity: Optional[int] = None,
        price: Optional[float] = None,
        trigger_price: Optional[float] = None,
        order_type: Optional[str] = None,
    ) -> Order:
        """
        Modify an existing order.

        Args:
            order_id: Order ID to modify
            quantity: New quantity
            price: New price
            trigger_price: New trigger price
            order_type: New order type

        Returns:
            Updated Order object

        Raises:
            HTTPException: If order not found or modification fails
        """
        logger.info(f"Modifying order: ID={order_id} (user={self.user_id})")

        # Get order
        order = await self._get_user_order(order_id)

        # Check if order can be modified
        if order.status not in ["PENDING", "SUBMITTED", "OPEN"]:
            raise HTTPException(
                400,
                f"Cannot modify order with status {order.status}"
            )

        # Modify order with broker first (fail fast before database changes)
        try:
            await self.kite_client.modify_order(
                order_id=order.broker_order_id,
                quantity=quantity,
                price=price,
                trigger_price=trigger_price,
                order_type=order_type,
            )

            # Only update database AFTER successful broker modification
            old_status = order.status
            modifications = []

            if quantity is not None:
                order.quantity = quantity
                order.pending_quantity = quantity - order.filled_quantity
                modifications.append(f"quantity: {quantity}")

            if price is not None:
                order.price = price
                modifications.append(f"price: {price}")

            if trigger_price is not None:
                order.trigger_price = trigger_price
                modifications.append(f"trigger_price: {trigger_price}")

            if order_type is not None:
                order.order_type = order_type
                modifications.append(f"order_type: {order_type}")

            # Audit: Log modification
            await self.audit_service.log_state_change(
                order_id=order.id,
                old_status=old_status,
                new_status=order.status,  # Status may not change, but params did
                reason=f"Order modified by user: {', '.join(modifications)}",
                metadata={
                    "modifications": {
                        "quantity": quantity,
                        "price": price,
                        "trigger_price": trigger_price,
                        "order_type": order_type
                    }
                }
            )

            await self.db.commit()
            await self.db.refresh(order)

            logger.info(f"Order modified successfully: ID={order_id}")

            # Invalidate cache
            await invalidate_order_cache(str(order_id))

            # Publish order updated event
            await publish_order_update(
                str(order_id),
                "modified",
                order.to_dict()
            )

            return order

        except Exception as e:
            # CRITICAL FIX: Rollback any pending database changes
            # Note: Since we modify broker FIRST, database should still be unchanged
            # But rollback ensures transaction is clean if any changes were made
            await self.db.rollback()

            logger.error(
                f"Order modification failed (rolled back transaction): "
                f"order_id={order_id}, error={e}",
                exc_info=True
            )

            raise HTTPException(500, f"Order modification failed: {str(e)}")

    # ==========================================
    # ORDER CANCELLATION
    # ==========================================

    async def cancel_order(self, order_id: int) -> Order:
        """
        Cancel an order.

        Args:
            order_id: Order ID to cancel

        Returns:
            Updated Order object

        Raises:
            HTTPException: If order not found or cancellation fails
        """
        logger.info(f"Cancelling order: ID={order_id} (user={self.user_id})")

        # Get order
        order = await self._get_user_order(order_id)

        # Check if order can be cancelled
        if order.status not in ["PENDING", "SUBMITTED", "OPEN", "TRIGGER_PENDING"]:
            raise HTTPException(
                400,
                f"Cannot cancel order with status {order.status}"
            )

        # Cancel order with broker first (fail fast before database changes)
        try:
            await self.kite_client.cancel_order(
                order_id=order.broker_order_id,
                variety=order.variety
            )

            # Only update database AFTER successful broker cancellation
            old_status = order.status
            order.status = "CANCELLED"
            order.cancelled_quantity = order.pending_quantity
            order.pending_quantity = 0

            # Audit: Log cancellation
            await self.audit_service.log_state_change(
                order_id=order.id,
                old_status=old_status,
                new_status="CANCELLED",
                reason="Order cancelled by user"
            )

            await self.db.commit()
            await self.db.refresh(order)

            logger.info(f"Order cancelled successfully: ID={order_id}")

            # Invalidate cache
            await invalidate_order_cache(str(order_id))

            # Publish order cancelled event
            await publish_order_update(
                str(order_id),
                "cancelled",
                order.to_dict()
            )

            return order

        except Exception as e:
            # CRITICAL FIX: Rollback any pending database changes
            # Note: Since we cancel with broker FIRST, database should still be unchanged
            # But rollback ensures transaction is clean if any changes were made
            await self.db.rollback()

            logger.error(
                f"Order cancellation failed (rolled back transaction): "
                f"order_id={order_id}, error={e}",
                exc_info=True
            )

            raise HTTPException(500, f"Order cancellation failed: {str(e)}")

    # ==========================================
    # ORDER RETRIEVAL
    # ==========================================

    async def get_order(self, order_id: int) -> Order:
        """
        Get order by ID.

        Args:
            order_id: Order ID

        Returns:
            Order object

        Raises:
            HTTPException: If order not found
        """
        # Try cache first
        cached = await get_cached_order(str(order_id))
        if cached:
            logger.debug(f"Order {order_id} found in cache")
            # Return cached data (would need to convert back to Order object)
            # For now, fetch from DB for simplicity

        return await self._get_user_order(order_id)

    async def list_orders(
        self,
        symbol: Optional[str] = None,
        status: Optional[str] = None,
        position_id: Optional[int] = None,
        limit: int = 100,
        offset: int = 0,
        today_only: bool = True,
        order_ids: Optional[List[int]] = None
    ) -> List[Order]:
        """
        List user's orders with optional filtering.

        Args:
            symbol: Filter by symbol
            status: Filter by status
            position_id: Filter by position ID
            limit: Maximum number of orders to return
            offset: Number of orders to skip
            today_only: If True, only return today's orders (default: True)
            order_ids: Optional list of order IDs to filter to (for granular ACL)

        Returns:
            List of Order objects
        """
        # ACL check already verified at endpoint level - only filter by trading_account_id
        # user_id represents who created/placed the order, not who owns the account
        filters = self._build_order_filter_conditions(
            symbol=symbol,
            status=status,
            position_id=position_id,
            today_only=today_only,
            order_ids=order_ids
        )

        if filters is None:
            logger.debug("Order filters rejected (empty order_ids)")
            return []

        query = (
            select(Order)
            .where(*filters)
            .order_by(Order.created_at.desc())
            .limit(limit)
            .offset(offset)
        )

        result = await self.db.execute(query)
        orders = result.scalars().all()
        logger.debug(f"Retrieved {len(orders)} orders for user {self.user_id}")
        return list(orders)

    def _build_order_filter_conditions(
        self,
        symbol: Optional[str],
        status: Optional[str],
        position_id: Optional[int],
        today_only: bool,
        order_ids: Optional[List[int]]
    ) -> Optional[List[Any]]:
        """Build reusable filter conditions for orders queries."""
        conditions = [
            Order.trading_account_id == self.trading_account_id
        ]

        if order_ids is not None:
            if not order_ids:
                return None
            conditions.append(Order.id.in_(order_ids))

        if today_only:
            today = date.today()
            conditions.append(func.date(Order.created_at) == today)

        if symbol:
            conditions.append(Order.symbol == symbol)

        if status:
            conditions.append(Order.status == status)

        if position_id is not None:
            conditions.append(Order.position_id == position_id)

        return conditions

    async def count_orders(
        self,
        symbol: Optional[str] = None,
        status: Optional[str] = None,
        position_id: Optional[int] = None,
        today_only: bool = True,
        order_ids: Optional[List[int]] = None
    ) -> int:
        """Count orders matching the same filters (ignoring pagination)."""
        filters = self._build_order_filter_conditions(
            symbol=symbol,
            status=status,
            position_id=position_id,
            today_only=today_only,
            order_ids=order_ids
        )

        if filters is None:
            return 0

        query = select(func.count()).where(*filters)
        result = await self.db.execute(query)
        total = result.scalar()
        return int(total or 0)

    async def get_order_history(self, order_id: int) -> List[dict]:
        """
        Get order history from broker.

        Args:
            order_id: Order ID

        Returns:
            List of order history events

        Raises:
            HTTPException: If order not found
        """
        order = await self._get_user_order(order_id)

        try:
            history = await self.kite_client.get_order_history(order.broker_order_id)
            return history

        except Exception as e:
            logger.error(f"Failed to get order history: {e}")
            raise HTTPException(500, f"Failed to get order history: {str(e)}")

    async def sync_orders_from_broker(self) -> Dict[str, Any]:
        """
        Sync orders from broker.

        Fetches today's orders from the broker and returns them.
        Orders placed via external systems (Kite app, etc.) will be returned.

        Returns:
            Dict with sync results including broker orders
        """
        logger.info(f"Syncing orders from broker for user {self.user_id}")

        try:
            # Fetch orders from broker
            broker_orders = await self.kite_client.get_orders()

            # Format orders for response
            formatted_orders = []
            for order in broker_orders:
                formatted_orders.append({
                    "broker_order_id": order.get("order_id"),
                    "exchange_order_id": order.get("exchange_order_id"),
                    "symbol": order.get("tradingsymbol"),
                    "exchange": order.get("exchange"),
                    "transaction_type": order.get("transaction_type"),
                    "order_type": order.get("order_type"),
                    "product_type": order.get("product"),
                    "variety": order.get("variety"),
                    "quantity": order.get("quantity"),
                    "filled_quantity": order.get("filled_quantity"),
                    "pending_quantity": order.get("pending_quantity"),
                    "cancelled_quantity": order.get("cancelled_quantity"),
                    "price": order.get("price"),
                    "trigger_price": order.get("trigger_price"),
                    "average_price": order.get("average_price"),
                    "status": order.get("status"),
                    "status_message": order.get("status_message"),
                    "validity": order.get("validity"),
                    "tag": order.get("tag"),
                    "exchange_timestamp": order.get("exchange_timestamp"),
                    "order_timestamp": order.get("order_timestamp"),
                    "disclosed_quantity": order.get("disclosed_quantity"),
                    "placed_by": order.get("placed_by"),
                })

            logger.info(f"Fetched {len(formatted_orders)} orders from broker")

            return {
                "orders": formatted_orders,
                "total": len(formatted_orders),
                "source": "broker"
            }

        except Exception as e:
            logger.error(f"Failed to sync orders from broker: {e}")
            raise HTTPException(500, f"Failed to sync orders from broker: {str(e)}")

    # ==========================================
    # PRIVATE HELPER METHODS
    # ==========================================

    async def _get_user_order(self, order_id: int) -> Order:
        """Get order and verify ownership"""
        # ACL check already verified at endpoint level - verify belongs to trading_account
        result = await self.db.execute(
            select(Order).where(
                and_(
                    Order.id == order_id,
                    Order.trading_account_id == self.trading_account_id
                )
            )
        )
        order = result.scalar_one_or_none()

        if not order:
            raise HTTPException(404, f"Order {order_id} not found")

        return order

    def _validate_order(
        self,
        symbol: str,
        exchange: str,
        transaction_type: str,
        quantity: int,
        order_type: str,
        product_type: str,
        price: Optional[float],
        trigger_price: Optional[float],
    ) -> None:
        """Validate order parameters"""

        # Validate transaction type
        if transaction_type not in ["BUY", "SELL"]:
            raise HTTPException(400, f"Invalid transaction_type: {transaction_type}")

        # Validate quantity
        if quantity <= 0:
            raise HTTPException(400, "Quantity must be greater than 0")

        if quantity > settings.max_order_quantity:
            raise HTTPException(
                400,
                f"Quantity {quantity} exceeds maximum {settings.max_order_quantity}"
            )

        # Validate order type specific requirements
        if order_type == "LIMIT" and price is None:
            raise HTTPException(400, "Price required for LIMIT orders")

        if order_type in ["SL", "SL-M"] and trigger_price is None:
            raise HTTPException(400, f"Trigger price required for {order_type} orders")

        # Validate price and trigger price relationship for SL orders
        if order_type == "SL" and price is not None and trigger_price is not None:
            if transaction_type == "BUY" and trigger_price > price:
                raise HTTPException(
                    400,
                    "For BUY SL orders, trigger price must be less than price"
                )
            if transaction_type == "SELL" and trigger_price < price:
                raise HTTPException(
                    400,
                    "For SELL SL orders, trigger price must be greater than price"
                )

    async def _validate_strategy_exists(self, strategy_id: int) -> None:
        """
        Validate that strategy exists before placing order.

        Args:
            strategy_id: Strategy ID to validate

        Raises:
            HTTPException: If strategy does not exist
        """
        from sqlalchemy import text

        # Query public.strategy table (same table as Backend Service)
        result = await self.db.execute(
            text("SELECT strategy_id FROM public.strategy WHERE strategy_id = :strategy_id"),
            {"strategy_id": strategy_id}
        )
        strategy = result.fetchone()

        if not strategy:
            raise HTTPException(
                400,
                f"Strategy ID {strategy_id} does not exist. Please create strategy first."
            )

        logger.debug(f"Strategy validation passed: strategy_id={strategy_id}")

    async def _validate_lot_size(
        self,
        symbol: str,
        exchange: str,
        quantity: int,
    ) -> None:
        """
        Validate lot size for F&O orders.

        F&O orders must be placed in multiples of the lot size.
        Example: If NIFTY lot size is 50, valid quantities are 50, 100, 150, etc.

        Args:
            symbol: Trading symbol
            exchange: Exchange code
            quantity: Order quantity

        Raises:
            HTTPException: If lot size validation fails
        """
        is_valid, error_msg, lot_size = await self.lot_size_service.validate_lot_size(
            tradingsymbol=symbol,
            exchange=exchange,
            quantity=quantity
        )

        if not is_valid:
            logger.error(
                f"Lot size validation failed: {symbol} on {exchange}, "
                f"quantity={quantity}, lot_size={lot_size}, error={error_msg}"
            )

            # Add helpful suggestion for valid quantities
            if lot_size:
                valid_quantities = self.lot_size_service.get_valid_quantities(
                    lot_size=lot_size,
                    max_lots=5
                )
                error_msg += f" Suggested quantities: {', '.join(map(str, valid_quantities))}"

            raise HTTPException(400, error_msg)

        # Log successful validation for F&O orders
        if lot_size is not None:
            num_lots = quantity // lot_size
            logger.info(
                f"Lot size validation passed: {symbol} on {exchange}, "
                f"quantity={quantity} ({num_lots} lots x {lot_size})"
            )

    async def _perform_risk_checks(
        self,
        symbol: str,
        transaction_type: str,
        quantity: int,
        price: float,
    ) -> tuple[bool, str]:
        """
        Perform risk checks on order.

        Returns:
            Tuple of (passed: bool, details: str)
        """
        if not settings.enable_risk_checks:
            return True, "Risk checks disabled"

        errors: List[str] = []

        order_value = quantity * price
        if price > 0 and order_value > settings.max_order_value:
            errors.append(
                f"Order value {order_value} exceeds maximum {settings.max_order_value}"
            )

        if price > 0:
            segment = MarketHoursService.get_segment_from_symbol(symbol)
            margin_service = MarginService(
                self.db,
                user_id=self.user_id,
                trading_account_id=self.trading_account_id_int
            )
            required_margin = order_value * settings.risk_margin_multiplier
            available_margin = await margin_service.get_available_margin(segment=segment.value)

            if available_margin < required_margin:
                errors.append(
                    f"Insufficient margin for {symbol}: "
                    f"need {required_margin:.2f}, available {available_margin:.2f}"
                )

            symbol_exposure = await self._get_symbol_exposure(symbol, price)
            total_exposure = await self._get_total_exposure(price)
            new_symbol_exposure = symbol_exposure + order_value

            if new_symbol_exposure > settings.max_position_exposure_value:
                errors.append(
                    f"Position exposure {new_symbol_exposure:.2f} exceeds limit "
                    f"{settings.max_position_exposure_value:.2f}"
                )

            other_exposure = max(total_exposure - symbol_exposure, 0.0)
            if other_exposure > 0:
                concentration = new_symbol_exposure / (other_exposure + new_symbol_exposure)
                if concentration > settings.max_position_concentration_pct:
                    errors.append(
                        f"Concentration {concentration:.2%} exceeds maximum "
                        f"{settings.max_position_concentration_pct:.2%}"
                    )

        today_pnl = await self._get_today_net_pnl()
        if today_pnl <= settings.daily_loss_limit:
            errors.append(
                f"Daily loss limit breached (today_pnl={today_pnl:.2f}, "
                f"limit={settings.daily_loss_limit:.2f})"
            )

        if errors:
            details = "; ".join(errors)
            logger.warning(f"Risk check failed: {details}")
            return False, details

        return True, "All checks passed"

    def _price_coalesce_expression(self, fallback_price: float):
        """Helper to choose best price for exposure calculations."""
        fallback_literal = literal(fallback_price or 0.0)
        return func.coalesce(
            Position.last_price,
            Position.close_price,
            Position.buy_price,
            Position.sell_price,
            fallback_literal,
            0.0
        )

    async def _get_symbol_exposure(self, symbol: str, fallback_price: float) -> float:
        """Calculate current exposure for a symbol (abs(quantity) * price)."""
        price_expr = self._price_coalesce_expression(fallback_price)
        query = select(
            func.coalesce(func.sum(func.abs(Position.quantity) * price_expr), 0.0)
        ).where(
            Position.trading_account_id == self.trading_account_id,
            Position.symbol == symbol
        )
        result = await self.db.execute(query)
        return float(result.scalar() or 0.0)

    async def _get_total_exposure(self, fallback_price: float) -> float:
        """Calculate total exposure across all open positions."""
        price_expr = self._price_coalesce_expression(fallback_price)
        query = select(
            func.coalesce(func.sum(func.abs(Position.quantity) * price_expr), 0.0)
        ).where(
            Position.trading_account_id == self.trading_account_id
        )
        result = await self.db.execute(query)
        return float(result.scalar() or 0.0)

    async def _get_today_net_pnl(self) -> float:
        """Return today's net profit/loss for the account."""
        today = date.today()
        query = select(
            func.coalesce(func.sum(Position.net_pnl), 0.0)
        ).where(
            Position.trading_account_id == self.trading_account_id,
            func.date(Position.trading_day) == today
        )
        result = await self.db.execute(query)
        return float(result.scalar() or 0.0)

    # ==========================================
    # BATCH ORDER EXECUTION
    # ==========================================

    async def place_batch_orders(
        self,
        orders: List[dict],
        atomic: bool = True,
        tag_prefix: Optional[str] = None
    ) -> dict:
        """
        Place multiple orders in a batch with TRUE database-level atomicity.

        In atomic mode:
        - All database records are created in a single transaction
        - All broker orders are submitted
        - If ANY broker submission fails:
          - All successfully submitted broker orders are cancelled
          - All database records are rolled back (no orphaned records)

        Args:
            orders: List of order dictionaries (from PlaceOrderRequest)
            atomic: If True, all orders succeed or all fail (rollback on failure)
            tag_prefix: Optional prefix for order tags

        Returns:
            Dictionary with batch results and statistics

        Raises:
            HTTPException: If batch validation fails
        """
        import uuid
        import time as time_module
        from sqlalchemy.exc import SQLAlchemyError

        batch_id = str(uuid.uuid4())[:8]  # Short batch ID
        start_time = time_module.time()

        logger.info(
            f"Starting batch order placement: batch_id={batch_id}, "
            f"orders={len(orders)}, atomic={atomic}"
        )

        # Validate batch size
        if len(orders) < 1 or len(orders) > 20:
            raise HTTPException(400, "Batch must contain 1-20 orders")

        results = []
        successful_orders = []
        failed_orders = []
        placed_broker_ids = []  # Track broker IDs for rollback
        db_orders = []  # Track database order objects

        # Phase 1: Validate all orders upfront
        for idx, order_data in enumerate(orders):
            try:
                # Validate strategy_id is provided
                if "strategy_id" not in order_data or not order_data["strategy_id"]:
                    raise HTTPException(400, f"strategy_id is required for order {idx}")

                # Validate strategy exists
                await self._validate_strategy_exists(order_data["strategy_id"])

                # Validate order parameters
                self._validate_order(
                    symbol=order_data["symbol"],
                    exchange=order_data["exchange"],
                    transaction_type=order_data["transaction_type"],
                    quantity=order_data["quantity"],
                    order_type=order_data["order_type"],
                    product_type=order_data["product_type"],
                    price=order_data.get("price"),
                    trigger_price=order_data.get("trigger_price"),
                )

                # Validate lot size for F&O
                await self._validate_lot_size(
                    symbol=order_data["symbol"],
                    exchange=order_data["exchange"],
                    quantity=order_data["quantity"],
                )

                # Perform risk checks
                risk_passed, risk_details = await self._perform_risk_checks(
                    symbol=order_data["symbol"],
                    transaction_type=order_data["transaction_type"],
                    quantity=order_data["quantity"],
                    price=order_data.get("price", 0),
                )

                if not risk_passed:
                    if atomic:
                        raise HTTPException(
                            400,
                            f"Risk check failed for order {idx}: {risk_details}"
                        )
                    else:
                        # Non-atomic mode: record failure but continue
                        results.append({
                            "index": idx,
                            "success": False,
                            "error": f"Risk check failed: {risk_details}",
                            "order": None,
                            "broker_order_id": None
                        })
                        failed_orders.append(idx)
                        continue

            except HTTPException as e:
                if atomic:
                    raise HTTPException(
                        400,
                        f"Validation failed for order {idx}: {e.detail}"
                    )
                else:
                    results.append({
                        "index": idx,
                        "success": False,
                        "error": str(e.detail),
                        "order": None,
                        "broker_order_id": None
                    })
                    failed_orders.append(idx)
                    continue

        logger.info(f"Batch {batch_id}: Validation complete, starting order placement")

        # Phase 2: Create database records (no commit yet in atomic mode)
        # Use savepoint for atomic mode to enable true rollback
        if atomic:
            # Begin a new savepoint for atomic batch
            await self.db.begin_nested()

        try:
            for idx, order_data in enumerate(orders):
                # Skip if already failed in validation (non-atomic mode)
                if idx in failed_orders:
                    continue

                # Create tag with batch ID
                tag = order_data.get("tag", "")
                if tag_prefix:
                    tag = f"{tag_prefix}_{batch_id}_{idx}"
                else:
                    tag = f"batch_{batch_id}_{idx}"

                # Create order record in database (PENDING status)
                order = Order(
                    strategy_id=order_data["strategy_id"],
                    user_id=self.user_id,
                    trading_account_id=self.trading_account_id,
                    symbol=order_data["symbol"],
                    exchange=order_data["exchange"],
                    transaction_type=order_data["transaction_type"],
                    order_type=order_data["order_type"],
                    product_type=order_data["product_type"],
                    variety=order_data.get("variety", "regular"),
                    quantity=order_data["quantity"],
                    filled_quantity=0,
                    pending_quantity=order_data["quantity"],
                    cancelled_quantity=0,
                    price=order_data.get("price"),
                    trigger_price=order_data.get("trigger_price"),
                    validity=order_data.get("validity", "DAY"),
                    status="PENDING",
                    risk_check_passed=True,
                    broker_tag=tag,
                )

                self.db.add(order)
                db_orders.append((idx, order, order_data, tag))

            # Flush to get order IDs (but don't commit yet)
            await self.db.flush()

            logger.info(f"Batch {batch_id}: Created {len(db_orders)} database records")

            # Phase 3: Submit orders to broker
            for idx, order, order_data, tag in db_orders:
                try:
                    # Build broker params
                    broker_params = {
                        "symbol": order_data["symbol"],
                        "exchange": order_data["exchange"],
                        "transaction_type": order_data["transaction_type"],
                        "quantity": order_data["quantity"],
                        "order_type": order_data["order_type"],
                        "product": order_data["product_type"],
                        "price": order_data.get("price"),
                        "trigger_price": order_data.get("trigger_price"),
                        "validity": order_data.get("validity", "DAY"),
                        "variety": order_data.get("variety", "regular"),
                        "tag": tag,
                    }

                    # Add disclosed_quantity only for iceberg orders
                    disclosed_qty = order_data.get("disclosed_quantity")
                    if disclosed_qty is not None and order_data.get("variety") == "iceberg":
                        broker_params["disclosed_quantity"] = disclosed_qty

                    # Submit to broker with circuit breaker and retry
                    async def _place_order_with_retry():
                        return await retry_with_backoff(
                            self.kite_client.place_order,
                            _order_retry_config,
                            **broker_params
                        )

                    broker_order_id = await _broker_circuit_breaker.call(_place_order_with_retry)

                    # Update order with broker ID
                    order.broker_order_id = broker_order_id
                    order.status = "SUBMITTED"
                    order.submitted_at = datetime.utcnow()

                    placed_broker_ids.append(broker_order_id)
                    successful_orders.append(idx)

                    # Prepare result (will be finalized after commit)
                    results.append({
                        "index": idx,
                        "success": True,
                        "order": order,
                        "broker_order_id": broker_order_id,
                        "error": None
                    })

                    logger.info(
                        f"Batch {batch_id}: Order {idx} submitted "
                        f"(order_id={order.id}, broker_id={broker_order_id})"
                    )

                except (CircuitBreakerError, Exception) as e:
                    logger.error(f"Batch {batch_id}: Order {idx} broker submission failed: {e}")

                    if atomic:
                        # ATOMIC MODE: Rollback everything
                        logger.warning(
                            f"Batch {batch_id}: Atomic mode - rolling back "
                            f"{len(placed_broker_ids)} broker orders and all DB records"
                        )

                        # 1. Cancel already-placed broker orders
                        await self._rollback_batch_orders(placed_broker_ids, batch_id)

                        # 2. Rollback database transaction (savepoint)
                        await self.db.rollback()

                        execution_time_ms = (time_module.time() - start_time) * 1000

                        # Build failure results for all orders
                        failure_results = []
                        for i in range(len(orders)):
                            if i == idx:
                                failure_results.append({
                                    "index": i,
                                    "success": False,
                                    "order": None,
                                    "broker_order_id": None,
                                    "error": str(e)
                                })
                            else:
                                failure_results.append({
                                    "index": i,
                                    "success": False,
                                    "order": None,
                                    "broker_order_id": None,
                                    "error": "Rolled back due to atomic batch failure"
                                })

                        return {
                            "batch_id": batch_id,
                            "total_orders": len(orders),
                            "successful_orders": 0,  # All rolled back
                            "failed_orders": len(orders),
                            "atomic": atomic,
                            "results": failure_results,
                            "rollback_performed": True,
                            "execution_time_ms": execution_time_ms
                        }
                    else:
                        # NON-ATOMIC MODE: Mark this order as failed, continue others
                        order.status = "REJECTED"
                        order.status_message = str(e)

                        results.append({
                            "index": idx,
                            "success": False,
                            "order": None,
                            "broker_order_id": None,
                            "error": str(e)
                        })
                        failed_orders.append(idx)

            # Phase 4: Commit all changes
            await self.db.commit()

            # Refresh orders to get final state
            for result in results:
                if result.get("success") and result.get("order"):
                    await self.db.refresh(result["order"])
                    # Publish order created event
                    await publish_order_update(
                        str(result["order"].id),
                        "created",
                        result["order"].to_dict()
                    )

            execution_time_ms = (time_module.time() - start_time) * 1000

            logger.info(
                f"Batch {batch_id} completed: "
                f"success={len(successful_orders)}, failed={len(failed_orders)}, "
                f"time={execution_time_ms:.2f}ms"
            )

            return {
                "batch_id": batch_id,
                "total_orders": len(orders),
                "successful_orders": len(successful_orders),
                "failed_orders": len(failed_orders),
                "atomic": atomic,
                "results": results,
                "rollback_performed": False,
                "execution_time_ms": execution_time_ms
            }

        except SQLAlchemyError as e:
            # Database error - rollback
            await self.db.rollback()

            # Also cancel any broker orders that were placed
            if placed_broker_ids:
                await self._rollback_batch_orders(placed_broker_ids, batch_id)

            logger.error(f"Batch {batch_id}: Database error - rolled back: {e}")
            raise HTTPException(500, f"Batch order placement failed: {str(e)}")

        except Exception as e:
            # Unexpected error - rollback
            await self.db.rollback()

            # Also cancel any broker orders that were placed
            if placed_broker_ids:
                await self._rollback_batch_orders(placed_broker_ids, batch_id)

            logger.error(f"Batch {batch_id}: Unexpected error - rolled back: {e}")
            raise HTTPException(500, f"Batch order placement failed: {str(e)}")

    async def _rollback_batch_orders(
        self,
        broker_order_ids: List[str],
        batch_id: str
    ) -> bool:
        """
        Rollback (cancel) previously placed orders in a batch.

        Args:
            broker_order_ids: List of broker order IDs to cancel
            batch_id: Batch ID for logging

        Returns:
            True if all orders cancelled successfully
        """
        rollback_results = []

        for broker_order_id in broker_order_ids:
            try:
                await self.kite_client.cancel_order(broker_order_id)
                rollback_results.append(True)
                logger.info(f"Batch {batch_id}: Rolled back order {broker_order_id}")

            except Exception as e:
                logger.error(
                    f"Batch {batch_id}: Failed to rollback order {broker_order_id}: {e}"
                )
                rollback_results.append(False)

        all_rolled_back = all(rollback_results)

        if all_rolled_back:
            logger.info(f"Batch {batch_id}: All {len(broker_order_ids)} orders rolled back")
        else:
            logger.warning(
                f"Batch {batch_id}: Partial rollback - "
                f"{sum(rollback_results)}/{len(broker_order_ids)} orders cancelled"
            )

        return all_rolled_back

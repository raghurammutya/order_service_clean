"""
Position Service Business Logic

Handles position tracking, updates, and queries.
"""
import logging
from datetime import datetime, date
from typing import List, Optional, Dict, Any, Union
from sqlalchemy import select, update, and_, or_, text
from sqlalchemy.ext.asyncio import AsyncSession
from fastapi import HTTPException

from ..models.position import Position, PositionSource
from ..database.redis_client import (
    cache_position,
    get_cached_position,
    invalidate_position_cache
)
from .kite_client_multi import get_kite_client_for_account
from .brokerage_service import BrokerageService
from .default_strategy_service import DefaultStrategyService, get_or_create_default_strategy
from .subscription_manager import SubscriptionManager
from decimal import Decimal

logger = logging.getLogger(__name__)


class PositionService:
    """Position tracking and management service"""

    def __init__(self, db: AsyncSession, user_id: int, trading_account_id: Union[int, str]):
        """
        Initialize position service.

        Args:
            db: Database session
            user_id: User ID from JWT token
            trading_account_id: Trading account ID (can be int or string)
        """
        self.db = db
        self.user_id = user_id
        # Convert to string since DB column is VARCHAR(100)
        self.trading_account_id = str(trading_account_id)
        self.kite_client = get_kite_client_for_account(trading_account_id)
        self.brokerage_service = BrokerageService()
        # Lazy initialization for subscription manager
        self._subscription_manager: Optional[SubscriptionManager] = None

    async def _get_subscription_manager(self) -> SubscriptionManager:
        """Get or create subscription manager instance."""
        if self._subscription_manager is None:
            from ..config.settings import settings
            ticker_url = getattr(settings, 'ticker_service_url', 'http://localhost:8089')
            self._subscription_manager = SubscriptionManager(
                db=self.db,
                ticker_service_url=ticker_url
            )
        return self._subscription_manager

    async def _manage_position_subscription(
        self,
        symbol: str,
        exchange: str,
        is_open: bool
    ) -> None:
        """
        Manage subscription for a position.

        Subscribe when position is opened, unsubscribe when closed.

        Args:
            symbol: Trading symbol
            exchange: Exchange code
            is_open: Whether the position is open
        """
        try:
            subscription_manager = await self._get_subscription_manager()

            if is_open:
                # Subscribe to tick feed for open position
                await subscription_manager.subscribe_for_position(
                    trading_account_id=self.trading_account_id,
                    symbol=symbol,
                    exchange=exchange,
                    source="position"
                )
                logger.debug(f"Subscribed to ticks for {symbol}")
            else:
                # Unsubscribe from tick feed when position closes
                await subscription_manager.unsubscribe_for_position(
                    trading_account_id=self.trading_account_id,
                    symbol=symbol,
                    exchange=exchange,
                    source="position"
                )
                logger.debug(f"Unsubscribed from ticks for {symbol}")

        except Exception as e:
            # Don't fail position operations due to subscription errors
            logger.warning(f"Failed to manage subscription for {symbol}: {e}")

    async def _get_instrument_token(self, symbol: str, exchange: str) -> Optional[int]:
        """
        Look up instrument_token from instrument_registry.

        Args:
            symbol: Trading symbol (e.g., RELIANCE, NIFTY25D0226400CE)
            exchange: Exchange code (e.g., NSE, NFO)

        Returns:
            Instrument token if found, None otherwise
        """
        try:
            result = await self.db.execute(text("""
                SELECT instrument_token
                FROM public.instrument_registry
                WHERE symbol = :symbol
                  AND exchange = :exchange
                  AND is_active = true
                LIMIT 1
            """), {"symbol": symbol, "exchange": exchange})

            row = result.fetchone()
            if row:
                return row.instrument_token
            return None
        except Exception as e:
            logger.warning(f"Failed to lookup instrument_token for {symbol}/{exchange}: {e}")
            return None

    # ==========================================
    # POSITION SYNC FROM BROKER (DEPRECATED - Use real-time updates)
    # ==========================================

    async def sync_positions_from_broker(self) -> Dict[str, Any]:
        """
        DEPRECATED: Sync positions from broker API.

        This method is DEPRECATED. Positions are now updated in real-time via:
        - update_position_from_order() - Called when orders complete
        - validate_positions() - Periodic validation every 5 minutes

        This method is kept for backward compatibility and manual sync endpoints.

        Returns:
            Dictionary with sync statistics

        Raises:
            HTTPException: If sync fails
        """
        logger.warning(
            "sync_positions_from_broker() called - consider using real-time updates instead"
        )
        try:
            logger.info(f"Syncing positions for user {self.user_id}")

            # Fetch positions from broker
            broker_positions = await self.kite_client.get_positions()

            net_positions = broker_positions.get('net', [])
            day_positions = broker_positions.get('day', [])

            stats = {
                'net_positions_synced': 0,
                'day_positions_synced': 0,
                'positions_created': 0,
                'positions_updated': 0,
                'errors': []
            }

            # Sync net positions (contains complete data including day positions)
            for broker_pos in net_positions:
                try:
                    await self._sync_position(broker_pos, is_day_position=False)
                    await self.db.flush()  # Ensure position is visible for next iteration
                    stats['net_positions_synced'] += 1
                except Exception as e:
                    logger.error(f"Failed to sync net position {broker_pos.get('tradingsymbol')}: {e}")
                    stats['errors'].append(str(e))

            # Day positions are skipped because net positions already contain complete data
            # and syncing both causes duplicate key violations on the unique constraint
            stats['day_positions_synced'] = len(day_positions)  # Count but don't sync

            await self.db.commit()

            logger.info(f"Position sync completed: {stats}")
            return stats

        except Exception as e:
            logger.error(f"Position sync failed: {e}")
            raise HTTPException(500, f"Position sync failed: {str(e)}")

    async def _sync_position(
        self,
        broker_position: Dict[str, Any],
        is_day_position: bool,
        strategy_id: Optional[int] = None
    ) -> Position:
        """
        Sync a single position from broker data.

        If position is new and has no strategy_id, it's tagged to the default
        strategy as an external position.

        Args:
            broker_position: Position data from broker
            is_day_position: Whether this is a day position
            strategy_id: Optional strategy ID to assign (if None, uses default strategy)

        Returns:
            Updated or created Position object
        """
        symbol = broker_position["symbol"]
        exchange = broker_position['exchange']
        product = broker_position['product']
        trading_day = date.today()

        # Find existing position
        # ACL check already verified at endpoint level
        result = await self.db.execute(
            select(Position).where(
                and_(
                    Position.trading_account_id == self.trading_account_id,
                    Position.symbol == symbol,
                    Position.exchange == exchange,
                    Position.product_type == product,
                    Position.trading_day == trading_day
                )
            )
        )
        position = result.scalar_one_or_none()

        # Calculate values
        quantity = broker_position.get('quantity', 0)
        average_price = broker_position.get('average_price', 0.0)
        last_price = broker_position.get('last_price', 0.0)
        is_open = quantity != 0

        # Calculate buy/sell prices from broker data
        buy_quantity = broker_position.get('buy_quantity', 0)
        sell_quantity = broker_position.get('sell_quantity', 0)
        buy_value = broker_position.get('buy_value', 0.0)
        sell_value = broker_position.get('sell_value', 0.0)

        buy_price = (buy_value / buy_quantity) if buy_quantity > 0 else None
        sell_price = (sell_value / sell_quantity) if sell_quantity > 0 else None

        # P&L values
        realized_pnl = broker_position.get('realised', 0.0)
        unrealized_pnl = broker_position.get('unrealised', 0.0)
        total_pnl = realized_pnl + unrealized_pnl

        # Look up instrument_token for WebSocket subscriptions
        instrument_token = await self._get_instrument_token(symbol, exchange)

        if position:
            # Update existing position
            position.quantity = quantity
            position.buy_quantity = buy_quantity
            position.sell_quantity = sell_quantity
            position.buy_value = buy_value
            position.sell_value = sell_value
            position.buy_price = buy_price
            position.sell_price = sell_price
            position.last_price = last_price
            position.realized_pnl = realized_pnl
            position.unrealized_pnl = unrealized_pnl
            position.total_pnl = total_pnl
            position.is_open = is_open
            position.updated_at = datetime.utcnow()

            # Update instrument_token if not set or if we found a new one
            if instrument_token and not position.instrument_token:
                position.instrument_token = instrument_token

            # If position exists but has no strategy_id, tag to default strategy
            if position.strategy_id is None:
                default_strategy_id = await get_or_create_default_strategy(
                    self.db, str(self.trading_account_id), self.user_id
                )
                position.strategy_id = default_strategy_id
                position.source = PositionSource.EXTERNAL
                logger.info(f"Tagged orphan position {symbol} to default strategy {default_strategy_id}")

            logger.debug(f"Updated position for {symbol}")

        else:
            # Create new position - this is from broker sync, so it's external
            # Get the default strategy for this trading account
            if strategy_id is None:
                default_strategy_id = await get_or_create_default_strategy(
                    self.db, str(self.trading_account_id), self.user_id
                )
                logger.info(f"New external position {symbol} tagged to default strategy {default_strategy_id}")
            else:
                default_strategy_id = strategy_id

            position = Position(
                user_id=self.user_id,
                trading_account_id=self.trading_account_id,
                symbol=symbol,
                exchange=exchange,
                product_type=product,
                trading_day=trading_day,
                quantity=quantity,
                buy_quantity=buy_quantity,
                sell_quantity=sell_quantity,
                buy_value=buy_value,
                sell_value=sell_value,
                buy_price=buy_price,
                sell_price=sell_price,
                last_price=last_price,
                realized_pnl=realized_pnl,
                unrealized_pnl=unrealized_pnl,
                total_pnl=total_pnl,
                is_open=is_open,
                strategy_id=default_strategy_id,
                source=PositionSource.EXTERNAL,
                instrument_token=instrument_token,
            )

            self.db.add(position)
            logger.debug(f"Created new external position for {symbol} (token={instrument_token})")

        # Invalidate cache
        if position.id:
            await invalidate_position_cache(f"user:{self.user_id}")

        # Manage real-time tick subscription based on position state
        await self._manage_position_subscription(symbol, exchange, is_open)

        return position

    # ==========================================
    # POSITION QUERIES
    # ==========================================

    async def get_position(self, position_id: int) -> Position:
        """
        Get position by ID.

        Args:
            position_id: Position ID

        Returns:
            Position object

        Raises:
            HTTPException: If position not found
        """
        # ACL check already verified at endpoint level - verify belongs to trading_account
        result = await self.db.execute(
            select(Position).where(
                and_(
                    Position.id == position_id,
                    Position.trading_account_id == self.trading_account_id
                )
            )
        )
        position = result.scalar_one_or_none()

        if not position:
            raise HTTPException(404, f"Position {position_id} not found in account {self.trading_account_id}")

        return position

    async def list_positions(
        self,
        symbol: Optional[str] = None,
        exchange: Optional[str] = None,
        only_open: bool = True,
        trading_day: Optional[date] = None,
        limit: int = 100,
        offset: int = 0,
        position_ids: Optional[List[int]] = None,
        execution_id: Optional[str] = None
    ) -> List[Position]:
        """
        List user's positions with optional filtering.

        Args:
            symbol: Filter by symbol
            exchange: Filter by exchange
            only_open: Only return open positions (default True)
            trading_day: Filter by trading day (default today)
            limit: Maximum number of positions to return
            offset: Number of positions to skip
            position_ids: Optional list of position IDs to filter to (for granular ACL)
            execution_id: Optional execution ID to filter by (for unified execution architecture)

        Returns:
            List of Position objects
        """
        # ACL check already verified at endpoint level - only filter by trading_account_id
        # user_id represents who created/imported the data, not who owns the account
        query = select(Position).where(
            Position.trading_account_id == self.trading_account_id
        )

        # Granular ACL filtering - only return positions user has access to
        if position_ids is not None:
            if not position_ids:
                # Empty list means no access to any positions
                return []
            query = query.where(Position.position_id.in_(position_ids))

        if symbol:
            query = query.where(Position.symbol == symbol)

        if exchange:
            query = query.where(Position.exchange == exchange)

        if only_open:
            query = query.where(Position.is_open == True)

        if trading_day:
            query = query.where(Position.trading_day == trading_day)
        # Note: If only_open=True and no trading_day specified, we return ALL open positions
        # across all days. This is the expected behavior for dashboard/P&L displays.
        # If you need today's positions specifically, pass trading_day explicitly.

        # Filter by execution_id if provided (unified execution architecture)
        if execution_id:
            query = query.where(Position.execution_id == execution_id)

        query = query.order_by(Position.updated_at.desc()).limit(limit).offset(offset)

        result = await self.db.execute(query)
        positions = result.scalars().all()

        logger.debug(f"Retrieved {len(positions)} positions for user {self.user_id}")

        return list(positions)

    async def get_position_summary(
        self,
        symbol: Optional[str] = None,
        strategy_id: Optional[int] = None,
        segment: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Get summary of all positions for the user with optional filtering.

        Args:
            symbol: Filter by trading symbol (optional)
            strategy_id: Filter by strategy ID (optional)
            segment: Filter by exchange/segment (optional)

        Returns:
            Dictionary with position summary
        """
        # Fetch positions with filters
        positions = await self.list_positions(
            only_open=True,
            symbol=symbol,
            exchange=segment  # segment is the same as exchange filter
        )

        # Apply strategy_id filter if provided (list_positions doesn't support this)
        if strategy_id is not None:
            positions = [p for p in positions if p.strategy_id == strategy_id]

        total_pnl = sum(float(p.total_pnl) for p in positions if p.total_pnl)
        total_realized = sum(float(p.realized_pnl) for p in positions if p.realized_pnl)
        total_unrealized = sum(float(p.unrealized_pnl) for p in positions if p.unrealized_pnl)

        return {
            'total_positions': len(positions),
            'total_pnl': total_pnl,
            'total_realized_pnl': total_realized,
            'total_unrealized_pnl': total_unrealized,
            'positions': positions  # Return list of Position objects, not dicts
        }

    # ==========================================
    # REAL-TIME POSITION UPDATES FROM ORDERS
    # ==========================================

    async def update_position_from_order(
        self,
        order_data: Dict[str, Any],
        strategy_id: Optional[int] = None
    ) -> Optional[Position]:
        """
        Update position from completed order (real-time).

        Called by WebSocket listener when order status = COMPLETE.

        Args:
            order_data: Order data from WebSocket or broker API
            strategy_id: Strategy ID from the order (inherited from order)

        Returns:
            Updated Position object, or None if not applicable (CNC orders)
        """
        # Only process MIS/NRML orders (intraday/F&O)
        product_type = order_data.get("product")
        if product_type == "CNC":
            # CNC orders update holdings, not positions
            return None

        symbol = order_data.get("tradingsymbol")
        exchange = order_data.get("exchange")
        transaction_type = order_data.get("transaction_type")  # BUY or SELL
        filled_quantity = order_data.get("filled_quantity", 0)
        average_price = order_data.get("average_price", 0.0)

        # Get strategy_id from order_data if not passed directly
        if strategy_id is None:
            strategy_id = order_data.get("strategy_id")

        if filled_quantity == 0:
            # No fills yet
            return None

        logger.info(
            f"Updating position from order: {symbol} {transaction_type} "
            f"qty={filled_quantity} price={average_price} strategy_id={strategy_id}"
        )

        # Get or create position
        trading_day = date.today()
        # ACL check already verified at endpoint level
        result = await self.db.execute(
            select(Position).where(
                and_(
                    Position.trading_account_id == self.trading_account_id,
                    Position.symbol == symbol,
                    Position.exchange == exchange,
                    Position.product_type == product_type,
                    Position.trading_day == trading_day
                )
            )
        )
        position = result.scalar_one_or_none()

        if not position:
            # Create new position
            # If strategy_id is not set, use default strategy
            if strategy_id is None:
                strategy_id = await get_or_create_default_strategy(
                    self.db, str(self.trading_account_id), self.user_id
                )
                source = PositionSource.EXTERNAL
                logger.info(f"New position {symbol} tagged to default strategy (external)")
            else:
                source = PositionSource.INTERNAL

            # Look up instrument_token for WebSocket subscriptions
            instrument_token = await self._get_instrument_token(symbol, exchange)

            position = Position(
                user_id=self.user_id,
                trading_account_id=self.trading_account_id,
                symbol=symbol,
                exchange=exchange,
                product_type=product_type,
                trading_day=trading_day,
                quantity=0,
                buy_quantity=0,
                sell_quantity=0,
                buy_value=0,
                sell_value=0,
                realized_pnl=0,
                unrealized_pnl=0,
                total_pnl=0,
                is_open=True,
                strategy_id=strategy_id,
                source=source,
                instrument_token=instrument_token,
            )
            self.db.add(position)
            logger.info(f"Created new position for {symbol} (strategy={strategy_id}, source={source}, token={instrument_token})")
        else:
            # Update instrument_token if not already set
            if not position.instrument_token:
                position.instrument_token = await self._get_instrument_token(symbol, exchange)

        # Update buy/sell sides
        if transaction_type == "BUY":
            position.buy_quantity += filled_quantity
            position.buy_value += filled_quantity * average_price
            position.buy_price = position.buy_value / position.buy_quantity if position.buy_quantity > 0 else None

        elif transaction_type == "SELL":
            position.sell_quantity += filled_quantity
            position.sell_value += filled_quantity * average_price
            position.sell_price = position.sell_value / position.sell_quantity if position.sell_quantity > 0 else None

        # Calculate net quantity
        position.quantity = position.buy_quantity - position.sell_quantity

        # Calculate realized P&L (for closed portions) - GROSS (before charges)
        closed_quantity = min(position.buy_quantity, position.sell_quantity)
        if closed_quantity > 0 and position.buy_price and position.sell_price:
            position.realized_pnl = (position.sell_price - position.buy_price) * closed_quantity

        # Calculate unrealized P&L (for open portions) - GROSS (before charges)
        if position.quantity != 0:
            # Use last_price from order data if available, otherwise keep existing
            last_price = order_data.get("last_price") or position.last_price or average_price
            position.last_price = last_price

            avg_entry = position.buy_price if position.quantity > 0 else position.sell_price
            if avg_entry:
                position.unrealized_pnl = (last_price - avg_entry) * abs(position.quantity)
        else:
            position.unrealized_pnl = 0

        # Total P&L (GROSS)
        position.total_pnl = position.realized_pnl + position.unrealized_pnl

        # Calculate brokerage and charges
        # Determine instrument type from exchange
        instrument_type = "EQ"  # Default to equity
        if position.exchange in ['NFO', 'BFO', 'MCX', 'CDS']:
            # F&O - would need to check if FUT/CE/PE from symbol
            # For now, assume FUT (future enhancement: parse from symbol)
            instrument_type = "FUT"

        total_charges = self.brokerage_service.calculate_position_charges(
            exchange=position.exchange,
            product_type=position.product_type,
            buy_quantity=position.buy_quantity,
            buy_value=Decimal(str(position.buy_value)),
            sell_quantity=position.sell_quantity,
            sell_value=Decimal(str(position.sell_value)),
            instrument_type=instrument_type
        )

        # Update charges fields
        position.total_charges = float(total_charges)

        # For detailed breakdown, we'd need to call calculate_brokerage separately
        # For now, set total_charges and leave breakdown at 0
        # (Can be enhanced later to show brokerage/STT/GST breakdown)
        position.brokerage = 0  # Future: breakdown from total_charges
        position.stt = 0
        position.exchange_charges = 0
        position.gst = 0

        # Calculate NET P&L (after charges)
        position.net_pnl = position.total_pnl - Decimal(str(position.total_charges))

        # Mark as open/closed
        position.is_open = (position.quantity != 0)
        if not position.is_open:
            position.closed_at = datetime.utcnow()

        position.updated_at = datetime.utcnow()

        await self.db.commit()

        # Invalidate cache
        await invalidate_position_cache(f"user:{self.user_id}")

        logger.info(
            f"Position updated: {symbol} qty={position.quantity} "
            f"realized={position.realized_pnl} unrealized={position.unrealized_pnl} "
            f"gross_pnl={position.total_pnl} charges={position.total_charges} "
            f"net_pnl={position.net_pnl} is_open={position.is_open}"
        )

        # Manage real-time tick subscription based on position state
        await self._manage_position_subscription(symbol, exchange, position.is_open)

        return position

    async def validate_positions(self) -> Dict[str, Any]:
        """
        Validate all positions against broker API.

        Runs periodically (every 5 minutes) to detect drift.

        Returns:
            Dictionary with validation statistics
        """
        logger.info("Starting position validation")

        stats = {
            "positions_checked": 0,
            "positions_corrected": 0,
            "quantity_drifts": [],
            "pnl_drifts": [],
            "missing_positions": [],
            "extra_positions": []
        }

        try:
            # Fetch positions from broker
            broker_positions_data = await self.kite_client.get_positions()
            broker_net = broker_positions_data.get('net', [])

            # Get our calculated positions
            our_positions = await self.list_positions(only_open=True, trading_day=date.today())
            our_positions_dict = {
                (p.symbol, p.product_type): p for p in our_positions
            }

            # Check broker positions against ours
            for broker_pos in broker_net:
                symbol = broker_pos["symbol"]
                product = broker_pos['product']
                broker_qty = broker_pos.get('quantity', 0)

                stats["positions_checked"] += 1

                key = (symbol, product)
                if key in our_positions_dict:
                    our_pos = our_positions_dict[key]

                    # Check quantity drift
                    quantity_drift = abs(our_pos.quantity - broker_qty)
                    if quantity_drift > 0:
                        logger.error(
                            f"Position quantity drift: {symbol} ({product}) "
                            f"our={our_pos.quantity} broker={broker_qty} drift={quantity_drift}"
                        )
                        stats["quantity_drifts"].append({
                            "symbol": symbol,
                            "product": product,
                            "our_qty": our_pos.quantity,
                            "broker_qty": broker_qty,
                            "drift": quantity_drift
                        })

                        # Correct from broker (source of truth)
                        await self._sync_position(broker_pos, is_day_position=False)
                        stats["positions_corrected"] += 1

                    # Check P&L drift (with tolerance)
                    broker_pnl = broker_pos.get('pnl', 0.0)
                    pnl_drift = abs(float(our_pos.total_pnl) - broker_pnl)
                    if pnl_drift > 1.0:  # Allow 1 rupee tolerance
                        logger.warning(
                            f"Position P&L drift: {symbol} ({product}) "
                            f"our={our_pos.total_pnl} broker={broker_pnl} drift={pnl_drift}"
                        )
                        stats["pnl_drifts"].append({
                            "symbol": symbol,
                            "product": product,
                            "our_pnl": float(our_pos.total_pnl),
                            "broker_pnl": broker_pnl,
                            "drift": pnl_drift
                        })

                        # Recalculate P&L
                        await self._sync_position(broker_pos, is_day_position=False)
                        stats["positions_corrected"] += 1

                else:
                    # Position exists in broker but not in our DB
                    if broker_qty != 0:
                        logger.warning(
                            f"Missing position in DB: {symbol} ({product}) qty={broker_qty}"
                        )
                        stats["missing_positions"].append({
                            "symbol": symbol,
                            "product": product,
                            "quantity": broker_qty
                        })

                        # Add from broker
                        await self._sync_position(broker_pos, is_day_position=False)
                        stats["positions_corrected"] += 1

            # Check for positions in our DB but not in broker (shouldn't happen)
            broker_keys = {(p["symbol"], p['product']) for p in broker_net if p.get('quantity', 0) != 0}
            for key, our_pos in our_positions_dict.items():
                if key not in broker_keys and our_pos.is_open:
                    logger.warning(
                        f"Extra position in DB (not in broker): {key[0]} ({key[1]}) qty={our_pos.quantity}"
                    )
                    stats["extra_positions"].append({
                        "symbol": key[0],
                        "product": key[1],
                        "quantity": our_pos.quantity
                    })

                    # Close this position
                    our_pos.is_open = False
                    our_pos.closed_at = datetime.utcnow()
                    stats["positions_corrected"] += 1

            await self.db.commit()

            logger.info(
                f"Position validation complete: checked={stats['positions_checked']} "
                f"corrected={stats['positions_corrected']}"
            )

            return stats

        except Exception as e:
            logger.error(f"Position validation failed: {e}", exc_info=True)
            await self.db.rollback()
            raise

    # ==========================================
    # POSITION CLOSE
    # ==========================================

    async def close_position(self, position_id: int) -> Position:
        """
        Close a position by placing a closing order.

        Args:
            position_id: Position ID to close

        Returns:
            Updated Position object

        Raises:
            HTTPException: If position not found or close fails
        """
        position = await self.get_position(position_id)

        if not position.is_open:
            raise HTTPException(400, "Position is already closed")

        # Determine closing transaction type and quantity
        if position.quantity > 0:
            transaction_type = "SELL"
            quantity = position.quantity
        else:
            transaction_type = "BUY"
            quantity = abs(position.quantity)

        # Place a market order to close the position
        logger.info(
            f"Closing position {position_id}: {transaction_type} {quantity} {position.symbol}"
        )

        try:
            # Import OrderService to place the closing order
            from .order_service import OrderService

            # Create order service instance
            order_service = OrderService(self.db, self.user_id, self.trading_account_id)

            # Place market order to close position
            closing_order = await order_service.place_order(
                symbol=position.symbol,
                exchange=position.exchange,
                transaction_type=transaction_type,
                quantity=quantity,
                order_type="MARKET",
                product_type=position.product_type,
                price=None,
                trigger_price=None,
                validity="DAY",
                variety="regular",
                tag=f"close_position_{position_id}"
            )

            logger.info(
                f"Successfully placed closing order {closing_order.id} for position {position_id}"
            )

            return position

        except Exception as e:
            logger.error(f"Failed to place closing order for position {position_id}: {e}")
            raise HTTPException(
                500,
                f"Failed to close position: {str(e)}"
            )

    async def move_to_strategy(
        self,
        position_id: int,
        target_strategy_id: int,
        target_execution_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Move a position and its associated orders/trades to another strategy.

        Optionally also transfers execution ownership (for unified execution architecture).

        Args:
            position_id: ID of the position to move
            target_strategy_id: ID of the target strategy
            target_execution_id: Optional UUID of target execution (for execution transfer)

        Returns:
            Dict with counts of moved records
        """
        try:
            # Get the position first to verify ownership and get details
            position = await self.get_position(position_id)
            if not position:
                raise HTTPException(404, f"Position {position_id} not found")

            old_strategy_id = position.strategy_id
            symbol = position.symbol
            exchange = position.exchange
            product_type = position.product_type
            trading_day = position.trading_day

            # Verify target strategy exists and belongs to user
            strategy_check = await self.db.execute(
                text("SELECT id FROM order_service.strategies WHERE id = :sid AND user_id = :uid"),
                {"sid": target_strategy_id, "uid": self.user_id}
            )
            if not strategy_check.fetchone():
                raise HTTPException(404, f"Target strategy {target_strategy_id} not found")

            # Start transaction to move all related records atomically
            moved_counts = {
                "position": 0,
                "orders": 0,
                "trades": 0,
                "old_strategy_id": old_strategy_id,
                "new_strategy_id": target_strategy_id,
                "execution_transferred": False,
                "transfer_logged": False
            }

            # Get current execution_id for transfer logging
            old_execution_id = position.execution_id if hasattr(position, 'execution_id') else None

            # 1. Move the position (including execution_id if provided)
            if target_execution_id:
                # Transfer execution ownership
                await self.db.execute(
                    text("""
                        UPDATE order_service.positions
                        SET strategy_id = :new_sid,
                            execution_id = :new_exec_id::uuid,
                            updated_at = NOW()
                        WHERE id = :pid AND user_id = :uid
                    """),
                    {
                        "new_sid": target_strategy_id,
                        "new_exec_id": target_execution_id,
                        "pid": position_id,
                        "uid": self.user_id
                    }
                )
                moved_counts["execution_transferred"] = True

                # Log the transfer in position_transfers table
                if old_execution_id:
                    await self.db.execute(
                        text("""
                            INSERT INTO order_service.position_transfers (
                                position_id,
                                source_execution_id,
                                target_execution_id,
                                quantity,
                                transfer_price,
                                transfer_value,
                                reason,
                                transfer_type,
                                initiated_by_user_id
                            ) VALUES (
                                :pid,
                                :old_exec_id::uuid,
                                :new_exec_id::uuid,
                                :quantity,
                                :price,
                                :value,
                                :reason,
                                'manual',
                                :user_id
                            )
                        """),
                        {
                            "pid": position_id,
                            "old_exec_id": str(old_execution_id),
                            "new_exec_id": target_execution_id,
                            "quantity": abs(position.quantity),
                            "price": position.last_price or 0,
                            "value": abs(position.quantity) * (position.last_price or 0),
                            "reason": f"Position moved from order_service.strategy {old_strategy_id} to {target_strategy_id}",
                            "user_id": self.user_id
                        }
                    )
                    moved_counts["transfer_logged"] = True
            else:
                # Just move strategy_id (legacy behavior)
                await self.db.execute(
                    text("""
                        UPDATE order_service.positions
                        SET strategy_id = :new_sid, updated_at = NOW()
                        WHERE id = :pid AND user_id = :uid
                    """),
                    {"new_sid": target_strategy_id, "pid": position_id, "uid": self.user_id}
                )

            moved_counts["position"] = 1

            # 2. Move associated orders (by symbol, exchange, product_type, trading_day)
            if target_execution_id:
                # Also update execution_id for orders
                orders_result = await self.db.execute(
                    text("""
                        UPDATE order_service.orders
                        SET strategy_id = :new_sid,
                            execution_id = :new_exec_id::uuid,
                            updated_at = NOW()
                        WHERE user_id = :uid
                          AND trading_account_id = :account_id
                          AND symbol = :symbol
                          AND exchange = :exchange
                          AND product_type = :product_type
                          AND DATE(created_at) = :trading_day
                          AND (strategy_id = :old_sid OR strategy_id IS NULL)
                    """),
                    {
                        "new_sid": target_strategy_id,
                        "new_exec_id": target_execution_id,
                        "uid": self.user_id,
                        "account_id": self.trading_account_id,
                        "symbol": symbol,
                        "exchange": exchange,
                        "product_type": product_type,
                        "trading_day": trading_day,
                        "old_sid": old_strategy_id
                    }
                )
            else:
                # Legacy: only update strategy_id
                orders_result = await self.db.execute(
                    text("""
                        UPDATE order_service.orders
                        SET strategy_id = :new_sid, updated_at = NOW()
                        WHERE user_id = :uid
                          AND trading_account_id = :account_id
                          AND symbol = :symbol
                          AND exchange = :exchange
                          AND product_type = :product_type
                          AND DATE(created_at) = :trading_day
                          AND (strategy_id = :old_sid OR strategy_id IS NULL)
                    """),
                    {
                        "new_sid": target_strategy_id,
                        "uid": self.user_id,
                        "account_id": self.trading_account_id,
                        "symbol": symbol,
                        "exchange": exchange,
                        "product_type": product_type,
                        "trading_day": trading_day,
                        "old_sid": old_strategy_id
                    }
                )
            moved_counts["orders"] = orders_result.rowcount

            # 3. Move associated trades (by symbol, exchange, product_type, trading_day)
            if target_execution_id:
                # Also update execution_id for trades
                trades_result = await self.db.execute(
                    text("""
                        UPDATE order_service.trades
                        SET strategy_id = :new_sid,
                            execution_id = :new_exec_id::uuid
                        WHERE user_id = :uid
                          AND trading_account_id = :account_id
                          AND symbol = :symbol
                          AND exchange = :exchange
                          AND product_type = :product_type
                          AND DATE(trade_time) = :trading_day
                          AND (strategy_id = :old_sid OR strategy_id IS NULL)
                    """),
                    {
                        "new_sid": target_strategy_id,
                        "new_exec_id": target_execution_id,
                        "uid": self.user_id,
                        "account_id": self.trading_account_id,
                        "symbol": symbol,
                        "exchange": exchange,
                        "product_type": product_type,
                        "trading_day": trading_day,
                        "old_sid": old_strategy_id
                    }
                )
            else:
                # Legacy: only update strategy_id
                trades_result = await self.db.execute(
                    text("""
                        UPDATE order_service.trades
                        SET strategy_id = :new_sid
                        WHERE user_id = :uid
                          AND trading_account_id = :account_id
                          AND symbol = :symbol
                          AND exchange = :exchange
                          AND product_type = :product_type
                          AND DATE(trade_time) = :trading_day
                          AND (strategy_id = :old_sid OR strategy_id IS NULL)
                    """),
                    {
                        "new_sid": target_strategy_id,
                        "uid": self.user_id,
                        "account_id": self.trading_account_id,
                        "symbol": symbol,
                        "exchange": exchange,
                        "product_type": product_type,
                        "trading_day": trading_day,
                        "old_sid": old_strategy_id
                    }
                )
            moved_counts["trades"] = trades_result.rowcount

            await self.db.commit()

            # Invalidate cache
            await invalidate_position_cache(
                self.user_id,
                self.trading_account_id,
                position_id
            )

            logger.info(
                f"Moved position {position_id} ({symbol}) from order_service.strategy {old_strategy_id} "
                f"to {target_strategy_id}. Orders: {moved_counts['orders']}, Trades: {moved_counts['trades']}"
            )

            return moved_counts

        except HTTPException:
            raise
        except Exception as e:
            await self.db.rollback()
            logger.error(f"Failed to move position {position_id}: {e}")
            raise HTTPException(500, f"Failed to move position: {str(e)}")

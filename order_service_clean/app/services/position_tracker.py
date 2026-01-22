"""
Position Tracker Service

Tracks positions with weighted average price calculation and real-time P&L updates.
Updates order_service.positions table on every trade.
"""
import logging
from datetime import date
from decimal import Decimal
from typing import Optional
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ..models.position import Position
from ..models.trade import Trade

logger = logging.getLogger(__name__)


class PositionTracker:
    """Track positions with average price calculation"""

    def __init__(self, db: AsyncSession):
        """
        Initialize position tracker.

        Args:
            db: Database session
        """
        self.db = db

    async def update_position_from_trade(self, trade: Trade, last_price: Optional[Decimal] = None) -> Optional[Position]:
        """
        Update position when trade occurs.

        Calculates weighted average price for buy/sell sides and updates P&L.

        Args:
            trade: Trade object
            last_price: Optional current market price for unrealized P&L calculation

        Returns:
            Updated Position object or None if error
        """
        try:
            logger.info(
                f"Updating position from trade: {trade.transaction_type} {trade.quantity} "
                f"{trade.symbol} @ {trade.price}"
            )

            # Get or create position
            position = await self._get_or_create_position(
                strategy_id=trade.strategy_id,
                user_id=trade.user_id,
                trading_account_id=trade.trading_account_id,
                symbol=trade.symbol,
                exchange=trade.exchange,
                product_type=trade.product_type,
                trading_day=trade.trade_time.date() if trade.trade_time else date.today()
            )

            if not position:
                logger.error(f"Failed to get or create position for trade {trade.id}")
                return None

            # Update position based on transaction type
            if trade.transaction_type == "BUY":
                await self._update_buy_side(position, trade)
            elif trade.transaction_type == "SELL":
                await self._update_sell_side(position, trade)

            # Calculate quantity (net position)
            position.quantity = position.buy_quantity - position.sell_quantity

            # Update day/overnight quantities
            if position.product_type == "MIS":
                position.day_quantity = position.quantity
                position.overnight_quantity = 0
            else:  # CNC or NRML
                position.overnight_quantity = position.quantity
                position.day_quantity = 0

            # Calculate realized P&L if position is closed
            if position.buy_quantity > 0 and position.sell_quantity > 0:
                min_qty = min(position.buy_quantity, position.sell_quantity)
                avg_buy_price = position.buy_price or Decimal('0')
                avg_sell_price = position.sell_price or Decimal('0')
                position.realized_pnl = (avg_sell_price - avg_buy_price) * Decimal(min_qty)

            # Calculate unrealized P&L for open positions
            if last_price and position.quantity != 0:
                position.unrealized_pnl = await self._calculate_unrealized_pnl(
                    position, last_price
                )
                position.last_price = last_price
            elif position.quantity == 0:
                position.unrealized_pnl = Decimal('0')

            # Calculate total P&L
            position.total_pnl = position.realized_pnl + position.unrealized_pnl

            # Determine if position is still open
            position.is_open = (position.quantity != 0)

            # Commit changes
            await self.db.commit()
            await self.db.refresh(position)

            # Invalidate cache (Issue #419)
            from ..database.redis_client import invalidate_dashboard_cache, invalidate_positions_summary_cache
            try:
                await invalidate_dashboard_cache(trade.trading_account_id)
                await invalidate_positions_summary_cache(trade.trading_account_id)
                logger.debug(f"Invalidated cache for account {trade.trading_account_id}")
            except Exception as cache_error:
                # Non-critical - log and continue
                logger.warning(f"Failed to invalidate cache: {cache_error}")

            logger.info(
                f"✅ Position updated: {position.symbol} qty={position.quantity} "
                f"realized={position.realized_pnl} unrealized={position.unrealized_pnl}"
            )

            return position

        except Exception as e:
            logger.error(f"Error updating position from trade {trade.id}: {e}", exc_info=True)
            await self.db.rollback()
            return None

    async def _get_or_create_position(
        self,
        strategy_id: int,
        user_id: int,
        trading_account_id: int,
        symbol: str,
        exchange: str,
        product_type: str,
        trading_day: date
    ) -> Optional[Position]:
        """
        Get existing position or create new one.

        Args:
            strategy_id: Strategy ID
            user_id: User ID
            trading_account_id: Trading account ID
            symbol: Trading symbol
            exchange: Exchange
            product_type: Product type (CNC, MIS, NRML)
            trading_day: Trading day

        Returns:
            Position object or None if error
        """
        try:
            # Try to get existing position
            result = await self.db.execute(
                select(Position).where(
                    Position.user_id == user_id,
                    Position.symbol == symbol,
                    Position.product_type == product_type,
                    Position.trading_day == trading_day
                )
            )
            position = result.scalar_one_or_none()

            if position:
                return position

            # Create new position
            position = Position(
                strategy_id=strategy_id,
                user_id=user_id,
                trading_account_id=trading_account_id,
                symbol=symbol,
                exchange=exchange,
                product_type=product_type,
                trading_day=trading_day,
                quantity=0,
                overnight_quantity=0,
                day_quantity=0,
                buy_quantity=0,
                buy_value=Decimal('0'),
                buy_price=None,
                sell_quantity=0,
                sell_value=Decimal('0'),
                sell_price=None,
                realized_pnl=Decimal('0'),
                unrealized_pnl=Decimal('0'),
                total_pnl=Decimal('0'),
                is_open=True
            )

            self.db.add(position)
            await self.db.flush()  # Get ID without committing

            logger.debug(f"Created new position: {symbol} for strategy {strategy_id}")
            return position

        except Exception as e:
            logger.error(f"Error getting or creating position: {e}", exc_info=True)
            return None

    async def _update_buy_side(self, position: Position, trade: Trade):
        """
        Update buy side of position with weighted average price.

        Args:
            position: Position object to update
            trade: Buy trade
        """
        trade_value = Decimal(str(trade.price)) * Decimal(trade.quantity)

        # Calculate weighted average buy price
        old_total_value = position.buy_value
        old_total_qty = position.buy_quantity

        new_total_value = old_total_value + trade_value
        new_total_qty = old_total_qty + trade.quantity

        if new_total_qty > 0:
            position.buy_price = new_total_value / Decimal(new_total_qty)
        else:
            position.buy_price = Decimal('0')

        position.buy_quantity = new_total_qty
        position.buy_value = new_total_value

        logger.debug(
            f"Updated buy side: qty={position.buy_quantity} "
            f"avg_price={position.buy_price} value={position.buy_value}"
        )

    async def _update_sell_side(self, position: Position, trade: Trade):
        """
        Update sell side of position with weighted average price.

        Args:
            position: Position object to update
            trade: Sell trade
        """
        trade_value = Decimal(str(trade.price)) * Decimal(trade.quantity)

        # Calculate weighted average sell price
        old_total_value = position.sell_value
        old_total_qty = position.sell_quantity

        new_total_value = old_total_value + trade_value
        new_total_qty = old_total_qty + trade.quantity

        if new_total_qty > 0:
            position.sell_price = new_total_value / Decimal(new_total_qty)
        else:
            position.sell_price = Decimal('0')

        position.sell_quantity = new_total_qty
        position.sell_value = new_total_value

        logger.debug(
            f"Updated sell side: qty={position.sell_quantity} "
            f"avg_price={position.sell_price} value={position.sell_value}"
        )

    async def _calculate_unrealized_pnl(
        self,
        position: Position,
        current_price: Decimal
    ) -> Decimal:
        """
        Calculate unrealized P&L for open position.

        Args:
            position: Position object
            current_price: Current market price

        Returns:
            Unrealized P&L as Decimal
        """
        if position.quantity == 0:
            return Decimal('0')

        # Net position P&L = (current_price - avg_entry_price) * net_quantity
        if position.quantity > 0:  # Long position
            avg_entry_price = position.buy_price or Decimal('0')
            unrealized_pnl = (current_price - avg_entry_price) * Decimal(abs(position.quantity))
        else:  # Short position
            avg_entry_price = position.sell_price or Decimal('0')
            unrealized_pnl = (avg_entry_price - current_price) * Decimal(abs(position.quantity))

        return unrealized_pnl

    async def update_positions_with_market_prices(
        self,
        strategy_id: int,
        market_prices: dict
    ) -> int:
        """
        Update all open positions with current market prices.

        Useful for end-of-day or periodic mark-to-market calculations.

        Args:
            strategy_id: Strategy ID
            market_prices: Dict mapping symbol → current price

        Returns:
            Number of positions updated
        """
        try:
            result = await self.db.execute(
                select(Position).where(
                    Position.strategy_id == strategy_id,
                    Position.is_open == True,
                    Position.trading_day == date.today()
                )
            )
            positions = result.scalars().all()

            updated_count = 0

            for position in positions:
                if position.symbol in market_prices:
                    current_price = Decimal(str(market_prices[position.symbol]))
                    position.last_price = current_price

                    # Recalculate unrealized P&L
                    position.unrealized_pnl = await self._calculate_unrealized_pnl(
                        position, current_price
                    )
                    position.total_pnl = position.realized_pnl + position.unrealized_pnl

                    updated_count += 1

            await self.db.commit()

            logger.info(f"Updated {updated_count} positions with market prices for strategy {strategy_id}")
            return updated_count

        except Exception as e:
            logger.error(f"Error updating positions with market prices: {e}", exc_info=True)
            await self.db.rollback()
            return 0

    async def close_position(
        self,
        position_id: int,
        close_price: Decimal
    ) -> bool:
        """
        Close a position and finalize P&L.

        Args:
            position_id: Position ID
            close_price: Final closing price

        Returns:
            True if successful, False otherwise
        """
        try:
            result = await self.db.execute(
                select(Position).where(Position.id == position_id)
            )
            position = result.scalar_one_or_none()

            if not position:
                logger.error(f"Position {position_id} not found")
                return False

            # Calculate final realized P&L
            if position.buy_quantity > 0 and position.sell_quantity > 0:
                min_qty = min(position.buy_quantity, position.sell_quantity)
                avg_buy_price = position.buy_price or Decimal('0')
                avg_sell_price = position.sell_price or Decimal('0')
                position.realized_pnl = (avg_sell_price - avg_buy_price) * Decimal(min_qty)

            # Mark as closed
            position.is_open = False
            position.unrealized_pnl = Decimal('0')
            position.total_pnl = position.realized_pnl
            position.close_price = close_price

            await self.db.commit()

            logger.info(f"✅ Closed position {position_id}: realized P&L = {position.realized_pnl}")
            return True

        except Exception as e:
            logger.error(f"Error closing position {position_id}: {e}", exc_info=True)
            await self.db.rollback()
            return False

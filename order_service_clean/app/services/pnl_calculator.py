"""
P&L Calculator Service

Calculates realized and unrealized P&L for strategies from trades and positions.
Updates public.strategy_pnl_metrics table in real-time.
"""
import logging
from datetime import date, datetime
from decimal import Decimal
from typing import Dict, Optional, Tuple
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)


class PnLCalculator:
    """Calculate strategy P&L from trades and positions"""

    def __init__(self, db: AsyncSession):
        """
        Initialize P&L calculator.

        Args:
            db: Database session
        """
        self.db = db

    async def calculate_realized_pnl(self, strategy_id: int, trading_day: Optional[date] = None) -> Decimal:
        """
        Calculate realized P&L from closed positions.

        Realized P&L = Sum of (sell_value - buy_value) for closed positions

        Args:
            strategy_id: Strategy ID
            trading_day: Optional date filter (defaults to today)

        Returns:
            Realized P&L as Decimal
        """
        if trading_day is None:
            trading_day = date.today()

        try:
            result = await self.db.execute(
                text("""
                    SELECT COALESCE(SUM(realized_pnl), 0) as total_realized_pnl
                    FROM order_service.positions
                    WHERE strategy_id = :strategy_id
                      AND trading_day = :trading_day
                      AND is_open = false
                """),
                {"strategy_id": strategy_id, "trading_day": trading_day}
            )
            row = result.fetchone()
            realized_pnl = Decimal(str(row.total_realized_pnl)) if row else Decimal('0')

            logger.debug(f"Realized P&L for strategy {strategy_id}: {realized_pnl}")
            return realized_pnl

        except Exception as e:
            logger.error(f"Error calculating realized P&L for strategy {strategy_id}: {e}")
            return Decimal('0')

    async def calculate_unrealized_pnl(self, strategy_id: int, trading_day: Optional[date] = None) -> Decimal:
        """
        Calculate unrealized P&L from open positions.

        Unrealized P&L = Sum of unrealized_pnl from open positions

        Args:
            strategy_id: Strategy ID
            trading_day: Optional date filter (defaults to today)

        Returns:
            Unrealized P&L as Decimal
        """
        if trading_day is None:
            trading_day = date.today()

        try:
            result = await self.db.execute(
                text("""
                    SELECT COALESCE(SUM(unrealized_pnl), 0) as total_unrealized_pnl
                    FROM order_service.positions
                    WHERE strategy_id = :strategy_id
                      AND trading_day = :trading_day
                      AND is_open = true
                """),
                {"strategy_id": strategy_id, "trading_day": trading_day}
            )
            row = result.fetchone()
            unrealized_pnl = Decimal(str(row.total_unrealized_pnl)) if row else Decimal('0')

            logger.debug(f"Unrealized P&L for strategy {strategy_id}: {unrealized_pnl}")
            return unrealized_pnl

        except Exception as e:
            logger.error(f"Error calculating unrealized P&L for strategy {strategy_id}: {e}")
            return Decimal('0')

    async def calculate_trade_metrics(
        self,
        strategy_id: int,
        trading_day: Optional[date] = None
    ) -> Dict[str, int]:
        """
        Calculate trade metrics (total trades, winning trades, losing trades).

        Args:
            strategy_id: Strategy ID
            trading_day: Optional date filter (defaults to today)

        Returns:
            Dict with total_trades, winning_trades, losing_trades
        """
        if trading_day is None:
            trading_day = date.today()

        try:
            # Count trades
            result = await self.db.execute(
                text("""
                    SELECT COUNT(*) as total_trades
                    FROM order_service.trades
                    WHERE strategy_id = :strategy_id
                      AND DATE(trade_time) = :trading_day
                """),
                {"strategy_id": strategy_id, "trading_day": trading_day}
            )
            row = result.fetchone()
            total_trades = row.total_trades if row else 0

            # Count winning/losing positions (closed positions only)
            result = await self.db.execute(
                text("""
                    SELECT
                        COUNT(*) FILTER (WHERE realized_pnl > 0) as winning_trades,
                        COUNT(*) FILTER (WHERE realized_pnl < 0) as losing_trades
                    FROM order_service.positions
                    WHERE strategy_id = :strategy_id
                      AND trading_day = :trading_day
                      AND is_open = false
                """),
                {"strategy_id": strategy_id, "trading_day": trading_day}
            )
            row = result.fetchone()
            winning_trades = row.winning_trades if row else 0
            losing_trades = row.losing_trades if row else 0

            return {
                "total_trades": total_trades,
                "winning_trades": winning_trades,
                "losing_trades": losing_trades
            }

        except Exception as e:
            logger.error(f"Error calculating trade metrics for strategy {strategy_id}: {e}")
            return {"total_trades": 0, "winning_trades": 0, "losing_trades": 0}

    async def calculate_position_counts(
        self,
        strategy_id: int,
        trading_day: Optional[date] = None
    ) -> Dict[str, int]:
        """
        Calculate position counts (open positions, closed positions).

        Args:
            strategy_id: Strategy ID
            trading_day: Optional date filter (defaults to today)

        Returns:
            Dict with open_positions, closed_positions
        """
        if trading_day is None:
            trading_day = date.today()

        try:
            result = await self.db.execute(
                text("""
                    SELECT
                        COUNT(*) FILTER (WHERE is_open = true) as open_positions,
                        COUNT(*) FILTER (WHERE is_open = false) as closed_positions
                    FROM order_service.positions
                    WHERE strategy_id = :strategy_id
                      AND trading_day = :trading_day
                """),
                {"strategy_id": strategy_id, "trading_day": trading_day}
            )
            row = result.fetchone()

            return {
                "open_positions": row.open_positions if row else 0,
                "closed_positions": row.closed_positions if row else 0
            }

        except Exception as e:
            logger.error(f"Error calculating position counts for strategy {strategy_id}: {e}")
            return {"open_positions": 0, "closed_positions": 0}

    async def calculate_win_rate(self, winning_trades: int, losing_trades: int) -> Decimal:
        """
        Calculate win rate percentage.

        Args:
            winning_trades: Number of winning trades
            losing_trades: Number of losing trades

        Returns:
            Win rate as percentage (0-100)
        """
        total = winning_trades + losing_trades
        if total == 0:
            return Decimal('0')

        win_rate = (Decimal(winning_trades) / Decimal(total)) * Decimal('100')
        return win_rate.quantize(Decimal('0.01'))

    async def calculate_avg_position_size(
        self,
        strategy_id: int,
        trading_day: Optional[date] = None
    ) -> Decimal:
        """
        Calculate average position size (average capital per position).

        Args:
            strategy_id: Strategy ID
            trading_day: Optional date filter (defaults to today)

        Returns:
            Average position size as Decimal
        """
        if trading_day is None:
            trading_day = date.today()

        try:
            result = await self.db.execute(
                text("""
                    SELECT
                        COUNT(*) as position_count,
                        COALESCE(AVG(buy_value + sell_value), 0) as avg_size
                    FROM order_service.positions
                    WHERE strategy_id = :strategy_id
                      AND trading_day = :trading_day
                """),
                {"strategy_id": strategy_id, "trading_day": trading_day}
            )
            row = result.fetchone()
            return Decimal(str(row.avg_size)) if row and row.position_count > 0 else Decimal('0')

        except Exception as e:
            logger.error(f"Error calculating avg position size: {e}")
            return Decimal('0')

    async def calculate_capital_deployed(
        self,
        strategy_id: int,
        trading_day: Optional[date] = None
    ) -> Decimal:
        """
        Calculate total capital deployed (sum of buy values).

        Args:
            strategy_id: Strategy ID
            trading_day: Optional date filter (defaults to today)

        Returns:
            Total capital deployed as Decimal
        """
        if trading_day is None:
            trading_day = date.today()

        try:
            result = await self.db.execute(
                text("""
                    SELECT COALESCE(SUM(buy_value), 0) as total_deployed
                    FROM order_service.positions
                    WHERE strategy_id = :strategy_id
                      AND trading_day = :trading_day
                """),
                {"strategy_id": strategy_id, "trading_day": trading_day}
            )
            row = result.fetchone()
            return Decimal(str(row.total_deployed)) if row else Decimal('0')

        except Exception as e:
            logger.error(f"Error calculating capital deployed: {e}")
            return Decimal('0')

    async def calculate_max_drawdown(
        self,
        strategy_id: int,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None
    ) -> Decimal:
        """
        Calculate maximum drawdown from peak to trough.

        Args:
            strategy_id: Strategy ID
            start_date: Start date (defaults to 30 days ago)
            end_date: End date (defaults to today)

        Returns:
            Maximum drawdown as percentage
        """
        from datetime import timedelta

        if end_date is None:
            end_date = date.today()
        if start_date is None:
            start_date = end_date - timedelta(days=30)

        try:
            # Get daily cumulative P&L
            result = await self.db.execute(
                text("""
                    SELECT metric_date, cumulative_pnl
                    FROM public.strategy_pnl_metrics
                    WHERE strategy_id = :strategy_id
                      AND metric_date BETWEEN :start_date AND :end_date
                    ORDER BY metric_date ASC
                """),
                {"strategy_id": strategy_id, "start_date": start_date, "end_date": end_date}
            )
            rows = result.fetchall()

            if not rows or len(rows) < 2:
                return Decimal('0')

            # Calculate max drawdown
            peak = Decimal(str(rows[0].cumulative_pnl))
            max_dd = Decimal('0')

            for row in rows:
                cumulative = Decimal(str(row.cumulative_pnl))
                if cumulative > peak:
                    peak = cumulative

                if peak > 0:
                    drawdown = ((peak - cumulative) / peak) * Decimal('100')
                    if drawdown > max_dd:
                        max_dd = drawdown

            return max_dd.quantize(Decimal('0.01'))

        except Exception as e:
            logger.error(f"Error calculating max drawdown: {e}")
            return Decimal('0')

    async def calculate_roi_percent(
        self,
        strategy_id: int,
        trading_day: Optional[date] = None
    ) -> Decimal:
        """
        Calculate ROI percentage (return on investment).

        ROI% = (Total P&L / Capital Deployed) * 100

        Args:
            strategy_id: Strategy ID
            trading_day: Optional date filter (defaults to today)

        Returns:
            ROI as percentage
        """
        if trading_day is None:
            trading_day = date.today()

        try:
            total_pnl = await self.calculate_realized_pnl(strategy_id, trading_day) + \
                        await self.calculate_unrealized_pnl(strategy_id, trading_day)
            capital_deployed = await self.calculate_capital_deployed(strategy_id, trading_day)

            if capital_deployed == 0:
                return Decimal('0')

            roi = (total_pnl / capital_deployed) * Decimal('100')
            return roi.quantize(Decimal('0.01'))

        except Exception as e:
            logger.error(f"Error calculating ROI: {e}")
            return Decimal('0')

    async def calculate_max_consecutive_losses(
        self,
        strategy_id: int,
        trading_day: Optional[date] = None
    ) -> int:
        """
        Calculate maximum consecutive losing trades.

        Args:
            strategy_id: Strategy ID
            trading_day: Optional date filter (defaults to today)

        Returns:
            Maximum consecutive losses as integer
        """
        if trading_day is None:
            trading_day = date.today()

        try:
            # Get closed positions ordered by time
            result = await self.db.execute(
                text("""
                    SELECT realized_pnl
                    FROM order_service.positions
                    WHERE strategy_id = :strategy_id
                      AND trading_day = :trading_day
                      AND is_open = false
                    ORDER BY closed_at ASC
                """),
                {"strategy_id": strategy_id, "trading_day": trading_day}
            )
            rows = result.fetchall()

            if not rows:
                return 0

            max_consecutive = 0
            current_consecutive = 0

            for row in rows:
                if Decimal(str(row.realized_pnl)) < 0:
                    current_consecutive += 1
                    max_consecutive = max(max_consecutive, current_consecutive)
                else:
                    current_consecutive = 0

            return max_consecutive

        except Exception as e:
            logger.error(f"Error calculating max consecutive losses: {e}")
            return 0

    async def update_strategy_pnl_metrics(
        self,
        strategy_id: int,
        trading_day: Optional[date] = None
    ) -> bool:
        """
        Update public.strategy_pnl_metrics table with current P&L and metrics.

        This is the main method called after order completion to update all metrics.

        Args:
            strategy_id: Strategy ID
            trading_day: Optional date filter (defaults to today)

        Returns:
            True if successful, False otherwise
        """
        if trading_day is None:
            trading_day = date.today()

        try:
            logger.info(f"Updating P&L metrics for strategy {strategy_id} on {trading_day}")

            # Calculate all core metrics
            realized_pnl = await self.calculate_realized_pnl(strategy_id, trading_day)
            unrealized_pnl = await self.calculate_unrealized_pnl(strategy_id, trading_day)
            total_pnl = realized_pnl + unrealized_pnl

            trade_metrics = await self.calculate_trade_metrics(strategy_id, trading_day)
            position_counts = await self.calculate_position_counts(strategy_id, trading_day)

            win_rate = await self.calculate_win_rate(
                trade_metrics["winning_trades"],
                trade_metrics["losing_trades"]
            )

            # Calculate day P&L (difference from previous day's cumulative)
            previous_cumulative = await self._get_previous_cumulative_pnl(strategy_id, trading_day)
            day_pnl = total_pnl - previous_cumulative

            # Calculate additional performance metrics
            avg_position_size = await self.calculate_avg_position_size(strategy_id, trading_day)
            capital_deployed = await self.calculate_capital_deployed(strategy_id, trading_day)
            max_drawdown = await self.calculate_max_drawdown(strategy_id)
            roi_percent = await self.calculate_roi_percent(strategy_id, trading_day)
            max_consecutive_losses = await self.calculate_max_consecutive_losses(strategy_id, trading_day)

            # Note: margin_used would require broker API call - set to 0 for now
            margin_used = Decimal('0')

            # Note: sharpe_ratio and sortino_ratio require volatility calc - set to 0 for now
            # These should be calculated separately with sufficient historical data
            sharpe_ratio = Decimal('0')
            sortino_ratio = Decimal('0')

            # Update order_service.strategy_pnl_metrics table with ALL metrics
            await self.db.execute(
                text("""
                    INSERT INTO public.strategy_pnl_metrics (
                        strategy_id,
                        metric_date,
                        day_pnl,
                        cumulative_pnl,
                        realized_pnl,
                        unrealized_pnl,
                        open_positions,
                        closed_positions,
                        total_trades,
                        winning_trades,
                        losing_trades,
                        win_rate,
                        avg_position_size,
                        capital_deployed,
                        margin_used,
                        max_drawdown,
                        sharpe_ratio,
                        sortino_ratio,
                        max_consecutive_losses,
                        roi_percent,
                        last_calculated_at,
                        created_at,
                        updated_at
                    ) VALUES (
                        :strategy_id,
                        :metric_date,
                        :day_pnl,
                        :cumulative_pnl,
                        :realized_pnl,
                        :unrealized_pnl,
                        :open_positions,
                        :closed_positions,
                        :total_trades,
                        :winning_trades,
                        :losing_trades,
                        :win_rate,
                        :avg_position_size,
                        :capital_deployed,
                        :margin_used,
                        :max_drawdown,
                        :sharpe_ratio,
                        :sortino_ratio,
                        :max_consecutive_losses,
                        :roi_percent,
                        NOW(),
                        NOW(),
                        NOW()
                    )
                    ON CONFLICT (strategy_id, metric_date)
                    DO UPDATE SET
                        day_pnl = EXCLUDED.day_pnl,
                        cumulative_pnl = EXCLUDED.cumulative_pnl,
                        realized_pnl = EXCLUDED.realized_pnl,
                        unrealized_pnl = EXCLUDED.unrealized_pnl,
                        open_positions = EXCLUDED.open_positions,
                        closed_positions = EXCLUDED.closed_positions,
                        total_trades = EXCLUDED.total_trades,
                        winning_trades = EXCLUDED.winning_trades,
                        losing_trades = EXCLUDED.losing_trades,
                        win_rate = EXCLUDED.win_rate,
                        avg_position_size = EXCLUDED.avg_position_size,
                        capital_deployed = EXCLUDED.capital_deployed,
                        margin_used = EXCLUDED.margin_used,
                        max_drawdown = EXCLUDED.max_drawdown,
                        sharpe_ratio = EXCLUDED.sharpe_ratio,
                        sortino_ratio = EXCLUDED.sortino_ratio,
                        max_consecutive_losses = EXCLUDED.max_consecutive_losses,
                        roi_percent = EXCLUDED.roi_percent,
                        last_calculated_at = NOW(),
                        updated_at = NOW()
                """),
                {
                    "strategy_id": strategy_id,
                    "metric_date": trading_day,
                    "day_pnl": float(day_pnl),
                    "cumulative_pnl": float(total_pnl),
                    "realized_pnl": float(realized_pnl),
                    "unrealized_pnl": float(unrealized_pnl),
                    "open_positions": position_counts["open_positions"],
                    "closed_positions": position_counts["closed_positions"],
                    "total_trades": trade_metrics["total_trades"],
                    "winning_trades": trade_metrics["winning_trades"],
                    "losing_trades": trade_metrics["losing_trades"],
                    "win_rate": float(win_rate),
                    "avg_position_size": float(avg_position_size),
                    "capital_deployed": float(capital_deployed),
                    "margin_used": float(margin_used),
                    "max_drawdown": float(max_drawdown),
                    "sharpe_ratio": float(sharpe_ratio),
                    "sortino_ratio": float(sortino_ratio),
                    "max_consecutive_losses": max_consecutive_losses,
                    "roi_percent": float(roi_percent),
                }
            )

            await self.db.commit()

            logger.info(
                f"✅ Updated P&L metrics for strategy {strategy_id}: "
                f"Realized={realized_pnl}, Unrealized={unrealized_pnl}, Total={total_pnl}"
            )
            return True

        except Exception as e:
            logger.error(f"Error updating P&L metrics for strategy {strategy_id}: {e}", exc_info=True)
            await self.db.rollback()
            return False

    async def _get_previous_cumulative_pnl(self, strategy_id: int, trading_day: date) -> Decimal:
        """
        Get previous day's cumulative P&L.

        Args:
            strategy_id: Strategy ID
            trading_day: Current trading day

        Returns:
            Previous cumulative P&L as Decimal (0 if not found)
        """
        try:
            result = await self.db.execute(
                text("""
                    SELECT cumulative_pnl
                    FROM public.strategy_pnl_metrics
                    WHERE strategy_id = :strategy_id
                      AND metric_date < :trading_day
                    ORDER BY metric_date DESC
                    LIMIT 1
                """),
                {"strategy_id": strategy_id, "trading_day": trading_day}
            )
            row = result.fetchone()
            return Decimal(str(row.cumulative_pnl)) if row else Decimal('0')

        except Exception as e:
            logger.error(f"Error getting previous cumulative P&L: {e}")
            return Decimal('0')

    async def get_strategy_pnl_summary(self, strategy_id: int) -> Dict:
        """
        Get current P&L summary for a strategy (today's metrics).

        Args:
            strategy_id: Strategy ID

        Returns:
            Dict with P&L summary
        """
        trading_day = date.today()

        realized_pnl = await self.calculate_realized_pnl(strategy_id, trading_day)
        unrealized_pnl = await self.calculate_unrealized_pnl(strategy_id, trading_day)
        trade_metrics = await self.calculate_trade_metrics(strategy_id, trading_day)
        position_counts = await self.calculate_position_counts(strategy_id, trading_day)
        win_rate = await self.calculate_win_rate(
            trade_metrics["winning_trades"],
            trade_metrics["losing_trades"]
        )

        return {
            "strategy_id": strategy_id,
            "trading_day": trading_day.isoformat(),
            "realized_pnl": float(realized_pnl),
            "unrealized_pnl": float(unrealized_pnl),
            "total_pnl": float(realized_pnl + unrealized_pnl),
            "open_positions": position_counts["open_positions"],
            "closed_positions": position_counts["closed_positions"],
            "total_trades": trade_metrics["total_trades"],
            "winning_trades": trade_metrics["winning_trades"],
            "losing_trades": trade_metrics["losing_trades"],
            "win_rate": float(win_rate),
        }

    # ==================================================================================
    # EXECUTION-AWARE P&L METHODS (Unified Execution Architecture)
    # ==================================================================================
    # These methods calculate P&L at the execution level for proper attribution
    # between manual (user_managed) and algorithmic (algo_managed) trading.
    #
    # Key attribution rules:
    # - Realized P&L: Attributed to entry_execution_id (who opened the position)
    # - Unrealized P&L: Attributed to execution_id (current owner)
    # - Position transfers: Tracked separately for audit trail
    # ==================================================================================

    async def calculate_execution_realized_pnl(
        self,
        execution_id: str,
        trading_day: Optional[date] = None
    ) -> Decimal:
        """
        Calculate realized P&L for a specific execution.

        Uses entry_execution_id to attribute P&L to the execution that opened
        the position, even if it was later transferred to another execution.

        Args:
            execution_id: Execution UUID
            trading_day: Optional date filter (defaults to today)

        Returns:
            Realized P&L as Decimal
        """
        if trading_day is None:
            trading_day = date.today()

        try:
            result = await self.db.execute(
                text("""
                    SELECT COALESCE(SUM(realized_pnl), 0) as total_realized_pnl
                    FROM order_service.positions
                    WHERE entry_execution_id = :execution_id
                      AND trading_day = :trading_day
                      AND is_open = false
                """),
                {"execution_id": execution_id, "trading_day": trading_day}
            )
            row = result.fetchone()
            realized_pnl = Decimal(str(row.total_realized_pnl)) if row else Decimal('0')

            logger.debug(f"Execution {execution_id} realized P&L: {realized_pnl}")
            return realized_pnl

        except Exception as e:
            logger.error(f"Error calculating execution realized P&L for {execution_id}: {e}")
            return Decimal('0')

    async def calculate_execution_unrealized_pnl(
        self,
        execution_id: str,
        trading_day: Optional[date] = None
    ) -> Decimal:
        """
        Calculate unrealized P&L for a specific execution.

        Uses execution_id (current owner) to attribute unrealized P&L to whoever
        currently holds the position.

        Args:
            execution_id: Execution UUID
            trading_day: Optional date filter (defaults to today)

        Returns:
            Unrealized P&L as Decimal
        """
        if trading_day is None:
            trading_day = date.today()

        try:
            result = await self.db.execute(
                text("""
                    SELECT COALESCE(SUM(unrealized_pnl), 0) as total_unrealized_pnl
                    FROM order_service.positions
                    WHERE execution_id = :execution_id
                      AND trading_day = :trading_day
                      AND is_open = true
                """),
                {"execution_id": execution_id, "trading_day": trading_day}
            )
            row = result.fetchone()
            unrealized_pnl = Decimal(str(row.total_unrealized_pnl)) if row else Decimal('0')

            logger.debug(f"Execution {execution_id} unrealized P&L: {unrealized_pnl}")
            return unrealized_pnl

        except Exception as e:
            logger.error(f"Error calculating execution unrealized P&L for {execution_id}: {e}")
            return Decimal('0')

    async def calculate_execution_position_counts(
        self,
        execution_id: str,
        trading_day: Optional[date] = None
    ) -> Dict[str, int]:
        """
        Calculate position counts for a specific execution.

        Tracks:
        - positions_opened: Positions where entry_execution_id = execution_id
        - positions_owned: Positions where execution_id = execution_id (current owner)
        - positions_transferred_in: Received from other executions
        - positions_transferred_out: Sent to other executions

        Args:
            execution_id: Execution UUID
            trading_day: Optional date filter (defaults to today)

        Returns:
            Dict with position counts
        """
        if trading_day is None:
            trading_day = date.today()

        try:
            # Count positions opened by this execution
            result = await self.db.execute(
                text("""
                    SELECT
                        COUNT(*) FILTER (WHERE is_open = true) as open_positions,
                        COUNT(*) FILTER (WHERE is_open = false) as closed_positions
                    FROM order_service.positions
                    WHERE entry_execution_id = :execution_id
                      AND trading_day = :trading_day
                """),
                {"execution_id": execution_id, "trading_day": trading_day}
            )
            row = result.fetchone()
            positions_opened = (row.open_positions + row.closed_positions) if row else 0
            open_positions = row.open_positions if row else 0
            closed_positions = row.closed_positions if row else 0

            # Count positions currently owned (may differ from opened if transferred)
            result = await self.db.execute(
                text("""
                    SELECT COUNT(*) as positions_owned
                    FROM order_service.positions
                    WHERE execution_id = :execution_id
                      AND trading_day = :trading_day
                      AND is_open = true
                """),
                {"execution_id": execution_id, "trading_day": trading_day}
            )
            row = result.fetchone()
            positions_owned = row.positions_owned if row else 0

            # Count transfers IN (positions received from other executions)
            result = await self.db.execute(
                text("""
                    SELECT COUNT(*) as transferred_in
                    FROM order_service.position_transfers
                    WHERE target_execution_id = :execution_id
                      AND DATE(transferred_at) = :trading_day
                """),
                {"execution_id": execution_id, "trading_day": trading_day}
            )
            row = result.fetchone()
            positions_transferred_in = row.transferred_in if row else 0

            # Count transfers OUT (positions sent to other executions)
            result = await self.db.execute(
                text("""
                    SELECT COUNT(*) as transferred_out
                    FROM order_service.position_transfers
                    WHERE source_execution_id = :execution_id
                      AND DATE(transferred_at) = :trading_day
                """),
                {"execution_id": execution_id, "trading_day": trading_day}
            )
            row = result.fetchone()
            positions_transferred_out = row.transferred_out if row else 0

            return {
                "positions_opened": positions_opened,
                "positions_owned": positions_owned,
                "open_positions": open_positions,
                "closed_positions": closed_positions,
                "positions_transferred_in": positions_transferred_in,
                "positions_transferred_out": positions_transferred_out,
            }

        except Exception as e:
            logger.error(f"Error calculating execution position counts for {execution_id}: {e}")
            return {
                "positions_opened": 0,
                "positions_owned": 0,
                "open_positions": 0,
                "closed_positions": 0,
                "positions_transferred_in": 0,
                "positions_transferred_out": 0,
            }

    async def calculate_execution_trade_metrics(
        self,
        execution_id: str,
        trading_day: Optional[date] = None
    ) -> Dict[str, int]:
        """
        Calculate trade metrics for a specific execution.

        Args:
            execution_id: Execution UUID
            trading_day: Optional date filter (defaults to today)

        Returns:
            Dict with total_trades, winning_trades, losing_trades
        """
        if trading_day is None:
            trading_day = date.today()

        try:
            # Count trades
            result = await self.db.execute(
                text("""
                    SELECT COUNT(*) as total_trades
                    FROM order_service.trades
                    WHERE execution_id = :execution_id
                      AND DATE(trade_time) = :trading_day
                """),
                {"execution_id": execution_id, "trading_day": trading_day}
            )
            row = result.fetchone()
            total_trades = row.total_trades if row else 0

            # Count winning/losing positions (use entry_execution_id for attribution)
            result = await self.db.execute(
                text("""
                    SELECT
                        COUNT(*) FILTER (WHERE realized_pnl > 0) as winning_trades,
                        COUNT(*) FILTER (WHERE realized_pnl < 0) as losing_trades
                    FROM order_service.positions
                    WHERE entry_execution_id = :execution_id
                      AND trading_day = :trading_day
                      AND is_open = false
                """),
                {"execution_id": execution_id, "trading_day": trading_day}
            )
            row = result.fetchone()
            winning_trades = row.winning_trades if row else 0
            losing_trades = row.losing_trades if row else 0

            return {
                "total_trades": total_trades,
                "winning_trades": winning_trades,
                "losing_trades": losing_trades
            }

        except Exception as e:
            logger.error(f"Error calculating execution trade metrics for {execution_id}: {e}")
            return {"total_trades": 0, "winning_trades": 0, "losing_trades": 0}

    async def get_execution_pnl_summary(self, execution_id: str, trading_day: Optional[date] = None) -> Dict:
        """
        Get complete P&L summary for an execution (stateless computation).

        This is the main method for batch P&L calculation API endpoint.
        Returns all metrics without storing them.

        Args:
            execution_id: Execution UUID
            trading_day: Optional date filter (defaults to today)

        Returns:
            Dict with complete P&L summary
        """
        if trading_day is None:
            trading_day = date.today()

        realized_pnl = await self.calculate_execution_realized_pnl(execution_id, trading_day)
        unrealized_pnl = await self.calculate_execution_unrealized_pnl(execution_id, trading_day)
        position_counts = await self.calculate_execution_position_counts(execution_id, trading_day)
        trade_metrics = await self.calculate_execution_trade_metrics(execution_id, trading_day)

        win_rate = await self.calculate_win_rate(
            trade_metrics["winning_trades"],
            trade_metrics["losing_trades"]
        )

        return {
            "execution_id": execution_id,
            "trading_day": trading_day.isoformat(),
            "realized_pnl": float(realized_pnl),
            "unrealized_pnl": float(unrealized_pnl),
            "total_pnl": float(realized_pnl + unrealized_pnl),
            "positions_opened": position_counts["positions_opened"],
            "positions_owned": position_counts["positions_owned"],
            "open_positions": position_counts["open_positions"],
            "closed_positions": position_counts["closed_positions"],
            "positions_transferred_in": position_counts["positions_transferred_in"],
            "positions_transferred_out": position_counts["positions_transferred_out"],
            "total_trades": trade_metrics["total_trades"],
            "winning_trades": trade_metrics["winning_trades"],
            "losing_trades": trade_metrics["losing_trades"],
            "win_rate": float(win_rate),
        }

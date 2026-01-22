"""
Strategy Service Business Logic (Read-Only + Metrics)

Provides read-only access to strategies for P&L tracking and metrics calculation.

ARCHITECTURE:
- Strategies are owned by backend or algo_engine (CRUD operations)
- order_service has read-only access for P&L calculation and portfolio linking
- signal_service computes technical indicators, Greeks, and metrics (NOT strategy management)
"""
import logging
from datetime import datetime, date
from typing import List, Optional, Dict, Any
from sqlalchemy import select, and_, func, text
from sqlalchemy.ext.asyncio import AsyncSession
from fastapi import HTTPException

from ..models.strategy import Strategy
from ..models.portfolio import PortfolioStrategy
from decimal import Decimal

logger = logging.getLogger(__name__)


class StrategyService:
    """Strategy query and metrics service"""

    def __init__(self, db: AsyncSession, user_id: int):
        """
        Initialize strategy service.

        Args:
            db: Database session
            user_id: User ID from JWT token
        """
        self.db = db
        self.user_id = user_id

    async def list_strategies(
        self,
        trading_account_id: Optional[str] = None,
        state: Optional[str] = None,
        mode: Optional[str] = None,
        is_active: Optional[bool] = None,
        include_default: bool = True,
        limit: int = 100,
        offset: int = 0
    ) -> List[Strategy]:
        """
        List user's strategies.

        Args:
            trading_account_id: Filter by trading account (optional)
            state: Filter by state (created, active, closed, etc.)
            mode: Filter by mode (paper, live)
            is_active: Filter by active status
            include_default: Whether to include default strategies
            limit: Maximum number of strategies to return
            offset: Number of strategies to skip

        Returns:
            List of Strategy objects
        """
        # Base query - filter by user_id
        query = select(Strategy).where(Strategy.user_id == self.user_id)

        # Apply filters
        if trading_account_id:
            query = query.where(Strategy.trading_account_id == trading_account_id)

        if state:
            query = query.where(Strategy.state == state)

        if mode:
            query = query.where(Strategy.mode == mode)

        if is_active is not None:
            query = query.where(Strategy.is_active == is_active)

        if not include_default:
            query = query.where(Strategy.is_default == False)

        # Order by default first, then creation date
        query = query.order_by(Strategy.is_default.desc(), Strategy.created_at.desc())
        query = query.limit(limit).offset(offset)

        result = await self.db.execute(query)
        strategies = result.scalars().all()

        logger.debug(f"Retrieved {len(strategies)} strategies for user {self.user_id}")
        return list(strategies)

    async def get_strategy(self, strategy_id: int) -> Strategy:
        """
        Get strategy by ID.

        Args:
            strategy_id: Strategy ID

        Returns:
            Strategy object

        Raises:
            HTTPException: If strategy not found or user doesn't have access
        """
        result = await self.db.execute(
            select(Strategy).where(
                and_(
                    Strategy.id == strategy_id,
                    Strategy.user_id == self.user_id
                )
            )
        )
        strategy = result.scalar_one_or_none()

        if not strategy:
            raise HTTPException(
                404,
                f"Strategy {strategy_id} not found for user {self.user_id}"
            )

        return strategy

    async def get_strategy_metrics(
        self,
        strategy_id: int,
        trading_day: Optional[date] = None
    ) -> Dict[str, Any]:
        """
        Calculate P&L metrics for a strategy.

        Aggregates positions, orders, and trades linked to this strategy.

        Args:
            strategy_id: Strategy ID
            trading_day: Filter by trading day (default: today)

        Returns:
            Dictionary with strategy metrics
        """
        # Verify strategy exists and belongs to user
        strategy = await self.get_strategy(strategy_id)

        # Default to today if not specified
        if trading_day is None:
            trading_day = date.today()

        # Aggregate positions for this strategy
        positions_result = await self.db.execute(
            text("""
                SELECT
                    COUNT(*) as position_count,
                    SUM(CASE WHEN is_open = true THEN 1 ELSE 0 END) as open_positions,
                    SUM(realized_pnl) as total_realized_pnl,
                    SUM(unrealized_pnl) as total_unrealized_pnl,
                    SUM(total_pnl) as total_pnl,
                    SUM(total_charges) as total_charges,
                    SUM(net_pnl) as net_pnl
                FROM order_service.positions
                WHERE strategy_id = :strategy_id
                  AND user_id = :user_id
                  AND trading_day = :trading_day
            """),
            {
                "strategy_id": strategy_id,
                "user_id": self.user_id,
                "trading_day": trading_day
            }
        )
        positions = positions_result.fetchone()

        # Aggregate orders for this strategy
        orders_result = await self.db.execute(
            text("""
                SELECT
                    COUNT(*) as total_orders,
                    SUM(CASE WHEN status = 'COMPLETE' THEN 1 ELSE 0 END) as completed_orders,
                    SUM(CASE WHEN status = 'CANCELLED' THEN 1 ELSE 0 END) as cancelled_orders,
                    SUM(CASE WHEN status IN ('OPEN', 'PENDING') THEN 1 ELSE 0 END) as pending_orders,
                    SUM(filled_quantity) as total_filled_quantity
                FROM order_service.orders
                WHERE strategy_id = :strategy_id
                  AND user_id = :user_id
                  AND DATE(created_at) = :trading_day
            """),
            {
                "strategy_id": strategy_id,
                "user_id": self.user_id,
                "trading_day": trading_day
            }
        )
        orders = orders_result.fetchone()

        # Aggregate trades for this strategy
        trades_result = await self.db.execute(
            text("""
                SELECT
                    COUNT(*) as total_trades,
                    SUM(CASE WHEN transaction_type = 'BUY' THEN quantity ELSE 0 END) as buy_quantity,
                    SUM(CASE WHEN transaction_type = 'SELL' THEN quantity ELSE 0 END) as sell_quantity,
                    SUM(CASE WHEN transaction_type = 'BUY' THEN trade_value ELSE 0 END) as buy_value,
                    SUM(CASE WHEN transaction_type = 'SELL' THEN trade_value ELSE 0 END) as sell_value
                FROM order_service.trades
                WHERE strategy_id = :strategy_id
                  AND user_id = :user_id
                  AND DATE(trade_time) = :trading_day
            """),
            {
                "strategy_id": strategy_id,
                "user_id": self.user_id,
                "trading_day": trading_day
            }
        )
        trades = trades_result.fetchone()

        # Build metrics response
        return {
            "strategy_id": strategy_id,
            "strategy_name": strategy.display_name or strategy.name,
            "trading_day": trading_day.isoformat(),
            "positions": {
                "total": positions.position_count or 0,
                "open": positions.open_positions or 0,
                "closed": (positions.position_count or 0) - (positions.open_positions or 0)
            },
            "pnl": {
                "realized": float(positions.total_realized_pnl or 0),
                "unrealized": float(positions.total_unrealized_pnl or 0),
                "total": float(positions.total_pnl or 0),
                "charges": float(positions.total_charges or 0),
                "net": float(positions.net_pnl or 0)
            },
            "orders": {
                "total": orders.total_orders or 0,
                "completed": orders.completed_orders or 0,
                "cancelled": orders.cancelled_orders or 0,
                "pending": orders.pending_orders or 0,
                "filled_quantity": orders.total_filled_quantity or 0
            },
            "trades": {
                "total": trades.total_trades or 0,
                "buy_quantity": trades.buy_quantity or 0,
                "sell_quantity": trades.sell_quantity or 0,
                "buy_value": float(trades.buy_value or 0),
                "sell_value": float(trades.sell_value or 0)
            }
        }

    async def link_to_portfolio(
        self,
        portfolio_id: int,
        strategy_id: int
    ) -> PortfolioStrategy:
        """
        Link a strategy to a portfolio.

        Args:
            portfolio_id: Portfolio ID
            strategy_id: Strategy ID to link

        Returns:
            Created PortfolioStrategy object

        Raises:
            HTTPException: If portfolio/strategy not found or already linked
        """
        # Verify portfolio exists and belongs to user
        from .portfolio_service import PortfolioService
        portfolio_service = PortfolioService(self.db, self.user_id)
        await portfolio_service.get_portfolio(portfolio_id)

        # Verify strategy exists and belongs to user
        await self.get_strategy(strategy_id)

        # Check if already linked
        existing = await self.db.execute(
            select(PortfolioStrategy).where(
                and_(
                    PortfolioStrategy.portfolio_id == portfolio_id,
                    PortfolioStrategy.strategy_id == strategy_id
                )
            )
        )
        if existing.scalar_one_or_none():
            raise HTTPException(
                400,
                f"Strategy {strategy_id} is already linked to portfolio {portfolio_id}"
            )

        # Create link
        portfolio_strategy = PortfolioStrategy(
            portfolio_id=portfolio_id,
            strategy_id=strategy_id,
            added_at=datetime.utcnow()
        )

        self.db.add(portfolio_strategy)
        await self.db.commit()
        await self.db.refresh(portfolio_strategy)

        logger.info(
            f"Linked strategy {strategy_id} to portfolio {portfolio_id}"
        )

        return portfolio_strategy

    async def unlink_from_portfolio(
        self,
        portfolio_id: int,
        strategy_id: int
    ) -> Dict[str, Any]:
        """
        Unlink a strategy from a portfolio.

        Args:
            portfolio_id: Portfolio ID
            strategy_id: Strategy ID to unlink

        Returns:
            Dictionary with unlink status

        Raises:
            HTTPException: If link not found
        """
        # Verify portfolio exists and belongs to user
        from .portfolio_service import PortfolioService
        portfolio_service = PortfolioService(self.db, self.user_id)
        await portfolio_service.get_portfolio(portfolio_id)

        # Find link
        result = await self.db.execute(
            select(PortfolioStrategy).where(
                and_(
                    PortfolioStrategy.portfolio_id == portfolio_id,
                    PortfolioStrategy.strategy_id == strategy_id
                )
            )
        )
        link = result.scalar_one_or_none()

        if not link:
            raise HTTPException(
                404,
                f"Strategy {strategy_id} is not linked to portfolio {portfolio_id}"
            )

        # Delete link
        await self.db.delete(link)
        await self.db.commit()

        logger.info(
            f"Unlinked strategy {strategy_id} from portfolio {portfolio_id}"
        )

        return {
            "portfolio_id": portfolio_id,
            "strategy_id": strategy_id,
            "unlinked_at": datetime.utcnow().isoformat()
        }

    async def get_portfolio_strategies(self, portfolio_id: int) -> List[int]:
        """
        Get all strategies linked to a portfolio.

        Args:
            portfolio_id: Portfolio ID

        Returns:
            List of strategy IDs
        """
        # Verify portfolio exists and belongs to user
        from .portfolio_service import PortfolioService
        portfolio_service = PortfolioService(self.db, self.user_id)
        await portfolio_service.get_portfolio(portfolio_id)

        result = await self.db.execute(
            select(PortfolioStrategy.strategy_id).where(
                PortfolioStrategy.portfolio_id == portfolio_id
            )
        )
        strategy_ids = result.scalars().all()

        return list(strategy_ids)

"""
Portfolio Service Business Logic

Handles portfolio management, account linking, and aggregated metrics.
"""
import logging
from datetime import datetime
from typing import List, Optional, Dict, Any
from sqlalchemy import select, and_, func, text
from sqlalchemy.ext.asyncio import AsyncSession
from fastapi import HTTPException

from ..models.portfolio import Portfolio, PortfolioAccount, PortfolioStrategy

logger = logging.getLogger(__name__)


class PortfolioService:
    """Portfolio management service"""

    def __init__(self, db: AsyncSession, user_id: int):
        """
        Initialize portfolio service.

        Args:
            db: Database session
            user_id: User ID from JWT token
        """
        self.db = db
        self.user_id = user_id

    async def list_portfolios(
        self,
        include_default: bool = True,
        limit: int = 100,
        offset: int = 0
    ) -> List[Portfolio]:
        """
        List user's portfolios.

        Args:
            include_default: Whether to include default portfolio (default: True)
            limit: Maximum number of portfolios to return
            offset: Number of portfolios to skip

        Returns:
            List of Portfolio objects
        """
        query = select(Portfolio).where(Portfolio.user_id == self.user_id)

        if not include_default:
            query = query.where(Portfolio.is_default == False)

        query = query.order_by(Portfolio.is_default.desc(), Portfolio.created_at.desc())
        query = query.limit(limit).offset(offset)

        result = await self.db.execute(query)
        portfolios = result.scalars().all()

        logger.debug(f"Retrieved {len(portfolios)} portfolios for user {self.user_id}")
        return list(portfolios)

    async def get_portfolio(self, portfolio_id: int) -> Portfolio:
        """
        Get portfolio by ID.

        Args:
            portfolio_id: Portfolio ID

        Returns:
            Portfolio object

        Raises:
            HTTPException: If portfolio not found or user doesn't have access
        """
        result = await self.db.execute(
            select(Portfolio).where(
                and_(
                    Portfolio.portfolio_id == portfolio_id,
                    Portfolio.user_id == self.user_id
                )
            )
        )
        portfolio = result.scalar_one_or_none()

        if not portfolio:
            raise HTTPException(
                404,
                f"Portfolio {portfolio_id} not found for user {self.user_id}"
            )

        return portfolio

    async def create_portfolio(
        self,
        portfolio_name: str,
        description: Optional[str] = None,
        is_default: bool = False
    ) -> Portfolio:
        """
        Create a new portfolio.

        Args:
            portfolio_name: Display name for the portfolio
            description: Optional description
            is_default: Whether this is the default portfolio

        Returns:
            Created Portfolio object

        Raises:
            HTTPException: If default portfolio already exists or name is taken
        """
        # Check if name is already taken
        existing = await self.db.execute(
            select(Portfolio).where(
                and_(
                    Portfolio.user_id == self.user_id,
                    Portfolio.portfolio_name == portfolio_name
                )
            )
        )
        if existing.scalar_one_or_none():
            raise HTTPException(
                400,
                f"Portfolio with name '{portfolio_name}' already exists"
            )

        # If marking as default, check if default already exists
        if is_default:
            existing_default = await self.db.execute(
                select(Portfolio).where(
                    and_(
                        Portfolio.user_id == self.user_id,
                        Portfolio.is_default == True
                    )
                )
            )
            if existing_default.scalar_one_or_none():
                raise HTTPException(
                    400,
                    "A default portfolio already exists. Please update the existing default portfolio or create a non-default portfolio."
                )

        # Create portfolio
        portfolio = Portfolio(
            user_id=self.user_id,
            portfolio_name=portfolio_name,
            description=description,
            is_default=is_default,
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow()
        )

        self.db.add(portfolio)
        await self.db.commit()
        await self.db.refresh(portfolio)

        logger.info(
            f"Created portfolio {portfolio.portfolio_id} ('{portfolio_name}') "
            f"for user {self.user_id}, is_default={is_default}"
        )

        return portfolio

    async def update_portfolio(
        self,
        portfolio_id: int,
        portfolio_name: Optional[str] = None,
        description: Optional[str] = None,
        is_default: Optional[bool] = None
    ) -> Portfolio:
        """
        Update portfolio details.

        Args:
            portfolio_id: Portfolio ID
            portfolio_name: New name (optional)
            description: New description (optional)
            is_default: New default status (optional)

        Returns:
            Updated Portfolio object

        Raises:
            HTTPException: If portfolio not found or validation fails
        """
        # Get existing portfolio
        portfolio = await self.get_portfolio(portfolio_id)

        # Can't modify default portfolio's is_default flag
        if portfolio.is_default and is_default is False:
            raise HTTPException(
                400,
                "Cannot remove default status from default portfolio. "
                "Create a new default portfolio instead."
            )

        # Check if new name is taken
        if portfolio_name and portfolio_name != portfolio.portfolio_name:
            existing = await self.db.execute(
                select(Portfolio).where(
                    and_(
                        Portfolio.user_id == self.user_id,
                        Portfolio.portfolio_name == portfolio_name
                    )
                )
            )
            if existing.scalar_one_or_none():
                raise HTTPException(
                    400,
                    f"Portfolio with name '{portfolio_name}' already exists"
                )

        # If marking as default, unmark existing default
        if is_default and not portfolio.is_default:
            await self.db.execute(
                text("""
                    UPDATE order_service.portfolios
                    SET is_default = false, updated_at = NOW()
                    WHERE user_id = :user_id AND is_default = true
                """),
                {"user_id": self.user_id}
            )

        # Update fields
        if portfolio_name:
            portfolio.portfolio_name = portfolio_name
        if description is not None:
            portfolio.description = description
        if is_default is not None:
            portfolio.is_default = is_default

        portfolio.updated_at = datetime.utcnow()

        await self.db.commit()
        await self.db.refresh(portfolio)

        logger.info(f"Updated portfolio {portfolio_id} for user {self.user_id}")

        return portfolio

    async def delete_portfolio(self, portfolio_id: int) -> Dict[str, Any]:
        """
        Delete a custom portfolio.

        Args:
            portfolio_id: Portfolio ID

        Returns:
            Dictionary with deletion statistics

        Raises:
            HTTPException: If portfolio not found or is default portfolio
        """
        portfolio = await self.get_portfolio(portfolio_id)

        # Cannot delete default portfolio
        if portfolio.is_default:
            raise HTTPException(
                400,
                "Cannot delete default portfolio. Please create a new default portfolio first."
            )

        # Count linked accounts and strategies
        account_count = await self.db.execute(
            select(func.count()).select_from(PortfolioAccount).where(
                PortfolioAccount.portfolio_id == portfolio_id
            )
        )
        accounts_removed = account_count.scalar()

        strategy_count = await self.db.execute(
            select(func.count()).select_from(PortfolioStrategy).where(
                PortfolioStrategy.portfolio_id == portfolio_id
            )
        )
        strategies_removed = strategy_count.scalar()

        # Delete portfolio (CASCADE will remove linked accounts and strategies)
        await self.db.delete(portfolio)
        await self.db.commit()

        logger.info(
            f"Deleted portfolio {portfolio_id} for user {self.user_id}. "
            f"Removed {accounts_removed} accounts and {strategies_removed} strategies."
        )

        return {
            "portfolio_id": portfolio_id,
            "accounts_removed": accounts_removed,
            "strategies_removed": strategies_removed
        }

    async def link_account(
        self,
        portfolio_id: int,
        trading_account_id: str
    ) -> PortfolioAccount:
        """
        Link a trading account to a portfolio.

        Args:
            portfolio_id: Portfolio ID
            trading_account_id: Trading account ID to link

        Returns:
            Created PortfolioAccount object

        Raises:
            HTTPException: If portfolio not found or account already linked
        """
        # Verify portfolio exists and belongs to user
        await self.get_portfolio(portfolio_id)

        # Check if already linked
        existing = await self.db.execute(
            select(PortfolioAccount).where(
                and_(
                    PortfolioAccount.portfolio_id == portfolio_id,
                    PortfolioAccount.trading_account_id == trading_account_id
                )
            )
        )
        if existing.scalar_one_or_none():
            raise HTTPException(
                400,
                f"Trading account {trading_account_id} is already linked to portfolio {portfolio_id}"
            )

        # Create link
        portfolio_account = PortfolioAccount(
            portfolio_id=portfolio_id,
            trading_account_id=trading_account_id,
            added_at=datetime.utcnow()
        )

        self.db.add(portfolio_account)
        await self.db.commit()
        await self.db.refresh(portfolio_account)

        logger.info(
            f"Linked trading account {trading_account_id} to portfolio {portfolio_id}"
        )

        return portfolio_account

    async def unlink_account(
        self,
        portfolio_id: int,
        trading_account_id: str
    ) -> Dict[str, Any]:
        """
        Unlink a trading account from a portfolio.

        Args:
            portfolio_id: Portfolio ID
            trading_account_id: Trading account ID to unlink

        Returns:
            Dictionary with unlink status

        Raises:
            HTTPException: If link not found
        """
        # Verify portfolio exists and belongs to user
        await self.get_portfolio(portfolio_id)

        # Find link
        result = await self.db.execute(
            select(PortfolioAccount).where(
                and_(
                    PortfolioAccount.portfolio_id == portfolio_id,
                    PortfolioAccount.trading_account_id == trading_account_id
                )
            )
        )
        link = result.scalar_one_or_none()

        if not link:
            raise HTTPException(
                404,
                f"Trading account {trading_account_id} is not linked to portfolio {portfolio_id}"
            )

        # Delete link
        await self.db.delete(link)
        await self.db.commit()

        logger.info(
            f"Unlinked trading account {trading_account_id} from portfolio {portfolio_id}"
        )

        return {
            "portfolio_id": portfolio_id,
            "trading_account_id": trading_account_id,
            "unlinked_at": datetime.utcnow().isoformat()
        }

    async def get_portfolio_accounts(self, portfolio_id: int) -> List[str]:
        """
        Get all trading accounts linked to a portfolio.

        Args:
            portfolio_id: Portfolio ID

        Returns:
            List of trading account IDs
        """
        # Verify portfolio exists and belongs to user
        await self.get_portfolio(portfolio_id)

        result = await self.db.execute(
            select(PortfolioAccount.trading_account_id).where(
                PortfolioAccount.portfolio_id == portfolio_id
            )
        )
        account_ids = result.scalars().all()

        return list(account_ids)

    async def get_portfolio_metrics(self, portfolio_id: int) -> Dict[str, Any]:
        """
        Calculate aggregated metrics for a portfolio.

        Aggregates positions and P&L across all linked trading accounts.

        Args:
            portfolio_id: Portfolio ID

        Returns:
            Dictionary with portfolio metrics
        """
        # Get linked accounts
        account_ids = await self.get_portfolio_accounts(portfolio_id)

        if not account_ids:
            return {
                "portfolio_id": portfolio_id,
                "account_count": 0,
                "total_positions": 0,
                "total_pnl": 0.0,
                "total_realized_pnl": 0.0,
                "total_unrealized_pnl": 0.0,
                "accounts": []
            }

        # Aggregate positions across all accounts
        # Note: This queries order_service.positions table
        result = await self.db.execute(
            text("""
                SELECT
                    trading_account_id,
                    COUNT(*) as position_count,
                    SUM(realized_pnl) as realized_pnl,
                    SUM(unrealized_pnl) as unrealized_pnl,
                    SUM(total_pnl) as total_pnl
                FROM order_service.positions
                WHERE trading_account_id = ANY(:account_ids)
                  AND is_open = true
                GROUP BY trading_account_id
            """),
            {"account_ids": account_ids}
        )

        account_metrics = []
        total_positions = 0
        total_pnl = 0.0
        total_realized_pnl = 0.0
        total_unrealized_pnl = 0.0

        for row in result:
            account_metrics.append({
                "trading_account_id": row.trading_account_id,
                "position_count": row.position_count,
                "realized_pnl": float(row.realized_pnl or 0),
                "unrealized_pnl": float(row.unrealized_pnl or 0),
                "total_pnl": float(row.total_pnl or 0)
            })
            total_positions += row.position_count
            total_realized_pnl += float(row.realized_pnl or 0)
            total_unrealized_pnl += float(row.unrealized_pnl or 0)
            total_pnl += float(row.total_pnl or 0)

        return {
            "portfolio_id": portfolio_id,
            "account_count": len(account_ids),
            "total_positions": total_positions,
            "total_pnl": total_pnl,
            "total_realized_pnl": total_realized_pnl,
            "total_unrealized_pnl": total_unrealized_pnl,
            "accounts": account_metrics
        }

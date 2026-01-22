"""
Default Portfolio Service

Handles auto-creation and management of default portfolios for trading accounts.
Default portfolios ensure that all external orders and manual trades are mapped
to both a default strategy AND a default portfolio for complete attribution.

Key Features:
- One Default Portfolio per Trading Account (linked to default strategy)
- Auto-creation of portfolio when default strategy is created
- Consistent portfolio mapping for external/manual trades
- Integration with algo_engine portfolio system
"""

import logging
from typing import Optional, Dict, Any, Tuple
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)


class DefaultPortfolioService:
    """
    Service for managing default portfolios per trading account.

    Default portfolios automatically contain:
    - Default strategy (passive tracking strategy)
    - All external orders/positions/trades from that account
    - Consistent attribution for manual trading

    IMPORTANT: This service uses broker_user_id (e.g., "XJ4540") as the trading_account_id
    for consistent mapping with default strategy service.
    """

    # Cache for default portfolio IDs (reduces DB queries)
    _default_portfolio_cache: Dict[str, int] = {}

    def __init__(self, db: AsyncSession):
        """
        Initialize the default portfolio service.

        Args:
            db: Async database session
        """
        self.db = db

    async def get_or_create_default_portfolio(
        self,
        trading_account_id: str,
        user_id: Optional[int] = None
    ) -> Tuple[int, int]:
        """
        Get the default portfolio and strategy for a trading account.

        Creates both if they don't exist with proper linking.

        Args:
            trading_account_id: The trading account ID - can be internal ID ("1") or broker ID ("XJ4540")
            user_id: Optional user ID for the portfolio

        Returns:
            Tuple of (portfolio_id: int, strategy_id: int)

        Raises:
            Exception: If portfolio or strategy creation fails
        """
        # Import here to avoid circular dependency
        from .default_strategy_service import DefaultStrategyService

        # Get or create default strategy first
        strategy_service = DefaultStrategyService(self.db)
        strategy_id, execution_id = await strategy_service.get_or_create_default_strategy(
            trading_account_id, user_id
        )

        # Check cache first
        cache_key = f"{trading_account_id}_{strategy_id}"
        if cache_key in self._default_portfolio_cache:
            logger.debug(f"Cache hit for default portfolio: {trading_account_id}")
            portfolio_id = self._default_portfolio_cache[cache_key]
            return (portfolio_id, strategy_id)

        try:
            from ..clients.strategy_service_client import get_strategy_client
            
            # Try to get existing portfolio linked to strategy via Strategy Service API
            strategy_client = await get_strategy_client()
            
            try:
                portfolio_info = await strategy_client.get_strategy_portfolio(strategy_id)
                if portfolio_info:
                    portfolio_id = portfolio_info["portfolio_id"]
                    self._default_portfolio_cache[cache_key] = portfolio_id
                    logger.debug(f"Found existing default portfolio {portfolio_id} for strategy {strategy_id}")
                    return (portfolio_id, strategy_id)
            except Exception as e:
                logger.warning(f"Strategy Service API failed: {e}, creating new portfolio")

            # Create new default portfolio via Portfolio Service API
            from ..clients.portfolio_service_client import get_portfolio_client
            portfolio_client = await get_portfolio_client()
            
            try:
                portfolio_info = await portfolio_client.get_or_create_default_portfolio(
                    trading_account_id=trading_account_id
                )
                portfolio_id = portfolio_info["portfolio_id"]
                
                # Link strategy to portfolio via Strategy Service
                await portfolio_client.link_portfolio_to_strategy(
                    portfolio_id=portfolio_id,
                    strategy_id=strategy_id,
                    allocation_percentage=100.0
                )
                
                self._default_portfolio_cache[cache_key] = portfolio_id
                return (portfolio_id, strategy_id)
            except Exception as e:
                logger.warning(f"Portfolio Service API failed: {e}, falling back to local creation")

            # Fallback: Create new default portfolio locally (will be deprecated)
            portfolio_id = await self._create_default_portfolio(
                trading_account_id, strategy_id, user_id
            )
            self._default_portfolio_cache[cache_key] = portfolio_id

            # Link strategy to portfolio
            await self._link_strategy_to_portfolio(strategy_id, portfolio_id)

            return (portfolio_id, strategy_id)

        except Exception as e:
            logger.error(f"Error getting/creating default portfolio for {trading_account_id}: {e}")
            raise

    async def _create_default_portfolio(
        self,
        trading_account_id: str,
        strategy_id: int,
        user_id: Optional[int] = None
    ) -> int:
        """
        Create a new default portfolio for a trading account.

        Uses public.portfolio table (consistent with algo_engine).

        Args:
            trading_account_id: Trading account ID (broker user ID like "XJ4540")
            strategy_id: Associated default strategy ID
            user_id: Optional user ID

        Returns:
            The new portfolio ID
        """
        logger.info(f"Creating default portfolio for trading account: {trading_account_id}")

        # Use Portfolio Service API instead of direct database access
        # CRITICAL: public.portfolio table doesn't exist in order_service database
        from ..clients.portfolio_service_client import get_portfolio_client
        
        try:
            portfolio_client = await get_portfolio_client()
            portfolio_data = {
                "trading_account_id": trading_account_id,
                "strategy_id": strategy_id,
                "user_id": user_id
            }
            portfolio_response = await portfolio_client.create_default_portfolio(trading_account_id, user_id)
            portfolio_id = portfolio_response.get("portfolio_id") or portfolio_response.get("id")
            
            if not portfolio_id:
                raise Exception(f"Portfolio service didn't return portfolio_id: {portfolio_response}")
                
            logger.info(f"Created default portfolio {portfolio_id} for {trading_account_id} via Portfolio Service API")
            return portfolio_id
                
        except Exception as e:
            logger.error(f"Portfolio service API failed: {e}")
            # CRITICAL: Cannot use fallback - public.portfolio table doesn't exist
            raise Exception(f"Failed to create default portfolio for {trading_account_id}: {e}. Portfolio Service must be available.")

    async def _link_strategy_to_portfolio(
        self,
        strategy_id: int,
        portfolio_id: int
    ) -> None:
        """
        Link strategy to portfolio in the strategy_portfolio table.

        Args:
            strategy_id: Strategy ID
            portfolio_id: Portfolio ID
        """
        logger.info(f"Linking strategy {strategy_id} to portfolio {portfolio_id}")

        # Use Portfolio Service API instead of direct database access  
        # CRITICAL: public.strategy_portfolio table doesn't exist in order_service database
        from ..clients.portfolio_service_client import get_portfolio_client
        
        try:
            portfolio_client = await get_portfolio_client()
            await portfolio_client.link_portfolio_to_strategy(portfolio_id, strategy_id, allocation_percentage=100.0)
            logger.info(f"Successfully linked strategy {strategy_id} to portfolio {portfolio_id} via Portfolio Service API")
            
        except Exception as e:
            logger.error(f"Portfolio service API failed while linking strategy: {e}")
            # CRITICAL: Cannot use fallback - public.strategy_portfolio table doesn't exist
            raise Exception(f"Failed to link strategy {strategy_id} to portfolio {portfolio_id}: {e}. Portfolio Service must be available.")

    async def tag_orphan_position_with_portfolio(
        self,
        position_id: int,
        trading_account_id: str,
        user_id: Optional[int] = None
    ) -> Tuple[int, int, str]:
        """
        Tag an orphan position to both default strategy and portfolio.

        Args:
            position_id: Position ID to tag
            trading_account_id: Trading account ID
            user_id: Optional user ID

        Returns:
            Tuple of (portfolio_id: int, strategy_id: int, execution_id: str)
        """
        portfolio_id, strategy_id = await self.get_or_create_default_portfolio(
            trading_account_id, user_id
        )

        # Get execution ID for the strategy
        from .default_strategy_service import DefaultStrategyService
        strategy_service = DefaultStrategyService(self.db)
        _, execution_id = await strategy_service.get_or_create_default_strategy(
            trading_account_id, user_id
        )

        # Update position with both strategy and portfolio
        await self.db.execute(
            text("""
                UPDATE order_service.positions
                SET strategy_id = :strategy_id,
                    portfolio_id = :portfolio_id,
                    execution_id = :execution_id::uuid,
                    entry_execution_id = :execution_id::uuid,
                    source = 'external',
                    updated_at = NOW()
                WHERE id = :position_id
                  AND (strategy_id IS NULL OR portfolio_id IS NULL)
            """),
            {
                "strategy_id": strategy_id,
                "portfolio_id": portfolio_id,
                "execution_id": execution_id,
                "position_id": position_id
            }
        )

        await self.db.commit()
        logger.info(
            f"Tagged position {position_id} to portfolio {portfolio_id}, "
            f"strategy {strategy_id}, execution {execution_id}"
        )

        return (portfolio_id, strategy_id, execution_id)

    async def tag_orphan_order_with_portfolio(
        self,
        order_id: int,
        trading_account_id: str,
        user_id: Optional[int] = None
    ) -> Tuple[int, int, str]:
        """
        Tag an orphan order to both default strategy and portfolio.

        Args:
            order_id: Order ID to tag
            trading_account_id: Trading account ID
            user_id: Optional user ID

        Returns:
            Tuple of (portfolio_id: int, strategy_id: int, execution_id: str)
        """
        portfolio_id, strategy_id = await self.get_or_create_default_portfolio(
            trading_account_id, user_id
        )

        # Get execution ID for the strategy
        from .default_strategy_service import DefaultStrategyService
        strategy_service = DefaultStrategyService(self.db)
        _, execution_id = await strategy_service.get_or_create_default_strategy(
            trading_account_id, user_id
        )

        # Update order with both strategy and portfolio
        await self.db.execute(
            text("""
                UPDATE order_service.orders
                SET strategy_id = :strategy_id,
                    portfolio_id = :portfolio_id,
                    execution_id = :execution_id::uuid,
                    source = 'external',
                    updated_at = NOW()
                WHERE id = :order_id
                  AND (strategy_id IS NULL OR portfolio_id IS NULL)
            """),
            {
                "strategy_id": strategy_id,
                "portfolio_id": portfolio_id,
                "execution_id": execution_id,
                "order_id": order_id
            }
        )

        await self.db.commit()
        logger.info(
            f"Tagged order {order_id} to portfolio {portfolio_id}, "
            f"strategy {strategy_id}, execution {execution_id}"
        )

        return (portfolio_id, strategy_id, execution_id)

    async def validate_portfolio_mapping_integrity(
        self,
        trading_account_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Validate that all external orders/positions have proper portfolio mapping.

        Args:
            trading_account_id: Optional - only validate this account

        Returns:
            Dict with validation results and counts of missing mappings
        """
        results = {
            "validation_passed": True,
            "missing_portfolio_positions": [],
            "missing_portfolio_orders": [],
            "orphan_strategies": [],
            "orphan_portfolios": [],
            "summary": {}
        }

        # Check for positions missing portfolio_id
        where_clause = ""
        params = {}
        if trading_account_id:
            where_clause = "AND trading_account_id = :trading_account_id"
            params["trading_account_id"] = trading_account_id

        # Missing portfolio positions
        result = await self.db.execute(
            text(f"""
                SELECT id, trading_account_id, symbol, strategy_id
                FROM order_service.positions
                WHERE (portfolio_id IS NULL OR portfolio_id = 0)
                  AND strategy_id IS NOT NULL
                  {where_clause}
                LIMIT 100
            """),
            params
        )
        missing_portfolio_positions = [
            {
                "position_id": row[0],
                "trading_account_id": row[1],
                "symbol": row[2],
                "strategy_id": row[3]
            }
            for row in result.fetchall()
        ]

        # Missing portfolio orders
        result = await self.db.execute(
            text(f"""
                SELECT id, trading_account_id, symbol, strategy_id
                FROM order_service.orders
                WHERE (portfolio_id IS NULL OR portfolio_id = 0)
                  AND strategy_id IS NOT NULL
                  {where_clause}
                LIMIT 100
            """),
            params
        )
        missing_portfolio_orders = [
            {
                "order_id": row[0],
                "trading_account_id": row[1],
                "symbol": row[2],
                "strategy_id": row[3]
            }
            for row in result.fetchall()
        ]

        # Check for strategies without portfolios via Strategy Service API
        # CRITICAL: public.strategy and public.strategy_portfolio tables don't exist
        orphan_strategies = []
        try:
            from ..clients.strategy_service_client import get_strategy_client
            
            strategy_client = await get_strategy_client()
            
            # Get default strategies and check if they have portfolio links
            if trading_account_id:
                strategies = await strategy_client.get_strategies_by_account(trading_account_id)
            else:
                strategies = await strategy_client.get_all_default_strategies()
            
            for strategy in strategies:
                if strategy.get("is_default"):
                    # Check if strategy has portfolio via Portfolio Service
                    try:
                        from ..clients.portfolio_service_client import get_portfolio_client
                        portfolio_client = await get_portfolio_client()
                        
                        portfolios = await portfolio_client.get_portfolios_by_strategy(strategy["id"])
                        if not portfolios:
                            orphan_strategies.append({
                                "strategy_id": strategy["id"],
                                "trading_account_id": strategy.get("trading_account_id")
                            })
                    except Exception:
                        # Assume orphaned if we can't check
                        orphan_strategies.append({
                            "strategy_id": strategy["id"],
                            "trading_account_id": strategy.get("trading_account_id")
                        })
                        
        except Exception as e:
            logger.warning(f"Strategy/Portfolio Service API failed: {e}")
            orphan_strategies = []

        results.update({
            "missing_portfolio_positions": missing_portfolio_positions,
            "missing_portfolio_orders": missing_portfolio_orders,
            "orphan_strategies": orphan_strategies,
            "summary": {
                "missing_portfolio_positions_count": len(missing_portfolio_positions),
                "missing_portfolio_orders_count": len(missing_portfolio_orders),
                "orphan_strategies_count": len(orphan_strategies),
                "total_issues": len(missing_portfolio_positions) + len(missing_portfolio_orders) + len(orphan_strategies)
            }
        })

        results["validation_passed"] = results["summary"]["total_issues"] == 0

        logger.info(
            f"Portfolio mapping validation: {results['summary']['total_issues']} issues found"
        )

        return results

    async def auto_fix_portfolio_mappings(
        self,
        trading_account_id: Optional[str] = None
    ) -> Dict[str, int]:
        """
        Automatically fix missing portfolio mappings for external items.

        Args:
            trading_account_id: Optional - only fix this account

        Returns:
            Dict with counts of fixed items
        """
        results = {
            "positions_fixed": 0,
            "orders_fixed": 0,
            "strategies_linked": 0,
            "errors": 0
        }

        # Get validation results first
        validation = await self.validate_portfolio_mapping_integrity(trading_account_id)

        # Fix missing portfolio positions
        for pos in validation["missing_portfolio_positions"]:
            try:
                await self.tag_orphan_position_with_portfolio(
                    pos["position_id"],
                    pos["trading_account_id"]
                )
                results["positions_fixed"] += 1
            except Exception as e:
                logger.error(f"Error fixing position {pos['position_id']}: {e}")
                results["errors"] += 1

        # Fix missing portfolio orders
        for order in validation["missing_portfolio_orders"]:
            try:
                await self.tag_orphan_order_with_portfolio(
                    order["order_id"],
                    order["trading_account_id"]
                )
                results["orders_fixed"] += 1
            except Exception as e:
                logger.error(f"Error fixing order {order['order_id']}: {e}")
                results["errors"] += 1

        # Fix orphan strategies (create portfolios for them)
        for strategy in validation["orphan_strategies"]:
            try:
                await self.get_or_create_default_portfolio(
                    strategy["trading_account_id"]
                )
                results["strategies_linked"] += 1
            except Exception as e:
                logger.error(f"Error linking strategy {strategy['strategy_id']}: {e}")
                results["errors"] += 1

        logger.info(
            f"Auto-fix completed: {results['positions_fixed']} positions, "
            f"{results['orders_fixed']} orders, {results['strategies_linked']} strategies linked"
        )

        return results

    def clear_cache(self):
        """Clear the default portfolio cache."""
        self._default_portfolio_cache.clear()
        logger.info("Cleared default portfolio cache")

    async def get_default_portfolio_info(
        self,
        trading_account_id: str
    ) -> Optional[Dict[str, Any]]:
        """
        Get information about the default portfolio for an account.

        Args:
            trading_account_id: Trading account ID

        Returns:
            Dict with portfolio info, or None if not found
        """
        # Get portfolio and strategy IDs
        try:
            portfolio_id, strategy_id = await self.get_or_create_default_portfolio(trading_account_id)
        except Exception:
            return None

        # Get portfolio details via Portfolio and Strategy Service APIs
        # CRITICAL: public.portfolio, public.strategy_portfolio, public.strategy tables don't exist
        try:
            from ..clients.portfolio_service_client import get_portfolio_client
            from ..clients.strategy_service_client import get_strategy_client
            
            portfolio_client = await get_portfolio_client()
            strategy_client = await get_strategy_client()
            
            # Get portfolio info
            portfolio_info = await portfolio_client.get_portfolio_info(portfolio_id)
            if not portfolio_info:
                return None
            
            # Get strategy info
            strategy_info = await strategy_client.get_strategy_info(str(strategy_id))
            if not strategy_info:
                return None
            
            # Get order_service specific metrics (positions and orders counts)
            positions_result = await self.db.execute(
                text("""
                    SELECT COUNT(*) FROM order_service.positions 
                    WHERE portfolio_id = :portfolio_id AND is_open = true
                """),
                {"portfolio_id": portfolio_id}
            )
            open_positions = positions_result.scalar() or 0
            
            orders_result = await self.db.execute(
                text("""
                    SELECT COUNT(*) FROM order_service.orders 
                    WHERE portfolio_id = :portfolio_id
                      AND status IN ('PENDING', 'SUBMITTED', 'OPEN', 'TRIGGER_PENDING')
                """),
                {"portfolio_id": portfolio_id}
            )
            active_orders = orders_result.scalar() or 0
            
        except Exception as e:
            logger.error(f"Portfolio/Strategy Service API failed: {e}")
            return None

        return {
            "portfolio_id": portfolio_id,
            "portfolio_name": portfolio_info.get("name"),
            "trading_account_id": portfolio_info.get("trading_account_id"),
            "is_default": portfolio_info.get("is_default"),
            "portfolio_type": portfolio_info.get("portfolio_type"),
            "status": portfolio_info.get("status"),
            "is_active": portfolio_info.get("is_active"),
            "created_at": portfolio_info.get("created_at"),
            "strategy_id": strategy_id,
            "strategy_name": strategy_info.get("name"),
            "open_positions": open_positions,
            "active_orders": active_orders
        }


# Helper function for use outside of class context
async def get_or_create_default_portfolio(
    db: AsyncSession,
    trading_account_id: str,
    user_id: Optional[int] = None
) -> Tuple[int, int]:
    """
    Convenience function to get or create default portfolio and strategy.

    Args:
        db: Database session
        trading_account_id: Trading account ID
        user_id: Optional user ID

    Returns:
        Tuple of (portfolio_id: int, strategy_id: int)
    """
    service = DefaultPortfolioService(db)
    return await service.get_or_create_default_portfolio(trading_account_id, user_id)
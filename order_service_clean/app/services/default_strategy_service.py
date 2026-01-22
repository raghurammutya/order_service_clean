"""
Default Strategy Service

Handles auto-creation and management of default strategies for trading accounts.
Default strategies are used to track external orders/positions (placed via broker
terminal, mobile app, or other sources outside our system).

Key Features:
- One Default Strategy per Trading Account
- Passive Strategy - Does NOT execute trades, only tracks external activity
- Auto-creation on first external order/position detection
- Real-time P&L tracking via ticker feeds
"""

import logging
from typing import Optional, Dict, Any, List
from datetime import datetime
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)


class DefaultStrategyService:
    """
    Service for managing default strategies per trading account.

    Default strategies automatically track:
    - External orders (placed via broker terminal/mobile)
    - External positions
    - External trades
    - Holdings changes from external buys

    IMPORTANT: This service uses broker_user_id (e.g., "XJ4540") as the trading_account_id
    for public.strategy table, NOT the internal trading_account_id (e.g., "1").
    """

    # Cache for default strategy IDs (reduces DB queries)
    _default_strategy_cache: Dict[str, int] = {}

    # Cache for internal ID to broker ID mapping
    _broker_id_cache: Dict[str, str] = {}

    def __init__(self, db: AsyncSession):
        """
        Initialize the default strategy service.

        Args:
            db: Async database session
        """
        self.db = db

    async def _get_broker_user_id(self, internal_account_id: str) -> str:
        """
        Convert internal trading_account_id to broker_user_id.

        The public.strategy table uses broker_user_id (e.g., "XJ4540") as trading_account_id,
        while order_service tables use internal ID (e.g., "1").

        Args:
            internal_account_id: Internal trading account ID (e.g., "1", "2")

        Returns:
            Broker user ID (e.g., "XJ4540", "WG7169")

        Note:
            If input looks like a broker ID already (not numeric), returns as-is.
        """
        # If it's already a broker ID (not numeric), return as-is
        if not internal_account_id.isdigit():
            return internal_account_id

        # Check cache
        if internal_account_id in self._broker_id_cache:
            return self._broker_id_cache[internal_account_id]

        # Look up from user_service.trading_accounts
        result = await self.db.execute(
            text("""
                SELECT broker_user_id
                FROM user_service.trading_accounts
                WHERE trading_account_id = :internal_id
            """),
            {"internal_id": int(internal_account_id)}
        )
        row = result.fetchone()

        if row and row[0]:
            broker_id = row[0]
            self._broker_id_cache[internal_account_id] = broker_id
            logger.debug(f"Resolved internal ID {internal_account_id} to broker ID {broker_id}")
            return broker_id

        # Fallback: return as-is if lookup fails
        logger.warning(f"Could not resolve internal ID {internal_account_id} to broker ID, using as-is")
        return internal_account_id

    async def _get_or_create_user_managed_execution(
        self,
        strategy_id: int,
        user_id: Optional[int] = None
    ) -> str:
        """
        Get or create user-managed execution for a strategy.

        Each strategy can have ONE user-managed execution (enforced by DB constraint).
        This execution tracks manual trading and external orders.

        Args:
            strategy_id: Strategy ID (from public.strategy table)
            user_id: Optional user ID

        Returns:
            Execution UUID string

        Raises:
            Exception: If execution creation fails
        """
        try:
            # Check if user-managed execution already exists
            result = await self.db.execute(
                text("""
                    SELECT id FROM algo_engine.executions
                    WHERE strategy_id = :strategy_id
                      AND execution_type = 'user_managed'
                      AND status IN ('running', 'idle', 'paused')
                      AND deleted_at IS NULL
                    LIMIT 1
                """),
                {"strategy_id": strategy_id}
            )
            row = result.fetchone()

            if row:
                execution_id = str(row[0])
                logger.debug(f"Found existing user-managed execution {execution_id} for strategy {strategy_id}")
                return execution_id

            # Create user-managed execution
            logger.info(f"Creating user-managed execution for strategy {strategy_id}")
            result = await self.db.execute(
                text("""
                    INSERT INTO algo_engine.executions (
                        strategy_id,
                        user_id,
                        execution_type,
                        managed_by_user_id,
                        name,
                        status,
                        capital_allocation_pct,
                        parameters,
                        created_at,
                        updated_at
                    ) VALUES (
                        :strategy_id,
                        :user_id,
                        'user_managed',
                        :user_id,
                        'Manual Trading',
                        'running',
                        100.0,
                        '{"is_tracking_only": true, "auto_created": true}'::jsonb,
                        NOW(),
                        NOW()
                    )
                    RETURNING id
                """),
                {
                    "strategy_id": strategy_id,
                    "user_id": user_id
                }
            )
            row = result.fetchone()
            if not row:
                raise Exception(f"Failed to create user-managed execution for strategy {strategy_id}")

            execution_id = str(row[0])
            await self.db.commit()

            logger.info(f"Created user-managed execution {execution_id} for strategy {strategy_id}")
            return execution_id

        except Exception as e:
            logger.error(f"Error creating user-managed execution: {e}")
            await self.db.rollback()
            raise

    async def get_or_create_default_strategy(
        self,
        trading_account_id: str,
        user_id: Optional[int] = None
    ) -> tuple[int, str]:
        """
        Get the default strategy and user-managed execution for a trading account.

        Creates both if they don't exist (unified execution architecture).

        This is the main entry point for auto-tagging external items.

        Args:
            trading_account_id: The trading account ID - can be internal ID ("1") or broker ID ("XJ4540")
            user_id: Optional user ID for the strategy

        Returns:
            Tuple of (strategy_id: int, execution_id: str)

        Raises:
            Exception: If strategy or execution creation fails
        """
        # Convert internal ID to broker ID if needed
        # public.strategy uses broker_user_id as trading_account_id
        broker_account_id = await self._get_broker_user_id(trading_account_id)

        # Check cache first (use broker ID as key)
        cache_key = broker_account_id
        if cache_key in self._default_strategy_cache:
            logger.debug(f"Cache hit for default strategy: {broker_account_id}")
            strategy_id = self._default_strategy_cache[cache_key]
            # Still need to get/create execution
            execution_id = await self._get_or_create_user_managed_execution(strategy_id, user_id)
            return (strategy_id, execution_id)

        try:
            # Try to get existing default strategy from public.strategy table
            # (same table used by Backend Service for consistency)
            result = await self.db.execute(
                text("""
                    SELECT strategy_id FROM public.strategy
                    WHERE trading_account_id = :account_id
                      AND is_default = TRUE
                    LIMIT 1
                """),
                {"account_id": broker_account_id}
            )
            row = result.fetchone()

            if row:
                strategy_id = row[0]
                self._default_strategy_cache[cache_key] = strategy_id
                logger.debug(f"Found existing default strategy {strategy_id} for {broker_account_id}")

                # Get or create user-managed execution
                execution_id = await self._get_or_create_user_managed_execution(strategy_id, user_id)
                return (strategy_id, execution_id)

            # Create new default strategy
            strategy_id = await self._create_default_strategy(broker_account_id, user_id)
            self._default_strategy_cache[cache_key] = strategy_id

            # Create user-managed execution for new strategy
            execution_id = await self._get_or_create_user_managed_execution(strategy_id, user_id)

            return (strategy_id, execution_id)

        except Exception as e:
            logger.error(f"Error getting/creating default strategy for {broker_account_id}: {e}")
            raise

    async def _create_default_strategy(
        self,
        trading_account_id: str,
        user_id: Optional[int] = None
    ) -> int:
        """
        Create a new default strategy for a trading account.

        Uses public.strategy table (same as Backend Service) for consistency.

        Args:
            trading_account_id: Trading account ID (broker user ID like "XJ4540")
            user_id: Optional user ID

        Returns:
            The new strategy ID
        """
        logger.info(f"Creating default strategy for trading account: {trading_account_id}")

        # Insert into public.strategy table (same table used by Backend Service)
        result = await self.db.execute(
            text("""
                INSERT INTO public.strategy (
                    strategy_name,
                    description,
                    strategy_type,
                    trading_account_id,
                    is_default,
                    is_active,
                    status,
                    config,
                    metadata,
                    created_by
                ) VALUES (
                    'Default Strategy',
                    'Auto-created default strategy for tracking external orders and positions. This strategy does not execute trades - it only tracks external activity.',
                    'passive',
                    :trading_account_id,
                    TRUE,
                    TRUE,
                    'active',
                    '{"auto_execute": false}'::jsonb,
                    '{"source": "auto_created", "created_reason": "default_strategy_auto_tagging"}'::jsonb,
                    'system'
                )
                RETURNING strategy_id
            """),
            {
                "trading_account_id": trading_account_id
            }
        )

        row = result.fetchone()
        if not row:
            raise Exception(f"Failed to create default strategy for {trading_account_id}")

        strategy_id = row[0]
        await self.db.commit()

        logger.info(f"Created default strategy {strategy_id} for {trading_account_id}")
        return strategy_id

    async def tag_orphan_position(
        self,
        position_id: int,
        trading_account_id: str,
        user_id: Optional[int] = None
    ) -> tuple[int, str]:
        """
        Tag an orphan position (no strategy_id) to the default strategy and execution.

        Args:
            position_id: Position ID to tag
            trading_account_id: Trading account ID
            user_id: Optional user ID

        Returns:
            Tuple of (strategy_id: int, execution_id: str)
        """
        strategy_id, execution_id = await self.get_or_create_default_strategy(trading_account_id, user_id)

        await self.db.execute(
            text("""
                UPDATE order_service.positions
                SET strategy_id = :strategy_id,
                    execution_id = :execution_id::uuid,
                    entry_execution_id = :execution_id::uuid,
                    source = 'external',
                    updated_at = NOW()
                WHERE id = :position_id
                  AND strategy_id IS NULL
            """),
            {
                "strategy_id": strategy_id,
                "execution_id": execution_id,
                "position_id": position_id
            }
        )

        await self.db.commit()
        logger.info(
            f"Tagged position {position_id} to default strategy {strategy_id} "
            f"and execution {execution_id}"
        )

        return (strategy_id, execution_id)

    async def tag_orphan_order(
        self,
        order_id: int,
        trading_account_id: str,
        user_id: Optional[int] = None
    ) -> tuple[int, str]:
        """
        Tag an orphan order (no strategy_id) to the default strategy and execution.

        Args:
            order_id: Order ID to tag
            trading_account_id: Trading account ID
            user_id: Optional user ID

        Returns:
            Tuple of (strategy_id: int, execution_id: str)
        """
        strategy_id, execution_id = await self.get_or_create_default_strategy(trading_account_id, user_id)

        await self.db.execute(
            text("""
                UPDATE order_service.orders
                SET strategy_id = :strategy_id,
                    execution_id = :execution_id::uuid,
                    source = 'external',
                    updated_at = NOW()
                WHERE id = :order_id
                  AND strategy_id IS NULL
            """),
            {
                "strategy_id": strategy_id,
                "execution_id": execution_id,
                "order_id": order_id
            }
        )

        await self.db.commit()
        logger.info(
            f"Tagged order {order_id} to default strategy {strategy_id} "
            f"and execution {execution_id}"
        )

        return (strategy_id, execution_id)

    async def tag_all_orphan_positions(
        self,
        trading_account_id: Optional[str] = None
    ) -> Dict[str, int]:
        """
        Tag all orphan positions to their respective default strategies and executions.

        Args:
            trading_account_id: Optional - only process this account

        Returns:
            Dict mapping trading_account_id to count of tagged positions
        """
        results = {}

        # Get all accounts with orphan positions
        if trading_account_id:
            accounts = [(trading_account_id,)]
        else:
            result = await self.db.execute(
                text("""
                    SELECT DISTINCT trading_account_id
                    FROM order_service.positions
                    WHERE strategy_id IS NULL
                """)
            )
            accounts = result.fetchall()

        for (account_id,) in accounts:
            try:
                strategy_id, execution_id = await self.get_or_create_default_strategy(account_id)

                result = await self.db.execute(
                    text("""
                        UPDATE order_service.positions
                        SET strategy_id = :strategy_id,
                            execution_id = :execution_id::uuid,
                            entry_execution_id = :execution_id::uuid,
                            source = CASE WHEN source = 'internal' THEN 'external' ELSE source END,
                            updated_at = NOW()
                        WHERE trading_account_id = :account_id
                          AND strategy_id IS NULL
                    """),
                    {
                        "strategy_id": strategy_id,
                        "execution_id": execution_id,
                        "account_id": account_id
                    }
                )

                results[account_id] = result.rowcount
                logger.info(
                    f"Tagged {result.rowcount} orphan positions for {account_id} "
                    f"to strategy {strategy_id} and execution {execution_id}"
                )

            except Exception as e:
                logger.error(f"Error tagging orphan positions for {account_id}: {e}")
                results[account_id] = 0

        await self.db.commit()
        return results

    async def tag_all_orphan_orders(
        self,
        trading_account_id: Optional[str] = None
    ) -> Dict[str, int]:
        """
        Tag all orphan orders to their respective default strategies and executions.

        Args:
            trading_account_id: Optional - only process this account

        Returns:
            Dict mapping trading_account_id to count of tagged orders
        """
        results = {}

        # Get all accounts with orphan orders
        if trading_account_id:
            accounts = [(trading_account_id,)]
        else:
            result = await self.db.execute(
                text("""
                    SELECT DISTINCT trading_account_id
                    FROM order_service.orders
                    WHERE strategy_id IS NULL
                """)
            )
            accounts = result.fetchall()

        for (account_id,) in accounts:
            try:
                strategy_id, execution_id = await self.get_or_create_default_strategy(account_id)

                result = await self.db.execute(
                    text("""
                        UPDATE order_service.orders
                        SET strategy_id = :strategy_id,
                            execution_id = :execution_id::uuid,
                            source = CASE WHEN source = 'internal' THEN 'external' ELSE source END,
                            updated_at = NOW()
                        WHERE trading_account_id = :account_id
                          AND strategy_id IS NULL
                    """),
                    {
                        "strategy_id": strategy_id,
                        "execution_id": execution_id,
                        "account_id": account_id
                    }
                )

                results[account_id] = result.rowcount
                logger.info(
                    f"Tagged {result.rowcount} orphan orders for {account_id} "
                    f"to strategy {strategy_id} and execution {execution_id}"
                )

            except Exception as e:
                logger.error(f"Error tagging orphan orders for {account_id}: {e}")
                results[account_id] = 0

        await self.db.commit()
        return results

    async def get_default_strategy_info(
        self,
        trading_account_id: str
    ) -> Optional[Dict[str, Any]]:
        """
        Get information about the default strategy for an account.

        Args:
            trading_account_id: Trading account ID

        Returns:
            Dict with strategy info, or None if not found
        """
        result = await self.db.execute(
            text("""
                SELECT
                    s.strategy_id,
                    s.strategy_name,
                    s.trading_account_id,
                    s.is_default,
                    s.strategy_type,
                    s.status,
                    s.is_active,
                    s.created_at,
                    (SELECT COUNT(*) FROM order_service.positions p
                     WHERE p.strategy_id = s.strategy_id AND p.is_open = true) as open_positions,
                    (SELECT COUNT(*) FROM order_service.orders o
                     WHERE o.strategy_id = s.strategy_id
                       AND o.status IN ('PENDING', 'SUBMITTED', 'OPEN', 'TRIGGER_PENDING')) as active_orders,
                    (SELECT COALESCE(SUM(p.total_pnl), 0) FROM order_service.positions p
                     WHERE p.strategy_id = s.strategy_id) as total_pnl
                FROM public.strategy s
                WHERE s.trading_account_id = :account_id
                  AND s.is_default = TRUE
                LIMIT 1
            """),
            {"account_id": trading_account_id}
        )

        row = result.fetchone()
        if not row:
            return None

        return {
            "id": row[0],
            "name": row[1],
            "trading_account_id": row[2],
            "is_default": row[3],
            "strategy_type": row[4],
            "state": row[5],
            "is_active": row[6],
            "created_at": row[7].isoformat() if row[7] else None,
            "open_positions": row[8],
            "active_orders": row[9],
            "total_pnl": float(row[10]) if row[10] else 0.0
        }

    async def get_default_strategy_positions(
        self,
        trading_account_id: str,
        only_open: bool = True
    ) -> List[Dict[str, Any]]:
        """
        Get all positions linked to the default strategy.

        Args:
            trading_account_id: Trading account ID
            only_open: If True, only return open positions

        Returns:
            List of position dictionaries
        """
        strategy_id = await self.get_or_create_default_strategy(trading_account_id)

        query = """
            SELECT
                p.id,
                p.symbol,
                p.exchange,
                p.product_type,
                p.quantity,
                p.buy_quantity,
                p.sell_quantity,
                p.buy_price,
                p.sell_price,
                p.last_price,
                p.realized_pnl,
                p.unrealized_pnl,
                p.total_pnl,
                p.net_pnl,
                p.total_charges,
                p.is_open,
                p.source,
                p.trading_day,
                p.updated_at
            FROM order_service.positions p
            WHERE p.strategy_id = :strategy_id
        """

        if only_open:
            query += " AND p.is_open = true"

        query += " ORDER BY p.updated_at DESC"

        result = await self.db.execute(
            text(query),
            {"strategy_id": strategy_id}
        )

        positions = []
        for row in result.fetchall():
            positions.append({
                "id": row[0],
                "symbol": row[1],
                "exchange": row[2],
                "product_type": row[3],
                "quantity": row[4],
                "buy_quantity": row[5],
                "sell_quantity": row[6],
                "buy_price": float(row[7]) if row[7] else None,
                "sell_price": float(row[8]) if row[8] else None,
                "last_price": float(row[9]) if row[9] else None,
                "realized_pnl": float(row[10]) if row[10] else 0.0,
                "unrealized_pnl": float(row[11]) if row[11] else 0.0,
                "total_pnl": float(row[12]) if row[12] else 0.0,
                "net_pnl": float(row[13]) if row[13] else 0.0,
                "total_charges": float(row[14]) if row[14] else 0.0,
                "is_open": row[15],
                "source": row[16],
                "trading_day": row[17].isoformat() if row[17] else None,
                "updated_at": row[18].isoformat() if row[18] else None
            })

        return positions

    def clear_cache(self):
        """Clear the default strategy cache."""
        self._default_strategy_cache.clear()
        logger.info("Cleared default strategy cache")

    async def is_default_strategy(self, strategy_id: int) -> bool:
        """
        Check if a strategy is a default strategy.

        Args:
            strategy_id: Strategy ID to check

        Returns:
            True if this is a default strategy
        """
        result = await self.db.execute(
            text("""
                SELECT is_default FROM public.strategy
                WHERE strategy_id = :strategy_id
            """),
            {"strategy_id": strategy_id}
        )
        row = result.fetchone()
        return row[0] if row else False

    async def validate_strategy_modification(
        self,
        strategy_id: int,
        action: str
    ) -> Dict[str, Any]:
        """
        Validate if a strategy modification is allowed.

        Default strategies have restrictions:
        - Cannot be deleted
        - Cannot be paused/stopped
        - Cannot be set to inactive
        - Cannot change is_default flag

        Args:
            strategy_id: Strategy ID
            action: One of 'delete', 'pause', 'stop', 'deactivate', 'modify'

        Returns:
            Dict with 'allowed' (bool) and 'reason' (str)
        """
        is_default = await self.is_default_strategy(strategy_id)

        if not is_default:
            return {"allowed": True, "reason": None}

        restricted_actions = {
            'delete': "Cannot delete default strategy - it must exist for tracking external items",
            'pause': "Cannot pause default strategy - it must always track external activity",
            'stop': "Cannot stop default strategy - it is a tracking-only strategy that's always active",
            'deactivate': "Cannot deactivate default strategy - it must remain active",
        }

        if action in restricted_actions:
            return {
                "allowed": False,
                "reason": restricted_actions[action],
                "is_default": True
            }

        return {"allowed": True, "reason": None, "is_default": True}

    async def get_strategy_summary_for_m2m(
        self,
        trading_account_id: str
    ) -> Dict[str, Any]:
        """
        Get default strategy summary optimized for M2M calculation.

        Returns all open positions with their current data for M2M updates.

        Args:
            trading_account_id: Trading account ID

        Returns:
            Dict with strategy info and positions for M2M calculation
        """
        strategy_id = await self.get_or_create_default_strategy(trading_account_id)

        # Get strategy basic info
        strategy_result = await self.db.execute(
            text("""
                SELECT strategy_id, strategy_name, trading_account_id, is_default, is_active
                FROM public.strategy
                WHERE strategy_id = :strategy_id
            """),
            {"strategy_id": strategy_id}
        )
        strategy_row = strategy_result.fetchone()

        if not strategy_row:
            return None

        # Get open positions for M2M calculation
        positions_result = await self.db.execute(
            text("""
                SELECT
                    p.id,
                    p.symbol,
                    p.exchange,
                    p.quantity,
                    p.buy_price,
                    p.sell_price,
                    p.last_price,
                    p.unrealized_pnl,
                    p.realized_pnl,
                    p.total_pnl
                FROM order_service.positions p
                WHERE p.strategy_id = :strategy_id
                  AND p.is_open = true
            """),
            {"strategy_id": strategy_id}
        )

        positions = []
        total_m2m = 0.0

        for row in positions_result.fetchall():
            quantity = row[3] or 0
            buy_price = float(row[4]) if row[4] else 0.0
            sell_price = float(row[5]) if row[5] else 0.0
            last_price = float(row[6]) if row[6] else 0.0
            unrealized_pnl = float(row[7]) if row[7] else 0.0

            # Determine entry price based on position direction
            if quantity > 0:  # Long position
                entry_price = buy_price
            elif quantity < 0:  # Short position
                entry_price = sell_price
            else:
                entry_price = 0.0

            total_m2m += unrealized_pnl

            positions.append({
                "id": row[0],
                "symbol": row[1],
                "exchange": row[2],
                "quantity": quantity,
                "entry_price": entry_price,
                "ltp": last_price,
                "unrealized_pnl": unrealized_pnl,
                "realized_pnl": float(row[8]) if row[8] else 0.0,
            })

        return {
            "strategy_id": strategy_row[0],
            "name": strategy_row[1],
            "trading_account_id": strategy_row[2],
            "is_default": strategy_row[3],
            "is_active": strategy_row[4],
            "current_m2m": total_m2m,
            "instruments": positions,
            "instrument_count": len(positions)
        }


# Helper function for use outside of class context
async def get_or_create_default_strategy(
    db: AsyncSession,
    trading_account_id: str,
    user_id: Optional[int] = None
) -> tuple[int, str]:
    """
    Convenience function to get or create default strategy and execution.

    Args:
        db: Database session
        trading_account_id: Trading account ID
        user_id: Optional user ID

    Returns:
        Tuple of (strategy_id: int, execution_id: str)
    """
    service = DefaultStrategyService(db)
    return await service.get_or_create_default_strategy(trading_account_id, user_id)

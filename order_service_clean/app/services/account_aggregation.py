"""
Account Aggregation Service

Provides utilities for aggregating data across multiple trading accounts
when users select "All Accounts" in the frontend.

GitHub Issue: #439
Author: Claude Code
Date: 2025-12-03
"""

import logging
from typing import List, Optional
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

from ..clients.user_service_client import UserServiceClient, UserServiceClientError

logger = logging.getLogger(__name__)


async def get_user_accessible_accounts(db: AsyncSession, user_id: int) -> List[int]:
    """
    Get all trading account IDs that the user has access to.

    Includes:
    - Owned accounts (user_service.trading_accounts where user_id = ?)
    - Shared accounts (user_service.trading_account_memberships where user_id = ?)

    Args:
        db: Database session
        user_id: User ID to query accounts for

    Returns:
        List of trading_account_id integers the user has access to

    Example:
        >>> account_ids = await get_user_accessible_accounts(db, 7)
        >>> print(account_ids)
        [1, 2, 5]  # User has access to 3 accounts
    """
    try:
        async with UserServiceClient() as client:
            accounts = await client.get_user_trading_accounts(user_id, include_shared=True)

        account_ids = [int(account["trading_account_id"]) for account in accounts]
        logger.info(
            f"User {user_id} has access to {len(account_ids)} accounts via user_service"
        )
        return account_ids
    except UserServiceClientError as exc:
        logger.error(f"Failed to get accessible accounts for user {user_id}: {exc}")
        return []
    except Exception as exc:
        logger.error(f"Unexpected error fetching accounts for user {user_id}: {exc}")
        return []


async def aggregate_positions(db: AsyncSession, account_ids: List[int]) -> dict:
    """
    NOTE: account_ids are integers from user_service, but trading_account_id
    in order_service tables is VARCHAR. We convert to strings for SQL queries.
    """
    """
    Aggregate positions across multiple accounts.

    For each position:
    - Merge same symbol positions across accounts
    - Sum quantities
    - Calculate weighted average prices
    - Sum P&L values

    Args:
        db: Database session
        account_ids: List of trading account IDs to aggregate

    Returns:
        Dict with aggregated position data including:
        - positions: List of aggregated positions
        - total: Total number of unique positions
        - aggregated: True
        - account_count: Number of accounts aggregated
    """
    if not account_ids:
        return {
            "positions": [],
            "total": 0,
            "aggregated": True,
            "account_count": 0,
            "message": "No accessible accounts found"
        }

    try:
        # Convert int IDs to strings (trading_account_id is VARCHAR in order_service)
        account_ids_str = [str(id) for id in account_ids]

        # Fetch positions for all accounts
        # Note: Using raw SQL for performance and multi-schema support
        query = text("""
            SELECT
                symbol,
                exchange,
                product_type,
                SUM(quantity) as total_quantity,
                SUM(buy_quantity) as total_buy_quantity,
                SUM(sell_quantity) as total_sell_quantity,
                SUM(buy_value) as total_buy_value,
                SUM(sell_value) as total_sell_value,
                SUM(realized_pnl) as total_realized_pnl,
                SUM(unrealized_pnl) as total_unrealized_pnl,
                MAX(last_price) as last_price,
                ARRAY_AGG(DISTINCT trading_account_id) as account_ids
            FROM order_service.positions
            WHERE trading_account_id = ANY(:account_ids)
            AND is_open = true
            GROUP BY symbol, exchange, product_type
            HAVING SUM(quantity) != 0
        """)

        result = await db.execute(query, {"account_ids": account_ids_str})
        rows = result.fetchall()

        positions = []
        for row in rows:
            # Calculate weighted average buy/sell prices
            buy_price = (row.total_buy_value / row.total_buy_quantity) if row.total_buy_quantity > 0 else None
            sell_price = (row.total_sell_value / row.total_sell_quantity) if row.total_sell_quantity > 0 else None

            total_pnl = (row.total_realized_pnl or 0) + (row.total_unrealized_pnl or 0)

            positions.append({
                "symbol": row.symbol,
                "exchange": row.exchange,
                "product_type": row.product_type,
                "quantity": int(row.total_quantity),
                "buy_quantity": int(row.total_buy_quantity),
                "sell_quantity": int(row.total_sell_quantity),
                "buy_price": float(buy_price) if buy_price else None,
                "sell_price": float(sell_price) if sell_price else None,
                "realized_pnl": float(row.total_realized_pnl or 0),
                "unrealized_pnl": float(row.total_unrealized_pnl or 0),
                "total_pnl": float(total_pnl),
                "last_price": float(row.last_price) if row.last_price else None,
                "accounts": row.account_ids,
                "is_aggregated": True
            })

        logger.info(
            f"Aggregated {len(positions)} unique positions from {len(account_ids)} accounts"
        )

        return {
            "positions": positions,
            "total": len(positions),
            "aggregated": True,
            "account_count": len(account_ids)
        }

    except Exception as e:
        logger.error(f"Failed to aggregate positions for accounts {account_ids}: {e}")
        raise


async def aggregate_orders(db: AsyncSession, account_ids: List[int], limit: int = 500) -> dict:
    """
    Aggregate orders across multiple accounts.

    Simply merge and sort by timestamp (no complex aggregation needed).

    Args:
        db: Database session
        account_ids: List of trading account IDs
        limit: Maximum number of orders to return

    Returns:
        Dict with aggregated order data
    """
    if not account_ids:
        return {
            "orders": [],
            "total": 0,
            "aggregated": True,
            "account_count": 0
        }

    try:
        # Convert int IDs to strings (trading_account_id is VARCHAR)
        account_ids_str = [str(id) for id in account_ids]

        query = text("""
            SELECT *
            FROM order_service.orders
            WHERE trading_account_id = ANY(:account_ids)
            ORDER BY created_at DESC
            LIMIT :limit
        """)

        result = await db.execute(query, {"account_ids": account_ids_str, "limit": limit})
        rows = result.fetchall()

        orders = [dict(row._mapping) for row in rows]

        return {
            "orders": orders,
            "total": len(orders),
            "aggregated": True,
            "account_count": len(account_ids),
            "limit": limit
        }

    except Exception as e:
        logger.error(f"Failed to aggregate orders for accounts {account_ids}: {e}")
        raise


async def aggregate_trades(db: AsyncSession, account_ids: List[int], limit: int = 500) -> dict:
    """
    Aggregate trades across multiple accounts.

    Merge and sort by trade timestamp.

    Args:
        db: Database session
        account_ids: List of trading account IDs
        limit: Maximum number of trades to return

    Returns:
        Dict with aggregated trade data
    """
    if not account_ids:
        return {
            "trades": [],
            "total": 0,
            "aggregated": True,
            "account_count": 0
        }

    try:
        # Convert int IDs to strings (trading_account_id is VARCHAR)
        account_ids_str = [str(id) for id in account_ids]

        query = text("""
            SELECT *
            FROM order_service.trades
            WHERE trading_account_id = ANY(:account_ids)
            ORDER BY trade_time DESC
            LIMIT :limit
        """)

        result = await db.execute(query, {"account_ids": account_ids_str, "limit": limit})
        rows = result.fetchall()

        trades = [dict(row._mapping) for row in rows]

        return {
            "trades": trades,
            "total": len(trades),
            "aggregated": True,
            "account_count": len(account_ids),
            "limit": limit
        }

    except Exception as e:
        logger.error(f"Failed to aggregate trades for accounts {account_ids}: {e}")
        raise


async def aggregate_holdings(db: AsyncSession, account_ids: List[int]) -> dict:
    """
    Aggregate holdings across multiple accounts.

    Merge holdings by symbol and sum quantities/values.

    Args:
        db: Database session
        account_ids: List of trading account IDs

    Returns:
        Dict with aggregated holdings data
    """
    if not account_ids:
        return {
            "holdings": [],
            "total": 0,
            "aggregated": True,
            "account_count": 0
        }

    try:
        # Get holdings from Account Service API instead of direct database access
        from ..clients.account_service_client import get_account_client
        
        account_client = await get_account_client()
        all_holdings = []
        
        # Fetch holdings for each account via API
        for account_id in account_ids:
            try:
                account_holdings = await account_client.get_holdings(str(account_id))
                for holding in account_holdings:
                    holding["trading_account_id"] = str(account_id)
                    all_holdings.append(holding)
            except Exception as e:
                logger.warning(f"Failed to get holdings for account {account_id}: {e}")
                continue
        
        # Aggregate holdings by symbol and exchange
        aggregated_holdings = {}
        for holding in all_holdings:
            symbol = holding.get("symbol", holding.get("tradingsymbol", ""))
            exchange = holding.get("exchange", "")
            key = f"{symbol}:{exchange}"
            
            if key not in aggregated_holdings:
                aggregated_holdings[key] = {
                    "symbol": symbol,
                    "exchange": exchange,
                    "quantity": 0,
                    "collateral_quantity": 0,
                    "average_price": 0.0,
                    "last_price": 0.0,
                    "pnl": 0.0,
                    "accounts": [],
                    "is_aggregated": True,
                    "total_value": 0.0
                }
            
            # Aggregate values
            agg = aggregated_holdings[key]
            quantity = holding.get("quantity", 0)
            collateral_qty = holding.get("collateral_quantity", 0)
            avg_price = holding.get("average_price", 0) or 0
            last_price = holding.get("last_price", 0) or 0
            pnl = holding.get("pnl", 0) or 0
            account_id = holding.get("trading_account_id")
            
            agg["quantity"] += quantity
            agg["collateral_quantity"] += collateral_qty
            agg["pnl"] += pnl
            agg["last_price"] = last_price  # Use latest price
            if account_id and account_id not in agg["accounts"]:
                agg["accounts"].append(account_id)
                
            # Calculate weighted average price
            if quantity > 0 and avg_price > 0:
                total_value = agg["total_value"] + (quantity * avg_price)
                total_qty = sum(h.get("quantity", 0) for h in all_holdings 
                               if (h.get("symbol", h.get("tradingsymbol", "")) == symbol and 
                                   h.get("exchange", "") == exchange))
                if total_qty > 0:
                    agg["average_price"] = total_value / total_qty
                agg["total_value"] = total_value

        # Filter out zero quantity holdings and convert to list
        holdings = [holding for holding in aggregated_holdings.values() 
                   if holding["quantity"] > 0]

        return {
            "holdings": holdings,
            "total": len(holdings),
            "aggregated": True,
            "account_count": len(account_ids)
        }

    except Exception as e:
        logger.error(f"Failed to aggregate holdings for accounts {account_ids}: {e}")
        raise


async def aggregate_dashboard_summary(db: AsyncSession, account_ids: List[int]) -> dict:
    """
    Aggregate dashboard metrics across accounts.

    Sum totals:
    - Total positions
    - Total orders (today)
    - Total P&L
    - Available margins

    Args:
        db: Database session
        account_ids: List of trading account IDs

    Returns:
        Dict with aggregated dashboard summary
    """
    if not account_ids:
        return {
            "aggregated": True,
            "account_count": 0,
            "positions": {"total": 0, "pnl": 0.0, "value": 0.0},
            "orders": {"total": 0, "complete": 0, "pending": 0},
            "trades": {"total": 0, "turnover": 0.0},
            "message": "No accessible accounts found"
        }

    try:
        from datetime import date

        # Convert int IDs to strings (trading_account_id is VARCHAR)
        account_ids_str = [str(id) for id in account_ids]

        # Get positions summary
        positions_query = text("""
            SELECT
                COUNT(*) as total_positions,
                SUM(realized_pnl + unrealized_pnl) as total_pnl,
                SUM(quantity * last_price) as total_value
            FROM order_service.positions
            WHERE trading_account_id = ANY(:account_ids)
            AND is_open = true
        """)
        positions_result = await db.execute(positions_query, {"account_ids": account_ids_str})
        positions_row = positions_result.fetchone()

        # Get orders summary (today)
        orders_query = text("""
            SELECT
                COUNT(*) as total_orders,
                COUNT(CASE WHEN status = 'COMPLETE' THEN 1 END) as complete_orders,
                COUNT(CASE WHEN status IN ('PENDING', 'OPEN', 'TRIGGER_PENDING') THEN 1 END) as pending_orders
            FROM order_service.orders
            WHERE trading_account_id = ANY(:account_ids)
            AND DATE(created_at) = :today
        """)
        orders_result = await db.execute(orders_query, {
            "account_ids": account_ids_str,
            "today": date.today()
        })
        orders_row = orders_result.fetchone()

        # Get trades summary (today)
        trades_query = text("""
            SELECT
                COUNT(*) as total_trades,
                SUM(price * quantity) as total_turnover
            FROM order_service.trades
            WHERE trading_account_id = ANY(:account_ids)
            AND DATE(trade_time) = :today
        """)
        trades_result = await db.execute(trades_query, {
            "account_ids": account_ids_str,
            "today": date.today()
        })
        trades_row = trades_result.fetchone()

        return {
            "aggregated": True,
            "account_count": len(account_ids),
            "positions": {
                "total": int(positions_row.total_positions or 0),
                "pnl": float(positions_row.total_pnl or 0),
                "value": float(positions_row.total_value or 0)
            },
            "orders": {
                "total": int(orders_row.total_orders or 0),
                "complete": int(orders_row.complete_orders or 0),
                "pending": int(orders_row.pending_orders or 0)
            },
            "trades": {
                "total": int(trades_row.total_trades or 0),
                "turnover": float(trades_row.total_turnover or 0)
            }
        }

    except Exception as e:
        logger.error(f"Failed to aggregate dashboard summary for accounts {account_ids}: {e}")
        raise


async def aggregate_margins(
    kite_clients: dict,
    account_ids: List[int],
    segment: Optional[str] = None,
    by_strategy: bool = False,
    db: Optional[AsyncSession] = None
) -> dict:
    """
    Aggregate margin data across multiple accounts.

    NOTE: Unlike other aggregation functions, this requires kite_clients dict
    because margin data must be fetched from broker APIs (not stored in DB).

    Args:
        kite_clients: Dict mapping account_id (int) -> KiteConnect client
        account_ids: List of trading account IDs to aggregate
        segment: Optional segment filter (equity, commodity)
        by_strategy: Include per-strategy margin breakdown
        db: Database session (required if by_strategy=True)

    Returns:
        Dict with aggregated margin data including:
        - accounts: List of per-account margin data
        - totals: Summed available/utilised margins
        - aggregated: True
        - account_count: Number of accounts aggregated
    """
    if not account_ids:
        return {
            "accounts": [],
            "totals": {
                "enabled": False,
                "net": 0.0,
                "available": {},
                "utilised": {}
            },
            "aggregated": True,
            "account_count": 0,
            "message": "No accessible accounts found"
        }

    try:
        import asyncio

        account_margins = []
        total_available = {}
        total_utilised = {}

        # Fetch margins from each account's broker API
        async def fetch_margin(account_id: int):
            try:
                kite_client = kite_clients.get(account_id)
                if not kite_client:
                    logger.warning(f"No Kite client found for account {account_id}")
                    return None

                margins = await kite_client.get_margins(segment=segment)
                return {
                    "trading_account_id": account_id,
                    "margins": margins
                }
            except Exception as e:
                logger.error(f"Failed to fetch margins for account {account_id}: {e}")
                return None

        # Fetch all margins in parallel
        results = await asyncio.gather(*[fetch_margin(acc_id) for acc_id in account_ids])

        # Aggregate results
        for result in results:
            if result is None:
                continue

            account_margins.append(result)
            margins = result["margins"]

            # Sum available balances
            if "available" in margins:
                for key, value in margins["available"].items():
                    total_available[key] = total_available.get(key, 0.0) + float(value or 0)

            # Sum utilised margins
            if "utilised" in margins:
                for key, value in margins["utilised"].items():
                    total_utilised[key] = total_utilised.get(key, 0.0) + float(value or 0)

        # Calculate total net (available cash across all accounts)
        total_net = sum(
            m["margins"].get("net", 0.0) for m in account_margins
            if "net" in m["margins"]
        )

        aggregated_result = {
            "accounts": account_margins,
            "totals": {
                "enabled": True,
                "net": float(total_net),
                "available": total_available,
                "utilised": total_utilised
            },
            "aggregated": True,
            "account_count": len(account_margins)
        }

        # Add per-strategy breakdown if requested (across all accounts)
        if by_strategy and db is not None:
            # Convert int IDs to strings (trading_account_id is VARCHAR)
            account_ids_str = [str(id) for id in account_ids]

            query = text("""
                SELECT
                    p.strategy_id,
                    CASE 
                        WHEN p.strategy_id IS NOT NULL THEN CONCAT('Strategy_', p.strategy_id)
                        ELSE 'Manual'
                    END as strategy_name,
                    p.exchange,
                    CASE
                        WHEN p.exchange IN ('NSE', 'BSE') THEN 'equity'
                        WHEN p.exchange IN ('NFO', 'BFO', 'CDS', 'MCX', 'NCDEX') THEN 'commodity'
                        ELSE 'other'
                    END as segment,
                    SUM(ABS(p.quantity * COALESCE(p.last_price, p.close_price, 0))) as notional_value,
                    SUM(CASE
                        WHEN p.product_type = 'MIS' THEN ABS(p.quantity * COALESCE(p.last_price, p.close_price, 0)) * 0.2
                        WHEN p.product_type = 'NRML' THEN ABS(p.quantity * COALESCE(p.last_price, p.close_price, 0)) * 0.1
                        WHEN p.product_type = 'CNC' THEN ABS(p.quantity * COALESCE(p.last_price, p.close_price, 0)) * 1.0
                        ELSE 0
                    END) as estimated_margin,
                    COUNT(*) as positions_count,
                    SUM(COALESCE(p.unrealized_pnl, 0)) as unrealized_pnl,
                    ARRAY_AGG(DISTINCT p.trading_account_id) as account_ids
                FROM order_service.positions p
                WHERE p.trading_account_id = ANY(:account_ids)
                AND p.is_open = true
                AND p.quantity != 0
                GROUP BY p.strategy_id, p.exchange
                ORDER BY estimated_margin DESC
            """)

            result = await db.execute(query, {"account_ids": account_ids_str})

            strategy_margins = []
            for row in result:
                strategy_margins.append({
                    "strategy_id": row.strategy_id,
                    "strategy_name": row.strategy_name,
                    "exchange": row.exchange,
                    "segment": row.segment,
                    "notional_value": float(row.notional_value or 0),
                    "estimated_margin": float(row.estimated_margin or 0),
                    "positions_count": row.positions_count,
                    "unrealized_pnl": float(row.unrealized_pnl or 0),
                    "accounts": row.account_ids
                })

            aggregated_result["by_strategy"] = strategy_margins

        logger.info(
            f"Aggregated margins from {len(account_margins)} accounts "
            f"(segment={segment}, by_strategy={by_strategy})"
        )

        return aggregated_result

    except Exception as e:
        logger.error(f"Failed to aggregate margins for accounts {account_ids}: {e}")
        raise

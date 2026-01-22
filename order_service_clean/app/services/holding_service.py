"""
Holding Service Business Logic

Handles long-term equity holdings tracking and updates.
"""
import logging
from datetime import datetime
from typing import List, Optional, Dict, Any
from sqlalchemy import select, and_
from sqlalchemy.ext.asyncio import AsyncSession
from fastapi import HTTPException

from .kite_client_multi import get_kite_client_for_account

logger = logging.getLogger(__name__)


class HoldingService:
    """
    Holding tracking and management service.

    Tracks long-term CNC (Cash and Carry) holdings with:
    - Real-time updates from CNC order completions
    - Daily sync from broker API to catch external changes
    """

    def __init__(self, db: AsyncSession, user_id: int, trading_account_id: int):
        """
        Initialize holding service.

        Args:
            db: Database session
            user_id: User ID from JWT token
            trading_account_id: Trading account ID
        """
        self.db = db
        self.user_id = user_id
        self.trading_account_id = trading_account_id
        self.kite_client = get_kite_client_for_account(trading_account_id)

    # ==========================================
    # REAL-TIME HOLDING UPDATES FROM ORDERS
    # ==========================================

    async def update_holding_from_order(self, order_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Update holdings from completed CNC order (real-time).

        Called by WebSocket listener when CNC order status = COMPLETE.

        Args:
            order_data: Order data from WebSocket or broker API

        Returns:
            Updated holding data, or None if not applicable (non-CNC orders)
        """
        # Only process CNC orders (delivery/long-term holdings)
        product_type = order_data.get("product")
        if product_type != "CNC":
            # Non-CNC orders update positions, not holdings
            return None

        symbol = order_data.get("tradingsymbol")
        exchange = order_data.get("exchange")
        transaction_type = order_data.get("transaction_type")  # BUY or SELL
        filled_quantity = order_data.get("filled_quantity", 0)
        average_price = order_data.get("average_price", 0.0)

        if filled_quantity == 0:
            # No fills yet
            return None

        logger.info(
            f"Updating holding from CNC order: {symbol} {transaction_type} "
            f"qty={filled_quantity} price={average_price}"
        )

        # Get existing holding from broker API (since we don't have a holdings table yet)
        # In production, you'd want a holdings table similar to positions
        holdings = await self.kite_client.get_holdings()
        existing_holding = next(
            (h for h in holdings if h.get('tradingsymbol') == symbol),
            None
        )

        if transaction_type == "BUY":
            # Add to holdings (weighted average)
            if existing_holding:
                old_qty = existing_holding.get('quantity', 0)
                old_avg = existing_holding.get('average_price', 0.0)

                new_qty = old_qty + filled_quantity
                new_avg = ((old_qty * old_avg) + (filled_quantity * average_price)) / new_qty if new_qty > 0 else average_price

                logger.info(
                    f"Updated holding: {symbol} qty={old_qty}+{filled_quantity}={new_qty} "
                    f"avg_price={old_avg}->{new_avg}"
                )

                return {
                    "symbol": symbol,
                    "exchange": exchange,
                    "quantity": new_qty,
                    "average_price": new_avg,
                    "action": "updated"
                }
            else:
                # New holding
                logger.info(f"Created new holding: {symbol} qty={filled_quantity} avg_price={average_price}")

                return {
                    "symbol": symbol,
                    "exchange": exchange,
                    "quantity": filled_quantity,
                    "average_price": average_price,
                    "action": "created"
                }

        elif transaction_type == "SELL":
            # Reduce holdings
            if existing_holding:
                old_qty = existing_holding.get('quantity', 0)
                new_qty = old_qty - filled_quantity

                if new_qty < 0:
                    logger.warning(
                        f"Selling more than holdings: {symbol} holding={old_qty} selling={filled_quantity}"
                    )
                    new_qty = 0

                if new_qty == 0:
                    logger.info(f"Fully sold holding: {symbol}")
                    return {
                        "symbol": symbol,
                        "exchange": exchange,
                        "quantity": 0,
                        "action": "sold_fully"
                    }
                else:
                    logger.info(f"Partially sold holding: {symbol} qty={old_qty}-{filled_quantity}={new_qty}")
                    return {
                        "symbol": symbol,
                        "exchange": exchange,
                        "quantity": new_qty,
                        "average_price": existing_holding.get('average_price', 0.0),  # Average doesn't change on sell
                        "action": "sold_partially"
                    }
            else:
                logger.warning(
                    f"Sold holding not in broker API: {symbol} qty={filled_quantity} "
                    "(may be from external source or short sell)"
                )
                return None

        return None

    # ==========================================
    # DAILY HOLDINGS SYNC FROM BROKER
    # ==========================================

    async def sync_holdings_daily(self) -> Dict[str, Any]:
        """
        Daily sync of holdings from broker API.

        Runs once per day at 4:30 PM to catch:
        - External buys (from other platforms)
        - Corporate actions (bonus, splits, dividends)
        - Demat transfers
        - Pledge/unpledge status

        Returns:
            Dictionary with sync statistics
        """
        from sqlalchemy import text
        from datetime import datetime, timezone
        import json

        logger.info("Starting daily holdings sync from broker")

        stats = {
            "holdings_synced": 0,
            "external_buys_detected": [],
            "external_sells_detected": [],
            "quantity_changes": [],
            "price_changes": []
        }

        try:
            # Get broker_user_id for this trading account via user_service API
            from ..clients.user_service_client import UserServiceClient, UserServiceClientError

            try:
                async with UserServiceClient() as client:
                    account_info = await client.get_trading_account_basic_info(self.trading_account_id)
            except UserServiceClientError as exc:
                logger.error(f"Trading account lookup failed: {exc}")
                return stats

            broker_user_id = account_info.get("broker_user_id")
            if not broker_user_id:
                logger.error(f"Trading account {self.trading_account_id} missing broker_user_id")
                return stats

            # Fetch holdings from broker (source of truth)
            broker_holdings = await self.kite_client.get_holdings()

            logger.info(f"Fetched {len(broker_holdings)} holdings from broker for account {broker_user_id}")

            # Delete existing holdings for this account
            delete_query = text("""
                DELETE FROM order_service.account_holding
                WHERE trading_account_id = :account_id
            """)
            await self.db.execute(delete_query, {"account_id": broker_user_id})

            # Insert updated holdings
            insert_query = text("""
                INSERT INTO order_service.account_holding (
                    trading_account_id, symbol, exchange, isin, quantity,
                    average_price, last_price, pnl, day_pnl,
                    synced_at, created_at, updated_at, raw_data
                ) VALUES (
                    :account_id, :tradingsymbol, :exchange, :isin, :quantity,
                    :average_price, :last_price, :pnl, :day_pnl,
                    :synced_at, :created_at, :updated_at, :raw_data
                )
            """)

            now = datetime.now(timezone.utc)

            for broker_holding in broker_holdings:
                symbol = broker_holding.get('tradingsymbol')
                quantity = broker_holding.get('quantity', 0)
                avg_price = broker_holding.get('average_price', 0.0)
                last_price = broker_holding.get('last_price', 0.0)
                pnl = broker_holding.get('pnl', 0.0)

                await self.db.execute(insert_query, {
                    "account_id": broker_user_id,
                    "symbol": symbol,
                    "exchange": broker_holding.get('exchange'),
                    "isin": broker_holding.get('isin'),
                    "quantity": quantity,
                    "average_price": avg_price,
                    "last_price": last_price,
                    "pnl": pnl,
                    "day_pnl": broker_holding.get('day_pnl', 0.0),
                    "synced_at": now,
                    "created_at": now,
                    "updated_at": now,
                    "raw_data": json.dumps(broker_holding)
                })

                logger.debug(
                    f"Synced holding: {symbol} qty={quantity} avg={avg_price} "
                    f"last={last_price} pnl={pnl}"
                )

                stats["holdings_synced"] += 1
                stats["quantity_changes"].append({
                    "symbol": symbol,
                    "quantity": quantity,
                    "average_price": avg_price,
                    "pnl": pnl
                })

            await self.db.commit()

            logger.info(
                f"Holdings sync complete: synced={stats['holdings_synced']} holdings "
                f"for account {broker_user_id}"
            )

            return stats

        except Exception as e:
            await self.db.rollback()
            logger.error(f"Holdings sync failed: {e}", exc_info=True)
            raise HTTPException(500, f"Holdings sync failed: {str(e)}")

    # ==========================================
    # HOLDING QUERIES
    # ==========================================

    async def get_holdings(self) -> List[Dict[str, Any]]:
        """
        Get all holdings for the user.

        Returns:
            List of holdings from broker API
        """
        try:
            holdings = await self.kite_client.get_holdings()
            logger.debug(f"Retrieved {len(holdings)} holdings for user {self.user_id}")
            return holdings

        except Exception as e:
            logger.error(f"Failed to fetch holdings: {e}")
            raise HTTPException(500, f"Failed to fetch holdings: {str(e)}")

    async def get_holdings_summary(self) -> Dict[str, Any]:
        """
        Get summary of all holdings.

        Returns:
            Dictionary with holdings summary
        """
        holdings = await self.get_holdings()

        total_value = sum(h.get('last_price', 0) * h.get('quantity', 0) for h in holdings)
        total_investment = sum(h.get('average_price', 0) * h.get('quantity', 0) for h in holdings)
        total_pnl = sum(h.get('pnl', 0) for h in holdings)

        return {
            'total_holdings': len(holdings),
            'total_value': total_value,
            'total_investment': total_investment,
            'total_pnl': total_pnl,
            'holdings': holdings
        }

"""
Position Subscription Manager

Manages real-time tick subscriptions for positions and holdings.
When a position is opened, subscribes to the instrument's tick feed.
When a position is closed, unsubscribes (if no other accounts need it).
"""
import logging
from typing import Optional, Dict, List, Set, Any
from datetime import datetime
import httpx
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)


# Non-subscribable instrument types (these use polling instead of real-time feeds)
NON_SUBSCRIBABLE_SEGMENTS = {
    'SGB',      # Sovereign Gold Bonds
    'GS',       # Government Securities
    'T-BILLS',  # Treasury Bills
    'SDL',      # State Development Loans
    'BOND',     # Corporate Bonds
    'DEBT',     # Debt instruments
}


class SubscriptionManager:
    """
    Manages instrument subscriptions for position P&L tracking.

    Responsibilities:
    - Track which instruments need subscriptions for positions/holdings
    - Coordinate with ticker_service_v2 for actual subscriptions
    - Handle non-subscribable instruments (bonds, debt) with polling fallback
    """

    def __init__(
        self,
        db: AsyncSession,
        ticker_service_url: str = "http://localhost:8089"
    ):
        """
        Initialize subscription manager.

        Args:
            db: Database session
            ticker_service_url: URL of ticker_service_v2
        """
        self.db = db
        self.ticker_service_url = ticker_service_url
        self._instrument_cache: Dict[str, Dict[str, Any]] = {}

    def _is_subscribable(self, segment: str, instrument_type: str = None) -> bool:
        """
        Check if an instrument can be subscribed for real-time feeds.

        Args:
            segment: Instrument segment (NSE, NFO, SGB, etc.)
            instrument_type: Optional instrument type

        Returns:
            True if subscribable, False for bonds/debt
        """
        segment_upper = segment.upper() if segment else ''

        # Check against non-subscribable segments
        if segment_upper in NON_SUBSCRIBABLE_SEGMENTS:
            return False

        # Check instrument type if provided
        if instrument_type:
            type_upper = instrument_type.upper()
            if any(t in type_upper for t in ['BOND', 'DEBT', 'SGB', 'GSEC', 'SDL']):
                return False

        return True

    async def _get_instrument_details(
        self,
        symbol: str,
        exchange: str
    ) -> Optional[Dict[str, Any]]:
        """
        Get instrument details including token from database.

        Args:
            symbol: Trading symbol (full tradingsymbol like NIFTY25D0226400CE)
            exchange: Exchange code (NSE, NFO, etc.)

        Returns:
            Instrument details dict or None
        """
        cache_key = f"{exchange}:{symbol}"

        if cache_key in self._instrument_cache:
            return self._instrument_cache[cache_key]

        # Query instrument_registry first - it has full tradingsymbol
        # This works for both equity and F&O instruments
        result = await self.db.execute(text("""
            SELECT
                instrument_token,
                symbol,
                name as symbol,
                segment,
                instrument_type,
                strike as strike_price,
                expiry as expiry_date
            FROM market_data.instrument_registry
            WHERE symbol = :symbol
              AND exchange = :exchange
              AND is_active = true
            LIMIT 1
        """), {"symbol": symbol, "exchange": exchange})

        row = result.fetchone()

        if row:
            instrument = {
                "instrument_token": row.instrument_token,
                "symbol": row.tradingsymbol,
                "symbol": row.symbol if row.symbol else symbol,
                "segment": row.segment,
                "instrument_type": row.instrument_type,
                "strike_price": row.strike_price,
                "expiry_date": row.expiry_date,
            }
            self._instrument_cache[cache_key] = instrument
            return instrument

        return None

    async def subscribe_for_position(
        self,
        trading_account_id: str,
        symbol: str,
        exchange: str,
        source: str = "position"
    ) -> Dict[str, Any]:
        """
        Subscribe to instrument for a position.

        Args:
            trading_account_id: Trading account ID
            symbol: Trading symbol
            exchange: Exchange code
            source: Subscription source (position, holding)

        Returns:
            Result dict with subscription status
        """
        # Get instrument details
        instrument = await self._get_instrument_details(symbol, exchange)

        if not instrument:
            logger.warning(f"Instrument not found: {symbol} on {exchange}")
            return {
                "success": False,
                "error": "Instrument not found",
                "symbol": symbol
            }

        instrument_token = instrument["instrument_token"]
        segment = instrument.get("segment", exchange)
        instrument_type = instrument.get("instrument_type", "")

        # Check if subscribable
        is_subscribable = self._is_subscribable(segment, instrument_type)

        # Insert or update subscription record
        await self.db.execute(text("""
            INSERT INTO order_service.position_subscriptions (
                instrument_token,
                symbol,
                exchange,
                segment,
                trading_account_id,
                source,
                is_active,
                is_subscribable,
                created_at,
                updated_at
            ) VALUES (
                :token,
                :symbol,
                :exchange,
                :segment,
                :account_id,
                :source,
                true,
                :subscribable,
                NOW(),
                NOW()
            )
            ON CONFLICT (instrument_token, trading_account_id, source)
            DO UPDATE SET
                is_active = true,
                updated_at = NOW()
        """), {
            "token": instrument_token,
            "symbol": symbol,
            "exchange": exchange,
            "segment": segment,
            "account_id": str(trading_account_id),
            "source": source,
            "subscribable": is_subscribable
        })

        await self.db.commit()

        logger.info(
            f"Subscribed {symbol} for account {trading_account_id} "
            f"(token={instrument_token}, subscribable={is_subscribable})"
        )

        # Notify ticker service to refresh subscriptions (if subscribable)
        if is_subscribable:
            await self._notify_ticker_service()

        return {
            "success": True,
            "instrument_token": instrument_token,
            "symbol": symbol,
            "is_subscribable": is_subscribable,
            "source": source
        }

    async def unsubscribe_for_position(
        self,
        trading_account_id: str,
        symbol: str,
        exchange: str,
        source: str = "position"
    ) -> Dict[str, Any]:
        """
        Unsubscribe from instrument for a position.

        Only actually unsubscribes if no other accounts need this instrument.

        Args:
            trading_account_id: Trading account ID
            symbol: Trading symbol
            exchange: Exchange code
            source: Subscription source (position, holding)

        Returns:
            Result dict with unsubscription status
        """
        # Get instrument details
        instrument = await self._get_instrument_details(symbol, exchange)

        if not instrument:
            return {
                "success": False,
                "error": "Instrument not found",
                "symbol": symbol
            }

        instrument_token = instrument["instrument_token"]

        # Mark this account's subscription as inactive
        await self.db.execute(text("""
            UPDATE order_service.position_subscriptions
            SET is_active = false, updated_at = NOW()
            WHERE instrument_token = :token
              AND trading_account_id = :account_id
              AND source = :source
        """), {
            "token": instrument_token,
            "account_id": str(trading_account_id),
            "source": source
        })

        await self.db.commit()

        logger.info(
            f"Unsubscribed {symbol} for account {trading_account_id} "
            f"(token={instrument_token})"
        )

        # Check if any other accounts still need this instrument
        result = await self.db.execute(text("""
            SELECT COUNT(*) as count
            FROM order_service.position_subscriptions
            WHERE instrument_token = :token
              AND is_active = true
        """), {"token": instrument_token})

        remaining = result.scalar()

        # Only notify ticker service if no accounts need this instrument
        if remaining == 0:
            await self._notify_ticker_service()

        return {
            "success": True,
            "instrument_token": instrument_token,
            "symbol": symbol,
            "fully_unsubscribed": (remaining == 0)
        }

    async def sync_subscriptions_for_account(
        self,
        trading_account_id: str,
        positions: List[Dict[str, Any]],
        holdings: List[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Sync all subscriptions for an account based on current positions/holdings.

        Args:
            trading_account_id: Trading account ID
            positions: List of current positions
            holdings: List of current holdings (optional)

        Returns:
            Sync statistics
        """
        holdings = holdings or []

        # Get currently subscribed tokens for this account
        result = await self.db.execute(text("""
            SELECT instrument_token, symbol, source
            FROM order_service.position_subscriptions
            WHERE trading_account_id = :account_id
              AND is_active = true
        """), {"account_id": str(trading_account_id)})

        current_subs = {
            (row.instrument_token, row.source): row.tradingsymbol
            for row in result.fetchall()
        }

        # Build set of required subscriptions
        required_subs = set()

        # From positions (quantity != 0)
        for pos in positions:
            if pos.get("quantity", 0) != 0:
                symbol = pos.get("tradingsymbol") or pos.get("symbol")
                exchange = pos.get("exchange", "NSE")
                instrument = await self._get_instrument_details(symbol, exchange)
                if instrument:
                    required_subs.add((instrument["instrument_token"], "position"))

        # From holdings (quantity > 0)
        for holding in holdings:
            if holding.get("quantity", 0) > 0:
                symbol = holding.get("tradingsymbol") or holding.get("symbol")
                exchange = holding.get("exchange", "NSE")
                instrument = await self._get_instrument_details(symbol, exchange)
                if instrument:
                    required_subs.add((instrument["instrument_token"], "holding"))

        # Determine additions and removals
        current_set = set(current_subs.keys())
        to_add = required_subs - current_set
        to_remove = current_set - required_subs

        added = 0
        removed = 0

        # Add new subscriptions
        for token, source in to_add:
            # Get symbol from cache
            symbol = None
            for key, inst in self._instrument_cache.items():
                if inst["instrument_token"] == token:
                    symbol = inst["symbol"]
                    exchange = key.split(":")[0]
                    break

            if symbol:
                await self.subscribe_for_position(
                    trading_account_id, symbol, exchange, source
                )
                added += 1

        # Remove old subscriptions
        for token, source in to_remove:
            symbol = current_subs.get((token, source))
            if symbol:
                await self.unsubscribe_for_position(
                    trading_account_id, symbol, "NSE", source
                )
                removed += 1

        # Notify ticker service once at the end
        if added > 0 or removed > 0:
            await self._notify_ticker_service()

        return {
            "success": True,
            "trading_account_id": trading_account_id,
            "subscriptions_added": added,
            "subscriptions_removed": removed,
            "total_active": len(required_subs)
        }

    async def get_active_subscriptions(
        self,
        trading_account_id: str = None
    ) -> List[Dict[str, Any]]:
        """
        Get all active position subscriptions.

        Args:
            trading_account_id: Optional filter by account

        Returns:
            List of active subscriptions
        """
        query = """
            SELECT
                instrument_token,
                symbol,
                exchange,
                segment,
                trading_account_id,
                source,
                is_subscribable,
                created_at,
                updated_at
            FROM order_service.position_subscriptions
            WHERE is_active = true
        """

        params = {}
        if trading_account_id:
            query += " AND trading_account_id = :account_id"
            params["account_id"] = str(trading_account_id)

        query += " ORDER BY updated_at DESC"

        result = await self.db.execute(text(query), params)

        return [
            {
                "instrument_token": row.instrument_token,
                "symbol": row.tradingsymbol,
                "exchange": row.exchange,
                "segment": row.segment,
                "trading_account_id": row.trading_account_id,
                "source": row.source,
                "is_subscribable": row.is_subscribable,
                "created_at": row.created_at.isoformat() if row.created_at else None,
                "updated_at": row.updated_at.isoformat() if row.updated_at else None,
            }
            for row in result.fetchall()
        ]

    async def _notify_ticker_service(self):
        """
        Notify ticker_service_v2 to refresh its subscription list.
        """
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.post(
                    f"{self.ticker_service_url}/admin/subscriptions/refresh"
                )

                if response.status_code == 200:
                    logger.info("Ticker service subscription refresh triggered")
                else:
                    logger.warning(
                        f"Failed to trigger subscription refresh: "
                        f"HTTP {response.status_code}"
                    )

        except httpx.RequestError as e:
            logger.warning(f"Could not reach ticker service: {e}")
        except Exception as e:
            logger.error(f"Error notifying ticker service: {e}")

    async def recover_subscriptions_on_startup(self):
        """
        Recover subscriptions for all open positions and holdings on service startup.

        Should be called during order_service initialization.
        """
        logger.info("Recovering position and holding subscriptions on startup...")

        # Get all open positions across all accounts
        positions_result = await self.db.execute(text("""
            SELECT DISTINCT
                p.trading_account_id,
                p.symbol,
                p.exchange,
                'position' as source
            FROM order_service.positions p
            WHERE p.is_open = true
              AND p.quantity != 0
        """))

        position_rows = positions_result.fetchall()

        # Get all holdings across all accounts (with error handling if table doesn't exist)
        holding_rows = []
        try:
            holdings_result = await self.db.execute(text("""
                SELECT DISTINCT
                    ta.trading_account_id::text as trading_account_id,
                    ah.symbol as symbol,
                    ah.exchange,
                    'holding' as source
                FROM account_holding ah
                INNER JOIN user_service.trading_accounts ta
                    ON ah.account_id = ta.broker_user_id
                WHERE ah.quantity > 0
            """))
            holding_rows = holdings_result.fetchall()
        except Exception as e:
            logger.warning(f"Could not query holdings table: {e}")

        # Combine positions and holdings
        all_rows = list(position_rows) + list(holding_rows)

        if not all_rows:
            logger.info("No open positions/holdings to subscribe")
            return {"recovered_positions": 0, "recovered_holdings": 0}

        recovered_positions = 0
        recovered_holdings = 0

        for row in all_rows:
            try:
                await self.subscribe_for_position(
                    trading_account_id=row.trading_account_id,
                    symbol=row.symbol,
                    exchange=row.exchange,
                    source=row.source
                )
                if row.source == "position":
                    recovered_positions += 1
                else:
                    recovered_holdings += 1
            except Exception as e:
                logger.error(
                    f"Failed to recover subscription for {row.symbol} ({row.source}): {e}"
                )

        # Single notification after all subscriptions
        await self._notify_ticker_service()

        logger.info(
            f"Recovered {recovered_positions} position subscriptions "
            f"and {recovered_holdings} holding subscriptions"
        )
        return {
            "recovered_positions": recovered_positions,
            "recovered_holdings": recovered_holdings,
            "total_recovered": recovered_positions + recovered_holdings
        }


# Convenience function for creating manager with proper dependencies
async def get_subscription_manager(
    db: AsyncSession,
    ticker_service_url: str = None
) -> SubscriptionManager:
    """
    Get a configured SubscriptionManager instance.

    Args:
        db: Database session
        ticker_service_url: Optional ticker service URL override

    Returns:
        Configured SubscriptionManager
    """
    from ..config.settings import settings

    url = ticker_service_url or getattr(
        settings, 'ticker_service_url', 'http://localhost:8089'
    )

    return SubscriptionManager(db=db, ticker_service_url=url)

"""
Multi-Account KiteConnect Client for Order Service

This client supports multiple trading accounts by:
1. Mapping trading_account_id to kite account nicknames
2. Fetching tokens dynamically from Token Manager
3. Creating per-account KiteConnect instances

Account Mapping:
- trading_account_id=1 -> nickname="primary" -> XJ4540
- trading_account_id=2 -> nickname="aparna" -> WG7169

Config Service Integration:
- TOKEN_MANAGER_URL and TOKEN_MANAGER_INTERNAL_API_KEY can be fetched from config service
- Falls back to environment variables if config service unavailable

Rate Limiting (Kite API limits):
- Order operations: 10/sec, 200/min, 3000/day per account
- API GET operations: 10/sec
- Quote operations: 1/sec
- Historical data: 3/sec

Usage:
    client = get_kite_client_for_account(trading_account_id=1)
    order_id = await client.place_order(...)
"""
import os
import logging
from typing import Optional, Dict, Any, Union, Callable, TypeVar
from functools import wraps
import httpx
from kiteconnect import KiteConnect
from kiteconnect.exceptions import TokenException

from ..config.settings import settings
from .kite_account_rate_limiter import (
    KiteOperation,
    RateLimitExceeded,
    DailyLimitExceeded,
    get_rate_limiter_manager_sync,
)

logger = logging.getLogger(__name__)

T = TypeVar('T')


async def resolve_trading_account_config(trading_account_id: int) -> Dict[str, str]:
    """
    Resolve trading_account_id to broker configuration via token_manager API.
    
    Sprint 1: Replaces hardcoded mapping with dynamic API lookup.
    
    Args:
        trading_account_id: User service trading account ID
        
    Returns:
        Dict with nickname, api_key, broker, and other account config
        
    Raises:
        ValueError: If account resolution fails or account not found
    """
    # Get token_manager configuration
    # ARCHITECTURE PRINCIPLE #24: Use single INTERNAL_API_KEY for all service-to-service auth
    token_manager_url = settings.token_manager_url
    api_key = settings.internal_api_key
    
    if not api_key:
        logger.error("No token_manager API key configured for account resolution")
        raise ValueError("Token manager API key not configured")
    
    headers = {
        "X-Internal-API-Key": api_key,
        "Content-Type": "application/json"
    }
    
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{token_manager_url}/api/v1/accounts/resolve/{trading_account_id}",
                headers=headers,
                timeout=10.0
            )
            
            if response.status_code != 200:
                logger.error(f"Account resolution failed with status {response.status_code}: {response.text}")
                raise ValueError(f"Account resolution failed: HTTP {response.status_code}")
            
            data = response.json()
            if not data.get("success") or not data.get("account"):
                error_msg = data.get("error", "Unknown error")
                logger.error(f"Account resolution unsuccessful: {error_msg}")
                raise ValueError(f"Account resolution failed: {error_msg}")
            
            account = data["account"]
            
            # Extract configuration in format expected by MultiAccountKiteClient
            return {
                "nickname": account["account_nickname"],
                "api_key": account["api_key"],
                "broker": account["broker"],
                "segment": account.get("segment", "equity"),
                "is_active": account.get("is_active", True)
            }
            
    except httpx.HTTPError as e:
        logger.error(f"HTTP error during account resolution for {trading_account_id}: {e}")
        raise ValueError(f"Network error resolving trading account: {e}")
    except Exception as e:
        logger.error(f"Unexpected error resolving trading account {trading_account_id}: {e}")
        raise ValueError(f"Failed to resolve trading account: {e}")


async def get_all_trading_accounts() -> Dict[int, Dict[str, str]]:
    """
    Get all trading accounts via dynamic resolution.
    
    Sprint 1: Uses config-driven account IDs instead of hardcoding.
    
    Returns:
        Dict mapping trading_account_id to account config
    """
    # Get trading account IDs from configuration
    # Try config service first, fallback to settings, then minimal default
    try:
        from ..config.settings import settings
        
        # Check if settings has a trading_account_ids list
        account_ids_config = getattr(settings, 'trading_account_ids', None)
        if account_ids_config:
            known_account_ids = account_ids_config
        else:
            # Try to get from config service  
            try:
                from ..config.settings import _get_config_value
                config_value = _get_config_value("TRADING_ACCOUNT_IDS", required=False)
                if config_value:
                    # Parse comma-separated list: "1,2,3" -> [1, 2, 3]
                    known_account_ids = [int(x.strip()) for x in config_value.split(",") if x.strip().isdigit()]
                else:
                    # Minimal fallback - only account 1 (primary)
                    logger.warning("No trading account IDs configured, using minimal fallback [1]")
                    known_account_ids = [1]
            except ImportError:
                # Config service not available - fail fast
                raise RuntimeError("Settings module required - config service unavailable")
                
    except Exception as e:
        logger.error(f"Failed to load trading account IDs from config: {e}")
        # Emergency fallback
        known_account_ids = [1]
    
    accounts = {}
    for account_id in known_account_ids:
        try:
            config = await resolve_trading_account_config(account_id)
            accounts[account_id] = config
        except Exception as e:
            logger.error(f"Failed to resolve trading_account {account_id}: {e}")
            # Skip failed accounts rather than breaking entirely
            continue
    
    return accounts


class MultiAccountKiteClient:
    """
    Multi-account KiteConnect REST API client.

    Supports routing orders to different Kite accounts based on trading_account_id.
    Uses Token Manager Service for access tokens.
    """

    def __init__(self, trading_account_id: Union[int, str]):
        """
        Initialize client for a specific trading account.

        Args:
            trading_account_id: Database trading account ID (int or string)
        """
        # Ensure trading_account_id is int for proper dictionary lookup
        # (account_context returns string from header, but mapping uses int keys)
        self.trading_account_id = int(trading_account_id) if isinstance(trading_account_id, str) else trading_account_id

        # Use settings for token manager config (config-service only - no fallbacks)
        self.token_manager_url = settings.token_manager_url
        self.token_manager_api_key = settings.internal_api_key

        # Sprint 1: Account config will be resolved dynamically via async factory
        # Initialize with placeholders, will be populated by async factory method
        self.account_nickname: Optional[str] = None
        self.api_key: Optional[str] = None
        self._account_config: Optional[Dict[str, str]] = None

        # Kite client state
        self._kite: Optional[KiteConnect] = None
        self._access_token: Optional[str] = None

        # Note: Account config not resolved yet - use async factory method
        logger.info(f"MultiAccountKiteClient created for trading_account={trading_account_id} (config pending)")

    @classmethod
    async def create(cls, trading_account_id: Union[int, str]) -> 'MultiAccountKiteClient':
        """
        Async factory method to create and initialize MultiAccountKiteClient.
        
        Sprint 1: Replaces direct constructor usage with dynamic account resolution.
        
        Args:
            trading_account_id: Database trading account ID
            
        Returns:
            Fully initialized MultiAccountKiteClient instance
            
        Raises:
            ValueError: If account resolution fails
        """
        # Create instance with sync constructor
        instance = cls(trading_account_id)
        
        # Resolve account configuration dynamically
        await instance._resolve_account_config()
        
        return instance
    
    async def _resolve_account_config(self):
        """
        Resolve and cache account configuration for this trading_account_id.
        
        Sprint 1: Uses new token_manager account resolution API.
        """
        try:
            # Use new dynamic resolution
            self._account_config = await resolve_trading_account_config(self.trading_account_id)
            self.account_nickname = self._account_config["nickname"]
            self.api_key = self._account_config["api_key"]
            
            # Validate account is active
            if not self._account_config.get("is_active", True):
                raise ValueError(f"Trading account {self.trading_account_id} is inactive")
            
            logger.info(
                f"MultiAccountKiteClient account resolved: "
                f"trading_account={self.trading_account_id} -> "
                f"nickname={self.account_nickname}, broker={self._account_config.get('broker', 'unknown')}"
            )
            
        except Exception as e:
            logger.error(f"Failed to resolve account config for trading_account {self.trading_account_id}: {e}")
            
            # Sprint 1: No fallback - fail hard to force proper account registry usage
            raise ValueError(
                f"Cannot resolve trading_account_id {self.trading_account_id}. "
                f"Account registry lookup failed: {e}. "
                f"Ensure trading account exists and token_manager is accessible."
            )

    async def _acquire_rate_limit(
        self,
        operation: KiteOperation,
        wait: bool = True,
        timeout: float = 30.0
    ) -> None:
        """
        Acquire rate limit before executing Kite API call.

        Args:
            operation: Type of Kite operation
            wait: Wait for rate limit if exceeded
            timeout: Maximum wait time

        Raises:
            RateLimitExceeded: If limit exceeded and wait=False
            DailyLimitExceeded: If daily order limit exhausted
        """
        manager = get_rate_limiter_manager_sync()
        if manager is None:
            logger.warning(
                "Rate limiter not initialized, skipping rate limit check",
                extra={"trading_account_id": self.trading_account_id}
            )
            return

        await manager.acquire(
            trading_account_id=self.trading_account_id,
            operation=operation,
            wait=wait,
            timeout=timeout
        )

    async def _fetch_access_token(self) -> str:
        """
        Fetch fresh access token from Token Manager Service.

        Returns:
            Access token string

        Raises:
            Exception: If token cannot be fetched
        """
        try:
            # Build headers with optional API key authentication
            headers = {}
            if self.token_manager_api_key:
                headers["X-Internal-API-Key"] = self.token_manager_api_key

            async with httpx.AsyncClient() as client:
                # Sprint 1: Use new trading_account_id based endpoint
                response = await client.get(
                    f"{self.token_manager_url}/api/v1/tokens/by-trading-account/{self.trading_account_id}",
                    headers=headers,
                    timeout=10.0
                )
                response.raise_for_status()

                data = response.json()
                access_token = data.get("access_token")
                api_key = data.get("api_key")

                if not access_token:
                    raise ValueError(f"No access_token for account {self.account_nickname}")

                # Update API key if returned by token manager
                if api_key:
                    self.api_key = api_key

                logger.info(
                    f"Fetched access token for {self.account_nickname} "
                    f"(trading_account={self.trading_account_id})"
                )
                return access_token

        except httpx.HTTPStatusError as e:
            logger.error(
                f"Token Manager error for {self.account_nickname}: "
                f"{e.response.status_code} - {e.response.text}"
            )
            raise
        except Exception as e:
            logger.error(f"Failed to fetch token for {self.account_nickname}: {e}")
            raise

    async def _get_kite_client(self) -> KiteConnect:
        """
        Get authenticated KiteConnect client for this account.

        Returns:
            Authenticated KiteConnect instance
        """
        if not self._access_token:
            self._access_token = await self._fetch_access_token()

        if not self._kite:
            self._kite = KiteConnect(api_key=self.api_key)
            self._kite.set_access_token(self._access_token)
            logger.info(
                f"KiteConnect client initialized for {self.account_nickname}"
            )

        return self._kite

    async def refresh_token(self) -> None:
        """Refresh access token from Token Manager."""
        self._access_token = await self._fetch_access_token()
        if self._kite:
            self._kite.set_access_token(self._access_token)
            logger.info(f"Token refreshed for {self.account_nickname}")

    async def _with_token_refresh(self, operation: Callable[[], T], operation_name: str) -> T:
        """
        Execute a Kite API operation with automatic token refresh on auth errors.

        This solves the stale token problem where Token Manager refreshes tokens
        but order_service continues using cached (now invalid) tokens.

        Args:
            operation: The Kite API operation to execute (sync callable)
            operation_name: Name of operation for logging

        Returns:
            Result of the operation
        """
        try:
            kite = await self._get_kite_client()
            return operation(kite)
        except (TokenException, Exception) as e:
            error_msg = str(e).lower()
            # Check if it's a token/session error
            if "token" in error_msg or "session" in error_msg or "api_key" in error_msg:
                logger.warning(
                    f"{operation_name} failed with token error for {self.account_nickname}, "
                    f"refreshing token and retrying: {e}"
                )
                await self.refresh_token()
                kite = await self._get_kite_client()
                return operation(kite)
            else:
                logger.error(f"Failed to {operation_name} ({self.account_nickname}): {e}")
                raise

    # ==========================================
    # ORDER OPERATIONS
    # ==========================================

    async def place_order(
        self,
        tradingsymbol: str,
        exchange: str,
        transaction_type: str,
        quantity: int,
        order_type: str,
        product: str,
        price: Optional[float] = None,
        trigger_price: Optional[float] = None,
        validity: str = "DAY",
        variety: str = "regular",
        tag: Optional[str] = None,
    ) -> str:
        """
        Place an order via KiteConnect REST API.

        Rate limited:
        - 10 orders per second
        - 200 orders per minute
        - 3000 orders per day

        Returns:
            Broker order ID

        Raises:
            RateLimitExceeded: If per-second/minute limit exceeded and wait timed out
            DailyLimitExceeded: If daily limit (3000) exhausted
        """
        # Acquire rate limit BEFORE placing order
        await self._acquire_rate_limit(KiteOperation.ORDER_PLACE)

        try:
            kite = await self._get_kite_client()

            order_params = {
                "symbol": tradingsymbol,
                "exchange": exchange,
                "transaction_type": transaction_type,
                "quantity": quantity,
                "order_type": order_type,
                "product": product,
                "validity": validity,
                "variety": variety,
            }

            if price is not None:
                order_params["price"] = price
            if trigger_price is not None:
                order_params["trigger_price"] = trigger_price
            if tag:
                order_params["tag"] = tag

            logger.info(
                f"Placing order via {self.account_nickname}: "
                f"{transaction_type} {quantity} {tradingsymbol} @ {order_type}"
            )

            order_id = kite.place_order(**order_params)

            logger.info(
                f"Order placed: {order_id} "
                f"(account={self.account_nickname}, trading_account={self.trading_account_id})"
            )
            return order_id

        except Exception as e:
            logger.error(f"Order placement failed ({self.account_nickname}): {e}")

            # Try refreshing token on auth errors
            if "token" in str(e).lower() or "session" in str(e).lower():
                logger.info("Attempting token refresh and retry...")
                await self.refresh_token()

                kite = await self._get_kite_client()
                order_id = kite.place_order(**order_params)
                logger.info(f"Order placed after token refresh: {order_id}")
                return order_id

            raise

    async def modify_order(
        self,
        order_id: str,
        quantity: Optional[int] = None,
        price: Optional[float] = None,
        trigger_price: Optional[float] = None,
        order_type: Optional[str] = None,
        validity: Optional[str] = None,
    ) -> str:
        """
        Modify an existing order.

        Rate limited: 10 modifications per second
        Note: Max 25 modifications per order (enforced by Kite)
        """
        # Acquire rate limit
        await self._acquire_rate_limit(KiteOperation.ORDER_MODIFY)

        try:
            kite = await self._get_kite_client()

            modify_params = {"order_id": order_id, "variety": "regular"}

            if quantity is not None:
                modify_params["quantity"] = quantity
            if price is not None:
                modify_params["price"] = price
            if trigger_price is not None:
                modify_params["trigger_price"] = trigger_price
            if order_type is not None:
                modify_params["order_type"] = order_type
            if validity is not None:
                modify_params["validity"] = validity

            logger.info(f"Modifying order {order_id} ({self.account_nickname})")

            result = kite.modify_order(**modify_params)
            logger.info(f"Order modified: {order_id}")
            return result

        except Exception as e:
            logger.error(f"Order modification failed ({self.account_nickname}): {e}")
            raise

    async def cancel_order(self, order_id: str, variety: str = "regular") -> str:
        """
        Cancel an order.

        Rate limited: 10 cancellations per second
        """
        # Acquire rate limit
        await self._acquire_rate_limit(KiteOperation.ORDER_CANCEL)

        try:
            kite = await self._get_kite_client()

            logger.info(f"Cancelling order {order_id} ({self.account_nickname})")

            result = kite.cancel_order(order_id=order_id, variety=variety)
            logger.info(f"Order cancelled: {order_id}")
            return result

        except Exception as e:
            logger.error(f"Order cancellation failed ({self.account_nickname}): {e}")
            raise

    async def get_orders(self) -> list:
        """
        Get all orders for the day with automatic token refresh on auth errors.

        Rate limited: 10 requests per second
        """
        await self._acquire_rate_limit(KiteOperation.API_GET)

        def _fetch(kite):
            orders = kite.orders()
            logger.debug(f"Fetched {len(orders)} orders ({self.account_nickname})")
            return orders
        return await self._with_token_refresh(_fetch, "get_orders")

    async def get_order_history(self, order_id: str) -> list:
        """
        Get order history with automatic token refresh on auth errors.

        Rate limited: 10 requests per second
        """
        await self._acquire_rate_limit(KiteOperation.API_GET)

        def _fetch(kite):
            return kite.order_history(order_id=order_id)
        try:
            return await self._with_token_refresh(_fetch, f"get_order_history({order_id})")
        except Exception as e:
            error_msg = str(e)
            if "Couldn't parse the JSON response" in error_msg:
                logger.warning(f"Malformed JSON for order {order_id}")
                return []
            raise

    async def get_positions(self) -> Dict[str, Any]:
        """
        Get current positions with automatic token refresh on auth errors.

        Rate limited: 10 requests per second
        """
        await self._acquire_rate_limit(KiteOperation.API_GET)

        def _fetch(kite):
            positions = kite.positions()
            logger.debug(
                f"Fetched positions ({self.account_nickname}): "
                f"net={len(positions.get('net', []))}"
            )
            return positions
        return await self._with_token_refresh(_fetch, "get_positions")

    async def get_holdings(self) -> list:
        """
        Get holdings with automatic token refresh on auth errors.

        Rate limited: 10 requests per second
        """
        await self._acquire_rate_limit(KiteOperation.API_GET)

        def _fetch(kite):
            holdings = kite.holdings()
            logger.debug(f"Fetched {len(holdings)} holdings ({self.account_nickname})")
            return holdings
        return await self._with_token_refresh(_fetch, "get_holdings")

    async def get_margins(self, segment: Optional[str] = None) -> Dict[str, Any]:
        """
        Get account margins with automatic token refresh on auth errors.

        Rate limited: 10 requests per second
        """
        await self._acquire_rate_limit(KiteOperation.API_GET)

        def _fetch(kite):
            if segment:
                return kite.margins(segment=segment)
            return kite.margins()
        return await self._with_token_refresh(_fetch, "get_margins")

    # ==========================================
    # GTT (GOOD-TILL-TRIGGERED) OPERATIONS
    # ==========================================

    async def place_gtt(
        self,
        gtt_type: str,
        tradingsymbol: str,
        exchange: str,
        trigger_values: list,
        last_price: float,
        orders: list
    ) -> int:
        """
        Place a GTT (Good-Till-Triggered) order.

        Rate limited: 10 requests per second (API_GET limit)

        Args:
            gtt_type: 'single' or 'two-leg' (OCO)
            tradingsymbol: Trading symbol
            exchange: Exchange code
            trigger_values: List of trigger prices
            last_price: Current market price
            orders: List of order specifications

        Returns:
            GTT trigger ID from broker
        """
        await self._acquire_rate_limit(KiteOperation.API_GET)

        try:
            kite = await self._get_kite_client()

            logger.info(
                f"Placing GTT ({self.account_nickname}): "
                f"type={gtt_type}, symbol={tradingsymbol}, triggers={trigger_values}"
            )

            result = kite.place_gtt(
                trigger_type=kite.GTT_TYPE_OCO if gtt_type == 'two-leg' else kite.GTT_TYPE_SINGLE,
                tradingsymbol=tradingsymbol,
                exchange=exchange,
                trigger_values=trigger_values,
                last_price=last_price,
                orders=orders
            )

            trigger_id = result.get('trigger_id')
            logger.info(f"GTT placed: trigger_id={trigger_id} ({self.account_nickname})")
            return trigger_id

        except Exception as e:
            logger.error(f"GTT placement failed ({self.account_nickname}): {e}")
            raise

    async def get_gtts(self) -> list:
        """
        Get all GTT orders for the account with automatic token refresh on auth errors.

        Rate limited: 10 requests per second
        """
        await self._acquire_rate_limit(KiteOperation.API_GET)

        def _fetch(kite):
            gtts = kite.get_gtts()
            logger.debug(f"Fetched {len(gtts)} GTT orders ({self.account_nickname})")
            return gtts
        return await self._with_token_refresh(_fetch, "get_gtts")

    async def get_gtt(self, trigger_id: int) -> Dict[str, Any]:
        """
        Get a specific GTT order by trigger ID with automatic token refresh on auth errors.

        Rate limited: 10 requests per second
        """
        await self._acquire_rate_limit(KiteOperation.API_GET)

        def _fetch(kite):
            return kite.get_gtt(trigger_id)
        return await self._with_token_refresh(_fetch, f"get_gtt({trigger_id})")

    async def modify_gtt(
        self,
        trigger_id: int,
        gtt_type: str,
        tradingsymbol: str,
        exchange: str,
        trigger_values: list,
        last_price: float,
        orders: list
    ) -> int:
        """
        Modify an existing GTT order.

        Rate limited: 10 requests per second
        """
        await self._acquire_rate_limit(KiteOperation.API_GET)

        try:
            kite = await self._get_kite_client()

            logger.info(f"Modifying GTT {trigger_id} ({self.account_nickname})")

            result = kite.modify_gtt(
                trigger_id=trigger_id,
                trigger_type=kite.GTT_TYPE_OCO if gtt_type == 'two-leg' else kite.GTT_TYPE_SINGLE,
                tradingsymbol=tradingsymbol,
                exchange=exchange,
                trigger_values=trigger_values,
                last_price=last_price,
                orders=orders
            )

            new_trigger_id = result.get('trigger_id')
            logger.info(f"GTT modified: trigger_id={new_trigger_id}")
            return new_trigger_id

        except Exception as e:
            logger.error(f"GTT modification failed ({self.account_nickname}): {e}")
            raise

    async def delete_gtt(self, trigger_id: int) -> int:
        """
        Delete (cancel) a GTT order.

        Rate limited: 10 requests per second
        """
        await self._acquire_rate_limit(KiteOperation.API_GET)

        try:
            kite = await self._get_kite_client()

            logger.info(f"Deleting GTT {trigger_id} ({self.account_nickname})")

            result = kite.delete_gtt(trigger_id)
            deleted_id = result.get('trigger_id')
            logger.info(f"GTT deleted: trigger_id={deleted_id}")
            return deleted_id

        except Exception as e:
            logger.error(f"GTT deletion failed ({self.account_nickname}): {e}")
            raise

    async def get_trades(self) -> list:
        """
        Get all trades for the day with automatic token refresh on auth errors.

        Rate limited: 10 requests per second
        """
        await self._acquire_rate_limit(KiteOperation.API_GET)

        def _fetch(kite):
            trades = kite.trades()
            logger.debug(f"Fetched {len(trades)} trades ({self.account_nickname})")
            return trades
        return await self._with_token_refresh(_fetch, "get_trades")

    # ==========================================
    # MARGIN CALCULATION
    # ==========================================

    async def calculate_order_margins(self, orders: list) -> list:
        """
        Calculate required margins for a list of orders with automatic token refresh.

        Rate limited: 10 requests per second

        Args:
            orders: List of order dictionaries

        Returns:
            List of margin requirements per order
        """
        await self._acquire_rate_limit(KiteOperation.API_GET)

        def _fetch(kite):
            margins = kite.order_margins(orders)
            logger.debug(f"Calculated margins for {len(orders)} orders ({self.account_nickname})")
            return margins
        return await self._with_token_refresh(_fetch, "calculate_order_margins")

    async def calculate_basket_margins(
        self,
        orders: list,
        consider_positions: bool = True,
        mode: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Calculate total margins for basket of orders with automatic token refresh.

        Rate limited: 10 requests per second

        Args:
            orders: List of order dictionaries
            consider_positions: Whether to consider existing positions
            mode: Response mode ('compact' for totals only)

        Returns:
            Dictionary with total margin requirements and per-order breakdown
        """
        await self._acquire_rate_limit(KiteOperation.API_GET)

        def _fetch(kite):
            margins = kite.basket_order_margins(
                orders,
                consider_positions=consider_positions,
                mode=mode
            )
            logger.debug(f"Calculated basket margins for {len(orders)} orders ({self.account_nickname})")
            return margins
        return await self._with_token_refresh(_fetch, "calculate_basket_margins")


# Client cache for reusing clients per trading_account_id
_client_cache: Dict[int, MultiAccountKiteClient] = {}


def get_kite_client_for_account(trading_account_id: Union[int, str]) -> MultiAccountKiteClient:
    """
    Get or create a KiteConnect client for a specific trading account.
    
    Sprint 1: Transitioning to async account resolution with sync fallback.

    Args:
        trading_account_id: Database trading account ID (int or string)

    Returns:
        MultiAccountKiteClient instance with resolved account config
    """
    global _client_cache

    # Normalize to int for consistent cache lookup
    account_id = int(trading_account_id) if isinstance(trading_account_id, str) else trading_account_id

    if account_id not in _client_cache:
        # Sprint 1: Create client with old constructor but try to resolve config
        client = MultiAccountKiteClient(account_id)
        
        # Attempt to resolve account config synchronously for Sprint 1 compatibility
        # This uses the fallback in _resolve_account_config
        try:
            import asyncio
            
            # Run the async resolution in sync context using run_in_executor
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                loop.run_until_complete(client._resolve_account_config())
            finally:
                loop.close()
                
        except Exception as e:
            logger.error(f"Account resolution failed for trading_account={account_id}: {e}")
            raise ValueError(
                f"Cannot create KiteClient for trading_account_id {account_id}. "
                f"Account resolution failed: {e}. Ensure account exists in registry."
            )
        
        # Only cache if resolution succeeded
        if client.account_nickname and client.api_key:
            _client_cache[account_id] = client
            logger.info(
                f"Created new KiteClient for trading_account={account_id} -> {client.account_nickname}"
            )
        else:
            raise ValueError(
                f"Incomplete account config for trading_account_id {account_id}. "
                f"Missing nickname or API key after resolution."
            )

    return _client_cache[account_id]


async def get_kite_client_for_account_async(trading_account_id: Union[int, str]) -> MultiAccountKiteClient:
    """
    Async version of get_kite_client_for_account using new dynamic resolution.
    
    Sprint 1: Preferred method for new async code.

    Args:
        trading_account_id: Database trading account ID

    Returns:
        MultiAccountKiteClient instance with dynamically resolved config
    """
    global _client_cache

    # Normalize to int for consistent cache lookup
    account_id = int(trading_account_id) if isinstance(trading_account_id, str) else trading_account_id

    if account_id not in _client_cache:
        # Use new async factory method
        client = await MultiAccountKiteClient.create(account_id)
        _client_cache[account_id] = client
        logger.info(
            f"Created new async KiteClient for trading_account={account_id} -> {client.account_nickname}"
        )

    return _client_cache[account_id]


def clear_client_cache():
    """Clear all cached clients (useful for testing)."""
    global _client_cache
    _client_cache.clear()
    logger.info("KiteClient cache cleared")

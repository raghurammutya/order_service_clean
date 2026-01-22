"""
KiteConnect Client for Order Service

This client uses the Token Manager Service to get fresh access tokens
and uses ONLY the KiteConnect REST API (no WebSocket connections).

The WebSocket connection is maintained exclusively by ticker_service_v2
for market data streaming.

Config Service Integration:
- TOKEN_MANAGER_URL and TOKEN_MANAGER_INTERNAL_API_KEY can be fetched from config service
- Falls back to environment variables if config service unavailable
"""
import os
import re
import logging
from typing import Optional, Dict, Any
import httpx
from kiteconnect import KiteConnect

from ..config.settings import settings, _get_from_config_service

logger = logging.getLogger(__name__)

# =============================================================================
# INPUT VALIDATION PATTERNS
# =============================================================================

# Trading symbol patterns for Indian markets
# Equity symbols: uppercase letters and numbers, 1-20 chars (e.g., RELIANCE, TATASTEEL, M&M)
EQUITY_SYMBOL_PATTERN = re.compile(r'^[A-Z0-9&-]{1,20}$')

# F&O symbols: symbol + expiry + strike + option type or FUT
# Examples: NIFTY25DEC24500CE, BANKNIFTY25DEC55000PE, RELIANCE25DECFUT
FNO_SYMBOL_PATTERN = re.compile(r'^[A-Z0-9&-]{1,20}\d{2}[A-Z]{3}(\d+[CP]E|FUT)$')

# Valid exchanges
VALID_EXCHANGES = {'NSE', 'BSE', 'NFO', 'BFO', 'CDS', 'MCX'}

# Valid transaction types
VALID_TRANSACTION_TYPES = {'BUY', 'SELL'}

# Valid order types
VALID_ORDER_TYPES = {'MARKET', 'LIMIT', 'SL', 'SL-M'}

# Valid products
VALID_PRODUCTS = {'CNC', 'MIS', 'NRML'}


def validate_trading_symbol(tradingsymbol: str, exchange: str) -> bool:
    """
    Validate trading symbol format to prevent injection attacks.

    Args:
        tradingsymbol: Trading symbol to validate
        exchange: Exchange code

    Returns:
        True if valid, False otherwise
    """
    if not tradingsymbol or len(tradingsymbol) > 50:
        return False

    # Check for dangerous characters
    if any(c in tradingsymbol for c in ['/', '\\', '..', '<', '>', ';', "'", '"']):
        logger.warning(f"Symbol contains dangerous characters: {tradingsymbol[:30]}")
        return False

    # Exchange-specific validation
    if exchange in {'NSE', 'BSE'}:
        return bool(EQUITY_SYMBOL_PATTERN.match(tradingsymbol))
    elif exchange in {'NFO', 'BFO', 'CDS', 'MCX'}:
        # F&O can be either equity-style or derivatives-style
        return bool(EQUITY_SYMBOL_PATTERN.match(tradingsymbol) or
                    FNO_SYMBOL_PATTERN.match(tradingsymbol))

    return False


def validate_exchange(exchange: str) -> bool:
    """Validate exchange code."""
    return exchange in VALID_EXCHANGES


def validate_order_params(
    tradingsymbol: str,
    exchange: str,
    transaction_type: str,
    quantity: int,
    order_type: str,
    product: str
) -> tuple[bool, str]:
    """
    Validate all order parameters.

    Returns:
        Tuple of (is_valid, error_message)
    """
    if not validate_exchange(exchange):
        return False, f"Invalid exchange: {exchange}"

    if not validate_trading_symbol(tradingsymbol, exchange):
        return False, f"Invalid trading symbol format: {tradingsymbol}"

    if transaction_type not in VALID_TRANSACTION_TYPES:
        return False, f"Invalid transaction type: {transaction_type}"

    if not isinstance(quantity, int) or quantity <= 0 or quantity > 10000000:
        return False, f"Invalid quantity: {quantity}"

    if order_type not in VALID_ORDER_TYPES:
        return False, f"Invalid order type: {order_type}"

    if product not in VALID_PRODUCTS:
        return False, f"Invalid product: {product}"

    return True, ""


class KiteOrderClient:
    """
    KiteConnect REST API client for order operations.

    Uses Token Manager Service for access tokens.
    Does NOT create WebSocket connections (those are in ticker_service_v2).
    """

    def __init__(self):
        self.api_key = settings.kite_api_key  # Fallback, will be updated from token_manager
        self.account_id = settings.kite_account_id

        # Try config service first, then env vars
        self.token_manager_url = _get_from_config_service(
            "TOKEN_MANAGER_URL",
            os.getenv("TOKEN_MANAGER_URL", "http://localhost:8086")
        )
        # ARCHITECTURE PRINCIPLE #24: Use single INTERNAL_API_KEY for all service-to-service auth
        self.internal_api_key = settings.internal_api_key

        self._kite: Optional[KiteConnect] = None
        self._access_token: Optional[str] = None

    async def _fetch_access_token(self) -> str:
        """
        Fetch fresh access token from Token Manager Service.

        Returns:
            Access token string

        Raises:
            HTTPException: If token cannot be fetched
        """
        try:
            # Build headers with optional API key authentication
            headers = {}
            if self.internal_api_key:
                headers["X-Internal-API-Key"] = self.internal_api_key

            async with httpx.AsyncClient() as client:
                response = await client.get(
                    f"{self.token_manager_url}/api/v1/tokens/{self.account_id}",
                    headers=headers,
                    timeout=10.0
                )
                response.raise_for_status()

                data = response.json()
                access_token = data.get("access_token")
                api_key = data.get("api_key")

                if not access_token:
                    raise ValueError("No access_token in response from Token Manager")

                # Update API key if returned by token manager
                if api_key:
                    self.api_key = api_key

                logger.info(f"Fetched access token from Token Manager for account: {self.account_id}")
                return access_token

        except httpx.HTTPStatusError as e:
            logger.error(f"Token Manager returned error: {e.response.status_code} - {e.response.text}")
            raise
        except Exception as e:
            logger.error(f"Failed to fetch access token: {e}")
            raise

    async def _get_kite_client(self) -> KiteConnect:
        """
        Get authenticated KiteConnect client.

        Returns:
            Authenticated KiteConnect instance
        """
        # Fetch fresh token if we don't have one
        if not self._access_token:
            self._access_token = await self._fetch_access_token()

        # Create KiteConnect client if needed
        if not self._kite:
            self._kite = KiteConnect(api_key=self.api_key)
            self._kite.set_access_token(self._access_token)
            logger.info("KiteConnect client initialized")

        return self._kite

    async def refresh_token(self) -> None:
        """Refresh access token from Token Manager"""
        self._access_token = await self._fetch_access_token()
        if self._kite:
            self._kite.set_access_token(self._access_token)
            logger.info("Access token refreshed")

    # ==========================================
    # ORDER OPERATIONS (REST API ONLY)
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

        Args:
            tradingsymbol: Trading symbol
            exchange: Exchange (NSE, NFO, BSE, etc.)
            transaction_type: BUY or SELL
            quantity: Order quantity
            order_type: MARKET, LIMIT, SL, SL-M
            product: CNC, MIS, NRML
            price: Limit price (for LIMIT orders)
            trigger_price: Trigger price (for SL orders)
            validity: DAY or IOC
            variety: regular, amo, iceberg, auction
            tag: Custom order tag

        Returns:
            Broker order ID

        Raises:
            ValueError: If order parameters are invalid
            Exception: If order placement fails
        """
        # Validate all order parameters before sending to broker
        is_valid, error_msg = validate_order_params(
            tradingsymbol=tradingsymbol,
            exchange=exchange,
            transaction_type=transaction_type,
            quantity=quantity,
            order_type=order_type,
            product=product
        )
        if not is_valid:
            logger.warning(f"Order validation failed: {error_msg}")
            raise ValueError(f"Order validation failed: {error_msg}")

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

            # Add optional parameters
            if price is not None:
                order_params["price"] = price
            if trigger_price is not None:
                order_params["trigger_price"] = trigger_price
            if tag:
                order_params["tag"] = tag

            logger.info(f"Placing order: {order_params}")

            # Place order via REST API
            order_id = kite.place_order(**order_params)

            logger.info(f"Order placed successfully: {order_id}")
            return order_id

        except Exception as e:
            logger.error(f"Order placement failed: {e}")

            # Try refreshing token once on authentication errors
            if "token" in str(e).lower() or "session" in str(e).lower():
                logger.info("Attempting to refresh token and retry...")
                await self.refresh_token()

                kite = await self._get_kite_client()
                order_id = kite.place_order(**order_params)
                logger.info(f"Order placed successfully after token refresh: {order_id}")
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
        Modify an existing order via KiteConnect REST API.

        Args:
            order_id: Broker order ID to modify
            quantity: New quantity
            price: New price
            trigger_price: New trigger price
            order_type: New order type
            validity: New validity

        Returns:
            Broker order ID
        """
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

            logger.info(f"Modifying order {order_id}: {modify_params}")

            result = kite.modify_order(**modify_params)

            logger.info(f"Order modified successfully: {result}")
            return result

        except Exception as e:
            logger.error(f"Order modification failed: {e}")
            raise

    async def cancel_order(self, order_id: str, variety: str = "regular") -> str:
        """
        Cancel an order via KiteConnect REST API.

        Args:
            order_id: Broker order ID to cancel
            variety: Order variety

        Returns:
            Broker order ID
        """
        try:
            kite = await self._get_kite_client()

            logger.info(f"Cancelling order: {order_id}")

            result = kite.cancel_order(order_id=order_id, variety=variety)

            logger.info(f"Order cancelled successfully: {result}")
            return result

        except Exception as e:
            logger.error(f"Order cancellation failed: {e}")
            raise

    async def get_orders(self) -> list:
        """
        Get all orders for the day via KiteConnect REST API.

        Returns:
            List of orders
        """
        try:
            kite = await self._get_kite_client()

            orders = kite.orders()

            logger.debug(f"Fetched {len(orders)} orders")
            return orders

        except Exception as e:
            logger.error(f"Failed to fetch orders: {e}")
            raise

    async def get_order_history(self, order_id: str) -> list:
        """
        Get order history via KiteConnect REST API.

        Args:
            order_id: Broker order ID

        Returns:
            Order history list
        """
        try:
            kite = await self._get_kite_client()

            history = kite.order_history(order_id=order_id)

            logger.debug(f"Fetched history for order {order_id}: {len(history)} entries")
            return history

        except Exception as e:
            error_msg = str(e)

            # Handle JSON parsing errors from malformed broker responses
            if "Couldn't parse the JSON response" in error_msg or "JSON decode error" in error_msg:
                logger.warning(f"Broker API returned malformed JSON for order {order_id}: {error_msg}")

                # Check if this is an invalid order_id error
                if "Invalid `order_id`" in error_msg:
                    logger.debug(f"Order {order_id} not found in broker system")
                    return []  # Return empty history for non-existent orders

                # For other JSON errors, return empty list with warning
                logger.error(f"JSON parsing error for order {order_id}, returning empty history")
                return []

            # For other errors, re-raise
            logger.error(f"Failed to fetch order history: {e}")
            raise

    async def get_trades(self) -> list:
        """
        Get all trades for the day via KiteConnect REST API.

        Returns:
            List of trades
        """
        try:
            kite = await self._get_kite_client()

            trades = kite.trades()

            logger.debug(f"Fetched {len(trades)} trades")
            return trades

        except Exception as e:
            logger.error(f"Failed to fetch trades: {e}")
            raise

    async def get_positions(self) -> Dict[str, Any]:
        """
        Get current positions via KiteConnect REST API.

        Returns:
            Positions dictionary with 'net' and 'day' positions
        """
        try:
            kite = await self._get_kite_client()

            positions = kite.positions()

            logger.debug(f"Fetched positions: net={len(positions.get('net', []))}, day={len(positions.get('day', []))}")
            return positions

        except Exception as e:
            logger.error(f"Failed to fetch positions: {e}")
            raise

    async def get_holdings(self) -> list:
        """
        Get long-term equity holdings via KiteConnect REST API.

        Returns:
            List of holdings with current value and P&L
        """
        try:
            kite = await self._get_kite_client()

            holdings = kite.holdings()

            logger.debug(f"Fetched {len(holdings)} holdings")
            return holdings

        except Exception as e:
            logger.error(f"Failed to fetch holdings: {e}")
            raise

    async def get_margins(self, segment: Optional[str] = None) -> Dict[str, Any]:
        """
        Get account margins and cash balances via KiteConnect REST API.

        Args:
            segment: Trading segment (equity, commodity). None for all segments.

        Returns:
            Margins dictionary with available balance, used margin, etc.
        """
        try:
            kite = await self._get_kite_client()

            if segment:
                margins = kite.margins(segment=segment)
            else:
                margins = kite.margins()

            logger.debug(f"Fetched margins for segment: {segment}")
            return margins

        except Exception as e:
            logger.error(f"Failed to fetch margins: {e}")
            raise

    async def calculate_order_margins(self, orders: list) -> list:
        """
        Calculate required margins for a list of orders.

        Args:
            orders: List of order dictionaries

        Returns:
            List of margin requirements per order
        """
        try:
            kite = await self._get_kite_client()

            margins = kite.order_margins(orders)

            logger.debug(f"Calculated margins for {len(orders)} orders")
            return margins

        except Exception as e:
            logger.error(f"Failed to calculate order margins: {e}")
            raise

    async def calculate_basket_margins(
        self,
        orders: list,
        consider_positions: bool = True,
        mode: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Calculate total margins for basket of orders including margin benefits.

        Args:
            orders: List of order dictionaries
            consider_positions: Whether to consider existing positions
            mode: Response mode ('compact' for totals only)

        Returns:
            Dictionary with total margin requirements and per-order breakdown
        """
        try:
            kite = await self._get_kite_client()

            margins = kite.basket_order_margins(
                orders,
                consider_positions=consider_positions,
                mode=mode
            )

            logger.debug(f"Calculated basket margins for {len(orders)} orders")
            return margins

        except Exception as e:
            logger.error(f"Failed to calculate basket margins: {e}")
            raise

    # ==========================================
    # GTT (GOOD-TILL-TRIGGERED) ORDERS
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
        Place a GTT (Good-Till-Triggered) order via KiteConnect REST API.

        Args:
            gtt_type: 'single' or 'two-leg' (OCO - One Cancels Other)
            tradingsymbol: Trading symbol (e.g., 'RELIANCE', 'NIFTY25DEC24500CE')
            exchange: Exchange code (NSE, NFO, BSE, etc.)
            trigger_values: List of trigger prices [price1] or [price1, price2]
            last_price: Current market price (required for validation)
            orders: List of order dicts to place when triggered

        Returns:
            GTT trigger ID from broker

        Example (Stop-loss):
            await place_gtt(
                gtt_type='single',
                tradingsymbol='RELIANCE',
                exchange='NSE',
                trigger_values=[2400],  # Trigger if price falls to 2400
                last_price=2500,
                orders=[{
                    'transaction_type': 'SELL',
                    'quantity': 10,
                    'order_type': 'LIMIT',
                    'product': 'CNC',
                    'price': 2400
                }]
            )

        Example (OCO - One Cancels Other):
            await place_gtt(
                gtt_type='two-leg',
                tradingsymbol='RELIANCE',
                exchange='NSE',
                trigger_values=[2400, 2600],  # Stop-loss at 2400, target at 2600
                last_price=2500,
                orders=[
                    {'transaction_type': 'SELL', 'quantity': 10, 'price': 2400, ...},
                    {'transaction_type': 'SELL', 'quantity': 10, 'price': 2600, ...}
                ]
            )
        """
        try:
            kite = await self._get_kite_client()

            logger.info(
                f"Placing GTT order: type={gtt_type}, symbol={tradingsymbol}, "
                f"triggers={trigger_values}"
            )

            # Prepare condition based on GTT type
            if gtt_type == 'single':
                condition = {
                    'exchange': exchange,
                    "symbol": tradingsymbol,
                    'trigger_values': trigger_values,
                    'last_price': last_price
                }
            else:  # two-leg
                condition = {
                    'exchange': exchange,
                    "symbol": tradingsymbol,
                    'trigger_values': trigger_values,
                    'last_price': last_price
                }

            # Place GTT via Kite API
            gtt_id = kite.place_gtt(
                trigger_type=gtt_type,
                tradingsymbol=tradingsymbol,
                exchange=exchange,
                trigger_values=trigger_values,
                last_price=last_price,
                orders=orders
            )

            logger.info(f"GTT order placed successfully: trigger_id={gtt_id}")
            return gtt_id

        except Exception as e:
            logger.error(f"Failed to place GTT order: {e}")
            raise

    async def get_gtts(self) -> list:
        """
        Get all active GTT orders via KiteConnect REST API.

        Returns:
            List of GTT orders
        """
        try:
            kite = await self._get_kite_client()

            gtts = kite.get_gtts()

            logger.debug(f"Fetched {len(gtts)} GTT orders")
            return gtts

        except Exception as e:
            logger.error(f"Failed to fetch GTT orders: {e}")
            raise

    async def get_gtt(self, gtt_id: int) -> dict:
        """
        Get a specific GTT order via KiteConnect REST API.

        Args:
            gtt_id: GTT trigger ID

        Returns:
            GTT order details
        """
        try:
            kite = await self._get_kite_client()

            gtt = kite.get_gtt(gtt_id)

            logger.debug(f"Fetched GTT order: {gtt_id}")
            return gtt

        except Exception as e:
            logger.error(f"Failed to fetch GTT order {gtt_id}: {e}")
            raise

    async def modify_gtt(
        self,
        gtt_id: int,
        trigger_values: list,
        last_price: float,
        orders: list
    ) -> int:
        """
        Modify an existing GTT order via KiteConnect REST API.

        Args:
            gtt_id: GTT trigger ID to modify
            trigger_values: New trigger prices
            last_price: Current market price
            orders: Updated orders to place when triggered

        Returns:
            GTT trigger ID
        """
        try:
            kite = await self._get_kite_client()

            logger.info(f"Modifying GTT order: {gtt_id}")

            result = kite.modify_gtt(
                trigger_id=gtt_id,
                trigger_values=trigger_values,
                last_price=last_price,
                orders=orders
            )

            logger.info(f"GTT order modified successfully: {gtt_id}")
            return result

        except Exception as e:
            logger.error(f"Failed to modify GTT order {gtt_id}: {e}")
            raise

    async def delete_gtt(self, gtt_id: int) -> int:
        """
        Delete (cancel) a GTT order via KiteConnect REST API.

        Args:
            gtt_id: GTT trigger ID to delete

        Returns:
            GTT trigger ID
        """
        try:
            kite = await self._get_kite_client()

            logger.info(f"Deleting GTT order: {gtt_id}")

            result = kite.delete_gtt(gtt_id)

            logger.info(f"GTT order deleted successfully: {gtt_id}")
            return result

        except Exception as e:
            logger.error(f"Failed to delete GTT order {gtt_id}: {e}")
            raise


# Global client instances per user
_kite_clients: Dict[str, KiteOrderClient] = {}


def get_kite_client_sync() -> KiteOrderClient:
    """Get or create the default Kite order client"""
    if "default" not in _kite_clients:
        _kite_clients["default"] = KiteOrderClient()
    return _kite_clients["default"]


# Backward compatibility
def get_kite_client() -> KiteOrderClient:
    """Get or create the global Kite order client (backward compatibility)"""
    return get_kite_client_sync()


async def get_kite_client_for_user(user_id: str) -> Optional[KiteOrderClient]:
    """Get or create a Kite client for specific user"""
    try:
        # For now, use default client - in future this would lookup user-specific API keys
        # from a secure store and create user-specific clients
        if "default" not in _kite_clients:
            _kite_clients["default"] = KiteOrderClient()
            
        client = _kite_clients["default"]
        
        # Ensure client has fresh token
        await client.refresh_token()
        
        return client
        
    except Exception as e:
        logger.error(f"Failed to get kite client for user {user_id}: {e}")
        return None

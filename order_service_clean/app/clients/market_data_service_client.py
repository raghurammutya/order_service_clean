"""
Market Data Service Client

Handles all communication with the market data service for instrument registry,
price data, and market information. Replaces direct public.instrument_registry access.
"""

import logging
import httpx
from typing import Optional, Dict, Any, List
from ..config.settings import _get_service_port

logger = logging.getLogger(__name__)


class MarketDataServiceError(Exception):
    """Market data service communication error"""
    pass


class InstrumentNotFoundError(MarketDataServiceError):
    """Instrument not found error"""
    pass


class MarketDataServiceClient:
    """
    Client for communicating with market data service for instrument operations.
    
    This replaces direct access to:
    - public.instrument_registry
    - public.market_data (if accessed)
    """

    def __init__(self, base_url: Optional[str] = None, timeout: int = 30):
        """
        Initialize market data service client.
        
        Args:
            base_url: Market data service base URL (if None, uses service discovery)
            timeout: Request timeout in seconds
        """
        self.base_url = base_url
        self.timeout = timeout
        self._http_client: Optional[httpx.AsyncClient] = None

    async def _get_base_url(self) -> str:
        """Get market data service base URL via service discovery"""
        if self.base_url:
            return self.base_url

        try:
            # Try market_data_service first
            port = await _get_service_port("market_data_service")
            return f"http://market-data-service:{port}"
        except Exception:
            try:
                # Fallback to ticker_service (may handle instrument data)
                port = await _get_service_port("ticker_service")
                return f"http://ticker-service:{port}"
            except Exception as e:
                logger.warning(f"Service discovery failed for market data services: {e}")
                return "http://market-data-service:8005"  # Default fallback

    async def _get_http_client(self) -> httpx.AsyncClient:
        """Get or create HTTP client"""
        if self._http_client is None:
            self._http_client = httpx.AsyncClient(timeout=self.timeout)
        return self._http_client

    async def close(self):
        """Close HTTP client"""
        if self._http_client:
            await self._http_client.aclose()
            self._http_client = None

    async def get_instrument_token(
        self, 
        symbol: str, 
        exchange: str
    ) -> Optional[int]:
        """
        Get instrument token for symbol/exchange combination.
        
        Replaces: SELECT instrument_token FROM public.instrument_registry
        
        Args:
            symbol: Trading symbol (e.g., RELIANCE, NIFTY25D0226400CE)
            exchange: Exchange code (e.g., NSE, NFO)
            
        Returns:
            Instrument token if found, None if not found
        """
        try:
            base_url = await self._get_base_url()
            client = await self._get_http_client()

            response = await client.get(
                f"{base_url}/api/v1/instruments/token",
                params={
                    "symbol": symbol,
                    "exchange": exchange,
                    "active_only": True
                }
            )
            
            if response.status_code == 200:
                data = response.json()
                return data.get("instrument_token")
            elif response.status_code == 404:
                return None  # Instrument not found
            else:
                logger.error(f"Get instrument token failed: {response.status_code} {response.text}")
                raise MarketDataServiceError(f"Get instrument token failed: {response.status_code}")

        except httpx.RequestError as e:
            logger.error(f"Market data service request failed: {e}")
            raise MarketDataServiceError(f"Market data service request failed: {e}")

    async def get_instrument_info(
        self, 
        symbol: str, 
        exchange: str
    ) -> Optional[Dict[str, Any]]:
        """
        Get comprehensive instrument information.
        
        Args:
            symbol: Trading symbol
            exchange: Exchange code
            
        Returns:
            Instrument information dictionary if found, None if not found
        """
        try:
            base_url = await self._get_base_url()
            client = await self._get_http_client()

            response = await client.get(
                f"{base_url}/api/v1/instruments/{symbol}",
                params={"exchange": exchange}
            )
            
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 404:
                return None  # Instrument not found
            else:
                logger.error(f"Get instrument info failed: {response.status_code} {response.text}")
                raise MarketDataServiceError(f"Get instrument info failed: {response.status_code}")

        except httpx.RequestError as e:
            logger.error(f"Market data service request failed: {e}")
            raise MarketDataServiceError(f"Market data service request failed: {e}")

    async def search_instruments(
        self, 
        query: str, 
        exchange: Optional[str] = None,
        limit: int = 50
    ) -> List[Dict[str, Any]]:
        """
        Search instruments by symbol or name.
        
        Args:
            query: Search query
            exchange: Optional exchange filter
            limit: Maximum results to return
            
        Returns:
            List of matching instrument dictionaries
        """
        try:
            base_url = await self._get_base_url()
            client = await self._get_http_client()

            params = {
                "q": query,
                "limit": limit
            }
            if exchange:
                params["exchange"] = exchange

            response = await client.get(
                f"{base_url}/api/v1/instruments/search",
                params=params
            )
            
            if response.status_code == 200:
                return response.json().get("instruments", [])
            else:
                logger.error(f"Search instruments failed: {response.status_code} {response.text}")
                raise MarketDataServiceError(f"Search instruments failed: {response.status_code}")

        except httpx.RequestError as e:
            logger.error(f"Market data service request failed: {e}")
            raise MarketDataServiceError(f"Market data service request failed: {e}")

    async def validate_instrument(
        self, 
        symbol: str, 
        exchange: str
    ) -> bool:
        """
        Validate that an instrument exists and is active.
        
        Args:
            symbol: Trading symbol
            exchange: Exchange code
            
        Returns:
            True if instrument exists and is active
        """
        try:
            token = await self.get_instrument_token(symbol, exchange)
            return token is not None
        except MarketDataServiceError:
            return False

    async def get_current_price(
        self, 
        symbol: str, 
        exchange: str
    ) -> Optional[Dict[str, Any]]:
        """
        Get current market price for an instrument.
        
        Args:
            symbol: Trading symbol
            exchange: Exchange code
            
        Returns:
            Price data dictionary if available
        """
        try:
            base_url = await self._get_base_url()
            client = await self._get_http_client()

            response = await client.get(
                f"{base_url}/api/v1/instruments/{symbol}/price",
                params={"exchange": exchange}
            )
            
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 404:
                return None  # Price not available
            else:
                logger.error(f"Get current price failed: {response.status_code} {response.text}")
                raise MarketDataServiceError(f"Get current price failed: {response.status_code}")

        except httpx.RequestError as e:
            logger.error(f"Market data service request failed: {e}")
            raise MarketDataServiceError(f"Market data service request failed: {e}")


# Singleton instance
_market_data_client: Optional[MarketDataServiceClient] = None


async def get_market_data_client() -> MarketDataServiceClient:
    """Get or create market data service client singleton"""
    global _market_data_client
    if _market_data_client is None:
        _market_data_client = MarketDataServiceClient()
    return _market_data_client


async def cleanup_market_data_client():
    """Cleanup market data service client"""
    global _market_data_client
    if _market_data_client:
        await _market_data_client.close()
        _market_data_client = None
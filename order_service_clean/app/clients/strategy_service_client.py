"""
Strategy Service Client

Handles all communication with the backend/algo-engine service for strategy-related operations.
Replaces direct public.strategy and public.strategy_portfolio table access.
"""

import logging
import httpx
from typing import Optional, Dict, Any, List
from datetime import datetime, timezone
import asyncio

from ..config.settings import _get_service_port

logger = logging.getLogger(__name__)


class StrategyServiceError(Exception):
    """Strategy service communication error"""
    pass


class StrategyNotFoundError(StrategyServiceError):
    """Strategy not found error"""
    pass


class StrategyServiceClient:
    """
    Client for communicating with backend/algo-engine service for strategy operations.
    
    This replaces direct access to:
    - public.strategy
    - public.strategy_portfolio  
    - public.portfolio (for strategy-related operations)
    """

    def __init__(self, base_url: Optional[str] = None, timeout: int = 30):
        """
        Initialize strategy service client.
        
        Args:
            base_url: Strategy service base URL (if None, uses service discovery)
            timeout: Request timeout in seconds
        """
        self.base_url = base_url
        self.timeout = timeout
        self._http_client: Optional[httpx.AsyncClient] = None

    async def _get_base_url(self) -> str:
        """Get strategy service base URL via service discovery"""
        if self.base_url:
            return self.base_url

        try:
            port = await _get_service_port("backend")
            return f"http://backend:{port}"
        except Exception as e:
            logger.warning(f"Service discovery failed for backend service: {e}")
            # Fallback to default
            return "http://backend:8001"

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

    async def validate_strategy(self, strategy_id: str) -> bool:
        """
        Validate that a strategy exists and is active.
        
        Replaces: SELECT strategy_id FROM public.strategy WHERE strategy_id = :strategy_id
        
        Args:
            strategy_id: Strategy ID to validate
            
        Returns:
            True if strategy exists and is active
        """
        try:
            base_url = await self._get_base_url()
            client = await self._get_http_client()

            response = await client.get(f"{base_url}/api/v1/strategies/{strategy_id}/validate")
            
            if response.status_code == 200:
                data = response.json()
                return data.get("valid", False)
            elif response.status_code == 404:
                return False
            else:
                logger.error(f"Strategy validation failed: {response.status_code} {response.text}")
                raise StrategyServiceError(f"Strategy validation failed: {response.status_code}")

        except httpx.RequestError as e:
            logger.error(f"Strategy service request failed: {e}")
            raise StrategyServiceError(f"Strategy service request failed: {e}")

    async def get_strategy_info(self, strategy_id: str) -> Dict[str, Any]:
        """
        Get strategy information including name, status, and metadata.
        
        Args:
            strategy_id: Strategy ID
            
        Returns:
            Strategy information dictionary
            
        Raises:
            StrategyNotFoundError: If strategy doesn't exist
        """
        try:
            base_url = await self._get_base_url()
            client = await self._get_http_client()

            response = await client.get(f"{base_url}/api/v1/strategies/{strategy_id}")
            
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 404:
                raise StrategyNotFoundError(f"Strategy {strategy_id} not found")
            else:
                logger.error(f"Get strategy info failed: {response.status_code} {response.text}")
                raise StrategyServiceError(f"Get strategy info failed: {response.status_code}")

        except httpx.RequestError as e:
            logger.error(f"Strategy service request failed: {e}")
            raise StrategyServiceError(f"Strategy service request failed: {e}")

    async def sync_strategy_pnl(self, strategy_id: str, pnl_data: Dict[str, Any]) -> bool:
        """
        Sync P&L data for a strategy.
        
        Replaces: Complex UPDATE queries on public.strategy with P&L calculations
        
        Args:
            strategy_id: Strategy ID
            pnl_data: P&L data containing total_pnl, unrealized_pnl, etc.
            
        Returns:
            True if sync successful
        """
        try:
            base_url = await self._get_base_url()
            client = await self._get_http_client()

            response = await client.post(
                f"{base_url}/api/v1/strategies/{strategy_id}/sync-pnl",
                json=pnl_data
            )
            
            if response.status_code == 200:
                return True
            elif response.status_code == 404:
                raise StrategyNotFoundError(f"Strategy {strategy_id} not found")
            else:
                logger.error(f"P&L sync failed: {response.status_code} {response.text}")
                raise StrategyServiceError(f"P&L sync failed: {response.status_code}")

        except httpx.RequestError as e:
            logger.error(f"Strategy service request failed: {e}")
            raise StrategyServiceError(f"Strategy service request failed: {e}")

    async def get_or_create_default_strategy(
        self, 
        trading_account_id: str, 
        strategy_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Get existing default strategy or create a new one for external order tracking.
        
        Replaces: Complex logic in default_strategy_service.py
        
        Args:
            trading_account_id: Trading account ID  
            strategy_name: Optional custom strategy name
            
        Returns:
            Strategy information dictionary
        """
        try:
            base_url = await self._get_base_url()
            client = await self._get_http_client()

            # First try to get existing default strategy
            response = await client.get(f"{base_url}/api/v1/strategies/default/{trading_account_id}")
            
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 404:
                # Create new default strategy
                create_data = {
                    "trading_account_id": trading_account_id,
                    "strategy_name": strategy_name or f"External-{trading_account_id}",
                    "type": "external_tracking"
                }
                
                response = await client.post(
                    f"{base_url}/api/v1/strategies/default",
                    json=create_data
                )
                
                if response.status_code == 201:
                    return response.json()
                else:
                    logger.error(f"Default strategy creation failed: {response.status_code} {response.text}")
                    raise StrategyServiceError(f"Default strategy creation failed: {response.status_code}")
            else:
                logger.error(f"Get default strategy failed: {response.status_code} {response.text}")
                raise StrategyServiceError(f"Get default strategy failed: {response.status_code}")

        except httpx.RequestError as e:
            logger.error(f"Strategy service request failed: {e}")
            raise StrategyServiceError(f"Strategy service request failed: {e}")

    async def get_strategy_portfolio(self, strategy_id: str) -> Optional[Dict[str, Any]]:
        """
        Get portfolio information linked to a strategy.
        
        Replaces: SELECT from public.strategy_portfolio + JOIN with public.portfolio
        
        Args:
            strategy_id: Strategy ID
            
        Returns:
            Portfolio information or None if no portfolio linked
        """
        try:
            base_url = await self._get_base_url()
            client = await self._get_http_client()

            response = await client.get(f"{base_url}/api/v1/strategies/{strategy_id}/portfolio")
            
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 404:
                return None
            else:
                logger.error(f"Get strategy portfolio failed: {response.status_code} {response.text}")
                raise StrategyServiceError(f"Get strategy portfolio failed: {response.status_code}")

        except httpx.RequestError as e:
            logger.error(f"Strategy service request failed: {e}")
            raise StrategyServiceError(f"Strategy service request failed: {e}")

    async def bulk_sync_strategy_pnl(self, pnl_updates: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Bulk sync P&L data for multiple strategies.
        
        Replaces: Large batch UPDATE queries in strategy_pnl_sync.py
        
        Args:
            pnl_updates: List of P&L update dictionaries
            
        Returns:
            Sync result summary
        """
        try:
            base_url = await self._get_base_url()
            client = await self._get_http_client()

            response = await client.post(
                f"{base_url}/api/v1/strategies/bulk-sync-pnl",
                json={"updates": pnl_updates}
            )
            
            if response.status_code == 200:
                return response.json()
            else:
                logger.error(f"Bulk P&L sync failed: {response.status_code} {response.text}")
                raise StrategyServiceError(f"Bulk P&L sync failed: {response.status_code}")

        except httpx.RequestError as e:
            logger.error(f"Strategy service request failed: {e}")
            raise StrategyServiceError(f"Strategy service request failed: {e}")

    async def create_default_strategy(
        self, 
        trading_account_id: str, 
        user_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Create a default strategy for a trading account.
        
        Replaces: INSERT INTO public.strategy (...) VALUES (...)
        
        Args:
            trading_account_id: Trading account ID
            user_id: Optional user ID
            
        Returns:
            Created strategy information including strategy_id
            
        Raises:
            StrategyServiceError: If creation fails
        """
        try:
            base_url = await self._get_base_url()
            client = await self._get_http_client()

            strategy_data = {
                "name": "Default Strategy",
                "description": "Auto-created default strategy for tracking external orders and positions. This strategy does not execute trades - it only tracks external activity.",
                "strategy_type": "passive",
                "trading_account_id": trading_account_id,
                "user_id": user_id,
                "is_default": True,
                "is_active": True,
                "state": "active",
                "mode": "live",
                "config": {
                    "auto_execute": False
                },
                "metadata": {
                    "source": "auto_created",
                    "created_reason": "default_strategy_auto_tagging"
                },
                "created_by": "system"
            }

            response = await client.post(
                f"{base_url}/api/v1/strategies",
                json=strategy_data
            )
            
            if response.status_code in [200, 201]:
                return response.json()
            else:
                logger.error(f"Create default strategy failed: {response.status_code} {response.text}")
                raise StrategyServiceError(f"Create default strategy failed: {response.status_code}")

        except httpx.RequestError as e:
            logger.error(f"Strategy service request failed: {e}")
            raise StrategyServiceError(f"Strategy service request failed: {e}")

    async def get_or_create_default_strategy(self, trading_account_id: str) -> Dict[str, Any]:
        """
        Get existing default strategy for account, or create one if it doesn't exist.
        
        Replaces: Complex SELECT with fallback to INSERT logic
        
        Args:
            trading_account_id: Trading account ID
            
        Returns:
            Default strategy information
            
        Raises:
            StrategyServiceError: If operation fails
        """
        try:
            base_url = await self._get_base_url()
            client = await self._get_http_client()

            # Try to get existing default strategy
            response = await client.get(
                f"{base_url}/api/v1/accounts/{trading_account_id}/default-strategy"
            )
            
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 404:
                # Create default strategy if none exists
                return await self.create_default_strategy(trading_account_id)
            else:
                logger.error(f"Get default strategy failed: {response.status_code} {response.text}")
                raise StrategyServiceError(f"Get default strategy failed: {response.status_code}")

        except httpx.RequestError as e:
            logger.error(f"Strategy service request failed: {e}")
            raise StrategyServiceError(f"Strategy service request failed: {e}")


# Singleton instance
_strategy_client: Optional[StrategyServiceClient] = None


async def get_strategy_client() -> StrategyServiceClient:
    """Get or create strategy service client singleton"""
    global _strategy_client
    if _strategy_client is None:
        _strategy_client = StrategyServiceClient()
    return _strategy_client


async def cleanup_strategy_client():
    """Cleanup strategy service client"""
    global _strategy_client
    if _strategy_client:
        await _strategy_client.close()
        _strategy_client = None
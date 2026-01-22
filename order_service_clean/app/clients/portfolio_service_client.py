"""
Portfolio Service Client

Handles all communication with the backend/algo-engine service for portfolio-related operations.
Replaces direct public.portfolio table access.
"""

import logging
import httpx
from typing import Optional, Dict, Any
from ..config.settings import _get_service_port

logger = logging.getLogger(__name__)


class PortfolioServiceError(Exception):
    """Portfolio service communication error"""
    pass


class PortfolioNotFoundError(PortfolioServiceError):
    """Portfolio not found error"""
    pass


class PortfolioServiceClient:
    """
    Client for communicating with backend/algo-engine service for portfolio operations.
    
    This replaces direct access to:
    - public.portfolio
    - public.strategy_portfolio
    """

    def __init__(self, base_url: Optional[str] = None, timeout: int = 30):
        """
        Initialize portfolio service client.
        
        Args:
            base_url: Portfolio service base URL (if None, uses service discovery)
            timeout: Request timeout in seconds
        """
        self.base_url = base_url
        self.timeout = timeout
        self._http_client: Optional[httpx.AsyncClient] = None

    async def _get_base_url(self) -> str:
        """Get portfolio service base URL via service discovery"""
        if self.base_url:
            return self.base_url

        try:
            port = await _get_service_port("backend")
            return f"http://backend:{port}"
        except Exception as e:
            logger.warning(f"Service discovery failed for backend service: {e}")
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

    async def get_or_create_default_portfolio(
        self, 
        trading_account_id: str,
        portfolio_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Get existing default portfolio or create a new one.
        
        Replaces: Complex portfolio creation logic in default_portfolio_service.py
        
        Args:
            trading_account_id: Trading account ID
            portfolio_name: Optional custom portfolio name
            
        Returns:
            Portfolio information dictionary
        """
        try:
            base_url = await self._get_base_url()
            client = await self._get_http_client()

            # First try to get existing default portfolio
            response = await client.get(f"{base_url}/api/v1/portfolios/default/{trading_account_id}")
            
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 404:
                # Create new default portfolio
                create_data = {
                    "trading_account_id": trading_account_id,
                    "portfolio_name": portfolio_name or f"Default-{trading_account_id}",
                    "type": "default"
                }
                
                response = await client.post(
                    f"{base_url}/api/v1/portfolios/default",
                    json=create_data
                )
                
                if response.status_code == 201:
                    return response.json()
                else:
                    logger.error(f"Default portfolio creation failed: {response.status_code} {response.text}")
                    raise PortfolioServiceError(f"Default portfolio creation failed: {response.status_code}")
            else:
                logger.error(f"Get default portfolio failed: {response.status_code} {response.text}")
                raise PortfolioServiceError(f"Get default portfolio failed: {response.status_code}")

        except httpx.RequestError as e:
            logger.error(f"Portfolio service request failed: {e}")
            raise PortfolioServiceError(f"Portfolio service request failed: {e}")

    async def link_portfolio_to_strategy(
        self, 
        portfolio_id: str, 
        strategy_id: str,
        allocation_percentage: float = 100.0
    ) -> bool:
        """
        Link a portfolio to a strategy with allocation percentage.
        
        Replaces: INSERT INTO public.strategy_portfolio with ON CONFLICT
        
        Args:
            portfolio_id: Portfolio ID
            strategy_id: Strategy ID  
            allocation_percentage: Allocation percentage (0-100)
            
        Returns:
            True if link successful
        """
        try:
            base_url = await self._get_base_url()
            client = await self._get_http_client()

            link_data = {
                "portfolio_id": portfolio_id,
                "strategy_id": strategy_id,
                "allocation_percentage": allocation_percentage
            }

            response = await client.post(
                f"{base_url}/api/v1/portfolios/{portfolio_id}/link-strategy",
                json=link_data
            )
            
            if response.status_code == 200:
                return True
            elif response.status_code == 404:
                raise PortfolioNotFoundError(f"Portfolio {portfolio_id} or Strategy {strategy_id} not found")
            else:
                logger.error(f"Portfolio-strategy link failed: {response.status_code} {response.text}")
                raise PortfolioServiceError(f"Portfolio-strategy link failed: {response.status_code}")

        except httpx.RequestError as e:
            logger.error(f"Portfolio service request failed: {e}")
            raise PortfolioServiceError(f"Portfolio service request failed: {e}")

    async def get_portfolio_strategies(self, portfolio_id: str) -> list:
        """
        Get all strategies linked to a portfolio.
        
        Args:
            portfolio_id: Portfolio ID
            
        Returns:
            List of strategy information dictionaries
        """
        try:
            base_url = await self._get_base_url()
            client = await self._get_http_client()

            response = await client.get(f"{base_url}/api/v1/portfolios/{portfolio_id}/strategies")
            
            if response.status_code == 200:
                return response.json().get("strategies", [])
            elif response.status_code == 404:
                raise PortfolioNotFoundError(f"Portfolio {portfolio_id} not found")
            else:
                logger.error(f"Get portfolio strategies failed: {response.status_code} {response.text}")
                raise PortfolioServiceError(f"Get portfolio strategies failed: {response.status_code}")

        except httpx.RequestError as e:
            logger.error(f"Portfolio service request failed: {e}")
            raise PortfolioServiceError(f"Portfolio service request failed: {e}")

    async def create_default_portfolio(
        self, 
        trading_account_id: str, 
        user_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Create a default portfolio for a trading account.
        
        Replaces: INSERT INTO public.portfolio (...) VALUES (...)
        
        Args:
            trading_account_id: Trading account ID
            user_id: Optional user ID
            
        Returns:
            Created portfolio information including portfolio_id
            
        Raises:
            PortfolioServiceError: If creation fails
        """
        try:
            base_url = await self._get_base_url()
            client = await self._get_http_client()

            portfolio_data = {
                "portfolio_name": f"Default Portfolio - {trading_account_id}",
                "description": "Auto-created default portfolio for organizing positions and strategies",
                "portfolio_type": "default",
                "trading_account_id": trading_account_id,
                "user_id": user_id,
                "is_default": True,
                "is_active": True,
                "status": "active",
                "allocation_method": "equal_weight",
                "rebalancing_frequency": "manual",
                "config": {
                    "auto_rebalance": False,
                    "max_allocation_per_strategy": 100
                },
                "metadata": {
                    "source": "auto_created",
                    "created_reason": "default_portfolio_creation"
                },
                "created_by": "system"
            }

            response = await client.post(
                f"{base_url}/api/v1/portfolios",
                json=portfolio_data
            )
            
            if response.status_code in [200, 201]:
                return response.json()
            else:
                logger.error(f"Create default portfolio failed: {response.status_code} {response.text}")
                raise PortfolioServiceError(f"Create default portfolio failed: {response.status_code}")

        except httpx.RequestError as e:
            logger.error(f"Portfolio service request failed: {e}")
            raise PortfolioServiceError(f"Portfolio service request failed: {e}")


# Singleton instance
_portfolio_client: Optional[PortfolioServiceClient] = None


async def get_portfolio_client() -> PortfolioServiceClient:
    """Get or create portfolio service client singleton"""
    global _portfolio_client
    if _portfolio_client is None:
        _portfolio_client = PortfolioServiceClient()
    return _portfolio_client


async def cleanup_portfolio_client():
    """Cleanup portfolio service client"""
    global _portfolio_client
    if _portfolio_client:
        await _portfolio_client.close()
        _portfolio_client = None
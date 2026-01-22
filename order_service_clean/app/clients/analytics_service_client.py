"""
Analytics Service Client

Handles all communication with the analytics service for P&L metrics operations.
Replaces direct public.strategy_pnl_metrics table access.
"""

import logging
import httpx
from typing import Optional, Dict, Any, List
from datetime import date, datetime
from decimal import Decimal

from ..config.settings import _get_service_port

logger = logging.getLogger(__name__)


class AnalyticsServiceError(Exception):
    """Analytics service communication error"""
    pass


class PnLMetricsNotFoundError(AnalyticsServiceError):
    """P&L metrics not found error"""
    pass


class AnalyticsServiceClient:
    """
    Client for communicating with analytics service for P&L metrics operations.
    
    This replaces direct access to:
    - public.strategy_pnl_metrics
    """

    def __init__(self, base_url: Optional[str] = None, timeout: int = 30):
        """
        Initialize analytics service client.
        
        Args:
            base_url: Analytics service base URL (if None, uses service discovery)
            timeout: Request timeout in seconds
        """
        self.base_url = base_url
        self.timeout = timeout
        self._http_client: Optional[httpx.AsyncClient] = None

    async def _get_base_url(self) -> str:
        """Get analytics service base URL via service discovery"""
        if self.base_url:
            return self.base_url

        try:
            port = await _get_service_port("analytics")
            return f"http://analytics:{port}"
        except Exception:
            try:
                # Fallback to backend service (may handle analytics)
                port = await _get_service_port("backend")
                return f"http://backend:{port}"
            except Exception as e:
                logger.warning(f"Service discovery failed for analytics services: {e}")
                return "http://analytics:8004"  # Default fallback

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

    async def calculate_and_store_pnl_metrics(
        self,
        strategy_id: str,
        metric_date: date,
        pnl_data: Dict[str, Any]
    ) -> bool:
        """
        Calculate and store comprehensive P&L metrics for a strategy.
        
        Replaces: Complex INSERT/UPDATE on public.strategy_pnl_metrics with 20+ columns
        
        Args:
            strategy_id: Strategy ID
            metric_date: Date for the metrics
            pnl_data: P&L calculation data (positions, trades, etc.)
            
        Returns:
            True if calculation and storage successful
        """
        try:
            base_url = await self._get_base_url()
            client = await self._get_http_client()

            request_data = {
                "strategy_id": strategy_id,
                "metric_date": metric_date.isoformat(),
                "pnl_data": pnl_data
            }

            response = await client.post(
                f"{base_url}/api/v1/analytics/pnl/calculate",
                json=request_data
            )
            
            if response.status_code == 200:
                return True
            elif response.status_code == 404:
                logger.warning(f"Strategy {strategy_id} not found for P&L calculation")
                return False
            else:
                logger.error(f"P&L calculation failed: {response.status_code} {response.text}")
                raise AnalyticsServiceError(f"P&L calculation failed: {response.status_code}")

        except httpx.RequestError as e:
            logger.error(f"Analytics service request failed: {e}")
            raise AnalyticsServiceError(f"Analytics service request failed: {e}")

    async def get_strategy_pnl_metrics(
        self,
        strategy_id: str,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None
    ) -> List[Dict[str, Any]]:
        """
        Get P&L metrics for a strategy within date range.
        
        Args:
            strategy_id: Strategy ID
            start_date: Start date for metrics (optional)
            end_date: End date for metrics (optional)
            
        Returns:
            List of P&L metrics dictionaries
        """
        try:
            base_url = await self._get_base_url()
            client = await self._get_http_client()

            params = {"strategy_id": strategy_id}
            if start_date:
                params["start_date"] = start_date.isoformat()
            if end_date:
                params["end_date"] = end_date.isoformat()

            response = await client.get(
                f"{base_url}/api/v1/analytics/pnl/metrics",
                params=params
            )
            
            if response.status_code == 200:
                return response.json().get("metrics", [])
            elif response.status_code == 404:
                return []
            else:
                logger.error(f"Get P&L metrics failed: {response.status_code} {response.text}")
                raise AnalyticsServiceError(f"Get P&L metrics failed: {response.status_code}")

        except httpx.RequestError as e:
            logger.error(f"Analytics service request failed: {e}")
            raise AnalyticsServiceError(f"Analytics service request failed: {e}")

    async def get_max_drawdown(
        self,
        strategy_id: str,
        start_date: date,
        end_date: date
    ) -> Optional[Dict[str, Any]]:
        """
        Get maximum drawdown for a strategy within date range.
        
        Replaces: Complex SELECT with MIN/MAX operations on public.strategy_pnl_metrics
        
        Args:
            strategy_id: Strategy ID
            start_date: Start date for analysis
            end_date: End date for analysis
            
        Returns:
            Drawdown information dictionary or None
        """
        try:
            base_url = await self._get_base_url()
            client = await self._get_http_client()

            params = {
                "strategy_id": strategy_id,
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat()
            }

            response = await client.get(
                f"{base_url}/api/v1/analytics/pnl/drawdown",
                params=params
            )
            
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 404:
                return None
            else:
                logger.error(f"Get drawdown failed: {response.status_code} {response.text}")
                raise AnalyticsServiceError(f"Get drawdown failed: {response.status_code}")

        except httpx.RequestError as e:
            logger.error(f"Analytics service request failed: {e}")
            raise AnalyticsServiceError(f"Analytics service request failed: {e}")

    async def get_previous_cumulative_pnl(
        self,
        strategy_id: str,
        before_date: date
    ) -> Optional[Decimal]:
        """
        Get previous cumulative P&L for a strategy before a specific date.
        
        Replaces: SELECT with ORDER BY and LIMIT on public.strategy_pnl_metrics
        
        Args:
            strategy_id: Strategy ID
            before_date: Date to get P&L before
            
        Returns:
            Previous cumulative P&L or None
        """
        try:
            base_url = await self._get_base_url()
            client = await self._get_http_client()

            params = {
                "strategy_id": strategy_id,
                "before_date": before_date.isoformat()
            }

            response = await client.get(
                f"{base_url}/api/v1/analytics/pnl/previous-cumulative",
                params=params
            )
            
            if response.status_code == 200:
                data = response.json()
                pnl_value = data.get("cumulative_pnl")
                return Decimal(str(pnl_value)) if pnl_value is not None else None
            elif response.status_code == 404:
                return None
            else:
                logger.error(f"Get previous cumulative P&L failed: {response.status_code} {response.text}")
                raise AnalyticsServiceError(f"Get previous cumulative P&L failed: {response.status_code}")

        except httpx.RequestError as e:
            logger.error(f"Analytics service request failed: {e}")
            raise AnalyticsServiceError(f"Analytics service request failed: {e}")

    async def check_existing_metrics(
        self,
        strategy_id: str,
        metric_dates: List[date]
    ) -> List[date]:
        """
        Check which dates already have P&L metrics for a strategy.
        
        Used for backfill validation to avoid duplicates.
        
        Args:
            strategy_id: Strategy ID
            metric_dates: List of dates to check
            
        Returns:
            List of dates that already have metrics
        """
        try:
            base_url = await self._get_base_url()
            client = await self._get_http_client()

            request_data = {
                "strategy_id": strategy_id,
                "metric_dates": [d.isoformat() for d in metric_dates]
            }

            response = await client.post(
                f"{base_url}/api/v1/analytics/pnl/check-existing",
                json=request_data
            )
            
            if response.status_code == 200:
                existing_dates = response.json().get("existing_dates", [])
                return [date.fromisoformat(d) for d in existing_dates]
            else:
                logger.error(f"Check existing metrics failed: {response.status_code} {response.text}")
                raise AnalyticsServiceError(f"Check existing metrics failed: {response.status_code}")

        except httpx.RequestError as e:
            logger.error(f"Analytics service request failed: {e}")
            raise AnalyticsServiceError(f"Analytics service request failed: {e}")

    async def bulk_calculate_pnl_metrics(
        self,
        calculations: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Bulk calculate P&L metrics for multiple strategies/dates.
        
        Args:
            calculations: List of calculation requests
            
        Returns:
            Calculation result summary
        """
        try:
            base_url = await self._get_base_url()
            client = await self._get_http_client()

            response = await client.post(
                f"{base_url}/api/v1/analytics/pnl/bulk-calculate",
                json={"calculations": calculations}
            )
            
            if response.status_code == 200:
                return response.json()
            else:
                logger.error(f"Bulk P&L calculation failed: {response.status_code} {response.text}")
                raise AnalyticsServiceError(f"Bulk P&L calculation failed: {response.status_code}")

        except httpx.RequestError as e:
            logger.error(f"Analytics service request failed: {e}")
            raise AnalyticsServiceError(f"Analytics service request failed: {e}")

    async def get_strategy_performance_summary(
        self,
        strategy_id: str,
        period_days: int = 30
    ) -> Dict[str, Any]:
        """
        Get comprehensive performance summary for a strategy.
        
        Args:
            strategy_id: Strategy ID
            period_days: Period in days for summary
            
        Returns:
            Performance summary dictionary
        """
        try:
            base_url = await self._get_base_url()
            client = await self._get_http_client()

            params = {
                "strategy_id": strategy_id,
                "period_days": period_days
            }

            response = await client.get(
                f"{base_url}/api/v1/analytics/performance/summary",
                params=params
            )
            
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 404:
                return {}
            else:
                logger.error(f"Get performance summary failed: {response.status_code} {response.text}")
                raise AnalyticsServiceError(f"Get performance summary failed: {response.status_code}")

        except httpx.RequestError as e:
            logger.error(f"Analytics service request failed: {e}")
            raise AnalyticsServiceError(f"Analytics service request failed: {e}")


# Singleton instance
_analytics_client: Optional[AnalyticsServiceClient] = None


async def get_analytics_client() -> AnalyticsServiceClient:
    """Get or create analytics service client singleton"""
    global _analytics_client
    if _analytics_client is None:
        _analytics_client = AnalyticsServiceClient()
    return _analytics_client


async def cleanup_analytics_client():
    """Cleanup analytics service client"""
    global _analytics_client
    if _analytics_client:
        await _analytics_client.close()
        _analytics_client = None
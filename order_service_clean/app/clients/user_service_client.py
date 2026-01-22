"""
User Service Client - internal API wrapper for trading account lookups.

This replaces direct cross-service DB access to user_service tables.
"""

import logging
from typing import Any, Dict, List, Optional

import httpx

# Handle missing common module in test environment
try:
    from common.service_registry import get_user_service_url
except ImportError:
    async def get_user_service_url():
        # Use config-service compliant settings with proper service discovery
        try:
            from ..config.settings import _get_service_port
            
            # Try service discovery first
            try:
                port = await _get_service_port("user_service")
                return f"http://user-service:{port}"
            except Exception:
                # Fallback to settings
                from ..config.settings import settings
                return settings.user_service_url if hasattr(settings, 'user_service_url') else "http://user-service:8002"
        except Exception:
            return "http://user-service:8002"  # Default fallback

logger = logging.getLogger(__name__)


class UserServiceClientError(Exception):
    """User service client error."""


class UserServiceClient:
    """HTTP client for user_service internal trading account APIs."""

    def __init__(self, base_url: Optional[str] = None, api_key: Optional[str] = None, timeout: float = 5.0):
        self.base_url = base_url
        self.api_key = api_key or _load_internal_api_key()
        self.timeout = timeout
        self._client: Optional[httpx.AsyncClient] = None

    async def _get_base_url(self) -> str:
        """Get user service base URL via service discovery"""
        if self.base_url:
            return self.base_url.rstrip("/")
        
        url = await get_user_service_url()
        return url.rstrip("/")

    async def __aenter__(self):
        self._client = await self._build_client()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def _build_client(self) -> httpx.AsyncClient:
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["X-Internal-API-Key"] = self.api_key
        base_url = await self._get_base_url()
        return httpx.AsyncClient(base_url=base_url, timeout=self.timeout, headers=headers)

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = await self._build_client()
        return self._client

    async def close(self) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None

    async def get_trading_account_basic_info(self, trading_account_id: int) -> Dict[str, Any]:
        client = await self._get_client()
        try:
            response = await client.get(
                f"/api/v1/trading-accounts-integration/{trading_account_id}/basic"
            )
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as exc:
            logger.error("user_service basic info failed: %s", exc.response.text)
            raise UserServiceClientError("Failed to fetch trading account info") from exc
        except (httpx.RequestError, httpx.TimeoutException, httpx.ConnectError) as exc:
            logger.error("user_service connection error: %s", exc)
            raise UserServiceClientError("User service connection failed") from exc
        except Exception as exc:
            logger.error("user_service unexpected error: %s", exc)
            raise UserServiceClientError("Unexpected error fetching trading account info") from exc

    async def bulk_query_trading_accounts(
        self,
        trading_account_ids: List[int],
        user_id: Optional[int] = None
    ) -> Dict[str, Any]:
        client = await self._get_client()
        payload: Dict[str, Any] = {"trading_account_ids": trading_account_ids}
        if user_id is not None:
            payload["user_id"] = user_id
        try:
            response = await client.post(
                "/api/v1/trading-accounts-integration/bulk-query",
                json=payload
            )
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as exc:
            logger.error("user_service bulk query failed: %s", exc.response.text)
            raise UserServiceClientError("Failed to bulk query trading accounts") from exc
        except Exception as exc:
            logger.error("user_service bulk query error: %s", exc)
            raise UserServiceClientError("Failed to bulk query trading accounts") from exc

    async def get_user_trading_accounts(self, user_id: int, include_shared: bool = True) -> List[Dict[str, Any]]:
        client = await self._get_client()
        try:
            response = await client.get(
                f"/api/v1/trading-accounts-integration/user/{user_id}/accounts",
                params={"include_shared": str(include_shared).lower()}
            )
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as exc:
            logger.error("user_service user accounts failed: %s", exc.response.text)
            raise UserServiceClientError("Failed to fetch user trading accounts") from exc
        except Exception as exc:
            logger.error("user_service user accounts error: %s", exc)
            raise UserServiceClientError("Failed to fetch user trading accounts") from exc

    async def list_active_trading_accounts(
        self,
        status_filter: str = "ACTIVE",
        broker: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        client = await self._get_client()
        params: Dict[str, Any] = {"status_filter": status_filter}
        if broker:
            params["broker"] = broker
        try:
            response = await client.get(
                "/api/v1/trading-accounts-integration/active",
                params=params
            )
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as exc:
            logger.error("user_service list active failed: %s", exc.response.text)
            raise UserServiceClientError("Failed to list active trading accounts") from exc
        except Exception as exc:
            logger.error("user_service list active error: %s", exc)
            raise UserServiceClientError("Failed to list active trading accounts") from exc

    async def get_by_broker_user_id(self, broker_user_id: str) -> Dict[str, Any]:
        client = await self._get_client()
        try:
            response = await client.get(
                f"/api/v1/trading-accounts-integration/by-broker-user-id/{broker_user_id}"
            )
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as exc:
            logger.error("user_service broker_user_id failed: %s", exc.response.text)
            raise UserServiceClientError("Failed to resolve broker user id") from exc
        except Exception as exc:
            logger.error("user_service broker_user_id error: %s", exc)
            raise UserServiceClientError("Failed to resolve broker user id") from exc


def _load_internal_api_key() -> str:
    try:
        # Use order service's config-compliant settings
        from ..config.settings import settings
        return settings.internal_api_key
    except Exception as exc:
        logger.error("Failed to load INTERNAL_API_KEY: %s", exc)
        return ""

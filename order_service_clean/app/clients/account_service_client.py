"""
Account Service Client

Handles all communication with the account service for trading account and holdings data.
Replaces direct public.kite_accounts table access.
"""

import logging
import httpx
from typing import Optional, Dict, Any, List
from datetime import datetime
from ..config.settings import _get_service_port

logger = logging.getLogger(__name__)


class AccountServiceError(Exception):
    """Account service communication error"""
    pass


class AccountNotFoundError(AccountServiceError):
    """Account not found error"""
    pass


class AccountServiceClient:
    """
    Client for communicating with account service for trading account operations.
    
    This replaces direct access to:
    - public.kite_accounts
    - public.holdings (if accessed)
    """

    def __init__(self, base_url: Optional[str] = None, timeout: int = 30):
        """
        Initialize account service client.
        
        Args:
            base_url: Account service base URL (if None, uses service discovery)
            timeout: Request timeout in seconds
        """
        self.base_url = base_url
        self.timeout = timeout
        self._http_client: Optional[httpx.AsyncClient] = None

    async def _get_base_url(self) -> str:
        """Get account service base URL via service discovery"""
        if self.base_url:
            return self.base_url

        try:
            # Try user_service first (may handle account management)
            port = await _get_service_port("user_service")
            return f"http://user-service:{port}"
        except Exception:
            try:
                # Fallback to token_manager (may handle account data)
                port = await _get_service_port("token_manager")
                return f"http://token-manager:{port}"
            except Exception as e:
                logger.warning(f"Service discovery failed for account services: {e}")
                return "http://user-service:8002"  # Default fallback

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

    async def update_account_tier(
        self, 
        trading_account_id: str, 
        sync_tier: str,
        temporary_until: Optional[datetime] = None
    ) -> bool:
        """
        Update sync tier for trading account.
        
        Replaces: UPDATE public.kite_accounts SET sync_tier = ? WHERE id = ?
        
        Args:
            trading_account_id: Trading account ID
            sync_tier: New sync tier ('HOT', 'WARM', 'COLD')
            temporary_until: Optional expiration time for temporary tier changes
            
        Returns:
            True if update successful
        """
        try:
            base_url = await self._get_base_url()
            client = await self._get_http_client()

            update_data = {
                "sync_tier": sync_tier
            }
            if temporary_until:
                update_data["temporary_until"] = temporary_until.isoformat()

            response = await client.put(
                f"{base_url}/api/v1/accounts/{trading_account_id}/tier",
                json=update_data
            )
            
            if response.status_code == 200:
                return True
            elif response.status_code == 404:
                raise AccountNotFoundError(f"Trading account {trading_account_id} not found")
            else:
                logger.error(f"Account tier update failed: {response.status_code} {response.text}")
                raise AccountServiceError(f"Account tier update failed: {response.status_code}")

        except httpx.RequestError as e:
            logger.error(f"Account service request failed: {e}")
            raise AccountServiceError(f"Account service request failed: {e}")

    async def get_account_tier_stats(self) -> Dict[str, Any]:
        """
        Get account tier distribution and statistics.
        
        Replaces: Complex JOIN queries on public.kite_accounts for tier analysis
        
        Returns:
            Dictionary with tier statistics
        """
        try:
            base_url = await self._get_base_url()
            client = await self._get_http_client()

            response = await client.get(f"{base_url}/api/v1/accounts/tier-summary")
            
            if response.status_code == 200:
                return response.json()
            else:
                logger.error(f"Get tier stats failed: {response.status_code} {response.text}")
                raise AccountServiceError(f"Get tier stats failed: {response.status_code}")

        except httpx.RequestError as e:
            logger.error(f"Account service request failed: {e}")
            raise AccountServiceError(f"Account service request failed: {e}")

    async def get_accounts_by_tier(self, sync_tier: str) -> List[Dict[str, Any]]:
        """
        Get accounts filtered by sync tier.
        
        Args:
            sync_tier: Sync tier to filter by ('HOT', 'WARM', 'COLD', 'ALL')
            
        Returns:
            List of account information dictionaries
        """
        try:
            base_url = await self._get_base_url()
            client = await self._get_http_client()

            if sync_tier.upper() == "ALL":
                # Get all accounts regardless of tier
                response = await client.get(f"{base_url}/api/v1/accounts")
            else:
                response = await client.get(
                    f"{base_url}/api/v1/accounts/by-tier/{sync_tier}"
                )
            
            if response.status_code == 200:
                return response.json().get("accounts", [])
            else:
                logger.error(f"Get accounts by tier failed: {response.status_code} {response.text}")
                raise AccountServiceError(f"Get accounts by tier failed: {response.status_code}")

        except httpx.RequestError as e:
            logger.error(f"Account service request failed: {e}")
            raise AccountServiceError(f"Account service request failed: {e}")

    async def get_account_info(self, trading_account_id: str) -> Dict[str, Any]:
        """
        Get comprehensive account information.
        
        Args:
            trading_account_id: Trading account ID
            
        Returns:
            Account information dictionary
            
        Raises:
            AccountNotFoundError: If account doesn't exist
        """
        try:
            base_url = await self._get_base_url()
            client = await self._get_http_client()

            response = await client.get(f"{base_url}/api/v1/accounts/{trading_account_id}")
            
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 404:
                raise AccountNotFoundError(f"Trading account {trading_account_id} not found")
            else:
                logger.error(f"Get account info failed: {response.status_code} {response.text}")
                raise AccountServiceError(f"Get account info failed: {response.status_code}")

        except httpx.RequestError as e:
            logger.error(f"Account service request failed: {e}")
            raise AccountServiceError(f"Account service request failed: {e}")

    async def promote_account_to_hot_tier(
        self, 
        trading_account_id: str,
        duration_minutes: int = 60
    ) -> bool:
        """
        Temporarily promote account to HOT tier for high-frequency trading.
        
        Replaces: Complex UPDATE with expiration logic on public.kite_accounts
        
        Args:
            trading_account_id: Trading account ID
            duration_minutes: How long to keep in HOT tier
            
        Returns:
            True if promotion successful
        """
        try:
            base_url = await self._get_base_url()
            client = await self._get_http_client()

            promotion_data = {
                "tier": "HOT",
                "duration_minutes": duration_minutes,
                "reason": "high_frequency_trading"
            }

            response = await client.post(
                f"{base_url}/api/v1/accounts/{trading_account_id}/promote-tier",
                json=promotion_data
            )
            
            if response.status_code == 200:
                return True
            elif response.status_code == 404:
                raise AccountNotFoundError(f"Trading account {trading_account_id} not found")
            else:
                logger.error(f"Account tier promotion failed: {response.status_code} {response.text}")
                raise AccountServiceError(f"Account tier promotion failed: {response.status_code}")

        except httpx.RequestError as e:
            logger.error(f"Account service request failed: {e}")
            raise AccountServiceError(f"Account service request failed: {e}")

    async def get_holdings(self, trading_account_id: str) -> List[Dict[str, Any]]:
        """
        Get holdings for a trading account.
        
        Replaces: SELECT from public.holdings (if accessed)
        
        Args:
            trading_account_id: Trading account ID
            
        Returns:
            List of holding information dictionaries
        """
        try:
            base_url = await self._get_base_url()
            client = await self._get_http_client()

            response = await client.get(
                f"{base_url}/api/v1/accounts/{trading_account_id}/holdings"
            )
            
            if response.status_code == 200:
                return response.json().get("holdings", [])
            elif response.status_code == 404:
                return []  # No holdings or account not found
            else:
                logger.error(f"Get holdings failed: {response.status_code} {response.text}")
                raise AccountServiceError(f"Get holdings failed: {response.status_code}")

        except httpx.RequestError as e:
            logger.error(f"Account service request failed: {e}")
            raise AccountServiceError(f"Account service request failed: {e}")

    async def bulk_update_account_tiers(
        self, 
        tier_updates: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Bulk update account tiers for multiple accounts.
        
        Args:
            tier_updates: List of tier update dictionaries
            
        Returns:
            Update result summary
        """
        try:
            base_url = await self._get_base_url()
            client = await self._get_http_client()

            response = await client.post(
                f"{base_url}/api/v1/accounts/bulk-update-tiers",
                json={"updates": tier_updates}
            )
            
            if response.status_code == 200:
                return response.json()
            else:
                logger.error(f"Bulk tier update failed: {response.status_code} {response.text}")
                raise AccountServiceError(f"Bulk tier update failed: {response.status_code}")

        except httpx.RequestError as e:
            logger.error(f"Account service request failed: {e}")
            raise AccountServiceError(f"Account service request failed: {e}")


# Singleton instance
_account_client: Optional[AccountServiceClient] = None


async def get_account_client() -> AccountServiceClient:
    """Get or create account service client singleton"""
    global _account_client
    if _account_client is None:
        _account_client = AccountServiceClient()
    return _account_client


async def cleanup_account_client():
    """Cleanup account service client"""
    global _account_client
    if _account_client:
        await _account_client.close()
        _account_client = None
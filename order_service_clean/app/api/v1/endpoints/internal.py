"""
Internal API Endpoints for Order Service

Service-to-service communication endpoints for account management.
Protected by internal API key authentication.
"""
import logging
import secrets
from datetime import datetime
from typing import List, Dict, Any
from fastapi import APIRouter, Header, HTTPException, status
from sqlalchemy import text

from ....config.settings import settings
from app.database.connection import get_async_session
from app.workers.sync_workers import get_worker_manager
from app.clients.user_service_client import UserServiceClient, UserServiceClientError

logger = logging.getLogger(__name__)

router = APIRouter()


def verify_internal_api_key(x_internal_api_key: str = Header(None)):
    """Verify internal API key for service-to-service authentication.

    Uses secrets.compare_digest() to prevent timing attacks.
    """
    expected_key = getattr(settings, 'internal_api_key', None)

    # If no internal API key configured, log warning and allow (backward compatibility)
    if not expected_key:
        logger.warning("No internal_api_key configured - allowing internal request without authentication")
        return True

    if not x_internal_api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing X-Internal-API-Key header"
        )

    # Use constant-time comparison to prevent timing attacks
    if not secrets.compare_digest(x_internal_api_key, expected_key):
        logger.warning(f"Invalid internal API key attempt")
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Invalid internal API key"
        )

    return True


async def _fetch_active_accounts() -> List[Dict[str, Any]]:
    """Load active Kite accounts that are currently synced."""
    try:
        async with UserServiceClient() as client:
            user_accounts = await client.list_active_trading_accounts(status_filter="ACTIVE")
    except UserServiceClientError as exc:
        logger.error("Failed to load active accounts from user_service: %s", exc)
        return []

    account_ids = [int(acc["trading_account_id"]) for acc in user_accounts]
    if not account_ids:
        return []

    async for session in get_async_session():
        result = await session.execute(
            text("""
                SELECT trading_account_id, COALESCE(sync_tier, 'cold') AS sync_tier
                FROM order_service.account_sync_tiers
                WHERE trading_account_id = ANY(:account_ids)
            """),
            {"account_ids": account_ids}
        )
        tier_map = {int(row.trading_account_id): row.sync_tier for row in result.fetchall()}

        accounts = []
        for acc in user_accounts:
            trading_account_id = int(acc["trading_account_id"])
            accounts.append({
                "trading_account_id": trading_account_id,
                "broker_account_id": None,
                "broker_user_id": acc.get("broker_user_id"),
                "nickname": acc.get("account_name") or "",
                "sync_tier": tier_map.get(trading_account_id, "cold"),
                "is_active": acc.get("status", "").upper() == "ACTIVE"
            })
        return accounts

    return []


@router.post("/internal/reload-accounts")
async def reload_accounts(x_internal_api_key: str = Header(None)):
    """
    Reload broker accounts and restart sync workers.

    Called by user_service when trading accounts are linked, updated, or unlinked.
    This triggers:
    1. Reload accounts from public.kite_accounts table
    2. Start/stop sync workers for changed accounts
    3. Re-initialize broker API clients

    **Authentication:** Requires X-Internal-API-Key header

    **Returns:**
    - `200 OK`: Accounts reloaded successfully
    - `401 Unauthorized`: Missing API key
    - `403 Forbidden`: Invalid API key
    """
    # Verify internal API key
    verify_internal_api_key(x_internal_api_key)

    try:
        logger.info("Received reload-accounts request from user_service")

        accounts = await _fetch_active_accounts()
        logger.info(f"Discovered {len(accounts)} active Kite accounts")

        worker_manager = get_worker_manager()
        refreshed_channels = []
        if worker_manager:
            refreshed_channels = list(await worker_manager.refresh_subscriptions())
            logger.info("WebSocket channel subscriptions refreshed for account reload")

        return {
            "success": True,
            "message": "Account reload completed",
            "active_accounts": accounts,
            "websocket_channels": refreshed_channels,
            "timestamp": datetime.utcnow().isoformat()
        }

    except Exception as e:
        logger.error(f"Failed to reload accounts: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to reload accounts: {str(e)}"
        )

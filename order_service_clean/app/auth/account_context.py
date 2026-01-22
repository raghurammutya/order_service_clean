"""
Account Context - Trading Account ID extraction and validation.

Provides FastAPI dependencies for extracting and validating trading account IDs
from JWT tokens and optional header overrides for multi-account users.

Usage:
    from app.auth.account_context import get_trading_account_id

    @router.get("/positions")
    async def get_positions(
        trading_account_id: int = Depends(get_trading_account_id)
    ):
        # trading_account_id is validated and ready to use
        ...

Author: Claude Code
Date: 2025-11-25
"""

import logging
from typing import Dict, Any, Optional
from fastapi import Depends, Header, Query

# Import from parent auth module to use the correct implementation (gateway_auth or jwt_auth)
from . import get_current_user, get_current_user_optional

logger = logging.getLogger(__name__)


async def get_trading_account_id(
    current_user: Dict[str, Any] = Depends(get_current_user),
    x_trading_account_id: Optional[str] = Header(None, description="Override trading account ID for multi-account users. Omit for 'All Accounts' aggregation."),
    trading_account_id: Optional[int] = Query(None, description="Query parameter for trading account ID (alternative to X-Trading-Account-ID header)"),
) -> Optional[int]:
    """
    Get trading account ID from JWT claims, header, or query parameter.

    **GitHub Issue #439: Support for "All Accounts" aggregation**
    - When trading_account_id is MISSING (not provided), returns None
    - Endpoints should detect None and aggregate data across all user's accessible accounts

    Priority:
    1. Query parameter trading_account_id (frontend compatibility)
    2. X-Trading-Account-Id header (if provided and user has access)
    3. trading_account_id from JWT claims
    4. None (triggers "All Accounts" aggregation mode)

    Args:
        current_user: Current authenticated user from JWT
        x_trading_account_id: Optional header override for multi-account users
                             OMIT this header entirely to trigger "All Accounts" mode
        trading_account_id: Optional query parameter (frontend uses this)

    Returns:
        Validated trading account ID (int) or None for "All Accounts" mode
        Note: Returns int since we fixed the type conversion in service classes

    Example:
        # Single account mode (query parameter - frontend uses this)
        curl "/api/v1/holdings?trading_account_id=1"
        # Returns: holdings for account 1

        # Single account mode (header - legacy)
        curl -H "X-Trading-Account-ID: 1" /api/v1/holdings
        # Returns: holdings for account 1

        # All accounts mode (omit both)
        curl /api/v1/holdings
        # Returns: aggregated holdings from all accessible accounts
    """
    user_id = current_user.get("user_id")

    # Priority 1: Check for query parameter (frontend uses this)
    if trading_account_id is not None:
        logger.info(
            f"Single account mode (query param): user={user_id}, "
            f"trading_account_id={trading_account_id}"
        )
        return trading_account_id

    # Priority 2: Check for header override
    if x_trading_account_id is not None:
        # Header provided - use single account mode
        logger.info(
            f"Single account mode (header): user={user_id}, "
            f"trading_account_id={x_trading_account_id}"
        )
        return int(x_trading_account_id)

    # Priority 3: Check if user has multiple accounts (Issue #439)
    # If user has acct_ids with multiple accounts, and no param/header provided,
    # this is "All Accounts" mode
    acct_ids = current_user.get("acct_ids", [])
    if isinstance(acct_ids, list) and len(acct_ids) > 1:
        # User has access to multiple accounts and didn't specify which one
        # Trigger "All Accounts" aggregation mode
        logger.info(
            f"All Accounts mode triggered: user={user_id}, "
            f"accessible_accounts={acct_ids} (no query param or header)"
        )
        return None

    # Priority 4: Get from JWT claims or acct_ids[0] if available
    account_id = current_user.get("trading_account_id")
    if account_id is None and acct_ids:
        # Fall back to first account if only one available
        account_id = acct_ids[0]

    if account_id is not None:
        logger.info(f"Single account mode (JWT): user={user_id}, trading_account_id={account_id}")
        return int(account_id) if isinstance(account_id, str) else account_id

    # No account ID found
    logger.warning(f"No trading account found for user: {user_id}")
    return None


async def get_trading_account_id_optional(
    current_user: Dict[str, Any] = Depends(get_current_user_optional),
    x_trading_account_id: Optional[str] = Header(None, description="Override trading account ID"),
) -> Optional[str]:
    """
    Get trading account ID, returning None if not available.

    Same as get_trading_account_id but returns None instead of raising error.
    Useful for endpoints that can work without an account (e.g., listing accounts).

    Args:
        current_user: Current user (may be mock user if auth disabled)
        x_trading_account_id: Optional header override

    Returns:
        Trading account ID or None
    """
    # Check for header override
    if x_trading_account_id is not None:
        return x_trading_account_id

    # Get from user context
    account_id = current_user.get("trading_account_id")

    if account_id is not None:
        return str(account_id)

    return None


async def require_trading_account(
    trading_account_id: str = Depends(get_trading_account_id),
) -> str:
    """
    Require a valid trading account ID.

    This is an alias for get_trading_account_id that makes the intent clearer.
    Use this when the endpoint absolutely requires a trading account.

    Args:
        trading_account_id: Trading account ID from get_trading_account_id

    Returns:
        Validated trading account ID
    """
    return trading_account_id

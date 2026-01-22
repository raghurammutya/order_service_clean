"""
Test Authentication - Bypass for ACL Integration Testing

This module provides a simple authentication bypass for testing ACL integration
without dealing with HTTPBearer/JWT complexities.

SECURITY WARNING: Only enable TEST_AUTH_MODE in non-production environments!
"""
import os
import logging
from typing import Dict, Any, Optional

from fastapi import Header, HTTPException, Request

logger = logging.getLogger(__name__)

# Check if test mode is enabled
TEST_AUTH_MODE = os.getenv("TEST_AUTH_MODE", "false").lower() == "true"


async def get_current_user(
    request: Request,
    x_user_id: Optional[str] = Header(None, alias="X-User-ID"),
    x_gateway_secret: Optional[str] = Header(None, alias="X-Gateway-Secret"),
) -> Dict[str, Any]:
    """
    Test authentication that bypasses HTTPBearer for ACL testing.

    Simply trusts X-User-ID header when X-Gateway-Secret is provided.
    """
    logger.info(f"TEST AUTH: get_current_user() CALLED! user_id={x_user_id}, has_secret={x_gateway_secret is not None}, TEST_AUTH_MODE={TEST_AUTH_MODE}")

    if not TEST_AUTH_MODE:
        logger.error("TEST AUTH: TEST_AUTH_MODE not enabled!")
        raise HTTPException(
            status_code=500,
            detail="TEST_AUTH_MODE not enabled"
        )

    if not x_user_id:
        logger.warning("TEST AUTH: No X-User-ID header provided")
        raise HTTPException(status_code=401, detail="X-User-ID header required")

    if not x_gateway_secret:
        logger.warning("TEST AUTH: No X-Gateway-Secret header provided")
        raise HTTPException(status_code=401, detail="X-Gateway-Secret header required")

    # For testing, we trust the X-User-ID header
    logger.info(f"TEST AUTH: Successfully authenticated user_id={x_user_id}")

    return {
        "user_id": x_user_id,
        "user_id_int": int(x_user_id) if x_user_id.isdigit() else None,
        "username": f"test_user_{x_user_id}",
        "email": f"test_{x_user_id}@test.com",
        "roles": ["user"],
        "permissions": ["*"],
        "session_id": "test_session",
        "mfa_verified": False,
        "trading_account_id": None,
        "acct_ids": [],
        "from_test_auth": True,
    }


async def get_current_user_optional(
    request: Request,
    x_user_id: Optional[str] = Header(None, alias="X-User-ID"),
    x_gateway_secret: Optional[str] = Header(None, alias="X-Gateway-Secret"),
) -> Optional[Dict[str, Any]]:
    """Optional test authentication."""
    try:
        return await get_current_user(request, x_user_id, x_gateway_secret)
    except HTTPException:
        return None


async def require_admin(user: Dict[str, Any]) -> Dict[str, Any]:
    """Test admin requirement (always passes)."""
    return user


def require_permission(permission: str):
    """Test permission requirement (always passes)."""
    async def dependency(user: Dict[str, Any]) -> Dict[str, Any]:
        return user
    return dependency


def require_role(*required_roles: str):
    """Test role requirement (always passes)."""
    async def dependency(user: Dict[str, Any]) -> Dict[str, Any]:
        return user
    return dependency


async def verify_jwt_token() -> Dict[str, Any]:
    """
    Dummy JWT verification for test mode.

    This function is called by router-level dependencies in main.py.
    In test mode, we bypass JWT verification entirely - no HTTPBearer required.

    Returns empty dict since actual authentication happens in get_current_user.
    """
    logger.info("TEST AUTH: verify_jwt_token() CALLED! (bypassed)")
    return {}


async def cleanup():
    """Dummy cleanup function for test mode."""
    logger.info("TEST AUTH: cleanup() called (no-op)")
    pass

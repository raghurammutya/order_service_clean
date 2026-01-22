"""
Gateway Authentication for Order Service

This module provides authentication by trusting the API Gateway's headers
instead of validating JWT tokens directly. This is more efficient and
follows the "gateway-only auth" pattern.

When TRUST_GATEWAY_HEADERS=true:
  - Requests are expected to come through the API Gateway
  - The gateway validates JWT and injects X-User-ID, X-User-Roles headers
  - This service trusts those headers (after verifying X-Gateway-Secret)

When TRUST_GATEWAY_HEADERS=false (or for backward compatibility):
  - Falls back to direct JWT validation using jwt_auth.py

Usage:
    from app.auth.gateway_auth import get_current_user

    @router.get("/positions")
    async def get_positions(user = Depends(get_current_user)):
        return {"user_id": user["user_id"]}
"""
import os
import hmac
import logging
from typing import Dict, Any, Optional, List

from fastapi import Header, HTTPException, Request, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

from ..config.settings import settings

logger = logging.getLogger(__name__)

# Gateway secret - from config service
try:
    GATEWAY_SECRET = getattr(settings, 'gateway_secret', 'gateway_secret_2024_prod')
    TRUST_GATEWAY_HEADERS = getattr(settings, 'trust_gateway_headers', False)
except Exception:
    # Config service not available - fail fast
    raise RuntimeError("Settings module required - config service unavailable")

# HTTP Bearer for legacy auth
security = HTTPBearer(auto_error=False)


def _verify_gateway_secret(secret: Optional[str]) -> bool:
    """Verify the gateway secret using constant-time comparison."""
    if not secret:
        return False
    return hmac.compare_digest(secret, GATEWAY_SECRET)


def _parse_user_id(user_id_str: str) -> int:
    """Parse user ID from gateway header format."""
    if not user_id_str:
        raise ValueError("Empty user ID")

    # Handle "user:7" format from gateway
    if user_id_str.startswith("user:"):
        user_id_str = user_id_str[5:]

    # Handle service tokens
    if user_id_str.startswith("service:"):
        return 0

    try:
        return int(user_id_str)
    except ValueError:
        raise ValueError(f"Invalid user ID: {user_id_str}")


async def get_current_user_from_gateway(
    request: Request,
    x_user_id: Optional[str] = Header(None, alias="X-User-ID"),
    x_user_roles: Optional[str] = Header("", alias="X-User-Roles"),
    x_session_id: Optional[str] = Header(None, alias="X-Session-ID"),
    x_gateway_secret: Optional[str] = Header(None, alias="X-Gateway-Secret"),
) -> Dict[str, Any]:
    """
    Get current user from gateway-injected headers.

    This is used when TRUST_GATEWAY_HEADERS=true.
    """
    # Verify request came from gateway
    if not _verify_gateway_secret(x_gateway_secret):
        logger.warning(
            f"Request rejected: invalid gateway secret "
            f"from {request.client.host if request.client else 'unknown'}"
        )
        raise HTTPException(
            status_code=403,
            detail="Direct access forbidden. Use the API Gateway."
        )

    # Check for user ID
    if not x_user_id:
        raise HTTPException(status_code=401, detail="Authentication required")

    # Parse user ID
    try:
        user_id = _parse_user_id(x_user_id)
    except ValueError as e:
        logger.warning(f"Invalid user ID header: {e}")
        raise HTTPException(status_code=401, detail="Invalid authentication")

    # Parse roles
    roles = [r.strip() for r in x_user_roles.split(",") if r.strip()]

    return {
        "user_id": x_user_id,  # Keep original format for compatibility
        "user_id_int": user_id,
        "username": None,
        "email": None,
        "roles": roles,
        "permissions": [],  # Gateway doesn't inject scopes by default
        "session_id": x_session_id,
        "mfa_verified": False,
        "trading_account_id": None,  # Will be set by account context
        "acct_ids": [],
        "from_gateway": True,
    }


async def get_current_user(
    request: Request,
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(security),
    x_user_id: Optional[str] = Header(None, alias="X-User-ID"),
    x_user_roles: Optional[str] = Header("", alias="X-User-Roles"),
    x_session_id: Optional[str] = Header(None, alias="X-Session-ID"),
    x_gateway_secret: Optional[str] = Header(None, alias="X-Gateway-Secret"),
) -> Dict[str, Any]:
    """
    Get current user - tries gateway auth first, falls back to JWT.

    This provides a smooth migration path:
    1. If gateway headers present and valid -> use gateway auth (fast path)
    2. Otherwise -> fall back to JWT validation (legacy path)

    Usage:
        @router.get("/endpoint")
        async def endpoint(user = Depends(get_current_user)):
            user_id = user["user_id"]
    """
    # DEBUG: Log authentication attempt
    logger.info(f"get_current_user called: TRUST_GATEWAY_HEADERS={TRUST_GATEWAY_HEADERS}, "
                f"has_gateway_secret={x_gateway_secret is not None}, "
                f"has_user_id={x_user_id is not None}, "
                f"has_bearer_token={credentials is not None}")

    # If gateway headers present and trust is enabled, use gateway auth
    if TRUST_GATEWAY_HEADERS and x_gateway_secret:
        logger.info(f"Checking gateway secret verification...")
        if _verify_gateway_secret(x_gateway_secret):
            logger.info(f"Gateway secret verified, user_id={x_user_id}")
            if x_user_id:
                logger.info(f"Returning user from gateway auth for user_id={x_user_id}")
                return await get_current_user_from_gateway(
                    request, x_user_id, x_user_roles, x_session_id, x_gateway_secret
                )
            else:
                logger.warning("Gateway secret verified but no user_id provided")
        else:
            logger.warning("Gateway secret verification FAILED")

    # Fall back to JWT validation if bearer token is present
    if credentials:
        logger.info("No valid gateway headers, falling back to JWT validation")
        try:
            from .jwt_auth import verify_jwt_token_sync

            # Extract the token string from credentials
            token = credentials.credentials
            logger.info(f"Received token length: {len(token) if token else 0}, first 50 chars: {token[:50] if token else 'None'}")

            # Verify the JWT token
            token_payload = verify_jwt_token_sync(token)

            # Extract user info from token payload
            trading_account_id = token_payload.get("trading_account_id")
            if trading_account_id is None:
                acct_ids = token_payload.get("acct_ids", [])
                if acct_ids:
                    trading_account_id = acct_ids[0]

            logger.info(f"JWT validation successful for user_id={token_payload.get('sub')}")
            return {
                "user_id": token_payload.get("sub"),
                "username": token_payload.get("username"),
                "email": token_payload.get("email"),
                "roles": token_payload.get("roles", []),
                "permissions": token_payload.get("scp", []),
                "session_id": token_payload.get("sid"),
                "mfa_verified": token_payload.get("mfa", False),
                "trading_account_id": trading_account_id,
                "acct_ids": token_payload.get("acct_ids", []),
            }
        except Exception as e:
            logger.error(f"JWT validation failed: {e}")
            raise HTTPException(status_code=401, detail=f"Invalid authentication token: {str(e)}")

    # No gateway auth and no JWT token - raise authentication error
    logger.warning(f"Authentication failed: gateway_headers_present={x_gateway_secret is not None}, user_id_present={x_user_id is not None}, trust_enabled={TRUST_GATEWAY_HEADERS}, bearer_token_present={credentials is not None}")
    raise HTTPException(status_code=401, detail="Authentication required. Provide Bearer token or gateway headers (X-User-ID and X-Gateway-Secret).")


async def get_current_user_optional(
    request: Request,
    x_user_id: Optional[str] = Header(None, alias="X-User-ID"),
    x_user_roles: Optional[str] = Header("", alias="X-User-Roles"),
    x_session_id: Optional[str] = Header(None, alias="X-Session-ID"),
    x_gateway_secret: Optional[str] = Header(None, alias="X-Gateway-Secret"),
) -> Optional[Dict[str, Any]]:
    """
    Optionally get current user - returns None if not authenticated.
    """
    try:
        return await get_current_user(
            request, x_user_id, x_user_roles, x_session_id, x_gateway_secret
        )
    except HTTPException:
        return None


async def require_admin(
    user: Dict[str, Any] = Depends(get_current_user)
) -> Dict[str, Any]:
    """Require admin role."""
    if "admin" not in user.get("roles", []):
        raise HTTPException(status_code=403, detail="Admin role required")
    return user


def require_role(*required_roles: str):
    """
    Dependency factory to require specific roles.

    Usage:
        @router.get("/admin-only")
        async def admin_endpoint(user = Depends(require_role("admin"))):
            return {"message": "Admin access"}
    """
    async def dependency(user: Dict[str, Any] = Depends(get_current_user)) -> Dict[str, Any]:
        user_roles = user.get("roles", [])
        if not any(role in user_roles for role in required_roles):
            logger.warning(
                f"User {user.get('user_id')} denied access: "
                f"required roles {required_roles}, has {user_roles}"
            )
            raise HTTPException(
                status_code=403,
                detail=f"One of these roles required: {', '.join(required_roles)}"
            )
        return user
    return dependency


def require_permission(permission: str):
    """
    Dependency factory to require specific permission (scope).

    Note: With gateway auth, permissions/scopes are not typically passed through.
    This is provided for API compatibility with legacy jwt_auth.
    Consider using require_role instead.

    Usage:
        @router.get("/trading")
        async def trade_endpoint(user = Depends(require_permission("trade"))):
            return {"message": "Trade access"}
    """
    async def dependency(user: Dict[str, Any] = Depends(get_current_user)) -> Dict[str, Any]:
        user_permissions = user.get("permissions", [])
        if permission not in user_permissions and "*" not in user_permissions:
            logger.warning(
                f"User {user.get('user_id')} denied access: "
                f"required permission '{permission}', has {user_permissions}"
            )
            raise HTTPException(
                status_code=403,
                detail=f"Permission '{permission}' required"
            )
        return user
    return dependency

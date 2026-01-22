"""
JWT authentication for ticker_service_v2.

Adapted from ticker_service v1 with simplifications for v2's focused scope.
"""
import logging
import time
import ipaddress
import json
from typing import Dict, Any, Optional
from urllib.parse import urlparse

import jwt
from jwt import PyJWKClient
import httpx
from fastapi import HTTPException, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
import redis.asyncio as aioredis

from ..config.settings import settings

logger = logging.getLogger(__name__)

# HTTP Bearer security scheme
security = HTTPBearer()

# Global Redis client for token revocation
_redis_client: Optional[aioredis.Redis] = None

# Cached JWKS with proactive refresh
_jwks_cache = {
    "keys": None,
    "fetched_at": 0,
    "ttl": 300,  # 5 minute cache (reduced from 1 hour for faster key rotation detection)
    "refresh_before": 60,  # Refresh 60s before expiry
    "last_refresh_attempt": 0,
    "min_refresh_interval": 30,  # Minimum 30s between refresh attempts
}


def _get_redis_client() -> Optional[aioredis.Redis]:
    """Get or create Redis client for token revocation."""
    global _redis_client

    if _redis_client is None:
        try:
            _redis_client = aioredis.from_url(
                settings.redis_url,
                decode_responses=True
            )
            logger.info("Redis client initialized for token revocation")
        except Exception as e:
            logger.error(f"Failed to initialize Redis client: {e}")
            return None

    return _redis_client


async def is_token_revoked(token: str) -> bool:
    """Check if a token has been revoked.

    SECURITY: Implements fail-closed security. If Redis is unavailable or
    there's an error checking revocation status, we reject the token
    (fail securely) rather than allowing it (fail-open).

    Args:
        token: JWT token string

    Returns:
        True if token is revoked, False if confirmed not revoked

    Raises:
        HTTPException: If revocation status cannot be determined
    """
    redis_client = _get_redis_client()
    if not redis_client:
        # SECURITY: Fail-closed - reject token if we can't verify revocation status
        logger.error("SECURITY: Redis unavailable - rejecting token (fail-closed security)")
        raise HTTPException(
            status_code=503,
            detail="Token revocation service unavailable - request rejected for security"
        )

    try:
        revoked = await redis_client.get(f"revoked_token:{token}")
        is_revoked = revoked == "1"

        if is_revoked:
            logger.warning("Token is revoked - access denied")
        else:
            logger.debug("Token revocation check passed")

        return is_revoked

    except Exception as e:
        # SECURITY: Fail-closed - reject token on error
        logger.error(f"SECURITY: Error checking token revocation - rejecting token (fail-closed): {e}")
        raise HTTPException(
            status_code=503,
            detail="Token revocation check failed - request rejected for security"
        )


async def revoke_token(token: str, ttl: int = 86400) -> None:
    """Revoke a token by adding it to Redis blacklist.

    Args:
        token: JWT token string
        ttl: Time-to-live for the revocation (default 24 hours)
    """
    redis_client = _get_redis_client()
    if not redis_client:
        raise HTTPException(503, "Token revocation service unavailable")

    try:
        await redis_client.setex(f"revoked_token:{token}", ttl, "1")
        logger.info(f"Token revoked (TTL: {ttl}s)")
    except Exception as e:
        logger.error(f"Error revoking token: {e}")
        raise HTTPException(503, "Failed to revoke token")


def validate_jwks_url(url: str) -> None:
    """Validate JWKS URL to prevent SSRF attacks.

    Args:
        url: JWKS URL to validate

    Raises:
        HTTPException: If URL is invalid or potentially malicious
    """
    parsed = urlparse(url)

    # 1. HTTPS-only enforcement (except localhost and internal Docker services)
    # Internal Docker service names must use HTTP because TLS isn't configured for inter-service communication
    allowed_http_hosts = (
        "localhost", "127.0.0.1", "host.docker.internal",
        "user-service", "user_service",  # Docker service names (always allowed for internal JWKS)
    )
    # Allow server's public IP via HTTP in development only
    if settings.environment != "production":
        allowed_http_hosts = allowed_http_hosts + ("5.223.52.98",)

    if parsed.scheme != "https" and parsed.hostname not in allowed_http_hosts:
        raise HTTPException(400, "JWKS URL must use HTTPS")

    # 2. Domain whitelist validation (if configured)
    if hasattr(settings, "jwks_allowed_domains") and settings.jwks_allowed_domains:
        if parsed.hostname not in settings.jwks_allowed_domains:
            raise HTTPException(403, f"JWKS domain not allowed: {parsed.hostname}")

    # 3. Block private IP addresses
    try:
        ip = ipaddress.ip_address(parsed.hostname)
        if ip.is_private or ip.is_loopback or ip.is_link_local:
            if settings.environment == "production":
                raise HTTPException(403, "Private IP addresses not allowed in production")
    except ValueError:
        # Not an IP address, it's a hostname - that's OK
        pass

    # 4. Block AWS metadata endpoint
    if parsed.hostname == "169.254.169.254":
        raise HTTPException(403, "Access to metadata endpoint is forbidden")

    logger.debug(f"JWKS URL validated: {url}")


def invalidate_jwks_cache() -> None:
    """Invalidate the JWKS cache to force a refresh on next fetch."""
    global _jwks_cache
    _jwks_cache["fetched_at"] = 0
    logger.info("JWKS cache invalidated")


def fetch_jwks(force_refresh: bool = False) -> Dict[str, Any]:
    """Fetch JWKS from user_service with caching and key rotation support.

    Args:
        force_refresh: Force fetch even if cache is valid (used on key not found)

    Returns:
        JWKS dictionary

    Raises:
        HTTPException: If JWKS cannot be fetched
    """
    global _jwks_cache

    now = time.time()
    cache_age = now - _jwks_cache["fetched_at"]

    # Check if we should use cache
    if not force_refresh and _jwks_cache["keys"] and cache_age < _jwks_cache["ttl"]:
        # Proactive refresh if cache is about to expire
        if cache_age > (_jwks_cache["ttl"] - _jwks_cache["refresh_before"]):
            # Only attempt refresh if we haven't tried recently
            if (now - _jwks_cache["last_refresh_attempt"]) > _jwks_cache["min_refresh_interval"]:
                logger.debug("Proactive JWKS refresh (cache expiring soon)")
                try:
                    _do_jwks_fetch(now)
                except Exception:
                    pass  # Proactive refresh failure is ok, cache is still valid

        logger.debug("Using cached JWKS")
        return _jwks_cache["keys"]

    # Rate limit forced refreshes to prevent hammering JWKS endpoint
    if force_refresh:
        if (now - _jwks_cache["last_refresh_attempt"]) < _jwks_cache["min_refresh_interval"]:
            logger.warning("JWKS refresh rate limited, using cached keys")
            if _jwks_cache["keys"]:
                return _jwks_cache["keys"]

    # Fetch new JWKS
    return _do_jwks_fetch(now)


def _do_jwks_fetch(now: float) -> Dict[str, Any]:
    """Actually fetch JWKS from user_service.

    Args:
        now: Current timestamp

    Returns:
        JWKS dictionary

    Raises:
        HTTPException: If JWKS cannot be fetched
    """
    global _jwks_cache

    _jwks_cache["last_refresh_attempt"] = now
    jwks_url = settings.jwks_url

    # Validate URL (SSRF protection)
    validate_jwks_url(jwks_url)

    try:
        logger.info(f"Fetching JWKS from: {jwks_url}")
        # Use httpx sync client (thread-safe, supports HTTP/2)
        with httpx.Client(timeout=10.0) as client:
            response = client.get(jwks_url)
            response.raise_for_status()

        jwks = response.json()

        # Update cache
        _jwks_cache["keys"] = jwks
        _jwks_cache["fetched_at"] = now

        logger.info(f"JWKS fetched successfully ({len(jwks.get('keys', []))} keys)")
        return jwks

    except httpx.HTTPError as e:
        logger.error(f"Failed to fetch JWKS: {e}")

        # Fall back to cached JWKS if available
        if _jwks_cache["keys"]:
            logger.warning("Using stale cached JWKS due to fetch failure")
            return _jwks_cache["keys"]

        raise HTTPException(503, "Unable to verify tokens - JWKS unavailable")


def verify_jwt_token_sync(token: str, _retry_on_key_not_found: bool = True) -> Dict[str, Any]:
    """Verify JWT token synchronously.

    Args:
        token: JWT token string
        _retry_on_key_not_found: Internal flag for retry logic

    Returns:
        Decoded token payload

    Raises:
        HTTPException: If token is invalid
    """
    if not token:
        raise HTTPException(401, "Missing authentication token")

    # Handle potential binary token data (same fix as alert_service/message_service/backend)
    if isinstance(token, bytes):
        try:
            token = token.decode('utf-8')
        except UnicodeDecodeError as e:
            logger.error(f"Token is binary and cannot be decoded: {e}")
            raise HTTPException(401, "Invalid token encoding")

    # Strip whitespace and validate token format
    token = token.strip()
    if not token:
        raise HTTPException(401, "Empty authentication token")

    try:
        # Fetch JWKS
        jwks = fetch_jwks()

        # Get unverified header to find correct key
        unverified_header = jwt.get_unverified_header(token)
        kid = unverified_header.get("kid")

        # Find matching key and convert JWK to proper key format
        signing_key = None
        for key in jwks.get("keys", []):
            if key.get("kid") == kid:
                # Convert JWK dict to PyJWK object which can be used for verification
                from jwt.algorithms import RSAAlgorithm
                signing_key = RSAAlgorithm.from_jwk(json.dumps(key))
                break

        if not signing_key:
            # Key not found - might be due to key rotation
            # Try refreshing JWKS cache and retry once
            if _retry_on_key_not_found:
                logger.info(f"Key {kid} not found, refreshing JWKS cache and retrying")
                fetch_jwks(force_refresh=True)
                return verify_jwt_token_sync(token, _retry_on_key_not_found=False)

            logger.warning(f"No matching key found for kid: {kid} (after cache refresh)")
            raise HTTPException(401, "Invalid token - key not found")

        # ADR-001 Exception: order_service validates JWT for defense-in-depth (financial transactions)
        # This is an approved exception to P3 (JWT validation at gateway only) because:
        # 1. Order service handles real money transactions requiring extra security
        # 2. Needs acct_ids claim for multi-account authorization (not in gateway headers)
        # 3. Prevents header spoofing attacks on financial operations
        # 4. Implements two-tier validation: fast path (JWT) + slow path (user_service API)
        # Reference: /docs/ADR-001-SECURITY-EXCEPTIONS.md
        payload = jwt.decode(
            token,
            key=signing_key,
            algorithms=["RS256"],
            audience=settings.jwt_audience,
            issuer=settings.jwt_issuer,
            options={"verify_exp": True, "verify_signature": True}
        )

        logger.debug(f"JWT verified for user: {payload.get('sub')}")
        return payload

    except jwt.ExpiredSignatureError:
        logger.warning("JWT expired")
        raise HTTPException(401, "Token has expired")
    except jwt.InvalidTokenError as e:
        logger.warning(f"Invalid JWT: {e}")
        raise HTTPException(401, "Invalid token")
    except HTTPException:
        raise  # Re-raise HTTP exceptions as-is
    except Exception as e:
        logger.error(f"JWT verification error: {e}")
        raise HTTPException(500, "Token verification failed")


async def verify_jwt_token(
    credentials: HTTPAuthorizationCredentials = Depends(security)
) -> Dict[str, Any]:
    """FastAPI dependency to verify JWT token.

    Args:
        credentials: HTTP Authorization credentials

    Returns:
        Decoded token payload

    Raises:
        HTTPException: If token is invalid or revoked
    """
    token = credentials.credentials

    # Check if token is revoked
    if await is_token_revoked(token):
        logger.warning("Attempt to use revoked token")
        raise HTTPException(401, "Token has been revoked")

    # Verify token
    return verify_jwt_token_sync(token)


async def get_current_user(
    token_payload: Dict[str, Any] = Depends(verify_jwt_token)
) -> Dict[str, Any]:
    """FastAPI dependency to get current user from verified token.

    Args:
        token_payload: Decoded JWT payload

    Returns:
        User information dictionary
    """
    # Handle trading account ID - support both single ID and array format
    # user_service sends acct_ids (array), legacy format uses trading_account_id
    trading_account_id = token_payload.get("trading_account_id")
    if trading_account_id is None:
        acct_ids = token_payload.get("acct_ids", [])
        if acct_ids:
            trading_account_id = acct_ids[0]  # Use first account as default

    return {
        "user_id": token_payload.get("sub"),
        "username": token_payload.get("username"),
        "email": token_payload.get("email"),
        "roles": token_payload.get("roles", []),
        "permissions": token_payload.get("scp", []),
        "session_id": token_payload.get("sid"),
        "mfa_verified": token_payload.get("mfa", False),
        "trading_account_id": trading_account_id,  # Multi-account support
        "acct_ids": token_payload.get("acct_ids", []),  # Full list for multi-account switching
    }


async def get_current_user_optional(
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(HTTPBearer(auto_error=False))
) -> Dict[str, Any]:
    """FastAPI dependency that returns current authenticated user.

    Always requires JWT authentication. No mock/bypass mode.

    Returns:
        User information dictionary

    Raises:
        HTTPException: If authentication fails or token is invalid
    """
    # Auth is always required - no bypasses
    if not credentials:
        raise HTTPException(401, "Authentication required")

    token = credentials.credentials

    # Check if token is revoked
    if await is_token_revoked(token):
        logger.warning("Attempt to use revoked token")
        raise HTTPException(401, "Token has been revoked")

    # Verify token
    token_payload = verify_jwt_token_sync(token)

    # Handle trading account ID - support both single ID and array format
    # user_service sends acct_ids (array), legacy format uses trading_account_id
    trading_account_id = token_payload.get("trading_account_id")
    if trading_account_id is None:
        acct_ids = token_payload.get("acct_ids", [])
        if acct_ids:
            trading_account_id = acct_ids[0]  # Use first account as default

    return {
        "user_id": token_payload.get("sub"),
        "username": token_payload.get("username"),
        "email": token_payload.get("email"),
        "roles": token_payload.get("roles", []),
        "permissions": token_payload.get("scp", []),
        "session_id": token_payload.get("sid"),
        "mfa_verified": token_payload.get("mfa", False),
        "trading_account_id": trading_account_id,  # Multi-account support
        "acct_ids": token_payload.get("acct_ids", []),  # Full list for multi-account switching
    }


async def require_admin(
    current_user: Dict[str, Any] = Depends(get_current_user)
) -> Dict[str, Any]:
    """FastAPI dependency that requires admin role.

    Args:
        current_user: Current user dictionary

    Returns:
        Current user dictionary

    Raises:
        HTTPException: If user is not admin
    """
    if "admin" not in current_user.get("roles", []):
        logger.warning(f"Access denied - admin role required for user: {current_user.get('user_id')}")
        raise HTTPException(403, "Admin role required")

    return current_user


async def require_permission(
    permission: str,
    current_user: Dict[str, Any] = Depends(get_current_user)
) -> Dict[str, Any]:
    """FastAPI dependency that requires specific permission.

    Args:
        permission: Required permission scope
        current_user: Current user dictionary

    Returns:
        Current user dictionary

    Raises:
        HTTPException: If user lacks permission
    """
    user_permissions = current_user.get("permissions", [])

    if permission not in user_permissions:
        logger.warning(
            f"Access denied - permission '{permission}' required for user: {current_user.get('user_id')}"
        )
        raise HTTPException(403, f"Permission '{permission}' required")

    return current_user


# WebSocket authentication
async def verify_ws_token(token: str) -> Dict[str, Any]:
    """Verify JWT token for WebSocket connections.

    Args:
        token: JWT token string (from query param or header)

    Returns:
        User information dictionary

    Raises:
        HTTPException: If token is invalid or revoked
    """
    if not token:
        raise HTTPException(401, "Missing authentication token")

    # Check if token is revoked
    if await is_token_revoked(token):
        raise HTTPException(401, "Token has been revoked")

    # Verify token
    payload = verify_jwt_token_sync(token)

    # Return user info
    return {
        "user_id": payload.get("sub"),
        "username": payload.get("username"),
        "roles": payload.get("roles", []),
        "permissions": payload.get("scp", []),
        "session_id": payload.get("sid")
    }


async def cleanup():
    """Cleanup Redis connection on shutdown."""
    global _redis_client

    if _redis_client:
        await _redis_client.close()
        _redis_client = None
        logger.info("Redis client closed")

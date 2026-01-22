"""
Authentication module for Order Service

This module provides a unified authentication interface that:
1. Uses test auth when TEST_AUTH_MODE=true (for ACL testing only!)
2. Uses gateway auth when TRUST_GATEWAY_HEADERS=true (recommended for production)
3. Falls back to JWT validation when gateway auth is not available

The gateway auth pattern is preferred because:
- Single point of JWT validation at the API Gateway
- Reduced latency (no JWKS fetch, no Redis revocation check per service)
- Simplified service code
- Improved security (single enforcement point)
"""
import os

# Check if test mode is enabled (for ACL testing)
TEST_AUTH_MODE = os.getenv("TEST_AUTH_MODE", "false").lower() == "true"

# Check if we should trust gateway headers
TRUST_GATEWAY_HEADERS = os.getenv("TRUST_GATEWAY_HEADERS", "false").lower() == "true"

if TEST_AUTH_MODE:
    # Use test auth - ONLY FOR TESTING ACL INTEGRATION
    from .test_auth import (
        get_current_user,
        get_current_user_optional,
        require_admin,
        require_role,
        require_permission,
        verify_jwt_token,
        cleanup,
    )
elif TRUST_GATEWAY_HEADERS:
    # Use gateway auth - services trust X-User-ID header from gateway
    from .gateway_auth import (
        get_current_user,
        get_current_user_optional,
        require_admin,
        require_role,
        require_permission,
    )
    # These are not needed with gateway auth but imported for compatibility
    from .jwt_auth import verify_jwt_token, cleanup
else:
    # Use legacy JWT auth - each service validates tokens
    from .jwt_auth import (
        verify_jwt_token,
        get_current_user,
        get_current_user_optional,
        require_admin,
        require_permission,
        cleanup,
    )

from .account_context import (
    get_trading_account_id,
    get_trading_account_id_optional,
    require_trading_account,
)

__all__ = [
    "verify_jwt_token",
    "cleanup",
    "get_current_user",
    "get_current_user_optional",
    "require_admin",
    "require_permission",
    "require_role" if TRUST_GATEWAY_HEADERS or TEST_AUTH_MODE else None,
    "get_trading_account_id",
    "get_trading_account_id_optional",
    "require_trading_account",
    "TRUST_GATEWAY_HEADERS",
    "TEST_AUTH_MODE",
]
# Clean up None values from __all__
__all__ = [x for x in __all__ if x is not None]

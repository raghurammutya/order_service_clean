"""
Internal Authentication Security Layer

Provides enhanced authentication for critical order endpoints including:
- X-Internal-API-Key validation with signature verification
- Service-to-service mutual authentication
- Request context validation for order operations
"""

import logging
import hashlib
import hmac
import time
from typing import Optional, Dict, Any
from fastapi import HTTPException, Request, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

from ..config.settings import settings

logger = logging.getLogger(__name__)

security = HTTPBearer(auto_error=False)


class InternalAuthError(Exception):
    """Internal authentication error"""
    pass


class CriticalServiceAuth:
    """
    Enhanced authentication for critical order endpoints.
    
    Validates:
    1. X-Internal-API-Key header
    2. X-Service-Identity header  
    3. Request signature (HMAC)
    4. Service authorization matrix
    """

    # Define which services can access order endpoints
    AUTHORIZED_SERVICES = {
        "algo_engine": ["place_order", "modify_order", "cancel_order"],
        "user_interface": ["place_order", "get_orders", "cancel_order"],
        "risk_manager": ["cancel_order", "get_positions"],
        "strategy_service": ["place_order", "modify_order"],
    }

    def __init__(self):
        self.internal_api_keys = self._load_internal_api_keys()
        self.service_secrets = self._load_service_secrets()

    def _load_internal_api_keys(self) -> Dict[str, str]:
        """Load internal API keys for each authorized service"""
        try:
            # In production, these would come from config service
            return {
                "algo_engine": getattr(settings, 'algo_engine_api_key', 'dev-key-algo'),
                "user_interface": getattr(settings, 'user_interface_api_key', 'dev-key-ui'),
                "risk_manager": getattr(settings, 'risk_manager_api_key', 'dev-key-risk'),
                "strategy_service": getattr(settings, 'strategy_service_api_key', 'dev-key-strategy'),
            }
        except Exception as e:
            logger.error(f"Failed to load internal API keys: {e}")
            return {}

    def _load_service_secrets(self) -> Dict[str, str]:
        """Load service secrets for HMAC signature verification"""
        try:
            # In production, these would come from config service
            return {
                "algo_engine": getattr(settings, 'algo_engine_secret', 'dev-secret-algo'),
                "user_interface": getattr(settings, 'user_interface_secret', 'dev-secret-ui'),
                "risk_manager": getattr(settings, 'risk_manager_secret', 'dev-secret-risk'),
                "strategy_service": getattr(settings, 'strategy_service_secret', 'dev-secret-strategy'),
            }
        except Exception as e:
            logger.error(f"Failed to load service secrets: {e}")
            return {}

    def validate_service_identity(self, request: Request) -> str:
        """
        Validate service identity from headers.
        
        Returns:
            Service name if valid
            
        Raises:
            HTTPException: If validation fails
        """
        # Check X-Service-Identity header
        service_identity = request.headers.get("X-Service-Identity")
        if not service_identity:
            raise HTTPException(
                status_code=401,
                detail="Missing X-Service-Identity header"
            )

        # Check X-Internal-API-Key header
        api_key = request.headers.get("X-Internal-API-Key")
        if not api_key:
            raise HTTPException(
                status_code=401,
                detail="Missing X-Internal-API-Key header"
            )

        # Validate service is authorized
        if service_identity not in self.AUTHORIZED_SERVICES:
            logger.warning(f"Unauthorized service attempted access: {service_identity}")
            raise HTTPException(
                status_code=403,
                detail=f"Service '{service_identity}' not authorized"
            )

        # Validate API key
        expected_key = self.internal_api_keys.get(service_identity)
        if not expected_key or api_key != expected_key:
            logger.warning(f"Invalid API key for service: {service_identity}")
            raise HTTPException(
                status_code=401,
                detail="Invalid API key"
            )

        return service_identity

    def validate_request_signature(self, request: Request, service_identity: str) -> bool:
        """
        Validate HMAC request signature.
        
        Args:
            request: FastAPI request
            service_identity: Validated service identity
            
        Returns:
            True if signature is valid
            
        Raises:
            HTTPException: If signature validation fails
        """
        # Get signature from header
        signature = request.headers.get("X-Request-Signature")
        if not signature:
            raise HTTPException(
                status_code=401,
                detail="Missing X-Request-Signature header"
            )

        # Get timestamp from header  
        timestamp = request.headers.get("X-Request-Timestamp")
        if not timestamp:
            raise HTTPException(
                status_code=401,
                detail="Missing X-Request-Timestamp header"
            )

        # Check timestamp freshness (5 minute window)
        try:
            request_time = float(timestamp)
            current_time = time.time()
            if abs(current_time - request_time) > 300:  # 5 minutes
                raise HTTPException(
                    status_code=401,
                    detail="Request timestamp too old"
                )
        except ValueError:
            raise HTTPException(
                status_code=401,
                detail="Invalid timestamp format"
            )

        # Get service secret
        service_secret = self.service_secrets.get(service_identity)
        if not service_secret:
            logger.error(f"No secret configured for service: {service_identity}")
            raise HTTPException(
                status_code=500,
                detail="Service secret not configured"
            )

        # Build signature payload
        method = request.method
        path = str(request.url.path)
        
        # For POST/PUT requests, include body in signature
        body = ""
        if hasattr(request.state, "body"):
            body = request.state.body.decode("utf-8")

        payload = f"{method}|{path}|{body}|{timestamp}"
        
        # Calculate expected signature
        expected_signature = hmac.new(
            service_secret.encode("utf-8"),
            payload.encode("utf-8"),
            hashlib.sha256
        ).hexdigest()

        # Compare signatures
        if not hmac.compare_digest(signature, expected_signature):
            logger.warning(f"Invalid signature from service: {service_identity}")
            raise HTTPException(
                status_code=401,
                detail="Invalid request signature"
            )

        return True

    def validate_service_authorization(self, service_identity: str, operation: str) -> bool:
        """
        Validate service is authorized for specific operation.
        
        Args:
            service_identity: Service identity
            operation: Operation name (e.g., 'place_order')
            
        Returns:
            True if authorized
            
        Raises:
            HTTPException: If not authorized
        """
        authorized_operations = self.AUTHORIZED_SERVICES.get(service_identity, [])
        
        if operation not in authorized_operations:
            logger.warning(
                f"Service {service_identity} attempted unauthorized operation: {operation}"
            )
            raise HTTPException(
                status_code=403,
                detail=f"Service not authorized for operation: {operation}"
            )

        return True


# Global auth instance
_critical_auth: Optional[CriticalServiceAuth] = None


def get_critical_auth() -> CriticalServiceAuth:
    """Get critical service auth instance"""
    global _critical_auth
    if _critical_auth is None:
        _critical_auth = CriticalServiceAuth()
    return _critical_auth


async def validate_internal_service(
    request: Request,
    operation: str = "general"
) -> str:
    """
    Dependency for validating internal service authentication.
    
    Args:
        request: FastAPI request
        operation: Operation being performed
        
    Returns:
        Service identity if valid
        
    Raises:
        HTTPException: If authentication fails
    """
    auth = get_critical_auth()
    
    # Validate service identity and API key
    service_identity = auth.validate_service_identity(request)
    
    # Validate request signature (HMAC)
    auth.validate_request_signature(request, service_identity)
    
    # Validate service authorization for operation
    auth.validate_service_authorization(service_identity, operation)
    
    logger.info(f"Authenticated service {service_identity} for operation {operation}")
    return service_identity


# Specific operation validators
async def validate_order_placement(request: Request) -> str:
    """Validate authentication for order placement"""
    return await validate_internal_service(request, "place_order")


async def validate_order_modification(request: Request) -> str:
    """Validate authentication for order modification"""
    return await validate_internal_service(request, "modify_order")


async def validate_order_cancellation(request: Request) -> str:
    """Validate authentication for order cancellation"""
    return await validate_internal_service(request, "cancel_order")


# Middleware to capture request body for signature validation
async def capture_request_body(request: Request, call_next):
    """Middleware to capture request body for signature validation"""
    if request.method in ["POST", "PUT", "PATCH"]:
        body = await request.body()
        request.state.body = body
    
    response = await call_next(request)
    return response
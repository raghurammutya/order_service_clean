"""
Internal API Authentication Middleware

Validates X-Internal-API-Key header for service-to-service communication.
Following StocksBlitz security patterns.
"""
import logging
from typing import List
from fastapi import Request, HTTPException, status
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response

logger = logging.getLogger(__name__)


class InternalAuthMiddleware(BaseHTTPMiddleware):
    """Middleware to validate internal API key authentication"""
    
    def __init__(self, app, required_header: str = "X-Internal-API-Key", 
                 valid_api_key: str = None, exclude_paths: List[str] = None):
        super().__init__(app)
        self.required_header = required_header
        self.valid_api_key = valid_api_key
        self.exclude_paths = exclude_paths or []
    
    async def dispatch(self, request: Request, call_next):
        """Validate API key for protected endpoints"""
        
        # Skip authentication for excluded paths
        if any(request.url.path.startswith(path) for path in self.exclude_paths):
            return await call_next(request)
        
        # Skip for OPTIONS requests (CORS preflight)
        if request.method == "OPTIONS":
            return await call_next(request)
        
        # Get API key from header
        api_key = request.headers.get(self.required_header)
        
        if not api_key:
            logger.warning(f"Missing {self.required_header} header for {request.url.path}")
            return Response(
                content=f"{self.required_header} header required",
                status_code=status.HTTP_401_UNAUTHORIZED,
                headers={"Content-Type": "text/plain"}
            )
        
        # Validate API key
        if self.valid_api_key and api_key != self.valid_api_key:
            logger.warning(f"Invalid API key for {request.url.path}: {api_key[:8]}...")
            return Response(
                content="Invalid API key",
                status_code=status.HTTP_401_UNAUTHORIZED,
                headers={"Content-Type": "text/plain"}
            )
        
        # Add authenticated flag to request state
        request.state.authenticated = True
        request.state.api_key = api_key
        
        return await call_next(request)


def verify_internal_token(request: Request) -> dict:
    """Dependency to verify internal API token"""
    if not getattr(request.state, 'authenticated', False):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing authentication"
        )
    
    return {
        "authenticated": True,
        "api_key": getattr(request.state, 'api_key', None)
    }
"""
Rate Limiting Middleware

Implements configurable rate limiting for API endpoints using
Redis-backed sliding window algorithm for production deployment.
"""
import time
import logging
import hashlib
from typing import Optional
from fastapi import Request, HTTPException, status
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response
import redis.asyncio as redis

logger = logging.getLogger(__name__)


class RateLimitMiddleware(BaseHTTPMiddleware):
    """
    Redis-backed rate limiting middleware with sliding window algorithm
    """
    
    def __init__(self, app, redis_url: str, 
                 requests_per_minute: int = 100, 
                 burst_capacity: int = 20,
                 exclude_paths: list = None):
        super().__init__(app)
        self.redis_url = redis_url
        self.requests_per_minute = requests_per_minute
        self.burst_capacity = burst_capacity
        self.exclude_paths = exclude_paths or ["/health", "/ready", "/metrics"]
        self.window_size = 60  # 1 minute window
        self._redis = None
        
    async def _get_redis(self):
        """Get or create Redis connection"""
        if self._redis is None:
            self._redis = await redis.from_url(self.redis_url)
        return self._redis
    
    def _get_client_key(self, request: Request) -> str:
        """Generate client identifier for rate limiting"""
        # Use API key if available (for service-to-service calls)
        api_key = request.headers.get("X-Internal-API-Key")
        if api_key:
            # Hash API key for security
            return f"rate_limit:api_key:{hashlib.sha256(api_key.encode()).hexdigest()[:16]}"
        
        # Fall back to IP address for unauthenticated requests
        client_ip = request.client.host if request.client else "unknown"
        forwarded_for = request.headers.get("X-Forwarded-For", "").split(",")[0].strip()
        client_ip = forwarded_for if forwarded_for else client_ip
        
        return f"rate_limit:ip:{client_ip}"
    
    async def _check_rate_limit(self, client_key: str) -> tuple[bool, dict]:
        """
        Check rate limit using sliding window algorithm
        
        Returns:
            (is_allowed, limit_info)
        """
        try:
            redis_conn = await self._get_redis()
            current_time = int(time.time())
            window_start = current_time - self.window_size
            
            # Use Redis pipeline for atomic operations
            pipe = redis_conn.pipeline()
            
            # Remove old entries
            pipe.zremrangebyscore(client_key, 0, window_start)
            
            # Count current requests in window
            pipe.zcard(client_key)
            
            # Add current request
            pipe.zadd(client_key, {str(current_time): current_time})
            
            # Set expiration
            pipe.expire(client_key, self.window_size * 2)
            
            # Execute pipeline
            results = await pipe.execute()
            current_count = results[1] + 1  # +1 for the current request
            
            # Check limits
            is_allowed = current_count <= self.requests_per_minute
            
            # Calculate reset time
            reset_time = current_time + self.window_size
            
            limit_info = {
                "limit": self.requests_per_minute,
                "remaining": max(0, self.requests_per_minute - current_count),
                "reset": reset_time,
                "current": current_count
            }
            
            return is_allowed, limit_info
            
        except Exception as e:
            logger.error(f"Rate limiting check failed: {e}")
            # In case of Redis failure, allow the request but log the error
            return True, {
                "limit": self.requests_per_minute,
                "remaining": self.requests_per_minute,
                "reset": int(time.time()) + self.window_size,
                "error": "rate_limit_check_failed"
            }
    
    async def dispatch(self, request: Request, call_next):
        """Apply rate limiting to requests"""
        
        # Skip rate limiting for excluded paths
        if any(request.url.path.startswith(path) for path in self.exclude_paths):
            return await call_next(request)
        
        # Skip for OPTIONS requests (CORS preflight)
        if request.method == "OPTIONS":
            return await call_next(request)
        
        # Get client identifier
        client_key = self._get_client_key(request)
        
        # Check rate limit
        is_allowed, limit_info = await self._check_rate_limit(client_key)
        
        if not is_allowed:
            # Rate limit exceeded
            correlation_id = getattr(request.state, 'correlation_id', 'unknown')
            logger.warning(f"Rate limit exceeded for {client_key} [correlation_id: {correlation_id}]")
            
            return Response(
                content=f"Rate limit exceeded. Limit: {limit_info['limit']}/minute",
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                headers={
                    "X-RateLimit-Limit": str(limit_info["limit"]),
                    "X-RateLimit-Remaining": str(limit_info["remaining"]),
                    "X-RateLimit-Reset": str(limit_info["reset"]),
                    "Retry-After": str(self.window_size)
                }
            )
        
        # Process request and add rate limit headers
        response = await call_next(request)
        
        # Add rate limit headers to response
        response.headers["X-RateLimit-Limit"] = str(limit_info["limit"])
        response.headers["X-RateLimit-Remaining"] = str(limit_info["remaining"])
        response.headers["X-RateLimit-Reset"] = str(limit_info["reset"])
        
        return response


class ConfigurableRateLimiter:
    """Factory for creating rate limiters with different configurations"""
    
    @staticmethod
    def create_from_config(redis_url: str, config_client) -> RateLimitMiddleware:
        """Create rate limiter from configuration"""
        
        # Get rate limiting configuration
        requests_per_minute = config_client.get_int("RATE_LIMIT_PER_MINUTE", 100)
        burst_capacity = config_client.get_int("RATE_LIMIT_BURST", 20)
        
        # Health check endpoints don't need rate limiting
        exclude_paths = [
            "/health", 
            "/ready", 
            "/metrics",
            "/api/docs",
            "/api/redoc",
            "/api/openapi.json"
        ]
        
        logger.info(f"Rate limiting configured: {requests_per_minute}/minute, burst: {burst_capacity}")
        
        return RateLimitMiddleware(
            app=None,  # Will be set by FastAPI
            redis_url=redis_url,
            requests_per_minute=requests_per_minute,
            burst_capacity=burst_capacity,
            exclude_paths=exclude_paths
        )


# Helper function to check current rate limit status
async def get_rate_limit_status(client_key: str, redis_url: str) -> Optional[dict]:
    """Get current rate limit status for a client"""
    try:
        redis_conn = await redis.from_url(redis_url)
        current_time = int(time.time())
        window_start = current_time - 60  # 1 minute window
        
        # Count requests in current window
        count = await redis_conn.zcount(client_key, window_start, current_time)
        
        await redis_conn.close()
        
        return {
            "current_requests": count,
            "window_start": window_start,
            "window_end": current_time
        }
        
    except Exception as e:
        logger.error(f"Failed to get rate limit status: {e}")
        return None
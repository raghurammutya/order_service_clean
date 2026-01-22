"""
Idempotency Service for Order Operations

Prevents duplicate order placement by tracking idempotency keys.
Implements RFC 9562 compliant idempotency handling.
"""
import logging
import hashlib
import json
import os
from typing import Optional, Dict, Any
from datetime import datetime, timedelta

import redis.asyncio as aioredis
from fastapi import HTTPException

logger = logging.getLogger(__name__)


class IdempotencyService:
    """Service for managing idempotency keys and preventing duplicate operations."""

    def __init__(self, redis_url: str, ttl_hours: int = 24, fail_closed: Optional[bool] = None):
        """
        Initialize idempotency service.

        Args:
            redis_url: Redis connection URL
            ttl_hours: Time-to-live for idempotency keys (default 24 hours)
            fail_closed: If True, raise 503 on Redis errors (prevents duplicates).
                        If False, fail-open (allow requests through on Redis errors).
                        Default: Read from IDEMPOTENCY_FAIL_CLOSED env var (default True)
        """
        self.redis_url = redis_url
        self.ttl_seconds = ttl_hours * 3600
        self._redis_client: Optional[aioredis.Redis] = None

        # Default to fail-closed for safety (prevents duplicate orders)
        if fail_closed is None:
            self.fail_closed = os.getenv("IDEMPOTENCY_FAIL_CLOSED", "true").lower() == "true"
        else:
            self.fail_closed = fail_closed

        logger.info(
            f"IdempotencyService initialized (TTL: {ttl_hours} hours, "
            f"fail_closed: {self.fail_closed})"
        )

    async def connect(self):
        """Connect to Redis."""
        if self._redis_client is None:
            try:
                self._redis_client = await aioredis.from_url(
                    self.redis_url,
                    decode_responses=True
                )
                logger.info("Idempotency service connected to Redis")
            except Exception as e:
                logger.error(f"Failed to connect to Redis for idempotency: {e}")
                raise

    async def disconnect(self):
        """Disconnect from Redis."""
        if self._redis_client:
            await self._redis_client.close()
            self._redis_client = None
            logger.info("Idempotency service disconnected from Redis")

    def _get_key(self, idempotency_key: str, user_id: int) -> str:
        """
        Generate Redis key for idempotency.

        Args:
            idempotency_key: User-provided idempotency key
            user_id: User ID (for isolation between users)

        Returns:
            Redis key string
        """
        # Hash the idempotency key for consistent length
        key_hash = hashlib.sha256(idempotency_key.encode()).hexdigest()
        return f"idempotency:user:{user_id}:key:{key_hash}"

    async def check_and_store(
        self,
        idempotency_key: str,
        user_id: int,
        request_data: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """
        Check if request with this idempotency key has been processed before.

        Args:
            idempotency_key: Idempotency key from header
            user_id: User ID making the request
            request_data: Request payload (for conflict detection)

        Returns:
            Cached response if duplicate request, None if first time

        Raises:
            HTTPException: If idempotency key conflicts with different request
        """
        if not self._redis_client:
            await self.connect()

        redis_key = self._get_key(idempotency_key, user_id)

        try:
            # Check if key exists
            cached_data = await self._redis_client.get(redis_key)

            if cached_data:
                # Parse cached data
                cached = json.loads(cached_data)

                # Verify request fingerprint matches
                # This prevents using same idempotency key for different requests
                request_fingerprint = self._fingerprint(request_data)
                cached_fingerprint = cached.get("request_fingerprint")

                if request_fingerprint != cached_fingerprint:
                    logger.warning(
                        f"Idempotency key conflict detected for user {user_id}: "
                        f"key={idempotency_key[:16]}..."
                    )
                    raise HTTPException(
                        status_code=422,
                        detail=(
                            "Idempotency key conflict: same key used for different request. "
                            "Please use a unique key for each unique request."
                        )
                    )

                # Return cached response
                logger.info(
                    f"Returning cached response for idempotency key "
                    f"{idempotency_key[:16]}... (user {user_id})"
                )
                return cached.get("response")

            # First time seeing this key - store request fingerprint
            # Response will be stored later via store_response()
            await self._redis_client.setex(
                f"{redis_key}:pending",
                300,  # 5 minute pending TTL
                json.dumps({
                    "request_fingerprint": self._fingerprint(request_data),
                    "started_at": datetime.utcnow().isoformat()
                })
            )

            return None

        except HTTPException:
            raise
        except Exception as e:
            # Log Redis error with details for monitoring/alerting
            logger.error(
                f"Redis error in idempotency check: {e.__class__.__name__}: {e}",
                extra={
                    "user_id": user_id,
                    "idempotency_key_prefix": idempotency_key[:16] if idempotency_key else None,
                    "error_type": e.__class__.__name__,
                    "fail_closed": self.fail_closed
                }
            )

            if self.fail_closed:
                # Fail-closed: Reject request to prevent duplicates
                # This is the safe default for financial operations
                logger.warning(
                    f"Rejecting order due to Redis unavailability (fail_closed=True). "
                    f"User: {user_id}, Key: {idempotency_key[:16]}..."
                )
                raise HTTPException(
                    status_code=503,
                    detail=(
                        "Idempotency service temporarily unavailable. "
                        "Please retry your request in a few moments. "
                        "This prevents duplicate order execution."
                    )
                )
            else:
                # Fail-open: Allow request through (not recommended for orders)
                logger.warning(
                    f"Allowing order despite Redis error (fail_closed=False - NOT RECOMMENDED). "
                    f"User: {user_id}, Key: {idempotency_key[:16]}..."
                )
                return None

    async def store_response(
        self,
        idempotency_key: str,
        user_id: int,
        request_data: Dict[str, Any],
        response_data: Dict[str, Any]
    ):
        """
        Store successful response for idempotency key.

        Args:
            idempotency_key: Idempotency key from header
            user_id: User ID
            request_data: Original request payload
            response_data: Response to cache
        """
        if not self._redis_client:
            await self.connect()

        redis_key = self._get_key(idempotency_key, user_id)

        try:
            # Store response with request fingerprint
            cached_data = {
                "request_fingerprint": self._fingerprint(request_data),
                "response": response_data,
                "stored_at": datetime.utcnow().isoformat()
            }

            await self._redis_client.setex(
                redis_key,
                self.ttl_seconds,
                json.dumps(cached_data)
            )

            # Delete pending marker
            await self._redis_client.delete(f"{redis_key}:pending")

            logger.info(
                f"Stored idempotent response for key {idempotency_key[:16]}... "
                f"(user {user_id}, TTL: {self.ttl_seconds}s)"
            )

        except Exception as e:
            logger.error(f"Error storing idempotency response: {e}")
            # Don't raise - response was already sent to client

    async def delete_key(self, idempotency_key: str, user_id: int):
        """
        Delete idempotency key (for testing or manual cleanup).

        Args:
            idempotency_key: Idempotency key to delete
            user_id: User ID
        """
        if not self._redis_client:
            await self.connect()

        redis_key = self._get_key(idempotency_key, user_id)

        try:
            deleted = await self._redis_client.delete(redis_key, f"{redis_key}:pending")
            logger.info(f"Deleted idempotency key {idempotency_key[:16]}... (deleted {deleted} keys)")
        except Exception as e:
            logger.error(f"Error deleting idempotency key: {e}")

    def _fingerprint(self, data: Dict[str, Any]) -> str:
        """
        Create fingerprint of request data for conflict detection.

        Args:
            data: Request data dictionary

        Returns:
            SHA256 hash of canonical JSON representation
        """
        # Create canonical JSON (sorted keys)
        canonical = json.dumps(data, sort_keys=True, default=str)
        return hashlib.sha256(canonical.encode()).hexdigest()

    async def get_stats(self) -> Dict[str, Any]:
        """
        Get idempotency service statistics.

        Returns:
            Statistics dictionary
        """
        if not self._redis_client:
            return {"error": "Not connected"}

        try:
            # Count active idempotency keys
            keys = await self._redis_client.keys("idempotency:*")
            pending_keys = [k for k in keys if k.endswith(":pending")]
            stored_keys = [k for k in keys if not k.endswith(":pending")]

            return {
                "total_keys": len(keys),
                "stored_responses": len(stored_keys),
                "pending_requests": len(pending_keys),
                "connected": True
            }
        except Exception as e:
            logger.error(f"Error getting idempotency stats: {e}")
            return {"error": str(e)}


# Global idempotency service instance
_idempotency_service: Optional[IdempotencyService] = None


def get_idempotency_service() -> IdempotencyService:
    """Get or create global idempotency service instance."""
    global _idempotency_service

    if _idempotency_service is None:
        raise RuntimeError("Idempotency service not initialized. Call init_idempotency_service() first.")

    return _idempotency_service


def init_idempotency_service(
    redis_url: str,
    ttl_hours: int = 24,
    fail_closed: Optional[bool] = None
) -> IdempotencyService:
    """
    Initialize global idempotency service.

    Args:
        redis_url: Redis connection URL
        ttl_hours: Time-to-live for idempotency keys
        fail_closed: If True, raise 503 on Redis errors (default True for safety)

    Returns:
        Initialized IdempotencyService instance
    """
    global _idempotency_service

    _idempotency_service = IdempotencyService(redis_url, ttl_hours, fail_closed)
    return _idempotency_service


async def shutdown_idempotency_service():
    """Shutdown global idempotency service."""
    global _idempotency_service

    if _idempotency_service:
        await _idempotency_service.disconnect()
        _idempotency_service = None

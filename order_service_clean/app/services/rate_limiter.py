"""
Redis-based rate limiter for hard refresh.

Uses Redis for distributed rate limiting across multiple instances.
Enforces 1 hard refresh per minute per account.
"""
import logging
from datetime import datetime, timezone
from typing import Optional, Tuple
import redis.asyncio as aioredis

from ..config.settings import settings

logger = logging.getLogger(__name__)


class HardRefreshRateLimiter:
    """Rate limiter for hard refresh API."""

    @property
    def RATE_LIMIT_SECONDS(self) -> int:
        """Get rate limit from config service"""
        from ..config.settings import settings
        return getattr(settings, 'hard_refresh_rate_limit_seconds', 60)
    KEY_PREFIX = "hard_refresh:"

    def __init__(self, redis_client: aioredis.Redis):
        """Initialize rate limiter.

        Args:
            redis_client: Redis client for distributed rate limiting
        """
        self.redis = redis_client

    async def check_rate_limit(self, account_id: int) -> Tuple[bool, int]:
        """Check if hard refresh is allowed.

        Args:
            account_id: Trading account ID

        Returns:
            Tuple of (is_allowed, seconds_remaining)
            - is_allowed: True if refresh is allowed, False if rate limited
            - seconds_remaining: Seconds until next refresh allowed (0 if allowed)
        """
        key = f"{self.KEY_PREFIX}{account_id}"

        # Check if key exists (rate limited)
        ttl = await self.redis.ttl(key)

        if ttl > 0:
            # Rate limited - return time remaining
            return False, ttl

        # Not rate limited - set the key with expiry
        await self.redis.setex(key, self.RATE_LIMIT_SECONDS, "1")
        return True, 0

    async def get_last_refresh(self, account_id: int) -> Optional[datetime]:
        """Get timestamp of last hard refresh.

        Args:
            account_id: Trading account ID

        Returns:
            Datetime of last refresh, or None if never refreshed
        """
        key = f"{self.KEY_PREFIX}{account_id}:timestamp"
        timestamp = await self.redis.get(key)

        if timestamp:
            return datetime.fromisoformat(timestamp)
        return None

    async def record_refresh(self, account_id: int):
        """Record a hard refresh.

        Args:
            account_id: Trading account ID
        """
        key = f"{self.KEY_PREFIX}{account_id}:timestamp"
        await self.redis.setex(key, 3600, datetime.now(timezone.utc).isoformat())


# Global rate limiter instance
_rate_limiter: Optional[HardRefreshRateLimiter] = None
_redis_client: Optional[aioredis.Redis] = None


async def get_rate_limiter() -> HardRefreshRateLimiter:
    """Get or create rate limiter instance.

    Creates Redis connection if needed.

    Returns:
        HardRefreshRateLimiter instance
    """
    global _rate_limiter, _redis_client

    if _rate_limiter is None:
        try:
            _redis_client = aioredis.from_url(
                settings.redis_url,
                encoding="utf-8",
                decode_responses=True
            )
            # Test connection
            await _redis_client.ping()
            _rate_limiter = HardRefreshRateLimiter(_redis_client)
            logger.info("Rate limiter initialized with Redis")
        except Exception as e:
            logger.error(f"Failed to initialize rate limiter: {e}")
            raise

    return _rate_limiter


async def close_rate_limiter():
    """Close Redis connection for rate limiter."""
    global _redis_client

    if _redis_client:
        await _redis_client.close()
        _redis_client = None
        logger.info("Rate limiter Redis connection closed")


# In-memory fallback for when Redis is unavailable
class InMemoryRateLimiter:
    """Fallback in-memory rate limiter when Redis is unavailable."""

    RATE_LIMIT_SECONDS = 60

    def __init__(self):
        self._timestamps: dict = {}

    async def check_rate_limit(self, account_id: int) -> Tuple[bool, int]:
        """Check if hard refresh is allowed.

        Args:
            account_id: Trading account ID

        Returns:
            Tuple of (is_allowed, seconds_remaining)
        """
        now = datetime.now(timezone.utc)
        last_refresh = self._timestamps.get(account_id)

        if last_refresh:
            elapsed = (now - last_refresh).total_seconds()
            if elapsed < self.RATE_LIMIT_SECONDS:
                seconds_remaining = int(self.RATE_LIMIT_SECONDS - elapsed)
                return False, seconds_remaining

        # Allowed - record timestamp
        self._timestamps[account_id] = now
        return True, 0

    async def get_last_refresh(self, account_id: int) -> Optional[datetime]:
        """Get timestamp of last hard refresh."""
        return self._timestamps.get(account_id)

    async def record_refresh(self, account_id: int):
        """Record a hard refresh."""
        self._timestamps[account_id] = datetime.now(timezone.utc)


# Fallback instance
_fallback_limiter = InMemoryRateLimiter()


async def get_rate_limiter_with_fallback() -> HardRefreshRateLimiter:
    """Get rate limiter with in-memory fallback.

    Tries to use Redis, falls back to in-memory if unavailable.

    Returns:
        Rate limiter instance
    """
    try:
        return await get_rate_limiter()
    except Exception as e:
        logger.warning(f"Redis unavailable, using in-memory rate limiter: {e}")
        return _fallback_limiter

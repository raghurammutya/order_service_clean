"""
Redis-backed Daily Order Counter

Tracks daily order count per trading account using Redis.
Resets at market close (15:30 IST) for the next trading day.

Features:
- Atomic increment operations
- Automatic TTL-based expiry
- Fallback to in-memory counter if Redis unavailable
- Statistics and monitoring

Key Format: kite:daily_orders:{account_id}:{date}
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, Optional, Any, List
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

# IST timezone
IST = ZoneInfo("Asia/Kolkata")


class RedisDailyCounter:
    """
    Redis-backed daily order counter.

    Tracks orders placed per day per account.
    Resets at market close time for next trading day.
    """

    KEY_PREFIX = "kite:daily_orders"
    @property
    def DEFAULT_LIMIT(self) -> int:
        """Get daily order limit from config service"""
        try:
            from ..config.settings import settings
            return getattr(settings, 'daily_order_limit', 3000)
        except ImportError:
            # Config service not available - fail fast
            raise RuntimeError("Settings module required - config service unavailable")
    
    @property 
    def DEFAULT_RESET_TIME(self) -> str:
        """Get daily reset time from config service"""
        try:
            from ..config.settings import settings
            return getattr(settings, 'daily_reset_time', "15:30")
        except ImportError:
            # Config service not available - fail fast
            raise RuntimeError("Settings module required - config service unavailable")

    def __init__(
        self,
        redis_client,
        daily_limit: Optional[int] = None,
        reset_time: Optional[str] = None,
    ):
        """
        Initialize daily counter.

        Args:
            redis_client: Async Redis client
            daily_limit: Maximum orders per day (default: 3000)
            reset_time: Reset time in HH:MM format IST (default: 15:30)
        """
        self.redis = redis_client
        self.daily_limit = daily_limit if daily_limit is not None else self.DEFAULT_LIMIT
        self.reset_time = reset_time if reset_time is not None else self.DEFAULT_RESET_TIME
        self._parse_reset_time()

        # Statistics
        self._total_increments = 0
        self._fallback_mode = False
        self._fallback_counts: Dict[str, int] = {}

        logger.info(
            f"RedisDailyCounter initialized: limit={daily_limit}, reset={reset_time} IST"
        )

    def _parse_reset_time(self) -> None:
        """Parse reset time string to hour and minute."""
        parts = self.reset_time.split(":")
        self._reset_hour = int(parts[0])
        self._reset_minute = int(parts[1]) if len(parts) > 1 else 0

    def _get_trading_date(self) -> str:
        """
        Get current trading date.

        Returns the date for which orders should be counted.
        After reset time, returns next trading date.
        """
        now = datetime.now(IST)

        # If before reset time, use today's date
        # If after reset time, use tomorrow's date (next trading day)
        reset_today = now.replace(
            hour=self._reset_hour,
            minute=self._reset_minute,
            second=0,
            microsecond=0
        )

        if now >= reset_today:
            # After market close, count towards next day
            trading_date = (now + timedelta(days=1)).date()
        else:
            trading_date = now.date()

        return trading_date.isoformat()

    def _get_key(self, trading_account_id: int) -> str:
        """Generate Redis key for account and date."""
        trading_date = self._get_trading_date()
        return f"{self.KEY_PREFIX}:{trading_account_id}:{trading_date}"

    def _get_next_reset_time(self) -> datetime:
        """Get next reset time as datetime."""
        now = datetime.now(IST)
        reset_today = now.replace(
            hour=self._reset_hour,
            minute=self._reset_minute,
            second=0,
            microsecond=0
        )

        if now >= reset_today:
            # Reset is tomorrow
            return reset_today + timedelta(days=1)
        else:
            return reset_today

    def _get_ttl_seconds(self) -> int:
        """Calculate TTL until next reset + buffer."""
        next_reset = self._get_next_reset_time()
        now = datetime.now(IST)
        ttl = (next_reset - now).total_seconds()

        # Add 1 hour buffer for safety
        return int(ttl + 3600)

    async def increment(self, trading_account_id: int) -> int:
        """
        Increment daily order count for account.

        Args:
            trading_account_id: Trading account ID

        Returns:
            New count after increment
        """
        self._total_increments += 1
        key = self._get_key(trading_account_id)

        try:
            if self._fallback_mode:
                raise Exception("Fallback mode active")

            # Atomic increment
            count = await self.redis.incr(key)

            # Set TTL on first increment
            if count == 1:
                ttl = self._get_ttl_seconds()
                await self.redis.expire(key, ttl)
                logger.debug(
                    f"Created daily counter for account {trading_account_id}, "
                    f"TTL={ttl}s"
                )

            logger.debug(
                f"Daily order count for account {trading_account_id}: {count}/{self.daily_limit}"
            )

            return int(count)

        except Exception as e:
            # Fallback to in-memory
            if not self._fallback_mode:
                self._fallback_mode = True
                logger.warning(
                    f"Redis unavailable, using in-memory fallback: {e}",
                    extra={"trading_account_id": trading_account_id}
                )

            # In-memory increment
            self._fallback_counts[key] = self._fallback_counts.get(key, 0) + 1
            return self._fallback_counts[key]

    async def get_count(self, trading_account_id: int) -> int:
        """
        Get current daily order count for account.

        Args:
            trading_account_id: Trading account ID

        Returns:
            Current count (0 if not set)
        """
        key = self._get_key(trading_account_id)

        try:
            if self._fallback_mode:
                raise Exception("Fallback mode active")

            count = await self.redis.get(key)
            return int(count) if count else 0

        except Exception:
            return self._fallback_counts.get(key, 0)

    async def get_remaining(self, trading_account_id: int) -> int:
        """
        Get remaining orders for today.

        Args:
            trading_account_id: Trading account ID

        Returns:
            Remaining orders (0 if limit exceeded)
        """
        count = await self.get_count(trading_account_id)
        return max(0, self.daily_limit - count)

    async def get_reset_time(self) -> datetime:
        """Get next reset time."""
        return self._get_next_reset_time()

    async def is_limit_exceeded(self, trading_account_id: int) -> bool:
        """Check if daily limit is exceeded."""
        return await self.get_remaining(trading_account_id) <= 0

    async def get_all_counts(self) -> Dict[int, int]:
        """
        Get counts for all accounts with orders today.

        Returns:
            Dict mapping account_id to count
        """
        pattern = f"{self.KEY_PREFIX}:*:{self._get_trading_date()}"
        result = {}

        try:
            if self._fallback_mode:
                raise Exception("Fallback mode active")

            async for key in self.redis.scan_iter(match=pattern):
                # Extract account_id from key
                key_str = key.decode() if isinstance(key, bytes) else key
                parts = key_str.split(":")
                if len(parts) >= 3:
                    account_id = int(parts[2])
                    count = await self.redis.get(key)
                    result[account_id] = int(count) if count else 0

        except Exception:
            # Return fallback counts
            trading_date = self._get_trading_date()
            for key, count in self._fallback_counts.items():
                if trading_date in key:
                    parts = key.split(":")
                    if len(parts) >= 3:
                        account_id = int(parts[2])
                        result[account_id] = count

        return result

    async def get_accounts_near_limit(
        self,
        threshold: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Get accounts with remaining orders below threshold.

        Args:
            threshold: Remaining orders threshold (default: 100)

        Returns:
            List of dicts with account_id, used, remaining
        """
        all_counts = await self.get_all_counts()
        near_limit = []

        for account_id, count in all_counts.items():
            remaining = self.daily_limit - count
            if remaining < threshold:
                near_limit.append({
                    "account_id": account_id,
                    "used": count,
                    "remaining": remaining,
                    "limit": self.daily_limit,
                })

        return sorted(near_limit, key=lambda x: x["remaining"])

    async def reset_fallback_mode(self) -> bool:
        """
        Attempt to reconnect to Redis and disable fallback mode.

        Returns:
            True if reconnection successful
        """
        try:
            await self.redis.ping()
            self._fallback_mode = False
            logger.info("Redis reconnected, fallback mode disabled")

            # Attempt to sync fallback counts to Redis
            for key, count in list(self._fallback_counts.items()):
                try:
                    await self.redis.set(key, count)
                    await self.redis.expire(key, self._get_ttl_seconds())
                    del self._fallback_counts[key]
                except Exception:
                    pass  # Keep in fallback for this key

            return True

        except Exception:
            return False

    def get_stats(self) -> Dict[str, Any]:
        """Get counter statistics."""
        return {
            "daily_limit": self.daily_limit,
            "reset_time": self.reset_time,
            "reset_timezone": "Asia/Kolkata",
            "next_reset": self._get_next_reset_time().isoformat(),
            "trading_date": self._get_trading_date(),
            "total_increments": self._total_increments,
            "fallback_mode": self._fallback_mode,
            "fallback_accounts": len(self._fallback_counts),
        }


class InMemoryDailyCounter:
    """
    In-memory daily counter (fallback when Redis unavailable).

    WARNING: Data is lost on service restart!
    Should only be used as fallback.
    """

    def __init__(self, daily_limit: int = 3000, reset_time: str = "15:30"):
        self.daily_limit = daily_limit
        self.reset_time = reset_time
        self._counts: Dict[str, int] = {}
        self._fallback_mode = True  # Always true for in-memory

        logger.warning(
            "InMemoryDailyCounter initialized - data will be lost on restart!"
        )

    def _get_key(self, trading_account_id: int) -> str:
        now = datetime.now(IST)
        parts = self.reset_time.split(":")
        reset_hour = int(parts[0])
        reset_minute = int(parts[1]) if len(parts) > 1 else 0

        reset_today = now.replace(hour=reset_hour, minute=reset_minute, second=0)

        if now >= reset_today:
            trading_date = (now + timedelta(days=1)).date()
        else:
            trading_date = now.date()

        return f"{trading_account_id}:{trading_date.isoformat()}"

    async def increment(self, trading_account_id: int) -> int:
        key = self._get_key(trading_account_id)
        self._counts[key] = self._counts.get(key, 0) + 1
        return self._counts[key]

    async def get_count(self, trading_account_id: int) -> int:
        key = self._get_key(trading_account_id)
        return self._counts.get(key, 0)

    async def get_remaining(self, trading_account_id: int) -> int:
        count = await self.get_count(trading_account_id)
        return max(0, self.daily_limit - count)

    async def get_reset_time(self) -> datetime:
        now = datetime.now(IST)
        parts = self.reset_time.split(":")
        reset_hour = int(parts[0])
        reset_minute = int(parts[1]) if len(parts) > 1 else 0

        reset_today = now.replace(hour=reset_hour, minute=reset_minute, second=0)
        if now >= reset_today:
            return reset_today + timedelta(days=1)
        return reset_today

    async def get_all_counts(self) -> Dict[int, int]:
        """Get all counts for today."""
        result = {}
        today = datetime.now(IST).date().isoformat()
        for key, count in self._counts.items():
            if today in key:
                account_id = int(key.split(":")[0])
                result[account_id] = count
        return result

    async def get_accounts_near_limit(self, threshold: int = 100) -> List[Dict[str, Any]]:
        """Get accounts near limit."""
        all_counts = await self.get_all_counts()
        near_limit = []
        for account_id, count in all_counts.items():
            remaining = self.daily_limit - count
            if remaining < threshold:
                near_limit.append({
                    "account_id": account_id,
                    "used": count,
                    "remaining": remaining,
                    "limit": self.daily_limit,
                })
        return sorted(near_limit, key=lambda x: x["remaining"])

    def get_stats(self) -> Dict[str, Any]:
        """Get counter statistics."""
        return {
            "daily_limit": self.daily_limit,
            "reset_time": self.reset_time,
            "fallback_mode": True,
            "total_accounts": len(self._counts),
        }


# Factory function
async def create_daily_counter(redis_client=None) -> RedisDailyCounter:
    """
    Create daily counter with Redis connection.

    Args:
        redis_client: Redis client (uses order_service Redis if not provided)

    Returns:
        RedisDailyCounter instance or InMemoryDailyCounter as fallback
    """
    daily_limit = 3000  # Kite's official limit
    reset_time = "15:30"  # Market close IST

    if redis_client is None:
        # Try to get Redis from order_service
        try:
            from ..database.redis_client import get_redis
            redis_client = get_redis()
        except Exception as e:
            logger.warning(f"Could not get Redis client: {e}")
            logger.warning("Using in-memory daily counter (data will be lost on restart)")
            return InMemoryDailyCounter(daily_limit=daily_limit, reset_time=reset_time)

    try:
        # Test connection
        await redis_client.ping()
        logger.info("Redis connected for daily counter")

        return RedisDailyCounter(
            redis_client=redis_client,
            daily_limit=daily_limit,
            reset_time=reset_time,
        )

    except Exception as e:
        logger.warning(f"Redis connection failed: {e}")
        logger.warning("Using in-memory daily counter (data will be lost on restart)")
        return InMemoryDailyCounter(daily_limit=daily_limit, reset_time=reset_time)

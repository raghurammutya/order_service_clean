"""
Per-Account Kite API Rate Limiter

Enforces Kite API rate limits on a per-trading-account basis.
Prevents 429 errors by throttling requests before they hit Kite.

Rate Limits (per Kite API docs):
- Orders: 10/sec, 200/min, 3000/day per account
- API GET: 10/sec per account
- Quote: 1/sec per account
- Historical: 3/sec per account

Architecture:
- Each trading account has its own rate limiter instance
- Per-second/minute limits use in-memory sliding windows
- Per-day limits use Redis for persistence across restarts
- Manager class provides centralized access
"""

import asyncio
import logging
import time
from collections import deque
from datetime import datetime, timezone
from enum import Enum
from typing import Dict, Optional, Tuple, Deque, Any

logger = logging.getLogger(__name__)


class KiteOperation(str, Enum):
    """Kite API operation types with their rate limit categories."""
    ORDER_PLACE = "order_place"      # Counted towards per-sec, per-min, per-day
    ORDER_MODIFY = "order_modify"    # Counted towards per-sec, per-min
    ORDER_CANCEL = "order_cancel"    # Counted towards per-sec, per-min
    API_GET = "api_get"              # Orders, positions, trades, margins
    QUOTE = "quote"                  # Quote/LTP API
    HISTORICAL = "historical"        # Historical data API


class RateLimitExceeded(Exception):
    """Raised when rate limit is exceeded and wait=False."""

    def __init__(
        self,
        message: str,
        limit_type: str,
        limit: int,
        current: int,
        retry_after: float = 0
    ):
        super().__init__(message)
        self.limit_type = limit_type
        self.limit = limit
        self.current = current
        self.retry_after = retry_after


class DailyLimitExceeded(RateLimitExceeded):
    """Raised when daily order limit is exceeded."""

    def __init__(self, trading_account_id: int, limit: int, used: int, reset_at: datetime):
        message = f"Daily order limit ({limit}) exceeded for account {trading_account_id}"
        super().__init__(message, "orders_per_day", limit, used, 0)
        self.trading_account_id = trading_account_id
        self.reset_at = reset_at


class SlidingWindowLimiter:
    """
    Sliding window rate limiter.

    Tracks actual request timestamps within a time window.
    More accurate than token bucket for short windows.
    """

    def __init__(self, max_requests: int, window_seconds: float):
        """
        Initialize rate limiter.

        Args:
            max_requests: Maximum requests allowed in window
            window_seconds: Time window in seconds
        """
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self._request_times: Deque[float] = deque()
        self._lock = asyncio.Lock()

        # Statistics
        self._total_requests = 0
        self._total_throttled = 0

    def _clean_old_requests(self, now: float) -> None:
        """Remove requests outside the current window."""
        cutoff_time = now - self.window_seconds
        while self._request_times and self._request_times[0] < cutoff_time:
            self._request_times.popleft()

    async def check_limit(self) -> Tuple[bool, float]:
        """
        Check if a request is allowed without consuming capacity.

        Returns:
            Tuple of (allowed, wait_time_seconds)
        """
        async with self._lock:
            now = time.time()
            self._clean_old_requests(now)

            if len(self._request_times) < self.max_requests:
                return True, 0.0
            else:
                # Calculate when oldest request will expire
                oldest_time = self._request_times[0]
                wait_time = (oldest_time + self.window_seconds) - now
                return False, max(0.0, wait_time)

    async def acquire(self, wait: bool = True, timeout: Optional[float] = None) -> bool:
        """
        Acquire permission to make a request.

        Args:
            wait: If True, wait for permission. If False, raise exception.
            timeout: Maximum time to wait (None = no timeout)

        Returns:
            True if permission granted

        Raises:
            RateLimitExceeded: If wait=False and limit exceeded
            asyncio.TimeoutError: If timeout exceeded
        """
        start_time = time.time()

        while True:
            async with self._lock:
                now = time.time()
                self._clean_old_requests(now)

                if len(self._request_times) < self.max_requests:
                    self._request_times.append(now)
                    self._total_requests += 1
                    return True

                if not wait:
                    raise RateLimitExceeded(
                        f"Rate limit exceeded: {self.max_requests} per {self.window_seconds}s",
                        limit_type=f"per_{int(self.window_seconds)}s",
                        limit=self.max_requests,
                        current=len(self._request_times),
                        retry_after=self._request_times[0] + self.window_seconds - now
                    )

                # Calculate wait time
                oldest_time = self._request_times[0]
                wait_time = (oldest_time + self.window_seconds) - now

            # Check timeout
            if timeout is not None:
                elapsed = time.time() - start_time
                if elapsed + wait_time > timeout:
                    raise asyncio.TimeoutError(
                        f"Rate limit wait would exceed timeout ({timeout}s)"
                    )

            if wait_time > 0:
                self._total_throttled += 1
                logger.debug(
                    f"Rate limit reached ({len(self._request_times)}/{self.max_requests}), "
                    f"waiting {wait_time:.3f}s"
                )
                await asyncio.sleep(wait_time + 0.01)  # Small buffer

    def get_stats(self) -> Dict[str, Any]:
        """Get current statistics."""
        now = time.time()
        self._clean_old_requests(now)

        return {
            "limit": self.max_requests,
            "window_seconds": self.window_seconds,
            "current": len(self._request_times),
            "available": self.max_requests - len(self._request_times),
            "utilization": len(self._request_times) / self.max_requests if self.max_requests > 0 else 0,
            "total_requests": self._total_requests,
            "total_throttled": self._total_throttled,
        }


class AccountRateLimiter:
    """
    Rate limiter for a single trading account.

    Enforces all Kite rate limits for one account:
    - Orders: 10/sec, 200/min, 3000/day
    - API: 10/sec
    - Quote: 1/sec
    - Historical: 3/sec
    """

    def __init__(
        self,
        trading_account_id: int,
        orders_per_second: int = 10,
        orders_per_minute: int = 200,
        orders_per_day: int = 3000,
        api_per_second: int = 10,
        quote_per_second: int = 1,
        historical_per_second: int = 3,
    ):
        """
        Initialize account rate limiter.

        Args:
            trading_account_id: Trading account ID
            orders_per_second: Max orders per second (default: 10)
            orders_per_minute: Max orders per minute (default: 200)
            orders_per_day: Max orders per day (default: 3000)
            api_per_second: Max API GET requests per second (default: 10)
            quote_per_second: Max quote requests per second (default: 1)
            historical_per_second: Max historical requests per second (default: 3)
        """
        self.trading_account_id = trading_account_id
        self.created_at = datetime.now(timezone.utc)

        # Order limiters
        self.orders_per_second = SlidingWindowLimiter(orders_per_second, 1.0)
        self.orders_per_minute = SlidingWindowLimiter(orders_per_minute, 60.0)

        # API limiters
        self.api_per_second = SlidingWindowLimiter(api_per_second, 1.0)
        self.quote_per_second = SlidingWindowLimiter(quote_per_second, 1.0)
        self.historical_per_second = SlidingWindowLimiter(historical_per_second, 1.0)

        # Daily limit (to be set by manager with Redis counter)
        self._daily_limit = orders_per_day
        self._daily_counter = None  # Set by manager

        # Statistics
        self._last_access = datetime.now(timezone.utc)
        self._requests_by_operation: Dict[str, int] = {}

    def _get_limiters_for_operation(self, operation: KiteOperation) -> list:
        """Get list of limiters to check for an operation."""
        if operation in [KiteOperation.ORDER_PLACE, KiteOperation.ORDER_MODIFY, KiteOperation.ORDER_CANCEL]:
            return [self.orders_per_second, self.orders_per_minute]
        elif operation == KiteOperation.API_GET:
            return [self.api_per_second]
        elif operation == KiteOperation.QUOTE:
            return [self.quote_per_second]
        elif operation == KiteOperation.HISTORICAL:
            return [self.historical_per_second]
        else:
            return [self.api_per_second]

    async def check_limit(self, operation: KiteOperation) -> Tuple[bool, float]:
        """
        Check if operation is allowed.

        Returns:
            Tuple of (allowed, max_wait_time)
        """
        limiters = self._get_limiters_for_operation(operation)
        max_wait = 0.0

        for limiter in limiters:
            allowed, wait_time = await limiter.check_limit()
            if not allowed:
                max_wait = max(max_wait, wait_time)

        return max_wait == 0, max_wait

    async def acquire(
        self,
        operation: KiteOperation,
        wait: bool = True,
        timeout: Optional[float] = None
    ) -> bool:
        """
        Acquire permission to execute operation.

        Args:
            operation: Type of Kite operation
            wait: Wait for permission if limit reached
            timeout: Maximum wait time

        Returns:
            True if permission granted

        Raises:
            RateLimitExceeded: If limit exceeded and wait=False
            DailyLimitExceeded: If daily order limit exceeded
        """
        self._last_access = datetime.now(timezone.utc)

        # Track by operation
        op_name = operation.value
        self._requests_by_operation[op_name] = self._requests_by_operation.get(op_name, 0) + 1

        # Check daily limit for order operations
        if operation == KiteOperation.ORDER_PLACE and self._daily_counter:
            daily_remaining = await self._daily_counter.get_remaining(self.trading_account_id)
            if daily_remaining <= 0:
                reset_at = await self._daily_counter.get_reset_time()
                raise DailyLimitExceeded(
                    trading_account_id=self.trading_account_id,
                    limit=self._daily_limit,
                    used=self._daily_limit,
                    reset_at=reset_at
                )

        # Acquire from all applicable limiters
        limiters = self._get_limiters_for_operation(operation)

        for limiter in limiters:
            await limiter.acquire(wait=wait, timeout=timeout)

        # Increment daily counter for order placements
        if operation == KiteOperation.ORDER_PLACE and self._daily_counter:
            await self._daily_counter.increment(self.trading_account_id)

        return True

    def get_stats(self) -> Dict[str, Any]:
        """Get account rate limiter statistics."""
        return {
            "trading_account_id": self.trading_account_id,
            "created_at": self.created_at.isoformat(),
            "last_access": self._last_access.isoformat(),
            "limits": {
                "orders_per_second": self.orders_per_second.get_stats(),
                "orders_per_minute": self.orders_per_minute.get_stats(),
                "api_per_second": self.api_per_second.get_stats(),
                "quote_per_second": self.quote_per_second.get_stats(),
                "historical_per_second": self.historical_per_second.get_stats(),
            },
            "requests_by_operation": self._requests_by_operation,
        }


class KiteAccountRateLimiterManager:
    """
    Manages rate limiters for all trading accounts.

    Features:
    - Lazy creation of per-account limiters
    - LRU eviction for inactive accounts
    - Redis integration for daily counters
    - Statistics aggregation
    """

    def __init__(
        self,
        daily_counter=None,
        max_cached_accounts: int = 1000,
        orders_per_second: int = 10,
        orders_per_minute: int = 200,
        orders_per_day: int = 3000,
        api_per_second: int = 10,
    ):
        """
        Initialize manager.

        Args:
            daily_counter: Redis daily counter instance
            max_cached_accounts: Max accounts to keep in memory
            orders_per_second: Default orders per second limit
            orders_per_minute: Default orders per minute limit
            orders_per_day: Default orders per day limit
            api_per_second: Default API requests per second limit
        """
        self._limiters: Dict[int, AccountRateLimiter] = {}
        self._access_order: Deque[int] = deque()
        self._lock = asyncio.Lock()

        self._daily_counter = daily_counter
        self._max_cached = max_cached_accounts

        # Default limits
        self._orders_per_second = orders_per_second
        self._orders_per_minute = orders_per_minute
        self._orders_per_day = orders_per_day
        self._api_per_second = api_per_second

        # Statistics
        self._total_requests = 0
        self._total_throttled = 0
        self._total_rejected = 0

        logger.info(
            f"KiteAccountRateLimiterManager initialized: "
            f"orders={orders_per_second}/s, {orders_per_minute}/min, {orders_per_day}/day"
        )

    async def _get_or_create_limiter(self, trading_account_id: int) -> AccountRateLimiter:
        """Get or create rate limiter for account."""
        async with self._lock:
            if trading_account_id in self._limiters:
                # Move to end of access order (LRU)
                try:
                    self._access_order.remove(trading_account_id)
                except ValueError:
                    pass
                self._access_order.append(trading_account_id)
                return self._limiters[trading_account_id]

            # Create new limiter
            limiter = AccountRateLimiter(
                trading_account_id=trading_account_id,
                orders_per_second=self._orders_per_second,
                orders_per_minute=self._orders_per_minute,
                orders_per_day=self._orders_per_day,
                api_per_second=self._api_per_second,
            )

            # Set daily counter
            limiter._daily_counter = self._daily_counter
            limiter._daily_limit = self._orders_per_day

            # Add to cache
            self._limiters[trading_account_id] = limiter
            self._access_order.append(trading_account_id)

            # Evict old entries if over limit
            while len(self._limiters) > self._max_cached:
                oldest = self._access_order.popleft()
                del self._limiters[oldest]
                logger.debug(f"Evicted rate limiter for account {oldest} (LRU)")

            logger.debug(f"Created rate limiter for account {trading_account_id}")
            return limiter

    async def check_limit(
        self,
        trading_account_id: int,
        operation: KiteOperation
    ) -> Tuple[bool, float]:
        """
        Check if operation is allowed for account.

        Args:
            trading_account_id: Trading account ID
            operation: Type of operation

        Returns:
            Tuple of (allowed, wait_time_seconds)
        """
        limiter = await self._get_or_create_limiter(trading_account_id)
        return await limiter.check_limit(operation)

    async def acquire(
        self,
        trading_account_id: int,
        operation: KiteOperation,
        wait: bool = True,
        timeout: Optional[float] = None
    ) -> bool:
        """
        Acquire permission to execute operation.

        Args:
            trading_account_id: Trading account ID
            operation: Type of Kite operation
            wait: Wait for permission if limit reached
            timeout: Maximum wait time

        Returns:
            True if permission granted
        """
        self._total_requests += 1
        limiter = await self._get_or_create_limiter(trading_account_id)

        try:
            # Check if we'll need to wait
            allowed, wait_time = await limiter.check_limit(operation)
            if not allowed and wait_time > 0:
                self._total_throttled += 1
                logger.info(
                    f"Rate limit throttling account {trading_account_id} "
                    f"for {operation.value}: waiting {wait_time:.2f}s",
                    extra={
                        "trading_account_id": trading_account_id,
                        "operation": operation.value,
                        "wait_time": wait_time,
                    }
                )

            return await limiter.acquire(operation, wait=wait, timeout=timeout)

        except (RateLimitExceeded, DailyLimitExceeded) as e:
            self._total_rejected += 1
            logger.warning(
                f"Rate limit rejected for account {trading_account_id}: {e}",
                extra={
                    "trading_account_id": trading_account_id,
                    "operation": operation.value,
                    "limit_type": e.limit_type,
                    "limit": e.limit,
                    "current": e.current,
                }
            )
            raise

    def get_account_stats(self, trading_account_id: int) -> Optional[Dict[str, Any]]:
        """Get statistics for a specific account."""
        if trading_account_id in self._limiters:
            return self._limiters[trading_account_id].get_stats()
        return None

    def get_all_stats(self) -> Dict[str, Any]:
        """Get aggregated statistics for all accounts."""
        return {
            "total_accounts_cached": len(self._limiters),
            "max_cached_accounts": self._max_cached,
            "total_requests": self._total_requests,
            "total_throttled": self._total_throttled,
            "total_rejected": self._total_rejected,
            "throttle_rate": (
                self._total_throttled / max(1, self._total_requests)
            ),
            "rejection_rate": (
                self._total_rejected / max(1, self._total_requests)
            ),
        }

    def set_daily_counter(self, daily_counter) -> None:
        """Set the daily counter for all limiters."""
        self._daily_counter = daily_counter
        for limiter in self._limiters.values():
            limiter._daily_counter = daily_counter


# Global instance
_rate_limiter_manager: Optional[KiteAccountRateLimiterManager] = None


def get_rate_limiter_manager_sync() -> Optional[KiteAccountRateLimiterManager]:
    """Get manager synchronously (may be None if not initialized)."""
    return _rate_limiter_manager


async def get_rate_limiter_manager() -> KiteAccountRateLimiterManager:
    """Get or create the global rate limiter manager."""
    global _rate_limiter_manager

    if _rate_limiter_manager is None:
        # Use default Kite limits
        _rate_limiter_manager = KiteAccountRateLimiterManager(
            daily_counter=None,
            orders_per_second=10,
            orders_per_minute=200,
            orders_per_day=3000,
            api_per_second=10,
        )

    return _rate_limiter_manager


async def init_rate_limiter_manager(daily_counter=None) -> KiteAccountRateLimiterManager:
    """Initialize the rate limiter manager with dependencies."""
    global _rate_limiter_manager

    manager = await get_rate_limiter_manager()
    if daily_counter:
        manager.set_daily_counter(daily_counter)

    logger.info("Kite account rate limiter manager initialized")
    return manager


async def shutdown_rate_limiter_manager() -> None:
    """Shutdown the rate limiter manager and clean up resources."""
    global _rate_limiter_manager

    if _rate_limiter_manager is not None:
        stats = _rate_limiter_manager.get_all_stats()
        logger.info(
            f"Rate limiter shutdown - total requests: {stats['total_requests']}, "
            f"throttled: {stats['total_throttled']}, rejected: {stats['total_rejected']}"
        )
        _rate_limiter_manager = None

    logger.info("Kite account rate limiter manager shutdown complete")

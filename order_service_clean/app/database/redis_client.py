"""
Redis Client for Caching and Rate Limiting

Provides async Redis connection for:
- Order caching
- Rate limiting
- Pub/sub for order updates
"""
import logging
import json
from typing import Optional, Any
import redis.asyncio as aioredis

from ..config.settings import settings

logger = logging.getLogger(__name__)

# Global Redis client
_redis_client: Optional[aioredis.Redis] = None


async def init_redis() -> None:
    """Initialize Redis connection"""
    global _redis_client

    if _redis_client is not None:
        logger.warning("Redis already initialized")
        return

    try:
        logger.info(f"Connecting to Redis: {settings.redis_url}")

        _redis_client = aioredis.from_url(
            settings.redis_url,
            decode_responses=True,
            encoding="utf-8",
            max_connections=20
        )

        # Test connection
        await _redis_client.ping()

        logger.info("Redis connection initialized successfully")

    except Exception as e:
        logger.error(f"Failed to initialize Redis: {e}")
        raise


async def close_redis() -> None:
    """Close Redis connection"""
    global _redis_client

    if _redis_client is None:
        logger.warning("Redis not initialized")
        return

    logger.info("Closing Redis connection...")
    await _redis_client.close()
    _redis_client = None
    logger.info("Redis connection closed")


def get_redis() -> aioredis.Redis:
    """
    Get Redis client.

    Returns:
        Redis client instance

    Raises:
        RuntimeError: If Redis not initialized
    """
    if _redis_client is None:
        raise RuntimeError("Redis not initialized. Call init_redis() first.")

    return _redis_client


async def get_redis_health() -> dict:
    """
    Check Redis health.

    Returns:
        dict: Health status
    """
    if _redis_client is None:
        return {
            "status": "unhealthy",
            "error": "Redis not initialized"
        }

    try:
        await _redis_client.ping()

        # Get Redis info
        info = await _redis_client.info()

        return {
            "status": "healthy",
            "redis_version": info.get("redis_version"),
            "used_memory_human": info.get("used_memory_human"),
            "connected_clients": info.get("connected_clients"),
        }

    except Exception as e:
        logger.error(f"Redis health check failed: {e}")
        return {
            "status": "unhealthy",
            "error": str(e)
        }


# =========================================
# ORDER CACHING UTILITIES
# =========================================

async def cache_order(order_id: str, order_data: dict, ttl: int = None) -> None:
    """
    Cache order data in Redis.

    Args:
        order_id: Order ID
        order_data: Order data dictionary
        ttl: Time-to-live in seconds (default from settings)
    """
    redis = get_redis()
    ttl = ttl or settings.redis_order_ttl

    key = f"order:{order_id}"

    try:
        await redis.setex(
            key,
            ttl,
            json.dumps(order_data)
        )
        logger.debug(f"Cached order {order_id}")

    except Exception as e:
        logger.error(f"Failed to cache order {order_id}: {e}")


async def get_cached_order(order_id: str) -> Optional[dict]:
    """
    Get cached order data.

    Args:
        order_id: Order ID

    Returns:
        Order data dict or None if not found
    """
    redis = get_redis()
    key = f"order:{order_id}"

    try:
        data = await redis.get(key)

        if data:
            return json.loads(data)

        return None

    except Exception as e:
        logger.error(f"Failed to get cached order {order_id}: {e}")
        return None


async def invalidate_order_cache(order_id: str) -> None:
    """
    Invalidate (delete) cached order.

    Args:
        order_id: Order ID
    """
    redis = get_redis()
    key = f"order:{order_id}"

    try:
        await redis.delete(key)
        logger.debug(f"Invalidated cache for order {order_id}")

    except Exception as e:
        logger.error(f"Failed to invalidate cache for order {order_id}: {e}")


# =========================================
# RATE LIMITING UTILITIES
# =========================================

async def check_rate_limit(
    user_id: int,
    endpoint: str,
    limit: int,
    window_seconds: int
) -> tuple[bool, int]:
    """
    Check if user has exceeded rate limit.

    Args:
        user_id: User ID
        endpoint: API endpoint name
        limit: Maximum requests allowed
        window_seconds: Time window in seconds

    Returns:
        Tuple of (allowed: bool, remaining: int)
    """
    redis = get_redis()
    key = f"rate_limit:{endpoint}:{user_id}"

    try:
        # Increment counter
        count = await redis.incr(key)

        # Set expiry on first request
        if count == 1:
            await redis.expire(key, window_seconds)

        # Check if within limit
        allowed = count <= limit
        remaining = max(0, limit - count)

        return allowed, remaining

    except Exception as e:
        logger.error(f"Rate limit check failed: {e}")
        # SECURITY: Fail-closed - deny requests when rate limiting unavailable
        # This prevents order flooding if Redis is down
        return False, 0


# =========================================
# PUB/SUB FOR ORDER UPDATES
# =========================================

async def publish_order_update(order_id: str, event_type: str, data: dict) -> None:
    """
    Publish order update event to Redis pub/sub.

    Args:
        order_id: Order ID
        event_type: Event type (created, updated, filled, cancelled, etc.)
        data: Event data
    """
    redis = get_redis()
    channel = f"order_updates:{order_id}"

    message = {
        "order_id": order_id,
        "event_type": event_type,
        "data": data,
        "timestamp": data.get("timestamp")
    }

    try:
        await redis.publish(channel, json.dumps(message))
        logger.debug(f"Published {event_type} event for order {order_id}")

    except Exception as e:
        logger.error(f"Failed to publish order update: {e}")


# =========================================
# POSITION CACHING UTILITIES
# =========================================

async def cache_position(position_id: str, position_data: dict, ttl: int = None) -> None:
    """
    Cache position data in Redis.

    Args:
        position_id: Position ID
        position_data: Position data dictionary
        ttl: Time-to-live in seconds (default from settings)
    """
    redis = get_redis()
    ttl = ttl or settings.redis_order_ttl

    key = f"position:{position_id}"

    try:
        await redis.setex(
            key,
            ttl,
            json.dumps(position_data)
        )
        logger.debug(f"Cached position {position_id}")

    except Exception as e:
        logger.error(f"Failed to cache position {position_id}: {e}")


async def get_cached_position(position_id: str) -> Optional[dict]:
    """
    Get cached position data.

    Args:
        position_id: Position ID

    Returns:
        Position data dict or None if not found
    """
    redis = get_redis()
    key = f"position:{position_id}"

    try:
        data = await redis.get(key)

        if data:
            return json.loads(data)

        return None

    except Exception as e:
        logger.error(f"Failed to get cached position {position_id}: {e}")
        return None


async def invalidate_position_cache(cache_key: str) -> None:
    """
    Invalidate (delete) cached position(s).

    Args:
        cache_key: Cache key pattern (e.g., 'position:123' or 'user:1')
    """
    redis = get_redis()

    try:
        if '*' in cache_key:
            # Pattern-based deletion
            keys = await redis.keys(cache_key)
            if keys:
                await redis.delete(*keys)
                logger.debug(f"Invalidated {len(keys)} position caches matching {cache_key}")
        else:
            # Single key deletion
            key = f"position:{cache_key}"
            await redis.delete(key)
            logger.debug(f"Invalidated cache for position {cache_key}")

    except Exception as e:
        logger.error(f"Failed to invalidate cache for {cache_key}: {e}")


# =========================================
# TRADE CACHING UTILITIES
# =========================================

async def cache_trade(trade_id: str, trade_data: dict, ttl: int = None) -> None:
    """
    Cache trade data in Redis.

    Args:
        trade_id: Trade ID
        trade_data: Trade data dictionary
        ttl: Time-to-live in seconds (default from settings)
    """
    redis = get_redis()
    ttl = ttl or settings.redis_order_ttl

    key = f"trade:{trade_id}"

    try:
        await redis.setex(
            key,
            ttl,
            json.dumps(trade_data)
        )
        logger.debug(f"Cached trade {trade_id}")

    except Exception as e:
        logger.error(f"Failed to cache trade {trade_id}: {e}")


async def get_cached_trade(trade_id: str) -> Optional[dict]:
    """
    Get cached trade data.

    Args:
        trade_id: Trade ID

    Returns:
        Trade data dict or None if not found
    """
    redis = get_redis()
    key = f"trade:{trade_id}"

    try:
        data = await redis.get(key)

        if data:
            return json.loads(data)

        return None

    except Exception as e:
        logger.error(f"Failed to get cached trade {trade_id}: {e}")
        return None


async def invalidate_trade_cache(cache_key: str) -> None:
    """
    Invalidate (delete) cached trade(s).

    Args:
        cache_key: Cache key pattern (e.g., 'trade:123' or 'user:1')
    """
    redis = get_redis()

    try:
        if '*' in cache_key:
            # Pattern-based deletion
            keys = await redis.keys(cache_key)
            if keys:
                await redis.delete(*keys)
                logger.debug(f"Invalidated {len(keys)} trade caches matching {cache_key}")
        else:
            # Single key deletion
            key = f"trade:{cache_key}"
            await redis.delete(key)
            logger.debug(f"Invalidated cache for trade {cache_key}")

    except Exception as e:
        logger.error(f"Failed to invalidate cache for {cache_key}: {e}")


# =========================================
# DASHBOARD CACHING UTILITIES (Issue #419)
# =========================================

async def cache_dashboard_overview(trading_account_id: int, data: dict, ttl: int = 300) -> None:
    """
    Cache dashboard overview data in Redis.

    Args:
        trading_account_id: Trading account ID
        data: Dashboard overview data
        ttl: Time-to-live in seconds (default: 300 = 5 minutes)
    """
    redis = get_redis()
    key = f"dashboard:{trading_account_id}"

    try:
        await redis.setex(
            key,
            ttl,
            json.dumps(data, default=str)
        )
        logger.debug(f"Cached dashboard overview for account {trading_account_id}")
    except Exception as e:
        logger.error(f"Failed to cache dashboard for account {trading_account_id}: {e}")


async def get_cached_dashboard_overview(trading_account_id: int) -> Optional[dict]:
    """
    Get cached dashboard overview data.

    Args:
        trading_account_id: Trading account ID

    Returns:
        Dashboard data dict or None if not found/expired
    """
    redis = get_redis()
    key = f"dashboard:{trading_account_id}"

    try:
        data = await redis.get(key)
        if data:
            logger.debug(f"Cache HIT for dashboard account {trading_account_id}")
            return json.loads(data)
        logger.debug(f"Cache MISS for dashboard account {trading_account_id}")
        return None
    except Exception as e:
        logger.error(f"Failed to get cached dashboard for account {trading_account_id}: {e}")
        return None


async def invalidate_dashboard_cache(trading_account_id: int) -> None:
    """
    Invalidate dashboard cache when positions or orders change.

    Args:
        trading_account_id: Trading account ID
    """
    redis = get_redis()
    key = f"dashboard:{trading_account_id}"

    try:
        await redis.delete(key)
        logger.debug(f"Invalidated dashboard cache for account {trading_account_id}")
    except Exception as e:
        logger.error(f"Failed to invalidate dashboard cache for account {trading_account_id}: {e}")


async def cache_positions_summary(trading_account_id: int, data: dict, ttl: int = 300) -> None:
    """
    Cache positions summary data.

    Args:
        trading_account_id: Trading account ID
        data: Positions summary data
        ttl: Time-to-live in seconds (default: 300 = 5 minutes)
    """
    redis = get_redis()
    key = f"positions:summary:{trading_account_id}"

    try:
        await redis.setex(
            key,
            ttl,
            json.dumps(data, default=str)
        )
        logger.debug(f"Cached positions summary for account {trading_account_id}")
    except Exception as e:
        logger.error(f"Failed to cache positions summary for account {trading_account_id}: {e}")


async def get_cached_positions_summary(trading_account_id: int) -> Optional[dict]:
    """
    Get cached positions summary data.

    Args:
        trading_account_id: Trading account ID

    Returns:
        Positions summary dict or None if not found/expired
    """
    redis = get_redis()
    key = f"positions:summary:{trading_account_id}"

    try:
        data = await redis.get(key)
        if data:
            logger.debug(f"Cache HIT for positions summary account {trading_account_id}")
            return json.loads(data)
        logger.debug(f"Cache MISS for positions summary account {trading_account_id}")
        return None
    except Exception as e:
        logger.error(f"Failed to get cached positions summary for account {trading_account_id}: {e}")
        return None


async def invalidate_positions_summary_cache(trading_account_id: int) -> None:
    """
    Invalidate positions summary cache.

    Args:
        trading_account_id: Trading account ID
    """
    redis = get_redis()
    key = f"positions:summary:{trading_account_id}"

    try:
        await redis.delete(key)
        logger.debug(f"Invalidated positions summary cache for account {trading_account_id}")
    except Exception as e:
        logger.error(f"Failed to invalidate positions summary cache for account {trading_account_id}: {e}")

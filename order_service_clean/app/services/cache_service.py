"""
Secure Redis Cache Service for Config Service

Provides encrypted caching for secrets to reduce database load.

Security:
- All cached data is encrypted with Fernet (AES-128-CBC + HMAC-SHA256)
- Encryption key from environment (CACHE_ENCRYPTION_KEY)
- TTL: 300 seconds (5 minutes)
- Automatic invalidation on secret updates/deletes

Architecture Compliance:
- Based on common/service_template pattern
- Metrics tracking (hits, misses, errors)
- Fail-open: Database fallback if cache unavailable
"""

import json
import logging
from typing import Optional, Any, Dict

import redis
from redis.exceptions import RedisError
from cryptography.fernet import Fernet

logger = logging.getLogger(__name__)


class SecureCacheService:
    """
    Secure caching service with encryption for sensitive data.

    Features:
    - Automatic encryption/decryption of all cached values
    - TTL support
    - Hit/miss/error metrics
    - Pattern-based invalidation
    """

    def __init__(
        self,
        redis_client: Optional[redis.Redis] = None,
        encryption_key: Optional[bytes] = None,
        cache_ttl: int = 300,
        enable_cache: bool = True
    ):
        """
        Initialize secure cache service.

        Args:
            redis_client: Redis client (if None, creates new connection)
            encryption_key: Fernet encryption key (required for production)
            cache_ttl: Default TTL in seconds (default: 300s = 5min)
            enable_cache: Whether caching is enabled
        """
        self.cache_ttl = cache_ttl
        self.enable_cache = enable_cache
        self._redis: Optional[redis.Redis] = redis_client

        # Initialize encryption
        if encryption_key:
            try:
                self.cipher = Fernet(encryption_key)
                logger.info("Secure cache service initialized with encryption")
            except Exception as e:
                logger.error(f"Invalid encryption key: {e}")
                self.cipher = None
                self.enable_cache = False
        else:
            logger.warning("No encryption key provided - caching disabled for security")
            self.cipher = None
            self.enable_cache = False

        # Metrics
        self.cache_hits = 0
        self.cache_misses = 0
        self.cache_errors = 0

    def _get_redis(self) -> Optional[redis.Redis]:
        """Get or create Redis connection"""
        if not self.enable_cache:
            return None

        if self._redis is None:
            try:
                # Use order service config-compliant settings for Redis URL
                try:
                    # Try to import order service settings (config-service compliant)
                    import sys
                    import os
                    # Check if we're in order service context
                    if 'app.config.settings' in sys.modules or any('order_service' in path for path in sys.path):
                        from ..config.settings import settings
                        redis_url = settings.redis_url
                    else:
                        # Fallback for standalone usage (config service itself)
                        redis_url = os.getenv("REDIS_URL", "redis://localhost:8202")
                except ImportError:
                    # Fallback for standalone usage (config service itself)
                    import os
                    redis_url = os.getenv("REDIS_URL", "redis://localhost:8202")

                self._redis = redis.from_url(
                    redis_url,
                    decode_responses=False,  # Binary mode for encryption
                    socket_connect_timeout=2,
                    socket_timeout=2
                )

                # Test connection
                self._redis.ping()
                logger.info(f"Cache service connected to Redis: {redis_url}")

            except Exception as e:
                logger.error(f"Failed to connect to Redis for cache: {e}")
                self.enable_cache = False
                return None

        return self._redis

    def _encrypt(self, plaintext: str) -> bytes:
        """Encrypt plaintext to bytes"""
        if not self.cipher:
            raise RuntimeError("Encryption not initialized")
        return self.cipher.encrypt(plaintext.encode())

    def _decrypt(self, ciphertext: bytes) -> str:
        """Decrypt bytes to plaintext"""
        if not self.cipher:
            raise RuntimeError("Decryption not initialized")
        return self.cipher.decrypt(ciphertext).decode()

    def get(self, key: str) -> Optional[Any]:
        """
        Get value from cache with automatic decryption.

        Args:
            key: Cache key

        Returns:
            Cached value (decrypted) or None if not found
        """
        if not self.enable_cache:
            self.cache_misses += 1
            return None

        try:
            r = self._get_redis()
            if not r:
                self.cache_errors += 1
                return None

            encrypted_data = r.get(key)
            if encrypted_data:
                # Decrypt and deserialize
                decrypted_json = self._decrypt(encrypted_data)
                value = json.loads(decrypted_json)

                self.cache_hits += 1
                logger.debug(f"Cache HIT: {key}")
                return value

            self.cache_misses += 1
            logger.debug(f"Cache MISS: {key}")
            return None

        except RedisError as e:
            self.cache_errors += 1
            logger.warning(f"Redis cache read error for key {key}: {e}")
            return None
        except Exception as e:
            self.cache_errors += 1
            logger.warning(f"Cache decryption error for key {key}: {e}")
            return None

    def set(
        self,
        key: str,
        value: Any,
        ttl: Optional[int] = None
    ) -> bool:
        """
        Set value in cache with automatic encryption.

        Args:
            key: Cache key
            value: Value to cache (will be JSON serialized + encrypted)
            ttl: TTL in seconds (defaults to self.cache_ttl)

        Returns:
            True if cached successfully, False otherwise
        """
        if not self.enable_cache:
            return False

        try:
            r = self._get_redis()
            if not r:
                return False

            # Serialize and encrypt
            json_data = json.dumps(value)
            encrypted_data = self._encrypt(json_data)

            # Store with TTL
            ttl = ttl or self.cache_ttl
            r.setex(key, ttl, encrypted_data)

            logger.debug(f"Cache SET: {key} (TTL: {ttl}s)")
            return True

        except RedisError as e:
            self.cache_errors += 1
            logger.warning(f"Redis cache write error for key {key}: {e}")
            return False
        except Exception as e:
            self.cache_errors += 1
            logger.warning(f"Cache encryption error for key {key}: {e}")
            return False

    def delete(self, key: str) -> bool:
        """
        Delete key from cache.

        Args:
            key: Cache key to delete

        Returns:
            True if deleted, False otherwise
        """
        if not self.enable_cache:
            return False

        try:
            r = self._get_redis()
            if not r:
                return False

            r.delete(key)
            logger.debug(f"Cache DELETE: {key}")
            return True

        except RedisError as e:
            self.cache_errors += 1
            logger.warning(f"Redis cache delete error for key {key}: {e}")
            return False

    def invalidate_pattern(self, pattern: str) -> int:
        """
        Invalidate all keys matching pattern.

        Args:
            pattern: Redis key pattern (e.g., "secret:prod:*")

        Returns:
            Number of keys deleted
        """
        if not self.enable_cache:
            return 0

        try:
            r = self._get_redis()
            if not r:
                return 0

            deleted = 0
            cursor = 0

            while True:
                cursor, keys = r.scan(cursor, match=pattern, count=100)
                if keys:
                    deleted += r.delete(*keys)

                if cursor == 0:
                    break

            logger.info(f"Cache invalidated pattern '{pattern}': {deleted} keys deleted")
            return deleted

        except RedisError as e:
            self.cache_errors += 1
            logger.warning(f"Redis cache invalidate error for pattern {pattern}: {e}")
            return 0

    def get_stats(self) -> Dict[str, Any]:
        """
        Get cache statistics.

        Returns:
            Dict with cache metrics
        """
        total = self.cache_hits + self.cache_misses
        hit_rate = (self.cache_hits / total * 100) if total > 0 else 0

        return {
            "cache_enabled": self.enable_cache,
            "cache_hits": self.cache_hits,
            "cache_misses": self.cache_misses,
            "cache_errors": self.cache_errors,
            "total_requests": total,
            "hit_rate_percent": round(hit_rate, 2),
            "encryption_enabled": self.cipher is not None
        }


# Global cache instance (singleton pattern)
_cache_service: Optional[SecureCacheService] = None


def get_cache_service(
    redis_client: Optional[redis.Redis] = None,
    encryption_key: Optional[bytes] = None
) -> SecureCacheService:
    """
    Get global cache service instance (singleton).

    Args:
        redis_client: Redis client (optional, only used on first call)
        encryption_key: Encryption key (optional, only used on first call)

    Returns:
        SecureCacheService instance
    """
    global _cache_service

    if _cache_service is None:
        # Get encryption key from config service if not provided
        if encryption_key is None:
            try:
                # Try to use order service config-compliant settings
                import sys
                import os
                # Check if we're in order service context
                if 'app.config.settings' in sys.modules or any('order_service' in path for path in sys.path):
                    from ..config.settings import settings
                    key_str = settings.cache_encryption_key
                    if key_str:
                        encryption_key = key_str.encode()
                    else:
                        logger.warning("CACHE_ENCRYPTION_KEY not set in config service - caching disabled")
                else:
                    # Fallback for standalone usage (config service itself)
                    key_b64 = os.getenv("CACHE_ENCRYPTION_KEY")
                    if key_b64:
                        encryption_key = key_b64.encode()
                    else:
                        logger.warning("CACHE_ENCRYPTION_KEY not set - caching disabled")
            except ImportError:
                # Fallback for standalone usage (config service itself)
                import os
                key_b64 = os.getenv("CACHE_ENCRYPTION_KEY")
                if key_b64:
                    encryption_key = key_b64.encode()
                else:
                    logger.warning("CACHE_ENCRYPTION_KEY not set - caching disabled")

        _cache_service = SecureCacheService(
            redis_client=redis_client,
            encryption_key=encryption_key
        )

    return _cache_service

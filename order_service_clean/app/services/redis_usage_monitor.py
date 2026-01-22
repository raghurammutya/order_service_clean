"""
Redis Usage Monitor

Monitors Redis usage patterns, connection health, and detects saturation.
Provides metrics and alerts for Redis data plane monitoring.
"""

import logging
import asyncio
from typing import Dict, Any, List, Optional
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import time
from enum import Enum

from ..database.redis_client import get_redis

logger = logging.getLogger(__name__)


class RedisUsagePattern(str, Enum):
    """Redis usage patterns in order service"""
    IDEMPOTENCY = "idempotency"           # Duplicate order protection
    RATE_LIMITING = "rate_limiting"       # API rate limits
    CACHING = "caching"                   # Performance cache
    SESSION_STORAGE = "session_storage"   # User sessions
    REAL_TIME_DATA = "real_time_data"     # Live market data
    WORKER_COORDINATION = "worker_coordination"  # Background job coordination


@dataclass
class RedisUsageMetrics:
    """Metrics for a specific Redis usage pattern"""
    pattern: RedisUsagePattern
    key_prefix: str
    operation_counts: Dict[str, int] = field(default_factory=dict)  # GET, SET, DEL, etc.
    total_operations: int = 0
    avg_latency_ms: float = 0.0
    error_count: int = 0
    memory_usage_bytes: int = 0
    key_count: int = 0
    expiration_usage: int = 0  # Keys with TTL
    last_updated: datetime = field(default_factory=datetime.now)


@dataclass
class RedisHealthStatus:
    """Overall Redis health and saturation status"""
    is_healthy: bool = True
    connection_pool_usage: float = 0.0  # Percentage of pool used
    memory_usage_mb: float = 0.0
    memory_limit_mb: float = 0.0
    memory_usage_percentage: float = 0.0
    total_operations_per_second: float = 0.0
    slow_queries_count: int = 0
    warning_messages: List[str] = field(default_factory=list)
    critical_messages: List[str] = field(default_factory=list)


class RedisUsageMonitor:
    """
    Monitors Redis usage patterns and detects saturation.
    
    Tracks different usage categories to ensure Redis data plane
    is properly partitioned and not overwhelming any single function.
    """

    def __init__(self):
        self.usage_patterns: Dict[RedisUsagePattern, RedisUsageMetrics] = {}
        self.monitoring_interval = 60  # seconds
        self.last_health_check = datetime.now()
        self._monitoring_task: Optional[asyncio.Task] = None
        self._redis_client = None
        
        # Saturation thresholds
        self.memory_warning_threshold = 0.80  # 80%
        self.memory_critical_threshold = 0.95  # 95%
        self.connection_pool_warning_threshold = 0.75  # 75%
        self.latency_warning_threshold_ms = 100  # 100ms
        self.ops_per_second_warning = 10000  # 10k ops/sec

    async def start_monitoring(self):
        """Start background monitoring of Redis usage"""
        if self._monitoring_task and not self._monitoring_task.done():
            return
        
        logger.info("Starting Redis usage monitoring")
        self._monitoring_task = asyncio.create_task(self._monitoring_loop())

    async def stop_monitoring(self):
        """Stop background monitoring"""
        if self._monitoring_task and not self._monitoring_task.done():
            self._monitoring_task.cancel()
            try:
                await self._monitoring_task
            except asyncio.CancelledError:
                pass
        logger.info("Stopped Redis usage monitoring")

    async def _monitoring_loop(self):
        """Background monitoring loop"""
        try:
            while True:
                try:
                    await self._collect_usage_metrics()
                    health_status = await self.get_health_status()
                    
                    # Log warnings and critical issues
                    for warning in health_status.warning_messages:
                        logger.warning(f"Redis usage warning: {warning}")
                    
                    for critical in health_status.critical_messages:
                        logger.critical(f"Redis saturation critical: {critical}")
                    
                    # Update last check time
                    self.last_health_check = datetime.now()
                    
                except Exception as e:
                    logger.error(f"Error in Redis monitoring loop: {e}")
                
                await asyncio.sleep(self.monitoring_interval)
                
        except asyncio.CancelledError:
            logger.info("Redis monitoring cancelled")
            raise

    async def _get_redis_client(self):
        """Get Redis client for monitoring"""
        if self._redis_client is None:
            self._redis_client = get_redis()
        return self._redis_client

    async def _collect_usage_metrics(self):
        """Collect Redis usage metrics by pattern"""
        try:
            redis_client = await self._get_redis_client()
            
            # Define key patterns for each usage type
            pattern_prefixes = {
                RedisUsagePattern.IDEMPOTENCY: "idempotency:",
                RedisUsagePattern.RATE_LIMITING: "rate_limit:",
                RedisUsagePattern.CACHING: "cache:",
                RedisUsagePattern.SESSION_STORAGE: "session:",
                RedisUsagePattern.REAL_TIME_DATA: "ticker:",
                RedisUsagePattern.WORKER_COORDINATION: "worker:",
            }
            
            for pattern, prefix in pattern_prefixes.items():
                await self._collect_pattern_metrics(redis_client, pattern, prefix)
                
        except Exception as e:
            logger.error(f"Error collecting Redis usage metrics: {e}")

    async def _collect_pattern_metrics(self, redis_client, pattern: RedisUsagePattern, prefix: str):
        """Collect metrics for a specific usage pattern"""
        try:
            # Get keys matching pattern
            keys = await redis_client.keys(f"{prefix}*")
            
            # Calculate memory usage for this pattern
            memory_usage = 0
            expiration_count = 0
            
            if keys:
                # Sample subset for performance (max 100 keys)
                sample_keys = keys[:100] if len(keys) > 100 else keys
                
                for key in sample_keys:
                    try:
                        # Get memory usage
                        memory_usage += await redis_client.memory_usage(key) or 0
                        
                        # Check if key has expiration
                        ttl = await redis_client.ttl(key)
                        if ttl > 0:
                            expiration_count += 1
                    except Exception:
                        # Skip keys that might have been deleted
                        continue
            
            # Update metrics
            if pattern not in self.usage_patterns:
                self.usage_patterns[pattern] = RedisUsageMetrics(
                    pattern=pattern,
                    key_prefix=prefix
                )
            
            metrics = self.usage_patterns[pattern]
            metrics.key_count = len(keys)
            metrics.memory_usage_bytes = memory_usage
            metrics.expiration_usage = expiration_count
            metrics.last_updated = datetime.now()
            
        except Exception as e:
            logger.error(f"Error collecting metrics for pattern {pattern}: {e}")

    async def get_health_status(self) -> RedisHealthStatus:
        """Get comprehensive Redis health status"""
        try:
            redis_client = await self._get_redis_client()
            
            # Get Redis INFO
            info = await redis_client.info()
            
            # Calculate health metrics
            used_memory = info.get('used_memory', 0)
            max_memory = info.get('maxmemory', 0)
            
            if max_memory == 0:
                # If no memory limit set, use a reasonable estimate
                max_memory = used_memory * 10  # Assume current usage is 10% of available
            
            memory_usage_percentage = (used_memory / max_memory) * 100 if max_memory > 0 else 0
            
            # Connection pool metrics
            connected_clients = info.get('connected_clients', 0)
            max_clients = info.get('maxclients', 1000)  # Redis default
            connection_pool_usage = (connected_clients / max_clients) * 100
            
            # Operations per second
            total_commands_processed = info.get('total_commands_processed', 0)
            ops_per_second = 0  # Would need historical data to calculate
            
            # Slow query count
            slow_queries = info.get('slowlog_len', 0)
            
            # Build health status
            health_status = RedisHealthStatus(
                memory_usage_mb=used_memory / (1024 * 1024),
                memory_limit_mb=max_memory / (1024 * 1024),
                memory_usage_percentage=memory_usage_percentage,
                connection_pool_usage=connection_pool_usage,
                total_operations_per_second=ops_per_second,
                slow_queries_count=slow_queries
            )
            
            # Evaluate health conditions
            await self._evaluate_health_conditions(health_status)
            
            return health_status
            
        except Exception as e:
            logger.error(f"Error getting Redis health status: {e}")
            return RedisHealthStatus(
                is_healthy=False,
                critical_messages=[f"Failed to get Redis health: {e}"]
            )

    async def _evaluate_health_conditions(self, health_status: RedisHealthStatus):
        """Evaluate Redis health conditions and set warnings/critical alerts"""
        
        # Memory usage checks
        if health_status.memory_usage_percentage >= self.memory_critical_threshold * 100:
            health_status.is_healthy = False
            health_status.critical_messages.append(
                f"Redis memory usage critical: {health_status.memory_usage_percentage:.1f}% "
                f"(limit: {self.memory_critical_threshold * 100}%)"
            )
        elif health_status.memory_usage_percentage >= self.memory_warning_threshold * 100:
            health_status.warning_messages.append(
                f"Redis memory usage high: {health_status.memory_usage_percentage:.1f}% "
                f"(warning: {self.memory_warning_threshold * 100}%)"
            )
        
        # Connection pool checks
        if health_status.connection_pool_usage >= self.connection_pool_warning_threshold * 100:
            health_status.warning_messages.append(
                f"Redis connection pool usage high: {health_status.connection_pool_usage:.1f}% "
                f"(warning: {self.connection_pool_warning_threshold * 100}%)"
            )
        
        # Slow queries check
        if health_status.slow_queries_count > 10:
            health_status.warning_messages.append(
                f"Redis slow queries detected: {health_status.slow_queries_count} queries"
            )
        
        # Check individual pattern health
        for pattern, metrics in self.usage_patterns.items():
            if metrics.error_count > 0:
                health_status.warning_messages.append(
                    f"Redis errors in {pattern.value}: {metrics.error_count} errors"
                )

    def get_usage_summary(self) -> Dict[str, Any]:
        """Get usage summary across all patterns"""
        total_keys = sum(m.key_count for m in self.usage_patterns.values())
        total_memory = sum(m.memory_usage_bytes for m in self.usage_patterns.values())
        total_errors = sum(m.error_count for m in self.usage_patterns.values())
        
        pattern_breakdown = {}
        for pattern, metrics in self.usage_patterns.items():
            pattern_breakdown[pattern.value] = {
                "key_count": metrics.key_count,
                "memory_usage_mb": metrics.memory_usage_bytes / (1024 * 1024),
                "error_count": metrics.error_count,
                "keys_with_expiration": metrics.expiration_usage,
                "last_updated": metrics.last_updated.isoformat()
            }
        
        return {
            "total_keys": total_keys,
            "total_memory_mb": total_memory / (1024 * 1024),
            "total_errors": total_errors,
            "patterns": pattern_breakdown,
            "last_health_check": self.last_health_check.isoformat()
        }

    async def record_operation(
        self, 
        pattern: RedisUsagePattern, 
        operation: str, 
        latency_ms: float = 0.0,
        error: bool = False
    ):
        """Record a Redis operation for monitoring"""
        if pattern not in self.usage_patterns:
            self.usage_patterns[pattern] = RedisUsageMetrics(
                pattern=pattern,
                key_prefix=f"{pattern.value}:"
            )
        
        metrics = self.usage_patterns[pattern]
        metrics.total_operations += 1
        
        if operation not in metrics.operation_counts:
            metrics.operation_counts[operation] = 0
        metrics.operation_counts[operation] += 1
        
        if latency_ms > 0:
            # Update average latency (simple moving average)
            if metrics.avg_latency_ms == 0:
                metrics.avg_latency_ms = latency_ms
            else:
                metrics.avg_latency_ms = (metrics.avg_latency_ms + latency_ms) / 2
        
        if error:
            metrics.error_count += 1


# Global monitor instance
_redis_monitor: Optional[RedisUsageMonitor] = None


def get_redis_monitor() -> RedisUsageMonitor:
    """Get the global Redis usage monitor"""
    global _redis_monitor
    if _redis_monitor is None:
        _redis_monitor = RedisUsageMonitor()
    return _redis_monitor


async def start_redis_monitoring():
    """Start Redis usage monitoring"""
    monitor = get_redis_monitor()
    await monitor.start_monitoring()


async def stop_redis_monitoring():
    """Stop Redis usage monitoring"""
    global _redis_monitor
    if _redis_monitor:
        await _redis_monitor.stop_monitoring()


async def get_redis_health_summary() -> Dict[str, Any]:
    """Get comprehensive Redis health summary for monitoring endpoints"""
    monitor = get_redis_monitor()
    
    health_status = await monitor.get_health_status()
    usage_summary = monitor.get_usage_summary()
    
    return {
        "health": {
            "is_healthy": health_status.is_healthy,
            "memory_usage_mb": health_status.memory_usage_mb,
            "memory_limit_mb": health_status.memory_limit_mb,
            "memory_usage_percentage": health_status.memory_usage_percentage,
            "connection_pool_usage": health_status.connection_pool_usage,
            "slow_queries": health_status.slow_queries_count,
            "warnings": health_status.warning_messages,
            "critical_issues": health_status.critical_messages
        },
        "usage": usage_summary
    }
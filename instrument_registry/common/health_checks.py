"""
Real Health Check Implementation

Provides actual database and Redis connectivity checks
for production readiness validation.
"""
import asyncio
import logging
import time
from typing import Dict, Optional, Tuple, Any
from dataclasses import dataclass
import asyncpg
import redis.asyncio as redis
from contextlib import asynccontextmanager

logger = logging.getLogger(__name__)


@dataclass
class HealthStatus:
    """Health status data class"""
    service_name: str
    is_healthy: bool
    details: Dict[str, Any]


class DatabaseHealthCheck:
    """PostgreSQL database health checker"""
    
    def __init__(self, database_url: str, timeout: float = 10.0):
        self.database_url = database_url
        self.timeout = timeout
        self._last_check_time = 0
        self._last_check_result = None
        self._cache_duration = 30  # Cache health check for 30 seconds
    
    async def check_health(self) -> Tuple[bool, str, Optional[float]]:
        """
        Check database health with connection test
        
        Returns:
            (is_healthy, message, response_time_seconds)
        """
        # Return cached result if recent
        now = time.time()
        if (now - self._last_check_time) < self._cache_duration and self._last_check_result:
            return self._last_check_result
        
        start_time = time.time()
        
        try:
            # Test connection with timeout
            conn = await asyncio.wait_for(
                asyncpg.connect(self.database_url),
                timeout=self.timeout
            )
            
            try:
                # Execute simple query
                result = await asyncio.wait_for(
                    conn.fetchval("SELECT 1 as health_check"),
                    timeout=self.timeout
                )
                
                if result == 1:
                    response_time = time.time() - start_time
                    self._last_check_result = (True, "Database connection healthy", response_time)
                    self._last_check_time = now
                    return self._last_check_result
                else:
                    response_time = time.time() - start_time
                    self._last_check_result = (False, "Database query returned unexpected result", response_time)
                    self._last_check_time = now
                    return self._last_check_result
                    
            finally:
                await conn.close()
                
        except asyncio.TimeoutError:
            response_time = time.time() - start_time
            message = f"Database connection timeout after {self.timeout}s"
            logger.error(message)
            self._last_check_result = (False, message, response_time)
            self._last_check_time = now
            return self._last_check_result
            
        except Exception as e:
            response_time = time.time() - start_time
            message = f"Database connection error: {str(e)}"
            logger.error(message)
            self._last_check_result = (False, message, response_time)
            self._last_check_time = now
            return self._last_check_result
    
    async def check_schema_access(self, schema: str = "instrument_registry") -> Tuple[bool, str]:
        """
        Check if we can access the specific schema
        
        Args:
            schema: Schema name to verify access to
            
        Returns:
            (can_access, message)
        """
        try:
            conn = await asyncio.wait_for(
                asyncpg.connect(self.database_url),
                timeout=self.timeout
            )
            
            try:
                # Check if schema exists
                schema_exists = await conn.fetchval("""
                    SELECT EXISTS (
                        SELECT 1 FROM information_schema.schemata 
                        WHERE schema_name = $1
                    )
                """, schema)
                
                if not schema_exists:
                    return False, f"Schema '{schema}' does not exist"
                
                # Try to query a system table in the schema
                # This verifies we have access to the schema
                result = await conn.fetchval(f"""
                    SELECT count(*) FROM information_schema.tables 
                    WHERE table_schema = $1
                """, schema)
                
                return True, f"Schema '{schema}' accessible, {result} tables found"
                
            finally:
                await conn.close()
                
        except Exception as e:
            return False, f"Schema access check failed: {str(e)}"


class RedisHealthCheck:
    """Redis health checker"""
    
    def __init__(self, redis_url: str, timeout: float = 10.0):
        self.redis_url = redis_url
        self.timeout = timeout
        self._last_check_time = 0
        self._last_check_result = None
        self._cache_duration = 30  # Cache health check for 30 seconds
    
    async def check_health(self) -> Tuple[bool, str, Optional[float]]:
        """
        Check Redis health with connection test
        
        Returns:
            (is_healthy, message, response_time_seconds)
        """
        # Return cached result if recent
        now = time.time()
        if (now - self._last_check_time) < self._cache_duration and self._last_check_result:
            return self._last_check_result
        
        start_time = time.time()
        
        try:
            # Create Redis connection with timeout
            redis_conn = await asyncio.wait_for(
                redis.from_url(self.redis_url),
                timeout=self.timeout
            )
            
            try:
                # Test ping
                pong = await asyncio.wait_for(
                    redis_conn.ping(),
                    timeout=self.timeout
                )
                
                if pong:
                    response_time = time.time() - start_time
                    self._last_check_result = (True, "Redis connection healthy", response_time)
                    self._last_check_time = now
                    return self._last_check_result
                else:
                    response_time = time.time() - start_time
                    self._last_check_result = (False, "Redis ping failed", response_time)
                    self._last_check_time = now
                    return self._last_check_result
                    
            finally:
                await redis_conn.close()
                
        except asyncio.TimeoutError:
            response_time = time.time() - start_time
            message = f"Redis connection timeout after {self.timeout}s"
            logger.error(message)
            self._last_check_result = (False, message, response_time)
            self._last_check_time = now
            return self._last_check_result
            
        except Exception as e:
            response_time = time.time() - start_time
            message = f"Redis connection error: {str(e)}"
            logger.error(message)
            self._last_check_result = (False, message, response_time)
            self._last_check_time = now
            return self._last_check_result
    
    async def check_queue_access(self, queue_name: str = "instrument_ingestion") -> Tuple[bool, str]:
        """
        Check if we can access the Redis queue
        
        Args:
            queue_name: Queue name to test
            
        Returns:
            (can_access, message)
        """
        try:
            redis_conn = await asyncio.wait_for(
                redis.from_url(self.redis_url),
                timeout=self.timeout
            )
            
            try:
                # Try to get queue length (this tests read access)
                queue_length = await redis_conn.llen(queue_name)
                
                # Try a test write/read operation
                test_key = f"{queue_name}:health_check"
                await redis_conn.set(test_key, "health_test", ex=60)  # 60 second expiry
                test_value = await redis_conn.get(test_key)
                await redis_conn.delete(test_key)  # Cleanup
                
                if test_value == b"health_test":
                    return True, f"Queue '{queue_name}' accessible, length: {queue_length}"
                else:
                    return False, f"Queue test write/read failed for '{queue_name}'"
                
            finally:
                await redis_conn.close()
                
        except Exception as e:
            return False, f"Queue access check failed: {str(e)}"


class HealthCheckManager:
    """Manages all health checks for the service"""
    
    def __init__(self, database_url: str, redis_url: str):
        self.db_checker = DatabaseHealthCheck(database_url)
        self.redis_checker = RedisHealthCheck(redis_url)
    
    async def get_comprehensive_health(self) -> Dict:
        """
        Get comprehensive health status for all dependencies
        
        Returns:
            Dictionary with health status of all components
        """
        health_status = {
            "overall": "healthy",
            "checks": {},
            "timestamp": time.time(),
            "response_times": {}
        }
        
        # Database health check
        try:
            db_healthy, db_message, db_time = await self.db_checker.check_health()
            health_status["checks"]["database"] = {
                "status": "healthy" if db_healthy else "unhealthy",
                "message": db_message,
                "response_time_ms": round(db_time * 1000, 2) if db_time else None
            }
            
            if db_time:
                health_status["response_times"]["database"] = round(db_time * 1000, 2)
            
            # Additional schema check
            schema_accessible, schema_message = await self.db_checker.check_schema_access()
            health_status["checks"]["database_schema"] = {
                "status": "healthy" if schema_accessible else "unhealthy",
                "message": schema_message
            }
            
            if not db_healthy or not schema_accessible:
                health_status["overall"] = "unhealthy"
                
        except Exception as e:
            health_status["checks"]["database"] = {
                "status": "error",
                "message": f"Database check failed: {str(e)}"
            }
            health_status["overall"] = "unhealthy"
        
        # Redis health check
        try:
            redis_healthy, redis_message, redis_time = await self.redis_checker.check_health()
            health_status["checks"]["redis"] = {
                "status": "healthy" if redis_healthy else "unhealthy",
                "message": redis_message,
                "response_time_ms": round(redis_time * 1000, 2) if redis_time else None
            }
            
            if redis_time:
                health_status["response_times"]["redis"] = round(redis_time * 1000, 2)
            
            # Additional queue check
            queue_accessible, queue_message = await self.redis_checker.check_queue_access()
            health_status["checks"]["redis_queue"] = {
                "status": "healthy" if queue_accessible else "unhealthy",
                "message": queue_message
            }
            
            if not redis_healthy or not queue_accessible:
                health_status["overall"] = "unhealthy"
                
        except Exception as e:
            health_status["checks"]["redis"] = {
                "status": "error",
                "message": f"Redis check failed: {str(e)}"
            }
            health_status["overall"] = "unhealthy"
        
        return health_status
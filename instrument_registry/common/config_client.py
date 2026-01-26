"""
Configuration Service Client for Instrument Registry

Handles configuration retrieval from the centralized config service
following StocksBlitz patterns.
"""
import asyncio
import logging
import httpx
from typing import Optional, Dict, Any
import time

logger = logging.getLogger(__name__)


class ConfigClient:
    """Client for config service integration"""
    
    def __init__(self, service_name: str, internal_api_key: str, config_service_url: str = "http://localhost:8100"):
        self.service_name = service_name
        self.internal_api_key = internal_api_key
        self.config_service_url = config_service_url
        self.cache = {}
        self.cache_ttl = 300  # 5 minutes
        self.last_cache_update = {}
        self._client = None
    
    async def initialize(self):
        """Initialize the HTTP client"""
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(30.0),
            headers={"X-Internal-API-Key": self.internal_api_key}
        )
        
        # Load initial configuration
        await self._load_config()
    
    async def close(self):
        """Close the HTTP client"""
        if self._client:
            await self._client.aclose()
    
    async def _load_config(self):
        """Load configuration from config service"""
        try:
            # Get global configurations
            global_configs = await self._fetch_global_configs()
            
            # Get service-specific configurations
            service_configs = await self._fetch_service_configs()
            
            # Merge configurations (service-specific takes precedence)
            self.cache.update(global_configs)
            self.cache.update(service_configs)
            
            self.last_cache_update[self.service_name] = time.time()
            logger.info(f"Loaded {len(self.cache)} configuration parameters")
            
        except Exception as e:
            logger.error(f"Failed to load configuration: {e}")
            # Use fallback values for critical configs
            self._set_fallback_config()
    
    async def _fetch_global_configs(self) -> Dict[str, str]:
        """Fetch global configuration parameters"""
        global_keys = [
            "LOG_LEVEL", "CORS_ORIGINS", "REQUEST_TIMEOUT", 
            "SHUTDOWN_TIMEOUT", "DEBUG"
        ]
        
        configs = {}
        for key in global_keys:
            try:
                response = await self._client.get(
                    f"{self.config_service_url}/api/v1/secrets/{key.lower()}/value",
                    params={"environment": "prod"}
                )
                if response.status_code == 200:
                    data = response.json()
                    configs[key] = data.get("value", "")
                else:
                    logger.warning(f"Global config {key} not found, using fallback")
            except Exception as e:
                logger.warning(f"Failed to fetch global config {key}: {e}")
        
        return configs
    
    async def _fetch_service_configs(self) -> Dict[str, str]:
        """Fetch service-specific configuration parameters"""
        service_keys = [
            "INSTRUMENT_REGISTRY_PORT",
            "INSTRUMENT_REGISTRY_CACHE_TTL_SECONDS",
            "INSTRUMENT_REGISTRY_HEALTH_CRITICAL_MINUTES",
            "INSTRUMENT_REGISTRY_HEALTH_WARNING_MINUTES",
            "INSTRUMENT_REGISTRY_HEALTH_HEALTHY_MINUTES",
            "INSTRUMENT_REGISTRY_INGESTION_QUEUE_NAME",
            "INSTRUMENT_REGISTRY_INGESTION_BATCH_SIZE",
            "INSTRUMENT_REGISTRY_INGESTION_WORKER_COUNT"
        ]
        
        configs = {}
        for key in service_keys:
            try:
                response = await self._client.get(
                    f"{self.config_service_url}/api/v1/secrets/{key.lower()}/value",
                    params={"environment": "prod"}
                )
                if response.status_code == 200:
                    data = response.json()
                    # Remove service prefix for local use
                    local_key = key.replace("INSTRUMENT_REGISTRY_", "")
                    configs[local_key] = data.get("value", "")
                else:
                    logger.warning(f"Service config {key} not found, using fallback")
            except Exception as e:
                logger.warning(f"Failed to fetch service config {key}: {e}")
        
        # Fetch infrastructure secrets
        infrastructure_keys = {
            "DATABASE_URL": "DATABASE_URL",
            "REDIS_URL": "REDIS_URL", 
            "INTERNAL_API_KEY": "INTERNAL_API_KEY"
        }
        
        for config_key, secret_key in infrastructure_keys.items():
            try:
                # Try both uppercase and lowercase like bootstrap script does
                for test_key in [secret_key, secret_key.lower()]:
                    response = await self._client.get(
                        f"{self.config_service_url}/api/v1/secrets/{test_key}/value",
                        params={"environment": "prod"}
                    )
                    if response.status_code == 200:
                        data = response.json()
                        configs[config_key] = data.get("secret_value") or data.get("value", "")
                        break
                    elif response.status_code == 404:
                        continue
            except Exception as e:
                logger.warning(f"Failed to fetch infrastructure config {config_key}: {e}")
        
        return configs
    
    def _set_fallback_config(self):
        """Set fallback configuration values"""
        fallback_config = {
            "LOG_LEVEL": "INFO",
            "CORS_ORIGINS": "http://localhost:3000,http://localhost:8080",
            "REQUEST_TIMEOUT": "30",
            "PORT": "8086",
            "CACHE_TTL_SECONDS": "300",
            "HEALTH_CRITICAL_MINUTES": "60",
            "HEALTH_WARNING_MINUTES": "15",
            "HEALTH_HEALTHY_MINUTES": "5",
            "INGESTION_QUEUE_NAME": "instrument_ingestion",
            "INGESTION_BATCH_SIZE": "1000",
            "INGESTION_WORKER_COUNT": "4"
        }
        
        for key, value in fallback_config.items():
            if key not in self.cache:
                self.cache[key] = value
                logger.info(f"Using fallback value for {key}: {value}")
    
    def get(self, key: str, default: Optional[str] = None) -> Optional[str]:
        """Get configuration value with optional default"""
        # Check if cache needs refresh
        if self._should_refresh_cache():
            asyncio.create_task(self._load_config())
        
        return self.cache.get(key, default)
    
    def get_int(self, key: str, default: int = 0) -> int:
        """Get configuration value as integer"""
        value = self.get(key, str(default))
        try:
            return int(value)
        except (ValueError, TypeError):
            logger.warning(f"Invalid integer value for {key}: {value}, using default: {default}")
            return default
    
    def get_bool(self, key: str, default: bool = False) -> bool:
        """Get configuration value as boolean"""
        value = self.get(key, str(default))
        if isinstance(value, str):
            return value.lower() in ("true", "1", "yes", "on")
        return bool(value)
    
    def _should_refresh_cache(self) -> bool:
        """Check if cache should be refreshed"""
        last_update = self.last_cache_update.get(self.service_name, 0)
        return time.time() - last_update > self.cache_ttl
    
    async def get_secret(self, secret_key: str, environment: str = "prod") -> str:
        """Get secret value from config service"""
        try:
            response = await self._client.get(
                f"{self.config_service_url}/api/v1/secrets/{secret_key}/value",
                params={"environment": environment}
            )
            if response.status_code == 200:
                data = response.json()
                return data.get("secret_value", data.get("value", ""))
            else:
                raise ValueError(f"Secret {secret_key} not found")
        except Exception as e:
            logger.error(f"Failed to get secret {secret_key}: {e}")
            raise
    
    async def health_check(self) -> bool:
        """Check if config service is healthy"""
        try:
            response = await self._client.get(f"{self.config_service_url}/health")
            return response.status_code == 200
        except Exception as e:
            logger.error(f"Config service health check failed: {e}")
            return False
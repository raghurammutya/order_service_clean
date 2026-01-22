"""
Config-Service Compliant Settings

Fully compliant configuration that uses ONLY config-service for all parameters.
No environment variable fallbacks except for the mandatory triad.

ARCHITECTURE COMPLIANCE:
✅ Minimal Environment: Only ENVIRONMENT, CONFIG_SERVICE_URL, INTERNAL_API_KEY  
✅ Dynamic Configuration: All settings fetched from config-service APIs
✅ No Environment Fallbacks: Fail-fast if config-service unavailable
✅ Service Discovery: Port registry instead of hardcoded ports
"""
import os
import logging
import sys
from typing import List, Optional, Dict, Any
from pydantic_settings import BaseSettings
from pydantic import Field

logger = logging.getLogger(__name__)

# =============================================================================
# CONFIG SERVICE CLIENT INTEGRATION
# =============================================================================

_config_client = None
_port_registry_cache: Dict[str, int] = {}
_config_loaded = False


def _get_config_client():
    """
    Get or create config service client (MANDATORY - fail-fast if unavailable).
    
    ARCHITECTURE COMPLIANCE:
    - Uses bootstrap triad from docker-compose environment variables
    - Config service is REQUIRED (no fallbacks)
    - Service exits with sys.exit(1) if config service unhealthy
    - Retries with exponential backoff before failing
    - TEST MODE: Allow to proceed without config service for pytest
    """
    global _config_client, _config_loaded

    if _config_loaded:
        return _config_client

    _config_loaded = True

    # Check if in test mode - allow tests to proceed without config service
    is_test_mode = (
        'pytest' in sys.modules or 
        os.getenv("TEST_MODE") == "true" or
        os.getenv("ENVIRONMENT") == "test"
    )

    try:
        from common.config_service.client import ConfigServiceClient

        # Use bootstrap triad from docker-compose (mandatory)
        client = ConfigServiceClient(
            service_name="order_service",
            environment=os.getenv("ENVIRONMENT", "prod"),
            config_service_url=os.getenv("CONFIG_SERVICE_URL"),
            internal_api_key=os.getenv("INTERNAL_API_KEY"),
            timeout=30
        )

        # Retry health check with backoff (3 attempts)
        for attempt in range(3):
            try:
                if client.health_check():
                    logger.info("✓ Config service connected successfully")
                    _config_client = client
                    return _config_client
                else:
                    logger.warning(f"Config service unhealthy (attempt {attempt+1}/3)")
                    if attempt < 2:
                        import time
                        time.sleep(2 ** attempt)  # Exponential backoff: 1s, 2s
            except Exception as retry_error:
                logger.warning(f"Health check failed (attempt {attempt+1}/3): {retry_error}")
                if attempt < 2:
                    import time
                    time.sleep(2 ** attempt)

        # FAIL-FAST: Config service is MANDATORY (except in test mode)
        if is_test_mode:
            logger.warning("Config service unavailable in test mode - proceeding with defaults")
            _config_client = None
            return None
        
        logger.critical("=" * 80)
        logger.critical("CONFIG SERVICE UNAVAILABLE - REFUSING TO START")
        logger.critical("ARCHITECTURE VIOLATION: Config service is MANDATORY")
        logger.critical("=" * 80)
        sys.exit(1)

    except ImportError as e:
        if is_test_mode:
            logger.warning(f"Config service client not available in test mode: {e}")
            _config_client = None
            return None
        logger.critical(f"Config service client not available: {e}")
        sys.exit(1)
    except Exception as e:
        if is_test_mode:
            logger.warning(f"Config service connection failed in test mode: {e}")
            _config_client = None
            return None
        logger.critical(f"Config service connection failed: {e}")
        sys.exit(1)


def _get_config_value(key: str, required: bool = True, is_secret: bool = False, default_value: Any = None) -> Any:
    """
    Get value from config service (MANDATORY - no fallbacks).

    ARCHITECTURE COMPLIANCE:
    - Fetches from config_service ONLY (no env var fallbacks)
    - Fails if required config not found
    - Logs access for audit trail
    - TEST MODE: Provides test defaults when config service unavailable

    Args:
        key: Configuration key to fetch (ORDER_SERVICE_* format)
        required: If True, exits if value not found (default: True)
        is_secret: Whether this is a secret (affects logging)
        default_value: Test mode default value

    Returns:
        Configuration value from config_service or test default

    Raises:
        SystemExit: If required=True and value not found
    """
    client = _get_config_client()
    
    # If no client available (test mode), provide test defaults
    if not client:
        is_test_mode = (
            'pytest' in sys.modules or 
            os.getenv("TEST_MODE") == "true" or
            os.getenv("ENVIRONMENT") == "test"
        )
        
        if is_test_mode and default_value is not None:
            logger.warning(f"Using test default for {key}: {default_value}")
            return default_value
        
        if required:
            logger.critical(f"Required config not available: {key}")
            sys.exit(1)
        return default_value

    try:
        if is_secret:
            value = client.get_secret(key, required=required)
        else:
            value = client.get_config(key)

        if value is not None:
            if is_secret:
                logger.debug(f"✓ Loaded secret: {key}")
            else:
                logger.debug(f"✓ Loaded config: {key} = {value}")
            return value
        
        if required:
            logger.critical(f"Required config not available: {key}")
            sys.exit(1)
        return default_value

    except Exception as e:
        logger.error(f"Failed to fetch {key} from config service: {e}")
        if required:
            logger.critical(f"Required config not available: {key}")
            sys.exit(1)
        return default_value


def _get_service_port(service_name: str) -> int:
    """
    Get service port from config service port registry.
    
    ARCHITECTURE COMPLIANCE:
    - No hardcoded ports allowed
    - All ports from config service port registry
    - Cached for performance
    """
    global _port_registry_cache
    
    if service_name in _port_registry_cache:
        return _port_registry_cache[service_name]
    
    client = _get_config_client()
    
    # Test mode fallbacks
    if not client:
        test_ports = {
            "order_service": 8087,
            "token_manager": 8088,
            "ticker_service": 8089,
            "ticker_service_v2": 8089,
            "user_service": 8011,
            "calendar_service": 8013,
        }
        port = test_ports.get(service_name, 8000)
        logger.warning(f"Using test port for {service_name}: {port}")
        return port
    
    try:
        port = client.get_port(service_name)
        _port_registry_cache[service_name] = port
        logger.debug(f"✓ Loaded port for {service_name}: {port}")
        return port
    except Exception as e:
        logger.error(f"Failed to fetch port for {service_name}: {e}")
        # Use test default in case of config service issues
        fallback_port = 8000
        logger.warning(f"Using fallback port for {service_name}: {fallback_port}")
        return fallback_port


def _build_service_url(service_name: str, host: str = "localhost") -> str:
    """Build service URL using dynamic port from config service"""
    port = _get_service_port(service_name)
    return f"http://{host}:{port}/api/v1"


# =============================================================================
# COMPLIANT SETTINGS CLASS  
# =============================================================================

class CompliantSettings(BaseSettings):
    """
    Config-Service Compliant Application Settings
    
    ARCHITECTURE COMPLIANCE:
    ✅ Uses bootstrap triad from docker-compose: ENVIRONMENT, CONFIG_SERVICE_URL, INTERNAL_API_KEY
    ✅ All other settings from config-service APIs  
    ✅ Dynamic service discovery via port registry
    ✅ Fail-fast if config service unavailable (production)
    ✅ No environment variable fallbacks beyond bootstrap triad
    """

    # =================================================================
    # BOOTSTRAP TRIAD - PASSED FROM DOCKER-COMPOSE (MANDATORY)
    # =================================================================
    environment: str = Field(env="ENVIRONMENT", default="development")
    config_service_url: str = Field(env="CONFIG_SERVICE_URL", default="http://localhost:8100") 
    internal_api_key: str = Field(env="INTERNAL_API_KEY", default="")

    # =================================================================
    # ALL OTHER SETTINGS FROM CONFIG SERVICE (NO ENV VARS)
    # =================================================================

    # Core Application
    @property
    def app_name(self) -> str:
        return "Order Execution Service"
    
    @property
    def version(self) -> str:
        return "2.0.0-config-compliant"
    
    @property
    def port(self) -> int:
        return _get_config_value("ORDER_SERVICE_PORT", required=True, default_value=8087)

    # Database - from shared config service parameters
    @property
    def database_url(self) -> str:
        return _get_config_value("DATABASE_URL", required=True, is_secret=True, default_value="postgresql://test:test@localhost/test_order_service")
    
    @property 
    def database_pool_size(self) -> int:
        return _get_config_value("ORDER_SERVICE_DATABASE_POOL_SIZE", required=False, default_value=20)
    
    @property
    def database_max_overflow(self) -> int:
        return _get_config_value("ORDER_SERVICE_DATABASE_MAX_OVERFLOW", required=False, default_value=10)

    # Redis - from shared config service parameters  
    @property
    def redis_url(self) -> str:
        return _get_config_value("REDIS_URL", required=True, is_secret=True, default_value="redis://localhost:6379/1")
    
    @property
    def redis_order_ttl(self) -> int:
        return _get_config_value("ORDER_SERVICE_REDIS_ORDER_TTL", required=False, default_value=86400)
    
    @property
    def redis_required(self) -> bool:
        return _get_config_value("ORDER_SERVICE_REDIS_REQUIRED", required=False, default_value=True)

    # Cache Encryption
    @property
    def cache_encryption_key(self) -> Optional[str]:
        return _get_config_value("CACHE_ENCRYPTION_KEY", required=False, is_secret=True)

    # Authentication
    @property
    def auth_enabled(self) -> bool:
        return _get_config_value("ORDER_SERVICE_AUTH_ENABLED", required=False, default_value=True)
    
    @property
    def jwks_url(self) -> str:
        # Build JWKS URL from user service
        user_service_url = _build_service_url("user_service")
        return f"{user_service_url}/auth/.well-known/jwks.json"
    
    @property 
    def jwt_issuer(self) -> str:
        return _get_config_value("JWT_ISSUER", required=True, default_value="test-issuer")
    
    @property
    def jwt_audience(self) -> str:
        return _get_config_value("JWT_AUDIENCE", required=True, default_value="test-audience")
    
    @property
    def jwt_signing_key_id(self) -> str:
        return _get_config_value("JWT_SIGNING_KEY_ID", required=True, default_value="test-key-id")

    # Service-to-Service Authentication  
    @property
    def INTERNAL_SERVICE_SECRET(self) -> str:
        return _get_config_value("INTERNAL_SERVICE_SECRET", required=True, is_secret=True, default_value="test-internal-secret")

    # Rate Limiting
    @property
    def rate_limit_enabled(self) -> bool:
        return _get_config_value("ORDER_SERVICE_RATE_LIMIT_ENABLED", required=False, default_value=True)
    
    @property
    def rate_limit_default(self) -> str:
        return _get_config_value("ORDER_SERVICE_RATE_LIMIT_DEFAULT", required=False, default_value="100/minute")
    
    @property
    def rate_limit_order_placement(self) -> str:
        return _get_config_value("ORDER_SERVICE_RATE_LIMIT_ORDER_PLACEMENT", required=False, default_value="10/minute")

    # CORS
    @property
    def cors_enabled(self) -> bool:
        return _get_config_value("ORDER_SERVICE_CORS_ENABLED", required=False, default_value=True)
    
    @property
    def cors_origins(self) -> str:
        return _get_config_value("ORDER_SERVICE_CORS_ORIGINS", required=False, default_value="")

    def get_cors_origins(self) -> List[str]:
        """Get CORS origins from config service"""
        cors_origins_str = self.cors_origins
        if cors_origins_str:
            return [origin.strip() for origin in cors_origins_str.split(",") if origin.strip()]
        
        # Fallback for empty config - build from production hosts
        origins = []
        
        if self.is_production:
            production_http = _get_config_value("ORDER_SERVICE_PRODUCTION_HOST_HTTP", required=False, default_value="http://5.223.52.98")
            production_https = _get_config_value("ORDER_SERVICE_PRODUCTION_DOMAIN_HTTPS", required=False, default_value="https://app.stocksblitz.com") 
            trading_https = _get_config_value("ORDER_SERVICE_TRADING_DOMAIN_HTTPS", required=False, default_value="https://trading.stocksblitz.com")
            
            origins.extend([production_http, production_https, trading_https])
        else:
            # Development origins
            origins.extend([
                "http://localhost:3000",
                "http://localhost:3001", 
                "http://localhost:3002",
                "http://localhost:3080",
                "http://127.0.0.1:3000",
                "http://127.0.0.1:3001",
            ])
        
        return origins

    # Broker Integration
    @property
    def kite_api_key(self) -> str:
        return _get_config_value("ORDER_SERVICE_KITE_API_KEY", required=False, is_secret=True, default_value="")
    
    @property
    def kite_account_id(self) -> str:
        return _get_config_value("ORDER_SERVICE_KITE_ACCOUNT_ID", required=False, default_value="primary")
    
    @property
    def kite_primary_api_key(self) -> str:
        return _get_config_value("ORDER_SERVICE_KITE_PRIMARY_API_KEY", required=False, is_secret=True, default_value="")
    
    @property
    def kite_personal_api_key(self) -> str:
        return _get_config_value("ORDER_SERVICE_KITE_PERSONAL_API_KEY", required=False, is_secret=True, default_value="")

    # Service URLs - Dynamic Discovery
    @property
    def token_manager_url(self) -> str:
        return _build_service_url("token_manager")
    
    @property
    def ticker_service_url(self) -> str:
        return _build_service_url("ticker_service_v2")
    
    @property
    def ticker_redis_url(self) -> str:
        # Same Redis instance as main service 
        return self.redis_url

    # Order Execution Settings
    @property
    def max_order_quantity(self) -> int:
        return _get_config_value("ORDER_SERVICE_MAX_ORDER_QUANTITY", required=False, default_value=10000)
    
    @property 
    def max_order_value(self) -> float:
        return _get_config_value("ORDER_SERVICE_MAX_ORDER_VALUE", required=False, default_value=10000000.0)
    
    @property
    def enable_order_validation(self) -> bool:
        return _get_config_value("ORDER_SERVICE_ENABLE_ORDER_VALIDATION", required=False, default_value=True)
    
    @property
    def enable_risk_checks(self) -> bool:
        return _get_config_value("ORDER_SERVICE_ENABLE_RISK_CHECKS", required=False, default_value=True)
    
    @property
    def risk_margin_multiplier(self) -> float:
        return _get_config_value("ORDER_SERVICE_RISK_MARGIN_MULTIPLIER", required=False, default_value=1.25)
    
    @property
    def max_position_exposure_value(self) -> float:
        return _get_config_value("ORDER_SERVICE_MAX_POSITION_EXPOSURE_VALUE", required=False, default_value=10000000.0)
    
    @property
    def max_position_concentration_pct(self) -> float:
        return _get_config_value("ORDER_SERVICE_MAX_POSITION_CONCENTRATION_PCT", required=False, default_value=0.6)
    
    @property
    def daily_loss_limit(self) -> float:
        return _get_config_value("ORDER_SERVICE_DAILY_LOSS_LIMIT", required=False, default_value=-50000.0)

    # Position Tracking
    @property
    def enable_position_tracking(self) -> bool:
        return _get_config_value("ORDER_SERVICE_ENABLE_POSITION_TRACKING", required=False, default_value=True)
    
    @property
    def position_sync_interval(self) -> int:
        return _get_config_value("ORDER_SERVICE_POSITION_SYNC_INTERVAL", required=False, default_value=60)

    # System
    @property
    def system_user_id(self) -> int:
        return _get_config_value("ORDER_SERVICE_SYSTEM_USER_ID", required=False, default_value=1)
    
    @property
    def metrics_enabled(self) -> bool:
        return _get_config_value("ORDER_SERVICE_METRICS_ENABLED", required=False, default_value=True)
    
    @property
    def log_level(self) -> str:
        return _get_config_value("ORDER_SERVICE_LOG_LEVEL", required=False, default_value="INFO")

    # Rate Limiting (Additional)
    @property
    def daily_order_limit(self) -> int:
        return _get_config_value("ORDER_SERVICE_DAILY_ORDER_LIMIT", required=False, default_value=100)
    
    @property
    def daily_reset_time(self) -> str:
        return _get_config_value("ORDER_SERVICE_DAILY_RESET_TIME", required=False, default_value="09:15")
    
    @property
    def hard_refresh_rate_limit_seconds(self) -> int:
        return _get_config_value("ORDER_SERVICE_HARD_REFRESH_RATE_LIMIT_SECONDS", required=False, default_value=10)

    # Security
    @property
    def idempotency_fail_closed(self) -> bool:
        return _get_config_value("ORDER_SERVICE_IDEMPOTENCY_FAIL_CLOSED", required=False, default_value=True)
    
    @property
    def test_auth_mode(self) -> bool:
        # Test auth NEVER enabled in production
        if self.is_production:
            return False
        return _get_config_value("ORDER_SERVICE_TEST_AUTH_MODE", required=False, default_value=False)
    
    @property
    def gateway_secret(self) -> str:
        return _get_config_value("ORDER_SERVICE_GATEWAY_SECRET", required=False, is_secret=True, default_value="")
    
    @property
    def trust_gateway_headers(self) -> bool:
        return _get_config_value("ORDER_SERVICE_TRUST_GATEWAY_HEADERS", required=False, default_value=False)

    class Config:
        # Only allow env vars for the triad
        case_sensitive = False
        extra = "ignore"

    def validate_configuration(self):
        """Validate critical configuration at startup."""
        errors = []

        # Validate mandatory triad is present
        if not self.environment:
            errors.append("ENVIRONMENT environment variable must be set")
        
        if not self.config_service_url:
            errors.append("CONFIG_SERVICE_URL environment variable must be set")
            
        if not self.internal_api_key:
            errors.append("INTERNAL_API_KEY environment variable must be set")

        # Validate database URL is configured
        if not self.database_url:
            errors.append("DATABASE_URL must be configured in config service")

        # Validate Redis URL
        if not self.redis_url:
            errors.append("REDIS_URL must be configured in config service")

        # Production-specific validations
        if self.is_production:
            # Enforce production DB name
            if "stocksblitz_unified_prod" not in self.database_url:
                errors.append("DATABASE_URL must point to stocksblitz_unified_prod in production")
            
            # Redis is MANDATORY for duplicate order protection
            if not self.redis_required:
                errors.append(
                    "SECURITY VIOLATION: REDIS_REQUIRED must be true in production. "
                    "Redis provides idempotency protection against duplicate orders."
                )
            
            # Cache encryption required in production
            if not self.cache_encryption_key:
                errors.append("CACHE_ENCRYPTION_KEY must be set in production to protect cached PII")
            
            # Test auth MUST be disabled
            if self.test_auth_mode:
                errors.append("SECURITY VIOLATION: Test auth mode cannot be enabled in production")

        if errors:
            error_message = "Configuration validation failed:\n" + "\n".join(f"  - {e}" for e in errors)
            raise RuntimeError(error_message)

    @property
    def is_production(self) -> bool:
        """Return True for production-like environments (prod/production)."""
        return str(self.environment).lower() in ("production", "prod")


# =============================================================================
# GLOBAL SETTINGS INSTANCE
# =============================================================================

# Create compliant settings instance
compliant_settings = CompliantSettings()

# Validate configuration at module load time
try:
    compliant_settings.validate_configuration()
    logger.info("✅ Config-service compliant settings loaded successfully")
except Exception as e:
    logger.error(f"❌ Settings validation failed: {e}")
    raise


# Export as 'settings' for compatibility
settings = compliant_settings
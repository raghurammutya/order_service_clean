"""
Order Service Configuration

Environment-aware configuration for the order execution service.
Centralized config service for secrets and configuration management.

ARCHITECTURE COMPLIANCE:
- Config service is MANDATORY (Principle #1)
- All secrets fetched from config_service (Principle #2)
- No fallbacks to environment variables for secrets (Principle #4)
- Fail-fast if config service unavailable

Service Discovery:
- Service URLs are resolved dynamically via config_service port registry
- Falls back to hardcoded ports if config_service is unavailable
- API prefix is always /api/v1 (standardized across all services)
"""
import os
import logging
import sys
from typing import List, Optional
from pydantic_settings import BaseSettings
from pydantic import Field

logger = logging.getLogger(__name__)

# =============================================================================
# Service Discovery Integration
# =============================================================================
# Dynamic service URL resolution - no hardcoded ports needed
# Resolution order: ENV var -> config_service -> fallback ports

try:
    import sys
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))))
    from common.service_registry import get_service_url, get_jwks_url
    _HAS_SERVICE_REGISTRY = True
    logger.info("Service registry module loaded")
except ImportError:
    _HAS_SERVICE_REGISTRY = False
    logger.warning("Service registry module not available, using fallbacks")

    def get_service_url(service_name: str, **kwargs) -> str:
        """Fallback when service registry module is not available."""
        # Get fallback URLs from environment variables
        env_var = f"{service_name.upper()}_SERVICE_URL"
        service_url = os.getenv(env_var)
        
        if service_url:
            return service_url
            
        # Legacy hardcoded fallbacks - should be externalized
        fallback_ports = {
            "token_manager": int(os.getenv("TOKEN_MANAGER_PORT", "8088")),
            "ticker_service": int(os.getenv("TICKER_SERVICE_PORT", "8089")),
            "ticker_service_v2": int(os.getenv("TICKER_SERVICE_V2_PORT", "8089")),
            "user_service": int(os.getenv("USER_SERVICE_PORT", "8011")),
            "calendar_service": int(os.getenv("CALENDAR_SERVICE_PORT", "8013")),
        }
        port = fallback_ports.get(service_name, int(os.getenv("DEFAULT_SERVICE_PORT", "8000")))
        host = os.getenv("SERVICE_HOST", "localhost")
        return f"http://{host}:{port}"

    def get_jwks_url(**kwargs) -> str:
        """Fallback JWKS URL when service registry is not available."""
        # Check environment variable first
        jwks_url = os.getenv("JWKS_URL")
        if jwks_url:
            return jwks_url
            
        # Fallback to user service
        user_service_url = get_service_url("user_service")
        return f"{user_service_url}/api/v1/auth/.well-known/jwks.json"


# =============================================================================
# Config Service Integration (for secrets)
# =============================================================================

_config_client = None
_config_loaded = False


def _get_config_client():
    """
    Get or create config service client (MANDATORY - fail-fast if unavailable).

    ARCHITECTURE COMPLIANCE:
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

        client = ConfigServiceClient(
            service_name="order_service",
            environment=os.getenv("ENVIRONMENT", "prod"),
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


def _get_from_config_service(key: str, required: bool = True, is_secret: bool = False) -> Optional[str]:
    """
    Get value from config service (MANDATORY - no fallbacks).

    ARCHITECTURE COMPLIANCE:
    - Fetches from config_service ONLY (no env var fallbacks)
    - Fails if required secret not found
    - Logs secret access for audit trail
    - TEST MODE: Provides test defaults when config service unavailable

    Args:
        key: Configuration key to fetch
        required: If True, exits if value not found (default: True)
        is_secret: Whether this is a secret (affects logging)

    Returns:
        Configuration value from config_service or None

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
        
        if is_test_mode:
            # Provide test defaults for required secrets
            test_defaults = {
                "JWT_SIGNING_KEY_ID": "test-key-id",
                "INTERNAL_SERVICE_SECRET": "test-internal-secret", 
                "DATABASE_URL": "postgresql://test:test@localhost/test_order_service",
                "REDIS_URL": "redis://localhost:6379/1",
                "JWT_ISSUER": "test-issuer",
                "JWT_AUDIENCE": "test-audience", 
                "INTERNAL_API_KEY": "test-internal-api-key",
                "CACHE_ENCRYPTION_KEY": "test-cache-encryption-key"
            }
            
            if key in test_defaults:
                logger.warning(f"Using test default for {key}")
                return test_defaults[key]
        
        if required:
            logger.critical(f"Required config/secret not available: {key}")
            sys.exit(1)
        return None

    try:
        if is_secret:
            value = client.get_secret(key, required=required)
            if value:
                logger.debug(f"✓ Loaded secret: {key}")
        else:
            value = client.get_config(key)
            if value:
                logger.debug(f"✓ Loaded config: {key}")

        return value

    except Exception as e:
        logger.error(f"Failed to fetch {key} from config service: {e}")
        if required:
            logger.critical(f"Required config/secret not available: {key}")
            sys.exit(1)
        return None


class Settings(BaseSettings):
    """Application settings with config service integration

    ARCHITECTURE COMPLIANCE:
    - All secrets MUST come from config_service (no fallbacks)
    - Required configs MUST come from config_service (no fallbacks)
    - Non-secret settings can use environment variables
    """

    # Application
    app_name: str = "Order Execution Service"
    version: str = "1.0.0"
    environment: str = Field(default="development", env="ENVIRONMENT")
    port: int = Field(default=8087, env="PORT")

    # Database - MUST fetch from config_service (no fallbacks)
    database_url: str = Field(
        default_factory=lambda: _get_from_config_service("DATABASE_URL", required=True, is_secret=True),
        description="Database URL - MANDATORY from config_service"
    )
    database_pool_size: int = Field(default=20, env="DATABASE_POOL_SIZE")
    database_max_overflow: int = Field(default=10, env="DATABASE_MAX_OVERFLOW")

    # Redis - MUST fetch from config_service (no fallbacks)
    redis_url: str = Field(
        default_factory=lambda: _get_from_config_service("REDIS_URL", required=True, is_secret=True),
        description="Redis URL - MANDATORY from config_service"
    )
    redis_order_ttl: int = Field(default=86400, env="REDIS_ORDER_TTL")  # 24 hours
    # CRITICAL: Redis is MANDATORY for idempotency (duplicate order protection)
    # SECURITY: MUST be True in production - duplicate orders could cause financial loss
    # Setting to False in production will cause validation failure and service exit
    redis_required: bool = Field(default=True, env="REDIS_REQUIRED")

    # Cache Encryption - fetched from config service (optional)
    # Used by SecureCacheService for encrypting sensitive cached data (order details, positions, etc.)
    # Optional: Service works without cache, but caching improves performance
    cache_encryption_key: Optional[str] = Field(
        default_factory=lambda: _get_from_config_service("CACHE_ENCRYPTION_KEY", required=False, is_secret=True),
        description="Cache encryption key - from config_service (required in production)"
    )

    # JWT Authentication - MUST fetch from config_service (no fallbacks)
    auth_enabled: bool = Field(default=True, env="AUTH_ENABLED")
    jwks_url: str = Field(default_factory=get_jwks_url, env="JWKS_URL")
    jwt_issuer: str = Field(
        default_factory=lambda: _get_from_config_service("JWT_ISSUER", required=True, is_secret=False),
        description="JWT issuer - MANDATORY from config_service"
    )
    jwt_audience: str = Field(
        default_factory=lambda: _get_from_config_service("JWT_AUDIENCE", required=True, is_secret=False),
        description="JWT audience - MANDATORY from config_service"
    )
    jwt_signing_key_id: str = Field(
        default_factory=lambda: _get_from_config_service("JWT_SIGNING_KEY_ID", required=True, is_secret=False),
        description="JWT signing key ID - MANDATORY from config_service"
    )
    
    # Service-to-Service Authentication
    INTERNAL_SERVICE_SECRET: str = Field(
        default_factory=lambda: _get_from_config_service("INTERNAL_SERVICE_SECRET", required=True, is_secret=True),
        description="Internal service authentication secret - MANDATORY from config_service"
    )

    # Rate Limiting
    rate_limit_enabled: bool = Field(default=True, env="RATE_LIMIT_ENABLED")
    rate_limit_default: str = Field(default="100/minute", env="RATE_LIMIT_DEFAULT")
    rate_limit_order_placement: str = Field(default="10/minute", env="RATE_LIMIT_ORDER_PLACEMENT")

    # CORS - Environment-aware configuration
    cors_enabled: bool = Field(default=True, env="CORS_ENABLED")
    cors_origins: str = Field(default="", env="CORS_ORIGINS")  # Empty = use defaults

    def get_cors_origins(self) -> List[str]:
        """Get CORS origins based on environment.

        Production: Only allows production origins (5.223.52.98, stocksblitz.com)
        Development: Also allows localhost origins
        """
        # If explicit CORS_ORIGINS set, use those
        if self.cors_origins:
            return [origin.strip() for origin in self.cors_origins.split(",") if origin.strip()]

        # Production defaults from environment variables
        production_origins = [
            os.getenv("PRODUCTION_HOST_HTTP", "http://5.223.52.98"),
            f"{os.getenv('PRODUCTION_HOST_HTTP', 'http://5.223.52.98')}:80",
            f"{os.getenv('PRODUCTION_HOST_HTTP', 'http://5.223.52.98')}:3001",
            os.getenv("PRODUCTION_DOMAIN_HTTPS", "https://app.stocksblitz.com"),
            os.getenv("TRADING_DOMAIN_HTTPS", "https://trading.stocksblitz.com"),
        ]

        # Development additional origins
        dev_origins = [
            "http://localhost:3000",
            "http://localhost:3001",
            "http://localhost:3002",
            "http://localhost:3080",
            "http://127.0.0.1:3000",
            "http://127.0.0.1:3001",
        ]

        if self.is_production:
            return production_origins
        else:
            return production_origins + dev_origins

    # Kite Integration (single account - legacy)
    kite_api_key: str = Field(default="", env="KITE_API_KEY")
    kite_account_id: str = Field(default="primary", env="KITE_ACCOUNT_ID")

    # Multi-Account Kite Integration
    # Maps trading_account_id to kite account:
    #   1 -> primary (XJ4540)
    #   2 -> personal (WG7169)
    # SECURITY: No default values - must be configured via environment variable
    kite_primary_api_key: str = Field(default="", env="KITE_PRIMARY_API_KEY")
    kite_personal_api_key: str = Field(default="", env="KITE_PERSONAL_API_KEY")

    # Token Manager (for multi-account token fetching)
    # URL resolved dynamically via service registry
    token_manager_url: str = Field(
        default_factory=lambda: os.getenv("TOKEN_MANAGER_URL") or get_service_url("token_manager"),
        description="Token manager service URL"
    )

    # Ticker Service (for market data validation)
    # URL from env var (docker-compose) or service registry fallback
    ticker_service_url: str = Field(
        default_factory=lambda: os.getenv("TICKER_SERVICE_URL") or get_service_url("ticker_service_v2"),
        description="Ticker service URL"
    )

    # Ticker Redis URL - for subscribing to tick pub/sub
    # Now using same Redis database as all other services (database 0)
    ticker_redis_url: str = Field(
        default_factory=lambda: _get_from_config_service("REDIS_URL", required=True, is_secret=True),
        description="Ticker Redis URL - MANDATORY from config_service"
    )

    # Order Execution Settings
    max_order_quantity: int = Field(default=10000, env="MAX_ORDER_QUANTITY")
    max_order_value: float = Field(default=10000000.0, env="MAX_ORDER_VALUE")  # 1 crore
    enable_order_validation: bool = Field(default=True, env="ENABLE_ORDER_VALIDATION")
    enable_risk_checks: bool = Field(default=True, env="ENABLE_RISK_CHECKS")
    risk_margin_multiplier: float = Field(
        default=1.25,
        env="RISK_MARGIN_MULTIPLIER",
        description="Extra buffer applied to estimated order value when checking margins"
    )
    max_position_exposure_value: float = Field(
        default=10000000.0,
        env="MAX_POSITION_EXPOSURE_VALUE",
        description="Absolute cap on exposure per trading account (symbol + new order)"
    )
    max_position_concentration_pct: float = Field(
        default=0.6,
        env="MAX_POSITION_CONCENTRATION_PCT",
        description="Limit for symbol concentration as fraction of total exposure"
    )
    daily_loss_limit: float = Field(
        default=-50000.0,
        env="DAILY_LOSS_LIMIT",
        description="Threshold for daily net loss (negative) beyond which new orders are blocked"
    )

    # Position Tracking
    # NOTE: Positions are now updated in real-time from order completions
    # - enable_position_tracking: DEPRECATED - kept for backward compatibility
    # - position_sync_interval: Now used for validation interval (not sync)
    enable_position_tracking: bool = Field(default=True, env="ENABLE_POSITION_TRACKING")
    position_sync_interval: int = Field(default=60, env="POSITION_SYNC_INTERVAL")  # DEPRECATED: Not used for sync anymore

    # System User ID for background workers (single-tenant mode)
    # In multi-tenant mode, workers should query active users from user_service
    system_user_id: int = Field(default=1, env="SYSTEM_USER_ID")

    # Metrics
    metrics_enabled: bool = Field(default=True, env="METRICS_ENABLED")

    # Logging
    log_level: str = Field(default="INFO", env="LOG_LEVEL")

    # Internal API key for service-to-service authentication - MUST fetch from config_service
    # ARCHITECTURE PRINCIPLE #24: Single INTERNAL_API_KEY across ALL services
    internal_api_key: str = Field(
        default_factory=lambda: _get_from_config_service("INTERNAL_API_KEY", required=True, is_secret=True),
        description="Single shared internal API key for all service-to-service auth - MANDATORY from config_service"
    )

    class Config:
        env_file = ".env"
        case_sensitive = False
        extra = "ignore"  # Ignore extra fields to prevent validation errors

    def validate_configuration(self):
        """Validate critical configuration at startup.

        Raises:
            RuntimeError: If critical configuration is missing or invalid
        """
        errors = []

        # Validate database URL is configured
        if not self.database_url:
            errors.append("DATABASE_URL environment variable must be set")

        # Enforce production DB name
        if self.is_production and "stocksblitz_unified_prod" not in self.database_url:
            errors.append("DATABASE_URL must point to stocksblitz_unified_prod in production")

        # Ensure not using default/insecure credentials (production only)
        if self.is_production and self.database_url and "stocksblitz123" in self.database_url:
            errors.append(
                "Default database credentials detected in DATABASE_URL - "
                "configure production credentials"
            )

        # Ensure production database in production environment
        if self.is_production:
            if not self.database_url:
                errors.append("DATABASE_URL must be set in production")
            elif "localhost" in self.database_url:
                errors.append(
                    "Production environment cannot use localhost database - "
                    "configure remote database URL"
                )

        # Validate Redis URL
        if not self.redis_url:
            errors.append("REDIS_URL environment variable must be set")

        # Enforce Redis and cache encryption in production
        if self.is_production:
            # SECURITY: Redis is MANDATORY for duplicate order protection
            # Disabling Redis in production could lead to duplicate orders being sent to exchange
            if not self.redis_required:
                errors.append(
                    "SECURITY VIOLATION: REDIS_REQUIRED must be true in production. "
                    "Redis provides idempotency protection against duplicate orders. "
                    "Disabling Redis could cause duplicate order submission to exchange."
                )
            if not self.cache_encryption_key:
                errors.append("CACHE_ENCRYPTION_KEY must be set in production to protect cached PII")

        # Note: Kite API keys are optional - fetched from token_manager per-account
        # Only warn if not set (used as fallback)
        if self.is_production:
            if not self.kite_api_key and not self.kite_primary_api_key:
                import logging
                logging.getLogger(__name__).warning(
                    "KITE_API_KEY/KITE_PRIMARY_API_KEY not set - "
                    "relying on token_manager for per-account API keys"
                )

            # Note: API key validation removed - keys are now fetched from token_manager
            # which loads them from encrypted database storage

        if errors:
            error_message = "Configuration validation failed:\n" + "\n".join(f"  - {e}" for e in errors)
            raise RuntimeError(error_message)

    @property
    def is_production(self) -> bool:
        """Return True for production-like environments (prod/production)."""
        return str(self.environment).lower() in ("production", "prod")


# Global settings instance
settings = Settings()

# Validate configuration at module load time
settings.validate_configuration()

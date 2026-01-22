# Config-Service Compliance Audit

## Environment Variables Currently Used (VIOLATIONS)

### 1. Settings Class (app/config/settings.py)
**Status: NON-COMPLIANT** - Uses 20+ environment variables instead of config-service

#### Core Application Settings
```python
environment: str = Field(default="development", env="ENVIRONMENT")  # KEEP - triad
port: int = Field(default=8087, env="PORT")  # MIGRATE to ORDER_SERVICE_PORT
```

#### Database Configuration  
```python
database_pool_size: int = Field(default=20, env="DATABASE_POOL_SIZE")  # MIGRATE
database_max_overflow: int = Field(default=10, env="DATABASE_MAX_OVERFLOW")  # MIGRATE
```

#### Redis Configuration
```python
redis_order_ttl: int = Field(default=86400, env="REDIS_ORDER_TTL")  # MIGRATE 
redis_required: bool = Field(default=True, env="REDIS_REQUIRED")  # MIGRATE
```

#### Authentication Settings
```python
auth_enabled: bool = Field(default=True, env="AUTH_ENABLED")  # MIGRATE
jwks_url: str = Field(default_factory=get_jwks_url, env="JWKS_URL")  # MIGRATE
```

#### Rate Limiting
```python
rate_limit_enabled: bool = Field(default=True, env="RATE_LIMIT_ENABLED")  # MIGRATE
rate_limit_default: str = Field(default="100/minute", env="RATE_LIMIT_DEFAULT")  # MIGRATE
rate_limit_order_placement: str = Field(default="10/minute", env="RATE_LIMIT_ORDER_PLACEMENT")  # MIGRATE
```

#### CORS Settings
```python
cors_enabled: bool = Field(default=True, env="CORS_ENABLED")  # MIGRATE
cors_origins: str = Field(default="", env="CORS_ORIGINS")  # MIGRATE
```

#### Broker Integration
```python
kite_api_key: str = Field(default="", env="KITE_API_KEY")  # MIGRATE
kite_account_id: str = Field(default="primary", env="KITE_ACCOUNT_ID")  # MIGRATE
kite_primary_api_key: str = Field(default="", env="KITE_PRIMARY_API_KEY")  # MIGRATE
kite_personal_api_key: str = Field(default="", env="KITE_PERSONAL_API_KEY")  # MIGRATE
```

#### Order Execution Settings
```python
max_order_quantity: int = Field(default=10000, env="MAX_ORDER_QUANTITY")  # MIGRATE
max_order_value: float = Field(default=10000000.0, env="MAX_ORDER_VALUE")  # MIGRATE
enable_order_validation: bool = Field(default=True, env="ENABLE_ORDER_VALIDATION")  # MIGRATE
enable_risk_checks: bool = Field(default=True, env="ENABLE_RISK_CHECKS")  # MIGRATE
risk_margin_multiplier: float = Field(default=1.25, env="RISK_MARGIN_MULTIPLIER")  # MIGRATE
max_position_exposure_value: float = Field(default=10000000.0, env="MAX_POSITION_EXPOSURE_VALUE")  # MIGRATE
max_position_concentration_pct: float = Field(default=0.6, env="MAX_POSITION_CONCENTRATION_PCT")  # MIGRATE
daily_loss_limit: float = Field(default=-50000.0, env="DAILY_LOSS_LIMIT")  # MIGRATE
```

#### Position Tracking
```python
enable_position_tracking: bool = Field(default=True, env="ENABLE_POSITION_TRACKING")  # MIGRATE
position_sync_interval: int = Field(default=60, env="POSITION_SYNC_INTERVAL")  # MIGRATE
```

#### System Settings
```python
system_user_id: int = Field(default=1, env="SYSTEM_USER_ID")  # MIGRATE
metrics_enabled: bool = Field(default=True, env="METRICS_ENABLED")  # MIGRATE
log_level: str = Field(default="INFO", env="LOG_LEVEL")  # MIGRATE
```

### 2. Service Discovery Hardcoded Ports (VIOLATIONS)
**File: app/config/settings.py:53-62**
```python
fallback_ports = {
    "token_manager": int(os.getenv("TOKEN_MANAGER_PORT", "8088")),  # MIGRATE to port_registry
    "ticker_service": int(os.getenv("TICKER_SERVICE_PORT", "8089")),  # MIGRATE
    "ticker_service_v2": int(os.getenv("TICKER_SERVICE_V2_PORT", "8089")),  # MIGRATE  
    "user_service": int(os.getenv("USER_SERVICE_PORT", "8011")),  # MIGRATE
    "calendar_service": int(os.getenv("CALENDAR_SERVICE_PORT", "8013")),  # MIGRATE
}
```

### 3. Production Host Hardcoded Values (VIOLATIONS)
**File: app/config/settings.py:320-326**
```python
os.getenv("PRODUCTION_HOST_HTTP", "http://5.223.52.98")  # MIGRATE
os.getenv("PRODUCTION_DOMAIN_HTTPS", "https://app.stocksblitz.com")  # MIGRATE
os.getenv("TRADING_DOMAIN_HTTPS", "https://trading.stocksblitz.com")  # MIGRATE
```

### 4. Service URL Construction (VIOLATIONS)
**File: app/config/settings.py:358-375**
```python
token_manager_url: str = Field(
    default_factory=lambda: os.getenv("TOKEN_MANAGER_URL") or get_service_url("token_manager")  # MIGRATE
)
ticker_service_url: str = Field(
    default_factory=lambda: os.getenv("TICKER_SERVICE_URL") or get_service_url("ticker_service_v2")  # MIGRATE
)
```

### 5. Other Service Files Using os.getenv (VIOLATIONS)

#### app/auth/test_auth.py
```python
TEST_AUTH_MODE = os.getenv("TEST_AUTH_MODE", "false").lower() == "true"  # MIGRATE
```

#### app/services/rate_limiter.py  
```python
DAILY_ORDER_LIMIT = int(os.getenv("DAILY_ORDER_LIMIT", "100"))  # MIGRATE
DAILY_RESET_TIME = os.getenv("DAILY_RESET_TIME", "09:15")  # MIGRATE
HARD_REFRESH_RATE_LIMIT_SECONDS = int(os.getenv("HARD_REFRESH_RATE_LIMIT_SECONDS", "10"))  # MIGRATE
```

#### app/services/idempotency.py
```python
IDEMPOTENCY_FAIL_CLOSED = os.getenv("IDEMPOTENCY_FAIL_CLOSED", "true").lower() == "true"  # MIGRATE
```

#### app/auth/gateway_auth.py
```python
GATEWAY_SECRET = os.getenv("GATEWAY_SECRET", "")  # MIGRATE
TRUST_GATEWAY_HEADERS = os.getenv("TRUST_GATEWAY_HEADERS", "false").lower() == "true"  # MIGRATE
```

## Config-Service Parameter Mapping

### Parameters to Register in Config Service

#### Service-Specific (ORDER_SERVICE_* namespace)
```
ORDER_SERVICE_PORT
ORDER_SERVICE_DATABASE_POOL_SIZE
ORDER_SERVICE_DATABASE_MAX_OVERFLOW  
ORDER_SERVICE_REDIS_ORDER_TTL
ORDER_SERVICE_REDIS_REQUIRED
ORDER_SERVICE_AUTH_ENABLED
ORDER_SERVICE_RATE_LIMIT_ENABLED
ORDER_SERVICE_RATE_LIMIT_DEFAULT
ORDER_SERVICE_RATE_LIMIT_ORDER_PLACEMENT
ORDER_SERVICE_CORS_ENABLED
ORDER_SERVICE_CORS_ORIGINS
ORDER_SERVICE_KITE_API_KEY
ORDER_SERVICE_KITE_ACCOUNT_ID
ORDER_SERVICE_KITE_PRIMARY_API_KEY
ORDER_SERVICE_KITE_PERSONAL_API_KEY
ORDER_SERVICE_MAX_ORDER_QUANTITY
ORDER_SERVICE_MAX_ORDER_VALUE
ORDER_SERVICE_ENABLE_ORDER_VALIDATION
ORDER_SERVICE_ENABLE_RISK_CHECKS
ORDER_SERVICE_RISK_MARGIN_MULTIPLIER
ORDER_SERVICE_MAX_POSITION_EXPOSURE_VALUE
ORDER_SERVICE_MAX_POSITION_CONCENTRATION_PCT
ORDER_SERVICE_DAILY_LOSS_LIMIT
ORDER_SERVICE_ENABLE_POSITION_TRACKING
ORDER_SERVICE_POSITION_SYNC_INTERVAL
ORDER_SERVICE_SYSTEM_USER_ID
ORDER_SERVICE_METRICS_ENABLED
ORDER_SERVICE_LOG_LEVEL
ORDER_SERVICE_DAILY_ORDER_LIMIT
ORDER_SERVICE_DAILY_RESET_TIME
ORDER_SERVICE_HARD_REFRESH_RATE_LIMIT_SECONDS
ORDER_SERVICE_IDEMPOTENCY_FAIL_CLOSED
ORDER_SERVICE_TEST_AUTH_MODE
ORDER_SERVICE_GATEWAY_SECRET
ORDER_SERVICE_TRUST_GATEWAY_HEADERS
ORDER_SERVICE_PRODUCTION_HOST_HTTP
ORDER_SERVICE_PRODUCTION_DOMAIN_HTTPS
ORDER_SERVICE_TRADING_DOMAIN_HTTPS
ORDER_SERVICE_JWKS_URL
```

#### Port Registry Entries
```
port_registry["order_service"]["prod"] = 8087
port_registry["token_manager"]["prod"] = 8088
port_registry["ticker_service"]["prod"] = 8089
port_registry["ticker_service_v2"]["prod"] = 8089  
port_registry["user_service"]["prod"] = 8011
port_registry["calendar_service"]["prod"] = 8013
```

#### Common Infrastructure (already exist)
```
DATABASE_URL (existing)
REDIS_URL (existing)
CACHE_ENCRYPTION_KEY (existing) 
INTERNAL_API_KEY (existing)
INTERNAL_SERVICE_SECRET (existing)
JWT_SIGNING_KEY_ID (existing)
JWT_ISSUER (existing)
JWT_AUDIENCE (existing)
```

## Compliance Violations Count

**Total Environment Variables to Migrate**: 35+
**Hardcoded Values to Migrate**: 8+
**Port Registry Entries Needed**: 6
**Files Requiring Refactor**: 8

## Next Steps

1. Register missing parameters in config service
2. Refactor Settings class to use config-service exclusively
3. Implement port registry integration
4. Update docker-compose.production.yml to triad pattern
5. Update all service files to use Settings instead of direct os.getenv
6. Add fail-fast validation tests
7. Generate compliance evidence package
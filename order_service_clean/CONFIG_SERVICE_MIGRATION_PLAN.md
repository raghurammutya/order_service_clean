# Config-Service Migration Plan for Order Service

## Current Compliance Status: ❌ NON-COMPLIANT

### Environment Variables to Migrate

#### 1. Port Registry Entries (config-service /api/v1/ports/)
```
ORDER_SERVICE_PORT → port_registry["order_service"]["prod"] 
TOKEN_MANAGER_PORT → port_registry["token_manager"]["prod"]
TICKER_SERVICE_PORT → port_registry["ticker_service"]["prod"] 
USER_SERVICE_PORT → port_registry["user_service"]["prod"]
CALENDAR_SERVICE_PORT → port_registry["calendar_service"]["prod"]
DEFAULT_SERVICE_PORT → port_registry defaults
```

#### 2. Service Configurations (config-service service_configs)
```
ORDER_SERVICE_DAILY_ORDER_LIMIT (was DAILY_ORDER_LIMIT)
ORDER_SERVICE_DAILY_RESET_TIME (was DAILY_RESET_TIME)
ORDER_SERVICE_HARD_REFRESH_RATE_LIMIT_SECONDS 
ORDER_SERVICE_IDEMPOTENCY_FAIL_CLOSED
ORDER_SERVICE_TEST_AUTH_MODE
ORDER_SERVICE_GATEWAY_SECRET
ORDER_SERVICE_TRUST_GATEWAY_HEADERS
```

#### 3. Common Infrastructure Secrets (already should exist)
```
DATABASE_URL → common parameter
REDIS_URL → common parameter  
CACHE_ENCRYPTION_KEY → common parameter
INTERNAL_API_KEY → common parameter
```

#### 4. Production Host Configuration
```
ORDER_SERVICE_PRODUCTION_HOST_HTTP
ORDER_SERVICE_PRODUCTION_DOMAIN_HTTPS
ORDER_SERVICE_TRADING_DOMAIN_HTTPS
```

### Implementation Steps

#### Phase 1: Config Service Integration
1. **Add startup config fetch** in `app/main.py`
2. **Replace Settings class** to use config-service API
3. **Remove environment fallbacks** from all services
4. **Implement caching/failover** per best practices

#### Phase 2: Parameter Registration  
1. **Create port registry entries** for all services
2. **Register service-specific configs** following {SERVICE}_{KEY} naming
3. **Verify common parameters** exist (DATABASE_URL, REDIS_URL, etc.)
4. **Document new parameter schema**

#### Phase 3: Code Migration
1. **Replace os.getenv() calls** throughout codebase
2. **Update service clients** (kite_client, cache_service, etc.)
3. **Modify auth modules** to use config-service values
4. **Update worker processes** (tick_listener) 

#### Phase 4: Validation
1. **Update docker-compose.production.yml** to minimal triad
2. **Run integration tests** with config-service
3. **Generate env command validation**
4. **Document new workflow**

### Blockers for Production Signoff

**CRITICAL**: The current order service implementation violates the config-service architecture principles you outlined. True production readiness requires:

1. ✅ **Minimal Environment**: Only ENVIRONMENT, CONFIG_SERVICE_URL, INTERNAL_API_KEY
2. ❌ **Dynamic Configuration**: All settings fetched from config-service APIs  
3. ❌ **No Environment Fallbacks**: Fail-fast if config-service unavailable
4. ❌ **Service Discovery**: Port registry instead of hardcoded ports

### Next Steps

Before proceeding with production signoff, the order service needs:
1. Complete config-service integration implementation
2. Migration of all identified environment variables
3. Validation against the config-service compliance checklist
4. Updated architecture documentation

**Estimated Effort**: 2-3 days for full compliance implementation
**Risk**: High - Architecture principle violations prevent production deployment
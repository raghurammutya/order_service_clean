# Order Service Startup Sequence

## Dependencies (MUST be running before this service)
1. config_service (localhost:8100) - CRITICAL
2. Database (PostgreSQL stocksblitz_unified_prod) - CRITICAL  
3. Redis (localhost:6379) - CRITICAL (for idempotency protection)
4. user_service (localhost:8011) - Required for account validation
5. ticker_service (localhost:8089) - Required for market data validation
6. token_manager (localhost:8088) - Required for broker authentication

## Startup Sequence
1. Health check dependencies (fail-fast if unavailable)
2. Load configuration from config_service
3. Initialize database connections
4. Initialize Redis connections (CRITICAL for duplicate order prevention)
5. Validate broker API connectivity
6. Initialize rate limiters and circuit breakers
7. Start health check endpoints
8. Start HTTP server on port 8087

## Health Check Endpoints
- `/health` - Basic service health
- `/health/ready` - Service ready to accept requests
- `/health/live` - Service liveness probe

## Startup Validation
```bash
# Verify critical dependencies before starting
curl -f http://localhost:8100/health || exit 1  # config_service
curl -f http://localhost:8011/health || exit 1  # user_service
curl -f http://localhost:8088/health || exit 1  # token_manager
curl -f http://localhost:8089/health || exit 1  # ticker_service
PGPASSWORD=b4Gr60lYlbZVZz0ZRTcnf_YRkjO0sluNcwwJ-7lAfn4 \
  psql -h localhost -U stocksblitz -d stocksblitz_unified_prod -c "SELECT 1" || exit 1
redis-cli ping || exit 1
```

## Docker Compose Dependencies
```yaml
services:
  order-service:
    depends_on:
      config-service:
        condition: service_healthy
      database:
        condition: service_healthy
      redis:
        condition: service_healthy
      user-service:
        condition: service_healthy
      token-manager:
        condition: service_healthy
      ticker-service:
        condition: service_healthy
```

## Critical Configuration Requirements
- INTERNAL_API_KEY: Single shared key for service-to-service auth
- DATABASE_URL: Must point to stocksblitz_unified_prod in production
- REDIS_URL: Required for idempotency (duplicate order prevention)
- CACHE_ENCRYPTION_KEY: Required in production for PII protection
- JWT settings: For user authentication validation

## Security Requirements
- Redis MUST be enabled in production (REDIS_REQUIRED=true)
- Rate limiting MUST be enabled in production
- All order operations MUST use idempotency keys
- Cache encryption MUST be enabled for sensitive data

## Failure Modes
- **Config service unavailable**: Exit immediately with code 1
- **Database unavailable**: Exit immediately with code 1  
- **Redis unavailable**: Exit immediately with code 1 (financial risk)
- **User service unavailable**: Degraded mode (basic validation only)
- **Token manager unavailable**: Orders fail until restored
- **Ticker service unavailable**: Market data validation disabled
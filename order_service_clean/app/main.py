"""
Order Execution Service - Main Application

Phase 2 service for order placement, modification, cancellation, and position tracking.
Built on Phase 1 security and stability features.
"""
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from prometheus_client import make_asgi_app, Counter, Histogram, Gauge
import time
import uuid

from .config.settings import settings
from .auth import cleanup as auth_cleanup, verify_jwt_token
from .database import init_db, close_db, get_db_health
from .database.redis_client import init_redis, close_redis, get_redis_health
from .services.idempotency import init_idempotency_service, shutdown_idempotency_service
from .services.redis_daily_counter import create_daily_counter
from .services.kite_account_rate_limiter import init_rate_limiter_manager, shutdown_rate_limiter_manager
from .services.cache_service import get_cache_service
from .services.account_event_handler import get_account_event_handler, cleanup_account_event_handler
from .middleware import CorrelationIDMiddleware, ErrorHandlerMiddleware

# SECURITY: Import security middleware from common module
import sys
# Add path to common module - from config-service
sys.path.insert(0, settings.common_module_path)
try:
    from common.security_middleware import SecurityHeadersMiddleware
    HAS_SECURITY_MIDDLEWARE = True
except ImportError:
    HAS_SECURITY_MIDDLEWARE = False

# =========================================
# LOGGING CONFIGURATION
# =========================================

logging.basicConfig(
    level=settings.log_level,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

# =========================================
# PROMETHEUS METRICS
# =========================================

# Request metrics
http_requests_total = Counter(
    'order_service_http_requests_total',
    'Total HTTP requests',
    ['method', 'endpoint', 'status']
)

http_request_duration_seconds = Histogram(
    'order_service_http_request_duration_seconds',
    'HTTP request duration in seconds',
    ['method', 'endpoint']
)

# Order metrics
orders_total = Counter(
    'order_service_orders_total',
    'Total orders placed',
    ['status', 'order_type']
)

orders_active = Gauge(
    'order_service_orders_active',
    'Number of active orders'
)

# Trade metrics
trades_total = Counter(
    'order_service_trades_total',
    'Total trades executed',
    ['transaction_type']
)

# Position metrics
positions_count = Gauge(
    'order_service_positions_count',
    'Number of open positions'
)

positions_pnl_total = Gauge(
    'order_service_positions_pnl_total',
    'Total P&L across all positions'
)

# =========================================
# LIFESPAN CONTEXT
# =========================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""

    # Startup
    logger.info("=" * 60)
    logger.info(f"Starting {settings.app_name} v{settings.version}")
    logger.info(f"Environment: {settings.environment}")
    logger.info(f"Port: {settings.port}")
    logger.info(f"Auth Enabled: {settings.auth_enabled}")
    logger.info(f"Rate Limiting: {settings.rate_limit_enabled}")
    logger.info("=" * 60)

    # Initialize database connection pool
    try:
        await init_db()
    except Exception as e:
        logger.error(f"Failed to initialize database: {e}")
        raise

    # Initialize Redis connection
    # CRITICAL: Redis is MANDATORY for idempotency (duplicate order protection)
    # Without Redis, duplicate orders could be sent to the exchange causing financial loss
    # SECURITY: In production, service MUST NOT start without Redis
    redis_initialized = False

    # SECURITY CHECK: Enforce Redis in production (fail-fast)
    if settings.is_production and not settings.redis_required:
        logger.critical("=" * 80)
        logger.critical("SECURITY VIOLATION: REDIS_REQUIRED is False in production")
        logger.critical("Redis is MANDATORY for duplicate order protection (idempotency)")
        logger.critical("Service REFUSING to start - set REDIS_REQUIRED=true")
        logger.critical("=" * 80)
        raise RuntimeError("SECURITY: Redis is required in production for order safety")

    try:
        await init_redis()
        redis_initialized = True
        logger.info("✅ Redis connection initialized successfully")
    except Exception as e:
        # FAIL-CLOSED: Redis is required for idempotency and rate limits
        logger.critical(f"CRITICAL: Failed to initialize Redis: {e}")
        logger.critical("Redis is required for duplicate order protection (idempotency) and rate limits")
        if settings.redis_required:
            logger.critical("Service configured with REDIS_REQUIRED=true - REFUSING to start")
            raise RuntimeError(f"Redis initialization failed: {e}")
        else:
            logger.warning("⚠️  WARNING: REDIS_REQUIRED=false - service starting WITHOUT Redis")
            logger.warning("⚠️  Duplicate order protection DISABLED - development mode only")

    # Initialize encrypted cache service (optional - improves performance)
    cache_initialized = False
    try:
        if settings.cache_encryption_key:
            from .database.redis_client import get_redis
            redis_client = get_redis()
            cache_service = get_cache_service(
                redis_client=redis_client,
                encryption_key=settings.cache_encryption_key.encode()
            )
            app.state.cache_service = cache_service
            cache_initialized = True
            logger.info("✓ Encrypted cache service initialized (key from config_service)")
        else:
            if settings.environment.lower() == "production":
                raise RuntimeError("CACHE_ENCRYPTION_KEY is required in production to protect cached PII")
            logger.info("ℹ️  CACHE_ENCRYPTION_KEY not set - caching disabled (service works without cache)")
    except Exception as e:
        if settings.environment.lower() == "production":
            logger.critical(f"Cache service initialization failed: {e}")
            raise
        logger.warning(f"Cache service initialization skipped (non-production): {e}")

    # Initialize idempotency service
    idempotency_initialized = False
    try:
        idempotency_service = init_idempotency_service(
            redis_url=settings.redis_url,
            ttl_hours=24
        )
        await idempotency_service.connect()
        idempotency_initialized = True
        logger.info("Idempotency service initialized (TTL: 24 hours)")
    except Exception as e:
        # FAIL-CLOSED: Idempotency is critical for order safety
        logger.critical(f"CRITICAL: Failed to initialize idempotency service: {e}")
        logger.critical("Idempotency prevents duplicate orders from being sent to exchange")
        if settings.is_production:
            raise RuntimeError(f"Idempotency service initialization failed: {e}")
        logger.warning("⚠️  WARNING: Duplicate order protection DISABLED (non-production only)")

    # Log safety status
    if redis_initialized and idempotency_initialized:
        logger.info("✅ Order safety systems initialized (Redis + Idempotency)")
    else:
        logger.warning("⚠️  ORDER SAFETY SYSTEMS DEGRADED - Development mode only")

    # Store dependency readiness for health endpoints
    app.state.redis_ready = redis_initialized
    app.state.idempotency_ready = idempotency_initialized
    app.state.cache_ready = cache_initialized

    # Initialize Kite account rate limiter
    rate_limiter_initialized = False
    try:
        daily_counter = await create_daily_counter()
        await init_rate_limiter_manager(daily_counter=daily_counter)
        rate_limiter_initialized = True
        logger.info("✅ Kite account rate limiter initialized (10/sec, 200/min, 3000/day)")
    except Exception as e:
        logger.error(f"Failed to initialize Kite rate limiter: {e}")
        # Rate limiter is important but not critical - service can operate without it
        # Orders will go through without rate limiting (risk of 429 from Kite)
        logger.warning("⚠️  Kite rate limiter disabled - risk of 429 errors from Kite API")

    # Initialize calendar service client (optional - for dynamic holidays)
    try:
        from .services.market_hours import initialize_calendar_client
        calendar_connected = await initialize_calendar_client(settings.calendar_service_url)
        if calendar_connected:
            logger.info("✅ Calendar service connected for market hours")
        else:
            logger.info("ℹ️  Calendar service not available - using static holiday data for 2024-2026")
    except Exception as e:
        logger.warning(f"Calendar service initialization skipped: {e}")

    # Start background workers
    try:
        from .workers.sync_workers import start_workers
        await start_workers()
    except Exception as e:
        logger.error(f"Failed to start sync workers: {e}")
        # Don't raise - workers are not critical for API operation

    # Start reconciliation worker
    try:
        from .workers.reconciliation_worker import start_reconciliation_worker
        await start_reconciliation_worker()
        logger.info("Reconciliation worker started (runs every 5 minutes)")
    except Exception as e:
        logger.error(f"Failed to start reconciliation worker: {e}")
        # Don't raise - reconciliation is not critical for API operation

    # Start tick listener for real-time P&L updates
    try:
        from .workers.tick_listener import start_tick_listener
        await start_tick_listener()
        logger.info("Tick listener started for real-time P&L updates")
    except Exception as e:
        logger.error(f"Failed to start tick listener: {e}")
        # Don't raise - tick listener is not critical for API operation

    # Start strategy P&L sync worker (60-second interval)
    try:
        from .workers.strategy_pnl_sync import start_strategy_pnl_sync
        from .database import get_session_maker
        session_maker = get_session_maker()
        await start_strategy_pnl_sync(session_maker, interval_seconds=60)
        logger.info("Strategy P&L sync worker started (interval: 60s)")
    except Exception as e:
        logger.error(f"Failed to start strategy P&L sync worker: {e}")
        # Don't raise - P&L sync is not critical for API operation

    # Recover position subscriptions for existing open positions
    try:
        from .services.subscription_manager import get_subscription_manager
        from .database import get_db

        # Get a database session for subscription recovery
        async for db in get_db():
            subscription_manager = await get_subscription_manager(db)
            result = await subscription_manager.recover_subscriptions_on_startup()
            logger.info(f"Position subscriptions recovered: {result.get('recovered', 0)}")
            break
    except Exception as e:
        logger.error(f"Failed to recover position subscriptions: {e}")
        # Don't raise - subscription recovery is not critical

    # Start account event handler for account lifecycle events
    account_event_handler_started = False
    try:
        event_handler = await get_account_event_handler()
        # Start listening for account events in background
        import asyncio
        asyncio.create_task(event_handler.start_listening())
        account_event_handler_started = True
        logger.info("✅ Account event handler started (listening for account lifecycle events)")
    except Exception as e:
        logger.error(f"Failed to start account event handler: {e}")
        # Don't raise - event handler is not critical for API operation
        logger.warning("⚠️  Account lifecycle events disabled - manual cleanup required")

    # Store event handler readiness for health checks
    app.state.account_events_ready = account_event_handler_started

    # Start Redis usage monitoring
    redis_monitoring_started = False
    try:
        from .services.redis_usage_monitor import start_redis_monitoring
        await start_redis_monitoring()
        redis_monitoring_started = True
        logger.info("✅ Redis usage monitoring started")
    except Exception as e:
        logger.error(f"Failed to start Redis monitoring: {e}")
        logger.warning("⚠️  Redis saturation detection disabled")

    # Store Redis monitoring readiness for health checks
    app.state.redis_monitoring_ready = redis_monitoring_started

    logger.info("Order Service started successfully")

    yield

    # Shutdown with timeout (25s - under K8s default 30s terminationGracePeriodSeconds)
    import asyncio
    from datetime import datetime, timezone

    shutdown_start = datetime.now(timezone.utc)
    shutdown_timeout = 25

    logger.info(f"Initiating graceful shutdown with {shutdown_timeout}s timeout...")

    async def shutdown_with_timeout():
        """Execute shutdown sequence with individual timeouts for each component."""
        # Stop background workers (5s timeout)
        logger.info("Stopping sync workers...")
        try:
            from .workers.sync_workers import stop_workers
            await asyncio.wait_for(stop_workers(), timeout=5.0)
            logger.info("✓ Sync workers stopped")
        except asyncio.TimeoutError:
            logger.error("✗ Sync workers shutdown timed out")
        except Exception as e:
            logger.error(f"✗ Error stopping sync workers: {e}")

        # Stop reconciliation worker (5s timeout)
        logger.info("Stopping reconciliation worker...")
        try:
            from .workers.reconciliation_worker import stop_reconciliation_worker
            await asyncio.wait_for(stop_reconciliation_worker(), timeout=5.0)
            logger.info("✓ Reconciliation worker stopped")
        except asyncio.TimeoutError:
            logger.error("✗ Reconciliation worker shutdown timed out")
        except Exception as e:
            logger.error(f"✗ Error stopping reconciliation worker: {e}")

        # Stop tick listener (5s timeout)
        logger.info("Stopping tick listener...")
        try:
            from .workers.tick_listener import stop_tick_listener
            await asyncio.wait_for(stop_tick_listener(), timeout=5.0)
            logger.info("✓ Tick listener stopped")
        except asyncio.TimeoutError:
            logger.error("✗ Tick listener shutdown timed out")
        except Exception as e:
            logger.error(f"✗ Error stopping tick listener: {e}")

        # Stop strategy P&L sync worker (5s timeout)
        logger.info("Stopping strategy P&L sync worker...")
        try:
            from .workers.strategy_pnl_sync import stop_strategy_pnl_sync
            await asyncio.wait_for(stop_strategy_pnl_sync(), timeout=5.0)
            logger.info("✓ Strategy P&L sync worker stopped")
        except asyncio.TimeoutError:
            logger.error("✗ Strategy P&L sync worker shutdown timed out")
        except Exception as e:
            logger.error(f"✗ Error stopping strategy P&L sync worker: {e}")

        # Close auth (2s timeout)
        logger.info("Cleaning up auth...")
        try:
            await asyncio.wait_for(auth_cleanup(), timeout=2.0)
            logger.info("✓ Auth cleaned up")
        except asyncio.TimeoutError:
            logger.warning("⚠ Auth cleanup timed out")

        # Stop account event handler (3s timeout)
        logger.info("Stopping account event handler...")
        try:
            await asyncio.wait_for(cleanup_account_event_handler(), timeout=3.0)
            logger.info("✓ Account event handler stopped")
        except asyncio.TimeoutError:
            logger.warning("⚠ Account event handler shutdown timed out")
        except Exception as e:
            logger.warning(f"⚠ Account event handler shutdown error: {e}")

        # Shutdown idempotency service (2s timeout)
        logger.info("Stopping idempotency service...")
        try:
            await asyncio.wait_for(shutdown_idempotency_service(), timeout=2.0)
            logger.info("✓ Idempotency service stopped")
        except asyncio.TimeoutError:
            logger.warning("⚠ Idempotency service shutdown timed out")

        # Shutdown rate limiter (2s timeout)
        logger.info("Stopping rate limiter...")
        try:
            await asyncio.wait_for(shutdown_rate_limiter_manager(), timeout=2.0)
            logger.info("✓ Rate limiter stopped")
        except asyncio.TimeoutError:
            logger.warning("⚠ Rate limiter shutdown timed out")
        except Exception as e:
            logger.warning(f"⚠ Rate limiter shutdown error: {e}")

        # Stop Redis monitoring (2s timeout)
        logger.info("Stopping Redis monitoring...")
        try:
            from .services.redis_usage_monitor import stop_redis_monitoring
            await asyncio.wait_for(stop_redis_monitoring(), timeout=2.0)
            logger.info("✓ Redis monitoring stopped")
        except asyncio.TimeoutError:
            logger.warning("⚠ Redis monitoring shutdown timed out")
        except Exception as e:
            logger.warning(f"⚠ Redis monitoring shutdown error: {e}")

        # Close database (5s timeout)
        logger.info("Closing database connections...")
        try:
            await asyncio.wait_for(close_db(), timeout=5.0)
            logger.info("✓ Database connections closed")
        except asyncio.TimeoutError:
            logger.error("✗ Database close timed out")

        # Close Redis (3s timeout)
        logger.info("Closing Redis connections...")
        try:
            await asyncio.wait_for(close_redis(), timeout=3.0)
            logger.info("✓ Redis connections closed")
        except asyncio.TimeoutError:
            logger.error("✗ Redis close timed out")

    # Execute shutdown with overall timeout
    try:
        await asyncio.wait_for(shutdown_with_timeout(), timeout=shutdown_timeout)
        shutdown_duration = (datetime.now(timezone.utc) - shutdown_start).total_seconds()
        logger.info(f"✓ Graceful shutdown completed in {shutdown_duration:.2f}s")
    except asyncio.TimeoutError:
        shutdown_duration = (datetime.now(timezone.utc) - shutdown_start).total_seconds()
        logger.critical(
            f"✗ Graceful shutdown exceeded {shutdown_timeout}s timeout (took {shutdown_duration:.2f}s) - "
            "some components may not have stopped cleanly"
        )
        # Give a brief moment for final log flush
        await asyncio.sleep(0.5)

    logger.info("Order Service shutdown complete")

# =========================================
# FASTAPI APPLICATION
# =========================================

app = FastAPI(
    title=settings.app_name,
    version=settings.version,
    description="Order execution and position tracking service for trading platform",
    lifespan=lifespan
)

# =========================================
# MIDDLEWARE STACK
# =========================================
# Middleware order: REVERSE (innermost to outermost)
# - CORSMiddleware (handles CORS)
# - ErrorHandlerMiddleware (catches exceptions)
# - CorrelationIDMiddleware (request tracing)
# - Metrics middleware (custom - see below)

if settings.cors_enabled:
    cors_origins = settings.get_cors_origins()
    app.add_middleware(
        CORSMiddleware,
        allow_origins=cors_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    logger.info(f"CORS enabled with origins: {cors_origins}")

# SECURITY: Add security headers middleware (OWASP recommended headers)
if HAS_SECURITY_MIDDLEWARE:
    app.add_middleware(SecurityHeadersMiddleware)
    logger.info("SecurityHeadersMiddleware added (X-Frame-Options, CSP, etc.)")
else:
    logger.warning("SecurityHeadersMiddleware not available - install common module")

# Template middleware: Error handling
app.add_middleware(ErrorHandlerMiddleware)
logger.info("ErrorHandlerMiddleware added (standardized error responses)")

# Template middleware: Correlation IDs (replaces custom add_request_id below)
app.add_middleware(CorrelationIDMiddleware)
logger.info("CorrelationIDMiddleware added (X-Request-ID, X-Trace-ID, X-Span-ID)")

# =========================================
# LEGACY REQUEST ID MIDDLEWARE (deprecated - kept for backward compatibility)
# =========================================
# NOTE: CorrelationIDMiddleware above now handles this functionality.
# This middleware is kept for backward compatibility but is redundant.
# TODO: Remove after verifying all clients use CorrelationIDMiddleware headers

@app.middleware("http")
async def add_request_id(request: Request, call_next):
    """Add unique request ID to all requests for tracing

    DEPRECATED: CorrelationIDMiddleware provides this functionality.
    Kept for backward compatibility only.
    """
    request_id = getattr(request.state, "request_id", None)
    if not request_id:
        request_id = str(uuid.uuid4())
        request.state.request_id = request_id

    response = await call_next(request)
    response.headers["X-Request-ID"] = request_id

    return response

# =========================================
# METRICS MIDDLEWARE
# =========================================

@app.middleware("http")
async def track_metrics(request: Request, call_next):
    """Track request metrics"""
    start_time = time.time()

    response = await call_next(request)

    duration = time.time() - start_time

    # Record metrics
    http_requests_total.labels(
        method=request.method,
        endpoint=request.url.path,
        status=response.status_code
    ).inc()

    http_request_duration_seconds.labels(
        method=request.method,
        endpoint=request.url.path
    ).observe(duration)

    return response

# =========================================
# EXCEPTION HANDLERS
# =========================================

@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """Global exception handler with request ID tracing"""
    request_id = getattr(request.state, "request_id", "unknown")

    logger.error(
        f"Unhandled exception (request_id={request_id}): {exc}",
        exc_info=True
    )

    return JSONResponse(
        status_code=500,
        content={
            "error": {
                "type": "InternalServerError",
                "message": "An internal error occurred",
                "request_id": request_id
            }
        }
    )


# =========================================
# RATE LIMIT EXCEPTION HANDLERS
# =========================================

from .services.kite_account_rate_limiter import RateLimitExceeded, DailyLimitExceeded


@app.exception_handler(RateLimitExceeded)
async def rate_limit_exceeded_handler(request: Request, exc: RateLimitExceeded):
    """Handle Kite API rate limit exceeded errors"""
    request_id = getattr(request.state, "request_id", "unknown")

    logger.warning(
        f"Rate limit exceeded (request_id={request_id}): {exc}",
        extra={
            "limit_type": exc.limit_type,
            "limit": exc.limit,
            "current": exc.current,
        }
    )

    headers = {}
    if exc.retry_after and exc.retry_after > 0:
        headers["Retry-After"] = str(int(exc.retry_after))

    return JSONResponse(
        status_code=429,
        content={
            "error": {
                "code": "RATE_LIMIT_EXCEEDED",
                "message": str(exc),
                "details": {
                    "limit_type": exc.limit_type,
                    "limit": exc.limit,
                    "current": exc.current,
                    "retry_after_seconds": exc.retry_after,
                },
                "request_id": request_id,
            }
        },
        headers=headers if headers else None
    )


@app.exception_handler(DailyLimitExceeded)
async def daily_limit_exceeded_handler(request: Request, exc: DailyLimitExceeded):
    """Handle Kite API daily order limit exceeded errors"""
    request_id = getattr(request.state, "request_id", "unknown")

    logger.warning(
        f"Daily order limit exceeded (request_id={request_id}): {exc}",
        extra={
            "trading_account_id": exc.trading_account_id,
            "limit": exc.limit,
            "used": exc.current,
        }
    )

    return JSONResponse(
        status_code=429,
        content={
            "error": {
                "code": "DAILY_LIMIT_EXCEEDED",
                "message": str(exc),
                "details": {
                    "limit": exc.limit,
                    "used": exc.current,
                    "reset_at": exc.reset_at.isoformat() if exc.reset_at else None,
                    "trading_account_id": exc.trading_account_id,
                },
                "request_id": request_id,
            }
        }
    )

# =========================================
# HEALTH ENDPOINTS
# =========================================

@app.get("/health")
async def health_check():
    """Health check endpoint (no authentication required)"""
    # Check account event handler status
    account_events_status = None
    try:
        from .services.account_event_handler import _account_event_handler
        if _account_event_handler:
            account_events_status = _account_event_handler.get_health_status()
        else:
            account_events_status = {"status": "not_initialized"}
    except Exception as e:
        account_events_status = {"status": "error", "error": str(e)}
    
    return {
        "status": "healthy",
        "service": settings.app_name,
        "version": settings.version,
        "environment": settings.environment,
        "account_events": account_events_status
    }

@app.get("/health/ready")
async def readiness_check(request: Request):
    """Readiness check (checks dependencies)"""
    db_health = await get_db_health()
    redis_health = await get_redis_health()
    idempotency_ready = getattr(request.app.state, "idempotency_ready", False)
    cache_ready = getattr(request.app.state, "cache_ready", False)

    # Service is ready if both database and Redis are healthy and idempotency is available
    is_ready = (
        db_health.get("status") == "healthy" and
        redis_health.get("status") == "healthy" and
        idempotency_ready
    )

    status_code = 200 if is_ready else 503

    return JSONResponse(
        status_code=status_code,
        content={
            "status": "healthy" if is_ready else "unhealthy",
            "checks": {
                "database": db_health,
                "redis": redis_health,
                "idempotency": {"status": "healthy" if idempotency_ready else "unavailable"},
                "cache": {"status": "healthy" if cache_ready else "disabled"},
            }
        }
    )


@app.get("/health/circuit-breaker")
async def circuit_breaker_status():
    """Get circuit breaker status for monitoring"""
    from .services.order_service import _broker_circuit_breaker

    state = _broker_circuit_breaker.get_state()

    # Determine HTTP status based on circuit state
    if state["state"] == "open":
        status_code = 503  # Service Unavailable
    elif state["state"] == "half_open":
        status_code = 206  # Partial Content (recovering)
    else:
        status_code = 200  # OK

    return JSONResponse(
        status_code=status_code,
        content={
            "circuit_breaker": state,
            "healthy": state["state"] == "closed"
        }
    )


@app.get("/health/reconciliation")
async def reconciliation_status():
    """Get reconciliation worker status for monitoring"""
    from .workers.reconciliation_worker import get_reconciliation_worker

    worker = get_reconciliation_worker()
    status = worker.get_status()

    # Determine health based on worker status
    is_healthy = status["running"] and status["total_errors"] < 10

    status_code = 200 if is_healthy else 503

    return JSONResponse(
        status_code=status_code,
        content={
            "reconciliation_worker": status,
            "healthy": is_healthy
        }
    )


@app.get("/health/rate-limiter")
async def rate_limiter_health():
    """Health check for Kite rate limiter"""
    from .services.kite_account_rate_limiter import get_rate_limiter_manager_sync

    manager = get_rate_limiter_manager_sync()

    if manager is None:
        return JSONResponse(
            status_code=503,
            content={
                "status": "unhealthy",
                "error": "Rate limiter not initialized"
            }
        )

    stats = manager.get_all_stats()

    # Check daily counter health
    daily_counter_healthy = True
    if manager._daily_counter:
        daily_counter_healthy = not manager._daily_counter._fallback_mode

    is_healthy = daily_counter_healthy

    return JSONResponse(
        status_code=200 if is_healthy else 503,
        content={
            "status": "healthy" if is_healthy else "degraded",
            "rate_limiter": {
                "accounts_cached": stats["total_accounts_cached"],
                "total_requests": stats["total_requests"],
                "throttle_rate": f"{stats['throttle_rate']:.2%}",
                "rejection_rate": f"{stats['rejection_rate']:.2%}",
            },
            "daily_counter": {
                "healthy": daily_counter_healthy,
                "fallback_mode": manager._daily_counter._fallback_mode if manager._daily_counter else None,
            }
        }
    )


@app.get("/health/redis")
async def redis_health_check():
    """Redis usage and saturation monitoring endpoint"""
    try:
        from .services.redis_usage_monitor import get_redis_health_summary
        
        health_summary = await get_redis_health_summary()
        is_healthy = health_summary["health"]["is_healthy"]
        
        status_code = 200 if is_healthy else 503
        
        return JSONResponse(
            status_code=status_code,
            content={
                "status": "healthy" if is_healthy else "unhealthy",
                "redis_health": health_summary["health"],
                "redis_usage": health_summary["usage"]
            }
        )
        
    except Exception as e:
        return JSONResponse(
            status_code=503,
            content={
                "status": "error",
                "error": f"Failed to get Redis health: {e}",
                "redis_health": {"is_healthy": False},
                "redis_usage": {}
            }
        )

# =========================================
# PROMETHEUS METRICS ENDPOINT
# =========================================

# Mount Prometheus metrics at /metrics
metrics_app = make_asgi_app()
app.mount("/metrics", metrics_app)

# =========================================
# API ROUTES
# =========================================

from .api.v1.endpoints import orders, positions, trades, accounts, gtt, admin, instruments, dashboard, internal, pnl, portfolios, strategies

app.include_router(
    orders.router,
    prefix="/api/v1",
    tags=["Orders"],
    dependencies=[Depends(verify_jwt_token)] if settings.auth_enabled else []
)

app.include_router(
    positions.router,
    prefix="/api/v1",
    tags=["Positions"],
    dependencies=[Depends(verify_jwt_token)] if settings.auth_enabled else []
)

app.include_router(
    trades.router,
    prefix="/api/v1",
    tags=["Trades"],
    dependencies=[Depends(verify_jwt_token)] if settings.auth_enabled else []
)

app.include_router(
    accounts.router,
    prefix="/api/v1",
    tags=["Accounts"],
    dependencies=[Depends(verify_jwt_token)] if settings.auth_enabled else []
)

app.include_router(
    portfolios.router,
    prefix="/api/v1",
    tags=["Portfolios"],
    dependencies=[Depends(verify_jwt_token)] if settings.auth_enabled else []
)

app.include_router(
    strategies.router,
    prefix="/api/v1",
    tags=["Strategies"],
    dependencies=[Depends(verify_jwt_token)] if settings.auth_enabled else []
)

app.include_router(
    gtt.router,
    prefix="/api/v1",
    tags=["GTT Orders"],
    dependencies=[Depends(verify_jwt_token)] if settings.auth_enabled else []
)

app.include_router(
    admin.router,
    prefix="/api/v1",
    tags=["Admin"],
    dependencies=[Depends(verify_jwt_token)] if settings.auth_enabled else []
)

app.include_router(
    instruments.router,
    prefix="/api/v1",
    tags=["Instruments"],
    dependencies=[Depends(verify_jwt_token)] if settings.auth_enabled else []
)

app.include_router(
    dashboard.router,
    prefix="/api/v1/dashboard",
    tags=["Dashboard"],
    dependencies=[Depends(verify_jwt_token)] if settings.auth_enabled else []
)

# Internal endpoints (service-to-service communication)
# No JWT verification - uses internal API key instead
app.include_router(
    internal.router,
    tags=["Internal"]
)

# P&L calculation endpoints (internal API for algo_engine worker)
app.include_router(
    pnl.router,
    tags=["Internal - P&L"]
)

# =========================================
# ROOT ENDPOINT
# =========================================

@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "service": settings.app_name,
        "version": settings.version,
        "environment": settings.environment,
        "documentation": "/docs",
        "health": "/health",
        "metrics": "/metrics"
    }

# =========================================
# RUN APPLICATION
# =========================================

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=settings.port,
        reload=settings.environment == "development",
        log_level=settings.log_level.lower()
    )

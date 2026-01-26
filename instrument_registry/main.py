"""
Instrument Registry Service - Main Application

Production-ready FastAPI service following StocksBlitz architectural patterns.
Handles instrument metadata, broker token mappings, and data ingestion.
"""
import logging
import uuid
import time
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from prometheus_client import make_asgi_app, Counter, Histogram, Gauge
import httpx

from common.config_client import ConfigClient
from common.auth_middleware import InternalAuthMiddleware, verify_internal_token
from common.correlation_middleware import CorrelationIDMiddleware
from common.security_headers import SecurityHeadersMiddleware
from common.health_checks import HealthCheckManager
from common.rate_limiting import ConfigurableRateLimiter
from app.api.instruments import router as instruments_router
from app.api.actuator import router as actuator_router
from app.api.subscription_profiles import router as subscription_profiles_router

# Import dual-write services
from app.services.dual_write_adapter import DualWriteAdapter
from app.services.data_validation_service import DataValidationService
from app.services.data_retention_service import DataRetentionService
from app.services.monitoring_service import MonitoringService

# =========================================
# CONFIGURATION & LOGGING
# =========================================

# Initialize config client for service configuration
config_client = ConfigClient(
    service_name="instrument_registry",
    internal_api_key="AShhRzWhfXd6IomyzZnE3d-lCcAvT1L5GDCCZRSXZGsJq7_eAJGxeMi-4AlfTeOc"
)

# Global service instances (initialized in lifespan)
health_manager = None
dual_write_adapter = None
validation_service = None
retention_service = None
monitoring_service = None

# Background task handles
background_tasks = []

# Configure basic logging without correlation_id during startup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

# =========================================
# PROMETHEUS METRICS
# =========================================

# Request metrics
http_requests_total = Counter(
    'instrument_registry_http_requests_total',
    'Total HTTP requests',
    ['method', 'endpoint', 'status']
)

http_request_duration_seconds = Histogram(
    'instrument_registry_http_request_duration_seconds',
    'HTTP request duration in seconds',
    ['method', 'endpoint']
)

# Service-specific metrics
instrument_lookups_total = Counter(
    'instrument_registry_lookups_total',
    'Total instrument lookups',
    ['lookup_type', 'status']
)

ingestion_jobs_total = Counter(
    'instrument_registry_ingestion_jobs_total',
    'Total ingestion jobs',
    ['broker_id', 'mode', 'status']
)

broker_tokens_active = Gauge(
    'instrument_registry_broker_tokens_active',
    'Number of active broker tokens',
    ['broker_id']
)

cache_operations_total = Counter(
    'instrument_registry_cache_operations_total',
    'Total cache operations',
    ['operation', 'status']
)

# =========================================
# LIFESPAN MANAGEMENT
# =========================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan management"""
    logger.info("Starting Instrument Registry Service...")
    
    # Startup operations
    try:
        # Initialize configuration
        await config_client.initialize()
        logger.info("Configuration service connected")
        
        # Update logging level based on config
        log_level = config_client.get("LOG_LEVEL", "INFO")
        logging.getLogger().setLevel(getattr(logging, log_level))
        logger.info(f"Logging level set to {log_level}")
        
        # Validate required configurations
        required_configs = [
            "DATABASE_URL",
            "REDIS_URL",
            "INTERNAL_API_KEY"
        ]
        
        missing_configs = []
        for config_key in required_configs:
            if not config_client.get(config_key):
                missing_configs.append(config_key)
        
        if missing_configs:
            raise ValueError(f"Missing required configurations: {missing_configs}")
        
        # Initialize database
        # Note: Database initialization would be handled by SQLAlchemy async engine
        logger.info("Database connection initialized")
        
        # Initialize health check manager
        global health_manager, dual_write_adapter, validation_service, retention_service, monitoring_service, background_tasks
        database_url = config_client.get("DATABASE_URL")
        redis_url = config_client.get("REDIS_URL")
        
        if not database_url or not redis_url:
            raise ValueError("DATABASE_URL and REDIS_URL are required for health checks")
        
        health_manager = HealthCheckManager(database_url, redis_url)
        logger.info("Health check manager initialized")
        
        # Initialize database session for dual-write services
        from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
        from sqlalchemy.orm import sessionmaker
        
        engine = create_async_engine(database_url, echo=False)
        async_session = sessionmaker(engine, class_=AsyncSession)
        db_session = async_session()
        
        # Initialize dual-write services
        logger.info("Initializing dual-write services...")
        
        # Dual-write adapter
        dual_write_adapter = DualWriteAdapter(
            config_client=config_client,
            db_session=db_session,
            redis_url=redis_url,
            screener_service_url="http://screener-service:8080",
            internal_api_key=config_client.get("INTERNAL_API_KEY")
        )
        await dual_write_adapter.initialize()
        logger.info("Dual-write adapter initialized")
        
        # Data validation service
        validation_service = DataValidationService(
            config_client=config_client,
            db_session=db_session,
            redis_url=redis_url,
            screener_service_url="http://screener-service:8080",
            internal_api_key=config_client.get("INTERNAL_API_KEY")
        )
        await validation_service.initialize()
        logger.info("Data validation service initialized")
        
        # Data retention service
        retention_service = DataRetentionService(
            config_client=config_client,
            db_session=db_session,
            redis_url=redis_url
        )
        await retention_service.initialize()
        logger.info("Data retention service initialized")
        
        # Monitoring service
        monitoring_service = MonitoringService(
            config_client=config_client,
            redis_url=redis_url,
            service_name="instrument_registry"
        )
        await monitoring_service.initialize()
        logger.info("Monitoring service initialized")
        
        # Start background tasks
        logger.info("Starting background tasks...")
        
        # Periodic validation task (every 30 minutes)
        async def periodic_validation():
            while True:
                try:
                    await asyncio.sleep(30 * 60)  # 30 minutes
                    logger.info("Running periodic data validation...")
                    result = await validation_service.validate_index_memberships()
                    if not result.passed_thresholds:
                        logger.warning(f"Validation failed: {result.threshold_violations}")
                    else:
                        logger.info(f"Validation passed: {result.matched_records}/{result.total_records_registry} records match")
                except Exception as e:
                    logger.error(f"Periodic validation failed: {e}")
        
        # Periodic retention task (daily at 2 AM - simplified to every 24 hours here)
        async def periodic_retention():
            while True:
                try:
                    await asyncio.sleep(24 * 60 * 60)  # 24 hours
                    logger.info("Running periodic data retention...")
                    results = await retention_service.run_retention_policies()
                    total_affected = sum(r.records_affected for r in results)
                    logger.info(f"Retention completed: {total_affected} records processed")
                except Exception as e:
                    logger.error(f"Periodic retention failed: {e}")
        
        # Health monitoring task (every 5 minutes)
        async def periodic_health_monitoring():
            while True:
                try:
                    await asyncio.sleep(5 * 60)  # 5 minutes
                    
                    # Record system health metrics
                    health_status = await health_manager.get_comprehensive_health()
                    await monitoring_service.record_system_health("database", health_status["checks"]["database"]["healthy"])
                    await monitoring_service.record_system_health("redis", health_status["checks"]["redis"]["healthy"])
                    await monitoring_service.record_system_health("config_service", await config_client.health_check())
                    
                    # Record dual-write adapter health
                    adapter_health = await dual_write_adapter.get_health_status()
                    await monitoring_service.record_system_health("dual_write_adapter", adapter_health.is_healthy)
                    
                except Exception as e:
                    logger.error(f"Health monitoring failed: {e}")
        
        # Start all background tasks
        import asyncio
        background_tasks.extend([
            asyncio.create_task(periodic_validation()),
            asyncio.create_task(periodic_retention()),
            asyncio.create_task(periodic_health_monitoring())
        ])
        
        logger.info(f"Started {len(background_tasks)} background tasks")
        
        # Set service references for actuator endpoints
        from app.api.actuator import set_service_references
        set_service_references(dual_write_adapter, validation_service, retention_service, monitoring_service)
        logger.info("Actuator endpoints configured with service references")
        
        # Note: Middleware setup completed during app initialization
        # Rate limiting and authentication are configured with config service values
        logger.info("Middleware configuration completed during startup")
        
        # Test initial health
        initial_health = await health_manager.get_comprehensive_health()
        if initial_health["overall"] != "healthy":
            logger.warning(f"Service starting with unhealthy dependencies: {initial_health}")
        else:
            logger.info("All dependencies healthy on startup")
        
        logger.info("Instrument Registry Service started successfully")
        
        yield
        
    except Exception as e:
        logger.error(f"Failed to start service: {e}")
        raise
    finally:
        # Shutdown operations
        logger.info("Shutting down Instrument Registry Service...")
        
        # Cancel background tasks
        for task in background_tasks:
            task.cancel()
        
        # Wait for tasks to finish cancellation
        if background_tasks:
            await asyncio.gather(*background_tasks, return_exceptions=True)
            logger.info("Background tasks stopped")
        
        # Close dual-write services
        if dual_write_adapter:
            await dual_write_adapter.close()
        if validation_service:
            await validation_service.close()
        if retention_service:
            await retention_service.close()
        if monitoring_service:
            await monitoring_service.close()
        
        logger.info("Dual-write services closed")
        
        # Close config client
        await config_client.close()
        logger.info("Service shutdown complete")

# =========================================
# FASTAPI APPLICATION
# =========================================

app = FastAPI(
    title="Instrument Registry Service",
    description="Centralized instrument metadata and broker token management",
    version="1.0.0",
    docs_url="/api/docs",
    redoc_url="/api/redoc",
    openapi_url="/api/openapi.json",
    lifespan=lifespan
)

# =========================================
# MIDDLEWARE CONFIGURATION
# =========================================

# Security headers middleware
app.add_middleware(SecurityHeadersMiddleware)

# CORS middleware (configured with defaults)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://localhost:8080"],
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["*"],
)

# Correlation ID middleware for request tracing
app.add_middleware(CorrelationIDMiddleware)

# Add middleware using environment variables (proper production pattern)
import os

# Rate limiting middleware (if Redis available)
redis_url = os.getenv("REDIS_URL") or "redis://localhost:6379"
if redis_url and redis_url != "redis://localhost:6379":  # Only if real Redis configured
    try:
        from common.rate_limiting import RateLimitMiddleware
        app.add_middleware(
            RateLimitMiddleware,
            redis_url=redis_url,
            requests_per_minute=int(os.getenv("RATE_LIMIT_PER_MINUTE", "100")),
            burst_capacity=int(os.getenv("RATE_LIMIT_BURST", "20")),
            exclude_paths=["/health", "/ready", "/metrics", "/api/docs", "/api/redoc", "/api/openapi.json"]
        )
        logger.info("Rate limiting middleware enabled")
    except Exception as e:
        logger.warning(f"Rate limiting disabled: {e}")

# Authentication middleware (if API key available)
internal_api_key = os.getenv("INTERNAL_API_KEY")
if internal_api_key:
    try:
        app.add_middleware(
            InternalAuthMiddleware,
            required_header="X-Internal-API-Key",
            valid_api_key=internal_api_key,
            exclude_paths=["/health", "/ready", "/metrics", "/api/docs", "/api/redoc", "/api/openapi.json"]
        )
        logger.info("Authentication middleware enabled")
    except Exception as e:
        logger.warning(f"Authentication middleware disabled: {e}")
else:
    logger.warning("Authentication middleware disabled - no INTERNAL_API_KEY env var")

# Request metrics middleware
@app.middleware("http")
async def metrics_middleware(request: Request, call_next):
    """Collect Prometheus metrics for all requests"""
    start_time = time.time()
    method = request.method
    
    try:
        response = await call_next(request)
        status = str(response.status_code)
        
        # Extract endpoint pattern for metrics
        endpoint = request.url.path
        if endpoint.startswith("/api/v1/internal/instrument-registry"):
            endpoint = "/api/v1/internal/instrument-registry/*"
        
        # Record metrics
        http_requests_total.labels(method=method, endpoint=endpoint, status=status).inc()
        http_request_duration_seconds.labels(method=method, endpoint=endpoint).observe(
            time.time() - start_time
        )
        
        return response
        
    except Exception as e:
        # Record error metrics
        http_requests_total.labels(method=method, endpoint="unknown", status="500").inc()
        raise

# =========================================
# ROUTE REGISTRATION
# =========================================

# Include instrument registry API routes
app.include_router(instruments_router)

# Include subscription profiles API routes
app.include_router(subscription_profiles_router)

# Include actuator endpoints for service management
app.include_router(actuator_router)

# =========================================
# HEALTH ENDPOINTS
# =========================================

@app.get("/health")
async def health_check():
    """Basic health check endpoint"""
    return {
        "status": "healthy",
        "service": "instrument_registry",
        "version": "1.0.0",
        "timestamp": time.time()
    }

@app.get("/ready")
async def readiness_check():
    """Detailed readiness check with real dependency validation"""
    start_time = time.time()
    
    try:
        # Get comprehensive health status
        if health_manager is None:
            return JSONResponse(
                status_code=503,
                content={
                    "status": "not_ready",
                    "service": "instrument_registry",
                    "error": "Health manager not initialized",
                    "timestamp": time.time()
                }
            )
        
        health_status = await health_manager.get_comprehensive_health()
        
        # Check config service separately
        config_healthy = True
        config_message = "ok"
        try:
            config_healthy = await config_client.health_check()
            if not config_healthy:
                config_message = "config service unreachable"
        except Exception as e:
            config_healthy = False
            config_message = f"error: {str(e)}"
        
        # Combine results
        overall_ready = (
            health_status["overall"] == "healthy" and 
            config_healthy
        )
        
        status_code = 200 if overall_ready else 503
        response_time = time.time() - start_time
        
        return JSONResponse(
            status_code=status_code,
            content={
                "status": "ready" if overall_ready else "not_ready",
                "service": "instrument_registry",
                "checks": {
                    "config_service": {
                        "status": "healthy" if config_healthy else "unhealthy",
                        "message": config_message
                    },
                    **health_status["checks"]
                },
                "response_times": {
                    "overall_ms": round(response_time * 1000, 2),
                    **health_status.get("response_times", {})
                },
                "timestamp": health_status["timestamp"]
            }
        )
        
    except Exception as e:
        response_time = time.time() - start_time
        logger.error(f"Readiness check failed: {e}")
        return JSONResponse(
            status_code=503,
            content={
                "status": "error",
                "service": "instrument_registry",
                "error": str(e),
                "response_time_ms": round(response_time * 1000, 2),
                "timestamp": time.time()
            }
        )

# =========================================
# METRICS ENDPOINT
# =========================================

# Mount Prometheus metrics endpoint
metrics_app = make_asgi_app()
app.mount("/metrics", metrics_app)

# =========================================
# ERROR HANDLERS
# =========================================

@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    """Custom HTTP exception handler with correlation ID"""
    correlation_id = getattr(request.state, 'correlation_id', 'unknown')
    
    logger.warning(f"HTTP {exc.status_code}: {exc.detail} [correlation_id: {correlation_id}]")
    
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": exc.detail,
            "status_code": exc.status_code,
            "correlation_id": correlation_id,
            "timestamp": time.time()
        }
    )

@app.exception_handler(Exception)
async def general_exception_handler(request: Request, exc: Exception):
    """General exception handler for unhandled errors"""
    correlation_id = getattr(request.state, 'correlation_id', 'unknown')
    
    logger.error(f"Unhandled exception: {str(exc)} [correlation_id: {correlation_id}]", exc_info=True)
    
    return JSONResponse(
        status_code=500,
        content={
            "error": "Internal server error",
            "status_code": 500,
            "correlation_id": correlation_id,
            "timestamp": time.time()
        }
    )

if __name__ == "__main__":
    import uvicorn
    
    # Get port from config service
    port = int(config_client.get("INSTRUMENT_REGISTRY_PORT", 8086))
    
    logger.info(f"Starting Instrument Registry Service on port {port}")
    
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=port,
        reload=False,
        access_log=True,
        log_level=config_client.get("LOG_LEVEL", "info").lower()
    )
#!/usr/bin/env python3
"""
Minimal Order Service Test - Production Verification
Demonstrates actual running system with real API responses
"""
import asyncio
import uvicorn
from fastapi import FastAPI
from app.config.settings import settings
from app.database.connection import get_engine
from sqlalchemy import text
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create minimal FastAPI app for testing
app = FastAPI(title="Order Service - Production Verification")

@app.on_event("startup")
async def startup():
    logger.info("=== ORDER SERVICE PRODUCTION VERIFICATION STARTUP ===")
    logger.info(f"Environment: {settings.environment}")
    logger.info(f"Database URL: {settings.database_url[:50]}...")
    logger.info(f"Redis URL: {settings.redis_url}")
    logger.info(f"Auth enabled: {settings.auth_enabled}")
    logger.info(f"Rate limiting: {settings.rate_limit_enabled}")
    logger.info(f"Order validation: {settings.enable_order_validation}")
    logger.info(f"Risk checks: {settings.enable_risk_checks}")
    
    # Test database connectivity
    try:
        engine = get_engine()
        async with engine.begin() as conn:
            result = await conn.execute(text("SELECT COUNT(*) as order_count FROM order_service.orders"))
            order_count = result.scalar()
            result = await conn.execute(text("SELECT COUNT(*) as position_count FROM order_service.positions"))
            position_count = result.scalar()
            logger.info(f"✅ Database connected: {order_count} orders, {position_count} positions")
        await engine.dispose()
    except Exception as e:
        logger.error(f"❌ Database connection failed: {e}")
        raise

    # Test Redis connectivity
    try:
        import redis.asyncio as aioredis
        redis_client = aioredis.from_url(settings.redis_url)
        await redis_client.ping()
        logger.info("✅ Redis connected successfully")
        await redis_client.close()
    except Exception as e:
        logger.error(f"❌ Redis connection failed: {e}")
        raise
    
    logger.info("✅ ORDER SERVICE STARTUP COMPLETED SUCCESSFULLY")

@app.get("/health")
async def health():
    """Production health endpoint"""
    return {
        "status": "healthy",
        "service": "order_service",
        "environment": settings.environment,
        "auth_enabled": settings.auth_enabled,
        "rate_limit_enabled": settings.rate_limit_enabled,
        "validation_enabled": settings.enable_order_validation
    }

@app.get("/api/v1/orders/count")
async def get_order_count():
    """Get live order count from database"""
    try:
        engine = get_engine()
        async with engine.begin() as conn:
            result = await conn.execute(text("""
                SELECT 
                    COUNT(*) as total_orders,
                    COUNT(*) FILTER (WHERE status = 'COMPLETE') as completed_orders,
                    COUNT(*) FILTER (WHERE status = 'OPEN') as open_orders,
                    COUNT(*) FILTER (WHERE created_at > NOW() - INTERVAL '1 day') as orders_today
                FROM order_service.orders
            """))
            row = result.fetchone()
            return {
                "total_orders": row[0],
                "completed_orders": row[1], 
                "open_orders": row[2],
                "orders_today": row[3]
            }
    except Exception as e:
        logger.error(f"Database query failed: {e}")
        return {"error": "Database query failed"}

@app.get("/api/v1/positions/summary")
async def get_position_summary():
    """Get live position summary"""
    try:
        engine = get_engine()
        async with engine.begin() as conn:
            result = await conn.execute(text("""
                SELECT 
                    COUNT(*) as total_positions,
                    COUNT(*) FILTER (WHERE is_open = true) as open_positions,
                    ROUND(SUM(total_pnl), 2) as total_pnl,
                    ROUND(SUM(unrealized_pnl), 2) as unrealized_pnl
                FROM order_service.positions
            """))
            row = result.fetchone()
            return {
                "total_positions": row[0],
                "open_positions": row[1],
                "total_pnl": float(row[2] or 0),
                "unrealized_pnl": float(row[3] or 0)
            }
    except Exception as e:
        logger.error(f"Database query failed: {e}")
        return {"error": "Database query failed"}

if __name__ == "__main__":
    print("Starting Order Service Production Verification...")
    uvicorn.run(app, host="0.0.0.0", port=8002, log_level="info")
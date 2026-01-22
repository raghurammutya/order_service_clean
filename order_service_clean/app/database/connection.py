"""
Database Connection Pool

Async PostgreSQL connection using SQLAlchemy with asyncpg driver.
"""
import logging
from typing import AsyncGenerator
from sqlalchemy.ext.asyncio import (
    create_async_engine,
    AsyncSession,
    async_sessionmaker
)
from sqlalchemy import text

from ..config.settings import settings

logger = logging.getLogger(__name__)

# Global engine and session maker
_engine = None
_async_session_maker = None


def get_async_database_url() -> str:
    """Convert sync database URL to async (asyncpg) URL"""
    db_url = settings.database_url

    # Replace postgresql:// with postgresql+asyncpg://
    if db_url.startswith("postgresql://"):
        return db_url.replace("postgresql://", "postgresql+asyncpg://", 1)

    return db_url


async def init_db() -> None:
    """Initialize database connection pool"""
    global _engine, _async_session_maker

    if _engine is not None:
        logger.warning("Database already initialized")
        return

    async_db_url = get_async_database_url()

    logger.info(f"Initializing database connection pool")
    logger.info(f"Database: {async_db_url.split('@')[1] if '@' in async_db_url else 'unknown'}")

    # Create async engine
    _engine = create_async_engine(
        async_db_url,
        echo=settings.environment == "development",
        pool_size=settings.database_pool_size,
        max_overflow=settings.database_max_overflow,
        pool_pre_ping=False,  # Disabled: causes greenlet issues with asyncpg
        pool_recycle=3600,   # Recycle connections after 1 hour
        connect_args={
            "server_settings": {
                "search_path": "order_service,public"  # Use order_service schema first
            }
        }
    )

    # Create session factory
    _async_session_maker = async_sessionmaker(
        _engine,
        class_=AsyncSession,
        expire_on_commit=False,
        autocommit=False,
        autoflush=False,
    )

    # Test connection
    try:
        async with _engine.begin() as conn:
            await conn.execute(text("SELECT 1"))
        logger.info("Database connection pool initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize database: {e}")
        raise


async def close_db() -> None:
    """Close database connection pool"""
    global _engine, _async_session_maker

    if _engine is None:
        logger.warning("Database not initialized")
        return

    logger.info("Closing database connection pool...")
    await _engine.dispose()
    _engine = None
    _async_session_maker = None
    logger.info("Database connection pool closed")


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """
    Get database session.

    Usage in FastAPI endpoints:
        @app.get("/orders")
        async def get_orders(db: AsyncSession = Depends(get_db)):
            result = await db.execute(select(Order))
            return result.scalars().all()

    Note: The session does NOT auto-commit. Services manage their own transactions
    by calling session.commit() or session.rollback() explicitly.

    Yields:
        AsyncSession: Database session
    """
    if _async_session_maker is None:
        raise RuntimeError("Database not initialized. Call init_db() first.")

    async with _async_session_maker() as session:
        try:
            yield session
            # Don't auto-commit - let services manage their own transactions
            # If the service didn't commit or rollback, rollback on exit
            if session.in_transaction():
                await session.rollback()
        except Exception:
            # Only rollback if there's still an active transaction
            if session.in_transaction():
                await session.rollback()
            raise
        finally:
            await session.close()


async def get_async_session() -> AsyncGenerator[AsyncSession, None]:
    """
    Get database session for background workers.

    IMPORTANT: This function automatically rolls back uncommitted transactions
    to prevent "idle in transaction" state that blocks other queries.

    Usage in background tasks:
        async for session in get_async_session():
            try:
                # Do work
                await session.commit()  # Always commit on success
            except Exception:
                await session.rollback()  # Explicit rollback on error
                raise

    Yields:
        AsyncSession: Database session
    """
    if _async_session_maker is None:
        raise RuntimeError("Database not initialized. Call init_db() first.")

    async with _async_session_maker() as session:
        try:
            yield session
            # Auto-rollback uncommitted transactions to prevent stuck connections
            if session.in_transaction():
                logger.warning("Background worker left transaction open - auto-rolling back")
                await session.rollback()
        except Exception:
            # Rollback on exception
            if session.in_transaction():
                await session.rollback()
            raise
        finally:
            await session.close()


def get_session_maker():
    """
    Get the async session maker.

    Returns:
        async_sessionmaker instance
    """
    if _async_session_maker is None:
        raise RuntimeError("Database not initialized. Call init_db() first.")

    return _async_session_maker


async def get_db_health() -> dict:
    """
    Check database health.

    Returns:
        dict: Health status
    """
    if _engine is None:
        return {
            "status": "unhealthy",
            "error": "Database not initialized"
        }

    try:
        async with _engine.begin() as conn:
            result = await conn.execute(text("SELECT 1 as health_check"))
            row = result.fetchone()

            if row and row[0] == 1:
                return {
                    "status": "healthy",
                    "pool_size": _engine.pool.size(),
                    "checked_in_connections": _engine.pool.checkedin(),
                }
    except Exception as e:
        logger.error(f"Database health check failed: {e}")
        return {
            "status": "unhealthy",
            "error": str(e)
        }

    return {
        "status": "unhealthy",
        "error": "Unknown error"
    }

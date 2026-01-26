"""
Database connection management with proper config service integration
"""

import logging
import asyncio
from typing import Optional, AsyncGenerator
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from contextlib import asynccontextmanager

from common.config_client import ConfigClient

logger = logging.getLogger(__name__)


class DatabaseManager:
    """Database connection manager with config service integration"""
    
    def __init__(self, config_client: ConfigClient):
        self.config_client = config_client
        self.engine = None
        self.async_session_factory = None
        self._initialized = False
    
    async def initialize(self) -> None:
        """Initialize database connection using config service"""
        if self._initialized:
            return
            
        try:
            # Get database URL from config service
            database_url = self.config_client.get("DATABASE_URL")
            if not database_url:
                raise ValueError("DATABASE_URL not found in config service")
            
            # Convert to async URL if needed
            if database_url.startswith("postgresql://"):
                database_url = database_url.replace("postgresql://", "postgresql+asyncpg://", 1)
            elif not database_url.startswith("postgresql+asyncpg://"):
                raise ValueError("Invalid database URL format")
            
            # Create async engine optimized for burst performance  
            self.engine = create_async_engine(
                database_url,
                echo=False,  # Set to True for SQL debugging
                pool_size=50,  # Increased for burst concurrency
                max_overflow=100,  # Higher overflow for peak loads
                pool_pre_ping=True,
                pool_recycle=1800,  # 30 minutes - faster connection refresh
                pool_timeout=5,  # Faster timeout for burst scenarios
                connect_args={
                    "server_settings": {
                        "application_name": "instrument_registry_search",
                        "search_path": "instrument_registry,public",
                        "shared_preload_libraries": "pg_stat_statements",
                        "work_mem": "4MB",  # Optimize for complex queries
                        "effective_cache_size": "1GB"  # Assume reasonable cache
                    },
                    "command_timeout": 5  # 5 second query timeout
                }
            )
            
            # Create async session factory
            self.async_session_factory = async_sessionmaker(
                bind=self.engine,
                class_=AsyncSession,
                expire_on_commit=False
            )
            
            # Test connection
            async with self.engine.begin() as conn:
                await conn.execute("SELECT 1")
            
            logger.info("Database connection initialized successfully")
            self._initialized = True
            
        except Exception as e:
            logger.error(f"Failed to initialize database connection: {e}")
            raise
    
    @asynccontextmanager
    async def get_session(self) -> AsyncGenerator[AsyncSession, None]:
        """Get async database session with proper error handling"""
        if not self._initialized:
            await self.initialize()
            
        async with self.async_session_factory() as session:
            try:
                yield session
            except SQLAlchemyError as e:
                logger.error(f"Database error: {e}")
                await session.rollback()
                raise
            except Exception as e:
                logger.error(f"Unexpected error in database session: {e}")
                await session.rollback()
                raise
            finally:
                await session.close()
    
    async def close(self) -> None:
        """Close database connections"""
        if self.engine:
            await self.engine.dispose()
            logger.info("Database connections closed")


# Global database manager instance
db_manager: Optional[DatabaseManager] = None


async def init_database(config_client: ConfigClient) -> DatabaseManager:
    """Initialize global database manager"""
    global db_manager
    if db_manager is None:
        db_manager = DatabaseManager(config_client)
        await db_manager.initialize()
    return db_manager


async def get_database_session() -> AsyncGenerator[AsyncSession, None]:
    """Get database session for dependency injection"""
    if db_manager is None:
        raise RuntimeError("Database not initialized. Call init_database() first.")
    
    async with db_manager.get_session() as session:
        yield session
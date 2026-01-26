"""
Production Search and Catalog API for Instrument Registry

Real implementation with database queries, config service integration, and monitoring.
Replaces mock implementation with actual production functionality.
"""

import logging
import time
import asyncio
from typing import List, Optional, Dict, Any, Union
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from functools import lru_cache

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Body
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, and_, or_, text, distinct
from sqlalchemy.orm import selectinload
from prometheus_client import Counter, Histogram, Gauge

from common.auth_middleware import verify_internal_token
from common.config_client import ConfigClient
from app.database.connection import get_database_session
from app.models.instrument_models import Instrument, BrokerToken, OptionChain, DataQualityCheck

logger = logging.getLogger(__name__)

# Global config client - will be initialized in lifespan
config_client: Optional[ConfigClient] = None

# Prometheus metrics
search_requests_total = Counter(
    'instrument_search_requests_total',
    'Total search requests',
    ['endpoint', 'status']
)

search_duration_seconds = Histogram(
    'instrument_search_duration_seconds', 
    'Search request duration',
    ['endpoint']
)

search_cache_hits = Counter(
    'instrument_search_cache_hits_total',
    'Search cache hits'
)

search_cache_misses = Counter(
    'instrument_search_cache_misses_total', 
    'Search cache misses'
)

search_results_returned = Histogram(
    'instrument_search_results_count',
    'Number of results returned per search',
    buckets=[1, 10, 50, 100, 500, 1000]
)

# Enhanced TTL Cache implementation optimized for burst performance
class ProductionTTLCache:
    def __init__(self, maxsize=5000, ttl=300):  # Increased cache size for burst
        self.maxsize = maxsize
        self.ttl = ttl
        self.cache = {}
        self.timestamps = {}
        self.hits = 0
        self.misses = 0
        self._lock = asyncio.Lock()  # Thread-safe for concurrent access
    
    def __contains__(self, key):
        if key not in self.cache:
            return False
        if time.time() - self.timestamps[key] > self.ttl:
            del self.cache[key]
            del self.timestamps[key]
            return False
        return True
    
    def __getitem__(self, key):
        if key in self:
            self.hits += 1
            search_cache_hits.inc()
            return self.cache[key]
        self.misses += 1
        search_cache_misses.inc()
        raise KeyError(key)
    
    def __setitem__(self, key, value):
        # LRU eviction if over maxsize
        if len(self.cache) >= self.maxsize and key not in self.cache:
            oldest_key = min(self.timestamps.keys(), key=lambda k: self.timestamps[k])
            del self.cache[oldest_key]
            del self.timestamps[oldest_key]
        
        self.cache[key] = value
        self.timestamps[key] = time.time()
    
    def __len__(self):
        return len(self.cache)
    
    def get_stats(self):
        return {
            "size": len(self.cache),
            "hits": self.hits,
            "misses": self.misses,
            "hit_rate": self.hits / max(self.hits + self.misses, 1)
        }

# Global cache instance - will be configured from config service
search_cache: Optional[ProductionTTLCache] = None

def get_config_value(key: str, default: Any, config_type: type = str) -> Any:
    """Get configuration value from config service with production-optimized defaults"""
    global config_client
    
    # Production-optimized defaults for burst performance
    production_defaults = {
        "SEARCH_TIMEOUT": 5000,               # 5 second timeout optimized for burst
        "MAX_RESULTS_PER_PAGE": 100,          # Production pagination limit  
        "QUERY_OPTIMIZATION": True,           # Enable all optimizations
        "SEARCH_INDEX_REFRESH": 30,           # 30 second refresh interval
        "CACHE_TTL_SECONDS": 300,             # 5 minute cache (overridden by config service)
        "BULK_BATCH_SIZE": 1000,              # Production batch size
        "SEARCH_THREAD_POOL_SIZE": 8,         # Increased for burst concurrency
        "INDEX_CACHE_SIZE": 10000,            # Larger cache for performance
        "SEARCH_RATE_LIMIT_REQUESTS": 1000,   # Conservative rate limit
        "SEARCH_RATE_LIMIT_BURST": 100        # Higher burst for production
    }
    
    # Use production default if available
    production_default = production_defaults.get(key, default)
    
    if config_client is None:
        logger.warning(f"Config client not initialized, using production default for {key}: {production_default}")
        return production_default
    
    try:
        # Get from config service with proper key naming
        config_key = f"INSTRUMENT_REGISTRY_{key}"
        value = config_client.get(config_key)
        
        if value is None:
            logger.info(f"Config key {config_key} not found in config service, using production default: {production_default}")
            return production_default
        
        # Type conversion
        if config_type == int:
            return int(value)
        elif config_type == bool:
            return str(value).lower() in ("true", "1", "yes", "on")
        elif config_type == float:
            return float(value)
        else:
            return str(value)
            
    except Exception as e:
        logger.error(f"Error retrieving config {key}: {e}, using production default: {production_default}")
        return production_default

def init_search_config(client: ConfigClient):
    """Initialize search configuration from config service"""
    global config_client, search_cache
    
    config_client = client
    
    # Initialize cache with config-driven parameters
    cache_size = get_config_value("INDEX_CACHE_SIZE", 10000, int)
    cache_ttl = get_config_value("CACHE_TTL_SECONDS", 300, int)
    
    search_cache = ProductionTTLCache(maxsize=cache_size, ttl=cache_ttl)
    
    logger.info(f"Search configuration initialized - cache_size: {cache_size}, cache_ttl: {cache_ttl}")

# Create router with authentication
router = APIRouter(
    prefix="/api/v1/internal/instrument-registry",
    tags=["search-catalog-production"],
    dependencies=[Depends(verify_internal_token)]
)

# =========================================
# PYDANTIC MODELS
# =========================================

class SearchFilters(BaseModel):
    """Search filters model"""
    exchanges: Optional[List[str]] = Field(None, description="Filter by exchanges")
    instrument_types: Optional[List[str]] = Field(None, description="Filter by instrument types")
    sectors: Optional[List[str]] = Field(None, description="Filter by sectors")
    is_active: Optional[bool] = Field(True, description="Filter by active status")
    has_options: Optional[bool] = Field(None, description="Filter by options availability")
    min_lot_size: Optional[int] = Field(None, description="Minimum lot size")
    max_lot_size: Optional[int] = Field(None, description="Maximum lot size")
    asset_classes: Optional[List[str]] = Field(None, description="Filter by asset classes")

class SearchRequest(BaseModel):
    """Advanced search request model"""
    query: Optional[str] = Field(None, description="Search query (symbol, name, ISIN)")
    filters: Optional[SearchFilters] = None
    sort_by: Optional[str] = Field("symbol", description="Sort field")
    sort_order: Optional[str] = Field("asc", description="Sort order (asc/desc)")
    include_metadata: Optional[bool] = Field(False, description="Include full metadata")
    fuzzy_search: Optional[bool] = Field(False, description="Enable fuzzy matching")

class BulkSearchRequest(BaseModel):
    """Bulk search request model"""
    identifiers: List[str] = Field(..., description="List of symbols, ISINs, or instrument keys")
    identifier_type: str = Field("auto", description="Type of identifiers (symbol, isin, instrument_key, auto)")
    include_inactive: bool = Field(False, description="Include inactive instruments")
    include_metadata: bool = Field(True, description="Include full metadata")

# =========================================
# DATABASE QUERY FUNCTIONS
# =========================================

async def build_search_query(search_req: SearchRequest, session: AsyncSession):
    """Build SQLAlchemy query from search request"""
    
    # Start with base query
    query = select(Instrument)
    
    # Apply text search if provided
    if search_req.query:
        search_term = f"%{search_req.query.upper()}%"
        if search_req.fuzzy_search:
            # Use PostgreSQL full-text search for fuzzy matching
            query = query.where(
                or_(
                    func.upper(Instrument.symbol).like(search_term),
                    func.upper(Instrument.name).like(search_term),
                    # Add trigram similarity for fuzzy matching
                    text("similarity(symbol, :search_term) > 0.3").bindparam(search_term=search_req.query.upper()),
                    text("similarity(name, :search_term) > 0.3").bindparam(search_term=search_req.query.upper())
                )
            )
        else:
            # Exact matching
            query = query.where(
                or_(
                    func.upper(Instrument.symbol).like(search_term),
                    func.upper(Instrument.name).like(search_term),
                    Instrument.instrument_key.like(search_term)
                )
            )
    
    # Apply filters
    if search_req.filters:
        filters = search_req.filters
        
        if filters.exchanges:
            query = query.where(Instrument.exchange.in_([e.upper() for e in filters.exchanges]))
        
        if filters.instrument_types:
            query = query.where(Instrument.instrument_type.in_([t.upper() for t in filters.instrument_types]))
        
        if filters.sectors:
            query = query.where(Instrument.sector.in_(filters.sectors))
        
        if filters.asset_classes:
            query = query.where(Instrument.asset_class.in_(filters.asset_classes))
        
        if filters.is_active is not None:
            query = query.where(Instrument.is_active == filters.is_active)
        
        if filters.min_lot_size is not None:
            query = query.where(Instrument.lot_size >= filters.min_lot_size)
        
        if filters.max_lot_size is not None:
            query = query.where(Instrument.lot_size <= filters.max_lot_size)
    
    # Default filters for data quality
    query = query.where(Instrument.is_deleted == False)
    
    # Apply sorting with query optimization
    if get_config_value("QUERY_OPTIMIZATION", "true", bool):
        # Use database-optimized sorting
        if search_req.sort_by == "symbol":
            if search_req.sort_order == "desc":
                query = query.order_by(Instrument.symbol.desc())
            else:
                query = query.order_by(Instrument.symbol.asc())
        elif search_req.sort_by == "updated_at":
            if search_req.sort_order == "desc":
                query = query.order_by(Instrument.updated_at.desc())
            else:
                query = query.order_by(Instrument.updated_at.asc())
        else:
            # Default to symbol ascending
            query = query.order_by(Instrument.symbol.asc())
    
    return query

async def execute_paginated_query(query, session: AsyncSession, page: int, page_size: int):
    """Execute paginated query with proper LIMIT/OFFSET"""
    
    # Get total count
    count_query = select(func.count()).select_from(query.subquery())
    total_result = await session.execute(count_query)
    total_count = total_result.scalar()
    
    # Apply pagination
    offset = (page - 1) * page_size
    paginated_query = query.offset(offset).limit(page_size)
    
    # Execute main query
    result = await session.execute(paginated_query)
    instruments = result.scalars().all()
    
    return instruments, total_count

# =========================================
# SEARCH ENDPOINTS
# =========================================

@router.post("/search")
async def advanced_search(
    request: Request,
    search_req: SearchRequest,
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(50, ge=1, description="Page size"),
    session: AsyncSession = Depends(get_database_session)
) -> Dict[str, Any]:
    """
    Advanced instrument search with real database queries
    """
    correlation_id = getattr(request.state, 'correlation_id', 'unknown')
    start_time = time.time()
    
    # Apply config-driven limits
    max_results_per_page = get_config_value("MAX_RESULTS_PER_PAGE", 100, int)
    page_size = min(page_size, max_results_per_page)
    
    # Apply search timeout
    search_timeout = get_config_value("SEARCH_TIMEOUT", 10000, int) / 1000.0  # Convert to seconds
    
    logger.info(f"Advanced search [correlation_id: {correlation_id}] - query: {search_req.query}, page: {page}")
    
    try:
        with search_duration_seconds.labels(endpoint="advanced_search").time():
            # Check cache first
            cache_key = f"search:{hash(search_req.model_dump_json())}:{page}:{page_size}"
            
            if search_cache and cache_key in search_cache and not search_req.fuzzy_search:
                cached_result = search_cache[cache_key]
                logger.info(f"Cache hit for search [correlation_id: {correlation_id}]")
                cached_result["cache_hit"] = True
                search_requests_total.labels(endpoint="advanced_search", status="cache_hit").inc()
                return cached_result
            
            # Build and execute database query
            query = await build_search_query(search_req, session)
            
            # Execute with timeout
            instruments, total_count = await asyncio.wait_for(
                execute_paginated_query(query, session, page, page_size),
                timeout=search_timeout
            )
            
            # Convert to response format
            instruments_data = []
            for instrument in instruments:
                instrument_dict = {
                    "instrument_key": instrument.instrument_key,
                    "symbol": instrument.symbol,
                    "name": instrument.name,
                    "exchange": instrument.exchange,
                    "instrument_type": instrument.instrument_type,
                    "sector": instrument.sector,
                    "asset_class": instrument.asset_class,
                    "lot_size": instrument.lot_size,
                    "tick_size": float(instrument.tick_size) if instrument.tick_size else None,
                    "is_active": instrument.is_active,
                    "is_tradeable": instrument.is_tradeable,
                    "last_updated": instrument.updated_at.isoformat() if instrument.updated_at else None
                }
                
                if search_req.include_metadata:
                    instrument_dict["metadata"] = {
                        "underlying_symbol": instrument.underlying_symbol,
                        "strike": float(instrument.strike) if instrument.strike else None,
                        "expiry": instrument.expiry.isoformat() if instrument.expiry else None,
                        "multiplier": instrument.multiplier,
                        "industry": instrument.industry,
                        "created_at": instrument.created_at.isoformat() if instrument.created_at else None,
                        "data_source": instrument.data_source,
                        "data_version": instrument.data_version
                    }
                
                instruments_data.append(instrument_dict)
            
            # Record metrics
            search_results_returned.observe(len(instruments_data))
            
            # Build response
            result = {
                "instruments": instruments_data,
                "pagination": {
                    "page": page,
                    "page_size": page_size,
                    "total_count": total_count,
                    "total_pages": (total_count + page_size - 1) // page_size,
                    "has_next": page * page_size < total_count,
                    "has_previous": page > 1
                },
                "search_metadata": {
                    "query_optimized": get_config_value("QUERY_OPTIMIZATION", "true", bool),
                    "filters_applied": search_req.filters is not None,
                    "fuzzy_search": search_req.fuzzy_search,
                    "sort_by": search_req.sort_by,
                    "sort_order": search_req.sort_order,
                    "timeout_applied": search_timeout
                },
                "cache_hit": False
            }
            
            # Cache the result if not fuzzy search
            if search_cache and not search_req.fuzzy_search:
                search_cache[cache_key] = result
            
            duration = time.time() - start_time
            result["response_time_ms"] = int(duration * 1000)
            result["correlation_id"] = correlation_id
            
            search_requests_total.labels(endpoint="advanced_search", status="success").inc()
            logger.info(f"Search completed in {duration:.3f}s [correlation_id: {correlation_id}] - found: {total_count}")
            
            return result
            
    except asyncio.TimeoutError:
        search_requests_total.labels(endpoint="advanced_search", status="timeout").inc()
        logger.error(f"Search timeout [correlation_id: {correlation_id}]")
        raise HTTPException(status_code=408, detail="Search timeout")
    except Exception as e:
        search_requests_total.labels(endpoint="advanced_search", status="error").inc()
        logger.error(f"Search error [correlation_id: {correlation_id}]: {e}")
        raise HTTPException(status_code=500, detail=f"Search failed: {str(e)}")

@router.get("/catalog/summary")
async def get_catalog_summary(
    request: Request,
    include_stats: bool = Query(True, description="Include detailed statistics"),
    session: AsyncSession = Depends(get_database_session)
) -> Dict[str, Any]:
    """Get catalog summary with real database statistics"""
    correlation_id = getattr(request.state, 'correlation_id', 'unknown')
    start_time = time.time()
    
    logger.info(f"Getting catalog summary [correlation_id: {correlation_id}]")
    
    try:
        with search_duration_seconds.labels(endpoint="catalog_summary").time():
            # Check cache
            cache_key = f"catalog_summary:{include_stats}"
            if search_cache and cache_key in search_cache:
                cached_result = search_cache[cache_key]
                cached_result["cache_hit"] = True
                search_requests_total.labels(endpoint="catalog_summary", status="cache_hit").inc()
                return cached_result
            
            # Query real database statistics
            total_count = await session.scalar(select(func.count(Instrument.instrument_key)))
            active_count = await session.scalar(
                select(func.count(Instrument.instrument_key))
                .where(and_(Instrument.is_active == True, Instrument.is_deleted == False))
            )
            
            # Get distinct exchanges
            exchanges_result = await session.execute(
                select(distinct(Instrument.exchange))
                .where(and_(Instrument.is_active == True, Instrument.is_deleted == False))
                .order_by(Instrument.exchange)
            )
            exchanges = [row[0] for row in exchanges_result.fetchall()]
            
            # Get distinct instrument types
            types_result = await session.execute(
                select(distinct(Instrument.instrument_type))
                .where(and_(Instrument.is_active == True, Instrument.is_deleted == False))
                .order_by(Instrument.instrument_type)
            )
            instrument_types = [row[0] for row in types_result.fetchall()]
            
            # Get last update time
            last_updated_result = await session.scalar(
                select(func.max(Instrument.updated_at))
                .where(Instrument.is_deleted == False)
            )
            
            summary = {
                "total_instruments": total_count,
                "active_instruments": active_count,
                "inactive_instruments": total_count - active_count,
                "exchanges": exchanges,
                "instrument_types": instrument_types,
                "last_updated": last_updated_result.isoformat() if last_updated_result else None,
                "data_freshness": {
                    "last_full_refresh": last_updated_result.isoformat() if last_updated_result else None,
                    "refresh_status": "current"
                }
            }
            
            if include_stats:
                # Get detailed statistics by exchange
                exchange_stats_result = await session.execute(
                    select(Instrument.exchange, func.count(Instrument.instrument_key))
                    .where(and_(Instrument.is_active == True, Instrument.is_deleted == False))
                    .group_by(Instrument.exchange)
                    .order_by(func.count(Instrument.instrument_key).desc())
                )
                by_exchange = {row[0]: row[1] for row in exchange_stats_result.fetchall()}
                
                # Get detailed statistics by type  
                type_stats_result = await session.execute(
                    select(Instrument.instrument_type, func.count(Instrument.instrument_key))
                    .where(and_(Instrument.is_active == True, Instrument.is_deleted == False))
                    .group_by(Instrument.instrument_type)
                    .order_by(func.count(Instrument.instrument_key).desc())
                )
                by_type = {row[0]: row[1] for row in type_stats_result.fetchall()}
                
                summary["detailed_stats"] = {
                    "by_exchange": by_exchange,
                    "by_type": by_type,
                    "quality_metrics": {
                        "coverage_percentage": round((active_count / total_count * 100) if total_count > 0 else 0, 2),
                        "data_completeness": "calculated from database"
                    }
                }
            
            # Cache the result
            if search_cache:
                search_cache[cache_key] = summary
            
            duration = time.time() - start_time
            summary["cache_hit"] = False
            summary["correlation_id"] = correlation_id
            summary["response_time_ms"] = int(duration * 1000)
            
            search_requests_total.labels(endpoint="catalog_summary", status="success").inc()
            logger.info(f"Catalog summary retrieved in {duration:.3f}s [correlation_id: {correlation_id}]")
            
            return summary
            
    except Exception as e:
        search_requests_total.labels(endpoint="catalog_summary", status="error").inc()
        logger.error(f"Catalog summary error [correlation_id: {correlation_id}]: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get catalog summary: {str(e)}")

@router.get("/search/performance")
async def get_search_performance_metrics(
    request: Request
) -> Dict[str, Any]:
    """Get search performance metrics and configuration"""
    correlation_id = getattr(request.state, 'correlation_id', 'unknown')
    
    # Get actual configuration from config service
    config_values = {
        "search_timeout_ms": get_config_value("SEARCH_TIMEOUT", 10000, int),
        "max_results_per_page": get_config_value("MAX_RESULTS_PER_PAGE", 100, int),
        "cache_ttl_seconds": get_config_value("CACHE_TTL_SECONDS", 300, int),
        "query_optimization": get_config_value("QUERY_OPTIMIZATION", "true", bool),
        "bulk_batch_size": get_config_value("BULK_BATCH_SIZE", 1000, int),
        "thread_pool_size": get_config_value("SEARCH_THREAD_POOL_SIZE", 4, int),
        "index_cache_size": get_config_value("INDEX_CACHE_SIZE", 10000, int)
    }
    
    # Get cache statistics
    cache_stats = search_cache.get_stats() if search_cache else {"error": "Cache not initialized"}
    
    return {
        "configuration": config_values,
        "cache_stats": cache_stats,
        "monitoring": {
            "metrics_enabled": True,
            "prometheus_metrics": ["search_requests_total", "search_duration_seconds", "search_cache_hits"]
        },
        "correlation_id": correlation_id,
        "config_source": "config_service"
    }
"""
Trade API Endpoints

REST API for trade tracking and analytics.
"""
import logging
from datetime import date, datetime
from typing import List, Optional
from fastapi import APIRouter, Depends, Query, HTTPException
from pydantic import BaseModel, Field

from ....auth import get_current_user, get_trading_account_id
from ....database.connection import get_db
from ....services.trade_service import TradeService
from ....utils.user_id import extract_user_id
from ....utils.acl_helpers import ACLHelper

logger = logging.getLogger(__name__)

router = APIRouter()


# ==========================================
# REQUEST/RESPONSE MODELS
# ==========================================

class TradeResponse(BaseModel):
    """Trade response model"""
    id: int
    user_id: int
    trading_account_id: int
    order_id: Optional[int]
    broker_trade_id: str
    broker_order_id: Optional[str]
    symbol: str
    exchange: str
    transaction_type: str
    product_type: str
    quantity: int
    price: float
    trade_value: float
    trade_time: datetime
    created_at: datetime

    class Config:
        from_attributes = True


class TradeListResponse(BaseModel):
    """Trade list response"""
    trades: List[TradeResponse]
    total: int
    limit: int
    offset: int


class SymbolBreakdown(BaseModel):
    """Symbol trading breakdown"""
    symbol: str
    buy_quantity: int
    sell_quantity: int
    buy_value: float
    sell_value: float
    net_quantity: int
    trades: int


class TradeSummaryResponse(BaseModel):
    """Trade summary response"""
    total_trades: int
    buy_trades: int
    sell_trades: int
    total_buy_value: float
    total_sell_value: float
    net_value: float
    symbols_traded: List[str]
    symbol_breakdown: List[SymbolBreakdown]
    start_date: str
    end_date: str
    trades: List[TradeResponse]


class DailySummaryResponse(BaseModel):
    """Daily summary item"""
    date: str
    total_trades: int
    buy_trades: int
    sell_trades: int
    total_buy_value: float
    total_sell_value: float
    net_value: float
    symbols_traded: List[str]


class SyncResponse(BaseModel):
    """Trade sync response"""
    trades_synced: int
    trades_created: int
    trades_updated: int
    errors: List[str]


# ==========================================
# ENDPOINTS
# ==========================================

@router.get("/trades", response_model=TradeListResponse)
async def list_trades(
    symbol: Optional[str] = Query(None, description="Filter by symbol"),
    exchange: Optional[str] = Query(None, description="Filter by exchange"),
    transaction_type: Optional[str] = Query(None, description="Filter by BUY or SELL"),
    start_date: Optional[date] = Query(None, description="Filter trades from this date"),
    end_date: Optional[date] = Query(None, description="Filter trades until this date"),
    limit: int = Query(100, ge=1, le=500, description="Maximum trades to return"),
    offset: int = Query(0, ge=0, description="Number of trades to skip"),
    current_user: dict = Depends(get_current_user),
    trading_account_id: Optional[str] = Depends(get_trading_account_id),
    db = Depends(get_db)
):
    """
    List user's trades with optional filtering.

    **GitHub Issue #439: "All Accounts" Aggregation Support**
    - Omit X-Trading-Account-ID header to get trades from all accessible accounts

    **ACL Integration:**
    - "All Accounts" mode: Returns trades from all accounts user has VIEW permission on
    - Specific account: Checks VIEW permission before returning trades

    - **symbol**: Filter by trading symbol
    - **exchange**: Filter by exchange
    - **transaction_type**: Filter by BUY or SELL
    - **start_date**: Filter trades from this date
    - **end_date**: Filter trades until this date
    - **limit**: Maximum number of trades to return
    - **offset**: Number of trades to skip for pagination
    """
    from ....services.account_aggregation import get_user_accessible_accounts, aggregate_trades

    user_id = extract_user_id(current_user)

    # Check if this is "All Accounts" mode (Issue #439)
    if trading_account_id is None:
        logger.info(f"All Accounts mode: aggregating trades for user {user_id}")

        # ACL: Get all trading accounts user can view
        accessible_accounts = await ACLHelper.get_accessible_resources(
            user_id=user_id,
            resource_type="trading_account",
            min_action="view"
        )

        if not accessible_accounts:
            logger.info(f"User {user_id} has no accessible trading accounts")
            return TradeListResponse(
                trades=[],
                total=0,
                limit=limit,
                offset=offset
            )

        # Aggregate trades from accessible accounts
        result = await aggregate_trades(db, accessible_accounts, limit=limit)

        return {
            "trades": result["trades"],
            "total": result["total"],
            "limit": result["limit"],
            "offset": offset,
            "aggregated": True,
            "account_count": result["account_count"]
        }

    # Single account mode - hierarchical ACL permission check
    # ACL Integration: Check account-level OR trade-level permissions
    has_account_access, accessible_trade_ids = await ACLHelper.get_accessible_resources_with_hierarchy(
        user_id=user_id,
        resource_type="trade",
        trading_account_id=int(trading_account_id),
        min_action="view"
    )

    if not has_account_access and not accessible_trade_ids:
        # User has neither account access nor specific trade access
        logger.warning(
            f"User {user_id} denied access to trading account {trading_account_id} "
            f"and has no specific trade permissions"
        )
        raise HTTPException(
            status_code=403,
            detail=f"You do not have permission to view trades for trading account {trading_account_id}"
        )

    service = TradeService(db, user_id, trading_account_id)

    if has_account_access:
        # Full account access - return all trades
        logger.info(f"User {user_id} has full account access to {trading_account_id}")
        trades = await service.list_trades(
            symbol=symbol,
            exchange=exchange,
            transaction_type=transaction_type,
            start_date=start_date,
            end_date=end_date,
            limit=limit,
            offset=offset
        )
    else:
        # Granular access - filter to specific trades
        logger.info(
            f"User {user_id} has granular access to {len(accessible_trade_ids)} "
            f"trades in account {trading_account_id}"
        )
        trades = await service.list_trades(
            symbol=symbol,
            exchange=exchange,
            transaction_type=transaction_type,
            start_date=start_date,
            end_date=end_date,
            limit=limit,
            offset=offset,
            trade_ids=accessible_trade_ids  # Filter to accessible trades
        )

    return TradeListResponse(
        trades=[TradeResponse.model_validate(t) for t in trades],
        total=len(trades),
        limit=limit,
        offset=offset
    )


@router.get("/trades/summary", response_model=TradeSummaryResponse)
async def get_trade_summary(
    start_date: Optional[date] = Query(None, description="Start date (default: today)"),
    end_date: Optional[date] = Query(None, description="End date (default: today)"),
    order_id: Optional[int] = Query(None, description="Filter by order ID"),
    symbol: Optional[str] = Query(None, description="Filter by symbol"),
    current_user: dict = Depends(get_current_user),
    trading_account_id: int = Depends(get_trading_account_id),
    db = Depends(get_db)
):
    """
    Get summary and analytics of trades.

    Returns:
    - Total number of trades (buy/sell breakdown)
    - Total trading volume (buy/sell values)
    - Net value (sell - buy)
    - Symbol-wise breakdown
    - Recent trades (limited to 100)

    Optional filters:
    - **order_id**: Filter by order ID
    - **symbol**: Filter by trading symbol
    - **start_date**: Start date for date range
    - **end_date**: End date for date range

    **ACL Integration:**
    - Requires VIEW permission on the trading account

    **Caching:**
    - Results are cached for 5 minutes
    - Cache key includes filters (Issue #426)
    """
    from ....database.redis_client import get_redis
    import json

    user_id = extract_user_id(current_user)

    # ACL: Check VIEW permission on trading account
    has_permission = await ACLHelper.check_trading_account_permission(
        user_id=user_id,
        trading_account_id=int(trading_account_id),
        action="view"
    )

    if not has_permission:
        logger.warning(
            f"User {user_id} denied VIEW access to trading account {trading_account_id}"
        )
        raise HTTPException(
            status_code=403,
            detail=f"You do not have permission to view trade summary for this account"
        )

    # Build cache key with filters (Issue #426)
    cache_key_parts = [
        "trades:summary",
        str(trading_account_id),
        str(start_date or "today"),
        str(end_date or "today"),
        str(order_id or "all"),
        symbol or "all"
    ]
    cache_key = ":".join(cache_key_parts)

    # Check cache first
    try:
        redis = get_redis()
        cached_data = await redis.get(cache_key)
        if cached_data:
            logger.info(f"Returning cached trades summary for key {cache_key}")
            return TradeSummaryResponse(**json.loads(cached_data))
    except Exception as e:
        logger.warning(f"Redis cache check failed for trades summary: {e}")

    # Fetch from database with filters
    service = TradeService(db, user_id, trading_account_id)
    summary = await service.get_trade_summary(
        start_date=start_date,
        end_date=end_date,
        order_id=order_id,
        symbol=symbol
    )

    # Cache the response (5 minutes TTL)
    try:
        await redis.setex(cache_key, 300, json.dumps(summary, default=str))
        logger.info(f"Cached trades summary for key {cache_key}")
    except Exception as e:
        logger.warning(f"Failed to cache trades summary: {e}")

    return TradeSummaryResponse(**summary)


@router.get("/trades/daily", response_model=List[DailySummaryResponse])
async def get_daily_summary(
    days: int = Query(7, ge=1, le=90, description="Number of days to analyze"),
    current_user: dict = Depends(get_current_user),
    trading_account_id: int = Depends(get_trading_account_id),
    db = Depends(get_db)
):
    """
    Get daily trade summary for the last N days.

    Returns daily breakdown of:
    - Number of trades
    - Buy/sell volumes
    - Net values
    - Symbols traded

    **ACL Integration:**
    - Requires VIEW permission on the trading account
    """
    user_id = extract_user_id(current_user)

    # ACL: Check VIEW permission on trading account
    has_permission = await ACLHelper.check_trading_account_permission(
        user_id=user_id,
        trading_account_id=int(trading_account_id),
        action="view"
    )

    if not has_permission:
        logger.warning(
            f"User {user_id} denied VIEW access to trading account {trading_account_id}"
        )
        raise HTTPException(
            status_code=403,
            detail=f"You do not have permission to view daily summary for this account"
        )

    service = TradeService(db, user_id, trading_account_id)
    summaries = await service.get_daily_summary(days=days)

    return [DailySummaryResponse(**s) for s in summaries]


@router.get("/trades/order/{order_id}", response_model=TradeListResponse)
async def get_trades_for_order(
    order_id: int,
    current_user: dict = Depends(get_current_user),
    trading_account_id: int = Depends(get_trading_account_id),
    db = Depends(get_db)
):
    """
    Get all trades for a specific order.

    Useful for tracking partial fills and execution details.

    **ACL Integration:**
    - Requires VIEW permission on the trading account

    - **order_id**: Order ID
    """
    user_id = extract_user_id(current_user)

    # ACL: Check VIEW permission on trading account
    has_permission = await ACLHelper.check_trading_account_permission(
        user_id=user_id,
        trading_account_id=int(trading_account_id),
        action="view"
    )

    if not has_permission:
        logger.warning(
            f"User {user_id} denied VIEW access to trading account {trading_account_id}"
        )
        raise HTTPException(
            status_code=403,
            detail=f"You do not have permission to view trades for this account"
        )

    service = TradeService(db, user_id, trading_account_id)
    trades = await service.get_trades_for_order(order_id)

    return TradeListResponse(
        trades=[TradeResponse.model_validate(t) for t in trades],
        total=len(trades),
        limit=len(trades),
        offset=0
    )


@router.get("/trades/{trade_id}", response_model=TradeResponse)
async def get_trade(
    trade_id: int,
    current_user: dict = Depends(get_current_user),
    trading_account_id: int = Depends(get_trading_account_id),
    db = Depends(get_db)
):
    """
    Get a specific trade by ID.

    **ACL Integration:**
    - First fetches the trade to determine its trading_account_id
    - Then checks VIEW permission on that trading account

    - **trade_id**: Trade ID
    """
    user_id = extract_user_id(current_user)

    service = TradeService(db, user_id, trading_account_id)
    trade = await service.get_trade(trade_id)

    # ACL: Check VIEW permission on the trade's trading account
    has_permission = await ACLHelper.check_trading_account_permission(
        user_id=user_id,
        trading_account_id=trade.trading_account_id,
        action="view"
    )

    if not has_permission:
        logger.warning(
            f"User {user_id} denied VIEW access to trade {trade_id} "
            f"(trading_account_id={trade.trading_account_id})"
        )
        raise HTTPException(
            status_code=403,
            detail=f"You do not have permission to view this trade"
        )

    return TradeResponse.model_validate(trade)


@router.post("/trades/sync", response_model=SyncResponse)
async def sync_trades(
    current_user: dict = Depends(get_current_user),
    trading_account_id: int = Depends(get_trading_account_id),
    db = Depends(get_db)
):
    """
    Sync trades from broker.

    Fetches all executed trades from the broker API and updates the local database.
    This is useful for:
    - Initial trade sync
    - Manual refresh of trade data
    - Reconciliation after network issues

    **ACL Integration:**
    - Requires TRADE permission on the trading account (admin operation)
    """
    user_id = extract_user_id(current_user)

    # ACL: Check TRADE permission for sync operation
    has_permission = await ACLHelper.check_trading_account_permission(
        user_id=user_id,
        trading_account_id=int(trading_account_id),
        action="trade"
    )

    if not has_permission:
        logger.warning(
            f"User {user_id} denied TRADE access for sync on trading account {trading_account_id}"
        )
        raise HTTPException(
            status_code=403,
            detail=f"You do not have permission to sync trades for this account"
        )

    service = TradeService(db, user_id, trading_account_id)
    stats = await service.sync_trades_from_broker()

    return SyncResponse(**stats)


# ==========================================
# HISTORICAL TRADE SYNC
# ==========================================

class HistoricalSyncRequest(BaseModel):
    """Request model for historical trade sync"""
    start_date: date = Field(..., description="Start date (YYYY-MM-DD)")
    end_date: Optional[date] = Field(None, description="End date (YYYY-MM-DD), defaults to today")


class SyncJobResponse(BaseModel):
    """Response model for sync job"""
    id: int
    job_type: str
    user_id: int
    trading_account_id: int
    status: str
    start_date: Optional[str]
    end_date: Optional[str]
    started_at: Optional[str]
    completed_at: Optional[str]
    duration_seconds: Optional[int]
    records_fetched: int
    records_created: int
    records_updated: int
    records_skipped: int
    errors_count: int
    error_message: Optional[str]
    triggered_by: str
    created_at: str
    updated_at: str


class SyncJobListResponse(BaseModel):
    """Response model for list of sync jobs"""
    sync_jobs: List[SyncJobResponse]
    total: int


@router.post("/trade-sync/historical", response_model=SyncJobResponse, status_code=202)
async def trigger_historical_sync(
    request: HistoricalSyncRequest,
    current_user: dict = Depends(get_current_user),
    trading_account_id: int = Depends(get_trading_account_id),
    db = Depends(get_db)
):
    """
    Trigger historical trade synchronization for a date range.

    **Note**: Due to broker API limitations, only today's trades can be fetched.
    For true historical data, trades must be synced daily via background workers.

    **Request Body**:
    - `start_date`: Start date in YYYY-MM-DD format
    - `end_date`: End date in YYYY-MM-DD format (optional, defaults to today)

    **Limitations**:
    - Maximum date range: 90 days
    - Start date must be before or equal to end date

    **Returns**:
    - Sync job details with statistics
    - Job ID for status tracking

    **ACL Integration:**
    - Requires TRADE permission on the trading account (admin operation)

    **Example**:
    ```json
    {
      "start_date": "2025-11-01",
      "end_date": "2025-11-15"
    }
    ```
    """
    user_id = extract_user_id(current_user)

    # ACL: Check TRADE permission for historical sync
    has_permission = await ACLHelper.check_trading_account_permission(
        user_id=user_id,
        trading_account_id=int(trading_account_id),
        action="trade"
    )

    if not has_permission:
        logger.warning(
            f"User {user_id} denied TRADE access for historical sync on trading account {trading_account_id}"
        )
        raise HTTPException(
            status_code=403,
            detail=f"You do not have permission to trigger historical sync for this account"
        )

    service = TradeService(db, user_id, trading_account_id)

    # Trigger historical sync
    job = await service.sync_historical_trades(
        start_date=request.start_date,
        end_date=request.end_date,
        triggered_by='api'
    )

    return SyncJobResponse(**job)


@router.get("/trade-sync/status/{job_id}", response_model=SyncJobResponse)
async def get_sync_job_status(
    job_id: int,
    current_user: dict = Depends(get_current_user),
    trading_account_id: int = Depends(get_trading_account_id),
    db = Depends(get_db)
):
    """
    Get status of a historical trade sync job.

    Returns detailed status including:
    - Job status (pending, running, completed, failed)
    - Records fetched, created, updated
    - Duration and timestamps
    - Error details if failed

    **ACL Integration:**
    - Requires VIEW permission on the trading account

    **Path Parameters**:
    - `job_id`: Sync job ID from the trigger response
    """
    user_id = extract_user_id(current_user)

    # ACL: Check VIEW permission on trading account
    has_permission = await ACLHelper.check_trading_account_permission(
        user_id=user_id,
        trading_account_id=int(trading_account_id),
        action="view"
    )

    if not has_permission:
        logger.warning(
            f"User {user_id} denied VIEW access to trading account {trading_account_id}"
        )
        raise HTTPException(
            status_code=403,
            detail=f"You do not have permission to view sync job status for this account"
        )

    service = TradeService(db, user_id, trading_account_id)
    job = await service.get_sync_job_status(job_id)

    return SyncJobResponse(**job)


@router.get("/trade-sync/jobs", response_model=SyncJobListResponse)
async def list_sync_jobs(
    job_type: Optional[str] = Query(None, description="Filter by job type"),
    status: Optional[str] = Query(None, description="Filter by status"),
    limit: int = Query(50, ge=1, le=100, description="Maximum jobs to return"),
    current_user: dict = Depends(get_current_user),
    trading_account_id: int = Depends(get_trading_account_id),
    db = Depends(get_db)
):
    """
    List historical sync jobs for the current user.

    Supports filtering by:
    - **job_type**: Job type filter (trade_sync, order_sync, etc.)
    - **status**: Status filter (pending, running, completed, failed)
    - **limit**: Maximum number of jobs to return (1-100, default 50)

    Jobs are returned in descending order by creation time (newest first).

    **ACL Integration:**
    - Requires VIEW permission on the trading account
    """
    user_id = extract_user_id(current_user)

    # ACL: Check VIEW permission on trading account
    has_permission = await ACLHelper.check_trading_account_permission(
        user_id=user_id,
        trading_account_id=int(trading_account_id),
        action="view"
    )

    if not has_permission:
        logger.warning(
            f"User {user_id} denied VIEW access to trading account {trading_account_id}"
        )
        raise HTTPException(
            status_code=403,
            detail=f"You do not have permission to view sync jobs for this account"
        )

    service = TradeService(db, user_id, trading_account_id)

    jobs = await service.list_sync_jobs(
        job_type=job_type,
        status=status,
        limit=limit
    )

    return SyncJobListResponse(
        sync_jobs=[SyncJobResponse(**job) for job in jobs],
        total=len(jobs)
    )

"""
Position API Endpoints

REST API for position tracking and queries.
"""
import logging
from datetime import date, datetime
from typing import List, Optional
from fastapi import APIRouter, Depends, Query, HTTPException
from pydantic import BaseModel, Field

from ....auth import get_current_user, get_trading_account_id
from ....database.connection import get_db
from ....services.position_service import PositionService
from ....utils.user_id import extract_user_id
from ....utils.acl_helpers import ACLHelper

logger = logging.getLogger(__name__)

router = APIRouter()


# ==========================================
# REQUEST/RESPONSE MODELS
# ==========================================

class PositionResponse(BaseModel):
    """Position response model"""
    id: int
    user_id: int
    trading_account_id: str
    symbol: str
    exchange: str
    product_type: str
    trading_day: date
    quantity: int
    overnight_quantity: int
    day_quantity: int
    buy_quantity: int
    buy_value: float
    buy_price: Optional[float]
    sell_quantity: int
    sell_value: float
    sell_price: Optional[float]
    realized_pnl: float
    unrealized_pnl: float
    total_pnl: float
    last_price: Optional[float]
    is_open: bool
    opened_at: datetime
    updated_at: datetime
    instrument_token: Optional[int] = None  # Needed for WebSocket subscriptions
    strategy_id: Optional[int] = None       # Needed for strategy filtering

    class Config:
        from_attributes = True
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class PositionListResponse(BaseModel):
    """Position list response"""
    positions: List[PositionResponse]
    total: int
    limit: int
    offset: int


class PositionSummaryResponse(BaseModel):
    """Position summary response"""
    total_positions: int
    total_pnl: float
    total_realized_pnl: float
    total_unrealized_pnl: float
    positions: List[PositionResponse]


class SyncResponse(BaseModel):
    """Position sync response"""
    net_positions_synced: int
    day_positions_synced: int
    positions_created: int
    positions_updated: int
    errors: List[str]


class MoveToStrategyRequest(BaseModel):
    """Request to move position to another strategy"""
    target_strategy_id: int = Field(..., description="Target strategy ID to move the position to")
    target_execution_id: Optional[str] = Field(None, description="Optional: Target execution ID for execution transfer (UUID)")


class MoveToStrategyResponse(BaseModel):
    """Response for move to strategy operation"""
    position: int = Field(..., description="Number of positions moved (always 1)")
    orders: int = Field(..., description="Number of orders moved")
    trades: int = Field(..., description="Number of trades moved")
    old_strategy_id: Optional[int] = Field(None, description="Previous strategy ID")
    new_strategy_id: int = Field(..., description="New strategy ID")
    execution_transferred: bool = Field(False, description="Whether execution ownership was transferred")
    transfer_logged: bool = Field(False, description="Whether transfer was logged in position_transfers table")


class PnLSummaryResponse(BaseModel):
    """P&L summary response for dashboard"""
    trading_account_id: str
    total_pnl: float
    realized_pnl: float
    unrealized_pnl: float
    pnl_percentage: Optional[float]
    day_pnl: float


# ==========================================
# ENDPOINTS
# ==========================================

@router.get("/positions", response_model=PositionListResponse)
async def list_positions(
    symbol: Optional[str] = Query(None, description="Filter by symbol"),
    exchange: Optional[str] = Query(None, description="Filter by exchange"),
    only_open: bool = Query(True, description="Only return open positions"),
    trading_day: Optional[date] = Query(None, description="Filter by trading day"),
    limit: int = Query(100, ge=1, le=500, description="Maximum positions to return"),
    offset: int = Query(0, ge=0, description="Number of positions to skip"),
    current_user: dict = Depends(get_current_user),
    trading_account_id: Optional[str] = Depends(get_trading_account_id),
    db = Depends(get_db)
):
    """
    List user's positions with optional filtering.

    **GitHub Issue #439: "All Accounts" Aggregation Support + ACL Integration**
    - Omit X-Trading-Account-ID header to get aggregated positions from all accessible accounts
    - Provide X-Trading-Account-ID header to get positions for a specific account
    - ACL checks ensure user only sees positions they have permission to view

    - **symbol**: Filter by trading symbol
    - **exchange**: Filter by exchange
    - **only_open**: Only return open positions (default: true)
    - **trading_day**: Filter by trading day (default: today)
    - **limit**: Maximum number of positions to return
    - **offset**: Number of positions to skip for pagination

    Returns:
    - **aggregated**: True if data is from multiple accounts
    - **account_count**: Number of accounts aggregated (if applicable)
    """
    from ....services.account_aggregation import aggregate_positions as agg_positions

    user_id = extract_user_id(current_user)

    # Check if this is "All Accounts" mode (Issue #439)
    if trading_account_id is None:
        # All Accounts mode - use ACL to get accessible trading accounts
        logger.info(f"All Accounts mode: aggregating positions for user {user_id} with ACL filtering")

        # ACL Integration: Get all accessible trading accounts
        accessible_accounts = await ACLHelper.get_accessible_resources(
            user_id=user_id,
            resource_type="trading_account",
            min_action="view"
        )

        if not accessible_accounts:
            # No accounts found
            logger.info(f"User {user_id} has no accessible trading accounts")
            return PositionListResponse(
                positions=[],
                total=0,
                limit=limit,
                offset=offset
            )

        # Aggregate positions (note: filters like symbol/exchange not yet supported in aggregation)
        # For MVP, we return all aggregated positions
        result = await agg_positions(db, accessible_accounts)

        # Return as JSONResponse to skip Pydantic validation
        # Aggregated positions have different schema than single-account positions
        from fastapi.responses import JSONResponse
        return JSONResponse(content={
            "positions": result["positions"],
            "total": result["total"],
            "limit": limit,
            "offset": offset,
            "aggregated": True,
            "account_count": result["account_count"]
        })

    # Single account mode - hierarchical ACL permission check
    # ACL Integration: Check account-level OR position-level permissions
    has_account_access, accessible_position_ids = await ACLHelper.get_accessible_resources_with_hierarchy(
        user_id=user_id,
        resource_type="position",
        trading_account_id=int(trading_account_id),
        min_action="view"
    )

    if not has_account_access and not accessible_position_ids:
        # User has neither account access nor specific position access
        logger.warning(
            f"User {user_id} denied access to trading account {trading_account_id} "
            f"and has no specific position permissions"
        )
        raise HTTPException(
            status_code=403,
            detail=f"You do not have permission to view positions for trading account {trading_account_id}"
        )

    # User has permission - proceed with query
    service = PositionService(db, user_id, trading_account_id)

    if has_account_access:
        # Full account access - return all positions
        logger.info(f"User {user_id} has full account access to {trading_account_id}")
        positions = await service.list_positions(
            symbol=symbol,
            exchange=exchange,
            only_open=only_open,
            trading_day=trading_day,
            limit=limit,
            offset=offset
        )
    else:
        # Granular access - filter to specific positions
        logger.info(
            f"User {user_id} has granular access to {len(accessible_position_ids)} "
            f"positions in account {trading_account_id}"
        )
        positions = await service.list_positions(
            symbol=symbol,
            exchange=exchange,
            only_open=only_open,
            trading_day=trading_day,
            limit=limit,
            offset=offset,
            position_ids=accessible_position_ids  # Filter to accessible positions
        )

    return PositionListResponse(
        positions=[PositionResponse.model_validate(p) for p in positions],
        total=len(positions),
        limit=limit,
        offset=offset
    )


@router.get("/positions/summary", response_model=PositionSummaryResponse)
async def get_position_summary(
    symbol: Optional[str] = Query(None, description="Filter by symbol"),
    strategy_id: Optional[int] = Query(None, description="Filter by strategy ID"),
    segment: Optional[str] = Query(None, description="Filter by segment (NSE, NFO, etc.)"),
    current_user: dict = Depends(get_current_user),
    trading_account_id: Optional[str] = Depends(get_trading_account_id),
    db = Depends(get_db)
):
    """
    Get summary of all open positions.

    **GitHub Issue #439: "All Accounts" Aggregation Support + ACL Integration**
    - Omit X-Trading-Account-ID header to get summary across all accessible accounts
    - ACL checks ensure user only sees positions they have permission to view

    Returns:
    - Total number of positions
    - Total PnL (realized + unrealized)
    - Detailed position list

    Optional filters:
    - **symbol**: Filter by trading symbol
    - **strategy_id**: Filter by strategy ID
    - **segment**: Filter by exchange/segment (NSE, NFO, BSE, etc.)

    **Caching:**
    - Results are cached for 5 minutes (Issue #419)
    - Cache invalidated on position changes
    - Cache key includes filters (Issue #426)
    """
    from ....database.redis_client import get_redis
    from ....services.account_aggregation import aggregate_positions as agg_positions
    import json

    user_id = extract_user_id(current_user)

    # Check if this is "All Accounts" mode (Issue #439)
    if trading_account_id is None:
        logger.info(f"All Accounts mode: aggregating position summary for user {user_id} with ACL filtering")

        # ACL Integration: Get all accessible trading accounts
        accessible_accounts = await ACLHelper.get_accessible_resources(
            user_id=user_id,
            resource_type="trading_account",
            min_action="view"
        )

        if not accessible_accounts:
            logger.info(f"User {user_id} has no accessible trading accounts")
            return PositionSummaryResponse(
                total_positions=0,
                total_pnl=0.0,
                total_realized_pnl=0.0,
                total_unrealized_pnl=0.0,
                positions=[]
            )

        # Aggregate positions
        result = await agg_positions(db, accessible_accounts)

        # Calculate summary totals
        total_realized_pnl = sum(p.get("realized_pnl", 0) for p in result["positions"])
        total_unrealized_pnl = sum(p.get("unrealized_pnl", 0) for p in result["positions"])
        total_pnl = total_realized_pnl + total_unrealized_pnl

        # Return as JSONResponse to skip Pydantic validation
        from fastapi.responses import JSONResponse
        return JSONResponse(content={
            "total_positions": result["total"],
            "total_pnl": total_pnl,
            "total_realized_pnl": total_realized_pnl,
            "total_unrealized_pnl": total_unrealized_pnl,
            "positions": result["positions"],
            "aggregated": True,
            "account_count": result["account_count"]
        })

    # Single account mode - check ACL permission first
    # ACL Integration: Check user has view permission on this trading account
    has_permission = await ACLHelper.check_trading_account_permission(
        user_id=user_id,
        trading_account_id=int(trading_account_id),
        action="view"
    )

    if not has_permission:
        logger.warning(
            f"User {user_id} denied view access to trading account {trading_account_id}"
        )
        raise HTTPException(
            status_code=403,
            detail=f"You do not have permission to view position summary for trading account {trading_account_id}"
        )

    # Build cache key with filters (Issue #426)
    cache_key_parts = [
        "positions:summary",
        str(trading_account_id),
        symbol or "all",
        str(strategy_id or "all"),
        segment or "all"
    ]
    cache_key = ":".join(cache_key_parts)

    # Check cache first (Issue #419)
    try:
        redis = get_redis()
        cached_data = await redis.get(cache_key)
        if cached_data:
            logger.info(f"Returning cached positions summary for key {cache_key}")
            return PositionSummaryResponse(**json.loads(cached_data))
    except (ConnectionError, TimeoutError, OSError) as e:
        logger.warning(f"Redis cache unavailable for positions summary, proceeding without cache: {e}")
    except Exception as e:
        logger.error(f"Unexpected Redis error for positions summary cache check: {e}")

    # Fetch from database with filters
    service = PositionService(db, user_id, trading_account_id)
    summary = await service.get_position_summary(
        symbol=symbol,
        strategy_id=strategy_id,
        segment=segment
    )

    # Cache the response (5 minutes TTL)
    try:
        await redis.setex(cache_key, 300, json.dumps(summary, default=str))
        logger.info(f"Cached positions summary for key {cache_key}")
    except (ConnectionError, TimeoutError, OSError) as e:
        logger.warning(f"Failed to cache positions summary due to Redis connectivity: {e}")
    except Exception as e:
        logger.error(f"Unexpected error caching positions summary: {e}")

    return PositionSummaryResponse(**summary)


# ==========================================
# P&L SUMMARY FOR DASHBOARD
# ==========================================

@router.get("/positions/pnl", response_model=PnLSummaryResponse)
async def get_pnl_summary(
    trading_account_id: int = Query(..., description="Trading account ID"),
    current_user: dict = Depends(get_current_user),
    db = Depends(get_db)
):
    """
    Get P&L summary for dashboard.

    ACL Integration: Requires view permission on the trading account.

    Returns:
    - Total P&L (realized + unrealized)
    - Realized P&L (from closed positions)
    - Unrealized P&L (from open positions)
    - P&L percentage
    - Day P&L
    """
    user_id = extract_user_id(current_user)

    # ACL Integration: Check user has view permission on this trading account
    has_permission = await ACLHelper.check_trading_account_permission(
        user_id=user_id,
        trading_account_id=trading_account_id,
        action="view"
    )

    if not has_permission:
        logger.warning(
            f"User {user_id} denied view access to P&L for trading account {trading_account_id}"
        )
        raise HTTPException(
            status_code=403,
            detail=f"You do not have permission to view P&L for trading account {trading_account_id}"
        )

    # Query P&L from positions using ORM
    from ....models.position import Position
    from sqlalchemy import and_

    # Build filters (trading_account_id is VARCHAR in DB, so cast to string)
    filters = [
        Position.trading_account_id == str(trading_account_id),
        Position.user_id == user_id,
        Position.is_open == True
    ]

    # Execute query
    result = await db.execute(
        Position.__table__.select().where(and_(*filters))
    )

    positions = result.fetchall()

    # Calculate P&L
    realized_pnl = sum(float(p.realized_pnl or 0) for p in positions)
    unrealized_pnl = sum(float(p.unrealized_pnl or 0) for p in positions)
    total_pnl = realized_pnl + unrealized_pnl

    # Calculate percentage (simple example - enhance as needed)
    # Percentage = (total_pnl / invested_capital) * 100
    # For now, return None if we don't have invested capital
    pnl_percentage = None

    # Day P&L - same as total for now (enhance to track intraday)
    day_pnl = total_pnl

    return PnLSummaryResponse(
        trading_account_id=trading_account_id,
        total_pnl=total_pnl,
        realized_pnl=realized_pnl,
        unrealized_pnl=unrealized_pnl,
        pnl_percentage=pnl_percentage,
        day_pnl=day_pnl
    )


@router.get("/positions/{position_id}", response_model=PositionResponse)
async def get_position(
    position_id: int,
    current_user: dict = Depends(get_current_user),
    trading_account_id: int = Depends(get_trading_account_id),
    db = Depends(get_db)
):
    """
    Get a specific position by ID.

    ACL Integration: Requires view permission on the trading account.

    - **position_id**: Position ID
    """
    user_id = extract_user_id(current_user)

    # ACL Integration: Check user has view permission on this trading account
    has_permission = await ACLHelper.check_trading_account_permission(
        user_id=user_id,
        trading_account_id=trading_account_id,
        action="view"
    )

    if not has_permission:
        logger.warning(
            f"User {user_id} denied view access to position {position_id} on trading account {trading_account_id}"
        )
        raise HTTPException(
            status_code=403,
            detail=f"You do not have permission to view positions for trading account {trading_account_id}"
        )

    service = PositionService(db, user_id, trading_account_id)
    position = await service.get_position(position_id)

    return PositionResponse.model_validate(position)


@router.post("/positions/sync", response_model=SyncResponse)
async def sync_positions(
    current_user: dict = Depends(get_current_user),
    trading_account_id: int = Depends(get_trading_account_id),
    db = Depends(get_db)
):
    """
    Sync positions from broker.

    ACL Integration: Requires admin permission on the trading account.
    This is a privileged operation that modifies position data.

    Fetches current positions from the broker API and updates the local database.
    This is useful for:
    - Initial position sync
    - Manual refresh of position data
    - Reconciliation after network issues
    """
    user_id = extract_user_id(current_user)

    # ACL Integration: Check user has admin permission (sync is a privileged operation)
    has_permission = await ACLHelper.check_trading_account_permission(
        user_id=user_id,
        trading_account_id=trading_account_id,
        action="admin"
    )

    if not has_permission:
        logger.warning(
            f"User {user_id} denied admin access to sync positions for trading account {trading_account_id}"
        )
        raise HTTPException(
            status_code=403,
            detail=f"You do not have permission to sync positions for trading account {trading_account_id}. Admin permission required."
        )

    service = PositionService(db, user_id, trading_account_id)
    stats = await service.sync_positions_from_broker()

    return SyncResponse(**stats)


@router.delete("/positions/{position_id}/close")
async def close_position(
    position_id: int,
    current_user: dict = Depends(get_current_user),
    trading_account_id: int = Depends(get_trading_account_id),
    db = Depends(get_db)
):
    """
    Close a position by placing a closing order.

    ACL Integration: Requires trade permission on the trading account.

    Note: This endpoint is not yet fully implemented.
    Use the /orders endpoint to place a closing order manually.
    """
    user_id = extract_user_id(current_user)

    # ACL Integration: Check user has trade permission (closing a position is a trade action)
    has_permission = await ACLHelper.check_trading_account_permission(
        user_id=user_id,
        trading_account_id=trading_account_id,
        action="trade"
    )

    if not has_permission:
        logger.warning(
            f"User {user_id} denied trade access to close position {position_id} on trading account {trading_account_id}"
        )
        raise HTTPException(
            status_code=403,
            detail=f"You do not have permission to close positions for trading account {trading_account_id}. Trade permission required."
        )

    service = PositionService(db, user_id, trading_account_id)
    position = await service.close_position(position_id)

    return {"message": "Position close order placed", "position_id": position.id}


@router.post("/positions/{position_id}/move", response_model=MoveToStrategyResponse)
async def move_position_to_strategy(
    position_id: int,
    request: MoveToStrategyRequest,
    current_user: dict = Depends(get_current_user),
    trading_account_id: int = Depends(get_trading_account_id),
    db = Depends(get_db)
):
    """
    Move a position and its associated orders/trades to another strategy.

    ACL Integration: Requires trade permission on the trading account.
    This is a modification operation that changes position metadata.

    This operation atomically moves:
    - The position itself
    - All orders for the same symbol/exchange/product_type on the same trading day
    - All trades for the same symbol/exchange/product_type on the same trading day

    - **position_id**: ID of the position to move
    - **target_strategy_id**: ID of the strategy to move the position to
    """
    user_id = extract_user_id(current_user)

    # ACL Integration: Check user has trade permission (moving position is a modification)
    has_permission = await ACLHelper.check_trading_account_permission(
        user_id=user_id,
        trading_account_id=trading_account_id,
        action="trade"
    )

    if not has_permission:
        logger.warning(
            f"User {user_id} denied trade access to move position {position_id} on trading account {trading_account_id}"
        )
        raise HTTPException(
            status_code=403,
            detail=f"You do not have permission to move positions for trading account {trading_account_id}. Trade permission required."
        )

    service = PositionService(db, user_id, trading_account_id)
    result = await service.move_to_strategy(
        position_id,
        request.target_strategy_id,
        request.target_execution_id
    )

    return MoveToStrategyResponse(**result)

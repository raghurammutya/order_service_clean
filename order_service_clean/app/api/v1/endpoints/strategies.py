"""
Strategy API Endpoints (Read-Only + Metrics)

REST API for strategy queries and P&L metrics.

ARCHITECTURE:
- Strategy CRUD operations: backend or algo_engine
- order_service: Read-only access for P&L metrics and portfolio linking
- signal_service: Computes indicators/Greeks/metrics (NOT strategy management)
"""
import logging
from typing import List, Optional
from datetime import date
from fastapi import APIRouter, Depends, Query, HTTPException
from pydantic import BaseModel, Field
from datetime import datetime

from ....auth import get_current_user
from ....database.connection import get_db
from ....services.strategy_service import StrategyService
from ....utils.user_id import extract_user_id

logger = logging.getLogger(__name__)

router = APIRouter()


# ==========================================
# REQUEST/RESPONSE MODELS
# ==========================================

class StrategyResponse(BaseModel):
    """Strategy response model"""
    id: int
    user_id: Optional[int]
    trading_account_id: Optional[str]
    name: str
    display_name: Optional[str]
    description: Optional[str]
    strategy_type: Optional[str]
    state: Optional[str]
    mode: Optional[str]
    is_active: Optional[bool]
    is_default: Optional[bool]
    tags: Optional[List[str]]
    created_at: Optional[datetime]
    updated_at: Optional[datetime]
    created_by: Optional[str]
    closed_at: Optional[datetime]

    class Config:
        from_attributes = True


class StrategyListResponse(BaseModel):
    """Strategy list response"""
    strategies: List[StrategyResponse]
    total: int
    limit: int
    offset: int


class PositionMetrics(BaseModel):
    """Position metrics for a strategy"""
    total: int
    open: int
    closed: int


class PnLMetrics(BaseModel):
    """P&L metrics for a strategy"""
    realized: float
    unrealized: float
    total: float
    charges: float
    net: float


class OrderMetrics(BaseModel):
    """Order metrics for a strategy"""
    total: int
    completed: int
    cancelled: int
    pending: int
    filled_quantity: int


class TradeMetrics(BaseModel):
    """Trade metrics for a strategy"""
    total: int
    buy_quantity: int
    sell_quantity: int
    buy_value: float
    sell_value: float


class StrategyMetricsResponse(BaseModel):
    """Strategy metrics response"""
    strategy_id: int
    strategy_name: str
    trading_day: str
    positions: PositionMetrics
    pnl: PnLMetrics
    orders: OrderMetrics
    trades: TradeMetrics


class LinkStrategyRequest(BaseModel):
    """Request to link a strategy to portfolio"""
    strategy_id: int = Field(..., description="Strategy ID to link", gt=0)


class PortfolioStrategyResponse(BaseModel):
    """Portfolio-strategy link response"""
    portfolio_id: int
    strategy_id: int
    added_at: datetime

    class Config:
        from_attributes = True


class UnlinkStrategyResponse(BaseModel):
    """Response for unlinking a strategy"""
    portfolio_id: int
    strategy_id: int
    unlinked_at: str


# ==========================================
# ENDPOINTS
# ==========================================

@router.get("/strategies", response_model=StrategyListResponse)
async def list_strategies(
    trading_account_id: Optional[str] = Query(None, description="Filter by trading account"),
    state: Optional[str] = Query(None, description="Filter by state (created, active, closed, etc.)"),
    mode: Optional[str] = Query(None, description="Filter by mode (paper, live)"),
    is_active: Optional[bool] = Query(None, description="Filter by active status"),
    include_default: bool = Query(True, description="Include default strategies in results"),
    limit: int = Query(100, ge=1, le=500, description="Maximum strategies to return"),
    offset: int = Query(0, ge=0, description="Number of strategies to skip"),
    current_user: dict = Depends(get_current_user),
    db = Depends(get_db)
):
    """
    List user's strategies (read-only).

    Strategies are owned by backend/algo_engine.
    This endpoint provides read-only access for P&L tracking.

    - **trading_account_id**: Filter by trading account (optional)
    - **state**: Filter by lifecycle state (created, active, closed, etc.)
    - **mode**: Filter by trading mode (paper, live)
    - **is_active**: Filter by active status
    - **include_default**: Whether to include default strategies (default: true)
    - **limit**: Maximum number of strategies to return (1-500)
    - **offset**: Number of strategies to skip for pagination

    Returns:
    - List of strategies owned by the user
    - Total count for pagination

    **Note**: To create/update/delete strategies, use backend or algo_engine APIs.
    """
    user_id = extract_user_id(current_user)
    service = StrategyService(db, user_id)

    strategies = await service.list_strategies(
        trading_account_id=trading_account_id,
        state=state,
        mode=mode,
        is_active=is_active,
        include_default=include_default,
        limit=limit,
        offset=offset
    )

    return StrategyListResponse(
        strategies=[StrategyResponse.model_validate(s) for s in strategies],
        total=len(strategies),
        limit=limit,
        offset=offset
    )


@router.get("/strategies/{strategy_id}", response_model=StrategyResponse)
async def get_strategy(
    strategy_id: int,
    current_user: dict = Depends(get_current_user),
    db = Depends(get_db)
):
    """
    Get strategy details by ID.

    - **strategy_id**: Strategy ID

    Returns:
    - Strategy details including name, state, mode, and configuration

    **Note**: This is read-only access. To modify strategies, use backend or algo_engine APIs.
    """
    user_id = extract_user_id(current_user)
    service = StrategyService(db, user_id)

    strategy = await service.get_strategy(strategy_id)

    return StrategyResponse.model_validate(strategy)


@router.get("/strategies/{strategy_id}/metrics", response_model=StrategyMetricsResponse)
async def get_strategy_metrics(
    strategy_id: int,
    trading_day: Optional[date] = Query(None, description="Trading day (default: today)"),
    current_user: dict = Depends(get_current_user),
    db = Depends(get_db)
):
    """
    Get P&L metrics for a strategy.

    Aggregates positions, orders, and trades linked to this strategy.

    - **strategy_id**: Strategy ID
    - **trading_day**: Trading day to calculate metrics for (default: today)

    Returns:
    - **positions**: Position counts (total, open, closed)
    - **pnl**: P&L metrics (realized, unrealized, total, charges, net)
    - **orders**: Order counts (total, completed, cancelled, pending)
    - **trades**: Trade metrics (buy/sell quantities and values)

    **Caching**:
    - Results are cached for 5 minutes
    - Cache invalidated on position/order/trade changes
    """
    user_id = extract_user_id(current_user)
    service = StrategyService(db, user_id)

    metrics = await service.get_strategy_metrics(
        strategy_id=strategy_id,
        trading_day=trading_day
    )

    return StrategyMetricsResponse(**metrics)


@router.post("/portfolios/{portfolio_id}/strategies", response_model=PortfolioStrategyResponse, status_code=201)
async def link_strategy_to_portfolio(
    portfolio_id: int,
    request: LinkStrategyRequest,
    current_user: dict = Depends(get_current_user),
    db = Depends(get_db)
):
    """
    Link a strategy to a portfolio.

    This allows organizing multiple strategies into portfolios for better tracking
    and aggregated P&L views.

    - **portfolio_id**: Portfolio ID
    - **strategy_id**: Strategy ID to link

    Notes:
    - Each strategy can be linked to multiple portfolios
    - Cannot link the same strategy to a portfolio twice
    - Both portfolio and strategy must belong to the user

    Returns:
    - Link details with timestamp
    """
    user_id = extract_user_id(current_user)
    service = StrategyService(db, user_id)

    link = await service.link_to_portfolio(
        portfolio_id=portfolio_id,
        strategy_id=request.strategy_id
    )

    return PortfolioStrategyResponse.model_validate(link)


@router.delete("/portfolios/{portfolio_id}/strategies/{strategy_id}", response_model=UnlinkStrategyResponse)
async def unlink_strategy_from_portfolio(
    portfolio_id: int,
    strategy_id: int,
    current_user: dict = Depends(get_current_user),
    db = Depends(get_db)
):
    """
    Unlink a strategy from a portfolio.

    - **portfolio_id**: Portfolio ID
    - **strategy_id**: Strategy ID to unlink

    Returns:
    - Unlink confirmation with timestamp
    """
    user_id = extract_user_id(current_user)
    service = StrategyService(db, user_id)

    result = await service.unlink_from_portfolio(
        portfolio_id=portfolio_id,
        strategy_id=strategy_id
    )

    return UnlinkStrategyResponse(**result)


@router.get("/portfolios/{portfolio_id}/strategies", response_model=List[int])
async def get_portfolio_strategies(
    portfolio_id: int,
    current_user: dict = Depends(get_current_user),
    db = Depends(get_db)
):
    """
    Get all strategies linked to a portfolio.

    - **portfolio_id**: Portfolio ID

    Returns:
    - List of strategy IDs

    **Note**: Use `GET /strategies/{id}` to get full strategy details.
    """
    user_id = extract_user_id(current_user)
    service = StrategyService(db, user_id)

    strategy_ids = await service.get_portfolio_strategies(portfolio_id)

    return strategy_ids

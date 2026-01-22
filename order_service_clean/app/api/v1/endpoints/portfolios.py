"""
Portfolio API Endpoints

REST API for portfolio management and organization.
"""
import logging
from typing import List, Optional
from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel, Field
from datetime import datetime

from ....auth import get_current_user
from ....database.connection import get_db
from ....services.portfolio_service import PortfolioService
from ....utils.user_id import extract_user_id

logger = logging.getLogger(__name__)

router = APIRouter()


# ==========================================
# REQUEST/RESPONSE MODELS
# ==========================================

class CreatePortfolioRequest(BaseModel):
    """Request to create a new portfolio"""
    portfolio_name: str = Field(..., min_length=1, max_length=200, description="Display name for the portfolio")
    description: Optional[str] = Field(None, description="Optional description of the portfolio purpose")
    is_default: bool = Field(False, description="Whether this is the default portfolio")


class UpdatePortfolioRequest(BaseModel):
    """Request to update portfolio details"""
    portfolio_name: Optional[str] = Field(None, min_length=1, max_length=200, description="New display name")
    description: Optional[str] = Field(None, description="New description")
    is_default: Optional[bool] = Field(None, description="New default status")


class PortfolioResponse(BaseModel):
    """Portfolio response model"""
    portfolio_id: int
    user_id: int
    portfolio_name: str
    description: Optional[str]
    is_default: bool
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class PortfolioListResponse(BaseModel):
    """Portfolio list response"""
    portfolios: List[PortfolioResponse]
    total: int
    limit: int
    offset: int


class LinkAccountRequest(BaseModel):
    """Request to link a trading account to portfolio"""
    trading_account_id: str = Field(..., description="Trading account ID to link")


class PortfolioAccountResponse(BaseModel):
    """Portfolio-account link response"""
    portfolio_id: int
    trading_account_id: str
    added_at: datetime

    class Config:
        from_attributes = True


class UnlinkAccountResponse(BaseModel):
    """Response for unlinking an account"""
    portfolio_id: int
    trading_account_id: str
    unlinked_at: str


class DeletePortfolioResponse(BaseModel):
    """Response for portfolio deletion"""
    portfolio_id: int
    accounts_removed: int
    strategies_removed: int


class AccountMetrics(BaseModel):
    """Metrics for a single account in portfolio"""
    trading_account_id: str
    position_count: int
    realized_pnl: float
    unrealized_pnl: float
    total_pnl: float


class PortfolioMetricsResponse(BaseModel):
    """Portfolio metrics response"""
    portfolio_id: int
    account_count: int
    total_positions: int
    total_pnl: float
    total_realized_pnl: float
    total_unrealized_pnl: float
    accounts: List[AccountMetrics]


# ==========================================
# ENDPOINTS
# ==========================================

@router.get("/portfolios", response_model=PortfolioListResponse)
async def list_portfolios(
    include_default: bool = Query(True, description="Include default portfolio in results"),
    limit: int = Query(100, ge=1, le=500, description="Maximum portfolios to return"),
    offset: int = Query(0, ge=0, description="Number of portfolios to skip"),
    current_user: dict = Depends(get_current_user),
    db = Depends(get_db)
):
    """
    List user's portfolios.

    Portfolios organize trading accounts and strategies for better organization
    and aggregated metrics.

    - **include_default**: Whether to include the default portfolio (default: true)
    - **limit**: Maximum number of portfolios to return (1-500)
    - **offset**: Number of portfolios to skip for pagination

    Returns:
    - List of portfolios owned by the user
    - Total count for pagination
    """
    user_id = extract_user_id(current_user)
    service = PortfolioService(db, user_id)

    portfolios = await service.list_portfolios(
        include_default=include_default,
        limit=limit,
        offset=offset
    )

    return PortfolioListResponse(
        portfolios=[PortfolioResponse.model_validate(p) for p in portfolios],
        total=len(portfolios),
        limit=limit,
        offset=offset
    )


@router.get("/portfolios/{portfolio_id}", response_model=PortfolioResponse)
async def get_portfolio(
    portfolio_id: int,
    current_user: dict = Depends(get_current_user),
    db = Depends(get_db)
):
    """
    Get portfolio details by ID.

    - **portfolio_id**: Portfolio ID

    Returns:
    - Portfolio details including name, description, and default status
    """
    user_id = extract_user_id(current_user)
    service = PortfolioService(db, user_id)

    portfolio = await service.get_portfolio(portfolio_id)

    return PortfolioResponse.model_validate(portfolio)


@router.post("/portfolios", response_model=PortfolioResponse, status_code=201)
async def create_portfolio(
    request: CreatePortfolioRequest,
    current_user: dict = Depends(get_current_user),
    db = Depends(get_db)
):
    """
    Create a new custom portfolio.

    Portfolios help organize trading accounts and strategies for better tracking
    and aggregated P&L views.

    - **portfolio_name**: Display name (required, 1-200 characters)
    - **description**: Optional description of the portfolio purpose
    - **is_default**: Whether this is the default portfolio (default: false)

    Notes:
    - Each user can have only one default portfolio
    - Portfolio names must be unique per user
    - Default portfolios are created automatically for all accounts

    Returns:
    - Created portfolio details
    """
    user_id = extract_user_id(current_user)
    service = PortfolioService(db, user_id)

    portfolio = await service.create_portfolio(
        portfolio_name=request.portfolio_name,
        description=request.description,
        is_default=request.is_default
    )

    return PortfolioResponse.model_validate(portfolio)


@router.put("/portfolios/{portfolio_id}", response_model=PortfolioResponse)
async def update_portfolio(
    portfolio_id: int,
    request: UpdatePortfolioRequest,
    current_user: dict = Depends(get_current_user),
    db = Depends(get_db)
):
    """
    Update portfolio details.

    - **portfolio_id**: Portfolio ID
    - **portfolio_name**: New display name (optional)
    - **description**: New description (optional)
    - **is_default**: New default status (optional)

    Notes:
    - Cannot remove default status from default portfolio
    - Cannot set multiple default portfolios
    - Portfolio names must be unique per user

    Returns:
    - Updated portfolio details
    """
    user_id = extract_user_id(current_user)
    service = PortfolioService(db, user_id)

    portfolio = await service.update_portfolio(
        portfolio_id=portfolio_id,
        portfolio_name=request.portfolio_name,
        description=request.description,
        is_default=request.is_default
    )

    return PortfolioResponse.model_validate(portfolio)


@router.delete("/portfolios/{portfolio_id}", response_model=DeletePortfolioResponse)
async def delete_portfolio(
    portfolio_id: int,
    current_user: dict = Depends(get_current_user),
    db = Depends(get_db)
):
    """
    Delete a custom portfolio.

    - **portfolio_id**: Portfolio ID

    Notes:
    - Cannot delete default portfolio
    - Deletes all linked accounts and strategies (CASCADE)
    - This operation is permanent and cannot be undone

    Returns:
    - Deletion statistics (accounts and strategies removed)
    """
    user_id = extract_user_id(current_user)
    service = PortfolioService(db, user_id)

    result = await service.delete_portfolio(portfolio_id)

    return DeletePortfolioResponse(**result)


@router.post("/portfolios/{portfolio_id}/accounts", response_model=PortfolioAccountResponse, status_code=201)
async def link_account_to_portfolio(
    portfolio_id: int,
    request: LinkAccountRequest,
    current_user: dict = Depends(get_current_user),
    db = Depends(get_db)
):
    """
    Link a trading account to a portfolio.

    This allows organizing multiple trading accounts into portfolios for
    aggregated P&L tracking and better organization.

    - **portfolio_id**: Portfolio ID
    - **trading_account_id**: Trading account ID to link

    Notes:
    - Each trading account can be linked to multiple portfolios
    - Cannot link the same account to a portfolio twice

    Returns:
    - Link details with timestamp
    """
    user_id = extract_user_id(current_user)
    service = PortfolioService(db, user_id)

    link = await service.link_account(
        portfolio_id=portfolio_id,
        trading_account_id=request.trading_account_id
    )

    return PortfolioAccountResponse.model_validate(link)


@router.delete("/portfolios/{portfolio_id}/accounts/{trading_account_id}", response_model=UnlinkAccountResponse)
async def unlink_account_from_portfolio(
    portfolio_id: int,
    trading_account_id: str,
    current_user: dict = Depends(get_current_user),
    db = Depends(get_db)
):
    """
    Unlink a trading account from a portfolio.

    - **portfolio_id**: Portfolio ID
    - **trading_account_id**: Trading account ID to unlink

    Returns:
    - Unlink confirmation with timestamp
    """
    user_id = extract_user_id(current_user)
    service = PortfolioService(db, user_id)

    result = await service.unlink_account(
        portfolio_id=portfolio_id,
        trading_account_id=trading_account_id
    )

    return UnlinkAccountResponse(**result)


@router.get("/portfolios/{portfolio_id}/accounts", response_model=List[str])
async def get_portfolio_accounts(
    portfolio_id: int,
    current_user: dict = Depends(get_current_user),
    db = Depends(get_db)
):
    """
    Get all trading accounts linked to a portfolio.

    - **portfolio_id**: Portfolio ID

    Returns:
    - List of trading account IDs
    """
    user_id = extract_user_id(current_user)
    service = PortfolioService(db, user_id)

    account_ids = await service.get_portfolio_accounts(portfolio_id)

    return account_ids


@router.get("/portfolios/{portfolio_id}/metrics", response_model=PortfolioMetricsResponse)
async def get_portfolio_metrics(
    portfolio_id: int,
    current_user: dict = Depends(get_current_user),
    db = Depends(get_db)
):
    """
    Get aggregated metrics for a portfolio.

    Aggregates positions and P&L across all linked trading accounts.

    - **portfolio_id**: Portfolio ID

    Returns:
    - **account_count**: Number of linked trading accounts
    - **total_positions**: Total open positions across all accounts
    - **total_pnl**: Aggregated P&L (realized + unrealized)
    - **total_realized_pnl**: Aggregated realized P&L
    - **total_unrealized_pnl**: Aggregated unrealized P&L
    - **accounts**: Per-account metrics breakdown

    **Caching:**
    - Results are cached for 5 minutes
    - Cache invalidated on position changes
    """
    user_id = extract_user_id(current_user)
    service = PortfolioService(db, user_id)

    metrics = await service.get_portfolio_metrics(portfolio_id)

    return PortfolioMetricsResponse(**metrics)

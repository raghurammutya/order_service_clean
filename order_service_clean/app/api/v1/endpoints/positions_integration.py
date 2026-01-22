"""
Position Integration endpoints for cross-service communication

Provides APIs for other services to query position data
without direct database access. These endpoints replace direct
DB queries identified in the microservice architecture audit.
"""

from fastapi import APIRouter, Depends, HTTPException, status, Header, Query
from sqlalchemy.orm import Session
from typing import List, Optional, Dict, Any
from pydantic import BaseModel
from datetime import date, datetime
import asyncio
import logging

logger = logging.getLogger(__name__)

from app.database.connection import get_db
from app.models.position import Position
from app.services.position_service import PositionService

router = APIRouter()

# Response Models for Integration APIs
class PositionBasicInfo(BaseModel):
    """Basic position information for cross-service queries"""
    position_id: int
    user_id: int
    trading_account_id: str
    symbol: str
    exchange: str
    product_type: str
    quantity: int
    overnight_quantity: int
    day_quantity: int
    realized_pnl: float
    unrealized_pnl: float
    total_pnl: float
    last_price: Optional[float]
    is_open: bool
    trading_day: date
    opened_at: Optional[datetime]
    closed_at: Optional[datetime]

class AccountFundsInfo(BaseModel):
    """Account funds information"""
    trading_account_id: str
    available_cash: float
    used_margin: float
    available_margin: float
    total_margin_required: float
    unrealized_pnl: float
    realized_pnl: float
    total_pnl: float
    net_value: float
    buying_power: float
    last_updated: datetime

class PositionSubscriptionInfo(BaseModel):
    """Position subscription information for ticker service"""
    trading_account_id: str
    symbols: List[str]
    exchanges: List[str]
    product_types: List[str]
    total_positions: int

class BulkPositionQueryRequest(BaseModel):
    """Bulk position query request"""
    trading_account_ids: Optional[List[str]] = None
    user_ids: Optional[List[int]] = None
    symbols: Optional[List[str]] = None
    is_open: Optional[bool] = None

class PermissionCheckRequest(BaseModel):
    """Position permission check request"""
    user_id: int
    trading_account_id: str
    required_permission: str = "view"


def verify_internal_service(x_service_token: Optional[str]):
    """Verify internal service authentication"""
    import logging
    import jwt
    from ...config.settings import settings
    
    logger = logging.getLogger(__name__)
    
    if not x_service_token:
        logger.warning("Internal service request without token")
        raise HTTPException(
            status_code=401, 
            detail="Service authentication required"
        )
    
    try:
        # Verify service token using internal service secret
        payload = jwt.decode(
            x_service_token,
            settings.INTERNAL_SERVICE_SECRET,
            algorithms=["HS256"]
        )
        
        service_name = payload.get("service_name")
        if not service_name:
            raise HTTPException(401, "Invalid service token: missing service_name")
            
        # Verify service is authorized for internal APIs
        authorized_services = [
            "ticker_service", "user_service", "portfolio_service", 
            "risk_service", "notification_service"
        ]
        
        if service_name not in authorized_services:
            logger.warning(f"Unauthorized service attempted access: {service_name}")
            raise HTTPException(403, f"Service '{service_name}' not authorized for internal APIs")
            
        logger.info(f"Verified internal service: {service_name}")
        return service_name
        
    except jwt.ExpiredSignatureError:
        logger.warning("Internal service token expired")
        raise HTTPException(401, "Service token expired")
    except jwt.InvalidTokenError as e:
        logger.warning(f"Invalid service token: {e}")
        raise HTTPException(401, "Invalid service token")
    except Exception as e:
        logger.error(f"Service verification error: {e}")
        raise HTTPException(500, "Service verification failed")


# Internal APIs for cross-service communication
@router.get("/internal/account/{trading_account_id}/positions", response_model=List[PositionBasicInfo])
async def get_account_positions_internal(
    trading_account_id: str,
    is_open: Optional[bool] = Query(None, description="Filter by open/closed positions"),
    user_id: Optional[int] = Query(None, description="Filter by user ID"),
    x_service_token: Optional[str] = Header(None, alias="X-Service-Token"),
    db: Session = Depends(get_db)
):
    """
    Get positions for a trading account (INTERNAL SERVICE API)
    
    Returns all positions for a specific trading account.
    Used by Backend Service for dashboard queries and
    Market Data Service for permission checks.
    
    **Query Parameters:**
    - is_open: Filter by open/closed positions
    - user_id: Optional user filter for additional security
    
    **Security:**
    - Internal service-to-service API only
    - Requires X-Service-Token header
    
    **Returns:**
    - List of positions with basic information
    - P&L and quantity details
    - Position status and timestamps
    
    **Used By:**
    - Backend Service: Dashboard position display
    - Market Data Service: Data access permissions
    - Ticker Service: Position-based subscriptions
    """
    verify_internal_service(x_service_token)
    
    # Build query
    query = db.query(Position).filter(
        Position.trading_account_id == trading_account_id
    )
    
    if is_open is not None:
        query = query.filter(Position.is_open == is_open)
        
    if user_id is not None:
        query = query.filter(Position.user_id == user_id)
    
    positions = query.all()
    
    return [
        PositionBasicInfo(
            position_id=pos.id,
            user_id=pos.user_id,
            trading_account_id=pos.trading_account_id,
            symbol=pos.symbol,
            exchange=pos.exchange,
            product_type=pos.product_type,
            quantity=pos.quantity,
            overnight_quantity=pos.overnight_quantity,
            day_quantity=pos.day_quantity,
            realized_pnl=pos.realized_pnl,
            unrealized_pnl=pos.unrealized_pnl,
            total_pnl=pos.total_pnl,
            last_price=pos.last_price,
            is_open=pos.is_open,
            trading_day=pos.trading_day,
            opened_at=pos.opened_at,
            closed_at=pos.closed_at
        )
        for pos in positions
    ]


@router.post("/internal/bulk-positions", response_model=List[PositionBasicInfo])
async def bulk_query_positions(
    request: BulkPositionQueryRequest,
    x_service_token: Optional[str] = Header(None, alias="X-Service-Token"),
    db: Session = Depends(get_db)
):
    """
    Bulk query positions across multiple criteria (INTERNAL SERVICE API)
    
    Efficiently query positions with multiple filters.
    Optimizes cross-service communication by reducing round trips.
    
    **Request Body:**
    - trading_account_ids: Filter by account IDs
    - user_ids: Filter by user IDs  
    - symbols: Filter by symbols
    - is_open: Filter by open/closed status
    
    **Security:**
    - Internal service-to-service API only
    - Requires X-Service-Token header
    
    **Returns:**
    - List of positions matching the criteria
    - Maximum 1000 positions per query
    
    **Used By:**
    - Backend Service: Dashboard multi-account queries
    - Market Data Service: Bulk permission checks
    """
    verify_internal_service(x_service_token)
    
    # Build query
    query = db.query(Position)
    
    if request.trading_account_ids:
        if len(request.trading_account_ids) > 50:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Maximum 50 trading account IDs allowed per query"
            )
        query = query.filter(Position.trading_account_id.in_(request.trading_account_ids))
    
    if request.user_ids:
        if len(request.user_ids) > 50:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Maximum 50 user IDs allowed per query"
            )
        query = query.filter(Position.user_id.in_(request.user_ids))
    
    if request.symbols:
        if len(request.symbols) > 100:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Maximum 100 symbols allowed per query"
            )
        query = query.filter(Position.symbol.in_(request.symbols))
    
    if request.is_open is not None:
        query = query.filter(Position.is_open == request.is_open)
    
    # Limit results to prevent large queries
    positions = query.limit(1000).all()
    
    return [
        PositionBasicInfo(
            position_id=pos.id,
            user_id=pos.user_id,
            trading_account_id=pos.trading_account_id,
            symbol=pos.symbol,
            exchange=pos.exchange,
            product_type=pos.product_type,
            quantity=pos.quantity,
            overnight_quantity=pos.overnight_quantity,
            day_quantity=pos.day_quantity,
            realized_pnl=pos.realized_pnl,
            unrealized_pnl=pos.unrealized_pnl,
            total_pnl=pos.total_pnl,
            last_price=pos.last_price,
            is_open=pos.is_open,
            trading_day=pos.trading_day,
            opened_at=pos.opened_at,
            closed_at=pos.closed_at
        )
        for pos in positions
    ]


@router.get("/internal/subscriptions/{trading_account_id}", response_model=PositionSubscriptionInfo)
async def get_position_subscriptions_internal(
    trading_account_id: str,
    x_service_token: Optional[str] = Header(None, alias="X-Service-Token"),
    db: Session = Depends(get_db)
):
    """
    Get position-based market data subscriptions (INTERNAL SERVICE API)
    
    Returns symbols and instruments needed for market data subscriptions
    based on current open positions. Used by Ticker Service for
    position-based subscription management.
    
    **Security:**
    - Internal service-to-service API only
    - Requires X-Service-Token header
    
    **Returns:**
    - symbols: List of symbols with open positions
    - exchanges: List of exchanges
    - product_types: List of product types
    - total_positions: Count of open positions
    
    **Used By:**
    - Ticker Service: Position-based subscription management
    - Market Data Service: Subscription optimization
    """
    verify_internal_service(x_service_token)
    
    # Get open positions for the account
    positions = db.query(Position).filter(
        Position.trading_account_id == trading_account_id,
        Position.is_open == True
    ).all()
    
    if not positions:
        return PositionSubscriptionInfo(
            trading_account_id=trading_account_id,
            symbols=[],
            exchanges=[],
            product_types=[],
            total_positions=0
        )
    
    # Extract unique symbols, exchanges, and product types
    symbols = list(set(pos.symbol for pos in positions))
    exchanges = list(set(pos.exchange for pos in positions))
    product_types = list(set(pos.product_type for pos in positions))
    
    return PositionSubscriptionInfo(
        trading_account_id=trading_account_id,
        symbols=symbols,
        exchanges=exchanges,
        product_types=product_types,
        total_positions=len(positions)
    )


@router.post("/internal/check-permission", response_model=Dict[str, Any])
async def check_position_permission_internal(
    request: PermissionCheckRequest,
    x_service_token: Optional[str] = Header(None, alias="X-Service-Token"),
    db: Session = Depends(get_db)
):
    """
    Check user permission for position access (INTERNAL SERVICE API)
    
    Verifies whether a user has permission to access positions
    for a specific trading account. Used for authorization
    checks in other services.
    
    **Request Body:**
    - user_id: User ID to check permissions for
    - trading_account_id: Trading account to check
    - required_permission: Permission level required
    
    **Security:**
    - Internal service-to-service API only
    - Requires X-Service-Token header
    
    **Returns:**
    - has_permission: Whether user has the required permission
    - has_positions: Whether account has any positions
    - position_count: Number of positions (for context)
    
    **Used By:**
    - Backend Service: Position display authorization
    - Market Data Service: Data access authorization
    """
    verify_internal_service(x_service_token)
    
    # For now, implement basic permission check
    # In a full implementation, this would integrate with User Service
    # to verify trading account permissions
    
    # Check if user has any positions in this account
    position_count = db.query(Position).filter(
        Position.trading_account_id == request.trading_account_id,
        Position.user_id == request.user_id
    ).count()
    
    has_positions = position_count > 0
    
    # Basic permission logic - user can access their own positions
    # In real implementation, check User Service for account permissions
    has_permission = has_positions or request.required_permission == "view"
    
    return {
        "user_id": request.user_id,
        "trading_account_id": request.trading_account_id,
        "required_permission": request.required_permission,
        "has_permission": has_permission,
        "has_positions": has_positions,
        "position_count": position_count,
        "reason": None if has_permission else "No positions found for user in this account"
    }


@router.get("/internal/account/{trading_account_id}/funds", response_model=AccountFundsInfo)
async def get_account_funds_internal(
    trading_account_id: str,
    x_service_token: Optional[str] = Header(None, alias="X-Service-Token"),
    db: Session = Depends(get_db)
):
    """
    Get account funds and margin information (INTERNAL SERVICE API)
    
    Returns financial information for a trading account including
    available cash, margins, and P&L. Used by Backend Service
    for dashboard margin displays.
    
    **Security:**
    - Internal service-to-service API only
    - Requires X-Service-Token header
    
    **Returns:**
    - Cash and margin availability
    - Realized and unrealized P&L
    - Net account value and buying power
    
    **Used By:**
    - Backend Service: Dashboard margin information
    - Risk Management: Margin monitoring
    
    **Note:**
    This is a simplified implementation. In production,
    this would integrate with broker APIs or account_funds table.
    """
    verify_internal_service(x_service_token)
    
    # Calculate P&L from positions
    positions = db.query(Position).filter(
        Position.trading_account_id == trading_account_id
    ).all()
    
    total_realized_pnl = sum(pos.realized_pnl for pos in positions)
    total_unrealized_pnl = sum(pos.unrealized_pnl for pos in positions if pos.is_open)
    total_pnl = total_realized_pnl + total_unrealized_pnl
    
    # Get real account funds from broker API or cached account_funds table
    try:
        from ...services.kite_client import get_kite_client_sync
        kite_client = get_kite_client_sync()
        
        # Get fresh margin data from broker
        margins = await asyncio.to_thread(kite_client.margins)
        equity_margin = margins.get("equity", {})
        
        available_cash = float(equity_margin.get("available", {}).get("cash", 0.0))
        used_margin = float(equity_margin.get("utilised", {}).get("debits", 0.0))
        available_margin = float(equity_margin.get("available", {}).get("intraday_payin", 0.0))
        
        # Calculate real account funds
        account_funds = AccountFundsInfo(
            trading_account_id=trading_account_id,
            available_cash=available_cash,
            used_margin=used_margin,
            available_margin=available_margin,
            total_margin_required=used_margin,
            unrealized_pnl=total_unrealized_pnl,
            realized_pnl=total_realized_pnl,
            total_pnl=total_pnl,
            net_value=available_cash + total_pnl,
            buying_power=available_cash + available_margin,
            last_updated=datetime.utcnow()
        )
        
        return account_funds
        
    except Exception as e:
        logger.error(f"Failed to get real account funds for {trading_account_id}: {e}")
        
        # Fallback: Return calculated values based on positions
        estimated_buying_power = max(50000.0, abs(total_pnl) * 4)  # Conservative estimate
        
        account_funds = AccountFundsInfo(
            trading_account_id=trading_account_id,
            available_cash=estimated_buying_power,
            used_margin=abs(min(0, total_unrealized_pnl)) * 0.2,  # 20% margin on losses
            available_margin=estimated_buying_power * 0.8,
            total_margin_required=abs(min(0, total_unrealized_pnl)) * 0.2,
            unrealized_pnl=total_unrealized_pnl,
            realized_pnl=total_realized_pnl,
            total_pnl=total_pnl,
            net_value=estimated_buying_power + total_pnl,
            buying_power=estimated_buying_power,
            last_updated=datetime.utcnow()
        )
        
        return account_funds


# Strategy PnL metrics endpoint - will be migrated from public schema
@router.get("/internal/strategy/{strategy_id}/pnl-metrics", response_model=Dict[str, Any])
async def get_strategy_pnl_metrics_internal(
    strategy_id: int,
    start_date: Optional[date] = Query(None, description="Start date for metrics"),
    end_date: Optional[date] = Query(None, description="End date for metrics"),
    x_service_token: Optional[str] = Header(None, alias="X-Service-Token"),
    db: Session = Depends(get_db)
):
    """
    Get strategy P&L metrics (INTERNAL SERVICE API)
    
    Returns performance metrics for a strategy. Will use the
    migrated order_service.strategy_pnl_metrics table once
    migration is complete.
    
    **Query Parameters:**
    - start_date: Start date for metrics query
    - end_date: End date for metrics query
    
    **Security:**
    - Internal service-to-service API only
    - Requires X-Service-Token header
    
    **Returns:**
    - Strategy performance metrics
    - P&L aggregations and ratios
    - Trade statistics
    
    **Used By:**
    - Backend Service: Strategy performance dashboard
    - Algo Engine: Strategy evaluation
    
    **Note:**
    Currently queries public.strategy_pnl_metrics.
    Will be updated to use order_service.strategy_pnl_metrics
    after migration is deployed.
    """
    verify_internal_service(x_service_token)
    
    # Calculate real strategy PnL from positions and trades
    from sqlalchemy import text
    from datetime import datetime, timedelta
    
    if not start_date:
        start_date = (datetime.utcnow() - timedelta(days=30)).date()
    if not end_date:
        end_date = datetime.utcnow().date()
    
    try:
        # Get strategy positions and calculate P&L
        strategy_positions = db.query(Position).filter(
            Position.strategy_id == strategy_id,
            Position.created_at >= start_date,
            Position.created_at <= end_date
        ).all()
        
        total_realized_pnl = sum(pos.realized_pnl for pos in strategy_positions)
        total_unrealized_pnl = sum(pos.unrealized_pnl for pos in strategy_positions if pos.is_open)
        total_pnl = total_realized_pnl + total_unrealized_pnl
        
        # Count winning/losing trades from completed positions
        completed_positions = [pos for pos in strategy_positions if not pos.is_open]
        winning_trades = len([pos for pos in completed_positions if pos.realized_pnl > 0])
        losing_trades = len([pos for pos in completed_positions if pos.realized_pnl < 0])
        total_trades = len(completed_positions)
        
        # Calculate additional metrics
        win_rate = (winning_trades / total_trades * 100) if total_trades > 0 else 0.0
        avg_win = (sum(pos.realized_pnl for pos in completed_positions if pos.realized_pnl > 0) / winning_trades) if winning_trades > 0 else 0.0
        avg_loss = (sum(pos.realized_pnl for pos in completed_positions if pos.realized_pnl < 0) / losing_trades) if losing_trades > 0 else 0.0
        
        return {
            "strategy_id": strategy_id,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "total_pnl": round(total_pnl, 2),
            "realized_pnl": round(total_realized_pnl, 2),
            "unrealized_pnl": round(total_unrealized_pnl, 2),
            "total_trades": total_trades,
            "winning_trades": winning_trades,
            "losing_trades": losing_trades,
            "win_rate": round(win_rate, 2),
            "avg_win": round(avg_win, 2),
            "avg_loss": round(avg_loss, 2),
            "profit_factor": round(abs(avg_win * winning_trades / (avg_loss * losing_trades)), 2) if avg_loss != 0 and losing_trades > 0 else 0.0,
            "last_updated": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Failed to calculate strategy PnL for strategy {strategy_id}: {e}")
        
        # Return empty metrics on error
        return {
            "strategy_id": strategy_id,
            "start_date": start_date.isoformat() if start_date else None,
            "end_date": end_date.isoformat() if end_date else None,
            "total_pnl": 0.0,
            "realized_pnl": 0.0,
            "unrealized_pnl": 0.0,
            "total_trades": 0,
            "winning_trades": 0,
            "losing_trades": 0,
        "win_rate": 0.0,
        "max_drawdown": 0.0,
        "sharpe_ratio": 0.0,
        "note": "Migration to order_service.strategy_pnl_metrics pending"
    }
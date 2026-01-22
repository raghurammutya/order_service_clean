"""
Dashboard Summary Endpoint
Unified endpoint that aggregates all trading account summary data in a single call
Reduces frontend 5*N API calls to 1 call
"""

import logging
from datetime import datetime, date
from typing import List, Optional
from fastapi import APIRouter, Depends, Header
from sqlalchemy import func, text
from sqlalchemy.orm import Session

from pydantic import BaseModel, Field
from ....auth import get_current_user
from ....database.connection import get_db
from ....clients.user_service_client import UserServiceClient, UserServiceClientError
from ..schemas import ErrorResponse

logger = logging.getLogger(__name__)

router = APIRouter()


# ==========================================
# RESPONSE MODELS
# ==========================================

class OrdersSummary(BaseModel):
    """Orders summary for a trading account"""
    total_orders_today: int = Field(0, description="Total orders placed today")
    pending_orders: int = Field(0, description="Orders pending execution")
    executed_orders: int = Field(0, description="Successfully executed orders")
    rejected_orders: int = Field(0, description="Rejected orders")
    cancelled_orders: int = Field(0, description="Cancelled orders")


class PositionsSummary(BaseModel):
    """Positions summary for a trading account"""
    total_positions: int = Field(0, description="Total positions")
    long_positions: int = Field(0, description="Long positions")
    short_positions: int = Field(0, description="Short positions")
    active_positions: int = Field(0, description="Currently open positions")
    closed_positions: int = Field(0, description="Closed positions")


class HoldingsSummary(BaseModel):
    """Holdings summary for a trading account"""
    total_holdings: int = Field(0, description="Total holdings")
    total_value: float = Field(0.0, description="Current market value")
    total_invested: float = Field(0.0, description="Total invested amount")
    total_pnl: float = Field(0.0, description="Total profit/loss")
    pnl_percentage: Optional[float] = Field(None, description="P&L percentage")


class MarginsSummary(BaseModel):
    """Margins summary for a trading account"""
    available_margin: float = Field(0.0, description="Available margin")
    used_margin: float = Field(0.0, description="Used margin")
    total_margin: float = Field(0.0, description="Total margin")
    utilized_percentage: Optional[float] = Field(None, description="Margin utilization %")


class PnLSummary(BaseModel):
    """P&L summary for a trading account"""
    total_pnl: float = Field(0.0, description="Total profit/loss")
    realized_pnl: float = Field(0.0, description="Realized P&L")
    unrealized_pnl: float = Field(0.0, description="Unrealized P&L")
    pnl_percentage: Optional[float] = Field(None, description="P&L percentage")
    day_pnl: float = Field(0.0, description="Today's P&L")


class AccountSummary(BaseModel):
    """Complete summary for a single trading account"""
    trading_account_id: str = Field(..., description="Trading account ID")
    account_name: str = Field(..., description="Account display name")
    broker: str = Field(..., description="Broker name")
    broker_user_id: Optional[str] = Field(None, description="Broker user ID")
    orders: OrdersSummary
    positions: PositionsSummary
    holdings: HoldingsSummary
    margins: MarginsSummary
    pnl: PnLSummary


class DashboardSummaryResponse(BaseModel):
    """Complete dashboard summary response"""
    user_id: int = Field(..., description="User ID")
    as_of: datetime = Field(..., description="Timestamp of data")
    accounts: List[AccountSummary] = Field(default_factory=list, description="Account summaries")


# ==========================================
# HELPER FUNCTIONS
# ==========================================

async def get_user_trading_accounts(db: Session, user_id: int) -> List[dict]:
    """
    Get all trading accounts user has access to (owned + shared)
    Returns list of dicts with trading_account_id, account_name, broker, broker_user_id
    """
    try:
        async with UserServiceClient() as client:
            accounts = await client.get_user_trading_accounts(user_id, include_shared=True)
    except UserServiceClientError as exc:
        logger.error("Failed to load trading accounts from user_service: %s", exc)
        return []

    return [
        {
            "trading_account_id": str(account["trading_account_id"]),
            "account_name": account.get("account_name") or "",
            "broker": account.get("broker") or "",
            "broker_user_id": account.get("broker_user_id")
        }
        for account in accounts
    ]


async def get_orders_summary(db: Session, user_id: int, trading_account_id: str, today: date) -> OrdersSummary:
    """Get orders summary for a trading account"""
    query = text("""
        SELECT
            COUNT(*) as total_orders_today,
            COUNT(*) FILTER (WHERE status IN ('PENDING', 'OPEN', 'TRIGGER_PENDING')) as pending_orders,
            COUNT(*) FILTER (WHERE status = 'COMPLETE') as executed_orders,
            COUNT(*) FILTER (WHERE status = 'REJECTED') as rejected_orders,
            COUNT(*) FILTER (WHERE status = 'CANCELLED') as cancelled_orders
        FROM order_service.orders
        WHERE user_id = :user_id
          AND trading_account_id = :trading_account_id
          AND DATE(created_at) = :today
    """)

    result = (await db.execute(query, {
        "user_id": user_id,
        "trading_account_id": trading_account_id,
        "today": today
    })).fetchone()

    if result:
        return OrdersSummary(
            total_orders_today=result.total_orders_today or 0,
            pending_orders=result.pending_orders or 0,
            executed_orders=result.executed_orders or 0,
            rejected_orders=result.rejected_orders or 0,
            cancelled_orders=result.cancelled_orders or 0
        )
    return OrdersSummary()


async def get_positions_summary(db: Session, user_id: int, trading_account_id: str) -> PositionsSummary:
    """Get positions summary for a trading account

    Uses DISTINCT ON to get only the latest trading_day per symbol+product_type.
    This prevents counting duplicate rows for NRML positions that carry forward across days.
    """
    query = text("""
        WITH latest_positions AS (
            SELECT DISTINCT ON (symbol, product_type)
                quantity
            FROM order_service.positions
            WHERE user_id = :user_id
              AND trading_account_id = :trading_account_id
            ORDER BY symbol, product_type, trading_day DESC
        )
        SELECT
            COUNT(*) as total_positions,
            COUNT(*) FILTER (WHERE quantity > 0) as long_positions,
            COUNT(*) FILTER (WHERE quantity < 0) as short_positions,
            COUNT(*) FILTER (WHERE quantity != 0) as active_positions,
            COUNT(*) FILTER (WHERE quantity = 0) as closed_positions
        FROM latest_positions
    """)

    result = (await db.execute(query, {
        "user_id": user_id,
        "trading_account_id": trading_account_id
    })).fetchone()

    if result:
        return PositionsSummary(
            total_positions=result.total_positions or 0,
            long_positions=result.long_positions or 0,
            short_positions=result.short_positions or 0,
            active_positions=result.active_positions or 0,
            closed_positions=result.closed_positions or 0
        )
    return PositionsSummary()


async def get_pnl_summary(db: Session, user_id: int, trading_account_id: str, today: date) -> PnLSummary:
    """Get P&L summary for a trading account"""
    # Get overall P&L
    query = text("""
        SELECT
            COALESCE(SUM(realized_pnl), 0) as realized_pnl,
            COALESCE(SUM(unrealized_pnl), 0) as unrealized_pnl,
            COALESCE(SUM(total_pnl), 0) as total_pnl
        FROM order_service.positions
        WHERE user_id = :user_id
          AND trading_account_id = :trading_account_id
    """)

    result = (await db.execute(query, {
        "user_id": user_id,
        "trading_account_id": trading_account_id
    })).fetchone()

    # Get today's P&L from trades
    day_pnl_query = text("""
        SELECT COALESCE(SUM(
            CASE
                WHEN transaction_type = 'SELL' THEN price * quantity
                WHEN transaction_type = 'BUY' THEN -price * quantity
                ELSE 0
            END
        ), 0) as day_pnl
        FROM order_service.trades
        WHERE user_id = :user_id
          AND trading_account_id = :trading_account_id
          AND DATE(trade_time) = :today
    """)

    day_result = (await db.execute(day_pnl_query, {
        "user_id": user_id,
        "trading_account_id": trading_account_id,
        "today": today
    })).fetchone()

    if result:
        total_pnl = float(result.total_pnl or 0)
        realized_pnl = float(result.realized_pnl or 0)
        unrealized_pnl = float(result.unrealized_pnl or 0)
        day_pnl = float(day_result.day_pnl or 0) if day_result else 0.0

        # Calculate percentage (avoiding division by zero)
        pnl_percentage = None
        if realized_pnl != 0:
            pnl_percentage = (total_pnl / abs(realized_pnl)) * 100

        return PnLSummary(
            total_pnl=total_pnl,
            realized_pnl=realized_pnl,
            unrealized_pnl=unrealized_pnl,
            pnl_percentage=pnl_percentage,
            day_pnl=day_pnl
        )

    return PnLSummary()


async def get_holdings_summary(db: Session, user_id: int, trading_account_id: str) -> HoldingsSummary:
    """Get holdings summary for a trading account (placeholder - no data yet)"""
    # Holdings table is currently empty, return zeros
    return HoldingsSummary()


async def get_margins_summary(db: Session, user_id: int, trading_account_id: str) -> MarginsSummary:
    """Get margins summary for a trading account (placeholder - no data yet)"""
    # Margins table is currently empty, return zeros
    return MarginsSummary()


# ==========================================
# ENDPOINT
# ==========================================

@router.get("/summary", response_model=DashboardSummaryResponse)
async def get_dashboard_summary(
    current_user: dict = Depends(get_current_user),
    x_entity_id: Optional[str] = Header(None, alias="X-Entity-ID"),
    db: Session = Depends(get_db)
):
    """
    Get unified dashboard summary for all user's trading accounts

    **Replaces 5*N parallel API calls with a single optimized call**

    Returns aggregated data for:
    - Orders (today's orders by status)
    - Positions (long/short/active/closed counts)
    - P&L (realized/unrealized/total)
    - Holdings (total value, P&L) - currently empty
    - Margins (available/used/total) - currently empty

    **Headers:**
    - X-Entity-ID (optional): Filter for specific trading_account_id

    **Performance:**
    - Uses SQL aggregation (COUNT, SUM) for efficiency
    - Single database query per account per data type
    - Much faster than N*5 separate API calls
    """
    try:
        # Get integer user_id (gateway auth uses "user_id_int", JWT uses "sub" as integer)
        user_id = current_user.get("user_id_int") or current_user.get("user_id")
        if isinstance(user_id, str) and user_id.startswith("user:"):
            user_id = int(user_id[5:])  # Parse "user:1" -> 1
        user_id = int(user_id)  # Ensure it's an integer
        today = date.today()

        # Get all trading accounts user has access to
        trading_accounts = await get_user_trading_accounts(db, user_id)

        # Filter by X-Entity-ID if provided
        if x_entity_id:
            trading_accounts = [
                acc for acc in trading_accounts
                if acc["trading_account_id"] == x_entity_id
            ]

        # Gather summary data for each account
        account_summaries = []

        for account in trading_accounts:
            account_id = account["trading_account_id"]

            # Get all summaries in parallel (async)
            orders_summary = await get_orders_summary(db, user_id, account_id, today)
            positions_summary = await get_positions_summary(db, user_id, account_id)
            pnl_summary = await get_pnl_summary(db, user_id, account_id, today)
            holdings_summary = await get_holdings_summary(db, user_id, account_id)
            margins_summary = await get_margins_summary(db, user_id, account_id)

            account_summaries.append(AccountSummary(
                trading_account_id=account_id,
                account_name=account["account_name"] or f"Account {account_id}",
                broker=account["broker"] or "unknown",
                broker_user_id=account.get("broker_user_id"),
                orders=orders_summary,
                positions=positions_summary,
                holdings=holdings_summary,
                margins=margins_summary,
                pnl=pnl_summary
            ))

        return DashboardSummaryResponse(
            user_id=user_id,
            as_of=datetime.utcnow(),
            accounts=account_summaries
        )

    except Exception as e:
        logger.error(f"Error fetching dashboard summary: {str(e)}", exc_info=True)
        raise

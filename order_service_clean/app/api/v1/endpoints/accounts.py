"""
Account API Endpoints

REST API for account information, holdings, margins, and tier management.
Supports multi-account routing via trading_account_id header.
"""
import logging
from typing import List, Optional, Dict, Any
from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

from ....auth import get_current_user, get_trading_account_id
from ....utils.user_id import extract_user_id
from ....services.kite_client_multi import get_kite_client_for_account
from ....services.account_tier_service import (
    AccountTierService, get_account_tier_info
)
from ....services.rate_limiter import get_rate_limiter_with_fallback
from ....services.order_service import OrderService
from ....services.position_service import PositionService
from ....services.trade_service import TradeService
from ....services.margin_service import MarginService
from ....services.kite_account_rate_limiter import get_rate_limiter_manager_sync
from ....database.connection import get_db
from ....clients.user_service_client import UserServiceClient, UserServiceClientError
from fastapi import HTTPException
from datetime import datetime, timezone, timedelta

logger = logging.getLogger(__name__)

router = APIRouter()


# ==========================================
# REQUEST/RESPONSE MODELS
# ==========================================

class HoldingResponse(BaseModel):
    """Holding response model"""
    tradingsymbol: str
    exchange: str
    isin: str
    quantity: int
    t1_quantity: int
    average_price: float
    last_price: float
    close_price: float
    pnl: float
    day_change: float
    day_change_percentage: float

    class Config:
        from_attributes = True


class MarginResponse(BaseModel):
    """Margin response model"""
    enabled: bool
    net: float
    available: Dict[str, float]
    utilised: Dict[str, float]

    class Config:
        from_attributes = True


class OrderMarginRequest(BaseModel):
    """Request model for order margin calculation"""
    exchange: str
    tradingsymbol: str
    transaction_type: str
    variety: str
    product: str
    order_type: str
    quantity: int
    price: Optional[float] = None
    trigger_price: Optional[float] = None


class BasketMarginRequest(BaseModel):
    """Request model for basket margin calculation"""
    orders: List[OrderMarginRequest]
    consider_positions: bool = True
    mode: Optional[str] = None


# ==========================================
# ENDPOINTS
# ==========================================

@router.get("/holdings", response_model=List[Dict[str, Any]])
async def get_holdings(
    current_user: dict = Depends(get_current_user),
    trading_account_id: Optional[int] = Depends(get_trading_account_id),
    db: AsyncSession = Depends(get_db)
):
    """
    Get long-term equity holdings from database.

    **GitHub Issue #439: "All Accounts" Aggregation Support**
    - Omit X-Trading-Account-ID header to get holdings from all accessible accounts
    - With header: Returns holdings for specific account (requires view permission)
    - Without header: Returns holdings from all accounts user has access to

    Returns:
    - List of holdings with current value and P&L
    - Each holding includes tradingsymbol, quantity, average price, last price, pnl, etc.
    """
    from ....utils.acl_helpers import ACLHelper

    user_id = extract_user_id(current_user)

    # Check if this is "All Accounts" mode (Issue #439)
    if trading_account_id is None:
        logger.info(f"All Accounts mode: fetching holdings for user {user_id}")

        # Get accessible trading accounts via ACL
        accessible_accounts = await ACLHelper.get_accessible_resources(
            user_id=user_id,
            resource_type="trading_account",
            min_action="view"
        )

        # Fall back to JWT acct_ids if ACL service is down
        if not accessible_accounts:
            accessible_accounts = current_user.get("acct_ids", [])
            if not accessible_accounts:
                logger.warning(f"User {user_id} has no accessible accounts")
                return []
            logger.info(f"Using JWT acct_ids fallback: {accessible_accounts}")

        # Resolve broker_user_id for each account via user_service API
        try:
            async with UserServiceClient() as client:
                bulk_response = await client.bulk_query_trading_accounts(accessible_accounts)
        except UserServiceClientError as exc:
            logger.error(f"Failed to resolve trading accounts: {exc}")
            return []

        account_map = {
            acc["trading_account_id"]: acc.get("broker_user_id")
            for acc in bulk_response.get("accounts", [])
            if acc.get("broker_user_id")
        }
        broker_user_ids = list(account_map.values())
        if not broker_user_ids:
            logger.warning(f"No broker_user_id values found for user {user_id}")
            return []

        # Query holdings from database for all accessible accounts
        query = text("""
            SELECT h.symbol,
                h.exchange,
                h.isin,
                h.quantity,
                h.average_price,
                h.last_price,
                h.pnl,
                h.day_pnl,
                h.synced_at,
                h.trading_account_id
            FROM order_service.account_holding h
            WHERE h.trading_account_id = ANY(:account_ids)
            ORDER BY h.synced_at DESC
        """)

        result = await db.execute(query, {"account_ids": broker_user_ids})
        rows = result.fetchall()

        broker_to_trading = {v: k for k, v in account_map.items()}
        all_holdings = []
        for row in rows:
            all_holdings.append({
                "symbol": row.tradingsymbol,
                "exchange": row.exchange,
                "isin": row.isin,
                "quantity": int(row.quantity) if row.quantity else 0,
                "average_price": float(row.average_price) if row.average_price else 0.0,
                "last_price": float(row.last_price) if row.last_price else 0.0,
                "pnl": float(row.pnl) if row.pnl else 0.0,
                "day_pnl": float(row.day_pnl) if row.day_pnl else 0.0,
                "trading_account_id": broker_to_trading.get(row.trading_account_id, row.trading_account_id)
            })

        logger.info(f"Fetched {len(all_holdings)} total holdings from {len(accessible_accounts)} accounts for user {user_id}")
        return all_holdings

    # Single account mode - check ACL permission
    has_permission = await ACLHelper.check_trading_account_permission(
        user_id=user_id,
        trading_account_id=trading_account_id,
        action="view"
    )

    if not has_permission:
        logger.warning(f"User {user_id} denied view access to account {trading_account_id}")
        raise HTTPException(
            status_code=403,
            detail=f"You do not have permission to view holdings for account {trading_account_id}"
        )

    try:
        # Resolve broker_user_id for account via user_service API
        try:
            async with UserServiceClient() as client:
                account_info = await client.get_trading_account_basic_info(trading_account_id)
        except UserServiceClientError as exc:
            logger.error(f"Failed to resolve trading account {trading_account_id}: {exc}")
            raise HTTPException(status_code=502, detail="Trading account lookup failed")

        broker_user_id = account_info.get("broker_user_id")
        if not broker_user_id:
            raise HTTPException(status_code=404, detail="Trading account broker_user_id missing")

        # Query holdings from database for specific account
        query = text("""
            SELECT h.symbol,
                h.exchange,
                h.isin,
                h.quantity,
                h.average_price,
                h.last_price,
                h.pnl,
                h.day_pnl,
                h.synced_at,
                h.trading_account_id
            FROM order_service.account_holding h
            WHERE h.trading_account_id = :trading_account_id
            ORDER BY h.synced_at DESC
        """)

        result = await db.execute(query, {"trading_account_id": broker_user_id})
        rows = result.fetchall()

        holdings = []
        for row in rows:
            holdings.append({
                "symbol": row.tradingsymbol,
                "exchange": row.exchange,
                "isin": row.isin,
                "quantity": int(row.quantity) if row.quantity else 0,
                "average_price": float(row.average_price) if row.average_price else 0.0,
                "last_price": float(row.last_price) if row.last_price else 0.0,
                "pnl": float(row.pnl) if row.pnl else 0.0,
                "day_pnl": float(row.day_pnl) if row.day_pnl else 0.0,
                "trading_account_id": trading_account_id
            })

        logger.info(f"Fetched {len(holdings)} holdings for user {user_id}, account {trading_account_id}")
        return holdings

    except Exception as e:
        logger.error(f"Failed to fetch holdings for account {trading_account_id}: {e}")
        raise


@router.get("/margins", response_model=Dict[str, Any])
async def get_margins(
    segment: Optional[str] = Query(None, description="Trading segment (equity, commodity)"),
    by_strategy: bool = Query(False, description="Include per-strategy margin breakdown"),
    current_user: dict = Depends(get_current_user),
    trading_account_id: Optional[str] = Depends(get_trading_account_id),
    db: AsyncSession = Depends(get_db)
):
    """
    Get account margins and cash balances.

    **GitHub Issue #439: "All Accounts" Aggregation Support**
    - Omit X-Trading-Account-ID header to get aggregated margins from all accessible accounts
    - With header: Returns margins for specific account (requires view permission)
    - Without header: Returns aggregated margins from all accounts

    - **segment**: Optional segment filter (equity, commodity). Returns all segments if not specified.
    - **by_strategy**: Include per-strategy margin breakdown (default: false)

    Returns:
    - Account margins with available balance, used margin, exposure limits
    - If by_strategy=true, includes strategy-wise margin allocation
    - For All Accounts mode: includes per-account breakdown and aggregated totals
    """
    from ....utils.acl_helpers import ACLHelper

    user_id = extract_user_id(current_user)

    # Check if this is "All Accounts" mode (Issue #439)
    if trading_account_id is None:
        logger.info(f"All Accounts mode: aggregating margins for user {user_id}")

        # Get accessible trading accounts via ACL
        account_ids = await ACLHelper.get_accessible_resources(
            user_id=user_id,
            resource_type="trading_account",
            min_action="view"
        )

        # Fall back to JWT acct_ids if ACL service is down
        if not account_ids:
            account_ids = current_user.get("acct_ids", [])
            if not account_ids:
                return {
                    "accounts": [],
                    "totals": {
                        "enabled": False,
                        "net": 0.0,
                        "available": {},
                        "utilised": {}
                    },
                    "aggregated": True,
                    "account_count": 0,
                    "message": "No accessible accounts found"
                }
            logger.info(f"Using JWT acct_ids fallback: {account_ids}")

        # Query margins from database for all accessible accounts
        # Resolve broker_user_id values for accounts
        try:
            async with UserServiceClient() as client:
                bulk_response = await client.bulk_query_trading_accounts(account_ids)
        except UserServiceClientError as exc:
            logger.error(f"Failed to resolve trading accounts for margins: {exc}")
            return {
                "accounts": [],
                "totals": {"enabled": False, "net": 0.0, "available": {}, "utilised": {}},
                "aggregated": True,
                "account_count": 0,
                "message": "Trading account lookup failed"
            }

        account_map = {
            acc["trading_account_id"]: acc.get("broker_user_id")
            for acc in bulk_response.get("accounts", [])
            if acc.get("broker_user_id")
        }
        broker_user_ids = list(account_map.values())
        if not broker_user_ids:
            return {
                "accounts": [],
                "totals": {"enabled": False, "net": 0.0, "available": {}, "utilised": {}},
                "aggregated": True,
                "account_count": 0,
                "message": "No broker accounts found"
            }

        if segment:
            query = text("""
                SELECT
                    af.account_id,
                    af.segment,
                    af.available_cash,
                    af.available_margin,
                    af.used_margin,
                    af.net,
                    af.synced_at,
                    af.trading_account_id
                FROM order_service.account_funds af
                WHERE af.trading_account_id = ANY(:account_ids)
                AND af.segment = :segment
                ORDER BY af.trading_account_id, af.segment
            """)
            result = await db.execute(query, {"account_ids": broker_user_ids, "segment": segment})
        else:
            query = text("""
                SELECT
                    af.account_id,
                    af.segment,
                    af.available_cash,
                    af.available_margin,
                    af.used_margin,
                    af.net,
                    af.synced_at,
                    af.trading_account_id
                FROM order_service.account_funds af
                WHERE af.trading_account_id = ANY(:account_ids)
                ORDER BY af.trading_account_id, af.segment
            """)
            result = await db.execute(query, {"account_ids": broker_user_ids})
        rows = result.fetchall()

        # Aggregate margins by account and total
        accounts_data = {}
        totals = {
            "available_cash": 0.0,
            "available_margin": 0.0,
            "used_margin": 0.0,
            "net": 0.0
        }

        for row in rows:
            acc_id = account_map.get(row.trading_account_id, row.trading_account_id)
            if acc_id not in accounts_data:
                accounts_data[acc_id] = {
                    "trading_account_id": acc_id,
                    "segments": {}
                }

            seg = row.segment
            accounts_data[acc_id]["segments"][seg] = {
                "available_cash": float(row.available_cash or 0),
                "available_margin": float(row.available_margin or 0),
                "used_margin": float(row.used_margin or 0),
                "net": float(row.net or 0)
            }

            totals["available_cash"] += float(row.available_cash or 0)
            totals["available_margin"] += float(row.available_margin or 0)
            totals["used_margin"] += float(row.used_margin or 0)
            totals["net"] += float(row.net or 0)

        return {
            "accounts": list(accounts_data.values()),
            "totals": totals,
            "aggregated": True,
            "account_count": len(accounts_data)
        }

    # Single account mode - check ACL permission
    has_permission = await ACLHelper.check_trading_account_permission(
        user_id=user_id,
        trading_account_id=int(trading_account_id),
        action="view"
    )

    if not has_permission:
        logger.warning(f"User {user_id} denied view access to account {trading_account_id}")
        raise HTTPException(
            status_code=403,
            detail=f"You do not have permission to view margins for account {trading_account_id}"
        )

    try:
        # Query margins from database for specific account
        # Resolve broker_user_id for account
        try:
            async with UserServiceClient() as client:
                account_info = await client.get_trading_account_basic_info(int(trading_account_id))
        except UserServiceClientError as exc:
            logger.error(f"Failed to resolve trading account {trading_account_id}: {exc}")
            raise HTTPException(status_code=502, detail="Trading account lookup failed")

        broker_user_id = account_info.get("broker_user_id")
        if not broker_user_id:
            raise HTTPException(status_code=404, detail="Trading account broker_user_id missing")

        if segment:
            query = text("""
                SELECT
                    af.segment,
                    af.available_cash,
                    af.available_margin,
                    af.used_margin,
                    af.net,
                    af.synced_at
                FROM order_service.account_funds af
                WHERE af.trading_account_id = :trading_account_id
                AND af.segment = :segment
                ORDER BY af.segment
            """)
            result = await db.execute(query, {
                "trading_account_id": broker_user_id,
                "segment": segment
            })
        else:
            query = text("""
                SELECT
                    af.segment,
                    af.available_cash,
                    af.available_margin,
                    af.used_margin,
                    af.net,
                    af.synced_at
                FROM order_service.account_funds af
                WHERE af.trading_account_id = :trading_account_id
                ORDER BY af.segment
            """)
            result = await db.execute(query, {
                "trading_account_id": broker_user_id
            })
        rows = result.fetchall()

        # Build margins response matching Kite API format
        margins = {}
        for row in rows:
            margins[row.segment] = {
                "enabled": True,
                "net": float(row.net or 0),
                "available": {
                    "adhoc_margin": 0.0,
                    "cash": float(row.available_cash or 0),
                    "collateral": 0.0,
                    "intraday_payin": 0.0
                },
                "utilised": {
                    "debits": float(row.used_margin or 0),
                    "exposure": float(row.used_margin or 0),
                    "m2m_realised": 0.0,
                    "m2m_unrealised": 0.0,
                    "option_premium": 0.0,
                    "payout": 0.0,
                    "span": 0.0,
                    "holding_sales": 0.0,
                    "turnover": 0.0,
                    "liquid_collateral": 0.0,
                    "stock_collateral": 0.0
                }
            }

        # Add per-strategy breakdown if requested
        if by_strategy:
            query = text("""
                SELECT
                    p.strategy_id,
                    CASE 
                        WHEN p.strategy_id IS NOT NULL THEN CONCAT('Strategy_', p.strategy_id)
                        ELSE 'Manual'
                    END as strategy_name,
                    p.exchange,
                    CASE
                        WHEN p.exchange IN ('NSE', 'BSE') THEN 'equity'
                        WHEN p.exchange IN ('NFO', 'BFO', 'CDS', 'MCX', 'NCDEX') THEN 'commodity'
                        ELSE 'other'
                    END as segment,
                    SUM(ABS(p.quantity * COALESCE(p.last_price, p.close_price, 0))) as notional_value,
                    SUM(CASE
                        WHEN p.product_type = 'MIS' THEN ABS(p.quantity * COALESCE(p.last_price, p.close_price, 0)) * 0.2
                        WHEN p.product_type = 'NRML' THEN ABS(p.quantity * COALESCE(p.last_price, p.close_price, 0)) * 0.1
                        WHEN p.product_type = 'CNC' THEN ABS(p.quantity * COALESCE(p.last_price, p.close_price, 0)) * 1.0
                        ELSE 0
                    END) as estimated_margin,
                    COUNT(*) as positions_count,
                    SUM(COALESCE(p.unrealized_pnl, 0)) as unrealized_pnl
                FROM order_service.positions p
                WHERE p.trading_account_id = :account_id
                AND p.is_open = true
                AND p.quantity != 0
                GROUP BY p.strategy_id, p.exchange
                ORDER BY estimated_margin DESC
            """)

            result = await db.execute(query, {"account_id": str(trading_account_id)})

            strategy_margins = []
            for row in result:
                strategy_margins.append({
                    "strategy_id": row.strategy_id,
                    "strategy_name": row.strategy_name,
                    "exchange": row.exchange,
                    "segment": row.segment,
                    "notional_value": float(row.notional_value or 0),
                    "estimated_margin": float(row.estimated_margin or 0),
                    "positions_count": row.positions_count,
                    "unrealized_pnl": float(row.unrealized_pnl or 0)
                })

            margins["by_strategy"] = strategy_margins

        logger.info(f"Fetched margins for user {current_user['user_id']}, account {trading_account_id}, segment: {segment}, by_strategy: {by_strategy}")
        return margins

    except Exception as e:
        logger.error(f"Failed to fetch margins for account {trading_account_id}: {e}")
        raise


@router.post("/orders/margins", response_model=List[Dict[str, Any]])
async def calculate_order_margins(
    request: BasketMarginRequest,
    current_user: dict = Depends(get_current_user),
    trading_account_id: int = Depends(get_trading_account_id)
):
    """
    Calculate required margins for a list of orders.

    Requires X-Trading-Account-ID header for multi-account routing.
    Requires 'trade' permission on the account (margin calculations require trading access).

    This endpoint calculates margins for each order in the list, considering:
    - Existing positions
    - Open orders
    - Available margin

    Returns per-order margin breakdown.
    """
    from ....utils.acl_helpers import ACLHelper

    user_id = extract_user_id(current_user)

    # Check ACL permission - margin calculations require trade permission
    has_permission = await ACLHelper.check_trading_account_permission(
        user_id=user_id,
        trading_account_id=trading_account_id,
        action="trade"
    )

    if not has_permission:
        logger.warning(f"User {user_id} denied trade access to account {trading_account_id}")
        raise HTTPException(
            status_code=403,
            detail=f"You do not have permission to calculate margins for account {trading_account_id}"
        )

    try:
        kite_client = get_kite_client_for_account(trading_account_id)

        # Convert Pydantic models to dicts
        orders = [order.dict(exclude_none=True) for order in request.orders]

        margins = await kite_client.calculate_order_margins(orders)

        logger.info(f"Calculated margins for {len(orders)} orders for user {current_user['user_id']}, account {trading_account_id}")
        return margins

    except Exception as e:
        logger.error(f"Failed to calculate order margins for account {trading_account_id}: {e}")
        raise


@router.post("/orders/basket-margins", response_model=Dict[str, Any])
async def calculate_basket_margins(
    request: BasketMarginRequest,
    current_user: dict = Depends(get_current_user),
    trading_account_id: int = Depends(get_trading_account_id)
):
    """
    Calculate total margins for basket of orders including margin benefits.

    Requires X-Trading-Account-ID header for multi-account routing.
    Requires 'trade' permission on the account (margin calculations require trading access).

    This endpoint calculates total margin requirements for a basket of orders,
    taking into account:
    - Hedge benefits (option spreads, futures hedging)
    - Existing positions
    - Combined margin optimization

    Returns:
    - Total required margin
    - Per-order breakdown (if mode is not 'compact')
    - Available vs. required comparison
    """
    from ....utils.acl_helpers import ACLHelper

    user_id = extract_user_id(current_user)

    # Check ACL permission - margin calculations require trade permission
    has_permission = await ACLHelper.check_trading_account_permission(
        user_id=user_id,
        trading_account_id=trading_account_id,
        action="trade"
    )

    if not has_permission:
        logger.warning(f"User {user_id} denied trade access to account {trading_account_id}")
        raise HTTPException(
            status_code=403,
            detail=f"You do not have permission to calculate margins for account {trading_account_id}"
        )

    try:
        kite_client = get_kite_client_for_account(trading_account_id)

        # Convert Pydantic models to dicts
        orders = [order.dict(exclude_none=True) for order in request.orders]

        margins = await kite_client.calculate_basket_margins(
            orders,
            consider_positions=request.consider_positions,
            mode=request.mode
        )

        logger.info(
            f"Calculated basket margins for {len(orders)} orders for user {current_user['user_id']}, "
            f"account {trading_account_id}, consider_positions={request.consider_positions}, mode={request.mode}"
        )
        return margins

    except Exception as e:
        logger.error(f"Failed to calculate basket margins for account {trading_account_id}: {e}")
        raise


# ==========================================
# TIER MANAGEMENT ENDPOINTS (Smart Operations)
# ==========================================

class TierResponse(BaseModel):
    """Response model for tier info"""
    trading_account_id: int
    tier: str
    description: str
    has_active_orders: bool = False
    has_open_positions: bool = False
    last_activity: Optional[str] = None


class TierSummaryResponse(BaseModel):
    """Response model for tier summary"""
    tiers: Dict[str, int]
    total_accounts: int


@router.get("/{account_id}/tier", response_model=TierResponse)
async def get_account_tier(
    account_id: int,
    current_user: dict = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Get current sync tier for an account.

    Tiers determine sync frequency:
    - **HOT**: Real-time + 30s backup sync (active orders/recent activity)
    - **WARM**: 2 minute polling (open positions/today's activity)
    - **COLD**: 15 minute polling (holdings only)
    - **DORMANT**: On-demand only (no activity 7+ days)

    **Requires 'view' permission** on the account.

    Returns tier info and activity status.
    """
    from ....utils.acl_helpers import ACLHelper

    user_id = extract_user_id(current_user)

    # Check ACL permission - viewing tier requires view permission
    has_permission = await ACLHelper.check_trading_account_permission(
        user_id=user_id,
        trading_account_id=account_id,
        action="view"
    )

    if not has_permission:
        logger.warning(f"User {user_id} denied view access to account {account_id}")
        raise HTTPException(
            status_code=403,
            detail=f"You do not have permission to view tier for account {account_id}"
        )

    try:
        tier_info = await get_account_tier_info(db, account_id)
        return TierResponse(**tier_info)

    except Exception as e:
        logger.error(f"Failed to get tier for account {account_id}: {e}")
        raise


@router.get("/tier-summary", response_model=TierSummaryResponse)
async def get_tier_summary(
    current_user: dict = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Get summary of accounts by tier.

    Returns count of accounts in each tier:
    - HOT: Accounts with active orders or recent activity
    - WARM: Accounts with open positions or today's activity
    - COLD: Accounts with holdings only
    - DORMANT: Inactive accounts

    **ACL-filtered**: Only includes accounts user has access to.

    Used for monitoring tier distribution.
    """
    from ....utils.acl_helpers import ACLHelper

    user_id = extract_user_id(current_user)

    # Get accessible trading accounts via ACL
    accessible_accounts = await ACLHelper.get_accessible_resources(
        user_id=user_id,
        resource_type="trading_account",
        min_action="view"
    )

    if not accessible_accounts:
        logger.warning(f"User {user_id} has no accessible accounts for tier summary")
        return TierSummaryResponse(
            tiers={},
            total_accounts=0
        )

    try:
        query = text("""
            SELECT COALESCE(sync_tier, 'cold') AS tier, COUNT(*) AS count
            FROM order_service.account_sync_tiers
            WHERE trading_account_id = ANY(:account_ids)
            GROUP BY COALESCE(sync_tier, 'cold')
        """)

        result = await db.execute(query, {"account_ids": accessible_accounts})
        filtered_summary = {row.tier: row.count for row in result.fetchall()}
        total = sum(filtered_summary.values())
        missing = len(accessible_accounts) - total
        if missing > 0:
            filtered_summary["cold"] = filtered_summary.get("cold", 0) + missing
            total = len(accessible_accounts)

        logger.info(f"Returning filtered tier summary for user {user_id} across {len(accessible_accounts)} accounts")

        return TierSummaryResponse(
            tiers=filtered_summary,
            total_accounts=total
        )

    except Exception as e:
        logger.error(f"Failed to get tier summary: {e}")
        raise


# ==========================================
# HARD REFRESH ENDPOINTS (Smart Operations)
# ==========================================

class HardRefreshResponse(BaseModel):
    """Response model for hard refresh."""
    status: str
    account_id: int
    synced: Dict[str, Any]
    drift_detected: bool
    drift_details: Optional[Dict[str, Any]] = None
    tier: str
    tier_expires_in: str


class RefreshStatusResponse(BaseModel):
    """Response model for refresh status."""
    account_id: int
    can_refresh: bool
    seconds_until_available: int
    last_refresh: Optional[str] = None


@router.post("/{account_id}/hard-refresh", response_model=HardRefreshResponse)
async def hard_refresh_account(
    account_id: int,
    current_user: dict = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Trigger immediate full sync for a trading account.

    Use when:
    - Account shows stale data
    - External orders placed via Kite app
    - Position mismatch suspected

    **Rate limited**: 1 request per minute per account.
    **Requires 'trade' permission** on the account (syncing can affect trading state).

    Effects:
    - Promotes account to HOT tier for 5 minutes
    - Syncs: orders, positions, trades, margins
    - Detects and reports any drift
    """
    from ....utils.acl_helpers import ACLHelper

    user_id = current_user['user_id']

    # Check ACL permission - hard refresh requires trade permission
    has_permission = await ACLHelper.check_trading_account_permission(
        user_id=user_id,
        trading_account_id=account_id,
        action="trade"
    )

    if not has_permission:
        logger.warning(f"User {user_id} denied trade access to account {account_id}")
        raise HTTPException(
            status_code=403,
            detail=f"You do not have permission to refresh account {account_id}"
        )

    # Rate limit check
    rate_limiter = await get_rate_limiter_with_fallback()
    is_allowed, seconds_remaining = await rate_limiter.check_rate_limit(account_id)

    if not is_allowed:
        raise HTTPException(
            status_code=429,
            detail=f"Hard refresh rate limited. Try again in {seconds_remaining} seconds."
        )

    # Promote to HOT tier
    tier_service = AccountTierService(db)
    await tier_service.promote_to_hot(
        trading_account_id=account_id,
        reason=f"User {user_id} requested hard refresh",
        duration_minutes=5
    )

    # Track sync results
    sync_results = {
        "orders": 0,
        "positions": 0,
        "trades": 0,
        "margins_fetched": False
    }
    drift_detected = False
    drift_details = {}

    # Sync orders
    try:
        order_service = OrderService(db, user_id, account_id)
        order_stats = await order_service.sync_orders_from_broker()
        sync_results["orders"] = order_stats.get("orders_synced", 0)
    except Exception as e:
        logger.error(f"Hard refresh - order sync failed: {e}")

    # Validate positions (detect drift)
    try:
        position_service = PositionService(db, user_id, account_id)
        position_stats = await position_service.validate_positions()
        sync_results["positions"] = position_stats.get("positions_checked", 0)

        if position_stats.get("positions_corrected", 0) > 0:
            drift_detected = True
            drift_details["positions"] = {
                "corrected": position_stats.get("positions_corrected", 0),
                "quantity_drifts": position_stats.get("quantity_drifts", []),
                "pnl_drifts": position_stats.get("pnl_drifts", [])
            }
    except Exception as e:
        logger.error(f"Hard refresh - position validation failed: {e}")

    # Sync trades
    try:
        trade_service = TradeService(db, user_id, account_id)
        trade_stats = await trade_service.sync_trades_from_broker()
        sync_results["trades"] = trade_stats.get("trades_synced", 0)
    except Exception as e:
        logger.error(f"Hard refresh - trade sync failed: {e}")

    # Fetch margins
    try:
        margin_service = MarginService(db, user_id, account_id)
        await margin_service.fetch_and_cache_margins()
        sync_results["margins_fetched"] = True
    except Exception as e:
        logger.error(f"Hard refresh - margin fetch failed: {e}")

    # Record the refresh
    await rate_limiter.record_refresh(account_id)

    # Log the hard refresh
    logger.info(
        f"Hard refresh completed for account {account_id} by user {user_id}: "
        f"orders={sync_results['orders']}, positions={sync_results['positions']}, "
        f"trades={sync_results['trades']}, drift={drift_detected}"
    )

    return HardRefreshResponse(
        status="success",
        account_id=account_id,
        synced=sync_results,
        drift_detected=drift_detected,
        drift_details=drift_details if drift_detected else None,
        tier="HOT",
        tier_expires_in="5 minutes"
    )


@router.get("/{account_id}/refresh-status", response_model=RefreshStatusResponse)
async def get_refresh_status(
    account_id: int,
    current_user: dict = Depends(get_current_user)
):
    """
    Get hard refresh availability status.

    Returns whether hard refresh is available and when.
    Rate limit: 1 refresh per minute per account.

    **Requires 'view' permission** on the account.
    """
    from ....utils.acl_helpers import ACLHelper

    user_id = extract_user_id(current_user)

    # Check ACL permission - viewing refresh status requires view permission
    has_permission = await ACLHelper.check_trading_account_permission(
        user_id=user_id,
        trading_account_id=account_id,
        action="view"
    )

    if not has_permission:
        logger.warning(f"User {user_id} denied view access to account {account_id}")
        raise HTTPException(
            status_code=403,
            detail=f"You do not have permission to view refresh status for account {account_id}"
        )

    rate_limiter = await get_rate_limiter_with_fallback()

    # Check rate limit without consuming it
    key = f"hard_refresh:{account_id}"

    try:
        # Get TTL to see how much time is left on rate limit
        if hasattr(rate_limiter, 'redis'):
            ttl = await rate_limiter.redis.ttl(key)
            if ttl > 0:
                last_refresh = await rate_limiter.get_last_refresh(account_id)
                return RefreshStatusResponse(
                    account_id=account_id,
                    can_refresh=False,
                    seconds_until_available=ttl,
                    last_refresh=last_refresh.isoformat() if last_refresh else None
                )
    except Exception:
        pass  # Fall through to allow refresh

    return RefreshStatusResponse(
        account_id=account_id,
        can_refresh=True,
        seconds_until_available=0,
        last_refresh=None
    )


# ==========================================
# KITE RATE LIMIT ENDPOINTS
# ==========================================

class RateLimitStatus(BaseModel):
    """Rate limit status for a single limit type."""
    limit: int
    current: int
    available: int
    utilization: float


class AccountRateLimitsResponse(BaseModel):
    """Response model for account rate limits."""
    trading_account_id: int
    limits: Dict[str, RateLimitStatus]
    daily_orders: Optional[Dict[str, Any]] = None
    next_reset: Dict[str, Optional[str]]
    status: str  # healthy, near_limit, exceeded


@router.get(
    "/{account_id}/rate-limits",
    response_model=AccountRateLimitsResponse,
    summary="Get account rate limit status",
    description="Get current Kite API rate limit usage for a trading account."
)
async def get_account_rate_limits(
    account_id: int,
    current_user: dict = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Get current Kite API rate limit status for a trading account.

    Returns:
    - Per-second limits (orders, API, quote, historical)
    - Per-minute limits (orders)
    - Per-day limits (orders) - from Redis
    - Next reset times

    Status values:
    - **healthy**: All limits under 80% utilization
    - **near_limit**: Any limit over 80% utilization
    - **exceeded**: Any limit at 100% (requests being throttled)

    **Requires 'view' permission** on the account.
    """
    from ....utils.acl_helpers import ACLHelper

    user_id = extract_user_id(current_user)

    # Check ACL permission - viewing rate limits requires view permission
    has_permission = await ACLHelper.check_trading_account_permission(
        user_id=user_id,
        trading_account_id=account_id,
        action="view"
    )

    if not has_permission:
        logger.warning(f"User {user_id} denied view access to account {account_id}")
        raise HTTPException(
            status_code=403,
            detail=f"You do not have permission to view rate limits for account {account_id}"
        )

    manager = get_rate_limiter_manager_sync()

    if manager is None:
        raise HTTPException(
            status_code=503,
            detail="Rate limiter not initialized"
        )

    # Get account stats
    stats = manager.get_account_stats(account_id)

    if stats is None:
        # Account not cached - create a minimal response
        return AccountRateLimitsResponse(
            trading_account_id=account_id,
            limits={},
            next_reset={},
            status="healthy"
        )

    # Transform stats to response format
    limits = {}
    max_utilization = 0.0

    for limit_name, limit_stats in stats.get("limits", {}).items():
        limits[limit_name] = RateLimitStatus(
            limit=limit_stats["limit"],
            current=limit_stats["current"],
            available=limit_stats["available"],
            utilization=limit_stats["utilization"],
        )
        max_utilization = max(max_utilization, limit_stats["utilization"])

    # Get daily order count
    daily_orders = None
    if manager._daily_counter:
        count = await manager._daily_counter.get_count(account_id)
        remaining = await manager._daily_counter.get_remaining(account_id)
        reset_time = await manager._daily_counter.get_reset_time()

        daily_orders = {
            "limit": manager._daily_counter.daily_limit,
            "used": count,
            "remaining": remaining,
            "utilization": count / manager._daily_counter.daily_limit if manager._daily_counter.daily_limit > 0 else 0,
            "reset_at": reset_time.isoformat(),
        }

        max_utilization = max(max_utilization, daily_orders["utilization"])

    # Determine status
    if max_utilization >= 1.0:
        status = "exceeded"
    elif max_utilization >= 0.8:
        status = "near_limit"
    else:
        status = "healthy"

    # Calculate next reset times
    now = datetime.now(timezone.utc)

    next_reset = {
        "per_second": (now + timedelta(seconds=1)).isoformat(),
        "per_minute": (now + timedelta(seconds=60)).isoformat(),
        "per_day": daily_orders["reset_at"] if daily_orders else None,
    }

    return AccountRateLimitsResponse(
        trading_account_id=account_id,
        limits=limits,
        daily_orders=daily_orders,
        next_reset=next_reset,
        status=status,
    )

"""
Order API Endpoints

Handles order placement, modification, cancellation, and listing.
"""
import logging
from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, Query, Header
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import date

from ....auth import get_current_user, get_trading_account_id
from ....security.internal_auth import validate_order_placement, validate_order_modification, validate_order_cancellation
from ....database import get_db
from ....services import OrderService
from ....services.idempotency import get_idempotency_service
from ....utils.user_id import extract_user_id
from ....utils.acl_helpers import ACLHelper
from ....services.market_hours import MarketHoursService
from ..schemas import (
    PlaceOrderRequest,
    ModifyOrderRequest,
    OrderResponse,
    OrderListResponse,
    BatchOrderRequest,
    BatchOrderResponse,
    ErrorResponse
)

# Additional response model for orders summary
class OrdersSummaryResponse(BaseModel):
    """Orders summary response for dashboard"""
    trading_account_id: str
    total_orders_today: int
    pending_orders: int
    executed_orders: int
    rejected_orders: int
    cancelled_orders: int

logger = logging.getLogger(__name__)

router = APIRouter()


# ==========================================
# PLACE ORDER
# ==========================================

@router.post(
    "/orders",
    response_model=OrderResponse,
    status_code=201,
    responses={
        400: {"model": ErrorResponse, "description": "Invalid order parameters"},
        401: {"model": ErrorResponse, "description": "Unauthorized"},
        500: {"model": ErrorResponse, "description": "Order placement failed"}
    },
    summary="Place a new order",
    description="Place a new trading order. Requires JWT authentication."
)
async def place_order(
    order_request: PlaceOrderRequest,
    current_user: dict = Depends(get_current_user),
    trading_account_id: int = Depends(get_trading_account_id),
    db: AsyncSession = Depends(get_db),
    idempotency_key: Optional[str] = Header(None, alias="Idempotency-Key"),
    service_identity: str = Depends(validate_order_placement)  # Enhanced security
):
    """
    Place a new order.

    - **strategy_id**: Strategy ID (required) - links order to strategy for P&L tracking
    - **symbol**: Trading symbol (e.g., RELIANCE, NIFTY25DEC24500CE)
    - **exchange**: Exchange code (NSE, NFO, BSE, etc.)
    - **transaction_type**: BUY or SELL
    - **quantity**: Order quantity (must be > 0)
    - **order_type**: MARKET, LIMIT, SL, or SL-M
    - **product_type**: CNC (delivery), MIS (intraday), or NRML (normal)
    - **price**: Limit price (required for LIMIT orders)
    - **trigger_price**: Trigger price (required for SL orders)
    - **validity**: DAY or IOC (Immediate or Cancel)
    - **variety**: regular, amo, iceberg, or auction
    - **disclosed_quantity**: Disclosed quantity (required for iceberg orders)

    **Idempotency**: Include `Idempotency-Key` header to prevent duplicate orders.
    Same key with same request returns cached response. Valid for 24 hours.

    Returns the created order with broker order ID.
    """
    user_id = extract_user_id(current_user)

    # ACL Check: Verify user has trade permission on trading account
    has_permission = await ACLHelper.check_trading_account_permission(
        user_id=user_id,
        trading_account_id=trading_account_id,
        action="trade"
    )
    if not has_permission:
        raise HTTPException(
            status_code=403,
            detail="No trade permission for this trading account"
        )

    # Market Status Check: Ensure markets are open
    segment = MarketHoursService.get_segment_from_symbol(order_request.symbol)
    market_state = await MarketHoursService.get_market_state_async(segment)
    if not await MarketHoursService.can_place_orders_async(segment):
        raise HTTPException(
            status_code=400,
            detail=f"Cannot place order: Market is {market_state.value}"
        )

    # Check idempotency if key provided
    if idempotency_key:
        idempotency_service = get_idempotency_service()

        # Convert request to dict for idempotency check
        request_data = order_request.dict()

        # Check if this request was already processed
        cached_response = await idempotency_service.check_and_store(
            idempotency_key=idempotency_key,
            user_id=user_id,
            request_data=request_data
        )

        if cached_response:
            # Return cached response (duplicate request)
            logger.info(f"Returning cached order response for idempotency key {idempotency_key[:16]}...")
            return OrderResponse(**cached_response)

    # Place new order
    service = OrderService(db, user_id, trading_account_id)

    order = await service.place_order(
        strategy_id=order_request.strategy_id,
        symbol=order_request.symbol,
        exchange=order_request.exchange,
        transaction_type=order_request.transaction_type,
        quantity=order_request.quantity,
        order_type=order_request.order_type,
        product_type=order_request.product_type,
        price=order_request.price,
        trigger_price=order_request.trigger_price,
        validity=order_request.validity,
        variety=order_request.variety,
        disclosed_quantity=order_request.disclosed_quantity,
        tag=order_request.tag,
    )

    # Convert to dict first to avoid detached ORM object issues
    order_dict = {
        "id": order.id,
        "strategy_id": order.strategy_id,
        "user_id": order.user_id,
        "trading_account_id": order.trading_account_id,
        "broker_order_id": order.broker_order_id,
        "symbol": order.symbol,
        "exchange": order.exchange,
        "transaction_type": order.transaction_type,
        "order_type": order.order_type,
        "product_type": order.product_type,
        "variety": order.variety,
        "quantity": order.quantity,
        "filled_quantity": order.filled_quantity,
        "pending_quantity": order.pending_quantity,
        "cancelled_quantity": order.cancelled_quantity,
        "price": order.price,
        "trigger_price": order.trigger_price,
        "average_price": order.average_price,
        "status": order.status,
        "status_message": order.status_message,
        "validity": order.validity,
        "created_at": order.created_at,
        "updated_at": order.updated_at,
        "submitted_at": order.submitted_at,
        "risk_check_passed": order.risk_check_passed,
    }
    response = OrderResponse(**order_dict)

    # Store response for idempotency (if key provided)
    if idempotency_key:
        await idempotency_service.store_response(
            idempotency_key=idempotency_key,
            user_id=user_id,
            request_data=request_data,
            response_data=response.dict()
        )

    return response


# ==========================================
# BATCH ORDER PLACEMENT
# ==========================================

@router.post(
    "/orders/batch",
    response_model=BatchOrderResponse,
    status_code=201,
    responses={
        400: {"model": ErrorResponse, "description": "Invalid batch or order parameters"},
        401: {"model": ErrorResponse, "description": "Unauthorized"},
        500: {"model": ErrorResponse, "description": "Batch order placement failed"},
        503: {"model": ErrorResponse, "description": "Broker service unavailable"}
    },
    summary="Place multiple orders in a batch",
    description="Place 1-20 orders atomically or independently. Atomic mode rolls back all orders if any fails."
)
async def place_batch_orders(
    batch_request: BatchOrderRequest,
    current_user: dict = Depends(get_current_user),
    trading_account_id: int = Depends(get_trading_account_id),
    db: AsyncSession = Depends(get_db)
):
    """
    Place multiple orders in a batch (1-20 orders).

    **Atomic Mode (default)**:
    - All orders succeed or all fail
    - If any order fails, all previously placed orders are cancelled
    - Guarantees consistency for multi-leg strategies

    **Non-Atomic Mode**:
    - Orders are placed independently
    - Failures don't affect successful orders
    - Useful for placing multiple unrelated orders

    **Features**:
    - Per-order validation before submission
    - Circuit breaker protection
    - Retry logic with exponential backoff
    - Detailed status for each order
    - Batch tagging for tracking

    **Example - Iron Condor**:
    ```json
    {
      "atomic": true,
      "tag_prefix": "iron_condor",
      "orders": [
        {"symbol": "NIFTY25DEC24500CE", "transaction_type": "SELL", ...},
        {"symbol": "NIFTY25DEC24600CE", "transaction_type": "BUY", ...},
        {"symbol": "NIFTY25DEC24400PE", "transaction_type": "SELL", ...},
        {"symbol": "NIFTY25DEC24300PE", "transaction_type": "BUY", ...}
      ]
    }
    ```

    Returns detailed results for each order including success status, broker order ID, or error message.
    """
    user_id = extract_user_id(current_user)

    # ACL Check: Verify user has trade permission on trading account
    has_permission = await ACLHelper.check_trading_account_permission(
        user_id=user_id,
        trading_account_id=trading_account_id,
        action="trade"
    )
    if not has_permission:
        raise HTTPException(
            status_code=403,
            detail="No trade permission for this trading account"
        )

    # Market Status Check: Ensure markets are open for all symbols
    for order in batch_request.orders:
        segment = MarketHoursService.get_segment_from_symbol(order.symbol)
        if not await MarketHoursService.can_place_orders_async(segment):
            market_state = await MarketHoursService.get_market_state_async(segment)
            raise HTTPException(
                status_code=400,
                detail=f"Cannot place order for {order.symbol}: Market is {market_state.value}"
            )

    service = OrderService(db, user_id, trading_account_id)

    # Convert Pydantic models to dicts
    orders_dict = [order.dict(exclude_none=True) for order in batch_request.orders]

    # Place batch orders
    result = await service.place_batch_orders(
        orders=orders_dict,
        atomic=batch_request.atomic,
        tag_prefix=batch_request.tag_prefix
    )

    # Convert result to response model
    return BatchOrderResponse(**result)


# ==========================================
# MODIFY ORDER
# ==========================================

@router.put(
    "/orders/{order_id}",
    response_model=OrderResponse,
    responses={
        400: {"model": ErrorResponse, "description": "Cannot modify order"},
        404: {"model": ErrorResponse, "description": "Order not found"},
        500: {"model": ErrorResponse, "description": "Order modification failed"}
    },
    summary="Modify an existing order",
    description="Modify quantity, price, or order type of an existing order."
)
async def modify_order(
    order_id: int,
    modify_request: ModifyOrderRequest,
    current_user: dict = Depends(get_current_user),
    trading_account_id: int = Depends(get_trading_account_id),
    db: AsyncSession = Depends(get_db)
):
    """
    Modify an existing order.

    You can modify:
    - **quantity**: Change order quantity
    - **price**: Change limit price
    - **trigger_price**: Change trigger price (for SL orders)
    - **order_type**: Change order type

    Only orders in PENDING, SUBMITTED, or OPEN status can be modified.
    """
    user_id = extract_user_id(current_user)

    # First, get the order to check its trading_account_id
    service = OrderService(db, user_id, trading_account_id)
    existing_order = await service.get_order(order_id)

    # ACL Check: Verify user has trade permission on the order's trading account
    has_permission = await ACLHelper.check_trading_account_permission(
        user_id=user_id,
        trading_account_id=int(existing_order.trading_account_id),
        action="trade"
    )
    if not has_permission:
        raise HTTPException(
            status_code=403,
            detail="No trade permission for this order's trading account"
        )

    order = await service.modify_order(
        order_id=order_id,
        quantity=modify_request.quantity,
        price=modify_request.price,
        trigger_price=modify_request.trigger_price,
        order_type=modify_request.order_type,
    )

    return OrderResponse.from_orm(order)


# ==========================================
# CANCEL ORDER
# ==========================================

@router.delete(
    "/orders/{order_id}",
    response_model=OrderResponse,
    responses={
        400: {"model": ErrorResponse, "description": "Cannot cancel order"},
        404: {"model": ErrorResponse, "description": "Order not found"},
        500: {"model": ErrorResponse, "description": "Order cancellation failed"}
    },
    summary="Cancel an order",
    description="Cancel a pending or open order."
)
async def cancel_order(
    order_id: int,
    current_user: dict = Depends(get_current_user),
    trading_account_id: int = Depends(get_trading_account_id),
    db: AsyncSession = Depends(get_db)
):
    """
    Cancel an order.

    Only orders in PENDING, SUBMITTED, OPEN, or TRIGGER_PENDING status can be cancelled.
    """
    user_id = extract_user_id(current_user)

    # First, get the order to check its trading_account_id
    service = OrderService(db, user_id, trading_account_id)
    existing_order = await service.get_order(order_id)

    # ACL Check: Verify user has trade permission on the order's trading account
    has_permission = await ACLHelper.check_trading_account_permission(
        user_id=user_id,
        trading_account_id=int(existing_order.trading_account_id),
        action="trade"
    )
    if not has_permission:
        raise HTTPException(
            status_code=403,
            detail="No trade permission for this order's trading account"
        )

    order = await service.cancel_order(order_id)

    return OrderResponse.from_orm(order)


# ==========================================
# ORDERS SUMMARY FOR DASHBOARD
# ==========================================

@router.get(
    "/orders/summary",
    response_model=OrdersSummaryResponse,
    summary="Get orders summary for dashboard",
    description="Get aggregated order counts by status for today with optional filtering."
)
async def get_orders_summary(
    trading_account_id: int = Query(..., description="Trading account ID"),
    position_id: Optional[int] = Query(None, description="Filter by position ID"),
    status: Optional[str] = Query(None, description="Filter by order status"),
    symbol: Optional[str] = Query(None, description="Filter by symbol"),
    current_user: dict = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Get orders summary for dashboard.

    Returns:
    - Total orders today
    - Orders by status (pending, executed, rejected, cancelled)

    Optional filters:
    - **position_id**: Filter by position ID
    - **status**: Filter by order status (PENDING, OPEN, COMPLETE, etc.)
    - **symbol**: Filter by trading symbol
    """
    from ....database.redis_client import get_redis
    import json

    user_id = extract_user_id(current_user)

    # ACL Check: Verify user has view permission on trading account
    has_permission = await ACLHelper.check_trading_account_permission(
        user_id=user_id,
        trading_account_id=trading_account_id,
        action="view"
    )
    if not has_permission:
        raise HTTPException(
            status_code=403,
            detail="No view permission for this trading account"
        )

    today = date.today()

    # Build cache key with filters (Issue #426)
    cache_key_parts = [
        "orders:summary",
        str(trading_account_id),
        str(position_id or "all"),
        status or "all",
        symbol or "all"
    ]
    cache_key = ":".join(cache_key_parts)

    # Check cache first
    try:
        redis = get_redis()
        cached_data = await redis.get(cache_key)
        if cached_data:
            logger.info(f"Returning cached orders summary for key {cache_key}")
            return OrdersSummaryResponse(**json.loads(cached_data))
    except (ConnectionError, TimeoutError, OSError) as e:
        logger.warning(f"Redis cache unavailable for orders summary, proceeding without cache: {e}")
    except Exception as e:
        logger.error(f"Unexpected Redis error for orders summary cache check: {e}")

    # Query orders for today with status aggregation
    from ....models.order import Order
    from sqlalchemy import func, and_

    # Build filters (trading_account_id is VARCHAR in DB, so cast to string)
    filters = [
        Order.trading_account_id == str(trading_account_id),
        Order.user_id == user_id,
        func.date(Order.created_at) == today
    ]

    # Add optional filters
    if position_id is not None:
        filters.append(Order.position_id == position_id)
    if status:
        filters.append(Order.status == status)
    if symbol:
        filters.append(Order.symbol == symbol)

    # Execute query
    result = await db.execute(
        Order.__table__.select().where(and_(*filters))
    )

    orders = result.fetchall()

    # Count by status
    total_orders = len(orders)
    pending_orders = sum(1 for o in orders if o.status in ('PENDING', 'SUBMITTED', 'OPEN', 'TRIGGER_PENDING'))
    executed_orders = sum(1 for o in orders if o.status == 'COMPLETE')
    rejected_orders = sum(1 for o in orders if o.status == 'REJECTED')
    cancelled_orders = sum(1 for o in orders if o.status == 'CANCELLED')

    response_data = {
        "trading_account_id": trading_account_id,
        "total_orders_today": total_orders,
        "pending_orders": pending_orders,
        "executed_orders": executed_orders,
        "rejected_orders": rejected_orders,
        "cancelled_orders": cancelled_orders
    }

    # Cache the response (5 minutes TTL)
    try:
        await redis.setex(cache_key, 300, json.dumps(response_data))
        logger.info(f"Cached orders summary for key {cache_key}")
    except (ConnectionError, TimeoutError, OSError) as e:
        logger.warning(f"Failed to cache orders summary due to Redis connectivity: {e}")
    except Exception as e:
        logger.error(f"Unexpected error caching orders summary: {e}")

    return OrdersSummaryResponse(**response_data)


# ==========================================
# GET ORDER
# ==========================================

@router.get(
    "/orders/{order_id}",
    response_model=OrderResponse,
    responses={
        404: {"model": ErrorResponse, "description": "Order not found"}
    },
    summary="Get order by ID",
    description="Retrieve order details by order ID."
)
async def get_order(
    order_id: int,
    current_user: dict = Depends(get_current_user),
    trading_account_id: int = Depends(get_trading_account_id),
    db: AsyncSession = Depends(get_db)
):
    """
    Get order details by ID.

    Returns complete order information including status, quantities, and prices.
    """
    user_id = extract_user_id(current_user)

    # First, get the order to check its trading_account_id
    service = OrderService(db, user_id, trading_account_id)
    order = await service.get_order(order_id)

    # ACL Check: Verify user has view permission on the order's trading account
    has_permission = await ACLHelper.check_trading_account_permission(
        user_id=user_id,
        trading_account_id=int(order.trading_account_id),
        action="view"
    )
    if not has_permission:
        raise HTTPException(
            status_code=403,
            detail="No view permission for this order's trading account"
        )

    return OrderResponse.from_orm(order)


# ==========================================
# LIST ORDERS
# ==========================================

@router.get(
    "/orders",
    response_model=OrderListResponse,
    summary="List orders",
    description="List user's orders with optional filtering."
)
async def list_orders(
    symbol: Optional[str] = Query(None, description="Filter by symbol"),
    status: Optional[str] = Query(None, description="Filter by status"),
    position_id: Optional[int] = Query(None, description="Filter by position ID"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum orders to return"),
    offset: int = Query(0, ge=0, description="Number of orders to skip"),
    today_only: bool = Query(True, description="Only return today's orders (default: True). Set to False for all orders."),
    current_user: dict = Depends(get_current_user),
    trading_account_id: Optional[str] = Depends(get_trading_account_id),
    db: AsyncSession = Depends(get_db)
):
    """
    List user's orders.

    **GitHub Issue #439: "All Accounts" Aggregation Support**
    - Omit X-Trading-Account-ID header to get orders from all accessible accounts

    Supports filtering by:
    - **symbol**: Filter by trading symbol
    - **status**: Filter by order status (PENDING, SUBMITTED, OPEN, COMPLETE, CANCELLED, REJECTED)
    - **position_id**: Filter by position ID
    - **limit**: Maximum number of orders to return (1-1000, default 100)
    - **offset**: Number of orders to skip for pagination

    Orders are returned in descending order by creation time (newest first).
    """
    user_id = extract_user_id(current_user)

    # Check if this is "All Accounts" mode (Issue #439)
    if trading_account_id is None:
        logger.info(f"All Accounts mode: aggregating orders for user {user_id}")

        # ACL Check: Get all accessible trading accounts with view permission
        accessible_account_ids = await ACLHelper.get_accessible_resources(
            user_id=user_id,
            resource_type="trading_account",
            min_action="view"
        )

        if not accessible_account_ids:
            return OrderListResponse(
                orders=[],
                total=0,
                limit=limit,
                offset=offset
            )

        # Aggregate orders from accessible accounts
        from ....services.account_aggregation import aggregate_orders
        result = await aggregate_orders(db, accessible_account_ids, limit=limit)

        return {
            "orders": result["orders"],
            "total": result["total"],
            "limit": result["limit"],
            "offset": offset,
            "aggregated": True,
            "account_count": result["account_count"]
        }

    # Single account mode - hierarchical ACL permission check
    # ACL Integration: Check account-level OR order-level permissions
    has_account_access, accessible_order_ids = await ACLHelper.get_accessible_resources_with_hierarchy(
        user_id=user_id,
        resource_type="order",
        trading_account_id=int(trading_account_id),
        min_action="view"
    )

    if not has_account_access and not accessible_order_ids:
        # User has neither account access nor specific order access
        logger.warning(
            f"User {user_id} denied access to trading account {trading_account_id} "
            f"and has no specific order permissions"
        )
        raise HTTPException(
            status_code=403,
            detail=f"You do not have permission to view orders for trading account {trading_account_id}"
        )

    service = OrderService(db, user_id, trading_account_id)

    if has_account_access:
        # Full account access - return all orders
        logger.info(f"User {user_id} has full account access to {trading_account_id}")
        orders = await service.list_orders(
            symbol=symbol,
            status=status,
            position_id=position_id,
            limit=limit,
            offset=offset,
            today_only=today_only
        )
    else:
        # Granular access - filter to specific orders
        logger.info(
            f"User {user_id} has granular access to {len(accessible_order_ids)} "
            f"orders in account {trading_account_id}"
        )
        orders = await service.list_orders(
            symbol=symbol,
            status=status,
            position_id=position_id,
            limit=limit,
            offset=offset,
            today_only=today_only,
            order_ids=accessible_order_ids  # Filter to accessible orders
        )

    total = await service.count_orders(
        symbol=symbol,
        status=status,
        position_id=position_id,
        today_only=today_only,
        order_ids=accessible_order_ids if not has_account_access else None
    )

    return OrderListResponse(
        orders=[OrderResponse.from_orm(order) for order in orders],
        total=total,
        limit=limit,
        offset=offset
    )


# ==========================================
# GET ORDER HISTORY
# ==========================================

@router.get(
    "/orders/{order_id}/history",
    response_model=List[dict],
    responses={
        404: {"model": ErrorResponse, "description": "Order not found"}
    },
    summary="Get order history",
    description="Get order status change history from broker."
)
async def get_order_history(
    order_id: int,
    current_user: dict = Depends(get_current_user),
    trading_account_id: int = Depends(get_trading_account_id),
    db: AsyncSession = Depends(get_db)
):
    """
    Get order history.

    Returns a list of status changes from the broker, including:
    - Order placement
    - Status updates
    - Partial fills
    - Complete fills
    - Rejections
    - Cancellations
    """
    user_id = extract_user_id(current_user)

    # First, get the order to check its trading_account_id
    service = OrderService(db, user_id, trading_account_id)
    order = await service.get_order(order_id)

    # ACL Check: Verify user has view permission on the order's trading account
    has_permission = await ACLHelper.check_trading_account_permission(
        user_id=user_id,
        trading_account_id=int(order.trading_account_id),
        action="view"
    )
    if not has_permission:
        raise HTTPException(
            status_code=403,
            detail="No view permission for this order's trading account"
        )

    history = await service.get_order_history(order_id)

    return history


# ==========================================
# ORDER AUDIT TRAIL
# ==========================================

@router.get(
    "/orders/{order_id}/audit-trail",
    response_model=List[dict],
    responses={
        404: {"model": ErrorResponse, "description": "Order not found"}
    },
    summary="Get order audit trail",
    description="Get complete audit trail for an order (all state transitions with actors and timestamps)."
)
async def get_order_audit_trail(
    order_id: int,
    current_user: dict = Depends(get_current_user),
    trading_account_id: int = Depends(get_trading_account_id),
    db: AsyncSession = Depends(get_db)
):
    """
    Get complete audit trail for an order.

    Returns all state transitions logged in the database, including:
    - Order creation
    - Broker submissions
    - Modifications (who changed what)
    - Cancellations (who cancelled)
    - Reconciliation corrections
    - System actions

    Each entry includes:
    - old_status → new_status
    - Who made the change (user or system)
    - Why it changed (reason)
    - When it changed (timestamp)
    - Additional context (metadata)

    Useful for:
    - Compliance audits (SEBI requirements)
    - Debugging order flow
    - Security investigations
    """
    from ....services.audit_service import OrderAuditService

    user_id = extract_user_id(current_user)

    # Verify user owns this order
    service = OrderService(db, user_id, trading_account_id)
    order = await service.get_order(order_id)

    # ACL Check: Verify user has view permission on the order's trading account
    has_permission = await ACLHelper.check_trading_account_permission(
        user_id=user_id,
        trading_account_id=int(order.trading_account_id),
        action="view"
    )
    if not has_permission:
        raise HTTPException(
            status_code=403,
            detail="No view permission for this order's trading account"
        )

    # Get audit history
    audit_service = OrderAuditService(db)
    history = await audit_service.get_order_history(order_id)

    return [h.to_dict() for h in history]


# ==========================================
# ORDER SYNC
# ==========================================

@router.post(
    "/orders/sync",
    response_model=dict,
    summary="Sync orders from broker",
    description="Fetch today's orders directly from the broker API (Kite Connect)."
)
async def sync_orders_from_broker(
    current_user: dict = Depends(get_current_user),
    trading_account_id: int = Depends(get_trading_account_id),
    db: AsyncSession = Depends(get_db)
):
    """
    Sync orders from broker.

    Fetches all orders for today from Kite Connect API.
    This includes orders placed via:
    - Kite web/app
    - API (our system)
    - AMO (after market orders)
    - Any other source

    Returns broker orders directly without storing them in database.
    Useful for:
    - After market hours when you need to see all orders
    - Reconciliation with external order placement
    - Audit and verification
    """
    user_id = extract_user_id(current_user)

    # ACL Check: Verify user has view permission on trading account (sync is read-only)
    has_permission = await ACLHelper.check_trading_account_permission(
        user_id=user_id,
        trading_account_id=trading_account_id,
        action="view"
    )
    if not has_permission:
        raise HTTPException(
            status_code=403,
            detail="No view permission for this trading account"
        )

    service = OrderService(db, user_id, trading_account_id)

    result = await service.sync_orders_from_broker()

    return result

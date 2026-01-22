"""
GTT Order API Endpoints

REST API for Good-Till-Triggered (GTT) conditional orders.
"""
import logging
from datetime import datetime
from typing import List, Optional
from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel, Field

from ....auth import get_current_user, get_trading_account_id
from ....database.connection import get_db
from ....services.gtt_service import GttService
from ....utils.user_id import extract_user_id

logger = logging.getLogger(__name__)

router = APIRouter()


# ==========================================
# REQUEST/RESPONSE MODELS
# ==========================================

class GttOrderSpec(BaseModel):
    """Order specification for GTT"""
    transaction_type: str = Field(..., description="BUY or SELL")
    quantity: int = Field(..., gt=0, description="Order quantity")
    order_type: str = Field(..., description="MARKET or LIMIT")
    product: str = Field(..., description="CNC, MIS, or NRML")
    price: Optional[float] = Field(None, description="Limit price (required for LIMIT orders)")


class PlaceGttRequest(BaseModel):
    """Request model for placing GTT order"""
    gtt_type: str = Field(..., description="single or two-leg (OCO)")
    symbol: str = Field(..., description="Trading symbol (e.g., RELIANCE, NIFTY)")
    exchange: str = Field(..., description="Exchange (NSE, NFO, BSE, etc.)")
    symbol: str = Field(..., description="Broker's trading symbol")
    trigger_values: List[float] = Field(..., description="Trigger price(s): [price] or [price1, price2]")
    last_price: float = Field(..., description="Current market price")
    orders: List[GttOrderSpec] = Field(..., min_items=1, description="Orders to place when triggered")
    expires_at: Optional[datetime] = Field(None, description="GTT expiry time (optional)")
    user_tag: Optional[str] = Field(None, max_length=100, description="Custom user tag")
    user_notes: Optional[str] = Field(None, description="User notes")

    class Config:
        json_schema_extra = {
            "example": {
                "gtt_type": "single",
                "symbol": "RELIANCE",
                "exchange": "NSE",
                "symbol": "RELIANCE",
                "trigger_values": [2400],
                "last_price": 2500,
                "orders": [
                    {
                        "transaction_type": "SELL",
                        "quantity": 10,
                        "order_type": "LIMIT",
                        "product": "CNC",
                        "price": 2400
                    }
                ],
                "user_tag": "stop_loss",
                "user_notes": "Stop-loss at 2400"
            }
        }


class ModifyGttRequest(BaseModel):
    """Request model for modifying GTT order"""
    trigger_values: List[float] = Field(..., description="New trigger price(s)")
    last_price: float = Field(..., description="Current market price")
    orders: List[GttOrderSpec] = Field(..., min_items=1, description="Updated orders")


class GttOrderResponse(BaseModel):
    """GTT order response model"""
    id: int
    user_id: int
    trading_account_id: int
    broker_gtt_id: Optional[int]
    gtt_type: str
    status: str
    symbol: str
    exchange: str
    tradingsymbol: str
    condition: dict
    orders: list
    expires_at: Optional[str]
    created_at: str
    updated_at: str
    triggered_at: Optional[str]
    cancelled_at: Optional[str]
    user_tag: Optional[str]
    user_notes: Optional[str]

    class Config:
        from_attributes = True


class GttListResponse(BaseModel):
    """GTT list response"""
    gtt_orders: List[GttOrderResponse]
    total: int
    limit: int
    offset: int


class GttSyncResponse(BaseModel):
    """GTT sync response"""
    gtts_synced: int
    gtts_updated: int
    gtts_created: int
    errors: List[str]


# ==========================================
# ENDPOINTS
# ==========================================

@router.post("/gtt/orders", response_model=GttOrderResponse, status_code=201)
async def place_gtt_order(
    request: PlaceGttRequest,
    current_user: dict = Depends(get_current_user),
    trading_account_id: int = Depends(get_trading_account_id),
    db = Depends(get_db)
):
    """
    Place a new GTT (Good-Till-Triggered) order.

    GTT orders are conditional orders that remain pending until market conditions are met.

    **GTT Types**:
    - **single**: Stop-loss or target orders (one trigger price)
    - **two-leg**: OCO (One Cancels Other) orders (two trigger prices)

    **Example - Stop-Loss**:
    ```json
    {
      "gtt_type": "single",
      "symbol": "RELIANCE",
      "exchange": "NSE",
      "symbol": "RELIANCE",
      "trigger_values": [2400],
      "last_price": 2500,
      "orders": [{
        "transaction_type": "SELL",
        "quantity": 10,
        "order_type": "LIMIT",
        "product": "CNC",
        "price": 2400
      }]
    }
    ```

    **Example - OCO (Stop-Loss + Target)**:
    ```json
    {
      "gtt_type": "two-leg",
      "symbol": "RELIANCE",
      "exchange": "NSE",
      "symbol": "RELIANCE",
      "trigger_values": [2400, 2600],
      "last_price": 2500,
      "orders": [
        {"transaction_type": "SELL", "quantity": 10, "price": 2400, ...},
        {"transaction_type": "SELL", "quantity": 10, "price": 2600, ...}
      ]
    }
    ```
    """
    user_id = extract_user_id(current_user)

    service = GttService(db, user_id, trading_account_id)

    # Convert orders to dictionaries
    orders_dict = [order.dict(exclude_none=True) for order in request.orders]

    gtt_order = await service.place_gtt_order(
        gtt_type=request.gtt_type,
        symbol=request.symbol,
        exchange=request.exchange,
        tradingsymbol=request.tradingsymbol,
        trigger_values=request.trigger_values,
        last_price=request.last_price,
        orders=orders_dict,
        expires_at=request.expires_at,
        user_tag=request.user_tag,
        user_notes=request.user_notes
    )

    return GttOrderResponse(**gtt_order)


@router.get("/gtt/orders", response_model=GttListResponse)
async def list_gtt_orders(
    status: Optional[str] = Query(None, description="Filter by status (active, triggered, cancelled)"),
    symbol: Optional[str] = Query(None, description="Filter by symbol"),
    gtt_type: Optional[str] = Query(None, description="Filter by GTT type (single, two-leg)"),
    limit: int = Query(100, ge=1, le=500, description="Maximum GTT orders to return"),
    offset: int = Query(0, ge=0, description="Number of GTT orders to skip"),
    current_user: dict = Depends(get_current_user),
    trading_account_id: int = Depends(get_trading_account_id),
    db = Depends(get_db)
):
    """
    List user's GTT orders with optional filtering.

    - **status**: Filter by status (active, triggered, cancelled, expired, deleted)
    - **symbol**: Filter by trading symbol
    - **gtt_type**: Filter by GTT type (single, two-leg)
    - **limit**: Maximum number of GTT orders to return
    - **offset**: Number of GTT orders to skip for pagination
    """
    user_id = extract_user_id(current_user)

    service = GttService(db, user_id, trading_account_id)
    gtt_orders = await service.list_gtt_orders(
        status=status,
        symbol=symbol,
        gtt_type=gtt_type,
        limit=limit,
        offset=offset
    )

    return GttListResponse(
        gtt_orders=[GttOrderResponse(**gtt) for gtt in gtt_orders],
        total=len(gtt_orders),
        limit=limit,
        offset=offset
    )


@router.get("/gtt/orders/{gtt_id}", response_model=GttOrderResponse)
async def get_gtt_order(
    gtt_id: int,
    current_user: dict = Depends(get_current_user),
    trading_account_id: int = Depends(get_trading_account_id),
    db = Depends(get_db)
):
    """
    Get a specific GTT order by ID.

    - **gtt_id**: GTT order ID
    """
    user_id = extract_user_id(current_user)

    service = GttService(db, user_id, trading_account_id)
    gtt_order = await service.get_gtt_order(gtt_id)

    return GttOrderResponse(**gtt_order)


@router.put("/gtt/orders/{gtt_id}", response_model=GttOrderResponse)
async def modify_gtt_order(
    gtt_id: int,
    request: ModifyGttRequest,
    current_user: dict = Depends(get_current_user),
    trading_account_id: int = Depends(get_trading_account_id),
    db = Depends(get_db)
):
    """
    Modify an existing GTT order.

    **Note**: Only active GTT orders can be modified.

    **Path Parameters**:
    - `gtt_id`: GTT order ID to modify

    **Request Body**:
    - `trigger_values`: New trigger price(s)
    - `last_price`: Current market price
    - `orders`: Updated order specifications
    """
    user_id = extract_user_id(current_user)

    service = GttService(db, user_id, trading_account_id)

    # Convert orders to dictionaries
    orders_dict = [order.dict(exclude_none=True) for order in request.orders]

    gtt_order = await service.modify_gtt_order(
        gtt_id=gtt_id,
        trigger_values=request.trigger_values,
        last_price=request.last_price,
        orders=orders_dict
    )

    return GttOrderResponse(**gtt_order)


@router.delete("/gtt/orders/{gtt_id}", response_model=GttOrderResponse)
async def cancel_gtt_order(
    gtt_id: int,
    current_user: dict = Depends(get_current_user),
    trading_account_id: int = Depends(get_trading_account_id),
    db = Depends(get_db)
):
    """
    Cancel (delete) a GTT order.

    **Note**: Only active GTT orders can be cancelled.

    **Path Parameters**:
    - `gtt_id`: GTT order ID to cancel
    """
    user_id = extract_user_id(current_user)

    service = GttService(db, user_id, trading_account_id)
    gtt_order = await service.cancel_gtt_order(gtt_id)

    return GttOrderResponse(**gtt_order)


@router.post("/gtt/sync", response_model=GttSyncResponse)
async def sync_gtt_orders(
    current_user: dict = Depends(get_current_user),
    trading_account_id: int = Depends(get_trading_account_id),
    db = Depends(get_db)
):
    """
    Sync GTT orders from broker.

    Fetches all active GTT orders from the broker API and updates the local database.
    This is useful for:
    - Initial GTT sync
    - Manual refresh of GTT data
    - Reconciliation after network issues

    Returns:
    - Number of GTT orders synced
    - Number created/updated
    - Any errors encountered
    """
    user_id = extract_user_id(current_user)

    service = GttService(db, user_id, trading_account_id)
    stats = await service.sync_gtt_orders_from_broker()

    return GttSyncResponse(**stats)

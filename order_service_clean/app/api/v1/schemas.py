"""
Pydantic schemas for request/response validation
"""
from pydantic import BaseModel, Field, validator
from typing import Optional, List
from datetime import datetime


# ==========================================
# ORDER SCHEMAS
# ==========================================

class PlaceOrderRequest(BaseModel):
    """Request schema for placing an order"""
    strategy_id: int = Field(..., gt=0, description="Strategy ID (required) - links order to strategy for P&L tracking", example=1)
    symbol: str = Field(..., description="Trading symbol", example="RELIANCE")
    exchange: str = Field(..., description="Exchange", example="NSE")
    transaction_type: str = Field(..., description="BUY or SELL", example="BUY")
    quantity: int = Field(..., gt=0, description="Order quantity", example=10)
    order_type: str = Field(..., description="MARKET, LIMIT, SL, SL-M", example="LIMIT")
    product_type: str = Field(..., description="CNC, MIS, NRML", example="CNC")
    price: Optional[float] = Field(None, description="Limit price", example=2450.50)
    trigger_price: Optional[float] = Field(None, description="Trigger price (for SL orders)")
    validity: str = Field("DAY", description="DAY or IOC", example="DAY")
    variety: str = Field("regular", description="Order variety: regular, amo, iceberg, auction, co (Cover Order), bo (Bracket Order)", example="regular")
    disclosed_quantity: Optional[int] = Field(None, gt=0, description="Disclosed quantity (for iceberg orders)")
    tag: Optional[str] = Field(None, description="Custom order tag", max_length=50)

    @validator("strategy_id")
    def validate_strategy_id(cls, v):
        """Validate strategy_id is provided (existence check done at service layer)"""
        if v <= 0:
            raise ValueError("strategy_id must be a positive integer")
        return v

    @validator("transaction_type")
    def validate_transaction_type(cls, v):
        if v not in ["BUY", "SELL"]:
            raise ValueError("transaction_type must be BUY or SELL")
        return v

    @validator("order_type")
    def validate_order_type(cls, v):
        if v not in ["MARKET", "LIMIT", "SL", "SL-M"]:
            raise ValueError("order_type must be MARKET, LIMIT, SL, or SL-M")
        return v

    @validator("product_type")
    def validate_product_type(cls, v):
        if v not in ["CNC", "MIS", "NRML"]:
            raise ValueError("product_type must be CNC, MIS, or NRML")
        return v

    @validator("validity")
    def validate_validity(cls, v):
        if v not in ["DAY", "IOC"]:
            raise ValueError("validity must be DAY or IOC")
        return v

    @validator("variety")
    def validate_variety(cls, v):
        if v not in ["regular", "amo", "iceberg", "auction", "co", "bo"]:
            raise ValueError("variety must be regular, amo, iceberg, auction, co, or bo")
        return v

    @validator("disclosed_quantity")
    def validate_disclosed_quantity(cls, v, values):
        if v is not None:
            # Must use iceberg variety
            variety = values.get("variety", "regular")
            if variety != "iceberg":
                raise ValueError("disclosed_quantity is only valid for iceberg variety")

            # Cannot exceed total quantity
            quantity = values.get("quantity", 0)
            if v > quantity:
                raise ValueError(
                    f"disclosed_quantity ({v}) cannot exceed total quantity ({quantity})"
                )
        return v

    @validator("trigger_price")
    def validate_trigger_price(cls, v, values):
        """Validate trigger_price is provided for SL orders."""
        order_type = values.get("order_type")
        if order_type in ["SL", "SL-M"] and v is None:
            raise ValueError(f"trigger_price is required for {order_type} orders")
        return v

    @validator("price")
    def validate_price(cls, v, values):
        """Validate price is provided for LIMIT/SL orders."""
        order_type = values.get("order_type")
        if order_type in ["LIMIT", "SL"] and v is None:
            raise ValueError(f"price is required for {order_type} orders")
        return v


class ModifyOrderRequest(BaseModel):
    """Request schema for modifying an order"""
    quantity: Optional[int] = Field(None, gt=0, description="New quantity")
    price: Optional[float] = Field(None, description="New price")
    trigger_price: Optional[float] = Field(None, description="New trigger price")
    order_type: Optional[str] = Field(None, description="New order type")

    @validator("order_type")
    def validate_order_type(cls, v):
        if v is not None and v not in ["MARKET", "LIMIT", "SL", "SL-M"]:
            raise ValueError("order_type must be MARKET, LIMIT, SL, or SL-M")
        return v


class OrderResponse(BaseModel):
    """Response schema for order data"""
    id: int
    strategy_id: int
    user_id: int
    trading_account_id: str
    broker_order_id: Optional[str]
    symbol: str
    exchange: str
    transaction_type: str
    order_type: str
    product_type: str
    variety: str
    quantity: int
    filled_quantity: int
    pending_quantity: int
    cancelled_quantity: int
    price: Optional[float]
    trigger_price: Optional[float]
    average_price: Optional[float]
    status: str
    status_message: Optional[str]
    validity: str
    created_at: datetime
    updated_at: datetime
    submitted_at: Optional[datetime]
    risk_check_passed: bool
    position_id: Optional[int] = None

    class Config:
        from_attributes = True


class OrderListResponse(BaseModel):
    """Response schema for list of orders"""
    orders: List[OrderResponse]
    total: int
    limit: int
    offset: int


# ==========================================
# BATCH ORDER SCHEMAS
# ==========================================

class BatchOrderRequest(BaseModel):
    """Request schema for batch order placement"""
    orders: List[PlaceOrderRequest] = Field(
        ...,
        min_items=1,
        max_items=20,
        description="List of orders to place (1-20 orders)"
    )
    atomic: bool = Field(
        True,
        description="If true, all orders succeed or all fail (rollback on any failure)"
    )
    tag_prefix: Optional[str] = Field(
        None,
        description="Prefix to add to all order tags for tracking",
        max_length=20
    )

    @validator("orders")
    def validate_batch_size(cls, v):
        if len(v) < 1:
            raise ValueError("Batch must contain at least 1 order")
        if len(v) > 20:
            raise ValueError("Batch cannot exceed 20 orders")
        return v


class BatchOrderResult(BaseModel):
    """Result for individual order in batch"""
    index: int = Field(..., description="Order index in the batch (0-based)")
    success: bool = Field(..., description="Whether order was placed successfully")
    order: Optional[OrderResponse] = Field(None, description="Order details if successful")
    error: Optional[str] = Field(None, description="Error message if failed")
    broker_order_id: Optional[str] = Field(None, description="Broker order ID if successful")


class BatchOrderResponse(BaseModel):
    """Response schema for batch order placement"""
    batch_id: str = Field(..., description="Unique batch ID for tracking")
    total_orders: int = Field(..., description="Total orders in batch")
    successful_orders: int = Field(..., description="Number of successfully placed orders")
    failed_orders: int = Field(..., description="Number of failed orders")
    atomic: bool = Field(..., description="Whether batch was atomic")
    results: List[BatchOrderResult] = Field(..., description="Per-order results")
    rollback_performed: bool = Field(
        False,
        description="Whether orders were rolled back due to failure in atomic mode"
    )
    execution_time_ms: float = Field(..., description="Total execution time in milliseconds")


# ==========================================
# POSITION SCHEMAS
# ==========================================

class PositionResponse(BaseModel):
    """Response schema for position data"""
    id: int
    strategy_id: int
    user_id: int
    trading_account_id: str
    symbol: str
    exchange: str
    product_type: str
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
    instrument_token: Optional[int] = None  # For real-time tick matching

    class Config:
        from_attributes = True


class PositionListResponse(BaseModel):
    """Response schema for list of positions"""
    positions: List[PositionResponse]
    total_pnl: float


# ==========================================
# TRADE SCHEMAS
# ==========================================

class TradeResponse(BaseModel):
    """Response schema for trade data"""
    id: int
    strategy_id: int
    order_id: int
    broker_order_id: str
    broker_trade_id: str
    user_id: int
    trading_account_id: str
    symbol: str
    exchange: str
    transaction_type: str
    product_type: str
    quantity: int
    price: float
    trade_value: float
    trade_time: datetime

    class Config:
        from_attributes = True


class TradeListResponse(BaseModel):
    """Response schema for list of trades"""
    trades: List[TradeResponse]
    total: int


# ==========================================
# ERROR SCHEMAS
# ==========================================

class ErrorResponse(BaseModel):
    """Standard error response"""
    error: dict = Field(
        ...,
        description="Error details",
        example={
            "type": "ValidationError",
            "message": "Invalid order parameters",
            "request_id": "uuid-here"
        }
    )

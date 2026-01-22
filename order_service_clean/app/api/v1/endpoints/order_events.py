"""
Order Events API Endpoints

SEBI-compliant order lifecycle audit trail API with comprehensive
event tracking, compliance reporting, and real-time event publishing.

Key Features:
- Complete order lifecycle event management
- SEBI compliance audit trail generation
- Real-time event statistics and analytics
- Regulatory compliance reporting
- Event processing and status management
"""
import logging
from typing import List, Optional, Dict, Any
from fastapi import APIRouter, Depends, Query, HTTPException, Path
from pydantic import BaseModel, Field, validator
from datetime import datetime
from sqlalchemy.ext.asyncio import AsyncSession

from ....auth.gateway_auth import get_current_user
from ....database.connection import get_db
from ....services.order_event_service import OrderEventService
from ....models.order_event import OrderEvent
from ....utils.user_id import extract_user_id

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/order-events", tags=["Order Events & Audit"])


# ==========================================
# REQUEST/RESPONSE MODELS
# ==========================================

class OrderEventCreateRequest(BaseModel):
    """Request to create order event"""
    order_id: int = Field(..., description="Order ID")
    event_type: str = Field(..., description="Event type (ORDER_CREATED, ORDER_PLACED, etc.)")
    event_data: Optional[Dict[str, Any]] = Field(None, description="Event-specific data")
    additional_context: Optional[Dict[str, Any]] = Field(None, description="Additional event context")

    @validator('event_type')
    def validate_event_type(cls, v):
        valid_types = OrderEvent.get_event_types()
        if v not in valid_types:
            raise ValueError(f'Invalid event type. Must be one of: {valid_types}')
        return v


class OrderCreatedEventRequest(BaseModel):
    """Request for ORDER_CREATED event"""
    order_id: int = Field(..., description="Order ID")
    order_data: Dict[str, Any] = Field(..., description="Order details")
    created_by: Optional[str] = Field(None, description="User/system creating event")
    additional_context: Optional[Dict[str, Any]] = Field(None, description="Additional context")


class OrderPlacedEventRequest(BaseModel):
    """Request for ORDER_PLACED event"""
    order_id: int = Field(..., description="Order ID")
    broker_order_id: str = Field(..., description="Broker's order ID")
    placement_details: Dict[str, Any] = Field(..., description="Broker placement details")
    additional_context: Optional[Dict[str, Any]] = Field(None, description="Additional context")


class OrderFilledEventRequest(BaseModel):
    """Request for ORDER_FILLED event"""
    order_id: int = Field(..., description="Order ID")
    fill_details: Dict[str, Any] = Field(..., description="Execution details")
    trade_data: Optional[Dict[str, Any]] = Field(None, description="Associated trade information")
    additional_context: Optional[Dict[str, Any]] = Field(None, description="Additional context")


class OrderCancelledEventRequest(BaseModel):
    """Request for ORDER_CANCELLED event"""
    order_id: int = Field(..., description="Order ID")
    cancellation_reason: str = Field(..., description="Reason for cancellation")
    cancelled_by: Optional[str] = Field(None, description="Who cancelled the order")
    cancellation_details: Optional[Dict[str, Any]] = Field(None, description="Cancellation details")


class OrderRejectedEventRequest(BaseModel):
    """Request for ORDER_REJECTED event"""
    order_id: int = Field(..., description="Order ID")
    rejection_reason: str = Field(..., description="Rejection reason")
    rejection_details: Dict[str, Any] = Field(..., description="Detailed rejection information")
    additional_context: Optional[Dict[str, Any]] = Field(None, description="Additional context")


class OrderModifiedEventRequest(BaseModel):
    """Request for ORDER_MODIFIED event"""
    order_id: int = Field(..., description="Order ID")
    modifications: Dict[str, Any] = Field(..., description="Changed parameters")
    modified_by: Optional[str] = Field(None, description="Who modified the order")
    modification_reason: Optional[str] = Field(None, description="Reason for modification")


class OrderEventResponse(BaseModel):
    """Order event response model"""
    id: int
    order_id: Optional[int]
    event_type: str
    event_data: Optional[Dict[str, Any]]
    status: str
    created_at: Optional[datetime]
    processed_at: Optional[datetime]
    is_pending: bool
    is_processed: bool
    is_failed: bool

    class Config:
        from_attributes = True


class EventHistoryResponse(BaseModel):
    """Event history response with pagination"""
    events: List[OrderEventResponse]
    total_count: int
    pagination: Dict[str, int]


class EventStatisticsResponse(BaseModel):
    """Event statistics response"""
    period: Dict[str, str]
    event_type_breakdown: Dict[str, int]
    status_breakdown: Dict[str, int]
    daily_breakdown: Dict[str, int]
    total_events: int


class AuditTrailResponse(BaseModel):
    """Audit trail response for compliance"""
    order_id: int
    order_details: Dict[str, Any]
    total_events: int
    audit_generated_at: str
    compliance_period: str
    events: List[Dict[str, Any]]


class ComplianceReportResponse(BaseModel):
    """Compliance report response"""
    report_period: Dict[str, str]
    compliance_standard: str
    retention_period: str
    total_events: int
    unique_orders: int
    event_type_summary: Dict[str, int]
    report_generated_at: str


# ==========================================
# ORDER EVENT CREATION ENDPOINTS
# ==========================================

@router.post("/order-created", response_model=OrderEventResponse)
async def create_order_created_event(
    request: OrderCreatedEventRequest,
    db: AsyncSession = Depends(get_db),
    current_user: dict = Depends(get_current_user)
):
    """
    Create ORDER_CREATED event when new order is placed.
    
    Records the initial order creation with complete order details
    for audit trail compliance.
    """
    user_id = extract_user_id(current_user)
    service = OrderEventService(db, user_id)
    
    try:
        event = await service.create_order_created_event(
            order_id=request.order_id,
            order_data=request.order_data,
            created_by=request.created_by,
            additional_context=request.additional_context
        )
        
        logger.info(f"Created ORDER_CREATED event for order {request.order_id}")
        return OrderEventResponse.from_orm(event)
        
    except Exception as e:
        logger.error(f"Failed to create ORDER_CREATED event: {e}")
        raise HTTPException(500, "Internal server error")


@router.post("/order-placed", response_model=OrderEventResponse)
async def create_order_placed_event(
    request: OrderPlacedEventRequest,
    db: AsyncSession = Depends(get_db),
    current_user: dict = Depends(get_current_user)
):
    """
    Create ORDER_PLACED event when order is submitted to broker.
    
    Records broker submission details including broker order ID
    and placement confirmation.
    """
    user_id = extract_user_id(current_user)
    service = OrderEventService(db, user_id)
    
    try:
        event = await service.create_order_placed_event(
            order_id=request.order_id,
            broker_order_id=request.broker_order_id,
            placement_details=request.placement_details,
            additional_context=request.additional_context
        )
        
        logger.info(f"Created ORDER_PLACED event for order {request.order_id}")
        return OrderEventResponse.from_orm(event)
        
    except Exception as e:
        logger.error(f"Failed to create ORDER_PLACED event: {e}")
        raise HTTPException(500, "Internal server error")


@router.post("/order-filled", response_model=OrderEventResponse)
async def create_order_filled_event(
    request: OrderFilledEventRequest,
    db: AsyncSession = Depends(get_db),
    current_user: dict = Depends(get_current_user)
):
    """
    Create ORDER_FILLED event when order is executed.
    
    Records execution details including filled quantity,
    execution price, and associated trade information.
    """
    user_id = extract_user_id(current_user)
    service = OrderEventService(db, user_id)
    
    try:
        event = await service.create_order_filled_event(
            order_id=request.order_id,
            fill_details=request.fill_details,
            trade_data=request.trade_data,
            additional_context=request.additional_context
        )
        
        logger.info(f"Created ORDER_FILLED event for order {request.order_id}")
        return OrderEventResponse.from_orm(event)
        
    except Exception as e:
        logger.error(f"Failed to create ORDER_FILLED event: {e}")
        raise HTTPException(500, "Internal server error")


@router.post("/order-cancelled", response_model=OrderEventResponse)
async def create_order_cancelled_event(
    request: OrderCancelledEventRequest,
    db: AsyncSession = Depends(get_db),
    current_user: dict = Depends(get_current_user)
):
    """
    Create ORDER_CANCELLED event when order is cancelled.
    
    Records cancellation reason, who cancelled, and
    any additional cancellation details.
    """
    user_id = extract_user_id(current_user)
    service = OrderEventService(db, user_id)
    
    try:
        event = await service.create_order_cancelled_event(
            order_id=request.order_id,
            cancellation_reason=request.cancellation_reason,
            cancelled_by=request.cancelled_by,
            cancellation_details=request.cancellation_details
        )
        
        logger.info(f"Created ORDER_CANCELLED event for order {request.order_id}")
        return OrderEventResponse.from_orm(event)
        
    except Exception as e:
        logger.error(f"Failed to create ORDER_CANCELLED event: {e}")
        raise HTTPException(500, "Internal server error")


@router.post("/order-rejected", response_model=OrderEventResponse)
async def create_order_rejected_event(
    request: OrderRejectedEventRequest,
    db: AsyncSession = Depends(get_db),
    current_user: dict = Depends(get_current_user)
):
    """
    Create ORDER_REJECTED event when order is rejected.
    
    Records rejection reason and detailed rejection information
    for audit and analysis purposes.
    """
    user_id = extract_user_id(current_user)
    service = OrderEventService(db, user_id)
    
    try:
        event = await service.create_order_rejected_event(
            order_id=request.order_id,
            rejection_reason=request.rejection_reason,
            rejection_details=request.rejection_details,
            additional_context=request.additional_context
        )
        
        logger.info(f"Created ORDER_REJECTED event for order {request.order_id}")
        return OrderEventResponse.from_orm(event)
        
    except Exception as e:
        logger.error(f"Failed to create ORDER_REJECTED event: {e}")
        raise HTTPException(500, "Internal server error")


@router.post("/order-modified", response_model=OrderEventResponse)
async def create_order_modified_event(
    request: OrderModifiedEventRequest,
    db: AsyncSession = Depends(get_db),
    current_user: dict = Depends(get_current_user)
):
    """
    Create ORDER_MODIFIED event when order parameters are changed.
    
    Records parameter changes, who modified the order,
    and the reason for modification.
    """
    user_id = extract_user_id(current_user)
    service = OrderEventService(db, user_id)
    
    try:
        event = await service.create_order_modified_event(
            order_id=request.order_id,
            modifications=request.modifications,
            modified_by=request.modified_by,
            modification_reason=request.modification_reason
        )
        
        logger.info(f"Created ORDER_MODIFIED event for order {request.order_id}")
        return OrderEventResponse.from_orm(event)
        
    except Exception as e:
        logger.error(f"Failed to create ORDER_MODIFIED event: {e}")
        raise HTTPException(500, "Internal server error")


# ==========================================
# EVENT QUERY ENDPOINTS
# ==========================================

@router.get("/order/{order_id}", response_model=List[OrderEventResponse])
async def get_order_events(
    order_id: int = Path(..., description="Order ID"),
    event_types: Optional[List[str]] = Query(None, description="Filter by event types"),
    status_filter: Optional[List[str]] = Query(None, description="Filter by processing status"),
    limit: int = Query(100, ge=1, le=500, description="Maximum results"),
    db: AsyncSession = Depends(get_db),
    current_user: dict = Depends(get_current_user)
):
    """
    Get all events for a specific order.
    
    Returns complete event history for an order with optional filtering.
    """
    user_id = extract_user_id(current_user)
    service = OrderEventService(db, user_id)
    
    try:
        events = await service.get_order_events(
            order_id=order_id,
            event_types=event_types,
            status_filter=status_filter,
            limit=limit
        )
        
        return [OrderEventResponse.from_orm(event) for event in events]
        
    except Exception as e:
        logger.error(f"Failed to get order events: {e}")
        raise HTTPException(500, "Internal server error")


@router.get("/user-events", response_model=EventHistoryResponse)
async def get_user_order_events(
    start_date: Optional[datetime] = Query(None, description="Filter from date"),
    end_date: Optional[datetime] = Query(None, description="Filter to date"),
    event_types: Optional[List[str]] = Query(None, description="Filter by event types"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum results"),
    offset: int = Query(0, ge=0, description="Offset for pagination"),
    db: AsyncSession = Depends(get_db),
    current_user: dict = Depends(get_current_user)
):
    """
    Get order events for the current user.
    
    Returns paginated list of user's order events with filtering options.
    """
    user_id = extract_user_id(current_user)
    service = OrderEventService(db, user_id)
    
    try:
        events, total_count = await service.get_user_order_events(
            start_date=start_date,
            end_date=end_date,
            event_types=event_types,
            limit=limit,
            offset=offset
        )
        
        event_responses = [OrderEventResponse.from_orm(event) for event in events]
        
        return EventHistoryResponse(
            events=event_responses,
            total_count=total_count,
            pagination={
                "limit": limit,
                "offset": offset,
                "total": total_count,
                "has_next": offset + limit < total_count
            }
        )
        
    except Exception as e:
        logger.error(f"Failed to get user order events: {e}")
        raise HTTPException(500, "Internal server error")


@router.get("/statistics", response_model=EventStatisticsResponse)
async def get_event_statistics(
    start_date: Optional[datetime] = Query(None, description="Statistics from date"),
    end_date: Optional[datetime] = Query(None, description="Statistics to date"),
    db: AsyncSession = Depends(get_db),
    current_user: dict = Depends(get_current_user)
):
    """
    Get order event statistics for reporting.
    
    Returns comprehensive event statistics including type breakdown,
    daily counts, and status distribution.
    """
    user_id = extract_user_id(current_user)
    service = OrderEventService(db, user_id)
    
    try:
        statistics = await service.get_event_statistics(
            start_date=start_date,
            end_date=end_date
        )
        
        return EventStatisticsResponse(**statistics)
        
    except Exception as e:
        logger.error(f"Failed to get event statistics: {e}")
        raise HTTPException(500, "Internal server error")


# ==========================================
# COMPLIANCE & AUDIT ENDPOINTS
# ==========================================

@router.get("/audit-trail/{order_id}", response_model=AuditTrailResponse)
async def generate_audit_trail(
    order_id: int = Path(..., description="Order ID"),
    include_metadata: bool = Query(True, description="Include detailed metadata"),
    db: AsyncSession = Depends(get_db),
    current_user: dict = Depends(get_current_user)
):
    """
    Generate complete audit trail for an order (SEBI compliance).
    
    Returns comprehensive audit trail with all order events
    formatted for regulatory compliance.
    """
    user_id = extract_user_id(current_user)
    service = OrderEventService(db, user_id)
    
    try:
        audit_trail = await service.generate_audit_trail(
            order_id=order_id,
            include_metadata=include_metadata
        )
        
        return AuditTrailResponse(**audit_trail)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to generate audit trail: {e}")
        raise HTTPException(500, "Internal server error")


@router.get("/compliance-report", response_model=ComplianceReportResponse)
async def get_compliance_report(
    start_date: datetime = Query(..., description="Report start date"),
    end_date: datetime = Query(..., description="Report end date"),
    order_ids: Optional[List[int]] = Query(None, description="Specific orders to include"),
    db: AsyncSession = Depends(get_db),
    current_user: dict = Depends(get_current_user)
):
    """
    Generate SEBI compliance report.
    
    Returns comprehensive compliance report with event statistics
    and audit trail information for regulatory purposes.
    """
    user_id = extract_user_id(current_user)
    service = OrderEventService(db, user_id)
    
    try:
        compliance_report = await service.get_compliance_report(
            start_date=start_date,
            end_date=end_date,
            order_ids=order_ids
        )
        
        return ComplianceReportResponse(**compliance_report)
        
    except Exception as e:
        logger.error(f"Failed to generate compliance report: {e}")
        raise HTTPException(500, "Internal server error")


# ==========================================
# EVENT PROCESSING ENDPOINTS
# ==========================================

@router.post("/process-pending")
async def process_pending_events(
    limit: int = Query(100, ge=1, le=500, description="Maximum events to process"),
    db: AsyncSession = Depends(get_db),
    current_user: dict = Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Process pending order events.
    
    Processes events in 'pending' status and marks them as 'processed' or 'failed'.
    Used for batch processing of accumulated events.
    """
    user_id = extract_user_id(current_user)
    service = OrderEventService(db, user_id)
    
    try:
        processed_count = await service.process_pending_events(limit=limit)
        
        return {
            "processed_count": processed_count,
            "limit": limit,
            "timestamp": datetime.utcnow().isoformat(),
            "status": "completed"
        }
        
    except Exception as e:
        logger.error(f"Failed to process pending events: {e}")
        raise HTTPException(500, "Internal server error")


# ==========================================
# UTILITY ENDPOINTS
# ==========================================

@router.get("/event-types")
async def get_valid_event_types() -> Dict[str, List[str]]:
    """
    Get all valid order event types.
    
    Returns list of supported event types for validation and UI purposes.
    """
    return {
        "event_types": OrderEvent.get_event_types(),
        "description": "Valid order event types for audit trail"
    }
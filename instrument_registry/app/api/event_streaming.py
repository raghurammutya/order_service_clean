"""
Event Streaming API Routes

Production-ready endpoints for event streaming infrastructure with config-driven
broker settings, ordering guarantees, retry policies, and dead letter queue support.
"""

import logging
import time
import uuid
from typing import List, Optional, Dict, Any
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, validator, Field
from enum import Enum

from common.auth_middleware import verify_internal_token
from app.services.event_streaming_service import EventStreamingService, StreamEvent, OrderingGuarantee, EventStatus

logger = logging.getLogger(__name__)

# Create router with authentication dependency
router = APIRouter(
    prefix="/api/v1/internal/instrument-registry/events",
    tags=["event-streaming"],
    dependencies=[Depends(verify_internal_token)]
)

# ===================================
# DEPENDENCY INJECTION
# ===================================

def get_event_streaming_service() -> EventStreamingService:
    """Get event streaming service instance"""
    from main import event_streaming_service
    return event_streaming_service

# ===================================
# REQUEST/RESPONSE MODELS
# ===================================

class EventPayload(BaseModel):
    """Event payload model"""
    event_type: str = Field(..., description="Type of event (e.g., instrument_updated, token_changed)")
    data: Dict[str, Any] = Field(..., description="Event data payload")
    partition_key: Optional[str] = Field(None, description="Partition key for ordering guarantees")
    
    @validator('event_type')
    def validate_event_type(cls, v):
        allowed_types = [
            'instrument_updated', 'token_changed', 'broker_status_changed',
            'ingestion_completed', 'validation_failed', 'subscription_created',
            'profile_updated', 'search_performed', 'catalog_updated'
        ]
        if v not in allowed_types:
            raise ValueError(f"Event type must be one of: {allowed_types}")
        return v

class PublishEventRequest(BaseModel):
    """Request model for publishing events"""
    events: List[EventPayload] = Field(..., description="List of events to publish")
    
    @validator('events')
    def validate_events_count(cls, v):
        if len(v) == 0:
            raise ValueError("At least one event is required")
        if len(v) > 100:  # Configurable batch size limit
            raise ValueError("Maximum 100 events per request")
        return v

class PublishEventResponse(BaseModel):
    """Response model for event publishing"""
    success: bool
    published_count: int
    failed_count: int
    event_ids: List[str]
    errors: List[str] = []

class EventStatusResponse(BaseModel):
    """Response model for event status"""
    event_id: str
    event_type: str
    status: str
    retry_count: int
    timestamp: datetime
    partition_key: Optional[str]

class DLQEventResponse(BaseModel):
    """Response model for DLQ events"""
    message_id: str
    event_data: Dict[str, Any]
    dlq_timestamp: datetime
    original_stream: str
    retry_count: int

class StreamingHealthResponse(BaseModel):
    """Response model for streaming health check"""
    service: str
    status: str
    timestamp: datetime
    configuration: Dict[str, Any]
    active_consumers: int
    broker_connected: bool
    error: Optional[str] = None

# ===================================
# API ENDPOINTS
# ===================================

@router.post("/publish", response_model=PublishEventResponse)
async def publish_events(
    request: PublishEventRequest,
    streaming_service: EventStreamingService = Depends(get_event_streaming_service)
):
    """
    Publish events to the event stream with ordering guarantees
    
    - **events**: List of events to publish
    - Returns published event IDs and success/failure counts
    """
    start_time = time.time()
    
    try:
        published_count = 0
        failed_count = 0
        event_ids = []
        errors = []
        
        for event_payload in request.events:
            try:
                # Create stream event
                event_id = str(uuid.uuid4())
                stream_event = StreamEvent(
                    event_id=event_id,
                    event_type=event_payload.event_type,
                    payload=event_payload.data,
                    partition_key=event_payload.partition_key
                )
                
                # Publish event
                success = await streaming_service.publish_event(stream_event)
                
                if success:
                    published_count += 1
                    event_ids.append(event_id)
                else:
                    failed_count += 1
                    errors.append(f"Failed to publish event {event_id}")
                    
            except Exception as e:
                failed_count += 1
                errors.append(f"Error processing event: {str(e)}")
        
        # Record request metrics  
        duration = time.time() - start_time
        logger.info(f"Published {published_count}/{len(request.events)} events in {duration:.3f}s")
        
        return PublishEventResponse(
            success=(failed_count == 0),
            published_count=published_count,
            failed_count=failed_count,
            event_ids=event_ids,
            errors=errors
        )
        
    except Exception as e:
        logger.error(f"Event publishing failed: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Event publishing failed: {str(e)}"
        )

@router.get("/health", response_model=StreamingHealthResponse)
async def get_streaming_health(
    streaming_service: EventStreamingService = Depends(get_event_streaming_service)
):
    """
    Get event streaming service health status
    
    - Returns service configuration and connectivity status
    - Includes broker connection status and active consumers
    """
    try:
        health_data = await streaming_service.health_check()
        
        return StreamingHealthResponse(
            service=health_data["service"],
            status=health_data["status"],
            timestamp=datetime.fromisoformat(health_data["timestamp"]),
            configuration=health_data["configuration"],
            active_consumers=health_data["active_consumers"],
            broker_connected=health_data["broker_connected"],
            error=health_data.get("error")
        )
        
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Health check failed: {str(e)}"
        )

@router.get("/dlq/{event_type}", response_model=List[DLQEventResponse])
async def get_dlq_events(
    event_type: str,
    limit: int = Query(100, le=1000, description="Maximum number of events to retrieve"),
    streaming_service: EventStreamingService = Depends(get_event_streaming_service)
):
    """
    Retrieve events from Dead Letter Queue for manual processing
    
    - **event_type**: Type of events to retrieve from DLQ
    - **limit**: Maximum number of events to return (default: 100, max: 1000)
    """
    try:
        dlq_events = await streaming_service.get_dlq_events(event_type, limit)
        
        response_events = []
        for event in dlq_events:
            response_events.append(DLQEventResponse(
                message_id=event["message_id"],
                event_data=event["data"],
                dlq_timestamp=datetime.fromisoformat(event["data"].get("dlq_timestamp")),
                original_stream=event["data"].get("original_stream", "unknown"),
                retry_count=int(event["data"].get("retry_count", 0))
            ))
        
        logger.info(f"Retrieved {len(response_events)} DLQ events for type {event_type}")
        return response_events
        
    except Exception as e:
        logger.error(f"Failed to retrieve DLQ events: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to retrieve DLQ events: {str(e)}"
        )

@router.post("/dlq/{event_type}/reprocess/{message_id}")
async def reprocess_dlq_event(
    event_type: str,
    message_id: str,
    streaming_service: EventStreamingService = Depends(get_event_streaming_service)
):
    """
    Reprocess event from Dead Letter Queue back to main stream
    
    - **event_type**: Type of event to reprocess
    - **message_id**: Message ID of the event in DLQ
    """
    try:
        success = await streaming_service.reprocess_dlq_event(event_type, message_id)
        
        if success:
            logger.info(f"Successfully reprocessed DLQ event {message_id}")
            return {"success": True, "message": f"Event {message_id} reprocessed successfully"}
        else:
            raise HTTPException(
                status_code=404,
                detail=f"Event {message_id} not found in DLQ or failed to reprocess"
            )
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to reprocess DLQ event: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to reprocess DLQ event: {str(e)}"
        )

@router.get("/config")
async def get_streaming_configuration(
    streaming_service: EventStreamingService = Depends(get_event_streaming_service)
):
    """
    Get current event streaming configuration
    
    - Returns configuration loaded from config service
    - Includes ordering guarantees, retry policies, and DLQ settings
    """
    try:
        return {
            "broker_url": streaming_service.broker_url[:20] + "..." if streaming_service.broker_url else None,
            "retry_attempts": streaming_service.retry_attempts,
            "batch_size": streaming_service.batch_size,
            "ordering_guarantee": streaming_service.ordering_guarantee.value,
            "dlq_retention_hours": streaming_service.dlq_retention_hours,
            "service_running": streaming_service._running
        }
        
    except Exception as e:
        logger.error(f"Failed to get configuration: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get configuration: {str(e)}"
        )
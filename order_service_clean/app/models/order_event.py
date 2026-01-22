"""
Order Event Model - Complete audit trail for order lifecycle

Tracks all events in the order lifecycle for compliance and monitoring:
- ORDER_CREATED, ORDER_PLACED, ORDER_MODIFIED, ORDER_CANCELLED
- ORDER_FILLED, ORDER_REJECTED, ORDER_EXPIRED
- Supports SEBI compliance requirements (7-year retention)
"""

from sqlalchemy import (
    Column, BigInteger, String, DateTime, ForeignKey, func
)
from sqlalchemy.dialects.postgresql import JSONB
from datetime import datetime
from typing import Optional, Dict, Any

from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class OrderEvent(Base):
    """
    Order Event audit trail for compliance and monitoring
    
    Event Types:
    - ORDER_CREATED: Order created in system
    - ORDER_PLACED: Order submitted to broker
    - ORDER_MODIFIED: Order parameters changed
    - ORDER_CANCELLED: Order cancelled by user/system
    - ORDER_FILLED: Order execution completed
    - ORDER_REJECTED: Order rejected by broker
    - ORDER_EXPIRED: Order expired (GTT, validity)
    
    Status:
    - pending: Event created but not processed
    - processed: Event processing completed
    - failed: Event processing failed
    
    Compliance:
    - 7-year retention for SEBI compliance
    - Complete audit trail for regulatory reporting
    """
    __tablename__ = "order_events"
    __table_args__ = {"schema": "order_service"}

    # Primary key
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    
    # Foreign key to orders table
    order_id = Column(
        BigInteger, 
        nullable=True,
        comment="Reference to order (null if order deleted)"
    )
    
    # Event details
    event_type = Column(
        String(50), 
        nullable=False,
        comment="Type of order event (ORDER_CREATED, ORDER_PLACED, etc.)"
    )
    
    # Event payload and metadata
    event_data = Column(
        JSONB, 
        nullable=True,
        comment="Event payload and context data"
    )
    
    # Processing status
    status = Column(
        String(50), 
        nullable=True, 
        default="pending",
        comment="Event processing status (pending/processed/failed)"
    )
    
    # Timestamps
    created_at = Column(
        DateTime(timezone=True), 
        nullable=True, 
        default=func.current_timestamp(),
        comment="Event creation timestamp"
    )
    
    processed_at = Column(
        DateTime(timezone=True), 
        nullable=True,
        comment="Event processing completion timestamp"
    )

    def __repr__(self):
        return (
            f"<OrderEvent("
            f"id={self.id}, "
            f"order_id={self.order_id}, "
            f"event_type='{self.event_type}', "
            f"status='{self.status}', "
            f"created_at='{self.created_at}'"
            f")>"
        )

    @property
    def is_pending(self) -> bool:
        """Check if event is pending processing"""
        return self.status == "pending"
    
    @property
    def is_processed(self) -> bool:
        """Check if event has been processed"""
        return self.status == "processed"
    
    @property
    def is_failed(self) -> bool:
        """Check if event processing failed"""
        return self.status == "failed"
    
    def mark_processed(self, processed_at: Optional[datetime] = None) -> None:
        """Mark event as processed"""
        self.status = "processed"
        self.processed_at = processed_at or datetime.utcnow()
    
    def mark_failed(self) -> None:
        """Mark event processing as failed"""
        self.status = "failed"
        self.processed_at = datetime.utcnow()
    
    @classmethod
    def create_order_event(
        cls, 
        order_id: int, 
        event_type: str, 
        event_data: Optional[Dict[str, Any]] = None
    ) -> 'OrderEvent':
        """
        Factory method to create order events
        
        Args:
            order_id: Order ID
            event_type: Event type (ORDER_CREATED, ORDER_PLACED, etc.)
            event_data: Additional event data
            
        Returns:
            OrderEvent instance
        """
        return cls(
            order_id=order_id,
            event_type=event_type,
            event_data=event_data or {},
            status="pending",
            created_at=datetime.utcnow()
        )

    @staticmethod
    def get_event_types():
        """Get all valid order event types"""
        return [
            "ORDER_CREATED",
            "ORDER_PLACED", 
            "ORDER_MODIFIED",
            "ORDER_CANCELLED",
            "ORDER_FILLED",
            "ORDER_REJECTED", 
            "ORDER_EXPIRED",
            "ORDER_RECONCILED"
        ]
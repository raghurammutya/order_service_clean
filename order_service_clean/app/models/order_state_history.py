"""
Order State History Model - Audit Trail

Tracks all state transitions for regulatory compliance and debugging.
Implements SEBI audit trail requirements.
"""
from datetime import datetime
from typing import Optional, Dict, Any
from sqlalchemy import Column, Integer, String, Text, DateTime, Index
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class OrderStateHistory(Base):
    """
    Audit trail for order state transitions.

    Captures every state change with:
    - What changed (old_status → new_status)
    - Who changed it (user or system)
    - Why it changed (reason)
    - When it changed (changed_at)

    Required for:
    - SEBI compliance (audit trail for all order actions)
    - Debugging (track why order entered certain states)
    - Security (detect unauthorized modifications)
    """

    __tablename__ = "order_state_history"
    __table_args__ = (
        Index("idx_order_state_history_order_id", "order_id"),
        Index("idx_order_state_history_changed_at", "changed_at"),
        Index("idx_order_state_history_new_status", "new_status"),
        Index("idx_order_state_history_changed_by_user", "changed_by_user_id"),
    )

    # Primary key
    id = Column(Integer, primary_key=True, autoincrement=True)

    # Reference to order (FK removed due to separate declarative_base())
    order_id = Column(
        Integer,
        nullable=False,
        comment="Reference to orders table (FK constraint handled at DB level)"
    )

    # State transition
    old_status = Column(
        String(20),
        nullable=True,  # NULL for order creation (first state)
        comment="Previous status (NULL for order creation)"
    )
    new_status = Column(
        String(20),
        nullable=False,
        comment="New status after transition"
    )

    # Actor (who/what made the change)
    changed_by_user_id = Column(
        Integer,
        nullable=True,
        comment="User ID if manual action (cancel, modify)"
    )
    changed_by_system = Column(
        String(50),
        nullable=True,
        comment="System component: order_service, broker_webhook, reconciliation"
    )

    # Context
    reason = Column(
        Text,
        nullable=True,
        comment="Human-readable reason for state change"
    )
    broker_response = Column(
        Text,
        nullable=True,
        comment="Broker response/error message if applicable"
    )

    # Additional data (flexible JSONB field)
    # Note: Using event_metadata as attribute name because 'metadata' is reserved by SQLAlchemy
    event_metadata = Column(
        "metadata",  # Column name in database
        JSONB,
        nullable=True,
        comment="Additional context in JSON format"
    )

    # Timing
    changed_at = Column(
        DateTime,
        nullable=False,
        default=datetime.utcnow,
        comment="When the state change occurred"
    )
    created_at = Column(
        DateTime,
        nullable=False,
        default=datetime.utcnow,
        comment="When this history record was created"
    )

    def __repr__(self):
        return (
            f"<OrderStateHistory("
            f"order_id={self.order_id}, "
            f"{self.old_status} → {self.new_status}, "
            f"by={self.changed_by_user_id or self.changed_by_system}, "
            f"at={self.changed_at}"
            f")>"
        )

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert to dictionary for API responses.

        Returns:
            Dictionary with all fields in API-friendly format
        """
        return {
            "id": self.id,
            "order_id": self.order_id,
            "transition": {
                "from": self.old_status,
                "to": self.new_status,
            },
            "actor": {
                "user_id": self.changed_by_user_id,
                "system": self.changed_by_system,
                "type": "user" if self.changed_by_user_id else "system"
            },
            "context": {
                "reason": self.reason,
                "broker_response": self.broker_response,
                "metadata": self.event_metadata
            },
            "timestamp": self.changed_at.isoformat() if self.changed_at else None,
            "created_at": self.created_at.isoformat() if self.created_at else None
        }

    @staticmethod
    def create_for_order_creation(
        order_id: int,
        user_id: int,
        initial_status: str = "PENDING",
        metadata: Optional[Dict[str, Any]] = None
    ) -> "OrderStateHistory":
        """
        Factory method for order creation event.

        Args:
            order_id: Order ID
            user_id: User who created the order
            initial_status: Initial status (default PENDING)
            metadata: Additional context

        Returns:
            OrderStateHistory instance for order creation
        """
        return OrderStateHistory(
            order_id=order_id,
            old_status=None,  # No previous state
            new_status=initial_status,
            changed_by_user_id=user_id,
            changed_by_system="order_service",
            reason="Order created by user",
            metadata=metadata
        )

    @staticmethod
    def create_for_broker_submission(
        order_id: int,
        user_id: int,
        broker_order_id: str,
        metadata: Optional[Dict[str, Any]] = None
    ) -> "OrderStateHistory":
        """
        Factory method for broker submission event.

        Args:
            order_id: Order ID
            user_id: User who placed the order
            broker_order_id: Broker's order ID
            metadata: Additional context

        Returns:
            OrderStateHistory instance for broker submission
        """
        return OrderStateHistory(
            order_id=order_id,
            old_status="PENDING",
            new_status="SUBMITTED",
            changed_by_user_id=user_id,
            changed_by_system="order_service",
            reason="Order submitted to broker",
            metadata={
                **(metadata or {}),
                "broker_order_id": broker_order_id
            }
        )

    @staticmethod
    def create_for_reconciliation(
        order_id: int,
        old_status: str,
        new_status: str,
        drift_detected: bool = True,
        metadata: Optional[Dict[str, Any]] = None
    ) -> "OrderStateHistory":
        """
        Factory method for reconciliation event.

        Args:
            order_id: Order ID
            old_status: Status in database
            new_status: Status from broker
            drift_detected: Whether drift was detected
            metadata: Additional context

        Returns:
            OrderStateHistory instance for reconciliation
        """
        return OrderStateHistory(
            order_id=order_id,
            old_status=old_status,
            new_status=new_status,
            changed_by_user_id=None,  # System action
            changed_by_system="reconciliation_worker",
            reason=f"Reconciliation: {'Corrected drift from broker' if drift_detected else 'Verified status matches broker'}",
            metadata={
                **(metadata or {}),
                "drift_detected": drift_detected,
                "db_status": old_status,
                "broker_status": new_status
            }
        )

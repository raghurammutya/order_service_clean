"""
Strategy Lifecycle Event Model

Tracks comprehensive strategy lifecycle events for audit, monitoring,
and compliance purposes.
"""
from sqlalchemy import (
    Column, BigInteger, Text, DateTime, Index, func
)
from sqlalchemy.dialects.postgresql import JSONB
from datetime import datetime
from typing import Optional, Dict, Any

from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class StrategyLifecycleEvent(Base):
    """
    Strategy Lifecycle Event Model
    
    Comprehensive audit trail for strategy lifecycle events including
    creation, activation, deactivation, modification, and deletion.
    
    Key Features:
    - Complete strategy event audit trail
    - JSONB event data for flexible event payloads
    - Event type categorization for filtering
    - User attribution for compliance
    - Time-series event ordering
    - Regulatory compliance support
    
    Event Types:
    - STRATEGY_CREATED: Strategy initially created
    - STRATEGY_ACTIVATED: Strategy started/enabled
    - STRATEGY_DEACTIVATED: Strategy stopped/disabled
    - STRATEGY_MODIFIED: Strategy parameters changed
    - STRATEGY_DEPLOYED: Strategy deployed to live trading
    - STRATEGY_SUSPENDED: Strategy temporarily suspended
    - STRATEGY_DELETED: Strategy permanently removed
    - RISK_LIMIT_BREACH: Strategy hit risk limits
    - PERFORMANCE_MILESTONE: Strategy performance events
    """
    __tablename__ = "strategy_lifecycle_events"
    __table_args__ = (
        # Performance indexes
        Index("idx_strategy_lifecycle_strategy_time", "strategy_id", "occurred_at"),
        Index("idx_strategy_lifecycle_type", "event_type"),
        Index("idx_strategy_lifecycle_created_by", "created_by"),
        Index("idx_strategy_lifecycle_occurred_at", "occurred_at"),
        {"schema": "order_service"},
    )

    # Primary key
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    
    # Strategy reference
    strategy_id = Column(
        BigInteger,
        nullable=False,
        comment="Strategy ID this event relates to"
    )
    
    # Event details
    event_type = Column(
        Text,
        nullable=False,
        comment="Type of lifecycle event (CREATED, ACTIVATED, DEACTIVATED, etc.)"
    )
    
    # Event payload
    event_data = Column(
        JSONB,
        nullable=True,
        comment="Event-specific data and context (JSON)"
    )
    
    # Timestamps
    occurred_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=func.now(),
        comment="When the event actually occurred"
    )
    
    # Audit fields
    created_by = Column(
        Text,
        nullable=True,
        comment="User or system that created this event record"
    )
    created_at = Column(
        DateTime(timezone=True),
        nullable=True,
        default=func.now(),
        comment="When this event record was created"
    )

    def __repr__(self):
        return (
            f"<StrategyLifecycleEvent("
            f"id={self.id}, "
            f"strategy={self.strategy_id}, "
            f"type='{self.event_type}', "
            f"occurred_at={self.occurred_at}, "
            f"created_by='{self.created_by}'"
            f")>"
        )

    @property
    def is_creation_event(self) -> bool:
        """Check if this is a strategy creation event"""
        return self.event_type == "STRATEGY_CREATED"

    @property
    def is_activation_event(self) -> bool:
        """Check if this is a strategy activation event"""
        return self.event_type == "STRATEGY_ACTIVATED"

    @property
    def is_deactivation_event(self) -> bool:
        """Check if this is a strategy deactivation event"""
        return self.event_type == "STRATEGY_DEACTIVATED"

    @property
    def is_modification_event(self) -> bool:
        """Check if this is a strategy modification event"""
        return self.event_type == "STRATEGY_MODIFIED"

    @property
    def is_risk_event(self) -> bool:
        """Check if this is a risk-related event"""
        return "RISK" in self.event_type or "LIMIT" in self.event_type

    @property
    def is_performance_event(self) -> bool:
        """Check if this is a performance-related event"""
        return "PERFORMANCE" in self.event_type or "MILESTONE" in self.event_type

    def get_event_summary(self) -> str:
        """Get a human-readable summary of the event"""
        event_summaries = {
            "STRATEGY_CREATED": "Strategy was created",
            "STRATEGY_ACTIVATED": "Strategy was activated",
            "STRATEGY_DEACTIVATED": "Strategy was deactivated",
            "STRATEGY_MODIFIED": "Strategy parameters were modified",
            "STRATEGY_DEPLOYED": "Strategy was deployed to live trading",
            "STRATEGY_SUSPENDED": "Strategy was suspended",
            "STRATEGY_DELETED": "Strategy was deleted",
            "RISK_LIMIT_BREACH": "Strategy breached risk limits",
            "PERFORMANCE_MILESTONE": "Strategy reached performance milestone",
        }
        return event_summaries.get(self.event_type, f"Unknown event: {self.event_type}")

    def get_event_data_field(self, field_name: str, default: Any = None) -> Any:
        """Get a specific field from event_data JSON"""
        if self.event_data and isinstance(self.event_data, dict):
            return self.event_data.get(field_name, default)
        return default

    def to_dict(self):
        """Convert event to dictionary"""
        return {
            "id": self.id,
            "strategy_id": self.strategy_id,
            "event_type": self.event_type,
            "event_data": self.event_data,
            "occurred_at": self.occurred_at.isoformat() if self.occurred_at else None,
            "created_by": self.created_by,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            # Derived properties
            "is_creation_event": self.is_creation_event,
            "is_activation_event": self.is_activation_event,
            "is_deactivation_event": self.is_deactivation_event,
            "is_modification_event": self.is_modification_event,
            "is_risk_event": self.is_risk_event,
            "is_performance_event": self.is_performance_event,
            "event_summary": self.get_event_summary(),
        }

    @classmethod
    def create_event(
        cls,
        strategy_id: int,
        event_type: str,
        event_data: Optional[Dict[str, Any]] = None,
        created_by: Optional[str] = None,
        occurred_at: Optional[datetime] = None
    ) -> 'StrategyLifecycleEvent':
        """
        Factory method to create a strategy lifecycle event
        
        Args:
            strategy_id: Strategy ID
            event_type: Type of event (CREATED, ACTIVATED, etc.)
            event_data: Event-specific data dictionary
            created_by: User or system creating the event
            occurred_at: When the event occurred (defaults to now)
            
        Returns:
            StrategyLifecycleEvent instance
        """
        return cls(
            strategy_id=strategy_id,
            event_type=event_type,
            event_data=event_data or {},
            created_by=created_by,
            occurred_at=occurred_at or datetime.utcnow(),
            created_at=datetime.utcnow()
        )

    @classmethod
    def create_creation_event(
        cls,
        strategy_id: int,
        strategy_name: str,
        created_by: Optional[str] = None,
        additional_data: Optional[Dict[str, Any]] = None
    ) -> 'StrategyLifecycleEvent':
        """Create a strategy creation event"""
        event_data = {
            "strategy_name": strategy_name,
            "action": "created",
            **(additional_data or {})
        }
        return cls.create_event(strategy_id, "STRATEGY_CREATED", event_data, created_by)

    @classmethod
    def create_activation_event(
        cls,
        strategy_id: int,
        created_by: Optional[str] = None,
        additional_data: Optional[Dict[str, Any]] = None
    ) -> 'StrategyLifecycleEvent':
        """Create a strategy activation event"""
        event_data = {
            "action": "activated",
            "status": "active",
            **(additional_data or {})
        }
        return cls.create_event(strategy_id, "STRATEGY_ACTIVATED", event_data, created_by)

    @classmethod
    def create_deactivation_event(
        cls,
        strategy_id: int,
        reason: Optional[str] = None,
        created_by: Optional[str] = None,
        additional_data: Optional[Dict[str, Any]] = None
    ) -> 'StrategyLifecycleEvent':
        """Create a strategy deactivation event"""
        event_data = {
            "action": "deactivated",
            "status": "inactive",
            "reason": reason,
            **(additional_data or {})
        }
        return cls.create_event(strategy_id, "STRATEGY_DEACTIVATED", event_data, created_by)

    @classmethod
    def create_modification_event(
        cls,
        strategy_id: int,
        modified_fields: Dict[str, Any],
        created_by: Optional[str] = None,
        additional_data: Optional[Dict[str, Any]] = None
    ) -> 'StrategyLifecycleEvent':
        """Create a strategy modification event"""
        event_data = {
            "action": "modified",
            "modified_fields": modified_fields,
            **(additional_data or {})
        }
        return cls.create_event(strategy_id, "STRATEGY_MODIFIED", event_data, created_by)

    @classmethod
    def create_risk_breach_event(
        cls,
        strategy_id: int,
        risk_type: str,
        breach_details: Dict[str, Any],
        created_by: Optional[str] = None
    ) -> 'StrategyLifecycleEvent':
        """Create a risk limit breach event"""
        event_data = {
            "action": "risk_breach",
            "risk_type": risk_type,
            "breach_details": breach_details,
            "severity": "high"
        }
        return cls.create_event(strategy_id, "RISK_LIMIT_BREACH", event_data, created_by)

    @staticmethod
    def get_valid_event_types():
        """Get all valid event types"""
        return [
            "STRATEGY_CREATED",
            "STRATEGY_ACTIVATED", 
            "STRATEGY_DEACTIVATED",
            "STRATEGY_MODIFIED",
            "STRATEGY_DEPLOYED",
            "STRATEGY_SUSPENDED",
            "STRATEGY_DELETED",
            "RISK_LIMIT_BREACH",
            "PERFORMANCE_MILESTONE",
        ]
"""
Strategy Models (Read-Only Reference)

References strategies for P&L tracking and metrics calculation.

ARCHITECTURE:
- Strategies are owned by backend or algo_engine (CRUD operations)
- order_service has read-only access for querying and metrics
- signal_service computes indicators/Greeks/metrics (NOT strategy management)
- Strategies table is in signal_service schema (historical)
"""
from datetime import datetime
from sqlalchemy import Column, BigInteger, String, Text, DateTime, Boolean, ARRAY
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class Strategy(Base):
    """
    Strategy reference model (READ-ONLY).

    order_service queries strategies for P&L tracking but does NOT create/update/delete.
    Use backend or algo_engine APIs for strategy CRUD operations.
    """

    __tablename__ = "strategies"
    # __table_args__ = {'schema': 'signal_service'}  # REMOVED: Cross-service schema causes SQLAlchemy errors

    # Primary Key
    id = Column(BigInteger, primary_key=True)

    # Ownership
    user_id = Column(BigInteger, nullable=True, comment="User who owns this strategy")
    trading_account_id = Column(Text, nullable=True, comment="Trading account linked to strategy")

    # Identification
    name = Column(String(255), nullable=False, comment="Internal strategy name")
    display_name = Column(Text, nullable=True, comment="User-friendly display name")
    description = Column(Text, nullable=True, comment="Strategy description")
    strategy_type = Column(String(50), nullable=True, comment="Type of strategy (e.g., options, equity)")

    # Status
    state = Column(Text, nullable=True, default='created', comment="Strategy lifecycle state")
    mode = Column(Text, nullable=True, default='paper', comment="Trading mode: paper or live")
    is_active = Column(Boolean, nullable=True, default=True, comment="Whether strategy is active")
    is_default = Column(Boolean, nullable=True, default=False, comment="Whether this is the default strategy")

    # Configuration
    parameters = Column(JSONB, nullable=True, default={}, comment="Strategy parameters")
    config = Column(JSONB, nullable=True, comment="Strategy configuration")
    strategy_metadata = Column(JSONB, nullable=True, comment="Additional metadata")
    tags = Column(ARRAY(Text), nullable=True, comment="Strategy tags")

    # Performance
    performance_metrics = Column(JSONB, nullable=True, default={}, comment="Performance metrics")

    # Lifecycle
    created_at = Column(DateTime, nullable=True, default=datetime.utcnow, comment="Creation timestamp")
    updated_at = Column(DateTime, nullable=True, default=datetime.utcnow, comment="Last update timestamp")
    created_by = Column(Text, nullable=True, comment="User who created the strategy")

    # Handoff tracking (for strategy transfer)
    handoff_initiated_at = Column(DateTime, nullable=True)
    handoff_completed_at = Column(DateTime, nullable=True)
    handoff_taken_by = Column(Text, nullable=True)

    # Closure tracking
    closed_at = Column(DateTime, nullable=True)
    closed_by = Column(Text, nullable=True)
    closure_reason = Column(Text, nullable=True)
    closure_type = Column(Text, nullable=True)

    # Strategy relationships (for handoffs)
    source_strategy_id = Column(Text, nullable=True, comment="Source strategy ID for handoffs")
    target_strategy_id = Column(Text, nullable=True, comment="Target strategy ID for handoffs")
    source_trading_account_id = Column(Text, nullable=True)

    def to_dict(self):
        """Convert strategy to dictionary"""
        return {
            "id": self.id,
            "user_id": self.user_id,
            "trading_account_id": self.trading_account_id,
            "name": self.name,
            "display_name": self.display_name,
            "description": self.description,
            "strategy_type": self.strategy_type,
            "state": self.state,
            "mode": self.mode,
            "is_active": self.is_active,
            "is_default": self.is_default,
            "parameters": self.parameters,
            "config": self.config,
            "metadata": self.metadata,
            "tags": self.tags,
            "performance_metrics": self.performance_metrics,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
            "created_by": self.created_by,
            "closed_at": self.closed_at.isoformat() if self.closed_at else None,
        }

"""
Portfolio Snapshot Model

Captures point-in-time portfolio state for historical tracking,
performance analysis, and reconciliation purposes.
"""
from sqlalchemy import (
    Column, Integer, Numeric, DateTime, Index
)
from datetime import datetime
from typing import Optional

from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class PortfolioSnapshot(Base):
    """
    Portfolio Snapshot Model
    
    Captures comprehensive portfolio state at specific points in time.
    Used for historical performance tracking, reconciliation, and
    risk management reporting.
    
    Key Features:
    - Point-in-time portfolio valuation
    - P&L tracking (total and daily)
    - Margin utilization monitoring
    - Position and order count tracking
    - Historical performance analysis support
    """
    __tablename__ = "portfolio_snapshots"
    __table_args__ = (
        # Performance indexes
        Index("idx_portfolio_snapshots_time", "snapshot_time"),
        Index("idx_portfolio_snapshots_portfolio_id", "portfolio_id"),
        Index("idx_portfolio_snapshots_user_id", "user_id"),
        Index("idx_portfolio_snapshots_portfolio_time", "portfolio_id", "snapshot_time"),
        Index("idx_portfolio_snapshots_user_time", "user_id", "snapshot_time"),
        {"schema": "order_service"},
    )

    # Composite primary key (time + portfolio)
    snapshot_time = Column(
        DateTime(timezone=True),
        primary_key=True,
        comment="Timestamp when snapshot was captured"
    )
    portfolio_id = Column(
        Integer,
        primary_key=True,
        comment="Portfolio ID for this snapshot"
    )
    
    # User reference
    user_id = Column(
        Integer,
        nullable=False,
        comment="User ID who owns this portfolio"
    )
    
    # Portfolio valuation
    total_value = Column(
        Numeric(20, 2),
        nullable=True,
        comment="Total portfolio value at snapshot time"
    )
    
    # P&L tracking
    total_pnl = Column(
        Numeric(20, 2),
        nullable=True,
        comment="Total P&L since portfolio inception"
    )
    day_pnl = Column(
        Numeric(20, 2),
        nullable=True,
        comment="Daily P&L for the snapshot day"
    )
    
    # Margin utilization
    total_margin_used = Column(
        Numeric(20, 2),
        nullable=True,
        comment="Total margin utilized at snapshot time"
    )
    total_margin_available = Column(
        Numeric(20, 2),
        nullable=True,
        comment="Total margin available at snapshot time"
    )
    
    # Portfolio composition counts
    position_count = Column(
        Integer,
        nullable=True,
        default=0,
        comment="Number of open positions at snapshot time"
    )
    holding_count = Column(
        Integer,
        nullable=True,
        default=0,
        comment="Number of holdings at snapshot time"
    )
    order_count = Column(
        Integer,
        nullable=True,
        default=0,
        comment="Total number of orders placed today"
    )
    pending_order_count = Column(
        Integer,
        nullable=True,
        default=0,
        comment="Number of pending orders at snapshot time"
    )

    def __repr__(self):
        return (
            f"<PortfolioSnapshot("
            f"time={self.snapshot_time}, "
            f"portfolio={self.portfolio_id}, "
            f"user={self.user_id}, "
            f"value={self.total_value}, "
            f"day_pnl={self.day_pnl}, "
            f"positions={self.position_count}"
            f")>"
        )

    @property
    def margin_utilization_pct(self) -> Optional[float]:
        """Calculate margin utilization percentage"""
        if self.total_margin_available and float(self.total_margin_available) > 0:
            total_margin = float(self.total_margin_used or 0) + float(self.total_margin_available)
            return (float(self.total_margin_used or 0) / total_margin) * 100.0
        return None

    @property
    def day_pnl_pct(self) -> Optional[float]:
        """Calculate daily P&L as percentage of portfolio value"""
        if self.total_value and float(self.total_value) > 0:
            return (float(self.day_pnl or 0) / float(self.total_value)) * 100.0
        return None

    @property
    def total_pnl_pct(self) -> Optional[float]:
        """Calculate total P&L as percentage of current portfolio value"""
        if self.total_value and float(self.total_value) > 0:
            # Calculate original investment by subtracting P&L from current value
            original_investment = float(self.total_value) - float(self.total_pnl or 0)
            if original_investment > 0:
                return (float(self.total_pnl or 0) / original_investment) * 100.0
        return None

    @property
    def is_profitable_day(self) -> bool:
        """Check if the day was profitable"""
        return self.day_pnl is not None and float(self.day_pnl) > 0

    @property
    def is_profitable_total(self) -> bool:
        """Check if the portfolio is profitable overall"""
        return self.total_pnl is not None and float(self.total_pnl) > 0

    @property
    def has_active_positions(self) -> bool:
        """Check if portfolio has active positions"""
        return (self.position_count or 0) > 0

    @property
    def has_pending_orders(self) -> bool:
        """Check if portfolio has pending orders"""
        return (self.pending_order_count or 0) > 0

    def calculate_portfolio_metrics(self) -> dict:
        """Calculate comprehensive portfolio metrics"""
        return {
            "portfolio_value": float(self.total_value) if self.total_value else 0,
            "day_pnl": float(self.day_pnl) if self.day_pnl else 0,
            "day_pnl_pct": self.day_pnl_pct,
            "total_pnl": float(self.total_pnl) if self.total_pnl else 0,
            "total_pnl_pct": self.total_pnl_pct,
            "margin_used": float(self.total_margin_used) if self.total_margin_used else 0,
            "margin_available": float(self.total_margin_available) if self.total_margin_available else 0,
            "margin_utilization_pct": self.margin_utilization_pct,
            "position_count": self.position_count or 0,
            "holding_count": self.holding_count or 0,
            "order_count": self.order_count or 0,
            "pending_order_count": self.pending_order_count or 0,
            "is_profitable_day": self.is_profitable_day,
            "is_profitable_total": self.is_profitable_total,
            "has_active_positions": self.has_active_positions,
            "has_pending_orders": self.has_pending_orders,
        }

    def to_dict(self):
        """Convert snapshot to dictionary"""
        return {
            "snapshot_time": self.snapshot_time.isoformat() if self.snapshot_time else None,
            "portfolio_id": self.portfolio_id,
            "user_id": self.user_id,
            "total_value": float(self.total_value) if self.total_value else None,
            "total_pnl": float(self.total_pnl) if self.total_pnl else None,
            "day_pnl": float(self.day_pnl) if self.day_pnl else None,
            "total_margin_used": float(self.total_margin_used) if self.total_margin_used else None,
            "total_margin_available": float(self.total_margin_available) if self.total_margin_available else None,
            "position_count": self.position_count,
            "holding_count": self.holding_count,
            "order_count": self.order_count,
            "pending_order_count": self.pending_order_count,
            # Calculated metrics
            "margin_utilization_pct": self.margin_utilization_pct,
            "day_pnl_pct": self.day_pnl_pct,
            "total_pnl_pct": self.total_pnl_pct,
            "is_profitable_day": self.is_profitable_day,
            "is_profitable_total": self.is_profitable_total,
            "has_active_positions": self.has_active_positions,
            "has_pending_orders": self.has_pending_orders,
        }

    @classmethod
    def create_snapshot(
        cls,
        portfolio_id: int,
        user_id: int,
        total_value: Optional[float] = None,
        total_pnl: Optional[float] = None,
        day_pnl: Optional[float] = None,
        total_margin_used: Optional[float] = None,
        total_margin_available: Optional[float] = None,
        position_count: Optional[int] = None,
        holding_count: Optional[int] = None,
        order_count: Optional[int] = None,
        pending_order_count: Optional[int] = None,
        snapshot_time: Optional[datetime] = None
    ) -> 'PortfolioSnapshot':
        """
        Factory method to create a portfolio snapshot
        
        Args:
            portfolio_id: Portfolio ID
            user_id: User ID
            total_value: Total portfolio value
            total_pnl: Total P&L since inception
            day_pnl: Daily P&L
            total_margin_used: Margin currently used
            total_margin_available: Margin available
            position_count: Number of open positions
            holding_count: Number of holdings
            order_count: Number of orders today
            pending_order_count: Number of pending orders
            snapshot_time: Snapshot timestamp (defaults to now)
            
        Returns:
            PortfolioSnapshot instance
        """
        return cls(
            snapshot_time=snapshot_time or datetime.utcnow(),
            portfolio_id=portfolio_id,
            user_id=user_id,
            total_value=total_value,
            total_pnl=total_pnl,
            day_pnl=day_pnl,
            total_margin_used=total_margin_used,
            total_margin_available=total_margin_available,
            position_count=position_count or 0,
            holding_count=holding_count or 0,
            order_count=order_count or 0,
            pending_order_count=pending_order_count or 0
        )
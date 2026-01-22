"""
Position Snapshot Model

Captures point-in-time position state for historical tracking,
reconciliation, and position management.
"""
from sqlalchemy import (
    Column, Integer, BigInteger, String, DateTime, Text, Index
)
from datetime import datetime
from typing import Optional

from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class PositionSnapshot(Base):
    """
    Position Snapshot Model
    
    Captures position state at specific points in time for historical
    tracking, reconciliation, and analysis purposes.
    
    Key Features:
    - Point-in-time position quantities
    - Strategy association tracking
    - Exchange and product type details
    - Overnight vs intraday position breakdown
    - Historical position evolution
    """
    __tablename__ = "position_snapshots"
    __table_args__ = (
        # Performance indexes
        Index("idx_position_snapshots_time", "snapshot_time"),
        Index("idx_position_snapshots_position_id", "position_id"),
        Index("idx_position_snapshots_user_id", "user_id"),
        Index("idx_position_snapshots_account", "trading_account_id"),
        Index("idx_position_snapshots_symbol", "symbol"),
        Index("idx_position_snapshots_strategy", "strategy_id"),
        Index("idx_position_snapshots_position_time", "position_id", "snapshot_time"),
        Index("idx_position_snapshots_user_time", "user_id", "snapshot_time"),
        Index("idx_position_snapshots_symbol_time", "symbol", "snapshot_time"),
        {"schema": "order_service"},
    )

    # Composite primary key (time + position)
    snapshot_time = Column(
        DateTime(timezone=True),
        primary_key=True,
        comment="Timestamp when snapshot was captured"
    )
    position_id = Column(
        Integer,
        primary_key=True,
        comment="Position ID for this snapshot"
    )
    
    # User and account references
    user_id = Column(
        Integer,
        nullable=False,
        comment="User ID who owns this position"
    )
    trading_account_id = Column(
        String(100),
        nullable=False,
        comment="Trading account containing this position"
    )
    
    # Instrument details
    symbol = Column(
        String(50),
        nullable=False,
        comment="Trading symbol (e.g., RELIANCE, NIFTY25DEC24500CE)"
    )
    exchange = Column(
        String(10),
        nullable=True,
        comment="Exchange (NSE, NFO, BSE, etc.)"
    )
    segment = Column(
        Text,
        nullable=True,
        comment="Market segment (equity, futures, options, etc.)"
    )
    product_type = Column(
        String(20),
        nullable=True,
        comment="Product type (CNC, MIS, NRML)"
    )
    
    # Strategy association
    strategy_id = Column(
        BigInteger,
        nullable=True,
        comment="Strategy ID that created/manages this position"
    )
    
    # Position quantities
    quantity = Column(
        Integer,
        nullable=True,
        comment="Total position quantity (positive=long, negative=short)"
    )
    overnight_quantity = Column(
        Integer,
        nullable=True,
        comment="Overnight/carry-forward position quantity"
    )
    day_quantity = Column(
        Integer,
        nullable=True,
        comment="Intraday position quantity"
    )

    def __repr__(self):
        return (
            f"<PositionSnapshot("
            f"time={self.snapshot_time}, "
            f"position={self.position_id}, "
            f"symbol='{self.symbol}', "
            f"user={self.user_id}, "
            f"qty={self.quantity}, "
            f"overnight={self.overnight_quantity}, "
            f"day={self.day_quantity}"
            f")>"
        )

    @property
    def is_long_position(self) -> bool:
        """Check if this is a long position"""
        return self.quantity is not None and self.quantity > 0

    @property
    def is_short_position(self) -> bool:
        """Check if this is a short position"""
        return self.quantity is not None and self.quantity < 0

    @property
    def is_flat_position(self) -> bool:
        """Check if position is flat (no quantity)"""
        return self.quantity is None or self.quantity == 0

    @property
    def has_overnight_position(self) -> bool:
        """Check if there's an overnight position component"""
        return self.overnight_quantity is not None and self.overnight_quantity != 0

    @property
    def has_intraday_position(self) -> bool:
        """Check if there's an intraday position component"""
        return self.day_quantity is not None and self.day_quantity != 0

    @property
    def is_equity_position(self) -> bool:
        """Check if this is an equity position"""
        return self.segment and 'equity' in self.segment.lower()

    @property
    def is_derivative_position(self) -> bool:
        """Check if this is a derivatives position"""
        return self.segment and ('futures' in self.segment.lower() or 'options' in self.segment.lower())

    @property
    def position_side(self) -> str:
        """Get position side as string"""
        if self.is_long_position:
            return "long"
        elif self.is_short_position:
            return "short"
        else:
            return "flat"

    def calculate_position_metrics(self) -> dict:
        """Calculate comprehensive position metrics"""
        return {
            "symbol": self.symbol,
            "exchange": self.exchange,
            "segment": self.segment,
            "product_type": self.product_type,
            "total_quantity": self.quantity or 0,
            "overnight_quantity": self.overnight_quantity or 0,
            "day_quantity": self.day_quantity or 0,
            "position_side": self.position_side,
            "is_long_position": self.is_long_position,
            "is_short_position": self.is_short_position,
            "is_flat_position": self.is_flat_position,
            "has_overnight_position": self.has_overnight_position,
            "has_intraday_position": self.has_intraday_position,
            "is_equity_position": self.is_equity_position,
            "is_derivative_position": self.is_derivative_position,
        }

    def to_dict(self):
        """Convert snapshot to dictionary"""
        return {
            "snapshot_time": self.snapshot_time.isoformat() if self.snapshot_time else None,
            "position_id": self.position_id,
            "user_id": self.user_id,
            "trading_account_id": self.trading_account_id,
            "symbol": self.symbol,
            "exchange": self.exchange,
            "segment": self.segment,
            "product_type": self.product_type,
            "strategy_id": self.strategy_id,
            "quantity": self.quantity,
            "overnight_quantity": self.overnight_quantity,
            "day_quantity": self.day_quantity,
            # Calculated metrics
            "position_side": self.position_side,
            "is_long_position": self.is_long_position,
            "is_short_position": self.is_short_position,
            "is_flat_position": self.is_flat_position,
            "has_overnight_position": self.has_overnight_position,
            "has_intraday_position": self.has_intraday_position,
            "is_equity_position": self.is_equity_position,
            "is_derivative_position": self.is_derivative_position,
        }

    @classmethod
    def create_snapshot(
        cls,
        position_id: int,
        user_id: int,
        trading_account_id: str,
        symbol: str,
        exchange: Optional[str] = None,
        segment: Optional[str] = None,
        product_type: Optional[str] = None,
        strategy_id: Optional[int] = None,
        quantity: Optional[int] = None,
        overnight_quantity: Optional[int] = None,
        day_quantity: Optional[int] = None,
        snapshot_time: Optional[datetime] = None
    ) -> 'PositionSnapshot':
        """
        Factory method to create a position snapshot
        
        Args:
            position_id: Position ID
            user_id: User ID
            trading_account_id: Trading account ID
            symbol: Trading symbol
            exchange: Exchange name
            segment: Market segment
            product_type: Product type
            strategy_id: Associated strategy ID
            quantity: Total position quantity
            overnight_quantity: Overnight position quantity
            day_quantity: Intraday position quantity
            snapshot_time: Snapshot timestamp (defaults to now)
            
        Returns:
            PositionSnapshot instance
        """
        return cls(
            snapshot_time=snapshot_time or datetime.utcnow(),
            position_id=position_id,
            user_id=user_id,
            trading_account_id=trading_account_id,
            symbol=symbol,
            exchange=exchange,
            segment=segment,
            product_type=product_type,
            strategy_id=strategy_id,
            quantity=quantity,
            overnight_quantity=overnight_quantity,
            day_quantity=day_quantity
        )
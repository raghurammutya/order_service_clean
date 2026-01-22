"""
Position Model

Tracks current and historical positions for users.
Positions are updated in real-time as trades are executed.
"""
from datetime import datetime
from sqlalchemy import (
    Column, Integer, BigInteger, String, Numeric, DateTime, Boolean, Index
)
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class PositionSource:
    """Position source enumeration"""
    MANUAL = "manual"        # Manual user trading
    SCRIPT = "script"        # Created by algo/script
    EXTERNAL = "external"    # From broker sync (placed externally)
    BROKER_DIRECT = "broker_direct"  # Direct broker API calls


class Position(Base):
    """Position tracking model"""

    __tablename__ = "positions"
    __table_args__ = (
        Index("idx_positions_user_id", "user_id"),
        Index("idx_positions_trading_account_id", "trading_account_id"),
        Index("idx_positions_symbol", "symbol"),
        Index("idx_positions_updated_at", "updated_at"),
        Index("idx_positions_user_symbol", "user_id", "symbol", "product_type"),  # Composite index
        Index("idx_positions_strategy_id", "strategy_id"),
        Index("idx_positions_execution_id", "execution_id"),
        Index("idx_positions_portfolio_id", "portfolio_id"),
        Index("idx_positions_source", "source"),
    )

    # Primary Key
    id = Column(Integer, primary_key=True, autoincrement=True)

    # User & Account
    user_id = Column(Integer, nullable=False, comment="User ID")
    trading_account_id = Column(String(100), nullable=False, comment="Trading account ID")

    # Sprint 7A Linkage Columns  
    execution_id = Column(
        UUID(as_uuid=True),
        nullable=True,
        comment="Execution context UUID for algo engine coordination"
    )
    
    strategy_id = Column(
        BigInteger,
        nullable=True,
        comment="Strategy that owns this position (FK to public.strategy in DB)"
    )
    
    portfolio_id = Column(
        String(255),
        nullable=True,
        comment="Portfolio identifier for grouping related positions"
    )

    # Position Details
    symbol = Column(String(50), nullable=False, comment="Trading symbol")
    exchange = Column(String(10), nullable=False, comment="Exchange")
    product_type = Column(String(10), nullable=False, comment="CNC, MIS, NRML")

    # Quantity
    quantity = Column(Integer, nullable=False, default=0, comment="Net quantity (positive=long, negative=short)")
    overnight_quantity = Column(Integer, nullable=False, default=0, comment="Overnight position quantity")
    day_quantity = Column(Integer, nullable=False, default=0, comment="Intraday position quantity")

    # Buy Side
    buy_quantity = Column(Integer, nullable=False, default=0, comment="Total buy quantity")
    buy_value = Column(Numeric(18, 2), nullable=False, default=0, comment="Total buy value")
    buy_price = Column(Numeric(18, 2), nullable=True, comment="Average buy price")

    # Sell Side
    sell_quantity = Column(Integer, nullable=False, default=0, comment="Total sell quantity")
    sell_value = Column(Numeric(18, 2), nullable=False, default=0, comment="Total sell value")
    sell_price = Column(Numeric(18, 2), nullable=True, comment="Average sell price")

    # P&L Calculation
    realized_pnl = Column(Numeric(18, 2), nullable=False, default=0, comment="Realized P&L (gross, before charges)")
    unrealized_pnl = Column(Numeric(18, 2), nullable=False, default=0, comment="Unrealized P&L (gross, before charges)")
    total_pnl = Column(Numeric(18, 2), nullable=False, default=0, comment="Total P&L (gross, realized + unrealized)")

    # Brokerage & Charges
    total_charges = Column(Numeric(18, 2), nullable=False, default=0, comment="Total brokerage + charges")
    brokerage = Column(Numeric(18, 2), nullable=False, default=0, comment="Brokerage fees")
    stt = Column(Numeric(18, 2), nullable=False, default=0, comment="Securities Transaction Tax")
    exchange_charges = Column(Numeric(18, 2), nullable=False, default=0, comment="Exchange transaction charges")
    gst = Column(Numeric(18, 2), nullable=False, default=0, comment="GST on brokerage + charges")

    # Net P&L (after charges)
    net_pnl = Column(Numeric(18, 2), nullable=False, default=0, comment="Net P&L (total_pnl - total_charges)")

    # Market Data
    last_price = Column(Numeric(18, 2), nullable=True, comment="Last traded price (for P&L calculation)")
    close_price = Column(Numeric(18, 2), nullable=True, comment="Previous day close price")

    # Status
    is_open = Column(Boolean, nullable=False, default=True, comment="Whether position is still open")

    # Timestamps
    opened_at = Column(DateTime, nullable=False, default=datetime.utcnow, comment="When position was first opened")
    closed_at = Column(DateTime, nullable=True, comment="When position was fully closed")
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Trading Day
    trading_day = Column(DateTime, nullable=False, comment="Trading day for this position")

    # Source Tracking
    source = Column(
        String(20),
        nullable=False,
        default=PositionSource.SCRIPT,
        comment="Source: manual (user), script (algo), external (broker sync), broker_direct (API)"
    )
    
    # Additional metadata for Sprint 7A features
    position_metadata = Column(
        JSONB,
        nullable=False,
        default={},
        comment="Additional position metadata for reconciliation and handoff features"
    )

    # Instrument Token for WebSocket subscriptions
    instrument_token = Column(
        BigInteger,
        nullable=True,
        comment="Instrument token from broker (for WebSocket tick subscriptions)"
    )

    def __repr__(self):
        return (
            f"<Position(id={self.id}, symbol={self.symbol}, "
            f"qty={self.quantity}, pnl={self.total_pnl})>"
        )

    def to_dict(self):
        """Convert position to dictionary"""
        return {
            "id": self.id,
            "user_id": self.user_id,
            "trading_account_id": self.trading_account_id,
            "symbol": self.symbol,
            "exchange": self.exchange,
            "product_type": self.product_type,
            "quantity": self.quantity,
            "overnight_quantity": self.overnight_quantity,
            "day_quantity": self.day_quantity,
            "buy_quantity": self.buy_quantity,
            "buy_value": float(self.buy_value),
            "buy_price": float(self.buy_price) if self.buy_price else None,
            "sell_quantity": self.sell_quantity,
            "sell_value": float(self.sell_value),
            "sell_price": float(self.sell_price) if self.sell_price else None,
            "realized_pnl": float(self.realized_pnl),
            "unrealized_pnl": float(self.unrealized_pnl),
            "total_pnl": float(self.total_pnl),
            "total_charges": float(self.total_charges),
            "brokerage": float(self.brokerage),
            "stt": float(self.stt),
            "exchange_charges": float(self.exchange_charges),
            "gst": float(self.gst),
            "net_pnl": float(self.net_pnl),
            "last_price": float(self.last_price) if self.last_price else None,
            "close_price": float(self.close_price) if self.close_price else None,
            "is_open": self.is_open,
            "opened_at": self.opened_at.isoformat() if self.opened_at else None,
            "closed_at": self.closed_at.isoformat() if self.closed_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
            "trading_day": self.trading_day.isoformat() if self.trading_day else None,
            "execution_id": str(self.execution_id) if self.execution_id else None,
            "strategy_id": self.strategy_id,
            "portfolio_id": self.portfolio_id,
            "source": self.source,
            "metadata": self.metadata,
            "instrument_token": self.instrument_token,
        }

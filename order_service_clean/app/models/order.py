"""
Order Model

Tracks all orders placed by users through the trading platform.
"""
import enum
from datetime import datetime
from sqlalchemy import (
    Column, Integer, BigInteger, String, Numeric, DateTime, Boolean, Text, Index
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class OrderSource:
    """Order source enumeration"""
    MANUAL = "manual"        # Manually placed by user
    SCRIPT = "script"        # Placed by algo/script
    EXTERNAL = "external"    # From broker sync (placed externally)
    BROKER_DIRECT = "broker_direct"  # Direct broker API calls


class OrderStatus(str, enum.Enum):
    """Order status enumeration"""
    PENDING = "PENDING"              # Order created, not yet sent to broker
    SUBMITTED = "SUBMITTED"          # Order submitted to broker
    OPEN = "OPEN"                    # Order accepted by exchange
    COMPLETE = "COMPLETE"            # Order fully executed
    CANCELLED = "CANCELLED"          # Order cancelled
    REJECTED = "REJECTED"            # Order rejected by broker/exchange
    TRIGGER_PENDING = "TRIGGER_PENDING"  # Pending trigger (for stop-loss orders)


class OrderType(str, enum.Enum):
    """Order type enumeration"""
    MARKET = "MARKET"
    LIMIT = "LIMIT"
    SL = "SL"          # Stop-loss limit
    SL_M = "SL-M"      # Stop-loss market


class ProductType(str, enum.Enum):
    """Product type enumeration"""
    CNC = "CNC"        # Cash and Carry (delivery)
    MIS = "MIS"        # Margin Intraday Square-off
    NRML = "NRML"      # Normal (for F&O)


class Variety(str, enum.Enum):
    """Order variety enumeration"""
    REGULAR = "regular"
    AMO = "amo"        # After Market Order
    ICEBERG = "iceberg"
    AUCTION = "auction"


class Order(Base):
    """Order model for tracking all trading orders"""

    __tablename__ = "orders"
    __table_args__ = (
        Index("idx_orders_user_id", "user_id"),
        Index("idx_orders_trading_account_id", "trading_account_id"),
        Index("idx_orders_strategy_id", "strategy_id"),
        Index("idx_orders_execution_id", "execution_id"),
        Index("idx_orders_portfolio_id", "portfolio_id"),
        Index("idx_orders_symbol", "symbol"),
        Index("idx_orders_status", "status"),
        Index("idx_orders_created_at", "created_at"),
        Index("idx_orders_broker_order_id", "broker_order_id"),
        Index("idx_orders_source", "source"),
    )

    # Primary Key
    id = Column(Integer, primary_key=True, autoincrement=True)

    # User & Account
    user_id = Column(Integer, nullable=False, comment="User who placed the order")
    trading_account_id = Column(String(100), nullable=False, comment="Trading account used")

    # Sprint 7A Linkage Columns
    execution_id = Column(
        UUID(as_uuid=True),
        nullable=True,
        comment="Execution context UUID for algo engine coordination"
    )
    
    strategy_id = Column(
        BigInteger,
        nullable=True,
        comment="Strategy that placed this order (FK to public.strategy in DB)"
    )
    
    portfolio_id = Column(
        String(255),
        nullable=True,
        comment="Portfolio identifier for grouping related positions"
    )

    # Position Linkage (nullable since orders may not be linked to a position)
    # Note: FK constraint exists in database (fk_orders_position_id), not in ORM
    position_id = Column(
        Integer,
        nullable=True,
        comment="Position that this order belongs to (FK to order_service.positions.id)"
    )

    # Source Tracking
    source = Column(
        String(20),
        nullable=False,
        default=OrderSource.MANUAL,
        comment="Source: manual (user), script (algo), external (broker sync), broker_direct (API)"
    )

    # Broker Information
    broker_order_id = Column(String(100), unique=True, nullable=True, comment="Broker's order ID (from Kite)")
    broker_tag = Column(String(50), nullable=True, comment="Custom tag for the order")

    # Order Details
    symbol = Column(String(50), nullable=False, comment="Trading symbol (e.g., RELIANCE, NIFTY25DEC24500CE)")
    exchange = Column(String(10), nullable=False, comment="Exchange (NSE, NFO, BSE, etc.)")

    # Order Specifications
    transaction_type = Column(String(4), nullable=False, comment="BUY or SELL")
    order_type = Column(String(10), nullable=False, comment="MARKET, LIMIT, SL, SL-M")
    product_type = Column(String(10), nullable=False, comment="CNC, MIS, NRML")
    variety = Column(String(20), nullable=False, default="regular", comment="Order variety")

    # Quantity
    quantity = Column(Integer, nullable=False, comment="Total quantity ordered")
    filled_quantity = Column(Integer, nullable=False, default=0, comment="Quantity filled")
    pending_quantity = Column(Integer, nullable=False, comment="Quantity pending")
    cancelled_quantity = Column(Integer, nullable=False, default=0, comment="Quantity cancelled")

    # Price
    price = Column(Numeric(18, 2), nullable=True, comment="Limit price (for LIMIT orders)")
    trigger_price = Column(Numeric(18, 2), nullable=True, comment="Trigger price (for SL orders)")
    average_price = Column(Numeric(18, 2), nullable=True, comment="Average execution price")

    # Status
    status = Column(String(20), nullable=False, default="PENDING", comment="Current order status")
    status_message = Column(Text, nullable=True, comment="Status message from broker")

    # Validity
    validity = Column(String(10), nullable=False, default="DAY", comment="DAY or IOC")
    disclosed_quantity = Column(Integer, nullable=True, comment="Disclosed quantity (for iceberg orders)")

    # Timestamps
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow, comment="Order creation time")
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)
    submitted_at = Column(DateTime, nullable=True, comment="When order was submitted to broker")
    exchange_timestamp = Column(DateTime, nullable=True, comment="Exchange timestamp")

    # Risk Checks
    risk_check_passed = Column(Boolean, nullable=False, default=False, comment="Whether risk checks passed")
    risk_check_details = Column(Text, nullable=True, comment="Details of risk checks")

    # Parent Order (for bracket/cover orders)
    parent_order_id = Column(Integer, nullable=True, comment="Parent order ID (for child orders)")

    # Additional Data
    order_metadata = Column(Text, nullable=True, comment="Additional metadata (JSON)")

    def __repr__(self):
        return (
            f"<Order(id={self.id}, symbol={self.symbol}, "
            f"type={self.transaction_type}, qty={self.quantity}, "
            f"status={self.status})>"
        )

    def to_dict(self):
        """Convert order to dictionary"""
        return {
            "id": self.id,
            "user_id": self.user_id,
            "trading_account_id": self.trading_account_id,
            "broker_order_id": self.broker_order_id,
            "symbol": self.symbol,
            "exchange": self.exchange,
            "transaction_type": self.transaction_type,
            "order_type": self.order_type.value if hasattr(self.order_type, 'value') else self.order_type,
            "product_type": self.product_type.value if hasattr(self.product_type, 'value') else self.product_type,
            "variety": self.variety.value if hasattr(self.variety, 'value') else self.variety,
            "quantity": self.quantity,
            "filled_quantity": self.filled_quantity,
            "pending_quantity": self.pending_quantity,
            "cancelled_quantity": self.cancelled_quantity,
            "price": float(self.price) if self.price else None,
            "trigger_price": float(self.trigger_price) if self.trigger_price else None,
            "average_price": float(self.average_price) if self.average_price else None,
            "status": self.status.value if hasattr(self.status, 'value') else self.status,
            "status_message": self.status_message,
            "validity": self.validity,
            "disclosed_quantity": self.disclosed_quantity,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
            "submitted_at": self.submitted_at.isoformat() if self.submitted_at else None,
            "exchange_timestamp": self.exchange_timestamp.isoformat() if self.exchange_timestamp else None,
            "risk_check_passed": self.risk_check_passed,
            "parent_order_id": self.parent_order_id,
            "execution_id": str(self.execution_id) if self.execution_id else None,
            "strategy_id": self.strategy_id,
            "portfolio_id": self.portfolio_id,
            "position_id": self.position_id,
            "source": self.source,
        }

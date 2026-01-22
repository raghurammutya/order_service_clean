"""
Trade Model

Tracks individual trade executions (fills) for orders.
Multiple trades can be associated with a single order.
"""
from datetime import datetime
from sqlalchemy import (
    Column, Integer, String, Numeric, DateTime, Index
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class Trade(Base):
    """Trade execution model"""

    __tablename__ = "trades"
    __table_args__ = (
        Index("idx_trades_order_id", "order_id"),
        Index("idx_trades_user_id", "user_id"),
        Index("idx_trades_symbol", "symbol"),
        Index("idx_trades_trade_time", "trade_time"),
        Index("idx_trades_broker_trade_id", "broker_trade_id"),
    )

    # Primary Key
    id = Column(Integer, primary_key=True, autoincrement=True)

    # References
    # Note: FK constraint removed due to SQLAlchemy Base class issue. FK exists at DB level.
    order_id = Column(Integer, nullable=True, comment="Internal order ID")
    broker_order_id = Column(String(100), nullable=False, comment="Broker's order ID")
    broker_trade_id = Column(String(100), unique=True, nullable=False, comment="Broker's unique trade ID")

    # User & Account
    user_id = Column(Integer, nullable=False, comment="User ID")
    trading_account_id = Column(String(100), nullable=False, comment="Trading account ID")

    # Trade Details
    symbol = Column(String(50), nullable=False, comment="Trading symbol")
    exchange = Column(String(10), nullable=False, comment="Exchange")
    transaction_type = Column(String(4), nullable=False, comment="BUY or SELL")
    product_type = Column(String(10), nullable=False, comment="CNC, MIS, NRML")

    # Execution Details
    quantity = Column(Integer, nullable=False, comment="Executed quantity")
    price = Column(Numeric(18, 2), nullable=False, comment="Execution price")
    trade_value = Column(Numeric(18, 2), nullable=False, comment="Total trade value (qty * price)")

    # Strategy Reference
    strategy_id = Column(Integer, nullable=True, comment="Strategy ID if from order_service.strategy")
    
    # Sprint 7A Linkage Columns
    execution_id = Column(
        UUID(as_uuid=True),
        nullable=True,
        comment="Execution context UUID for algo engine coordination"
    )
    
    portfolio_id = Column(
        String(255),
        nullable=True,
        comment="Portfolio identifier for grouping related positions"
    )
    
    # Source Tracking (Sprint 7B requirement)
    source = Column(String(50), nullable=False, default="manual", comment="Trade source: manual, script, external, broker_sync")

    # Timestamps
    trade_time = Column(DateTime, nullable=False, comment="Exchange trade timestamp")
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow, comment="Record creation time")

    def __repr__(self):
        return (
            f"<Trade(id={self.id}, symbol={self.symbol}, "
            f"qty={self.quantity}, price={self.price})>"
        )

    def to_dict(self):
        """Convert trade to dictionary"""
        return {
            "id": self.id,
            "order_id": self.order_id,
            "broker_order_id": self.broker_order_id,
            "broker_trade_id": self.broker_trade_id,
            "user_id": self.user_id,
            "trading_account_id": self.trading_account_id,
            "strategy_id": self.strategy_id,
            "symbol": self.symbol,
            "exchange": self.exchange,
            "transaction_type": self.transaction_type,
            "product_type": self.product_type,
            "quantity": self.quantity,
            "price": float(self.price),
            "trade_value": float(self.trade_value),
            "trade_time": self.trade_time.isoformat() if self.trade_time else None,
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }

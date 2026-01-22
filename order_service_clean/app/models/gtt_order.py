"""
GTT Order Model

Tracks Good-Till-Triggered (GTT) conditional orders.
GTT orders remain pending until market conditions are met.
"""
from datetime import datetime
from typing import Dict, Any
from sqlalchemy import Column, Integer, String, DateTime, Text, Index
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class GttOrder(Base):
    """
    Good-Till-Triggered order model.

    GTT orders are conditional orders that trigger when price conditions are met.
    Supports two types:
    - Single: Stop-loss or target orders (one trigger price)
    - Two-leg: OCO (One Cancels Other) orders (two trigger prices)
    """
    __tablename__ = "gtt_orders"
    __table_args__ = (
        Index("idx_gtt_orders_user_id", "user_id"),
        Index("idx_gtt_orders_trading_account_id", "trading_account_id"),
        Index("idx_gtt_orders_status", "status"),
        Index("idx_gtt_orders_broker_gtt_id", "broker_gtt_id"),
        Index("idx_gtt_orders_symbol", "symbol"),
        Index("idx_gtt_orders_created_at", "created_at"),
        Index("idx_gtt_orders_user_status", "user_id", "status"),
    )

    # Primary Key
    id = Column(Integer, primary_key=True, autoincrement=True)

    # User & Account
    user_id = Column(Integer, nullable=False, comment="User ID")
    trading_account_id = Column(String(100), nullable=False, comment="Trading account ID")

    # Broker Information
    broker_gtt_id = Column(Integer, nullable=True, comment="Broker's external GTT ID")

    # GTT Configuration
    gtt_type = Column(String(20), nullable=False, comment="single or two-leg (OCO)")
    status = Column(String(20), nullable=False, default='active', comment="active, triggered, cancelled, expired, deleted")

    # Instrument Details
    symbol = Column(String(50), nullable=False, comment="Trading symbol")
    exchange = Column(String(10), nullable=False, comment="Exchange")
    symbol = Column(String(50), nullable=False, comment="Broker's trading symbol")

    # Trigger Conditions (JSONB)
    condition = Column(JSONB, nullable=False, comment="Trigger conditions")

    # Orders to Place When Triggered (JSONB)
    orders = Column(JSONB, nullable=False, comment="Orders to place when triggered")

    # Metadata
    expires_at = Column(DateTime, nullable=True, comment="GTT expiry time")

    # Timestamps
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)
    triggered_at = Column(DateTime, nullable=True, comment="When GTT was triggered")
    cancelled_at = Column(DateTime, nullable=True, comment="When GTT was cancelled")

    # Broker Metadata
    broker_metadata = Column(JSONB, nullable=True, comment="Additional broker data")

    # User Notes
    user_tag = Column(String(100), nullable=True, comment="Custom user tag")
    user_notes = Column(Text, nullable=True, comment="User notes")

    def __repr__(self):
        return (
            f"<GttOrder(id={self.id}, type={self.gtt_type}, "
            f"symbol={self.symbol}, status={self.status})>"
        )

    def to_dict(self) -> Dict[str, Any]:
        """Convert GTT order to dictionary"""
        return {
            'id': self.id,
            'user_id': self.user_id,
            'trading_account_id': self.trading_account_id,
            'broker_gtt_id': self.broker_gtt_id,
            'gtt_type': self.gtt_type,
            'status': self.status,
            'symbol': self.symbol,
            'exchange': self.exchange,
            "symbol": self.tradingsymbol,
            'condition': self.condition,
            'orders': self.orders,
            'expires_at': self.expires_at.isoformat() if self.expires_at else None,
            'created_at': self.created_at.isoformat(),
            'updated_at': self.updated_at.isoformat(),
            'triggered_at': self.triggered_at.isoformat() if self.triggered_at else None,
            'cancelled_at': self.cancelled_at.isoformat() if self.cancelled_at else None,
            'broker_metadata': self.broker_metadata,
            'user_tag': self.user_tag,
            'user_notes': self.user_notes,
        }

"""
Portfolio Models

Tracks user portfolios for organizing trading accounts and strategies.
Portfolios aggregate positions, holdings, and P&L across multiple accounts.
"""
from datetime import datetime
from sqlalchemy import (
    Column, Integer, String, DateTime, Boolean, Index,
    ForeignKey, Text
)
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class Portfolio(Base):
    """Portfolio tracking model"""

    __tablename__ = "portfolios"
    __table_args__ = (
        Index("idx_portfolios_user", "user_id"),
        Index("idx_portfolios_default", "user_id", "is_default"),
    )

    # Primary Key
    portfolio_id = Column(Integer, primary_key=True, autoincrement=True)

    # User
    user_id = Column(Integer, nullable=False, comment="User ID who owns this portfolio")

    # Portfolio Details
    portfolio_name = Column(String(200), nullable=False, comment="Display name for the portfolio")
    description = Column(Text, nullable=True, comment="Optional description of the portfolio purpose")
    is_default = Column(Boolean, nullable=False, default=False, comment="Whether this is the default portfolio")

    # Timestamps
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow, comment="Creation timestamp")
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow, comment="Last update timestamp")

    def to_dict(self):
        """Convert portfolio to dictionary"""
        return {
            "portfolio_id": self.portfolio_id,
            "user_id": self.user_id,
            "portfolio_name": self.portfolio_name,
            "description": self.description,
            "is_default": self.is_default,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }


class PortfolioAccount(Base):
    """Portfolio-Account junction table"""

    __tablename__ = "portfolio_accounts"
    __table_args__ = (
        Index("idx_portfolio_accounts_portfolio", "portfolio_id"),
        Index("idx_portfolio_accounts_account", "trading_account_id"),
    )

    # Composite Primary Key
    portfolio_id = Column(Integer, ForeignKey("order_service.portfolios.portfolio_id", ondelete="CASCADE"), primary_key=True)
    trading_account_id = Column(String(100), primary_key=True, comment="Trading account ID (no FK - follows order_service pattern)")

    # Timestamp
    added_at = Column(DateTime, nullable=False, default=datetime.utcnow, comment="When the account was added to the portfolio")

    def to_dict(self):
        """Convert to dictionary"""
        return {
            "portfolio_id": self.portfolio_id,
            "trading_account_id": self.trading_account_id,
            "added_at": self.added_at.isoformat() if self.added_at else None,
        }


class PortfolioStrategy(Base):
    """Portfolio-Strategy junction table"""

    __tablename__ = "portfolio_strategies"
    __table_args__ = (
        Index("idx_portfolio_strategies_portfolio", "portfolio_id"),
        Index("idx_portfolio_strategies_strategy", "strategy_id"),
    )

    # Composite Primary Key
    portfolio_id = Column(Integer, ForeignKey("order_service.portfolios.portfolio_id", ondelete="CASCADE"), primary_key=True)
    strategy_id = Column(Integer, primary_key=True, comment="Strategy ID (references order_service.strategies)")

    # Timestamp
    added_at = Column(DateTime, nullable=False, default=datetime.utcnow, comment="When the strategy was added to the portfolio")

    def to_dict(self):
        """Convert to dictionary"""
        return {
            "portfolio_id": self.portfolio_id,
            "strategy_id": self.strategy_id,
            "added_at": self.added_at.isoformat() if self.added_at else None,
        }

"""
Portfolio Configuration Model

Manages portfolio-level configuration including capital allocation,
risk limits, and strategy allocation policies.
"""
from sqlalchemy import (
    Column, BigInteger, String, Numeric, Integer, DateTime, Text,
    Index, func
)
from datetime import datetime
from typing import Optional

from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class PortfolioConfig(Base):
    """
    Portfolio Configuration Model
    
    Stores portfolio-level settings for capital allocation, risk management,
    and strategy allocation policies.
    
    Key Features:
    - Total capital management per portfolio
    - Risk limit percentage controls
    - Allocation policy configuration (equal_weight, risk_parity, etc.)
    - Maximum strategy limits per portfolio
    - Minimum strategy allocation amounts
    - Rebalancing threshold settings
    """
    __tablename__ = "portfolio_config"
    __table_args__ = (
        Index("idx_portfolio_config_user_id", "user_id"),
        Index("idx_portfolio_config_portfolio_id", "portfolio_id"),
        {"schema": "order_service"},
    )

    # Primary key
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    
    # References
    portfolio_id = Column(
        String(255), 
        nullable=False, 
        comment="Portfolio identifier for this configuration"
    )
    user_id = Column(
        String(255), 
        nullable=False, 
        comment="User ID who owns this portfolio"
    )
    
    # Capital management
    total_capital = Column(
        Numeric(20, 8), 
        nullable=False,
        comment="Total capital allocated to this portfolio"
    )
    
    # Risk management
    risk_limit_pct = Column(
        Numeric(5, 2), 
        nullable=False, 
        default=20.00,
        comment="Maximum risk percentage allowed (0.00 to 100.00)"
    )
    
    # Allocation policies
    allocation_policy = Column(
        String(50), 
        nullable=False, 
        default="equal_weight",
        comment="Strategy allocation policy: equal_weight, risk_parity, momentum_based"
    )
    
    # Strategy limits
    max_strategies = Column(
        Integer, 
        nullable=False, 
        default=10,
        comment="Maximum number of strategies allowed in this portfolio"
    )
    min_strategy_allocation = Column(
        Numeric(20, 8), 
        nullable=False, 
        default=10000.00,
        comment="Minimum capital allocation per strategy"
    )
    
    # Rebalancing configuration
    rebalance_threshold_pct = Column(
        Numeric(5, 2), 
        nullable=False, 
        default=5.00,
        comment="Percentage drift threshold to trigger rebalancing"
    )
    
    # Metadata
    description = Column(Text, nullable=True, comment="Portfolio description/notes")
    
    # Audit fields
    created_at = Column(
        DateTime(timezone=True), 
        nullable=False, 
        default=func.now(),
        comment="Configuration creation timestamp"
    )
    updated_at = Column(
        DateTime(timezone=True), 
        nullable=True,
        comment="Last update timestamp"
    )
    created_by = Column(
        String(255), 
        nullable=True,
        comment="User who created this configuration"
    )
    updated_by = Column(
        String(255), 
        nullable=True,
        comment="User who last updated this configuration"
    )

    def __repr__(self):
        return (
            f"<PortfolioConfig("
            f"id={self.id}, "
            f"portfolio_id='{self.portfolio_id}', "
            f"user_id='{self.user_id}', "
            f"total_capital={self.total_capital}, "
            f"risk_limit={self.risk_limit_pct}%"
            f")>"
        )

    @property
    def risk_limit_decimal(self) -> float:
        """Convert risk limit percentage to decimal"""
        return float(self.risk_limit_pct) / 100.0
    
    @property
    def rebalance_threshold_decimal(self) -> float:
        """Convert rebalance threshold percentage to decimal"""
        return float(self.rebalance_threshold_pct) / 100.0
    
    def get_max_risk_amount(self) -> float:
        """Calculate maximum risk amount in absolute terms"""
        return float(self.total_capital) * self.risk_limit_decimal
    
    def get_strategy_allocation_budget(self) -> float:
        """Calculate total budget available for strategy allocation"""
        return float(self.total_capital)
    
    def can_add_strategy(self, current_strategy_count: int) -> bool:
        """Check if another strategy can be added to portfolio"""
        return current_strategy_count < self.max_strategies
    
    def update_metadata(self, updated_by: Optional[str] = None) -> None:
        """Update audit metadata"""
        self.updated_at = datetime.utcnow()
        if updated_by:
            self.updated_by = updated_by

    def to_dict(self):
        """Convert portfolio config to dictionary"""
        return {
            "id": self.id,
            "portfolio_id": self.portfolio_id,
            "user_id": self.user_id,
            "total_capital": float(self.total_capital) if self.total_capital else None,
            "risk_limit_pct": float(self.risk_limit_pct) if self.risk_limit_pct else None,
            "allocation_policy": self.allocation_policy,
            "max_strategies": self.max_strategies,
            "min_strategy_allocation": float(self.min_strategy_allocation) if self.min_strategy_allocation else None,
            "rebalance_threshold_pct": float(self.rebalance_threshold_pct) if self.rebalance_threshold_pct else None,
            "description": self.description,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
            "created_by": self.created_by,
            "updated_by": self.updated_by,
        }

    @classmethod
    def create_default_config(
        cls, 
        portfolio_id: str, 
        user_id: str, 
        total_capital: float,
        created_by: Optional[str] = None
    ) -> 'PortfolioConfig':
        """
        Factory method to create a default portfolio configuration
        
        Args:
            portfolio_id: Portfolio identifier
            user_id: User identifier
            total_capital: Total capital for the portfolio
            created_by: User creating the configuration
            
        Returns:
            PortfolioConfig instance with default settings
        """
        return cls(
            portfolio_id=portfolio_id,
            user_id=user_id,
            total_capital=total_capital,
            risk_limit_pct=20.00,  # 20% default risk limit
            allocation_policy="equal_weight",
            max_strategies=10,
            min_strategy_allocation=10000.00,  # ¹10,000 minimum
            rebalance_threshold_pct=5.00,  # 5% drift threshold
            created_at=datetime.utcnow(),
            created_by=created_by
        )
"""
Portfolio Allocation Model

Manages allocation of capital within portfolios to strategies or sub-portfolios.
Tracks target vs actual allocations, drift, and rebalancing history.
"""
from sqlalchemy import (
    Column, BigInteger, Integer, Numeric, DateTime, Text,
    Index, func, ForeignKey, CheckConstraint
)
from datetime import datetime
from typing import Optional

from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class PortfolioAllocation(Base):
    """
    Portfolio Allocation Model
    
    Tracks the allocation of capital within a portfolio to strategies
    or child portfolios. Manages target weights, actual positions,
    drift calculations, and rebalancing triggers.
    
    Key Features:
    - Target weight and capital allocation per strategy/sub-portfolio
    - Real-time drift calculation from target allocations
    - Minimum allocation constraints
    - Rebalancing history tracking
    - Support for hierarchical portfolio structures
    """
    __tablename__ = "portfolio_allocations"
    __table_args__ = (
        # Check constraints for data integrity
        CheckConstraint(
            "target_weight_pct >= 0.00 AND target_weight_pct <= 100.00",
            name="portfolio_allocation_target_weight_check"
        ),
        CheckConstraint(
            "current_weight_pct >= 0.00",
            name="portfolio_allocation_current_weight_check"
        ),
        CheckConstraint(
            "target_capital >= 0",
            name="portfolio_allocation_target_capital_check"
        ),
        CheckConstraint(
            "current_capital >= 0",
            name="portfolio_allocation_current_capital_check"
        ),
        CheckConstraint(
            "minimum_allocation >= 0",
            name="portfolio_allocation_min_allocation_check"
        ),
        # Ensure either strategy_id OR child_portfolio_id is set, not both
        CheckConstraint(
            "(strategy_id IS NOT NULL AND child_portfolio_id IS NULL) OR "
            "(strategy_id IS NULL AND child_portfolio_id IS NOT NULL)",
            name="portfolio_allocation_target_exclusivity_check"
        ),
        # Performance indexes
        Index("idx_portfolio_allocations_portfolio_id", "portfolio_id"),
        Index("idx_portfolio_allocations_strategy_id", "strategy_id"),
        Index("idx_portfolio_allocations_child_portfolio_id", "child_portfolio_id"),
        Index("idx_portfolio_allocations_drift", "drift_pct"),
        Index("idx_portfolio_allocations_rebalance", "last_rebalanced_at"),
        {"schema": "order_service"},
    )

    # Primary key
    allocation_id = Column(BigInteger, primary_key=True, autoincrement=True)
    
    # Portfolio reference
    portfolio_id = Column(
        Integer,
        nullable=False,
        comment="Portfolio ID containing this allocation"
    )
    
    # Allocation targets (mutually exclusive)
    strategy_id = Column(
        Integer,
        nullable=True,
        comment="Strategy ID for strategy allocation (NULL for sub-portfolio)"
    )
    child_portfolio_id = Column(
        Integer,
        nullable=True,
        comment="Child portfolio ID for hierarchical allocation (NULL for strategy)"
    )
    
    # Target allocation
    target_weight_pct = Column(
        Numeric(5, 2),
        nullable=False,
        comment="Target allocation weight as percentage (0.00-100.00)"
    )
    target_capital = Column(
        Numeric(20, 2),
        nullable=False,
        comment="Target capital allocation amount"
    )
    minimum_allocation = Column(
        Numeric(20, 2),
        nullable=False,
        default=1000.00,
        comment="Minimum allocation amount (position sizing constraint)"
    )
    
    # Current allocation (real-time calculated)
    current_capital = Column(
        Numeric(20, 2),
        nullable=False,
        default=0.00,
        comment="Current actual capital allocation"
    )
    current_weight_pct = Column(
        Numeric(5, 2),
        nullable=False,
        default=0.00,
        comment="Current actual weight percentage"
    )
    
    # Drift tracking
    drift_pct = Column(
        Numeric(5, 2),
        nullable=False,
        default=0.00,
        comment="Drift from target allocation (current - target)"
    )
    
    # Rebalancing history
    last_rebalanced_at = Column(
        DateTime(timezone=True),
        nullable=True,
        comment="Timestamp of last rebalancing action"
    )
    
    # Metadata
    allocation_notes = Column(
        Text,
        nullable=True,
        comment="Notes about allocation rationale or constraints"
    )
    
    # Audit timestamps (automatically managed by database)
    created_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=func.now(),
        comment="Allocation creation timestamp"
    )
    updated_at = Column(
        DateTime(timezone=True),
        nullable=True,
        comment="Last update timestamp"
    )

    def __repr__(self):
        target_type = f"strategy={self.strategy_id}" if self.strategy_id else f"child_portfolio={self.child_portfolio_id}"
        return (
            f"<PortfolioAllocation("
            f"id={self.allocation_id}, "
            f"portfolio={self.portfolio_id}, "
            f"{target_type}, "
            f"target={self.target_weight_pct}%, "
            f"current={self.current_weight_pct}%, "
            f"drift={self.drift_pct}%"
            f")>"
        )

    @property
    def target_weight_decimal(self) -> float:
        """Convert target weight percentage to decimal"""
        return float(self.target_weight_pct) / 100.0

    @property
    def current_weight_decimal(self) -> float:
        """Convert current weight percentage to decimal"""
        return float(self.current_weight_pct) / 100.0

    @property
    def drift_decimal(self) -> float:
        """Convert drift percentage to decimal"""
        return float(self.drift_pct) / 100.0

    @property
    def allocation_type(self) -> str:
        """Get the type of allocation (strategy or child_portfolio)"""
        if self.strategy_id:
            return "strategy"
        elif self.child_portfolio_id:
            return "child_portfolio"
        else:
            return "unknown"

    @property
    def allocation_target_id(self) -> Optional[int]:
        """Get the target ID (strategy_id or child_portfolio_id)"""
        return self.strategy_id or self.child_portfolio_id

    @property
    def is_over_allocated(self) -> bool:
        """Check if current allocation exceeds target"""
        return self.drift_pct > 0

    @property
    def is_under_allocated(self) -> bool:
        """Check if current allocation is below target"""
        return self.drift_pct < 0

    @property
    def needs_rebalancing(self, threshold_pct: float = 5.0) -> bool:
        """Check if allocation drift exceeds rebalancing threshold"""
        return abs(self.drift_pct) >= threshold_pct

    def calculate_drift(self) -> None:
        """Calculate and update drift percentage"""
        self.drift_pct = self.current_weight_pct - self.target_weight_pct
        self.updated_at = datetime.utcnow()

    def update_current_allocation(self, current_capital: float, total_portfolio_capital: float) -> None:
        """
        Update current allocation based on actual capital and total portfolio value
        
        Args:
            current_capital: Current capital allocated to this strategy/sub-portfolio
            total_portfolio_capital: Total capital in the parent portfolio
        """
        self.current_capital = current_capital
        
        if total_portfolio_capital > 0:
            self.current_weight_pct = (current_capital / total_portfolio_capital) * 100.0
        else:
            self.current_weight_pct = 0.0
            
        self.calculate_drift()

    def rebalance_to_target(self, total_portfolio_capital: float) -> float:
        """
        Calculate the capital adjustment needed to reach target allocation
        
        Args:
            total_portfolio_capital: Total capital available in portfolio
            
        Returns:
            Capital adjustment needed (positive = add capital, negative = remove capital)
        """
        target_capital_needed = (self.target_weight_decimal * total_portfolio_capital)
        adjustment = target_capital_needed - float(self.current_capital)
        return adjustment

    def mark_rebalanced(self) -> None:
        """Mark allocation as recently rebalanced"""
        self.last_rebalanced_at = datetime.utcnow()
        self.updated_at = datetime.utcnow()
        # After rebalancing, drift should be minimal
        self.calculate_drift()

    def to_dict(self):
        """Convert allocation to dictionary"""
        return {
            "allocation_id": self.allocation_id,
            "portfolio_id": self.portfolio_id,
            "strategy_id": self.strategy_id,
            "child_portfolio_id": self.child_portfolio_id,
            "allocation_type": self.allocation_type,
            "allocation_target_id": self.allocation_target_id,
            "target_weight_pct": float(self.target_weight_pct) if self.target_weight_pct else None,
            "target_capital": float(self.target_capital) if self.target_capital else None,
            "minimum_allocation": float(self.minimum_allocation) if self.minimum_allocation else None,
            "current_capital": float(self.current_capital) if self.current_capital else None,
            "current_weight_pct": float(self.current_weight_pct) if self.current_weight_pct else None,
            "drift_pct": float(self.drift_pct) if self.drift_pct else None,
            "is_over_allocated": self.is_over_allocated,
            "is_under_allocated": self.is_under_allocated,
            "last_rebalanced_at": self.last_rebalanced_at.isoformat() if self.last_rebalanced_at else None,
            "allocation_notes": self.allocation_notes,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }

    @classmethod
    def create_strategy_allocation(
        cls,
        portfolio_id: int,
        strategy_id: int,
        target_weight_pct: float,
        target_capital: float,
        minimum_allocation: float = 1000.00,
        allocation_notes: Optional[str] = None
    ) -> 'PortfolioAllocation':
        """
        Factory method to create a strategy allocation
        
        Args:
            portfolio_id: Parent portfolio ID
            strategy_id: Target strategy ID
            target_weight_pct: Target allocation weight (0.00-100.00)
            target_capital: Target capital amount
            minimum_allocation: Minimum allocation constraint
            allocation_notes: Optional notes
            
        Returns:
            PortfolioAllocation instance for strategy
        """
        return cls(
            portfolio_id=portfolio_id,
            strategy_id=strategy_id,
            child_portfolio_id=None,
            target_weight_pct=target_weight_pct,
            target_capital=target_capital,
            minimum_allocation=minimum_allocation,
            allocation_notes=allocation_notes,
            created_at=datetime.utcnow()
        )

    @classmethod
    def create_sub_portfolio_allocation(
        cls,
        portfolio_id: int,
        child_portfolio_id: int,
        target_weight_pct: float,
        target_capital: float,
        minimum_allocation: float = 1000.00,
        allocation_notes: Optional[str] = None
    ) -> 'PortfolioAllocation':
        """
        Factory method to create a sub-portfolio allocation
        
        Args:
            portfolio_id: Parent portfolio ID
            child_portfolio_id: Target child portfolio ID
            target_weight_pct: Target allocation weight (0.00-100.00)
            target_capital: Target capital amount
            minimum_allocation: Minimum allocation constraint
            allocation_notes: Optional notes
            
        Returns:
            PortfolioAllocation instance for sub-portfolio
        """
        return cls(
            portfolio_id=portfolio_id,
            strategy_id=None,
            child_portfolio_id=child_portfolio_id,
            target_weight_pct=target_weight_pct,
            target_capital=target_capital,
            minimum_allocation=minimum_allocation,
            allocation_notes=allocation_notes,
            created_at=datetime.utcnow()
        )
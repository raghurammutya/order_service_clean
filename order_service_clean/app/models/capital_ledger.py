"""
Capital Ledger Model - Order-level capital management and tracking

Implements the capital allocation state machine:
RESERVE -> ALLOCATE -> RELEASE/FAIL

Key Features:
- Capital reservation for pending orders
- Capital allocation for executed orders
- Capital release for completed/cancelled orders
- Audit trail with reconciliation support
- Order-level capital tracking (not portfolio-level)
"""

from sqlalchemy import (
    Column, BigInteger, String, Numeric, DateTime, Text, CheckConstraint,
    Index, func
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.sql import text
from datetime import datetime
from typing import Optional

from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class CapitalLedger(Base):
    """
    Capital Ledger for order-level capital management
    
    State Machine:
    - RESERVE: Capital reserved for pending order
    - ALLOCATE: Capital allocated for executed order  
    - RELEASE: Capital released (order completed/cancelled)
    - FAIL: Capital allocation failed
    
    Status Flow:
    - PENDING: Transaction initiated but not committed
    - COMMITTED: Transaction successfully committed
    - FAILED: Transaction failed
    - RECONCILING: Under reconciliation review
    """
    __tablename__ = "capital_ledger"
    __table_args__ = (
        # Check constraints for data integrity
        CheckConstraint(
            "status IN ('PENDING', 'COMMITTED', 'FAILED', 'RECONCILING')",
            name="capital_ledger_status_check"
        ),
        CheckConstraint(
            "transaction_type IN ('RESERVE', 'ALLOCATE', 'RELEASE', 'FAIL')",
            name="capital_ledger_transaction_type_check"
        ),
        # Performance indexes
        Index("idx_capital_ledger_portfolio_id", "portfolio_id"),
        Index("idx_capital_ledger_order_id", "order_id"),
        Index("idx_capital_ledger_status", "status"),
        Index("idx_capital_ledger_created_at", "created_at"),
        Index("idx_capital_ledger_portfolio_created", "portfolio_id", "created_at"),
        {"schema": "order_service"},
    )

    # Primary key
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    
    # References
    portfolio_id = Column(String(255), nullable=False, comment="Portfolio ID for capital allocation")
    strategy_id = Column(String(255), nullable=True, comment="Strategy ID (optional)")
    order_id = Column(String(255), nullable=True, comment="Order ID for order-level tracking")
    
    # Transaction details
    transaction_type = Column(
        String(50), 
        nullable=False,
        comment="RESERVE/ALLOCATE/RELEASE/FAIL"
    )
    status = Column(
        String(50), 
        nullable=False, 
        default="PENDING",
        comment="PENDING/COMMITTED/FAILED/RECONCILING"
    )
    
    # Financial data
    amount = Column(
        Numeric(20, 8), 
        nullable=False, 
        default=0,
        comment="Capital amount (high precision for financial calculations)"
    )
    running_balance = Column(
        Numeric(20, 8), 
        nullable=True,
        comment="Running balance after this transaction"
    )
    
    # Audit and reconciliation
    description = Column(Text, nullable=True, comment="Human-readable description")
    reference_id = Column(String(255), nullable=True, comment="External reference ID")
    transaction_metadata = Column(
        "metadata",  # Column name in database
        JSONB, 
        nullable=True, 
        default=text("'{}'::jsonb"),
        comment="Additional transaction metadata"
    )
    
    # Timestamps
    created_at = Column(
        DateTime(timezone=True), 
        nullable=False, 
        default=func.now(),
        comment="Transaction creation time"
    )
    updated_at = Column(
        DateTime(timezone=True), 
        nullable=True,
        comment="Last update time"
    )
    committed_at = Column(
        DateTime(timezone=True), 
        nullable=True,
        comment="Transaction commit time"
    )
    reconciled_at = Column(
        DateTime(timezone=True), 
        nullable=True,
        comment="Reconciliation completion time"
    )
    reconciliation_notes = Column(
        Text, 
        nullable=True,
        comment="Notes from reconciliation process"
    )

    def __repr__(self):
        return (
            f"<CapitalLedger("
            f"id={self.id}, "
            f"portfolio_id='{self.portfolio_id}', "
            f"order_id='{self.order_id}', "
            f"type='{self.transaction_type}', "
            f"status='{self.status}', "
            f"amount={self.amount}"
            f")>"
        )

    @property
    def is_pending(self) -> bool:
        """Check if transaction is pending"""
        return self.status == "PENDING"
    
    @property
    def is_committed(self) -> bool:
        """Check if transaction is committed"""
        return self.status == "COMMITTED"
    
    @property
    def is_failed(self) -> bool:
        """Check if transaction failed"""
        return self.status == "FAILED"
    
    @property
    def needs_reconciliation(self) -> bool:
        """Check if transaction needs reconciliation"""
        return self.status == "RECONCILING"
    
    def commit(self, committed_at: Optional[datetime] = None) -> None:
        """Mark transaction as committed"""
        self.status = "COMMITTED"
        self.committed_at = committed_at or datetime.utcnow()
        self.updated_at = datetime.utcnow()
    
    def fail(self, reason: Optional[str] = None) -> None:
        """Mark transaction as failed"""
        self.status = "FAILED"
        self.updated_at = datetime.utcnow()
        if reason:
            self.reconciliation_notes = reason
    
    def start_reconciliation(self, notes: Optional[str] = None) -> None:
        """Start reconciliation process"""
        self.status = "RECONCILING"
        self.updated_at = datetime.utcnow()
        if notes:
            self.reconciliation_notes = notes
    
    def complete_reconciliation(self, reconciled_at: Optional[datetime] = None) -> None:
        """Complete reconciliation and commit"""
        self.status = "COMMITTED"
        self.reconciled_at = reconciled_at or datetime.utcnow()
        self.updated_at = datetime.utcnow()
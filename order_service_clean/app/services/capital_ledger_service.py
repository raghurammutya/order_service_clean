"""
Capital Ledger Service - Enterprise Capital Management

Implements comprehensive capital allocation and tracking with state machine
management for order-level capital operations.

Key Features:
- Capital reservation and allocation state management
- Transaction lifecycle (RESERVE ’ ALLOCATE ’ RELEASE/FAIL)
- Reconciliation and audit trail support
- Risk-based capital allocation
- Portfolio-level capital constraints
"""
import logging
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any, Tuple
from decimal import Decimal
from sqlalchemy import select, and_, func, or_, text
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from fastapi import HTTPException

from ..models.capital_ledger import CapitalLedger
from ..models.portfolio_config import PortfolioConfig
from ..database.redis_client import get_redis
import json

logger = logging.getLogger(__name__)


class CapitalLedgerService:
    """
    Enterprise Capital Ledger Service
    
    Manages order-level capital allocation with state machine workflows,
    risk controls, and comprehensive audit trails.
    """

    def __init__(self, db: AsyncSession, user_id: int):
        """
        Initialize capital ledger service.

        Args:
            db: Database session
            user_id: User ID for access control and audit
        """
        self.db = db
        self.user_id = user_id
        self.redis = None  # Lazy initialize

    async def _get_redis(self):
        """Get Redis client for caching"""
        if not self.redis:
            self.redis = await get_redis()
        return self.redis

    # =================================
    # CAPITAL RESERVATION OPERATIONS
    # =================================

    async def reserve_capital(
        self,
        portfolio_id: str,
        amount: Decimal,
        order_id: Optional[str] = None,
        strategy_id: Optional[str] = None,
        description: Optional[str] = None,
        reference_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> CapitalLedger:
        """
        Reserve capital for pending order placement.
        
        Args:
            portfolio_id: Portfolio identifier
            amount: Capital amount to reserve
            order_id: Associated order ID (optional)
            strategy_id: Strategy ID (optional)
            description: Human-readable description
            reference_id: External reference ID
            metadata: Additional transaction metadata
            
        Returns:
            CapitalLedger entry with RESERVE transaction
            
        Raises:
            HTTPException: If insufficient capital or validation fails
        """
        # Validate available capital
        available_capital = await self.get_available_capital(portfolio_id)
        if available_capital < amount:
            raise HTTPException(
                400,
                f"Insufficient available capital. Required: {amount}, Available: {available_capital}"
            )

        # Create reservation entry
        ledger_entry = CapitalLedger(
            portfolio_id=portfolio_id,
            strategy_id=strategy_id,
            order_id=order_id,
            transaction_type="RESERVE",
            status="PENDING",
            amount=amount,
            description=description or f"Capital reservation for order {order_id}",
            reference_id=reference_id,
            transaction_metadata=metadata or {},
            created_at=datetime.utcnow()
        )

        self.db.add(ledger_entry)
        await self.db.commit()
        await self.db.refresh(ledger_entry)

        # Commit the reservation
        ledger_entry.commit()
        await self.db.commit()

        logger.info(
            f"Reserved capital: portfolio={portfolio_id}, amount={amount}, "
            f"order={order_id}, ledger_id={ledger_entry.id}"
        )

        # Invalidate portfolio capital cache
        await self._invalidate_capital_cache(portfolio_id)
        
        return ledger_entry

    async def allocate_capital(
        self,
        portfolio_id: str,
        amount: Decimal,
        order_id: str,
        strategy_id: Optional[str] = None,
        description: Optional[str] = None,
        reference_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> CapitalLedger:
        """
        Allocate capital for executed order.
        
        Args:
            portfolio_id: Portfolio identifier
            amount: Capital amount to allocate
            order_id: Order ID (required for allocation)
            strategy_id: Strategy ID
            description: Transaction description
            reference_id: External reference
            metadata: Additional metadata
            
        Returns:
            CapitalLedger entry with ALLOCATE transaction
        """
        # Create allocation entry
        ledger_entry = CapitalLedger(
            portfolio_id=portfolio_id,
            strategy_id=strategy_id,
            order_id=order_id,
            transaction_type="ALLOCATE",
            status="PENDING",
            amount=amount,
            description=description or f"Capital allocation for executed order {order_id}",
            reference_id=reference_id,
            transaction_metadata=metadata or {},
            created_at=datetime.utcnow()
        )

        self.db.add(ledger_entry)
        await self.db.commit()
        await self.db.refresh(ledger_entry)

        # Commit allocation
        ledger_entry.commit()
        await self.db.commit()

        logger.info(
            f"Allocated capital: portfolio={portfolio_id}, amount={amount}, "
            f"order={order_id}, ledger_id={ledger_entry.id}"
        )

        await self._invalidate_capital_cache(portfolio_id)
        return ledger_entry

    async def release_capital(
        self,
        portfolio_id: str,
        amount: Decimal,
        order_id: Optional[str] = None,
        reason: str = "Order completed",
        reference_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> CapitalLedger:
        """
        Release capital from completed/cancelled orders.
        
        Args:
            portfolio_id: Portfolio identifier
            amount: Capital amount to release
            order_id: Associated order ID
            reason: Reason for capital release
            reference_id: External reference
            metadata: Additional metadata
            
        Returns:
            CapitalLedger entry with RELEASE transaction
        """
        ledger_entry = CapitalLedger(
            portfolio_id=portfolio_id,
            order_id=order_id,
            transaction_type="RELEASE",
            status="PENDING",
            amount=amount,
            description=f"Capital release: {reason}",
            reference_id=reference_id,
            transaction_metadata=metadata or {},
            created_at=datetime.utcnow()
        )

        self.db.add(ledger_entry)
        await self.db.commit()
        await self.db.refresh(ledger_entry)

        # Commit release
        ledger_entry.commit()
        await self.db.commit()

        logger.info(
            f"Released capital: portfolio={portfolio_id}, amount={amount}, "
            f"reason={reason}, ledger_id={ledger_entry.id}"
        )

        await self._invalidate_capital_cache(portfolio_id)
        return ledger_entry

    # =================================
    # CAPITAL CALCULATIONS & QUERIES
    # =================================

    async def get_available_capital(self, portfolio_id: str) -> Decimal:
        """
        Calculate available capital for new allocations.
        
        Args:
            portfolio_id: Portfolio identifier
            
        Returns:
            Available capital amount
        """
        # Try cache first
        redis = await self._get_redis()
        cache_key = f"capital:available:{portfolio_id}"
        
        try:
            cached = await redis.get(cache_key)
            if cached:
                return Decimal(cached)
        except Exception as e:
            logger.warning(f"Redis cache error: {e}")

        # Get portfolio configuration for total capital
        portfolio_config = await self.db.execute(
            select(PortfolioConfig).where(PortfolioConfig.portfolio_id == portfolio_id)
        )
        config = portfolio_config.scalar_one_or_none()
        
        if not config:
            logger.warning(f"No portfolio config found for {portfolio_id}")
            return Decimal('0')

        total_capital = config.total_capital

        # Calculate committed capital (RESERVE + ALLOCATE - RELEASE)
        committed_query = select(
            func.coalesce(
                func.sum(
                    func.case(
                        (CapitalLedger.transaction_type.in_(["RESERVE", "ALLOCATE"]), CapitalLedger.amount),
                        (CapitalLedger.transaction_type == "RELEASE", -CapitalLedger.amount),
                        else_=0
                    )
                ),
                0
            )
        ).where(
            and_(
                CapitalLedger.portfolio_id == portfolio_id,
                CapitalLedger.status == "COMMITTED"
            )
        )

        result = await self.db.execute(committed_query)
        committed_capital = result.scalar() or Decimal('0')
        
        available_capital = total_capital - committed_capital

        # Cache result for 30 seconds
        try:
            await redis.setex(cache_key, 30, str(available_capital))
        except Exception as e:
            logger.warning(f"Redis cache set error: {e}")

        logger.debug(
            f"Capital calculation: portfolio={portfolio_id}, "
            f"total={total_capital}, committed={committed_capital}, "
            f"available={available_capital}"
        )

        return max(available_capital, Decimal('0'))

    async def get_capital_summary(self, portfolio_id: str) -> Dict[str, Any]:
        """
        Get comprehensive capital summary for portfolio.
        
        Args:
            portfolio_id: Portfolio identifier
            
        Returns:
            Dictionary with capital breakdown
        """
        # Get portfolio config
        config_result = await self.db.execute(
            select(PortfolioConfig).where(PortfolioConfig.portfolio_id == portfolio_id)
        )
        config = config_result.scalar_one_or_none()

        if not config:
            return {
                "portfolio_id": portfolio_id,
                "total_capital": 0,
                "error": "Portfolio configuration not found"
            }

        # Get capital breakdown by transaction type
        breakdown_query = select(
            CapitalLedger.transaction_type,
            CapitalLedger.status,
            func.sum(CapitalLedger.amount).label('total_amount'),
            func.count(CapitalLedger.id).label('transaction_count')
        ).where(
            CapitalLedger.portfolio_id == portfolio_id
        ).group_by(
            CapitalLedger.transaction_type,
            CapitalLedger.status
        )

        breakdown_result = await self.db.execute(breakdown_query)
        breakdown_rows = breakdown_result.all()

        # Process breakdown
        reserves = Decimal('0')
        allocations = Decimal('0')
        releases = Decimal('0')
        pending_reserves = Decimal('0')
        pending_allocations = Decimal('0')

        for row in breakdown_rows:
            amount = Decimal(str(row.total_amount))
            
            if row.transaction_type == "RESERVE":
                if row.status == "COMMITTED":
                    reserves += amount
                else:
                    pending_reserves += amount
            elif row.transaction_type == "ALLOCATE":
                if row.status == "COMMITTED":
                    allocations += amount
                else:
                    pending_allocations += amount
            elif row.transaction_type == "RELEASE":
                if row.status == "COMMITTED":
                    releases += amount

        committed_capital = reserves + allocations - releases
        available_capital = config.total_capital - committed_capital

        return {
            "portfolio_id": portfolio_id,
            "total_capital": float(config.total_capital),
            "committed_capital": float(committed_capital),
            "available_capital": float(available_capital),
            "utilization_pct": float((committed_capital / config.total_capital) * 100) if config.total_capital > 0 else 0,
            "breakdown": {
                "reserves": float(reserves),
                "allocations": float(allocations),
                "releases": float(releases),
                "pending_reserves": float(pending_reserves),
                "pending_allocations": float(pending_allocations),
            },
            "risk_limits": {
                "risk_limit_pct": float(config.risk_limit_pct),
                "max_risk_amount": float(config.get_max_risk_amount()),
                "remaining_risk_capacity": float(config.get_max_risk_amount() - committed_capital)
            }
        }

    async def get_ledger_history(
        self,
        portfolio_id: str,
        limit: int = 100,
        offset: int = 0,
        transaction_types: Optional[List[str]] = None,
        status_filter: Optional[List[str]] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> Tuple[List[CapitalLedger], int]:
        """
        Get capital ledger transaction history.
        
        Args:
            portfolio_id: Portfolio identifier
            limit: Maximum results to return
            offset: Offset for pagination
            transaction_types: Filter by transaction types
            status_filter: Filter by status
            start_date: Filter from date
            end_date: Filter to date
            
        Returns:
            Tuple of (transactions list, total count)
        """
        query = select(CapitalLedger).where(CapitalLedger.portfolio_id == portfolio_id)
        count_query = select(func.count(CapitalLedger.id)).where(CapitalLedger.portfolio_id == portfolio_id)

        # Apply filters
        filters = []
        
        if transaction_types:
            filters.append(CapitalLedger.transaction_type.in_(transaction_types))
            
        if status_filter:
            filters.append(CapitalLedger.status.in_(status_filter))
            
        if start_date:
            filters.append(CapitalLedger.created_at >= start_date)
            
        if end_date:
            filters.append(CapitalLedger.created_at <= end_date)

        if filters:
            query = query.where(and_(*filters))
            count_query = count_query.where(and_(*filters))

        # Get total count
        total_result = await self.db.execute(count_query)
        total_count = total_result.scalar()

        # Get paginated results
        query = query.order_by(CapitalLedger.created_at.desc())
        query = query.limit(limit).offset(offset)

        result = await self.db.execute(query)
        transactions = result.scalars().all()

        logger.debug(
            f"Retrieved {len(transactions)} capital transactions for portfolio {portfolio_id}, "
            f"total: {total_count}"
        )

        return list(transactions), total_count

    # =================================
    # RECONCILIATION & AUDIT
    # =================================

    async def start_reconciliation(
        self,
        ledger_id: int,
        notes: Optional[str] = None
    ) -> CapitalLedger:
        """
        Start reconciliation process for capital transaction.
        
        Args:
            ledger_id: Capital ledger entry ID
            notes: Reconciliation notes
            
        Returns:
            Updated CapitalLedger entry
            
        Raises:
            HTTPException: If ledger entry not found
        """
        result = await self.db.execute(
            select(CapitalLedger).where(CapitalLedger.id == ledger_id)
        )
        ledger_entry = result.scalar_one_or_none()

        if not ledger_entry:
            raise HTTPException(404, f"Capital ledger entry {ledger_id} not found")

        ledger_entry.start_reconciliation(notes)
        await self.db.commit()

        logger.info(f"Started reconciliation for capital ledger {ledger_id}")
        return ledger_entry

    async def complete_reconciliation(
        self,
        ledger_id: int,
        reconciled_at: Optional[datetime] = None
    ) -> CapitalLedger:
        """
        Complete reconciliation process.
        
        Args:
            ledger_id: Capital ledger entry ID
            reconciled_at: Reconciliation completion time
            
        Returns:
            Updated CapitalLedger entry
        """
        result = await self.db.execute(
            select(CapitalLedger).where(CapitalLedger.id == ledger_id)
        )
        ledger_entry = result.scalar_one_or_none()

        if not ledger_entry:
            raise HTTPException(404, f"Capital ledger entry {ledger_id} not found")

        ledger_entry.complete_reconciliation(reconciled_at)
        await self.db.commit()

        logger.info(f"Completed reconciliation for capital ledger {ledger_id}")
        return ledger_entry

    async def get_reconciliation_items(
        self,
        portfolio_id: Optional[str] = None,
        limit: int = 100
    ) -> List[CapitalLedger]:
        """
        Get capital transactions requiring reconciliation.
        
        Args:
            portfolio_id: Optional portfolio filter
            limit: Maximum results
            
        Returns:
            List of CapitalLedger entries needing reconciliation
        """
        query = select(CapitalLedger).where(CapitalLedger.status == "RECONCILING")

        if portfolio_id:
            query = query.where(CapitalLedger.portfolio_id == portfolio_id)

        query = query.order_by(CapitalLedger.created_at.asc()).limit(limit)

        result = await self.db.execute(query)
        return list(result.scalars().all())

    # =================================
    # UTILITY METHODS
    # =================================

    async def _invalidate_capital_cache(self, portfolio_id: str):
        """Invalidate capital-related cache entries"""
        try:
            redis = await self._get_redis()
            cache_keys = [
                f"capital:available:{portfolio_id}",
                f"capital:summary:{portfolio_id}"
            ]
            
            for key in cache_keys:
                await redis.delete(key)
                
        except Exception as e:
            logger.warning(f"Cache invalidation error: {e}")

    async def validate_capital_operation(
        self,
        portfolio_id: str,
        amount: Decimal,
        operation_type: str
    ) -> Dict[str, Any]:
        """
        Validate capital operation before execution.
        
        Args:
            portfolio_id: Portfolio identifier
            amount: Operation amount
            operation_type: RESERVE, ALLOCATE, or RELEASE
            
        Returns:
            Validation result dictionary
        """
        validation = {
            "valid": True,
            "warnings": [],
            "errors": []
        }

        if amount <= 0:
            validation["valid"] = False
            validation["errors"].append("Amount must be positive")
            return validation

        if operation_type in ["RESERVE", "ALLOCATE"]:
            available = await self.get_available_capital(portfolio_id)
            
            if available < amount:
                validation["valid"] = False
                validation["errors"].append(
                    f"Insufficient capital. Required: {amount}, Available: {available}"
                )
            elif available - amount < available * Decimal('0.1'):  # 10% buffer warning
                validation["warnings"].append(
                    "Operation will use >90% of available capital"
                )

        return validation
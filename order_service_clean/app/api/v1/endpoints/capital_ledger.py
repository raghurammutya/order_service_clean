"""
Capital Ledger API Endpoints

Enterprise REST API for capital allocation management, state machine operations,
and comprehensive capital tracking with audit trails.

Key Features:
- Capital reservation, allocation, and release operations
- Real-time capital availability queries
- Comprehensive capital summaries and analytics
- Audit trail and reconciliation endpoints
- Risk-based capital validation
"""
import logging
from typing import List, Optional, Dict, Any
from decimal import Decimal
from fastapi import APIRouter, Depends, Query, HTTPException, Path
from pydantic import BaseModel, Field, validator
from datetime import datetime
from sqlalchemy.ext.asyncio import AsyncSession

from ....auth.gateway_auth import get_current_user
from ....database.connection import get_db
from ....services.capital_ledger_service import CapitalLedgerService
from ....utils.user_id import extract_user_id

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/capital-ledger", tags=["Capital Management"])


# ==========================================
# REQUEST/RESPONSE MODELS
# ==========================================

class CapitalReservationRequest(BaseModel):
    """Request to reserve capital for pending order"""
    portfolio_id: str = Field(..., description="Portfolio identifier")
    amount: Decimal = Field(..., gt=0, description="Capital amount to reserve")
    order_id: Optional[str] = Field(None, description="Associated order ID")
    strategy_id: Optional[str] = Field(None, description="Strategy identifier")
    description: Optional[str] = Field(None, max_length=500, description="Human-readable description")
    reference_id: Optional[str] = Field(None, description="External reference ID")
    metadata: Optional[Dict[str, Any]] = Field(None, description="Additional transaction metadata")

    @validator('amount')
    def validate_amount(cls, v):
        if v <= 0:
            raise ValueError('Amount must be positive')
        return v


class CapitalAllocationRequest(BaseModel):
    """Request to allocate capital for executed order"""
    portfolio_id: str = Field(..., description="Portfolio identifier")
    amount: Decimal = Field(..., gt=0, description="Capital amount to allocate")
    order_id: str = Field(..., description="Order ID (required for allocation)")
    strategy_id: Optional[str] = Field(None, description="Strategy identifier")
    description: Optional[str] = Field(None, max_length=500, description="Transaction description")
    reference_id: Optional[str] = Field(None, description="External reference")
    metadata: Optional[Dict[str, Any]] = Field(None, description="Additional metadata")


class CapitalReleaseRequest(BaseModel):
    """Request to release capital from completed/cancelled orders"""
    portfolio_id: str = Field(..., description="Portfolio identifier")
    amount: Decimal = Field(..., gt=0, description="Capital amount to release")
    order_id: Optional[str] = Field(None, description="Associated order ID")
    reason: str = Field(..., max_length=200, description="Reason for capital release")
    reference_id: Optional[str] = Field(None, description="External reference")
    metadata: Optional[Dict[str, Any]] = Field(None, description="Additional metadata")


class ReconciliationRequest(BaseModel):
    """Request to start reconciliation process"""
    notes: Optional[str] = Field(None, max_length=1000, description="Reconciliation notes")


class CapitalLedgerResponse(BaseModel):
    """Capital ledger transaction response"""
    id: int
    portfolio_id: str
    strategy_id: Optional[str]
    order_id: Optional[str]
    transaction_type: str
    status: str
    amount: Decimal
    running_balance: Optional[Decimal]
    description: Optional[str]
    reference_id: Optional[str]
    metadata: Optional[Dict[str, Any]]
    created_at: datetime
    updated_at: Optional[datetime]
    committed_at: Optional[datetime]
    reconciled_at: Optional[datetime]
    reconciliation_notes: Optional[str]

    class Config:
        from_attributes = True


class CapitalSummaryResponse(BaseModel):
    """Comprehensive capital summary response"""
    portfolio_id: str
    total_capital: float
    committed_capital: float
    available_capital: float
    utilization_pct: float
    breakdown: Dict[str, float]
    risk_limits: Dict[str, float]


class CapitalHistoryResponse(BaseModel):
    """Capital transaction history response"""
    transactions: List[CapitalLedgerResponse]
    total_count: int
    pagination: Dict[str, int]


class CapitalValidationResponse(BaseModel):
    """Capital operation validation response"""
    valid: bool
    warnings: List[str]
    errors: List[str]


# ==========================================
# CAPITAL RESERVATION ENDPOINTS
# ==========================================

@router.post("/reserve", response_model=CapitalLedgerResponse)
async def reserve_capital(
    request: CapitalReservationRequest,
    db: AsyncSession = Depends(get_db),
    current_user: dict = Depends(get_current_user)
):
    """
    Reserve capital for pending order placement.
    
    Creates a capital reservation with RESERVE transaction type.
    Validates available capital before reservation.
    """
    user_id = extract_user_id(current_user)
    service = CapitalLedgerService(db, user_id)
    
    try:
        ledger_entry = await service.reserve_capital(
            portfolio_id=request.portfolio_id,
            amount=request.amount,
            order_id=request.order_id,
            strategy_id=request.strategy_id,
            description=request.description,
            reference_id=request.reference_id,
            metadata=request.metadata
        )
        
        logger.info(
            f"Capital reserved: user={user_id}, portfolio={request.portfolio_id}, "
            f"amount={request.amount}, ledger_id={ledger_entry.id}"
        )
        
        return CapitalLedgerResponse.from_orm(ledger_entry)
        
    except HTTPException:
        raise
    except (ConnectionError, TimeoutError, OSError) as e:
        logger.error(f"Capital reservation failed due to database error: {e}")
        raise HTTPException(503, "Capital reservation temporarily unavailable due to database connectivity")
    except ValueError as e:
        logger.error(f"Capital reservation failed due to invalid data: {e}")
        raise HTTPException(400, f"Invalid capital reservation parameters: {str(e)}")
    except Exception as e:
        logger.critical(f"CRITICAL: Unexpected capital reservation failure: {e}", exc_info=True)
        raise HTTPException(500, "Critical error in capital reservation - contact support")


@router.post("/allocate", response_model=CapitalLedgerResponse)
async def allocate_capital(
    request: CapitalAllocationRequest,
    db: AsyncSession = Depends(get_db),
    current_user: dict = Depends(get_current_user)
):
    """
    Allocate capital for executed order.
    
    Creates capital allocation with ALLOCATE transaction type.
    Used when orders are actually executed.
    """
    user_id = extract_user_id(current_user)
    service = CapitalLedgerService(db, user_id)
    
    try:
        ledger_entry = await service.allocate_capital(
            portfolio_id=request.portfolio_id,
            amount=request.amount,
            order_id=request.order_id,
            strategy_id=request.strategy_id,
            description=request.description,
            reference_id=request.reference_id,
            metadata=request.metadata
        )
        
        logger.info(
            f"Capital allocated: user={user_id}, portfolio={request.portfolio_id}, "
            f"amount={request.amount}, order={request.order_id}"
        )
        
        return CapitalLedgerResponse.from_orm(ledger_entry)
        
    except HTTPException:
        raise
    except (ConnectionError, TimeoutError, OSError) as e:
        logger.error(f"Capital allocation failed due to database error: {e}")
        raise HTTPException(503, "Capital allocation temporarily unavailable due to database connectivity")
    except ValueError as e:
        logger.error(f"Capital allocation failed due to invalid data: {e}")
        raise HTTPException(400, f"Invalid capital allocation parameters: {str(e)}")
    except Exception as e:
        logger.critical(f"CRITICAL: Unexpected capital allocation failure: {e}", exc_info=True)
        raise HTTPException(500, "Critical error in capital allocation - contact support")


@router.post("/release", response_model=CapitalLedgerResponse)
async def release_capital(
    request: CapitalReleaseRequest,
    db: AsyncSession = Depends(get_db),
    current_user: dict = Depends(get_current_user)
):
    """
    Release capital from completed/cancelled orders.
    
    Creates capital release with RELEASE transaction type.
    Frees up capital for future allocations.
    """
    user_id = extract_user_id(current_user)
    service = CapitalLedgerService(db, user_id)
    
    try:
        ledger_entry = await service.release_capital(
            portfolio_id=request.portfolio_id,
            amount=request.amount,
            order_id=request.order_id,
            reason=request.reason,
            reference_id=request.reference_id,
            metadata=request.metadata
        )
        
        logger.info(
            f"Capital released: user={user_id}, portfolio={request.portfolio_id}, "
            f"amount={request.amount}, reason={request.reason}"
        )
        
        return CapitalLedgerResponse.from_orm(ledger_entry)
        
    except HTTPException:
        raise
    except (ConnectionError, TimeoutError, OSError) as e:
        logger.error(f"Capital release failed due to database error: {e}")
        raise HTTPException(503, "Capital release temporarily unavailable due to database connectivity")
    except ValueError as e:
        logger.error(f"Capital release failed due to invalid data: {e}")
        raise HTTPException(400, f"Invalid capital release parameters: {str(e)}")
    except Exception as e:
        logger.critical(f"CRITICAL: Unexpected capital release failure: {e}", exc_info=True)
        raise HTTPException(500, "Critical error in capital release - contact support")


# ==========================================
# CAPITAL QUERY ENDPOINTS
# ==========================================

@router.get("/available/{portfolio_id}")
async def get_available_capital(
    portfolio_id: str = Path(..., description="Portfolio identifier"),
    db: AsyncSession = Depends(get_db),
    current_user: dict = Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Get available capital for new allocations.
    
    Returns real-time available capital amount considering
    all committed reservations and allocations.
    """
    user_id = extract_user_id(current_user)
    service = CapitalLedgerService(db, user_id)
    
    try:
        available_capital = await service.get_available_capital(portfolio_id)
        
        return {
            "portfolio_id": portfolio_id,
            "available_capital": float(available_capital),
            "currency": "INR",
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except (ConnectionError, TimeoutError, OSError) as e:
        logger.error(f"Failed to get available capital due to database error: {e}")
        raise HTTPException(503, "Capital information temporarily unavailable due to database connectivity")
    except Exception as e:
        logger.critical(f"CRITICAL: Unexpected error getting available capital: {e}", exc_info=True)
        raise HTTPException(500, "Critical error retrieving capital information - contact support")


@router.get("/summary/{portfolio_id}", response_model=CapitalSummaryResponse)
async def get_capital_summary(
    portfolio_id: str = Path(..., description="Portfolio identifier"),
    db: AsyncSession = Depends(get_db),
    current_user: dict = Depends(get_current_user)
):
    """
    Get comprehensive capital summary for portfolio.
    
    Returns detailed breakdown of capital allocation,
    utilization metrics, and risk limits.
    """
    user_id = extract_user_id(current_user)
    service = CapitalLedgerService(db, user_id)
    
    try:
        summary = await service.get_capital_summary(portfolio_id)
        
        if "error" in summary:
            raise HTTPException(404, summary["error"])
            
        return CapitalSummaryResponse(**summary)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get capital summary: {e}")
        raise HTTPException(500, "Internal server error")


@router.get("/history/{portfolio_id}", response_model=CapitalHistoryResponse)
async def get_capital_history(
    portfolio_id: str = Path(..., description="Portfolio identifier"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum results to return"),
    offset: int = Query(0, ge=0, description="Offset for pagination"),
    transaction_types: Optional[List[str]] = Query(None, description="Filter by transaction types"),
    status_filter: Optional[List[str]] = Query(None, description="Filter by status"),
    start_date: Optional[datetime] = Query(None, description="Filter from date"),
    end_date: Optional[datetime] = Query(None, description="Filter to date"),
    db: AsyncSession = Depends(get_db),
    current_user: dict = Depends(get_current_user)
):
    """
    Get capital ledger transaction history.
    
    Returns paginated list of capital transactions with filtering options.
    """
    user_id = extract_user_id(current_user)
    service = CapitalLedgerService(db, user_id)
    
    try:
        transactions, total_count = await service.get_ledger_history(
            portfolio_id=portfolio_id,
            limit=limit,
            offset=offset,
            transaction_types=transaction_types,
            status_filter=status_filter,
            start_date=start_date,
            end_date=end_date
        )
        
        transaction_responses = [
            CapitalLedgerResponse.from_orm(txn) for txn in transactions
        ]
        
        return CapitalHistoryResponse(
            transactions=transaction_responses,
            total_count=total_count,
            pagination={
                "limit": limit,
                "offset": offset,
                "total": total_count,
                "has_next": offset + limit < total_count
            }
        )
        
    except Exception as e:
        logger.error(f"Failed to get capital history: {e}")
        raise HTTPException(500, "Internal server error")


# ==========================================
# CAPITAL VALIDATION ENDPOINTS
# ==========================================

@router.post("/validate/{portfolio_id}", response_model=CapitalValidationResponse)
async def validate_capital_operation(
    portfolio_id: str = Path(..., description="Portfolio identifier"),
    amount: Decimal = Query(..., description="Operation amount"),
    operation_type: str = Query(..., description="Operation type (RESERVE, ALLOCATE, RELEASE)"),
    db: AsyncSession = Depends(get_db),
    current_user: dict = Depends(get_current_user)
):
    """
    Validate capital operation before execution.
    
    Checks if the operation is feasible and returns warnings/errors.
    """
    user_id = extract_user_id(current_user)
    service = CapitalLedgerService(db, user_id)
    
    if operation_type not in ["RESERVE", "ALLOCATE", "RELEASE"]:
        raise HTTPException(400, "Invalid operation type")
    
    try:
        validation_result = await service.validate_capital_operation(
            portfolio_id=portfolio_id,
            amount=amount,
            operation_type=operation_type
        )
        
        return CapitalValidationResponse(**validation_result)
        
    except Exception as e:
        logger.error(f"Failed to validate capital operation: {e}")
        raise HTTPException(500, "Internal server error")


# ==========================================
# RECONCILIATION ENDPOINTS
# ==========================================

@router.post("/reconciliation/{ledger_id}/start", response_model=CapitalLedgerResponse)
async def start_reconciliation(
    ledger_id: int = Path(..., description="Capital ledger entry ID"),
    request: ReconciliationRequest = ReconciliationRequest(),
    db: AsyncSession = Depends(get_db),
    current_user: dict = Depends(get_current_user)
):
    """
    Start reconciliation process for capital transaction.
    
    Marks transaction for manual reconciliation review.
    """
    user_id = extract_user_id(current_user)
    service = CapitalLedgerService(db, user_id)
    
    try:
        ledger_entry = await service.start_reconciliation(
            ledger_id=ledger_id,
            notes=request.notes
        )
        
        logger.info(f"Started reconciliation for capital ledger {ledger_id}")
        return CapitalLedgerResponse.from_orm(ledger_entry)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to start reconciliation: {e}")
        raise HTTPException(500, "Internal server error")


@router.post("/reconciliation/{ledger_id}/complete", response_model=CapitalLedgerResponse)
async def complete_reconciliation(
    ledger_id: int = Path(..., description="Capital ledger entry ID"),
    reconciled_at: Optional[datetime] = Query(None, description="Reconciliation completion time"),
    db: AsyncSession = Depends(get_db),
    current_user: dict = Depends(get_current_user)
):
    """
    Complete reconciliation process.
    
    Marks reconciliation as completed and commits the transaction.
    """
    user_id = extract_user_id(current_user)
    service = CapitalLedgerService(db, user_id)
    
    try:
        ledger_entry = await service.complete_reconciliation(
            ledger_id=ledger_id,
            reconciled_at=reconciled_at
        )
        
        logger.info(f"Completed reconciliation for capital ledger {ledger_id}")
        return CapitalLedgerResponse.from_orm(ledger_entry)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to complete reconciliation: {e}")
        raise HTTPException(500, "Internal server error")


@router.get("/reconciliation/items", response_model=List[CapitalLedgerResponse])
async def get_reconciliation_items(
    portfolio_id: Optional[str] = Query(None, description="Portfolio filter"),
    limit: int = Query(100, ge=1, le=500, description="Maximum results"),
    db: AsyncSession = Depends(get_db),
    current_user: dict = Depends(get_current_user)
):
    """
    Get capital transactions requiring reconciliation.
    
    Returns list of transactions in RECONCILING status.
    """
    user_id = extract_user_id(current_user)
    service = CapitalLedgerService(db, user_id)
    
    try:
        reconciliation_items = await service.get_reconciliation_items(
            portfolio_id=portfolio_id,
            limit=limit
        )
        
        return [
            CapitalLedgerResponse.from_orm(item) 
            for item in reconciliation_items
        ]
        
    except Exception as e:
        logger.error(f"Failed to get reconciliation items: {e}")
        raise HTTPException(500, "Internal server error")
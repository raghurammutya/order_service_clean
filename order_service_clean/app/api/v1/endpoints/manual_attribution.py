"""
Manual Attribution API Endpoints

Provides REST API endpoints for managing manual attribution workflow.
Handles cases where automatic attribution fails and requires human intervention.
"""

import logging
from typing import List, Optional
from datetime import datetime
from decimal import Decimal
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from pydantic import BaseModel, Field

from ...dependencies.acl import require_permissions
from ....auth.account_context import AuthContext
from ....database import get_session
from ....services.manual_attribution_service import (
    ManualAttributionService,
    AttributionDecision,
    AttributionPriority
)

logger = logging.getLogger(__name__)

router = APIRouter()


class AttributionCaseResponse(BaseModel):
    """Attribution case response model."""
    case_id: str
    trading_account_id: str
    symbol: str
    exit_quantity: str
    exit_price: Optional[str]
    exit_timestamp: datetime
    affected_positions: List[dict]
    suggested_allocation: Optional[dict]
    status: str
    priority: str
    created_at: datetime
    updated_at: datetime
    assigned_to: Optional[str]
    resolution_data: Optional[dict]
    audit_trail: List[dict]


class CreateCaseRequest(BaseModel):
    """Request model for creating attribution case."""
    trading_account_id: str = Field(..., description="Trading account ID where exit occurred")
    symbol: str = Field(..., description="Symbol that was exited")
    exit_quantity: str = Field(..., description="Quantity that was exited")
    exit_price: Optional[str] = Field(None, description="Price at which exit occurred")
    exit_timestamp: datetime = Field(..., description="When the exit occurred")
    affected_positions: List[dict] = Field(..., description="Positions that could be attributed")
    suggested_allocation: Optional[dict] = Field(None, description="Suggested allocation from auto-attribution")
    priority: str = Field("normal", description="Priority level (low/normal/high/urgent)")
    context: Optional[dict] = Field(None, description="Additional context")


class AttributionDecisionRequest(BaseModel):
    """Request model for attribution decision."""
    allocation_decisions: List[dict] = Field(..., description="Manual allocation decisions")
    decision_rationale: str = Field(..., description="Rationale for the decision")


class AssignCaseRequest(BaseModel):
    """Request model for assigning case."""
    assigned_to: str = Field(..., description="User ID to assign case to")


class CaseListResponse(BaseModel):
    """Response model for case listing."""
    cases: List[AttributionCaseResponse]
    total_count: int
    has_more: bool


@router.post("/cases", response_model=dict)
async def create_attribution_case(
    request: CreateCaseRequest,
    auth_context: AuthContext = Depends(require_permissions("manual_attribution:create")),
    db: AsyncSession = Depends(get_session)
):
    """
    Create a new manual attribution case.
    
    Requires: manual_attribution:create permission
    """
    try:
        service = ManualAttributionService(db)
        
        case_id = await service.create_attribution_case(
            trading_account_id=request.trading_account_id,
            symbol=request.symbol,
            exit_quantity=Decimal(request.exit_quantity),
            exit_price=Decimal(request.exit_price) if request.exit_price else None,
            exit_timestamp=request.exit_timestamp,
            affected_positions=request.affected_positions,
            suggested_allocation=request.suggested_allocation,
            priority=AttributionPriority(request.priority),
            context=request.context
        )
        
        return {"case_id": case_id, "status": "created"}
        
    except Exception as e:
        logger.error(f"Failed to create attribution case: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to create case: {str(e)}")


@router.get("/cases/{case_id}", response_model=AttributionCaseResponse)
async def get_attribution_case(
    case_id: str,
    auth_context: AuthContext = Depends(require_permissions("manual_attribution:read")),
    db: AsyncSession = Depends(get_session)
):
    """
    Get attribution case details by ID.
    
    Requires: manual_attribution:read permission
    """
    try:
        service = ManualAttributionService(db)
        case = await service.get_attribution_case(case_id)
        
        if not case:
            raise HTTPException(status_code=404, detail=f"Case {case_id} not found")
            
        return AttributionCaseResponse(
            case_id=case.case_id,
            trading_account_id=case.trading_account_id,
            symbol=case.symbol,
            exit_quantity=str(case.exit_quantity),
            exit_price=str(case.exit_price) if case.exit_price else None,
            exit_timestamp=case.exit_timestamp,
            affected_positions=case.affected_positions,
            suggested_allocation=case.suggested_allocation,
            status=case.status.value,
            priority=case.priority.value,
            created_at=case.created_at,
            updated_at=case.updated_at,
            assigned_to=case.assigned_to,
            resolution_data=case.resolution_data,
            audit_trail=case.audit_trail
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get attribution case {case_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to get case: {str(e)}")


@router.get("/cases", response_model=CaseListResponse)
async def list_attribution_cases(
    trading_account_id: Optional[str] = Query(None, description="Filter by trading account"),
    symbol: Optional[str] = Query(None, description="Filter by symbol"),
    priority: Optional[str] = Query(None, description="Filter by priority"),
    assigned_to: Optional[str] = Query(None, description="Filter by assigned user"),
    status: Optional[str] = Query(None, description="Filter by status"),
    limit: int = Query(50, description="Maximum cases to return", le=100),
    offset: int = Query(0, description="Number of cases to skip", ge=0),
    auth_context: AuthContext = Depends(require_permissions("manual_attribution:read")),
    db: AsyncSession = Depends(get_session)
):
    """
    List attribution cases with filtering.
    
    Requires: manual_attribution:read permission
    """
    try:
        service = ManualAttributionService(db)
        
        # Apply user's access control - can only see cases for their trading accounts
        user_trading_accounts = auth_context.trading_account_ids
        if trading_account_id and trading_account_id not in user_trading_accounts:
            raise HTTPException(status_code=403, detail="Access denied to trading account")
        
        # If no specific account requested, limit to user's accounts
        if not trading_account_id and len(user_trading_accounts) == 1:
            trading_account_id = user_trading_accounts[0]
        elif not trading_account_id and len(user_trading_accounts) > 1:
            # For multi-account users, we'd need to modify the service to handle multiple accounts
            # For now, return empty list
            return CaseListResponse(cases=[], total_count=0, has_more=False)
        
        cases = await service.list_pending_cases(
            trading_account_id=trading_account_id,
            symbol=symbol,
            priority=AttributionPriority(priority) if priority else None,
            assigned_to=assigned_to,
            limit=limit,
            offset=offset
        )
        
        # Convert to response format
        case_responses = []
        for case in cases:
            case_responses.append(AttributionCaseResponse(
                case_id=case.case_id,
                trading_account_id=case.trading_account_id,
                symbol=case.symbol,
                exit_quantity=str(case.exit_quantity),
                exit_price=str(case.exit_price) if case.exit_price else None,
                exit_timestamp=case.exit_timestamp,
                affected_positions=case.affected_positions,
                suggested_allocation=case.suggested_allocation,
                status=case.status.value,
                priority=case.priority.value,
                created_at=case.created_at,
                updated_at=case.updated_at,
                assigned_to=case.assigned_to,
                resolution_data=case.resolution_data,
                audit_trail=case.audit_trail
            ))
        
        # Get actual total count for proper pagination
        count_result = await db.execute(
            text(f"""
                SELECT COUNT(*)
                FROM order_service.manual_attribution_cases
                WHERE {where_clause}
            """),
            params
        )
        total_count = count_result.fetchone()[0]

        return CaseListResponse(
            cases=case_responses,
            total_count=total_count,
            has_more=offset + len(case_responses) < total_count
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to list attribution cases: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to list cases: {str(e)}")


@router.post("/cases/{case_id}/assign", response_model=dict)
async def assign_attribution_case(
    case_id: str,
    request: AssignCaseRequest,
    auth_context: AuthContext = Depends(require_permissions("manual_attribution:assign")),
    db: AsyncSession = Depends(get_session)
):
    """
    Assign an attribution case to a user.
    
    Requires: manual_attribution:assign permission
    """
    try:
        service = ManualAttributionService(db)
        
        success = await service.assign_case(
            case_id=case_id,
            assigned_to=request.assigned_to,
            assigned_by=auth_context.user_id
        )
        
        return {"case_id": case_id, "assigned_to": request.assigned_to, "status": "assigned"}
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to assign case {case_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to assign case: {str(e)}")


@router.post("/cases/{case_id}/resolve", response_model=dict)
async def resolve_attribution_case(
    case_id: str,
    request: AttributionDecisionRequest,
    auth_context: AuthContext = Depends(require_permissions("manual_attribution:resolve")),
    db: AsyncSession = Depends(get_session)
):
    """
    Resolve an attribution case with manual decision.
    
    Requires: manual_attribution:resolve permission
    """
    try:
        service = ManualAttributionService(db)
        
        decision = AttributionDecision(
            case_id=case_id,
            decision_maker=auth_context.user_id,
            allocation_decisions=request.allocation_decisions,
            decision_rationale=request.decision_rationale,
            decision_timestamp=datetime.now()
        )
        
        success = await service.resolve_case(case_id, decision)
        
        return {"case_id": case_id, "status": "resolved", "decision_maker": auth_context.user_id}
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to resolve case {case_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to resolve case: {str(e)}")


@router.post("/cases/{case_id}/apply", response_model=dict)
async def apply_attribution_resolution(
    case_id: str,
    auth_context: AuthContext = Depends(require_permissions("manual_attribution:apply")),
    db: AsyncSession = Depends(get_session)
):
    """
    Apply the resolution to actual positions/trades.
    
    Requires: manual_attribution:apply permission
    """
    try:
        service = ManualAttributionService(db)
        
        success = await service.apply_resolution(
            case_id=case_id,
            applied_by=auth_context.user_id
        )
        
        return {"case_id": case_id, "status": "applied", "applied_by": auth_context.user_id}
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to apply resolution for case {case_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to apply resolution: {str(e)}")


@router.get("/stats", response_model=dict)
async def get_attribution_stats(
    trading_account_id: Optional[str] = Query(None, description="Filter by trading account"),
    auth_context: AuthContext = Depends(require_permissions("manual_attribution:read")),
    db: AsyncSession = Depends(get_session)
):
    """
    Get attribution case statistics.
    
    Requires: manual_attribution:read permission
    """
    try:
        # Apply access control
        user_trading_accounts = auth_context.trading_account_ids
        if trading_account_id and trading_account_id not in user_trading_accounts:
            raise HTTPException(status_code=403, detail="Access denied to trading account")
        
        # Get basic stats from database
        params = {}
        where_clause = ""
        
        if trading_account_id:
            where_clause = "WHERE trading_account_id = :trading_account_id"
            params["trading_account_id"] = trading_account_id
        elif len(user_trading_accounts) == 1:
            where_clause = "WHERE trading_account_id = :trading_account_id"
            params["trading_account_id"] = user_trading_accounts[0]
        
        from sqlalchemy import text
        
        result = await db.execute(
            text(f"""
                SELECT 
                    status,
                    priority,
                    COUNT(*) as count
                FROM order_service.manual_attribution_cases
                {where_clause}
                GROUP BY status, priority
                ORDER BY status, priority
            """),
            params
        )
        
        stats = {
            "by_status": {},
            "by_priority": {},
            "total_cases": 0
        }
        
        for row in result.fetchall():
            status, priority, count = row
            
            if status not in stats["by_status"]:
                stats["by_status"][status] = 0
            stats["by_status"][status] += count
            
            if priority not in stats["by_priority"]:
                stats["by_priority"][priority] = 0
            stats["by_priority"][priority] += count
            
            stats["total_cases"] += count
        
        return stats
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get attribution stats: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to get stats: {str(e)}")
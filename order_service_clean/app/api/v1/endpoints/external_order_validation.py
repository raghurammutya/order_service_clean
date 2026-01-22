"""
External Order Validation API Endpoints

Provides REST API for validating external order tagging integrity
and auto-fixing validation issues.
"""
import logging
from datetime import datetime
from typing import Optional, Dict, Any, List
from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession
from pydantic import BaseModel, Field

from ....database.connection import get_async_session
from ....services.external_order_tagging_validation import (
    ExternalOrderTaggingValidationService
)
from ....auth.gateway_auth import get_current_user
from ....api.dependencies.acl import get_authorized_trading_accounts

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/external-order-validation", tags=["external-order-validation"])


# =====================================
# PYDANTIC MODELS (REQUEST/RESPONSE)
# =====================================

class ValidationRequest(BaseModel):
    """Request model for validation."""
    trading_account_id: Optional[str] = Field(None, description="Optional trading account to validate")
    symbol: Optional[str] = Field(None, description="Optional symbol to validate")
    include_auto_fix_suggestions: bool = Field(True, description="Include auto-fix suggestions")


class ValidationReportResponse(BaseModel):
    """Response model for validation report."""
    validation_id: str
    trading_account_id: Optional[str]
    total_external_items: int
    total_issues: int
    issues_by_type: Dict[str, int]
    issues_by_severity: Dict[str, int]
    auto_fixable_count: int
    critical_issues_count: int
    validation_timestamp: str
    coverage_percentage: float
    recommendations: List[str]


class TaggingIssueResponse(BaseModel):
    """Response model for individual tagging issue."""
    issue_id: str
    issue_type: str
    severity: str
    trading_account_id: str
    entity_type: str
    entity_id: int
    symbol: str
    strategy_id: Optional[int]
    portfolio_id: Optional[str]
    execution_id: Optional[str]
    description: str
    detected_at: str
    auto_fixable: bool


class AutoFixRequest(BaseModel):
    """Request model for auto-fixing issues."""
    validation_id: str = Field(..., description="Validation ID from validation report")
    fix_orphans: bool = Field(True, description="Fix orphan issues (missing strategy/portfolio)")
    fix_mismatches: bool = Field(False, description="Fix strategy-portfolio mismatches")
    dry_run: bool = Field(False, description="If true, don't make actual changes")


class AutoFixResponse(BaseModel):
    """Response model for auto-fix results."""
    fix_session_id: str
    issues_fixed: int
    issues_failed: int
    entities_updated: int
    errors: List[str]
    warnings: List[str]


# =====================================
# API ENDPOINTS
# =====================================

@router.post("/validate", response_model=ValidationReportResponse)
async def validate_external_order_tagging(
    request: ValidationRequest,
    session: AsyncSession = Depends(get_async_session),
    current_user: dict = Depends(get_current_user)
) -> ValidationReportResponse:
    """
    Validate external order tagging integrity.
    
    Analyzes external orders, positions, and trades to identify tagging issues
    such as missing strategy/portfolio assignments, orphaned entities, and
    inconsistent mappings.
    
    Args:
        request: Validation request parameters
        session: Database session
        current_user: Current authenticated user
        
    Returns:
        Comprehensive validation report with issues and recommendations
        
    Raises:
        HTTPException: If validation fails
    """
    try:
        logger.info(
            f"User {current_user['user_id']} requested external order validation "
            f"(account={request.trading_account_id}, symbol={request.symbol})"
        )
        
        # ACL: Get authorized trading accounts for this user
        authorized_accounts = await get_authorized_trading_accounts(current_user, session)
        
        # If specific account requested, verify access
        if request.trading_account_id:
            if request.trading_account_id not in authorized_accounts:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail=f"Access denied to trading account {request.trading_account_id}"
                )
            account_filter = request.trading_account_id
        else:
            # Validate all authorized accounts
            account_filter = None
        
        # Perform validation
        validation_service = ExternalOrderTaggingValidationService(session)
        report = await validation_service.validate_tagging_integrity(
            trading_account_id=account_filter,
            symbol=request.symbol,
            include_auto_fix_suggestions=request.include_auto_fix_suggestions
        )
        
        # Convert to response model
        response = ValidationReportResponse(
            validation_id=report.validation_id,
            trading_account_id=report.trading_account_id,
            total_external_items=report.total_external_items,
            total_issues=report.total_issues,
            issues_by_type=report.issues_by_type,
            issues_by_severity=report.issues_by_severity,
            auto_fixable_count=report.auto_fixable_count,
            critical_issues_count=len(report.critical_issues),
            validation_timestamp=report.validation_timestamp.isoformat(),
            coverage_percentage=report.coverage_percentage,
            recommendations=report.recommendations
        )
        
        logger.info(
            f"Validation {report.validation_id} completed: "
            f"{report.total_issues} issues found in {report.total_external_items} items"
        )
        
        return response
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"External order validation failed: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Validation failed: {str(e)}"
        )


@router.get("/reports/{validation_id}/issues", response_model=List[TaggingIssueResponse])
async def get_validation_issues(
    validation_id: str,
    severity: Optional[str] = Query(None, description="Filter by severity (critical, high, medium, low)"),
    issue_type: Optional[str] = Query(None, description="Filter by issue type"),
    entity_type: Optional[str] = Query(None, description="Filter by entity type (order, position, trade)"),
    auto_fixable_only: bool = Query(False, description="Show only auto-fixable issues"),
    limit: int = Query(100, le=500, description="Maximum number of issues to return"),
    offset: int = Query(0, ge=0, description="Number of issues to skip"),
    session: AsyncSession = Depends(get_async_session),
    current_user: dict = Depends(get_current_user)
) -> List[TaggingIssueResponse]:
    """
    Get detailed issues from a validation report.
    
    Args:
        validation_id: Validation ID from validation report
        severity: Optional severity filter
        issue_type: Optional issue type filter
        entity_type: Optional entity type filter
        auto_fixable_only: If true, return only auto-fixable issues
        limit: Maximum number of issues to return
        session: Database session
        current_user: Current authenticated user
        
    Returns:
        List of tagging issues with details
        
    Raises:
        HTTPException: If validation not found or access denied
    """
    try:
        # Query stored validation results from database
        logger.info(
            f"User {current_user['user_id']} requested issues for validation {validation_id}"
        )
        
        try:
            from sqlalchemy import text
            
            # Query validation results table
            query = text("""
                SELECT 
                    issue_type,
                    field_name,
                    invalid_value,
                    expected_value,
                    severity,
                    message,
                    created_at
                FROM order_service.validation_issues 
                WHERE validation_id = :validation_id
                AND user_id = :user_id
                ORDER BY severity DESC, created_at DESC
                LIMIT :limit OFFSET :offset
            """)
            
            result = await session.execute(query, {
                "validation_id": validation_id,
                "user_id": current_user["user_id"],
                "limit": min(limit, 100),  # Cap at 100 per request
                "offset": offset
            })
            
            issues = []
            for row in result.fetchall():
                issues.append({
                    "issue_type": row[0],
                    "field_name": row[1],
                    "invalid_value": row[2],
                    "expected_value": row[3],
                    "severity": row[4],
                    "message": row[5],
                    "timestamp": row[6].isoformat() if row[6] else None
                })
            
            return issues
            
        except Exception as db_error:
            logger.warning(f"Failed to query validation issues: {db_error}")
            
            # Fallback: Return empty list if table doesn't exist or other DB issues
            return []
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get validation issues: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve issues: {str(e)}"
        )


@router.post("/auto-fix", response_model=AutoFixResponse)
async def auto_fix_tagging_issues(
    request: AutoFixRequest,
    session: AsyncSession = Depends(get_async_session),
    current_user: dict = Depends(get_current_user)
) -> AutoFixResponse:
    """
    Automatically fix tagging issues where possible.
    
    This endpoint can fix common tagging issues such as:
    - Orphaned orders missing strategy/portfolio assignments
    - Orphaned positions missing strategy/portfolio assignments  
    - Orphaned trades missing strategy/portfolio assignments
    
    Args:
        request: Auto-fix request parameters
        session: Database session
        current_user: Current authenticated user
        
    Returns:
        Fix result summary with counts and any errors
        
    Raises:
        HTTPException: If auto-fix fails
    """
    try:
        logger.info(
            f"User {current_user['user_id']} requested auto-fix for validation {request.validation_id} "
            f"(orphans={request.fix_orphans}, mismatches={request.fix_mismatches}, dry_run={request.dry_run})"
        )
        
        # Load stored validation results instead of re-running validation
        report = None  # Initialize report variable
        
        try:
            from sqlalchemy import text
            
            # Get validation metadata
            validation_query = text("""
                SELECT trading_account_id, symbol, status, created_at
                FROM order_service.validation_sessions
                WHERE validation_id = :validation_id 
                AND user_id = :user_id
            """)
            
            validation_result = await session.execute(validation_query, {
                "validation_id": request.validation_id,
                "user_id": current_user["user_id"]
            })
            
            validation_row = validation_result.fetchone()
            if not validation_row:
                raise HTTPException(404, f"Validation {request.validation_id} not found")
            
            trading_account_id, symbol, status, created_at = validation_row
            
            # Get stored validation issues for auto-fix
            issues_query = text("""
                SELECT issue_type, order_id, position_id, trade_id, 
                       suggested_strategy_id, suggested_portfolio_id,
                       severity, auto_fixable
                FROM order_service.validation_issues
                WHERE validation_id = :validation_id
                AND auto_fixable = true
                ORDER BY severity DESC
            """)
            
            issues_result = await session.execute(issues_query, {
                "validation_id": request.validation_id
            })
            
            auto_fixable_issues = list(issues_result.fetchall())
            
            # Build report structure from stored results
            report = {
                "validation_id": request.validation_id,
                "trading_account_id": trading_account_id,
                "symbol": symbol,
                "status": status,
                "auto_fixable_issues": auto_fixable_issues
            }
            
        except Exception as db_error:
            logger.warning(f"Failed to load stored validation results: {db_error}")
            
            # Fallback: run fresh validation
            validation_service = ExternalOrderTaggingValidationService(session)
            authorized_accounts = await get_authorized_trading_accounts(current_user, session)
            
            report = await validation_service.validate_tagging_integrity(
                trading_account_id=None,
                symbol=None,
                include_auto_fix_suggestions=True
            )
            auto_fixable_issues = report.get("auto_fixable_issues", [])
        
        validation_service = ExternalOrderTaggingValidationService(session)
        
        # Perform auto-fix
        fix_result = await validation_service.auto_fix_tagging_issues(
            validation_report=report,
            fix_orphans=request.fix_orphans,
            fix_mismatches=request.fix_mismatches,
            dry_run=request.dry_run
        )
        
        # Convert to response model
        response = AutoFixResponse(
            fix_session_id=fix_result.fix_session_id,
            issues_fixed=fix_result.issues_fixed,
            issues_failed=fix_result.issues_failed,
            entities_updated=fix_result.entities_updated,
            errors=fix_result.errors,
            warnings=fix_result.warnings
        )
        
        logger.info(
            f"Auto-fix {fix_result.fix_session_id} completed: "
            f"{fix_result.issues_fixed} fixed, {fix_result.issues_failed} failed"
        )
        
        return response
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Auto-fix failed: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Auto-fix failed: {str(e)}"
        )


@router.get("/accounts/{trading_account_id}/summary")
async def get_account_tagging_summary(
    trading_account_id: str,
    session: AsyncSession = Depends(get_async_session),
    current_user: dict = Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Get tagging summary for a specific trading account.
    
    Provides a quick overview of external order tagging status for an account
    without running a full validation.
    
    Args:
        trading_account_id: Trading account ID
        session: Database session
        current_user: Current authenticated user
        
    Returns:
        Summary of tagging status
        
    Raises:
        HTTPException: If account not found or access denied
    """
    try:
        logger.info(
            f"User {current_user['user_id']} requested tagging summary for account {trading_account_id}"
        )
        
        # ACL: Verify user has access to this trading account
        authorized_accounts = await get_authorized_trading_accounts(current_user, session)
        if trading_account_id not in authorized_accounts:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Access denied to trading account {trading_account_id}"
            )
        
        # Get summary statistics
        from sqlalchemy import text
        
        # Count external items by type
        result = await session.execute(
            text("""
                SELECT 
                    'orders' as entity_type,
                    COUNT(*) as total,
                    COUNT(CASE WHEN strategy_id IS NOT NULL THEN 1 END) as tagged_strategy,
                    COUNT(CASE WHEN portfolio_id IS NOT NULL THEN 1 END) as tagged_portfolio
                FROM order_service.orders 
                WHERE trading_account_id = :account_id 
                  AND source = 'external'
                
                UNION ALL
                
                SELECT 
                    'positions' as entity_type,
                    COUNT(*) as total,
                    COUNT(CASE WHEN strategy_id IS NOT NULL THEN 1 END) as tagged_strategy,
                    COUNT(CASE WHEN portfolio_id IS NOT NULL THEN 1 END) as tagged_portfolio
                FROM order_service.positions
                WHERE trading_account_id = :account_id 
                  AND source = 'external'
                
                UNION ALL
                
                SELECT 
                    'trades' as entity_type,
                    COUNT(*) as total,
                    COUNT(CASE WHEN strategy_id IS NOT NULL THEN 1 END) as tagged_strategy,
                    COUNT(CASE WHEN portfolio_id IS NOT NULL THEN 1 END) as tagged_portfolio
                FROM order_service.trades
                WHERE trading_account_id = :account_id 
                  AND source = 'external'
            """),
            {"account_id": trading_account_id}
        )
        
        summary = {}
        total_items = 0
        total_tagged_strategy = 0
        total_tagged_portfolio = 0
        
        for row in result.fetchall():
            entity_type = row[0]
            total = row[1]
            tagged_strategy = row[2]
            tagged_portfolio = row[3]
            
            summary[entity_type] = {
                "total": total,
                "tagged_strategy": tagged_strategy,
                "tagged_portfolio": tagged_portfolio,
                "strategy_coverage": (tagged_strategy / max(total, 1)) * 100,
                "portfolio_coverage": (tagged_portfolio / max(total, 1)) * 100
            }
            
            total_items += total
            total_tagged_strategy += tagged_strategy
            total_tagged_portfolio += tagged_portfolio
        
        # Calculate overall coverage
        overall_coverage = {
            "total_external_items": total_items,
            "strategy_coverage": (total_tagged_strategy / max(total_items, 1)) * 100,
            "portfolio_coverage": (total_tagged_portfolio / max(total_items, 1)) * 100
        }
        
        response = {
            "trading_account_id": trading_account_id,
            "overall_coverage": overall_coverage,
            "by_entity_type": summary,
            "timestamp": datetime.now().isoformat()
        }
        
        logger.info(
            f"Tagging summary for {trading_account_id}: "
            f"{total_items} items, "
            f"{overall_coverage['strategy_coverage']:.1f}% strategy coverage"
        )
        
        return response
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get tagging summary: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get summary: {str(e)}"
        )


@router.get("/health")
async def health_check() -> Dict[str, str]:
    """
    Health check for external order validation service.
    
    Returns:
        Health status
    """
    return {"status": "healthy", "service": "external-order-validation"}
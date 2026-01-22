"""
P&L Calculation API Endpoints

Internal API for batch P&L calculation at the execution level.
Used by algo_engine worker to compute P&L metrics.

Authentication: Internal API key (service-to-service)
"""
import logging
from datetime import date
from typing import List, Optional
from fastapi import APIRouter, Depends, Header, HTTPException, status
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from ....database import get_db
from ....services.pnl_calculator import PnLCalculator
from .internal import verify_internal_api_key

logger = logging.getLogger(__name__)

router = APIRouter()


# ==================================================================================
# REQUEST/RESPONSE MODELS
# ==================================================================================

class ExecutionPnLRequest(BaseModel):
    """Request model for execution P&L calculation"""
    execution_id: str = Field(..., description="Execution UUID")
    trading_day: Optional[str] = Field(None, description="Trading day (YYYY-MM-DD), defaults to today")

    class Config:
        json_schema_extra = {
            "example": {
                "execution_id": "aa0e8400-e29b-41d4-a716-446655440000",
                "trading_day": "2025-12-11"
            }
        }


class BatchPnLRequest(BaseModel):
    """Request model for batch P&L calculation (multiple executions)"""
    execution_ids: List[str] = Field(..., description="List of execution UUIDs", min_length=1, max_length=100)
    trading_day: Optional[str] = Field(None, description="Trading day (YYYY-MM-DD), defaults to today")

    class Config:
        json_schema_extra = {
            "example": {
                "execution_ids": [
                    "aa0e8400-e29b-41d4-a716-446655440000",
                    "bb0e8400-e29b-41d4-a716-446655440001"
                ],
                "trading_day": "2025-12-11"
            }
        }


class ExecutionPnLResponse(BaseModel):
    """Response model for execution P&L calculation"""
    execution_id: str
    trading_day: str
    realized_pnl: float
    unrealized_pnl: float
    total_pnl: float
    positions_opened: int = Field(..., description="Positions opened by this execution (entry_execution_id)")
    positions_owned: int = Field(..., description="Positions currently owned by this execution")
    open_positions: int
    closed_positions: int
    positions_transferred_in: int = Field(..., description="Positions received from other executions")
    positions_transferred_out: int = Field(..., description="Positions sent to other executions")
    total_trades: int
    winning_trades: int
    losing_trades: int
    win_rate: float

    class Config:
        json_schema_extra = {
            "example": {
                "execution_id": "aa0e8400-e29b-41d4-a716-446655440000",
                "trading_day": "2025-12-11",
                "realized_pnl": 5000.50,
                "unrealized_pnl": 1500.25,
                "total_pnl": 6500.75,
                "positions_opened": 10,
                "positions_owned": 8,
                "open_positions": 8,
                "closed_positions": 2,
                "positions_transferred_in": 2,
                "positions_transferred_out": 4,
                "total_trades": 24,
                "winning_trades": 15,
                "losing_trades": 5,
                "win_rate": 75.0
            }
        }


class BatchPnLResponse(BaseModel):
    """Response model for batch P&L calculation"""
    trading_day: str
    executions: List[ExecutionPnLResponse]
    total_count: int
    success_count: int
    error_count: int
    errors: Optional[List[dict]] = None

    class Config:
        json_schema_extra = {
            "example": {
                "trading_day": "2025-12-11",
                "executions": [
                    {
                        "execution_id": "aa0e8400-e29b-41d4-a716-446655440000",
                        "trading_day": "2025-12-11",
                        "realized_pnl": 5000.50,
                        "unrealized_pnl": 1500.25,
                        "total_pnl": 6500.75,
                        "positions_opened": 10,
                        "positions_owned": 8,
                        "open_positions": 8,
                        "closed_positions": 2,
                        "positions_transferred_in": 2,
                        "positions_transferred_out": 4,
                        "total_trades": 24,
                        "winning_trades": 15,
                        "losing_trades": 5,
                        "win_rate": 75.0
                    }
                ],
                "total_count": 2,
                "success_count": 2,
                "error_count": 0,
                "errors": None
            }
        }


# ==================================================================================
# ENDPOINTS
# ==================================================================================

@router.post("/internal/pnl/execution", response_model=ExecutionPnLResponse)
async def calculate_execution_pnl(
    request: ExecutionPnLRequest,
    db: AsyncSession = Depends(get_db),
    x_internal_api_key: str = Header(None)
):
    """
    Calculate P&L for a single execution (stateless computation).

    This endpoint computes P&L metrics without storing them.
    Used by algo_engine worker to get real-time P&L data.

    **Authentication:** Requires X-Internal-API-Key header

    **Returns:**
    - `200 OK`: P&L calculation successful
    - `401 Unauthorized`: Missing API key
    - `403 Forbidden`: Invalid API key
    - `500 Internal Server Error`: Calculation failed
    """
    # Verify internal API key
    verify_internal_api_key(x_internal_api_key)

    try:
        # Parse trading day
        trading_day = date.fromisoformat(request.trading_day) if request.trading_day else None

        # Calculate P&L
        pnl_calculator = PnLCalculator(db)
        result = await pnl_calculator.get_execution_pnl_summary(
            execution_id=request.execution_id,
            trading_day=trading_day
        )

        logger.info(
            f"Calculated P&L for execution {request.execution_id}: "
            f"Total={result['total_pnl']:.2f}, Realized={result['realized_pnl']:.2f}, "
            f"Unrealized={result['unrealized_pnl']:.2f}"
        )

        return ExecutionPnLResponse(**result)

    except ValueError as e:
        logger.error(f"Invalid date format: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid date format: {str(e)}"
        )
    except (ConnectionError, TimeoutError, OSError) as e:
        logger.error(f"P&L calculation failed due to database error: {e}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="P&L calculation temporarily unavailable due to database connectivity"
        )
    except ValueError as e:
        logger.error(f"P&L calculation failed due to invalid data: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid parameters for P&L calculation: {str(e)}"
        )
    except Exception as e:
        logger.critical(f"CRITICAL: Unexpected P&L calculation failure: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Critical error in P&L calculation - contact support"
        )


@router.post("/internal/pnl/batch", response_model=BatchPnLResponse)
async def calculate_batch_pnl(
    request: BatchPnLRequest,
    db: AsyncSession = Depends(get_db),
    x_internal_api_key: str = Header(None)
):
    """
    Calculate P&L for multiple executions in a single request (batch operation).

    This endpoint computes P&L metrics for up to 100 executions in one call.
    Used by algo_engine worker to efficiently calculate P&L for all active executions.

    **Hybrid Architecture:**
    - order_service: Stateless computation (this endpoint)
    - algo_engine: Storage in execution_pnl_metrics table

    **Authentication:** Requires X-Internal-API-Key header

    **Performance:**
    - Each execution: ~5-10ms SQL queries
    - Batch of 20 executions: ~100-200ms total
    - Called every 60 seconds by algo_engine worker

    **Returns:**
    - `200 OK`: Batch calculation completed (check error_count for individual failures)
    - `400 Bad Request`: Invalid request (too many executions, invalid date, etc.)
    - `401 Unauthorized`: Missing API key
    - `403 Forbidden`: Invalid API key
    """
    # Verify internal API key
    verify_internal_api_key(x_internal_api_key)

    # Validate batch size
    if len(request.execution_ids) > 100:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Batch size cannot exceed 100 executions"
        )

    try:
        # Parse trading day
        trading_day = date.fromisoformat(request.trading_day) if request.trading_day else None
        trading_day_str = trading_day.isoformat() if trading_day else date.today().isoformat()

        # Calculate P&L for each execution
        pnl_calculator = PnLCalculator(db)
        results = []
        errors = []

        logger.info(f"Starting batch P&L calculation for {len(request.execution_ids)} executions")

        for execution_id in request.execution_ids:
            try:
                result = await pnl_calculator.get_execution_pnl_summary(
                    execution_id=execution_id,
                    trading_day=trading_day
                )
                results.append(ExecutionPnLResponse(**result))
            except (ConnectionError, TimeoutError, OSError) as e:
                logger.error(f"P&L calculation failed for execution {execution_id} due to database error: {e}")
                errors.append({
                    "execution_id": execution_id,
                    "error": "Database connectivity error during P&L calculation"
                })
            except ValueError as e:
                logger.error(f"P&L calculation failed for execution {execution_id} due to invalid data: {e}")
                errors.append({
                    "execution_id": execution_id,
                    "error": f"Invalid data: {str(e)}"
                })
            except Exception as e:
                logger.critical(f"CRITICAL: Unexpected P&L calculation failure for execution {execution_id}: {e}", exc_info=True)
                errors.append({
                    "execution_id": execution_id,
                    "error": "Critical system error - contact support"
                })

        success_count = len(results)
        error_count = len(errors)

        logger.info(
            f"Batch P&L calculation complete: {success_count} succeeded, {error_count} failed"
        )

        return BatchPnLResponse(
            trading_day=trading_day_str,
            executions=results,
            total_count=len(request.execution_ids),
            success_count=success_count,
            error_count=error_count,
            errors=errors if errors else None
        )

    except ValueError as e:
        logger.error(f"Invalid date format: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid date format: {str(e)}"
        )
    except (ConnectionError, TimeoutError, OSError) as e:
        logger.error(f"Batch P&L calculation failed due to database error: {e}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Batch P&L calculation temporarily unavailable due to database connectivity"
        )
    except ValueError as e:
        logger.error(f"Batch P&L calculation failed due to invalid parameters: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid parameters for batch P&L calculation: {str(e)}"
        )
    except Exception as e:
        logger.critical(f"CRITICAL: Unexpected batch P&L calculation failure: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Critical error in batch P&L calculation - contact support"
        )

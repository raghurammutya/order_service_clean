"""
Admin API Endpoints

Administrative functions for order service management.
Requires authentication (and ideally admin role, TBD).
"""
import logging
from typing import List, Dict, Any
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from ....auth import get_current_user
from ....database import get_db
from ....services.reconciliation_service import ReconciliationService
from ....services.kite_account_rate_limiter import get_rate_limiter_manager_sync
from ..schemas import ErrorResponse

logger = logging.getLogger(__name__)

router = APIRouter()


# ==========================================
# MANUAL RECONCILIATION
# ==========================================

@router.post(
    "/admin/reconciliation/run",
    response_model=dict,
    responses={
        401: {"model": ErrorResponse, "description": "Unauthorized"},
        500: {"model": ErrorResponse, "description": "Reconciliation failed"}
    },
    summary="Trigger manual reconciliation",
    description="Manually trigger order reconciliation with broker. Useful for debugging or after broker issues."
)
async def trigger_reconciliation(
    max_age_hours: int = Query(24, ge=1, le=168, description="Max age of orders to reconcile (hours)"),
    batch_size: int = Query(100, ge=1, le=1000, description="Max orders to reconcile"),
    current_user: dict = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Manually trigger order reconciliation.

    This endpoint allows admins to manually run reconciliation outside
    of the automatic 5-minute schedule. Useful for:
    - Debugging order state issues
    - After broker API outages
    - Testing reconciliation logic
    - Immediate drift correction

    **Parameters:**
    - **max_age_hours**: Only reconcile orders created in last N hours (1-168, default 24)
    - **batch_size**: Maximum orders to reconcile (1-1000, default 100)

    **Returns:**
    - **total_checked**: Number of orders checked
    - **drift_count**: Number of orders with drift
    - **corrected**: Number of drifts corrected
    - **errors**: Number of errors encountered
    - **corrections**: List of corrections made
    """
    logger.info(
        f"Manual reconciliation triggered by user {current_user.get('user_id')} "
        f"(max_age={max_age_hours}h, batch_size={batch_size})"
    )

    try:
        reconciliation = ReconciliationService(db)

        result = await reconciliation.reconcile_pending_orders(
            max_age_hours=max_age_hours,
            batch_size=batch_size
        )

        logger.info(
            f"Manual reconciliation complete: "
            f"checked={result.get('total_checked')}, "
            f"drift={result.get('drift_count')}, "
            f"corrected={result.get('corrected')}"
        )

        return result

    except Exception as e:
        logger.error(f"Manual reconciliation failed: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Reconciliation failed: {str(e)}"
        )


@router.post(
    "/admin/reconciliation/order/{order_id}",
    response_model=dict,
    responses={
        401: {"model": ErrorResponse, "description": "Unauthorized"},
        404: {"model": ErrorResponse, "description": "Order not found"},
        400: {"model": ErrorResponse, "description": "Cannot reconcile order"},
        500: {"model": ErrorResponse, "description": "Reconciliation failed"}
    },
    summary="Reconcile a single order",
    description="Manually reconcile a specific order by ID."
)
async def reconcile_single_order(
    order_id: int,
    current_user: dict = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Reconcile a single order by ID.

    Useful for:
    - Investigating specific order issues
    - Quick drift correction for important orders
    - Testing reconciliation logic

    **Returns:**
    - **success**: True if reconciliation succeeded
    - **drift_detected**: True if drift was found
    - **correction**: Details of correction (if drift found)
    - **error**: Error message (if failed)
    """
    logger.info(
        f"Manual order reconciliation triggered by user {current_user.get('user_id')} "
        f"for order {order_id}"
    )

    try:
        reconciliation = ReconciliationService(db)

        result = await reconciliation.reconcile_single_order_by_id(order_id)

        if not result.get("success"):
            raise HTTPException(
                status_code=400,
                detail=result.get("error", "Reconciliation failed")
            )

        logger.info(
            f"Order {order_id} reconciliation complete: "
            f"drift_detected={result.get('drift_detected')}"
        )

        return result

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Order {order_id} reconciliation failed: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Reconciliation failed: {str(e)}"
        )


# ==========================================
# WORKER STATUS
# ==========================================

@router.get(
    "/admin/reconciliation/status",
    response_model=dict,
    summary="Get reconciliation worker status",
    description="Get detailed status of the reconciliation background worker."
)
async def get_reconciliation_worker_status(
    current_user: dict = Depends(get_current_user)
):
    """
    Get reconciliation worker status.

    Returns detailed information about:
    - Worker running state
    - Configuration (interval, max age, batch size)
    - Metrics (total runs, drift corrected, errors)
    - Last run timestamp and result
    - Next scheduled run

    Useful for monitoring and debugging.
    """
    from ....workers.reconciliation_worker import get_reconciliation_worker

    worker = get_reconciliation_worker()
    status = worker.get_status()

    return status


# ==========================================
# SYNC METRICS (Smart Operations)
# ==========================================

@router.get(
    "/admin/sync/metrics",
    response_model=dict,
    summary="Get tiered sync worker metrics",
    description="Get metrics for tiered sync workers including per-tier stats."
)
async def get_sync_metrics(
    current_user: dict = Depends(get_current_user)
):
    """
    Get tiered sync worker metrics.

    Returns per-tier statistics:
    - **HOT**: Active accounts, 30s sync interval
    - **WARM**: Today's activity, 2min sync interval
    - **COLD**: Holdings only, 15min sync interval
    - **DORMANT**: On-demand only, no auto sync

    Each tier shows:
    - sync_count: Number of sync cycles completed
    - accounts_synced: Total accounts synced
    - errors: Number of sync errors
    - last_sync: Timestamp of last sync
    - interval_seconds: Configured sync interval
    """
    from ....workers.sync_workers import get_worker_manager
    from ....config.sync_config import get_all_tier_configs

    metrics = {
        "sync_workers": {},
        "tier_configs": get_all_tier_configs()
    }

    # Get sync worker manager metrics
    worker_manager = get_worker_manager()
    if worker_manager:
        metrics["sync_workers"] = worker_manager.get_metrics()

    return metrics


@router.get(
    "/admin/sync/tier-distribution",
    response_model=dict,
    summary="Get account tier distribution",
    description="Get count of accounts in each sync tier."
)
async def get_tier_distribution(
    current_user: dict = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Get current account tier distribution.

    Shows how many accounts are in each tier:
    - HOT: Accounts with active orders or recent activity
    - WARM: Accounts with open positions or today's activity
    - COLD: Accounts with holdings only
    - DORMANT: Inactive accounts (7+ days)

    Useful for monitoring tier calculation effectiveness.
    """
    from ....services.account_tier_service import AccountTierService

    tier_service = AccountTierService(db)
    summary = await tier_service.get_tier_summary()

    total = sum(summary.values())

    return {
        "tiers": summary,
        "total_accounts": total,
        "distribution": {
            tier: f"{(count / total * 100):.1f}%" if total > 0 else "0%"
            for tier, count in summary.items()
        }
    }


# ==========================================
# KITE RATE LIMIT ADMIN ENDPOINTS
# ==========================================

class AccountNearLimit(BaseModel):
    """Account approaching rate limit."""
    account_id: int
    daily_used: int
    daily_remaining: int
    daily_limit: int


class RateLimitSummaryResponse(BaseModel):
    """Admin summary of rate limit status."""
    total_accounts_cached: int
    total_requests: int
    total_throttled: int
    total_rejected: int
    throttle_rate: float
    rejection_rate: float
    accounts_near_daily_limit: List[AccountNearLimit]
    daily_counter_status: Dict[str, Any]


@router.get(
    "/admin/rate-limits/summary",
    response_model=RateLimitSummaryResponse,
    summary="Get rate limit admin summary",
    description="Get summary of rate limiting across all accounts."
)
async def get_rate_limit_summary(
    near_limit_threshold: int = Query(
        default=100,
        ge=0,
        le=500,
        description="Show accounts with remaining orders below this threshold"
    ),
    current_user: dict = Depends(get_current_user),
):
    """
    Get administrative summary of Kite API rate limiting.

    Useful for monitoring:
    - Total throttled/rejected requests
    - Accounts approaching daily limits
    - Overall rate limiter health

    **Parameters:**
    - **near_limit_threshold**: Show accounts with remaining daily orders below this value (default: 100)

    Requires admin privileges (enforced by api-gateway).
    """
    manager = get_rate_limiter_manager_sync()

    if manager is None:
        raise HTTPException(
            status_code=503,
            detail="Rate limiter not initialized"
        )

    # Get overall stats
    stats = manager.get_all_stats()

    # Get accounts near daily limit
    accounts_near_limit = []
    if manager._daily_counter:
        near_limit_data = await manager._daily_counter.get_accounts_near_limit(
            threshold=near_limit_threshold
        )
        accounts_near_limit = [
            AccountNearLimit(
                account_id=item["account_id"],
                daily_used=item["used"],
                daily_remaining=item["remaining"],
                daily_limit=item["limit"],
            )
            for item in near_limit_data
        ]

    # Get daily counter status
    daily_counter_status = {}
    if manager._daily_counter:
        daily_counter_status = manager._daily_counter.get_stats()

    return RateLimitSummaryResponse(
        total_accounts_cached=stats["total_accounts_cached"],
        total_requests=stats["total_requests"],
        total_throttled=stats["total_throttled"],
        total_rejected=stats["total_rejected"],
        throttle_rate=stats["throttle_rate"],
        rejection_rate=stats["rejection_rate"],
        accounts_near_daily_limit=accounts_near_limit,
        daily_counter_status=daily_counter_status,
    )

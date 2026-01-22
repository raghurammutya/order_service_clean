"""
Strategy P&L Sync Worker

Updates public.strategy table with aggregated P&L from order_service.positions.
Runs every 60 seconds to keep strategy totals in sync with position P&L.
"""
import asyncio
import logging
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)


async def sync_strategy_pnl(db: AsyncSession) -> dict:
    """
    Aggregate execution P&L and update order_service.strategy totals.

    Uses algo_engine.execution_pnl_metrics as the source of truth,
    which is populated by the execution P&L sync worker from order_service.

    Returns:
        dict with sync results
    """
    try:
        from ..clients.strategy_service_client import get_strategy_client
        
        # Get execution-based P&L data via Analytics Service API
        # CRITICAL: algo_engine.* tables don't exist in order_service database
        try:
            from ..clients.analytics_service_client import get_analytics_client
            
            analytics_client = await get_analytics_client()
            
            # This would require implementing a bulk P&L sync endpoint in the analytics service
            # For now, we'll disable this sync and rely on real-time P&L updates
            logger.info("Execution P&L sync disabled - using real-time P&L calculation instead")
            execution_pnl = []
            
        except Exception as e:
            logger.error(f"Analytics Service API failed: {e}")
            execution_pnl = []

        # Prepare bulk P&L updates
        pnl_updates = []
        execution_updated_strategies = set()
        
        for row in execution_pnl.fetchall():
            strategy_id, total_pnl, unrealized_pnl = row
            pnl_updates.append({
                "strategy_id": strategy_id,
                "total_pnl": float(total_pnl or 0),
                "unrealized_pnl": float(unrealized_pnl or 0),
                "source": "execution_metrics"
            })
            execution_updated_strategies.add(strategy_id)

        # Get position-based P&L for all strategies (since execution metrics disabled)
        # CRITICAL: Removed algo_engine checks since tables don't exist in order_service database
        fallback_pnl = await db.execute(text("""
            SELECT
                p.strategy_id,
                SUM(p.total_pnl) as total_pnl,
                SUM(p.unrealized_pnl) as unrealized_pnl
            FROM order_service.positions p
            WHERE p.is_open = true
              AND p.strategy_id IS NOT NULL
            GROUP BY p.strategy_id
        """))

        fallback_strategies = set()
        for row in fallback_pnl.fetchall():
            strategy_id, total_pnl, unrealized_pnl = row
            if strategy_id not in execution_updated_strategies:
                pnl_updates.append({
                    "strategy_id": strategy_id,
                    "total_pnl": float(total_pnl or 0),
                    "unrealized_pnl": float(unrealized_pnl or 0),
                    "source": "position_aggregation"
                })
                fallback_strategies.add(strategy_id)

        # Send bulk P&L updates to Strategy Service (replaces direct public.strategy updates)
        if pnl_updates:
            strategy_client = await get_strategy_client()
            result = await strategy_client.bulk_sync_strategy_pnl(pnl_updates)
            
            execution_metrics_count = len(execution_updated_strategies)
            fallback_count = len(fallback_strategies)

        total_updated = execution_metrics_count + fallback_count
        logger.debug(
            f"Synced P&L for {total_updated} strategies "
            f"({execution_metrics_count} from execution metrics, {fallback_count} from positions)"
        )

        return {
            "strategies_updated": total_updated,
            "execution_metrics_count": execution_metrics_count,
            "fallback_count": fallback_count,
            "strategy_ids": updated_ids + fallback_ids
        }

    except Exception as e:
        logger.error(f"Error syncing strategy P&L: {e}")
        await db.rollback()
        return {"error": str(e)}


class StrategyPnLSyncWorker:
    """Background worker that syncs strategy P&L every 60 seconds."""

    def __init__(self, db_session_factory, interval_seconds: int = 60):
        self.db_session_factory = db_session_factory
        self.interval_seconds = interval_seconds
        self._running = False
        self._task = None

    async def start(self):
        """Start the sync worker."""
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._run_loop())
        logger.info(f"Strategy P&L sync worker started (interval: {self.interval_seconds}s)")

    async def stop(self):
        """Stop the sync worker."""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("Strategy P&L sync worker stopped")

    async def _run_loop(self):
        """Main worker loop."""
        while self._running:
            try:
                async with self.db_session_factory() as db:
                    await sync_strategy_pnl(db)
            except Exception as e:
                logger.error(f"Strategy P&L sync error: {e}")

            await asyncio.sleep(self.interval_seconds)


# Singleton instance
_sync_worker = None


async def start_strategy_pnl_sync(db_session_factory, interval_seconds: int = 60):
    """Start the strategy P&L sync worker."""
    global _sync_worker
    if _sync_worker is None:
        _sync_worker = StrategyPnLSyncWorker(db_session_factory, interval_seconds)
        await _sync_worker.start()


async def stop_strategy_pnl_sync():
    """Stop the strategy P&L sync worker."""
    global _sync_worker
    if _sync_worker:
        await _sync_worker.stop()
        _sync_worker = None
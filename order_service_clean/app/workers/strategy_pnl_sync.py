"""
Strategy P&L Sync Worker

Updates public.strategy table with aggregated P&L from order_service.positions.
Runs every 60 seconds to keep strategy totals in sync with position P&L.
"""
import asyncio
import logging
from datetime import datetime
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
        # Try to aggregate from execution_pnl_metrics first (preferred)
        result = await db.execute(text("""
            UPDATE public.strategy s
            SET
                total_pnl = COALESCE(sub.total_pnl, 0),
                current_m2m = COALESCE(sub.unrealized_pnl, 0),
                updated_at = NOW()
            FROM (
                SELECT
                    e.strategy_id,
                    SUM(m.total_pnl) as total_pnl,
                    SUM(m.unrealized_pnl) as unrealized_pnl
                FROM algo_engine.execution_pnl_metrics m
                JOIN algo_engine.executions e ON m.execution_id = e.id
                WHERE m.metric_date = CURRENT_DATE
                  AND e.deleted_at IS NULL
                  AND e.status NOT IN ('stopped', 'error', 'completed')
                GROUP BY e.strategy_id
            ) sub
            WHERE s.strategy_id = sub.strategy_id
            RETURNING s.strategy_id
        """))

        updated_ids = [row[0] for row in result.fetchall()]
        execution_metrics_count = len(updated_ids)

        # Fallback: Update order_service.strategies that don't have execution metrics yet
        # This handles legacy strategies or newly created strategies
        # Uses direct position aggregation as fallback
        fallback_result = await db.execute(text("""
            UPDATE public.strategy s
            SET
                total_pnl = COALESCE(sub.total_pnl, 0),
                current_m2m = COALESCE(sub.unrealized_pnl, 0),
                updated_at = NOW()
            FROM (
                SELECT
                    p.strategy_id,
                    SUM(p.total_pnl) as total_pnl,
                    SUM(p.unrealized_pnl) as unrealized_pnl
                FROM order_service.positions p
                WHERE p.is_open = true
                  AND NOT EXISTS (
                      SELECT 1
                      FROM algo_engine.executions e
                      JOIN algo_engine.execution_pnl_metrics m ON e.id = m.execution_id
                      WHERE e.strategy_id = p.strategy_id
                        AND m.metric_date = CURRENT_DATE
                  )
                GROUP BY p.strategy_id
            ) sub
            WHERE s.strategy_id = sub.strategy_id
            RETURNING s.strategy_id
        """))

        fallback_ids = [row[0] for row in fallback_result.fetchall()]
        fallback_count = len(fallback_ids)

        await db.commit()

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
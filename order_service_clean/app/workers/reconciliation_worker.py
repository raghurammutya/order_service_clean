"""
Order Reconciliation Background Worker

Periodically reconciles order state between database and broker.
Runs every 5 minutes to detect and correct drift.
"""
import asyncio
import logging
from datetime import datetime
from typing import Optional
from contextlib import asynccontextmanager


from ..database import get_session_maker
from ..services.reconciliation_service import ReconciliationService

logger = logging.getLogger(__name__)


class ReconciliationWorker:
    """
    Background worker that periodically reconciles orders with broker.

    Usage:
        worker = ReconciliationWorker(interval_seconds=300)
        await worker.start()  # Runs in background
        ...
        await worker.stop()   # Graceful shutdown
    """

    def __init__(
        self,
        interval_seconds: int = 300,  # 5 minutes
        max_age_hours: int = 24,      # Only reconcile orders < 24 hours old
        batch_size: int = 100          # Max orders per run
    ):
        """
        Initialize reconciliation worker.

        Args:
            interval_seconds: How often to run reconciliation (default 300 = 5 minutes)
            max_age_hours: Only reconcile orders created in last N hours (default 24)
            batch_size: Maximum orders to reconcile per run (default 100)
        """
        self.interval_seconds = interval_seconds
        self.max_age_hours = max_age_hours
        self.batch_size = batch_size

        self._task: Optional[asyncio.Task] = None
        self._stop_event = asyncio.Event()
        self._running = False

        # Metrics
        self.total_runs = 0
        self.total_drift_corrected = 0
        self.total_errors = 0
        self.last_run_at: Optional[datetime] = None
        self.last_run_result: Optional[dict] = None

    async def start(self):
        """
        Start the background worker.

        This creates an asyncio task that runs the reconciliation
        loop in the background.
        """
        if self._running:
            logger.warning("Reconciliation worker already running")
            return

        logger.info(
            f"Starting reconciliation worker "
            f"(interval={self.interval_seconds}s, max_age={self.max_age_hours}h)"
        )

        self._running = True
        self._stop_event.clear()
        self._task = asyncio.create_task(self._run_loop())

    async def stop(self):
        """
        Stop the background worker gracefully.

        Waits for current reconciliation to complete before stopping.
        """
        if not self._running:
            logger.warning("Reconciliation worker not running")
            return

        logger.info("Stopping reconciliation worker...")

        self._stop_event.set()

        if self._task:
            try:
                # Wait for task to complete (with timeout)
                await asyncio.wait_for(self._task, timeout=30.0)
                logger.info("Reconciliation worker stopped gracefully")
            except asyncio.TimeoutError:
                logger.warning("Reconciliation worker stop timeout, cancelling task")
                self._task.cancel()
                try:
                    await self._task
                except asyncio.CancelledError:
                    logger.info("Reconciliation worker task cancelled")

        self._running = False

    async def _run_loop(self):
        """
        Main reconciliation loop.

        Runs reconciliation every N seconds until stopped.
        """
        logger.info("Reconciliation worker loop started")

        while not self._stop_event.is_set():
            try:
                # Run reconciliation
                await self._run_reconciliation()

                # Wait for next interval (or until stop signal)
                try:
                    await asyncio.wait_for(
                        self._stop_event.wait(),
                        timeout=self.interval_seconds
                    )
                    # If we got here, stop was signaled
                    break
                except asyncio.TimeoutError:
                    # Timeout is normal - continue to next iteration
                    pass

            except Exception as e:
                logger.error(f"Reconciliation loop error: {e}", exc_info=True)
                self.total_errors += 1

                # Wait a bit before retrying on error
                try:
                    await asyncio.wait_for(
                        self._stop_event.wait(),
                        timeout=60  # Wait 1 minute on error
                    )
                    break
                except asyncio.TimeoutError:
                    pass

        logger.info("Reconciliation worker loop stopped")

    async def _run_reconciliation(self):
        """
        Run a single reconciliation cycle.

        Creates a database session, runs reconciliation, and updates metrics.
        """
        run_start = datetime.utcnow()

        logger.info(
            f"Running reconciliation cycle #{self.total_runs + 1} "
            f"(max_age={self.max_age_hours}h, batch_size={self.batch_size})"
        )

        try:
            async with self._get_db_session() as db:
                reconciliation = ReconciliationService(db)

                result = await reconciliation.reconcile_pending_orders(
                    max_age_hours=self.max_age_hours,
                    batch_size=self.batch_size
                )

                # Update metrics
                self.total_runs += 1
                self.total_drift_corrected += result.get("corrected", 0)
                self.total_errors += result.get("errors", 0)
                self.last_run_at = run_start
                self.last_run_result = result

                # Log summary
                duration = (datetime.utcnow() - run_start).total_seconds()

                logger.info(
                    f"Reconciliation cycle #{self.total_runs} complete: "
                    f"checked={result.get('total_checked', 0)}, "
                    f"drift={result.get('drift_count', 0)}, "
                    f"corrected={result.get('corrected', 0)}, "
                    f"errors={result.get('errors', 0)}, "
                    f"duration={duration:.2f}s"
                )

                # Detailed logging if drift detected
                if result.get("drift_count", 0) > 0:
                    logger.warning(
                        f"DRIFT ALERT: {result['drift_count']} orders had status drift. "
                        f"Corrections: {result.get('corrections', [])}"
                    )

        except Exception as e:
            logger.error(f"Reconciliation cycle failed: {e}", exc_info=True)
            self.total_errors += 1
            self.last_run_result = {
                "error": str(e),
                "timestamp": run_start.isoformat()
            }

    @asynccontextmanager
    async def _get_db_session(self):
        """
        Get database session for reconciliation.

        Yields:
            AsyncSession instance
        """
        async with get_session_maker()() as session:
            try:
                yield session
            except Exception as e:
                logger.error(f"Database session error: {e}", exc_info=True)
                await session.rollback()
                raise
            finally:
                await session.close()

    def get_status(self) -> dict:
        """
        Get worker status for monitoring/health checks.

        Returns:
            Dictionary with worker metrics and status
        """
        return {
            "running": self._running,
            "interval_seconds": self.interval_seconds,
            "max_age_hours": self.max_age_hours,
            "batch_size": self.batch_size,
            "total_runs": self.total_runs,
            "total_drift_corrected": self.total_drift_corrected,
            "total_errors": self.total_errors,
            "last_run_at": self.last_run_at.isoformat() if self.last_run_at else None,
            "last_run_result": self.last_run_result,
            "next_run_in_seconds": (
                self.interval_seconds - (
                    datetime.utcnow() - self.last_run_at
                ).total_seconds()
                if self.last_run_at else self.interval_seconds
            ) if self._running else None
        }


# Global worker instance
_worker: Optional[ReconciliationWorker] = None


def get_reconciliation_worker() -> ReconciliationWorker:
    """
    Get the global reconciliation worker instance.

    Returns:
        ReconciliationWorker instance
    """
    global _worker

    if _worker is None:
        _worker = ReconciliationWorker(
            interval_seconds=300,  # 5 minutes
            max_age_hours=24,
            batch_size=100
        )

    return _worker


async def start_reconciliation_worker():
    """
    Start the global reconciliation worker.

    Call this from the application startup lifespan.
    """
    worker = get_reconciliation_worker()
    await worker.start()
    logger.info("Reconciliation worker started")


async def stop_reconciliation_worker():
    """
    Stop the global reconciliation worker.

    Call this from the application shutdown lifespan.
    """
    worker = get_reconciliation_worker()
    await worker.stop()
    logger.info("Reconciliation worker stopped")

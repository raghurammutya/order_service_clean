"""
Background worker for periodic tier recalculation.

Runs every 5 minutes to update account tiers based on activity.
Also handles HOT tier expiration (auto-demote after duration).
"""
import asyncio
import logging
from datetime import datetime, timezone

from ..services.account_tier_service import AccountTierService
from ..database.connection import get_async_session

logger = logging.getLogger(__name__)


class TierWorker:
    """Background worker for tier management."""

    RECALCULATION_INTERVAL = 300  # 5 minutes

    def __init__(self):
        self.is_running = False
        self._task = None
        self._recalculation_count = 0
        self._last_recalculation = None

    async def start(self):
        """Start the tier calculation worker."""
        if self.is_running:
            logger.warning("TierWorker already running")
            return

        self.is_running = True
        self._task = asyncio.create_task(self._run_loop())
        logger.info(f"Tier calculation worker started (interval: {self.RECALCULATION_INTERVAL}s)")

    async def stop(self):
        """Stop the worker."""
        self.is_running = False

        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

        logger.info("Tier calculation worker stopped")

    async def _run_loop(self):
        """Main worker loop."""
        while self.is_running:
            try:
                await self._recalculate_tiers()
                await asyncio.sleep(self.RECALCULATION_INTERVAL)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Tier calculation error: {e}", exc_info=True)
                await asyncio.sleep(60)  # Back off on error

    async def _recalculate_tiers(self):
        """Recalculate all account tiers."""
        try:
            async for session in get_async_session():
                try:
                    tier_service = AccountTierService(session)
                    summary = await tier_service.recalculate_all_tiers()

                    self._recalculation_count += 1
                    self._last_recalculation = datetime.now(timezone.utc)

                    logger.info(
                        f"Tier recalculation #{self._recalculation_count}: "
                        f"HOT={summary.get('hot', 0)}, "
                        f"WARM={summary.get('warm', 0)}, "
                        f"COLD={summary.get('cold', 0)}, "
                        f"DORMANT={summary.get('dormant', 0)}"
                    )
                finally:
                    break  # Only need one session

        except Exception as e:
            logger.error(f"Failed to recalculate tiers: {e}")

    def get_status(self) -> dict:
        """Get worker status."""
        return {
            "running": self.is_running,
            "interval_seconds": self.RECALCULATION_INTERVAL,
            "recalculation_count": self._recalculation_count,
            "last_recalculation": self._last_recalculation.isoformat() if self._last_recalculation else None
        }


# Singleton instance
_tier_worker: TierWorker = None


def get_tier_worker() -> TierWorker:
    """Get or create tier worker singleton."""
    global _tier_worker
    if _tier_worker is None:
        _tier_worker = TierWorker()
    return _tier_worker

"""
Tiered Sync Worker

Manages sync operations based on account tiers.
Each tier has its own sync loop with appropriate frequency.

Tier Frequencies:
- HOT: 30 seconds (real-time backup)
- WARM: 2 minutes
- COLD: 15 minutes
- DORMANT: No automatic sync (on-demand only)

This reduces API calls by ~90% compared to uniform polling.
"""
import asyncio
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional

from ..config.sync_config import get_tier_config
from ..services.account_tier_service import AccountTierService, SyncTier
from ..services.kite_client_multi import get_kite_client_for_account
from ..database.connection import get_async_session

logger = logging.getLogger(__name__)


class TieredSyncWorker:
    """
    Manages tier-based sync operations.

    Architecture:
    - Separate sync loop for each tier (HOT, WARM, COLD)
    - DORMANT tier has no automatic sync
    - Each tier processes accounts in batches
    - Respects Kite API rate limits
    """

    def __init__(self):
        self.is_running = False
        self.tasks: Dict[SyncTier, asyncio.Task] = {}

        # Metrics
        self.sync_counts: Dict[SyncTier, int] = {tier: 0 for tier in SyncTier}
        self.last_sync_times: Dict[SyncTier, Optional[datetime]] = {tier: None for tier in SyncTier}
        self.accounts_synced: Dict[SyncTier, int] = {tier: 0 for tier in SyncTier}
        self.errors: Dict[SyncTier, int] = {tier: 0 for tier in SyncTier}

    async def start(self):
        """Start tiered sync workers."""
        self.is_running = True
        logger.info("=" * 60)
        logger.info("Starting Tiered Sync Workers")
        logger.info("=" * 60)

        # Start a sync loop for each active tier
        for tier in [SyncTier.HOT, SyncTier.WARM, SyncTier.COLD]:
            config = get_tier_config(tier)
            if config.sync_interval_seconds > 0:
                task = asyncio.create_task(self._tier_sync_loop(tier))
                self.tasks[tier] = task
                logger.info(
                    f"✓ {tier.value.upper()} tier worker started "
                    f"(interval: {config.sync_interval_seconds}s, batch: {config.batch_size})"
                )

        # DORMANT tier has no automatic sync
        logger.info("✓ DORMANT tier: on-demand sync only (no background worker)")

        logger.info("=" * 60)

    async def stop(self):
        """Stop all tier workers."""
        self.is_running = False

        for tier, task in self.tasks.items():
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        self.tasks.clear()
        logger.info("Tiered sync workers stopped")

    async def _tier_sync_loop(self, tier: SyncTier):
        """
        Sync loop for a specific tier.

        Continuously syncs accounts in this tier at the configured interval.
        """
        config = get_tier_config(tier)
        logger.info(f"[{tier.value}] Sync loop started (interval: {config.sync_interval_seconds}s)")

        while self.is_running:
            try:
                start_time = datetime.now(timezone.utc)

                # Get accounts in this tier
                async for session in get_async_session():
                    try:
                        tier_service = AccountTierService(session)
                        account_ids = await tier_service.get_accounts_by_tier(tier)

                        if account_ids:
                            logger.debug(f"[{tier.value}] Syncing {len(account_ids)} accounts")
                            synced, errors = await self._sync_accounts_batch(
                                session, tier, account_ids, config.batch_size
                            )
                            self.accounts_synced[tier] += synced
                            self.errors[tier] += errors

                        self.sync_counts[tier] += 1
                        self.last_sync_times[tier] = datetime.now(timezone.utc)

                    finally:
                        break  # Only need one session

                # Calculate sleep time (account for sync duration)
                elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
                sleep_time = max(0, config.sync_interval_seconds - elapsed)

                if sleep_time > 0:
                    await asyncio.sleep(sleep_time)

            except asyncio.CancelledError:
                logger.info(f"[{tier.value}] Sync loop cancelled")
                break
            except Exception as e:
                logger.error(f"[{tier.value}] Sync loop error: {e}", exc_info=True)
                await asyncio.sleep(30)  # Back off on error

    async def _sync_accounts_batch(
        self,
        session,
        tier: SyncTier,
        account_ids: List[int],
        batch_size: int
    ) -> tuple:
        """
        Sync accounts in batches.

        Processes accounts in parallel batches while respecting rate limits.

        Args:
            session: Database session
            tier: Current tier
            account_ids: List of account IDs to sync
            batch_size: Number of accounts per batch

        Returns:
            Tuple of (synced_count, error_count)
        """
        total = len(account_ids)
        synced = 0
        errors = 0

        for i in range(0, total, batch_size):
            if not self.is_running:
                break

            batch = account_ids[i:i + batch_size]

            # Sync batch in parallel
            tasks = [self._sync_single_account(session, acc_id, tier) for acc_id in batch]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            for result in results:
                if isinstance(result, Exception):
                    errors += 1
                elif result:
                    synced += 1
                else:
                    errors += 1

            # Rate limit pause between batches (100ms = ~10 batches/sec)
            await asyncio.sleep(0.1)

        if total > 0:
            logger.info(f"[{tier.value}] Batch sync complete: {synced}/{total} success, {errors} errors")

        return synced, errors

    async def _sync_single_account(self, session, account_id: int, tier: SyncTier) -> bool:
        """
        Sync a single account.

        What gets synced depends on the tier:
        - HOT: orders, positions (already covered by WebSocket mostly)
        - WARM: orders, positions
        - COLD: positions only (minimal sync)

        Args:
            session: Database session
            account_id: Trading account ID
            tier: Account tier

        Returns:
            True if sync succeeded, False otherwise
        """
        try:
            kite_client = get_kite_client_for_account(account_id)

            if tier in [SyncTier.HOT, SyncTier.WARM]:
                # Full sync for active tiers
                orders = await kite_client.get_orders()
                positions = await kite_client.get_positions()

                # Log sync
                logger.debug(
                    f"[{tier.value}] Synced account {account_id}: "
                    f"{len(orders)} orders, {len(positions.get('net', []))} positions"
                )

            elif tier == SyncTier.COLD:
                # Minimal sync for cold tier - just positions
                positions = await kite_client.get_positions()

                logger.debug(
                    f"[{tier.value}] Synced account {account_id}: "
                    f"{len(positions.get('net', []))} positions (minimal sync)"
                )

            return True

        except Exception as e:
            logger.error(f"[{tier.value}] Sync failed for account {account_id}: {e}")
            return False

    def get_metrics(self) -> dict:
        """Get sync worker metrics."""
        return {
            "is_running": self.is_running,
            "tiers": {
                tier.value: {
                    "sync_count": self.sync_counts[tier],
                    "last_sync": self.last_sync_times[tier].isoformat() if self.last_sync_times[tier] else None,
                    "accounts_synced": self.accounts_synced[tier],
                    "errors": self.errors[tier],
                    "interval_seconds": get_tier_config(tier).sync_interval_seconds
                }
                for tier in SyncTier
            }
        }


# Singleton instance
_tiered_sync_worker: Optional[TieredSyncWorker] = None


def get_tiered_sync_worker() -> TieredSyncWorker:
    """Get or create tiered sync worker singleton."""
    global _tiered_sync_worker
    if _tiered_sync_worker is None:
        _tiered_sync_worker = TieredSyncWorker()
    return _tiered_sync_worker

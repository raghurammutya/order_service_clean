"""
Account Tier Management Service

Calculates and maintains sync tiers for trading accounts based on activity.

Tiers:
- HOT: Active orders or recent activity (< 5 min) -> Real-time + 30s backup
- WARM: Open positions or activity today -> 2 min polling
- COLD: Holdings only, no intraday -> 15 min polling
- DORMANT: No activity 7+ days -> On-demand only

This reduces API calls by ~90%:
- Before: 5000 accounts x 2/min = 10,000/min
- After: ~1,100/min (HOT:400 + WARM:500 + COLD:200)
"""
import logging
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Optional, List, Dict, Any
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)


class SyncTier(str, Enum):
    """Account sync tiers with different polling frequencies."""
    HOT = "hot"       # Real-time + 30s backup polling
    WARM = "warm"     # 2 minute polling
    COLD = "cold"     # 15 minute polling
    DORMANT = "dormant"  # On-demand only


# Tier thresholds
HOT_ACTIVITY_MINUTES = 5       # Activity in last 5 min = HOT
WARM_ACTIVITY_HOURS = 24       # Activity today = WARM
DORMANT_DAYS = 7               # No activity for 7 days = DORMANT


class AccountTierService:
    """Manages account sync tiers based on activity patterns."""

    def __init__(self, db: AsyncSession):
        """Initialize tier service.

        Args:
            db: AsyncSession for database operations
        """
        self.db = db

    async def calculate_tier(self, trading_account_id: int) -> SyncTier:
        """Calculate sync tier for an account based on activity.

        Priority:
        1. Active orders -> HOT
        2. Recent activity (< 5 min) -> HOT
        3. Open positions -> WARM
        4. Activity today -> WARM
        5. No activity 7+ days -> DORMANT
        6. Default -> COLD

        Args:
            trading_account_id: The trading account ID

        Returns:
            Calculated SyncTier
        """
        now = datetime.now(timezone.utc)

        # Check for active orders
        has_active_orders = await self._has_active_orders(trading_account_id)
        if has_active_orders:
            logger.debug(f"Account {trading_account_id}: HOT (active orders)")
            return SyncTier.HOT

        # Check for recent activity
        recent_cutoff = now - timedelta(minutes=HOT_ACTIVITY_MINUTES)
        recent_activity = await self._get_last_activity(trading_account_id)
        if recent_activity and recent_activity > recent_cutoff:
            logger.debug(f"Account {trading_account_id}: HOT (recent activity)")
            return SyncTier.HOT

        # Check for open positions
        has_positions = await self._has_open_positions(trading_account_id)
        if has_positions:
            logger.debug(f"Account {trading_account_id}: WARM (open positions)")
            return SyncTier.WARM

        # Check for activity today
        today_cutoff = now - timedelta(hours=WARM_ACTIVITY_HOURS)
        if recent_activity and recent_activity > today_cutoff:
            logger.debug(f"Account {trading_account_id}: WARM (activity today)")
            return SyncTier.WARM

        # Check for dormant (no activity 7+ days)
        dormant_cutoff = now - timedelta(days=DORMANT_DAYS)
        if not recent_activity or recent_activity < dormant_cutoff:
            logger.debug(f"Account {trading_account_id}: DORMANT (no recent activity)")
            return SyncTier.DORMANT

        # Default to COLD
        logger.debug(f"Account {trading_account_id}: COLD (default)")
        return SyncTier.COLD

    async def update_tier(
        self,
        trading_account_id: int,
        force_tier: Optional[SyncTier] = None
    ) -> SyncTier:
        """Update the sync tier for an account.

        Args:
            trading_account_id: Account to update
            force_tier: Optional tier to force (e.g., for hard refresh)

        Returns:
            The new tier
        """
        if force_tier:
            tier = force_tier
        else:
            tier = await self.calculate_tier(trading_account_id)

        from ..clients.account_service_client import get_account_client
        
        # Update tier via Account Service API (replaces direct public.kite_accounts access)
        try:
            account_client = await get_account_client()
            success = await account_client.update_account_tier(
                trading_account_id=trading_account_id,
                sync_tier=tier.value
            )
            
            if success:
                logger.info(f"Account {trading_account_id} tier updated to {tier.value}")
            else:
                logger.error(f"Failed to update tier for account {trading_account_id}")
                
        except Exception as e:
            logger.error(f"Account Service API failed: {e}, skipping tier update")
            # In production, we might want to queue this for retry
            return
        return tier

    async def promote_to_hot(
        self,
        trading_account_id: int,
        reason: str,
        duration_minutes: int = 5
    ):
        """Temporarily promote account to HOT tier.

        Used for:
        - User hard refresh
        - Order placement
        - Position change detection

        Args:
            trading_account_id: Account to promote
            reason: Reason for promotion (for logging)
            duration_minutes: How long to stay HOT
        """
        from ..clients.account_service_client import get_account_client
        
        # Promote to HOT tier via Account Service API (replaces direct public.kite_accounts access)
        try:
            account_client = await get_account_client()
            success = await account_client.promote_account_to_hot_tier(
                trading_account_id=str(trading_account_id),
                duration_minutes=duration_minutes
            )
            
            if success:
                expires_at = datetime.now(timezone.utc) + timedelta(minutes=duration_minutes)
                logger.info(
                    f"Account {trading_account_id} promoted to HOT: {reason} "
                    f"(expires: {expires_at.isoformat()})"
                )
            else:
                logger.error(f"Failed to promote account {trading_account_id} to HOT tier")
                
        except Exception as e:
            logger.error(f"Account Service API failed: {e}, skipping HOT promotion")
            return

    async def get_accounts_by_tier(self, tier: SyncTier) -> List[int]:
        """Get all trading account IDs for a specific tier.

        Args:
            tier: The tier to filter by

        Returns:
            List of trading_account_id values
        """
        from ..clients.account_service_client import get_account_client
        
        # Get accounts by tier via Account Service API (replaces direct public.kite_accounts access)
        try:
            account_client = await get_account_client()
            accounts = await account_client.get_accounts_by_tier(tier.value)
            return [int(account["trading_account_id"]) for account in accounts]
        except Exception as e:
            logger.error(f"Account Service API failed: {e}, returning empty list")
            return []

    async def get_tier_summary(self) -> Dict[str, int]:
        """Get count of accounts per tier.

        Returns:
            Dict mapping tier name to count
        """
        from ..clients.account_service_client import get_account_client
        
        # Get tier summary via Account Service API (replaces direct public.kite_accounts access)
        try:
            account_client = await get_account_client()
            stats = await account_client.get_account_tier_stats()
            return stats.get("tier_counts", {})
        except Exception as e:
            logger.error(f"Account Service API failed: {e}, returning empty summary")
            return {}

    async def recalculate_all_tiers(self) -> Dict[str, int]:
        """Recalculate tiers for all active accounts.

        Run periodically (e.g., every 5 minutes).

        Returns:
            Dict with tier counts after recalculation
        """
        # First, demote any expired HOT promotions
        await self._demote_expired_hot_accounts()

        # Get all active trading accounts via Account Service API
        # CRITICAL: public.kite_accounts table doesn't exist - use API instead
        from ..clients.account_service_client import get_account_client
        
        try:
            account_client = await get_account_client()
            all_accounts = await account_client.get_accounts_by_tier("ALL")  # Get all accounts
            # Filter for active accounts
            account_ids = [
                int(account["trading_account_id"]) for account in all_accounts 
                if account.get("status") == "ACTIVE" and account.get("is_active", True)
            ]
        except Exception as e:
            logger.error(f"Account Service API failed: {e}, cannot recalculate tiers")
            return {tier.value: 0 for tier in SyncTier}

        tier_counts = {tier.value: 0 for tier in SyncTier}

        for account_id in account_ids:
            try:
                tier = await self.update_tier(account_id)
                tier_counts[tier.value] += 1
            except Exception as e:
                logger.error(f"Failed to update tier for account {account_id}: {e}")

        logger.info(f"Tier recalculation complete: {tier_counts}")
        return tier_counts

    async def _demote_expired_hot_accounts(self):
        """Demote accounts whose HOT tier promotion has expired."""
        # Use Account Service API to handle HOT tier expiration
        # CRITICAL: public.kite_accounts table doesn't exist - use API instead
        from ..clients.account_service_client import get_account_client
        
        try:
            account_client = await get_account_client()
            
            # Get accounts that need demotion via API
            hot_accounts = await account_client.get_accounts_by_tier("HOT")
            
            demoted_count = 0
            now = datetime.now(timezone.utc)
            
            for account in hot_accounts:
                # Check if promotion has expired
                expires_at_str = account.get("hot_tier_expires_at")
                if expires_at_str:
                    try:
                        expires_at = datetime.fromisoformat(expires_at_str.replace('Z', '+00:00'))
                        if expires_at < now:
                            # Demote to WARM tier
                            success = await account_client.update_account_tier(
                                trading_account_id=account["trading_account_id"],
                                sync_tier="WARM"
                            )
                            if success:
                                demoted_count += 1
                    except Exception as e:
                        logger.warning(f"Failed to parse expiry time for account {account.get('trading_account_id')}: {e}")
            
            if demoted_count > 0:
                logger.info(f"Demoted {demoted_count} accounts from HOT (promotion expired)")
                
        except Exception as e:
            logger.error(f"Account Service API failed during HOT demotion: {e}")

    # Private helper methods

    async def _has_active_orders(self, trading_account_id: int) -> bool:
        """Check if account has active (non-terminal) orders."""
        result = await self.db.execute(
            text("""
                SELECT EXISTS(
                    SELECT 1 FROM order_service.orders
                    WHERE trading_account_id = :trading_account_id
                      AND status IN ('PENDING', 'SUBMITTED', 'OPEN', 'TRIGGER_PENDING')
                )
            """),
            {"trading_account_id": trading_account_id}
        )
        return result.scalar()

    async def _has_open_positions(self, trading_account_id: int) -> bool:
        """Check if account has open positions."""
        result = await self.db.execute(
            text("""
                SELECT EXISTS(
                    SELECT 1 FROM order_service.positions
                    WHERE trading_account_id = :trading_account_id
                      AND quantity != 0
                )
            """),
            {"trading_account_id": trading_account_id}
        )
        return result.scalar()

    async def _get_last_activity(self, trading_account_id: int) -> Optional[datetime]:
        """Get timestamp of last order activity."""
        result = await self.db.execute(
            text("""
                SELECT MAX(updated_at) FROM order_service.orders
                WHERE trading_account_id = :trading_account_id
            """),
            {"trading_account_id": trading_account_id}
        )
        return result.scalar()


async def get_account_tier_info(db: AsyncSession, trading_account_id: int) -> Dict[str, Any]:
    """Get detailed tier info for an account.

    Args:
        db: Database session
        trading_account_id: Account to query

    Returns:
        Dict with tier info
    """
    tier_service = AccountTierService(db)
    current_tier = await tier_service.calculate_tier(trading_account_id)

    # Get additional info
    has_orders = await tier_service._has_active_orders(trading_account_id)
    has_positions = await tier_service._has_open_positions(trading_account_id)
    last_activity = await tier_service._get_last_activity(trading_account_id)

    tier_descriptions = {
        SyncTier.HOT: "Real-time + 30s backup sync",
        SyncTier.WARM: "2 minute polling",
        SyncTier.COLD: "15 minute polling",
        SyncTier.DORMANT: "On-demand only"
    }

    return {
        "trading_account_id": trading_account_id,
        "tier": current_tier.value,
        "description": tier_descriptions[current_tier],
        "has_active_orders": has_orders,
        "has_open_positions": has_positions,
        "last_activity": last_activity.isoformat() if last_activity else None
    }

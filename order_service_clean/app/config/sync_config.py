"""
Tiered Sync Configuration

Defines sync frequencies and batch sizes for each account tier.
All values should come from config_service in production.

API Call Reduction:
- Before: 5000 accounts x 2/min = 10,000/min
- After: ~1,100/min (90% reduction)

| Tier    | Accounts | Frequency | Calls/min |
|---------|----------|-----------|-----------|
| HOT     | 200      | 2/min     | 400       |
| WARM    | 1000     | 0.5/min   | 500       |
| COLD    | 3000     | 0.067/min | 200       |
| DORMANT | 800      | 0/min     | 0         |
| Total   | 5000     | -         | 1,100/min |
"""
import os
from dataclasses import dataclass
from typing import Dict
from ..services.account_tier_service import SyncTier


@dataclass
class TierSyncConfig:
    """Configuration for a single tier."""
    sync_interval_seconds: int
    batch_size: int
    enable_websocket: bool
    description: str


# Default tier configurations
# Can be overridden via environment variables
TIER_CONFIGS: Dict[SyncTier, TierSyncConfig] = {
    SyncTier.HOT: TierSyncConfig(
        sync_interval_seconds=int(os.getenv("SYNC_INTERVAL_HOT", "30")),
        batch_size=int(os.getenv("SYNC_BATCH_SIZE_HOT", "50")),
        enable_websocket=True,
        description="Active orders/positions - real-time + 30s backup"
    ),
    SyncTier.WARM: TierSyncConfig(
        sync_interval_seconds=int(os.getenv("SYNC_INTERVAL_WARM", "120")),  # 2 minutes
        batch_size=int(os.getenv("SYNC_BATCH_SIZE_WARM", "100")),
        enable_websocket=False,
        description="Today's activity - 2 minute polling"
    ),
    SyncTier.COLD: TierSyncConfig(
        sync_interval_seconds=int(os.getenv("SYNC_INTERVAL_COLD", "900")),  # 15 minutes
        batch_size=int(os.getenv("SYNC_BATCH_SIZE_COLD", "200")),
        enable_websocket=False,
        description="Holdings only - 15 minute polling"
    ),
    SyncTier.DORMANT: TierSyncConfig(
        sync_interval_seconds=0,  # No automatic sync
        batch_size=0,
        enable_websocket=False,
        description="Inactive - on-demand only"
    )
}


def get_tier_config(tier: SyncTier) -> TierSyncConfig:
    """Get configuration for a tier.

    Args:
        tier: The sync tier

    Returns:
        TierSyncConfig for the tier
    """
    return TIER_CONFIGS.get(tier, TIER_CONFIGS[SyncTier.COLD])


def get_all_tier_configs() -> Dict[str, dict]:
    """Get all tier configurations as dict for API responses.

    Returns:
        Dict with tier configs
    """
    return {
        tier.value: {
            "sync_interval_seconds": config.sync_interval_seconds,
            "batch_size": config.batch_size,
            "enable_websocket": config.enable_websocket,
            "description": config.description
        }
        for tier, config in TIER_CONFIGS.items()
    }

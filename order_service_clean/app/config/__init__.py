"""Configuration module for order_service."""
from .settings import settings
from .sync_config import TIER_CONFIGS, get_tier_config, get_all_tier_configs

__all__ = [
    "settings",
    "TIER_CONFIGS",
    "get_tier_config",
    "get_all_tier_configs"
]

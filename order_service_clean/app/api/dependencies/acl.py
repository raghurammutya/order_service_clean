"""
ACL Dependencies for FastAPI Endpoints

Provides dependency functions to check ACL permissions for trading resources.
"""
from typing import Optional, List, Literal
from fastapi import Depends, HTTPException, Header, Request

from app.auth.dependencies import get_current_user
from app.models.user import User
from app.utils.acl_helpers import ACLHelper

# Import ACL client
import sys
sys.path.append('/mnt/stocksblitz-data/Quantagro/tradingview-viz/common')
from acl_client.client import ACLClient


async def get_trading_account_id_validated(
    request: Request,
    x_trading_account_id: Optional[int] = Header(None, alias="X-Trading-Account-ID"),
    current_user: User = Depends(get_current_user),
    min_permission: Literal["view", "trade", "admin", "delete"] = "view"
) -> Optional[int]:
    """
    Get and validate trading account ID from header with ACL check.

    Args:
        x_trading_account_id: Trading account ID from header
        current_user: Current authenticated user
        min_permission: Minimum required permission level

    Returns:
        Validated trading_account_id or None for "All Accounts" mode

    Raises:
        HTTPException: If user doesn't have required permission
    """
    if x_trading_account_id is None:
        # "All Accounts" mode - will be handled by endpoint
        return None

    # Check ACL permission
    acl_client = ACLClient(base_url="http://localhost:8011")
    has_permission = await acl_client.check_permission(
        user_id=current_user.id,
        resource_type="trading_account",
        resource_id=x_trading_account_id,
        action=min_permission
    )

    if not has_permission:
        raise HTTPException(
            status_code=403,
            detail=f"You do not have {min_permission} permission for trading account {x_trading_account_id}"
        )

    return x_trading_account_id


async def require_trading_account_permission(
    trading_account_id: int,
    user_id: int,
    action: str
) -> bool:
    """
    Check if user has specific permission on trading account.

    Args:
        trading_account_id: Trading account ID
        user_id: User ID
        action: Required action (view, trade, admin, delete)

    Returns:
        True if allowed

    Raises:
        HTTPException: If permission denied
    """
    has_permission = await ACLHelper.check_trading_account_permission(
        user_id=user_id,
        trading_account_id=trading_account_id,
        action=action
    )

    if not has_permission:
        raise HTTPException(
            status_code=403,
            detail=f"You do not have {action} permission for trading account {trading_account_id}"
        )

    return True


async def require_order_permission(
    order_id: int,
    user_id: int,
    action: str
) -> bool:
    """
    Check if user has specific permission on order.

    Args:
        order_id: Order ID
        user_id: User ID
        action: Required action (view, edit, cancel, delete)

    Returns:
        True if allowed

    Raises:
        HTTPException: If permission denied
    """
    has_permission = await ACLHelper.check_order_permission(
        user_id=user_id,
        order_id=order_id,
        action=action
    )

    if not has_permission:
        raise HTTPException(
            status_code=403,
            detail=f"You do not have {action} permission for order {order_id}"
        )

    return True


async def require_position_permission(
    position_id: int,
    user_id: int,
    action: str
) -> bool:
    """
    Check if user has specific permission on position.

    Args:
        position_id: Position ID
        user_id: User ID
        action: Required action (view, squareoff, delete)

    Returns:
        True if allowed

    Raises:
        HTTPException: If permission denied
    """
    has_permission = await ACLHelper.check_position_permission(
        user_id=user_id,
        position_id=position_id,
        action=action
    )

    if not has_permission:
        raise HTTPException(
            status_code=403,
            detail=f"You do not have {action} permission for position {position_id}"
        )

    return True


async def require_trade_permission(
    trade_id: int,
    user_id: int,
    action: str = "view"
) -> bool:
    """
    Check if user has specific permission on trade.

    Args:
        trade_id: Trade ID
        user_id: User ID
        action: Required action (view only for trades)

    Returns:
        True if allowed

    Raises:
        HTTPException: If permission denied
    """
    has_permission = await ACLHelper.check_trade_permission(
        user_id=user_id,
        trade_id=trade_id,
        action=action
    )

    if not has_permission:
        raise HTTPException(
            status_code=403,
            detail=f"You do not have {action} permission for trade {trade_id}"
        )

    return True


async def get_accessible_trading_accounts(
    user_id: int,
    min_action: str = "view"
) -> List[int]:
    """
    Get list of trading account IDs user can access with minimum permission.

    Args:
        user_id: User ID
        min_action: Minimum required action

    Returns:
        List of accessible trading account IDs
    """
    acl_client = ACLClient()
    account_ids = await acl_client.get_user_resources(
        user_id=user_id,
        resource_type="trading_account",
        min_action=min_action
    )
    return account_ids


async def get_accessible_orders(
    user_id: int,
    min_action: str = "view"
) -> List[int]:
    """
    Get list of order IDs user can access.

    Args:
        user_id: User ID
        min_action: Minimum required action

    Returns:
        List of accessible order IDs
    """
    return await ACLHelper.get_accessible_orders(user_id=user_id, min_action=min_action)


async def get_accessible_positions(
    user_id: int,
    min_action: str = "view"
) -> List[int]:
    """
    Get list of position IDs user can access.

    Args:
        user_id: User ID
        min_action: Minimum required action

    Returns:
        List of accessible position IDs
    """
    return await ACLHelper.get_accessible_positions(user_id=user_id, min_action=min_action)


async def get_accessible_trades(
    user_id: int,
    min_action: str = "view"
) -> List[int]:
    """
    Get list of trade IDs user can access.

    Args:
        user_id: User ID
        min_action: Minimum required action

    Returns:
        List of accessible trade IDs
    """
    return await ACLHelper.get_accessible_trades(user_id=user_id, min_action=min_action)

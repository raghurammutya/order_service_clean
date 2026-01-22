"""
ACL Helper Functions for Order Service

Provides utility functions for ACL permission checks and resource filtering.
"""

import logging
from typing import List, Optional, Dict, Any
from common.acl_client import ACLClient, ACLClientException, ACLServiceUnavailableException

logger = logging.getLogger(__name__)


class ACLHelper:
    """Helper class for ACL operations in order service"""

    @staticmethod
    async def get_accessible_resources(
        user_id: int,
        resource_type: str,
        min_action: str = "view"
    ) -> List[int]:
        """
        Get list of resource IDs user can access.

        Args:
            user_id: User ID
            resource_type: Resource type (order, position, trade, trading_account)
            min_action: Minimum action required (view, edit, cancel, etc.)

        Returns:
            List of resource IDs user can access

        Raises:
            ACLServiceUnavailableException: If ACL service is down
        """
        try:
            acl_client = ACLClient(base_url="http://localhost:8011")
            resource_ids = await acl_client.get_user_resources(
                user_id=user_id,
                resource_type=resource_type,
                min_action=min_action
            )
            logger.debug(
                f"User {user_id} can access {len(resource_ids)} {resource_type}s "
                f"with min_action={min_action}"
            )
            return resource_ids

        except ACLServiceUnavailableException as e:
            logger.error(f"ACL service unavailable: {e}")
            # In degraded mode, allow access (fail open)
            # Production should fail closed
            logger.warning(f"ACL service down, allowing access (degraded mode)")
            return []  # Return empty list to be safe

        except Exception as e:
            logger.error(f"Unexpected error checking accessible resources: {e}")
            return []

    @staticmethod
    async def check_permission(
        user_id: int,
        resource_type: str,
        resource_id: int,
        action: str
    ) -> bool:
        """
        Check if user has permission to perform action on resource.

        Args:
            user_id: User ID
            resource_type: Resource type (order, position, trade)
            resource_id: Resource ID
            action: Action to check (view, edit, cancel, squareoff, etc.)

        Returns:
            True if allowed, False if denied

        Raises:
            ACLServiceUnavailableException: If ACL service is down
        """
        try:
            acl_client = ACLClient(base_url="http://localhost:8011")
            # Use check_permission_detailed which doesn't require authentication
            # (designed for internal service-to-service calls)
            result = await acl_client.check_permission_detailed(
                user_id=user_id,
                resource_type=resource_type,
                resource_id=resource_id,
                action=action
            )

            if not result.allowed:
                logger.warning(
                    f"Permission denied: user_id={user_id}, "
                    f"resource={resource_type}:{resource_id}, action={action}"
                )

            return result.allowed

        except ACLServiceUnavailableException as e:
            logger.error(f"ACL service unavailable: {e}")
            # In degraded mode, allow access (fail open)
            logger.warning(f"ACL service down, allowing access (degraded mode)")
            return True  # Fail open for now

        except Exception as e:
            logger.error(f"Unexpected error checking permission: {e}")
            return False  # Fail closed for unexpected errors

    @staticmethod
    async def check_permission_detailed(
        user_id: int,
        resource_type: str,
        resource_id: int,
        action: str
    ) -> Dict[str, Any]:
        """
        Check permission with detailed information.

        Args:
            user_id: User ID
            resource_type: Resource type
            resource_id: Resource ID
            action: Action to check

        Returns:
            Dict with keys: allowed, permission_level, source_type, denial_reason, metadata
        """
        try:
            acl_client = ACLClient(base_url="http://localhost:8011")
            result = await acl_client.check_permission_detailed(
                user_id=user_id,
                resource_type=resource_type,
                resource_id=resource_id,
                action=action
            )

            return {
                "allowed": result.allowed,
                "permission_level": result.permission_level,
                "source_type": result.source_type,
                "denial_reason": result.denial_reason,
                "metadata": result.metadata
            }

        except Exception as e:
            logger.error(f"Error checking detailed permission: {e}")
            return {
                "allowed": False,
                "permission_level": None,
                "source_type": None,
                "denial_reason": "error",
                "metadata": {"error": str(e)}
            }

    @staticmethod
    async def bulk_check_permissions(
        user_id: int,
        checks: List[Dict[str, Any]]
    ) -> List[bool]:
        """
        Perform bulk permission checks efficiently.

        Args:
            user_id: User ID
            checks: List of dicts with keys: resource_type, resource_id, action

        Returns:
            List of booleans (same order as checks)

        Example:
            checks = [
                {"resource_type": "order", "resource_id": 1, "action": "cancel"},
                {"resource_type": "order", "resource_id": 2, "action": "cancel"}
            ]
            results = await bulk_check_permissions(123, checks)
            # results = [True, False]
        """
        try:
            acl_client = ACLClient(base_url="http://localhost:8011")
            results = await acl_client.bulk_check_permissions(user_id, checks)
            return results

        except ACLServiceUnavailableException as e:
            logger.error(f"ACL service unavailable for bulk check: {e}")
            # Fail open in degraded mode
            return [True] * len(checks)

        except Exception as e:
            logger.error(f"Error in bulk permission check: {e}")
            # Fail closed for unexpected errors
            return [False] * len(checks)

    @staticmethod
    async def filter_accessible_ids(
        user_id: int,
        resource_type: str,
        all_resource_ids: List[int],
        action: str = "view"
    ) -> List[int]:
        """
        Filter a list of resource IDs to only those user can access.

        More efficient than checking each individually.

        Args:
            user_id: User ID
            resource_type: Resource type
            all_resource_ids: List of all resource IDs to filter
            action: Action required

        Returns:
            Filtered list of accessible resource IDs
        """
        if not all_resource_ids:
            return []

        # Get all accessible resources
        accessible = await ACLHelper.get_accessible_resources(
            user_id=user_id,
            resource_type=resource_type,
            min_action=action
        )

        # Return intersection
        accessible_set = set(accessible)
        filtered = [rid for rid in all_resource_ids if rid in accessible_set]

        logger.debug(
            f"Filtered {len(all_resource_ids)} {resource_type}s to "
            f"{len(filtered)} accessible for user {user_id}"
        )

        return filtered

    @staticmethod
    async def check_trading_account_permission(
        user_id: int,
        trading_account_id: int,
        action: str = "view"
    ) -> bool:
        """
        Check if user has permission on trading account.

        Args:
            user_id: User ID
            trading_account_id: Trading account ID
            action: Action to check (view, trade, admin)

        Returns:
            True if allowed, False if denied
        """
        return await ACLHelper.check_permission(
            user_id=user_id,
            resource_type="trading_account",
            resource_id=trading_account_id,
            action=action
        )

    @staticmethod
    async def get_accessible_resources_with_hierarchy(
        user_id: int,
        resource_type: str,
        trading_account_id: Optional[int] = None,
        min_action: str = "view"
    ) -> tuple[bool, List[int]]:
        """
        Get accessible resource IDs with hierarchical permission inheritance.

        **Hierarchical Permission Model:**
        1. If user has trading_account permission → Return (True, []) meaning ALL resources
        2. If user has no account permission → Return (False, [specific_resource_ids])

        This allows:
        - Account owners to see all positions/orders/trades in their account
        - Specific resource sharing (e.g., share only position #123, not entire account)

        Args:
            user_id: User ID
            resource_type: Resource type (position, order, trade, holding)
            trading_account_id: Optional trading account ID to check hierarchy
            min_action: Minimum action required

        Returns:
            Tuple of (has_account_access, resource_ids)
            - If has_account_access=True: User has full account access, resource_ids is empty (means ALL)
            - If has_account_access=False: User has limited access, resource_ids lists accessible resources

        Example:
            >>> has_account_access, resource_ids = await get_accessible_resources_with_hierarchy(
            ...     user_id=7, resource_type="position", trading_account_id=1, min_action="view"
            ... )
            >>> if has_account_access:
            ...     # Return all positions in account
            ...     query = select(Position).where(Position.trading_account_id == 1)
            >>> else:
            ...     # Filter to specific positions
            ...     query = select(Position).where(Position.position_id.in_(resource_ids))
        """
        # Step 1: Check if user has trading_account permission (hierarchical inheritance)
        if trading_account_id is not None:
            has_account_access = await ACLHelper.check_trading_account_permission(
                user_id=user_id,
                trading_account_id=trading_account_id,
                action=min_action
            )

            if has_account_access:
                # User has full account access → return ALL resources
                logger.info(
                    f"Hierarchical access: user {user_id} has {min_action} on "
                    f"trading_account {trading_account_id} → granting access to ALL {resource_type}s"
                )
                return (True, [])

        # Step 2: No account-level access → check resource-level permissions
        resource_ids = await ACLHelper.get_accessible_resources(
            user_id=user_id,
            resource_type=resource_type,
            min_action=min_action
        )

        logger.info(
            f"Granular access: user {user_id} has {min_action} on "
            f"{len(resource_ids)} specific {resource_type}(s)"
        )

        return (False, resource_ids)

    @staticmethod
    def get_permission_error_response(
        resource_type: str,
        resource_id: int,
        action: str
    ) -> Dict[str, str]:
        """
        Get standardized permission error response.

        Args:
            resource_type: Resource type
            resource_id: Resource ID
            action: Action that was denied

        Returns:
            Error response dict
        """
        return {
            "error": "permission_denied",
            "message": f"You do not have permission to {action} {resource_type} {resource_id}",
            "resource_type": resource_type,
            "resource_id": str(resource_id),
            "action": action
        }


# Convenience functions for common operations

async def check_order_permission(user_id: int, order_id: int, action: str) -> bool:
    """Check permission on an order"""
    return await ACLHelper.check_permission(user_id, "order", order_id, action)


async def check_position_permission(user_id: int, position_id: int, action: str) -> bool:
    """Check permission on a position"""
    return await ACLHelper.check_permission(user_id, "position", position_id, action)


async def check_trade_permission(user_id: int, trade_id: int, action: str) -> bool:
    """Check permission on a trade"""
    return await ACLHelper.check_permission(user_id, "trade", trade_id, action)


async def get_accessible_orders(user_id: int, min_action: str = "view") -> List[int]:
    """Get all orders user can access"""
    return await ACLHelper.get_accessible_resources(user_id, "order", min_action)


async def get_accessible_positions(user_id: int, min_action: str = "view") -> List[int]:
    """Get all positions user can access"""
    return await ACLHelper.get_accessible_resources(user_id, "position", min_action)


async def get_accessible_trades(user_id: int, min_action: str = "view") -> List[int]:
    """Get all trades user can access"""
    return await ACLHelper.get_accessible_resources(user_id, "trade", min_action)

"""
Order Reconciliation Service

Detects and corrects drift between database state and broker state.
Ensures data quality and prevents stale order data.
"""
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from ..models.order import Order
from ..services.audit_service import OrderAuditService
from ..services.kite_client_multi import get_kite_client_for_account

logger = logging.getLogger(__name__)


class ReconciliationService:
    """
    Service for reconciling order state between database and broker.

    Usage:
        reconciliation = ReconciliationService(db)
        result = await reconciliation.reconcile_pending_orders()
        logger.info(f"Found {result['drift_count']} orders with drift")
    """

    # Terminal states that don't need reconciliation
    TERMINAL_STATES = {"COMPLETE", "CANCELLED", "REJECTED"}

    # States that need reconciliation
    NON_TERMINAL_STATES = {"PENDING", "SUBMITTED", "OPEN", "TRIGGER_PENDING"}

    # Broker status to our status mapping
    BROKER_STATUS_MAP = {
        "OPEN": "OPEN",
        "COMPLETE": "COMPLETE",
        "CANCELLED": "CANCELLED",
        "REJECTED": "REJECTED",
        "TRIGGER PENDING": "TRIGGER_PENDING",
        "PENDING": "SUBMITTED",  # Broker "PENDING" means submitted but not open yet
    }

    def __init__(self, db: AsyncSession):
        """
        Initialize reconciliation service.

        Args:
            db: Database session
        """
        self.db = db
        self.audit_service = OrderAuditService(db, user_id=None)  # System action

    async def reconcile_pending_orders(
        self,
        max_age_hours: int = 24,
        batch_size: int = 100
    ) -> Dict[str, Any]:
        """
        Reconcile all non-terminal orders with broker.

        This method:
        1. Queries all orders in non-terminal states
        2. Fetches current status from broker
        3. Detects drift (database != broker)
        4. Corrects drift by updating database
        5. Logs corrections to audit trail

        Args:
            max_age_hours: Only reconcile orders created in last N hours (default 24)
            batch_size: Maximum orders to reconcile in one run (default 100)

        Returns:
            Dictionary with reconciliation results:
            {
                "total_checked": 50,
                "drift_count": 3,
                "corrected": 3,
                "errors": 0,
                "corrections": [
                    {
                        "order_id": 123,
                        "symbol": "RELIANCE",
                        "db_status": "SUBMITTED",
                        "broker_status": "COMPLETE",
                        "corrected": True
                    }
                ]
            }
        """
        result = {
            "total_checked": 0,
            "drift_count": 0,
            "corrected": 0,
            "errors": 0,
            "corrections": [],
            "started_at": datetime.utcnow().isoformat(),
        }

        try:
            # Get all non-terminal orders
            orders = await self._get_pending_orders(max_age_hours, batch_size)
            result["total_checked"] = len(orders)

            if not orders:
                logger.info("No orders to reconcile")
                return result

            logger.info(f"Reconciling {len(orders)} orders...")

            # Group orders by trading account to minimize broker API calls
            orders_by_account = self._group_by_trading_account(orders)

            # Reconcile each account's orders
            for trading_account_id, account_orders in orders_by_account.items():
                account_result = await self._reconcile_account_orders(
                    trading_account_id,
                    account_orders
                )

                result["drift_count"] += account_result["drift_count"]
                result["corrected"] += account_result["corrected"]
                result["errors"] += account_result["errors"]
                result["corrections"].extend(account_result["corrections"])

            # Commit all changes
            await self.db.commit()

            result["completed_at"] = datetime.utcnow().isoformat()

            logger.info(
                f"Reconciliation complete: {result['drift_count']} drifts found, "
                f"{result['corrected']} corrected, {result['errors']} errors"
            )

            return result

        except Exception as e:
            logger.error(f"Reconciliation failed: {e}", exc_info=True)
            result["errors"] += 1
            result["error_message"] = str(e)
            await self.db.rollback()
            return result

    async def _get_pending_orders(
        self,
        max_age_hours: int,
        batch_size: int
    ) -> List[Order]:
        """
        Get all non-terminal orders from database.

        Args:
            max_age_hours: Only get orders created in last N hours
            batch_size: Maximum orders to return

        Returns:
            List of Order objects
        """
        cutoff = datetime.utcnow() - timedelta(hours=max_age_hours)

        result = await self.db.execute(
            select(Order)
            .where(
                Order.status.in_(self.NON_TERMINAL_STATES),
                Order.created_at >= cutoff,
                Order.broker_order_id.isnot(None)  # Only reconcile orders submitted to broker
            )
            .order_by(Order.created_at.desc())
            .limit(batch_size)
        )

        return list(result.scalars().all())

    def _group_by_trading_account(
        self,
        orders: List[Order]
    ) -> Dict[int, List[Order]]:
        """
        Group orders by trading account to batch broker API calls.

        Args:
            orders: List of orders

        Returns:
            Dictionary mapping trading_account_id to list of orders
        """
        grouped = {}
        for order in orders:
            account_id = order.trading_account_id
            if account_id not in grouped:
                grouped[account_id] = []
            grouped[account_id].append(order)

        return grouped

    async def _reconcile_account_orders(
        self,
        trading_account_id: int,
        orders: List[Order]
    ) -> Dict[str, Any]:
        """
        Reconcile all orders for a specific trading account.

        Args:
            trading_account_id: Trading account ID
            orders: List of orders for this account

        Returns:
            Reconciliation results for this account
        """
        result = {
            "drift_count": 0,
            "corrected": 0,
            "errors": 0,
            "corrections": []
        }

        try:
            # Get broker client for this account
            kite = get_kite_client_for_account(trading_account_id)

            # Fetch all orders from broker in one call
            broker_orders = await kite.get_orders()

            # Create lookup dict by broker_order_id
            broker_orders_dict = {
                order["order_id"]: order
                for order in broker_orders
            }

            # Check each order
            for order in orders:
                try:
                    correction = await self._reconcile_single_order(
                        order,
                        broker_orders_dict
                    )

                    if correction:
                        result["drift_count"] += 1
                        if correction["corrected"]:
                            result["corrected"] += 1
                        result["corrections"].append(correction)

                except Exception as e:
                    logger.error(
                        f"Error reconciling order {order.id}: {e}",
                        exc_info=True
                    )
                    result["errors"] += 1

            return result

        except Exception as e:
            logger.error(
                f"Error fetching broker orders for account {trading_account_id}: {e}",
                exc_info=True
            )
            result["errors"] += len(orders)
            return result

    async def _reconcile_single_order(
        self,
        order: Order,
        broker_orders_dict: Dict[str, Dict[str, Any]]
    ) -> Optional[Dict[str, Any]]:
        """
        Reconcile a single order with broker state.

        Args:
            order: Order object from database
            broker_orders_dict: Dictionary of broker orders by order_id

        Returns:
            Correction details if drift detected, None otherwise
        """
        broker_order_id = order.broker_order_id

        if not broker_order_id:
            logger.warning(f"Order {order.id} has no broker_order_id, skipping")
            return None

        # Find order in broker data
        broker_order = broker_orders_dict.get(broker_order_id)

        if not broker_order:
            logger.warning(
                f"Order {order.id} (broker_id={broker_order_id}) not found in broker. "
                f"May have been placed > 24 hours ago."
            )
            return None

        # Extract broker status
        broker_status_raw = broker_order.get("status", "").upper()
        broker_status = self.BROKER_STATUS_MAP.get(broker_status_raw, broker_status_raw)

        db_status = order.status

        # Check for drift
        if db_status == broker_status:
            # No drift
            return None

        # Drift detected!
        logger.warning(
            f"DRIFT DETECTED: Order {order.id} ({order.symbol}) - "
            f"DB={db_status}, Broker={broker_status}"
        )

        # Correct the drift
        correction = {
            "order_id": order.id,
            "broker_order_id": broker_order_id,
            "symbol": order.symbol,
            "db_status": db_status,
            "broker_status": broker_status,
            "corrected": False,
            "timestamp": datetime.utcnow().isoformat()
        }

        try:
            # Update order status
            old_status = order.status
            order.status = broker_status

            # Update filled quantities if available
            if "filled_quantity" in broker_order:
                order.filled_quantity = broker_order["filled_quantity"]
                order.pending_quantity = order.quantity - order.filled_quantity

            # Update average price if available
            if "average_price" in broker_order and broker_order["average_price"]:
                order.average_price = float(broker_order["average_price"])

            # Update timestamps
            order.updated_at = datetime.utcnow()
            if broker_status in self.TERMINAL_STATES:
                order.completed_at = datetime.utcnow()

            # Log to audit trail
            await self.audit_service.log_state_change(
                order_id=order.id,
                old_status=old_status,
                new_status=broker_status,
                reason=f"Reconciliation: Corrected drift from broker (was {old_status})",
                changed_by_system="reconciliation_worker",
                metadata={
                    "drift_detected": True,
                    "broker_order_id": broker_order_id,
                    "broker_data": {
                        "status": broker_status,
                        "filled_quantity": broker_order.get("filled_quantity"),
                        "average_price": broker_order.get("average_price")
                    }
                }
            )

            await self.db.flush()

            correction["corrected"] = True
            logger.info(f"Corrected order {order.id}: {old_status} → {broker_status}")

        except Exception as e:
            logger.error(f"Failed to correct order {order.id}: {e}", exc_info=True)
            correction["error"] = str(e)

        return correction

    async def reconcile_single_order_by_id(
        self,
        order_id: int
    ) -> Dict[str, Any]:
        """
        Reconcile a single order by ID (for manual reconciliation).

        Args:
            order_id: Order ID to reconcile

        Returns:
            Reconciliation result for this order
        """
        result = await self.db.execute(
            select(Order).where(Order.id == order_id)
        )
        order = result.scalar_one_or_none()

        if not order:
            return {
                "success": False,
                "error": f"Order {order_id} not found"
            }

        if order.status in self.TERMINAL_STATES:
            return {
                "success": False,
                "error": f"Order {order_id} is in terminal state {order.status}"
            }

        if not order.broker_order_id:
            return {
                "success": False,
                "error": f"Order {order_id} has no broker_order_id"
            }

        try:
            # Get broker client for this order's account
            kite = get_kite_client_for_account(order.trading_account_id)

            # Fetch all orders from broker
            broker_orders = await kite.get_orders()
            broker_orders_dict = {o["order_id"]: o for o in broker_orders}

            # Reconcile
            correction = await self._reconcile_single_order(order, broker_orders_dict)

            if correction:
                await self.db.commit()
                return {
                    "success": True,
                    "drift_detected": True,
                    "correction": correction
                }
            else:
                return {
                    "success": True,
                    "drift_detected": False,
                    "message": "Order status matches broker"
                }

        except Exception as e:
            logger.error(f"Error reconciling order {order_id}: {e}", exc_info=True)
            await self.db.rollback()
            return {
                "success": False,
                "error": str(e)
            }

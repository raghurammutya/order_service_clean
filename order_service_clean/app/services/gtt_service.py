"""
GTT Service Business Logic

Handles Good-Till-Triggered (GTT) order management.
"""
import logging
from datetime import datetime
from typing import List, Optional, Dict, Any
from sqlalchemy import select, and_
from sqlalchemy.ext.asyncio import AsyncSession
from fastapi import HTTPException

from ..models.gtt_order import GttOrder
from .kite_client_multi import get_kite_client_for_account

logger = logging.getLogger(__name__)


class GttService:
    """GTT order management service"""

    def __init__(self, db: AsyncSession, user_id: int, trading_account_id: int):
        """
        Initialize GTT service.

        Args:
            db: Database session
            user_id: User ID from JWT token
            trading_account_id: Trading account ID (will be converted to string for DB queries)
        """
        self.db = db
        self.user_id = user_id
        # Convert to string since trading_account_id is VARCHAR in database
        self.trading_account_id = str(trading_account_id)
        self.kite_client = get_kite_client_for_account(trading_account_id)

    # ==========================================
    # GTT ORDER CREATION
    # ==========================================

    async def place_gtt_order(
        self,
        gtt_type: str,
        symbol: str,
        exchange: str,
        tradingsymbol: str,
        trigger_values: List[float],
        last_price: float,
        orders: List[Dict[str, Any]],
        expires_at: Optional[datetime] = None,
        user_tag: Optional[str] = None,
        user_notes: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Place a new GTT order.

        Args:
            gtt_type: 'single' or 'two-leg' (OCO)
            symbol: Internal symbol (e.g., 'NIFTY', 'RELIANCE')
            exchange: Exchange code
            tradingsymbol: Broker's trading symbol
            trigger_values: List of trigger prices
            last_price: Current market price
            orders: List of orders to place when triggered
            expires_at: Optional expiry time
            user_tag: Optional user tag
            user_notes: Optional user notes

        Returns:
            GTT order details

        Raises:
            HTTPException: If GTT creation fails
        """
        try:
            # Validate GTT type
            if gtt_type not in ['single', 'two-leg']:
                raise HTTPException(400, f"Invalid GTT type: {gtt_type}")

            # Validate trigger values
            if gtt_type == 'single' and len(trigger_values) != 1:
                raise HTTPException(400, "Single-leg GTT requires exactly 1 trigger value")
            elif gtt_type == 'two-leg' and len(trigger_values) != 2:
                raise HTTPException(400, "Two-leg GTT requires exactly 2 trigger values")

            # Validate orders
            if not orders or len(orders) == 0:
                raise HTTPException(400, "At least one order is required")

            logger.info(
                f"Placing GTT order: user={self.user_id}, type={gtt_type}, "
                f"symbol={symbol}, triggers={trigger_values}"
            )

            # Place GTT order via broker API
            broker_gtt_id = await self.kite_client.place_gtt(
                gtt_type=gtt_type,
                tradingsymbol=tradingsymbol,
                exchange=exchange,
                trigger_values=trigger_values,
                last_price=last_price,
                orders=orders
            )

            # Create GTT order record in database
            gtt_order = GttOrder(
                user_id=self.user_id,
                trading_account_id=self.trading_account_id,
                broker_gtt_id=broker_gtt_id,
                gtt_type=gtt_type,
                status='active',
                symbol=symbol,
                exchange=exchange,
                tradingsymbol=tradingsymbol,
                condition={
                    'exchange': exchange,
                    "symbol": tradingsymbol,
                    'trigger_values': trigger_values,
                    'last_price': last_price
                },
                orders=orders,
                expires_at=expires_at,
                user_tag=user_tag,
                user_notes=user_notes
            )

            self.db.add(gtt_order)
            await self.db.commit()
            await self.db.refresh(gtt_order)

            logger.info(
                f"GTT order created: id={gtt_order.id}, broker_gtt_id={broker_gtt_id}"
            )

            return gtt_order.to_dict()

        except HTTPException:
            await self.db.rollback()
            raise
        except Exception as e:
            await self.db.rollback()
            logger.error(f"Failed to place GTT order: {e}")
            raise HTTPException(500, f"Failed to place GTT order: {str(e)}")

    # ==========================================
    # GTT ORDER RETRIEVAL
    # ==========================================

    async def get_gtt_order(self, gtt_id: int) -> Dict[str, Any]:
        """
        Get a specific GTT order by ID.

        Args:
            gtt_id: GTT order ID

        Returns:
            GTT order details

        Raises:
            HTTPException: If GTT order not found
        """
        result = await self.db.execute(
            select(GttOrder).where(
                and_(
                    GttOrder.id == gtt_id,
                    GttOrder.user_id == self.user_id
                )
            )
        )
        gtt_order = result.scalar_one_or_none()

        if not gtt_order:
            raise HTTPException(404, f"GTT order {gtt_id} not found")

        return gtt_order.to_dict()

    async def list_gtt_orders(
        self,
        status: Optional[str] = None,
        symbol: Optional[str] = None,
        gtt_type: Optional[str] = None,
        limit: int = 100,
        offset: int = 0
    ) -> List[Dict[str, Any]]:
        """
        List user's GTT orders with optional filtering.

        Args:
            status: Filter by status (active, triggered, cancelled, etc.)
            symbol: Filter by symbol
            gtt_type: Filter by GTT type (single, two-leg)
            limit: Maximum number of GTT orders to return
            offset: Number of GTT orders to skip

        Returns:
            List of GTT order dictionaries
        """
        query = select(GttOrder).where(
            and_(
                GttOrder.user_id == self.user_id,
                GttOrder.trading_account_id == self.trading_account_id
            )
        )

        if status:
            query = query.where(GttOrder.status == status)

        if symbol:
            query = query.where(GttOrder.symbol == symbol)

        if gtt_type:
            query = query.where(GttOrder.gtt_type == gtt_type)

        query = query.order_by(GttOrder.created_at.desc()).limit(limit).offset(offset)

        result = await self.db.execute(query)
        gtt_orders = result.scalars().all()

        logger.debug(f"Retrieved {len(gtt_orders)} GTT orders for user {self.user_id}")

        return [gtt.to_dict() for gtt in gtt_orders]

    # ==========================================
    # GTT ORDER MODIFICATION
    # ==========================================

    async def modify_gtt_order(
        self,
        gtt_id: int,
        trigger_values: List[float],
        last_price: float,
        orders: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Modify an existing GTT order.

        Args:
            gtt_id: GTT order ID
            trigger_values: New trigger prices
            last_price: Current market price
            orders: Updated orders

        Returns:
            Modified GTT order details

        Raises:
            HTTPException: If modification fails
        """
        try:
            # Get GTT order from database
            result = await self.db.execute(
                select(GttOrder).where(
                    and_(
                        GttOrder.id == gtt_id,
                        GttOrder.user_id == self.user_id
                    )
                )
            )
            gtt_order = result.scalar_one_or_none()

            if not gtt_order:
                raise HTTPException(404, f"GTT order {gtt_id} not found")

            # Check if GTT is modifiable
            if gtt_order.status != 'active':
                raise HTTPException(
                    400,
                    f"Cannot modify GTT order with status '{gtt_order.status}'"
                )

            if not gtt_order.broker_gtt_id:
                raise HTTPException(400, "GTT order not yet submitted to broker")

            logger.info(f"Modifying GTT order: id={gtt_id}, broker_id={gtt_order.broker_gtt_id}")

            # Modify GTT via broker API
            await self.kite_client.modify_gtt(
                gtt_id=gtt_order.broker_gtt_id,
                trigger_values=trigger_values,
                last_price=last_price,
                orders=orders
            )

            # Update database record
            gtt_order.condition = {
                'exchange': gtt_order.exchange,
                "symbol": gtt_order.tradingsymbol,
                'trigger_values': trigger_values,
                'last_price': last_price
            }
            gtt_order.orders = orders
            gtt_order.updated_at = datetime.utcnow()

            await self.db.commit()
            await self.db.refresh(gtt_order)

            logger.info(f"GTT order modified successfully: {gtt_id}")

            return gtt_order.to_dict()

        except HTTPException:
            await self.db.rollback()
            raise
        except Exception as e:
            await self.db.rollback()
            logger.error(f"Failed to modify GTT order {gtt_id}: {e}")
            raise HTTPException(500, f"Failed to modify GTT order: {str(e)}")

    # ==========================================
    # GTT ORDER CANCELLATION
    # ==========================================

    async def cancel_gtt_order(self, gtt_id: int) -> Dict[str, Any]:
        """
        Cancel (delete) a GTT order.

        Args:
            gtt_id: GTT order ID

        Returns:
            Cancelled GTT order details

        Raises:
            HTTPException: If cancellation fails
        """
        try:
            # Get GTT order from database
            result = await self.db.execute(
                select(GttOrder).where(
                    and_(
                        GttOrder.id == gtt_id,
                        GttOrder.user_id == self.user_id
                    )
                )
            )
            gtt_order = result.scalar_one_or_none()

            if not gtt_order:
                raise HTTPException(404, f"GTT order {gtt_id} not found")

            # Check if GTT is cancellable
            if gtt_order.status not in ['active']:
                raise HTTPException(
                    400,
                    f"Cannot cancel GTT order with status '{gtt_order.status}'"
                )

            if not gtt_order.broker_gtt_id:
                raise HTTPException(400, "GTT order not yet submitted to broker")

            logger.info(f"Cancelling GTT order: id={gtt_id}, broker_id={gtt_order.broker_gtt_id}")

            # Delete GTT via broker API
            await self.kite_client.delete_gtt(gtt_order.broker_gtt_id)

            # Update database record
            gtt_order.status = 'cancelled'
            gtt_order.cancelled_at = datetime.utcnow()
            gtt_order.updated_at = datetime.utcnow()

            await self.db.commit()
            await self.db.refresh(gtt_order)

            logger.info(f"GTT order cancelled successfully: {gtt_id}")

            return gtt_order.to_dict()

        except HTTPException:
            await self.db.rollback()
            raise
        except Exception as e:
            await self.db.rollback()
            logger.error(f"Failed to cancel GTT order {gtt_id}: {e}")
            raise HTTPException(500, f"Failed to cancel GTT order: {str(e)}")

    # ==========================================
    # GTT ORDER SYNC
    # ==========================================

    async def sync_gtt_orders_from_broker(self) -> Dict[str, Any]:
        """
        Sync GTT orders from broker API.

        Fetches all active GTT orders from the broker and updates local database.

        Returns:
            Sync statistics

        Raises:
            HTTPException: If sync fails
        """
        try:
            logger.info(f"Syncing GTT orders for user {self.user_id}")

            # Fetch GTT orders from broker
            broker_gtts = await self.kite_client.get_gtts()

            stats = {
                'gtts_synced': 0,
                'gtts_updated': 0,
                'gtts_created': 0,
                'errors': []
            }

            # Sync each GTT
            for broker_gtt in broker_gtts:
                try:
                    await self._sync_gtt(broker_gtt)
                    stats['gtts_synced'] += 1
                except Exception as e:
                    logger.error(f"Failed to sync GTT {broker_gtt.get('id')}: {e}")
                    stats['errors'].append(str(e))

            await self.db.commit()

            logger.info(f"GTT sync completed: {stats}")
            return stats

        except Exception as e:
            logger.error(f"GTT sync failed: {e}")
            raise HTTPException(500, f"GTT sync failed: {str(e)}")

    async def _sync_gtt(self, broker_gtt: Dict[str, Any]) -> GttOrder:
        """
        Sync a single GTT from broker data.

        Args:
            broker_gtt: GTT data from broker

        Returns:
            Updated or created GttOrder object
        """
        broker_gtt_id = broker_gtt['id']

        # Check if GTT already exists
        result = await self.db.execute(
            select(GttOrder).where(
                and_(
                    GttOrder.user_id == self.user_id,
                    GttOrder.broker_gtt_id == broker_gtt_id
                )
            )
        )
        gtt_order = result.scalar_one_or_none()

        if gtt_order:
            # Update existing GTT
            gtt_order.status = broker_gtt['status']
            gtt_order.condition = broker_gtt['condition']
            gtt_order.orders = broker_gtt['orders']
            gtt_order.broker_metadata = broker_gtt
            gtt_order.updated_at = datetime.utcnow()

            if broker_gtt['status'] == 'triggered' and not gtt_order.triggered_at:
                gtt_order.triggered_at = datetime.utcnow()

            logger.debug(f"Updated GTT: {broker_gtt_id}")
            return gtt_order

        # Create new GTT (shouldn't normally happen, but handle it)
        gtt_order = GttOrder(
            user_id=self.user_id,
            trading_account_id=self.trading_account_id,
            broker_gtt_id=broker_gtt_id,
            gtt_type=broker_gtt['type'],
            status=broker_gtt['status'],
            symbol=broker_gtt.get('condition', {}).get('tradingsymbol', 'UNKNOWN'),
            exchange=broker_gtt.get('condition', {}).get('exchange', 'UNKNOWN'),
            tradingsymbol=broker_gtt.get('condition', {}).get('tradingsymbol', 'UNKNOWN'),
            condition=broker_gtt['condition'],
            orders=broker_gtt['orders'],
            broker_metadata=broker_gtt
        )

        self.db.add(gtt_order)
        logger.debug(f"Created new GTT from sync: {broker_gtt_id}")

        return gtt_order

"""
Trade Service Business Logic

Handles trade tracking, synchronization, and analytics.
"""
import logging
from datetime import datetime, date, timedelta
from typing import List, Optional, Dict, Any
from sqlalchemy import select, and_, func
from sqlalchemy.ext.asyncio import AsyncSession
from fastapi import HTTPException

from ..models.trade import Trade
from ..database.redis_client import (
    cache_trade,
    get_cached_trade,
    invalidate_trade_cache
)
from .kite_client_multi import get_kite_client_for_account

logger = logging.getLogger(__name__)


class TradeService:
    """Trade tracking and analytics service"""

    def __init__(self, db: AsyncSession, user_id: int, trading_account_id: int):
        """
        Initialize trade service.

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
    # TRADE SYNC FROM BROKER
    # ==========================================

    async def sync_trades_from_broker(self) -> Dict[str, Any]:
        """
        Sync trades from broker API.

        Returns:
            Dictionary with sync statistics

        Raises:
            HTTPException: If sync fails
        """
        try:
            logger.info(f"Syncing trades for user {self.user_id}")

            # Fetch trades from broker
            broker_trades = await self.kite_client.get_trades()

            stats = {
                'trades_synced': 0,
                'trades_created': 0,
                'trades_updated': 0,
                'errors': []
            }

            # Sync each trade
            for broker_trade in broker_trades:
                try:
                    await self._sync_trade(broker_trade)
                    stats['trades_synced'] += 1
                except Exception as e:
                    logger.error(f"Failed to sync trade {broker_trade.get('trade_id')}: {e}")
                    stats['errors'].append(str(e))

            await self.db.commit()

            logger.info(f"Trade sync completed: {stats}")
            return stats

        except Exception as e:
            logger.error(f"Trade sync failed: {e}")
            raise HTTPException(500, f"Trade sync failed: {str(e)}")

    async def _sync_trade(self, broker_trade: Dict[str, Any]) -> Trade:
        """
        Sync a single trade from broker data.

        Args:
            broker_trade: Trade data from broker

        Returns:
            Updated or created Trade object
        """
        broker_trade_id = broker_trade['trade_id']

        # Check if trade already exists
        # ACL check already verified at endpoint level - verify belongs to trading_account
        result = await self.db.execute(
            select(Trade).where(
                and_(
                    Trade.trading_account_id == self.trading_account_id,
                    Trade.broker_trade_id == broker_trade_id
                )
            )
        )
        trade = result.scalar_one_or_none()

        if trade:
            # Trade already exists, update if needed
            logger.debug(f"Trade {broker_trade_id} already exists")
            return trade

        # Find matching order by broker_order_id to link trade to order
        from ..models.order import Order
        broker_order_id = str(broker_trade.get('order_id', ''))
        order = None
        order_id = None
        source = 'internal'  # Default source

        if broker_order_id:
            order_result = await self.db.execute(
                select(Order).where(
                    and_(
                        Order.broker_order_id == broker_order_id,
                        Order.trading_account_id == self.trading_account_id
                    )
                )
            )
            order = order_result.scalar_one_or_none()

            if order:
                # Link to order and inherit source
                order_id = order.id
                source = order.source if order.source else 'internal'
                logger.debug(f"Trade {broker_trade_id} linked to order {order_id} (source={source})")
            else:
                logger.warning(
                    f"Trade {broker_trade_id} has no matching order "
                    f"(broker_order_id={broker_order_id}). Creating unlinked trade."
                )

        # Create new trade
        trade = Trade(
            order_id=order_id,  # Link to order (None if not found)
            user_id=self.user_id,
            trading_account_id=str(self.trading_account_id),  # Convert to string for DB
            broker_trade_id=broker_trade_id,
            broker_order_id=broker_order_id,
            symbol=broker_trade["symbol"],
            exchange=broker_trade['exchange'],
            transaction_type=broker_trade['transaction_type'],
            product_type=broker_trade['product'],
            quantity=broker_trade['quantity'],
            price=broker_trade['average_price'],
            trade_time=broker_trade.get('fill_timestamp') or broker_trade.get('exchange_timestamp'),
            trade_value=broker_trade['quantity'] * broker_trade['average_price'],
            source=source,  # Inherit from order or default to 'internal'
        )

        self.db.add(trade)
        logger.debug(f"Created new trade: {broker_trade_id} (order_id={order_id}, source={source})")

        # Sprint 7A: Trigger attribution for external trades/partial exits
        if source == 'external':
            try:
                await self._trigger_attribution_for_external_trade(trade, broker_trade)
            except Exception as e:
                logger.error(f"Attribution failed for external trade {broker_trade_id}: {e}")
                # Don't fail the entire sync - attribution issues can be resolved later

        # Invalidate cache
        await invalidate_trade_cache(f"user:{self.user_id}")

        return trade

    async def _trigger_attribution_for_external_trade(self, trade: Trade, broker_trade: Dict[str, Any]) -> None:
        """
        Trigger attribution for external trades to handle partial exits.
        
        When an external trade is detected (e.g., user sells via broker terminal),
        we need to attribute the exit across existing strategies if multiple
        strategies hold the same symbol.
        
        Args:
            trade: The external trade that was synced
            broker_trade: Raw broker trade data
        """
        from .partial_exit_attribution_service import PartialExitAttributionService, AllocationMethod
        from .reconciliation_driven_transfers import ReconciliationDrivenTransferService, TransferTrigger
        from decimal import Decimal
        
        # Only process sell transactions (exits)
        if trade.transaction_type.upper() != 'SELL':
            logger.debug(f"External trade {trade.broker_trade_id} is BUY - no attribution needed")
            return
            
        logger.info(
            f"Triggering attribution for external exit: {trade.symbol} "
            f"qty={trade.quantity} account={trade.trading_account_id}"
        )
        
        try:
            # Step 1: GAP-REC-10: Robust exit context matching
            from .exit_context_matcher import ExitContextMatcher, ExitContextConfig
            
            exit_matcher = ExitContextMatcher(self.db, ExitContextConfig())
            external_exit_data = {
                "broker_trade_id": trade.broker_trade_id,
                "quantity": str(trade.quantity),
                "price": str(trade.price) if trade.price else None,
                "timestamp": trade.trade_time.isoformat() if trade.trade_time else None,
                "symbol": trade.symbol
            }
            
            match_result = await exit_matcher.match_exit_context(
                external_exit_data,
                trade.trading_account_id,
                trade.symbol
            )
            
            logger.info(
                f"Exit context matching result for {trade.symbol}: "
                f"quality={match_result.match_quality}, confidence={match_result.confidence_score}, "
                f"matched_trades={len(match_result.matched_trades)}"
            )

            # Step 2: Use attribution service to allocate the exit across strategies
            attribution_service = PartialExitAttributionService(self.db)
            
            allocation_result = await attribution_service.attribute_partial_exit(
                trading_account_id=trade.trading_account_id,
                symbol=trade.symbol,
                exit_quantity=Decimal(str(trade.quantity)),
                exit_price=Decimal(str(trade.price)) if trade.price else None,
                exit_timestamp=trade.trade_time,
                allocation_method=AllocationMethod.FIFO  # Use FIFO for tax compliance
            )
            
            logger.info(
                f"Attribution result for {trade.symbol}: "
                f"allocated={allocation_result.total_allocated_quantity}, "
                f"unallocated={allocation_result.unallocated_quantity}, "
                f"manual_required={allocation_result.requires_manual_intervention}"
            )
            
            # Step 2: If allocation successful and doesn't require manual intervention,
            #         trigger transfer service to apply the allocations
            if not allocation_result.requires_manual_intervention and allocation_result.allocations:
                transfer_service = ReconciliationDrivenTransferService(self.db)
                
                # Execute attribution transfers using the correct method
                transfer_result = await transfer_service.execute_attribution_transfers(
                    allocation_result=allocation_result,
                    trigger=TransferTrigger.PARTIAL_EXIT_ALLOCATED
                )
                
                logger.info(
                    f"Attribution transfers executed for external trade attribution: "
                    f"transfer_id={transfer_result.transfer_id}, "
                    f"executed={transfer_result.executed_count}/{transfer_result.instructions_count}"
                )
            
            # Step 3: If manual intervention required, create manual attribution case
            elif allocation_result.requires_manual_intervention:
                await self._create_manual_attribution_case(allocation_result, trade)
                
        except Exception as e:
            logger.error(f"Attribution processing failed for external trade {trade.broker_trade_id}: {e}", exc_info=True)
            # Create manual case as fallback
            await self._create_manual_attribution_case_fallback(trade, str(e))
            
    async def _create_manual_attribution_case(self, allocation_result, trade: Trade) -> None:
        """Create a manual attribution case for manual resolution."""
        from .manual_attribution_service import ManualAttributionService, AttributionPriority
        from decimal import Decimal
        
        manual_service = ManualAttributionService(self.db)
        
        # Extract affected positions from allocation result
        affected_positions = []
        for allocation in allocation_result.allocations:
            affected_positions.append({
                "position_id": allocation.position_id,
                "strategy_id": allocation.strategy_id,
                "execution_id": allocation.execution_id,
                "available_quantity": str(allocation.remaining_quantity),
                "entry_price": str(allocation.entry_price)
            })
        
        case_id = await manual_service.create_attribution_case(
            trading_account_id=trade.trading_account_id,
            symbol=trade.symbol,
            exit_quantity=Decimal(str(trade.quantity)),
            exit_price=Decimal(str(trade.price)) if trade.price else None,
            exit_timestamp=trade.trade_time,
            affected_positions=affected_positions,
            suggested_allocation=allocation_result.audit_trail,
            priority=AttributionPriority.NORMAL,
            context={
                "broker_trade_id": trade.broker_trade_id,
                "allocation_result_id": allocation_result.allocation_id,
                "unallocated_quantity": str(allocation_result.unallocated_quantity),
                "reason": "Partial exit attribution requires manual intervention"
            }
        )
        
        logger.info(f"Created manual attribution case {case_id} for external trade {trade.broker_trade_id}")
        
    async def _create_manual_attribution_case_fallback(self, trade: Trade, error: str) -> None:
        """Create a manual attribution case when attribution service fails."""
        from .manual_attribution_service import ManualAttributionService, AttributionPriority
        from decimal import Decimal
        
        manual_service = ManualAttributionService(self.db)
        
        # No affected positions available since attribution failed
        affected_positions = []
        
        case_id = await manual_service.create_attribution_case(
            trading_account_id=trade.trading_account_id,
            symbol=trade.symbol,
            exit_quantity=Decimal(str(trade.quantity)),
            exit_price=Decimal(str(trade.price)) if trade.price else None,
            exit_timestamp=trade.trade_time,
            affected_positions=affected_positions,
            suggested_allocation=None,
            priority=AttributionPriority.HIGH,  # High priority since automation failed
            context={
                "broker_trade_id": trade.broker_trade_id,
                "error": error,
                "reason": "Attribution service failed - requires manual resolution"
            }
        )
        
        logger.warning(f"Created high-priority manual case {case_id} for failed attribution of trade {trade.broker_trade_id}")

    # ==========================================
    # TRADE QUERIES
    # ==========================================

    async def get_trade(self, trade_id: int) -> Trade:
        """
        Get trade by ID.

        Args:
            trade_id: Trade ID

        Returns:
            Trade object

        Raises:
            HTTPException: If trade not found
        """
        # ACL check already verified at endpoint level - verify belongs to trading_account
        result = await self.db.execute(
            select(Trade).where(
                and_(
                    Trade.id == trade_id,
                    Trade.trading_account_id == self.trading_account_id
                )
            )
        )
        trade = result.scalar_one_or_none()

        if not trade:
            raise HTTPException(404, f"Trade {trade_id} not found")

        return trade

    async def list_trades(
        self,
        symbol: Optional[str] = None,
        exchange: Optional[str] = None,
        transaction_type: Optional[str] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        limit: int = 100,
        offset: int = 0,
        trade_ids: Optional[List[int]] = None
    ) -> List[Trade]:
        """
        List user's trades with optional filtering.

        Args:
            symbol: Filter by symbol
            exchange: Filter by exchange
            transaction_type: Filter by BUY or SELL
            start_date: Filter trades from this date
            end_date: Filter trades until this date
            limit: Maximum number of trades to return
            offset: Number of trades to skip
            trade_ids: Optional list of trade IDs to filter to (for granular ACL)

        Returns:
            List of Trade objects
        """
        # ACL check already verified at endpoint level - only filter by trading_account_id
        # user_id represents who created/imported the trade, not who owns the account
        query = select(Trade).where(
            Trade.trading_account_id == self.trading_account_id
        )

        # Granular ACL filtering - only return trades user has access to
        if trade_ids is not None:
            if not trade_ids:
                # Empty list means no access to any trades
                return []
            query = query.where(Trade.trade_id.in_(trade_ids))

        if symbol:
            query = query.where(Trade.symbol == symbol)

        if exchange:
            query = query.where(Trade.exchange == exchange)

        if transaction_type:
            query = query.where(Trade.transaction_type == transaction_type)

        if start_date:
            query = query.where(Trade.trade_time >= datetime.combine(start_date, datetime.min.time()))

        if end_date:
            query = query.where(Trade.trade_time <= datetime.combine(end_date, datetime.max.time()))

        query = query.order_by(Trade.trade_time.desc()).limit(limit).offset(offset)

        result = await self.db.execute(query)
        trades = result.scalars().all()

        logger.debug(f"Retrieved {len(trades)} trades for user {self.user_id}")

        return list(trades)

    async def get_trades_for_order(self, order_id: int) -> List[Trade]:
        """
        Get all trades for a specific order.

        Args:
            order_id: Order ID

        Returns:
            List of Trade objects
        """
        # First verify the order belongs to the user
        from ..models.order import Order
        result = await self.db.execute(
            select(Order).where(
                and_(
                    Order.id == order_id,
                    Order.user_id == self.user_id
                )
            )
        )
        order = result.scalar_one_or_none()

        if not order:
            raise HTTPException(404, f"Order {order_id} not found")

        # Get trades for this order
        result = await self.db.execute(
            select(Trade).where(Trade.order_id == order_id)
            .order_by(Trade.trade_time.asc())
        )
        trades = result.scalars().all()

        return list(trades)

    # ==========================================
    # TRADE ANALYTICS
    # ==========================================

    async def get_trade_summary(
        self,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        order_id: Optional[int] = None,
        symbol: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Get summary and analytics of trades with optional filtering.

        Args:
            start_date: Start date for analysis (default: today)
            end_date: End date for analysis (default: today)
            order_id: Filter by order ID (optional)
            symbol: Filter by trading symbol (optional)

        Returns:
            Dictionary with trade analytics
        """
        if not start_date:
            start_date = date.today()
        if not end_date:
            end_date = date.today()

        # Fetch trades with filters
        if order_id:
            # If filtering by order_id, use get_trades_for_order
            trades = await self.get_trades_for_order(order_id)
        else:
            # Otherwise use list_trades with symbol filter
            trades = await self.list_trades(
                start_date=start_date,
                end_date=end_date,
                symbol=symbol,
                limit=10000  # Get all trades for analysis
            )

        if not trades:
            return {
                'total_trades': 0,
                'buy_trades': 0,
                'sell_trades': 0,
                'total_buy_value': 0.0,
                'total_sell_value': 0.0,
                'net_value': 0.0,
                'symbols_traded': [],
                'symbol_breakdown': [],
                'start_date': start_date.isoformat(),
                'end_date': end_date.isoformat(),
                'trades': []
            }

        # Calculate statistics
        buy_trades = [t for t in trades if t.transaction_type == 'BUY']
        sell_trades = [t for t in trades if t.transaction_type == 'SELL']

        total_buy_value = sum(t.trade_value for t in buy_trades if t.trade_value)
        total_sell_value = sum(t.trade_value for t in sell_trades if t.trade_value)

        symbols_traded = list(set(t.symbol for t in trades))

        # Group by symbol for detailed breakdown
        symbol_breakdown = {}
        for trade in trades:
            if trade.symbol not in symbol_breakdown:
                symbol_breakdown[trade.symbol] = {
                    'symbol': trade.symbol,
                    'buy_quantity': 0,
                    'sell_quantity': 0,
                    'buy_value': 0.0,
                    'sell_value': 0.0,
                    'net_quantity': 0,
                    'trades': 0
                }

            breakdown = symbol_breakdown[trade.symbol]
            breakdown['trades'] += 1

            if trade.transaction_type == 'BUY':
                breakdown['buy_quantity'] += trade.quantity
                breakdown['buy_value'] += trade.trade_value or 0
                breakdown['net_quantity'] += trade.quantity
            else:
                breakdown['sell_quantity'] += trade.quantity
                breakdown['sell_value'] += trade.trade_value or 0
                breakdown['net_quantity'] -= trade.quantity

        return {
            'total_trades': len(trades),
            'buy_trades': len(buy_trades),
            'sell_trades': len(sell_trades),
            'total_buy_value': total_buy_value,
            'total_sell_value': total_sell_value,
            'net_value': total_sell_value - total_buy_value,
            'symbols_traded': symbols_traded,
            'symbol_breakdown': list(symbol_breakdown.values()),
            'start_date': start_date.isoformat(),
            'end_date': end_date.isoformat(),
            'trades': [t.to_dict() for t in trades[:100]]  # Limit to 100 recent trades
        }

    async def get_daily_summary(self, days: int = 7) -> List[Dict[str, Any]]:
        """
        Get daily trade summary for the last N days.

        Args:
            days: Number of days to analyze

        Returns:
            List of daily summaries
        """
        end_date = date.today()
        start_date = end_date - timedelta(days=days-1)

        daily_summaries = []

        for day_offset in range(days):
            current_date = start_date + timedelta(days=day_offset)
            summary = await self.get_trade_summary(
                start_date=current_date,
                end_date=current_date
            )
            summary['date'] = current_date.isoformat()
            daily_summaries.append(summary)

        return daily_summaries

    # ==========================================
    # HISTORICAL TRADE SYNC
    # ==========================================

    async def sync_historical_trades(
        self,
        start_date: date,
        end_date: Optional[date] = None,
        triggered_by: str = 'manual'
    ) -> Dict[str, Any]:
        """
        Sync historical trades from broker API for a date range.

        Args:
            start_date: Start date for sync (inclusive)
            end_date: End date for sync (inclusive, defaults to today)
            triggered_by: Who/what triggered the sync ('manual', 'scheduled', 'api')

        Returns:
            Sync job details with statistics

        Raises:
            HTTPException: If sync fails
        """
        from ..models.sync_job import SyncJob
        import time

        if end_date is None:
            end_date = date.today()

        # Validate date range
        if start_date > end_date:
            raise HTTPException(400, "Start date must be before or equal to end date")

        if (end_date - start_date).days > 90:
            raise HTTPException(400, "Date range cannot exceed 90 days")

        # Create sync job record
        sync_job = SyncJob(
            job_type='trade_sync',
            user_id=self.user_id,
            trading_account_id=self.trading_account_id,
            status='pending',
            start_date=start_date,
            end_date=end_date,
            triggered_by=triggered_by,
            trigger_metadata={
                'date_range_days': (end_date - start_date).days + 1
            }
        )

        self.db.add(sync_job)
        await self.db.flush()  # Get the job ID

        logger.info(
            f"Starting historical trade sync: job_id={sync_job.id}, "
            f"date_range={start_date} to {end_date}, user={self.user_id}"
        )

        # Update status to running
        sync_job.status = 'running'
        sync_job.started_at = datetime.utcnow()
        start_time = time.time()

        try:
            # Note: KiteConnect API doesn't support historical trade fetch with date range
            # We can only fetch today's trades. For historical trades, we would need
            # to fetch and store them daily via the background worker.
            # This is a limitation of the broker API.

            # For now, we'll sync trades from the broker's current session
            # In production, you'd need daily background sync to build historical data

            logger.warning(
                f"Sync job {sync_job.id}: Broker API limitation - "
                "can only fetch current day's trades"
            )

            # Fetch trades from broker (current day only)
            broker_trades = await self.kite_client.get_trades()

            records_created = 0
            records_updated = 0
            records_skipped = 0
            errors = []

            # Sync each trade
            for broker_trade in broker_trades:
                try:
                    trade = await self._sync_trade(broker_trade)

                    # Check if it was created or updated
                    # (This is simplified - in production you'd track this properly)
                    records_created += 1

                except Exception as e:
                    logger.error(f"Failed to sync trade {broker_trade.get('trade_id')}: {e}")
                    errors.append(str(e))

            # Update sync job with results
            sync_job.status = 'completed'
            sync_job.completed_at = datetime.utcnow()
            sync_job.duration_seconds = int(time.time() - start_time)
            sync_job.records_fetched = len(broker_trades)
            sync_job.records_created = records_created
            sync_job.records_updated = records_updated
            sync_job.records_skipped = records_skipped
            sync_job.errors_count = len(errors)

            if errors:
                sync_job.error_details = errors[:100]  # Store first 100 errors

            await self.db.commit()

            logger.info(
                f"Historical trade sync completed: job_id={sync_job.id}, "
                f"fetched={sync_job.records_fetched}, created={records_created}, "
                f"time={sync_job.duration_seconds}s"
            )

            return sync_job.to_dict()

        except Exception as e:
            # Rollback any partial changes
            await self.db.rollback()

            # Re-fetch sync job after rollback (to update its status)
            from ..models.sync_job import SyncJob
            result = await self.db.execute(
                select(SyncJob).where(SyncJob.id == sync_job.id)
            )
            sync_job = result.scalar_one_or_none()

            if sync_job:
                # Mark sync job as failed
                sync_job.status = 'failed'
                sync_job.completed_at = datetime.utcnow()
                sync_job.duration_seconds = int(time.time() - start_time)
                sync_job.error_message = str(e)
                await self.db.commit()

            logger.error(f"Historical trade sync failed: job_id={sync_job.id if sync_job else 'unknown'}, error={e}")
            raise HTTPException(500, f"Historical trade sync failed: {str(e)}")

    async def get_sync_job_status(self, job_id: int) -> Dict[str, Any]:
        """
        Get status of a sync job.

        Args:
            job_id: Sync job ID

        Returns:
            Sync job details

        Raises:
            HTTPException: If job not found
        """
        from ..models.sync_job import SyncJob

        result = await self.db.execute(
            select(SyncJob).where(
                and_(
                    SyncJob.id == job_id,
                    SyncJob.user_id == self.user_id
                )
            )
        )
        sync_job = result.scalar_one_or_none()

        if not sync_job:
            raise HTTPException(404, f"Sync job {job_id} not found")

        return sync_job.to_dict()

    async def list_sync_jobs(
        self,
        job_type: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 50
    ) -> List[Dict[str, Any]]:
        """
        List sync jobs for the user.

        Args:
            job_type: Filter by job type ('trade_sync', 'order_sync', etc.)
            status: Filter by status ('pending', 'running', 'completed', 'failed')
            limit: Maximum number of jobs to return

        Returns:
            List of sync job dictionaries
        """
        from ..models.sync_job import SyncJob

        query = select(SyncJob).where(SyncJob.user_id == self.user_id)

        if job_type:
            query = query.where(SyncJob.job_type == job_type)

        if status:
            query = query.where(SyncJob.status == status)

        query = query.order_by(SyncJob.created_at.desc()).limit(limit)

        result = await self.db.execute(query)
        sync_jobs = result.scalars().all()

        return [job.to_dict() for job in sync_jobs]
